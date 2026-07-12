// SPDX-License-Identifier: Apache-2.0
//! The **generation set** manifest — the indirection that turns the core from a
//! single immutable image into a bounded stack of immutable segments (the
//! segmented-core track; see `docs/SEGMENTED-CORE-PLAN.md`).
//!
//! `<graph>/current` names a **set** uuid; `<graph>/sets/<set-uuid>.json` is this
//! manifest, listing the **base** generation (the large clustered image
//! `slater-build` produces) plus an ordered stack of **upper segments** (each the
//! O(delta) product of a flush). A reader opens the base generation's `.blk` files
//! as before and folds the segments over it.
//!
//! # Phase 1 (this file): singleton only
//! A set currently always has exactly one base and **zero** segments, so behaviour is
//! identical to the pre-set format. Critically, in a singleton the set uuid, the base
//! uuid, and the generation-directory uuid are the **same** value, so `current` keeps
//! naming the generation directory and nothing that reads `current` breaks. A reader
//! that finds no `sets/<uuid>.json` treats `<uuid>` as an implicit singleton (base =
//! `<uuid>`, no segments) — the on-disk fallback for fixtures and older images. Real
//! segments and diverging (set uuid ≠ base uuid) sets arrive in later phases.
//!
//! # Integrity
//! The set manifest is a small pointer; the *data* it points at (the base generation,
//! and later each segment) is authenticated by that image's own `MANIFEST`/`SEGMENT`
//! MAC + per-block AEAD + the server's ACL stamp on open. A `mac` field is reserved
//! for authenticating the set pointer itself; wiring it is a later hardening step.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::ids::Generation;
use crate::store::{join_key, ObjectStore};

/// Magic at the head of a set manifest, distinguishing it from a generation
/// `MANIFEST` (`SLATER01`).
pub const SET_MAGIC: &str = "SLSET01";

/// Set-manifest schema version. Bumped on any incompatible change; a reader refuses a
/// version it does not understand.
pub const SET_VERSION: u32 = 1;

/// A reference to one upper core segment in the set's stack (oldest→newest). Unused in
/// Phase 1 (the stack is always empty); the shape is forward-looking so Phase 2 can
/// populate it without a schema break. Every field beyond the essentials is
/// `#[serde(default)]` so older manifests deserialise unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRef {
    /// The segment's own uuid (its directory under `<graph>/segments/<uuid>/`).
    pub uuid: Generation,
    /// Node id band this segment owns, `[start, end)`.
    #[serde(default)]
    pub node_band: (u64, u64),
    /// Edge id band this segment owns, `[start, end)`.
    #[serde(default)]
    pub edge_band: (u64, u64),
    /// The segment's content hash (its `SEGMENT.json` self-hash), for integrity.
    #[serde(default)]
    pub content_hash: String,
}

impl SegmentRef {
    /// Build the set-level reference to a segment from its own `SEGMENT.json` — copying
    /// the uuid, id bands and content hash the set needs to route and integrity-check it.
    /// This is the bridge that lets a flush append a `SegmentRef` to the set from the
    /// segment manifest it just sealed (Phase 4).
    pub fn from_manifest(m: &crate::segmanifest::SegmentManifest) -> Self {
        Self {
            uuid: m.segment_uuid,
            node_band: m.node_band,
            edge_band: m.edge_band,
            content_hash: m.content_hash.clone(),
        }
    }
}

/// The `<graph>/sets/<set-uuid>.json` manifest: the base generation plus the ordered
/// upper-segment stack.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetManifest {
    pub magic: String,
    pub version: u32,
    /// This set's uuid (== the value in `<graph>/current`).
    pub set_uuid: Generation,
    /// The base generation's uuid — its `.blk` files live under `<graph>/<base>/`.
    pub base: Generation,
    /// Upper segments, oldest→newest. Empty in Phase 1.
    #[serde(default)]
    pub segments: Vec<SegmentRef>,
    pub created_unix: i64,
    /// Reserved: keyed-MAC over the canonical manifest, authenticating the set
    /// pointer. `None` for a plaintext set; verification wiring is a later step.
    #[serde(default)]
    pub mac: Option<String>,
}

impl SetManifest {
    /// A singleton set over `base` (no upper segments), with the set uuid equal to the
    /// base uuid so `current` keeps naming the generation directory.
    pub fn singleton(base: Generation, created_unix: i64) -> Self {
        Self {
            magic: SET_MAGIC.to_string(),
            version: SET_VERSION,
            set_uuid: base,
            base,
            segments: Vec::new(),
            created_unix,
            mac: None,
        }
    }

    /// The backend-relative key of the set manifest for `set_uuid` under `graph`.
    pub fn key(graph: &str, set_uuid: Generation) -> String {
        join_key(graph, &format!("sets/{}.json", set_uuid.0))
    }

    /// Validate magic + version. Called after every read before the fields are trusted.
    pub fn validate(&self) -> Result<()> {
        if self.magic != SET_MAGIC {
            bail!(
                "not a set manifest: magic {:?} != {SET_MAGIC:?}",
                self.magic
            );
        }
        if self.version != SET_VERSION {
            bail!(
                "unsupported set-manifest version {} (this build understands {SET_VERSION})",
                self.version
            );
        }
        Ok(())
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).context("serialise set manifest")
    }

    /// Serialise this bytes-form (`to_json`), for the caller to write locally and/or
    /// upload — the write path stays with the publisher so it controls fsync/atomicity.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.to_json()?.into_bytes())
    }

    /// Read and validate `<graph>/sets/<set_uuid>.json` from any backend.
    pub fn read_via(store: &dyn ObjectStore, graph: &str, set_uuid: Generation) -> Result<Self> {
        let key = Self::key(graph, set_uuid);
        let bytes = store
            .read_all(&key)
            .with_context(|| format!("read {key}"))?;
        let m: SetManifest =
            serde_json::from_slice(&bytes).with_context(|| format!("parse {key}"))?;
        m.validate()?;
        Ok(m)
    }

    /// Whether a set manifest exists for `set_uuid` under `graph` (the reader uses this
    /// to distinguish a real set from an implicit singleton — a bare generation uuid).
    pub fn exists_via(store: &dyn ObjectStore, graph: &str, set_uuid: Generation) -> bool {
        store.exists(&Self::key(graph, set_uuid)).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::mem::MemObjectStore;

    fn uuid(n: u128) -> Generation {
        Generation(uuid::Uuid::from_u128(n))
    }

    #[test]
    fn singleton_ties_set_to_base() {
        let base = uuid(0x1234);
        let s = SetManifest::singleton(base, 42);
        assert_eq!(s.set_uuid, base);
        assert_eq!(s.base, base);
        assert!(s.segments.is_empty());
        s.validate().unwrap();
    }

    #[test]
    fn segment_ref_from_manifest_and_tiles() {
        use crate::extents::{Extents, SegmentOrd};
        use crate::segmanifest::SegmentManifest;

        // Two flushes over a base of 50 nodes / 200 edges, appending contiguous bands.
        let base = uuid(1);
        let mut seg1 = SegmentManifest {
            magic: crate::segmanifest::SEGMENT_MAGIC.into(),
            version: crate::segmanifest::SEGMENT_MANIFEST_VERSION,
            segment_uuid: uuid(2),
            base,
            created_unix: 0,
            node_band: (50, 60),
            edge_band: (200, 205),
            content_hash: String::new(),
            encryption: None,
            node_count_delta: 10,
            edge_count_delta: 5,
            reltype_edge_deltas: vec![],
            label_node_deltas: vec![],
            hub_degree_out_deltas: vec![],
            hub_degree_in_deltas: vec![],
            marginals_exact: false,
            dirty_indexes: vec![],
            label_membership_touch: None,
            mac: None,
            files: vec![],
        };
        seg1.set_content_hash();
        let mut seg2 = seg1.clone();
        seg2.segment_uuid = uuid(3);
        seg2.node_band = (60, 63);
        seg2.edge_band = (205, 205);
        seg2.set_content_hash();

        let mut set = SetManifest::singleton(base, 0);
        set.segments.push(SegmentRef::from_manifest(&seg1));
        set.segments.push(SegmentRef::from_manifest(&seg2));

        assert_eq!(set.segments[0].uuid, uuid(2));
        assert_eq!(set.segments[0].node_band, (50, 60));
        assert_eq!(set.segments[0].content_hash, seg1.content_hash);

        // The populated refs must build a valid, tiling routing table.
        let e = Extents::from_set(&set, 50, 200).unwrap();
        assert_eq!(e.nodes.route(55), Some(SegmentOrd::Upper(0)));
        assert_eq!(e.nodes.route(62), Some(SegmentOrd::Upper(1)));
        assert_eq!(e.nodes.total(), 63);
        assert_eq!(e.edges.route(204), Some(SegmentOrd::Upper(0)));
    }

    #[test]
    fn json_roundtrip() {
        let mut s = SetManifest::singleton(uuid(7), 100);
        s.segments.push(SegmentRef {
            uuid: uuid(8),
            node_band: (1000, 1010),
            edge_band: (2000, 2005),
            content_hash: "abc".into(),
        });
        let json = s.to_json().unwrap();
        let back: SetManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
        back.validate().unwrap();
    }

    #[test]
    fn read_write_via_store() {
        let store = MemObjectStore::new();
        let base = uuid(0xabcd);
        let s = SetManifest::singleton(base, 5);
        assert!(!SetManifest::exists_via(&store, "g", base));
        store
            .put(&SetManifest::key("g", base), &s.to_bytes().unwrap(), None)
            .unwrap();
        assert!(SetManifest::exists_via(&store, "g", base));
        let back = SetManifest::read_via(&store, "g", base).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn rejects_foreign_magic_and_version() {
        let store = MemObjectStore::new();
        let base = uuid(1);
        let mut s = SetManifest::singleton(base, 0);
        s.magic = "NOTASET".into();
        store
            .put(&SetManifest::key("g", base), &s.to_bytes().unwrap(), None)
            .unwrap();
        let err = match SetManifest::read_via(&store, "g", base) {
            Ok(_) => panic!("expected a foreign-magic refusal"),
            Err(e) => e,
        };
        assert!(format!("{err:#}").contains("not a set manifest"));

        let mut s2 = SetManifest::singleton(base, 0);
        s2.version = SET_VERSION + 1;
        store
            .put(&SetManifest::key("g2", base), &s2.to_bytes().unwrap(), None)
            .unwrap();
        let err = match SetManifest::read_via(&store, "g2", base) {
            Ok(_) => panic!("expected an unsupported-version refusal"),
            Err(e) => e,
        };
        assert!(format!("{err:#}").contains("unsupported set-manifest version"));
    }
}
