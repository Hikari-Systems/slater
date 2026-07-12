// SPDX-License-Identifier: Apache-2.0
//! The **segment manifest** (`SEGMENT.json`) — a core segment's self-description:
//! its id bands, signed marginal deltas, per-index dirty bits, file inventory, and
//! integrity (content hash + optional AEAD header + keyed-MAC), exactly parallel to the
//! generation [`Manifest`](crate::manifest::Manifest) (the segmented-core track; see
//! `docs/SEGMENTED-CORE-PLAN.md`).
//!
//! Written last by a flush (after every section/fragment file is fsynced and hashed) and
//! validated first when a segment is opened. The set manifest points at the segment; this
//! manifest authenticates the segment's own bytes.
//!
//! # Signed marginals
//! A segment records **deltas** (`i64`, signed — a delete is negative) against the base's
//! marginals: `node_count_delta`, `edge_count_delta`, and sparse per-reltype / per-label
//! deltas (keyed by name, so a segment may introduce a reltype/label the base never had).
//! At open the read path *sums* these over the base totals. `marginals_exact = false` is
//! the "decline, never wrong" escape hatch: a flush that cannot prove its deltas exact
//! clears the flag and every count fast path skips this segment and scans. This slice
//! defines and round-trips the fields (with a self-consistency invariant); the summation /
//! decline logic is the Phase 3 read path.
//!
//! # Per-index dirty bits
//! [`SegmentManifest::dirty_indexes`] lists the `(label, property)` range indexes this
//! segment carries a fragment for (see [`crate::segindex`]). A probe consults a segment's
//! ISAM fragment + removal sidecar only for a dirty index; a clean index reads base-only.
//!
//! # Integrity
//! `content_hash` is BLAKE3 over the file inventory (every section/fragment file, not
//! `SEGMENT.json` itself), identical to the generation manifest. `mac` is the keyed-BLAKE3
//! over the canonicalised manifest under the same master-key-derived subkey, sealed last
//! and verified first — it blanket-covers every field including `content_hash`, the bands,
//! the marginals, and the encryption header.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::ids::Generation;
use crate::manifest::{EncryptionHeader, FileEntry};
use crate::store::{join_key, ObjectStore};

/// Magic at the head of `SEGMENT.json`, distinct from a generation MANIFEST (`SLATER01`)
/// and a set manifest (`SLSET01`).
pub const SEGMENT_MAGIC: &str = "SLSEG01";

/// Segment-manifest schema version. Bumped on any incompatible change; a reader refuses a
/// version it does not understand.
pub const SEGMENT_MANIFEST_VERSION: u32 = 1;

/// One `(label, property)` range index this segment carries a fragment for — a per-index
/// "dirty bit". `fragment` names the segment's ISAM file (`idx_<k>.isam`, see
/// [`crate::segindex`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirtyIndex {
    pub label: String,
    pub property: String,
    pub fragment: String,
}

/// The `SEGMENT.json` manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentManifest {
    /// `"SLSEG01"` — quick sniff before trusting the JSON.
    pub magic: String,
    pub version: u32,
    /// This segment's uuid (its directory under `<graph>/segments/<uuid>/`).
    pub segment_uuid: Generation,
    /// The base generation this segment deltas over — for provenance and integrity.
    pub base: Generation,
    pub created_unix: i64,
    /// Node id band this segment owns, `[start, end)`.
    pub node_band: (u64, u64),
    /// Edge id band this segment owns, `[start, end)`.
    pub edge_band: (u64, u64),
    /// BLAKE3 content hash over the file inventory (excludes `SEGMENT.json` itself).
    pub content_hash: String,
    #[serde(default)]
    pub encryption: Option<EncryptionHeader>,

    // ── signed marginals (deltas against the base) ────────────────────────────────────
    /// Net change in node count (born − tombstoned).
    pub node_count_delta: i64,
    /// Net change in edge count (born − removed).
    pub edge_count_delta: i64,
    /// Sparse per-reltype edge-count deltas (`reltype → Δ`). A new reltype is allowed.
    #[serde(default)]
    pub reltype_edge_deltas: Vec<(String, i64)>,
    /// Sparse per-label node-occurrence deltas (`label → Δ`). A new label is allowed.
    #[serde(default)]
    pub label_node_deltas: Vec<(String, i64)>,
    /// Sparse per-node **out**-degree deltas (`node_id → born − removed`), listing only
    /// nodes whose `|Δ| >=` the build hub-degree floor, ascending by id. Lets the hub
    /// probe add a segment's degree contribution in O(1) per segment (binary search) with
    /// no adjacency read. A node absent (its `|Δ|` was below the floor, or the manifest
    /// predates this field) contributes 0 — safe: only a million-edge hub matters, and any
    /// single flush that creates one records it (`|Δ| ≥ floor`); a missed node's segment
    /// degree is bounded by `~floor × #segments` and materialises cheaply.
    #[serde(default)]
    pub hub_degree_out_deltas: Vec<(u64, i64)>,
    /// Sparse per-node **in**-degree deltas — the reverse-direction counterpart of
    /// [`Self::hub_degree_out_deltas`].
    #[serde(default)]
    pub hub_degree_in_deltas: Vec<(u64, i64)>,
    /// Whether the marginals are provably exact. `false` ⇒ the read path declines every
    /// count fast path for this segment and scans (the "empty ⇒ decline, never wrong"
    /// discipline). Defaults to `false` so an under-specified manifest is safe.
    #[serde(default)]
    pub marginals_exact: bool,

    // ── per-index dirty bits ──────────────────────────────────────────────────────────
    /// Range indexes this segment carries a fragment for; a probe consults the segment
    /// only for these. Empty ⇒ the segment touched no indexed property.
    #[serde(default)]
    pub dirty_indexes: Vec<DirtyIndex>,

    /// The set of labels whose **node membership** this segment changes relative to the base:
    /// a node gains or loses the label, is born carrying it, or is tombstoned while carrying
    /// it. Resident and sorted, so a whole-graph label scan can **skip** a segment that
    /// provably preserves a label's membership (no block reads) rather than decoding its every
    /// touched row. `None` ⇒ *unknown* (a manifest predating this field, or a decline) and the
    /// reader must not skip; `Some(set)` is authoritative. `Some(empty)` means the segment
    /// changes no node's label membership at all (a pure property/edge patch).
    #[serde(default)]
    pub label_membership_touch: Option<Vec<String>>,

    /// Keyed-BLAKE3 MAC (hex) over the canonicalised manifest. `None` ⇒ plaintext segment.
    #[serde(default)]
    pub mac: Option<String>,
    /// Inventory of section/fragment files (everything except `SEGMENT.json`).
    pub files: Vec<FileEntry>,
}

impl SegmentManifest {
    /// The backend-relative key of `SEGMENT.json` for `segment_uuid` under `graph`.
    pub fn key(graph: &str, segment_uuid: Generation) -> String {
        join_key(graph, &format!("segments/{}/SEGMENT.json", segment_uuid.0))
    }

    /// Validate magic + version. Called after every read before the fields are trusted.
    pub fn validate(&self) -> Result<()> {
        if self.magic != SEGMENT_MAGIC {
            bail!(
                "not a segment manifest: magic {:?} != {SEGMENT_MAGIC:?}",
                self.magic
            );
        }
        if self.version != SEGMENT_MANIFEST_VERSION {
            bail!(
                "unsupported segment-manifest version {} (this build understands {SEGMENT_MANIFEST_VERSION})",
                self.version
            );
        }
        Ok(())
    }

    /// Recompute the content hash over `files` and compare to `content_hash`.
    pub fn verify_content_hash(&self) -> Result<()> {
        let inv: Vec<(String, String)> = self
            .files
            .iter()
            .map(|f| (f.name.clone(), f.blake3.clone()))
            .collect();
        let computed = crate::integrity::content_hash(&inv);
        if computed != self.content_hash {
            bail!(
                "segment content hash mismatch (declared {}, recomputed {})",
                self.content_hash,
                computed
            );
        }
        Ok(())
    }

    /// Recompute the content hash over `files` and store it in `content_hash`. Call before
    /// [`seal_mac`](Self::seal_mac).
    pub fn set_content_hash(&mut self) {
        let inv: Vec<(String, String)> = self
            .files
            .iter()
            .map(|f| (f.name.clone(), f.blake3.clone()))
            .collect();
        self.content_hash = crate::integrity::content_hash(&inv);
    }

    /// Check the marginals' internal consistency: when `marginals_exact`, the sparse
    /// per-reltype edge deltas must sum to `edge_count_delta` (mirroring the generation
    /// manifest's `sum(reltype_edge_counts) == edge_count`). Label deltas may exceed the
    /// node delta (a multi-label node contributes to each label), so they are *not*
    /// summed. A non-exact manifest is not checked — it is already declined.
    pub fn verify_marginals(&self) -> Result<()> {
        if !self.marginals_exact {
            return Ok(());
        }
        let rt_sum: i64 = self.reltype_edge_deltas.iter().map(|(_, d)| *d).sum();
        if rt_sum != self.edge_count_delta {
            bail!(
                "segment marginals inconsistent: reltype edge deltas sum to {rt_sum} \
                 but edge_count_delta is {} (exact marginals must reconcile)",
                self.edge_count_delta
            );
        }
        Ok(())
    }

    /// Whether this segment may change node membership in `label`. `true` when the touch set
    /// is **unknown** (`None` — conservative: the reader must fold the segment) or explicitly
    /// lists `label`; `false` only when an authoritative touch set omits it. A whole-graph
    /// label scan folds a segment only when this is `true`.
    pub fn membership_touches(&self, label: &str) -> bool {
        match &self.label_membership_touch {
            None => true,
            Some(set) => set.binary_search_by(|l| l.as_str().cmp(label)).is_ok(),
        }
    }

    /// The canonical byte string the MAC is computed over: this manifest with `mac`
    /// cleared, serialised compactly. Deterministic (serde fixes field order; every
    /// collection is an order-stable `Vec`).
    fn mac_message(&self) -> Result<Vec<u8>> {
        let mut canon = self.clone();
        canon.mac = None;
        serde_json::to_vec(&canon).context("serialise segment manifest for MAC")
    }

    /// Compute the keyed-BLAKE3 MAC under the master-key-derived subkey and store it in
    /// `mac`. Call **last** — after `content_hash` and every other field is final.
    pub fn seal_mac(&mut self, master_key: &[u8]) -> Result<()> {
        let key = crate::crypto::derive_manifest_mac_key(master_key);
        self.mac = Some(crate::crypto::manifest_mac(&key, &self.mac_message()?));
        Ok(())
    }

    /// Recompute the MAC and compare it to the stored `mac`. Errors if `mac` is absent
    /// (callers gate on presence first).
    pub fn verify_mac(&self, master_key: &[u8]) -> Result<()> {
        let stored = self.mac.as_deref().ok_or_else(|| {
            anyhow::anyhow!("segment manifest carries no MAC but one was required")
        })?;
        let key = crate::crypto::derive_manifest_mac_key(master_key);
        let computed = crate::crypto::manifest_mac(&key, &self.mac_message()?);
        if computed != stored {
            bail!("segment manifest MAC mismatch — refusing to open a tampered segment");
        }
        Ok(())
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).context("serialise segment manifest")
    }

    /// Serialise to bytes (`to_json`) for the caller to write locally and/or upload — the
    /// write path stays with the flush so it controls fsync/atomicity.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.to_json()?.into_bytes())
    }

    /// Write `SEGMENT.json` into `dir`.
    pub fn write_to_dir(&self, dir: impl AsRef<Path>) -> Result<()> {
        let path = dir.as_ref().join("SEGMENT.json");
        std::fs::write(&path, self.to_json()?)
            .with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }

    /// Read and validate `SEGMENT.json` from `dir`.
    pub fn read_from_dir(dir: impl AsRef<Path>) -> Result<Self> {
        let path = dir.as_ref().join("SEGMENT.json");
        let text =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let m: SegmentManifest =
            serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        m.validate()?;
        Ok(m)
    }

    /// Read and validate `SEGMENT.json` for `segment_uuid` under `graph` in any backend.
    pub fn read_via(
        store: &dyn ObjectStore,
        graph: &str,
        segment_uuid: Generation,
    ) -> Result<Self> {
        let key = Self::key(graph, segment_uuid);
        let bytes = store
            .read_all(&key)
            .with_context(|| format!("read {key}"))?;
        let m: SegmentManifest =
            serde_json::from_slice(&bytes).with_context(|| format!("parse {key}"))?;
        m.validate()?;
        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(n: u128) -> Generation {
        Generation(uuid::Uuid::from_u128(n))
    }

    fn sample() -> SegmentManifest {
        let files = vec![
            FileEntry {
                name: "node.blk".into(),
                bytes: 200,
                blake3: "aa".into(),
                sha256: None,
                crc32c: None,
            },
            FileEntry {
                name: "edge.blk".into(),
                bytes: 100,
                blake3: "bb".into(),
                sha256: None,
                crc32c: None,
            },
        ];
        let mut m = SegmentManifest {
            magic: SEGMENT_MAGIC.into(),
            version: SEGMENT_MANIFEST_VERSION,
            segment_uuid: uuid(2),
            base: uuid(1),
            created_unix: 1_800_000_000,
            node_band: (50, 60),
            edge_band: (200, 205),
            content_hash: String::new(),
            encryption: None,
            node_count_delta: 10,
            edge_count_delta: 5,
            reltype_edge_deltas: vec![("KNOWS".into(), 3), ("IN".into(), 2)],
            label_node_deltas: vec![("Person".into(), 8), ("City".into(), 4)],
            hub_degree_out_deltas: vec![],
            hub_degree_in_deltas: vec![],
            marginals_exact: true,
            dirty_indexes: vec![DirtyIndex {
                label: "Person".into(),
                property: "age".into(),
                fragment: "idx_0.isam".into(),
            }],
            label_membership_touch: Some(vec!["City".into(), "Person".into()]),
            mac: None,
            files,
        };
        m.set_content_hash();
        m
    }

    #[test]
    fn json_roundtrips_and_validates() {
        let m = sample();
        let json = m.to_json().unwrap();
        let back: SegmentManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
        back.validate().unwrap();
    }

    #[test]
    fn content_hash_verifies_and_detects_tamper() {
        let mut m = sample();
        m.verify_content_hash().unwrap();
        m.files[0].blake3 = "cc".into();
        assert!(m.verify_content_hash().is_err());
    }

    #[test]
    fn marginals_consistency_enforced_when_exact() {
        let m = sample();
        m.verify_marginals().unwrap(); // 3 + 2 == 5
        let mut bad = sample();
        bad.edge_count_delta = 6; // no longer == 3 + 2
        assert!(bad.verify_marginals().is_err());
        // The same imbalance is tolerated when the marginals are not claimed exact.
        bad.marginals_exact = false;
        bad.verify_marginals().unwrap();
    }

    #[test]
    fn signed_deltas_roundtrip_negative() {
        let mut m = sample();
        m.node_count_delta = -4; // a net-delete flush
        m.edge_count_delta = -2;
        m.reltype_edge_deltas = vec![("KNOWS".into(), -2)];
        m.set_content_hash();
        let back: SegmentManifest = serde_json::from_str(&m.to_json().unwrap()).unwrap();
        assert_eq!(back.node_count_delta, -4);
        assert_eq!(back.reltype_edge_deltas, vec![("KNOWS".to_string(), -2)]);
        back.verify_marginals().unwrap(); // -2 == -2
    }

    #[test]
    fn mac_seal_verify_and_tamper() {
        let key = b"operator master key";
        let mut base = sample();
        base.seal_mac(key).unwrap();
        base.verify_mac(key).unwrap();

        let check = |what: &str, tamper: &dyn Fn(&mut SegmentManifest)| {
            let mut m = base.clone();
            tamper(&mut m);
            assert!(
                m.verify_mac(key).is_err(),
                "tampering with {what} must break the MAC"
            );
        };
        check("content_hash", &|m| m.content_hash = "00".into());
        check("node_band", &|m| m.node_band = (0, 1));
        check("edge_count_delta", &|m| m.edge_count_delta = 999);
        check("dirty index", &|m| m.dirty_indexes.clear());
        check("file hash", &|m| m.files[0].blake3 = "zz".into());
        check("base uuid", &|m| m.base = uuid(999));
    }

    #[test]
    fn mac_rejects_wrong_key_and_absence() {
        let mut m = sample();
        m.seal_mac(b"key A").unwrap();
        assert!(m.verify_mac(b"key B").is_err());
        let plain = sample(); // mac: None
        assert!(plain.verify_mac(b"key").is_err());
    }

    #[test]
    fn rejects_foreign_magic_and_version() {
        let mut m = sample();
        m.magic = "NOPE".into();
        assert!(m.validate().is_err());
        let mut m2 = sample();
        m2.version = SEGMENT_MANIFEST_VERSION + 1;
        assert!(m2.validate().is_err());
    }

    #[test]
    fn optional_fields_default_when_absent() {
        // A minimal manifest missing the additive keys still deserialises.
        let mut v = serde_json::to_value(sample()).unwrap();
        let obj = v.as_object_mut().unwrap();
        for k in [
            "reltypeEdgeDeltas",
            "labelNodeDeltas",
            "marginalsExact",
            "dirtyIndexes",
            "labelMembershipTouch",
            "encryption",
            "mac",
        ] {
            obj.remove(k);
        }
        let back: SegmentManifest = serde_json::from_value(v).unwrap();
        assert!(back.reltype_edge_deltas.is_empty());
        assert!(back.label_node_deltas.is_empty());
        assert!(!back.marginals_exact);
        assert!(back.dirty_indexes.is_empty());
        // Absent ⇒ unknown ⇒ the reader must not skip (membership_touches is true).
        assert!(back.label_membership_touch.is_none());
        assert!(back.membership_touches("anything"));
        assert!(back.encryption.is_none());
        assert!(back.mac.is_none());
    }

    #[test]
    fn write_read_dir_and_via_store() {
        use crate::store::mem::MemObjectStore;
        let dir = std::env::temp_dir().join(format!("slater_segman_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let m = sample();
        m.write_to_dir(&dir).unwrap();
        let back = SegmentManifest::read_from_dir(&dir).unwrap();
        assert_eq!(m, back);
        let _ = std::fs::remove_dir_all(&dir);

        let store = MemObjectStore::new();
        store
            .put(
                &SegmentManifest::key("g", m.segment_uuid),
                &m.to_bytes().unwrap(),
                None,
            )
            .unwrap();
        let via = SegmentManifest::read_via(&store, "g", m.segment_uuid).unwrap();
        assert_eq!(via, m);
    }

    #[test]
    fn key_path_is_under_segments() {
        assert_eq!(
            SegmentManifest::key("mygraph", uuid(0)),
            "mygraph/segments/00000000-0000-0000-0000-000000000000/SEGMENT.json"
        );
    }
}
