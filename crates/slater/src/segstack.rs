// SPDX-License-Identifier: Apache-2.0
//! The immutable **core stack** under the write-delta.
//!
//! A served image is a *generation set* (`docs/SEGMENTED-CORE-PLAN.md`): one large
//! clustered **base generation** (whose columnar readers live directly on
//! [`Generation`](crate::generation::Generation)) plus a bounded LSM stack of small
//! immutable **upper core segments**, each the O(delta) at-rest product of a flush. This
//! module owns the *segment* half of that stack — loading each segment's readers and the
//! [`Extents`] routing table that maps a dense id to its owning set member.
//!
//! # Precedence
//! A read resolves newest-wins: the write-delta first (patches / tombstones / born rows),
//! then the upper segments **newest → oldest** (each holds *full rows*, so the newest
//! segment carrying an id wins in a single record read — no cross-segment fold), then the
//! base generation. `segments` is ordered **oldest → newest**, so index `i` is
//! [`SegmentOrd::Upper(i)`] and a newest-first probe iterates it in reverse.
//!
//! # Singleton
//! A graph with no flushes yet (every graph today, until the Phase 4 flush lands) has an
//! empty segment list and a singleton [`Extents`] — [`CoreStack::is_singleton`] is `true`
//! and every read short-circuits straight to the base, byte-identical to the pre-stack
//! path. This is the at-rest adapter Phase 3's read seams build on; it does not itself
//! change any read.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use graph_format::blockcache::BlockCache as GfBlockCache;
use graph_format::crypto::{self, BlockCipher};
use graph_format::extents::Extents;
use graph_format::ids::Generation as GenId;
use graph_format::segindex::SegmentIndexReader;
use graph_format::segmanifest::SegmentManifest;
use graph_format::segment::SegmentReader;
use graph_format::segpostings::SegmentPostingsReader;
use graph_format::setmanifest::SetManifest;
use graph_format::store::{join_key, ObjectStore};

/// Default byte budget for the per-image segment block cache when the opener supplies no
/// explicit one (tests and non-server openers). The server threads its configured
/// range-index budget through instead; segments are only present post-flush, so this bound
/// only ever applies to a hand-built fixture.
const DEFAULT_SEGMENT_CACHE_BYTES: usize = 8 << 20;

/// The store key prefix of a segment's directory: `<graph>/segments/<uuid>`.
pub fn segment_prefix(graph: &str, uuid: GenId) -> String {
    join_key(graph, &format!("segments/{uuid}"))
}

/// One loaded upper core segment: its signed manifest plus the section, index and posting
/// readers. `index`/`postings` are `None` when the flush touched no indexed property / no
/// edges (the fragment file is simply absent).
pub struct LoadedSegment {
    pub manifest: SegmentManifest,
    pub reader: SegmentReader,
    pub index: Option<SegmentIndexReader>,
    pub postings: Option<SegmentPostingsReader>,
}

impl std::fmt::Debug for LoadedSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedSegment")
            .field("uuid", &self.manifest.segment_uuid)
            .field("node_band", &self.manifest.node_band)
            .field("edge_band", &self.manifest.edge_band)
            .field("indexed", &self.index.is_some())
            .field("postings", &self.postings.is_some())
            .finish()
    }
}

/// The immutable core stack of a served set: the ordered upper segments and the id→member
/// routing table. The base generation's readers are held by [`Generation`] itself; this
/// carries only what stacks *over* the base.
#[derive(Debug)]
pub struct CoreStack {
    /// Oldest → newest; index `i` corresponds to [`SegmentOrd::Upper(i)`].
    segments: Vec<LoadedSegment>,
    /// Node- and edge-id → owning member routing tables (base band + one band per segment).
    extents: Extents,
}

impl CoreStack {
    /// The empty stack of a singleton set: no segments, a single-band [`Extents`] over the
    /// base. Every read short-circuits to the base — behaviourally identical to a bare
    /// generation.
    pub fn singleton(base_node_count: u64, base_edge_count: u64) -> Self {
        Self {
            segments: Vec::new(),
            extents: Extents::singleton(base_node_count, base_edge_count),
        }
    }

    /// Load the upper segments a set declares. Reads each `SEGMENT.json`, authenticates it
    /// (content hash, marginals, and MAC when a key is configured and the segment carries
    /// one), derives its block cipher, and opens its readers through `store` under
    /// `<graph>/segments/<uuid>/`. Returns a singleton stack when the set has no segments.
    ///
    /// `cache_budget` sizes the shared block cache that pages every segment's sections
    /// (`None`/`0` ⇒ [`DEFAULT_SEGMENT_CACHE_BYTES`]); one cache is built and shared across
    /// all of this set's segments so they age out of a single eviction domain.
    #[allow(clippy::too_many_arguments)] // mirrors Generation::open's open-time context
    pub fn load(
        store: &dyn ObjectStore,
        graph: &str,
        set: &SetManifest,
        base_node_count: u64,
        base_edge_count: u64,
        master_key: Option<&[u8]>,
        verify_integrity: bool,
        cache_budget: Option<usize>,
    ) -> Result<Self> {
        let extents =
            Extents::from_set(set, base_node_count, base_edge_count).with_context(|| {
                format!("build routing extents for set {} of {graph}", set.set_uuid)
            })?;

        let cache = Arc::new(GfBlockCache::new(
            cache_budget
                .filter(|&n| n > 0)
                .unwrap_or(DEFAULT_SEGMENT_CACHE_BYTES),
        ));
        let mut segments = Vec::with_capacity(set.segments.len());
        for seg_ref in &set.segments {
            let uuid = seg_ref.uuid;
            let prefix = segment_prefix(graph, uuid);

            let manifest = SegmentManifest::read_via(store, graph, uuid)
                .with_context(|| format!("read SEGMENT.json for segment {uuid} of {graph}"))?;

            // Authenticate the manifest before trusting any field. The keyed MAC (when a key
            // is configured and the segment carries one) authenticates the content hash, the
            // file inventory, bands and the encryption header — mirroring the base MANIFEST.
            if let Some(key) = master_key {
                if manifest.mac.is_some() {
                    manifest.verify_mac(key).with_context(|| {
                        format!("verify SEGMENT.json MAC for segment {uuid} of {graph}")
                    })?;
                }
            }
            if verify_integrity {
                manifest.verify_content_hash().with_context(|| {
                    format!("verify SEGMENT.json content hash for segment {uuid}")
                })?;
            }
            manifest
                .verify_marginals()
                .with_context(|| format!("verify SEGMENT.json marginals for segment {uuid}"))?;

            // Cross-check the set's SegmentRef against the segment's own manifest: a set that
            // disagrees with the segment it points at (bands, uuid) is a corrupt/forged image.
            if manifest.segment_uuid != uuid {
                bail!(
                    "segment {uuid} of {graph} carries manifest uuid {} — set/segment mismatch",
                    manifest.segment_uuid
                );
            }
            if manifest.node_band != seg_ref.node_band || manifest.edge_band != seg_ref.edge_band {
                bail!(
                    "segment {uuid} of {graph} bands {:?}/{:?} disagree with the set's {:?}/{:?}",
                    manifest.node_band,
                    manifest.edge_band,
                    seg_ref.node_band,
                    seg_ref.edge_band
                );
            }

            let cipher = derive_segment_cipher(&manifest, master_key, graph, uuid)?;
            let reader = SegmentReader::open_via(store, &prefix, cache.clone(), cipher.clone())
                .with_context(|| format!("open segment {uuid} sections"))?;
            let index = SegmentIndexReader::open_if_present_via(store, &prefix, cipher.clone())
                .with_context(|| format!("open segment {uuid} index fragments"))?;
            let postings = SegmentPostingsReader::open_if_present_via(store, &prefix)
                .with_context(|| format!("open segment {uuid} posting fragments"))?;

            segments.push(LoadedSegment {
                manifest,
                reader,
                index,
                postings,
            });
        }

        Ok(Self { segments, extents })
    }

    /// `true` when the set carries no upper segments (every graph until the Phase 4 flush).
    pub fn is_singleton(&self) -> bool {
        self.segments.is_empty()
    }

    /// The loaded upper segments, **oldest → newest**. Iterate in reverse for a newest-wins
    /// probe.
    pub fn segments(&self) -> &[LoadedSegment] {
        &self.segments
    }

    /// The id → owning-member routing tables (node and edge id spaces).
    pub fn extents(&self) -> &Extents {
        &self.extents
    }
}

/// Derive a segment's at-rest block cipher from its `SEGMENT.json` encryption header and
/// the runtime master key — the segment analogue of the base generation's `derive_cipher`.
/// `None` for a plaintext segment (any key is then ignored); an error if an encrypted
/// segment is opened without a key or with an unimplemented AEAD/KDF.
fn derive_segment_cipher(
    manifest: &SegmentManifest,
    master_key: Option<&[u8]>,
    graph: &str,
    uuid: GenId,
) -> Result<Option<Arc<BlockCipher>>> {
    let Some(header) = &manifest.encryption else {
        return Ok(None);
    };
    if header.aead != crypto::AEAD_NAME {
        bail!(
            "segment {uuid} of {graph} uses AEAD {:?}, which this build does not implement",
            header.aead
        );
    }
    if header.kdf != crypto::KDF_NAME {
        bail!(
            "segment {uuid} of {graph} uses KDF {:?}, which this build does not implement",
            header.kdf
        );
    }
    let key = master_key.ok_or_else(|| {
        anyhow::anyhow!(
            "segment {uuid} of {graph} is encrypted at rest but no key was supplied \
             (set config.encryption.keyEnv or keyFile)"
        )
    })?;
    let salt = crypto::hex_decode(&header.salt_hex)
        .with_context(|| format!("decode encryption salt for segment {uuid}"))?;
    Ok(Some(Arc::new(BlockCipher::from_master(key, &salt))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::extents::SegmentOrd;
    use graph_format::ids::Value;
    use graph_format::manifest::FileEntry;
    use graph_format::segmanifest::{SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION};
    use graph_format::segment::{EdgeRow, NodeRow, SegmentWriter};
    use graph_format::setmanifest::SegmentRef;
    use graph_format::store::fs::FsObjectStore;

    fn gid(n: u128) -> GenId {
        GenId(uuid::Uuid::from_u128(n))
    }

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("slater_segstack_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    /// A one-node (dense id 3) / one-edge (id 2) segment plus a self-consistent SEGMENT.json,
    /// written under `<root>/<graph>/segments/<seg>/`. `node_band`/`edge_band` let a caller
    /// deliberately desynchronise the SegmentRef from the on-disk manifest.
    fn write_segment(
        root: &std::path::Path,
        graph: &str,
        seg: GenId,
        base: GenId,
        node_band: (u64, u64),
        edge_band: (u64, u64),
    ) -> SegmentManifest {
        let seg_dir = root.join(graph).join("segments").join(seg.to_string());
        std::fs::create_dir_all(seg_dir.parent().unwrap()).unwrap();
        let mut w = SegmentWriter::create(&seg_dir, 0xABCD, 4096, 3).unwrap();
        w.push_node(
            3,
            &NodeRow {
                labels: vec!["Person".into()],
                props: vec![("name".into(), Value::Str("Zed".into()))],
                tombstoned: false,
            },
        )
        .unwrap();
        w.push_edge(
            2,
            &EdgeRow {
                src: 0,
                dst: 3,
                reltype: "KNOWS".into(),
                props: vec![],
                tombstoned: false,
            },
        )
        .unwrap();
        w.finish().unwrap();

        let mut m = SegmentManifest {
            magic: SEGMENT_MAGIC.into(),
            version: SEGMENT_MANIFEST_VERSION,
            segment_uuid: seg,
            base,
            created_unix: 0,
            node_band,
            edge_band,
            content_hash: String::new(),
            encryption: None,
            node_count_delta: 1,
            edge_count_delta: 1,
            reltype_edge_deltas: vec![("KNOWS".into(), 1)],
            label_node_deltas: vec![("Person".into(), 1)],
            marginals_exact: true,
            dirty_indexes: vec![],
            mac: None,
            files: vec![FileEntry {
                name: "node.blk".into(),
                bytes: 0,
                blake3: "aa".into(),
                sha256: None,
                crc32c: None,
            }],
        };
        m.set_content_hash();
        m.write_to_dir(&seg_dir).unwrap();
        m
    }

    #[test]
    fn singleton_stack_short_circuits() {
        let s = CoreStack::singleton(3, 2);
        assert!(s.is_singleton());
        assert!(s.segments().is_empty());
        assert_eq!(s.extents().nodes.route(0), Some(SegmentOrd::Base));
        assert_eq!(s.extents().nodes.route(2), Some(SegmentOrd::Base));
        assert_eq!(s.extents().nodes.route(3), None, "past the base node count");
        assert_eq!(s.extents().edges.route(1), Some(SegmentOrd::Base));
        assert_eq!(s.extents().edges.route(2), None);
    }

    #[test]
    fn loads_a_stacked_set() {
        let (root, graph) = (tmp("load"), "g");
        let (base, seg) = (gid(1), gid(2));
        let m = write_segment(&root, graph, seg, base, (3, 4), (2, 3));
        let mut set = SetManifest::singleton(base, 0);
        set.set_uuid = gid(3);
        set.segments = vec![SegmentRef::from_manifest(&m)];

        let store = FsObjectStore::new(&root);
        let stack = CoreStack::load(&store, graph, &set, 3, 2, None, true, None).unwrap();

        assert!(!stack.is_singleton());
        assert_eq!(stack.segments().len(), 1);
        // Routing: base owns [0,3) nodes / [0,2) edges; the segment owns the appended band.
        assert_eq!(stack.extents().nodes.route(0), Some(SegmentOrd::Base));
        assert_eq!(stack.extents().nodes.route(3), Some(SegmentOrd::Upper(0)));
        assert_eq!(stack.extents().edges.route(1), Some(SegmentOrd::Base));
        assert_eq!(stack.extents().edges.route(2), Some(SegmentOrd::Upper(0)));
        // The loaded segment serves the born node's full row and misses on a base id.
        let row = stack.segments()[0].reader.node_row(3).unwrap().unwrap();
        assert_eq!(row.labels, vec!["Person".to_string()]);
        assert_eq!(
            row.props,
            vec![("name".to_string(), Value::Str("Zed".into()))]
        );
        assert!(stack.segments()[0].reader.node_row(0).unwrap().is_none());
        assert_eq!(stack.segments()[0].manifest.node_count_delta, 1);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn rejects_set_segment_band_mismatch() {
        // The set's SegmentRef bands must agree with the segment's own manifest. Give the
        // ref a band that still tiles ([3,5)) but disagrees with the manifest's ([3,4)).
        let (root, graph) = (tmp("mismatch"), "g");
        let (base, seg) = (gid(1), gid(2));
        let m = write_segment(&root, graph, seg, base, (3, 4), (2, 3));
        let mut seg_ref = SegmentRef::from_manifest(&m);
        seg_ref.node_band = (3, 5);
        let mut set = SetManifest::singleton(base, 0);
        set.set_uuid = gid(3);
        set.segments = vec![seg_ref];

        let store = FsObjectStore::new(&root);
        let err = CoreStack::load(&store, graph, &set, 3, 2, None, true, None).unwrap_err();
        assert!(format!("{err:#}").contains("disagree"), "{err:#}");
        let _ = std::fs::remove_dir_all(&root);
    }
}
