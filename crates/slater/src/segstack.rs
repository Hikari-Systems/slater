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
use graph_format::ids::{Generation as GenId, Value};
use graph_format::segindex::SegmentIndexReader;
use graph_format::segmanifest::SegmentManifest;
use graph_format::segment::{EdgeRow, NodeRow, SegmentReader};
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

    /// Resolve node `id`'s **full effective row** as the core stack sees it (below the
    /// write-delta): the newest segment that carries the id wins in a single record read —
    /// segments hold full rows, so there is no cross-segment fold. Returns:
    /// - `Some(row)` — a segment carries `id` (`row.tombstoned` = the flush deleted it);
    /// - `None` — no segment touches `id`, so the caller reads the base generation.
    ///
    /// A binary-search miss inside a segment costs no I/O (the `may_hold_node` fence + the
    /// resident key column gate it), so an untouched id skips the whole stack in
    /// O(#segments) resident checks. Instant `None` for a singleton set.
    pub fn resolve_node_row(&self, id: u64) -> Result<Option<NodeRow>> {
        for seg in self.segments.iter().rev() {
            if seg.reader.may_hold_node(id) {
                if let Some(row) = seg.reader.node_row(id)? {
                    return Ok(Some(row));
                }
            }
        }
        Ok(None)
    }

    /// Resolve edge `id`'s full effective row over the stack — the edge mirror of
    /// [`resolve_node_row`](Self::resolve_node_row).
    pub fn resolve_edge_row(&self, id: u64) -> Result<Option<EdgeRow>> {
        for seg in self.segments.iter().rev() {
            if seg.reader.may_hold_edge(id) {
                if let Some(row) = seg.reader.edge_row(id)? {
                    return Ok(Some(row));
                }
            }
        }
        Ok(None)
    }

    /// Whether node `id` is effectively deleted by the stack: the newest segment carrying it
    /// is a tombstone. `false` for a live override, a born row, or a base-only id.
    pub fn is_node_tombstoned(&self, id: u64) -> Result<bool> {
        Ok(self.resolve_node_row(id)?.is_some_and(|r| r.tombstoned))
    }

    // ── signed marginals (summed across the stack, for the count fast paths) ────────────

    /// Whether every segment's `SEGMENT.json` marginals are provably exact. A count fast
    /// path may sum the stack's marginals only when this holds; otherwise it declines and
    /// falls back to full execution (the "empty ⇒ decline, never wrong" discipline). Vacuously
    /// `true` for a singleton set.
    pub fn marginals_exact(&self) -> bool {
        self.segments.iter().all(|s| s.manifest.marginals_exact)
    }

    /// Net change in node count across the stack (Σ each segment's born − tombstoned).
    pub fn node_count_delta(&self) -> i64 {
        self.segments
            .iter()
            .map(|s| s.manifest.node_count_delta)
            .sum()
    }

    /// Net change in edge count across the stack.
    pub fn edge_count_delta(&self) -> i64 {
        self.segments
            .iter()
            .map(|s| s.manifest.edge_count_delta)
            .sum()
    }

    /// Net change in the occurrence count of `label` across the stack.
    pub fn label_node_delta(&self, label: &str) -> i64 {
        self.segments
            .iter()
            .flat_map(|s| &s.manifest.label_node_deltas)
            .filter(|(l, _)| l == label)
            .map(|(_, d)| *d)
            .sum()
    }

    /// Net change in `reltype`'s edge count across the stack.
    pub fn reltype_edge_delta(&self, reltype: &str) -> i64 {
        self.segments
            .iter()
            .flat_map(|s| &s.manifest.reltype_edge_deltas)
            .filter(|(t, _)| t == reltype)
            .map(|(_, d)| *d)
            .sum()
    }

    /// The distinct reltype names carrying a delta anywhere in the stack — so a group-by /
    /// enumeration can surface a reltype a flush introduced that the base never had.
    pub fn segment_reltype_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .segments
            .iter()
            .flat_map(|s| {
                s.manifest
                    .reltype_edge_deltas
                    .iter()
                    .map(|(t, _)| t.clone())
            })
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    /// Fold the stack's index fragments into a base equality-probe result `ids`, oldest→
    /// newest: each segment first **suppresses** the base/older ids it supersedes (its
    /// `removals` sidecar), then **unions** its own matching fragment ids. Processing oldest
    /// →newest makes a newer flush's value win over an older flush's.
    ///
    /// Correctness rests on a Phase-4 writer obligation: a segment's `removals` must list
    /// every id whose indexed value it supersedes (base *or* an older segment's), not only
    /// base ids — so the `retain` drops any stale earlier contribution. `ids` need not be
    /// sorted on entry; the caller re-sorts after (the delta overlay relies on it).
    pub fn fold_index_eq(
        &self,
        ids: &mut Vec<u64>,
        label: &str,
        prop: &str,
        key: &Value,
    ) -> Result<()> {
        for seg in &self.segments {
            let Some(idx) = &seg.index else { continue };
            let removals = idx.removals(label, prop);
            if !removals.is_empty() {
                ids.retain(|id| removals.binary_search(id).is_err());
            }
            // The fence skips the leaf-block decompress when `key` can't be in this fragment;
            // it never gates the removal suppress above (which is by id, not value).
            if idx.may_hold_eq(label, prop, key) {
                ids.extend(idx.lookup_eq(label, prop, key)?);
            }
        }
        Ok(())
    }

    /// Batch counterpart of [`fold_index_eq`](Self::fold_index_eq): fold the stack over a
    /// whole **sorted, distinct** key list in one merge-join sweep per fragment, instead of
    /// a point probe per key. `ids[i]` enters carrying the base equality-probe result for
    /// `keys[i]` (from [`IsamReader::lookup_eq_sorted`]); on return each has the stack folded
    /// in oldest→newest — every id vec has the segment's `removals` suppressed (by id, value-
    /// independent, so the same removal set applies to every key), then its fence-gated
    /// fragment sweep unioned. The result is **exactly** what `keys.len()` calls to
    /// `fold_index_eq` leave (removal-then-union in the same order), but at one block
    /// decompress per touched fragment block for the whole batch — the bulk-write ISAM floor
    /// (memory `bulk-delete-isam-resolve-floor`). The caller re-sorts/dedups each `ids[i]`.
    pub fn fold_index_eq_batch(
        &self,
        ids: &mut [Vec<u64>],
        label: &str,
        prop: &str,
        keys: &[&Value],
    ) -> Result<()> {
        debug_assert_eq!(ids.len(), keys.len(), "ids must align with keys");
        for seg in &self.segments {
            let Some(idx) = &seg.index else { continue };
            let removals = idx.removals(label, prop);
            if !removals.is_empty() {
                for v in ids.iter_mut() {
                    v.retain(|id| removals.binary_search(id).is_err());
                }
            }
            // One fence-gated sweep for the whole batch (each out-of-fence key skips its
            // leaf-block read); union each key's hits into its accumulator.
            for (v, hits) in ids.iter_mut().zip(idx.lookup_eq_sorted(label, prop, keys)?) {
                v.extend(hits);
            }
        }
        Ok(())
    }

    /// Range-probe counterpart of [`fold_index_eq`](Self::fold_index_eq).
    #[allow(clippy::too_many_arguments)]
    pub fn fold_index_range(
        &self,
        ids: &mut Vec<u64>,
        label: &str,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Result<()> {
        for seg in &self.segments {
            let Some(idx) = &seg.index else { continue };
            let removals = idx.removals(label, prop);
            if !removals.is_empty() {
                ids.retain(|id| removals.binary_search(id).is_err());
            }
            if idx.may_hold_range(label, prop, lo, lo_inclusive, hi, hi_inclusive) {
                ids.extend(idx.lookup_range(label, prop, lo, lo_inclusive, hi, hi_inclusive)?);
            }
        }
        Ok(())
    }

    /// Fold the stack's effect on a base **label scan** into `ids`: every id the stack
    /// carries a row for has its base membership dropped and re-decided by its *effective*
    /// row (segments hold full label sets, so an override can add or drop a label, and a
    /// tombstone drops it entirely); born ids carrying the label are added. `ids` is not
    /// re-sorted here (the caller sorts/dedups after the delta overlay).
    pub fn fold_label_scan(&self, ids: &mut Vec<u64>, label: &str) -> Result<()> {
        if self.segments.is_empty() {
            return Ok(());
        }
        let mut touched: Vec<u64> = Vec::new();
        for seg in &self.segments {
            touched.extend_from_slice(seg.reader.node_ids());
        }
        touched.sort_unstable();
        touched.dedup();
        let touched_set: std::collections::HashSet<u64> = touched.iter().copied().collect();
        ids.retain(|id| !touched_set.contains(id));
        for id in touched {
            if let Some(row) = self.resolve_node_row(id)? {
                if !row.tombstoned && row.labels.iter().any(|l| l == label) {
                    ids.push(id);
                }
            }
        }
        Ok(())
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

    /// A one-node segment (born id 3) that also carries an index fragment over
    /// `(Person, name)`: entries `{"New"→1, "Zed"→3}` (fence `["New","Zed"]`) and a removal
    /// of base id 1 (its stale `"Old"` value is superseded by the flush). Lets a `fold_index_eq`
    /// test exercise the fence gate and the removal suppress together.
    fn write_indexed_segment(
        root: &std::path::Path,
        graph: &str,
        seg: GenId,
        base: GenId,
    ) -> SegmentManifest {
        use graph_format::segindex::{write_index_fragments, IndexSpec};
        let m = write_segment(root, graph, seg, base, (3, 4), (2, 3));
        let seg_dir = root.join(graph).join("segments").join(seg.to_string());
        write_index_fragments(
            &seg_dir,
            &[IndexSpec {
                label: "Person".into(),
                prop: "name".into(),
                entries: vec![(Value::Str("New".into()), 1), (Value::Str("Zed".into()), 3)],
                removals: vec![1],
            }],
            4096,
            3,
            None,
        )
        .unwrap();
        m
    }

    #[test]
    fn fold_index_eq_gates_on_the_fence_and_suppresses_removals() {
        let (root, graph) = (tmp("foldfence"), "g");
        let (base, seg) = (gid(1), gid(2));
        let m = write_indexed_segment(&root, graph, seg, base);
        let mut set = SetManifest::singleton(base, 0);
        set.set_uuid = gid(3);
        set.segments = vec![SegmentRef::from_manifest(&m)];
        let store = FsObjectStore::new(&root);
        let stack = CoreStack::load(&store, graph, &set, 3, 2, None, true, None).unwrap();

        // A base hit on the superseded value: removal drops it, fence admits "Old" (inside
        // ["New","Zed"]) but the fragment has no "Old" entry ⇒ the flushed node is gone.
        let mut ids = vec![1];
        stack
            .fold_index_eq(&mut ids, "Person", "name", &Value::Str("Old".into()))
            .unwrap();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(
            ids,
            Vec::<u64>::new(),
            "the moved-away value must not resolve"
        );

        // The new value now resolves to the patched id.
        let mut ids = vec![];
        stack
            .fold_index_eq(&mut ids, "Person", "name", &Value::Str("New".into()))
            .unwrap();
        assert_eq!(ids, vec![1]);

        // A born id resolves under its own value.
        let mut ids = vec![];
        stack
            .fold_index_eq(&mut ids, "Person", "name", &Value::Str("Zed".into()))
            .unwrap();
        assert_eq!(ids, vec![3]);

        // A key below the fence ("Aaa" < "New") is a certain miss — the fold skips the
        // fragment's ISAM read and returns the (empty) base result unchanged.
        let mut ids = vec![];
        stack
            .fold_index_eq(&mut ids, "Person", "name", &Value::Str("Aaa".into()))
            .unwrap();
        assert_eq!(ids, Vec::<u64>::new());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn fold_index_eq_batch_matches_point_folds() {
        // The batch fold over a stacked segment must be byte-identical to N point folds —
        // same removal suppress, same fence gating, same union — for every probed key.
        let (root, graph) = (tmp("foldbatch"), "g");
        let (base, seg) = (gid(1), gid(2));
        let m = write_indexed_segment(&root, graph, seg, base);
        let mut set = SetManifest::singleton(base, 0);
        set.set_uuid = gid(3);
        set.segments = vec![SegmentRef::from_manifest(&m)];
        let store = FsObjectStore::new(&root);
        let stack = CoreStack::load(&store, graph, &set, 3, 2, None, true, None).unwrap();

        // A sorted, distinct key list: below-fence miss, the superseded value (base id 1 in
        // `removals`, no fragment entry ⇒ gone), the patched value, and the born value.
        let keys = [
            Value::Str("Aaa".into()),
            Value::Str("New".into()),
            Value::Str("Old".into()),
            Value::Str("Zed".into()),
        ];
        // Per-key base probe result (base id 1 carries the stale "Old" value pre-flush).
        let base_ids = |k: &Value| -> Vec<u64> {
            if *k == Value::Str("Old".into()) {
                vec![1]
            } else {
                vec![]
            }
        };
        let refs: Vec<&Value> = keys.iter().collect();

        // Batch fold.
        let mut batch: Vec<Vec<u64>> = keys.iter().map(base_ids).collect();
        stack
            .fold_index_eq_batch(&mut batch, "Person", "name", &refs)
            .unwrap();
        for v in &mut batch {
            v.sort_unstable();
            v.dedup();
        }

        // N point folds.
        let single: Vec<Vec<u64>> = keys
            .iter()
            .map(|k| {
                let mut ids = base_ids(k);
                stack.fold_index_eq(&mut ids, "Person", "name", k).unwrap();
                ids.sort_unstable();
                ids.dedup();
                ids
            })
            .collect();

        assert_eq!(batch, single, "batch fold diverges from point folds");
        // And the fold is what we expect: Old gone, New→1, Zed→3, Aaa empty.
        assert_eq!(
            batch,
            vec![vec![], vec![1u64], vec![], vec![3u64]],
            "batch fold verdict"
        );
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
