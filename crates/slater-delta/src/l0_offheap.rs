// SPDX-License-Identifier: Apache-2.0
//! Off-heap L0 segment — the RSS-bounded reader for a sealed delta level.
//!
//! The resident [`L0Segment`](crate::l0::L0Segment) reloads a whole flushed memtable
//! into RAM. This variant instead spills the level to a **directory** of block files and
//! reads the bulk **off-heap**: per-entity payloads (`node`/`adjacency`/`edge`) are paged
//! on demand through the shared [`BlockCache`], and only a compact index stays resident.
//!
//! # Layout (a segment is a directory)
//! ```text
//! <seg>/node.blk       BlockFile: one record per patched/born node   (record order = dense-id order)
//! <seg>/adj_out.blk    BlockFile: one record per node with outgoing delta edges
//! <seg>/adj_in.blk     BlockFile: one record per node with incoming delta edges
//! <seg>/edge.blk       BlockFile: one record per edge carrying a delta (born or core-patch)
//! <seg>/meta.bin       resident: MAGIC ‖ crc32c ‖ { scalars, per-section key columns, secondaries }
//! ```
//! Each `.blk` section is read by **binary-searching a resident sorted `u64` key column**
//! (dense id / edge id) to a record index, then fetching that record through
//! `BlockCache::record` — so a *miss* (the overwhelming common case on the tombstone /
//! patch hot path) costs a resident binary search and **no I/O**, and a *hit* pages (and
//! caches) one block. Resident cost is the key columns (8 B/entry) plus the secondary
//! scan/write indexes; the per-entity payload bulk lives on disk.
//!
//! **Secondary indexes stay resident** (`born_by_label`, `born_index`, `core_patched`,
//! `born_by_identity`) — they back the scan-planning (`scan_candidates`) and write-path
//! (`MERGE` reuse) axes, are proportionally smaller, and blocking them is a mechanical
//! follow-up (they re-hold born *values* resident, the only unbounded term left, and only
//! for insert-heavy deltas). The hot read path (traversal, property overlay, tombstone
//! suppression) and every per-entity payload are fully off-heap.
//!
//! No back-compat: an L0 segment lives only between a flush and the next consolidation, so
//! the format may change freely; a magic/version/crc mismatch is a hard error on open.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use graph_format::blockcache::BlockCache;
use graph_format::blockfile::{BlockFileReader, BlockFileWriter};
use graph_format::ids::Value;
use graph_format::plane::{KeyColumn, PlaneCodecOpts};
use graph_format::wire::{capacity_for, read_uvarint, read_value, write_uvarint, write_value};

use crate::memtable::{DeltaEdge, DeltaSnapshot, EdgeDelta, LevelRead, Memtable, NodeDelta};
use crate::seal::{bind, frame_blob, l0_name, unframe_blob, DeltaCipher};

const META_MAGIC: &[u8; 8] = b"SLL0OFF1";
/// Magic prefix identifying a **sealed** off-heap `meta.bin` (HIK-146). The four block
/// sections seal themselves — `BlockFileWriter` carries its own encrypted magic — but the
/// meta is a plain blob and needs its own framing.
const META_MAGIC_SEALED: &[u8; 8] = b"SLL0OFFE";
/// How a refusal names an off-heap segment's meta to an operator.
const META_SUBJECT: &str = "off-heap L0 segment meta";

/// The four block-section file names, in a single place so the writer, the reader and the
/// per-file subkey derivation can never disagree about them.
const SECTION_FILES: [&str; 4] = ["node.blk", "adj_out.blk", "adj_in.blk", "edge.blk"];
/// v2 adds the resident `tombstoned` dense-id column, so a merged live `count(*)` can
/// enumerate this segment's suppressed rows without paging `node.blk`.
// v5: the four presence key columns (node/adj_out/adj_in/edge) moved from dense uvarint arrays
// to compact plane records (`KeyColumn`); the secondary id lists stay `w_u64s`. ~6× smaller
// resident key columns. Zero legacy L0 segments persist a version bump, so exact-match is safe.
const OFFHEAP_VERSION: u64 = 5;

/// Per-section cache discriminants (the `sub` in a [`BlockCache`] key). Distinct so the
/// four sections of one segment never collide in the shared cache.
const SUB_NODE: u32 = 0;
const SUB_ADJ_OUT: u32 = 1;
const SUB_ADJ_IN: u32 = 2;
const SUB_EDGE: u32 = 3;

/// A delta-born node's resident index material: its label, dense id, and the effective
/// `(indexed-property → value)` pairs (business key overlaid by patches, patches winning —
/// matching `Memtable::born_index_value`). Drives `born_ids_in_index_*` off the resident
/// index without paging.
#[derive(Debug, Clone)]
pub struct BornIndexEntry {
    pub label: String,
    pub id: u64,
    pub props: Vec<(String, Value)>,
}

/// The plain, owned data a [`Memtable`](crate::memtable::Memtable) hands to
/// [`write_segment`] — gathered through the memtable's own read methods, so an off-heap
/// segment answers reads identically to the resident level it was flushed from.
#[derive(Debug, Default)]
pub struct SegmentData {
    pub synthetic_base: u64,
    pub edge_synthetic_base: u64,
    pub node_delta_count: u64,
    pub edge_delta_count: u64,
    pub born_count: u64,
    pub born_edge_count: u64,
    /// `(dense id, label, key, key-value, delta)` — **sorted by dense id**.
    pub nodes: Vec<(u64, String, String, Value, NodeDelta)>,
    /// `(src dense id, outgoing delta edges)` — **sorted by src**.
    pub adj_out: Vec<(u64, Vec<DeltaEdge>)>,
    /// `(dst dense id, incoming delta edges)` — **sorted by dst**.
    pub adj_in: Vec<(u64, Vec<DeltaEdge>)>,
    /// `(edge id, delta)` — **sorted by edge id**.
    pub edges: Vec<(u64, EdgeDelta)>,
    /// `(core edge id, src dense, dst dense, reltype name)` for every in-place property
    /// patch of an existing **core** edge (a `SET r.p = v`). A patch leaves topology
    /// alone, so — unlike a born edge — the endpoints are absent from `adj_out`/`adj_in`;
    /// the core-segment flush writer needs them to materialise the edge's full replace
    /// row (base props ⊕ patch). The off-heap L0 writer ignores this field (a delta-shaped
    /// level reads a patched edge's value through `edge_delta_by_id`, never its endpoints).
    /// **sorted by core edge id.**
    pub core_patched_edges: Vec<(u64, u64, u64, String)>,
    /// `label → born dense ids` in born-allocation (ascending) order.
    pub born_by_label: Vec<(String, Vec<u64>)>,
    /// One entry per born node, in born-allocation order.
    pub born_index: Vec<BornIndexEntry>,
    /// `(label, patched-prop, core dense id)` for the moved-indexed-value overlay.
    pub core_patched: Vec<(String, String, u64)>,
    /// Identity-key bytes → born dense id, for `MERGE`-reuse resolution.
    pub born_by_identity: Vec<(Vec<u8>, u64)>,
}

// ── payload codecs (block record bodies) ────────────────────────────────────────────

fn w_str(buf: &mut Vec<u8>, s: &str) {
    write_uvarint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

fn r_str(r: &mut &[u8]) -> Result<String> {
    let n = read_uvarint(r)? as usize;
    if r.len() < n {
        bail!("l0 offheap: short string");
    }
    let s = std::str::from_utf8(&r[..n])
        .context("l0 offheap: invalid utf8")?
        .to_string();
    *r = &r[n..];
    Ok(s)
}

fn w_patches(
    buf: &mut Vec<u8>,
    patches: &std::collections::BTreeMap<String, Value>,
    tombstoned: bool,
) {
    buf.push(u8::from(tombstoned));
    write_uvarint(buf, patches.len() as u64);
    for (k, v) in patches {
        w_str(buf, k);
        write_value(buf, v);
    }
}

fn r_patches(r: &mut &[u8]) -> Result<(std::collections::BTreeMap<String, Value>, bool)> {
    if r.is_empty() {
        bail!("l0 offheap: short delta");
    }
    let tombstoned = r[0] != 0;
    *r = &r[1..];
    let n = read_uvarint(r)? as usize;
    let mut patches = std::collections::BTreeMap::new();
    for _ in 0..n {
        let k = r_str(r)?;
        let v = read_value(r)?;
        patches.insert(k, v);
    }
    Ok((patches, tombstoned))
}

/// Encode a node record body: `label ‖ key ‖ key-value ‖ delta`. The delta carries the
/// shared `(tombstoned, patches)` image plus the `replaced` flag and `removed`
/// property-name set (per-property removal + replace-all).
fn encode_node(label: &str, key: &str, value: &Value, delta: &NodeDelta) -> Vec<u8> {
    let mut buf = Vec::new();
    w_str(&mut buf, label);
    w_str(&mut buf, key);
    write_value(&mut buf, value);
    w_patches(&mut buf, &delta.patches, delta.tombstoned);
    buf.push(u8::from(delta.replaced));
    w_name_set(&mut buf, &delta.removed);
    w_name_set(&mut buf, &delta.labels_added);
    w_name_set(&mut buf, &delta.labels_removed);
    buf
}

fn w_name_set(buf: &mut Vec<u8>, set: &std::collections::BTreeSet<String>) {
    write_uvarint(buf, set.len() as u64);
    for name in set {
        w_str(buf, name);
    }
}

fn r_name_set(r: &mut &[u8]) -> Result<std::collections::BTreeSet<String>> {
    let n = read_uvarint(r)? as usize;
    let mut set = std::collections::BTreeSet::new();
    for _ in 0..n {
        set.insert(r_str(r)?);
    }
    Ok(set)
}

fn decode_node(mut r: &[u8]) -> Result<(String, String, Value, NodeDelta)> {
    let label = r_str(&mut r)?;
    let key = r_str(&mut r)?;
    let value = read_value(&mut r)?;
    let (patches, tombstoned) = r_patches(&mut r)?;
    if r.is_empty() {
        bail!("l0 offheap: short delta (missing replaced flag)");
    }
    let replaced = r[0] != 0;
    r = &r[1..];
    let removed = r_name_set(&mut r)?;
    let labels_added = r_name_set(&mut r)?;
    let labels_removed = r_name_set(&mut r)?;
    Ok((
        label,
        key,
        value,
        NodeDelta {
            patches,
            removed,
            replaced,
            labels_added,
            labels_removed,
            tombstoned,
        },
    ))
}

fn encode_adj(edges: &[DeltaEdge]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_uvarint(&mut buf, edges.len() as u64);
    for e in edges {
        write_uvarint(&mut buf, e.other);
        w_str(&mut buf, &e.reltype);
        match e.edge_id {
            Some(id) => {
                buf.push(1);
                write_uvarint(&mut buf, id);
            }
            None => buf.push(0),
        }
        buf.push(u8::from(e.tombstoned));
    }
    buf
}

fn decode_adj(mut r: &[u8]) -> Result<Vec<DeltaEdge>> {
    let n = read_uvarint(&mut r)? as usize;
    // `n` is an untrusted count out of an mmapped L0 record; each edge costs ≥1 byte, so clamp
    // the reservation to the bytes present (`wire::capacity_for`) — a forged count then runs
    // the record dry and errors instead of aborting the process in the allocator.
    let mut out = Vec::with_capacity(capacity_for(n, r.len(), 1));
    for _ in 0..n {
        let other = read_uvarint(&mut r)?;
        let reltype = r_str(&mut r)?;
        if r.is_empty() {
            bail!("l0 offheap: short adj edge_id tag");
        }
        let has_id = r[0] != 0;
        r = &r[1..];
        let edge_id = if has_id {
            Some(read_uvarint(&mut r)?)
        } else {
            None
        };
        if r.is_empty() {
            bail!("l0 offheap: short adj tombstone");
        }
        let tombstoned = r[0] != 0;
        r = &r[1..];
        out.push(DeltaEdge {
            other,
            reltype,
            edge_id,
            tombstoned,
        });
    }
    Ok(out)
}

fn encode_edge(delta: &EdgeDelta) -> Vec<u8> {
    let mut buf = Vec::new();
    w_patches(&mut buf, &delta.patches, delta.tombstoned);
    buf
}

fn decode_edge(mut r: &[u8]) -> Result<EdgeDelta> {
    let (patches, tombstoned) = r_patches(&mut r)?;
    Ok(EdgeDelta {
        patches,
        tombstoned,
    })
}

// ── writer ──────────────────────────────────────────────────────────────────────────

/// A **streaming** writer for an off-heap L0 segment. Payload records are appended to the
/// four block sections incrementally (`BlockFileWriter` buffers one block at a time), while
/// only the compact meta accumulates in RAM (scalars, per-section `u64` key columns, the
/// secondary indexes). So a *merge* streams sorted runs through here without ever holding
/// the merged payloads resident — the whole point of the disk-native compaction (D54).
/// `push_*` **must** be called in ascending key order per section (records are key-sorted).
/// The image is staged in a sibling `.tmp` directory and atomically `rename`d in at `finish`.
pub struct OffheapSegmentWriter {
    tmp: PathBuf,
    dir: PathBuf,
    /// The cipher for this segment's `meta.bin`, `None` on a plaintext deployment. Bound
    /// to the segment's **final** directory name, not the staging `.tmp` one.
    meta_cipher: Option<Arc<graph_format::crypto::FileCipher>>,
    /// A unique id for this written segment, persisted in the meta and used as the shared
    /// cache **scope** — fresh on every (re)write, so a compaction that reuses a segment
    /// directory can never collide with the pre-merge segment's stale cached blocks.
    scope: u128,
    node: BlockFileWriter,
    node_keys: Vec<u64>,
    adj_out: BlockFileWriter,
    adj_out_keys: Vec<u64>,
    adj_in: BlockFileWriter,
    adj_in_keys: Vec<u64>,
    edge: BlockFileWriter,
    edge_keys: Vec<u64>,
    synthetic_base: u64,
    edge_synthetic_base: u64,
    node_delta_count: u64,
    edge_delta_count: u64,
    born_count: u64,
    born_edge_count: u64,
    born_by_label: Vec<(String, Vec<u64>)>,
    born_index: Vec<BornIndexEntry>,
    core_patched: Vec<(String, String, u64)>,
    born_by_identity: Vec<(Vec<u8>, u64)>,
    /// `(core edge id, src dense, dst dense, reltype)` for every in-place core-edge property
    /// patch (v4). Persisted so a T2 flush over an off-heap L0 level can recover a patched
    /// edge's endpoints — absent from the adjacency, they would otherwise be lost.
    core_patched_edges: Vec<(u64, u64, u64, String)>,
    /// Dense ids this segment tombstones, accumulated from the pushed node deltas (so
    /// both the flush path and the streaming merge populate it for free) and kept
    /// resident in `meta.bin`.
    tombstoned: Vec<u64>,
    /// Whether any pushed edge is a tombstone, and the per-reltype born-edge tally — both
    /// derived from the outgoing adjacency records (every delta edge appears exactly once
    /// as an out-edge of its source), so the flush and merge paths populate them alike.
    edge_tombstones: bool,
    born_edges_by_reltype: BTreeMap<String, u64>,
    /// Dense ids that gained / dropped each label via `SET`/`REMOVE n:Label`, the full set
    /// of label-mutated ids, and whether any node carries a label mutation at all — all
    /// accumulated from the pushed node deltas (so the flush and streaming-merge paths
    /// populate them for free) and kept resident for the exact live-count overlay.
    added_label_ids: BTreeMap<String, Vec<u64>>,
    removed_label_ids: BTreeMap<String, Vec<u64>>,
    label_overlay_ids: Vec<u64>,
    has_label_overlay: bool,
}

impl OffheapSegmentWriter {
    pub fn create(
        dir: impl AsRef<Path>,
        scope: u128,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<&DeltaCipher>,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let tmp = dir.with_extension("tmp");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).with_context(|| format!("create L0 tmp dir {tmp:?}"))?;
        // Every subkey is derived from the segment's **final** directory name: the sections
        // are written into `<dir>.tmp/` and renamed into `<dir>/`, and the reader derives
        // from what it opens.
        let mk = |name: &str| -> Result<BlockFileWriter> {
            let p = tmp.join(name);
            let fc = bind(cipher, &l0_name(&dir, Some(name))?);
            BlockFileWriter::create_with_cipher(&p, target_block_bytes, zstd_level, fc)
                .with_context(|| format!("create block file {p:?}"))
        };
        let meta_cipher = bind(cipher, &l0_name(&dir, Some("meta.bin"))?);
        Ok(Self {
            node: mk(SECTION_FILES[0])?,
            node_keys: Vec::new(),
            adj_out: mk(SECTION_FILES[1])?,
            adj_out_keys: Vec::new(),
            adj_in: mk(SECTION_FILES[2])?,
            adj_in_keys: Vec::new(),
            edge: mk(SECTION_FILES[3])?,
            edge_keys: Vec::new(),
            tmp,
            dir,
            meta_cipher,
            scope,
            synthetic_base: 0,
            edge_synthetic_base: 0,
            node_delta_count: 0,
            edge_delta_count: 0,
            born_count: 0,
            born_edge_count: 0,
            born_by_label: Vec::new(),
            born_index: Vec::new(),
            core_patched: Vec::new(),
            born_by_identity: Vec::new(),
            core_patched_edges: Vec::new(),
            tombstoned: Vec::new(),
            edge_tombstones: false,
            born_edges_by_reltype: BTreeMap::new(),
            added_label_ids: BTreeMap::new(),
            removed_label_ids: BTreeMap::new(),
            label_overlay_ids: Vec::new(),
            has_label_overlay: false,
        })
    }

    pub fn push_node(
        &mut self,
        dense: u64,
        label: &str,
        key: &str,
        value: &Value,
        delta: &NodeDelta,
    ) -> Result<()> {
        self.node
            .append_record(&encode_node(label, key, value, delta))?;
        self.node_keys.push(dense);
        // Every pushed node carries a dense id (a no-op tombstone of a key that exists
        // nowhere is never emitted), so this is exactly the segment's suppressed set.
        if delta.tombstoned {
            self.tombstoned.push(dense);
        }
        // Label overlay: index each gained/dropped label → dense (ascending, since push
        // order is dense order), record the label-mutated id, and flag the segment.
        if !delta.labels_added.is_empty() || !delta.labels_removed.is_empty() {
            self.has_label_overlay = true;
            self.label_overlay_ids.push(dense);
            for l in &delta.labels_added {
                self.added_label_ids
                    .entry(l.clone())
                    .or_default()
                    .push(dense);
            }
            for l in &delta.labels_removed {
                self.removed_label_ids
                    .entry(l.clone())
                    .or_default()
                    .push(dense);
            }
        }
        Ok(())
    }

    pub fn push_adj_out(&mut self, node: u64, edges: &[DeltaEdge]) -> Result<()> {
        // Every delta edge is the out-edge of exactly one source, so this pass sees each
        // one once: enough to derive both resident edge columns without a second walk.
        for e in edges {
            if e.tombstoned {
                self.edge_tombstones = true;
            } else if e.edge_id.is_some() {
                *self
                    .born_edges_by_reltype
                    .entry(e.reltype.clone())
                    .or_default() += 1;
            }
        }
        self.adj_out.append_record(&encode_adj(edges))?;
        self.adj_out_keys.push(node);
        Ok(())
    }

    pub fn push_adj_in(&mut self, node: u64, edges: &[DeltaEdge]) -> Result<()> {
        self.adj_in.append_record(&encode_adj(edges))?;
        self.adj_in_keys.push(node);
        Ok(())
    }

    pub fn push_edge(&mut self, edge_id: u64, delta: &EdgeDelta) -> Result<()> {
        self.edge.append_record(&encode_edge(delta))?;
        self.edge_keys.push(edge_id);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set_scalars(
        &mut self,
        synthetic_base: u64,
        edge_synthetic_base: u64,
        node_delta_count: u64,
        edge_delta_count: u64,
        born_count: u64,
        born_edge_count: u64,
    ) {
        self.synthetic_base = synthetic_base;
        self.edge_synthetic_base = edge_synthetic_base;
        self.node_delta_count = node_delta_count;
        self.edge_delta_count = edge_delta_count;
        self.born_count = born_count;
        self.born_edge_count = born_edge_count;
    }

    /// Record the core-edge patch endpoints (v4) — the flush needs them to rebuild a
    /// patched edge's replace row from an off-heap level.
    pub fn set_core_patched_edges(&mut self, core_patched_edges: Vec<(u64, u64, u64, String)>) {
        self.core_patched_edges = core_patched_edges;
    }

    pub fn set_secondaries(
        &mut self,
        born_by_label: Vec<(String, Vec<u64>)>,
        born_index: Vec<BornIndexEntry>,
        core_patched: Vec<(String, String, u64)>,
        born_by_identity: Vec<(Vec<u8>, u64)>,
    ) {
        self.born_by_label = born_by_label;
        self.born_index = born_index;
        self.core_patched = core_patched;
        self.born_by_identity = born_by_identity;
    }

    pub fn finish(self) -> Result<()> {
        let meta = self.meta_bytes()?;
        self.node.finish().context("finish node.blk")?;
        self.adj_out.finish().context("finish adj_out.blk")?;
        self.adj_in.finish().context("finish adj_in.blk")?;
        self.edge.finish().context("finish edge.blk")?;
        std::fs::write(self.tmp.join("meta.bin"), &meta).context("write L0 meta.bin")?;
        let _ = std::fs::remove_dir_all(&self.dir);
        std::fs::rename(&self.tmp, &self.dir)
            .with_context(|| format!("rename L0 segment into place {:?}", self.dir))?;
        if let Some(parent) = self.dir.parent() {
            if let Ok(d) = std::fs::File::open(parent) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }

    fn meta_bytes(&self) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        write_uvarint(&mut body, OFFHEAP_VERSION);
        body.extend_from_slice(&self.scope.to_le_bytes());
        for s in [
            self.synthetic_base,
            self.edge_synthetic_base,
            self.node_delta_count,
            self.edge_delta_count,
            self.born_count,
            self.born_edge_count,
        ] {
            write_uvarint(&mut body, s);
        }
        w_keycol(&mut body, &self.node_keys);
        w_keycol(&mut body, &self.adj_out_keys);
        w_keycol(&mut body, &self.adj_in_keys);
        w_keycol(&mut body, &self.edge_keys);
        write_uvarint(&mut body, self.born_by_label.len() as u64);
        for (label, ids) in &self.born_by_label {
            w_str(&mut body, label);
            w_u64s(&mut body, ids.iter().copied());
        }
        write_uvarint(&mut body, self.born_index.len() as u64);
        for e in &self.born_index {
            w_str(&mut body, &e.label);
            write_uvarint(&mut body, e.id);
            write_uvarint(&mut body, e.props.len() as u64);
            for (p, v) in &e.props {
                w_str(&mut body, p);
                write_value(&mut body, v);
            }
        }
        write_uvarint(&mut body, self.core_patched.len() as u64);
        for (label, prop, id) in &self.core_patched {
            w_str(&mut body, label);
            w_str(&mut body, prop);
            write_uvarint(&mut body, *id);
        }
        write_uvarint(&mut body, self.born_by_identity.len() as u64);
        for (ck, id) in &self.born_by_identity {
            write_uvarint(&mut body, ck.len() as u64);
            body.extend_from_slice(ck);
            write_uvarint(&mut body, *id);
        }
        // v2: the resident live-count columns — the suppressed node ids (ascending, since
        // `push_node` runs in dense-id order), the edge-tombstone flag, and the per-reltype
        // born-edge tally — so a live count never pages a payload block.
        w_u64s(&mut body, self.tombstoned.iter().copied());
        body.push(u8::from(self.edge_tombstones));
        write_uvarint(&mut body, self.born_edges_by_reltype.len() as u64);
        for (reltype, n) in &self.born_edges_by_reltype {
            w_str(&mut body, reltype);
            write_uvarint(&mut body, *n);
        }
        // v3: the label-overlay index — dense ids that gained / dropped each label, the
        // full label-mutated id set (for the exact live first-label grouping), and a flag
        // set when any node carries a label mutation (so a scan re-checks candidates).
        body.push(u8::from(self.has_label_overlay));
        let w_label_ids = |body: &mut Vec<u8>, m: &BTreeMap<String, Vec<u64>>| {
            write_uvarint(body, m.len() as u64);
            for (label, ids) in m {
                w_str(body, label);
                w_u64s(body, ids.iter().copied());
            }
        };
        w_label_ids(&mut body, &self.added_label_ids);
        w_label_ids(&mut body, &self.removed_label_ids);
        w_u64s(&mut body, self.label_overlay_ids.iter().copied());
        // v4: core-edge patch endpoints, so a T2 flush can rebuild a patched edge's replace row.
        write_uvarint(&mut body, self.core_patched_edges.len() as u64);
        for (eid, src, dst, reltype) in &self.core_patched_edges {
            write_uvarint(&mut body, *eid);
            write_uvarint(&mut body, *src);
            write_uvarint(&mut body, *dst);
            w_str(&mut body, reltype);
        }

        frame_blob(
            META_MAGIC,
            META_MAGIC_SEALED,
            self.meta_cipher.as_deref(),
            &body,
        )
    }
}

/// Write `data` as an off-heap L0 segment directory at `dir` (created fresh), tagged with
/// cache `scope` (persisted in the meta). The flush path's materialised writer — a merge
/// uses [`OffheapSegmentWriter`] + [`merge_run`] directly to stream.
pub fn write_segment(
    data: &SegmentData,
    dir: impl AsRef<Path>,
    scope: u128,
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<&DeltaCipher>,
) -> Result<()> {
    let mut w = OffheapSegmentWriter::create(dir, scope, target_block_bytes, zstd_level, cipher)?;
    for (dense, label, key, value, delta) in &data.nodes {
        w.push_node(*dense, label, key, value, delta)?;
    }
    for (node, edges) in &data.adj_out {
        w.push_adj_out(*node, edges)?;
    }
    for (node, edges) in &data.adj_in {
        w.push_adj_in(*node, edges)?;
    }
    for (id, delta) in &data.edges {
        w.push_edge(*id, delta)?;
    }
    w.set_scalars(
        data.synthetic_base,
        data.edge_synthetic_base,
        data.node_delta_count,
        data.edge_delta_count,
        data.born_count,
        data.born_edge_count,
    );
    w.set_secondaries(
        data.born_by_label.clone(),
        data.born_index.clone(),
        data.core_patched.clone(),
        data.born_by_identity.clone(),
    );
    w.set_core_patched_edges(data.core_patched_edges.clone());
    w.finish()
}

fn w_u64s(buf: &mut Vec<u8>, it: impl ExactSizeIterator<Item = u64>) {
    write_uvarint(buf, it.len() as u64);
    for v in it {
        write_uvarint(buf, v);
    }
}

fn r_u64s(r: &mut &[u8]) -> Result<Vec<u64>> {
    let n = read_uvarint(r)? as usize;
    // Untrusted on-disk count, ≥1 byte per id — clamp the reservation to the bytes left.
    let mut out = Vec::with_capacity(capacity_for(n, r.len(), 1));
    for _ in 0..n {
        out.push(read_uvarint(r)?);
    }
    Ok(out)
}

/// Write an ascending distinct key column as a length-framed compact plane record
/// (`uvarint(plane_len) ‖ plane_bytes`) — the `w_u64s` replacement for the four presence sets,
/// ~6× smaller resident and on disk. Decoded once at open, so the latency-biased default opts
/// (prefer a decompress-free codec) are right.
fn w_keycol(buf: &mut Vec<u8>, keys: &[u64]) {
    let rec =
        KeyColumn::encode(keys, &PlaneCodecOpts::default()).expect("plane encode of key column");
    write_uvarint(buf, rec.len() as u64);
    buf.extend_from_slice(&rec);
}

/// Inverse of [`w_keycol`]. Fuzz-safe against a bogus length that overruns the buffer.
fn r_keycol(r: &mut &[u8]) -> Result<KeyColumn> {
    let len = read_uvarint(r)? as usize;
    if len > r.len() {
        bail!(
            "L0 meta: key-column length {len} exceeds {} remaining bytes",
            r.len()
        );
    }
    let (rec, rest) = r.split_at(len);
    *r = rest;
    KeyColumn::decode(rec)
}

// ── reader ──────────────────────────────────────────────────────────────────────────

/// An opened off-heap L0 segment: resident key columns + secondary indexes, with the
/// per-entity payloads read on demand through the shared [`BlockCache`].
pub struct L0Reader {
    dir: PathBuf,
    scope: u128,
    cache: Arc<BlockCache>,

    node_rdr: BlockFileReader,
    node_keys: KeyColumn,
    adj_out_rdr: BlockFileReader,
    adj_out_keys: KeyColumn,
    adj_in_rdr: BlockFileReader,
    adj_in_keys: KeyColumn,
    edge_rdr: BlockFileReader,
    edge_keys: KeyColumn,

    synthetic_base: u64,
    edge_synthetic_base: u64,
    node_delta_count: u64,
    edge_delta_count: u64,
    born_count: u64,
    born_edge_count: u64,

    born_by_label: HashMap<String, Vec<u64>>,
    born_index: Vec<BornIndexEntry>,
    core_patched: Vec<(String, String, u64)>,
    born_by_identity: HashMap<Vec<u8>, u64>,
    /// Resident core-edge patch endpoints (v4 meta) — `(edge id, src, dst, reltype)`.
    core_patched_edges: Vec<(u64, u64, u64, String)>,
    /// Resident suppressed dense ids (v2 meta) — the live-count summary's candidate set.
    tombstoned: Vec<u64>,
    /// Resident edge live-count columns (v2 meta).
    edge_tombstones: bool,
    born_edges_by_reltype: HashMap<String, u64>,
    /// Resident label-overlay index (v3 meta): dense ids that gained / dropped each label,
    /// the full label-mutated id set, and a flag set when any node carries a label mutation.
    added_label_ids: HashMap<String, Vec<u64>>,
    removed_label_ids: HashMap<String, Vec<u64>>,
    label_overlay_ids: Vec<u64>,
    has_label_overlay: bool,
}

impl std::fmt::Debug for L0Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L0Reader")
            .field("dir", &self.dir)
            .field("scope", &self.scope)
            .field("nodes", &self.node_keys.len())
            .field("edges", &self.edge_keys.len())
            .field("born", &self.born_count)
            .finish()
    }
}

impl L0Reader {
    /// Open the segment directory `dir`, verifying `meta.bin` and opening the four block
    /// sections. The shared-cache **scope** is read from the meta (persisted, fresh per
    /// write), so it is unique per live segment and stable across a reopen — even when a
    /// compaction reuses a segment directory. A retired segment's scope is simply never
    /// queried again, so its blocks age out of the LRU.
    pub fn open(
        dir: impl AsRef<Path>,
        cache: Arc<BlockCache>,
        cipher: Option<&DeltaCipher>,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let raw = std::fs::read(dir.join("meta.bin"))
            .with_context(|| format!("read L0 meta {dir:?}/meta.bin"))?;
        let meta = unframe_blob(
            META_MAGIC,
            META_MAGIC_SEALED,
            bind(cipher, &l0_name(&dir, Some("meta.bin"))?).as_deref(),
            &raw,
            META_SUBJECT,
        )
        .with_context(|| format!("open L0 meta {dir:?}/meta.bin"))?;
        let mut r = &*meta;
        let version = read_uvarint(&mut r)?;
        if version != OFFHEAP_VERSION {
            bail!("unsupported off-heap L0 version {version} (expected {OFFHEAP_VERSION})");
        }
        if r.len() < 16 {
            bail!("L0 segment {dir:?}: short meta (scope)");
        }
        let scope = u128::from_le_bytes(r[..16].try_into().unwrap());
        r = &r[16..];
        let synthetic_base = read_uvarint(&mut r)?;
        let edge_synthetic_base = read_uvarint(&mut r)?;
        let node_delta_count = read_uvarint(&mut r)?;
        let edge_delta_count = read_uvarint(&mut r)?;
        let born_count = read_uvarint(&mut r)?;
        let born_edge_count = read_uvarint(&mut r)?;
        let node_keys = r_keycol(&mut r)?;
        let adj_out_keys = r_keycol(&mut r)?;
        let adj_in_keys = r_keycol(&mut r)?;
        let edge_keys = r_keycol(&mut r)?;

        // Every count below is an untrusted uvarint out of the mmapped meta record. Each
        // entry costs at least the bytes noted, so clamp the reservation to what the record
        // could actually hold (`wire::capacity_for`): a forged count then runs the buffer dry
        // in the loop and errors, instead of aborting the process in the allocator first.
        let n_label = read_uvarint(&mut r)? as usize;
        let mut born_by_label = HashMap::with_capacity(capacity_for(n_label, r.len(), 2));
        for _ in 0..n_label {
            let label = r_str(&mut r)?;
            born_by_label.insert(label, r_u64s(&mut r)?);
        }
        let n_bi = read_uvarint(&mut r)? as usize;
        let mut born_index = Vec::with_capacity(capacity_for(n_bi, r.len(), 3));
        for _ in 0..n_bi {
            let label = r_str(&mut r)?;
            let id = read_uvarint(&mut r)?;
            let np = read_uvarint(&mut r)? as usize;
            let mut props = Vec::with_capacity(capacity_for(np, r.len(), 2));
            for _ in 0..np {
                let p = r_str(&mut r)?;
                let v = read_value(&mut r)?;
                props.push((p, v));
            }
            born_index.push(BornIndexEntry { label, id, props });
        }
        let n_cp = read_uvarint(&mut r)? as usize;
        let mut core_patched = Vec::with_capacity(capacity_for(n_cp, r.len(), 3));
        for _ in 0..n_cp {
            let label = r_str(&mut r)?;
            let prop = r_str(&mut r)?;
            let id = read_uvarint(&mut r)?;
            core_patched.push((label, prop, id));
        }
        let n_bid = read_uvarint(&mut r)? as usize;
        let mut born_by_identity = HashMap::with_capacity(capacity_for(n_bid, r.len(), 2));
        for _ in 0..n_bid {
            let len = read_uvarint(&mut r)? as usize;
            if r.len() < len {
                bail!("L0 segment {dir:?}: short identity key");
            }
            let ck = r[..len].to_vec();
            r = &r[len..];
            born_by_identity.insert(ck, read_uvarint(&mut r)?);
        }
        let tombstoned = r_u64s(&mut r)?;
        if r.is_empty() {
            bail!("L0 segment {dir:?}: short edge-tombstone flag");
        }
        let edge_tombstones = r[0] != 0;
        r = &r[1..];
        let n_ber = read_uvarint(&mut r)? as usize;
        let mut born_edges_by_reltype = HashMap::with_capacity(capacity_for(n_ber, r.len(), 2));
        for _ in 0..n_ber {
            let reltype = r_str(&mut r)?;
            born_edges_by_reltype.insert(reltype, read_uvarint(&mut r)?);
        }
        // v3: the label-overlay index.
        if r.is_empty() {
            bail!("L0 segment {dir:?}: short label-overlay flag");
        }
        let has_label_overlay = r[0] != 0;
        r = &r[1..];
        let r_label_ids = |r: &mut &[u8]| -> Result<HashMap<String, Vec<u64>>> {
            let n = read_uvarint(r)? as usize;
            let mut m = HashMap::with_capacity(capacity_for(n, r.len(), 2));
            for _ in 0..n {
                let label = r_str(r)?;
                m.insert(label, r_u64s(r)?);
            }
            Ok(m)
        };
        let added_label_ids = r_label_ids(&mut r)?;
        let removed_label_ids = r_label_ids(&mut r)?;
        let label_overlay_ids = r_u64s(&mut r)?;
        // v4: core-edge patch endpoints.
        let n_cpe = read_uvarint(&mut r)? as usize;
        let mut core_patched_edges = Vec::with_capacity(capacity_for(n_cpe, r.len(), 4));
        for _ in 0..n_cpe {
            let eid = read_uvarint(&mut r)?;
            let src = read_uvarint(&mut r)?;
            let dst = read_uvarint(&mut r)?;
            let reltype = r_str(&mut r)?;
            core_patched_edges.push((eid, src, dst, reltype));
        }
        if !r.is_empty() {
            bail!("L0 segment {dir:?} meta has {} trailing bytes", r.len());
        }

        let open_section = |name: &str| -> Result<BlockFileReader> {
            BlockFileReader::open_with_cipher(
                dir.join(name),
                bind(cipher, &l0_name(&dir, Some(name))?),
            )
            .with_context(|| format!("open L0 section {dir:?}/{name}"))
        };
        Ok(Self {
            node_rdr: open_section(SECTION_FILES[0])?,
            adj_out_rdr: open_section(SECTION_FILES[1])?,
            adj_in_rdr: open_section(SECTION_FILES[2])?,
            edge_rdr: open_section(SECTION_FILES[3])?,
            dir,
            scope,
            cache,
            node_keys,
            adj_out_keys,
            adj_in_keys,
            edge_keys,
            synthetic_base,
            edge_synthetic_base,
            node_delta_count,
            edge_delta_count,
            born_count,
            born_edge_count,
            tombstoned,
            edge_tombstones,
            born_edges_by_reltype,
            born_by_label,
            born_index,
            core_patched,
            born_by_identity,
            core_patched_edges,
            added_label_ids,
            removed_label_ids,
            label_overlay_ids,
            has_label_overlay,
        })
    }

    /// The segment directory (retired at consolidation).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // ── merge accessors (resident key columns + secondary indexes) ──────────────────
    // A disk-native compaction ([`merge_run`]) enumerates the sorted union of keys from
    // these resident columns, then folds the payloads through a `DeltaSnapshot`.
    // These materialise the compact key column for the disk-native compaction enumeration
    // (a `Vec<u64>` the k-way `sorted_union` merges). Membership lookups elsewhere `find`
    // directly rather than materialising.
    pub fn node_dense_ids(&self) -> Vec<u64> {
        self.node_keys.iter().collect()
    }
    pub fn adj_out_nodes(&self) -> Vec<u64> {
        self.adj_out_keys.iter().collect()
    }
    pub fn adj_in_nodes(&self) -> Vec<u64> {
        self.adj_in_keys.iter().collect()
    }
    pub fn edge_ids(&self) -> Vec<u64> {
        self.edge_keys.iter().collect()
    }
    pub fn born_by_label_ref(&self) -> &HashMap<String, Vec<u64>> {
        &self.born_by_label
    }
    pub fn born_index_ref(&self) -> &[BornIndexEntry] {
        &self.born_index
    }
    pub fn core_patched_ref(&self) -> &[(String, String, u64)] {
        &self.core_patched
    }
    pub fn born_by_identity_ref(&self) -> &HashMap<Vec<u8>, u64> {
        &self.born_by_identity
    }

    /// Fetch record `idx` of `reader` (section `sub`) through the shared cache, returning
    /// its decoded body bytes.
    fn record_bytes(&self, reader: &BlockFileReader, sub: u32, idx: usize) -> Result<Vec<u8>> {
        let rec = self.cache.record(reader, self.scope, sub, idx as u64)?;
        Ok(rec.as_slice().to_vec())
    }

    fn node_entry(&self, dense_id: u64) -> Option<(String, String, Value, NodeDelta)> {
        let idx = self.node_keys.find(dense_id)?;
        let bytes = self
            .record_bytes(&self.node_rdr, SUB_NODE, idx)
            .expect("l0 node record");
        Some(decode_node(&bytes).expect("decode l0 node"))
    }
}

fn born_index_value<'a>(entry: &'a BornIndexEntry, prop: &str) -> Option<&'a Value> {
    entry.props.iter().find(|(p, _)| p == prop).map(|(_, v)| v)
}

impl LevelRead for L0Reader {
    fn is_empty(&self) -> bool {
        self.node_keys.is_empty()
            && self.edge_keys.is_empty()
            && self.born_count == 0
            && self.born_edge_count == 0
    }

    fn node_delta_count(&self) -> usize {
        self.node_delta_count as usize
    }

    fn edge_delta_count(&self) -> usize {
        self.edge_delta_count as usize
    }

    fn tombstoned_ids(&self) -> Vec<u64> {
        self.tombstoned.clone()
    }

    fn born_count_with_label(&self, label: &str) -> u64 {
        self.born_by_label
            .get(label)
            .map_or(0, |ids| ids.len() as u64)
    }

    fn has_edge_tombstones(&self) -> bool {
        self.edge_tombstones
    }

    fn born_edge_count_with_reltype(&self, reltype: &str) -> u64 {
        self.born_edges_by_reltype
            .get(reltype)
            .copied()
            .unwrap_or(0)
    }

    fn born_labels(&self) -> Vec<String> {
        self.born_by_label.keys().cloned().collect()
    }

    fn born_edge_reltypes(&self) -> Vec<String> {
        self.born_edges_by_reltype.keys().cloned().collect()
    }

    fn synthetic_base(&self) -> u64 {
        self.synthetic_base
    }

    fn edge_synthetic_base(&self) -> u64 {
        self.edge_synthetic_base
    }

    fn born_count(&self) -> u64 {
        self.born_count
    }

    fn born_edge_count(&self) -> u64 {
        self.born_edge_count
    }

    fn node_patch_owned(&self, dense_id: u64) -> Option<NodeDelta> {
        self.node_entry(dense_id).map(|(_, _, _, d)| d)
    }

    fn node_tombstoned(&self, dense_id: u64) -> Option<bool> {
        // A hit pages one block; a miss is a resident binary search with no I/O.
        self.node_entry(dense_id).map(|(_, _, _, d)| d.tombstoned)
    }

    fn node_identity_owned(&self, dense_id: u64) -> Option<(String, String, Value)> {
        self.node_entry(dense_id).map(|(l, k, v, _)| (l, k, v))
    }

    fn born_ids_with_label(&self, label: &str) -> Vec<u64> {
        self.born_by_label.get(label).cloned().unwrap_or_default()
    }

    fn ids_with_added_label(&self, label: &str) -> Vec<u64> {
        self.added_label_ids.get(label).cloned().unwrap_or_default()
    }

    fn ids_with_removed_label(&self, label: &str) -> Vec<u64> {
        self.removed_label_ids
            .get(label)
            .cloned()
            .unwrap_or_default()
    }

    fn label_overlay_ids(&self) -> Vec<u64> {
        self.label_overlay_ids.clone()
    }

    fn has_label_overlay(&self) -> bool {
        self.has_label_overlay
    }

    fn born_synthetic_for_identity(&self, label: &str, key: &str, value: &Value) -> Option<u64> {
        let ck = identity_key_bytes(label, key, value);
        self.born_by_identity.get(&ck).copied()
    }

    fn born_ids_in_index_eq(&self, label: &str, prop: &str, key: &Value) -> Vec<u64> {
        use std::cmp::Ordering;
        self.born_ids_in_index(label, prop, |v| v.cmp_key(key) == Ordering::Equal)
    }

    fn born_ids_in_index_range(
        &self,
        label: &str,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Vec<u64> {
        self.born_ids_in_index(label, prop, |v| {
            crate::memtable::value_in_range(v, lo, lo_inclusive, hi, hi_inclusive)
        })
    }

    fn core_ids_patched_on_index(&self, label: &str, prop: &str) -> Vec<u64> {
        self.core_patched
            .iter()
            .filter(|(l, p, _)| l == label && p == prop)
            .map(|(_, _, id)| *id)
            .collect()
    }

    fn out_edges(&self, node: u64) -> Vec<DeltaEdge> {
        let Some(idx) = self.adj_out_keys.find(node) else {
            return Vec::new();
        };
        let bytes = self
            .record_bytes(&self.adj_out_rdr, SUB_ADJ_OUT, idx)
            .expect("l0 adj_out record");
        decode_adj(&bytes).expect("decode l0 adj_out")
    }

    fn in_edges(&self, node: u64) -> Vec<DeltaEdge> {
        let Some(idx) = self.adj_in_keys.find(node) else {
            return Vec::new();
        };
        let bytes = self
            .record_bytes(&self.adj_in_rdr, SUB_ADJ_IN, idx)
            .expect("l0 adj_in record");
        decode_adj(&bytes).expect("decode l0 adj_in")
    }

    fn edge_delta_owned(&self, edge_id: u64) -> Option<EdgeDelta> {
        let idx = self.edge_keys.find(edge_id)?;
        let bytes = self
            .record_bytes(&self.edge_rdr, SUB_EDGE, idx)
            .expect("l0 edge record");
        Some(decode_edge(&bytes).expect("decode l0 edge"))
    }
    fn node_dense_ids(&self) -> Vec<u64> {
        self.node_keys.iter().collect()
    }
    fn adj_out_nodes(&self) -> Vec<u64> {
        self.adj_out_keys.iter().collect()
    }
    fn edge_ids(&self) -> Vec<u64> {
        self.edge_keys.iter().collect()
    }
    fn core_patched_edges(&self) -> Vec<(u64, u64, u64, String)> {
        self.core_patched_edges.clone()
    }
}

impl L0Reader {
    /// Born nodes carrying `label` whose effective indexed value for `prop` satisfies
    /// `pred`, in born-allocation (ascending id) order — mirrors `Memtable::born_ids_in_index`.
    fn born_ids_in_index(
        &self,
        label: &str,
        prop: &str,
        pred: impl Fn(&Value) -> bool,
    ) -> Vec<u64> {
        let mut out = Vec::new();
        for e in &self.born_index {
            if e.label != label {
                continue;
            }
            if let Some(v) = born_index_value(e, prop) {
                if pred(v) {
                    out.push(e.id);
                }
            }
        }
        out
    }
}

/// The identity-key bytes used for `born_synthetic_for_identity` resolution — an
/// interner-independent `label ‖ key ‖ value` encoding, matched by the writer and reader.
pub fn identity_key_bytes(label: &str, key: &str, value: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    w_str(&mut buf, label);
    w_str(&mut buf, key);
    write_value(&mut buf, value);
    buf
}

/// Merge a **contiguous, stacked run** of off-heap L0 segments (newest-first) into one
/// off-heap segment at `out_dir`, tagged `scope`. Streaming + disk-native: the run is
/// folded through a `DeltaSnapshot` and each merged record is written out immediately, so
/// the merged payloads are **never all held resident** — peak RSS is the key columns +
/// secondaries + a block window (the point of the off-heap compaction, D54). The result is
/// read-equivalent to `DeltaSnapshot::with_levels(run)`; born ids are preserved (the run's
/// synthetic ranges are disjoint + stacked, so `synthetic_base` is the oldest member's base
/// and every born id passes through at its original value).
pub fn merge_run(
    run: &[Arc<L0Reader>],
    out_dir: impl AsRef<Path>,
    scope: u128,
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<&DeltaCipher>,
) -> Result<()> {
    assert!(!run.is_empty(), "merge_run needs at least one segment");
    let oldest = run.last().unwrap();

    // Fold the run through a DeltaSnapshot whose active memtable is empty but carries the
    // oldest member's bases — so `synthetic_base`/`edge_synthetic_base` (the min across
    // levels) stay correct while the empty level contributes no read.
    let empty = Arc::new(Memtable::with_bases(
        oldest.synthetic_base(),
        oldest.edge_synthetic_base(),
    ));
    let levels: Vec<Arc<dyn LevelRead>> = run
        .iter()
        .map(|r| r.clone() as Arc<dyn LevelRead>)
        .collect();
    let snap = DeltaSnapshot::with_levels(empty, levels);

    let mut w =
        OffheapSegmentWriter::create(out_dir, scope, target_block_bytes, zstd_level, cipher)?;

    // Node section: sorted union of dense ids, folded newest-wins (a tombstoned entry is
    // kept — it must keep suppressing the core row).
    let mut node_count = 0u64;
    for id in sorted_union(run.iter().map(|r| r.node_dense_ids())) {
        let (Some(delta), Some((label, key, value))) =
            (snap.node_patch(id), snap.node_identity_by_dense(id))
        else {
            continue;
        };
        w.push_node(id, &label, &key, &value, &delta)?;
        node_count += 1;
    }

    // Adjacency: sorted union of endpoints, deduped by (reltype, neighbour) newest-wins.
    for node in sorted_union(run.iter().map(|r| r.adj_out_nodes())) {
        let edges = snap.out_edges(node);
        if !edges.is_empty() {
            w.push_adj_out(node, &edges)?;
        }
    }
    for node in sorted_union(run.iter().map(|r| r.adj_in_nodes())) {
        let edges = snap.in_edges(node);
        if !edges.is_empty() {
            w.push_adj_in(node, &edges)?;
        }
    }

    // Edge section: sorted union of edge ids, folded newest-wins. The newest level that
    // touches an id decides `tombstoned`; `edge_patches` folds the properties (and clears
    // them on a newer tombstone).
    let mut edge_count = 0u64;
    for id in sorted_union(run.iter().map(|r| r.edge_ids())) {
        let Some(newest) = run.iter().find_map(|r| r.edge_delta_owned(id)) else {
            continue;
        };
        let merged = EdgeDelta {
            patches: snap.edge_patches(id),
            tombstoned: newest.tombstoned,
        };
        w.push_edge(id, &merged)?;
        edge_count += 1;
    }

    w.set_scalars(
        snap.synthetic_base(),
        snap.edge_synthetic_base(),
        node_count,
        edge_count,
        snap.born_count(),
        snap.born_edge_count(),
    );

    // Secondary indexes: concatenate the run's resident structures **oldest-first**,
    // preserving the exact per-level born-index semantics (a born id lives in one level;
    // born-id ranges stack ascending, so oldest-first concatenation keeps lists ascending).
    let mut by_label: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut born_index: Vec<BornIndexEntry> = Vec::new();
    let mut core_patched: Vec<(String, String, u64)> = Vec::new();
    let mut born_by_identity: Vec<(Vec<u8>, u64)> = Vec::new();
    // Core-edge patch endpoints, newest-wins by edge id (endpoints are stable, so the fold
    // only matters if a newer level re-patched the same edge — either way the endpoints agree).
    let mut cpe: BTreeMap<u64, (u64, u64, String)> = BTreeMap::new();
    for r in run.iter().rev() {
        for (label, ids) in r.born_by_label_ref() {
            by_label
                .entry(label.clone())
                .or_default()
                .extend_from_slice(ids);
        }
        born_index.extend(r.born_index_ref().iter().cloned());
        core_patched.extend(r.core_patched_ref().iter().cloned());
        for (ck, id) in r.born_by_identity_ref() {
            born_by_identity.push((ck.clone(), *id));
        }
        for (eid, src, dst, reltype) in r.core_patched_edges() {
            cpe.insert(eid, (src, dst, reltype));
        }
    }
    w.set_secondaries(
        by_label.into_iter().collect(),
        born_index,
        core_patched,
        born_by_identity,
    );
    w.set_core_patched_edges(
        cpe.into_iter()
            .map(|(eid, (src, dst, rt))| (eid, src, dst, rt))
            .collect(),
    );
    w.finish()
}

/// Sorted, de-duplicated union of several `u64` key columns.
fn sorted_union(cols: impl Iterator<Item = Vec<u64>>) -> Vec<u64> {
    let mut all: Vec<u64> = cols.flatten().collect();
    all.sort_unstable();
    all.dedup();
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    // HIK-80: the L0 meta/adjacency decoders sized their `Vec`s and `HashMap`s from untrusted
    // on-disk counts. These records are mmapped straight off the data dir, so a forged count is
    // an allocator abort — the whole process — on delta open. Reaching the assertion is the
    // proof: pre-fix, the test binary dies in the allocator rather than failing.
    #[test]
    fn forged_l0_counts_are_refused_not_preallocated() {
        let mut rec = Vec::new();
        write_uvarint(&mut rec, u64::MAX);
        assert!(r_u64s(&mut &rec[..]).is_err());
        assert!(decode_adj(&rec).is_err());

        // Honest records still round-trip: the clamp bounds the reservation, never acceptance.
        let mut ok = Vec::new();
        w_u64s(&mut ok, [7u64, 8, 9].into_iter());
        assert_eq!(r_u64s(&mut &ok[..]).unwrap(), vec![7, 8, 9]);
    }

    use crate::memtable::Memtable;
    use crate::wal::WalOp;
    use crate::OpResolution;

    fn tmp(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("slater_l0off_{tag}_{}", std::process::id()))
    }

    fn upsert(m: &mut Memtable, name: &str, patches: &[(&str, Value)], resolved: Option<u64>) {
        m.apply(
            &WalOp::UpsertNode {
                label: "Person".into(),
                key: "name".into(),
                value: Value::Str(name.into()),
                patches: patches
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            },
            OpResolution::Node(resolved),
        );
    }

    /// A memtable exercising every stored shape: a core-node patch (incl. an indexed
    /// property move), a born node with a patched indexed value, a tombstoned core node,
    /// a born edge (core → born endpoint) with a property, and a tombstoned core edge.
    fn populate() -> Memtable {
        let mut m = Memtable::with_bases(100, 10);
        upsert(
            &mut m,
            "Alice",
            &[
                ("age", Value::Int(30)),
                ("name", Value::Str("Alicia".into())),
            ],
            Some(5),
        );
        upsert(&mut m, "Zoe", &[("age", Value::Int(9))], None); // born → 100
        upsert(
            &mut m,
            "Yan",
            &[("name", Value::Str("Yannick".into()))],
            None,
        ); // born → 101
        m.apply(
            &WalOp::DeleteNode {
                label: "Person".into(),
                key: "name".into(),
                value: Value::Str("Bob".into()),
            },
            OpResolution::Node(Some(7)),
        );
        // Born edge Alice(5) -> Zoe(born), with a property.
        m.apply(
            &WalOp::UpsertEdge {
                src_label: "Person".into(),
                src_key: "name".into(),
                src_value: Value::Str("Alice".into()),
                reltype: "KNOWS".into(),
                dst_label: "Person".into(),
                dst_key: "name".into(),
                dst_value: Value::Str("Zoe".into()),
                patches: [("since".to_string(), Value::Int(2020))]
                    .into_iter()
                    .collect(),
            },
            OpResolution::Edge {
                src: Some(5),
                dst: None,
                edge_id: None,
            },
        );
        // Born edge Alice(5) -> Newbie, whose destination endpoint does not exist yet and
        // is therefore *created by the edge merge itself* (`endpoint_dense_or_create`) —
        // a born node that must be tallied per label like any other.
        m.apply(
            &WalOp::UpsertEdge {
                src_label: "Person".into(),
                src_key: "name".into(),
                src_value: Value::Str("Alice".into()),
                reltype: "KNOWS".into(),
                dst_label: "City".into(),
                dst_key: "name".into(),
                dst_value: Value::Str("Newbie".into()),
                patches: Default::default(),
            },
            OpResolution::Edge {
                src: Some(5),
                dst: None,
                edge_id: None,
            },
        );
        // Tombstone a core edge Alice(5) -> Carol(8).
        m.apply(
            &WalOp::DeleteEdge {
                src_label: "Person".into(),
                src_key: "name".into(),
                src_value: Value::Str("Alice".into()),
                reltype: "KNOWS".into(),
                dst_label: "Person".into(),
                dst_key: "name".into(),
                dst_value: Value::Str("Carol".into()),
            },
            OpResolution::Edge {
                src: Some(5),
                dst: Some(8),
                edge_id: None,
            },
        );
        // Patch a core edge (id 3) in place.
        m.apply(
            &WalOp::UpsertEdge {
                src_label: "Person".into(),
                src_key: "name".into(),
                src_value: Value::Str("Alice".into()),
                reltype: "KNOWS".into(),
                dst_label: "Person".into(),
                dst_key: "name".into(),
                dst_value: Value::Str("Dave".into()),
                patches: [("weight".to_string(), Value::Int(7))]
                    .into_iter()
                    .collect(),
            },
            OpResolution::Edge {
                src: Some(5),
                dst: Some(9),
                edge_id: Some(3),
            },
        );
        m
    }

    /// Flush `m` off-heap and reopen it as an `L0Reader`.
    fn roundtrip(m: &Memtable, tag: &str) -> (PathBuf, L0Reader) {
        let dir = tmp(tag);
        let _ = std::fs::remove_dir_all(&dir);
        write_segment(&m.to_segment_data(), &dir, 0x1234, 256, 3, None).unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        let reader = L0Reader::open(&dir, cache, None).unwrap();
        (dir, reader)
    }

    /// Every `LevelRead` read matches between the resident memtable and the off-heap reader.
    fn assert_parity(m: &Memtable, r: &L0Reader) {
        let a: &dyn LevelRead = m;
        let b: &dyn LevelRead = r;
        assert_eq!(a.is_empty(), b.is_empty());
        assert_eq!(a.node_delta_count(), b.node_delta_count());
        assert_eq!(a.edge_delta_count(), b.edge_delta_count());
        assert_eq!(a.synthetic_base(), b.synthetic_base());
        assert_eq!(a.edge_synthetic_base(), b.edge_synthetic_base());
        assert_eq!(a.born_count(), b.born_count());
        assert_eq!(a.born_edge_count(), b.born_edge_count());
        // The v2 resident live-count columns: the suppressed set (order-insensitive — the
        // memtable's is a HashSet) and the per-label born tally.
        let (mut ta, mut tb) = (a.tombstoned_ids(), b.tombstoned_ids());
        ta.sort_unstable();
        tb.sort_unstable();
        assert_eq!(ta, tb, "tombstoned_ids");
        for label in ["Person", "City", "Absent"] {
            assert_eq!(
                a.born_count_with_label(label),
                b.born_count_with_label(label),
                "born_count_with_label({label})"
            );
        }
        assert_eq!(
            a.has_edge_tombstones(),
            b.has_edge_tombstones(),
            "has_edge_tombstones"
        );
        for rt in ["KNOWS", "R", "Absent"] {
            assert_eq!(
                a.born_edge_count_with_reltype(rt),
                b.born_edge_count_with_reltype(rt),
                "born_edge_count_with_reltype({rt})"
            );
        }
        // Node patch / tombstone / identity over the whole dense range + some misses.
        for id in 0..a.synthetic_base() + a.born_count() + 3 {
            assert_eq!(
                a.node_patch_owned(id),
                b.node_patch_owned(id),
                "node_patch({id})"
            );
            assert_eq!(
                a.node_tombstoned(id),
                b.node_tombstoned(id),
                "node_tombstoned({id})"
            );
            assert_eq!(
                a.node_identity_owned(id),
                b.node_identity_owned(id),
                "identity({id})"
            );
            assert_eq!(a.out_edges(id), b.out_edges(id), "out_edges({id})");
            assert_eq!(a.in_edges(id), b.in_edges(id), "in_edges({id})");
        }
        // Edge deltas over born + core-patched ids + misses.
        for id in 0..a.edge_synthetic_base() + a.born_edge_count() + 3 {
            assert_eq!(
                a.edge_delta_owned(id),
                b.edge_delta_owned(id),
                "edge_delta({id})"
            );
        }
        // Secondary axes.
        assert_eq!(
            a.born_ids_with_label("Person"),
            b.born_ids_with_label("Person")
        );
        assert_eq!(
            a.born_ids_with_label("Ghost"),
            b.born_ids_with_label("Ghost")
        );
        assert_eq!(
            a.born_ids_in_index_eq("Person", "name", &Value::Str("Zoe".into())),
            b.born_ids_in_index_eq("Person", "name", &Value::Str("Zoe".into())),
        );
        assert_eq!(
            a.born_ids_in_index_eq("Person", "name", &Value::Str("Yannick".into())),
            b.born_ids_in_index_eq("Person", "name", &Value::Str("Yannick".into())),
        );
        assert_eq!(
            a.born_ids_in_index_range(
                "Person",
                "age",
                Some(&Value::Int(0)),
                true,
                Some(&Value::Int(100)),
                true
            ),
            b.born_ids_in_index_range(
                "Person",
                "age",
                Some(&Value::Int(0)),
                true,
                Some(&Value::Int(100)),
                true
            ),
        );
        assert_eq!(
            a.core_ids_patched_on_index("Person", "name"),
            b.core_ids_patched_on_index("Person", "name"),
        );
        for (l, k, v) in [
            ("Person", "name", Value::Str("Zoe".into())),
            ("Person", "name", Value::Str("Nobody".into())),
            ("Ghost", "name", Value::Str("Zoe".into())),
        ] {
            assert_eq!(
                a.born_synthetic_for_identity(l, k, &v),
                b.born_synthetic_for_identity(l, k, &v),
                "born_synthetic({l},{k})",
            );
        }
    }

    #[test]
    fn offheap_reader_matches_resident_memtable() {
        let m = populate();
        let (dir, r) = roundtrip(&m, "parity");
        assert_parity(&m, &r);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_memtable_round_trips_offheap() {
        let m = Memtable::with_bases(42, 3);
        let (dir, r) = roundtrip(&m, "empty");
        assert!(<L0Reader as LevelRead>::is_empty(&r));
        assert_eq!(r.synthetic_base(), 42);
        assert_eq!(r.edge_synthetic_base(), 3);
        assert_parity(&m, &r);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_meta_is_rejected() {
        let m = populate();
        let dir = tmp("corrupt");
        let _ = std::fs::remove_dir_all(&dir);
        write_segment(&m.to_segment_data(), &dir, 0x1234, 256, 3, None).unwrap();
        let mut meta = std::fs::read(dir.join("meta.bin")).unwrap();
        let last = meta.len() - 1;
        meta[last] ^= 0xff;
        std::fs::write(dir.join("meta.bin"), &meta).unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        assert!(L0Reader::open(&dir, cache, None).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    fn sort_edges(mut v: Vec<DeltaEdge>) -> Vec<DeltaEdge> {
        v.sort_by(|a, b| (a.other, &a.reltype).cmp(&(b.other, &b.reltype)));
        v
    }

    /// A disk-native merge of a stacked off-heap run is read-equivalent to the
    /// `DeltaSnapshot` fold over that run (the compaction invariant), exercising a
    /// cross-level core patch, disjoint born unions, a tombstone, and a born edge —
    /// with a small block size so payloads span multiple paged blocks.
    #[test]
    fn merge_run_matches_the_snapshot_fold() {
        let base = tmp("merge_run");
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));

        // Oldest level: core 5 patched (age=1), born Zoe→100, born edge 5→Zoe.
        let mut older = Memtable::with_bases(100, 10);
        upsert(&mut older, "Alice", &[("age", Value::Int(1))], Some(5));
        upsert(&mut older, "Zoe", &[("age", Value::Int(9))], None); // born 100
        older.apply(
            &WalOp::UpsertEdge {
                src_label: "Person".into(),
                src_key: "name".into(),
                src_value: Value::Str("Alice".into()),
                reltype: "KNOWS".into(),
                dst_label: "Person".into(),
                dst_key: "name".into(),
                dst_value: Value::Str("Zoe".into()),
                patches: Default::default(),
            },
            OpResolution::Edge {
                src: Some(5),
                dst: None,
                edge_id: None,
            },
        );

        // Newer level (rebased past `older`): core 5 re-patched (age=2 wins, +city),
        // tombstone core 7, born Yan→101.
        let mut newer =
            Memtable::with_bases(100 + older.born_count(), 10 + older.born_edge_count());
        upsert(
            &mut newer,
            "Alice",
            &[("age", Value::Int(2)), ("city", Value::Str("NYC".into()))],
            Some(5),
        );
        newer.apply(
            &WalOp::DeleteNode {
                label: "Person".into(),
                key: "name".into(),
                value: Value::Str("Bob".into()),
            },
            OpResolution::Node(Some(7)),
        );
        upsert(&mut newer, "Yan", &[("age", Value::Int(20))], None); // born 101

        // Write each off-heap (tiny blocks → multi-block sections), open as readers.
        write_segment(
            &older.to_segment_data(),
            base.join("old.l0"),
            1,
            64,
            3,
            None,
        )
        .unwrap();
        write_segment(
            &newer.to_segment_data(),
            base.join("new.l0"),
            2,
            64,
            3,
            None,
        )
        .unwrap();
        let r_old = Arc::new(L0Reader::open(base.join("old.l0"), cache.clone(), None).unwrap());
        let r_new = Arc::new(L0Reader::open(base.join("new.l0"), cache.clone(), None).unwrap());
        let run = vec![r_new.clone(), r_old.clone()]; // newest-first

        let mk_snap = |levels: Vec<Arc<dyn LevelRead>>, oldest: &Arc<L0Reader>| {
            let empty = Arc::new(Memtable::with_bases(
                oldest.synthetic_base(),
                oldest.edge_synthetic_base(),
            ));
            DeltaSnapshot::with_levels(empty, levels)
        };
        let reference = mk_snap(
            run.iter()
                .map(|r| r.clone() as Arc<dyn LevelRead>)
                .collect(),
            &r_old,
        );

        // Merge, reopen, wrap in a snapshot the same way.
        let merged_dir = base.join("merged.l0");
        merge_run(&run, &merged_dir, 99, 64, 3, None).unwrap();
        let merged = Arc::new(L0Reader::open(&merged_dir, cache.clone(), None).unwrap());
        let folded = mk_snap(vec![merged.clone() as Arc<dyn LevelRead>], &merged);

        assert_eq!(reference.synthetic_base(), folded.synthetic_base());
        assert_eq!(
            reference.edge_synthetic_base(),
            folded.edge_synthetic_base()
        );
        assert_eq!(reference.born_count(), folded.born_count());
        assert_eq!(reference.born_edge_count(), folded.born_edge_count());
        let hi = reference.synthetic_base() + reference.born_count();
        for id in 0..hi + 3 {
            assert_eq!(
                reference.node_patch(id),
                folded.node_patch(id),
                "node_patch({id})"
            );
            assert_eq!(
                reference.is_tombstoned(id),
                folded.is_tombstoned(id),
                "tombstoned({id})"
            );
            assert_eq!(
                sort_edges(reference.out_edges(id)),
                sort_edges(folded.out_edges(id)),
                "out_edges({id})",
            );
            assert_eq!(
                sort_edges(reference.in_edges(id)),
                sort_edges(folded.in_edges(id)),
                "in_edges({id})",
            );
        }
        assert_eq!(
            reference.born_ids_with_label("Person"),
            folded.born_ids_with_label("Person"),
        );
        assert_eq!(
            reference.born_ids_in_index_eq("Person", "name", &Value::Str("Yan".into())),
            folded.born_ids_in_index_eq("Person", "name", &Value::Str("Yan".into())),
        );
        // Headline folds: core 5 has the newer age + city; core 7 deleted; both born nodes.
        assert_eq!(
            folded.node_patch(5).unwrap().patches.get("age"),
            Some(&Value::Int(2))
        );
        assert_eq!(
            folded.node_patch(5).unwrap().patches.get("city"),
            Some(&Value::Str("NYC".into())),
        );
        assert!(folded.is_tombstoned(7));
        assert_eq!(folded.born_count(), 2);
        std::fs::remove_dir_all(&base).ok();
    }
}
