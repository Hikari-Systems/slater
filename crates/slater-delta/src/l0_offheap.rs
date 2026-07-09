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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use graph_format::blockcache::BlockCache;
use graph_format::blockfile::{BlockFileReader, BlockFileWriter};
use graph_format::ids::Value;
use graph_format::wire::{read_uvarint, read_value, write_uvarint, write_value};

use crate::memtable::{DeltaEdge, EdgeDelta, LevelRead, NodeDelta};

const META_MAGIC: &[u8; 8] = b"SLL0OFF1";
const OFFHEAP_VERSION: u64 = 1;

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

/// Encode a node record body: `label ‖ key ‖ key-value ‖ delta`.
fn encode_node(label: &str, key: &str, value: &Value, delta: &NodeDelta) -> Vec<u8> {
    let mut buf = Vec::new();
    w_str(&mut buf, label);
    w_str(&mut buf, key);
    write_value(&mut buf, value);
    w_patches(&mut buf, &delta.patches, delta.tombstoned);
    buf
}

fn decode_node(mut r: &[u8]) -> Result<(String, String, Value, NodeDelta)> {
    let label = r_str(&mut r)?;
    let key = r_str(&mut r)?;
    let value = read_value(&mut r)?;
    let (patches, tombstoned) = r_patches(&mut r)?;
    Ok((
        label,
        key,
        value,
        NodeDelta {
            patches,
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
    let mut out = Vec::with_capacity(n);
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

/// Write `data` as an off-heap L0 segment directory at `dir` (created fresh). Block
/// sections are sized at `target_block_bytes` (zstd level `zstd_level`). Written to a
/// sibling `.tmp` directory then atomically `rename`d into place, so a reader never
/// observes a partial segment.
pub fn write_segment(
    data: &SegmentData,
    dir: impl AsRef<Path>,
    target_block_bytes: usize,
    zstd_level: i32,
) -> Result<()> {
    let dir = dir.as_ref();
    let tmp = dir.with_extension("tmp");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).with_context(|| format!("create L0 tmp dir {tmp:?}"))?;

    write_blk(&tmp.join("node.blk"), target_block_bytes, zstd_level, |w| {
        for (_, label, key, value, delta) in &data.nodes {
            w.append_record(&encode_node(label, key, value, delta))?;
        }
        Ok(())
    })?;
    write_blk(
        &tmp.join("adj_out.blk"),
        target_block_bytes,
        zstd_level,
        |w| {
            for (_, edges) in &data.adj_out {
                w.append_record(&encode_adj(edges))?;
            }
            Ok(())
        },
    )?;
    write_blk(
        &tmp.join("adj_in.blk"),
        target_block_bytes,
        zstd_level,
        |w| {
            for (_, edges) in &data.adj_in {
                w.append_record(&encode_adj(edges))?;
            }
            Ok(())
        },
    )?;
    write_blk(&tmp.join("edge.blk"), target_block_bytes, zstd_level, |w| {
        for (_, delta) in &data.edges {
            w.append_record(&encode_edge(delta))?;
        }
        Ok(())
    })?;

    let meta = encode_meta(data);
    std::fs::write(tmp.join("meta.bin"), &meta).with_context(|| "write L0 meta.bin")?;

    let _ = std::fs::remove_dir_all(dir);
    std::fs::rename(&tmp, dir).with_context(|| format!("rename L0 segment into place {dir:?}"))?;
    if let Some(parent) = dir.parent() {
        if let Ok(d) = std::fs::File::open(parent) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

fn write_blk(
    path: &Path,
    target: usize,
    level: i32,
    fill: impl FnOnce(&mut BlockFileWriter) -> Result<()>,
) -> Result<()> {
    let mut w = BlockFileWriter::create(path, target, level)
        .with_context(|| format!("create block file {path:?}"))?;
    fill(&mut w)?;
    w.finish()
        .with_context(|| format!("finish block file {path:?}"))?;
    Ok(())
}

fn encode_meta(data: &SegmentData) -> Vec<u8> {
    let mut body = Vec::new();
    write_uvarint(&mut body, OFFHEAP_VERSION);
    for s in [
        data.synthetic_base,
        data.edge_synthetic_base,
        data.node_delta_count,
        data.edge_delta_count,
        data.born_count,
        data.born_edge_count,
    ] {
        write_uvarint(&mut body, s);
    }
    // Key columns (record order == on-disk order == the vectors below).
    w_u64s(&mut body, data.nodes.iter().map(|e| e.0));
    w_u64s(&mut body, data.adj_out.iter().map(|e| e.0));
    w_u64s(&mut body, data.adj_in.iter().map(|e| e.0));
    w_u64s(&mut body, data.edges.iter().map(|e| e.0));
    // Secondary indexes.
    write_uvarint(&mut body, data.born_by_label.len() as u64);
    for (label, ids) in &data.born_by_label {
        w_str(&mut body, label);
        w_u64s(&mut body, ids.iter().copied());
    }
    write_uvarint(&mut body, data.born_index.len() as u64);
    for e in &data.born_index {
        w_str(&mut body, &e.label);
        write_uvarint(&mut body, e.id);
        write_uvarint(&mut body, e.props.len() as u64);
        for (p, v) in &e.props {
            w_str(&mut body, p);
            write_value(&mut body, v);
        }
    }
    write_uvarint(&mut body, data.core_patched.len() as u64);
    for (label, prop, id) in &data.core_patched {
        w_str(&mut body, label);
        w_str(&mut body, prop);
        write_uvarint(&mut body, *id);
    }
    write_uvarint(&mut body, data.born_by_identity.len() as u64);
    for (ck, id) in &data.born_by_identity {
        write_uvarint(&mut body, ck.len() as u64);
        body.extend_from_slice(ck);
        write_uvarint(&mut body, *id);
    }

    let crc = crc32c::crc32c(&body);
    let mut out = Vec::with_capacity(body.len() + 12);
    out.extend_from_slice(META_MAGIC);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

fn w_u64s(buf: &mut Vec<u8>, it: impl ExactSizeIterator<Item = u64>) {
    write_uvarint(buf, it.len() as u64);
    for v in it {
        write_uvarint(buf, v);
    }
}

fn r_u64s(r: &mut &[u8]) -> Result<Vec<u64>> {
    let n = read_uvarint(r)? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(read_uvarint(r)?);
    }
    Ok(out)
}

// ── reader ──────────────────────────────────────────────────────────────────────────

/// An opened off-heap L0 segment: resident key columns + secondary indexes, with the
/// per-entity payloads read on demand through the shared [`BlockCache`].
pub struct L0Reader {
    dir: PathBuf,
    scope: u128,
    cache: Arc<BlockCache>,

    node_rdr: BlockFileReader,
    node_keys: Vec<u64>,
    adj_out_rdr: BlockFileReader,
    adj_out_keys: Vec<u64>,
    adj_in_rdr: BlockFileReader,
    adj_in_keys: Vec<u64>,
    edge_rdr: BlockFileReader,
    edge_keys: Vec<u64>,

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
    /// sections. `scope` must be unique per live segment (it namespaces this segment's
    /// blocks in the shared `cache`); a retired segment's scope is simply never queried
    /// again, so its blocks age out of the LRU.
    pub fn open(dir: impl AsRef<Path>, scope: u128, cache: Arc<BlockCache>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let meta = std::fs::read(dir.join("meta.bin"))
            .with_context(|| format!("read L0 meta {dir:?}/meta.bin"))?;
        if meta.len() < 12 || &meta[..8] != META_MAGIC {
            bail!("L0 segment {dir:?} has bad meta magic");
        }
        let crc = u32::from_le_bytes([meta[8], meta[9], meta[10], meta[11]]);
        let body = &meta[12..];
        if crc32c::crc32c(body) != crc {
            bail!("L0 segment {dir:?} meta failed checksum");
        }
        let mut r = body;
        let version = read_uvarint(&mut r)?;
        if version != OFFHEAP_VERSION {
            bail!("unsupported off-heap L0 version {version} (expected {OFFHEAP_VERSION})");
        }
        let synthetic_base = read_uvarint(&mut r)?;
        let edge_synthetic_base = read_uvarint(&mut r)?;
        let node_delta_count = read_uvarint(&mut r)?;
        let edge_delta_count = read_uvarint(&mut r)?;
        let born_count = read_uvarint(&mut r)?;
        let born_edge_count = read_uvarint(&mut r)?;
        let node_keys = r_u64s(&mut r)?;
        let adj_out_keys = r_u64s(&mut r)?;
        let adj_in_keys = r_u64s(&mut r)?;
        let edge_keys = r_u64s(&mut r)?;

        let n_label = read_uvarint(&mut r)? as usize;
        let mut born_by_label = HashMap::with_capacity(n_label);
        for _ in 0..n_label {
            let label = r_str(&mut r)?;
            born_by_label.insert(label, r_u64s(&mut r)?);
        }
        let n_bi = read_uvarint(&mut r)? as usize;
        let mut born_index = Vec::with_capacity(n_bi);
        for _ in 0..n_bi {
            let label = r_str(&mut r)?;
            let id = read_uvarint(&mut r)?;
            let np = read_uvarint(&mut r)? as usize;
            let mut props = Vec::with_capacity(np);
            for _ in 0..np {
                let p = r_str(&mut r)?;
                let v = read_value(&mut r)?;
                props.push((p, v));
            }
            born_index.push(BornIndexEntry { label, id, props });
        }
        let n_cp = read_uvarint(&mut r)? as usize;
        let mut core_patched = Vec::with_capacity(n_cp);
        for _ in 0..n_cp {
            let label = r_str(&mut r)?;
            let prop = r_str(&mut r)?;
            let id = read_uvarint(&mut r)?;
            core_patched.push((label, prop, id));
        }
        let n_bid = read_uvarint(&mut r)? as usize;
        let mut born_by_identity = HashMap::with_capacity(n_bid);
        for _ in 0..n_bid {
            let len = read_uvarint(&mut r)? as usize;
            if r.len() < len {
                bail!("L0 segment {dir:?}: short identity key");
            }
            let ck = r[..len].to_vec();
            r = &r[len..];
            born_by_identity.insert(ck, read_uvarint(&mut r)?);
        }
        if !r.is_empty() {
            bail!("L0 segment {dir:?} meta has {} trailing bytes", r.len());
        }

        Ok(Self {
            node_rdr: BlockFileReader::open(dir.join("node.blk"))?,
            adj_out_rdr: BlockFileReader::open(dir.join("adj_out.blk"))?,
            adj_in_rdr: BlockFileReader::open(dir.join("adj_in.blk"))?,
            edge_rdr: BlockFileReader::open(dir.join("edge.blk"))?,
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
            born_by_label,
            born_index,
            core_patched,
            born_by_identity,
        })
    }

    /// The segment directory (retired at consolidation).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Fetch record `idx` of `reader` (section `sub`) through the shared cache, returning
    /// its decoded body bytes.
    fn record_bytes(&self, reader: &BlockFileReader, sub: u32, idx: usize) -> Result<Vec<u8>> {
        let rec = self.cache.record(reader, self.scope, sub, idx as u64)?;
        Ok(rec.as_slice().to_vec())
    }

    fn node_entry(&self, dense_id: u64) -> Option<(String, String, Value, NodeDelta)> {
        let idx = self.node_keys.binary_search(&dense_id).ok()?;
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
        let Ok(idx) = self.adj_out_keys.binary_search(&node) else {
            return Vec::new();
        };
        let bytes = self
            .record_bytes(&self.adj_out_rdr, SUB_ADJ_OUT, idx)
            .expect("l0 adj_out record");
        decode_adj(&bytes).expect("decode l0 adj_out")
    }

    fn in_edges(&self, node: u64) -> Vec<DeltaEdge> {
        let Ok(idx) = self.adj_in_keys.binary_search(&node) else {
            return Vec::new();
        };
        let bytes = self
            .record_bytes(&self.adj_in_rdr, SUB_ADJ_IN, idx)
            .expect("l0 adj_in record");
        decode_adj(&bytes).expect("decode l0 adj_in")
    }

    fn edge_delta_owned(&self, edge_id: u64) -> Option<EdgeDelta> {
        let idx = self.edge_keys.binary_search(&edge_id).ok()?;
        let bytes = self
            .record_bytes(&self.edge_rdr, SUB_EDGE, idx)
            .expect("l0 edge record");
        Some(decode_edge(&bytes).expect("decode l0 edge"))
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

#[cfg(test)]
mod tests {
    use super::*;
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
        write_segment(&m.to_segment_data(), &dir, 256, 3).unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        let reader = L0Reader::open(&dir, 0x1234, cache).unwrap();
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
        write_segment(&m.to_segment_data(), &dir, 256, 3).unwrap();
        let mut meta = std::fs::read(dir.join("meta.bin")).unwrap();
        let last = meta.len() - 1;
        meta[last] ^= 0xff;
        std::fs::write(dir.join("meta.bin"), &meta).unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        assert!(L0Reader::open(&dir, 1, cache).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
