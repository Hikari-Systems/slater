// SPDX-License-Identifier: Apache-2.0
//! A **core segment** — the off-heap, at-rest product of a flush in a generation
//! set (the segmented-core track; see `docs/SEGMENTED-CORE-PLAN.md`).
//!
//! A segment is the O(delta) core analogue of a sealed delta level. It is read
//! off-heap exactly like [`slater-delta`'s off-heap L0](../../../slater-delta/src/l0_offheap.rs)
//! — a resident sorted `u64` key column per section, binary-searched to a record
//! index, then the per-entity payload paged through the shared [`BlockCache`]. The
//! decisive difference from an L0 level is that a segment stores **full rows**, not
//! deltas: a node record carries the node's *complete* effective label set and
//! property map, and an edge record its *complete* endpoints, reltype and properties.
//! So a property/label read never folds across segments — the newest segment that
//! holds an id wins in a single record read (the "full-row short-circuit" the read
//! path relies on in Phase 3).
//!
//! # Layout (a segment is a directory)
//! ```text
//! <seg>/node.blk     BlockFile: one full-row record per node the segment carries   (record order = dense-id order)
//! <seg>/adj_out.blk  BlockFile: one adjacency fragment per node with outgoing born/removed edges
//! <seg>/adj_in.blk   BlockFile: one adjacency fragment per node with incoming born/removed edges
//! <seg>/edge.blk     BlockFile: one full-row record per edge the segment carries    (record order = edge-id order)
//! <seg>/meta.bin     resident: MAGIC ‖ crc32c ‖ { version, scope, per-section key columns }
//! ```
//! `push_*` **must** be called in ascending key order per section; the image is staged
//! in a sibling `.tmp` directory and atomically `rename`d in at [`SegmentWriter::finish`].
//!
//! # Fences
//! Each section's resident key column *is* the segment's exact presence set for that
//! id space (a binary-search miss costs no I/O). The min/max **id-band fence**
//! ([`SegmentReader::node_fence`] etc.) is the O(1) pre-filter derived from the sorted
//! column's ends: an id outside `[min, max]` cannot be present, so an untouched node
//! skips the whole segment without even a binary search — the mechanism that keeps a
//! stacked read O(#segments) resident checks + one base block read.
//!
//! # Tombstones
//! A node/edge record with `tombstoned = true` *suppresses* the base (or any older
//! segment's) row for that id. It carries no labels/props/endpoints of its own — it is
//! a deletion marker the read-path fold honours before consulting older levels.
//!
//! # Scope / cache
//! The shared-cache **scope** (a `u128`) is persisted in `meta.bin`, fresh per write, so
//! a segment's paged blocks never collide with another segment's — or with a stale image
//! reusing the same directory after a compaction. A retired segment's scope is simply
//! never queried again and ages out of the LRU.
//!
//! No back-compat: a magic/version/crc mismatch is a hard error on open. `SEGMENT.json`
//! signed marginals, per-index dirty bits and MAC parity are a later slice; this slice is
//! the section format + fences only, and does not wire the read path (Phase 3).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::blockcache::BlockCache;
use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::ids::Value;
use crate::store::{join_key, ObjectStore};
use crate::wire::{read_uvarint, read_value, write_uvarint, write_value};

/// Magic at the head of a segment's `meta.bin`, distinct from a generation MANIFEST
/// (`SLATER01`) and an off-heap L0 meta (`SLL0OFF1`).
const META_MAGIC: &[u8; 8] = b"SLSEG001";
/// Segment section-format version. Bumped on any incompatible change to the on-disk
/// section/meta layout; a reader refuses a version it does not understand.
const SEGMENT_VERSION: u64 = 1;

/// Per-section cache discriminants (the `sub` in a [`BlockCache`] key). Distinct so the
/// four sections of one segment never collide in the shared cache.
const SUB_NODE: u32 = 0;
const SUB_ADJ_OUT: u32 = 1;
const SUB_ADJ_IN: u32 = 2;
const SUB_EDGE: u32 = 3;

// ── record types ─────────────────────────────────────────────────────────────────────

/// A node's **full** effective row as this segment records it: its complete label set and
/// property map. `tombstoned` marks a deletion (labels/props are then empty).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NodeRow {
    pub labels: Vec<String>,
    pub props: Vec<(String, Value)>,
    pub tombstoned: bool,
}

impl NodeRow {
    /// A deletion marker: suppresses the base/older row for this id.
    pub fn tombstone() -> Self {
        Self {
            labels: Vec::new(),
            props: Vec::new(),
            tombstoned: true,
        }
    }
}

/// An edge's **full** row: its endpoints (core dense ids), reltype and property map.
/// `tombstoned` marks a deletion (the row is then a bare marker for `edge_id`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EdgeRow {
    pub src: u64,
    pub dst: u64,
    pub reltype: String,
    pub props: Vec<(String, Value)>,
    pub tombstoned: bool,
}

/// One entry in a node's adjacency **fragment**: a born or removed incident edge. A
/// fragment never rewrites a node's whole neighbour list — it carries only the edges this
/// flush added or removed, folded over the base list at read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjEdge {
    /// The neighbour's dense node id (the *other* endpoint).
    pub other: u64,
    pub reltype: String,
    /// The edge's dense id.
    pub edge_id: u64,
    /// `true` if this fragment entry *removes* the edge (a tombstone in adjacency).
    pub removed: bool,
}

// ── payload codecs ─────────────────────────────────────────────────────────────────────

fn w_str(buf: &mut Vec<u8>, s: &str) {
    write_uvarint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

fn r_str(r: &mut &[u8]) -> Result<String> {
    let n = read_uvarint(r)? as usize;
    if r.len() < n {
        bail!("segment: short string");
    }
    let s = std::str::from_utf8(&r[..n])
        .context("segment: invalid utf8")?
        .to_string();
    *r = &r[n..];
    Ok(s)
}

fn w_props(buf: &mut Vec<u8>, props: &[(String, Value)]) {
    write_uvarint(buf, props.len() as u64);
    for (k, v) in props {
        w_str(buf, k);
        write_value(buf, v);
    }
}

fn r_props(r: &mut &[u8]) -> Result<Vec<(String, Value)>> {
    let n = read_uvarint(r)? as usize;
    // Do not pre-size from the untrusted count: each pair consumes ≥1 byte, so a bogus
    // huge `n` simply runs `r` dry and errors rather than aborting on a giant allocation
    // (these decoders are a fuzz target).
    let mut out = Vec::new();
    for _ in 0..n {
        let k = r_str(r)?;
        let v = read_value(r)?;
        out.push((k, v));
    }
    Ok(out)
}

fn encode_node(row: &NodeRow) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(u8::from(row.tombstoned));
    write_uvarint(&mut buf, row.labels.len() as u64);
    for l in &row.labels {
        w_str(&mut buf, l);
    }
    w_props(&mut buf, &row.props);
    buf
}

fn decode_node(mut r: &[u8]) -> Result<NodeRow> {
    if r.is_empty() {
        bail!("segment: short node record");
    }
    let tombstoned = r[0] != 0;
    r = &r[1..];
    let nl = read_uvarint(&mut r)? as usize;
    let mut labels = Vec::new(); // not pre-sized: fuzz-safe against a bogus count
    for _ in 0..nl {
        labels.push(r_str(&mut r)?);
    }
    let props = r_props(&mut r)?;
    Ok(NodeRow {
        labels,
        props,
        tombstoned,
    })
}

fn encode_edge(row: &EdgeRow) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(u8::from(row.tombstoned));
    write_uvarint(&mut buf, row.src);
    write_uvarint(&mut buf, row.dst);
    w_str(&mut buf, &row.reltype);
    w_props(&mut buf, &row.props);
    buf
}

fn decode_edge(mut r: &[u8]) -> Result<EdgeRow> {
    if r.is_empty() {
        bail!("segment: short edge record");
    }
    let tombstoned = r[0] != 0;
    r = &r[1..];
    let src = read_uvarint(&mut r)?;
    let dst = read_uvarint(&mut r)?;
    let reltype = r_str(&mut r)?;
    let props = r_props(&mut r)?;
    Ok(EdgeRow {
        src,
        dst,
        reltype,
        props,
        tombstoned,
    })
}

fn encode_adj(edges: &[AdjEdge]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_uvarint(&mut buf, edges.len() as u64);
    for e in edges {
        write_uvarint(&mut buf, e.other);
        w_str(&mut buf, &e.reltype);
        write_uvarint(&mut buf, e.edge_id);
        buf.push(u8::from(e.removed));
    }
    buf
}

fn decode_adj(mut r: &[u8]) -> Result<Vec<AdjEdge>> {
    let n = read_uvarint(&mut r)? as usize;
    let mut out = Vec::new(); // not pre-sized: fuzz-safe against a bogus count
    for _ in 0..n {
        let other = read_uvarint(&mut r)?;
        let reltype = r_str(&mut r)?;
        let edge_id = read_uvarint(&mut r)?;
        if r.is_empty() {
            bail!("segment: short adj removed flag");
        }
        let removed = r[0] != 0;
        r = &r[1..];
        out.push(AdjEdge {
            other,
            reltype,
            edge_id,
            removed,
        });
    }
    Ok(out)
}

fn w_u64s(buf: &mut Vec<u8>, keys: &[u64]) {
    write_uvarint(buf, keys.len() as u64);
    for &k in keys {
        write_uvarint(buf, k);
    }
}

fn r_u64s(r: &mut &[u8]) -> Result<Vec<u64>> {
    let n = read_uvarint(r)? as usize;
    let mut out = Vec::new(); // not pre-sized: fuzz-safe against a bogus count
    for _ in 0..n {
        out.push(read_uvarint(r)?);
    }
    Ok(out)
}

// ── public codec surface (goldens + fuzz) ──────────────────────────────────────────────

impl NodeRow {
    /// Encode this node full-row record to its on-disk body bytes.
    pub fn encode(&self) -> Vec<u8> {
        encode_node(self)
    }
    /// Decode a node full-row record body. Never panics on arbitrary bytes — it returns
    /// `Err` on any truncation or bad count (a fuzz invariant).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode_node(bytes)
    }
}

impl EdgeRow {
    /// Encode this edge full-row record to its on-disk body bytes.
    pub fn encode(&self) -> Vec<u8> {
        encode_edge(self)
    }
    /// Decode an edge full-row record body. Never panics on arbitrary bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode_edge(bytes)
    }
}

/// Encode an adjacency fragment (list of born/removed incident edges) to its body bytes.
pub fn encode_adj_fragment(edges: &[AdjEdge]) -> Vec<u8> {
    encode_adj(edges)
}

/// Decode an adjacency fragment body. Never panics on arbitrary bytes.
pub fn decode_adj_fragment(bytes: &[u8]) -> Result<Vec<AdjEdge>> {
    decode_adj(bytes)
}

/// The resident material a segment's `meta.bin` carries: the cache scope and the four
/// per-section sorted key columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub scope: u128,
    pub node_keys: Vec<u64>,
    pub adj_out_keys: Vec<u64>,
    pub adj_in_keys: Vec<u64>,
    pub edge_keys: Vec<u64>,
}

/// Parse and verify a segment `meta.bin` byte image (magic ‖ crc32c ‖ body). Never panics
/// on arbitrary bytes — magic/crc/version/trailing-byte mismatches all return `Err`.
pub fn decode_segment_meta(meta: &[u8]) -> Result<SegmentMeta> {
    if meta.len() < 12 || &meta[..8] != META_MAGIC {
        bail!("segment meta: bad magic");
    }
    let crc = u32::from_le_bytes([meta[8], meta[9], meta[10], meta[11]]);
    let body = &meta[12..];
    if crc32c::crc32c(body) != crc {
        bail!("segment meta: failed checksum");
    }
    let mut r = body;
    let version = read_uvarint(&mut r)?;
    if version != SEGMENT_VERSION {
        bail!("unsupported segment version {version} (this build understands {SEGMENT_VERSION})");
    }
    if r.len() < 16 {
        bail!("segment meta: short (scope)");
    }
    let scope = u128::from_le_bytes(r[..16].try_into().unwrap());
    r = &r[16..];
    let node_keys = r_u64s(&mut r)?;
    let adj_out_keys = r_u64s(&mut r)?;
    let adj_in_keys = r_u64s(&mut r)?;
    let edge_keys = r_u64s(&mut r)?;
    if !r.is_empty() {
        bail!("segment meta: {} trailing bytes", r.len());
    }
    Ok(SegmentMeta {
        scope,
        node_keys,
        adj_out_keys,
        adj_in_keys,
        edge_keys,
    })
}

// ── writer ─────────────────────────────────────────────────────────────────────────────

/// A streaming writer for a core segment. Payload records append to the four block
/// sections incrementally (`BlockFileWriter` buffers one block at a time) while only the
/// compact key columns accumulate in RAM — so a flush or a merge streams sorted runs
/// through here without ever holding the payloads resident.
pub struct SegmentWriter {
    tmp: PathBuf,
    dir: PathBuf,
    scope: u128,
    node: BlockFileWriter,
    node_keys: Vec<u64>,
    adj_out: BlockFileWriter,
    adj_out_keys: Vec<u64>,
    adj_in: BlockFileWriter,
    adj_in_keys: Vec<u64>,
    edge: BlockFileWriter,
    edge_keys: Vec<u64>,
}

impl SegmentWriter {
    /// Create a plaintext segment writer staged at `dir` (its `.tmp` sibling is created
    /// fresh). `scope` tags the shared cache, `target_block_bytes`/`zstd_level` size and
    /// compress the block payloads.
    pub fn create(
        dir: impl AsRef<Path>,
        scope: u128,
        target_block_bytes: usize,
        zstd_level: i32,
    ) -> Result<Self> {
        Self::create_with_cipher(dir, scope, target_block_bytes, zstd_level, None)
    }

    /// [`SegmentWriter::create`], optionally AEAD-encrypting the block sections with
    /// `cipher` (the `meta.bin` self-authenticating MAC is a later slice). `cipher = None`
    /// writes the plaintext format.
    pub fn create_with_cipher(
        dir: impl AsRef<Path>,
        scope: u128,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let tmp = dir.with_extension("tmp");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).with_context(|| format!("create segment tmp dir {tmp:?}"))?;
        let mk = |name: &str| -> Result<BlockFileWriter> {
            let p = tmp.join(name);
            BlockFileWriter::create_with_cipher(&p, target_block_bytes, zstd_level, cipher.clone())
                .with_context(|| format!("create block file {p:?}"))
        };
        Ok(Self {
            node: mk("node.blk")?,
            node_keys: Vec::new(),
            adj_out: mk("adj_out.blk")?,
            adj_out_keys: Vec::new(),
            adj_in: mk("adj_in.blk")?,
            adj_in_keys: Vec::new(),
            edge: mk("edge.blk")?,
            edge_keys: Vec::new(),
            tmp,
            dir,
            scope,
        })
    }

    /// Append a node full-row record. Nodes **must** be pushed in ascending dense-id order.
    pub fn push_node(&mut self, dense: u64, row: &NodeRow) -> Result<()> {
        debug_assert!(
            self.node_keys.last().map_or(true, |&p| dense > p),
            "segment nodes must be pushed in ascending dense-id order"
        );
        self.node.append_record(&encode_node(row))?;
        self.node_keys.push(dense);
        Ok(())
    }

    /// Append a node's outgoing adjacency fragment. Nodes **must** be pushed in ascending
    /// src-id order.
    pub fn push_adj_out(&mut self, node: u64, edges: &[AdjEdge]) -> Result<()> {
        debug_assert!(self.adj_out_keys.last().map_or(true, |&p| node > p));
        self.adj_out.append_record(&encode_adj(edges))?;
        self.adj_out_keys.push(node);
        Ok(())
    }

    /// Append a node's incoming adjacency fragment. Nodes **must** be pushed in ascending
    /// dst-id order.
    pub fn push_adj_in(&mut self, node: u64, edges: &[AdjEdge]) -> Result<()> {
        debug_assert!(self.adj_in_keys.last().map_or(true, |&p| node > p));
        self.adj_in.append_record(&encode_adj(edges))?;
        self.adj_in_keys.push(node);
        Ok(())
    }

    /// Append an edge full-row record. Edges **must** be pushed in ascending edge-id order.
    pub fn push_edge(&mut self, edge_id: u64, row: &EdgeRow) -> Result<()> {
        debug_assert!(self.edge_keys.last().map_or(true, |&p| edge_id > p));
        self.edge.append_record(&encode_edge(row))?;
        self.edge_keys.push(edge_id);
        Ok(())
    }

    /// Seal the sections, write `meta.bin`, and atomically rename the staged `.tmp`
    /// directory into place.
    pub fn finish(self) -> Result<()> {
        let meta = self.meta_bytes();
        self.node.finish().context("finish node.blk")?;
        self.adj_out.finish().context("finish adj_out.blk")?;
        self.adj_in.finish().context("finish adj_in.blk")?;
        self.edge.finish().context("finish edge.blk")?;
        std::fs::write(self.tmp.join("meta.bin"), &meta).context("write segment meta.bin")?;
        let _ = std::fs::remove_dir_all(&self.dir);
        std::fs::rename(&self.tmp, &self.dir)
            .with_context(|| format!("rename segment into place {:?}", self.dir))?;
        if let Some(parent) = self.dir.parent() {
            if let Ok(d) = std::fs::File::open(parent) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }

    fn meta_bytes(&self) -> Vec<u8> {
        let mut body = Vec::new();
        write_uvarint(&mut body, SEGMENT_VERSION);
        body.extend_from_slice(&self.scope.to_le_bytes());
        w_u64s(&mut body, &self.node_keys);
        w_u64s(&mut body, &self.adj_out_keys);
        w_u64s(&mut body, &self.adj_in_keys);
        w_u64s(&mut body, &self.edge_keys);
        let crc = crc32c::crc32c(&body);
        let mut out = Vec::with_capacity(body.len() + 12);
        out.extend_from_slice(META_MAGIC);
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&body);
        out
    }
}

// ── reader ─────────────────────────────────────────────────────────────────────────────

/// An opened core segment: resident sorted key columns per section, with the per-entity
/// payloads read on demand through the shared [`BlockCache`].
pub struct SegmentReader {
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
}

impl std::fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentReader")
            .field("dir", &self.dir)
            .field("scope", &self.scope)
            .field("nodes", &self.node_keys.len())
            .field("edges", &self.edge_keys.len())
            .finish()
    }
}

impl SegmentReader {
    /// Open a plaintext segment directory.
    pub fn open(dir: impl AsRef<Path>, cache: Arc<BlockCache>) -> Result<Self> {
        Self::open_with_cipher(dir, cache, None)
    }

    /// Open a segment directory, decrypting the block sections with `cipher` if they were
    /// written encrypted. Verifies `meta.bin` magic/version/crc.
    pub fn open_with_cipher(
        dir: impl AsRef<Path>,
        cache: Arc<BlockCache>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let meta_bytes = std::fs::read(dir.join("meta.bin"))
            .with_context(|| format!("read segment meta {dir:?}/meta.bin"))?;
        let SegmentMeta {
            scope,
            node_keys,
            adj_out_keys,
            adj_in_keys,
            edge_keys,
        } = decode_segment_meta(&meta_bytes).with_context(|| format!("segment {dir:?} meta"))?;
        let open = |name: &str| -> Result<BlockFileReader> {
            BlockFileReader::open_with_cipher(dir.join(name), cipher.clone())
                .with_context(|| format!("open {name}"))
        };
        Ok(Self {
            node_rdr: open("node.blk")?,
            adj_out_rdr: open("adj_out.blk")?,
            adj_in_rdr: open("adj_in.blk")?,
            edge_rdr: open("edge.blk")?,
            dir,
            scope,
            cache,
            node_keys,
            adj_out_keys,
            adj_in_keys,
            edge_keys,
        })
    }

    /// Store-native counterpart of [`open_with_cipher`](SegmentReader::open_with_cipher) —
    /// reads `meta.bin` and pages the four block sections through `store` under key prefix
    /// `prefix`, so a segment on any backend (fs / mem / S3) opens exactly like the base
    /// generation's `.blk` files. Verifies `meta.bin` magic/version/crc.
    pub fn open_via(
        store: &dyn ObjectStore,
        prefix: &str,
        cache: Arc<BlockCache>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let meta_key = join_key(prefix, "meta.bin");
        let meta_bytes = store
            .read_all(&meta_key)
            .with_context(|| format!("read segment meta {meta_key}"))?;
        let SegmentMeta {
            scope,
            node_keys,
            adj_out_keys,
            adj_in_keys,
            edge_keys,
        } = decode_segment_meta(&meta_bytes).with_context(|| format!("segment {prefix} meta"))?;
        let open = |name: &str| -> Result<BlockFileReader> {
            BlockFileReader::open_src(store.open(&join_key(prefix, name))?, cipher.clone())
                .with_context(|| format!("open {prefix}/{name}"))
        };
        Ok(Self {
            node_rdr: open("node.blk")?,
            adj_out_rdr: open("adj_out.blk")?,
            adj_in_rdr: open("adj_in.blk")?,
            edge_rdr: open("edge.blk")?,
            dir: PathBuf::from(prefix),
            scope,
            cache,
            node_keys,
            adj_out_keys,
            adj_in_keys,
            edge_keys,
        })
    }

    /// The segment directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // ── resident key columns (the exact presence sets) ─────────────────────────────────
    pub fn node_ids(&self) -> &[u64] {
        &self.node_keys
    }
    pub fn adj_out_ids(&self) -> &[u64] {
        &self.adj_out_keys
    }
    pub fn adj_in_ids(&self) -> &[u64] {
        &self.adj_in_keys
    }
    pub fn edge_ids(&self) -> &[u64] {
        &self.edge_keys
    }

    // ── id-band fences (min, max) — the O(1) skip pre-filter ───────────────────────────
    fn fence(keys: &[u64]) -> Option<(u64, u64)> {
        match (keys.first(), keys.last()) {
            (Some(&lo), Some(&hi)) => Some((lo, hi)),
            _ => None,
        }
    }
    /// `[min, max]` node dense ids this segment carries, or `None` if it carries none.
    pub fn node_fence(&self) -> Option<(u64, u64)> {
        Self::fence(&self.node_keys)
    }
    /// `[min, max]` edge dense ids this segment carries, or `None`.
    pub fn edge_fence(&self) -> Option<(u64, u64)> {
        Self::fence(&self.edge_keys)
    }
    /// Whether `dense` *could* be present (inside the node fence). A `false` lets a caller
    /// skip this segment with no binary search; a `true` still requires [`node_row`] to
    /// confirm. Empty segment ⇒ always `false`.
    ///
    /// [`node_row`]: SegmentReader::node_row
    pub fn may_hold_node(&self, dense: u64) -> bool {
        matches!(self.node_fence(), Some((lo, hi)) if lo <= dense && dense <= hi)
    }
    /// Whether `edge_id` *could* be present (inside the edge fence).
    pub fn may_hold_edge(&self, edge_id: u64) -> bool {
        matches!(self.edge_fence(), Some((lo, hi)) if lo <= edge_id && edge_id <= hi)
    }
    /// `[min, max]` node ids carrying an **outgoing** adjacency fragment, or `None`. Distinct
    /// from [`node_fence`](Self::node_fence): a node whose only change is a new incident edge
    /// carries an adjacency fragment but *no* node row, so the node fence would wrongly skip
    /// it. Adjacency gating must use this fence.
    pub fn out_adj_fence(&self) -> Option<(u64, u64)> {
        Self::fence(&self.adj_out_keys)
    }
    /// `[min, max]` node ids carrying an **incoming** adjacency fragment, or `None`.
    pub fn in_adj_fence(&self) -> Option<(u64, u64)> {
        Self::fence(&self.adj_in_keys)
    }
    /// Whether `node` *could* carry an outgoing fragment (inside [`out_adj_fence`]). A `false`
    /// skips the segment for this node's out-adjacency with no binary search.
    ///
    /// [`out_adj_fence`]: SegmentReader::out_adj_fence
    pub fn may_hold_out_adj(&self, node: u64) -> bool {
        matches!(self.out_adj_fence(), Some((lo, hi)) if lo <= node && node <= hi)
    }
    /// Whether `node` *could* carry an incoming fragment (inside [`in_adj_fence`]).
    ///
    /// [`in_adj_fence`]: SegmentReader::in_adj_fence
    pub fn may_hold_in_adj(&self, node: u64) -> bool {
        matches!(self.in_adj_fence(), Some((lo, hi)) if lo <= node && node <= hi)
    }

    fn record_bytes(&self, reader: &BlockFileReader, sub: u32, idx: usize) -> Result<Vec<u8>> {
        let rec = self.cache.record(reader, self.scope, sub, idx as u64)?;
        Ok(rec.as_slice().to_vec())
    }

    /// The full node row for `dense`, or `None` if this segment does not carry it. A hit
    /// pages one block; a miss is a resident binary search with no I/O.
    pub fn node_row(&self, dense: u64) -> Result<Option<NodeRow>> {
        let Ok(idx) = self.node_keys.binary_search(&dense) else {
            return Ok(None);
        };
        let bytes = self.record_bytes(&self.node_rdr, SUB_NODE, idx)?;
        Ok(Some(decode_node(&bytes)?))
    }

    /// The full edge row for `edge_id`, or `None`.
    pub fn edge_row(&self, edge_id: u64) -> Result<Option<EdgeRow>> {
        let Ok(idx) = self.edge_keys.binary_search(&edge_id) else {
            return Ok(None);
        };
        let bytes = self.record_bytes(&self.edge_rdr, SUB_EDGE, idx)?;
        Ok(Some(decode_edge(&bytes)?))
    }

    /// This node's outgoing adjacency fragment (born/removed edges), or an empty vec if the
    /// segment carries no outgoing fragment for it.
    pub fn out_adj(&self, node: u64) -> Result<Vec<AdjEdge>> {
        let Ok(idx) = self.adj_out_keys.binary_search(&node) else {
            return Ok(Vec::new());
        };
        let bytes = self.record_bytes(&self.adj_out_rdr, SUB_ADJ_OUT, idx)?;
        decode_adj(&bytes)
    }

    /// This node's incoming adjacency fragment, or an empty vec.
    pub fn in_adj(&self, node: u64) -> Result<Vec<AdjEdge>> {
        let Ok(idx) = self.adj_in_keys.binary_search(&node) else {
            return Ok(Vec::new());
        };
        let bytes = self.record_bytes(&self.adj_in_rdr, SUB_ADJ_IN, idx)?;
        decode_adj(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("slater_seg_{tag}_{}", std::process::id()))
    }

    fn node(labels: &[&str], props: &[(&str, Value)]) -> NodeRow {
        NodeRow {
            labels: labels.iter().map(|s| s.to_string()).collect(),
            props: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            tombstoned: false,
        }
    }

    fn write_sample(dir: &Path, block_bytes: usize, cipher: Option<Arc<BlockCipher>>) {
        let _ = std::fs::remove_dir_all(dir);
        let mut w = SegmentWriter::create_with_cipher(dir, 0xABCD, block_bytes, 3, cipher).unwrap();
        // Nodes 10, 12, 15 (12 is a tombstone).
        w.push_node(
            10,
            &node(
                &["Person"],
                &[("age", Value::Int(30)), ("name", Value::Str("Al".into()))],
            ),
        )
        .unwrap();
        w.push_node(12, &NodeRow::tombstone()).unwrap();
        w.push_node(15, &node(&["City", "Place"], &[("pop", Value::Int(9))]))
            .unwrap();
        // Adjacency fragments.
        w.push_adj_out(
            10,
            &[
                AdjEdge {
                    other: 15,
                    reltype: "IN".into(),
                    edge_id: 100,
                    removed: false,
                },
                AdjEdge {
                    other: 12,
                    reltype: "KNOWS".into(),
                    edge_id: 101,
                    removed: true,
                },
            ],
        )
        .unwrap();
        w.push_adj_in(
            15,
            &[AdjEdge {
                other: 10,
                reltype: "IN".into(),
                edge_id: 100,
                removed: false,
            }],
        )
        .unwrap();
        // Edges 100 (born), 101 (tombstone).
        w.push_edge(
            100,
            &EdgeRow {
                src: 10,
                dst: 15,
                reltype: "IN".into(),
                props: vec![("since".into(), Value::Int(2020))],
                tombstoned: false,
            },
        )
        .unwrap();
        w.push_edge(
            101,
            &EdgeRow {
                src: 10,
                dst: 12,
                reltype: "KNOWS".into(),
                props: vec![],
                tombstoned: true,
            },
        )
        .unwrap();
        w.finish().unwrap();
    }

    fn assert_sample(r: &SegmentReader) {
        // Full-row node reads.
        let n10 = r.node_row(10).unwrap().unwrap();
        assert_eq!(n10.labels, vec!["Person".to_string()]);
        assert_eq!(
            n10.props,
            vec![
                ("age".to_string(), Value::Int(30)),
                ("name".to_string(), Value::Str("Al".into())),
            ]
        );
        assert!(!n10.tombstoned);
        let n12 = r.node_row(12).unwrap().unwrap();
        assert!(n12.tombstoned);
        assert!(n12.labels.is_empty() && n12.props.is_empty());
        let n15 = r.node_row(15).unwrap().unwrap();
        assert_eq!(n15.labels, vec!["City".to_string(), "Place".to_string()]);
        // Misses cost no I/O and return None.
        assert!(r.node_row(11).unwrap().is_none());
        assert!(r.node_row(99).unwrap().is_none());

        // Edge full rows.
        let e100 = r.edge_row(100).unwrap().unwrap();
        assert_eq!((e100.src, e100.dst, e100.reltype.as_str()), (10, 15, "IN"));
        assert_eq!(e100.props, vec![("since".to_string(), Value::Int(2020))]);
        let e101 = r.edge_row(101).unwrap().unwrap();
        assert!(e101.tombstoned);
        assert!(r.edge_row(50).unwrap().is_none());

        // Adjacency fragments.
        let out10 = r.out_adj(10).unwrap();
        assert_eq!(out10.len(), 2);
        assert_eq!(
            out10[0],
            AdjEdge {
                other: 15,
                reltype: "IN".into(),
                edge_id: 100,
                removed: false
            }
        );
        assert!(out10[1].removed);
        assert!(r.out_adj(15).unwrap().is_empty());
        assert_eq!(r.in_adj(15).unwrap().len(), 1);
        assert!(r.in_adj(10).unwrap().is_empty());

        // Fences and the O(1) skip pre-filter.
        assert_eq!(r.node_fence(), Some((10, 15)));
        assert_eq!(r.edge_fence(), Some((100, 101)));
        assert!(!r.may_hold_node(9));
        assert!(r.may_hold_node(12));
        assert!(!r.may_hold_node(16));
        assert!(!r.may_hold_edge(99));
        assert!(r.may_hold_edge(100));
        assert_eq!(r.node_ids(), &[10, 12, 15]);
        assert_eq!(r.edge_ids(), &[100, 101]);
        // Adjacency fences are separate from the node fence: node 10 carries the only
        // outgoing fragment, node 15 the only incoming one.
        assert_eq!(r.out_adj_fence(), Some((10, 10)));
        assert_eq!(r.in_adj_fence(), Some((15, 15)));
        assert!(r.may_hold_out_adj(10) && !r.may_hold_out_adj(15));
        assert!(r.may_hold_in_adj(15) && !r.may_hold_in_adj(10));
    }

    #[test]
    fn round_trip_full_rows_fences_and_misses() {
        let dir = tmp("rt");
        write_sample(&dir, 4096, None);
        let cache = Arc::new(BlockCache::new(1 << 20));
        let r = SegmentReader::open(&dir, cache).unwrap();
        assert_sample(&r);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn round_trip_with_tiny_blocks_spans_many_blocks() {
        // A block size small enough that each section spans several paged blocks — exercises
        // the cache's block-boundary record location.
        let dir = tmp("tiny");
        write_sample(&dir, 16, None);
        let cache = Arc::new(BlockCache::new(1 << 20));
        let r = SegmentReader::open(&dir, cache).unwrap();
        assert_sample(&r);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn round_trips_via_object_store() {
        // The store-native open path must read byte-identically to the fs open path — this
        // is what lets a stacked set's segments live on mem / S3 like the base generation.
        use crate::store::mem::MemObjectStore;
        let dir = tmp("via");
        write_sample(&dir, 64, None);
        let store = MemObjectStore::new();
        for name in [
            "meta.bin",
            "node.blk",
            "adj_out.blk",
            "adj_in.blk",
            "edge.blk",
        ] {
            let bytes = std::fs::read(dir.join(name)).unwrap();
            store.put(&format!("seg/{name}"), &bytes, None).unwrap();
        }
        let cache = Arc::new(BlockCache::new(1 << 20));
        let r = SegmentReader::open_via(&store, "seg", cache, None).unwrap();
        assert_sample(&r);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn encrypted_round_trips_and_refuses_without_key() {
        let dir = tmp("enc");
        let cipher = Arc::new(BlockCipher::from_key(&[7u8; 32]));
        write_sample(&dir, 64, Some(cipher.clone()));
        let cache = Arc::new(BlockCache::new(1 << 20));
        // With the key: identical reads.
        let r = SegmentReader::open_with_cipher(&dir, cache.clone(), Some(cipher)).unwrap();
        assert_sample(&r);
        // Without the key: the encrypted block sections refuse to open.
        assert!(SegmentReader::open(&dir, cache).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_segment_round_trips() {
        let dir = tmp("empty");
        let _ = std::fs::remove_dir_all(&dir);
        SegmentWriter::create(&dir, 1, 4096, 3)
            .unwrap()
            .finish()
            .unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        let r = SegmentReader::open(&dir, cache).unwrap();
        assert_eq!(r.node_fence(), None);
        assert_eq!(r.edge_fence(), None);
        assert!(!r.may_hold_node(0));
        assert!(r.node_row(0).unwrap().is_none());
        assert!(r.out_adj(0).unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_meta_is_rejected() {
        let dir = tmp("corrupt");
        write_sample(&dir, 64, None);
        let mut meta = std::fs::read(dir.join("meta.bin")).unwrap();
        let last = meta.len() - 1;
        meta[last] ^= 0xff;
        std::fs::write(dir.join("meta.bin"), &meta).unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        assert!(SegmentReader::open(&dir, cache).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn foreign_meta_magic_is_rejected() {
        let dir = tmp("magic");
        write_sample(&dir, 64, None);
        let mut meta = std::fs::read(dir.join("meta.bin")).unwrap();
        meta[..8].copy_from_slice(b"NOTASEG0");
        std::fs::write(dir.join("meta.bin"), &meta).unwrap();
        let cache = Arc::new(BlockCache::new(1 << 20));
        assert!(SegmentReader::open(&dir, cache).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── hand-computed codec goldens ────────────────────────────────────────────────────
    // Exact byte images for known records, so an accidental codec change is caught even if
    // encode/decode stay mutually consistent. See the module codec section for the layout.

    #[test]
    fn node_record_golden() {
        // labels ["A"], props [("k", Int 1)], live:
        //   00            tombstoned=false
        //   01            labels len = 1
        //   01 41         w_str "A"
        //   01            props len = 1
        //   01 6B         w_str "k"
        //   02 02         Int: TAG_INT(2), zigzag(1)=2
        let row = NodeRow {
            labels: vec!["A".into()],
            props: vec![("k".into(), Value::Int(1))],
            tombstoned: false,
        };
        assert_eq!(
            row.encode(),
            vec![0x00, 0x01, 0x01, 0x41, 0x01, 0x01, 0x6B, 0x02, 0x02]
        );
        assert_eq!(NodeRow::decode(&row.encode()).unwrap(), row);
        // Tombstone: 01 (flag) 00 (no labels) 00 (no props).
        assert_eq!(NodeRow::tombstone().encode(), vec![0x01, 0x00, 0x00]);
    }

    #[test]
    fn edge_record_golden() {
        // src 10, dst 15, reltype "R", no props, live:
        //   00  0A  0F  01 52  00
        let row = EdgeRow {
            src: 10,
            dst: 15,
            reltype: "R".into(),
            props: vec![],
            tombstoned: false,
        };
        assert_eq!(row.encode(), vec![0x00, 0x0A, 0x0F, 0x01, 0x52, 0x00]);
        assert_eq!(EdgeRow::decode(&row.encode()).unwrap(), row);
    }

    #[test]
    fn adj_fragment_golden() {
        // one entry: other 3, reltype "T", edge_id 7, removed:
        //   01  03  01 54  07  01
        let edges = vec![AdjEdge {
            other: 3,
            reltype: "T".into(),
            edge_id: 7,
            removed: true,
        }];
        assert_eq!(
            encode_adj_fragment(&edges),
            vec![0x01, 0x03, 0x01, 0x54, 0x07, 0x01]
        );
        assert_eq!(
            decode_adj_fragment(&encode_adj_fragment(&edges)).unwrap(),
            edges
        );
    }

    #[test]
    fn decoders_never_panic_on_arbitrary_bytes() {
        // The property the fuzz targets assert, exercised over a spread of inputs including
        // bogus counts that must error (not abort on a giant allocation) rather than panic.
        for len in 0..40usize {
            let bytes: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(37)).collect();
            let _ = NodeRow::decode(&bytes);
            let _ = EdgeRow::decode(&bytes);
            let _ = decode_adj_fragment(&bytes);
            let _ = decode_segment_meta(&bytes);
        }
        // A record whose count varints claim a huge length must Err, not OOM-abort.
        assert!(NodeRow::decode(&[0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F]).is_err());
        assert!(decode_adj_fragment(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]).is_err());
    }

    #[test]
    fn meta_decode_matches_written_segment() {
        // The public `decode_segment_meta` reproduces exactly what a writer put in meta.bin.
        let dir = tmp("meta");
        write_sample(&dir, 4096, None);
        let bytes = std::fs::read(dir.join("meta.bin")).unwrap();
        let m = decode_segment_meta(&bytes).unwrap();
        assert_eq!(m.scope, 0xABCD);
        assert_eq!(m.node_keys, vec![10, 12, 15]);
        assert_eq!(m.adj_out_keys, vec![10]);
        assert_eq!(m.adj_in_keys, vec![15]);
        assert_eq!(m.edge_keys, vec![100, 101]);
        std::fs::remove_dir_all(&dir).ok();
    }
}
