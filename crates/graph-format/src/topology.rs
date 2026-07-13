// SPDX-License-Identifier: Apache-2.0
//! CSR adjacency — forward and reverse — so the reader can traverse either
//! direction without a scan.
//!
//! Both directions live in one `topology.csr.blk`: records `0..N` are each node's
//! **outgoing** adjacency in node-id order, records `N..2N` are each node's
//! **incoming** adjacency. Isolated nodes get an empty (count 0) record so the global
//! index stays aligned with the dense node id.
//!
//! ## Record layout: a reltype directory over per-run neighbour bytes
//!
//! Reltype is factored **out** of the per-edge loop into a small per-record **directory** — one
//! `(reltype, run_count, run_nbytes)` entry per maximal run of consecutive same-reltype edges —
//! followed by the concatenated per-run neighbour bytes. Because the builder sorts each node's
//! edges reltype-major (`(src, reltype, dst)` forward, `(dst, reltype, src)` reverse), a node has
//! one run per distinct reltype. `run_nbytes` lets [`decode_adj_into_filtered`] **skip** a whole
//! non-matching reltype run without decoding it — so `(v)-[:T]->(?)` on a hub touches only `T`'s
//! run instead of decoding every neighbour and filtering afterwards. Factoring reltype into the
//! directory also shrinks the record (one reltype varint per *run*, not per *edge*).
//!
//! The two halves differ only in the edge-id encoding, because edge ids are dense-contiguous in
//! the **forward** direction but not the reverse:
//! - **forward** (`0..N`): `uvarint(count) ‖ [ u8(flag) ‖ … ] (when count>0)`. The builder
//!   assigns edge ids as a running counter in forward-CSR storage order, so a source node's
//!   outgoing edges have gap-free ascending ids — then `flag = CONTIGUOUS`, the head carries one
//!   `uvarint(edge_id_base)`, and the `k`-th edge's id is `edge_id_base + k` (`k` counts across
//!   runs, in record order). Body per run is `run_count × uvarint(neighbour)`. A writer that
//!   produces non-contiguous ids (a fixture) sets `flag = EXPLICIT` and the body keeps a per-edge
//!   id (`run_count × ( uvarint(neighbour) ‖ uvarint(edge_id) )`) — always exact, just no saving.
//! - **reverse** (`N..2N`): no flag; the body always carries per-edge ids
//!   (`run_count × ( uvarint(neighbour) ‖ uvarint(edge_id) )`), since a node's incoming edges
//!   come from many sources (a sparse ascending id subset with no base to derive from).
//!
//! `edge_id` must equal the dense `edge_props.blk` row index (and the segment/delta join key),
//! so the forward `base + k` derivation reproduces the exact absolute ids the builder wrote — the
//! builder assigns `final_edge_id` and writes `edge_props` in the same forward-emit order, so the
//! reltype-major sort reorders both together. Decoders take the record's direction (`forward`),
//! which every reader knows from the global record index (`< N` ⇒ forward). `adj_count` reads
//! only the leading count uvarint, identical in both halves.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};

use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::ids::{EdgeId, NodeId};
use crate::wire::{read_uvarint, write_uvarint};

/// One adjacency entry: a typed edge to a neighbouring node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adj {
    pub reltype: u32,
    pub neighbour: NodeId,
    pub edge: EdgeId,
}

/// An edge in builder input form.
#[derive(Debug, Clone, Copy)]
pub struct Edge {
    pub src: NodeId,
    pub dst: NodeId,
    pub reltype: u32,
    pub edge: EdgeId,
}

/// Marks a non-empty forward record as `edge_id_base`-encoded (`CONTIGUOUS`) or per-edge
/// (`EXPLICIT`). The one-byte flag follows the count uvarint.
const FWD_CONTIGUOUS: u8 = 1;
const FWD_EXPLICIT: u8 = 0;

/// Build the **reltype directory** and neighbour body for an adjacency list. Reltype is factored
/// out of the per-edge loop into a directory of consecutive-equal-reltype *runs* — one
/// `(reltype, run_count, run_nbytes)` entry per run — followed by the concatenated per-run
/// neighbour bytes. `run_nbytes` is the byte length a run's neighbours occupy, so a
/// reltype-filtered decode can skip a whole non-matching run without decoding it (the typed-expand
/// win). `per_edge_id` appends each edge's id after its neighbour (EXPLICIT forward / reverse);
/// under `edge_id_base` (CONTIGUOUS forward) the id is derived and omitted. Runs preserve list
/// order, so the `k`-th emitted edge is the `k`-th list element — which keeps `base + k` exact.
fn encode_runs(list: &[Adj], per_edge_id: bool) -> (Vec<(u32, usize, usize)>, Vec<u8>) {
    let mut dir: Vec<(u32, usize, usize)> = Vec::new();
    let mut body: Vec<u8> = Vec::new();
    let mut i = 0;
    while i < list.len() {
        let rt = list[i].reltype;
        let start = body.len();
        let mut j = i;
        while j < list.len() && list[j].reltype == rt {
            write_uvarint(&mut body, list[j].neighbour.0);
            if per_edge_id {
                write_uvarint(&mut body, list[j].edge.0);
            }
            j += 1;
        }
        dir.push((rt, j - i, body.len() - start));
        i = j;
    }
    (dir, body)
}

/// Append `uvarint(num_runs) ‖ num_runs × (uvarint(reltype) ‖ uvarint(run_count) ‖
/// uvarint(run_nbytes))`.
fn write_directory(rec: &mut Vec<u8>, dir: &[(u32, usize, usize)]) {
    write_uvarint(rec, dir.len() as u64);
    for &(rt, rc, nb) in dir {
        write_uvarint(rec, rt as u64);
        write_uvarint(rec, rc as u64);
        write_uvarint(rec, nb as u64);
    }
}

/// Encode a **forward** (outgoing) adjacency record: `uvarint(count) ‖ u8(flag) ‖ … `. When a
/// source's outgoing edge ids are dense-contiguous — which the real builder always produces (edge
/// ids are a running counter in forward-CSR order) — the per-edge `edge_id` collapses to one
/// `edge_id_base` at the head (`CONTIGUOUS`), derived on read as `base + k`. Otherwise (a
/// hand-written fixture, or a writer that assigns ids out of forward order) it falls back to
/// per-edge ids (`EXPLICIT`), always exact. Both carry the [reltype directory](encode_runs).
fn encode_adj_forward(list: &[Adj]) -> Vec<u8> {
    let mut rec = Vec::new();
    write_uvarint(&mut rec, list.len() as u64);
    let Some(first) = list.first() else {
        return rec; // empty record: just the count-0
    };
    let base = first.edge.0;
    let contiguous = list
        .iter()
        .enumerate()
        .all(|(k, a)| a.edge.0 == base + k as u64);
    if contiguous {
        rec.push(FWD_CONTIGUOUS);
        write_uvarint(&mut rec, base);
        let (dir, body) = encode_runs(list, false);
        write_directory(&mut rec, &dir);
        rec.extend_from_slice(&body);
    } else {
        rec.push(FWD_EXPLICIT);
        let (dir, body) = encode_runs(list, true);
        write_directory(&mut rec, &dir);
        rec.extend_from_slice(&body);
    }
    rec
}

/// Encode a **reverse** (incoming) adjacency record: incoming edge ids are a sparse ascending
/// subset (edges from many sources), so keep the per-edge `edge_id`. Carries the same reltype
/// directory as forward (no flag byte — reverse is always per-edge id).
fn encode_adj_reverse(list: &[Adj]) -> Vec<u8> {
    let mut rec = Vec::new();
    write_uvarint(&mut rec, list.len() as u64);
    if list.is_empty() {
        return rec;
    }
    let (dir, body) = encode_runs(list, true);
    write_directory(&mut rec, &dir);
    rec.extend_from_slice(&body);
    rec
}

/// The edge count of an adjacency record — its leading uvarint — **without** decoding
/// the edges. O(1); lets a reader decide a node is a hub from the record bytes it
/// already holds, before paying to decode a multi-million-edge neighbour list.
pub fn adj_count(rec: &[u8]) -> Result<u64> {
    read_uvarint(&mut { rec })
}

/// Parsed record header: whether edge ids are derived (`base + k`) or per-edge, the base, and the
/// reltype directory — followed by the remaining `body` (the concatenated per-run neighbour bytes).
struct AdjHeader<'a> {
    /// `true` ⇒ CONTIGUOUS forward: edge id is `base + k`, no per-edge id in the body. `false` ⇒
    /// EXPLICIT forward or reverse: each edge carries its own id after its neighbour.
    derive_base: bool,
    base: u64,
    dir: Vec<(u32, usize, usize)>,
    body: &'a [u8],
}

/// Parse a record's count, flag/base and reltype directory. Returns `None` for an empty record.
fn parse_adj_header(rec: &[u8], forward: bool) -> Result<Option<AdjHeader<'_>>> {
    let mut r = rec;
    let count = read_uvarint(&mut r)? as usize;
    if count == 0 {
        return Ok(None);
    }
    // A non-empty forward record carries a one-byte flag after the count: CONTIGUOUS ⇒ one
    // `edge_id_base` follows and the k-th edge's id is `base + k`; EXPLICIT ⇒ per-edge ids.
    // Reverse records have no flag and always carry per-edge ids.
    let (derive_base, base) = if forward {
        if r.is_empty() {
            bail!("truncated forward adjacency record (missing codec flag)");
        }
        let flag = r[0];
        r = &r[1..];
        match flag {
            FWD_CONTIGUOUS => (true, read_uvarint(&mut r)?),
            FWD_EXPLICIT => (false, 0),
            other => bail!("unknown forward adjacency flag {other}"),
        }
    } else {
        (false, 0)
    };
    let num_runs = read_uvarint(&mut r)? as usize;
    let mut dir = Vec::with_capacity(num_runs);
    for _ in 0..num_runs {
        let rt = read_uvarint(&mut r)? as u32;
        let rc = read_uvarint(&mut r)? as usize;
        let nb = read_uvarint(&mut r)? as usize;
        dir.push((rt, rc, nb));
    }
    Ok(Some(AdjHeader {
        derive_base,
        base,
        dir,
        body: r,
    }))
}

/// Decode one node's adjacency record edge-by-edge, invoking `f` for each [`Adj`]
/// **without materialising the whole neighbour list**. `forward` selects the record format
/// (see the module docs): a forward record derives `edge_id = edge_id_base + k`, a reverse
/// record reads a per-edge id. It stays streamable — the only unbounded part of a hub node's
/// adjacency (out-degree in the millions) never needs to be held resident. [`decode_adj`] is
/// this visitor collected into a `Vec`; the streaming adjacency reader drives it in bounded
/// chunks. See [`decode_adj_into_filtered`] to decode only selected reltypes.
pub fn decode_adj_into(
    rec: &[u8],
    forward: bool,
    mut f: impl FnMut(Adj) -> Result<()>,
) -> Result<()> {
    let Some(mut h) = parse_adj_header(rec, forward)? else {
        return Ok(());
    };
    let mut k = 0u64;
    for (rt, rc, _nb) in h.dir {
        for _ in 0..rc {
            let neighbour = NodeId(read_uvarint(&mut h.body)?);
            let edge = EdgeId(if h.derive_base {
                h.base + k
            } else {
                read_uvarint(&mut h.body)?
            });
            f(Adj {
                reltype: rt,
                neighbour,
                edge,
            })?;
            k += 1;
        }
    }
    Ok(())
}

/// Decode only the edges whose reltype satisfies `want`, invoking `f` for each — skipping a
/// non-matching reltype run **without decoding its neighbour bytes** (the run's `run_nbytes` in
/// the directory is jumped over). This is the typed-expand fast path: `(v)-[:T]->(?)` on a hub
/// touches only `T`'s run instead of decoding every neighbour of every reltype and filtering
/// afterwards. `base + k` stays exact because `k` advances across skipped runs too.
pub fn decode_adj_into_filtered(
    rec: &[u8],
    forward: bool,
    want: impl Fn(u32) -> bool,
    mut f: impl FnMut(Adj) -> Result<()>,
) -> Result<()> {
    let Some(mut h) = parse_adj_header(rec, forward)? else {
        return Ok(());
    };
    let mut k = 0u64;
    for (rt, rc, nb) in h.dir {
        if want(rt) {
            for _ in 0..rc {
                let neighbour = NodeId(read_uvarint(&mut h.body)?);
                let edge = EdgeId(if h.derive_base {
                    h.base + k
                } else {
                    read_uvarint(&mut h.body)?
                });
                f(Adj {
                    reltype: rt,
                    neighbour,
                    edge,
                })?;
                k += 1;
            }
        } else {
            // Skip the whole run: jump its neighbour bytes and advance the id counter.
            if nb > h.body.len() {
                bail!(
                    "adjacency run byte length {nb} exceeds remaining {}",
                    h.body.len()
                );
            }
            h.body = &h.body[nb..];
            k += rc as u64;
        }
    }
    Ok(())
}

/// Decode one node's adjacency record (see the module docs for the forward/reverse formats;
/// `forward` selects it). Public so a cached-block reader can decode a record it already holds.
/// Collects [`decode_adj_into`], reserving the leading `count` up front so a large hub record
/// materialises without regrowth.
pub fn decode_adj(rec: &[u8], forward: bool) -> Result<Vec<Adj>> {
    // The leading uvarint is the edge count — peek it to size the Vec exactly.
    let count = read_uvarint(&mut { rec })? as usize;
    let mut out = Vec::with_capacity(count);
    decode_adj_into(rec, forward, |a| {
        out.push(a);
        Ok(())
    })?;
    Ok(out)
}

/// Build `topology.csr.blk` from an edge list. Offline only — adjacency is
/// materialised in memory (fine for the builder).
pub fn write_csr(
    path: impl AsRef<Path>,
    node_count: u64,
    edges: &[Edge],
    target_block_bytes: usize,
    zstd_level: i32,
) -> Result<u64> {
    write_csr_with_cipher(
        path,
        node_count,
        edges,
        target_block_bytes,
        zstd_level,
        None,
    )
}

/// Write the CSR adjacency, optionally AEAD-encrypted (`cipher = None` ⇒
/// plaintext, identical to [`write_csr`]).
pub fn write_csr_with_cipher(
    path: impl AsRef<Path>,
    node_count: u64,
    edges: &[Edge],
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<u64> {
    let n = node_count as usize;
    let mut fwd: Vec<Vec<Adj>> = vec![Vec::new(); n];
    let mut rev: Vec<Vec<Adj>> = vec![Vec::new(); n];
    for e in edges {
        if e.src.index() >= n || e.dst.index() >= n {
            bail!("edge endpoint out of range (node_count {node_count})");
        }
        fwd[e.src.index()].push(Adj {
            reltype: e.reltype,
            neighbour: e.dst,
            edge: e.edge,
        });
        rev[e.dst.index()].push(Adj {
            reltype: e.reltype,
            neighbour: e.src,
            edge: e.edge,
        });
    }

    let mut w = BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
    for list in &fwd {
        w.append_record(&encode_adj_forward(list))?;
    }
    for list in &rev {
        w.append_record(&encode_adj_reverse(list))?;
    }
    w.finish()
}

/// Streaming CSR writer fed edges already grouped by key-node in ascending order.
/// Replaces the `fwd`/`rev: Vec<Vec<Adj>>` materialisation of [`write_csr`] for the
/// external builder — only the current node's adjacency (`cur`) is held resident, so
/// peak memory is `O(max degree)` rather than `O(edges)`.
///
/// Drive it in two halves into one file: push every edge keyed by **source** node
/// (forward adjacency) and call [`CsrStreamWriter::finish_half`]; then push every edge
/// keyed by **destination** node (reverse adjacency) and call [`CsrStreamWriter::finish`].
/// Each half emits exactly one record per node `0..node_count` — an empty (count-0)
/// record for a node with no edges — so the file holds the `2N` records that
/// [`TopologyReader::open`] requires.
pub struct CsrStreamWriter {
    inner: BlockFileWriter,
    node_count: u64,
    next_node: u64,
    cur: Vec<Adj>,
    halves_done: u8,
}

impl CsrStreamWriter {
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        node_count: u64,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        Ok(Self {
            inner: BlockFileWriter::create_with_cipher(
                path,
                target_block_bytes,
                zstd_level,
                cipher,
            )?,
            node_count,
            next_node: 0,
            cur: Vec::new(),
            halves_done: 0,
        })
    }

    /// Emit the accumulated record for `next_node` and advance. Half 0 is forward
    /// (`edge_id_base`), half 1 is reverse (per-edge `edge_id`).
    fn emit_one(&mut self) -> Result<()> {
        let rec = if self.halves_done == 0 {
            encode_adj_forward(&self.cur)
        } else {
            encode_adj_reverse(&self.cur)
        };
        self.inner.append_record(&rec)?;
        self.cur.clear();
        self.next_node += 1;
        Ok(())
    }

    /// Add `adj` to node `key_node`'s adjacency. `key_node` must be non-decreasing
    /// across calls within a half and strictly `< node_count`. Skipped nodes get
    /// empty records so a record's global index stays equal to its node id.
    pub fn push(&mut self, key_node: u64, adj: Adj) -> Result<()> {
        if key_node >= self.node_count {
            bail!("csr key node {key_node} >= node_count {}", self.node_count);
        }
        if key_node < self.next_node {
            bail!(
                "csr edges not ascending by node (got {key_node}, already at {})",
                self.next_node
            );
        }
        while self.next_node < key_node {
            self.emit_one()?;
        }
        self.cur.push(adj);
        Ok(())
    }

    /// Close the current half, padding empty records through `node_count - 1`, and
    /// reset for the next half.
    pub fn finish_half(&mut self) -> Result<()> {
        while self.next_node < self.node_count {
            self.emit_one()?;
        }
        self.next_node = 0;
        self.halves_done += 1;
        Ok(())
    }

    /// Flush the file; both halves (forward then reverse) must have been closed.
    pub fn finish(self) -> Result<u64> {
        if self.halves_done != 2 {
            bail!(
                "csr stream needs both halves finished (forward+reverse), got {}",
                self.halves_done
            );
        }
        self.inner.finish()
    }
}

/// Writes **one CSR half** for a contiguous node range `[lo, hi)`: exactly `hi - lo`
/// records (an empty record for any node with no pushed adjacency), so concatenating
/// the bands' half-files in node order — then the forward set before the reverse set
/// — reproduces the full 2N-record CSR that [`CsrStreamWriter`] would have streamed.
/// This is the per-band writer for the range-partitioned parallel emit; the bands are
/// stitched with [`crate::blockfile::concat_block_files`].
pub struct CsrHalfWriter {
    inner: BlockFileWriter,
    lo: u64,
    hi: u64,
    next_node: u64,
    cur: Vec<Adj>,
    /// `true` for the forward (outgoing) half — records use `edge_id_base`; `false` for the
    /// reverse (incoming) half — records keep the per-edge `edge_id`.
    forward: bool,
}

impl CsrHalfWriter {
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        lo: u64,
        hi: u64,
        forward: bool,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        Ok(Self {
            inner: BlockFileWriter::create_with_cipher(
                path,
                target_block_bytes,
                zstd_level,
                cipher,
            )?,
            lo,
            hi,
            next_node: lo,
            cur: Vec::new(),
            forward,
        })
    }

    fn emit_one(&mut self) -> Result<()> {
        let rec = if self.forward {
            encode_adj_forward(&self.cur)
        } else {
            encode_adj_reverse(&self.cur)
        };
        self.inner.append_record(&rec)?;
        self.cur.clear();
        self.next_node += 1;
        Ok(())
    }

    /// Push `adj` onto node `key_node`'s adjacency. `key_node` must be in `[lo, hi)`
    /// and non-decreasing across calls; gaps emit empty records.
    pub fn push(&mut self, key_node: u64, adj: Adj) -> Result<()> {
        if key_node < self.lo || key_node >= self.hi {
            bail!(
                "csr-half key {key_node} outside band [{}, {})",
                self.lo,
                self.hi
            );
        }
        if key_node < self.next_node {
            bail!(
                "csr-half edges not ascending by node (got {key_node}, already at {})",
                self.next_node
            );
        }
        while self.next_node < key_node {
            self.emit_one()?;
        }
        self.cur.push(adj);
        Ok(())
    }

    /// Pad empty records through `hi - 1` and flush; returns the block count.
    pub fn finish(mut self) -> Result<u64> {
        while self.next_node < self.hi {
            self.emit_one()?;
        }
        self.inner.finish()
    }
}

/// Reader over the CSR adjacency.
pub struct TopologyReader {
    inner: BlockFileReader,
    node_count: u64,
}

impl TopologyReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cipher(path, None)
    }

    /// Open the CSR, supplying the per-generation cipher for an encrypted file.
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let src = Arc::new(crate::store::fs::FileObject::open(path)?);
        Self::open_src(src, cipher)
    }

    /// Open from any positional-read source (local file or remote object).
    pub fn open_src(
        src: Arc<dyn crate::store::RandomReadAt>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let inner = BlockFileReader::open_src(src, cipher)?;
        let total = inner.total_records();
        if total % 2 != 0 {
            bail!("topology record count {total} is not even (forward+reverse)");
        }
        Ok(Self {
            inner,
            node_count: total / 2,
        })
    }

    pub fn node_count(&self) -> u64 {
        self.node_count
    }

    /// Outgoing adjacency of `node`.
    pub fn outgoing(&self, node: NodeId) -> Result<Vec<Adj>> {
        self.adj(node.0)
    }

    /// Incoming adjacency of `node`.
    pub fn incoming(&self, node: NodeId) -> Result<Vec<Adj>> {
        self.adj(self.node_count + node.0)
    }

    fn adj(&self, global: u64) -> Result<Vec<Adj>> {
        let rec = self.inner.read_record_global(global)?;
        // Records `0..N` are forward (outgoing), `N..2N` reverse (incoming).
        decode_adj(&rec, global < self.node_count)
    }

    /// Global record index of a node's outgoing adjacency (= the node id).
    pub fn outgoing_global(&self, node: NodeId) -> u64 {
        node.0
    }

    /// Global record index of a node's incoming adjacency (`node_count + id`).
    pub fn incoming_global(&self, node: NodeId) -> u64 {
        self.node_count + node.0
    }

    /// The underlying block file, so a caller holding a block cache can read this
    /// store's adjacency records through it and decode them with [`decode_adj`].
    pub fn inner(&self) -> &BlockFileReader {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_topo_{}_{}", std::process::id(), name))
    }

    #[test]
    fn forward_and_reverse_are_consistent() {
        let path = tmp("csr");
        // 4 nodes; a small directed graph with a couple of relationship types.
        let edges = vec![
            Edge {
                src: NodeId(0),
                dst: NodeId(1),
                reltype: 0,
                edge: EdgeId(0),
            },
            Edge {
                src: NodeId(0),
                dst: NodeId(2),
                reltype: 1,
                edge: EdgeId(1),
            },
            Edge {
                src: NodeId(1),
                dst: NodeId(2),
                reltype: 0,
                edge: EdgeId(2),
            },
            Edge {
                src: NodeId(3),
                dst: NodeId(0),
                reltype: 1,
                edge: EdgeId(3),
            },
        ];
        write_csr(&path, 4, &edges, 256, 3).unwrap();

        let r = TopologyReader::open(&path).unwrap();
        assert_eq!(r.node_count(), 4);

        // Outgoing of 0 → {1 via rt0/e0, 2 via rt1/e1}
        let out0 = r.outgoing(NodeId(0)).unwrap();
        assert_eq!(out0.len(), 2);
        assert!(out0.contains(&Adj {
            reltype: 0,
            neighbour: NodeId(1),
            edge: EdgeId(0)
        }));
        assert!(out0.contains(&Adj {
            reltype: 1,
            neighbour: NodeId(2),
            edge: EdgeId(1)
        }));

        // Node 2 has no outgoing, two incoming (from 0 and 1).
        assert!(r.outgoing(NodeId(2)).unwrap().is_empty());
        let in2 = r.incoming(NodeId(2)).unwrap();
        assert_eq!(in2.len(), 2);
        assert!(in2.contains(&Adj {
            reltype: 1,
            neighbour: NodeId(0),
            edge: EdgeId(1)
        }));
        assert!(in2.contains(&Adj {
            reltype: 0,
            neighbour: NodeId(1),
            edge: EdgeId(2)
        }));

        // Reverse/forward equivalence: every forward edge appears in some reverse list.
        for src in 0..4u64 {
            for a in r.outgoing(NodeId(src)).unwrap() {
                let back = r.incoming(a.neighbour).unwrap();
                assert!(
                    back.iter()
                        .any(|b| b.neighbour == NodeId(src) && b.edge == a.edge),
                    "forward edge {src}->{} (e{}) missing from reverse",
                    a.neighbour.0,
                    a.edge.0
                );
            }
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn forward_contiguous_and_explicit_both_roundtrip() {
        // Contiguous forward ids (real-builder shape): node 0 owns e0,e1,e2 in order.
        let contiguous = vec![
            Adj {
                reltype: 0,
                neighbour: NodeId(1),
                edge: EdgeId(0),
            },
            Adj {
                reltype: 1,
                neighbour: NodeId(2),
                edge: EdgeId(1),
            },
            Adj {
                reltype: 0,
                neighbour: NodeId(3),
                edge: EdgeId(2),
            },
        ];
        let enc = encode_adj_forward(&contiguous);
        assert_eq!(enc[1], FWD_CONTIGUOUS, "contiguous ids use base+k encoding");
        let got = decode_adj(&enc, true).unwrap();
        assert_eq!(got, contiguous);

        // Non-contiguous forward ids (fixture / out-of-order writer): falls back to per-edge.
        let explicit = vec![
            Adj {
                reltype: 0,
                neighbour: NodeId(1),
                edge: EdgeId(0),
            },
            Adj {
                reltype: 1,
                neighbour: NodeId(3),
                edge: EdgeId(2),
            },
            Adj {
                reltype: 0,
                neighbour: NodeId(2),
                edge: EdgeId(4),
            },
        ];
        let enc2 = encode_adj_forward(&explicit);
        assert_eq!(enc2[1], FWD_EXPLICIT, "non-contiguous ids keep per-edge id");
        let got2 = decode_adj(&enc2, true).unwrap();
        assert_eq!(got2, explicit, "explicit fallback preserves exact ids");
        // The contiguous form is smaller (its whole point) for the same-size list.
        assert!(enc.len() < enc2.len());

        // Empty forward record: just the count, no flag.
        let empty = encode_adj_forward(&[]);
        assert!(decode_adj(&empty, true).unwrap().is_empty());
        assert_eq!(adj_count(&empty).unwrap(), 0);
    }

    #[test]
    fn filtered_decode_matches_full_then_filter() {
        // A hub with several reltypes; reltype-major so each reltype is one run. Filtered decode
        // must equal decode-all-then-filter, and derive exact edge ids across skipped runs.
        let mut list = Vec::new();
        let mut e = 0u64;
        // reltype 0: neighbours 1,4,9 ; reltype 2: 2,3 ; reltype 5: 7,8,10,11
        for (rt, nbrs) in [
            (0u32, vec![1u64, 4, 9]),
            (2, vec![2, 3]),
            (5, vec![7, 8, 10, 11]),
        ] {
            for nb in nbrs {
                list.push(Adj {
                    reltype: rt,
                    neighbour: NodeId(nb),
                    edge: EdgeId(e),
                });
                e += 1;
            }
        }
        let enc = encode_adj_forward(&list);
        assert_eq!(enc[1], FWD_CONTIGUOUS);
        // Full decode round-trips the whole list in order.
        assert_eq!(decode_adj(&enc, true).unwrap(), list);

        // Filter to reltypes {2, 5}: must equal full-decode filtered, with edge ids intact.
        for want_set in [
            vec![2u32],
            vec![5],
            vec![0, 5],
            vec![2, 5],
            vec![9 /*absent*/],
        ] {
            let mut got = Vec::new();
            decode_adj_into_filtered(
                &enc,
                true,
                |rt| want_set.contains(&rt),
                |a| {
                    got.push(a);
                    Ok(())
                },
            )
            .unwrap();
            let want: Vec<Adj> = list
                .iter()
                .copied()
                .filter(|a| want_set.contains(&a.reltype))
                .collect();
            assert_eq!(got, want, "filter {want_set:?}");
        }

        // Reverse records (per-edge ids) filter identically.
        let enc_rev = encode_adj_reverse(&list);
        let mut got = Vec::new();
        decode_adj_into_filtered(
            &enc_rev,
            false,
            |rt| rt == 2,
            |a| {
                got.push(a);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            got,
            list.iter()
                .copied()
                .filter(|a| a.reltype == 2)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn stream_writer_matches_write_csr() {
        // A graph with an isolated sink (node 2: no outgoing) and a node with two
        // out-edges, so the empty-record padding and multi-edge paths are exercised.
        let node_count = 5u64;
        let edges = vec![
            Edge {
                src: NodeId(0),
                dst: NodeId(1),
                reltype: 0,
                edge: EdgeId(0),
            },
            Edge {
                src: NodeId(0),
                dst: NodeId(2),
                reltype: 1,
                edge: EdgeId(1),
            },
            Edge {
                src: NodeId(1),
                dst: NodeId(2),
                reltype: 0,
                edge: EdgeId(2),
            },
            Edge {
                src: NodeId(3),
                dst: NodeId(0),
                reltype: 1,
                edge: EdgeId(3),
            },
            Edge {
                src: NodeId(4),
                dst: NodeId(4),
                reltype: 2,
                edge: EdgeId(4),
            }, // self-loop
        ];

        let ref_path = tmp("csr_ref");
        write_csr(&ref_path, node_count, &edges, 256, 3).unwrap();

        // Streaming: forward edges sorted by (src, edge); reverse by (dst, edge).
        let mut fwd = edges.clone();
        fwd.sort_by_key(|e| (e.src.0, e.edge.0));
        let mut rev = edges.clone();
        rev.sort_by_key(|e| (e.dst.0, e.edge.0));

        let s_path = tmp("csr_stream");
        let mut w = CsrStreamWriter::create_with_cipher(&s_path, node_count, 256, 3, None).unwrap();
        for e in &fwd {
            w.push(
                e.src.0,
                Adj {
                    reltype: e.reltype,
                    neighbour: e.dst,
                    edge: e.edge,
                },
            )
            .unwrap();
        }
        w.finish_half().unwrap();
        for e in &rev {
            w.push(
                e.dst.0,
                Adj {
                    reltype: e.reltype,
                    neighbour: e.src,
                    edge: e.edge,
                },
            )
            .unwrap();
        }
        w.finish_half().unwrap();
        w.finish().unwrap();

        let r1 = TopologyReader::open(&ref_path).unwrap();
        let r2 = TopologyReader::open(&s_path).unwrap();
        assert_eq!(r1.node_count(), r2.node_count());
        let key = |a: &Adj| (a.reltype, a.neighbour.0, a.edge.0);
        for n in 0..node_count {
            let (mut o1, mut o2) = (
                r1.outgoing(NodeId(n)).unwrap(),
                r2.outgoing(NodeId(n)).unwrap(),
            );
            o1.sort_by_key(key);
            o2.sort_by_key(key);
            assert_eq!(o1, o2, "outgoing mismatch at node {n}");
            let (mut i1, mut i2) = (
                r1.incoming(NodeId(n)).unwrap(),
                r2.incoming(NodeId(n)).unwrap(),
            );
            i1.sort_by_key(key);
            i2.sort_by_key(key);
            assert_eq!(i1, i2, "incoming mismatch at node {n}");
        }
        // The isolated sink still has an (empty) outgoing record.
        assert!(r2.outgoing(NodeId(2)).unwrap().is_empty());
        let _ = std::fs::remove_file(&ref_path);
        let _ = std::fs::remove_file(&s_path);
    }

    #[test]
    fn banded_half_writers_concat_matches_csr() {
        // The Option-B path: forward + reverse CSR halves written per node band by
        // `CsrHalfWriter`, then stitched with `concat_block_files`, must be logically
        // identical to a single streamed CSR.
        let node_count = 6u64;
        let mk = |s, d, rt, ed| Edge {
            src: NodeId(s),
            dst: NodeId(d),
            reltype: rt,
            edge: EdgeId(ed),
        };
        let edges = vec![
            mk(0, 1, 0, 0),
            mk(0, 5, 1, 1),
            mk(1, 2, 0, 2),
            mk(3, 0, 1, 3),
            mk(4, 4, 2, 4),
            mk(5, 1, 0, 5),
        ];
        let ref_path = tmp("csr_band_ref");
        write_csr(&ref_path, node_count, &edges, 256, 3).unwrap();

        let bands = [(0u64, 2u64), (2, 4), (4, 6)];
        let mut fwd = edges.clone();
        fwd.sort_by_key(|e| (e.src.0, e.edge.0));
        let mut rev = edges.clone();
        rev.sort_by_key(|e| (e.dst.0, e.edge.0));
        let mut parts = Vec::new();
        for (bi, (lo, hi)) in bands.iter().enumerate() {
            let p = tmp(&format!("csr_fwd{bi}"));
            let mut w =
                CsrHalfWriter::create_with_cipher(&p, *lo, *hi, true, 256, 3, None).unwrap();
            for e in fwd.iter().filter(|e| e.src.0 >= *lo && e.src.0 < *hi) {
                w.push(
                    e.src.0,
                    Adj {
                        reltype: e.reltype,
                        neighbour: e.dst,
                        edge: e.edge,
                    },
                )
                .unwrap();
            }
            w.finish().unwrap();
            parts.push(p);
        }
        for (bi, (lo, hi)) in bands.iter().enumerate() {
            let p = tmp(&format!("csr_rev{bi}"));
            let mut w =
                CsrHalfWriter::create_with_cipher(&p, *lo, *hi, false, 256, 3, None).unwrap();
            for e in rev.iter().filter(|e| e.dst.0 >= *lo && e.dst.0 < *hi) {
                w.push(
                    e.dst.0,
                    Adj {
                        reltype: e.reltype,
                        neighbour: e.src,
                        edge: e.edge,
                    },
                )
                .unwrap();
            }
            w.finish().unwrap();
            parts.push(p);
        }
        let out = tmp("csr_band_concat");
        crate::blockfile::concat_block_files(&out, &parts).unwrap();

        let r1 = TopologyReader::open(&ref_path).unwrap();
        let r2 = TopologyReader::open(&out).unwrap();
        assert_eq!(r2.node_count(), node_count);
        let key = |a: &Adj| (a.reltype, a.neighbour.0, a.edge.0);
        for n in 0..node_count {
            let (mut o1, mut o2) = (
                r1.outgoing(NodeId(n)).unwrap(),
                r2.outgoing(NodeId(n)).unwrap(),
            );
            o1.sort_by_key(key);
            o2.sort_by_key(key);
            assert_eq!(o1, o2, "outgoing {n}");
            let (mut i1, mut i2) = (
                r1.incoming(NodeId(n)).unwrap(),
                r2.incoming(NodeId(n)).unwrap(),
            );
            i1.sort_by_key(key);
            i2.sort_by_key(key);
            assert_eq!(i1, i2, "incoming {n}");
        }
        for p in &parts {
            let _ = std::fs::remove_file(p);
        }
        let _ = std::fs::remove_file(&ref_path);
        let _ = std::fs::remove_file(&out);
    }
}
