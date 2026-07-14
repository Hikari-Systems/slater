// SPDX-License-Identifier: Apache-2.0
//! Disk-native single-layer Vamana graph (DiskANN-style) for the large-vector
//! ANN path.
//!
//! For a vector index at or above the ANN threshold, brute force is too slow, so
//! the builder lays the index out as a navigable proximity graph: every vector is
//! a node with a bounded out-degree (`R`) of carefully-chosen neighbours, and a
//! query is answered by a greedy beam walk from a fixed entry **medoid** towards
//! the query. Construction uses the Vamana **robust prune** (the `alpha` long-edge
//! rule) so the graph stays navigable with small degree.
//!
//! Three things live here, in build → store → search order:
//! 1. [`build_vamana`] — construct the adjacency + pick the medoid (pure, in-memory,
//!    offline-only). Operates on **normalised** vectors with squared-L2 (monotonic
//!    in cosine on unit vectors — D29), so it is metric-agnostic given the caller
//!    normalised for cosine.
//! 2. [`bfs_order`] + [`VamanaWriter`]/[`VamanaReader`] — the on-disk block file
//!    `[node_id ‖ full vec ‖ adjacency]`, laid out by BFS-from-medoid for locality
//!    so a walk touches few distinct blocks. Goes through the same `blockfile` seam
//!    as every other store (zstd + the M6 AEAD for free — D28).
//! 3. [`beam_search`] — the generic greedy beam walk. It is parameterised over a
//!    resident PQ *estimate* (navigation, no IO) and a block-reading *fetch* (full
//!    vector + neighbours), with an *exact* re-rank closure, so the **same** search
//!    drives the pure in-memory recall test here and the cache-backed reader in
//!    `slater` with no duplicated logic.
//!
// DESIGN (D30): adjacency is stored as **global vamana indices** (a node's
// position in the file = its index), NOT as on-disk `(block_id, slot)` pairs. The
// blockfile already keeps a resident, tiny prefix-sum directory that maps any
// global record index to its `(block, slot)` via `locate()`, so the reader derives
// the block-relative address for free and coalesces a frontier's reads by block.
// Storing the pair on disk instead would be circular to size (variable-width
// records whose neighbour fields encode the very block boundaries that depend on
// those fields' widths); storing the index sidesteps that entirely and costs no
// extra resident memory.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::pq::{sq_l2, Lcg};
use crate::wire::{capacity_for, read_uvarint, write_uvarint};

/// A `(0..count)` index into a Vamana graph — a node's position in the file.
pub type VamanaIndex = u32;

// ── Build (offline, in-memory) ─────────────────────────────────────────────────

/// The result of constructing a Vamana graph: each node's out-neighbours (by
/// index) and the entry medoid.
pub struct VamanaGraph {
    pub adjacency: Vec<Vec<VamanaIndex>>,
    pub medoid: VamanaIndex,
}

// ── The construction seams ─────────────────────────────────────────────────────
//
// The two construction primitives (`greedy_search_over`, `robust_prune_over`) need
// exactly two things from the world: the **distance between two stored points**, and
// a **node's out-neighbours**. Neither ever looks at a raw vector, and neither cares
// where the data lives. Cutting the seam there is what lets one definition serve all
// three arms:
//
//   * the offline slab build (here)          — points in RAM, adjacency in RAM
//   * the in-memory RW index (FreshDiskANN)  — points in RAM, adjacency in RAM
//   * the on-disk StreamingMerge             — points and adjacency read through a
//                                              block cache, with a dirty overlay
//
// The disk arm cannot materialise its adjacency (91.6M × R=32 × 4 B ≈ 11.7 GB), which
// is why the adjacency is a trait and not a `&[Vec<u32>]`.

/// The distance between two *stored* points, in the space the graph is built in
/// (squared-L2 over already-normalised vectors, for a cosine index — D29).
///
/// `dist` returns `Result` because a disk-backed implementation does IO.
pub trait PointSet {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Distance from point `a` to point `b`.
    fn dist(&self, a: VamanaIndex, b: VamanaIndex) -> Result<f64>;
}

/// The offline build's point set: a resident slab of vectors.
pub struct SlabPoints<'a>(pub &'a [Vec<f32>]);

impl PointSet for SlabPoints<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn dist(&self, a: VamanaIndex, b: VamanaIndex) -> Result<f64> {
        Ok(sq_l2(&self.0[a as usize], &self.0[b as usize]))
    }
}

/// Read a node's out-neighbours.
///
/// Fills a **caller-owned buffer** rather than returning a `Vec` or a `Cow`: the build
/// expands O(n·L) nodes, so allocating per expansion would be a real regression at
/// 91.6M — and this is a refactor that must not change performance any more than it
/// changes output. The slab impl allocates nothing in steady state (the buffer is
/// reused); a disk impl decodes straight into it. It also sidesteps the `Cow` lifetime
/// that would otherwise fight the `&mut` borrow in [`insert_point`]'s back-link loop.
pub trait AdjRead {
    fn neighbours_into(&self, i: VamanaIndex, out: &mut Vec<VamanaIndex>) -> Result<()>;
}

/// Replace a node's out-neighbours. Split from [`AdjRead`] so a search can take a
/// shared borrow of the graph while an insert takes an exclusive one.
pub trait AdjWrite: AdjRead {
    fn set_neighbours(&mut self, i: VamanaIndex, nbrs: Vec<VamanaIndex>) -> Result<()>;
}

impl AdjRead for Vec<Vec<VamanaIndex>> {
    fn neighbours_into(&self, i: VamanaIndex, out: &mut Vec<VamanaIndex>) -> Result<()> {
        out.clear();
        out.extend_from_slice(&self[i as usize]);
        Ok(())
    }
}

impl AdjWrite for Vec<Vec<VamanaIndex>> {
    fn set_neighbours(&mut self, i: VamanaIndex, nbrs: Vec<VamanaIndex>) -> Result<()> {
        self[i as usize] = nbrs;
        Ok(())
    }
}

/// The "already expanded" set for one greedy search.
///
/// Two shapes, because the two arms have opposite cost profiles:
/// - [`Expanded::Stamps`] — a generation-stamped array. O(n) memory, but **O(1) to
///   clear** (bump the generation), so the offline build allocates it *once* for the
///   whole build instead of reallocating an n-sized array per node — which was O(n²)
///   allocation before it was fixed.
/// - [`Expanded::Set`] — grows with the work done, not the index size. The disk arm
///   must not allocate a 366 MB stamp array per search over a 91.6M-record index.
///   (This is the same trade-off [`beam_search`] already makes for itself.)
pub enum Expanded<'a> {
    Stamps { buf: &'a mut [u32], gen: u32 },
    Set(HashSet<VamanaIndex>),
}

impl Expanded<'_> {
    pub fn seen(&self, i: VamanaIndex) -> bool {
        match self {
            Expanded::Stamps { buf, gen } => buf[i as usize] == *gen,
            Expanded::Set(s) => s.contains(&i),
        }
    }

    pub fn mark(&mut self, i: VamanaIndex) {
        match self {
            Expanded::Stamps { buf, gen } => buf[i as usize] = *gen,
            Expanded::Set(s) => {
                s.insert(i);
            }
        }
    }

    /// Begin a fresh search. O(1) for the stamped arm, O(visited) for the set.
    ///
    /// Called by [`greedy_search_over`] itself, so a caller cannot forget it. That is
    /// deliberate: the stamped arm starts at `gen = 0` over a **zeroed** buffer, so
    /// searching without a `begin` would read *every* node as already-expanded, expand
    /// nothing, and hand robust-prune an empty candidate pool — a silently degraded
    /// graph with no error and no panic. The scratch is purely an allocation-reuse
    /// vehicle; making it own its own reset removes the landmine.
    fn begin(&mut self) {
        match self {
            Expanded::Stamps { buf, gen } => {
                *gen = gen.wrapping_add(1);
                // On wrap, `gen` collides with the zeroed buffer and every node reads
                // as seen. Not hypothetical: the RW index runs one search per vector
                // insert for the lifetime of the process, so a long-lived writer
                // reaches 2^32 searches in weeks. Re-zero and skip 0.
                if *gen == 0 {
                    buf.fill(0);
                    *gen = 1;
                }
            }
            Expanded::Set(s) => s.clear(),
        }
    }
}

/// Greedy search over the *current* graph from `start` towards point `p`. Returns every
/// node it expanded — the candidate pool for pruning.
///
/// Generic (not `dyn`) so the offline build keeps static dispatch in its hot loop.
pub fn greedy_search_over<A, P>(
    start: VamanaIndex,
    p: VamanaIndex,
    adj: &A,
    points: &P,
    l_size: usize,
    expanded: &mut Expanded,
) -> Result<Vec<VamanaIndex>>
where
    A: AdjRead + ?Sized,
    P: PointSet + ?Sized,
{
    // Own the reset rather than trusting the caller to have done it — see `begin`.
    expanded.begin();
    let mut beam: Vec<(f64, VamanaIndex)> = vec![(points.dist(start, p)?, start)];
    let mut visited = Vec::new();
    let mut nbrs: Vec<VamanaIndex> = Vec::new();
    while let Some((_, cur)) = beam
        .iter()
        .copied()
        .filter(|(_, i)| !expanded.seen(*i))
        .min_by(|a, b| a.0.total_cmp(&b.0))
    {
        expanded.mark(cur);
        visited.push(cur);
        adj.neighbours_into(cur, &mut nbrs)?;
        for &nb in &nbrs {
            if !beam.iter().any(|(_, i)| *i == nb) {
                beam.push((points.dist(nb, p)?, nb));
            }
        }
        // A *stable* sort: swapping this for `sort_unstable_by` would reorder ties and
        // silently change which nodes survive the truncate — i.e. change the output.
        beam.sort_by(|a, b| a.0.total_cmp(&b.0));
        beam.truncate(l_size);
    }
    Ok(visited)
}

/// Vamana robust prune: from `candidates`, pick up to `r` out-neighbours for `p`,
/// greedily taking the closest and discarding any candidate `c` that the chosen `p*`
/// "dominates" (`alpha · d(p*, c) ≤ d(p, c)`).
pub fn robust_prune_over<P>(
    p: VamanaIndex,
    candidates: &[VamanaIndex],
    alpha: f32,
    r: usize,
    points: &P,
) -> Result<Vec<VamanaIndex>>
where
    P: PointSet + ?Sized,
{
    let alpha = alpha as f64;
    let mut pool: Vec<(f64, VamanaIndex)> = Vec::with_capacity(candidates.len());
    for &c in candidates {
        if c == p {
            continue;
        }
        pool.push((points.dist(c, p)?, c));
    }
    pool.sort_by(|a, b| a.0.total_cmp(&b.0));

    let mut out: Vec<VamanaIndex> = Vec::with_capacity(r);
    while let Some((_, pstar)) = pool.first().copied() {
        out.push(pstar);
        if out.len() >= r {
            break;
        }
        // Drop pstar and everything it dominates. Rebuilt rather than `retain`ed
        // because the domination test now does IO and `?` cannot cross a closure — the
        // rebuild preserves relative order exactly as `retain` did, which matters: the
        // pool stays distance-sorted and `pool.first()` is the next `p*`.
        let mut next = Vec::with_capacity(pool.len());
        for &(d_pc, c) in &pool {
            if c == pstar {
                continue;
            }
            let d_star_c = points.dist(pstar, c)?;
            if alpha * d_star_c > d_pc {
                next.push((d_pc, c));
            }
        }
        pool = next;
    }
    Ok(out)
}

/// The scalar shape of one insertion: where to enter, how hard to search, how far to
/// prune. Bundled (like [`BeamParams`]) so [`insert_point`]'s signature stays legible.
#[derive(Debug, Clone, Copy)]
pub struct InsertParams {
    /// Entry point for the greedy search.
    pub medoid: VamanaIndex,
    /// Robust-prune long-edge factor.
    pub alpha: f32,
    /// Out-degree bound.
    pub r: usize,
    /// Search-list size during construction — wider than `r` for better candidates.
    pub l_build: usize,
}

/// One Vamana insertion: greedy-search from the medoid, robust-prune the visited set
/// into `p`'s out-neighbours, then make those edges symmetric — re-pruning any
/// neighbour whose degree overflows `r`.
///
/// The single definition shared by the offline build, the in-memory RW index and the
/// on-disk StreamingMerge.
pub fn insert_point<G, P>(
    p: VamanaIndex,
    graph: &mut G,
    points: &P,
    params: InsertParams,
    expanded: &mut Expanded,
) -> Result<()>
where
    G: AdjWrite + ?Sized,
    P: PointSet + ?Sized,
{
    let InsertParams {
        medoid,
        alpha,
        r,
        l_build,
    } = params;
    let visited = greedy_search_over(medoid, p, &*graph, points, l_build, expanded)?;

    // Candidate pool = everything the search touched, plus p's current neighbours,
    // minus p itself.
    let mut cands: Vec<VamanaIndex> = visited;
    let mut cur: Vec<VamanaIndex> = Vec::new();
    graph.neighbours_into(p, &mut cur)?;
    cands.extend_from_slice(&cur);
    cands.sort_unstable();
    cands.dedup();
    cands.retain(|&c| c != p);

    let pruned = robust_prune_over(p, &cands, alpha, r, points)?;
    graph.set_neighbours(p, pruned.clone())?;

    // Make the new edges symmetric, re-pruning any neighbour that overflows.
    let mut nbrs_j: Vec<VamanaIndex> = Vec::new();
    for &j in &pruned {
        graph.neighbours_into(j, &mut nbrs_j)?;
        let mut changed = false;
        if !nbrs_j.contains(&p) {
            nbrs_j.push(p);
            changed = true;
        }
        if nbrs_j.len() > r {
            nbrs_j = robust_prune_over(j, &nbrs_j, alpha, r, points)?;
            changed = true;
        }
        // Writing only when changed is equivalent to the original's unconditional
        // in-place write, and saves the disk arm a pointless dirty-page mark.
        if changed {
            graph.set_neighbours(j, std::mem::take(&mut nbrs_j))?;
        }
    }
    Ok(())
}

/// Build a single-layer Vamana graph over `vectors` (each `dim`-long, expected
/// already L2-normalised for a cosine index — D29). `r` bounds out-degree; `alpha`
/// is the robust-prune long-edge factor (1.2 is typical). Deterministic.
///
/// Output is an input to the generation content hash — see
/// `build_vamana_adjacency_is_golden`. The LCG seed, the Fisher–Yates order, the
/// candidate-pool sort/dedup and the beam's stable sort are all load-bearing.
pub fn build_vamana(vectors: &[Vec<f32>], r: usize, alpha: f32) -> Result<VamanaGraph> {
    let n = vectors.len();
    if n == 0 {
        bail!("cannot build a Vamana graph over zero vectors");
    }
    let r = r.max(1);
    let medoid = compute_medoid(vectors);

    // Trivially small index: a complete graph (capped at R) is already navigable.
    if n <= r + 1 {
        let adjacency = (0..n)
            .map(|i| (0..n).filter(|&j| j != i).map(|j| j as u32).collect())
            .collect();
        return Ok(VamanaGraph {
            adjacency,
            medoid: medoid as u32,
        });
    }

    let mut rng = Lcg(0x5111_a7e1_3a3a_0001);
    // Random R-regular initial graph.
    let mut adjacency: Vec<Vec<u32>> = (0..n)
        .map(|i| {
            let mut nbrs = Vec::with_capacity(r);
            while nbrs.len() < r {
                let j = rng.next_below(n);
                if j != i && !nbrs.contains(&(j as u32)) {
                    nbrs.push(j as u32);
                }
            }
            nbrs
        })
        .collect();

    // Search-list size during construction — wider than R for better candidates.
    let l_build = (r * 2).max(64);
    let points = SlabPoints(vectors);

    // One stamp array for the whole build; `begin()` clears it in O(1).
    let mut stamps = vec![0u32; n];
    let mut expanded = Expanded::Stamps {
        buf: &mut stamps,
        gen: 0,
    };

    // Two passes: alpha = 1 (short edges first), then the real alpha (long edges).
    for &pass_alpha in &[1.0f32, alpha.max(1.0)] {
        let order = random_permutation(n, &mut rng);
        let params = InsertParams {
            medoid: medoid as VamanaIndex,
            alpha: pass_alpha,
            r,
            l_build,
        };
        for &p in &order {
            insert_point(
                p as VamanaIndex,
                &mut adjacency,
                &points,
                params,
                &mut expanded,
            )?;
        }
    }

    Ok(VamanaGraph {
        adjacency,
        medoid: medoid as u32,
    })
}

/// The medoid: the point closest to the centroid of all vectors (a cheap, stable
/// stand-in for the true graph medoid that is good enough as a fixed entry point).
fn compute_medoid(vectors: &[Vec<f32>]) -> usize {
    let dim = vectors[0].len();
    let mut mean = vec![0.0f64; dim];
    for v in vectors {
        for (acc, &x) in mean.iter_mut().zip(v) {
            *acc += x as f64;
        }
    }
    for x in mean.iter_mut() {
        *x /= vectors.len() as f64;
    }
    let mean_f32: Vec<f32> = mean.iter().map(|&x| x as f32).collect();
    (0..vectors.len())
        .min_by(|&a, &b| sq_l2(&vectors[a], &mean_f32).total_cmp(&sq_l2(&vectors[b], &mean_f32)))
        .unwrap()
}

fn random_permutation(n: usize, rng: &mut Lcg) -> Vec<usize> {
    let mut order: Vec<usize> = (0..n).collect();
    // Fisher–Yates with the deterministic LCG.
    for i in (1..n).rev() {
        let j = rng.next_below(i + 1);
        order.swap(i, j);
    }
    order
}

/// A BFS order from the medoid — the locality layout. Nodes unreachable from the
/// medoid (rare, but possible in a sparse graph) are appended in index order so the
/// permutation always covers all `n` nodes.
pub fn bfs_order(graph: &VamanaGraph) -> Vec<VamanaIndex> {
    let n = graph.adjacency.len();
    let mut seen = vec![false; n];
    let mut order = Vec::with_capacity(n);
    let mut q = VecDeque::new();
    q.push_back(graph.medoid);
    seen[graph.medoid as usize] = true;
    while let Some(cur) = q.pop_front() {
        order.push(cur);
        for &nb in &graph.adjacency[cur as usize] {
            if !seen[nb as usize] {
                seen[nb as usize] = true;
                q.push_back(nb);
            }
        }
    }
    for (i, &was_seen) in seen.iter().enumerate() {
        if !was_seen {
            order.push(i as u32);
        }
    }
    order
}

// ── `.vamana` block-file store ─────────────────────────────────────────────────

/// One decoded Vamana node: its dense graph node id, full vector, and out-neighbour
/// indices (in vamana-index space).
#[derive(Debug, Clone, PartialEq)]
pub struct VamanaNode {
    pub node_id: u64,
    pub vector: Vec<f32>,
    pub neighbours: Vec<VamanaIndex>,
}

/// Writer for `vector/<l>.<p>.vamana`. Append nodes in **layout order** (the BFS
/// permutation) — a node's append position is its vamana index, and neighbour
/// fields must already be expressed in that same permuted index space.
pub struct VamanaWriter {
    inner: BlockFileWriter,
    next: u64,
}

impl VamanaWriter {
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
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
            next: 0,
        })
    }

    /// Append one node; returns its vamana index (= append position).
    pub fn append(
        &mut self,
        node_id: u64,
        vector: &[f32],
        neighbours: &[VamanaIndex],
    ) -> Result<VamanaIndex> {
        let mut rec = Vec::with_capacity(16 + vector.len() * 4 + neighbours.len() * 4);
        write_uvarint(&mut rec, node_id);
        write_uvarint(&mut rec, vector.len() as u64);
        for x in vector {
            rec.write_f32::<LittleEndian>(*x)?;
        }
        write_uvarint(&mut rec, neighbours.len() as u64);
        for nb in neighbours {
            write_uvarint(&mut rec, *nb as u64);
        }
        self.inner.append_record(&rec)?;
        let idx = self.next as u32;
        self.next += 1;
        Ok(idx)
    }

    pub fn finish(self) -> Result<u64> {
        self.inner.finish()
    }
}

/// Reader for `vector/<l>.<p>.vamana`.
pub struct VamanaReader {
    inner: BlockFileReader,
}

impl VamanaReader {
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
        Ok(Self {
            inner: BlockFileReader::open_src(src, cipher)?,
        })
    }

    /// Number of nodes (= records).
    pub fn len(&self) -> u64 {
        self.inner.total_records()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Decode the node at vamana index `i` (uncached `pread`). The cache-backed
    /// reader in `slater` reads through its vector-index pool instead, decoding the
    /// record with [`decode_node`].
    pub fn node(&self, i: VamanaIndex) -> Result<VamanaNode> {
        let rec = self.inner.read_record_global(i as u64)?;
        decode_node(&rec)
    }

    /// The underlying block file, so a cache holder can read records through it and
    /// map indices to `(block, slot)` via `BlockFileReader::locate` for coalesced
    /// frontier reads (D30).
    pub fn inner(&self) -> &BlockFileReader {
        &self.inner
    }
}

/// Decode a Vamana record (`uvarint(node_id) ‖ uvarint(dim) ‖ dim×f32 ‖
/// uvarint(degree) ‖ degree×uvarint(index)`). Public so a cached-block reader can
/// decode a record sliced out of a block it already holds decompressed.
pub fn decode_node(rec: &[u8]) -> Result<VamanaNode> {
    let mut r = rec;
    let node_id = read_uvarint(&mut r)?;
    let dim = read_uvarint(&mut r)? as usize;
    // `dim` and `degree` are untrusted on-disk uvarints. A vector element is 4 bytes and a
    // neighbour ≥1, so reserve only what the record's remaining bytes could hold: a forged
    // count then errors on the first short read instead of aborting on the allocation.
    let mut vector = Vec::with_capacity(capacity_for(dim, r.len(), 4));
    for _ in 0..dim {
        vector.push(r.read_f32::<LittleEndian>()?);
    }
    let degree = read_uvarint(&mut r)? as usize;
    let mut neighbours = Vec::with_capacity(capacity_for(degree, r.len(), 1));
    for _ in 0..degree {
        neighbours.push(read_uvarint(&mut r)? as u32);
    }
    Ok(VamanaNode {
        node_id,
        vector,
        neighbours,
    })
}

// ── Generic beam search ─────────────────────────────────────────────────────────

/// One search result: the vamana index reached, the emit key `emit` returned for it
/// (the caller's dense node id), and its **exact** distance.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchHit {
    pub index: VamanaIndex,
    pub node_id: u64,
    pub exact: f32,
}

/// The scalar shape of a beam search: where to enter, how wide to search, how many
/// hits to keep, and how big the graph is. Bundled so [`beam_search`]'s signature is
/// the four closures plus one parameter block rather than eight positional arguments.
#[derive(Debug, Clone, Copy)]
pub struct BeamParams {
    /// Entry point: the medoid's layout index (D30).
    pub medoid: VamanaIndex,
    /// Search-list size `L` (≥ `k` for good recall); raised to `k` if smaller.
    pub beam_width: usize,
    /// How many hits to return.
    pub k: usize,
    /// Total nodes in the graph (bounds-checks neighbour ids).
    pub num_nodes: usize,
}

/// Greedy beam search from the medoid for the `k` nearest neighbours.
///
/// - `estimate(i)` — the **PQ-estimated** distance of index `i` to the query;
///   resident, cheap, used for *navigation* (which node to expand next). No IO.
/// - `fetch(i)` — reads node `i`'s block and returns `(full_vector, neighbours)`;
///   the one IO per expansion. The full vector feeds the exact re-rank.
/// - `exact(&v)` — the exact distance of a full vector to the query (the score the
///   caller ultimately reports), used to re-rank the expanded set.
/// - `emit(i)` — maps index `i` to the dense node id to report, or `None` to
///   **suppress** it from the results. A suppressed node is still fetched and its
///   neighbours are still pushed onto the beam, so a node deleted in the delta
///   remains a navigational **waypoint** — dropping it from the graph instead would
///   disconnect the region behind it and silently cost recall on the live nodes.
///   Navigation is therefore identical with or without suppression; only the emitted
///   set shrinks. (If more than `k`-worth of the neighbourhood is suppressed the
///   search can return fewer than `k` hits — widen `beam_width` to compensate.)
///
/// Returns up to `k` hits, ascending by exact distance, ties broken by ascending
/// node id (the D26 contract — the same total order the brute-force arm produces).
pub fn beam_search<F, FE, FL>(
    params: BeamParams,
    estimate: impl Fn(VamanaIndex) -> f32,
    mut fetch: F,
    exact: FE,
    emit: FL,
) -> Result<Vec<SearchHit>>
where
    F: FnMut(VamanaIndex) -> Result<(Vec<f32>, Vec<VamanaIndex>)>,
    FE: Fn(&[f32]) -> f32,
    FL: Fn(VamanaIndex) -> Result<Option<u64>>,
{
    let BeamParams {
        medoid,
        beam_width,
        k,
        num_nodes,
    } = params;
    let beam_width = beam_width.max(k).max(1);
    // A search touches only O(beam_width · expansions) nodes, so track the expanded
    // set in a `HashSet` that grows with the work done — not a `vec![false; num_nodes]`
    // bool array sized to the *whole index*, which made each query allocate (and zero)
    // memory proportional to the index rather than the search.
    let mut expanded: HashSet<VamanaIndex> = HashSet::new();
    // Working candidate list, kept ascending by PQ estimate and capped to L.
    let mut beam: Vec<(f32, VamanaIndex)> = vec![(estimate(medoid), medoid)];
    let mut hits: Vec<SearchHit> = Vec::new();

    while let Some((_, cur)) = beam
        .iter()
        .copied()
        .filter(|(_, i)| !expanded.contains(i))
        .min_by(|a, b| a.0.total_cmp(&b.0))
    {
        expanded.insert(cur);
        let (vector, neighbours) = fetch(cur)?;
        // Emit only if the caller still considers this node live; either way its
        // neighbours go on the beam below, so a dead node stays a waypoint.
        if let Some(node_id) = emit(cur)? {
            hits.push(SearchHit {
                index: cur,
                node_id,
                exact: exact(&vector),
            });
        }
        for nb in neighbours {
            if (nb as usize) < num_nodes
                && !expanded.contains(&nb)
                && !beam.iter().any(|(_, i)| *i == nb)
            {
                beam.push((estimate(nb), nb));
            }
        }
        beam.sort_by(|a, b| a.0.total_cmp(&b.0));
        beam.truncate(beam_width);
    }

    // Tie-break on the **node id**, not the layout index: the layout is BFS-from-medoid
    // order, which is arbitrary w.r.t. node id, so tie-breaking on it would give a
    // different (though still deterministic) order from the brute-force arm and break
    // the D26 contract at the k boundary.
    hits.sort_by(|a, b| a.exact.total_cmp(&b.exact).then(a.node_id.cmp(&b.node_id)));
    hits.truncate(k);
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pq::{normalise, train_codebooks, AdcTable, PqParams};

    /// `n` unit vectors in `dim` dimensions, deterministic.
    fn unit_vectors(dim: usize, n: usize) -> Vec<Vec<f32>> {
        let mut rng = Lcg(0x1234_5678_9abc_def0);
        (0..n)
            .map(|_| {
                let v: Vec<f32> = (0..dim).map(|_| (rng.next_f64() as f32) - 0.5).collect();
                normalise(&v)
            })
            .collect()
    }

    fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        let mut dot = 0.0f64;
        let (mut na, mut nb) = (0.0f64, 0.0f64);
        for (x, y) in a.iter().zip(b) {
            dot += *x as f64 * *y as f64;
            na += *x as f64 * *x as f64;
            nb += *y as f64 * *y as f64;
        }
        if na == 0.0 || nb == 0.0 {
            return 1.0;
        }
        (1.0 - dot / (na.sqrt() * nb.sqrt())) as f32
    }

    fn brute_force_topk(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<usize> {
        let mut scored: Vec<(f32, usize)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (cosine_distance(query, v), i))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(_, i)| i).collect()
    }

    /// The build's adjacency, serialised in a canonical, order-sensitive form. Any
    /// change to the neighbour *order* (not just the neighbour *set*) shows up here —
    /// which is the point: the order is what the BFS layout and the on-disk bytes
    /// depend on.
    fn adjacency_digest(g: &VamanaGraph) -> String {
        let mut h = blake3::Hasher::new();
        h.update(&(g.medoid as u64).to_le_bytes());
        h.update(&(g.adjacency.len() as u64).to_le_bytes());
        for nbrs in &g.adjacency {
            h.update(&(nbrs.len() as u64).to_le_bytes());
            for &nb in nbrs {
                h.update(&nb.to_le_bytes());
            }
        }
        h.finalize().to_hex().to_string()
    }

    /// `build_vamana`'s output is an input to the generation content hash, so it must
    /// be byte-stable across refactors. This pins it to a **recorded artefact** — the
    /// digest was captured from the pre-`PointSet`-refactor implementation and must not
    /// move. (A recorded constant is independently-derived truth; comparing two live
    /// implementations against each other would not be — see CONTRIBUTING.)
    ///
    /// If this fails, something in the construction order changed: the LCG seed, the
    /// Fisher–Yates permutation, the candidate-pool sort/dedup, the beam's *stable*
    /// sort, or the robust-prune domination order. None of those are safe to change.
    #[test]
    fn build_vamana_adjacency_is_golden() {
        let vectors = unit_vectors(16, 200);
        let g = build_vamana(&vectors, 24, 1.2).unwrap();
        assert_eq!(
            adjacency_digest(&g),
            "9b637d11308e6f76392b2b6792bf577c283ba4780cd24efc258c8ac47f3fef81",
            "build_vamana adjacency changed — see the doc comment; this is not a test to \
             re-baseline casually"
        );
    }

    #[test]
    fn build_produces_bounded_degree_and_reachable_graph() {
        let vectors = unit_vectors(16, 200);
        let g = build_vamana(&vectors, 24, 1.2).unwrap();
        assert_eq!(g.adjacency.len(), 200);
        for nbrs in &g.adjacency {
            assert!(nbrs.len() <= 24, "degree {} exceeds R", nbrs.len());
        }
        // BFS from the medoid reaches every node (the layout permutation is total).
        let order = bfs_order(&g);
        assert_eq!(order.len(), 200);
        let mut sorted = order.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            200,
            "layout must be a permutation of all nodes"
        );
    }

    #[test]
    fn greedy_search_build_scratch_reuse_is_isolated_across_generations() {
        // The build reuses one stamp buffer across every per-node search, bumping the
        // generation instead of reallocating. A search run against a buffer already
        // dirtied by a prior generation must return exactly what a pristine buffer
        // would — the generation stamp isolates the reuse.
        //
        // Truth here is hand-derived, not read off a second implementation. The graph
        // is the path 0—1—2—3 with the points evenly spaced on a line, so a greedy walk
        // from one end towards the other expands the nodes strictly in path order.
        let vectors = vec![
            vec![0.0f32, 0.0],
            vec![1.0, 0.0],
            vec![2.0, 0.0],
            vec![3.0, 0.0],
        ];
        let adjacency: Vec<Vec<u32>> = vec![vec![1], vec![0, 2], vec![1, 3], vec![2]];
        let points = SlabPoints(&vectors);
        let n = vectors.len();
        let l = 8;

        let forward: Vec<VamanaIndex> = vec![0, 1, 2, 3]; // walk 0 → 3
        let backward: Vec<VamanaIndex> = vec![3, 2, 1, 0]; // walk 3 → 0

        // A reused buffer across two searches. `greedy_search_over` resets the scratch
        // itself, so a stale stamp from the first search must not leak into the second.
        let mut stamps = vec![0u32; n];
        let mut ex = Expanded::Stamps {
            buf: &mut stamps,
            gen: 0,
        };
        let a = greedy_search_over(0, 3, &adjacency, &points, l, &mut ex).unwrap();
        let b = greedy_search_over(3, 0, &adjacency, &points, l, &mut ex).unwrap();

        assert_eq!(
            a, forward,
            "the FIRST search over a zeroed stamp buffer must expand the whole path — \
             if the generation still matched the zeroed buffer, every node would read as \
             already-expanded and this would come back empty"
        );
        assert_eq!(
            b, backward,
            "a bumped generation must isolate the reused scratch buffer — a stale stamp \
             would make the second search skip already-'expanded' nodes"
        );

        // The `Set` arm (the disk arm's scratch, which grows with work done rather than
        // with index size) must satisfy the same contract. Asserted against the same
        // hand-derived truth, not against the `Stamps` arm.
        let mut ex = Expanded::Set(HashSet::new());
        let a = greedy_search_over(0, 3, &adjacency, &points, l, &mut ex).unwrap();
        let b = greedy_search_over(3, 0, &adjacency, &points, l, &mut ex).unwrap();
        assert_eq!(a, forward);
        assert_eq!(b, backward, "Expanded::Set must reset per search");
    }

    /// The stamp generation is a `u32` bumped once per search, and the RW index runs one
    /// search per vector insert for the whole life of the process — so a long-lived
    /// writer really does reach 2^32 searches. On wrap, a naive `gen` returns to `0`,
    /// which is exactly what a *zeroed* stamp buffer holds: every node would read as
    /// already-expanded, the search would expand nothing, and robust-prune would be
    /// handed an empty candidate pool. Silently degraded graph, no error, no panic.
    ///
    /// Drive the counter right up to the wrap and check the search still works.
    #[test]
    fn expanded_stamp_generation_survives_a_u32_wrap() {
        let vectors = vec![
            vec![0.0f32, 0.0],
            vec![1.0, 0.0],
            vec![2.0, 0.0],
            vec![3.0, 0.0],
        ];
        let adjacency: Vec<Vec<u32>> = vec![vec![1], vec![0, 2], vec![1, 3], vec![2]];
        let points = SlabPoints(&vectors);

        // Park the generation one short of wrapping, and dirty the buffer with stamps
        // that a naive wrap-to-zero would collide with.
        let mut stamps = vec![0u32; vectors.len()];
        let mut ex = Expanded::Stamps {
            buf: &mut stamps,
            gen: u32::MAX,
        };

        // This search wraps the generation. It must still expand the whole path.
        let visited = greedy_search_over(0, 3, &adjacency, &points, 8, &mut ex).unwrap();
        assert_eq!(
            visited,
            vec![0, 1, 2, 3],
            "the search across the generation wrap expanded nothing — the wrapped \
             generation collided with the zeroed stamp buffer"
        );

        // And the search *after* the wrap must still be isolated from it.
        let visited = greedy_search_over(3, 0, &adjacency, &points, 8, &mut ex).unwrap();
        assert_eq!(visited, vec![3, 2, 1, 0]);
    }

    #[test]
    fn vamana_store_roundtrips_nodes_and_adjacency() {
        let vectors = unit_vectors(8, 40);
        let g = build_vamana(&vectors, 8, 1.2).unwrap();
        let order = bfs_order(&g);
        // old index -> new (storage) index.
        let mut new_of = vec![0u32; order.len()];
        for (new_idx, &old) in order.iter().enumerate() {
            new_of[old as usize] = new_idx as u32;
        }

        let path = std::env::temp_dir().join(format!("slater_vam_{}_{}", std::process::id(), "rt"));
        let mut w = VamanaWriter::create_with_cipher(&path, 4096, 3, None).unwrap();
        for &old in &order {
            let nbrs: Vec<u32> = g.adjacency[old as usize]
                .iter()
                .map(|&j| new_of[j as usize])
                .collect();
            w.append(old as u64, &vectors[old as usize], &nbrs).unwrap();
        }
        w.finish().unwrap();

        let r = VamanaReader::open_with_cipher(&path, None).unwrap();
        assert_eq!(r.len(), 40);
        // Node at storage index 0 is the medoid (BFS root); its node_id is the old
        // medoid index and its neighbours are remapped.
        let n0 = r.node(0).unwrap();
        assert_eq!(n0.node_id, g.medoid as u64);
        assert_eq!(n0.vector, vectors[g.medoid as usize]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn beam_search_recall_matches_brute_force() {
        // The headline scorer test (PLAN.md): Vamana + PQ navigation must recover
        // most of the brute-force top-k on a synthetic set.
        let dim = 32;
        let n = 1000;
        let vectors = unit_vectors(dim, n);
        let g = build_vamana(&vectors, 32, 1.2).unwrap();

        // Train PQ on the same (normalised) vectors and encode them.
        let params = PqParams::new(dim as u32, 8, 8).unwrap();
        let cb = train_codebooks(&vectors, params, 25).unwrap();
        let codes: Vec<Vec<u8>> = vectors.iter().map(|v| cb.encode(v).unwrap()).collect();

        let k = 10;
        let beam_width = 64;
        let mut total_recall = 0.0f64;
        let queries = 20;
        for q in 0..queries {
            // Use a held-out-ish query: a stored vector perturbed slightly.
            let mut query = vectors[(q * 37) % n].clone();
            query[0] += 0.05;
            let query = normalise(&query);

            let adc = AdcTable::new(&cb, &query).unwrap();
            let hits = beam_search(
                BeamParams {
                    medoid: g.medoid,
                    beam_width,
                    k,
                    num_nodes: n,
                },
                |i| adc.estimate(&codes[i as usize]),
                |i| Ok((vectors[i as usize].clone(), g.adjacency[i as usize].clone())),
                |v| cosine_distance(&query, v),
                // No index→node-id remap and nothing suppressed in this fixture.
                |i| Ok(Some(i as u64)),
            )
            .unwrap();

            let got: std::collections::HashSet<usize> =
                hits.iter().map(|h| h.index as usize).collect();
            let truth = brute_force_topk(&vectors, &query, k);
            let found = truth.iter().filter(|i| got.contains(i)).count();
            total_recall += found as f64 / k as f64;
        }
        let recall = total_recall / queries as f64;
        assert!(
            recall >= 0.85,
            "Vamana+PQ recall@{k} was {recall:.3}, expected ≥ 0.85"
        );
    }

    #[test]
    fn beam_search_scratch_is_bounded_by_visited_not_index_size() {
        // A search over a *huge* index that only touches a handful of nodes must not
        // allocate memory proportional to the index. Pre-fix `beam_search` opened with
        // `vec![false; num_nodes]`, so this `num_nodes` would have forced a ~1 GiB
        // bool array per query; the `HashSet` now grows only with the nodes expanded.
        // The search still returns the correct exact-ranked top-k over the tiny graph.
        let vectors: Vec<Vec<f32>> = vec![
            vec![0.0, 0.0], // 0 = medoid / entry
            vec![1.0, 0.0], // 1
            vec![0.0, 1.0], // 2
            vec![2.0, 0.0], // 3
            vec![0.0, 2.0], // 4
        ];
        // A tiny connected graph: 0 -> {1,2}, 1 -> {3}, 2 -> {4}.
        let adjacency: Vec<Vec<VamanaIndex>> = vec![vec![1, 2], vec![3], vec![4], vec![], vec![]];
        let query = [0.5f32, 0.0];
        let exact = |v: &[f32]| (v[0] - query[0]).powi(2) + (v[1] - query[1]).powi(2);

        let huge_num_nodes = 1usize << 30; // 1 073 741 824 — a billion-entry bool array pre-fix.
        let hits = beam_search(
            BeamParams {
                medoid: 0,
                beam_width: 8,
                k: 3,
                num_nodes: huge_num_nodes,
            },
            |i| exact(&vectors[i as usize]), // estimate == exact here (navigation still valid)
            |i| Ok((vectors[i as usize].clone(), adjacency[i as usize].clone())),
            exact,
            |i| Ok(Some(i as u64)),
        )
        .unwrap();

        // Closest three to (0.5, 0) by squared-L2: nodes 0 and 1 tie at 0.25, node 2
        // is 1.25. With `emit` mapping index → index, the D26 node-id tie-break puts
        // 0 before 1.
        let got: Vec<VamanaIndex> = hits.iter().map(|h| h.index).collect();
        assert_eq!(got, vec![0, 1, 2], "exact-ranked top-3 over the tiny graph");
    }

    #[test]
    fn beam_search_breaks_distance_ties_on_node_id_not_layout_index() {
        // D26: ties on score break by **ascending node id**, so the Vamana arm agrees
        // with the brute-force arm. The `.vamana` layout is BFS-from-medoid order,
        // which is arbitrary w.r.t. node id — tie-breaking on the layout index (as the
        // search used to) silently disagrees with brute force at the k boundary.
        //
        // Indices 0 and 1 sit at the same distance from the query, but their node ids
        // are in the *opposite* order: index 0 → id 100, index 1 → id 5. A node-id
        // tie-break must therefore emit index 1 first; a layout-index tie-break emits
        // index 0 first, which is the bug.
        let vectors: Vec<Vec<f32>> = vec![
            vec![0.0, 0.0], // index 0 → node id 100
            vec![1.0, 0.0], // index 1 → node id 5
            vec![0.0, 1.0], // index 2 → node id 7
        ];
        let adjacency: Vec<Vec<VamanaIndex>> = vec![vec![1, 2], vec![0, 2], vec![0, 1]];
        let node_ids: [u64; 3] = [100, 5, 7];
        let query = [0.5f32, 0.0];
        let exact = |v: &[f32]| (v[0] - query[0]).powi(2) + (v[1] - query[1]).powi(2);

        let hits = beam_search(
            BeamParams {
                medoid: 0,
                beam_width: 8,
                k: 2,
                num_nodes: vectors.len(),
            },
            |i| exact(&vectors[i as usize]),
            |i| Ok((vectors[i as usize].clone(), adjacency[i as usize].clone())),
            exact,
            |i| Ok(Some(node_ids[i as usize])),
        )
        .unwrap();

        // Both tie at 0.25; ascending node id puts 5 (index 1) ahead of 100 (index 0).
        assert_eq!(
            hits.iter().map(|h| h.node_id).collect::<Vec<_>>(),
            vec![5, 100],
            "a distance tie must break on ascending node id (D26), not on layout index"
        );
    }

    #[test]
    fn beam_search_suppressed_nodes_stay_navigable_waypoints() {
        // A deleted node must not be *emitted*, but it must still be *expanded* — it is
        // the only way to reach what lies behind it. Here index 1 is the sole bridge to
        // index 2: suppressing it must still yield index 2, which is the whole point of
        // gating `hits.push` rather than pruning the node from the walk.
        let vectors: Vec<Vec<f32>> = vec![
            vec![9.0, 0.0], // 0 = entry, far from the query
            vec![1.0, 0.0], // 1 = the bridge — suppressed
            vec![0.0, 0.0], // 2 = the nearest live node, reachable only via 1
        ];
        let adjacency: Vec<Vec<VamanaIndex>> = vec![vec![1], vec![2], vec![]];
        let query = [0.0f32, 0.0];
        let exact = |v: &[f32]| (v[0] - query[0]).powi(2) + (v[1] - query[1]).powi(2);

        let hits = beam_search(
            BeamParams {
                medoid: 0,
                beam_width: 8,
                k: 3,
                num_nodes: vectors.len(),
            },
            |i| exact(&vectors[i as usize]),
            |i| Ok((vectors[i as usize].clone(), adjacency[i as usize].clone())),
            exact,
            // Index 1 is "deleted": navigable, never emitted.
            |i| Ok((i != 1).then_some(i as u64)),
        )
        .unwrap();

        let got: Vec<u64> = hits.iter().map(|h| h.node_id).collect();
        assert_eq!(
            got,
            vec![2, 0],
            "the suppressed bridge must not be emitted but must still be walked through to reach 2"
        );
    }
}
