// SPDX-License-Identifier: Apache-2.0
//! Brute-force vector KNN.
//!
//! Slater's whole live estate is below the 50k-vector ANN threshold (PLAN.md
//! "Scale & graph inventory"), so the *real* read path for `db.idx.vector`
//! `.queryNodes` is a brute-force scan over the index's full-precision vectors:
//! read the contiguous index group from `vectors.f32.blk` (D10) through the block
//! LRU, score every candidate against the query vector, keep the `k` best. The
//! disk-native Vamana/PQ path (`AnnMode::Vamana`) is M7 — this module is the
//! `AnnMode::BruteForce` arm only.
//!
//! The scoring + selection here is a pure function over a slice of
//! [`VectorEntry`]s so it can be unit-tested against a hand-computed reference
//! independently of the store/cache plumbing; [`crate::exec`] supplies the entries
//! (read through the cache) and the query vector.
//
// DESIGN (D26): `score` mirrors FalkorDB's `db.idx.vector.queryNodes` contract —
// it is the **distance** under the index's metric, and results are ordered
// **ascending** (nearest first), so a smaller score is a closer match. For a
// cosine index that distance is `1 - cosine_similarity`, in `[0, 2]`. The
// companion scalar `similarity(a, b)` returns the complementary cosine
// *similarity* in `[-1, 1]` (so `score == 1 - similarity(query, node)`). Ties on
// score are broken by ascending node id so a query is deterministic.
#![allow(dead_code)]

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use anyhow::{bail, Result};
use rayon::prelude::*;
use wide::f32x8;

use graph_format::manifest::Metric;
use graph_format::vectors::VectorEntry;

/// A liveness predicate over dense node ids: `false` suppresses the node from the
/// results without otherwise affecting the scan. A node deleted in the delta (or by
/// a segment flush) still sits in the sealed base index — the base is immutable, so
/// the only place a delete can take effect on the read path is here.
///
/// Fallible because resolving a node over the core stack can read a block. Callers
/// pass `None` on a pure-core generation with an empty delta, so a read-only estate
/// pays nothing.
pub type LivePredicate<'a> = &'a (dyn Fn(u64) -> Result<bool> + Sync);

/// One KNN hit: the dense node id and its distance score (see the module DESIGN
/// note — smaller is closer).
#[derive(Debug, Clone, PartialEq)]
pub struct Neighbour {
    pub node_id: u64,
    pub score: f64,
}

/// Cosine similarity of two equal-length vectors, in `[-1, 1]`. A zero-norm
/// vector has no direction, so its similarity to anything is defined as `0`
/// (rather than `NaN`), which makes it maximally distant under the cosine metric.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;
    for (x, y) in a.iter().zip(b) {
        let (x, y) = (*x as f64, *y as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// Euclidean (L2) distance between two equal-length vectors:
/// `sqrt(sum((a[i] - b[i])^2))`. Mirrors FalkorDB `SIVector_EuclideanDistance`
/// (`vec.euclideanDistance`), which computes in float32; we accumulate in f64 for
/// consistency with [`cosine_similarity`] (the values round-trip identically at the
/// 3-decimal precision FalkorDB's own tests assert). The caller guarantees equal
/// lengths.
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f64 {
    let mut sum = 0.0f64;
    for (x, y) in a.iter().zip(b) {
        let d = *x as f64 - *y as f64;
        sum += d * d;
    }
    sum.sqrt()
}

/// Cosine *distance* between two equal-length vectors: `1 - cosine_similarity`,
/// in `[0, 2]`. Mirrors FalkorDB `vec.cosineDistance`. A zero-norm vector yields a
/// similarity of `0` (see [`cosine_similarity`]) and hence a distance of `1`, where
/// FalkorDB would produce `NaN` — a deliberate, more-useful divergence.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    1.0 - cosine_similarity(a, b)
}

/// The distance score for `query` vs `candidate` under `metric` — the value
/// surfaced as `score` and ordered ascending. Public so the Vamana arm uses the
/// identical exact re-rank scoring as the brute-force arm (same `score` contract).
pub fn distance(metric: Metric, query: &[f32], candidate: &[f32]) -> f64 {
    match metric {
        Metric::Cosine => 1.0 - cosine_similarity(query, candidate),
        // Inner-product "distance": larger dot product = more similar = smaller
        // distance, so negate. (Not used by the live estate; cosine is the path.)
        Metric::Dot => -query
            .iter()
            .zip(candidate)
            .map(|(x, y)| *x as f64 * *y as f64)
            .sum::<f64>(),
        // Squared Euclidean — monotonic in the true L2 distance, so it orders
        // identically while avoiding a per-candidate sqrt.
        Metric::L2 => query
            .iter()
            .zip(candidate)
            .map(|(x, y)| {
                let d = *x as f64 - *y as f64;
                d * d
            })
            .sum::<f64>(),
    }
}

// ── Fast scoring kernels ─────────────────────────────────────────────────────
//
// The brute-force scan is compute-bound on the per-candidate distance over
// 1024-dim vectors, so the kernels below vectorize it with explicit SIMD
// (`wide::f32x8`) and accumulate in f32 — guaranteed vectorization regardless of
// the `opt-level = "s"` release profile's (largely absent) autovectorization, and
// half the data width of the f64 reference in [`cosine_similarity`] et al. The
// f64 reference functions above are kept verbatim as the *exact* contract used by
// the `similarity()`/`*Distance()` Cypher builtins and the Vamana re-rank; these
// kernels are the **ordering** path (kNN selection), where an f32 reduction error
// in the ~6th–7th significant digit is invisible at the 3-decimal score contract
// but lets the scan run several times faster. The reduction order is fixed (lane
// count + in-order chunking), so results are deterministic run-to-run.

/// `a·b` over equal-length slices, f32 SIMD with a scalar tail.
#[inline]
fn dot_simd(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = f32x8::ZERO;
    let mut ar = a.chunks_exact(8);
    let mut br = b.chunks_exact(8);
    for (ac, bc) in ar.by_ref().zip(br.by_ref()) {
        let av = f32x8::from(<[f32; 8]>::try_from(ac).unwrap());
        let bv = f32x8::from(<[f32; 8]>::try_from(bc).unwrap());
        acc = av.mul_add(bv, acc);
    }
    let mut sum = acc.reduce_add();
    for (x, y) in ar.remainder().iter().zip(br.remainder()) {
        sum += x * y;
    }
    sum
}

/// One fused SIMD pass returning `(q·v, v·v)` — the dot product and the
/// candidate's squared norm, so cosine needs a single sweep over `v`.
#[inline]
fn dot_and_norm2(q: &[f32], v: &[f32]) -> (f32, f32) {
    let mut accd = f32x8::ZERO;
    let mut accn = f32x8::ZERO;
    let mut qr = q.chunks_exact(8);
    let mut vr = v.chunks_exact(8);
    for (qc, vc) in qr.by_ref().zip(vr.by_ref()) {
        let qv = f32x8::from(<[f32; 8]>::try_from(qc).unwrap());
        let vv = f32x8::from(<[f32; 8]>::try_from(vc).unwrap());
        accd = qv.mul_add(vv, accd);
        accn = vv.mul_add(vv, accn);
    }
    let (mut d, mut n) = (accd.reduce_add(), accn.reduce_add());
    for (x, y) in qr.remainder().iter().zip(vr.remainder()) {
        d += x * y;
        n += y * y;
    }
    (d, n)
}

/// Squared Euclidean distance `Σ(q-v)²`, f32 SIMD with a scalar tail. Monotonic in
/// the true L2 distance (same ordering as the f64 reference's `Metric::L2`).
#[inline]
fn l2_sq_simd(q: &[f32], v: &[f32]) -> f32 {
    let mut acc = f32x8::ZERO;
    let mut qr = q.chunks_exact(8);
    let mut vr = v.chunks_exact(8);
    for (qc, vc) in qr.by_ref().zip(vr.by_ref()) {
        let qv = f32x8::from(<[f32; 8]>::try_from(qc).unwrap());
        let vv = f32x8::from(<[f32; 8]>::try_from(vc).unwrap());
        let d = qv - vv;
        acc = d.mul_add(d, acc);
    }
    let mut sum = acc.reduce_add();
    for (x, y) in qr.remainder().iter().zip(vr.remainder()) {
        let d = x - y;
        sum += d * d;
    }
    sum
}

/// The ascending distance score under `metric` for `query` vs `candidate`, using
/// the fast f32 kernels. `query_norm` is `|query|` (hoisted once per query — it is
/// constant across all candidates, so the reference's per-candidate recompute is
/// pure waste). Same contract as [`distance`], at f32-reduction precision.
#[inline]
fn score_fast(metric: Metric, query: &[f32], query_norm: f64, candidate: &[f32]) -> f64 {
    match metric {
        Metric::Cosine => {
            let (dot, vn2) = dot_and_norm2(query, candidate);
            // Zero-norm vector has no direction → similarity 0 → distance 1
            // (mirrors [`cosine_similarity`]'s defined-as-0 behaviour).
            if query_norm == 0.0 || vn2 == 0.0 {
                return 1.0;
            }
            let sim = dot as f64 / (query_norm * (vn2 as f64).sqrt());
            1.0 - sim
        }
        Metric::Dot => -(dot_simd(query, candidate) as f64),
        Metric::L2 => l2_sq_simd(query, candidate) as f64,
    }
}

/// `|query|` for the cosine path (f64 sum for stability; once per query). Returns
/// `0.0` for the other metrics, which don't use it.
#[inline]
fn query_norm_for(metric: Metric, query: &[f32]) -> f64 {
    match metric {
        Metric::Cosine => query
            .iter()
            .map(|x| (*x as f64) * (*x as f64))
            .sum::<f64>()
            .sqrt(),
        _ => 0.0,
    }
}

/// A bounded top-`k` collector: keeps only the `k` lowest-distance neighbours in a
/// `k`-capped max-heap (the worst sits at the top, ready to be evicted), so the
/// scan is O(N log k) and allocates `O(k)` instead of sorting all N. The heap
/// order is the exact `(score asc, node_id asc)` contract, so `into_sorted`
/// returns the same deterministic ascending list the old full sort produced.
struct TopK {
    k: usize,
    heap: BinaryHeap<HeapItem>,
}

#[derive(PartialEq)]
struct HeapItem {
    score: f64,
    node_id: u64,
}
impl Eq for HeapItem {}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TopK {
    fn new(k: usize) -> Self {
        TopK {
            k,
            heap: BinaryHeap::with_capacity(k.saturating_add(1).min(1 << 16)),
        }
    }
    /// Whether `(score, node_id)` would enter the top-`k` on **distance alone**.
    /// Checked before the liveness probe so a candidate that already loses on
    /// distance never pays for one — which is what keeps filtering ~free: the probe
    /// runs O(k) times over a scan, not O(N).
    #[inline]
    fn admits(&self, node_id: u64, score: f64) -> bool {
        if self.k == 0 {
            return false;
        }
        if self.heap.len() < self.k {
            return true;
        }
        let item = HeapItem { score, node_id };
        self.heap.peek().is_some_and(|worst| item < *worst)
    }
    /// Score `(node_id, score)` into the heap, consulting `live` only if it would be
    /// admitted. A suppressed node is dropped outright — it does not displace a live
    /// candidate, so the result is the exact top-`k` over the *live* set.
    #[inline]
    fn push_live(&mut self, node_id: u64, score: f64, live: Option<LivePredicate>) -> Result<()> {
        if !self.admits(node_id, score) {
            return Ok(());
        }
        if let Some(f) = live {
            if !f(node_id)? {
                return Ok(());
            }
        }
        self.push(node_id, score);
        Ok(())
    }
    #[inline]
    fn push(&mut self, node_id: u64, score: f64) {
        if self.k == 0 {
            return;
        }
        let item = HeapItem { score, node_id };
        if self.heap.len() < self.k {
            self.heap.push(item);
        } else if let Some(worst) = self.heap.peek() {
            // Strictly better than the current worst → it takes its place.
            if item < *worst {
                self.heap.pop();
                self.heap.push(item);
            }
        }
    }
    fn into_sorted(self) -> Vec<Neighbour> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|h| Neighbour {
                node_id: h.node_id,
                score: h.score,
            })
            .collect()
    }
}

/// Brute-force `k`-nearest-neighbour scan over a vector index group.
///
/// Every entry must have the same dimensionality as `query` (the store is
/// self-describing about `dim`, so a mismatch is a hard error rather than a
/// silently-wrong score). Returns the `k` lowest-distance neighbours, ascending
/// by score then by node id; fewer than `k` if the group is smaller. Scoring uses
/// the fast f32 SIMD kernels and a bounded top-`k` heap (see [`score_fast`] /
/// [`TopK`]).
///
/// `live` (when present) suppresses deleted nodes from the result — see
/// [`LivePredicate`]. The top-`k` is taken over the live set, so a deleted node
/// never occupies one of the `k` slots.
pub fn brute_force_knn(
    entries: &[VectorEntry],
    query: &[f32],
    k: usize,
    metric: Metric,
    live: Option<LivePredicate>,
) -> Result<Vec<Neighbour>> {
    let query_norm = query_norm_for(metric, query);
    let mut topk = TopK::new(k);
    for e in entries {
        if e.vector.len() != query.len() {
            bail!(
                "query vector has dimension {} but indexed node {} has dimension {}",
                query.len(),
                e.node_id,
                e.vector.len()
            );
        }
        let score = score_fast(metric, query, query_norm, &e.vector);
        topk.push_live(e.node_id, score, live)?;
    }
    Ok(topk.into_sorted())
}

/// Parallel brute-force kNN over a vector index group — identical contract and result
/// to [`brute_force_knn`].
///
/// When `pool` is present, `k > 0`, and `entries.len() >= min_par`, the group is split
/// into one chunk per worker thread; each chunk is scored exactly and reduced to its own
/// top-`k`, then the per-chunk lists are merged with the same `(score ascending, node id
/// ascending)` comparator and truncated. Because the merge re-sorts globally and node ids
/// are unique, the output matches the sequential scan element-for-element regardless of
/// chunk boundaries. Falls back to [`brute_force_knn`] below the threshold or with no pool.
pub fn brute_force_knn_par(
    pool: Option<&rayon::ThreadPool>,
    entries: &[VectorEntry],
    query: &[f32],
    k: usize,
    metric: Metric,
    min_par: usize,
    live: Option<LivePredicate>,
) -> Result<Vec<Neighbour>> {
    let pool = match pool {
        Some(p) if k > 0 && entries.len() >= min_par => p,
        _ => return brute_force_knn(entries, query, k, metric, live),
    };
    // One chunk per worker keeps the per-chunk top-k bounded (≤ k each) and the merge
    // small. `brute_force_knn` over a chunk scores it exactly and reduces to its top-k.
    let chunk = entries
        .len()
        .div_ceil(pool.current_num_threads().max(1))
        .max(1);
    let partials: Vec<Vec<Neighbour>> = pool.install(|| {
        entries
            .par_chunks(chunk)
            .map(|c| brute_force_knn(c, query, k, metric, live))
            .collect::<Result<Vec<_>>>()
    })?;
    // Merge the per-chunk top-k lists into one bounded top-k. The global k-smallest
    // are a subset of the union of the per-chunk k-smallest, so this yields the
    // exact same ordered list as the sequential scan (scores are per-candidate, so
    // chunking cannot change them; the `(score, node_id)` order is a total order).
    let mut merged = TopK::new(k);
    for n in partials.into_iter().flatten() {
        merged.push(n.node_id, n.score);
    }
    Ok(merged.into_sorted())
}

// ── Resident pre-decoded matrix (the no-gather kNN path) ─────────────────────
//
// The [`brute_force_knn`] path above takes `&[VectorEntry]` — a slice the caller
// gathers per query by reading + decoding every record through the block cache,
// allocating a fresh `Vec<f32>` per vector (tens of MiB of alloc churn per query
// for a 10k-vector group). [`ResidentMatrix`] instead holds the whole index group
// **decoded once** into one contiguous row-major buffer (unit-normalized up front
// for cosine, so scoring is a single dot product), so repeat queries scan resident
// memory with no gather, no allocation, and no per-row pointer chasing. It lives in
// the vector-index pool, charged to `vector_cache_bytes` (see [`crate::cache`]).

/// A whole vector index group decoded once into a contiguous, row-major f32 matrix
/// (`rows × dim`), with `node_ids[i]` the dense id of row `i`. For [`Metric::Cosine`]
/// every row is L2-normalized to unit length, so the cosine distance to a (likewise
/// normalized) query is `1 - dot`. Built once per `(generation, index)` and reused.
pub struct ResidentMatrix {
    pub dim: usize,
    pub metric: Metric,
    pub node_ids: Vec<u64>,
    /// Row-major `node_ids.len() * dim`. Unit-normalized rows when `metric == Cosine`.
    pub data: Vec<f32>,
}

impl ResidentMatrix {
    /// Decode an index group (one [`VectorEntry`] per row) into the contiguous,
    /// (for cosine) unit-normalized matrix. Errors if any row's dimension differs.
    pub fn from_entries(dim: usize, metric: Metric, entries: Vec<VectorEntry>) -> Result<Self> {
        let mut node_ids = Vec::with_capacity(entries.len());
        let mut data = Vec::with_capacity(entries.len() * dim);
        for e in entries {
            if e.vector.len() != dim {
                bail!(
                    "indexed node {} has dimension {} but the index is {}-dimensional",
                    e.node_id,
                    e.vector.len(),
                    dim
                );
            }
            node_ids.push(e.node_id);
            if metric == Metric::Cosine {
                let norm = e
                    .vector
                    .iter()
                    .map(|x| (*x as f64) * (*x as f64))
                    .sum::<f64>()
                    .sqrt();
                if norm > 0.0 {
                    let inv = (1.0 / norm) as f32;
                    data.extend(e.vector.iter().map(|x| x * inv));
                } else {
                    data.extend_from_slice(&e.vector); // zero stays zero (sim 0 → dist 1)
                }
            } else {
                data.extend_from_slice(&e.vector);
            }
        }
        Ok(Self {
            dim,
            metric,
            node_ids,
            data,
        })
    }

    pub fn rows(&self) -> usize {
        self.node_ids.len()
    }

    /// Resident footprint charged against the vector-index budget.
    pub fn resident_bytes(&self) -> usize {
        self.data.len() * std::mem::size_of::<f32>()
            + self.node_ids.len() * std::mem::size_of::<u64>()
            + std::mem::size_of::<Self>()
    }
}

/// A query prepared once for a whole matrix scan: for cosine, the unit-normalized
/// query (or a flag that the query is zero-norm); for L2/Dot, the raw query.
enum PreparedQuery<'a> {
    CosineUnit(Vec<f32>),
    CosineZero,
    Raw(&'a [f32]),
}

impl PreparedQuery<'_> {
    fn prepare(metric: Metric, query: &[f32]) -> PreparedQuery<'_> {
        match metric {
            Metric::Cosine => {
                let n = query
                    .iter()
                    .map(|x| (*x as f64) * (*x as f64))
                    .sum::<f64>()
                    .sqrt();
                if n == 0.0 {
                    PreparedQuery::CosineZero
                } else {
                    let inv = (1.0 / n) as f32;
                    PreparedQuery::CosineUnit(query.iter().map(|x| x * inv).collect())
                }
            }
            _ => PreparedQuery::Raw(query),
        }
    }
}

/// Score matrix rows `[start, end)` against the prepared query into a bounded top-k.
fn scan_matrix_range(
    m: &ResidentMatrix,
    pq: &PreparedQuery,
    metric: Metric,
    start: usize,
    end: usize,
    k: usize,
    live: Option<LivePredicate>,
) -> Result<TopK> {
    let mut topk = TopK::new(k);
    for i in start..end {
        let row = &m.data[i * m.dim..(i + 1) * m.dim];
        let score = match pq {
            // Rows are unit-normalized, so cosine distance is `1 - dot`.
            PreparedQuery::CosineUnit(q) => 1.0 - dot_simd(q, row) as f64,
            PreparedQuery::CosineZero => 1.0,
            PreparedQuery::Raw(q) => match metric {
                Metric::L2 => l2_sq_simd(q, row) as f64,
                Metric::Dot => -(dot_simd(q, row) as f64),
                Metric::Cosine => unreachable!("cosine uses CosineUnit/CosineZero"),
            },
        };
        topk.push_live(m.node_ids[i], score, live)?;
    }
    Ok(topk)
}

/// Brute-force kNN over a [`ResidentMatrix`] — same contract and result as
/// [`brute_force_knn`] (ascending by score then node id), but scanning the resident
/// contiguous matrix instead of a gathered `&[VectorEntry]`.
pub fn brute_force_knn_matrix(
    m: &ResidentMatrix,
    query: &[f32],
    k: usize,
    live: Option<LivePredicate>,
) -> Result<Vec<Neighbour>> {
    if query.len() != m.dim {
        bail!(
            "query vector has dimension {} but the index is {}-dimensional",
            query.len(),
            m.dim
        );
    }
    let pq = PreparedQuery::prepare(m.metric, query);
    Ok(scan_matrix_range(m, &pq, m.metric, 0, m.rows(), k, live)?.into_sorted())
}

/// Parallel [`brute_force_knn_matrix`] — identical result, chunked one range per
/// worker (scores are per-row, so the merged top-k matches the sequential scan).
pub fn brute_force_knn_matrix_par(
    pool: Option<&rayon::ThreadPool>,
    m: &ResidentMatrix,
    query: &[f32],
    k: usize,
    min_par: usize,
    live: Option<LivePredicate>,
) -> Result<Vec<Neighbour>> {
    if query.len() != m.dim {
        bail!(
            "query vector has dimension {} but the index is {}-dimensional",
            query.len(),
            m.dim
        );
    }
    let n = m.rows();
    let pool = match pool {
        Some(p) if k > 0 && n >= min_par => p,
        _ => return brute_force_knn_matrix(m, query, k, live),
    };
    let pq = PreparedQuery::prepare(m.metric, query);
    let metric = m.metric;
    let chunk = n.div_ceil(pool.current_num_threads().max(1)).max(1);
    let ranges: Vec<(usize, usize)> = (0..n)
        .step_by(chunk)
        .map(|s| (s, (s + chunk).min(n)))
        .collect();
    let partials: Vec<Vec<Neighbour>> = pool.install(|| {
        ranges
            .par_iter()
            .map(|&(s, e)| Ok(scan_matrix_range(m, &pq, metric, s, e, k, live)?.into_sorted()))
            .collect::<Result<Vec<_>>>()
    })?;
    let mut merged = TopK::new(k);
    for nb in partials.into_iter().flatten() {
        merged.push(nb.node_id, nb.score);
    }
    Ok(merged.into_sorted())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(node_id: u64, v: &[f32]) -> VectorEntry {
        VectorEntry {
            node_id,
            vector: v.to_vec(),
        }
    }

    #[test]
    fn cosine_similarity_matches_hand_computation() {
        // Identical direction → 1; orthogonal → 0; opposite → -1.
        assert!((cosine_similarity(&[1.0, 0.0], &[2.0, 0.0]) - 1.0).abs() < 1e-12);
        assert!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-12);
        assert!((cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]) + 1.0).abs() < 1e-12);
        // A worked non-trivial case: a=(1,2,3), b=(2,0,1).
        // dot=2+0+3=5; |a|=sqrt(14); |b|=sqrt(5); cos=5/sqrt(70).
        let want = 5.0 / 70.0f64.sqrt();
        assert!((cosine_similarity(&[1.0, 2.0, 3.0], &[2.0, 0.0, 1.0]) - want).abs() < 1e-12);
    }

    #[test]
    fn zero_norm_vector_is_maximally_distant() {
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]), 0.0);
        let n = brute_force_knn(
            &[entry(7, &[0.0, 0.0])],
            &[1.0, 1.0],
            1,
            Metric::Cosine,
            None,
        )
        .unwrap();
        assert!((n[0].score - 1.0).abs() < 1e-12);
    }

    #[test]
    fn knn_orders_by_distance_with_scores_matching_reference() {
        // Query near node 1's direction; node 2 orthogonal; node 0 opposite-ish.
        let entries = vec![
            entry(0, &[-1.0, 0.0]),
            entry(1, &[1.0, 0.1]),
            entry(2, &[0.0, 1.0]),
            entry(3, &[0.9, 0.05]),
        ];
        let query = [1.0, 0.0];
        let got = brute_force_knn(&entries, &query, 3, Metric::Cosine, None).unwrap();

        // Reference: distance = 1 - cosine_similarity, ascending, tie-break node id.
        let mut reference: Vec<Neighbour> = entries
            .iter()
            .map(|e| Neighbour {
                node_id: e.node_id,
                score: 1.0 - cosine_similarity(&query, &e.vector),
            })
            .collect();
        reference.sort_by(|a, b| {
            a.score
                .total_cmp(&b.score)
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
        reference.truncate(3);

        assert_eq!(
            got.iter().map(|n| n.node_id).collect::<Vec<_>>(),
            reference.iter().map(|n| n.node_id).collect::<Vec<_>>()
        );
        // The kNN path scores with the fast f32 SIMD kernel, so it tracks the f64
        // reference to ~1e-5, not bit-for-bit (the f64 reference itself is asserted
        // to 1e-12 in `cosine_similarity_matches_hand_computation`). This is well
        // inside the 3-decimal score contract.
        for (g, r) in got.iter().zip(&reference) {
            assert!((g.score - r.score).abs() < 1e-5, "score {g:?} vs {r:?}");
        }
        // Sanity: node 3 (smallest angle to +x) is closest, then node 1; node 2
        // (orthogonal) is third and node 0 (-x) is furthest, so it falls outside k=3.
        assert_eq!(got[0].node_id, 3);
        assert_eq!(got[1].node_id, 1);
        assert_eq!(got[2].node_id, 2);
    }

    #[test]
    fn k_larger_than_group_returns_all() {
        let entries = vec![entry(0, &[1.0, 0.0]), entry(1, &[0.0, 1.0])];
        let got = brute_force_knn(&entries, &[1.0, 0.0], 10, Metric::Cosine, None).unwrap();
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn dimension_mismatch_is_an_error() {
        let entries = vec![entry(0, &[1.0, 0.0, 0.0])];
        let err = brute_force_knn(&entries, &[1.0, 0.0], 1, Metric::Cosine, None)
            .err()
            .unwrap();
        assert!(err.to_string().contains("dimension"), "got: {err}");
    }

    /// A deterministic spread of vectors, large enough to cross several rayon chunks.
    fn spread(n: u64) -> Vec<VectorEntry> {
        (0..n)
            .map(|i| {
                let a = (i % 17) as f32 * 0.13 - 1.0;
                let b = (i % 31) as f32 * 0.07 + 0.5;
                let c = ((i * 7) % 23) as f32 * 0.05;
                entry(i, &[a, b, c])
            })
            .collect()
    }

    #[test]
    fn knn_par_matches_sequential() {
        // The chunked parallel scan must return the exact same ordered (id, score)
        // list as the sequential scan across metrics and a range of k (including
        // k > group size). `min_par = 1` forces the rayon branch even for the pool.
        let entries = spread(1000);
        let query = [0.3f32, -0.2, 0.8];
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        for metric in [Metric::Cosine, Metric::L2, Metric::Dot] {
            for k in [1usize, 5, 50, 999, 1000, 2000] {
                let seq = brute_force_knn(&entries, &query, k, metric, None).unwrap();
                let par =
                    brute_force_knn_par(Some(&pool), &entries, &query, k, metric, 1, None).unwrap();
                assert_eq!(seq, par, "metric {metric:?}, k {k}");
            }
        }
    }

    #[test]
    fn knn_par_falls_back_below_threshold_and_without_pool() {
        let entries = spread(50);
        let query = [0.1f32, 0.2, 0.3];
        let seq = brute_force_knn(&entries, &query, 5, Metric::Cosine, None).unwrap();
        // No pool → sequential.
        assert_eq!(
            brute_force_knn_par(None, &entries, &query, 5, Metric::Cosine, 1, None).unwrap(),
            seq
        );
        // Pool present but group below `min_par` → sequential fallback, still correct.
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        assert_eq!(
            brute_force_knn_par(Some(&pool), &entries, &query, 5, Metric::Cosine, 256, None)
                .unwrap(),
            seq
        );
    }

    #[test]
    fn knn_par_propagates_dimension_mismatch() {
        let mut entries = spread(300);
        entries[200] = entry(200, &[1.0, 0.0]); // a 2-dim entry in a 3-dim group
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let err = brute_force_knn_par(
            Some(&pool),
            &entries,
            &[0.1, 0.2, 0.3],
            5,
            Metric::Cosine,
            1,
            None,
        )
        .err()
        .unwrap();
        assert!(err.to_string().contains("dimension"), "got: {err}");
    }

    #[test]
    fn matrix_knn_matches_entry_scan() {
        // The resident-matrix path must return the same ordered ids and (to f32
        // tolerance) the same scores as the gathered `&[VectorEntry]` path, across
        // metrics and a range of k, including the parallel matrix scan.
        let entries = spread(1000);
        let query = [0.3f32, -0.2, 0.8];
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        for metric in [Metric::Cosine, Metric::L2, Metric::Dot] {
            let m = ResidentMatrix::from_entries(3, metric, entries.clone()).unwrap();
            for k in [1usize, 5, 50, 999, 1000, 2000] {
                let want = brute_force_knn(&entries, &query, k, metric, None).unwrap();
                let mat = brute_force_knn_matrix(&m, &query, k, None).unwrap();
                let mat_par =
                    brute_force_knn_matrix_par(Some(&pool), &m, &query, k, 1, None).unwrap();
                assert_eq!(
                    mat.iter().map(|n| n.node_id).collect::<Vec<_>>(),
                    want.iter().map(|n| n.node_id).collect::<Vec<_>>(),
                    "ids differ, metric {metric:?}, k {k}"
                );
                assert_eq!(mat, mat_par, "seq vs par matrix, metric {metric:?}, k {k}");
                for (g, r) in mat.iter().zip(&want) {
                    assert!(
                        (g.score - r.score).abs() < 1e-5,
                        "score {g:?} vs {r:?}, metric {metric:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn matrix_zero_query_and_zero_row_are_maximally_distant() {
        // Zero-norm query → every distance 1; zero-norm row → that row's distance 1.
        let entries = vec![entry(0, &[0.0, 0.0]), entry(1, &[1.0, 1.0])];
        let m = ResidentMatrix::from_entries(2, Metric::Cosine, entries).unwrap();
        let zero_q = brute_force_knn_matrix(&m, &[0.0, 0.0], 2, None).unwrap();
        for n in &zero_q {
            assert!((n.score - 1.0).abs() < 1e-6, "{n:?}");
        }
        let real_q = brute_force_knn_matrix(&m, &[1.0, 1.0], 2, None).unwrap();
        // Node 1 (same direction) is closest (~0); node 0 (zero) is distance 1.
        assert_eq!(real_q[0].node_id, 1);
        assert!(real_q[0].score < 1e-6);
        assert!((real_q[1].score - 1.0).abs() < 1e-6);
    }
}
