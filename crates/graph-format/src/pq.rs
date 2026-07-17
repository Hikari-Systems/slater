// SPDX-License-Identifier: Apache-2.0
//! Product quantisation (PQ) codebooks + codes for the large-vector ANN path.
//!
//! A PQ index splits each `dim`-dimensional vector into `m` contiguous
//! sub-vectors of length `dsub = dim / m`, and quantises each sub-vector against a
//! per-subspace codebook of `k = 2^bits` centroids trained by k-means. A vector is
//! then stored as `m` small codes (one centroid id per subspace, `bits ≤ 8` ⇒ one
//! byte each), so a 1024-dim f32 vector (4 KiB) compresses to `m` bytes
//! (~16–128 B). Those codes are what the beam search holds **resident** (the
//! `// DESIGN:` of the whole milestone — never a full in-memory graph), navigating
//! by a PQ-*estimated* distance computed from a small per-query lookup table.
//!
//! The estimate is **asymmetric distance computation** (ADC): the query stays
//! full-precision; for each subspace we precompute the squared-L2 distance from the
//! query sub-vector to every centroid (`AdcTable`), then a candidate's estimated
//! distance is the sum of `m` table look-ups keyed by its codes. ADC is the
//! standard, more accurate PQ estimator (the query is never quantised).
//!
// DESIGN (D29): for a **cosine** index every vector is L2-normalised before
// training/encoding, and the PQ estimate is squared-L2 in that normalised space.
// On unit vectors squared-L2 is `2 − 2·cos`, i.e. monotonic in cosine distance, so
// navigating by PQ-estimated squared-L2 ranks candidates identically to cosine —
// while the final re-rank still uses the *exact* cosine distance on the full
// vectors. Training on normalised vectors keeps the codebooks in the same space the
// estimate works in. Callers pass already-normalised vectors, and they normalise
// through [`normalise`] — this module owns the **single** definition of that space.
// Every arm whose score reaches `slater`'s `vector::merge_topk` must go through it; two
// arms that normalise differently score on different scales and the merge interleaves
// them wrongly, with no error and no panic. Beyond that, this module does the
// quantisation maths only.

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::blockfile::{parse_block, record_from_block, BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::manifest::Metric;
use crate::wire::{
    capacity_for, capacity_hint, checked_span, read_uvarint, write_uvarint, DecodeRejected,
};

/// PQ structural parameters, recorded so the store is self-describing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PqParams {
    pub dim: u32,
    /// Number of subspaces (`m`). Must divide `dim`.
    pub subspaces: u32,
    /// Sub-vector length (`dim / m`).
    pub dsub: u32,
    /// Centroids per subspace (`k = 2^bits`).
    pub k: u32,
}

impl PqParams {
    pub fn new(dim: u32, subspaces: u32, bits: u32) -> Result<Self> {
        if subspaces == 0 || dim == 0 {
            bail!("PQ requires non-zero dim and subspaces");
        }
        if !dim.is_multiple_of(subspaces) {
            bail!("PQ subspaces ({subspaces}) must divide dim ({dim})");
        }
        if !(1..=8).contains(&bits) {
            bail!("PQ bits ({bits}) must be in 1..=8 (codes are stored one byte each)");
        }
        Ok(Self {
            dim,
            subspaces,
            dsub: dim / subspaces,
            k: 1u32 << bits,
        })
    }
}

/// A trained codebook: `subspaces × k × dsub` centroids, stored flat. The
/// centroid `c` of subspace `s` is `centroids[(s*k + c)*dsub .. +dsub]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Codebook {
    pub params: PqParams,
    pub centroids: Vec<f32>,
}

impl Codebook {
    fn centroid(&self, s: usize, c: usize) -> &[f32] {
        let dsub = self.params.dsub as usize;
        let k = self.params.k as usize;
        let base = (s * k + c) * dsub;
        &self.centroids[base..base + dsub]
    }

    /// Encode one full vector into its `m` subspace codes (one byte per subspace).
    /// The vector must be `dim`-long and is expected already normalised for a
    /// cosine index (D29).
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>> {
        if vector.len() != self.params.dim as usize {
            bail!(
                "cannot encode dim {} vector with a dim {} codebook",
                vector.len(),
                self.params.dim
            );
        }
        let m = self.params.subspaces as usize;
        let dsub = self.params.dsub as usize;
        let k = self.params.k as usize;
        let mut codes = Vec::with_capacity(m);
        for s in 0..m {
            let sub = &vector[s * dsub..(s + 1) * dsub];
            let mut best = 0usize;
            let mut best_d = f64::INFINITY;
            for c in 0..k {
                let d = sq_l2(sub, self.centroid(s, c));
                if d < best_d {
                    best_d = d;
                    best = c;
                }
            }
            codes.push(best as u8);
        }
        Ok(codes)
    }
}

/// Squared-L2 distance between two equal-length slices (f64 accumulation).
/// `pub(crate)` so the Vamana builder shares one definition.
pub(crate) fn sq_l2(a: &[f32], b: &[f32]) -> f64 {
    let mut acc = 0.0f64;
    for (x, y) in a.iter().zip(b) {
        let d = *x as f64 - *y as f64;
        acc += d * d;
    }
    acc
}

/// A non-finite (`NaN` or `±inf`) f32 was about to enter an embedding — at write, at
/// query, into the RW delta index, or (as a read backstop) out of a codebook on disk.
///
/// This is not a nuisance: `f64::max` **returns the non-NaN operand**, so a NaN never
/// raises `max_norm` and every `is_infinite` overflow guard is structurally blind to it;
/// the augment coord `(M²−NaN).max(0.0).sqrt()` collapses to `0.0` while the NaN survives
/// verbatim in the copied raw coordinates, poisoning the k-means centroids and the exact
/// re-rank (a NaN is *ordered largest* by `total_cmp`, not rejected). So it has to be
/// refused at the boundary where an untrusted value first becomes vector data. Typed so
/// callers branch on the type, not the message text (house rule; HIK-134).
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error(
    "embedding component {index} is not finite ({value}); NaN and ±inf are not valid vector data"
)]
pub struct NonFiniteEmbedding {
    pub index: usize,
    pub value: f32,
}

/// The single finiteness gate shared by every embedding ingest, query, and read site
/// (HIK-134). Returns the value unchanged when finite, a typed [`NonFiniteEmbedding`]
/// otherwise. It **rejects, never coerces** — a silent `NaN`→0 or clamp would hide the
/// corrupt input this whole invariant exists to catch.
pub fn finite_f32(index: usize, value: f32) -> Result<f32, NonFiniteEmbedding> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(NonFiniteEmbedding { index, value })
    }
}

/// Reject any non-finite component of `v` — the slice-level counterpart of
/// [`finite_f32`], for the graph-format entry points that receive an already-materialised
/// `&[f32]` (the RW-index insert, the query transform, the augmentation, and the on-disk
/// codebook). Errors on the first offending component.
pub fn require_finite(v: &[f32]) -> Result<(), NonFiniteEmbedding> {
    for (i, &x) in v.iter().enumerate() {
        finite_f32(i, x)?;
    }
    Ok(())
}

/// The L2 norm `|v|`, accumulated in f64. The f64 accumulation is not incidental:
/// see [`normalise`].
pub fn l2_norm(v: &[f32]) -> f64 {
    v.iter()
        .map(|&x| (x as f64) * (x as f64))
        .sum::<f64>()
        .sqrt()
}

/// L2-normalise `v` to unit length — **the one definition of the cosine space (D29)**.
///
/// # The invariant
///
/// Every arm that produces a score consumed by `slater`'s `vector::merge_topk` must
/// normalise through *this* function. `merge_topk` folds the per-level neighbours of
/// several independent arms (base Vamana/PQ, brute-force, resident matrix, and the
/// FreshDiskANN RW index) into one global top-`k` by comparing their scores directly.
/// Two arms that normalise even slightly differently — a different zero-norm guard, a
/// different accumulation width — emit scores on subtly different scales, and the merge
/// then interleaves them *wrongly with no error and no panic*. Do not re-introduce a
/// local copy; that is precisely how this becomes a silent-wrong-answer bug.
///
/// # Zero norm
///
/// A zero vector has no direction, so it is returned **unchanged** (all-zero). That is
/// what makes the downstream contract hold: its dot product with any unit vector is 0,
/// hence cosine similarity 0 and cosine **distance 1** — the same value
/// `slater`'s `cosine_similarity`/`score_fast` define for a zero-norm operand, i.e.
/// maximally distant rather than `NaN`.
///
/// # Why the division is in f64
///
/// The obvious `let inv = (1.0 / norm) as f32; v.iter().map(|x| x * inv)` is *wrong* for
/// a small-but-nonzero norm: for `|v| < ~1.2e-38` (f32 min-normal) the reciprocal
/// overflows f32 to `+inf`, and every component becomes `inf`/`NaN`. A legal subnormal
/// f32 embedding such as `[1e-44, 0.0, 0.0]` hits it, and a `NaN` row is *silently*
/// mis-ordered in the top-`k` rather than rejected. Dividing in f64 and rounding once,
/// at the end, is exact for the same input.
pub fn normalise(v: &[f32]) -> Vec<f32> {
    let mut out = Vec::with_capacity(v.len());
    normalise_into(v, &mut out);
    out
}

/// [`normalise`], **appended** to `out` rather than freshly allocated — for callers
/// decoding many vectors into one contiguous row-major buffer (`ResidentMatrix`), which
/// must not allocate per row. Identical semantics, zero-norm contract included.
pub fn normalise_into(v: &[f32], out: &mut Vec<f32>) {
    let norm = l2_norm(v);
    if norm == 0.0 {
        out.extend_from_slice(v);
        return;
    }
    out.extend(v.iter().map(|&x| (x as f64 / norm) as f32));
}

// ── The ANN space: one transform, shared by the builder and the query path ─────
//
// DESIGN (v8): the Vamana graph is built with **squared-L2** and navigated by a PQ
// estimate of the same, because robust-prune's domination test (`alpha·d(p*,c) ≤ d(p,c)`)
// is only sound over a true metric. Not every index metric *is* one, so each is first
// mapped into a space where squared-L2 ranks the way the metric does. The three
// transforms live here, in **one** definition, for exactly the reason `normalise` does:
// the builder transforms the stored points and the query path transforms the query, and
// two arms that disagree about the space navigate by a quantity that is not the one they
// rank by — a quietly degraded recall with no error and no panic.
//
// The exact re-rank is untouched by all of this: it scores the **raw** stored vector
// against the **raw** query with the true metric (`slater::vector::distance`). The ANN
// space is a navigation device only.

/// Map a **stored** vector into the ANN space for `metric` — the space its PQ codes are
/// trained/encoded in and its Vamana edges are chosen in.
///
/// * **Cosine** — L2-normalise (D29). Squared-L2 on unit vectors is `2 − 2·cos`, monotone
///   in cosine distance.
/// * **L2** — identity. Squared-L2 *is* the metric.
/// * **Dot / MIPS** — the trap. Inner product is not a metric (no triangle inequality), so
///   robust-prune over raw dot is unsound and the PQ estimate of `‖q − x‖²` is not even
///   monotone in `⟨q, x⟩` (it carries a `‖x‖²` term that varies per candidate). The
///   standard fix is the **norm augmentation**: store `x' = [x, √(M² − ‖x‖²), 0…]` with
///   `M = max‖x‖` over the indexed set, and query with `q' = [q, 0, 0…]`. Then
///   `‖q' − x'‖² = ‖q‖² + M² − 2⟨q, x⟩`, which is a per-query **constant minus twice the
///   true dot** — so nearest-neighbour in L2 on `x'` is exactly maximum inner product on
///   `x`, and every Vamana/PQ primitive is reused unchanged over a genuine metric.
///
/// `space_dim` must be [`ann_pq_params`]'s `dim` for the same `(metric, dim, subspaces)`.
///
/// `max_norm` is only read for [`Metric::Dot`]. `M² − ‖x‖²` is `≥ 0` in exact arithmetic
/// (M is the maximum), but `M` round-trips through the manifest as an `f32` and can land a
/// hair below the f64 norm of the argmax vector, so the difference is clamped at zero —
/// which is that vector's true augmentation anyway. Without the clamp it is a `NaN`
/// coordinate, and a `NaN` is *ordered*, not rejected, by `total_cmp`.
pub fn ann_point(metric: Metric, v: &[f32], max_norm: f64, space_dim: usize) -> Result<Vec<f32>> {
    // Train-time backstop: the primary gate is at ingest, but a build reading vectors from a
    // dump (or a past-poisoned column) must not fold a non-finite component into the codebook.
    // The `max_norm`/`is_infinite` asserts below screen f32 *overflow* of M; they are
    // structurally blind to a per-component NaN, which survives verbatim into `v.to_vec()`.
    require_finite(v)?;
    match metric {
        Metric::Cosine => Ok(normalise(v)),
        Metric::L2 => Ok(v.to_vec()),
        Metric::Dot => {
            if space_dim <= v.len() {
                bail!(
                    "the dot/MIPS ANN space needs room for the norm augmentation: \
                     space_dim {space_dim} must exceed dim {}",
                    v.len()
                );
            }
            // `M` must be finite. It is `max‖x‖` accumulated in f64 and stored as an f32,
            // and a vector of large-but-perfectly-legal f32 components (1024 dimensions of
            // 3e38, say) overflows f32 to `+inf`. Every augmentation would then be `inf`,
            // every squared-L2 between two points `inf − inf` = **NaN** — and a NaN is
            // *ordered* by `total_cmp`, not rejected, so the graph build would silently
            // produce garbage. Refuse loudly instead.
            if !max_norm.is_finite() {
                bail!(
                    "the dot/MIPS augmentation needs a finite max norm, got {max_norm}: the \
                     indexed vectors' magnitudes overflow f32"
                );
            }
            let mut out = v.to_vec();
            out.resize(space_dim, 0.0);
            let norm = l2_norm(v);
            out[v.len()] = (max_norm * max_norm - norm * norm).max(0.0).sqrt() as f32;
            Ok(out)
        }
    }
}

/// Map a **query** vector into the ANN space for `metric` — the counterpart of
/// [`ann_point`], and the only transform the read path performs.
///
/// Note the asymmetry for [`Metric::Dot`]: the query's augmented coordinates are **zero**,
/// not `√(M² − ‖q‖²)`. That is what makes the augmentation work (it kills the cross term),
/// and it is why the read path never needs `M`.
pub fn ann_query(metric: Metric, q: &[f32], space_dim: usize) -> Result<Vec<f32>> {
    // The sharpest case (HIK-134): a NaN/±inf query needs no write at all and would return
    // `total_cmp`-ordered garbage against a completely clean index. Gate the query vector here
    // — the one transform the read path performs — so every metric (cosine/L2/dot) is covered,
    // including a Bolt-sent `Vector` param that bypassed the `vecf32`/`eval_query_vector` gate.
    require_finite(q)?;
    match metric {
        Metric::Cosine => Ok(normalise(q)),
        Metric::L2 => Ok(q.to_vec()),
        Metric::Dot => {
            if space_dim < q.len() {
                bail!(
                    "query dim {} exceeds the ANN space dim {space_dim}",
                    q.len()
                );
            }
            let mut out = q.to_vec();
            out.resize(space_dim, 0.0);
            Ok(out)
        }
    }
}

/// The PQ parameters for `metric`'s ANN space over `dim`-dimensional vectors, given the
/// index's configured `subspaces`/`bits`. `subspaces` must divide `dim` (the same gate the
/// builder already applies).
///
/// Cosine and L2 quantise the vector as-is, so the params are the caller's. Dot needs room
/// for the norm augmentation, and a single extra *coordinate* would leave the dimension
/// indivisible by `subspaces` for every realistic shape (`dim = 768, m = 8` ⇒ 769). So it
/// gets one extra **subspace** instead: `dim + dsub` over `m + 1` subspaces, which leaves
/// `dsub = dim/m` exactly as it was and needs no new divisibility rule. That last subspace
/// holds `[√(M² − ‖x‖²), 0, …, 0]` — the augmentation alone, quantised against its own `k`
/// centroids (effectively a 1-D codebook, so it is quantised finely), while the padding
/// zeros are the same in every point and in the query and so contribute exactly 0 to the
/// ADC.
pub fn ann_pq_params(metric: Metric, dim: u32, subspaces: u32, bits: u32) -> Result<PqParams> {
    match metric {
        Metric::Cosine | Metric::L2 => PqParams::new(dim, subspaces, bits),
        Metric::Dot => {
            if subspaces == 0 || dim == 0 || !dim.is_multiple_of(subspaces) {
                bail!("PQ subspaces ({subspaces}) must divide dim ({dim})");
            }
            PqParams::new(dim + dim / subspaces, subspaces + 1, bits)
        }
    }
}

/// Per-query ADC lookup table: `table[s*k + c]` is the squared-L2 distance from the
/// query's `s`-th sub-vector to subspace `s`'s centroid `c`. Estimating a
/// candidate's distance is then `m` adds — no access to the candidate's full vector.
pub struct AdcTable {
    table: Vec<f32>,
    m: usize,
    k: usize,
}

impl AdcTable {
    /// Build the table for `query` (which must already be normalised for a cosine
    /// index, matching how the codebook was trained — D29).
    pub fn new(codebook: &Codebook, query: &[f32]) -> Result<Self> {
        if query.len() != codebook.params.dim as usize {
            bail!(
                "query dim {} does not match codebook dim {}",
                query.len(),
                codebook.params.dim
            );
        }
        let m = codebook.params.subspaces as usize;
        let dsub = codebook.params.dsub as usize;
        let k = codebook.params.k as usize;
        let mut table = vec![0.0f32; m * k];
        for s in 0..m {
            let sub = &query[s * dsub..(s + 1) * dsub];
            for c in 0..k {
                table[s * k + c] = sq_l2(sub, codebook.centroid(s, c)) as f32;
            }
        }
        Ok(Self { table, m, k })
    }

    /// Estimated squared-L2 distance of the vector with these `m` codes.
    pub fn estimate(&self, codes: &[u8]) -> f32 {
        debug_assert_eq!(codes.len(), self.m);
        let mut acc = 0.0f32;
        for (s, &c) in codes.iter().enumerate() {
            acc += self.table[s * self.k + c as usize];
        }
        acc
    }
}

// ── k-means training ──────────────────────────────────────────────────────────

/// A tiny deterministic LCG (Numerical Recipes constants). The build must be
/// reproducible — same vectors in ⇒ same codebooks out — so k-means init uses this
/// rather than a system RNG, and there is no `rand` dependency in the tree.
/// `pub(crate)` so the Vamana builder shares one definition.
pub(crate) struct Lcg(pub(crate) u64);
impl Lcg {
    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    /// A float in `[0, 1)`.
    pub(crate) fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    pub(crate) fn next_below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Train PQ codebooks over `vectors` (each `dim`-long, expected already normalised
/// for a cosine index). `iters` Lloyd iterations per subspace. Deterministic.
pub fn train_codebooks(vectors: &[Vec<f32>], params: PqParams, iters: usize) -> Result<Codebook> {
    let m = params.subspaces as usize;
    let dsub = params.dsub as usize;
    let k = params.k as usize;
    for (i, v) in vectors.iter().enumerate() {
        if v.len() != params.dim as usize {
            bail!(
                "training vector {i} has dim {} but codebook dim is {}",
                v.len(),
                params.dim
            );
        }
    }
    let mut centroids = vec![0.0f32; m * k * dsub];
    // One LCG for the whole training run; seeded by a constant so the codebooks are
    // reproducible across builds of the same data.
    let mut rng = Lcg(0x5111_a7e1_5eed_1234);

    for s in 0..m {
        // Gather this subspace's sub-vectors as references (no copy).
        let subs: Vec<&[f32]> = vectors
            .iter()
            .map(|v| &v[s * dsub..(s + 1) * dsub])
            .collect();
        let cents = kmeans(&subs, k, dsub, iters, &mut rng);
        for (c, cent) in cents.iter().enumerate() {
            let base = (s * k + c) * dsub;
            centroids[base..base + dsub].copy_from_slice(cent);
        }
    }
    Ok(Codebook { params, centroids })
}

/// k-means++ initialisation + Lloyd iterations over `points` (each `dsub`-long).
/// Returns exactly `k` centroids; empty clusters are reseeded to a random point so
/// the codebook is always fully populated.
fn kmeans(points: &[&[f32]], k: usize, dsub: usize, iters: usize, rng: &mut Lcg) -> Vec<Vec<f32>> {
    let n = points.len();
    if n == 0 {
        return vec![vec![0.0f32; dsub]; k];
    }
    // k-means++ seeding: first centroid random, each subsequent chosen with
    // probability proportional to squared distance from the nearest chosen one.
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    centroids.push(points[rng.next_below(n)].to_vec());
    let mut nearest = vec![f64::INFINITY; n];
    while centroids.len() < k {
        let last = centroids.last().unwrap();
        let mut total = 0.0f64;
        for (i, p) in points.iter().enumerate() {
            let d = sq_l2(p, last);
            if d < nearest[i] {
                nearest[i] = d;
            }
            total += nearest[i];
        }
        if total <= 0.0 {
            // All remaining points coincide with a centroid — pad with copies.
            centroids.push(points[rng.next_below(n)].to_vec());
            continue;
        }
        let mut target = rng.next_f64() * total;
        let mut chosen = n - 1;
        for (i, &d) in nearest.iter().enumerate() {
            target -= d;
            if target <= 0.0 {
                chosen = i;
                break;
            }
        }
        centroids.push(points[chosen].to_vec());
    }

    // Lloyd iterations.
    let mut assign = vec![0usize; n];
    for _ in 0..iters.max(1) {
        let mut changed = false;
        for (i, p) in points.iter().enumerate() {
            let mut best = 0usize;
            let mut best_d = f64::INFINITY;
            for (c, cent) in centroids.iter().enumerate() {
                let d = sq_l2(p, cent);
                if d < best_d {
                    best_d = d;
                    best = c;
                }
            }
            if assign[i] != best {
                assign[i] = best;
                changed = true;
            }
        }
        // Recompute centroids as cluster means.
        let mut sums = vec![vec![0.0f64; dsub]; k];
        let mut counts = vec![0usize; k];
        for (i, p) in points.iter().enumerate() {
            let c = assign[i];
            counts[c] += 1;
            for (acc, &x) in sums[c].iter_mut().zip(p.iter()) {
                *acc += x as f64;
            }
        }
        for c in 0..k {
            if counts[c] == 0 {
                // Reseed an empty cluster to a random point so it stays useful.
                centroids[c] = points[rng.next_below(n)].to_vec();
            } else {
                for (d, acc) in centroids[c].iter_mut().zip(&sums[c]) {
                    *d = (*acc / counts[c] as f64) as f32;
                }
            }
        }
        if !changed {
            break;
        }
    }
    centroids
}

// ── `.pq` store (codebook + per-vector codes), block-file backed ───────────────
//
// Record 0 is the header+codebook; records 1..=count are one code record per
// vector (`uvarint(node_id) ‖ m × u8`). The file goes through the same blockfile
// seam as every other store, so it inherits zstd + the M6 AEAD for free (D28).

/// Writer for `vector/<l>.<p>.pq`.
pub struct PqWriter {
    inner: BlockFileWriter,
    m: usize,
}

impl PqWriter {
    /// Create the store and write the codebook header as record 0.
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        codebook: &Codebook,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let mut inner =
            BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
        let mut hdr = Vec::new();
        let p = codebook.params;
        write_uvarint(&mut hdr, p.dim as u64);
        write_uvarint(&mut hdr, p.subspaces as u64);
        write_uvarint(&mut hdr, p.dsub as u64);
        write_uvarint(&mut hdr, p.k as u64);
        for x in &codebook.centroids {
            hdr.write_f32::<LittleEndian>(*x)?;
        }
        inner.append_record(&hdr)?;
        Ok(Self {
            inner,
            m: p.subspaces as usize,
        })
    }

    /// Append one vector's codes (in vamana-index / layout order). `node_id` is the dense
    /// graph node this record maps to — or [`HOLE`] if the record is a tombstoned hole,
    /// which is navigable but never emitted.
    pub fn append_codes(&mut self, node_id: u64, codes: &[u8]) -> Result<()> {
        if codes.len() != self.m {
            bail!("expected {} codes, got {}", self.m, codes.len());
        }
        let mut rec = Vec::with_capacity(10 + self.m);
        write_uvarint(&mut rec, node_id);
        rec.extend_from_slice(codes);
        self.inner.append_record(&rec)?;
        Ok(())
    }

    pub fn finish(self) -> Result<u64> {
        self.inner.finish()
    }
}

/// The tombstone sentinel in a `.pq` node-id column: `node_ids[i] == HOLE` ⇒ layout
/// ordinal `i` is a **hole** — a record whose vector is deleted.
///
/// A hole is *never emitted* from a search but is *still navigated through*: it keeps its
/// out-edges and stays a waypoint, which is precisely the `emit → None` contract
/// `vamana::beam_search` already implements. Dropping it from the walk instead would
/// disconnect whatever lies behind it and silently cost recall on the **live** nodes.
///
/// Why holes and never compaction: a layout ordinal *is* a record position, and every
/// adjacency entry in every record is an ordinal. Compacting one deleted record shifts
/// every subsequent ordinal and invalidates the entire file — an O(N) rewrite of ~370 GB
/// to reclaim a few per cent. Freed slots are instead reused by later inserts.
///
/// `u64::MAX` is safe as a sentinel because it is not a reachable dense node id: ids are
/// assigned densely from 0 and the graph would need 2^64 nodes to reach it.
pub const HOLE: u64 = u64::MAX;

/// All of a PQ index's codes held **resident** — the navigation set for the beam
/// search — and, since v8, the **single** layout→id map for the index (the `.vamana`
/// record no longer carries one). `node_ids[i]` is the dense graph node for vamana index
/// `i`, or [`HOLE`] if that record is a tombstoned hole; its codes are
/// `codes[i*m .. i*m+m]`.
#[derive(Debug, Clone)]
pub struct ResidentPq {
    pub codebook: Codebook,
    pub node_ids: Vec<u64>,
    pub codes: Vec<u8>,
    pub m: usize,
}

impl ResidentPq {
    /// The `m` codes for vamana index `i`.
    pub fn codes_of(&self, i: usize) -> &[u8] {
        &self.codes[i * self.m..i * self.m + self.m]
    }

    /// Number of **records** — holes included. This is the bound on a valid layout
    /// ordinal, so it (never [`Self::live_count`]) is what bounds-checks a neighbour id.
    pub fn len(&self) -> usize {
        self.node_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.node_ids.is_empty()
    }

    /// Whether layout ordinal `i` is a tombstoned [`HOLE`]. Out-of-range ⇒ `false`: a
    /// forged neighbour ordinal is rejected by the search's `num_nodes` bound, not here.
    pub fn is_hole(&self, i: usize) -> bool {
        self.node_ids.get(i).copied() == Some(HOLE)
    }

    /// Number of records that are **not** holes — the emitted-eligible count.
    pub fn live_count(&self) -> usize {
        self.node_ids.iter().filter(|&&id| id != HOLE).count()
    }

    /// Approximate resident footprint in bytes (codes + node-id table + codebook).
    pub fn resident_bytes(&self) -> usize {
        self.codes.len() + self.node_ids.len() * 8 + self.codebook.centroids.len() * 4
    }
}

/// Reader for `vector/<l>.<p>.pq`.
pub struct PqReader {
    inner: BlockFileReader,
    codebook: Codebook,
}

impl PqReader {
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
        let hdr = inner
            .read_record_global(0)
            .context("read PQ codebook header (record 0)")?;
        let mut r = &hdr[..];
        let dim = read_uvarint(&mut r)? as u32;
        let subspaces = read_uvarint(&mut r)? as u32;
        let dsub = read_uvarint(&mut r)? as u32;
        let k = read_uvarint(&mut r)? as u32;
        // The four header fields are untrusted on-disk uvarints, and the codebook size is
        // their *product*: `subspaces * k * dsub` was computed in `u32`, so it wrapped — a
        // forged header could name a small `n` and a `dim`/`k` the rest of the reader then
        // used at full width, or (in a debug build) simply panic on the overflow.
        //
        // Re-derive the params through the constructor that the writer used, so the invariants
        // that make the product meaningful (`dsub == dim / subspaces`, `k = 2^bits` with
        // `bits` in `1..=8`) are re-checked against the image rather than assumed.
        if !k.is_power_of_two() {
            bail!("PQ codebook header: k ({k}) is not a power of two");
        }
        let params = PqParams::new(dim, subspaces, k.trailing_zeros())
            .context("PQ codebook header failed validation")?;
        if params.dsub != dsub || params.k != k {
            bail!(
                "PQ codebook header is inconsistent: dsub={dsub}, k={k}, but dim={dim} over \
                 {subspaces} subspaces implies dsub={}, k={}",
                params.dsub,
                params.k
            );
        }
        // Even validated, `dim` is an unbounded `u32`, so the product still needs a checked
        // multiply and the reservation still needs clamping by the bytes actually present
        // (4 per `f32`) — the loop below errors on the first short read.
        let n = checked_span("PQ codebook", subspaces as u64 * k as u64, dsub as usize)?;
        let mut centroids = Vec::with_capacity(capacity_for(n, r.len(), 4));
        for _ in 0..n {
            centroids.push(r.read_f32::<LittleEndian>()?);
        }
        // Read backstop (HIK-134): the ingest gate keeps a live build's centroids finite, but a
        // bit-rotted image or a codebook trained by a *past* build (before the gate existed) can
        // still carry a NaN/±inf centroid, which a query would then score by `total_cmp`. Refuse it
        // at open rather than serve silent garbage. The primary fix is at ingest; this is defence.
        require_finite(&centroids)?;
        Ok(Self {
            inner,
            codebook: Codebook { params, centroids },
        })
    }

    pub fn params(&self) -> PqParams {
        self.codebook.params
    }

    /// Load every code record (records `1..total`) into one resident structure.
    /// Reads block-by-block so each block is decompressed exactly once.
    pub fn load_resident(&self) -> Result<ResidentPq> {
        let m = self.codebook.params.subspaces as usize;
        // Validated against the manifest descriptor by `generation.rs`, and re-derived
        // through `PqParams::new` when the header was parsed — so `k` is a trustworthy
        // bound to check the untrusted code bytes against.
        let k = self.codebook.params.k;
        let total = self.inner.total_records();
        // `total` is the block directory's record count — an on-disk number, not a count
        // backed by a buffer we hold, and `n * m` would wrap. Reserve a bounded prefix and
        // let the `Vec`s grow as the blocks are actually read (`wire::capacity_hint`).
        let records = total.saturating_sub(1) as usize;
        let mut node_ids = Vec::with_capacity(capacity_hint(records));
        let mut codes = Vec::with_capacity(capacity_hint(records.saturating_mul(m)));
        let mut global: u64 = 0;
        // Whole-file load via a bounded concurrent read-ahead, so a remote backend
        // overlaps its fetch round-trips at generation open without holding more
        // than the read-ahead window resident (no-op fan-out on a local file).
        self.inner.for_each_block(|_bi, raw| {
            let (offsets, data) = parse_block(raw)?;
            for slot in 0..offsets.len().saturating_sub(1) {
                // Skip record 0 (the codebook header).
                if global == 0 {
                    global += 1;
                    continue;
                }
                // `parse_block` has already validated the table against `data`, so this cannot
                // fail — but say it through the shared slicing path rather than re-deriving the
                // bounds by hand, which is how this site came to skip the check in the first place.
                let rec = record_from_block(&offsets, data, slot as u32)?;
                let mut rr = rec;
                node_ids.push(read_uvarint(&mut rr)?);
                if rr.len() != m {
                    bail!("PQ code record has {} bytes, expected {m}", rr.len());
                }
                let mut buf = vec![0u8; m];
                rr.read_exact(&mut buf)?;
                // The count above says there are `m` bytes; it says nothing about their
                // *values*. Each is a centroid index that `AdcTable::estimate` uses to index
                // an `m * k` table with no bounds branch — deliberately, the scoring loop is
                // hot — so `c >= k` is an out-of-bounds panic inside beam search, on the
                // query path. Validate here, the sole point where these bytes enter memory,
                // so the resident structure carries the invariant and the loop stays
                // branch-free (HIK-133).
                if let Some(&c) = buf.iter().find(|&&c| u32::from(c) >= k) {
                    return Err(DecodeRejected::PqCodeOutOfRange {
                        what: "PQ code record",
                        code: c,
                        k,
                    }
                    .into());
                }
                codes.extend_from_slice(&buf);
                global += 1;
            }
            Ok(())
        })?;
        Ok(ResidentPq {
            codebook: self.codebook.clone(),
            node_ids,
            codes,
            m,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::blockfile::BlockCodec;
    use crate::wire::DecodeRejected;

    /// A 4-dim / 2-subspace / 1-bit codebook: `subspaces*k*dsub = 8` centroids.
    fn tiny_codebook() -> Codebook {
        Codebook {
            params: PqParams::new(4, 2, 1).unwrap(),
            centroids: (0..8).map(|i| i as f32).collect(),
        }
    }

    /// Byte image of the codebook record, exactly as [`PqWriter::create_with_cipher`]
    /// lays record 0 out. Hand-written because the forging test below needs the `Raw`
    /// codec, which `PqWriter` does not expose; if the header layout ever diverges,
    /// `PqReader::open` stops parsing it and this test fails loudly rather than rotting.
    fn codebook_record(cb: &Codebook) -> Vec<u8> {
        let mut hdr = Vec::new();
        let p = cb.params;
        write_uvarint(&mut hdr, p.dim as u64);
        write_uvarint(&mut hdr, p.subspaces as u64);
        write_uvarint(&mut hdr, p.dsub as u64);
        write_uvarint(&mut hdr, p.k as u64);
        for x in &cb.centroids {
            hdr.write_f32::<LittleEndian>(*x).unwrap();
        }
        hdr
    }

    /// Offset of the slot-offset table of the block starting at `off`, and the block's
    /// total length. The image is self-describing (`count ‖ offsets ‖ data`), so this
    /// walks it without needing the file's private directory.
    fn block_table(bytes: &[u8], off: usize) -> (usize, usize) {
        let count = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        let table = off + 4;
        let last = table + count * 4;
        let data_len = u32::from_le_bytes(bytes[last..last + 4].try_into().unwrap()) as usize;
        (table, 4 + (count + 1) * 4 + data_len)
    }

    /// Write a `.pq` whose block 0 is the codebook alone and whose block 1 holds three
    /// code records, then rewrite block 1's offset table to `bad` and hand the result to
    /// `load_resident`. `Raw` + no cipher means blocks are stored verbatim, so the patch
    /// is a byte poke rather than a re-seal (the validated table is post-decode, so the
    /// codec is irrelevant to what is under test).
    fn load_resident_with_forged_offsets(name: &str, bad: &[u32]) -> anyhow::Error {
        let path = std::env::temp_dir().join(format!("slater_pq_{}_{name}", std::process::id()));
        let cb = tiny_codebook();
        let hdr = codebook_record(&cb);
        // `append_record` flushes once `cur_data.len() >= target`: the 36-byte header trips
        // it immediately (block 0), while the three 3-byte code records do not (block 1).
        let mut w = crate::blockfile::BlockFileWriter::create_with_codec(
            &path,
            20,
            BlockCodec::Raw,
            0,
            None,
        )
        .unwrap();
        w.append_record(&hdr).unwrap();
        for id in 1..=3u64 {
            let mut rec = Vec::new();
            write_uvarint(&mut rec, id);
            rec.extend_from_slice(&[0u8, 1]); // m = 2 codes
            w.append_record(&rec).unwrap();
        }
        assert_eq!(
            w.finish().unwrap(),
            2,
            "expected a codebook block + one code block"
        );

        let mut bytes = std::fs::read(&path).unwrap();
        let (_, len0) = block_table(&bytes, 8); // MAGIC(8) ‖ block_0 ‖ block_1 ‖ …
        let (table1, _) = block_table(&bytes, 8 + len0);
        assert_eq!(
            u32::from_le_bytes(bytes[8 + len0..12 + len0].try_into().unwrap()),
            3,
            "block 1 must hold the three code records"
        );
        for (i, o) in bad.iter().enumerate() {
            bytes[table1 + i * 4..table1 + i * 4 + 4].copy_from_slice(&o.to_le_bytes());
        }
        std::fs::write(&path, &bytes).unwrap();

        // Opening still works: it reads record 0 out of the untouched block 0. The forged
        // block is only reached by the resident load — which is the reported hazard.
        let r = PqReader::open_with_cipher(&path, None).expect("open reads only block 0");
        let err = r
            .load_resident()
            .expect_err("a forged offset table must be refused, not panicked on");
        let _ = std::fs::remove_file(&path);
        err
    }

    /// **HIK-128.** `load_resident` used to slice `data[offsets[slot]..offsets[slot+1]]`
    /// straight off an unvalidated on-disk table, so a corrupt/forged `.pq` block panicked
    /// (slice out of bounds / `start > end`) at generation open where a clean error was
    /// available. Both records preceding the forged slot are well-formed, so pre-fix these
    /// reach the bad slice and panic rather than erroring earlier for another reason.
    #[test]
    fn forged_pq_offsets_error_not_panic() {
        // (a) an offset past the end of the data region.
        let err = load_resident_with_forged_offsets("overrun", &[0, 3, 6, 999]);
        assert!(
            matches!(
                err.downcast_ref::<DecodeRejected>(),
                Some(DecodeRejected::BlockOffsetTable { .. })
            ),
            "expected a typed BlockOffsetTable rejection, got: {err:#}"
        );
        // (b) `start > end`.
        let err = load_resident_with_forged_offsets("decrease", &[0, 3, 2, 9]);
        assert!(
            matches!(
                err.downcast_ref::<DecodeRejected>(),
                Some(DecodeRejected::BlockOffsetTable { .. })
            ),
            "expected a typed BlockOffsetTable rejection, got: {err:#}"
        );
    }

    /// Write a `.pq` whose block 0 is the codebook and whose block 1 holds one code record
    /// per entry of `codes`, each record laid out exactly as [`PqWriter::append_codes`] does
    /// (`uvarint(node_id) ‖ m × u8`). Built through `BlockFileWriter` rather than `PqWriter`
    /// so the record bytes are hand-placed: this is a *forged image*, and it must not depend
    /// on what the writer would or would not have emitted.
    fn write_pq_with_code_records(name: &str, codes: &[[u8; 2]]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("slater_pq_{}_{name}", std::process::id()));
        let hdr = codebook_record(&tiny_codebook());
        let mut w = crate::blockfile::BlockFileWriter::create_with_codec(
            &path,
            20,
            BlockCodec::Raw,
            0,
            None,
        )
        .unwrap();
        w.append_record(&hdr).unwrap();
        for (i, c) in codes.iter().enumerate() {
            let mut rec = Vec::new();
            write_uvarint(&mut rec, (i + 1) as u64);
            rec.extend_from_slice(c);
            w.append_record(&rec).unwrap();
        }
        w.finish().unwrap();
        path
    }

    /// **HIK-133.** `load_resident` checked only the *count* of a record's code bytes
    /// (`rr.len() != m`), never their *values*. A code byte is a centroid index, and
    /// [`AdcTable::estimate`] uses it to index an `m * k` table with no bounds branch — so
    /// any `c >= k` reads out of bounds and panics *inside query execution* (the
    /// `segvamana` beam search), not merely at open.
    ///
    /// **The non-default `bits` is load-bearing.** `pq_bits` defaults to 8 ⇒ `k = 256` ⇒
    /// every `u8` is a valid index and no failing input exists at all. `tiny_codebook` is
    /// `bits = 1` ⇒ `k = 2`, which is what makes 200 out of range.
    ///
    /// The record is deliberately **correctly sized** (`m = 2` bytes): a wrong-length record
    /// would trip the pre-existing `rr.len() != m` count check and fail pre-fix for the
    /// wrong reason, pinning nothing about the value check this test exists for.
    #[test]
    fn forged_pq_code_byte_above_k_errors_not_panics() {
        let p = tiny_codebook().params;
        assert_eq!(
            (p.subspaces, p.k),
            (2, 2),
            "premise: non-default bits ⇒ k = 2, so a code byte of 200 is out of range \
             (at the default k = 256 it would be perfectly valid)"
        );

        let path = write_pq_with_code_records("code_oob", &[[0, 1], [0, 200]]);
        // Opening still works: it reads record 0 out of the untouched codebook block. The
        // bad byte is only reached by the resident load.
        let r = PqReader::open_with_cipher(&path, None).expect("open reads only block 0");
        let err = r
            .load_resident()
            .expect_err("a code byte >= k must be refused, not carried into the scoring loop");
        let _ = std::fs::remove_file(&path);

        match err.downcast_ref::<DecodeRejected>() {
            Some(DecodeRejected::PqCodeOutOfRange { code, k, .. }) => {
                assert_eq!(
                    (*code, *k),
                    (200, 2),
                    "the rejection must name the offending byte and the bound it broke"
                );
            }
            _ => panic!("expected a typed PqCodeOutOfRange rejection, got: {err:#}"),
        }
    }

    /// The premise of the guard above: an out-of-range code byte really does panic in the
    /// scoring loop, so rejecting it at load is not defending against nothing. `estimate`
    /// stays branch-free by design — the invariant is upheld upstream, not here.
    ///
    /// This is a plain slice bounds check, not the `debug_assert_eq!` on `codes.len()` above
    /// it, so it panics in **release** as well — the hazard is not a debug-only one.
    #[test]
    #[should_panic(expected = "out of bounds")]
    fn estimate_panics_on_a_code_byte_above_k() {
        let cb = tiny_codebook(); // dim 4, m 2, k 2 ⇒ a 4-entry ADC table
        let adc = AdcTable::new(&cb, &[0.0, 0.0, 0.0, 0.0]).unwrap();
        // Correctly sized (m = 2), so the length `debug_assert` is satisfied and this is
        // purely the *value* going out of range.
        adc.estimate(&[0, 200]);
    }

    /// The guard is not a no-op: an untampered `.pq` written the same way still loads, and
    /// returns exactly the records that went in.
    #[test]
    fn well_formed_pq_still_loads() {
        let path = std::env::temp_dir().join(format!("slater_pq_{}_ok", std::process::id()));
        let cb = tiny_codebook();
        let mut w = PqWriter::create_with_cipher(&path, &cb, 20, 0, None).unwrap();
        w.append_codes(7, &[0, 1]).unwrap();
        w.append_codes(9, &[1, 0]).unwrap();
        w.finish().unwrap();

        let rp = PqReader::open_with_cipher(&path, None)
            .unwrap()
            .load_resident()
            .unwrap();
        assert_eq!(rp.node_ids, vec![7, 9]);
        assert_eq!(rp.codes_of(0), &[0, 1]);
        assert_eq!(rp.codes_of(1), &[1, 0]);
        let _ = std::fs::remove_file(&path);
    }

    /// The 3-4-5 triangle: `|(3,4)| = 5`, so the unit vector is exactly `(0.6, 0.8)`
    /// — both are exactly representable in f32, so this is an equality, not an epsilon.
    #[test]
    fn normalise_matches_hand_computation() {
        assert_eq!(normalise(&[3.0, 4.0]), vec![0.6, 0.8]);
        assert_eq!(normalise(&[-3.0, 4.0]), vec![-0.6, 0.8]);
        // Already unit → unchanged; scale-invariance: 10x the input, same output.
        assert_eq!(normalise(&[0.0, 1.0]), vec![0.0, 1.0]);
        assert_eq!(normalise(&[30.0, 40.0]), vec![0.6, 0.8]);
    }

    /// **The zero-norm contract** (the `slater` side of it is pinned in
    /// `vector::zero_norm_vector_is_maximally_distant`): a zero vector has no
    /// direction, so it is returned unchanged rather than becoming `NaN`. Its dot
    /// product with any unit row is then 0 ⇒ cosine similarity 0 ⇒ **distance 1**.
    #[test]
    fn normalise_leaves_a_zero_vector_alone() {
        let z = normalise(&[0.0, 0.0, 0.0]);
        assert_eq!(z, vec![0.0, 0.0, 0.0]);
        assert!(
            z.iter().all(|x| x.is_finite()),
            "a zero vector must not be NaN"
        );
        // The consequence the read path depends on: dot(zero, unit) == 0 ⇒ dist 1.
        let unit = normalise(&[3.0, 4.0, 0.0]);
        let dot: f32 = z.iter().zip(&unit).map(|(a, b)| a * b).sum();
        assert_eq!(1.0 - dot, 1.0);
        // -0.0 is still a zero norm (`sqrt(0.0) == 0.0`), so it must not divide either.
        assert!(normalise(&[-0.0, 0.0]).iter().all(|x| x.is_finite()));
    }

    /// A **subnormal** norm must still normalise to a finite unit vector. The naive
    /// `let inv = (1.0 / norm) as f32; x * inv` overflows f32 here (`1/1e-44 ≈ 1e44`
    /// → `+inf`), turning every component into `inf`/`NaN` — which a top-k orders by
    /// `total_cmp` and so *silently* mis-ranks rather than rejecting. The zero guard
    /// does not catch it: the norm is small, not zero.
    #[test]
    fn normalise_survives_a_subnormal_norm() {
        let tiny = 1e-44f32; // subnormal, but not zero
        assert!(tiny > 0.0 && tiny.is_finite());
        assert!(
            ((1.0f64 / l2_norm(&[tiny, 0.0])) as f32).is_infinite(),
            "premise: the f32 reciprocal of this norm really does overflow"
        );

        let u = normalise(&[tiny, 0.0]);
        assert!(u.iter().all(|x| x.is_finite()), "got {u:?}");
        assert_eq!(u, vec![1.0, 0.0]);

        // And in the general (non-axis-aligned) case it is still unit length.
        let u = normalise(&[3.0 * tiny, 4.0 * tiny]);
        assert!(u.iter().all(|x| x.is_finite()), "got {u:?}");
        assert!((l2_norm(&u) - 1.0).abs() < 1e-6, "|u| = {}", l2_norm(&u));
    }

    /// `normalise_into` appends (it is how `ResidentMatrix` fills one contiguous
    /// row-major buffer without a per-row allocation) and never disturbs what is
    /// already in the buffer.
    #[test]
    fn normalise_into_appends_without_clobbering() {
        let mut buf = vec![7.0f32, 7.0];
        normalise_into(&[3.0, 4.0], &mut buf);
        normalise_into(&[0.0, 0.0], &mut buf);
        assert_eq!(buf, vec![7.0, 7.0, 0.6, 0.8, 0.0, 0.0]);
    }

    /// `l2_norm` is the same f64 accumulation `normalise` divides by — hand-checked.
    #[test]
    fn l2_norm_matches_hand_computation() {
        assert_eq!(l2_norm(&[3.0, 4.0]), 5.0);
        assert_eq!(l2_norm(&[0.0, 0.0]), 0.0);
        assert!((l2_norm(&[1.0, 2.0, 3.0]) - 14.0f64.sqrt()).abs() < 1e-12);
    }

    // ── The ANN space ────────────────────────────────────────────────────────────

    /// Dot's ANN space adds one **subspace** (not one coordinate): a single extra
    /// coordinate would leave `dim + 1` indivisible by `subspaces` for every realistic
    /// shape, and the index would fall back to brute force. `dsub` must come out unchanged.
    #[test]
    fn ann_pq_params_adds_one_subspace_for_dot_and_keeps_dsub() {
        // The realistic shape: 768-dim embeddings, 8 subspaces. `dim + 1 = 769` is prime.
        assert!(
            !769u32.is_multiple_of(8),
            "premise: a bare +1 would not divide"
        );
        let p = ann_pq_params(Metric::Dot, 768, 8, 8).unwrap();
        assert_eq!((p.dim, p.subspaces, p.dsub), (768 + 96, 9, 96));
        // Cosine and L2 quantise the vector as-is.
        for m in [Metric::Cosine, Metric::L2] {
            let p = ann_pq_params(m, 768, 8, 8).unwrap();
            assert_eq!((p.dim, p.subspaces, p.dsub), (768, 8, 96));
        }
        // The divisibility gate is the same one for all three.
        assert!(ann_pq_params(Metric::Dot, 10, 3, 8).is_err());
    }

    /// **The one place a subtle maths error hides behind plausible-looking recall.**
    ///
    /// The MIPS→L2 norm augmentation claims that ranking by squared-L2 in the augmented
    /// space *is* ranking by the true inner product, reversed. If it is only *correlated*
    /// with it, recall against a dot-product ground truth still looks respectable and
    /// nothing anywhere errors — the index just quietly returns the wrong neighbours.
    ///
    /// So check the claim itself, not a recall proxy. Two assertions, both against
    /// hand-derived truth:
    ///  1. the identity `‖q′ − x′‖² = ‖q‖² + M² − 2⟨q, x⟩` — the augmented distance is a
    ///     per-query **constant minus twice the true dot**, so the `‖x‖²` term that makes
    ///     plain L2 useless for MIPS is exactly cancelled;
    ///  2. therefore, for every pair `(x, y)`, `augL2(x) < augL2(y)` **iff**
    ///     `⟨q,x⟩ > ⟨q,y⟩`. Exhaustive over every pair of a random set, with vectors of
    ///     deliberately *unequal* norms — the case where a scale-invariant (cosine-shaped)
    ///     mistake would rank correctly by direction and wrongly by magnitude.
    #[test]
    fn dot_augmentation_ranks_by_the_true_inner_product() {
        let dim = 12;
        let n = 60;
        let mut rng = Lcg(0x5111_a7e1_d070_0001);
        // Wildly varying magnitudes: |v| spans ~0.1 to ~10. If the transform were secretly
        // ranking by direction alone, these are what expose it.
        let raw: Vec<Vec<f32>> = (0..n)
            .map(|_| {
                let scale = 0.1 + 10.0 * rng.next_f64();
                (0..dim)
                    .map(|_| ((rng.next_f64() - 0.5) * scale) as f32)
                    .collect()
            })
            .collect();
        let max_norm = raw.iter().map(|v| l2_norm(v)).fold(0.0f64, f64::max);
        let params = ann_pq_params(Metric::Dot, dim as u32, 4, 8).unwrap();
        let space_dim = params.dim as usize;

        let dot = |a: &[f32], b: &[f32]| -> f64 {
            a.iter()
                .zip(b)
                .map(|(x, y)| *x as f64 * *y as f64)
                .sum::<f64>()
        };

        let points: Vec<Vec<f32>> = raw
            .iter()
            .map(|v| ann_point(Metric::Dot, v, max_norm, space_dim).unwrap())
            .collect();

        // Every augmented point is exactly M long — that is what the augmentation *is*
        // (it lifts every point onto the sphere of radius M), and it is why the cross term
        // vanishes. A NaN from an unclamped sqrt would fail here first.
        for p in &points {
            assert!(
                (l2_norm(p) - max_norm).abs() < 1e-4,
                "augmented norm {} != M {max_norm}",
                l2_norm(p)
            );
        }

        for qi in 0..8 {
            let q = &raw[qi * 7 % n];
            let qa = ann_query(Metric::Dot, q, space_dim).unwrap();
            let qn2 = l2_norm(q).powi(2);

            // (1) The identity.
            for (x, xa) in raw.iter().zip(&points) {
                let aug = sq_l2(&qa, xa);
                let predicted = qn2 + max_norm * max_norm - 2.0 * dot(q, x);
                assert!(
                    (aug - predicted).abs() < 1e-3 * predicted.abs().max(1.0),
                    "‖q'-x'‖² = {aug} but ‖q‖² + M² − 2⟨q,x⟩ = {predicted}"
                );
            }

            // (2) The consequence: the augmented-L2 order IS the reversed true-dot order.
            for i in 0..n {
                for j in (i + 1)..n {
                    let (di, dj) = (dot(q, &raw[i]), dot(q, &raw[j]));
                    let (ai, aj) = (sq_l2(&qa, &points[i]), sq_l2(&qa, &points[j]));
                    // Skip near-ties, where f32 storage noise can legitimately flip an
                    // order the f64 truth calls a hair apart.
                    if (di - dj).abs() < 1e-4 {
                        continue;
                    }
                    assert_eq!(
                        ai < aj,
                        di > dj,
                        "augmented L2 ranked {i} vs {j} the wrong way: augL2 {ai} vs {aj}, \
                         true dot {di} vs {dj}"
                    );
                }
            }
        }
    }

    /// The query is augmented with **zeros**, not with `√(M² − ‖q‖²)`. That asymmetry is
    /// the whole trick — it is what kills the cross term — and it is why the read path
    /// never needs `M`. Augmenting the query like a point would reintroduce a
    /// query-dependent term and break the ranking, so pin the shape.
    #[test]
    fn ann_query_pads_dot_with_zeros_and_leaves_the_others_alone() {
        let params = ann_pq_params(Metric::Dot, 4, 2, 8).unwrap();
        let space_dim = params.dim as usize; // 4 + 2 = 6
        assert_eq!(space_dim, 6);
        let q = ann_query(Metric::Dot, &[1.0, 2.0, 3.0, 4.0], space_dim).unwrap();
        assert_eq!(q, vec![1.0, 2.0, 3.0, 4.0, 0.0, 0.0]);

        // A *point* with the same coordinates gets the augmentation in slot `dim`, and
        // zeros only in the padding.
        let m = 10.0f64;
        let p = ann_point(Metric::Dot, &[1.0, 2.0, 3.0, 4.0], m, space_dim).unwrap();
        let norm2 = 1.0f64 + 4.0 + 9.0 + 16.0;
        assert_eq!(p[..4], [1.0, 2.0, 3.0, 4.0]);
        assert!((p[4] as f64 - (m * m - norm2).sqrt()).abs() < 1e-4);
        assert_eq!(p[5], 0.0);

        // Cosine normalises both sides; L2 is the identity on both.
        assert_eq!(
            ann_query(Metric::Cosine, &[3.0, 4.0], 2).unwrap(),
            [0.6, 0.8]
        );
        assert_eq!(
            ann_point(Metric::Cosine, &[3.0, 4.0], 5.0, 2).unwrap(),
            [0.6, 0.8]
        );
        assert_eq!(ann_query(Metric::L2, &[3.0, 4.0], 2).unwrap(), [3.0, 4.0]);
        assert_eq!(
            ann_point(Metric::L2, &[3.0, 4.0], 5.0, 2).unwrap(),
            [3.0, 4.0]
        );
    }

    /// `M` round-trips through the MANIFEST as an `f32`, so for the argmax vector the
    /// stored `M` can land a hair *below* its true f64 norm and `M² − ‖x‖²` goes negative.
    /// Unclamped that is `sqrt(-ε)` = **NaN**, and a NaN coordinate is *ordered* by
    /// `total_cmp`, not rejected — it would sink into the results as a plausible-looking
    /// distance. The clamp yields 0.0, which is that vector's true augmentation anyway.
    #[test]
    fn dot_augmentation_clamps_the_argmax_vector_instead_of_producing_nan() {
        let v = vec![0.1f32, 0.7, 0.3, 0.5];
        let true_norm = l2_norm(&v);
        // Exactly what the builder does: compute M in f64, store f32, read back f32.
        let m_stored = (true_norm as f32) as f64;
        assert!(
            m_stored < true_norm,
            "premise: this vector's f32 norm really does round below its f64 norm \
             ({m_stored} vs {true_norm})"
        );
        let p = ann_point(Metric::Dot, &v, m_stored, 6).unwrap();
        assert!(p.iter().all(|x| x.is_finite()), "got {p:?}");
        assert_eq!(p[4], 0.0, "the argmax vector's augmentation is exactly 0");
    }

    /// An `M` that overflows f32 must be **refused**, not propagated. `M` is `max‖x‖` in
    /// f64 but is stored as an f32, and a vector of large-but-legal f32 components
    /// overflows it to `+inf` — after which every augmented point is `inf`, every pairwise
    /// squared-L2 is `inf − inf` = **NaN**, and `total_cmp` *orders* NaNs rather than
    /// rejecting them. The graph build would run to completion over garbage geometry and
    /// report nothing wrong.
    #[test]
    fn dot_augmentation_refuses_a_max_norm_that_overflows_f32() {
        // 1024 dimensions of 3e38 — every component a finite, legal f32.
        let v = vec![3.0e38f32; 1024];
        assert!(v.iter().all(|x| x.is_finite()), "premise: legal f32 input");
        let m = l2_norm(&v);
        assert!(
            m.is_finite(),
            "the f64 norm is fine — it is the f32 that is not"
        );
        assert!(
            (m as f32).is_infinite(),
            "premise: this norm really does overflow f32"
        );

        let err = ann_point(Metric::Dot, &v, m as f32 as f64, 1024 + 128).unwrap_err();
        assert!(
            err.to_string().contains("finite max norm"),
            "expected a refusal, got: {err}"
        );

        // And the failure it prevents: an infinite M silently yields NaN geometry.
        let bad = {
            let mut out = v.clone();
            out.resize(1024 + 128, 0.0);
            out[1024] = (f64::INFINITY - m * m).max(0.0).sqrt() as f32;
            out
        };
        assert!(
            sq_l2(&bad, &bad).is_nan() || bad[1024].is_infinite(),
            "premise: this is what an unguarded infinite M produces"
        );
    }

    #[test]
    fn finite_gate_rejects_nan_and_both_infinities() {
        // The one shared decision function: finite passes through unchanged; NaN and ±inf are
        // typed errors carrying the offending index (HIK-134). It rejects, never coerces.
        assert_eq!(finite_f32(0, 1.5).unwrap(), 1.5);
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert_eq!(finite_f32(2, bad).unwrap_err().index, 2);
        }
        assert!(require_finite(&[1.0, 2.0, 3.0]).is_ok());
        assert_eq!(require_finite(&[1.0, f32::NAN, 3.0]).unwrap_err().index, 1);
    }

    #[test]
    fn ann_query_rejects_a_nonfinite_query_for_every_metric() {
        // The sharpest case: a NaN/±inf QUERY needs no write and would otherwise return
        // `total_cmp`-ordered garbage against a clean index. The query transform is the one
        // read-path transform, so gating it here covers every metric.
        for metric in [Metric::Cosine, Metric::L2, Metric::Dot] {
            let space_dim = match metric {
                Metric::Dot => 6, // room for the augmentation
                _ => 4,
            };
            for bad in [
                vec![f32::NAN, 0.2, 0.3, 0.4],
                vec![0.1, f32::INFINITY, 0.3, 0.4],
                vec![0.1, 0.2, 0.3, f32::NEG_INFINITY],
            ] {
                let err = ann_query(metric, &bad, space_dim).unwrap_err();
                assert!(
                    err.downcast_ref::<NonFiniteEmbedding>().is_some(),
                    "{metric:?}: must be the typed finiteness error, got: {err}"
                );
            }
            // A finite query of the same shape still transforms cleanly.
            assert!(ann_query(metric, &[0.1, 0.2, 0.3, 0.4], space_dim).is_ok());
        }
    }

    #[test]
    fn ann_point_rejects_a_nonfinite_component_before_augmentation() {
        // Train-time backstop: distinct from the max-norm overflow guard, which is blind to a
        // per-component NaN (it survives `f64::max` and rides verbatim into `v.to_vec()`).
        for metric in [Metric::Cosine, Metric::L2, Metric::Dot] {
            let space_dim = if metric == Metric::Dot { 6 } else { 4 };
            let err = ann_point(metric, &[f32::NAN, 0.2, 0.3, 0.4], 1.0, space_dim).unwrap_err();
            assert!(
                err.downcast_ref::<NonFiniteEmbedding>().is_some(),
                "{metric:?}: must be the typed finiteness error, got: {err}"
            );
        }
    }

    #[test]
    fn open_src_rejects_a_codebook_with_a_nonfinite_centroid() {
        // Read backstop: a bit-rotted image, or a codebook trained by a *past* build before
        // the ingest gate existed, can carry a NaN centroid that a query would score by
        // `total_cmp`. `open` must refuse it rather than serve silent garbage.
        let dim = 8;
        let data = clustered(dim, 3, 8);
        let params = PqParams::new(dim as u32, 2, 4).unwrap();
        let mut cb = train_codebooks(&data, params, 15).unwrap();
        cb.centroids[0] = f32::NAN; // poison one centroid

        let path =
            std::env::temp_dir().join(format!("slater_pq_{}_{}", std::process::id(), "nanc"));
        let mut w = PqWriter::create_with_cipher(&path, &cb, 4096, 3, None).unwrap();
        w.append_codes(0, &cb.encode(&data[0]).unwrap()).unwrap();
        w.finish().unwrap();

        let err = PqReader::open_with_cipher(&path, None).err().unwrap();
        assert!(
            err.downcast_ref::<NonFiniteEmbedding>().is_some(),
            "open must reject a non-finite centroid, got: {err}"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Deterministic synthetic clusters: `clusters` blobs of `per` points in
    /// `dim` dimensions, each blob centred at a distinct corner, lightly jittered.
    fn clustered(dim: usize, clusters: usize, per: usize) -> Vec<Vec<f32>> {
        let mut out = Vec::new();
        let mut seed = Lcg(0xABCD_1234);
        for _ in 0..clusters {
            let mut centre = vec![0.0f32; dim];
            for x in centre.iter_mut() {
                *x = seed.next_f64() as f32;
            }
            for _ in 0..per {
                let mut v = centre.clone();
                for x in v.iter_mut() {
                    *x += (seed.next_f64() as f32 - 0.5) * 0.01;
                }
                out.push(v);
            }
        }
        out
    }

    #[test]
    fn params_validate_divisibility_and_bits() {
        assert!(PqParams::new(1024, 16, 8).is_ok());
        assert!(PqParams::new(10, 3, 8).is_err()); // 3 ∤ 10
        assert!(PqParams::new(8, 4, 9).is_err()); // bits > 8
        let p = PqParams::new(64, 8, 4).unwrap();
        assert_eq!((p.dsub, p.k), (8, 16));
    }

    #[test]
    fn encode_assigns_clustered_points_to_distinct_codes() {
        // With one subspace and exactly as many centroids as clusters (k=4,
        // bits=2), points from different clusters must land on different codes and
        // same-cluster points share a code.
        let dim = 8;
        let data = clustered(dim, 4, 25);
        let params = PqParams::new(dim as u32, 1, 2).unwrap();
        let cb = train_codebooks(&data, params, 25).unwrap();
        let c0 = cb.encode(&data[0]).unwrap()[0];
        let c1 = cb.encode(&data[25]).unwrap()[0]; // second cluster
        let c2 = cb.encode(&data[50]).unwrap()[0]; // third cluster
        assert_ne!(c0, c1);
        assert_ne!(c1, c2);
        assert_ne!(c0, c2);
        // A point in the same cluster shares its code.
        assert_eq!(cb.encode(&data[1]).unwrap()[0], c0);
    }

    #[test]
    fn adc_estimate_tracks_true_distance_ordering() {
        // The ADC estimate should rank candidates the same way the true squared-L2
        // distance does, on well-separated clusters.
        let dim = 16;
        let data = clustered(dim, 6, 20);
        let params = PqParams::new(dim as u32, 4, 4).unwrap();
        let cb = train_codebooks(&data, params, 30).unwrap();
        let codes: Vec<Vec<u8>> = data.iter().map(|v| cb.encode(v).unwrap()).collect();

        let query = &data[0];
        let adc = AdcTable::new(&cb, query).unwrap();

        // The nearest candidate by ADC must be in the query's own cluster (the
        // first 20 points), and the estimate must be small there and large for a
        // far cluster.
        let near = adc.estimate(&codes[1]); // same cluster
        let far = adc.estimate(&codes[100]); // a distant cluster
        assert!(near < far, "near {near} should beat far {far}");

        // Argmin of ADC over all candidates is within the query's cluster.
        let best = (0..data.len())
            .min_by(|&a, &b| adc.estimate(&codes[a]).total_cmp(&adc.estimate(&codes[b])))
            .unwrap();
        assert!(
            best < 20,
            "ADC argmin {best} should be in the query cluster"
        );
    }

    #[test]
    fn pq_store_roundtrips_codebook_and_codes() {
        let dim = 16;
        let data = clustered(dim, 4, 10);
        let params = PqParams::new(dim as u32, 4, 4).unwrap();
        let cb = train_codebooks(&data, params, 20).unwrap();
        let codes: Vec<Vec<u8>> = data.iter().map(|v| cb.encode(v).unwrap()).collect();

        let path = std::env::temp_dir().join(format!("slater_pq_{}_{}", std::process::id(), "rt"));
        let mut w = PqWriter::create_with_cipher(&path, &cb, 4096, 3, None).unwrap();
        for (i, c) in codes.iter().enumerate() {
            w.append_codes(i as u64, c).unwrap();
        }
        w.finish().unwrap();

        let r = PqReader::open_with_cipher(&path, None).unwrap();
        assert_eq!(r.params(), params);
        let resident = r.load_resident().unwrap();
        assert_eq!(resident.len(), data.len());
        for (i, code) in codes.iter().enumerate() {
            assert_eq!(resident.node_ids[i], i as u64);
            assert_eq!(resident.codes_of(i), code.as_slice());
        }
        // The codebook read back equals the one written.
        assert_eq!(resident.codebook, cb);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pq_store_roundtrips_under_encryption() {
        let dim = 8;
        let data = clustered(dim, 3, 8);
        let params = PqParams::new(dim as u32, 2, 4).unwrap();
        let cb = train_codebooks(&data, params, 15).unwrap();
        let codes: Vec<Vec<u8>> = data.iter().map(|v| cb.encode(v).unwrap()).collect();
        let cipher = Arc::new(BlockCipher::from_master(b"pq-key", &[5u8; 32]));

        let path = std::env::temp_dir().join(format!("slater_pq_{}_{}", std::process::id(), "enc"));
        let mut w =
            PqWriter::create_with_cipher(&path, &cb, 4096, 3, Some(cipher.clone())).unwrap();
        for (i, c) in codes.iter().enumerate() {
            w.append_codes(i as u64, c).unwrap();
        }
        w.finish().unwrap();

        // Right key reads the codes; absent key is refused at open.
        let r = PqReader::open_with_cipher(&path, Some(cipher)).unwrap();
        let resident = r.load_resident().unwrap();
        assert_eq!(resident.codes_of(2), codes[2].as_slice());
        assert!(PqReader::open_with_cipher(&path, None).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
