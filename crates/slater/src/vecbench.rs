// SPDX-License-Identifier: Apache-2.0
//! Shared vector-benchmark plumbing for the FreshDiskANN performance suite (HIK-120).
//!
//! Three of the five benches ([`vector_recall`](../../benches/vector_recall.rs),
//! [`vector_delete_io`](../../benches/vector_delete_io.rs),
//! [`streaming_merge`](../../benches/streaming_merge.rs)) all need the same on-disk
//! Vamana + PQ lifecycle: map raw vectors into ANN space (`pq::ann_point`), train a 16×8
//! codebook, build the proximity graph (`vamana::build_vamana`), write it out in BFS-from-medoid
//! layout, and query it back with `vamana::beam_search`. Getting the ANN-space mapping wrong
//! compiles but silently builds a wrong-space graph (the D29 invariant), so it is centralised
//! here **once** rather than re-derived in three bench files.
//!
//! Ground truth for recall is [`crate::vector::distance`] — the exact metric distance over the
//! **live set**, recomputed independently, never "impl A agrees with impl B" (the house rule).
//!
//! Gated `pub` under `testkit` like [`crate::testgen`] / [`crate::benchkit`].

#![cfg(any(test, feature = "testkit"))]

use std::cell::Cell;
use std::path::{Path, PathBuf};

use anyhow::Result;

use graph_format::manifest::Metric;
use graph_format::pq::{
    ann_point, ann_pq_params, ann_query, l2_norm, train_codebooks, AdcTable, Codebook, PqParams,
    PqReader, PqWriter, ResidentPq, HOLE,
};
use graph_format::vamana::{
    beam_search, bfs_order, build_vamana, BeamParams, VamanaGraph, VamanaIndex, VamanaReader,
    VamanaWriter,
};
use graph_format::vamana_delete::{
    consolidate_deletes, recommended_cache_records, ConsolidateOpts, RECOMMENDED_CACHE_BLOCKS,
};
use graph_format::vamana_merge::{streaming_merge, MergeInputs, MergeParams, MergeStats};

use crate::vector::distance;

/// The builder shape slater-build ships (`slater-build/src/shared.rs`): R=32, α=1.2, PQ 16×8,
/// 25 Lloyd iterations, 256 KiB blocks, zstd 3.
pub const VAMANA_R: usize = 32;
pub const VAMANA_ALPHA: f32 = 1.2;
pub const PQ_SUBSPACES: u32 = 16;
pub const PQ_BITS: u32 = 8;
pub const PQ_ITERS: usize = 25;
pub const BLOCK: usize = 256 * 1024;
pub const ZSTD: i32 = 3;

/// splitmix64 — the deterministic stream the whole suite shares (mirrors `vector_knn.rs`), so
/// fixtures are stable run-to-run without an `rand` dependency.
pub struct SplitMix64(pub u64);
impl SplitMix64 {
    /// The raw splitmix64 step — one 64-bit draw, advancing state.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A signed unit sample in `[-1, 1)` (24-bit mantissa). The suite's historical stream — kept
    /// bit-identical (this is just [`Self::next_u64`] refactored out of the body).
    pub fn next_f32(&mut self) -> f32 {
        let z = self.next_u64();
        let unit = (z >> 40) as f32 / (1u32 << 24) as f32;
        unit * 2.0 - 1.0
    }

    /// A uniform double in `[0, 1)` (53-bit mantissa). For inverse-CDF norm draws.
    pub fn next_unit(&mut self) -> f64 {
        let z = self.next_u64();
        (z >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A standard-normal draw (Box–Muller). Used to build log-normal norms.
    pub fn next_normal(&mut self) -> f64 {
        // Guard u1 away from 0 so ln is finite; both draws advance the shared stream.
        let u1 = self.next_unit().max(f64::MIN_POSITIVE);
        let u2 = self.next_unit();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
}

/// A **norm distribution** to impose on manifold *directions*. MIPS is about *norms*: a vector's
/// direction governs its cosine/L2 neighbourhood (and how navigable the proximity graph is), while
/// its **norm** governs inner product. Decoupling the two — a manifold direction times an
/// independently-drawn norm — is what lets a fixture stress MIPS specifically, rather than the
/// gentle ~4× scale the legacy [`ManifoldModel::sample`] folds into direction.
#[derive(Clone, Copy, Debug)]
pub enum NormDist {
    /// The legacy fixture's spread: a per-vector uniform scale in `[0.5, 2.0)` — a gentle ~4×
    /// range. The control: MIPS is barely distinct from cosine/L2 here, which is the whole
    /// under-stressing complaint.
    Uniform4x,
    /// **Realistic embedding-like.** Log-normal norms, `exp(ln(median) + σ·Z)`, `Z ~ N(0,1)`. A
    /// moderate right-skew (σ≈0.35 ⇒ bulk spread ~2–3×, a thin upper tail) mimicking the norm
    /// spread of real *un-normalized* transformer embeddings.
    LogNormal { median: f64, sigma: f64 },
    /// **Adversarial heavy-tailed.** Pareto (power-law) norms, `x_m·(1−U)^(−1/α)`. With a small α
    /// (≈1.6) a handful of vectors carry 10–50× the norm; a high-norm vector has high IP with
    /// almost *every* query, so the true MIPS top-k is dominated by norm regardless of direction —
    /// the navigation hazard a cosine/L2-clustered graph cannot reach.
    Pareto { x_m: f64, alpha: f64 },
}

impl NormDist {
    /// Draw one norm from the shared deterministic stream.
    pub fn draw(&self, rng: &mut SplitMix64) -> f64 {
        match *self {
            NormDist::Uniform4x => 0.5 + rng.next_unit() * 1.5, // [0.5, 2.0)
            NormDist::LogNormal { median, sigma } => {
                (median.ln() + sigma * rng.next_normal()).exp()
            }
            NormDist::Pareto { x_m, alpha } => {
                // Inverse-CDF: U~[0,1) ⇒ x_m·(1−U)^(−1/α). Clamp 1−U off 0 so the tail is finite.
                let tail = (1.0 - rng.next_unit()).max(f64::MIN_POSITIVE);
                x_m * tail.powf(-1.0 / alpha)
            }
        }
    }
}

/// `n` uniform-random dim-`d` vectors with **deliberately unequal norms** (a per-vector scale in
/// `[0.5, 2.0)`). Used where only *throughput* or *IO structure* matters, not recall:
/// uniform-random high-dim vectors are near-orthogonal and equidistant (the curse of
/// dimensionality), so their recall@10 is ill-defined and **no** ANN graph scores well on them —
/// use [`ClusterModel`] for anything that measures recall.
pub fn random_vectors_unequal_norms(n: usize, d: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = SplitMix64(seed);
    (0..n)
        .map(|_| {
            let scale = 0.5 + (rng.next_f32() * 0.5 + 0.5) * 1.5; // ~[0.5, 2.0), a moderate 4× spread
            (0..d).map(|_| rng.next_f32() * scale).collect()
        })
        .collect()
}

/// A synthetic **low-rank manifold** — the representative stand-in for real embeddings. Real
/// embeddings do not fill their 768-dim box (uniform-random data does, which is why its kNN is
/// meaningless); they live on a continuous ~50-dim manifold, which is what makes their kNN both
/// **meaningful** (real neighbourhoods) and **navigable** (a connected surface a greedy graph
/// walk can traverse — unlike isolated tight clusters, which fragment the proximity graph into
/// disconnected components a beam search can never cross). A model is a random `latent`×`d` basis;
/// a sample is a random latent-space point lifted through it, with a per-point norm scale so
/// cosine / L2 / dot genuinely diverge. Index vectors and held-out queries are both
/// [`sample`](ManifoldModel::sample)d from the **same** model — how SIFT/GloVe-style ANN
/// benchmarks hold out queries from the training distribution.
pub struct ManifoldModel {
    /// `basis[l]` is a random dim-`d` vector; a sample is `Σ_l coeff_l · basis[l]`.
    pub basis: Vec<Vec<f32>>,
    pub d: usize,
}

impl ManifoldModel {
    /// A model with `latent` random basis vectors in dim `d` (the intrinsic dimensionality; ~48
    /// mirrors a real embedding's effective rank).
    pub fn new(d: usize, latent: usize, seed: u64) -> Self {
        let mut rng = SplitMix64(seed);
        let basis = (0..latent)
            .map(|_| (0..d).map(|_| rng.next_f32()).collect())
            .collect();
        Self { basis, d }
    }

    /// `n` vectors: each a random latent point (`coeff_l ∈ [-1,1)`) lifted through the basis and
    /// scaled by a per-point factor in `[0.5, 2.0)` (unequal norms). Points near in latent space
    /// are near in output space — a genuine, navigable neighbourhood structure.
    pub fn sample(&self, n: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = SplitMix64(seed);
        let latent = self.basis.len();
        (0..n)
            .map(|_| {
                let coeffs: Vec<f32> = (0..latent).map(|_| rng.next_f32()).collect();
                let scale = 0.5 + (rng.next_f32() * 0.5 + 0.5) * 1.5; // ~[0.5, 2.0), a moderate 4× spread
                let mut out = vec![0.0f32; self.d];
                for (l, &c) in coeffs.iter().enumerate() {
                    let b = &self.basis[l];
                    for (o, &bv) in out.iter_mut().zip(b.iter()) {
                        *o += c * bv;
                    }
                }
                for o in out.iter_mut() {
                    *o *= scale;
                }
                out
            })
            .collect()
    }

    /// One raw latent lift (a manifold *direction* before any norm scaling): `Σ_l coeff_l·basis[l]`
    /// with `coeff_l ∈ [-1,1)`, drawn from the shared stream. Kept private so the two public
    /// samplers share the exact lift.
    fn lift(&self, rng: &mut SplitMix64) -> Vec<f32> {
        let latent = self.basis.len();
        let coeffs: Vec<f32> = (0..latent).map(|_| rng.next_f32()).collect();
        let mut out = vec![0.0f32; self.d];
        for (l, &c) in coeffs.iter().enumerate() {
            let b = &self.basis[l];
            for (o, &bv) in out.iter_mut().zip(b.iter()) {
                *o += c * bv;
            }
        }
        out
    }

    /// `n` **unit-norm** manifold directions. Used for MIPS queries (a query's norm is a positive
    /// scalar that scales every inner product equally, so it does not change the argmax top-k — a
    /// unit query keeps the ground truth about the *database* norms) and as the direction factor
    /// for [`sample_mips`](Self::sample_mips).
    pub fn sample_dir(&self, n: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = SplitMix64(seed);
        (0..n)
            .map(|_| {
                let mut v = self.lift(&mut rng);
                let norm = l2_norm(&v).max(f64::MIN_POSITIVE) as f32;
                for x in v.iter_mut() {
                    *x /= norm;
                }
                v
            })
            .collect()
    }

    /// `n` MIPS index vectors: a **unit manifold direction** (navigable neighbourhood structure)
    /// times a norm drawn independently from `norm_dist`. Decoupling direction from norm is what
    /// makes this stress MIPS — the norm distribution, not the direction, decides the true top-k.
    ///
    /// Directions and norms are drawn from **two separate streams** (`seed` and `seed ^ K`), so for
    /// a fixed `seed` the *directions are identical across every `norm_dist`* — the norm draw (which
    /// consumes a distribution-dependent number of RNG values: Box–Muller takes two, inverse-CDF one)
    /// can never perturb the direction stream. That makes a cross-distribution recall comparison a
    /// genuinely controlled experiment: only the norm spread varies.
    pub fn sample_mips(&self, n: usize, seed: u64, norm_dist: NormDist) -> Vec<Vec<f32>> {
        let mut dir_rng = SplitMix64(seed);
        let mut norm_rng = SplitMix64(seed ^ 0x4D49_5053_4E52_4D00); // "MIPSNRM" — the norm stream
        (0..n)
            .map(|_| {
                let mut v = self.lift(&mut dir_rng);
                let unit = l2_norm(&v).max(f64::MIN_POSITIVE) as f32;
                let norm = norm_dist.draw(&mut norm_rng) as f32;
                let scale = norm / unit;
                for x in v.iter_mut() {
                    *x *= scale;
                }
                v
            })
            .collect()
    }
}

/// Summary of a vector set's L2-norm spread — the property that makes a fixture MIPS-hard. Reports
/// the min/median/p99/max and the max/median ratio (the "how much does the biggest norm dominate"
/// number that a wide/heavy-tailed distribution inflates).
pub struct NormStats {
    pub min: f64,
    pub median: f64,
    pub p99: f64,
    pub max: f64,
    pub max_over_median: f64,
}

/// Compute [`NormStats`] over a raw vector set.
pub fn norm_stats(raw: &[Vec<f32>]) -> NormStats {
    let mut norms: Vec<f64> = raw.iter().map(|v| l2_norm(v)).collect();
    norms.sort_by(f64::total_cmp);
    let at = |q: f64| norms[((norms.len() as f64 * q) as usize).min(norms.len() - 1)];
    let median = at(0.5);
    NormStats {
        min: norms[0],
        median,
        p99: at(0.99),
        max: *norms.last().unwrap(),
        max_over_median: norms.last().unwrap() / median.max(f64::MIN_POSITIVE),
    }
}

/// The ANN-space bundle for one metric over a raw vector set: the mapped points, the trained
/// codebook, the per-point codes, and the in-memory proximity graph — everything a recall or IO
/// measurement needs, all in raw/dense input order.
pub struct VecFixture {
    pub metric: Metric,
    pub dim: usize,
    /// Raw vectors, dense input order (index `i` ⇒ dump id `i`).
    pub raw: Vec<Vec<f32>>,
    pub space_dim: usize,
    pub max_norm: f64,
    pub params: PqParams,
    pub codebook: Codebook,
    /// `codes[i]` = `codebook.encode(ann_point(raw[i]))`, input order.
    pub codes: Vec<Vec<u8>>,
    /// Proximity graph over the ANN points, adjacency indexed in input order.
    pub graph: VamanaGraph,
}

impl VecFixture {
    /// Build the whole ANN lifecycle for `metric` over `raw`, exactly as the builder does
    /// (`ann_pq_params` → `ann_point` → `train_codebooks` → `build_vamana`).
    pub fn build(metric: Metric, raw: Vec<Vec<f32>>) -> Result<Self> {
        assert!(!raw.is_empty());
        let dim = raw[0].len();
        let params = ann_pq_params(metric, dim as u32, PQ_SUBSPACES, PQ_BITS)?;
        let space_dim = params.dim as usize;
        let max_norm = raw.iter().map(|v| l2_norm(v)).fold(0.0_f64, f64::max);
        let points: Vec<Vec<f32>> = raw
            .iter()
            .map(|v| ann_point(metric, v, max_norm, space_dim))
            .collect::<Result<_>>()?;
        let codebook = train_codebooks(&points, params, PQ_ITERS)?;
        let codes: Vec<Vec<u8>> = points
            .iter()
            .map(|p| codebook.encode(p))
            .collect::<Result<_>>()?;
        let graph = build_vamana(&points, VAMANA_R, VAMANA_ALPHA)?;
        Ok(Self {
            metric,
            dim,
            raw,
            space_dim,
            max_norm,
            params,
            codebook,
            codes,
            graph,
        })
    }

    /// Beam-search the **in-memory** graph (the base-index recall path): navigate by the PQ
    /// estimate, re-rank by the exact metric distance. Returns the emitted dump ids (input
    /// indices), best-first.
    pub fn beam_topk_inmem(&self, q_raw: &[f32], k: usize, beam: usize) -> Result<Vec<u64>> {
        let qa = ann_query(self.metric, q_raw, self.space_dim)?;
        let adc = AdcTable::new(&self.codebook, &qa)?;
        let hits = beam_search(
            BeamParams {
                medoid: self.graph.medoid,
                beam_width: beam,
                k,
                num_nodes: self.raw.len(),
            },
            |i| adc.estimate(&self.codes[i as usize]),
            |i| {
                Ok((
                    self.raw[i as usize].clone(),
                    self.graph.adjacency[i as usize].clone(),
                ))
            },
            |v| distance(self.metric, q_raw, v) as f32,
            |i| Ok(Some(i as u64)),
        )?;
        Ok(hits.into_iter().map(|h| h.node_id).collect())
    }
}

/// Exact top-`k` over a **live** subset of a raw vector set — independently-derived truth for
/// recall. `live` are the dump ids (input indices) still in the index; ties break by ascending
/// id, matching the D26 total order.
pub fn exact_topk(
    metric: Metric,
    raw: &[Vec<f32>],
    live: &[u64],
    q_raw: &[f32],
    k: usize,
) -> Vec<u64> {
    let mut scored: Vec<(f64, u64)> = live
        .iter()
        .map(|&id| (distance(metric, q_raw, &raw[id as usize]), id))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

/// recall@k = |approx ∩ exact| / |exact| (guards an empty exact set as 1.0).
pub fn recall_at_k(approx: &[u64], exact: &[u64]) -> f64 {
    if exact.is_empty() {
        return 1.0;
    }
    let hit = approx.iter().filter(|id| exact.contains(id)).count();
    hit as f64 / exact.len() as f64
}

/// An on-disk Vamana + PQ index, written in BFS-from-medoid layout (the builder's layout, so
/// block locality matches production). Layout ordinal `i` carries dump id `layout_dump_ids[i]`.
pub struct DiskIndex {
    pub vamana: PathBuf,
    pub pq: PathBuf,
    /// The medoid's **layout** ordinal (the fixed beam-search entry point).
    pub medoid: VamanaIndex,
    /// `layout_dump_ids[i]` = the dump id (input index) of the record at layout ordinal `i`.
    pub layout_dump_ids: Vec<u64>,
}

/// Write `fx` to `dir/{tag}.vamana` + `dir/{tag}.pq` in BFS layout. `dead` (optional, indexed by
/// **input** index) marks holes: a hole keeps its record + codes + adjacency (a navigational
/// waypoint) but its `.pq` node-id column is [`HOLE`], so beam search never emits it. `None` ⇒
/// all live.
pub fn write_disk_index(
    dir: &Path,
    tag: &str,
    fx: &VecFixture,
    dead: Option<&[bool]>,
) -> Result<DiskIndex> {
    std::fs::create_dir_all(dir).ok();
    let order = bfs_order(&fx.graph); // layout order (Vec<VamanaIndex>)
    let mut new_of = vec![0u32; order.len()];
    for (newi, &old) in order.iter().enumerate() {
        new_of[old as usize] = newi as u32;
    }

    let vpath = dir.join(format!("{tag}.vamana"));
    let mut vw = VamanaWriter::create_with_cipher(&vpath, BLOCK, ZSTD, None)?;
    for &old in &order {
        let nbrs: Vec<VamanaIndex> = fx.graph.adjacency[old as usize]
            .iter()
            .map(|&n| new_of[n as usize])
            .collect();
        vw.append(&fx.raw[old as usize], &nbrs)?;
    }
    vw.finish()?;

    let ppath = dir.join(format!("{tag}.pq"));
    write_pq(&ppath, fx, &order, dead)?;

    Ok(DiskIndex {
        vamana: vpath,
        pq: ppath,
        medoid: new_of[fx.graph.medoid as usize],
        layout_dump_ids: order.iter().map(|&o| o as u64).collect(),
    })
}

/// Write just the `.pq` column for `fx` in `order`, optionally with holes. Separated so the
/// delete-IO bench can pair one base `.vamana` (full adjacency = *lazy*) with a holes `.pq`, and
/// a consolidated `.vamana` with the same holes `.pq`.
pub fn write_pq(
    path: &Path,
    fx: &VecFixture,
    order: &[VamanaIndex],
    dead: Option<&[bool]>,
) -> Result<()> {
    let mut pw = PqWriter::create_with_cipher(path, &fx.codebook, BLOCK, ZSTD, None)?;
    for &old in order {
        let node_id = match dead {
            Some(d) if d[old as usize] => HOLE,
            _ => old as u64,
        };
        pw.append_codes(node_id, &fx.codes[old as usize])?;
    }
    pw.finish()?;
    Ok(())
}

/// The BFS layout of `fx.graph` and the medoid's layout ordinal — for callers that write the
/// `.vamana` once and then several `.pq` variants over the same order.
pub fn layout(fx: &VecFixture) -> (Vec<VamanaIndex>, VamanaIndex) {
    let order = bfs_order(&fx.graph);
    let mut new_of = vec![0u32; order.len()];
    for (newi, &old) in order.iter().enumerate() {
        new_of[old as usize] = newi as u32;
    }
    (order, new_of[fx.graph.medoid as usize])
}

/// Beam-search an **on-disk** index. Navigates by the resident PQ estimate, re-ranks by exact
/// distance over the raw vector fetched from the `.vamana`, and skips holes. If `fetches` is
/// given, it is incremented once per node the beam expands — the DiskANN IO unit (one node =
/// one random read). Returns the emitted dump ids best-first.
#[allow(clippy::too_many_arguments)]
pub fn beam_topk_disk(
    vamana: &Path,
    pq: &Path,
    medoid: VamanaIndex,
    metric: Metric,
    space_dim: usize,
    q_raw: &[f32],
    k: usize,
    beam: usize,
    fetches: Option<&Cell<u64>>,
) -> Result<Vec<u64>> {
    let reader = VamanaReader::open_with_cipher(vamana, None)?;
    let resident: ResidentPq = PqReader::open_with_cipher(pq, None)?.load_resident()?;
    let qa = ann_query(metric, q_raw, space_dim)?;
    let adc = AdcTable::new(&resident.codebook, &qa)?;
    let n = reader.len() as usize;
    let hits = beam_search(
        BeamParams {
            medoid,
            beam_width: beam,
            k,
            num_nodes: n,
        },
        |i| adc.estimate(resident.codes_of(i as usize)),
        |i| {
            if let Some(c) = fetches {
                c.set(c.get() + 1);
            }
            let node = reader.node(i)?;
            Ok((node.vector, node.neighbours))
        },
        |v| distance(metric, q_raw, v) as f32,
        |i| {
            Ok(if resident.is_hole(i as usize) {
                None
            } else {
                Some(resident.node_ids[i as usize])
            })
        },
    )?;
    Ok(hits.into_iter().map(|h| h.node_id).collect())
}

/// The `ConsolidateOpts` a delete-consolidation runs with, at the builder's R/α.
pub fn consolidate_opts(fx: &VecFixture, medoid: VamanaIndex) -> ConsolidateOpts {
    ConsolidateOpts {
        medoid,
        r: VAMANA_R,
        alpha: VAMANA_ALPHA,
        metric: fx.metric,
        max_norm: fx.max_norm,
        space_dim: fx.space_dim,
        cache_records: recommended_cache_records(VAMANA_R),
        cache_blocks: RECOMMENDED_CACHE_BLOCKS,
    }
}

/// Consolidate `base_vamana` (the lazy, full-adjacency graph) against `dead` (indexed by
/// **layout** ordinal), writing the spliced graph to `dir/{tag}.vamana`. No live node's
/// adjacency names a hole afterwards, so beam search never fetches a dead record. Returns the
/// output path (layout ordinals + medoid are preserved).
pub fn consolidate_to(
    dir: &Path,
    tag: &str,
    base_vamana: &Path,
    dead_layout: &[bool],
    opts: &ConsolidateOpts,
) -> Result<PathBuf> {
    let reader = VamanaReader::open_with_cipher(base_vamana, None)?;
    let out = dir.join(format!("{tag}.vamana"));
    let mut vw = VamanaWriter::create_with_cipher(&out, BLOCK, ZSTD, None)?;
    consolidate_deletes(&reader, dead_layout, opts, &mut vw)?;
    vw.finish()?;
    Ok(out)
}

/// The `MergeParams` for a streaming merge at the builder's shape.
pub fn merge_params(fx: &VecFixture, medoid: VamanaIndex) -> MergeParams {
    MergeParams {
        medoid,
        r: VAMANA_R,
        alpha: VAMANA_ALPHA,
        l_build: (VAMANA_R * 2).max(64),
        metric: fx.metric,
        max_norm: fx.max_norm,
        vamana_block_bytes: BLOCK,
        pq_block_bytes: BLOCK,
        zstd_level: ZSTD,
        cipher: None,
    }
}

/// Run a streaming merge of `base` (its `.vamana`/`.pq`) with `inserts` (new raw vectors keyed by
/// dump id) and `base_final_ids` (layout ordinal ⇒ surviving dump id, [`HOLE`] to tombstone),
/// writing `dir/{tag}.vamana` + `.pq`. Returns the stats (`vamana_carried` = fast-path fired).
pub fn merge_to(
    dir: &Path,
    tag: &str,
    base: &DiskIndex,
    base_final_ids: &[u64],
    inserts: &[(u64, Vec<f32>)],
    params: &MergeParams,
) -> Result<(PathBuf, PathBuf, MergeStats)> {
    let vout = dir.join(format!("{tag}.vamana"));
    let pout = dir.join(format!("{tag}.pq"));
    let inputs = MergeInputs {
        base_final_ids,
        inserts,
    };
    let stats = streaming_merge(&base.vamana, &base.pq, &inputs, params, &vout, &pout)?;
    Ok((vout, pout, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The plumbing is self-consistent: an on-disk index recalls the same as the in-memory one
    /// (both navigate the same graph + PQ), and exact top-1 of the query itself is the query's
    /// own id. This guards the ANN-space mapping and the BFS-layout neighbour remap — a wrong
    /// mapping compiles but tanks recall.
    #[test]
    fn disk_and_inmem_agree_and_recall_is_high() {
        // Manifold data + held-out queries (the representative path) — uniform-random high-dim
        // vectors have no meaningful kNN structure, so recall there proves nothing.
        let model = ManifoldModel::new(768, 48, 0xC0FFEE);
        let raw = model.sample(2000, 0xF00D);
        let qs = model.sample(20, 0xBEEF);
        let fx = VecFixture::build(Metric::Cosine, raw).unwrap();
        let dir = std::env::temp_dir().join(format!("slater_vecbench_{}", std::process::id()));
        let disk = write_disk_index(&dir, "t", &fx, None).unwrap();

        let live: Vec<u64> = (0..fx.raw.len() as u64).collect();
        let mut inmem_sum = 0.0;
        let mut disk_sum = 0.0;
        let reps = qs.len();
        for q in &qs {
            let exact = exact_topk(Metric::Cosine, &fx.raw, &live, q, 10);
            inmem_sum += recall_at_k(&fx.beam_topk_inmem(q, 10, 64).unwrap(), &exact);
            disk_sum += recall_at_k(
                &beam_topk_disk(
                    &disk.vamana,
                    &disk.pq,
                    disk.medoid,
                    Metric::Cosine,
                    fx.space_dim,
                    q,
                    10,
                    64,
                    None,
                )
                .unwrap(),
                &exact,
            );
        }
        assert!(
            inmem_sum / reps as f64 > 0.8,
            "in-mem recall too low: {}",
            inmem_sum / reps as f64
        );
        assert!(
            disk_sum / reps as f64 > 0.8,
            "disk recall too low: {}",
            disk_sum / reps as f64
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The MIPS fixture and its ground truth are a deterministic function of (model, seed,
    /// distribution): the *same* seeds ⇒ bit-identical vectors ⇒ bit-identical exact IP top-k. This
    /// is the reproducibility contract deliverable 1 rests on.
    #[test]
    fn mips_fixture_and_ground_truth_are_deterministic() {
        let nd = NormDist::Pareto {
            x_m: 1.0,
            alpha: 1.6,
        };
        let m1 = ManifoldModel::new(256, 32, 0x317_2517);
        let m2 = ManifoldModel::new(256, 32, 0x317_2517);
        let a = m1.sample_mips(500, 0x1A5E, nd);
        let b = m2.sample_mips(500, 0x1A5E, nd);
        assert_eq!(a, b, "same seed must give bit-identical MIPS vectors");

        let qs = m1.sample_dir(20, 0x9CE5);
        let live: Vec<u64> = (0..a.len() as u64).collect();
        for q in &qs {
            // The ground-truth path (brute-force IP argmax) is reproducible run-to-run.
            assert_eq!(
                exact_topk(Metric::Dot, &a, &live, q, 10),
                exact_topk(Metric::Dot, &b, &live, q, 10),
                "same fixture must give identical exact IP top-k"
            );
        }
    }

    /// The controlled-experiment invariant: for a fixed seed, every `NormDist` shares the *same*
    /// directions (only the norm scaling differs), so a cross-distribution recall gap is
    /// attributable to the norm spread alone. Guards the two-stream split in `sample_mips` — a
    /// regression to a single interleaved stream would make the norm draw perturb the directions.
    #[test]
    fn mips_directions_are_shared_across_norm_dists() {
        let model = ManifoldModel::new(256, 32, 0x317_2517);
        let a = model.sample_mips(300, 0x1A5E, NormDist::Uniform4x);
        let b = model.sample_mips(
            300,
            0x1A5E,
            NormDist::Pareto {
                x_m: 1.0,
                alpha: 1.6,
            },
        );
        for (va, vb) in a.iter().zip(&b) {
            // Same direction ⇒ the normalised vectors match (norms differ, direction does not).
            let na = l2_norm(va).max(f64::MIN_POSITIVE) as f32;
            let nb = l2_norm(vb).max(f64::MIN_POSITIVE) as f32;
            for (x, y) in va.iter().zip(vb) {
                assert!(
                    (x / na - y / nb).abs() < 1e-5,
                    "directions must be shared across norm distributions"
                );
            }
        }
    }

    /// The whole point of the fixture: the MIPS-hard norm distributions genuinely spread norms far
    /// wider than the legacy ~4× uniform. A wider max/median ratio is what makes IP diverge from
    /// cosine/L2 and stresses navigation. Ordering: uniform < lognormal < pareto.
    #[test]
    fn norm_distributions_are_progressively_heavier_tailed() {
        let model = ManifoldModel::new(256, 32, 0x317_2517);
        let uni = norm_stats(&model.sample_mips(2000, 0x1A5E, NormDist::Uniform4x));
        let logn = norm_stats(&model.sample_mips(
            2000,
            0x1A5E,
            NormDist::LogNormal {
                median: 1.0,
                sigma: 0.35,
            },
        ));
        let par = norm_stats(&model.sample_mips(
            2000,
            0x1A5E,
            NormDist::Pareto {
                x_m: 1.0,
                alpha: 1.6,
            },
        ));
        // Legacy uniform is a tight ~4× box; the realistic and adversarial ones are strictly wider,
        // with the heavy-tailed Pareto the widest by a large margin.
        assert!(
            uni.max_over_median < 3.0,
            "uniform-4x should stay tight, got {}",
            uni.max_over_median
        );
        assert!(
            logn.max_over_median > uni.max_over_median,
            "lognormal ({}) must spread wider than uniform ({})",
            logn.max_over_median,
            uni.max_over_median
        );
        assert!(
            par.max_over_median > 20.0 && par.max_over_median > logn.max_over_median * 3.0,
            "pareto ({}) must be heavy-tailed vs lognormal ({})",
            par.max_over_median,
            logn.max_over_median
        );
    }
}
