// SPDX-License-Identifier: Apache-2.0
//! **MIPS (maximum-inner-product) recall@10 against independent exact ground truth** — deliverable
//! 1 of HIK-137. This is the *yardstick* a later MIPS-native index is measured against, so the
//! correctness of the ground truth is the whole point.
//!
//! ## Why a separate fixture from `vector_recall.rs`
//!
//! MIPS is about **norms**. A vector's *direction* governs its cosine/L2 neighbourhood (and how
//! navigable the proximity graph is); its *norm* governs inner product. The ladder bench's
//! `ManifoldModel::sample` folds a gentle ~4× uniform scale into the direction — which barely
//! separates Dot from cosine/L2 and so **under-stresses** MIPS. Here we decouple the two: every
//! index vector is a **unit manifold direction** (navigable structure preserved) times a norm drawn
//! **independently** from a chosen distribution. The norm distribution — not the direction — then
//! decides the true top-k, which is exactly the MIPS regime.
//!
//! Three norm distributions, hardest last:
//!   * **uniform-4x** (control) — the legacy `[0.5, 2.0)` spread; MIPS barely distinct from L2 here.
//!   * **lognormal** (realistic) — `exp(ln 1 + 0.35·Z)`; a moderate right-skew mimicking the norm
//!     spread of real un-normalized transformer embeddings.
//!   * **pareto** (adversarial) — heavy-tailed `x_m·(1−U)^(−1/α)`, α=1.6; a handful of vectors carry
//!     10–50× the norm. A high-norm vector has high IP with almost *every* query, so the true top-k
//!     is dominated by norm regardless of direction — the navigation hazard a cosine/L2-clustered
//!     graph cannot reach.
//!
//! ## The ground truth is independent of any index
//!
//! Top-k truth is `exact_topk(Metric::Dot, …)` — a plain O(N·Q·dim) brute-force argmax of the raw
//! inner product (`vector::distance(Dot,…)`). No Vamana, no PQ, no augmentation touches the truth
//! path; it is **never** "index A agrees with index B" (the house rule). The measured number is the
//! *current augmented base index*'s dot recall@10 against that truth — an honest re-measurement of
//! the PERF-REPORT 0.32–0.50 figure on MIPS-hard data.
//!
//! ## Size (modest — the box is disk-constrained)
//!
//! dim=256, N=2000 index vectors, Q=100 held-out queries, latent=32. Brute force ≈ 100·2000·256 ≈
//! 51M mults (trivial); the on-disk Vamana+PQ write stays small. This is a microbench fixture, not
//! a scale set. Deterministic: same seeds ⇒ same vectors ⇒ same ground truth (run it twice).
//!
//! Run: `cargo bench -p slater --features testkit --bench mips_recall`

use graph_format::manifest::Metric;

use slater::vecbench::{
    beam_topk_disk, exact_topk, norm_stats, recall_at_k, write_disk_index, ManifoldModel, NormDist,
    VecFixture,
};

const DIM: usize = 256;
const K: usize = 10;
const BEAM: usize = 64;
const N_BASE: usize = 2_000;
const N_QUERIES: usize = 100;
/// Intrinsic (latent) dimensionality of the manifold — well below the ambient 256, mirroring a
/// real embedding's effective rank so the kNN is meaningful and navigable.
const N_LATENT: usize = 32;

/// Seeds — fixed, so the fixture and its ground truth are reproducible run-to-run.
const MODEL_SEED: u64 = 0x317_2517;
const INDEX_SEED: u64 = 0x_1A5E;
const QUERY_SEED: u64 = 0x_9CE5;

fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// Measure the current augmented base index's dot recall@10 on `raw` against the independent
/// brute-force IP top-k. Returns `(in_mem_recall, on_disk_recall)` — both navigate the same
/// augmented graph+PQ; the on-disk arm is the production-served path (BFS layout, PQ side table).
fn measure(dir: &std::path::Path, tag: &str, raw: &[Vec<f32>], qs: &[Vec<f32>]) -> (f64, f64) {
    let fx = VecFixture::build(Metric::Dot, raw.to_vec()).unwrap();
    let disk = write_disk_index(dir, tag, &fx, None).unwrap();
    let live: Vec<u64> = (0..fx.raw.len() as u64).collect();

    let mut inmem = Vec::with_capacity(qs.len());
    let mut ondisk = Vec::with_capacity(qs.len());
    for q in qs {
        // GROUND TRUTH: exact IP argmax over the raw vectors — no index in this path.
        let exact = exact_topk(Metric::Dot, &fx.raw, &live, q, K);

        let approx_mem = fx.beam_topk_inmem(q, K, BEAM).unwrap();
        inmem.push(recall_at_k(&approx_mem, &exact));

        let approx_disk = beam_topk_disk(
            &disk.vamana,
            &disk.pq,
            disk.medoid,
            Metric::Dot,
            fx.space_dim,
            q,
            K,
            BEAM,
            None,
        )
        .unwrap();
        ondisk.push(recall_at_k(&approx_disk, &exact));
    }
    (mean(&inmem), mean(&ondisk))
}

fn main() {
    let dir = std::env::temp_dir().join(format!("slater_mips_recall_{}", std::process::id()));
    std::fs::create_dir_all(&dir).ok();

    // ONE manifold model; the three fixtures differ only in the norm distribution imposed on it, so
    // any recall difference is attributable to the norm spread, not the direction structure.
    let model = ManifoldModel::new(DIM, N_LATENT, MODEL_SEED);
    // Queries are unit directions: a query's norm scales every inner product equally, so it does
    // not change the argmax top-k — the ground truth is about the *database* norms.
    let qs = model.sample_dir(N_QUERIES, QUERY_SEED);

    let dists: [(&str, NormDist); 3] = [
        ("uniform-4x", NormDist::Uniform4x),
        (
            "lognormal",
            NormDist::LogNormal {
                median: 1.0,
                sigma: 0.35,
            },
        ),
        (
            "pareto",
            NormDist::Pareto {
                x_m: 1.0,
                alpha: 1.6,
            },
        ),
    ];

    eprintln!(
        "\n=== MIPS (Dot) recall@{K} vs INDEPENDENT exact brute-force IP top-k (HIK-137 D1) ===",
    );
    eprintln!(
        "dim={DIM}, N={N_BASE}, {N_QUERIES} unit-direction queries, latent={N_LATENT}, beam L={BEAM}\n"
    );
    eprintln!(
        "{:<12}  {:>7} {:>8} {:>8} {:>8} {:>10}  |  {:>10} {:>10}",
        "norm dist", "min", "median", "p99", "max", "max/med", "recall@10", "recall@10"
    );
    eprintln!(
        "{:<12}  {:>7} {:>8} {:>8} {:>8} {:>10}  |  {:>10} {:>10}",
        "", "", "", "", "", "", "(in-mem)", "(on-disk)"
    );

    for (name, nd) in dists {
        let raw = model.sample_mips(N_BASE, INDEX_SEED, nd);
        let ns = norm_stats(&raw);
        let (r_mem, r_disk) = measure(&dir, name, &raw, &qs);
        eprintln!(
            "{:<12}  {:>7.2} {:>8.2} {:>8.2} {:>8.2} {:>10.1}  |  {:>10.3} {:>10.3}",
            name, ns.min, ns.median, ns.p99, ns.max, ns.max_over_median, r_mem, r_disk
        );
    }
    eprintln!(
        "\nGround truth: exact_topk(Dot) — brute-force inner-product argmax, no index in the path.\n"
    );
    std::fs::remove_dir_all(&dir).ok();
}
