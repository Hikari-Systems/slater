// SPDX-License-Identifier: Apache-2.0
//! **HIK-137 phase-2 CHECKPOINT — PQ-under-IP.**
//!
//! The phase-1 spike proved an inner-product-native proximity GRAPH navigates to the true IP
//! top-k (0.998 / 0.998 / 1.000 vs the augmented 0.868 / 0.407 / 0.395) — but it navigated by
//! *exact* resident IP, deliberately isolating graph recall from PQ-estimate recall. This bench
//! measures the **last recall unknown before the irreversible FORMAT_VERSION work**: how much of
//! that exact-IP recall survives when the beam navigates by the **PQ estimate** instead.
//!
//! The PQ estimator is the **IP-ADC** (`AdcTable::new_ip`): the codebook is trained on the **raw**
//! vectors (plain `PqParams`, no norm-augmentation subspace), and a candidate's inner product is
//! estimated by reconstruct-and-dot, `−⟨q, x̂⟩` — an estimate of `distance(Dot) = −⟨q,x⟩`. The
//! same min-based `beam_search` descends on it, then re-ranks exact IP.
//!
//! Four arms, all against the **same** `exact_topk(Metric::Dot)` ground truth (raw f64
//! inner-product argmax — no index in the truth path, never index-vs-index):
//!   * **augmented (baseline)** — the production base index's in-memory recall path (PQ over
//!     norm-augmented ANN points). The 0.868 / 0.407 / 0.395 bar to beat.
//!   * **ip-NSW exact (ceiling)** — the spike's IP graph walked by EXACT IP. The graph's reach —
//!     the ceiling the PQ arm is measured against.
//!   * **ip-NSW + IP-ADC PQ** — the SAME IP graph walked by the IP-ADC PQ estimate + exact
//!     re-rank. **This is the checkpoint number.**
//!
//! GATE: the PQ arm must materially beat the ~0.40 augmented baseline and retain a clear majority
//! of the exact-IP ceiling → proceed to the format bump + base build. Too noisy → stop, evaluate
//! ScaNN-style anisotropic PQ (design §9.7).
//!
//! Params match the spike / D1 benches: R=32, L(beam)=64, K=10, dim=256, N=2000, Q=100, PQ 16×8,
//! 25 Lloyd iters. Deterministic: same seeds ⇒ same graph ⇒ same codebook ⇒ same recall.
//!
//! Run: `cargo bench -p slater --features testkit --bench ipnsw_pq_checkpoint`

use graph_format::manifest::Metric;

use slater::vecbench::{
    build_ip_graph, build_ip_pq, exact_topk, ip_walk_topk, ip_walk_topk_pq, norm_stats,
    recall_at_k, ManifoldModel, NormDist, VecFixture, PQ_BITS, PQ_ITERS, PQ_SUBSPACES, VAMANA_R,
};

const DIM: usize = 256;
const K: usize = 10;
const BEAM: usize = 64;
const N_BASE: usize = 2_000;
const N_QUERIES: usize = 100;
const N_LATENT: usize = 32;
const L_BUILD: usize = if VAMANA_R * 2 > 64 { VAMANA_R * 2 } else { 64 };

// Seeds — identical to the spike, so this bench's IP graph is bit-identical to the one the spike
// measured the exact-IP recall on (the PQ arm and the ceiling arm share the same graph).
const MODEL_SEED: u64 = 0x317_2517;
const INDEX_SEED: u64 = 0x_1A5E;
const QUERY_SEED: u64 = 0x_9CE5;
const IP_GRAPH_SEED: u64 = 0x1D_9250;

fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

struct Row {
    augmented: f64,
    ip_exact: f64,
    ip_pq: f64,
}

fn measure(raw: &[Vec<f32>], qs: &[Vec<f32>]) -> Row {
    let fx = VecFixture::build(Metric::Dot, raw.to_vec()).unwrap();
    let ip = build_ip_graph(raw, VAMANA_R, L_BUILD, IP_GRAPH_SEED).unwrap();
    // IP-native PQ: codebook trained on the RAW vectors (no augmentation), same 16×8 shape.
    let pq = build_ip_pq(raw, PQ_SUBSPACES, PQ_BITS, PQ_ITERS).unwrap();

    let live: Vec<u64> = (0..raw.len() as u64).collect();
    let (mut aug, mut exact, mut pqr) = (
        Vec::with_capacity(qs.len()),
        Vec::with_capacity(qs.len()),
        Vec::with_capacity(qs.len()),
    );
    for q in qs {
        let truth = exact_topk(Metric::Dot, raw, &live, q, K);
        aug.push(recall_at_k(
            &fx.beam_topk_inmem(q, K, BEAM).unwrap(),
            &truth,
        ));
        exact.push(recall_at_k(
            &ip_walk_topk(&ip, raw, q, K, BEAM).unwrap(),
            &truth,
        ));
        pqr.push(recall_at_k(
            &ip_walk_topk_pq(&ip, raw, &pq, q, K, BEAM).unwrap(),
            &truth,
        ));
    }
    Row {
        augmented: mean(&aug),
        ip_exact: mean(&exact),
        ip_pq: mean(&pqr),
    }
}

fn main() {
    let model = ManifoldModel::new(DIM, N_LATENT, MODEL_SEED);
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
        "\n=== HIK-137 phase-2 CHECKPOINT: PQ-under-IP recall@{K} vs INDEPENDENT exact IP top-k ===",
    );
    eprintln!(
        "dim={DIM}, N={N_BASE}, {N_QUERIES} unit-direction queries, latent={N_LATENT}, R={VAMANA_R}, L(beam)={BEAM}, PQ={PQ_SUBSPACES}x{PQ_BITS}"
    );
    eprintln!(
        "all arms measured against the SAME brute-force exact_topk(Dot) — never index-vs-index\n"
    );
    eprintln!(
        "{:<12} {:>8}  |  {:>10} {:>14} {:>16}",
        "norm dist", "max/med", "augmented", "ip-NSW exact", "ip-NSW + IP-ADC"
    );
    eprintln!(
        "{:<12} {:>8}  |  {:>10} {:>14} {:>16}",
        "", "", "(baseline)", "(ceiling)", "PQ (CHECKPOINT)"
    );

    for (name, nd) in dists {
        let raw = model.sample_mips(N_BASE, INDEX_SEED, nd);
        let ns = norm_stats(&raw);
        let r = measure(&raw, &qs);
        eprintln!(
            "{:<12} {:>8.1}  |  {:>10.3} {:>14.3} {:>16.3}",
            name, ns.max_over_median, r.augmented, r.ip_exact, r.ip_pq
        );
    }
    eprintln!(
        "\nGround truth: exact_topk(Dot) — brute-force inner-product argmax, no index in the path."
    );
    eprintln!(
        "IP-ADC PQ: codebook trained on RAW vectors (no augmentation), estimate = −⟨q, x̂⟩ via AdcTable::new_ip."
    );
    eprintln!(
        "GATE: PQ arm must materially beat the augmented baseline and retain most of the exact ceiling.\n"
    );
}
