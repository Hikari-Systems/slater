// SPDX-License-Identifier: Apache-2.0
//! **HIK-137 phase-1 SPIKE — bench-only IP-native (MIPS) navigator.**
//!
//! Throwaway measurement code. It answers one question, against the deliverable-1 independent
//! brute-force IP ground truth: **can an inner-product-native proximity GRAPH navigate to the
//! true IP top-k**, where the current augmented Vamana index recalls only ~0.40 on MIPS-hard
//! norm distributions? Nothing here touches the production path — no `AnnMode`, no manifest, no
//! FORMAT_VERSION, no server/query/ladder wiring, no on-disk format. The current Dot index
//! (`VecFixture` = `ann_point`/`ann_query`/`max_norm` augmentation + PQ) is measured **as is**,
//! beside a *parallel* IP-native navigator built solely in the bench harness.
//!
//! Three navigators, all measured against the **same** `exact_topk(Metric::Dot)` ground truth
//! (raw f64 inner-product argmax — no index in the truth path, never index-vs-index):
//!   * **augmented (baseline)** — the production base index's in-memory recall path: navigate by
//!     the PQ estimate over norm-augmented ANN points, re-rank exact. This is the 0.868 / 0.407 /
//!     0.395 bar to beat.
//!   * **ip-NSW (plain)** — an IP proximity graph over the RAW vectors: neighbour selection is
//!     **top-R by inner product** (s-Delaunay, design §2.3 — robust-prune's α-domination is
//!     unsound over IP and is dropped), the entry is the **highest-norm** node (design §3), and
//!     the walk navigates + re-ranks by **EXACT resident IP** (no PQ — isolates graph recall from
//!     PQ-estimate recall, design §4/§8).
//!   * **ip-NSW+ (angular-seed)** — the same exact-IP descent, but the beam is seeded from an
//!     exact-cosine neighbourhood of the query (design §2.1 D / §2.2) so a few extreme-norm hubs
//!     cannot strand the walk away from the query's directions (the Pareto hazard).
//!
//! Graph params are held comparable to the ladder/D1 benches for a fair comparison: R=32,
//! L(beam)=64, K=10, dim=256, N=2000, Q=100. Deterministic: same seeds ⇒ same graphs ⇒ same
//! recall (run it twice, the numbers are identical).
//!
//! Run: `cargo bench -p slater --features testkit --bench ipnsw_spike`

use graph_format::manifest::Metric;

use slater::vecbench::{
    angular_seeds, build_angular_graph, build_ip_graph, exact_topk, ip_walk_seeded, ip_walk_topk,
    norm_stats, recall_at_k, ManifoldModel, NormDist, VecFixture, VAMANA_ALPHA, VAMANA_R,
};

const DIM: usize = 256;
const K: usize = 10;
const BEAM: usize = 64;
const N_BASE: usize = 2_000;
const N_QUERIES: usize = 100;
const N_LATENT: usize = 32;
/// Construction search-list width (matches `build_vamana`'s `l_build`).
const L_BUILD: usize = if VAMANA_R * 2 > 64 { VAMANA_R * 2 } else { 64 };
/// How many angular (cosine) neighbours seed the ip-NSW+ walk's beam.
const N_SEEDS: usize = BEAM;

// Seeds — fixed, so every graph and its recall are reproducible run-to-run.
const MODEL_SEED: u64 = 0x317_2517;
const INDEX_SEED: u64 = 0x_1A5E;
const QUERY_SEED: u64 = 0x_9CE5;
const IP_GRAPH_SEED: u64 = 0x1D_9250;

fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// Measured recall@10 of the three navigators against the independent IP truth, for one fixture.
struct Row {
    augmented: f64,
    ip_nsw: f64,
    ip_nsw_seeded: f64,
}

fn measure(raw: &[Vec<f32>], qs: &[Vec<f32>]) -> Row {
    // The augmented production base index (unchanged) — the baseline arm.
    let fx = VecFixture::build(Metric::Dot, raw.to_vec()).unwrap();
    // The IP-native graph: top-R-by-IP selection, highest-norm entry, raw vectors.
    let ip = build_ip_graph(raw, VAMANA_R, L_BUILD, IP_GRAPH_SEED).unwrap();
    // The cosine seed graph for the angular-seed variant.
    let ang = build_angular_graph(raw, VAMANA_R, VAMANA_ALPHA).unwrap();

    let live: Vec<u64> = (0..raw.len() as u64).collect();
    let (mut aug, mut plain, mut seeded) = (
        Vec::with_capacity(qs.len()),
        Vec::with_capacity(qs.len()),
        Vec::with_capacity(qs.len()),
    );
    for q in qs {
        // GROUND TRUTH: exact IP argmax over the raw vectors — no index in this path.
        let truth = exact_topk(Metric::Dot, raw, &live, q, K);

        aug.push(recall_at_k(
            &fx.beam_topk_inmem(q, K, BEAM).unwrap(),
            &truth,
        ));
        plain.push(recall_at_k(
            &ip_walk_topk(&ip, raw, q, K, BEAM).unwrap(),
            &truth,
        ));

        let seeds = angular_seeds(&ang, q, N_SEEDS, BEAM).unwrap();
        seeded.push(recall_at_k(
            &ip_walk_seeded(&ip, raw, &seeds, q, K, BEAM).unwrap(),
            &truth,
        ));
    }
    Row {
        augmented: mean(&aug),
        ip_nsw: mean(&plain),
        ip_nsw_seeded: mean(&seeded),
    }
}

fn main() {
    let model = ManifoldModel::new(DIM, N_LATENT, MODEL_SEED);
    // Unit-direction queries: a query's norm scales every IP equally, so it never changes the
    // argmax top-k — the truth is about the *database* norms.
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
        "\n=== HIK-137 phase-1 SPIKE: IP-native (MIPS) navigator recall@{K} vs INDEPENDENT exact IP top-k ===",
    );
    eprintln!(
        "dim={DIM}, N={N_BASE}, {N_QUERIES} unit-direction queries, latent={N_LATENT}, R={VAMANA_R}, L(beam)={BEAM}, seeds={N_SEEDS}"
    );
    eprintln!("all three arms measured against the SAME brute-force exact_topk(Dot) — never index-vs-index\n");
    eprintln!(
        "{:<12} {:>8}  |  {:>10} {:>12} {:>16}",
        "norm dist", "max/med", "augmented", "ip-NSW", "ip-NSW+seeded"
    );
    eprintln!(
        "{:<12} {:>8}  |  {:>10} {:>12} {:>16}",
        "", "(baseline)", "(baseline)", "(plain)", "(angular-seed)"
    );

    for (name, nd) in dists {
        let raw = model.sample_mips(N_BASE, INDEX_SEED, nd);
        let ns = norm_stats(&raw);
        let r = measure(&raw, &qs);
        eprintln!(
            "{:<12} {:>8.1}  |  {:>10.3} {:>12.3} {:>16.3}",
            name, ns.max_over_median, r.augmented, r.ip_nsw, r.ip_nsw_seeded
        );
    }
    eprintln!(
        "\nGround truth: exact_topk(Dot) — brute-force inner-product argmax, no index in the path."
    );
    eprintln!(
        "ip-NSW arms navigate by EXACT resident IP (no PQ): this isolates GRAPH recall from PQ-estimate recall.\n"
    );
}
