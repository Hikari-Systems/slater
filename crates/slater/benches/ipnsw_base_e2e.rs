// SPDX-License-Identifier: Apache-2.0
//! **HIK-137 phase-2 base build — END TO END on disk.**
//!
//! The checkpoint (`ipnsw_pq_checkpoint`) measured the IP graph + IP-ADC PQ in memory. This bench
//! measures the **production base-rung path written to and read back from disk**: the IP-native
//! base index is built by `build_vamana_ip`, written as a `.vamana` (raw vectors) + `.pq` (IP
//! codebook trained on raw), then served by `beam_search` over `AdcTable::new_ip` through the real
//! `VamanaReader`/`PqReader`/resident-PQ machinery — exactly the server's `InnerProduct` read path.
//!
//! Two arms, both against the **same** `exact_topk(Metric::Dot)` independent brute-force IP ground
//! truth (no index in the truth path, never index-vs-index):
//!   * **augmented on-disk (baseline)** — the *current* production Dot base index written to disk
//!     (`write_disk_index` over the augmented `VecFixture`) and read back by `beam_topk_disk`
//!     (`ann_query` + `AdcTable::new`). The ~0.40 bar to beat.
//!   * **IP-native on-disk (HIK-137)** — `write_ip_disk_index` + `beam_topk_disk_ip`. The
//!     phase-2 deliverable, end to end.
//!
//! Params match the checkpoint: R=32, L(beam)=64, K=10, dim=256, N=2000, Q=100, PQ 16×8, 25 iters.
//! Deterministic. Run: `cargo bench -p slater --features testkit --bench ipnsw_base_e2e`

use graph_format::manifest::Metric;

use slater::vecbench::{
    beam_topk_disk, beam_topk_disk_ip, exact_topk, norm_stats, recall_at_k, write_disk_index,
    write_ip_disk_index, ManifoldModel, NormDist, VecFixture, PQ_BITS, PQ_ITERS, PQ_SUBSPACES,
    VAMANA_R,
};

const DIM: usize = 256;
const K: usize = 10;
const BEAM: usize = 64;
const N_BASE: usize = 2_000;
const N_QUERIES: usize = 100;
const N_LATENT: usize = 32;

const MODEL_SEED: u64 = 0x317_2517;
const INDEX_SEED: u64 = 0x_1A5E;
const QUERY_SEED: u64 = 0x_9CE5;

fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

struct Row {
    augmented: f64,
    ip_native: f64,
}

fn measure(dir: &std::path::Path, name: &str, raw: &[Vec<f32>], qs: &[Vec<f32>]) -> Row {
    // Augmented on-disk base index (the current production Dot path).
    let fx = VecFixture::build(Metric::Dot, raw.to_vec()).unwrap();
    let aug_ix = write_disk_index(dir, &format!("aug_{name}"), &fx, None).unwrap();
    // IP-native on-disk base index (HIK-137 phase 2).
    let ip_ix = write_ip_disk_index(
        dir,
        &format!("ip_{name}"),
        raw,
        VAMANA_R,
        PQ_SUBSPACES,
        PQ_BITS,
        PQ_ITERS,
    )
    .unwrap();

    let live: Vec<u64> = (0..raw.len() as u64).collect();
    let (mut aug, mut ipn) = (Vec::with_capacity(qs.len()), Vec::with_capacity(qs.len()));
    for q in qs {
        let truth = exact_topk(Metric::Dot, raw, &live, q, K);
        aug.push(recall_at_k(
            &beam_topk_disk(
                &aug_ix.vamana,
                &aug_ix.pq,
                aug_ix.medoid,
                Metric::Dot,
                fx.space_dim,
                q,
                K,
                BEAM,
                None,
            )
            .unwrap(),
            &truth,
        ));
        ipn.push(recall_at_k(
            &beam_topk_disk_ip(&ip_ix.vamana, &ip_ix.pq, ip_ix.medoid, q, K, BEAM).unwrap(),
            &truth,
        ));
    }
    Row {
        augmented: mean(&aug),
        ip_native: mean(&ipn),
    }
}

fn main() {
    let model = ManifoldModel::new(DIM, N_LATENT, MODEL_SEED);
    let qs = model.sample_dir(N_QUERIES, QUERY_SEED);
    let dir = std::env::temp_dir().join(format!("hik137_e2e_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

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
        "\n=== HIK-137 phase-2 base build END TO END (on disk): recall@{K} vs INDEPENDENT exact IP top-k ===",
    );
    eprintln!(
        "dim={DIM}, N={N_BASE}, {N_QUERIES} unit-direction queries, R={VAMANA_R}, L(beam)={BEAM}, PQ={PQ_SUBSPACES}x{PQ_BITS}"
    );
    eprintln!("both arms read back through the real VamanaReader/PqReader + beam_search; truth is brute-force exact_topk(Dot)\n");
    eprintln!(
        "{:<12} {:>8}  |  {:>16} {:>16}",
        "norm dist", "max/med", "augmented disk", "IP-native disk"
    );
    eprintln!(
        "{:<12} {:>8}  |  {:>16} {:>16}",
        "", "", "(baseline)", "(HIK-137)"
    );

    for (name, nd) in dists {
        let raw = model.sample_mips(N_BASE, INDEX_SEED, nd);
        let ns = norm_stats(&raw);
        let r = measure(&dir, name, &raw, &qs);
        eprintln!(
            "{:<12} {:>8.1}  |  {:>16.3} {:>16.3}",
            name, ns.max_over_median, r.augmented, r.ip_native
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
    eprintln!(
        "\nIP-native disk path = build_vamana_ip + raw .vamana + IP codebook .pq, served by AdcTable::new_ip.\n"
    );
}
