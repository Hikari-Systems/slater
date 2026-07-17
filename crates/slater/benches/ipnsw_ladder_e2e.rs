// SPDX-License-Identifier: Apache-2.0
//! **HIK-137 phase-3 ladder — recall@10 for EVERY IP-native rung, END TO END.**
//!
//! Phase 2 proved the base rung (T3). This bench proves the ladder *preserves* that recall for Dot
//! through every mutable rung, each driven end to end through the real `graph-format` writers and
//! readers on the deliverable-1 (D1) fixture, against the **same independent brute-force IP ground
//! truth** (`exact_topk(Metric::Dot)` — no index in the truth path, never index-vs-index):
//!
//!   * **base (T3)** — `build_vamana_ip` + raw `.vamana` + IP codebook `.pq`, served by
//!     `AdcTable::new_ip` (the phase-2 deliverable, reproduced here as the bar).
//!   * **T0 (delta)** — a mutable `RwVamana` built IP-native (raw `−IP`, top-R-by-IP prune,
//!     highest-norm entry) and beam-searched by exact IP.
//!   * **T2 (segment)** — a sealed segment (`seal_segment_index`, Dot ⇒ IP-native) served by the
//!     IP-ADC estimate; asserts the sealed `nav` is `InnerProduct`.
//!   * **T4b (merge inserts)** — an IP base over the first 75% with the last 25% woven in by
//!     `streaming_merge`'s IP insert-weave; read back over the whole set.
//!   * **T4a (delete re-prune / T4c holes)** — an IP base with 25% of nodes tombstoned to `HOLE`;
//!     `streaming_merge`'s IP delete re-prune splices the survivors; read back over the live set,
//!     and every deleted id must be absent (holes suppressed).
//!
//! Params match the base e2e bench: dim=256, N=2000, Q=100, R=32, L(beam)=64, PQ 16×8, 25 iters.
//! Deterministic. Run: `cargo bench -p slater --features testkit --bench ipnsw_ladder_e2e`

use graph_format::manifest::{AnnNav, Metric};
use graph_format::pq::HOLE;

use slater::vecbench::{
    beam_topk_disk_ip, exact_topk, ip_merge_params, merge_to, norm_stats, recall_at_k,
    rw_topk_ip_batch, seal_ip_topk_batch, write_ip_disk_index, ManifoldModel, NormDist, PQ_BITS,
    PQ_ITERS, PQ_SUBSPACES, VAMANA_R,
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
    base: f64,
    t0: f64,
    t2: f64,
    t4b: f64,
    t4a: f64,
}

fn measure(dir: &std::path::Path, name: &str, raw: &[Vec<f32>], qs: &[Vec<f32>]) -> Row {
    let all_live: Vec<u64> = (0..raw.len() as u64).collect();

    // ── base (T3): the phase-2 bar, reproduced here.
    let base_ix = write_ip_disk_index(
        dir,
        &format!("base_{name}"),
        raw,
        VAMANA_R,
        PQ_SUBSPACES,
        PQ_BITS,
        PQ_ITERS,
    )
    .unwrap();

    // ── T4b (merge inserts): IP base over the first 75%, last 25% woven in by streaming_merge.
    let split = raw.len() * 3 / 4;
    let base_sub = &raw[..split];
    let t4b_base = write_ip_disk_index(
        dir,
        &format!("t4b_base_{name}"),
        base_sub,
        VAMANA_R,
        PQ_SUBSPACES,
        PQ_BITS,
        PQ_ITERS,
    )
    .unwrap();
    // base_final_ids: ordinal → its dump id (= original index, since ids are 0..split). No holes.
    let t4b_final_ids = t4b_base.layout_dump_ids.clone();
    let t4b_inserts: Vec<(u64, Vec<f32>)> = (split..raw.len())
        .map(|i| (i as u64, raw[i].clone()))
        .collect();
    let (t4b_vam, t4b_pq, t4b_stats) = merge_to(
        dir,
        &format!("t4b_out_{name}"),
        &t4b_base,
        &t4b_final_ids,
        &t4b_inserts,
        &ip_merge_params(t4b_base.medoid, VAMANA_R),
    )
    .unwrap();
    assert!(
        !t4b_stats.vamana_carried && t4b_stats.inserted == (raw.len() - split) as u64,
        "T4b must weave inserts, not carry: {t4b_stats:?}"
    );

    // ── T4a (delete re-prune) / T4c (holes): IP base over all N, delete every 4th id → HOLE.
    let t4a_base = write_ip_disk_index(
        dir,
        &format!("t4a_base_{name}"),
        raw,
        VAMANA_R,
        PQ_SUBSPACES,
        PQ_BITS,
        PQ_ITERS,
    )
    .unwrap();
    let is_deleted = |id: u64| id.is_multiple_of(4);
    let t4a_final_ids: Vec<u64> = t4a_base
        .layout_dump_ids
        .iter()
        .map(|&dump_id| if is_deleted(dump_id) { HOLE } else { dump_id })
        .collect();
    let survivors: Vec<u64> = all_live
        .iter()
        .copied()
        .filter(|&id| !is_deleted(id))
        .collect();
    let (t4a_vam, t4a_pq, t4a_stats) = merge_to(
        dir,
        &format!("t4a_out_{name}"),
        &t4a_base,
        &t4a_final_ids,
        &[],
        &ip_merge_params(t4a_base.medoid, VAMANA_R),
    )
    .unwrap();
    assert!(
        !t4a_stats.vamana_carried && t4a_stats.live == survivors.len() as u64,
        "T4a must splice deletes, not carry: {t4a_stats:?}"
    );

    // Build each rung ONCE, then score every query against it.
    let t0_ids = rw_topk_ip_batch(raw, qs, K, BEAM).unwrap();
    let (t2_ids, t2_nav) =
        seal_ip_topk_batch(dir, &format!("t2_{name}"), raw, qs, K, BEAM).unwrap();
    assert_eq!(
        t2_nav,
        AnnNav::InnerProduct,
        "a Dot segment seals IP-native"
    );

    let (mut base_r, mut t0_r, mut t2_r, mut t4b_r, mut t4a_r) = (
        Vec::with_capacity(qs.len()),
        Vec::with_capacity(qs.len()),
        Vec::with_capacity(qs.len()),
        Vec::with_capacity(qs.len()),
        Vec::with_capacity(qs.len()),
    );
    for (qi, q) in qs.iter().enumerate() {
        let truth_all = exact_topk(Metric::Dot, raw, &all_live, q, K);
        let truth_live = exact_topk(Metric::Dot, raw, &survivors, q, K);

        base_r.push(recall_at_k(
            &beam_topk_disk_ip(&base_ix.vamana, &base_ix.pq, base_ix.medoid, q, K, BEAM).unwrap(),
            &truth_all,
        ));
        t0_r.push(recall_at_k(&t0_ids[qi], &truth_all));
        t2_r.push(recall_at_k(&t2_ids[qi], &truth_all));
        t4b_r.push(recall_at_k(
            &beam_topk_disk_ip(&t4b_vam, &t4b_pq, t4b_base.medoid, q, K, BEAM).unwrap(),
            &truth_all,
        ));

        let t4a_hits = beam_topk_disk_ip(&t4a_vam, &t4a_pq, t4a_base.medoid, q, K, BEAM).unwrap();
        assert!(
            t4a_hits.iter().all(|&id| !is_deleted(id)),
            "a deleted (HOLE) id was returned — hole suppression is broken: {t4a_hits:?}"
        );
        t4a_r.push(recall_at_k(&t4a_hits, &truth_live));
    }
    Row {
        base: mean(&base_r),
        t0: mean(&t0_r),
        t2: mean(&t2_r),
        t4b: mean(&t4b_r),
        t4a: mean(&t4a_r),
    }
}

fn main() {
    let model = ManifoldModel::new(DIM, N_LATENT, MODEL_SEED);
    let qs = model.sample_dir(N_QUERIES, QUERY_SEED);
    let dir = std::env::temp_dir().join(format!("hik137_ladder_{}", std::process::id()));
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
        "\n=== HIK-137 phase-3 LADDER (end to end): recall@{K} vs INDEPENDENT exact IP top-k ==="
    );
    eprintln!(
        "dim={DIM}, N={N_BASE}, {N_QUERIES} queries, R={VAMANA_R}, L(beam)={BEAM}, PQ={PQ_SUBSPACES}x{PQ_BITS}"
    );
    eprintln!(
        "every rung through the real writers/readers; truth is brute-force exact_topk(Dot)\n"
    );
    eprintln!(
        "{:<12} {:>7}  |  {:>7} {:>7} {:>7} {:>7} {:>7}",
        "norm dist", "max/med", "base", "T0", "T2", "T4b", "T4a"
    );

    for (name, nd) in dists {
        let raw = model.sample_mips(N_BASE, INDEX_SEED, nd);
        let ns = norm_stats(&raw);
        let r = measure(&dir, name, &raw, &qs);
        eprintln!(
            "{:<12} {:>7.1}  |  {:>7.3} {:>7.3} {:>7.3} {:>7.3} {:>7.3}",
            name, ns.max_over_median, r.base, r.t0, r.t2, r.t4b, r.t4a
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
    eprintln!(
        "\nbase=T3 build_vamana_ip · T0=RwVamana delta · T2=sealed segment · T4b=merge inserts · T4a=delete re-prune (+T4c holes)\n"
    );
}
