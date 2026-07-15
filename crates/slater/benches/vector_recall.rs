// SPDX-License-Identifier: Apache-2.0
//! Recall@10 across the FreshDiskANN write ladder, per metric (HIK-120, feature 2).
//!
//! Measures **recall**, not timing. At each rung of the ladder the served top-10 is compared to
//! an **exact brute force over the live set** — recomputed independently with
//! `slater::vector::distance`, never "index A agrees with index B" (the house rule). The rungs:
//!
//! * **delta** — the in-memory `RwVamana` over the write delta (HIK-112);
//! * **base** — the on-disk `build_vamana` + PQ index (navigated by the PQ estimate, re-ranked
//!   exact), the state a freshly-built core serves;
//! * **consolidated** — the base after a delete-consolidation spliced holes out of adjacency
//!   (HIK-114);
//! * **merged** — the base after a streaming merge folded in a batch of inserts (HIK-115/119).
//!
//! For **each of cosine / L2 / dot**, over vectors with **unequal norms** — on unit vectors the
//! three metrics coincide and a per-metric comparison proves nothing. Data is a low-rank
//! **manifold** (`vecbench::ManifoldModel`), not uniform-random: real embeddings live on such a
//! manifold, which is what makes their kNN both meaningful and navigable (uniform-random high-dim
//! vectors are near-orthogonal and equidistant, so *no* ANN graph recalls well on them).
//!
//! The engine-level "delta+segments" *merged read* is not a distinct index kind — a core segment
//! is itself a small on-disk vamana, so its recall is the **base** rung's, and the merged top-k
//! is bounded below by each level's recall (`vector::merge_topk`). We therefore report the
//! ladder's four distinct **index kinds** rather than standing up a multi-segment flush in a
//! microbench; that is called out in PERF-REPORT.md.
//!
//! Run: `cargo bench -p slater --features testkit --bench vector_recall`

use graph_format::manifest::Metric;
use graph_format::rwvamana::RwVamana;

use slater::vecbench::{
    beam_topk_disk, consolidate_opts, exact_topk, layout, merge_params, merge_to, recall_at_k,
    write_disk_index, write_pq, DiskIndex, ManifoldModel, VecFixture,
};
use slater::vector::distance;

const DIM: usize = 768;
const K: usize = 10;
const BEAM: usize = 64;
const N_BASE: usize = 3_000;
const N_QUERIES: usize = 60;
/// Intrinsic (latent) dimensionality of the representative manifold — mirrors a real embedding's
/// effective rank, well below the ambient 768.
const N_LATENT: usize = 48;
/// Fraction of the base marked dead for the consolidation rung.
const DEAD_FRAC: f64 = 0.5;
/// New vectors folded in for the merge rung (on the SAME manifold model).
const N_INSERT: usize = 500;

fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn recall_delta(metric: Metric, raw: &[Vec<f32>], qs: &[Vec<f32>]) -> f64 {
    let mut rw = RwVamana::new(DIM, metric);
    for (i, v) in raw.iter().enumerate() {
        rw.insert(i as u64, v).unwrap();
    }
    let live: Vec<u64> = (0..raw.len() as u64).collect();
    let mut rs = Vec::new();
    for q in qs {
        let hits = rw
            .search(q, K, BEAM, |v| distance(metric, q, v) as f32, |_| Ok(true))
            .unwrap();
        let approx: Vec<u64> = hits.into_iter().map(|h| h.node_id).collect();
        let exact = exact_topk(metric, raw, &live, q, K);
        rs.push(recall_at_k(&approx, &exact));
    }
    mean(&rs)
}

fn recall_base(fx: &VecFixture, qs: &[Vec<f32>]) -> f64 {
    let live: Vec<u64> = (0..fx.raw.len() as u64).collect();
    let mut rs = Vec::new();
    for q in qs {
        let approx = fx.beam_topk_inmem(q, K, BEAM).unwrap();
        let exact = exact_topk(fx.metric, &fx.raw, &live, q, K);
        rs.push(recall_at_k(&approx, &exact));
    }
    mean(&rs)
}

fn recall_consolidated(
    dir: &std::path::Path,
    fx: &VecFixture,
    base: &DiskIndex,
    qs: &[Vec<f32>],
) -> f64 {
    // Deterministic dead mask over input ids; the live set is the survivors.
    let dead_in: Vec<bool> = (0..fx.raw.len())
        .map(|i| (i as f64 * 0.6180339).fract() < DEAD_FRAC)
        .collect();
    let live: Vec<u64> = (0..fx.raw.len() as u64)
        .filter(|&id| !dead_in[id as usize])
        .collect();

    // The dead mask in layout order (consolidate takes layout ordinals).
    let dead_layout: Vec<bool> = base
        .layout_dump_ids
        .iter()
        .map(|&d| dead_in[d as usize])
        .collect();
    let opts = consolidate_opts(fx, base.medoid);
    let cons_vamana =
        slater::vecbench::consolidate_to(dir, "cons", &base.vamana, &dead_layout, &opts).unwrap();
    // A holes `.pq` (same layout) so beam search skips the dead.
    let holes_pq = dir.join("cons.pq");
    let (order, _) = layout(fx);
    write_pq(&holes_pq, fx, &order, Some(&dead_in)).unwrap();

    let mut rs = Vec::new();
    for q in qs {
        let approx = beam_topk_disk(
            &cons_vamana,
            &holes_pq,
            base.medoid,
            fx.metric,
            fx.space_dim,
            q,
            K,
            BEAM,
            None,
        )
        .unwrap();
        let exact = exact_topk(fx.metric, &fx.raw, &live, q, K);
        rs.push(recall_at_k(&approx, &exact));
    }
    mean(&rs)
}

fn recall_merged(
    dir: &std::path::Path,
    fx: &VecFixture,
    base: &DiskIndex,
    qs: &[Vec<f32>],
    model: &ManifoldModel,
) -> f64 {
    // A batch of new vectors on the SAME manifold model, keyed by fresh dump ids after the base.
    let ins_raw = model.sample(N_INSERT, 0xADD ^ fx.metric as u64);
    let inserts: Vec<(u64, Vec<f32>)> = ins_raw
        .iter()
        .enumerate()
        .map(|(j, v)| ((fx.raw.len() + j) as u64, v.clone()))
        .collect();
    // No deletes: every base record survives with its own dump id.
    let base_final_ids = base.layout_dump_ids.clone();
    let params = merge_params(fx, base.medoid);
    let (mv, mp, stats) =
        merge_to(dir, "merged", base, &base_final_ids, &inserts, &params).unwrap();
    assert!(
        !stats.vamana_carried,
        "a merge with inserts must take the slow (rewrite) path, not the hard-link fast path"
    );

    // Live set = all base ids + the inserted ids; raw over the combined space.
    let mut raw_all = fx.raw.clone();
    raw_all.extend(ins_raw);
    let live: Vec<u64> = (0..raw_all.len() as u64).collect();

    let mut rs = Vec::new();
    for q in qs {
        // The base medoid record is emitted first in the rewrite, so its layout ordinal is the
        // merged entry point too.
        let approx = beam_topk_disk(
            &mv,
            &mp,
            base.medoid,
            fx.metric,
            fx.space_dim,
            q,
            K,
            BEAM,
            None,
        )
        .unwrap();
        let exact = exact_topk(fx.metric, &raw_all, &live, q, K);
        rs.push(recall_at_k(&approx, &exact));
    }
    mean(&rs)
}

fn main() {
    let dir = std::env::temp_dir().join(format!("slater_recall_{}", std::process::id()));
    std::fs::create_dir_all(&dir).ok();

    eprintln!("\n=== recall@{K} across the write ladder (approx vs exact brute force over the LIVE set) ===");
    eprintln!(
        "dim={DIM}, base N={N_BASE}, beam L={BEAM}, {N_QUERIES} held-out manifold queries \
         (latent {N_LATENT}, unequal norms)\n"
    );
    eprintln!(
        "{:<8}  {:>10}  {:>10}  {:>12}  {:>10}",
        "metric", "delta", "base", "consolidated", "merged"
    );

    for metric in [Metric::Cosine, Metric::L2, Metric::Dot] {
        // Representative fixture: index vectors + held-out queries sampled from ONE manifold model,
        // so a query has a genuine neighbourhood among the index points (uniform-random high-dim
        // vectors have no meaningful kNN and would make recall noise).
        let model = ManifoldModel::new(DIM, N_LATENT, 0x1234 ^ metric as u64);
        let raw = model.sample(N_BASE, 0xA1 ^ metric as u64);
        let qs = model.sample(N_QUERIES, 0x9999 ^ metric as u64);

        let r_delta = recall_delta(metric, &raw, &qs);
        let fx = VecFixture::build(metric, raw).unwrap();
        let r_base = recall_base(&fx, &qs);
        let base = write_disk_index(&dir, "base", &fx, None).unwrap();
        let r_cons = recall_consolidated(&dir, &fx, &base, &qs);
        let r_merged = recall_merged(&dir, &fx, &base, &qs, &model);

        eprintln!(
            "{:<8}  {:>10.3}  {:>10.3}  {:>12.3}  {:>10.3}",
            format!("{metric:?}"),
            r_delta,
            r_base,
            r_cons,
            r_merged
        );
    }
    eprintln!();
    std::fs::remove_dir_all(&dir).ok();
}
