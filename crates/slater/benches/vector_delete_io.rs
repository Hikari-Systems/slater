// SPDX-License-Identifier: Apache-2.0
//! Delete-consolidation read-IO benchmark, at **iso-recall** (HIK-120, feature 4).
//!
//! The S5 headline (HIK-114): a lazily-deleted node stays a **hole** — a record its neighbours
//! still name, so beam search fetches its block, finds it un-emittable, and moves on. At a
//! *fixed* beam width a hole costs a beam slot, not a fetch, so a fixed-width comparison shows a
//! flat line and proves nothing. The cost surfaces at **iso-recall**: holes crowd the candidate
//! list with dead ends, so to hit a recall target the lazy index needs a **wider** beam — and a
//! wider beam fetches more nodes. Consolidation splices holes out of live adjacency, so no live
//! node ever names a dead one and the target is met at a narrower beam.
//!
//! So the measurement fixes a recall target and, for each variant, finds the **smallest beam
//! width** that reaches it, then reports the **node fetches per query** there. A "fetch" is one
//! `beam_search` node expansion — the DiskANN IO unit (one node = one random block read); it is
//! counted exactly via the fetch closure (see `vecbench::beam_topk_disk`).
//!
//! Recall is measured against an exact brute force over the **live** set. Cosine only (the IO
//! behaviour is metric-agnostic — the graph structure, not the distance, decides what is
//! fetched). Cold: each `beam_topk_disk` opens the readers fresh.
//!
//! Run: `cargo bench -p slater --features testkit --bench vector_delete_io`

use std::cell::Cell;
use std::path::Path;

use graph_format::manifest::Metric;

use slater::vecbench::{
    beam_topk_disk, consolidate_opts, consolidate_to, exact_topk, layout, recall_at_k,
    write_disk_index, write_pq, ManifoldModel, VecFixture,
};

const DIM: usize = 768;
const K: usize = 10;
const N_TOTAL: usize = 6_000;
const N_QUERIES: usize = 40;
/// Intrinsic (latent) dimensionality of the representative manifold fixture.
const N_LATENT: usize = 48;
const RECALL_TARGET: f64 = 0.90;
/// Dead fractions to sweep.
const DEAD_FRACS: &[f64] = &[0.0, 0.25, 0.50, 0.67, 0.80];
/// Beam-width ladder searched for the smallest L meeting `RECALL_TARGET`.
const BEAM_LADDER: &[usize] = &[16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512];

/// Average (recall, fetches/query) over the query set for one on-disk index at beam `L`.
#[allow(clippy::too_many_arguments)]
fn measure(
    vamana: &Path,
    pq: &Path,
    medoid: u32,
    space_dim: usize,
    qs: &[Vec<f32>],
    raw: &[Vec<f32>],
    live: &[u64],
    l: usize,
) -> (f64, f64) {
    let mut recall_sum = 0.0;
    let mut fetch_sum = 0u64;
    for q in qs {
        let fetch = Cell::new(0u64);
        let approx = beam_topk_disk(
            vamana,
            pq,
            medoid,
            Metric::Cosine,
            space_dim,
            q,
            K,
            l,
            Some(&fetch),
        )
        .unwrap();
        let exact = exact_topk(Metric::Cosine, raw, live, q, K);
        recall_sum += recall_at_k(&approx, &exact);
        fetch_sum += fetch.get();
    }
    (
        recall_sum / qs.len() as f64,
        fetch_sum as f64 / qs.len() as f64,
    )
}

/// The smallest beam width in the ladder that reaches `RECALL_TARGET`, and the fetches/query
/// there. Returns the widest-tried result flagged if the target is never met.
fn iso_recall(
    vamana: &Path,
    pq: &Path,
    medoid: u32,
    space_dim: usize,
    qs: &[Vec<f32>],
    raw: &[Vec<f32>],
    live: &[u64],
) -> (usize, f64, f64, bool) {
    let mut last = (0usize, 0.0, 0.0);
    for &l in BEAM_LADDER {
        let (recall, fetches) = measure(vamana, pq, medoid, space_dim, qs, raw, live, l);
        last = (l, recall, fetches);
        if recall >= RECALL_TARGET {
            return (l, recall, fetches, true);
        }
    }
    (last.0, last.1, last.2, false)
}

fn main() {
    let dir = std::env::temp_dir().join(format!("slater_deleteio_{}", std::process::id()));
    std::fs::create_dir_all(&dir).ok();

    // One base fixture; the dead mask varies per fraction over the same vectors. Representative
    // manifold data + held-out queries on the same model (uniform-random high-dim vectors have no
    // meaningful kNN, so the iso-recall target would be unreachable and the reads meaningless).
    let model = ManifoldModel::new(DIM, N_LATENT, 0xDE1E7E);
    let raw = model.sample(N_TOTAL, 0xC0DE);
    let fx = VecFixture::build(Metric::Cosine, raw).unwrap();
    let qs = model.sample(N_QUERIES, 0x0071C5);
    let base = write_disk_index(&dir, "base", &fx, None).unwrap();
    let (order, _) = layout(&fx);

    eprintln!("\n=== delete-IO at iso-recall (target recall@{K} ≥ {RECALL_TARGET}) ===");
    eprintln!(
        "dim={DIM}, N={N_TOTAL}, {N_QUERIES} queries; fetches = node expansions/query (the DiskANN IO unit)\n"
    );
    eprintln!(
        "{:>6}  {:>6}  | {:>8} {:>8} {:>8} | {:>8} {:>8} {:>8} | {:>8}",
        "dead%",
        "live",
        "lazy_L",
        "lazy_rec",
        "lazy_rd",
        "cons_L",
        "cons_rec",
        "cons_rd",
        "reads_x"
    );

    for &f in DEAD_FRACS {
        let dead_in: Vec<bool> = (0..N_TOTAL)
            .map(|i| (i as f64 * 0.6180339).fract() < f)
            .collect();
        let live: Vec<u64> = (0..N_TOTAL as u64)
            .filter(|&id| !dead_in[id as usize])
            .collect();

        // A holes `.pq` over the base layout (same for lazy and consolidated).
        let holes_pq = dir.join(format!("holes_{}.pq", (f * 100.0) as u32));
        write_pq(&holes_pq, &fx, &order, Some(&dead_in)).unwrap();

        // lazy: base `.vamana` (full adjacency still names the holes) + holes `.pq`.
        let (lz_l, lz_rec, lz_rd, lz_ok) = iso_recall(
            &base.vamana,
            &holes_pq,
            base.medoid,
            fx.space_dim,
            &qs,
            &fx.raw,
            &live,
        );

        // consolidated: adjacency spliced so no live node names a hole.
        let dead_layout: Vec<bool> = base
            .layout_dump_ids
            .iter()
            .map(|&d| dead_in[d as usize])
            .collect();
        let opts = consolidate_opts(&fx, base.medoid);
        let cons_vamana = consolidate_to(
            &dir,
            &format!("cons_{}", (f * 100.0) as u32),
            &base.vamana,
            &dead_layout,
            &opts,
        )
        .unwrap();
        let (cn_l, cn_rec, cn_rd, cn_ok) = iso_recall(
            &cons_vamana,
            &holes_pq,
            base.medoid,
            fx.space_dim,
            &qs,
            &fx.raw,
            &live,
        );

        let ratio = if cn_rd > 0.0 { lz_rd / cn_rd } else { 0.0 };
        eprintln!(
            "{:>5.0}%  {:>6}  | {:>8} {:>8.3} {:>8.1} | {:>8} {:>8.3} {:>8.1} | {:>7.2}x{}",
            f * 100.0,
            live.len(),
            lz_l,
            lz_rec,
            lz_rd,
            cn_l,
            cn_rec,
            cn_rd,
            ratio,
            if lz_ok && cn_ok {
                ""
            } else {
                " (target not met at max L)"
            }
        );
    }
    eprintln!();
    std::fs::remove_dir_all(&dir).ok();
}
