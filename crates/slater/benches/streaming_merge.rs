// SPDX-License-Identifier: Apache-2.0
//! StreamingMerge rewrite-throughput benchmark (HIK-120, feature 5).
//!
//! A core consolidation folds the delta's inserts/deletes into the base `.vamana`/`.pq` via
//! `graph_format::vamana_merge::streaming_merge`, which has two regimes:
//!
//!   * **fast path** — a pure permutation (no new dead, no inserts): the `.vamana` is carried by
//!     reference (hard-link, byte-identical) and only the small `.pq` id column is rewritten.
//!     O(1) in the graph size — ~instant regardless of file size (`MergeStats::vamana_carried`).
//!   * **slow path** — any insert or new tombstone forces a sequential rewrite: `emit_merged`
//!     decodes each block **once** and re-emits it (the HIK-119 fix — the old path re-inflated
//!     each block ~20× per record). This is the throughput-critical pass; we isolate it with a
//!     **Δ=1 insert** so the measured cost is the sequential rewrite, not insert work.
//!
//! We report **MiB/s** of the slow-path rewrite (base `.vamana` bytes / wall time) and the
//! fast-path wall time, plus the 370 GB core extrapolation (1 rewrite pass for an insert-only
//! consolidation, 2 for a delete consolidation). The base graph is a **real** `build_vamana`
//! (navigable, not random adjacency — a large-Δ merge over a locality-free graph is the known
//! OOM/pathology trap; Δ=1 over a real graph sidesteps it). Throughput is size-independent, so a
//! modest base suffices. Release profile (criterion).
//!
//! Run: `cargo bench -p slater --features testkit --bench streaming_merge`

use std::time::Instant;

use criterion::{Criterion, Throughput};

use graph_format::manifest::Metric;

use slater::vecbench::{
    layout, merge_params, merge_to, random_vectors_unequal_norms, write_disk_index, DiskIndex,
    VecFixture,
};

const DIM: usize = 768;
/// A modest base: throughput is size-independent, and `build_vamana` at 25k is the setup cost.
const N_BASE: usize = 25_000;
/// The full-core extrapolation target (bytes). 370 GB of `.vamana`.
const CORE_BYTES: f64 = 370.0 * 1024.0 * 1024.0 * 1024.0;

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.total_cmp(b));
    xs[xs.len() / 2]
}

struct Bench {
    dir: std::path::PathBuf,
    fx: VecFixture,
    base: DiskIndex,
    base_final_ids: Vec<u64>,
    vamana_bytes: u64,
}

fn setup() -> Bench {
    let dir = std::env::temp_dir().join(format!("slater_merge_{}", std::process::id()));
    std::fs::create_dir_all(&dir).ok();
    let raw = random_vectors_unequal_norms(N_BASE, DIM, 0x11EE7);
    let fx = VecFixture::build(Metric::Cosine, raw).unwrap();
    let base = write_disk_index(&dir, "base", &fx, None).unwrap();
    let vamana_bytes = std::fs::metadata(&base.vamana).unwrap().len();
    let base_final_ids = base.layout_dump_ids.clone();
    Bench {
        dir,
        fx,
        base,
        base_final_ids,
        vamana_bytes,
    }
}

/// One fast-path merge (no inserts, no dead → hard-link). Returns whether the fast path fired.
fn run_fast(b: &Bench) -> bool {
    let (_, _, stats) = merge_to(
        &b.dir,
        "fast_out",
        &b.base,
        &b.base_final_ids,
        &[],
        &merge_params(&b.fx, b.base.medoid),
    )
    .unwrap();
    stats.vamana_carried
}

/// One slow-path merge (Δ=1 insert → sequential rewrite). Returns whether the slow path fired.
fn run_slow(b: &Bench, insert: &[(u64, Vec<f32>)]) -> bool {
    let (_, _, stats) = merge_to(
        &b.dir,
        "slow_out",
        &b.base,
        &b.base_final_ids,
        insert,
        &merge_params(&b.fx, b.base.medoid),
    )
    .unwrap();
    !stats.vamana_carried
}

fn main() {
    let b = setup();
    let (_order, _medoid) = layout(&b.fx);
    let insert_raw = random_vectors_unequal_norms(1, DIM, 0xDEAD);
    let insert: Vec<(u64, Vec<f32>)> = vec![(N_BASE as u64, insert_raw[0].clone())];

    let mib = b.vamana_bytes as f64 / (1024.0 * 1024.0);
    eprintln!("\n=== StreamingMerge rewrite throughput ===");
    eprintln!("base N={N_BASE}, .vamana = {:.1} MiB (dim {DIM})\n", mib);

    // Fast path — median wall time over reps (hard-link + tiny .pq id rewrite).
    assert!(
        run_fast(&b),
        "no-insert no-dead merge must take the fast path"
    );
    let mut fast = Vec::new();
    for _ in 0..7 {
        let t = Instant::now();
        assert!(run_fast(&b));
        fast.push(t.elapsed().as_secs_f64());
    }
    let fast_med = median(fast);
    eprintln!(
        "fast path (hard-link):  {:.2} ms  → {:.0} MiB/s effective ({}‑independent, carries the .vamana by reference)",
        fast_med * 1e3,
        mib / fast_med,
        "size"
    );

    // Slow path — median MiB/s over reps (decode-once sequential rewrite, Δ=1).
    assert!(
        run_slow(&b, &insert),
        "a Δ=1 insert must take the slow path"
    );
    let mut slow = Vec::new();
    for _ in 0..7 {
        let t = Instant::now();
        assert!(run_slow(&b, &insert));
        slow.push(t.elapsed().as_secs_f64());
    }
    let slow_med = median(slow);
    let mibps = mib / slow_med;
    eprintln!(
        "slow path (rewrite):    {:.0} ms  → {:.0} MiB/s (decode-once emit, post-HIK-119)",
        slow_med * 1e3,
        mibps
    );

    // 370 GB core extrapolation.
    let secs_1pass = CORE_BYTES / (mibps * 1024.0 * 1024.0);
    eprintln!(
        "\n370 GB core extrapolation @ {:.0} MiB/s:  insert consolidation (1 pass) ≈ {:.0} s ({:.1} min); \
         delete consolidation (2 passes) ≈ {:.0} s ({:.1} min)\n",
        mibps,
        secs_1pass,
        secs_1pass / 60.0,
        2.0 * secs_1pass,
        2.0 * secs_1pass / 60.0
    );

    // Criterion groups for the re-runnable harness (throughput reported as bytes/s = MiB/s).
    let mut c = Criterion::default().configure_from_args();
    let mut g = c.benchmark_group("streaming_merge/rewrite");
    g.sample_size(10);
    g.throughput(Throughput::Bytes(b.vamana_bytes));
    g.bench_function("slow_path_delta1", |bencher| {
        bencher.iter(|| criterion::black_box(run_slow(&b, &insert)));
    });
    g.bench_function("fast_path_hardlink", |bencher| {
        bencher.iter(|| criterion::black_box(run_fast(&b)));
    });
    g.finish();
    c.final_summary();

    std::fs::remove_dir_all(&b.dir).ok();
}
