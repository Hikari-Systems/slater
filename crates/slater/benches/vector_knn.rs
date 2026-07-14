// SPDX-License-Identifier: Apache-2.0
//! Microbenchmark for the brute-force vector-kNN kernel (`slater::vector`).
//!
//! This is the tight iteration loop for the kNN-latency work: it measures the
//! pure scoring + top-k selection over a synthetic vector group, decoupled from
//! the store/cache plumbing and the end-to-end Python harness
//! (`perf/cross-engine-hs/bench_vec.py`). Sizes and dimensionality mirror the
//! EU-AI-Act benchmark: 1024-dim cosine vectors, the two live groups
//! (Concept ≈ 10.5k, Chunk ≈ 4.6k), top-10.
//!
//! Run: `cargo bench -p slater --bench vector_knn`
//!
//! Data is deterministic (a fixed splitmix64 stream), so runs are comparable
//! across stages without pulling in an RNG crate.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use graph_format::manifest::Metric;
use graph_format::vectors::VectorEntry;
use slater::vector::{
    brute_force_knn, brute_force_knn_matrix, brute_force_knn_matrix_par, brute_force_knn_par,
    ResidentMatrix,
};

const DIM: usize = 1024;
const K: usize = 10;
/// Live EU-AI-Act group sizes (Concept ≈ 10.5k, Chunk ≈ 4.6k), plus the combined
/// 15.2k as an upper anchor.
const GROUPS: &[(&str, usize)] = &[
    ("chunk_4600", 4_600),
    ("concept_10500", 10_500),
    ("all_15238", 15_238),
];

/// splitmix64 — a tiny deterministic stream so the benchmark vectors are stable
/// run-to-run (and stage-to-stage) without an `rand` dependency.
struct SplitMix64(u64);
impl SplitMix64 {
    fn next_f32(&mut self) -> f32 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // Map to [-1, 1): 24 mantissa bits → f32 in [0,1), then recentre.
        let unit = (z >> 40) as f32 / (1u32 << 24) as f32;
        unit * 2.0 - 1.0
    }
}

fn make_group(n: usize, seed: u64) -> Vec<VectorEntry> {
    let mut rng = SplitMix64(seed);
    (0..n)
        .map(|i| VectorEntry {
            node_id: i as u64,
            vector: (0..DIM).map(|_| rng.next_f32()).collect(),
        })
        .collect()
}

fn make_query(seed: u64) -> Vec<f32> {
    let mut rng = SplitMix64(seed);
    (0..DIM).map(|_| rng.next_f32()).collect()
}

fn bench_knn(c: &mut Criterion) {
    let query = make_query(0xDEAD_BEEF);
    let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

    let mut seq = c.benchmark_group("brute_force_knn/cosine/k10");
    for &(name, n) in GROUPS {
        let entries = make_group(n, 0x1234_5678 ^ n as u64);
        seq.throughput(Throughput::Elements(n as u64));
        seq.bench_with_input(BenchmarkId::from_parameter(name), &entries, |b, entries| {
            b.iter(|| brute_force_knn(entries, &query, K, Metric::Cosine, None).unwrap());
        });
    }
    seq.finish();

    let mut par = c.benchmark_group("brute_force_knn_par/cosine/k10");
    for &(name, n) in GROUPS {
        let entries = make_group(n, 0x1234_5678 ^ n as u64);
        par.throughput(Throughput::Elements(n as u64));
        par.bench_with_input(BenchmarkId::from_parameter(name), &entries, |b, entries| {
            b.iter(|| {
                brute_force_knn_par(Some(&pool), entries, &query, K, Metric::Cosine, 256, None)
                    .unwrap()
            });
        });
    }
    par.finish();

    // The Stage-3 path: scan a resident, pre-normalized contiguous matrix (built
    // once). This is what a warm production query runs — no per-query gather.
    let mut mat = c.benchmark_group("brute_force_knn_matrix/cosine/k10");
    for &(name, n) in GROUPS {
        let m = ResidentMatrix::from_entries(
            DIM,
            Metric::Cosine,
            make_group(n, 0x1234_5678 ^ n as u64),
        )
        .unwrap();
        mat.throughput(Throughput::Elements(n as u64));
        mat.bench_with_input(BenchmarkId::from_parameter(name), &m, |b, m| {
            b.iter(|| brute_force_knn_matrix(m, &query, K, None).unwrap());
        });
    }
    mat.finish();

    let mut mat_par = c.benchmark_group("brute_force_knn_matrix_par/cosine/k10");
    for &(name, n) in GROUPS {
        let m = ResidentMatrix::from_entries(
            DIM,
            Metric::Cosine,
            make_group(n, 0x1234_5678 ^ n as u64),
        )
        .unwrap();
        mat_par.throughput(Throughput::Elements(n as u64));
        mat_par.bench_with_input(BenchmarkId::from_parameter(name), &m, |b, m| {
            b.iter(|| brute_force_knn_matrix_par(Some(&pool), m, &query, K, 256, None).unwrap());
        });
    }
    mat_par.finish();
}

criterion_group!(benches, bench_knn);
criterion_main!(benches);
