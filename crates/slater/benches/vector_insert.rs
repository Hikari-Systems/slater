// SPDX-License-Identifier: Apache-2.0
//! FreshDiskANN insert-cost benchmark (HIK-120, feature 3).
//!
//! Times one `graph_format::rwvamana::RwVamana` insert at production shape — dim 768, and the
//! module's fixed `R = RW_R = 32` / `L = RW_L_BUILD = 64` (greedy-search + robust-prune +
//! back-link, HIK-112). This is the per-vector cost the RW-index pays to keep the write delta
//! searchable, and it is what sizes the delta-rebuild budget: a rebuild re-inserts the whole
//! delta at `~ms/insert × maxVectors`, on the read path, under the write guard (see
//! `slater::rwindex::rebuild` and `RwIndexConfig::max_vectors`).
//!
//! We report **amortized ms/insert** over building an index to a few thousand vectors: the
//! per-insert cost drifts up slowly with the live set (a wider graph to search+prune), so an
//! amortized build to N≈the `maxVectors`-relevant range is the representative single number,
//! not the cost of the very first (near-empty) insert.
//!
//! Deterministic splitmix64 vectors (like `vector_knn.rs`); release profile (criterion).
//!
//! Run: `cargo bench -p slater --features testkit --bench vector_insert`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use graph_format::manifest::Metric;
use graph_format::rwvamana::{RwVamana, RW_L_BUILD, RW_R};

/// Production embedding width (the EU-AI-Act / wikidata vector shape).
const DIM: usize = 768;

/// Build sizes: the amortized per-insert cost is reported at each. 2 000 straddles the default
/// `minVectors`; 5 000 is a realistic mid-delta.
const SIZES: &[usize] = &[2_000, 5_000];

/// splitmix64 — a tiny deterministic stream (mirrors `vector_knn.rs`), so the vectors are stable
/// run-to-run without an `rand` dependency.
struct SplitMix64(u64);
impl SplitMix64 {
    fn next_f32(&mut self) -> f32 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let unit = (z >> 40) as f32 / (1u32 << 24) as f32;
        unit * 2.0 - 1.0
    }
}

fn make_vectors(n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = SplitMix64(seed);
    (0..n)
        .map(|_| (0..DIM).map(|_| rng.next_f32()).collect())
        .collect()
}

fn bench_insert(c: &mut Criterion) {
    assert_eq!(RW_R, 32, "bench documents R=32");
    assert_eq!(RW_L_BUILD, 64, "bench documents L=64");

    let mut group = c.benchmark_group("rwvamana_insert/cosine/dim768");
    // Building a 5 000-vector index per sample is ~10 s at ~2 ms/insert; keep the sample count
    // small so the whole bench stays a few minutes. Throughput reports per-element (per-insert)
    // time directly.
    group.sample_size(10);
    for &n in SIZES {
        let vectors = make_vectors(n, 0xA11CE ^ n as u64);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &vectors, |b, vectors| {
            b.iter(|| {
                let mut rw = RwVamana::new(DIM, Metric::Cosine);
                for (i, v) in vectors.iter().enumerate() {
                    rw.insert(i as u64, v).unwrap();
                }
                criterion::black_box(rw.live_count())
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_insert);
criterion_main!(benches);
