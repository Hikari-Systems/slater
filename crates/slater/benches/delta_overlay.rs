// SPDX-License-Identifier: Apache-2.0
//! Empty-delta read-regression benchmark for the writable layer.
//!
//! The writable layer threads every read through the [`ReadView`] seam so a
//! `MergedView` can overlay a delta on the immutable core. Phase 0's contract is
//! that a graph with **no** writes pays nothing for this: reads over the core stay
//! exactly as fast as before. Because `Engine` is generic over the view
//! (`Engine<'_, V>`), the read-only path monomorphises to `Engine<'_, Generation>`
//! — the same codegen as before the seam existed — and the empty-delta path is
//! `Engine<'_, MergedView>`, whose forwards inline to the core.
//!
//! This bench proves that empirically: it A/Bs a node-materialisation loop (the
//! per-node `node_record`, the hot read the property overlay will wrap in Phase 1)
//! over the two views against the *same* underlying generation. The two arms
//! should sit within measurement noise of each other.
//!
//! Run: `cargo bench -p slater --features testkit --bench delta_overlay`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use slater::cache::BlockCache;
use slater::exec::Engine;
use slater::generation::Generation;
use slater::read_view::MergedView;
use slater::testgen;

/// Node counts spanning several blocks so the loop reflects real block-cache
/// traffic rather than a single resident block.
const SIZES: &[u64] = &[1_000, 10_000];

/// Materialise every node's record through `engine`, summing a byte of each to
/// keep the work observable to the optimiser.
fn materialise_all<V: slater::read_view::ReadView>(engine: &Engine<'_, V>, n: u64) -> usize {
    let mut acc = 0usize;
    for id in 0..n {
        let (labels, props) = engine.node_record(id).expect("node_record");
        acc = acc.wrapping_add(labels.len()).wrapping_add(props.len());
    }
    acc
}

fn bench_empty_delta(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_materialise");
    for &n in SIZES {
        let (root, graph) = testgen::write_wide("delta_overlay_bench", n);
        let gen = Generation::open(&root, &graph).expect("open generation");
        // A generous cache so the two arms differ only in the view seam, not in
        // eviction behaviour.
        let cache = BlockCache::new(64 << 20);

        group.throughput(Throughput::Elements(n));

        // Baseline: the read-only path (monomorphises to `Engine<'_, Generation>`).
        group.bench_with_input(BenchmarkId::new("core", n), &n, |b, &n| {
            b.iter(|| {
                let engine = Engine::new(&gen, &cache);
                criterion::black_box(materialise_all(&engine, n))
            });
        });

        // Empty delta: the same reads through a `MergedView` with nothing to
        // overlay — must sit within noise of `core`.
        let merged = MergedView::read_only(&gen);
        group.bench_with_input(BenchmarkId::new("empty_delta", n), &n, |b, &n| {
            b.iter(|| {
                let engine = Engine::new(&merged, &cache);
                criterion::black_box(materialise_all(&engine, n))
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_empty_delta);
criterion_main!(benches);
