// SPDX-License-Identifier: Apache-2.0
//! Concurrent-hit throughput of the ISAM `DecodedBlockCache` (HIK-106).
//!
//! Mirrors `benches/blockcache.rs` for the second cache the same contention shape
//! afflicted. The decoded-block cache's steady state is a *resident* working set:
//! the hot ISAM leaves are already decoded and every indexed-seek / bulk-resolve
//! probe is a hit. What matters is therefore how many **hits per second** the cache
//! serves in aggregate as thread count rises. A hit path on one global exclusive
//! lock (the pre-HIK-106 shape) caps that at roughly one core's worth no matter how
//! many cores are seeking.
//!
//! Every key is pre-loaded before timing, so the loader closure is never invoked and
//! the measurement is the pure cache path (read-lock lookup + `Arc` clone + a single
//! relaxed atomic store on the entry's CLOCK bit).
//!
//! `Throughput::Elements(threads)` paired with `iter_custom` makes criterion's
//! reported "elem/s" the **aggregate** hit rate across all threads; `threads/1` is
//! also the single-threaded regression guard (one uncontended hit).

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use graph_format::ids::Value;
use graph_format::isam::DecodedBlockCache;

/// Fully resident working set: 512 decoded leaves × 64 `(Value, u64)` pairs, inside a
/// 64 MiB budget (well above the 16 MiB default), so nothing is ever evicted and every
/// timed lookup is a hit.
const BUDGET: usize = 64 << 20;
const BLOCKS: u32 = 512;
const PAIRS: u64 = 64;

fn resident_cache() -> Arc<DecodedBlockCache> {
    let cache = Arc::new(DecodedBlockCache::new(BUDGET));
    for b in 0..BLOCKS {
        cache
            .get_or_load(0, b, || {
                Ok((0..PAIRS).map(|i| (Value::Int(i as i64), i)).collect())
            })
            .unwrap();
    }
    assert_eq!(cache.len(), BLOCKS as usize);
    cache
}

/// One worker's key stream: threads walk the block space from different offsets with
/// an odd stride, so they neither march in lockstep on one key nor partition into
/// disjoint slices.
#[inline]
fn block_for(worker: u64, i: u64) -> u32 {
    ((worker.wrapping_mul(37).wrapping_add(i.wrapping_mul(7))) % BLOCKS as u64) as u32
}

fn hit_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("decodedblockcache_hits");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    for threads in [1usize, 4, 16] {
        group.throughput(Throughput::Elements(threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let cache = resident_cache();
                b.iter_custom(|iters| {
                    let barrier = Arc::new(Barrier::new(threads + 1));
                    let nanos = Arc::new(AtomicU64::new(0));
                    let handles: Vec<_> = (0..threads)
                        .map(|w| {
                            let cache = cache.clone();
                            let barrier = barrier.clone();
                            let nanos = nanos.clone();
                            std::thread::spawn(move || {
                                barrier.wait();
                                let start = Instant::now();
                                for i in 0..iters {
                                    let blk = block_for(w as u64, i);
                                    let v = cache
                                        .get_or_load(0, blk, || unreachable!("resident"))
                                        .unwrap();
                                    black_box(v.len());
                                }
                                // Aggregate rate is bounded by the slowest worker.
                                nanos
                                    .fetch_max(start.elapsed().as_nanos() as u64, Ordering::SeqCst);
                            })
                        })
                        .collect();
                    barrier.wait();
                    for h in handles {
                        h.join().unwrap();
                    }
                    Duration::from_nanos(nanos.load(Ordering::SeqCst))
                });
                let m = cache.metrics();
                assert_eq!(m.misses, BLOCKS as u64, "timed lookups must all be hits");
            },
        );
    }
    group.finish();
}

criterion_group!(benches, hit_throughput);
criterion_main!(benches);
