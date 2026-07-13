// SPDX-License-Identifier: Apache-2.0
//! Concurrent-hit throughput of the shared block cache.
//!
//! The block cache's steady state is a *resident* working set: the hot adjacency
//! blocks are already decompressed and every traversal read is a hit. What this
//! bench measures is therefore the only thing that matters for HIK-86 — how many
//! **hits per second** the cache can serve in aggregate as thread count rises.
//! A cache whose hit path takes one global exclusive lock caps that number at
//! roughly one core's worth no matter how many cores are traversing.
//!
//! Every key is pre-loaded before timing starts, so no iteration touches the
//! filesystem or zstd: the loader closure is never invoked and the measurement is
//! pure cache path (lookup + `Arc` clone + recency bookkeeping).
//!
//! `Throughput::Elements(threads)` is paired with an `iter_custom` in which each
//! of `threads` workers performs `iters` lookups, so criterion's reported
//! "elem/s" is the **aggregate** hit rate across all threads. `threads/1` is
//! also the single-threaded regression guard: its `time` is one uncontended hit.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use graph_format::blockcache::{BlockCache, BlockKey};

/// Fully resident working set: 512 blocks × 4 KiB = 2 MiB, inside a 64 MiB budget
/// (the server's `block_cache_bytes` default), so nothing is ever evicted and every
/// timed lookup is a hit.
const BUDGET: usize = 64 << 20;
const BLOCKS: u32 = 512;
const BLOCK_BYTES: usize = 4 << 10;

fn resident_cache() -> Arc<BlockCache> {
    let cache = Arc::new(BlockCache::new(BUDGET));
    for b in 0..BLOCKS {
        cache
            .get_or_try_insert(BlockKey::new(0, 0, b), || Ok(vec![b as u8; BLOCK_BYTES]))
            .unwrap();
    }
    assert_eq!(cache.len(), BLOCKS as usize);
    cache
}

/// One worker's key stream. Threads walk the block space from different offsets
/// with an odd stride, so they neither march in lockstep on one key (which would
/// measure a single cacheline, not the cache) nor partition into disjoint slices
/// (which would understate sharing).
#[inline]
fn key_for(worker: u64, i: u64) -> BlockKey {
    let b = (worker.wrapping_mul(37).wrapping_add(i.wrapping_mul(7))) % BLOCKS as u64;
    BlockKey::new(0, 0, b as u32)
}

fn hit_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("blockcache_hits");
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
                                    let k = key_for(w as u64, i);
                                    let v = cache
                                        .get_or_try_insert(k, || unreachable!("resident"))
                                        .unwrap();
                                    black_box(v[0]);
                                }
                                // The aggregate rate is bounded by the slowest worker,
                                // so take the max wall time across workers.
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
