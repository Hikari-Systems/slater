// SPDX-License-Identifier: Apache-2.0
//! Shared byte-budgeted LRU over decompressed blocks.
//!
//! Every `.blk` file is a sequence of zstd blocks; the readers fetch a block
//! with one `pread` + decompress per access (no mmap — see the blockfile docs).
//! Repeated reads of the same block would otherwise re-`pread` and re-decompress
//! every time — from a hot adjacency block during query traversal to, at the
//! other extreme, a *sequential* whole-file scan where a naive per-record reader
//! would redundantly re-decompress each block once per record it holds (see
//! [`crate::blockfile::BlockFileReader::for_each_record`]'s docs). This LRU holds
//! **decompressed** block bytes keyed by a caller-defined `(scope, sub, block)`
//! triple under a global byte budget — bounded memory regardless of how many
//! distinct blocks exist, from a large multi-generation server cache down to a
//! tiny few-block window for a strictly-ascending build-time scan.
//!
//! `scope`/`sub` are opaque to this module — a caller distinguishes generations,
//! named stores, or anything else it needs by picking distinct values; this
//! module only ever compares them for equality via the `BlockKey` they compose.
//!
//! [`record`](BlockCache::record) returns a [`BlockRecord`] that borrows the
//! cached block by `Arc` rather than copying the record out.
//!
//! Eviction order is LRU, tracked with a monotonic tick and a `BTreeMap` ordering
//! (O(log n) per access) — simple and obviously correct, which matters more here
//! than shaving a constant factor off a HashMap-list LRU.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::blockfile::{record_range_in_block, BlockFileReader};

/// A record sliced out of a cached, decompressed block. Holds an `Arc` clone of
/// the block so the borrowed bytes stay alive, plus the record's byte range
/// within it. Copies nothing — cloning the `Arc` is one atomic increment.
#[derive(Clone)]
pub struct BlockRecord {
    block: Arc<Vec<u8>>,
    start: usize,
    end: usize,
}

impl BlockRecord {
    /// Build a `BlockRecord` from an already-fetched block and a record's byte
    /// range within it — for a caller with its own cache pool (e.g. a second,
    /// differently-keyed LRU) that still wants to return this same borrowed-slice
    /// type at its call sites.
    pub fn new(block: Arc<Vec<u8>>, start: usize, end: usize) -> Self {
        Self { block, start, end }
    }

    /// The record bytes (borrowing the cached block).
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.block[self.start..self.end]
    }
}

impl std::ops::Deref for BlockRecord {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for BlockRecord {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::fmt::Debug for BlockRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockRecord")
            .field("len", &(self.end - self.start))
            .finish()
    }
}

/// LRU key for one decompressed block: a caller-defined `scope` (e.g. a
/// generation id) and `sub` (e.g. which named store within it), plus the block
/// index. graph-format has no notion of "generation" or "named store" itself —
/// callers sharing one `BlockCache` must agree on non-colliding `(scope, sub)`
/// values (a build scanning a single file for its own lifetime can just use
/// `(0, 0)`; the server keys on the generation UUID and a per-file discriminant).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub scope: u128,
    pub sub: u32,
    pub block: u32,
}

impl BlockKey {
    pub fn new(scope: u128, sub: u32, block: u32) -> Self {
        Self { scope, sub, block }
    }
}

/// A point-in-time snapshot of the cache counters (for metrics/logging).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

struct Entry {
    value: Arc<Vec<u8>>,
    bytes: usize,
    tick: u64,
    /// Wall-clock instant of the most recent access; reset on every touch and
    /// consulted by an idle-TTL sweep. Assigned together with `tick`, so the
    /// `order` map (keyed by tick) is also sorted by `last_used`.
    last_used: Instant,
}

struct Inner {
    map: HashMap<BlockKey, Entry>,
    /// `tick → key`, ascending — the front is the least-recently-used entry.
    order: BTreeMap<u64, BlockKey>,
    tick: u64,
    bytes: usize,
    budget: usize,
}

impl Inner {
    fn next_tick(&mut self) -> u64 {
        let t = self.tick;
        self.tick += 1;
        t
    }

    /// Look a key up and, on a hit, move it to most-recently-used.
    fn touch_get(&mut self, key: &BlockKey) -> Option<Arc<Vec<u8>>> {
        let (value, old_tick) = {
            let e = self.map.get(key)?;
            (e.value.clone(), e.tick)
        };
        self.order.remove(&old_tick);
        let new_tick = self.next_tick();
        self.order.insert(new_tick, *key);
        let e = self.map.get_mut(key).unwrap();
        e.tick = new_tick;
        e.last_used = Instant::now();
        Some(value)
    }

    /// Evict entries idle for at least `ttl`, walking the LRU order front-to-back
    /// and stopping at the first still-live entry. Returns the count evicted.
    /// Unlike budget eviction this has no keep-at-least-one floor — an entirely
    /// idle cache is fully reclaimed.
    fn evict_expired(&mut self, now: Instant, ttl: Duration) -> u64 {
        let mut evicted = 0;
        while let Some((&t, &key)) = self.order.iter().next() {
            if now.saturating_duration_since(self.map[&key].last_used) <= ttl {
                break;
            }
            self.order.remove(&t);
            if let Some(e) = self.map.remove(&key) {
                self.bytes -= e.bytes;
            }
            evicted += 1;
        }
        evicted
    }

    /// Insert `value` for `key` (or return the existing entry if a concurrent
    /// load beat us to it), then evict LRU entries until within budget. Returns
    /// the canonical `Arc` and the number of entries evicted.
    fn insert(&mut self, key: BlockKey, value: Arc<Vec<u8>>) -> (Arc<Vec<u8>>, u64) {
        if let Some(existing) = self.touch_get(&key) {
            return (existing, 0);
        }
        let bytes = value.len();
        let tick = self.next_tick();
        self.order.insert(tick, key);
        self.map.insert(
            key,
            Entry {
                value: value.clone(),
                bytes,
                tick,
                last_used: Instant::now(),
            },
        );
        self.bytes += bytes;

        // Evict from the LRU end. Keep at least one entry resident so a single
        // block larger than the whole budget is still returnable.
        let mut evicted = 0;
        while self.bytes > self.budget && self.order.len() > 1 {
            let (&lru_tick, &lru_key) = self.order.iter().next().unwrap();
            self.order.remove(&lru_tick);
            if let Some(e) = self.map.remove(&lru_key) {
                self.bytes -= e.bytes;
            }
            evicted += 1;
        }
        (value, evicted)
    }
}

/// Byte-budgeted LRU over decompressed blocks, safe to share across threads.
pub struct BlockCache {
    inner: Mutex<Inner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl BlockCache {
    /// Create a cache with the given byte budget (clamped to at least 1).
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: BTreeMap::new(),
                tick: 0,
                bytes: 0,
                budget: budget_bytes.max(1),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Fetch a block from the cache, loading it with `load` on a miss. The load
    /// runs **outside** the lock so a slow `pread`+decompress never serialises
    /// other readers; if two readers miss the same key at once they both load and
    /// the second insert deduplicates to the first's `Arc`.
    pub fn get_or_try_insert(
        &self,
        key: BlockKey,
        load: impl FnOnce() -> Result<Vec<u8>>,
    ) -> Result<Arc<Vec<u8>>> {
        if let Some(v) = self.inner.lock().unwrap().touch_get(&key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(v);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let value = Arc::new(load()?);
        let (canonical, evicted) = self.inner.lock().unwrap().insert(key, value);
        if evicted > 0 {
            self.evictions.fetch_add(evicted, Ordering::Relaxed);
        }
        Ok(canonical)
    }

    /// Read the `global`-th record of `reader` (identified by the caller's
    /// `scope`/`sub`) through the cache: locate the block, fetch it (cached),
    /// then slice the record out of the already-decompressed block.
    pub fn record(
        &self,
        reader: &BlockFileReader,
        scope: u128,
        sub: u32,
        global: u64,
    ) -> Result<BlockRecord> {
        let loc = reader.locate(global)?;
        let key = BlockKey::new(scope, sub, loc.block.0);
        let raw = self.get_or_try_insert(key, || reader.read_block(loc.block))?;
        let range = record_range_in_block(&raw[..], loc.slot)?;
        Ok(BlockRecord {
            block: raw,
            start: range.start,
            end: range.end,
        })
    }

    /// Evict every block idle for at least `ttl` as of `now`, freeing its bytes.
    /// Returns the number evicted; the count is folded into the `evictions`
    /// counter.
    pub fn evict_expired(&self, now: Instant, ttl: Duration) -> u64 {
        let evicted = self.inner.lock().unwrap().evict_expired(now, ttl);
        if evicted > 0 {
            self.evictions.fetch_add(evicted, Ordering::Relaxed);
        }
        evicted
    }

    /// Counter snapshot.
    pub fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Current number of cached blocks.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Current resident byte usage (sum of cached block sizes).
    pub fn bytes(&self) -> usize {
        self.inner.lock().unwrap().bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockfile::{BlockFileReader, BlockFileWriter};

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "graph_format_blockcache_test_{}_{}",
            std::process::id(),
            name
        ))
    }

    fn build_store(path: &std::path::Path, n: u32) {
        let mut w = BlockFileWriter::create(path, 256, 1).unwrap();
        for i in 0..n {
            w.append_record(format!("rec{i}").as_bytes()).unwrap();
        }
        w.finish().unwrap();
    }

    #[test]
    fn hits_on_repeated_lookups_in_same_block() {
        let path = tmp("hits");
        build_store(&path, 200);
        let reader = BlockFileReader::open(&path).unwrap();
        let cache = BlockCache::new(1 << 20);

        // Two lookups landing in the same block: first misses, second hits.
        let a = cache.record(&reader, 0, 0, 0).unwrap();
        let b = cache.record(&reader, 0, 0, 1).unwrap();
        assert_eq!(&*a, b"rec0");
        assert_eq!(&*b, b"rec1");
        let m = cache.metrics();
        assert_eq!(m.misses, 1, "same block should decompress once");
        assert_eq!(m.hits, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn distinct_scopes_do_not_collide() {
        let path = tmp("scopes");
        build_store(&path, 10);
        let reader = BlockFileReader::open(&path).unwrap();
        let cache = BlockCache::new(1 << 20);

        cache.record(&reader, 1, 0, 0).unwrap();
        cache.record(&reader, 2, 0, 0).unwrap();
        // Same underlying block, different scope — both are misses, not a hit.
        assert_eq!(cache.metrics().misses, 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn budget_evicts_lru_but_keeps_at_least_one() {
        let path = tmp("evict");
        build_store(&path, 4000); // many blocks at block_size=256
        let reader = BlockFileReader::open(&path).unwrap();
        let cache = BlockCache::new(1); // tiniest possible budget

        for i in 0..3000u64 {
            cache.record(&reader, 0, 0, i).unwrap();
        }
        assert!(
            cache.len() <= 2,
            "tiny budget should keep ~1 block resident"
        );
        assert!(cache.metrics().evictions > 0);
        let _ = std::fs::remove_file(&path);
    }
}
