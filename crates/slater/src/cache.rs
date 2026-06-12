// SPDX-License-Identifier: Apache-2.0
//! Bounded block cache — the decompressed-block LRU.
//!
//! Every `.blk` file is a sequence of zstd blocks; the readers fetch a block
//! with one `pread` + decompress per access (no mmap — D16/the blockfile docs).
//! Repeated reads of the same block (a hot adjacency block during traversal, the
//! property block of a popular node) would otherwise re-`pread` and re-decompress
//! every time. This LRU holds **decompressed** block bytes keyed by
//! `(generation, file, block)` under a global byte budget, so resident memory is
//! bounded regardless of graph size while hot blocks stay warm.
//!
//! `graph_format::blockfile` exposes `parse_block` + `record_from_block` precisely
//! so a cache holder can slice an individual record out of a cached decompressed
//! block without decompressing again; [`BlockCache::record`] is that path.
//!
//! Eviction order is LRU, tracked with a monotonic tick and a `BTreeMap` ordering
//! (O(log n) per access) — simple and obviously correct, which matters more here
//! than shaving a constant factor off a HashMap-list LRU.
//
// Consumed by the executor from M4.5; allow dead_code for the standalone cache
// until those call sites land.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use graph_format::blockfile::{parse_block, record_from_block, BlockFileReader};
use graph_format::ids::Generation as GenId;
use graph_format::pq::ResidentPq;

/// Identifies which file within a generation a block belongs to. Encoded into a
/// `u32` for the cache key; range indexes carry their MANIFEST position in the
/// low bits behind a flag so they never collide with the fixed files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    NodeProps,
    NodeLabels,
    EdgeProps,
    Topology,
    Vectors,
    /// The `i`-th range index in `Manifest::range_indexes`.
    Range(u16),
}

const RANGE_FLAG: u32 = 0x8000_0000;

impl FileKind {
    /// Stable per-file code used in the cache key.
    pub fn code(self) -> u32 {
        match self {
            FileKind::NodeProps => 0,
            FileKind::NodeLabels => 1,
            FileKind::EdgeProps => 2,
            FileKind::Topology => 3,
            FileKind::Vectors => 4,
            FileKind::Range(i) => RANGE_FLAG | i as u32,
        }
    }
}

/// LRU key for one decompressed block.
///
/// `gen` is the generation UUID as a `u128`. Generation UUIDs are globally
/// unique, so the UUID alone subsumes the `(graph, generation)` pair from the
/// plan — two graphs can never share a generation id — and a generation swap
/// changes the UUID, which orphans every stale entry for free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub gen: u128,
    pub file: u32,
    pub block: u32,
}

impl BlockKey {
    pub fn new(gen: GenId, file: FileKind, block: u32) -> Self {
        Self {
            gen: gen.0.as_u128(),
            file: file.code(),
            block,
        }
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
    /// consulted by the idle-TTL sweep. Assigned together with `tick`, so the
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

/// Byte-budgeted LRU over decompressed blocks, safe to share across Bolt tasks.
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

    /// Read the `global`-th record of `reader` (the file identified by `file` in
    /// generation `gen`) through the cache: locate the block, fetch it (cached),
    /// then slice the record out of the already-decompressed block.
    pub fn record(
        &self,
        reader: &BlockFileReader,
        gen: GenId,
        file: FileKind,
        global: u64,
    ) -> Result<Vec<u8>> {
        let loc = reader.locate(global)?;
        let key = BlockKey::new(gen, file, loc.block.0);
        let raw = self.get_or_try_insert(key, || reader.read_block(loc.block))?;
        let (offsets, data) = parse_block(&raw[..])?;
        let rec = record_from_block(&offsets, data, loc.slot)?;
        Ok(rec.to_vec())
    }

    /// Evict every block idle for at least `ttl` as of `now`, freeing its bytes.
    /// Returns the number evicted; the count is folded into the `evictions`
    /// counter. Called by the background cache-maintenance task.
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

// ── Result cache (the third pool) ─────────────────────────────────────────────

/// Key for a cached query result: the generation UUID plus a normalised query
/// (the query text + serialised parameters). The generation UUID is part of the
/// key on purpose — a generation swap mints a new UUID, so every entry for the old
/// generation is orphaned for free and a stale result can never be served (the
/// same "gen UUID in key → swap orphans stale" trick the block LRU uses, D18).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ResultKey {
    pub gen: u128,
    pub query: String,
}

impl ResultKey {
    /// Build a key from a generation and an already-normalised query string
    /// (collapsed whitespace + serialised params — see `server::result_key`).
    pub fn new(gen: GenId, query: impl Into<String>) -> Self {
        Self {
            gen: gen.0.as_u128(),
            query: query.into(),
        }
    }
}

struct ResultEntry<V> {
    value: Arc<V>,
    bytes: usize,
    tick: u64,
    last_used: Instant,
}

struct ResultInner<V> {
    map: HashMap<ResultKey, ResultEntry<V>>,
    order: BTreeMap<u64, ResultKey>,
    tick: u64,
    bytes: usize,
    budget: usize,
}

impl<V> ResultInner<V> {
    fn next_tick(&mut self) -> u64 {
        let t = self.tick;
        self.tick += 1;
        t
    }

    fn touch_get(&mut self, key: &ResultKey) -> Option<Arc<V>> {
        let (value, old_tick) = {
            let e = self.map.get(key)?;
            (e.value.clone(), e.tick)
        };
        self.order.remove(&old_tick);
        let new_tick = self.next_tick();
        self.order.insert(new_tick, key.clone());
        let e = self.map.get_mut(key).unwrap();
        e.tick = new_tick;
        e.last_used = Instant::now();
        Some(value)
    }

    /// Evict results idle for at least `ttl`; see `Inner::evict_expired`.
    fn evict_expired(&mut self, now: Instant, ttl: Duration) -> u64 {
        let mut evicted = 0;
        while let Some((&t, key)) = self.order.iter().next() {
            let key = key.clone();
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

    fn insert(&mut self, key: ResultKey, value: Arc<V>, bytes: usize) -> u64 {
        if let Some(old) = self.map.remove(&key) {
            self.order.remove(&old.tick);
            self.bytes -= old.bytes;
        }
        let tick = self.next_tick();
        self.order.insert(tick, key.clone());
        self.map.insert(
            key,
            ResultEntry {
                value,
                bytes,
                tick,
                last_used: Instant::now(),
            },
        );
        self.bytes += bytes;

        // Evict LRU-first, but keep at least one entry so a single oversized result
        // stays returnable (mirrors the block LRU's policy).
        let mut evicted = 0;
        while self.bytes > self.budget && self.order.len() > 1 {
            let (&lru_tick, lru_key) = self.order.iter().next().unwrap();
            let lru_key = lru_key.clone();
            self.order.remove(&lru_tick);
            if let Some(e) = self.map.remove(&lru_key) {
                self.bytes -= e.bytes;
            }
            evicted += 1;
        }
        evicted
    }
}

/// Byte-budgeted LRU over whole query results — the third cache pool (PLAN.md
/// `cache`), separate from the block LRU and with its own `result_cache_bytes`
/// budget. Generic over the stored value so it carries no dependency on the
/// executor's result type and is unit-testable in isolation; `slater::server`
/// instantiates it over `exec::QueryResult`.
pub struct ResultCache<V> {
    inner: Mutex<ResultInner<V>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl<V> ResultCache<V> {
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(ResultInner {
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

    /// Look a result up, recording a hit or miss. On a hit it becomes most-recently
    /// used.
    pub fn get(&self, key: &ResultKey) -> Option<Arc<V>> {
        let hit = self.inner.lock().unwrap().touch_get(key);
        if hit.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    /// Cache a result under `key`. `value_bytes` is the caller's estimate of the
    /// value's resident footprint; the key's query string length is added on top so
    /// a large inlined-`vecf32` query is charged for the memory its key occupies and
    /// the pool stays bounded.
    pub fn insert(&self, key: ResultKey, value: Arc<V>, value_bytes: usize) {
        let bytes = value_bytes + key.query.len();
        let evicted = self.inner.lock().unwrap().insert(key, value, bytes);
        if evicted > 0 {
            self.evictions.fetch_add(evicted, Ordering::Relaxed);
        }
    }

    /// Evict every result idle for at least `ttl` as of `now`. See
    /// [`BlockCache::evict_expired`].
    pub fn evict_expired(&self, now: Instant, ttl: Duration) -> u64 {
        let evicted = self.inner.lock().unwrap().evict_expired(now, ttl);
        if evicted > 0 {
            self.evictions.fetch_add(evicted, Ordering::Relaxed);
        }
        evicted
    }

    pub fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn bytes(&self) -> usize {
        self.inner.lock().unwrap().bytes
    }
}

// ── Vector-index cache (the second pool) ───────────────────────────────────────

/// LRU key for one decompressed Vamana block. `ord` is the vector index's position
/// in `Manifest::vector_indexes`, so two indexes (or two generations) never collide;
/// like the block LRU, the generation UUID in the key orphans stale entries on swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VectorBlockKey {
    pub gen: u128,
    pub ord: u32,
    pub block: u32,
}

impl VectorBlockKey {
    pub fn new(gen: GenId, ord: u32, block: u32) -> Self {
        Self {
            gen: gen.0.as_u128(),
            ord,
            block,
        }
    }
}

struct VecInner {
    /// Pinned, resident PQ codes per `(gen, ord)` — the navigation set. Never
    /// evicted (the `// DESIGN:` of the milestone: PQ codes stay resident), but
    /// their bytes are charged against the budget so the pool stays bounded.
    pinned: HashMap<(u128, u32), Arc<ResidentPq>>,
    pinned_bytes: usize,
    /// The 1–2 MiB Vamana block LRU (decompressed), sharing the budget with the
    /// pinned PQ codes.
    blocks: HashMap<VectorBlockKey, Entry>,
    order: BTreeMap<u64, VectorBlockKey>,
    tick: u64,
    block_bytes: usize,
    budget: usize,
}

impl VecInner {
    fn next_tick(&mut self) -> u64 {
        let t = self.tick;
        self.tick += 1;
        t
    }

    fn touch_get(&mut self, key: &VectorBlockKey) -> Option<Arc<Vec<u8>>> {
        let (value, old_tick) = {
            let e = self.blocks.get(key)?;
            (e.value.clone(), e.tick)
        };
        self.order.remove(&old_tick);
        let new_tick = self.next_tick();
        self.order.insert(new_tick, *key);
        let e = self.blocks.get_mut(key).unwrap();
        e.tick = new_tick;
        e.last_used = Instant::now();
        Some(value)
    }

    /// Evict Vamana blocks idle for at least `ttl`; the pinned PQ codes are never
    /// touched, so the resident navigation set is exempt. See
    /// `Inner::evict_expired`.
    fn evict_expired(&mut self, now: Instant, ttl: Duration) -> u64 {
        let mut evicted = 0;
        while let Some((&t, &key)) = self.order.iter().next() {
            if now.saturating_duration_since(self.blocks[&key].last_used) <= ttl {
                break;
            }
            self.order.remove(&t);
            if let Some(e) = self.blocks.remove(&key) {
                self.block_bytes -= e.bytes;
            }
            evicted += 1;
        }
        evicted
    }

    /// Evict LRU blocks until pinned + blocks fit the budget, keeping at least one
    /// block so a single oversized block stays returnable (pinned PQ is never
    /// evicted — it is the resident navigation set).
    fn evict_to_budget(&mut self) -> u64 {
        let mut evicted = 0;
        while self.pinned_bytes + self.block_bytes > self.budget && self.order.len() > 1 {
            let (&lru_tick, &lru_key) = self.order.iter().next().unwrap();
            self.order.remove(&lru_tick);
            if let Some(e) = self.blocks.remove(&lru_key) {
                self.block_bytes -= e.bytes;
            }
            evicted += 1;
        }
        evicted
    }

    fn insert(&mut self, key: VectorBlockKey, value: Arc<Vec<u8>>) -> (Arc<Vec<u8>>, u64) {
        if let Some(existing) = self.touch_get(&key) {
            return (existing, 0);
        }
        let bytes = value.len();
        let tick = self.next_tick();
        self.order.insert(tick, key);
        self.blocks.insert(
            key,
            Entry {
                value: value.clone(),
                bytes,
                tick,
                last_used: Instant::now(),
            },
        );
        self.block_bytes += bytes;
        let evicted = self.evict_to_budget();
        (value, evicted)
    }
}

/// The vector-index pool (PLAN.md `cache`): a separate byte budget
/// (`vector_cache_bytes`) holding the **resident PQ codes** (pinned per
/// `(label, property)`) the beam search navigates by, plus an LRU of the 1–2 MiB
/// Vamana blocks it reads for the frontier and exact re-rank. Distinct from the
/// block LRU so the large-vector path cannot evict hot graph blocks and vice versa.
pub struct VectorIndexCache {
    inner: Mutex<VecInner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl VectorIndexCache {
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(VecInner {
                pinned: HashMap::new(),
                pinned_bytes: 0,
                blocks: HashMap::new(),
                order: BTreeMap::new(),
                tick: 0,
                block_bytes: 0,
                budget: budget_bytes.max(1),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Pin a generation's resident PQ codes for index `ord`. Idempotent (re-pinning
    /// replaces the entry). Charges the codes' footprint to the budget and evicts
    /// blocks if needed so the pool stays bounded.
    pub fn pin(&self, gen: GenId, ord: u32, pq: Arc<ResidentPq>) {
        let mut inner = self.inner.lock().unwrap();
        let key = (gen.0.as_u128(), ord);
        if let Some(old) = inner.pinned.remove(&key) {
            inner.pinned_bytes -= old.resident_bytes();
        }
        inner.pinned_bytes += pq.resident_bytes();
        inner.pinned.insert(key, pq);
        let evicted = inner.evict_to_budget();
        drop(inner);
        if evicted > 0 {
            self.evictions.fetch_add(evicted, Ordering::Relaxed);
        }
    }

    /// Drop a pinned index (e.g. on generation swap), freeing its PQ footprint.
    pub fn unpin(&self, gen: GenId, ord: u32) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(old) = inner.pinned.remove(&(gen.0.as_u128(), ord)) {
            inner.pinned_bytes -= old.resident_bytes();
        }
    }

    /// The pinned resident PQ codes for an index, if any.
    pub fn resident_pq(&self, gen: GenId, ord: u32) -> Option<Arc<ResidentPq>> {
        self.inner
            .lock()
            .unwrap()
            .pinned
            .get(&(gen.0.as_u128(), ord))
            .cloned()
    }

    /// Fetch a Vamana block, loading it with `load` on a miss (outside the lock).
    pub fn get_or_try_insert(
        &self,
        key: VectorBlockKey,
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

    /// Read the `global`-th record of a Vamana store (index `ord` in generation
    /// `gen`) through the pool: locate the block, fetch it (cached), slice the
    /// record. Popping several nodes in the same block reuses the one decompressed
    /// block — the coalescing the disk-native path relies on (D30).
    pub fn record(
        &self,
        reader: &BlockFileReader,
        gen: GenId,
        ord: u32,
        global: u64,
    ) -> Result<Vec<u8>> {
        let loc = reader.locate(global)?;
        let key = VectorBlockKey::new(gen, ord, loc.block.0);
        let raw = self.get_or_try_insert(key, || reader.read_block(loc.block))?;
        let (offsets, data) = parse_block(&raw[..])?;
        let rec = record_from_block(&offsets, data, loc.slot)?;
        Ok(rec.to_vec())
    }

    /// Evict every Vamana block idle for at least `ttl` as of `now`. Pinned PQ
    /// codes are exempt — they are the resident navigation set. See
    /// [`BlockCache::evict_expired`].
    pub fn evict_expired(&self, now: Instant, ttl: Duration) -> u64 {
        let evicted = self.inner.lock().unwrap().evict_expired(now, ttl);
        if evicted > 0 {
            self.evictions.fetch_add(evicted, Ordering::Relaxed);
        }
        evicted
    }

    pub fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Current resident byte usage: pinned PQ codes + cached blocks.
    pub fn bytes(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.pinned_bytes + inner.block_bytes
    }

    /// Number of cached blocks (excludes pinned PQ entries).
    pub fn block_count(&self) -> usize {
        self.inner.lock().unwrap().blocks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::blockfile::BlockFileWriter;

    fn gen(n: u128) -> GenId {
        GenId(uuid::Uuid::from_u128(n))
    }

    #[test]
    fn hit_then_miss_counts_and_returns_same_bytes() {
        let cache = BlockCache::new(1 << 20);
        let g = gen(1);
        let key = BlockKey::new(g, FileKind::NodeProps, 0);

        let mut loads = 0;
        let first = cache
            .get_or_try_insert(key, || {
                loads += 1;
                Ok(vec![1, 2, 3, 4])
            })
            .unwrap();
        // Second access is a hit and must not invoke the loader.
        let second = cache
            .get_or_try_insert(key, || {
                loads += 1;
                Ok(vec![9, 9])
            })
            .unwrap();

        assert_eq!(&*first, &[1, 2, 3, 4]);
        assert_eq!(&*second, &[1, 2, 3, 4]);
        assert_eq!(loads, 1);
        let m = cache.metrics();
        assert_eq!((m.hits, m.misses, m.evictions), (1, 1, 0));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn evicts_least_recently_used_over_budget() {
        // Budget holds two 100-byte blocks but not three.
        let cache = BlockCache::new(250);
        let g = gen(2);
        let k = |b: u32| BlockKey::new(g, FileKind::Topology, b);
        let load = |fill: u8| move || Ok(vec![fill; 100]);

        cache.get_or_try_insert(k(0), load(0)).unwrap(); // [0]
        cache.get_or_try_insert(k(1), load(1)).unwrap(); // [0,1]
        assert_eq!(cache.len(), 2);

        // Touch block 0 so block 1 becomes the LRU victim.
        cache.get_or_try_insert(k(0), load(0)).unwrap();
        // Insert block 2 → over budget → evict block 1.
        cache.get_or_try_insert(k(2), load(2)).unwrap();

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.metrics().evictions, 1);
        // Block 1 was evicted (its loader would re-run, a fresh miss); block 0
        // and block 2 are still hits.
        let mut reload_1 = false;
        cache
            .get_or_try_insert(k(1), || {
                reload_1 = true;
                Ok(vec![1; 100])
            })
            .unwrap();
        assert!(reload_1, "block 1 should have been evicted");
        assert!(cache.bytes() <= 250);
    }

    #[test]
    fn single_oversized_block_is_retained() {
        let cache = BlockCache::new(10);
        let g = gen(3);
        let big = cache
            .get_or_try_insert(BlockKey::new(g, FileKind::Vectors, 0), || Ok(vec![7; 1000]))
            .unwrap();
        assert_eq!(big.len(), 1000);
        assert_eq!(cache.len(), 1); // kept despite exceeding budget
    }

    #[test]
    fn generation_id_isolates_keys() {
        let a = BlockKey::new(gen(10), FileKind::NodeProps, 0);
        let b = BlockKey::new(gen(11), FileKind::NodeProps, 0);
        assert_ne!(
            a, b,
            "same file+block in different generations must not collide"
        );
    }

    #[test]
    fn record_reads_through_cache_against_a_real_blockfile() {
        // Build a real multi-block file, then read records through the cache and
        // confirm bytes match the uncached path and that re-reads are hits.
        let path = std::env::temp_dir().join(format!("slater_cache_bf_{}", std::process::id()));
        let mut w = BlockFileWriter::create(&path, 64, 3).unwrap();
        let mut expected = Vec::new();
        for i in 0..50u32 {
            let rec = format!("rec-{i}-{}", "y".repeat((i % 20) as usize)).into_bytes();
            w.append_record(&rec).unwrap();
            expected.push(rec);
        }
        w.finish().unwrap();
        let reader = BlockFileReader::open(&path).unwrap();
        assert!(reader.num_blocks() > 1, "test needs multiple blocks");

        let cache = BlockCache::new(1 << 20);
        let g = gen(42);
        for (i, want) in expected.iter().enumerate() {
            let got = cache
                .record(&reader, g, FileKind::EdgeProps, i as u64)
                .unwrap();
            assert_eq!(&got, want);
        }
        // Read everything again — now every block is resident, so the second
        // sweep adds only hits (no new misses beyond the first sweep's blocks).
        let after_first = cache.metrics();
        for (i, want) in expected.iter().enumerate() {
            let got = cache
                .record(&reader, g, FileKind::EdgeProps, i as u64)
                .unwrap();
            assert_eq!(&got, want);
        }
        let after_second = cache.metrics();
        assert_eq!(
            after_second.misses, after_first.misses,
            "second sweep should be served entirely from cache"
        );
        assert!(after_second.hits > after_first.hits);

        let _ = std::fs::remove_file(&path);
    }

    // ── Result cache ─────────────────────────────────────────────────────────

    #[test]
    fn result_cache_hit_then_miss() {
        let cache: ResultCache<String> = ResultCache::new(1 << 20);
        let key = ResultKey::new(gen(1), "MATCH (n) RETURN n");

        assert!(cache.get(&key).is_none()); // miss
        cache.insert(key.clone(), Arc::new("rows".to_string()), 4);
        let hit = cache.get(&key).expect("should be cached");
        assert_eq!(&*hit, "rows");

        let m = cache.metrics();
        assert_eq!((m.hits, m.misses, m.evictions), (1, 1, 0));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn result_cache_evicts_least_recently_used() {
        // Budget holds two 100-byte values but not three (keys are short).
        let cache: ResultCache<Vec<u8>> = ResultCache::new(230);
        let g = gen(7);
        let k = |q: &str| ResultKey::new(g, q);

        cache.insert(k("a"), Arc::new(vec![0u8; 100]), 100);
        cache.insert(k("b"), Arc::new(vec![0u8; 100]), 100);
        assert_eq!(cache.len(), 2);

        // Touch "a" so "b" is the LRU victim, then insert "c" → over budget.
        assert!(cache.get(&k("a")).is_some());
        cache.insert(k("c"), Arc::new(vec![0u8; 100]), 100);

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.metrics().evictions, 1);
        assert!(cache.get(&k("b")).is_none(), "b should have been evicted");
        assert!(cache.get(&k("a")).is_some());
        assert!(cache.get(&k("c")).is_some());
        assert!(cache.bytes() <= 230);
    }

    #[test]
    fn result_cache_generation_swap_orphans_stale_entries() {
        // The gen UUID is part of the key, so the same query text under a new
        // generation is a miss — a swapped-out generation's results can never be
        // served, which is exactly the staleness guarantee we want.
        let cache: ResultCache<String> = ResultCache::new(1 << 20);
        let query = "MATCH (n:Person) RETURN n.name";
        let old = ResultKey::new(gen(100), query);
        let new = ResultKey::new(gen(101), query);

        cache.insert(old.clone(), Arc::new("old result".to_string()), 16);
        assert!(cache.get(&new).is_none(), "new generation must miss");
        // The old entry is still physically present but unreachable by the new key.
        assert!(cache.get(&old).is_some());
        assert_ne!(old, new);
    }

    #[test]
    fn result_cache_single_oversized_result_is_retained() {
        let cache: ResultCache<Vec<u8>> = ResultCache::new(10);
        cache.insert(ResultKey::new(gen(3), "q"), Arc::new(vec![1u8; 1000]), 1000);
        assert_eq!(cache.len(), 1); // kept despite exceeding budget
        assert!(cache.get(&ResultKey::new(gen(3), "q")).is_some());
    }

    // ── Vector-index cache (second pool) ───────────────────────────────────────

    use graph_format::pq::{Codebook, PqParams, ResidentPq};

    fn small_pq(n: usize, m: usize) -> Arc<ResidentPq> {
        let params = PqParams::new((m * 2) as u32, m as u32, 8).unwrap();
        let codebook = Codebook {
            params,
            centroids: vec![0.0f32; m * params.k as usize * params.dsub as usize],
        };
        Arc::new(ResidentPq {
            codebook,
            node_ids: (0..n as u64).collect(),
            codes: vec![0u8; n * m],
            m,
        })
    }

    #[test]
    fn vector_index_cache_pins_pq_and_serves_blocks() {
        let path = std::env::temp_dir().join(format!("slater_vcache_{}", std::process::id()));
        let mut w = BlockFileWriter::create(&path, 64, 3).unwrap();
        let mut expected = Vec::new();
        for i in 0..40u32 {
            let rec = format!("vn-{i}-{}", "z".repeat((i % 15) as usize)).into_bytes();
            w.append_record(&rec).unwrap();
            expected.push(rec);
        }
        w.finish().unwrap();
        let reader = BlockFileReader::open(&path).unwrap();
        assert!(reader.num_blocks() > 1);

        let cache = VectorIndexCache::new(1 << 20);
        let g = gen(7);
        let pq = small_pq(40, 8);
        cache.pin(g, 0, pq.clone());
        assert!(cache.resident_pq(g, 0).is_some());
        assert!(cache.resident_pq(g, 1).is_none());

        for (i, want) in expected.iter().enumerate() {
            assert_eq!(&cache.record(&reader, g, 0, i as u64).unwrap(), want);
        }
        let after_first = cache.metrics();
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(&cache.record(&reader, g, 0, i as u64).unwrap(), want);
        }
        assert_eq!(
            cache.metrics().misses,
            after_first.misses,
            "second sweep all hits"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn vector_index_cache_evicts_blocks_but_keeps_pinned_pq() {
        // Budget admits the pinned PQ plus only a couple of small blocks.
        let pq = small_pq(4, 2);
        let budget = pq.resident_bytes() + 250;
        let cache = VectorIndexCache::new(budget);
        let g = gen(9);
        cache.pin(g, 0, pq.clone());

        // Three 100-byte blocks won't all fit alongside the pinned PQ.
        for b in 0..3u32 {
            cache
                .get_or_try_insert(VectorBlockKey::new(g, 0, b), || Ok(vec![b as u8; 100]))
                .unwrap();
        }
        assert!(
            cache.metrics().evictions >= 1,
            "a block should have evicted"
        );
        // The pinned PQ is never evicted.
        assert!(cache.resident_pq(g, 0).is_some());
        assert!(cache.bytes() <= budget + 100, "stays bounded near budget");
    }

    // ── Idle-TTL eviction ──────────────────────────────────────────────────────

    #[test]
    fn evict_expired_reclaims_idle_blocks() {
        let cache = BlockCache::new(1 << 20);
        let g = gen(50);
        // Capture a reference instant *before* the inserts, so every entry's
        // `last_used` sits just after `t0` and we can fabricate a future `now`.
        let t0 = Instant::now();
        for b in 0..3u32 {
            cache
                .get_or_try_insert(BlockKey::new(g, FileKind::Topology, b), || {
                    Ok(vec![b as u8; 10])
                })
                .unwrap();
        }
        assert_eq!(cache.len(), 3);

        // As of t0 nothing has aged past the TTL.
        assert_eq!(cache.evict_expired(t0, Duration::from_secs(60)), 0);
        assert_eq!(cache.len(), 3);

        // Two minutes later every block is idle past a 60s TTL — all reclaimed
        // (no keep-one floor, unlike budget eviction).
        let n = cache.evict_expired(t0 + Duration::from_secs(120), Duration::from_secs(60));
        assert_eq!(n, 3);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.metrics().evictions, 3);
    }

    #[test]
    fn evict_expired_resets_idle_clock_on_touch() {
        use std::thread::sleep;
        let cache = BlockCache::new(1 << 20);
        let g = gen(51);
        let k = |b| BlockKey::new(g, FileKind::NodeProps, b);
        cache.get_or_try_insert(k(0), || Ok(vec![0u8; 10])).unwrap();
        cache.get_or_try_insert(k(1), || Ok(vec![1u8; 10])).unwrap();

        let ttl = Duration::from_millis(100);
        sleep(Duration::from_millis(60));
        // Touch block 0 — its idle clock resets while block 1 keeps aging.
        cache.get_or_try_insert(k(0), || Ok(vec![0u8; 10])).unwrap();
        sleep(Duration::from_millis(60));

        // Block 1 has been idle ~120ms (> ttl); block 0 ~60ms (< ttl).
        assert_eq!(
            cache.evict_expired(Instant::now(), ttl),
            1,
            "only the idle block should be reclaimed"
        );
        // Block 0 survived (a hit, no reload); block 1 was reclaimed (a fresh miss).
        let mut reload_0 = false;
        cache
            .get_or_try_insert(k(0), || {
                reload_0 = true;
                Ok(vec![0u8; 10])
            })
            .unwrap();
        assert!(
            !reload_0,
            "recently-touched block 0 should still be resident"
        );
        let mut reload_1 = false;
        cache
            .get_or_try_insert(k(1), || {
                reload_1 = true;
                Ok(vec![1u8; 10])
            })
            .unwrap();
        assert!(reload_1, "idle block 1 should have been reclaimed");
    }

    #[test]
    fn result_cache_evict_expired_reclaims_idle() {
        let cache: ResultCache<String> = ResultCache::new(1 << 20);
        let g = gen(60);
        let t0 = Instant::now();
        cache.insert(ResultKey::new(g, "q1"), Arc::new("a".to_string()), 1);
        cache.insert(ResultKey::new(g, "q2"), Arc::new("b".to_string()), 1);

        assert_eq!(cache.evict_expired(t0, Duration::from_secs(60)), 0);
        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.evict_expired(t0 + Duration::from_secs(120), Duration::from_secs(60)),
            2
        );
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn vector_index_cache_evict_expired_keeps_pinned_pq() {
        let cache = VectorIndexCache::new(1 << 20);
        let g = gen(61);
        cache.pin(g, 0, small_pq(4, 2));
        let t0 = Instant::now();
        for b in 0..3u32 {
            cache
                .get_or_try_insert(VectorBlockKey::new(g, 0, b), || Ok(vec![b as u8; 10]))
                .unwrap();
        }
        assert_eq!(cache.block_count(), 3);

        let n = cache.evict_expired(t0 + Duration::from_secs(120), Duration::from_secs(60));
        assert_eq!(n, 3);
        assert_eq!(cache.block_count(), 0);
        // The pinned PQ codes are the resident navigation set — never swept.
        assert!(
            cache.resident_pq(g, 0).is_some(),
            "pinned PQ is exempt from TTL"
        );
    }
}
