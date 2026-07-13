// SPDX-License-Identifier: Apache-2.0
//! Shared byte-budgeted cache over decompressed blocks.
//!
//! Every `.blk` file is a sequence of zstd blocks; the readers fetch a block
//! with one `pread` + decompress per access (no mmap — see the blockfile docs).
//! Repeated reads of the same block would otherwise re-`pread` and re-decompress
//! every time — from a hot adjacency block during query traversal to, at the
//! other extreme, a *sequential* whole-file scan where a naive per-record reader
//! would redundantly re-decompress each block once per record it holds (see
//! [`crate::blockfile::BlockFileReader::for_each_record`]'s docs). This cache holds
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
//! # Concurrency: a hit must not serialise
//!
//! In steady state the working set is *resident*, so essentially every read here
//! is a hit — and every query/rayon thread in the process shares one cache. A hit
//! path that takes an exclusive lock therefore caps aggregate hit throughput at one
//! core's worth no matter how many cores are traversing (it gets *worse* with more
//! cores). Two things follow, and they shape the whole design:
//!
//! * **A hit takes a shared read lock and mutates nothing** but two atomics on the
//!   entry itself. That rules out a strict LRU: re-ticking an ordered structure on
//!   every access needs exclusive access by construction. Eviction is instead
//!   **CLOCK** (second-chance): a hit sets a `referenced` bit, and the eviction hand
//!   sweeps a ring, clearing a set bit (a reprieve) and evicting the first entry it
//!   finds already clear. This is the classic buffer-pool approximation of LRU (cf.
//!   Postgres clock-sweep) and preserves the property the workload depends on — a
//!   hot adjacency block survives a cold sequential scan.
//! * **The map is sharded** by key hash, so the lock word and the hit/miss counters
//!   sit on ~one cacheline per shard rather than one for the whole process.
//!
//! Two CLOCK refinements matter here and are not optional:
//!
//! * A freshly inserted block is placed *just behind the hand* and is never the
//!   victim of its own insert's sweep (unless it is the only entry). Otherwise the
//!   tiny-budget sequential scan above would evict the block it just decompressed
//!   before reading the records out of it, and re-decompress it per record — the
//!   exact pathology this cache exists to prevent.
//! * The "keep at least one entry" floor is retained, so a block larger than the
//!   whole budget is still returnable and still cached.
//!
//! ## The byte budget is a hard bound, and stays global
//!
//! Sharding a cache normally frays its memory bound into N loosely-coupled ones.
//! Here the budget is a product guarantee (the server's default envelope is ~144 MiB
//! across the block/vector/result pools), so instead each shard owns a *hard*
//! sub-budget of `budget / shards` enforced under its own write lock: the shard
//! trims itself back within budget in the same critical section that grew it, so
//! `Σ shard_bytes ≤ budget` always — a strict global bound, not a sloppy one. The
//! price of a fixed sub-budget is utilisation, not safety: hash skew can leave one
//! shard evicting while another is under-filled. Shard count is therefore capped so
//! a shard is never smaller than [`MIN_SHARD_BYTES`], and a *small* cache — a
//! build-time scan window, a test — falls back to a single shard, i.e. one exact
//! eviction domain with exactly the old bound.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
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

/// Smallest byte budget a shard may own. Sharding buys parallelism with
/// utilisation (a fixed sub-budget can evict while a sibling shard is under-filled),
/// and that trade is only worth making when a shard still holds a useful number of
/// blocks. Below `shards × MIN_SHARD_BYTES` the cache stays a single shard — which
/// is also exactly the old single-eviction-domain behaviour for a build-time scan
/// window or a test.
const MIN_SHARD_BYTES: usize = 4 << 20;

/// Upper bound on shards. Contention relief is already near-flat well before this
/// (a hit takes a *read* lock, so same-shard hits do not exclude each other; the
/// shard count only spreads the lock word and counters over cachelines).
const MAX_SHARDS: usize = 32;

/// One cached block. `referenced` and `last_used_ms` are atomics because a hit
/// updates them through a **shared** read guard — that is the whole point: a hit
/// mutates no shared structure and blocks no other hit.
struct Entry {
    value: Arc<Vec<u8>>,
    bytes: usize,
    /// CLOCK's second-chance bit: set by a hit, cleared (as a reprieve) by the
    /// eviction hand. Deliberately **false on insert** — a block that is loaded and
    /// never touched again is the right first victim; a block that is read again
    /// earns its bit. (Its own insert's sweep cannot evict it regardless, see
    /// [`ShardInner::evict_to_budget`].)
    referenced: AtomicBool,
    /// Milliseconds since the cache epoch at the most recent access, for the idle-TTL
    /// sweep. Re-stamped only when it has moved by at least [`LAST_USED_GRANULARITY_MS`],
    /// so a block hammered by every thread does not ping-pong its own cacheline.
    last_used_ms: AtomicU64,
}

/// Resolution of the idle-TTL stamp. Coarsening it is what keeps a hot entry's
/// cacheline *shared* (read-only) across the cores hitting it: a stamp written on
/// every hit would bounce that line between cores instead. 10 ms bounds the error in
/// an entry's measured idle time — negligible against any real TTL (seconds), while
/// still cutting the stamp writes on a hot block to ~100/s no matter how many
/// millions of hits it takes.
const LAST_USED_GRANULARITY_MS: u64 = 10;

impl Entry {
    /// Record an access. Both stores are skipped when they would be no-ops, so a
    /// hot entry's cacheline stays shared across the cores reading it.
    #[inline]
    fn touch(&self, now_ms: u64) {
        if !self.referenced.load(Ordering::Relaxed) {
            self.referenced.store(true, Ordering::Relaxed);
        }
        let last = self.last_used_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) >= LAST_USED_GRANULARITY_MS {
            self.last_used_ms.store(now_ms, Ordering::Relaxed);
        }
    }
}

struct ShardInner {
    map: HashMap<BlockKey, Entry>,
    /// CLOCK ring: every live key, exactly once (`ring.len() == map.len()` is an
    /// invariant). Order is insertion-relative, not recency — recency lives in the
    /// `referenced` bits.
    ring: Vec<BlockKey>,
    /// The sweep hand: an index into `ring`, normalised at the top of every sweep step.
    hand: usize,
    bytes: usize,
    budget: usize,
}

impl ShardInner {
    /// Sweep the hand until the shard is back within budget, evicting the first
    /// entry found with a clear `referenced` bit and clearing (reprieving) set bits
    /// on the way. `protect` — the key this sweep's insert just added — is never the
    /// victim: it is by definition the most recently used entry, and evicting it
    /// would turn a tiny-budget sequential scan into one decompress per *record*.
    ///
    /// Terminates: each step either clears a bit (at most `ring.len()` of those before
    /// every bit is clear), skips `protect` (at most once per revolution), or evicts
    /// (shrinking the ring toward the `> 1` floor).
    fn evict_to_budget(&mut self, protect: BlockKey) -> u64 {
        let mut evicted = 0;
        // Keep at least one entry resident so a single block larger than the whole
        // budget is still returnable (and still cached).
        while self.bytes > self.budget && self.ring.len() > 1 {
            if self.hand >= self.ring.len() {
                self.hand = 0;
            }
            let key = self.ring[self.hand];
            if key == protect {
                self.hand += 1;
                continue;
            }
            if self.map[&key].referenced.swap(false, Ordering::Relaxed) {
                // Second chance: it was read since the hand last passed.
                self.hand += 1;
                continue;
            }
            self.ring.remove(self.hand);
            if let Some(e) = self.map.remove(&key) {
                self.bytes -= e.bytes;
            }
            evicted += 1;
        }
        evicted
    }

    /// Insert `value` for `key` (or return the existing entry if a concurrent load
    /// beat us to it), then sweep back within budget. Returns the canonical `Arc`
    /// and the number of entries evicted.
    fn insert(&mut self, key: BlockKey, value: Arc<Vec<u8>>, now_ms: u64) -> (Arc<Vec<u8>>, u64) {
        if let Some(existing) = self.map.get(&key) {
            existing.touch(now_ms);
            return (existing.value.clone(), 0);
        }
        let bytes = value.len();
        // Place the new entry *just behind* the hand, so it gets a full revolution
        // before the hand considers it — the standard CLOCK insertion point.
        let at = self.hand.min(self.ring.len());
        self.ring.insert(at, key);
        self.hand = at + 1;
        self.map.insert(
            key,
            Entry {
                value: value.clone(),
                bytes,
                referenced: AtomicBool::new(false),
                last_used_ms: AtomicU64::new(now_ms),
            },
        );
        self.bytes += bytes;
        let evicted = self.evict_to_budget(key);
        (value, evicted)
    }

    /// Evict every entry idle for longer than `ttl`. Unlike budget eviction this has
    /// no keep-at-least-one floor — an entirely idle cache is fully reclaimed. O(n)
    /// in the shard's entries (there is no recency *order* to walk any more), which
    /// is fine: this runs on the background maintenance timer, never on a read.
    fn evict_expired(&mut self, now_ms: u64, ttl_ms: u64) -> u64 {
        let before = self.map.len();
        let bytes = &mut self.bytes;
        self.map.retain(|_, e| {
            let idle = now_ms.saturating_sub(e.last_used_ms.load(Ordering::Relaxed));
            let keep = idle <= ttl_ms;
            if !keep {
                *bytes -= e.bytes;
            }
            keep
        });
        let evicted = before - self.map.len();
        if evicted > 0 {
            let map = &self.map;
            self.ring.retain(|k| map.contains_key(k));
            if self.hand >= self.ring.len() {
                self.hand = 0;
            }
        }
        evicted as u64
    }
}

/// One lock domain. `align(64)` keeps two shards' lock words and counters off the
/// same cacheline — sharding is pointless if the shards false-share.
#[repr(align(64))]
struct Shard {
    inner: RwLock<ShardInner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

/// Byte-budgeted CLOCK cache over decompressed blocks, safe to share across threads.
/// See the module docs for why the hit path is a shared read lock and eviction is
/// CLOCK rather than a strict LRU.
pub struct BlockCache {
    shards: Vec<Shard>,
    /// `shards.len() - 1`; `shards.len()` is always a power of two.
    mask: usize,
    /// Origin for the `last_used_ms` stamps.
    epoch: Instant,
}

/// Shard selector: a splitmix64 finaliser over the key's fields. The `HashMap`s
/// inside the shards do their own (SipHash) hashing; this only has to spread keys —
/// including the common `(scope, sub) = (0, 0)` build-time case, where the key is
/// just an ascending block index.
fn shard_hash(key: &BlockKey) -> u64 {
    let mut x = (key.scope as u64) ^ ((key.scope >> 64) as u64);
    x ^= ((key.sub as u64) << 32) | (key.block as u64);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

impl BlockCache {
    /// Create a cache with the given byte budget (clamped to at least 1). The shard
    /// count is derived from the machine's parallelism and the budget: a cache too
    /// small to give every shard [`MIN_SHARD_BYTES`] stays single-sharded.
    pub fn new(budget_bytes: usize) -> Self {
        let budget = budget_bytes.max(1);
        let by_cpu = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .saturating_mul(2)
            .next_power_of_two()
            .min(MAX_SHARDS);
        let mut by_budget = 1;
        while by_budget * 2 * MIN_SHARD_BYTES <= budget && by_budget < MAX_SHARDS {
            by_budget *= 2;
        }
        Self::with_shards(budget, by_cpu.min(by_budget))
    }

    /// Create a cache with an explicit shard count (rounded up to a power of two and
    /// capped at [`MAX_SHARDS`]). For tests and benchmarks that need a deterministic
    /// number of eviction domains; production callers want [`BlockCache::new`].
    pub fn with_shards(budget_bytes: usize, shards: usize) -> Self {
        let budget = budget_bytes.max(1);
        let mut shards = shards.max(1).next_power_of_two().min(MAX_SHARDS);
        // A shard must own at least one byte, or the sub-budgets would round *up* and
        // their sum would exceed the caller's budget.
        while shards > 1 && budget / shards == 0 {
            shards /= 2;
        }
        // Each shard's hard sub-budget. `shards * per_shard <= budget`, so the sum of
        // the shards' bounds never exceeds the caller's budget (integer division only
        // ever loses bytes, never gains them).
        let per_shard = budget / shards;
        Self {
            shards: (0..shards)
                .map(|_| Shard {
                    inner: RwLock::new(ShardInner {
                        map: HashMap::new(),
                        ring: Vec::new(),
                        hand: 0,
                        bytes: 0,
                        budget: per_shard,
                    }),
                    hits: AtomicU64::new(0),
                    misses: AtomicU64::new(0),
                    evictions: AtomicU64::new(0),
                })
                .collect(),
            mask: shards - 1,
            epoch: Instant::now(),
        }
    }

    #[inline]
    fn shard(&self, key: &BlockKey) -> &Shard {
        &self.shards[(shard_hash(key) as usize) & self.mask]
    }

    #[inline]
    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// Number of independent eviction domains (lock shards).
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Fetch a block from the cache, loading it with `load` on a miss.
    ///
    /// A **hit** takes only the shard's *read* lock: it clones an `Arc`, sets the
    /// entry's CLOCK bit, and mutates nothing shared — so concurrent hits (the
    /// steady state) run in parallel instead of queueing behind one another.
    ///
    /// A **miss** runs `load` outside every lock, so a slow `pread`+decompress never
    /// serialises other readers; if two readers miss the same key at once they both
    /// load and the second insert deduplicates to the first's `Arc`.
    pub fn get_or_try_insert(
        &self,
        key: BlockKey,
        load: impl FnOnce() -> Result<Vec<u8>>,
    ) -> Result<Arc<Vec<u8>>> {
        let shard = self.shard(&key);
        let now_ms = self.now_ms();
        if let Some(entry) = shard.inner.read().unwrap().map.get(&key) {
            entry.touch(now_ms);
            let value = entry.value.clone();
            shard.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(value);
        }
        shard.misses.fetch_add(1, Ordering::Relaxed);
        let value = Arc::new(load()?);
        let (canonical, evicted) = shard
            .inner
            .write()
            .unwrap()
            .insert(key, value, self.now_ms());
        if evicted > 0 {
            shard.evictions.fetch_add(evicted, Ordering::Relaxed);
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
    /// counter. Sweeps every shard.
    pub fn evict_expired(&self, now: Instant, ttl: Duration) -> u64 {
        let now_ms = now.saturating_duration_since(self.epoch).as_millis() as u64;
        let ttl_ms = ttl.as_millis().min(u64::MAX as u128) as u64;
        let mut total = 0;
        for shard in &self.shards {
            let evicted = shard.inner.write().unwrap().evict_expired(now_ms, ttl_ms);
            if evicted > 0 {
                shard.evictions.fetch_add(evicted, Ordering::Relaxed);
                total += evicted;
            }
        }
        total
    }

    /// Counter snapshot, summed across shards. Not a consistent snapshot across all
    /// three counters (they are read shard by shard, without a global lock) — these
    /// are metrics, not invariants.
    pub fn metrics(&self) -> CacheMetrics {
        let mut m = CacheMetrics {
            hits: 0,
            misses: 0,
            evictions: 0,
        };
        for shard in &self.shards {
            m.hits += shard.hits.load(Ordering::Relaxed);
            m.misses += shard.misses.load(Ordering::Relaxed);
            m.evictions += shard.evictions.load(Ordering::Relaxed);
        }
        m
    }

    /// Current number of cached blocks.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.inner.read().unwrap().map.len())
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Current resident byte usage (sum of cached block sizes). Never exceeds the
    /// configured budget: each shard trims itself back inside its sub-budget in the
    /// same critical section that grew it (bar the deliberate keep-at-least-one
    /// oversized-block floor).
    pub fn bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.inner.read().unwrap().bytes)
            .sum()
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

    /// A cache too small to give each shard `MIN_SHARD_BYTES` must stay a single
    /// eviction domain — a build-time scan window or a test keeps exactly the old
    /// bound, with no per-shard keep-one floor to multiply.
    #[test]
    fn a_small_budget_stays_single_sharded() {
        assert_eq!(BlockCache::new(1).shard_count(), 1);
        assert_eq!(BlockCache::new(1 << 20).shard_count(), 1);
        assert_eq!(BlockCache::new(MIN_SHARD_BYTES).shard_count(), 1);
        // Big enough for two shards of MIN_SHARD_BYTES (given >= 1 cpu).
        assert!(BlockCache::new(64 << 20).shard_count() >= 2);
    }

    /// CLOCK's core promise, and the one that keeps the miss rate honest: a block
    /// that was read again survives; the one that was not is the victim. (This is
    /// the strict-LRU victim choice too — the property the cache is *for*.)
    #[test]
    fn a_referenced_block_outlives_an_unreferenced_one() {
        let cache = BlockCache::with_shards(250, 1); // holds two 100-byte blocks
        let k = |b: u32| BlockKey::new(0, 0, b);
        let load = |fill: u8| move || Ok(vec![fill; 100]);

        cache.get_or_try_insert(k(0), load(0)).unwrap();
        cache.get_or_try_insert(k(1), load(1)).unwrap();
        assert_eq!(cache.len(), 2);
        // Read block 0 again — now 1 is the only unreferenced block.
        cache.get_or_try_insert(k(0), load(0)).unwrap();
        // Over budget → sweep must take block 1.
        cache.get_or_try_insert(k(2), load(2)).unwrap();

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.metrics().evictions, 1);
        let mut reloaded_1 = false;
        cache
            .get_or_try_insert(k(1), || {
                reloaded_1 = true;
                Ok(vec![1; 100])
            })
            .unwrap();
        assert!(reloaded_1, "the unreferenced block should be the victim");
        assert!(cache.bytes() <= 250);
    }

    /// The block a miss just decompressed must not be evicted by that same miss's
    /// sweep: a strictly-ascending scan through a budget that holds ~one block would
    /// otherwise re-decompress the block once per record it contains.
    #[test]
    fn a_just_loaded_block_survives_its_own_insert_sweep() {
        let path = tmp("scan_window");
        build_store(&path, 400);
        let reader = BlockFileReader::open(&path).unwrap();
        // Budget too small for two blocks: every new block forces an eviction.
        let cache = BlockCache::with_shards(64, 1);

        // Walk every record in order. Records in the same block must hit — i.e. the
        // block loaded for record i is still there for record i+1.
        let mut last_block = u32::MAX;
        let mut same_block_reads = 0u64;
        for i in 0..400u64 {
            let loc = reader.locate(i).unwrap();
            if loc.block.0 == last_block {
                same_block_reads += 1;
            }
            last_block = loc.block.0;
            cache.record(&reader, 0, 0, i).unwrap();
        }
        let m = cache.metrics();
        assert!(same_block_reads > 0, "test needs multi-record blocks");
        assert_eq!(
            m.hits, same_block_reads,
            "every re-read of the current block must hit ({} misses)",
            m.misses
        );
        let _ = std::fs::remove_file(&path);
    }

    /// The byte budget is a product guarantee, so it has to hold while N threads are
    /// hammering the cache — not just single-threaded. Sharding is what could break
    /// it (each shard evicts independently), so force many shards and check the
    /// *global* sum, both continuously during the load and at rest.
    #[test]
    fn concurrent_load_never_exceeds_the_byte_budget() {
        const BUDGET: usize = 64 * 1024;
        const BLOCK: usize = 512;
        let cache = Arc::new(BlockCache::with_shards(BUDGET, 16));
        assert_eq!(cache.shard_count(), 16);

        let threads: Vec<_> = (0..8u32)
            .map(|t| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for i in 0..4000u32 {
                        // A working set far larger than the budget, overlapping
                        // between threads, so eviction runs constantly.
                        let key = BlockKey::new(0, t % 3, i % 600);
                        let v = cache
                            .get_or_try_insert(key, || Ok(vec![t as u8; BLOCK]))
                            .unwrap();
                        assert_eq!(v.len(), BLOCK);
                        if i % 64 == 0 {
                            assert!(
                                cache.bytes() <= BUDGET,
                                "over budget mid-flight: {} > {BUDGET}",
                                cache.bytes()
                            );
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        assert!(
            cache.bytes() <= BUDGET,
            "over budget at rest: {} > {BUDGET}",
            cache.bytes()
        );
        assert!(cache.len() * BLOCK <= BUDGET);
        let m = cache.metrics();
        assert_eq!(m.hits + m.misses, 8 * 4000, "every access is counted once");
        assert!(m.evictions > 0);
    }

    /// The regression test for the finding itself: with the hit path on one global
    /// exclusive lock, adding threads *reduces* aggregate hit throughput (measured
    /// on the pre-fix cache: 9.7 M hits/s at 1 thread → 2.0 M/s at 4). It must now
    /// scale instead. The 1.2× bar is far below the ~3.5× the sharded read-lock path
    /// achieves and far above the ~0.2× the global mutex managed, so it discriminates
    /// even on a busy machine.
    #[test]
    fn concurrent_hits_scale_with_threads() {
        const THREADS: usize = 4;
        if std::thread::available_parallelism().map_or(1, |n| n.get()) < THREADS {
            return; // not enough cores to say anything
        }
        const BLOCKS: u32 = 256;
        let cache = Arc::new(BlockCache::new(64 << 20));
        for b in 0..BLOCKS {
            cache
                .get_or_try_insert(BlockKey::new(0, 0, b), || Ok(vec![0u8; 4096]))
                .unwrap();
        }

        // Aggregate hits/sec with `n` threads each doing `per_thread` resident lookups.
        let rate = |n: usize| -> f64 {
            const PER_THREAD: u64 = 200_000;
            let barrier = Arc::new(std::sync::Barrier::new(n));
            let handles: Vec<_> = (0..n)
                .map(|w| {
                    let cache = cache.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        let start = Instant::now();
                        for i in 0..PER_THREAD {
                            let b = (w as u64 * 37 + i * 7) % BLOCKS as u64;
                            let key = BlockKey::new(0, 0, b as u32);
                            let v = cache
                                .get_or_try_insert(key, || unreachable!("resident"))
                                .unwrap();
                            std::hint::black_box(v[0]);
                        }
                        start.elapsed()
                    })
                })
                .collect();
            // Aggregate rate is bounded by the slowest worker.
            let slowest = handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .max()
                .unwrap();
            (n as u64 * PER_THREAD) as f64 / slowest.as_secs_f64()
        };

        let one = rate(1);
        let many = rate(THREADS);
        assert!(
            many >= one * 1.2,
            "hits must scale with cores: 1 thread {one:.0}/s, {THREADS} threads \
             {many:.0}/s (ratio {:.2}×)",
            many / one
        );
    }

    #[test]
    fn idle_ttl_sweep_reclaims_every_shard() {
        let cache = BlockCache::with_shards(1 << 20, 8);
        for b in 0..64u32 {
            cache
                .get_or_try_insert(BlockKey::new(0, 0, b), || Ok(vec![0u8; 256]))
                .unwrap();
        }
        assert_eq!(cache.len(), 64);
        // Nothing is idle yet.
        assert_eq!(
            cache.evict_expired(Instant::now(), Duration::from_secs(60)),
            0
        );
        // Pretend an hour has passed: everything is idle, and unlike budget eviction
        // the TTL sweep has no keep-one floor.
        let later = Instant::now() + Duration::from_secs(3600);
        assert_eq!(cache.evict_expired(later, Duration::from_secs(60)), 64);
        assert!(cache.is_empty());
        assert_eq!(cache.bytes(), 0);
    }

    /// Two readers missing the same key at once must converge on one `Arc` (the
    /// second insert deduplicates), so the block is never double-counted in `bytes`.
    #[test]
    fn concurrent_duplicate_loads_deduplicate() {
        let cache = Arc::new(BlockCache::new(1 << 20));
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let key = BlockKey::new(7, 1, 2);
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let cache = cache.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    cache
                        .get_or_try_insert(key, || Ok(vec![3u8; 1024]))
                        .unwrap()
                })
            })
            .collect();
        let arcs: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        for a in &arcs {
            assert!(Arc::ptr_eq(a, &arcs[0]), "all readers share one block");
        }
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.bytes(), 1024, "the block is counted exactly once");
    }
}
