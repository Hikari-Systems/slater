// SPDX-License-Identifier: Apache-2.0
//! ISAM-style sorted range index.
//!
//! `(value, id)` pairs are sorted, packed into compressed key-blocks, and a small
//! **sparse top-level** (the first key of each block → block location) is held
//! resident at open. Equality and range lookups binary-search the top-level to
//! find the candidate block(s), then read and scan only those — never the whole
//! index.
//!
//! On-disk layout:
//! ```text
//! MAGIC(8) ‖ block_0 ‖ … ‖ block_{n-1} ‖ top_level ‖ footer(24)
//! footer    = top_offset:u64 ‖ top_len:u64 ‖ block_count:u64
//! top_level = block_count × ( value(first_key) ‖ offset:u64 ‖ comp_len:u32 ‖ raw_len:u32 )
//! block raw = uvarint(count) ‖ count × ( value(key) ‖ uvarint(id) )   (then zstd)
//! ```

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};

use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::codec;
use crate::crypto::{BlockCipher, NONCE_LEN};
use crate::ids::Value;
use crate::store::fs::FileObject;
use crate::store::RandomReadAt;
use crate::wire::{
    capacity_for, read_uvarint, read_value, write_uvarint, write_value, DecodeRejected,
};

const ISAM_MAGIC: &[u8; 8] = b"SLISM001";
/// Magic for an AEAD-encrypted ISAM index: the block bytes are sealed compressed
/// blocks, and the whole resident top-level — which holds each block's first key
/// in the clear — is itself sealed so no key material leaks at rest (D28).
const ISAM_MAGIC_ENC: &[u8; 8] = b"SLISME01";
const FOOTER_LEN: u64 = 24;
/// Encrypted footer also carries the nonce that seals the top-level.
const FOOTER_LEN_ENC: u64 = FOOTER_LEN + NONCE_LEN as u64;

/// Build an ISAM index from `(value, id)` pairs. The input need not be sorted —
/// it is sorted here by key order then id.
pub fn write_isam(
    path: impl AsRef<Path>,
    entries: Vec<(Value, u64)>,
    target_block_bytes: usize,
    zstd_level: i32,
) -> Result<u64> {
    write_isam_with_cipher(path, entries, target_block_bytes, zstd_level, None)
}

/// Build an ISAM index, optionally AEAD-encrypted (`cipher = None` ⇒ plaintext,
/// identical to [`write_isam`]). Each compressed block is sealed under its own
/// random nonce, which is stored in the resident top-level beside the block's
/// location — the key never touches the file (D28).
///
/// Convenience wrapper that sorts in memory and delegates to
/// [`write_isam_sorted`]; the external builder, which cannot hold all entries,
/// feeds an already-sorted stream to `write_isam_sorted` directly.
pub fn write_isam_with_cipher(
    path: impl AsRef<Path>,
    mut entries: Vec<(Value, u64)>,
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<u64> {
    entries.sort_by(|a, b| a.0.cmp_key(&b.0).then(a.1.cmp(&b.1)));
    write_isam_sorted(
        path,
        entries.into_iter().map(Ok),
        target_block_bytes,
        zstd_level,
        cipher,
    )
}

/// One block's resident top-level entry (first key → block location + nonce).
struct Top {
    first_key: Value,
    offset: u64,
    comp_len: u32,
    raw_len: u32,
    nonce: Option<[u8; NONCE_LEN]>,
}

/// Compress, optionally seal, and write one key-block; record its top-level entry.
#[allow(clippy::too_many_arguments)] // a cohesive write step; bundling into a struct would not aid clarity
fn flush_isam_block(
    file: &mut BufWriter<File>,
    offset: &mut u64,
    tops: &mut Vec<Top>,
    first_key: Value,
    count: u64,
    body: &[u8],
    zstd_level: i32,
    cipher: &Option<Arc<BlockCipher>>,
) -> Result<()> {
    let mut raw = Vec::with_capacity(body.len() + 8);
    write_uvarint(&mut raw, count);
    raw.extend_from_slice(body);
    let comp = codec::compress(&raw, zstd_level)?;
    let (stored, nonce) = match cipher {
        Some(c) => {
            let nonce = BlockCipher::random_nonce();
            (c.encrypt(&nonce, &comp)?, Some(nonce))
        }
        None => (comp, None),
    };
    file.write_all(&stored)?;
    tops.push(Top {
        first_key,
        offset: *offset,
        comp_len: stored.len() as u32,
        raw_len: raw.len() as u32,
        nonce,
    });
    *offset += stored.len() as u64;
    Ok(())
}

/// Build an ISAM index from an **already-sorted** stream of `(value, id)` pairs
/// (ascending by [`Value::cmp_key`] then id — the same order [`write_isam`]
/// produces internally). Byte-for-byte identical to `write_isam` on the same
/// entries, but holds only the current key-block resident, so the external
/// builder can stream entries straight from an [`crate::extsort::ExtSorter`]
/// without materialising them all.
pub fn write_isam_sorted<I>(
    path: impl AsRef<Path>,
    entries: I,
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<u64>
where
    I: IntoIterator<Item = Result<(Value, u64)>>,
{
    let f = File::create(path.as_ref())
        .with_context(|| format!("create isam {}", path.as_ref().display()))?;
    let mut file = BufWriter::new(f);
    let magic = if cipher.is_some() {
        ISAM_MAGIC_ENC
    } else {
        ISAM_MAGIC
    };
    file.write_all(magic)?;
    let mut offset = magic.len() as u64;

    let mut tops: Vec<Top> = Vec::new();
    let mut first_key: Option<Value> = None;
    let mut count = 0u64;
    let mut body: Vec<u8> = Vec::new();

    for e in entries {
        let (k, id) = e?;
        if first_key.is_none() {
            first_key = Some(k.clone());
        }
        write_value(&mut body, &k);
        write_uvarint(&mut body, id);
        count += 1;
        // Close the block once it reaches target — the entry that crosses the
        // threshold is the block's last, matching `write_isam`'s boundary exactly.
        if body.len() >= target_block_bytes {
            flush_isam_block(
                &mut file,
                &mut offset,
                &mut tops,
                first_key.take().unwrap(),
                count,
                &body,
                zstd_level,
                &cipher,
            )?;
            count = 0;
            body.clear();
        }
    }
    if count > 0 {
        flush_isam_block(
            &mut file,
            &mut offset,
            &mut tops,
            first_key.take().unwrap(),
            count,
            &body,
            zstd_level,
            &cipher,
        )?;
    }

    // Top level.
    let top_offset = offset;
    let mut top_bytes = Vec::new();
    for t in &tops {
        write_value(&mut top_bytes, &t.first_key);
        top_bytes.write_u64::<LittleEndian>(t.offset)?;
        top_bytes.write_u32::<LittleEndian>(t.comp_len)?;
        top_bytes.write_u32::<LittleEndian>(t.raw_len)?;
        if let Some(nonce) = &t.nonce {
            top_bytes.write_all(nonce)?;
        }
    }
    // Seal the whole top-level (first keys + per-block nonces) when encrypting so
    // no plaintext key material is left resident on disk.
    let (stored_top, top_nonce) = match &cipher {
        Some(c) => {
            let nonce = BlockCipher::random_nonce();
            (c.encrypt(&nonce, &top_bytes)?, Some(nonce))
        }
        None => (top_bytes, None),
    };
    file.write_all(&stored_top)?;

    let mut footer = Vec::with_capacity(FOOTER_LEN_ENC as usize);
    footer.write_u64::<LittleEndian>(top_offset)?;
    footer.write_u64::<LittleEndian>(stored_top.len() as u64)?;
    footer.write_u64::<LittleEndian>(tops.len() as u64)?;
    if let Some(nonce) = &top_nonce {
        footer.write_all(nonce)?;
    }
    file.write_all(&footer)?;
    file.flush()?;
    file.get_ref().sync_all().context("fsync isam")?;
    Ok(tops.len() as u64)
}

struct TopEntry {
    first_key: Value,
    offset: u64,
    comp_len: u32,
    raw_len: u32,
    /// Per-block AEAD nonce, present iff the index is encrypted.
    nonce: Option<[u8; NONCE_LEN]>,
}

/// A decoded ISAM leaf block: `(key, id)` pairs, ascending by key. Shared read-only.
type DecodedBlock = Arc<Vec<(Value, u64)>>;

/// Resident-byte estimate of a decoded block, for LRU budgeting: the inline slice
/// footprint plus the heap each key value owns beyond its `Value` struct. Counting the
/// `Str`/`List`/`Vector` heap keeps the byte budget honest for string-keyed indexes
/// (the old estimate counted only the fixed struct, so a block of long string keys
/// looked far smaller than it was and the budget could be badly overshot).
fn decoded_bytes(entries: &[(Value, u64)]) -> usize {
    std::mem::size_of_val(entries)
        + entries
            .iter()
            .map(|(v, _)| value_heap_bytes(v))
            .sum::<usize>()
}

/// Heap bytes a `Value` owns beyond its own struct footprint (already counted by
/// `size_of_val` on the containing slice).
fn value_heap_bytes(v: &Value) -> usize {
    match v {
        Value::Str(s) => s.len(),
        Value::List(items) => {
            items.len() * std::mem::size_of::<Value>()
                + items.iter().map(value_heap_bytes).sum::<usize>()
        }
        Value::Vector(f) => f.len() * std::mem::size_of::<f32>(),
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => 0,
    }
}

/// Cache key for one decoded leaf block: the index's per-generation ordinal `sub`
/// and the block index within that index.
type DbcKey = (u32, u32);

/// Smallest byte budget a shard may own. Sharding buys parallelism with utilisation
/// (a fixed sub-budget can evict while a sibling shard is under-filled), and that
/// trade is only worth making when a shard still holds a useful number of blocks.
/// Below `shards × MIN_SHARD_BYTES` the cache stays a single shard — which is also
/// exactly the old single-eviction-domain bound for a small/test cache. (Mirrors
/// `blockcache::BlockCache`; the default range-index budget is 16 MiB, so a real
/// cache lands at ≤4 shards.)
const MIN_SHARD_BYTES: usize = 4 << 20;

/// Upper bound on shards. A hit takes a *read* lock, so same-shard hits do not
/// exclude each other; the shard count only spreads the lock word and counters over
/// cachelines, and the relief is near-flat well before this.
const MAX_SHARDS: usize = 32;

/// One cached decoded block. `referenced` is an atomic because a hit sets it through
/// a **shared** read guard — that is the whole point: a hit mutates no shared
/// structure (no LRU re-tick) and blocks no other hit.
struct DbcEntry {
    value: DecodedBlock,
    /// Resident byte estimate (`decoded_bytes`, incl. Str/List/Vector key heap — the
    /// HIK-101 accounting). Stored once at insert; the budget sums these.
    bytes: usize,
    /// CLOCK's second-chance bit: set by a hit, cleared (as a reprieve) by the
    /// eviction hand. Deliberately **false on insert** — a block loaded and never
    /// touched again is the right first victim; a block read again earns its bit.
    /// (Its own insert's sweep cannot evict it regardless, see `evict_to_budget`.)
    referenced: AtomicBool,
}

struct ShardInner {
    map: HashMap<DbcKey, DbcEntry>,
    /// CLOCK ring: every live key, exactly once (`ring.len() == map.len()` is an
    /// invariant). Order is insertion-relative, not recency — recency lives in the
    /// `referenced` bits.
    ring: Vec<DbcKey>,
    /// The sweep hand: an index into `ring`, normalised at the top of every sweep step.
    hand: usize,
    bytes: usize,
    budget: usize,
}

impl ShardInner {
    /// Sweep the hand until the shard is back within budget, evicting the first entry
    /// found with a clear `referenced` bit and clearing (reprieving) set bits on the
    /// way. `protect` — the key this sweep's insert just added — is never the victim:
    /// it is by definition the most recently used entry, and evicting it would turn a
    /// tiny-budget sequential scan into one decompress+decode per *record*.
    ///
    /// Terminates: each step either clears a bit (at most `ring.len()` before every
    /// bit is clear), skips `protect` (at most once per revolution), or evicts
    /// (shrinking the ring toward the `> 1` floor).
    fn evict_to_budget(&mut self, protect: DbcKey) -> u64 {
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
            if self.map[&key]
                .referenced
                .swap(false, AtomicOrdering::Relaxed)
            {
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
    /// beat us to it), then sweep back within budget. Returns the canonical `Arc` and
    /// the number of entries evicted.
    fn insert(&mut self, key: DbcKey, value: DecodedBlock, bytes: usize) -> (DecodedBlock, u64) {
        if let Some(existing) = self.map.get(&key) {
            existing.referenced.store(true, AtomicOrdering::Relaxed);
            return (existing.value.clone(), 0);
        }
        // Place the new entry *just behind* the hand, so it gets a full revolution
        // before the hand considers it — the standard CLOCK insertion point.
        let at = self.hand.min(self.ring.len());
        self.ring.insert(at, key);
        self.hand = at + 1;
        self.map.insert(
            key,
            DbcEntry {
                value: value.clone(),
                bytes,
                referenced: AtomicBool::new(false),
            },
        );
        self.bytes += bytes;
        let evicted = self.evict_to_budget(key);
        (value, evicted)
    }
}

/// A point-in-time snapshot of the cache counters (for metrics/tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DbcMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
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

/// A byte-budgeted **CLOCK** cache of **decoded** ISAM leaf blocks, keyed by
/// `(sub, block)` where `sub` is the index's per-generation ordinal. One instance is
/// shared across a generation's range readers, safe to hit concurrently.
///
/// This caches decoded entries, not raw bytes: an ISAM leaf can hold tens of thousands
/// of `(key, id)` pairs, so decoding it on *every* probe (as a raw-byte cache would
/// still force) dominates a point lookup. Decoding once and holding the sorted `Vec`
/// lets a repeated equality/range probe **binary-search** the cached block — O(log n)
/// instead of O(n) — which is the bulk-write-resolve / indexed-seek hot path.
///
/// # Concurrency: a hit must not serialise (HIK-106)
///
/// In steady state the working set is resident and essentially every probe is a hit,
/// and every query/rayon thread shares one cache. A hit path on one exclusive lock
/// therefore caps aggregate hit throughput at one core's worth no matter how many
/// cores are seeking (it gets *worse* with more cores). So, mirroring
/// [`crate::blockcache::BlockCache`]:
///
/// * **A hit takes a shared read lock and mutates nothing** but the entry's own
///   `referenced` atomic. That rules out a strict LRU (re-ticking an order needs
///   exclusive access by construction); eviction is instead **CLOCK** (second-chance),
///   the classic buffer-pool LRU approximation, which keeps the property the workload
///   needs — a hot block survives a cold sequential scan.
/// * **The map is sharded** by key hash, each shard a `#[repr(align(64))]` lock
///   domain, so the lock word and counters spread over cachelines.
///
/// The byte budget stays a **hard, global** bound (a product guarantee): each shard
/// owns a hard sub-budget of `budget / shards` and trims itself back inside it in the
/// same critical section that grew it, so `Σ shard_bytes ≤ budget` always. A cache too
/// small to give every shard [`MIN_SHARD_BYTES`] stays single-sharded — exactly the
/// old single-eviction-domain bound.
pub struct DecodedBlockCache {
    shards: Vec<Shard>,
    /// `shards.len() - 1`; `shards.len()` is always a power of two.
    mask: usize,
}

/// Shard selector: a splitmix64 finaliser over the packed `(sub, block)` key. The
/// inner `HashMap`s do their own (SipHash) hashing; this only has to spread keys,
/// including the common ascending-block-index case within one index.
fn shard_hash(key: &DbcKey) -> u64 {
    let mut x = ((key.0 as u64) << 32) | (key.1 as u64);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

impl DecodedBlockCache {
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
    /// number of eviction domains; production callers want [`DecodedBlockCache::new`].
    pub fn with_shards(budget_bytes: usize, shards: usize) -> Self {
        let budget = budget_bytes.max(1);
        let mut shards = shards.max(1).next_power_of_two().min(MAX_SHARDS);
        // A shard must own at least one byte, or the sub-budgets would round *up* and
        // their sum would exceed the caller's budget.
        while shards > 1 && budget / shards == 0 {
            shards /= 2;
        }
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
        }
    }

    #[inline]
    fn shard(&self, key: &DbcKey) -> &Shard {
        &self.shards[(shard_hash(key) as usize) & self.mask]
    }

    /// Number of independent eviction domains (lock shards).
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Fetch decoded block `(sub, block)`, decoding it with `load` on a miss.
    ///
    /// A **hit** takes only the shard's *read* lock: it clones an `Arc`, sets the
    /// entry's CLOCK bit, and mutates nothing shared — so concurrent hits (the steady
    /// state) run in parallel instead of queueing behind one another.
    ///
    /// A **miss** runs `load` **outside** every lock, so a slow decompress+decode never
    /// serialises other readers; if two readers miss the same key at once they both
    /// load and the second insert deduplicates to the first's `Arc`.
    pub fn get_or_load(
        &self,
        sub: u32,
        block: u32,
        load: impl FnOnce() -> Result<Vec<(Value, u64)>>,
    ) -> Result<DecodedBlock> {
        let key = (sub, block);
        let shard = self.shard(&key);
        if let Some(entry) = shard.inner.read().unwrap().map.get(&key) {
            entry.referenced.store(true, AtomicOrdering::Relaxed);
            let value = entry.value.clone();
            shard.hits.fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(value);
        }
        shard.misses.fetch_add(1, AtomicOrdering::Relaxed);
        let value: DecodedBlock = Arc::new(load()?);
        let bytes = decoded_bytes(&value);
        let (canonical, evicted) = shard.inner.write().unwrap().insert(key, value, bytes);
        if evicted > 0 {
            shard.evictions.fetch_add(evicted, AtomicOrdering::Relaxed);
        }
        Ok(canonical)
    }

    /// Counter snapshot, summed across shards. Not a consistent snapshot across the
    /// three counters (read shard by shard without a global lock) — these are metrics,
    /// not invariants.
    pub fn metrics(&self) -> DbcMetrics {
        let mut m = DbcMetrics {
            hits: 0,
            misses: 0,
            evictions: 0,
        };
        for shard in &self.shards {
            m.hits += shard.hits.load(AtomicOrdering::Relaxed);
            m.misses += shard.misses.load(AtomicOrdering::Relaxed);
            m.evictions += shard.evictions.load(AtomicOrdering::Relaxed);
        }
        m
    }

    /// Current number of cached blocks (sum across shards).
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

/// Reader holding the resident sparse top-level.
pub struct IsamReader {
    src: Arc<dyn RandomReadAt>,
    top: Vec<TopEntry>,
    /// Per-generation cipher, set iff the index is encrypted.
    cipher: Option<Arc<BlockCipher>>,
    /// Optional decoded-leaf-block cache (see [`Self::with_block_cache`]). When present,
    /// a leaf is decompressed **and decoded** once and the sorted `(key, id)` `Vec` is
    /// held, so a repeated equality/range probe into the same block (e.g. a bulk-write
    /// resolve over a contiguous key range) binary-searches the cached block instead of
    /// re-reading + re-decoding it. `None` = decode every block fresh (the plain default).
    cache: Option<Arc<DecodedBlockCache>>,
    /// The cache `sub`-key for this reader's blocks — the index's ordinal within its
    /// generation. The cache is per-generation, so `(sub, block)` uniquely identifies a
    /// leaf within it.
    cache_sub: u32,
}

impl IsamReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cipher(path, None)
    }

    /// Open an ISAM index by path (local filesystem) — a convenience wrapper
    /// over [`open_src`](IsamReader::open_src) for path-holding callers.
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let src = Arc::new(FileObject::open(path.as_ref())?);
        Self::open_src(src, cipher)
    }

    /// Open an ISAM index from any positional-read source (local file or remote
    /// object), supplying the per-generation cipher for an encrypted index. An
    /// encrypted index opened without a key is refused with a clear error rather
    /// than returning garbage.
    pub fn open_src(src: Arc<dyn RandomReadAt>, cipher: Option<Arc<BlockCipher>>) -> Result<Self> {
        let len = src.len();
        if len < ISAM_MAGIC.len() as u64 + FOOTER_LEN {
            bail!("isam too short");
        }
        let mut magic = [0u8; 8];
        src.read_exact_at(&mut magic, 0)?;
        let encrypted = match &magic {
            m if m == ISAM_MAGIC => false,
            m if m == ISAM_MAGIC_ENC => true,
            _ => bail!("bad isam magic"),
        };
        let cipher = if encrypted {
            match cipher {
                Some(c) => Some(c),
                None => bail!("isam index is encrypted but no key was supplied"),
            }
        } else {
            None
        };
        let footer_len = if encrypted {
            FOOTER_LEN_ENC
        } else {
            FOOTER_LEN
        };
        if len < ISAM_MAGIC.len() as u64 + footer_len {
            bail!("isam too short");
        }
        let mut footer = vec![0u8; footer_len as usize];
        src.read_exact_at(&mut footer, len - footer_len)?;
        let mut fr = &footer[..];
        let top_offset = fr.read_u64::<LittleEndian>()?;
        let top_len = fr.read_u64::<LittleEndian>()?;
        let block_count = fr.read_u64::<LittleEndian>()?;
        let top_nonce = if encrypted {
            let mut n = [0u8; NONCE_LEN];
            fr.read_exact(&mut n)?;
            Some(n)
        } else {
            None
        };

        // `top_offset`/`top_len` come out of the footer, which is **plaintext even for an
        // encrypted index** (the cipher covers the blocks and the top-level body, not the
        // footer that locates them). So they are unauthenticated on-disk `u64`s on every
        // configuration, and `vec![0u8; top_len]` sized one of them straight — a forged length
        // is a 16-exabyte request that aborts the process before the short read can fail.
        // The top-level must lie inside the file; check the claim, then the ceiling.
        if top_offset.saturating_add(top_len) > len {
            return Err(DecodeRejected::OutOfFile {
                what: "isam top-level",
                offset: top_offset,
                len: top_len,
                file_len: len,
            }
            .into());
        }
        codec::check_stored_len(top_len as usize)?;
        let mut stored_top = vec![0u8; top_len as usize];
        src.read_exact_at(&mut stored_top, top_offset)?;
        // Unseal the top-level when encrypted (a wrong key is caught here, at open).
        let top_bytes = match (&cipher, &top_nonce) {
            (Some(c), Some(nonce)) => c.decrypt(nonce, &stored_top)?,
            _ => stored_top,
        };
        let mut tr = &top_bytes[..];
        // `block_count` is likewise an unauthenticated footer `u64`. A top-level entry costs
        // ≥17 bytes (a value tag ‖ u64 offset ‖ u32 comp_len ‖ u32 raw_len; the nonce only
        // adds to that), so reserve no more than the top-level body could hold — the loop
        // below errors on the first short read.
        let mut top = Vec::with_capacity(capacity_for(block_count as usize, top_bytes.len(), 17));
        for _ in 0..block_count {
            let first_key = read_value(&mut tr)?;
            let offset = tr.read_u64::<LittleEndian>()?;
            let comp_len = tr.read_u32::<LittleEndian>()?;
            let raw_len = tr.read_u32::<LittleEndian>()?;
            let nonce = if encrypted {
                let mut n = [0u8; NONCE_LEN];
                tr.read_exact(&mut n)?;
                Some(n)
            } else {
                None
            };
            top.push(TopEntry {
                first_key,
                offset,
                comp_len,
                raw_len,
                nonce,
            });
        }
        Ok(Self {
            src,
            top,
            cipher,
            cache: None,
            cache_sub: 0,
        })
    }

    /// Attach a decoded-leaf-block cache to this reader, keyed under `sub` (the index's
    /// per-generation ordinal). The `cache` is shared across all of a generation's range
    /// readers, so one byte budget bounds them all and dropping the generation frees the
    /// lot. Builder-style so a caller can `open_src(..).with_block_cache(..)`.
    pub fn with_block_cache(mut self, cache: Arc<DecodedBlockCache>, sub: u32) -> Self {
        self.cache = Some(cache);
        self.cache_sub = sub;
        self
    }

    pub fn num_blocks(&self) -> usize {
        self.top.len()
    }

    /// A leaf block's decoded `(key, id)` pairs (ascending by key), served from the
    /// decoded-block cache when one is attached — so a repeated probe into the same block
    /// decompresses + decodes it once and then binary-searches the cached copy — and
    /// decoded fresh into a one-off `Arc` otherwise.
    fn block(&self, b: usize) -> Result<DecodedBlock> {
        match &self.cache {
            Some(cache) => cache.get_or_load(self.cache_sub, b as u32, || {
                Self::decode_block(&self.decompress_block(b)?)
            }),
            None => Ok(Arc::new(Self::decode_block(&self.decompress_block(b)?)?)),
        }
    }

    /// Read + decrypt + decompress leaf block `b` to its raw (decompressed) bytes. The
    /// expensive part (a positional read and a zstd inflate) lives here, alongside the
    /// decode the cache memoises.
    fn decompress_block(&self, b: usize) -> Result<Vec<u8>> {
        let t = &self.top[b];
        // `comp_len` is an unvalidated on-disk `u32`; check the claim before sizing a buffer
        // from it (see `codec::check_stored_len`). `raw_len` below is likewise a claim, which
        // `codec::decompress` enforces as a hard output cap.
        codec::check_stored_len(t.comp_len as usize)?;
        let mut stored = vec![0u8; t.comp_len as usize];
        self.src.read_exact_at(&mut stored, t.offset)?;
        let comp = match (&self.cipher, &t.nonce) {
            (Some(cipher), Some(nonce)) => cipher.decrypt(nonce, &stored)?,
            _ => stored,
        };
        codec::decompress(&comp, t.raw_len as usize)
    }

    /// Decode `(key, id)` pairs from a leaf block's decompressed bytes.
    fn decode_block(raw: &[u8]) -> Result<Vec<(Value, u64)>> {
        let mut r = raw;
        let count = read_uvarint(&mut r)? as usize;
        // Untrusted count out of the (decompressed) leaf block; each entry costs ≥2 bytes
        // (a value tag ‖ an id uvarint). Clamp the reservation — see `wire::capacity_for`.
        let mut out = Vec::with_capacity(capacity_for(count, r.len(), 2));
        for _ in 0..count {
            let key = read_value(&mut r)?;
            let id = read_uvarint(&mut r)?;
            out.push((key, id));
        }
        Ok(out)
    }

    /// Exact-match lookup: all ids whose key equals `key`, sorted ascending.
    pub fn lookup_eq(&self, key: &Value) -> Result<Vec<u64>> {
        if self.top.is_empty() {
            return Ok(Vec::new());
        }
        // Count blocks whose first key is <= key; the matches live in the last
        // such block, possibly continuing backwards into earlier blocks if the
        // key value spans a block boundary.
        let le = self
            .top
            .partition_point(|t| t.first_key.cmp_key(key) != Ordering::Greater);
        if le == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut b = le - 1;
        loop {
            let entries = self.block(b)?;
            // The block is sorted by key: binary-search the first entry >= `key`, then
            // collect the equal run — O(log n) + run-length, not a full O(n) block scan.
            let start = entries.partition_point(|(k, _)| k.cmp_key(key) == Ordering::Less);
            let mut i = start;
            while i < entries.len() && entries[i].0.cmp_key(key) == Ordering::Equal {
                out.push(entries[i].1);
                i += 1;
            }
            // If the block's first key already equals `key`, an equal run may continue
            // backwards into the previous block.
            let first_is_key = entries
                .first()
                .map(|(k, _)| k.cmp_key(key) == Ordering::Equal)
                .unwrap_or(false);
            if first_is_key && b > 0 {
                b -= 1;
            } else {
                break;
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    /// Batch equality lookup: `out[i]` is the ids whose index key equals `keys[i]`,
    /// each inner `Vec` sorted ascending — exactly what `keys.len()` calls to
    /// [`lookup_eq`](Self::lookup_eq) would return, aligned to the input.
    ///
    /// `keys` **must be sorted ascending by `cmp_key` and distinct** (the write-batch
    /// resolver dedups + sorts its business-key values before calling). A single forward
    /// merge-join pass over the leaf blocks then decodes each **touched** block once and
    /// binary-searches every key that falls in it — so a whole batch of `m` keys costs
    /// O(blocks touched) block decompresses instead of O(m) (the bulk-write ISAM floor).
    /// Because `keys` is ascending, the anchor block index is non-decreasing across the
    /// sweep, so a block shared by several consecutive keys is decoded once; a decoded-block
    /// memo carries it forward (and the reader's own decoded-block cache, when attached,
    /// backs the equal-run backward walk that a high-cardinality key can trigger).
    pub fn lookup_eq_sorted(&self, keys: &[&Value]) -> Result<Vec<Vec<u64>>> {
        let mut out: Vec<Vec<u64>> = (0..keys.len()).map(|_| Vec::new()).collect();
        if self.top.is_empty() {
            return Ok(out);
        }
        // Decoded-block memo for this sweep: the anchor block is non-decreasing across the
        // ascending keys, so a block serving several consecutive keys decodes once here.
        let mut memo_b: usize = usize::MAX;
        let mut memo: Option<DecodedBlock> = None;
        for (i, &key) in keys.iter().enumerate() {
            // The equal run for `key` ends in the last block whose first key is <= `key`
            // (mirrors `lookup_eq`); it may extend backwards when that block starts on `key`.
            let le = self
                .top
                .partition_point(|t| t.first_key.cmp_key(key) != Ordering::Greater);
            if le == 0 {
                continue;
            }
            let mut b = le - 1;
            loop {
                let entries = if b == memo_b {
                    memo.as_ref().expect("memo set when memo_b valid").clone()
                } else {
                    let e = self.block(b)?;
                    memo_b = b;
                    memo = Some(e.clone());
                    e
                };
                let start = entries.partition_point(|(k, _)| k.cmp_key(key) == Ordering::Less);
                let mut j = start;
                while j < entries.len() && entries[j].0.cmp_key(key) == Ordering::Equal {
                    out[i].push(entries[j].1);
                    j += 1;
                }
                let first_is_key = entries
                    .first()
                    .map(|(k, _)| k.cmp_key(key) == Ordering::Equal)
                    .unwrap_or(false);
                if first_is_key && b > 0 {
                    b -= 1;
                } else {
                    break;
                }
            }
            out[i].sort_unstable();
        }
        Ok(out)
    }

    /// Range lookup over `[lo, hi]` with per-bound inclusivity. A `None` bound is
    /// unbounded on that side. Returns matching ids, sorted ascending.
    pub fn lookup_range(
        &self,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Result<Vec<u64>> {
        if self.top.is_empty() {
            return Ok(Vec::new());
        }
        // First block that may contain `lo`: the block before the first whose
        // first_key >= lo (or block 0 when lo is unbounded).
        let start = match lo {
            None => 0,
            Some(lo) => {
                let ip = self
                    .top
                    .partition_point(|t| t.first_key.cmp_key(lo) == Ordering::Less);
                ip.saturating_sub(1)
            }
        };

        let past_hi = |k: &Value| match hi {
            None => false,
            Some(hi) => match k.cmp_key(hi) {
                Ordering::Greater => true,
                Ordering::Equal => !hi_inclusive,
                Ordering::Less => false,
            },
        };

        let mut out = Vec::new();
        for b in start..self.top.len() {
            // If this block starts beyond hi, no later block can match.
            if past_hi(&self.top[b].first_key) {
                break;
            }
            let entries = self.block(b)?;
            // Binary-search past the leading entries below `lo` (only the first scanned
            // block has any); from there every entry is `>= lo` (in_lo holds).
            let begin = match lo {
                None => 0,
                Some(lo) => entries.partition_point(|(k, _)| match k.cmp_key(lo) {
                    Ordering::Less => true,
                    Ordering::Equal => !lo_inclusive,
                    Ordering::Greater => false,
                }),
            };
            for (k, id) in &entries[begin..] {
                if past_hi(k) {
                    // entries are sorted; nothing later in this scan matches hi
                    out.sort_unstable();
                    out.dedup();
                    return Ok(out);
                }
                out.push(*id);
            }
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    /// Distinct keys in ascending order, each paired with the number of entries
    /// (ids) sharing that key. One sequential pass over all blocks — no per-key
    /// lookups and no node-record reads. Keys are globally sorted with equal keys
    /// adjacent (see `write_isam`), so a run-length count over the concatenated
    /// blocks is exact; an open run is carried across the block boundary.
    pub fn distinct_key_counts(&self) -> Result<Vec<(Value, u64)>> {
        Ok(self
            .distinct_key_counts_bounded(u64::MAX)?
            .expect("an unbounded count never abandons"))
    }

    /// [`distinct_key_counts`](IsamReader::distinct_key_counts), abandoning as soon
    /// as more than `max_distinct` distinct keys have been seen and returning `None`.
    ///
    /// The caller that wants a *bounded* histogram must say so here rather than
    /// counting everything and checking the length afterwards. On a near-unique index
    /// the two differ by gigabytes: `node_Entity_wikidata_id` over 91.6M Wikidata nodes
    /// has ~91.6M distinct keys, and materialising all of them only to discard the
    /// `Vec` was — measured — the peak RSS of the entire build (5.78 GB, in a phase
    /// that runs for five seconds and keeps none of it).
    pub fn distinct_key_counts_bounded(
        &self,
        max_distinct: u64,
    ) -> Result<Option<Vec<(Value, u64)>>> {
        let mut out: Vec<(Value, u64)> = Vec::new();
        for b in 0..self.top.len() {
            for (k, _) in self.block(b)?.iter() {
                match out.last_mut() {
                    Some((prev, n)) if prev.cmp_key(k) == Ordering::Equal => *n += 1,
                    _ => {
                        if out.len() as u64 >= max_distinct {
                            return Ok(None);
                        }
                        out.push((k.clone(), 1));
                    }
                }
            }
        }
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoded_bytes_counts_str_key_heap() {
        // Fixed-width keys: exactly the slice footprint, no heap.
        let ints: Vec<(Value, u64)> = (0..8u64).map(|i| (Value::Int(i as i64), i)).collect();
        assert_eq!(decoded_bytes(&ints), std::mem::size_of_val(ints.as_slice()));

        // String keys own heap the old `size_of_val`-only estimate ignored.
        let strs: Vec<(Value, u64)> = vec![
            (Value::Str("a-fairly-long-string-key".into()), 0),
            (Value::Str("another-long-key-value".into()), 1),
        ];
        let base = std::mem::size_of_val(strs.as_slice());
        let heap: usize = strs
            .iter()
            .map(|(v, _)| match v {
                Value::Str(s) => s.len(),
                _ => 0,
            })
            .sum();
        assert_eq!(decoded_bytes(&strs), base + heap);
        assert!(decoded_bytes(&strs) > base, "Str heap must be counted");
    }

    // HIK-80: the ISAM footer is **plaintext even for an encrypted index** — the cipher covers
    // the blocks and the top-level body, not the footer that locates them. `top_len` is an
    // unauthenticated on-disk `u64` that sized a read buffer directly, so forging it is an
    // allocator abort at index open, on every configuration. Reaching the assertion is the
    // proof: pre-fix the test binary dies allocating 16 exabytes.
    #[test]
    fn forged_footer_top_len_is_refused_at_open() {
        let path = tmp("forged_top_len");
        let entries: Vec<(Value, u64)> = (0..200u64).map(|i| (Value::Int(i as i64), i)).collect();
        write_isam(&path, entries, 1 << 12, 3).unwrap();
        assert!(IsamReader::open(&path).is_ok());

        // Overwrite `top_len` (the second u64 of the 24-byte plaintext footer) with a claim the
        // file cannot possibly honour. Every other byte of the index is untouched.
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        let top_len_at = n - FOOTER_LEN as usize + 8;
        bytes[top_len_at..top_len_at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let err = match IsamReader::open(&path) {
            Ok(_) => panic!("a forged top_len must be refused"),
            Err(e) => e,
        };
        assert!(
            matches!(
                err.downcast_ref::<DecodeRejected>(),
                Some(DecodeRejected::OutOfFile { .. })
            ),
            "expected a typed OutOfFile rejection, got: {err}"
        );
        let _ = std::fs::remove_file(&path);
    }

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_isam_{}_{}", std::process::id(), name))
    }

    fn linear_eq(entries: &[(Value, u64)], key: &Value) -> Vec<u64> {
        let mut v: Vec<u64> = entries
            .iter()
            .filter(|(k, _)| k.cmp_key(key) == Ordering::Equal)
            .map(|(_, id)| *id)
            .collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn string_index_eq_matches_linear_scan() {
        let path = tmp("streq");
        // Repeated source names, the camelid-scale shape; many ids per key.
        let mut entries = Vec::new();
        let sources = ["Fowler-2010", "Whitehead-2024", "Smith-1999"];
        for id in 0..600u64 {
            let s = sources[(id % 3) as usize];
            entries.push((Value::Str(s.to_string()), id));
        }
        // Tiny blocks so keys span many blocks (stresses the boundary walk).
        write_isam(&path, entries.clone(), 64, 3).unwrap();

        let r = IsamReader::open(&path).unwrap();
        assert!(r.num_blocks() > 1);
        for s in sources {
            let key = Value::Str(s.to_string());
            assert_eq!(r.lookup_eq(&key).unwrap(), linear_eq(&entries, &key));
        }
        // Absent key.
        assert!(r.lookup_eq(&Value::Str("Nope".into())).unwrap().is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cached_reader_matches_uncached_and_reuses_blocks() {
        let path = tmp("cache");
        // Contiguous integer keys (the `wikidata_id` shape), tiny blocks so a key range
        // spans many blocks — the bulk-write-resolve case that re-probes the same block.
        let entries: Vec<(Value, u64)> = (0..600u64).map(|i| (Value::Int(i as i64), i)).collect();
        write_isam(&path, entries.clone(), 64, 3).unwrap();

        let plain = IsamReader::open(&path).unwrap();
        assert!(plain.num_blocks() > 1);

        let cache = Arc::new(DecodedBlockCache::new(1 << 20));
        let cached = IsamReader::open(&path)
            .unwrap()
            .with_block_cache(cache.clone(), 7);

        // A cached reader answers every lookup (eq + range) identically to the uncached
        // one — the binary-search path must not change results.
        for i in [0u64, 1, 250, 599] {
            let k = Value::Int(i as i64);
            assert_eq!(
                cached.lookup_eq(&k).unwrap(),
                plain.lookup_eq(&k).unwrap(),
                "cached lookup diverges at {i}"
            );
        }
        assert!(cached.lookup_eq(&Value::Int(10_000)).unwrap().is_empty());
        assert_eq!(
            cached
                .lookup_range(Some(&Value::Int(100)), true, Some(&Value::Int(400)), false)
                .unwrap(),
            plain
                .lookup_range(Some(&Value::Int(100)), true, Some(&Value::Int(400)), false)
                .unwrap(),
            "cached range diverges"
        );

        // A warm block is not re-decoded: sweeping every key once caches each block
        // exactly once, so the cache holds precisely `num_blocks` decoded blocks (with a
        // budget big enough to retain them all).
        let fresh = Arc::new(DecodedBlockCache::new(1 << 20));
        let r2 = IsamReader::open(&path)
            .unwrap()
            .with_block_cache(fresh.clone(), 0);
        for i in 0..600u64 {
            let _ = r2.lookup_eq(&Value::Int(i as i64)).unwrap();
        }
        assert_eq!(
            fresh.len(),
            plain.num_blocks(),
            "every block decoded exactly once and retained"
        );

        let _ = std::fs::remove_file(&path);
    }

    // ---- HIK-106: sharded CLOCK DecodedBlockCache ----

    fn ints(n: u64) -> Vec<(Value, u64)> {
        (0..n).map(|i| (Value::Int(i as i64), i)).collect()
    }

    /// A cache too small to give each shard `MIN_SHARD_BYTES` must stay a single
    /// eviction domain — a small/test cache keeps exactly the old bound, with no
    /// per-shard keep-one floor to multiply.
    #[test]
    fn a_small_budget_stays_single_sharded() {
        assert_eq!(DecodedBlockCache::new(1).shard_count(), 1);
        assert_eq!(DecodedBlockCache::new(1 << 20).shard_count(), 1);
        assert_eq!(DecodedBlockCache::new(MIN_SHARD_BYTES).shard_count(), 1);
        // Big enough for two shards of MIN_SHARD_BYTES (given >= 1 cpu).
        assert!(DecodedBlockCache::new(64 << 20).shard_count() >= 2);
    }

    /// CLOCK's core promise, and what keeps the miss rate honest: a block read again
    /// survives; the one that was not is the victim. (This is the strict-LRU victim
    /// choice too — the property the cache is *for*.)
    #[test]
    fn a_referenced_block_outlives_an_unreferenced_one() {
        // Two ~one-int blocks fit; a third forces a sweep. `size_of::<(Value,u64)>()`
        // is the per-entry footprint; give budget for two, plus slack for the Vec.
        let one = decoded_bytes(&ints(1));
        let cache = DecodedBlockCache::with_shards(one * 2, 1);
        let load = |fill: u64| move || Ok(vec![(Value::Int(fill as i64), fill)]);

        cache.get_or_load(0, 0, load(0)).unwrap();
        cache.get_or_load(0, 1, load(1)).unwrap();
        assert_eq!(cache.len(), 2);
        // Read block 0 again — now block 1 is the only unreferenced entry.
        cache.get_or_load(0, 0, load(0)).unwrap();
        // Over budget → the sweep must take block 1.
        cache.get_or_load(0, 2, load(2)).unwrap();

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.metrics().evictions, 1);
        let mut reloaded_1 = false;
        cache
            .get_or_load(0, 1, || {
                reloaded_1 = true;
                Ok(vec![(Value::Int(1), 1)])
            })
            .unwrap();
        assert!(reloaded_1, "the unreferenced block should be the victim");
        assert!(cache.bytes() <= one * 2);
    }

    /// The block a miss just decoded must not be evicted by that same miss's sweep: a
    /// strictly-ascending scan through a budget that holds ~one block would otherwise
    /// re-decode the block once per record it contains (the exact ISAM bulk-resolve
    /// pathology). Walk every leaf in order and require the in-block re-probes to hit.
    /// The block a miss just decoded must not be evicted by that same miss's sweep. A
    /// budget that holds ~one block: inserting a second must sweep out the *first*, not
    /// the freshly-inserted second — otherwise a strictly-ascending scan through a tiny
    /// budget would re-decode the block it just built once per record (the ISAM
    /// bulk-resolve pathology). Driven at the cache API so the eviction choice is exact.
    #[test]
    fn a_just_loaded_block_survives_its_own_insert_sweep() {
        let one = decoded_bytes(&ints(4)); // a multi-record block's footprint
                                           // Big enough for one block, too small for two: every new block forces a sweep.
        let cache = DecodedBlockCache::with_shards(one + one / 2, 1);
        let load = |b: u64| {
            move || {
                Ok((0..4u64)
                    .map(|i| (Value::Int(i as i64), b * 4 + i))
                    .collect())
            }
        };

        cache.get_or_load(0, 0, load(0)).unwrap();
        cache.get_or_load(0, 1, load(1)).unwrap(); // over budget → sweep
        assert_eq!(cache.len(), 1, "only one block fits");
        assert_eq!(cache.metrics().evictions, 1);

        // Block 1 (the block the sweeping insert just added) must be the survivor.
        let mut reloaded_1 = false;
        cache
            .get_or_load(0, 1, || {
                reloaded_1 = true;
                Ok(vec![(Value::Int(0), 0)])
            })
            .unwrap();
        assert!(
            !reloaded_1,
            "the just-inserted block must survive its own insert's sweep"
        );
        // …and block 0 (the older one) is correctly the victim.
        let mut reloaded_0 = false;
        cache
            .get_or_load(0, 0, || {
                reloaded_0 = true;
                Ok(vec![(Value::Int(0), 0)])
            })
            .unwrap();
        assert!(reloaded_0, "the older block should have been evicted");
        assert!(cache.bytes() <= (one + one / 2).max(one));
    }

    /// The whole-scan flavour of the same guarantee, through the real ISAM path: a
    /// budget generous enough to hold the leaves keeps every in-block re-probe a hit
    /// (no re-decode), and results stay correct.
    #[test]
    fn ascending_scan_reuses_cached_leaves() {
        let path = tmp("dbc_scan_window");
        write_isam(&path, ints(600), 64, 3).unwrap();
        let reader = IsamReader::open(&path).unwrap();
        let num_blocks = reader.num_blocks() as u64;
        assert!(num_blocks > 1 && num_blocks < 600);

        let cache = Arc::new(DecodedBlockCache::new(1 << 20)); // holds them all
        let cached = IsamReader::open(&path)
            .unwrap()
            .with_block_cache(cache.clone(), 0);
        for i in 0..600u64 {
            assert_eq!(cached.lookup_eq(&Value::Int(i as i64)).unwrap(), vec![i]);
        }
        let m = cache.metrics();
        // Each leaf decoded exactly once; the rest of the 600 probes hit warm leaves.
        assert_eq!(m.misses, num_blocks, "each leaf decoded once");
        assert!(m.hits >= 600 - num_blocks, "in-block re-probes must hit");
        assert_eq!(cache.len() as u64, num_blocks);
        let _ = std::fs::remove_file(&path);
    }

    /// The HIK-101 heap accounting must survive the rewrite: a block of long `Str` keys
    /// counts its heap, so a string-keyed cache honours its byte budget instead of
    /// wildly overshooting it (the old `size_of_val`-only estimate).
    #[test]
    fn str_key_heap_is_counted_in_the_budget() {
        let big = |b: u32| {
            move || {
                Ok((0..10u64)
                    .map(|i| (Value::Str("x".repeat(4096)), b as u64 * 10 + i))
                    .collect())
            }
        };
        // Budget in the fixed-struct-only world would hold dozens of these; with heap
        // counted it holds ~one, so the resident bytes stay bounded.
        let cache = DecodedBlockCache::with_shards(64 * 1024, 1);
        for b in 0..32u32 {
            cache.get_or_load(0, b, big(b)).unwrap();
        }
        assert!(
            cache.bytes() <= 64 * 1024,
            "heap-aware budget must hold: {} > 65536",
            cache.bytes()
        );
        assert!(cache.metrics().evictions > 0, "heap pressure must evict");
        // And it really counted the heap, not just the struct.
        let strs: Vec<(Value, u64)> = (0..10).map(|i| (Value::Str("x".repeat(4096)), i)).collect();
        assert!(decoded_bytes(&strs) > std::mem::size_of_val(strs.as_slice()));
    }

    /// The byte budget is a product guarantee, so it must hold while N threads hammer
    /// the cache — not just single-threaded. Sharding is what could break it (each
    /// shard evicts independently), so force many shards and check the *global* sum,
    /// both continuously during the load and at rest.
    #[test]
    fn concurrent_load_never_exceeds_the_byte_budget() {
        const BUDGET: usize = 64 * 1024;
        let cache = Arc::new(DecodedBlockCache::with_shards(BUDGET, 16));
        assert_eq!(cache.shard_count(), 16);
        // Each block is 8 ints ≈ small, working set far larger than the budget so
        // eviction runs constantly.
        let block = |b: u32| {
            move || {
                Ok((0..8u64)
                    .map(|i| (Value::Int(i as i64), b as u64))
                    .collect())
            }
        };

        let threads: Vec<_> = (0..8u32)
            .map(|t| {
                let cache = cache.clone();
                std::thread::spawn(move || {
                    for i in 0..4000u32 {
                        let key_block = i % 600;
                        cache
                            .get_or_load(t % 3, key_block, block(key_block))
                            .unwrap();
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
        let m = cache.metrics();
        assert_eq!(m.hits + m.misses, 8 * 4000, "every access is counted once");
        assert!(m.evictions > 0);
    }

    /// Two readers missing the same key at once must converge on one `Arc` (the second
    /// insert deduplicates), so the block is never double-counted in `bytes`.
    #[test]
    fn concurrent_duplicate_loads_deduplicate() {
        let cache = Arc::new(DecodedBlockCache::new(1 << 20));
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let cache = cache.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    cache
                        .get_or_load(7, 2, || {
                            Ok((0..100u64).map(|i| (Value::Int(i as i64), i)).collect())
                        })
                        .unwrap()
                })
            })
            .collect();
        let arcs: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        for a in &arcs {
            assert!(Arc::ptr_eq(a, &arcs[0]), "all readers share one block");
        }
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.bytes(),
            decoded_bytes(&arcs[0]),
            "counted exactly once"
        );
    }

    /// The regression test for the finding itself: with the hit path on one global
    /// exclusive lock (the pre-HIK-106 shape) adding threads *reduces* aggregate hit
    /// throughput. It must now scale. The 1.2× bar is far below the multiple the
    /// sharded read-lock path achieves and far above the ~0.2× a global mutex managed,
    /// so it discriminates even on a busy machine.
    #[test]
    fn concurrent_hits_scale_with_threads() {
        const THREADS: usize = 4;
        if std::thread::available_parallelism().map_or(1, |n| n.get()) < THREADS {
            return; // not enough cores to say anything
        }
        const BLOCKS: u32 = 256;
        let cache = Arc::new(DecodedBlockCache::new(64 << 20));
        for b in 0..BLOCKS {
            cache
                .get_or_load(0, b, || {
                    Ok((0..64u64).map(|i| (Value::Int(i as i64), i)).collect())
                })
                .unwrap();
        }

        let rate = |n: usize| -> f64 {
            const PER_THREAD: u64 = 200_000;
            let barrier = Arc::new(std::sync::Barrier::new(n));
            let handles: Vec<_> = (0..n)
                .map(|w| {
                    let cache = cache.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        let start = std::time::Instant::now();
                        for i in 0..PER_THREAD {
                            let b = (w as u64 * 37 + i * 7) % BLOCKS as u64;
                            let v = cache
                                .get_or_load(0, b as u32, || unreachable!("resident"))
                                .unwrap();
                            std::hint::black_box(v.len());
                        }
                        start.elapsed()
                    })
                })
                .collect();
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
    fn lookup_eq_sorted_matches_point_lookups() {
        // Two shapes that stress the merge-join sweep: contiguous ints (each key one id,
        // keys span many tiny blocks) and repeated strings (many ids per key, equal runs
        // span block boundaries). The sweep must be byte-identical to N point lookups, and
        // its absent-key gaps must be empty.
        for shape in ["ints", "strings"] {
            let path = tmp(&format!("sweep_{shape}"));
            let entries: Vec<(Value, u64)> = if shape == "ints" {
                (0..600u64).map(|i| (Value::Int(i as i64), i)).collect()
            } else {
                let sources = ["Fowler-2010", "Whitehead-2024", "Smith-1999"];
                (0..600u64)
                    .map(|id| (Value::Str(sources[(id % 3) as usize].to_string()), id))
                    .collect()
            };
            write_isam(&path, entries.clone(), 64, 3).unwrap();
            let r = IsamReader::open(&path).unwrap();
            assert!(r.num_blocks() > 1);

            // A sorted, distinct probe list mixing present and absent keys.
            let mut keys: Vec<Value> = if shape == "ints" {
                (0..600i64).step_by(1).map(Value::Int).collect()
            } else {
                vec![
                    Value::Str("Aaa".into()),
                    Value::Str("Fowler-2010".into()),
                    Value::Str("Smith-1999".into()),
                    Value::Str("Whitehead-2024".into()),
                    Value::Str("Zzz".into()),
                ]
            };
            if shape == "ints" {
                keys.push(Value::Int(10_000)); // an absent tail key
            }
            keys.sort_by(|a, b| a.cmp_key(b));
            let refs: Vec<&Value> = keys.iter().collect();

            let swept = r.lookup_eq_sorted(&refs).unwrap();
            assert_eq!(swept.len(), keys.len());
            for (k, got) in keys.iter().zip(&swept) {
                assert_eq!(got, &r.lookup_eq(k).unwrap(), "sweep diverges at {k:?}");
            }
            // An empty probe list is a well-formed no-op.
            assert!(r.lookup_eq_sorted(&[]).unwrap().is_empty());
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    #[ignore = "perf measurement — smaller leaf blocks make an uncached point lookup cheaper"]
    fn bench_range_block_size_point_lookups() {
        // 1M contiguous int keys (the wikidata_id shape) built at 256 KiB vs 16 KiB leaf
        // blocks; time uncached equality probes over an ascending run.
        let entries: Vec<(Value, u64)> = (0..1_000_000u64)
            .map(|i| (Value::Int(i as i64), i))
            .collect();
        for bs in [256 * 1024usize, 16 * 1024] {
            let path = tmp(&format!("bs_{bs}"));
            write_isam(&path, entries.clone(), bs, 3).unwrap();
            let r = IsamReader::open(&path).unwrap();
            let n = 20_000u64;
            let t = std::time::Instant::now();
            for k in 0..n {
                let _ = r.lookup_eq(&Value::Int(k as i64)).unwrap();
            }
            println!(
                "block_size={bs}: {} blocks (~{} entries/block), {:.1} µs/uncached-lookup",
                r.num_blocks(),
                1_000_000 / r.num_blocks().max(1),
                t.elapsed().as_micros() as f64 / n as f64
            );
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    fn distinct_key_counts_matches_lookup_eq() {
        let path = tmp("distinct");
        // Repeated keys whose runs span block boundaries (tiny blocks below).
        let mut entries = Vec::new();
        let sources = ["Fowler-2010", "Smith-1999", "Whitehead-2024"];
        for id in 0..600u64 {
            entries.push((Value::Str(sources[(id % 3) as usize].to_string()), id));
        }
        write_isam(&path, entries.clone(), 64, 3).unwrap();
        let r = IsamReader::open(&path).unwrap();
        assert!(r.num_blocks() > 1);

        let got = r.distinct_key_counts().unwrap();
        // Keys ascending and distinct.
        assert_eq!(
            got.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            sources
                .iter()
                .map(|s| Value::Str(s.to_string()))
                .collect::<Vec<_>>(),
        );
        // Each count equals the per-key lookup length (the run-length is exact).
        for (k, n) in &got {
            assert_eq!(*n, r.lookup_eq(k).unwrap().len() as u64);
            assert_eq!(*n, linear_eq(&entries, k).len() as u64);
        }
        // Counts sum to the total number of entries.
        assert_eq!(
            got.iter().map(|(_, n)| n).sum::<u64>(),
            entries.len() as u64
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_isam_sorted_is_byte_identical_to_write_isam() {
        // Feeding a pre-sorted stream to write_isam_sorted must reproduce exactly
        // the bytes write_isam emits after its in-RAM sort (deterministic zstd,
        // plaintext). This is the contract the external builder relies on.
        let mut entries = Vec::new();
        let sources = ["Fowler-2010", "Whitehead-2024", "Smith-1999", "Adams-2001"];
        for id in 0..500u64 {
            entries.push((Value::Str(sources[(id % 4) as usize].to_string()), id));
        }
        // Mix in some ints to exercise cross-type ordering at the boundary.
        for id in 500..600u64 {
            entries.push((Value::Int((id as i64) % 7), id));
        }

        let p1 = tmp("sorted_ref");
        write_isam(&p1, entries.clone(), 96, 3).unwrap();

        let mut sorted = entries.clone();
        sorted.sort_by(|a, b| a.0.cmp_key(&b.0).then(a.1.cmp(&b.1)));
        let p2 = tmp("sorted_stream");
        write_isam_sorted(&p2, sorted.into_iter().map(Ok), 96, 3, None).unwrap();

        assert_eq!(
            std::fs::read(&p1).unwrap(),
            std::fs::read(&p2).unwrap(),
            "write_isam_sorted output differs from write_isam"
        );
        let _ = std::fs::remove_file(&p1);
        let _ = std::fs::remove_file(&p2);
    }

    #[test]
    fn int_index_range_matches_linear_scan() {
        let path = tmp("intrange");
        let entries: Vec<(Value, u64)> = (0..400u64).map(|i| (Value::Int(i as i64), i)).collect();
        write_isam(&path, entries.clone(), 128, 3).unwrap();
        let r = IsamReader::open(&path).unwrap();

        // [100, 200) inclusive lo, exclusive hi.
        let got = r
            .lookup_range(Some(&Value::Int(100)), true, Some(&Value::Int(200)), false)
            .unwrap();
        let want: Vec<u64> = (100..200).collect();
        assert_eq!(got, want);

        // Unbounded below, <= 5.
        let got = r
            .lookup_range(None, true, Some(&Value::Int(5)), true)
            .unwrap();
        assert_eq!(got, (0..=5).collect::<Vec<_>>());

        // Unbounded above, > 395.
        let got = r
            .lookup_range(Some(&Value::Int(395)), false, None, true)
            .unwrap();
        assert_eq!(got, (396..400).collect::<Vec<_>>());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encrypted_index_roundtrips_and_refuses_wrong_or_absent_key() {
        let path = tmp("enc");
        let cipher = Arc::new(BlockCipher::from_master(b"isam-master", &[5u8; 32]));
        let mut entries = Vec::new();
        let sources = ["Fowler-2010", "Whitehead-2024", "Smith-1999"];
        for id in 0..600u64 {
            entries.push((Value::Str(sources[(id % 3) as usize].to_string()), id));
        }
        // Tiny blocks → many encrypted blocks, each with its own nonce.
        write_isam_with_cipher(&path, entries.clone(), 64, 3, Some(cipher.clone())).unwrap();

        // Right key: lookups match a linear scan.
        let r = IsamReader::open_with_cipher(&path, Some(cipher)).unwrap();
        assert!(r.num_blocks() > 1);
        for s in sources {
            let key = Value::Str(s.to_string());
            assert_eq!(r.lookup_eq(&key).unwrap(), linear_eq(&entries, &key));
        }

        // The raw key values must not appear in plaintext on disk.
        let bytes = std::fs::read(&path).unwrap();
        assert!(!bytes
            .windows("Whitehead-2024".len())
            .any(|w| w == b"Whitehead-2024"));

        // Absent key: refused at open.
        assert!(IsamReader::open(&path).is_err());

        // Wrong key: refused at open — the sealed top-level fails its tag check
        // before any lookup, so a wrong key never even sees the block directory.
        let wrong = Arc::new(BlockCipher::from_master(b"nope", &[5u8; 32]));
        assert!(IsamReader::open_with_cipher(&path, Some(wrong)).is_err());
        let _ = std::fs::remove_file(&path);
    }

    // TEMPORARY (red observation, HIK-140): the pre-fix shims. `for_file` does not exist
    // yet, so a file cipher *is* the generation cipher — which is exactly the bug.
    fn gen_cipher(master: &[u8], salt: &[u8]) -> Arc<BlockCipher> {
        Arc::new(BlockCipher::from_master(master, salt))
    }
    fn file_cipher(c: &Arc<BlockCipher>, _name: &str) -> Arc<BlockCipher> {
        c.clone()
    }

    fn isam_of(path: &std::path::Path, name: &str, gen: &Arc<BlockCipher>, tag: u64) {
        let entries: Vec<(Value, u64)> = (0..600u64)
            .map(|id| (Value::Int((id + tag * 10_000) as i64), id))
            .collect();
        write_isam_with_cipher(path, entries, 64, 3, Some(file_cipher(gen, name))).unwrap();
    }

    /// HIK-140: an ISAM index is bound to its file name, so a whole index lifted from
    /// one `range/*.isam` into another of the same generation — same key, same
    /// structure — does not open. Today the sealed top-level opens under the shared
    /// per-generation key and the reader silently serves the *wrong index's* postings.
    #[test]
    fn isam_refuses_an_index_lifted_from_another_file() {
        use crate::crypto::AeadRejected;
        let gen = gen_cipher(b"isam-master", &[6u8; 32]);
        let a = tmp("lift_isam_a");
        let b = tmp("lift_isam_b");
        isam_of(&a, "range/title.isam", &gen, 0);
        isam_of(&b, "range/author.isam", &gen, 1);
        std::fs::copy(&a, &b).unwrap();

        let err =
            match IsamReader::open_with_cipher(&b, Some(file_cipher(&gen, "range/author.isam"))) {
                Ok(_) => panic!("range/title.isam must not open as range/author.isam"),
                Err(e) => e,
            };
        assert_eq!(
            err.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch),
            "cross-file substitution must fail in the AEAD: {err:#}"
        );
        let _ = std::fs::remove_file(&a);
        let _ = std::fs::remove_file(&b);
    }

    /// HIK-140: the top-level block is sealed under a reserved ordinal tag, so a *leaf*
    /// block of the same index cannot be presented as the top level. The footer that
    /// locates the top level is plaintext, so repointing it needs no key — only the
    /// leaf's nonce, which is not a secret.
    #[test]
    fn isam_refuses_a_leaf_promoted_into_the_top_slot() {
        use crate::crypto::AeadRejected;
        let path = tmp("leaf_as_top");
        let gen = gen_cipher(b"isam-master", &[7u8; 32]);
        isam_of(&path, "range/title.isam", &gen, 0);

        // Learn leaf 0's location + nonce (an attacker who has seen an earlier copy of
        // this index knows them; nonces are not secret).
        let r = IsamReader::open_with_cipher(&path, Some(file_cipher(&gen, "range/title.isam")))
            .unwrap();
        assert!(r.num_blocks() > 1);
        let (leaf_off, leaf_len, leaf_nonce) = {
            let leaf = &r.top[0];
            (leaf.offset, leaf.comp_len, leaf.nonce.unwrap())
        };
        drop(r);

        // Repoint the plaintext footer at leaf 0, claiming it is the top level.
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        let f = n - FOOTER_LEN_ENC as usize;
        bytes[f..f + 8].copy_from_slice(&leaf_off.to_le_bytes());
        bytes[f + 8..f + 16].copy_from_slice(&(leaf_len as u64).to_le_bytes());
        bytes[f + 16..f + 24].copy_from_slice(&1u64.to_le_bytes());
        bytes[f + 24..f + 24 + NONCE_LEN].copy_from_slice(&leaf_nonce);
        std::fs::write(&path, &bytes).unwrap();

        let err = match IsamReader::open_with_cipher(
            &path,
            Some(file_cipher(&gen, "range/title.isam")),
        ) {
            Ok(_) => panic!("a leaf block must not open as the top level"),
            Err(e) => e,
        };
        assert_eq!(
            err.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch),
            "a leaf promoted into the top slot must fail in the AEAD, not later in the \
             decode of whatever it happens to inflate to: {err:#}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
