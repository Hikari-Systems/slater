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
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::codec;
use crate::crypto::{BlockCipher, NONCE_LEN};
use crate::ids::Value;
use crate::store::fs::FileObject;
use crate::store::RandomReadAt;
use crate::wire::{read_uvarint, read_value, write_uvarint, write_value};

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

/// Rough resident-byte estimate of a decoded block, for LRU budgeting. Exact for
/// fixed-width keys (Int/Float); a small under-count for `Str` heap, which the budget
/// tolerates.
fn decoded_bytes(entries: &[(Value, u64)]) -> usize {
    std::mem::size_of_val(entries)
}

struct DbcEntry {
    value: DecodedBlock,
    bytes: usize,
    tick: u64,
}

struct DbcInner {
    map: HashMap<(u32, u32), DbcEntry>,
    /// `tick → key`, ascending — the front is least-recently-used.
    order: BTreeMap<u64, (u32, u32)>,
    tick: u64,
    bytes: usize,
    budget: usize,
}

/// A byte-budgeted LRU of **decoded** ISAM leaf blocks, keyed by `(sub, block)` where
/// `sub` is the index's per-generation ordinal. One instance is shared across a
/// generation's range readers.
///
/// This caches decoded entries, not raw bytes: an ISAM leaf can hold tens of thousands
/// of `(key, id)` pairs, so decoding it on *every* probe (as a raw-byte cache would
/// still force) dominates a point lookup. Decoding once and holding the sorted `Vec`
/// lets a repeated equality/range probe **binary-search** the cached block — O(log n)
/// instead of O(n) — which is the bulk-write-resolve / indexed-seek hot path. Modelled
/// on `blockcache::BlockCache` (same LRU shape); loads run outside the lock.
pub struct DecodedBlockCache {
    inner: Mutex<DbcInner>,
}

impl DecodedBlockCache {
    /// Create a cache with the given byte budget (clamped to at least 1).
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(DbcInner {
                map: HashMap::new(),
                order: BTreeMap::new(),
                tick: 0,
                bytes: 0,
                budget: budget_bytes.max(1),
            }),
        }
    }

    /// Fetch decoded block `(sub, block)`, decoding it with `load` on a miss. `load`
    /// runs **outside** the lock so a slow decompress+decode never serialises other
    /// readers; a concurrent duplicate load deduplicates to the first insert's `Arc`.
    fn get_or_load(
        &self,
        sub: u32,
        block: u32,
        load: impl FnOnce() -> Result<Vec<(Value, u64)>>,
    ) -> Result<DecodedBlock> {
        let key = (sub, block);
        {
            let mut g = self.inner.lock().unwrap();
            if let Some(v) = g.touch_get(&key) {
                return Ok(v);
            }
        }
        let value: DecodedBlock = Arc::new(load()?);
        let bytes = decoded_bytes(&value);
        let mut g = self.inner.lock().unwrap();
        Ok(g.insert(key, value, bytes))
    }
}

impl DbcInner {
    fn next_tick(&mut self) -> u64 {
        let t = self.tick;
        self.tick += 1;
        t
    }

    fn touch_get(&mut self, key: &(u32, u32)) -> Option<DecodedBlock> {
        let (value, old_tick) = {
            let e = self.map.get(key)?;
            (e.value.clone(), e.tick)
        };
        self.order.remove(&old_tick);
        let new_tick = self.next_tick();
        self.order.insert(new_tick, *key);
        self.map.get_mut(key).unwrap().tick = new_tick;
        Some(value)
    }

    /// Insert (or return an existing concurrent load), then evict LRU until within
    /// budget, keeping at least one entry so a block larger than the budget is returnable.
    fn insert(&mut self, key: (u32, u32), value: DecodedBlock, bytes: usize) -> DecodedBlock {
        if let Some(existing) = self.touch_get(&key) {
            return existing;
        }
        let tick = self.next_tick();
        self.order.insert(tick, key);
        self.map.insert(
            key,
            DbcEntry {
                value: value.clone(),
                bytes,
                tick,
            },
        );
        self.bytes += bytes;
        while self.bytes > self.budget && self.order.len() > 1 {
            let (&lru_tick, &lru_key) = self.order.iter().next().unwrap();
            self.order.remove(&lru_tick);
            if let Some(e) = self.map.remove(&lru_key) {
                self.bytes -= e.bytes;
            }
        }
        value
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

        let mut stored_top = vec![0u8; top_len as usize];
        src.read_exact_at(&mut stored_top, top_offset)?;
        // Unseal the top-level when encrypted (a wrong key is caught here, at open).
        let top_bytes = match (&cipher, &top_nonce) {
            (Some(c), Some(nonce)) => c.decrypt(nonce, &stored_top)?,
            _ => stored_top,
        };
        let mut tr = &top_bytes[..];
        let mut top = Vec::with_capacity(block_count as usize);
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
        let mut out = Vec::with_capacity(count);
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
            fresh.inner.lock().unwrap().map.len(),
            plain.num_blocks(),
            "every block decoded exactly once and retained"
        );

        let _ = std::fs::remove_file(&path);
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
}
