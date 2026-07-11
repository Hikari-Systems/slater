// SPDX-License-Identifier: Apache-2.0
//! Optional local-SSD second cache tier for the S3 backend (enabled by the `s3`
//! cargo feature, opt-in at runtime via `diskCacheBytes > 0`).
//!
//! The S3 backend serves every cold block as an HTTP `Range` GET (~10–50 ms
//! RTT). The in-memory `BlockCache` is small (bounded-RSS is the headline
//! guarantee), so on any working set larger than RAM the same blocks are
//! re-fetched from S3 on every spill. This cache sits *below* the readers and
//! *below* decrypt/decompress as a [`RandomReadAt`] decorator: a block evicted
//! from RAM is served from local disk (~0.1 ms) instead of S3, and survives the
//! in-memory eviction.
//!
//! ## What it stores (and what it deliberately does not do)
//!
//! It is an **inclusive read-through of the *sealed* S3 bytes**, keyed by
//! `(object_key, offset, len)` — exactly the bytes `read_exact_at` returns from
//! S3, which are already compressed and (for encrypted generations) already
//! AEAD-sealed. The cache layer is therefore **key-free**: it never decrypts and
//! never re-encrypts, so at-rest status is preserved for free (an encrypted
//! generation lands on disk still sealed, a plaintext one lands plaintext — both
//! match S3). Decrypt/decompress happen *above* this layer, unchanged; the cache
//! only swaps the fetch source (slow S3 GET → fast local read).
//!
//! A victim cache (write-on-RAM-eviction) was rejected: the RAM `BlockCache`
//! holds decompressed *plaintext*, so spilling it would force a re-encrypt on
//! every eviction to keep the at-rest claim. Storing the sealed bytes avoids that
//! entirely. The cost is bounded duplication (a hot block is in both RAM and
//! disk), capped by the *RAM* budget and a rounding error against a disk cache
//! that is meant to be ≫ RAM.
//!
//! ## Concurrency model
//!
//! Reads serve off the query thread: a hit reads the cache file off-lock, a miss
//! returns the S3 bytes immediately and then **write-behind** enqueues them to a
//! bounded channel. A single background writer thread owns *all* disk mutations
//! (writes, LRU eviction, self-heal deletes), so the query thread never blocks on
//! disk I/O and the channel sheds under pressure (a dropped write just re-fetches
//! later) rather than stalling queries.
//!
//! ## Self-heal
//!
//! Each cache file carries a CRC-32 of its payload, verified on every read; a
//! mismatch (bit-rot, torn write) evicts the entry and returns a miss → S3
//! refetch. Corruption is never served. This is key-free and covers both
//! encrypted and plaintext generations; it is integrity-only, not a security MAC
//! (the sealed bytes already carry their own AEAD above this layer).

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::{ObjectStore, RandomReadAt};

/// Cache-file magic (`S`later `D`isk `C`ache, format v1).
const MAGIC: &[u8; 4] = b"SDC1";
const VERSION: u8 = 1;

/// Bound on the write-behind channel. Past this many queued writes the query
/// thread drops the write (the block is simply re-fetched on its next miss)
/// rather than blocking on disk — the channel is a relief valve, not a queue we
/// guarantee to drain.
const WRITE_QUEUE_DEPTH: usize = 1024;

/// A request to the background writer thread.
enum Req {
    /// Write `payload` (the sealed S3 bytes for `key`@`offset`) into the cache.
    Write {
        name: String,
        key: String,
        offset: u64,
        payload: Vec<u8>,
    },
    /// Delete a cache file whose entry the reader already removed from the index
    /// after a CRC/parse failure (self-heal).
    Delete { name: String },
    /// Drain the queue up to this point and acknowledge (test hook).
    Flush(SyncSender<()>),
    /// Stop the writer (sent on [`DiskCache`] drop, after which the thread is
    /// joined). The writer processes everything queued ahead of it first.
    Shutdown,
}

/// In-memory LRU index over the on-disk cache. Tracks, per cache-file name, its
/// on-disk byte footprint and a recency tick; eviction pops the lowest tick.
///
/// The name is `blake3(key‖offset‖len)` hex — derivable from the lookup key — so
/// the index keys *are* the file names and a lookup never touches disk. The index
/// starts empty (cold) on open; restart-warmth (rebuilding it from cache-file
/// headers) is a deliberate follow-up, not v1.
#[derive(Default)]
struct Lru {
    tick: u64,
    total_bytes: u64,
    /// name → (on-disk size, recency tick).
    entries: HashMap<String, (u64, u64)>,
    /// recency tick → name (ordered, so the front is the coldest entry).
    order: BTreeMap<u64, String>,
}

impl Lru {
    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    /// Bump `name` to most-recently-used. Returns its size if present.
    fn touch(&mut self, name: &str) -> Option<u64> {
        let (size, old_tick) = *self.entries.get(name)?;
        self.order.remove(&old_tick);
        let t = self.next_tick();
        self.order.insert(t, name.to_string());
        self.entries.insert(name.to_string(), (size, t));
        Some(size)
    }

    /// Insert (or replace) `name` with `size`, accounting the byte delta.
    fn insert(&mut self, name: &str, size: u64) {
        if let Some((old_size, old_tick)) = self.entries.remove(name) {
            self.order.remove(&old_tick);
            self.total_bytes -= old_size;
        }
        let t = self.next_tick();
        self.order.insert(t, name.to_string());
        self.entries.insert(name.to_string(), (size, t));
        self.total_bytes += size;
    }

    /// Remove `name` if present, returning its size.
    fn remove(&mut self, name: &str) -> Option<u64> {
        let (size, tick) = self.entries.remove(name)?;
        self.order.remove(&tick);
        self.total_bytes -= size;
        Some(size)
    }

    /// Pop the coldest entry (lowest tick). Returns its name.
    fn pop_coldest(&mut self) -> Option<String> {
        let (&tick, _) = self.order.iter().next()?;
        let name = self.order.remove(&tick).unwrap();
        if let Some((size, _)) = self.entries.remove(&name) {
            self.total_bytes -= size;
        }
        Some(name)
    }
}

/// A local-disk read-through cache of sealed object bytes, shared (`Arc`) by the
/// [`CachingObjectStore`] and every [`CachingRandomReadAt`] it opens.
pub struct DiskCache {
    dir: PathBuf,
    index: Arc<Mutex<Lru>>,
    writer_tx: SyncSender<Req>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

impl DiskCache {
    /// Open (creating `dir` if needed) a disk cache bounded to `budget_bytes` and
    /// spawn its background writer thread.
    ///
    /// `dir` must be a **real writable volume — never tmpfs**, which is RAM and
    /// would defeat the bounded-RSS guarantee. The in-memory index starts cold;
    /// any files already present in `dir` (e.g. from a previous run) are ignored
    /// until overwritten, and aged out by the LRU as new writes land.
    pub fn open(dir: impl AsRef<Path>, budget_bytes: u64) -> Result<Arc<Self>> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .with_context(|| format!("create disk-cache dir {}", dir.display()))?;
        let index = Arc::new(Mutex::new(Lru::default()));
        let (writer_tx, writer_rx) = sync_channel::<Req>(WRITE_QUEUE_DEPTH);
        let writer = {
            let dir = dir.clone();
            let index = index.clone();
            std::thread::Builder::new()
                .name("slater-diskcache-writer".into())
                .spawn(move || writer_loop(dir, budget_bytes, index, writer_rx))
                .context("spawn disk-cache writer thread")?
        };
        Ok(Arc::new(Self {
            dir,
            index,
            writer_tx,
            writer: Mutex::new(Some(writer)),
        }))
    }

    /// Look up `(key, offset, len)`. On a hit, bumps recency, reads + verifies the
    /// cache file off-lock, and returns the sealed bytes; on a CRC/parse failure,
    /// evicts the entry and returns `None` (→ caller refetches from S3). A miss
    /// (not in the index) returns `None` without touching disk.
    pub fn get(&self, key: &str, offset: u64, len: u64) -> Option<Vec<u8>> {
        let name = cache_name(key, offset, len);
        // Touch under the lock; read the file off-lock.
        {
            let mut idx = self.index.lock().unwrap();
            idx.touch(&name)?;
        }
        let path = self.file_path(&name);
        match fs::read(&path)
            .ok()
            .and_then(|raw| decode(&raw, key, offset, len))
        {
            Some(payload) => Some(payload),
            None => {
                // Corrupt, truncated, or vanished file (e.g. evicted between the
                // touch and the read). Drop the index entry and ask the writer to
                // delete the file; report a miss so the caller refetches.
                self.index.lock().unwrap().remove(&name);
                let _ = self.writer_tx.try_send(Req::Delete { name });
                None
            }
        }
    }

    /// Enqueue `bytes` to be written into the cache for `(key, offset)`. Never
    /// blocks: if the write-behind channel is full the write is dropped (the block
    /// re-fetches on its next miss). A key too long for the file header is also
    /// silently skipped (object keys are short generation paths in practice).
    pub fn put_async(&self, key: &str, offset: u64, bytes: &[u8]) {
        if key.len() > u16::MAX as usize {
            return;
        }
        let name = cache_name(key, offset, bytes.len() as u64);
        let _ = self.writer_tx.try_send(Req::Write {
            name,
            key: key.to_string(),
            offset,
            payload: bytes.to_vec(),
        });
    }

    /// Drain the write-behind queue up to this call and wait for the writer to
    /// reach it — so a test can assert a prior `put_async` has landed on disk.
    pub fn flush(&self) {
        let (tx, rx) = sync_channel(1);
        if self.writer_tx.send(Req::Flush(tx)).is_ok() {
            let _ = rx.recv();
        }
    }

    fn file_path(&self, name: &str) -> PathBuf {
        file_path_in(&self.dir, name)
    }
}

impl Drop for DiskCache {
    fn drop(&mut self) {
        // Ask the writer to finish what's queued and stop, then join it so no
        // disk I/O outlives the cache.
        let _ = self.writer_tx.send(Req::Shutdown);
        if let Some(h) = self.writer.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

/// Two-hex-char shard dir + flat file name (`<dir>/ab/abcd…`), keeping any single
/// directory small without a deep tree.
fn file_path_in(dir: &Path, name: &str) -> PathBuf {
    dir.join(&name[..2]).join(name)
}

/// Cache-file name: `blake3(key‖offset_le‖len_le)` hex. Stable per block (block
/// reads are always at a fixed `(offset, comp_len)`), and the key embeds the
/// generation UUID so a generation swap orphans old entries (they age out).
fn cache_name(key: &str, offset: u64, len: u64) -> String {
    let mut h = blake3::Hasher::new();
    h.update(key.as_bytes());
    h.update(&offset.to_le_bytes());
    h.update(&len.to_le_bytes());
    h.finalize().to_hex().to_string()
}

/// Encode a cache file: a self-describing header (so a future restart can rebuild
/// the index by scanning headers) + a CRC of the payload + the sealed bytes.
///
/// ```text
/// magic(4) ‖ version(1) ‖ key_len(u16) ‖ key ‖ offset(u64) ‖ len(u32)
///          ‖ crc32(u32 of payload) ‖ payload
/// ```
fn encode(key: &str, offset: u64, payload: &[u8]) -> Vec<u8> {
    let crc = crc32fast::hash(payload);
    let mut v = Vec::with_capacity(23 + key.len() + payload.len());
    v.extend_from_slice(MAGIC);
    v.push(VERSION);
    v.write_u16::<LittleEndian>(key.len() as u16).unwrap();
    v.extend_from_slice(key.as_bytes());
    v.write_u64::<LittleEndian>(offset).unwrap();
    v.write_u32::<LittleEndian>(payload.len() as u32).unwrap();
    v.write_u32::<LittleEndian>(crc).unwrap();
    v.extend_from_slice(payload);
    v
}

/// Parse + verify a cache file against the lookup it should answer. Returns the
/// payload only if the header is well-formed, the recorded `(key, offset, len)`
/// match the request (guards a blake3 name collision), and the CRC checks out.
/// Any deviation returns `None` → the caller treats it as a miss.
fn decode(raw: &[u8], want_key: &str, want_offset: u64, want_len: u64) -> Option<Vec<u8>> {
    let mut r = raw;
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic).ok()?;
    if &magic != MAGIC || r.read_u8().ok()? != VERSION {
        return None;
    }
    let key_len = r.read_u16::<LittleEndian>().ok()? as usize;
    if r.len() < key_len {
        return None;
    }
    let (key_bytes, rest) = r.split_at(key_len);
    r = rest;
    if key_bytes != want_key.as_bytes() {
        return None;
    }
    let offset = r.read_u64::<LittleEndian>().ok()?;
    let len = r.read_u32::<LittleEndian>().ok()? as u64;
    let crc = r.read_u32::<LittleEndian>().ok()?;
    if offset != want_offset || len != want_len || r.len() as u64 != len {
        return None;
    }
    if crc32fast::hash(r) != crc {
        return None;
    }
    Some(r.to_vec())
}

/// The background writer: the sole owner of disk mutation. Drains the channel,
/// writing each block (temp file → fsync → atomic rename → index insert) and then
/// trimming the LRU tail back under budget. Also services self-heal deletes and
/// flush acks. Exits on `Shutdown` or a closed channel.
fn writer_loop(dir: PathBuf, budget_bytes: u64, index: Arc<Mutex<Lru>>, rx: Receiver<Req>) {
    let mut tmp_seq: u64 = 0;
    while let Ok(req) = rx.recv() {
        match req {
            Req::Write {
                name,
                key,
                offset,
                payload,
            } => {
                let encoded = encode(&key, offset, &payload);
                tmp_seq += 1;
                match write_file(&dir, &name, &encoded, tmp_seq) {
                    Ok(()) => {
                        let mut idx = index.lock().unwrap();
                        idx.insert(&name, encoded.len() as u64);
                        // Trim the coldest entries until we're back under budget.
                        while idx.total_bytes > budget_bytes {
                            let Some(victim) = idx.pop_coldest() else {
                                break;
                            };
                            // Delete off the index lock would be cleaner, but the
                            // writer is single-threaded so holding it briefly over
                            // an unlink is fine and keeps eviction atomic with the
                            // accounting.
                            let _ = fs::remove_file(file_path_in(&dir, &victim));
                        }
                    }
                    Err(_) => {
                        // A failed write just means a future miss refetches; the
                        // index was never told about it, so nothing to undo.
                    }
                }
            }
            Req::Delete { name } => {
                let _ = fs::remove_file(file_path_in(&dir, &name));
            }
            Req::Flush(ack) => {
                let _ = ack.send(());
            }
            Req::Shutdown => break,
        }
    }
}

/// Write `bytes` to the cache file for `name` atomically: a uniquely-named temp
/// file in the same shard dir, fsync'd, then renamed into place (so a torn write
/// never leaves a half-file under the real name; concurrent identical writes are
/// idempotent under the rename).
fn write_file(dir: &Path, name: &str, bytes: &[u8], seq: u64) -> Result<()> {
    let shard = dir.join(&name[..2]);
    fs::create_dir_all(&shard)?;
    let final_path = shard.join(name);
    let tmp_path = shard.join(format!("{name}.tmp{seq}"));
    {
        let mut f = fs::File::create(&tmp_path)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// An [`ObjectStore`] decorator that serves positional reads through a
/// [`DiskCache`] in front of `inner` (the S3 store). All cold-path operations
/// (`read_all`, `list`, `exists`, `put`, `verify_file`) delegate straight to
/// `inner` — only the hot positional read path is cached.
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<DiskCache>,
}

impl CachingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<DiskCache>) -> Self {
        Self { inner, cache }
    }
}

impl ObjectStore for CachingObjectStore {
    fn open(&self, key: &str) -> Result<Arc<dyn RandomReadAt>> {
        let inner = self.inner.open(key)?;
        Ok(Arc::new(CachingRandomReadAt {
            key: key.to_string(),
            inner,
            cache: self.cache.clone(),
        }))
    }

    fn read_all(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.read_all(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key)
    }

    fn verify_file(&self, key: &str, expected: &super::FileIntegrity) -> Result<()> {
        self.inner.verify_file(key, expected)
    }

    fn put(&self, key: &str, bytes: &[u8], sha256_b64: Option<&str>) -> Result<()> {
        self.inner.put(key, bytes, sha256_b64)
    }

    fn delete(&self, key: &str) -> Result<()> {
        // Delete from the backing store. Any blocks of `key` still resident in the disk cache
        // are harmless — a GC'd object is unreferenced, so no reader requests them again, and
        // they age out of the LRU on their own.
        self.inner.delete(key)
    }
}

/// One open object, reading through the [`DiskCache`]. Holds the object key (so
/// cache entries are keyed per object) and the inner S3 handle.
struct CachingRandomReadAt {
    key: String,
    inner: Arc<dyn RandomReadAt>,
    cache: Arc<DiskCache>,
}

impl RandomReadAt for CachingRandomReadAt {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let len = buf.len() as u64;
        if let Some(bytes) = self.cache.get(&self.key, offset, len) {
            buf.copy_from_slice(&bytes);
            return Ok(());
        }
        // Miss: fetch from S3, serve the caller, then write-behind to disk.
        self.inner.read_exact_at(buf, offset)?;
        self.cache.put_async(&self.key, offset, buf);
        Ok(())
    }

    fn len(&self) -> u64 {
        self.inner.len()
    }

    fn read_ranges(&self, ranges: &[(u64, u64)]) -> Result<Vec<Vec<u8>>> {
        // Per-range cache lookup; the misses go to the inner store in one
        // (concurrent) batch, are written behind, and everything is reassembled
        // in request order.
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(ranges.len());
        let mut miss_idx = Vec::new();
        let mut miss_ranges = Vec::new();
        for (i, &(offset, len)) in ranges.iter().enumerate() {
            match self.cache.get(&self.key, offset, len) {
                Some(bytes) => out.push(Some(bytes)),
                None => {
                    out.push(None);
                    miss_idx.push(i);
                    miss_ranges.push((offset, len));
                }
            }
        }
        if !miss_ranges.is_empty() {
            let fetched = self.inner.read_ranges(&miss_ranges)?;
            for (j, bytes) in fetched.into_iter().enumerate() {
                let i = miss_idx[j];
                let (offset, _) = ranges[i];
                self.cache.put_async(&self.key, offset, &bytes);
                out[i] = Some(bytes);
            }
        }
        Ok(out.into_iter().map(|o| o.unwrap()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::mem::MemObjectStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Build a cache in a fresh temp dir; returns (cache, dir-guard).
    fn temp_cache(budget: u64) -> (Arc<DiskCache>, tempdir::Guard) {
        let dir = tempdir::Guard::new();
        let cache = DiskCache::open(dir.path(), budget).unwrap();
        (cache, dir)
    }

    #[test]
    fn put_then_get_round_trips() {
        let (cache, _g) = temp_cache(1 << 20);
        let key = "g/u/topology.csr.blk";
        let bytes = (0..4096u32).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        assert!(cache.get(key, 0, bytes.len() as u64).is_none(), "cold miss");
        cache.put_async(key, 0, &bytes);
        cache.flush();
        assert_eq!(
            cache.get(key, 0, bytes.len() as u64).as_deref(),
            Some(&bytes[..])
        );
        // A different (offset, len) for the same object is a distinct, absent key.
        assert!(cache.get(key, 4096, 16).is_none());
    }

    #[test]
    fn miss_on_unknown_key() {
        let (cache, _g) = temp_cache(1 << 20);
        assert!(cache.get("nope", 0, 10).is_none());
    }

    #[test]
    fn budget_evicts_coldest() {
        // Budget holds ~2 blocks; writing a third evicts the least-recently-used.
        let block = 4096usize;
        let entry = block + 23 + "g/u/f.blk".len(); // payload + header footprint
        let (cache, _g) = temp_cache((entry * 2) as u64 + 8);
        let key = "g/u/f.blk";
        let mk = |b: u8| vec![b; block];
        for (i, b) in [10u8, 20, 30].iter().enumerate() {
            cache.put_async(key, (i * block) as u64, &mk(*b));
            cache.flush();
        }
        // Offset 0 (oldest) was evicted; the two newest survive.
        assert!(cache.get(key, 0, block as u64).is_none(), "coldest evicted");
        assert_eq!(cache.get(key, block as u64, block as u64), Some(mk(20)));
        assert_eq!(
            cache.get(key, (2 * block) as u64, block as u64),
            Some(mk(30))
        );
    }

    #[test]
    fn recency_protects_hot_block() {
        let block = 4096usize;
        let entry = block + 23 + "g/u/f.blk".len();
        let (cache, _g) = temp_cache((entry * 2) as u64 + 8);
        let key = "g/u/f.blk";
        let mk = |b: u8| vec![b; block];
        cache.put_async(key, 0, &mk(10));
        cache.flush();
        cache.put_async(key, block as u64, &mk(20));
        cache.flush();
        // Touch offset 0 so it becomes most-recently-used, then insert a third.
        assert_eq!(cache.get(key, 0, block as u64), Some(mk(10)));
        cache.put_async(key, (2 * block) as u64, &mk(30));
        cache.flush();
        // Offset 0 survived (it was hot); offset `block` (now coldest) was evicted.
        assert_eq!(cache.get(key, 0, block as u64), Some(mk(10)));
        assert!(cache.get(key, block as u64, block as u64).is_none());
    }

    #[test]
    fn corrupt_file_self_heals_to_miss() {
        let (cache, dir) = temp_cache(1 << 20);
        let key = "g/u/f.blk";
        let bytes = vec![7u8; 256];
        cache.put_async(key, 0, &bytes);
        cache.flush();
        // Flip a payload byte on disk under the cache's feet.
        let name = cache_name(key, 0, bytes.len() as u64);
        let path = file_path_in(dir.path(), &name);
        let mut raw = std::fs::read(&path).unwrap();
        *raw.last_mut().unwrap() ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();
        // The CRC mismatch is detected: a miss, and the entry is evicted+deleted.
        assert!(
            cache.get(key, 0, bytes.len() as u64).is_none(),
            "corruption → miss"
        );
        cache.flush(); // let the self-heal Delete drain
        assert!(!path.exists(), "corrupt file deleted");
        assert!(
            cache.get(key, 0, bytes.len() as u64).is_none(),
            "still a miss"
        );
    }

    /// An [`ObjectStore`] that counts positional reads reaching the inner store,
    /// so a test can prove the disk tier absorbs the second read.
    struct CountingStore {
        inner: MemObjectStore,
        reads: Arc<AtomicUsize>,
    }
    struct CountingObj {
        inner: Arc<dyn RandomReadAt>,
        reads: Arc<AtomicUsize>,
    }
    impl RandomReadAt for CountingObj {
        fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.read_exact_at(buf, offset)
        }
        fn len(&self) -> u64 {
            self.inner.len()
        }
    }
    impl ObjectStore for CountingStore {
        fn open(&self, key: &str) -> Result<Arc<dyn RandomReadAt>> {
            Ok(Arc::new(CountingObj {
                inner: self.inner.open(key)?,
                reads: self.reads.clone(),
            }))
        }
        fn read_all(&self, key: &str) -> Result<Vec<u8>> {
            self.inner.read_all(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix)
        }
        fn exists(&self, key: &str) -> Result<bool> {
            self.inner.exists(key)
        }
    }

    #[test]
    fn second_read_served_from_disk_not_inner() {
        let dir = tempdir::Guard::new();
        let cache = DiskCache::open(dir.path(), 1 << 20).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let mem = MemObjectStore::new();
        let key = "g/u/topology.csr.blk";
        let data: Vec<u8> = (0..2000u32).map(|i| (i % 251) as u8).collect();
        mem.put(key, &data, None).unwrap();
        let store = CachingObjectStore::new(
            Arc::new(CountingStore {
                inner: mem,
                reads: reads.clone(),
            }),
            cache.clone(),
        );

        let obj = store.open(key).unwrap();
        let mut buf = vec![0u8; 500];
        obj.read_exact_at(&mut buf, 100).unwrap();
        assert_eq!(buf, data[100..600]);
        assert_eq!(reads.load(Ordering::SeqCst), 1, "first read hits S3");
        cache.flush();

        // A second open + identical read is served from disk — the inner store is
        // not touched again.
        let obj2 = store.open(key).unwrap();
        let mut buf2 = vec![0u8; 500];
        obj2.read_exact_at(&mut buf2, 100).unwrap();
        assert_eq!(buf2, data[100..600]);
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "second read served from disk"
        );
    }

    #[test]
    fn read_ranges_mixes_hits_and_misses() {
        let dir = tempdir::Guard::new();
        let cache = DiskCache::open(dir.path(), 1 << 20).unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let mem = MemObjectStore::new();
        let key = "g/u/f.blk";
        let data: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
        mem.put(key, &data, None).unwrap();
        let store = CachingObjectStore::new(
            Arc::new(CountingStore {
                inner: mem,
                reads: reads.clone(),
            }),
            cache.clone(),
        );
        let obj = store.open(key).unwrap();

        // Warm one range into the disk tier.
        let warm = obj.read_ranges(&[(0, 100)]).unwrap();
        assert_eq!(warm[0], data[0..100]);
        cache.flush();
        let baseline = reads.load(Ordering::SeqCst);
        assert_eq!(baseline, 1);

        // Now a batch where the first range is a hit and the rest are misses:
        // only the two misses reach the inner store, results stay in order.
        let got = obj
            .read_ranges(&[(0, 100), (500, 50), (2000, 200)])
            .unwrap();
        assert_eq!(got[0], data[0..100]);
        assert_eq!(got[1], data[500..550]);
        assert_eq!(got[2], data[2000..2200]);
        assert_eq!(
            reads.load(Ordering::SeqCst) - baseline,
            2,
            "only the two misses hit the inner store"
        );
    }

    /// Minimal self-cleaning temp directory (no external dev-dep): a unique dir
    /// under the system temp root, removed on drop. The unique suffix is derived
    /// from a process-wide counter + the thread id (no wall clock / RNG, which the
    /// surrounding crate avoids), so parallel tests don't collide.
    mod tempdir {
        use std::path::{Path, PathBuf};
        use std::sync::atomic::{AtomicU64, Ordering};

        static SEQ: AtomicU64 = AtomicU64::new(0);

        pub struct Guard(PathBuf);
        impl Guard {
            pub fn new() -> Self {
                let seq = SEQ.fetch_add(1, Ordering::SeqCst);
                let tid = format!("{:?}", std::thread::current().id());
                let tid: String = tid.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
                let dir = std::env::temp_dir().join(format!("slater-diskcache-{tid}-{seq}"));
                std::fs::create_dir_all(&dir).unwrap();
                Guard(dir)
            }
            pub fn path(&self) -> &Path {
                &self.0
            }
        }
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }
}
