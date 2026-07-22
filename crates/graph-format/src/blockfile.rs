// SPDX-License-Identifier: Apache-2.0
//! Generic block-container file used by every `.blk` file in a generation.
//!
//! A block file packs many small, length-delimited **records** into fixed-size
//! raw blocks (default 256 KiB), compresses each block independently, and writes
//! a small **block directory** (block id → file offset + lengths) as a trailer.
//! The directory is tiny and is held resident by the reader at open; block bytes
//! are fetched with positional reads (`pread`) and decompressed on demand — we do
//! not mmap (over a remote/network filesystem such as NFS, mmap gives
//! close-to-open surprises and unpredictable eviction; we want explicit, bounded
//! caching instead).
//!
//! On-disk layout:
//! ```text
//! MAGIC(8) ‖ block_0 ‖ … ‖ block_{n-1} ‖ directory ‖ footer(24)
//! footer    = dir_offset:u64 ‖ dir_len:u64 ‖ block_count:u64   (little-endian)
//! directory = block_count × ( offset:u64 ‖ comp_len:u32 ‖ raw_len:u32 ‖ rec_count:u32 [‖ nonce:24] )
//! block raw = count:u32 ‖ (count+1)×offset:u32 ‖ record_bytes…   (then zstd-compressed)
//! ```
//! A record at `slot` is `data[off[slot]..off[slot+1]]`; the trailing offset is
//! the data length, so record boundaries need no separate length prefixes.
//!
//! When a block file is **encrypted** the magic is `SLBLKE01`, each directory
//! entry carries the block's 24-byte XChaCha20-Poly1305 nonce, and the on-disk
//! block bytes are the compressed block *sealed* with the AEAD (so `comp_len`
//! counts ciphertext = compressed + 16-byte tag). The per-generation key never
//! touches the file — only the nonces do (D28). Reading is decrypt-then-
//! decompress on a cache miss; the cache therefore holds plaintext bytes.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use anyhow::{anyhow, bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::codec;
use crate::crypto::{BlockCipher, NONCE_LEN};
use crate::ids::BlockId;
use crate::store::fs::FileObject;
use crate::store::RandomReadAt;
use crate::wire::{capacity_for, DecodeRejected};

const BLOCKFILE_MAGIC: &[u8; 8] = b"SLBLK001";
/// Magic for an AEAD-encrypted block file. The directory entries are wider (they
/// carry a per-block nonce) so the two formats are never confused.
const BLOCKFILE_MAGIC_ENC: &[u8; 8] = b"SLBLKE01";
/// Magic for a **raw** (uncompressed) block file — blocks are stored verbatim, no
/// zstd. `comp_len == raw_len`, so a read is a bare `pread` + slice with no
/// decompress. Used where the record payload is already the queryable, near-entropy
/// form (the Elias–Fano degree column), so a codec pass would be a ~1.0× tax paid on
/// every fault. Directory layout is identical to the zstd format.
const BLOCKFILE_MAGIC_RAW: &[u8; 8] = b"SLBLKR01";
/// Magic for a raw + AEAD-encrypted block file (raw ⟂ encryption): blocks are the
/// verbatim raw bytes sealed with the per-block nonce, so `comp_len == raw_len + 16`.
const BLOCKFILE_MAGIC_RAW_ENC: &[u8; 8] = b"SLBLKX01";
const FOOTER_LEN: u64 = 24; // dir_offset(8) + dir_len(8) + block_count(8)
const DIR_ENTRY_LEN: usize = 20; // offset(8) + comp_len(4) + raw_len(4) + rec_count(4)
const DIR_ENTRY_LEN_ENC: usize = DIR_ENTRY_LEN + NONCE_LEN; // + per-block nonce

/// How a block file stores its block bodies. Orthogonal to encryption (either codec
/// may be sealed with a per-block nonce). Recorded in the file magic, so the reader
/// picks the decode path without a per-block flag.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum BlockCodec {
    /// zstd-compress each block independently (the default for every `.blk` file).
    #[default]
    Zstd,
    /// Store each block verbatim — no compression. `comp_len == raw_len`.
    Raw,
}

/// Pick the 8-byte magic for a `(codec, encrypted)` pair.
fn magic_for(codec: BlockCodec, encrypted: bool) -> &'static [u8; 8] {
    match (codec, encrypted) {
        (BlockCodec::Zstd, false) => BLOCKFILE_MAGIC,
        (BlockCodec::Zstd, true) => BLOCKFILE_MAGIC_ENC,
        (BlockCodec::Raw, false) => BLOCKFILE_MAGIC_RAW,
        (BlockCodec::Raw, true) => BLOCKFILE_MAGIC_RAW_ENC,
    }
}

/// Decode an 8-byte magic into `(codec, encrypted)`, or `None` if unrecognised.
fn magic_kind(magic: &[u8; 8]) -> Option<(BlockCodec, bool)> {
    match magic {
        m if m == BLOCKFILE_MAGIC => Some((BlockCodec::Zstd, false)),
        m if m == BLOCKFILE_MAGIC_ENC => Some((BlockCodec::Zstd, true)),
        m if m == BLOCKFILE_MAGIC_RAW => Some((BlockCodec::Raw, false)),
        m if m == BLOCKFILE_MAGIC_RAW_ENC => Some((BlockCodec::Raw, true)),
        _ => None,
    }
}

/// Location of a record: which block, and which slot within that block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordLoc {
    pub block: BlockId,
    pub slot: u32,
}

#[derive(Clone, Copy)]
struct DirEntry {
    offset: u64,
    comp_len: u32,
    raw_len: u32,
    rec_count: u32,
    /// Per-block AEAD nonce, present iff the file is encrypted.
    nonce: Option<[u8; NONCE_LEN]>,
}

// ── parallel block sealing ───────────────────────────────────────────────────
//
// Sealing a block — zstd-compress it, then AEAD-encrypt it when the generation is
// encrypted — is pure CPU, and it ran inline on whichever thread appended the
// record that filled the block. That made a single-threaded producer the ceiling
// for several build phases that are otherwise cheap: `cluster`'s stripe routing,
// `dedup`'s drain, `emit.node_stores`' drain. All three read a sorted stream on one
// thread and write it back out, so all three were really measuring one core's zstd
// throughput (measured: 84–116% CPU across the whole phase).
//
// So the seal moves to a shared, bounded pool. The appending thread hands off a full
// raw block and keeps filling the next; workers seal blocks concurrently; the
// appending thread then drains *in block order* and writes whatever contiguous
// prefix has completed. Block boundaries, block contents and directory order are all
// unchanged, and zstd is deterministic for a given (input, level) — so the emitted
// file is byte-identical to the serial path. (An encrypted file was never
// byte-reproducible anyway: each block takes a fresh random nonce.)
//
// Memory is bounded by a *global* in-flight cap rather than a per-writer one,
// because a single thread can hold many writers open at once — `cluster` routes into
// 1,398 stripe files — and a per-writer allowance would multiply by that.

/// One queued seal: compress (+encrypt) a raw block, executed on a pool worker.
type SealJob = Box<dyn FnOnce() + Send + 'static>;

static SEAL_POOL: OnceLock<Sender<SealJob>> = OnceLock::new();

/// Blocks submitted-but-not-yet-written across every live writer. Caps the raw +
/// sealed bytes the seal pipeline can hold: `cap × target_block_bytes × 2`, so ~16 MB
/// at the default 256 KiB block and 32 permits.
static INFLIGHT: OnceLock<Inflight> = OnceLock::new();

/// Caller-configured seal-worker cap (`0` = unset), set once from `--threads`.
static CONFIGURED_SEAL_THREADS: AtomicUsize = AtomicUsize::new(0);

/// Set the block-seal worker cap. Must be called before the first `BlockFileWriter`;
/// later calls are ignored once the pool has started. `SLATER_BLOCKFILE_SEAL_THREADS`
/// overrides it; `1` restores the original inline-seal behaviour.
pub fn configure_seal_threads(n: usize) {
    CONFIGURED_SEAL_THREADS.store(n.max(1), AtomicOrdering::Relaxed);
}

fn seal_threads() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("SLATER_BLOCKFILE_SEAL_THREADS")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or_else(
                || match CONFIGURED_SEAL_THREADS.load(AtomicOrdering::Relaxed) {
                    0 => std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4),
                    n => n,
                },
            )
            .clamp(1, 64)
    })
}

/// A counted semaphore over in-flight blocks.
struct Inflight {
    held: Mutex<usize>,
    cv: Condvar,
    cap: usize,
}

impl Inflight {
    fn acquire(&self) {
        let mut h = self.held.lock().unwrap();
        while *h >= self.cap {
            h = self.cv.wait(h).unwrap();
        }
        *h += 1;
    }
    fn release(&self) {
        *self.held.lock().unwrap() -= 1;
        self.cv.notify_one();
    }
}

fn inflight() -> &'static Inflight {
    INFLIGHT.get_or_init(|| Inflight {
        held: Mutex::new(0),
        cv: Condvar::new(),
        cap: (seal_threads() * 2).max(2),
    })
}

fn seal_pool() -> &'static Sender<SealJob> {
    SEAL_POOL.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<SealJob>();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..seal_threads() {
            let rx = Arc::clone(&rx);
            std::thread::Builder::new()
                .name("slater-blockfile-seal".into())
                .spawn(move || loop {
                    // Hold the lock only to dequeue; run the job unlocked.
                    let job = { rx.lock().unwrap().recv() };
                    match job {
                        Ok(job) => job(),
                        Err(_) => break, // sender dropped (never, in practice)
                    }
                })
                .expect("spawn blockfile seal worker");
        }
        tx
    })
}

/// A block that has been sealed but not yet written, awaiting its turn in block order.
struct Sealed {
    stored: Vec<u8>,
    raw_len: u32,
    rec_count: u32,
    nonce: Option<[u8; NONCE_LEN]>,
}

/// One writer's in-flight seals. Workers insert by block index; the appending thread
/// pops the contiguous prefix and writes it.
struct SealState {
    done: Mutex<BTreeMap<usize, Sealed>>,
    cv: Condvar,
    pending: Mutex<usize>,
    pending_cv: Condvar,
    err: Mutex<Option<anyhow::Error>>,
}

impl SealState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            done: Mutex::new(BTreeMap::new()),
            cv: Condvar::new(),
            pending: Mutex::new(0),
            pending_cv: Condvar::new(),
            err: Mutex::new(None),
        })
    }
    fn drain(&self) {
        let mut p = self.pending.lock().unwrap();
        while *p > 0 {
            p = self.pending_cv.wait(p).unwrap();
        }
    }
    fn take_err(&self) -> Option<anyhow::Error> {
        self.err.lock().unwrap().take()
    }
}

/// Compress and (optionally) seal one raw block. The unit of pool work. Under
/// [`BlockCodec::Raw`] the compress step is skipped and the block is stored verbatim
/// (still sealed when a cipher is present).
fn seal_block(
    raw: &[u8],
    codec: BlockCodec,
    level: i32,
    cipher: Option<&BlockCipher>,
) -> Result<(Vec<u8>, Option<[u8; NONCE_LEN]>)> {
    let comp = match codec {
        BlockCodec::Zstd => codec::compress(raw, level)?,
        BlockCodec::Raw => raw.to_vec(),
    };
    // On-disk bytes are the (compressed-or-raw) block, sealed with the AEAD when a
    // cipher is configured. `comp_len` then counts ciphertext (+16 tag).
    match cipher {
        Some(cipher) => {
            let nonce = BlockCipher::random_nonce();
            let sealed = cipher.encrypt(&nonce, &comp)?;
            Ok((sealed, Some(nonce)))
        }
        None => Ok((comp, None)),
    }
}

/// Streaming writer that packs records into compressed blocks.
pub struct BlockFileWriter {
    file: BufWriter<File>,
    target: usize,
    codec: BlockCodec,
    level: i32,
    offset: u64,
    dir: Vec<DirEntry>,
    cur_offsets: Vec<u32>,
    cur_data: Vec<u8>,
    /// When set, each compressed block is sealed with this cipher under a fresh
    /// per-block nonce before it is written.
    cipher: Option<Arc<BlockCipher>>,
    /// Index of the next block to *submit* for sealing. Not `dir.len()`: blocks are
    /// sealed out of order, so `dir` lags behind by whatever is in flight.
    next_block: usize,
    /// Index of the next block to *write*. `dir.len()` always equals this.
    next_write: usize,
    /// In-flight seals, or `None` when sealing runs inline (a single seal worker).
    seal: Option<Arc<SealState>>,
}

impl BlockFileWriter {
    /// Create a new plaintext block file with the given target block size
    /// (bytes, raw) and zstd level.
    pub fn create(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
    ) -> Result<Self> {
        Self::create_inner(path, target_block_bytes, BlockCodec::Zstd, zstd_level, None)
    }

    /// Create a block file, optionally AEAD-encrypted. `cipher = None` writes the
    /// plaintext format (identical to [`BlockFileWriter::create`]) so existing
    /// fixtures and the golden test keep working unchanged.
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        Self::create_inner(
            path,
            target_block_bytes,
            BlockCodec::Zstd,
            zstd_level,
            cipher,
        )
    }

    /// Create a block file with an explicit block codec, optionally AEAD-encrypted.
    /// [`BlockCodec::Raw`] stores blocks verbatim (no zstd) — for payloads that are
    /// already the queryable, near-entropy form (the Elias–Fano degree column), where a
    /// codec pass would be a ~1.0× tax paid on every fault. `zstd_level` is ignored
    /// under `Raw`.
    pub fn create_with_codec(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        codec: BlockCodec,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        Self::create_inner(path, target_block_bytes, codec, zstd_level, cipher)
    }

    /// [`BlockFileWriter::create`] that seals its blocks **inline**, on the appending
    /// thread, instead of handing them to the shared seal pool.
    ///
    /// Use it for a writer driven from inside an already-saturated worker pool — pass 1's
    /// shard writers, or `ExtSorter`'s run files, which are written by the spill pool.
    /// There the seal pool can hand the writer no extra cores; all it adds is a channel
    /// hop, a `BTreeMap` insert and contention on the global in-flight semaphore, and it
    /// oversubscribes the machine with a second pool's worth of threads. Mirrors
    /// [`ExtSorter::new_for_pool`](crate::extsort::ExtSorter::new_for_pool).
    pub fn create_inline(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
    ) -> Result<Self> {
        let mut w =
            Self::create_inner(path, target_block_bytes, BlockCodec::Zstd, zstd_level, None)?;
        w.seal = None;
        Ok(w)
    }

    fn create_inner(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        codec: BlockCodec,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let f = File::create(path.as_ref())
            .with_context(|| format!("create block file {}", path.as_ref().display()))?;
        let mut file = BufWriter::new(f);
        let magic = magic_for(codec, cipher.is_some());
        file.write_all(magic)?;
        Ok(Self {
            file,
            target: target_block_bytes.max(1),
            codec,
            level: zstd_level,
            offset: magic.len() as u64,
            dir: Vec::new(),
            cur_offsets: vec![0],
            cur_data: Vec::new(),
            cipher,
            next_block: 0,
            next_write: 0,
            seal: (seal_threads() > 1).then(SealState::new),
        })
    }

    /// Append a record and return its location. Records are packed into the
    /// current block until it reaches the target size, then the block is flushed.
    pub fn append_record(&mut self, record: &[u8]) -> Result<RecordLoc> {
        let block = BlockId(self.next_block as u32);
        let slot = (self.cur_offsets.len() - 1) as u32;
        self.cur_data.extend_from_slice(record);
        self.cur_offsets.push(self.cur_data.len() as u32);
        if self.cur_data.len() >= self.target {
            self.flush_block()?;
        }
        Ok(RecordLoc { block, slot })
    }

    /// Serialise the current block's records into one raw buffer and reset the
    /// accumulators. Returns `None` when there is nothing pending.
    fn take_raw_block(&mut self) -> Result<Option<(Vec<u8>, u32)>> {
        if self.cur_data.is_empty() {
            return Ok(None);
        }
        let count = (self.cur_offsets.len() - 1) as u32;
        let mut raw = Vec::with_capacity(4 + self.cur_offsets.len() * 4 + self.cur_data.len());
        raw.write_u32::<LittleEndian>(count)?;
        for off in &self.cur_offsets {
            raw.write_u32::<LittleEndian>(*off)?;
        }
        raw.extend_from_slice(&self.cur_data);
        self.cur_offsets.clear();
        self.cur_offsets.push(0);
        self.cur_data.clear();
        Ok(Some((raw, count)))
    }

    /// Write one sealed block at the current offset and record its directory entry.
    fn write_sealed(&mut self, b: Sealed) -> Result<()> {
        self.file.write_all(&b.stored)?;
        self.dir.push(DirEntry {
            offset: self.offset,
            comp_len: b.stored.len() as u32,
            raw_len: b.raw_len,
            rec_count: b.rec_count,
            nonce: b.nonce,
        });
        self.offset += b.stored.len() as u64;
        self.next_write += 1;
        Ok(())
    }

    /// Write every sealed block that is now contiguous with `next_write`. Blocks that
    /// finished early sit in `done` until their predecessors land, so the file's block
    /// order — and therefore its bytes — never depends on the order workers finish in.
    fn drain_ready(&mut self) -> Result<()> {
        let state = match &self.seal {
            Some(s) => Arc::clone(s),
            None => return Ok(()),
        };
        loop {
            let next = {
                let mut done = state.done.lock().unwrap();
                match done.remove(&self.next_write) {
                    Some(b) => b,
                    None => break,
                }
            };
            self.write_sealed(next)?;
        }
        if let Some(e) = state.take_err() {
            return Err(e);
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        let Some((raw, count)) = self.take_raw_block()? else {
            return Ok(());
        };
        let idx = self.next_block;
        self.next_block += 1;

        let Some(state) = self.seal.as_ref().map(Arc::clone) else {
            // Inline: the original single-threaded behaviour.
            let (stored, nonce) = seal_block(&raw, self.codec, self.level, self.cipher.as_deref())?;
            return self.write_sealed(Sealed {
                stored,
                raw_len: raw.len() as u32,
                rec_count: count,
                nonce,
            });
        };

        // Bound the pipeline before submitting: at most `cap` blocks across every live
        // writer are un-written at once, so 1,398 concurrently-open stripe writers cost
        // the same as one.
        inflight().acquire();
        *state.pending.lock().unwrap() += 1;
        let codec = self.codec;
        let level = self.level;
        let cipher = self.cipher.clone();
        let st = Arc::clone(&state);
        let submitted = seal_pool().send(Box::new(move || {
            let raw_len = raw.len() as u32;
            match seal_block(&raw, codec, level, cipher.as_deref()) {
                Ok((stored, nonce)) => {
                    st.done.lock().unwrap().insert(
                        idx,
                        Sealed {
                            stored,
                            raw_len,
                            rec_count: count,
                            nonce,
                        },
                    );
                }
                Err(e) => {
                    let mut slot = st.err.lock().unwrap();
                    if slot.is_none() {
                        *slot = Some(e);
                    }
                }
            }
            inflight().release();
            let mut p = st.pending.lock().unwrap();
            *p -= 1;
            st.pending_cv.notify_all();
            st.cv.notify_all();
        }));
        if submitted.is_err() {
            inflight().release();
            *state.pending.lock().unwrap() -= 1;
            return Err(anyhow!("blockfile seal pool unavailable"));
        }
        self.drain_ready()
    }

    /// Flush the final block, write the directory and footer, and return the
    /// number of blocks written.
    pub fn finish(mut self) -> Result<u64> {
        self.flush_block()?;
        // Every block is submitted; wait for the stragglers and write the tail in order.
        if let Some(state) = self.seal.as_ref().map(Arc::clone) {
            state.drain();
            if let Some(e) = state.take_err() {
                return Err(e);
            }
            self.drain_ready()?;
            debug_assert!(state.done.lock().unwrap().is_empty());
        }
        let entry_len = if self.cipher.is_some() {
            DIR_ENTRY_LEN_ENC
        } else {
            DIR_ENTRY_LEN
        };
        let dir_offset = self.offset;
        let mut dir_bytes = Vec::with_capacity(self.dir.len() * entry_len);
        for e in &self.dir {
            dir_bytes.write_u64::<LittleEndian>(e.offset)?;
            dir_bytes.write_u32::<LittleEndian>(e.comp_len)?;
            dir_bytes.write_u32::<LittleEndian>(e.raw_len)?;
            dir_bytes.write_u32::<LittleEndian>(e.rec_count)?;
            if let Some(nonce) = &e.nonce {
                dir_bytes.write_all(nonce)?;
            }
        }
        self.file.write_all(&dir_bytes)?;

        let mut footer = Vec::with_capacity(FOOTER_LEN as usize);
        footer.write_u64::<LittleEndian>(dir_offset)?;
        footer.write_u64::<LittleEndian>(dir_bytes.len() as u64)?;
        footer.write_u64::<LittleEndian>(self.dir.len() as u64)?;
        self.file.write_all(&footer)?;

        self.file.flush()?;
        // fsync so a generation survives an unclean shutdown / network-FS flush.
        self.file.get_ref().sync_all().context("fsync block file")?;
        Ok(self.dir.len() as u64)
    }
}

/// Concatenate block files (in order) into one, preserving global record order: the
/// output's records are `inputs[0]`'s records, then `inputs[1]`'s, and so on. All
/// inputs must share the same magic (all plaintext, or all encrypted under the same
/// generation cipher). Block payloads are copied **verbatim** — no re-compression or
/// re-encryption — and only the block directory's offsets are rebuilt, so this is
/// O(total bytes) and trivially correct for both formats. Returns the total record
/// count. This is the "cursor assembly" step for range-partitioned parallel emit:
/// each worker writes a disjoint, contiguous slice of an output store, and this
/// stitches the slices without a serial re-encode.
///
/// The copy goes through [`std::io::copy`], which on Linux dispatches a file-to-file
/// copy to `copy_file_range(2)`: the bytes never enter user space, so this costs no
/// memcpy and no second set of page-cache pages.
///
/// It is **not** parallelised. The reason is economic, not physical — the earlier
/// claim that this step "is bounded by write bandwidth" was wrong, so do not lean on
/// it. At 91.6M nodes it copies 18.0 GB in 50.7 s: 356 MB/s read + 356 MB/s write, at
/// 0.31 of a core, with `psi_io` 48.7. That `psi_io` looks like a saturated disk but
/// is not: PSI counts the fraction of time a *runnable task is stalled on I/O*, and
/// with exactly one thread that degenerates to "the one thread is waiting", which a
/// queue-depth-1 copy loop always is. In the same build on the same device, `publish`
/// reads 25.1 GB at 2,099 MB/s (`psi_io` 3.5) because it hashes the inventory with
/// `par_iter` and keeps the queue full.
///
/// So there *is* headroom — measured with concurrent `copy_file_range` on this ext4
/// volume, 4-6 streams reach ~1.6 GB/s aggregate against ~1.1 GB/s for one, i.e. about
/// 1.5x. But 1.5x on 59 s of a 43.5-min build is ~20 s, ~1%, which is inside the
/// build's ±10% run-to-run noise and could never be verified. (An older attempt fared
/// worse still by copying the regions with *user-space* positional writes, memcpying
/// every byte through user space and raising CPU to 110% for no gain — that measured
/// the cost of abandoning `copy_file_range`, not the cost of parallelism.)
///
/// Revisit only if the concat stops being 1% of the build, or lands on a filesystem
/// with reflink (XFS/btrfs), where `copy_file_range` would make it O(1) outright.
pub fn concat_block_files(out_path: impl AsRef<Path>, inputs: &[PathBuf]) -> Result<u64> {
    if inputs.is_empty() {
        bail!("concat_block_files: no inputs");
    }
    let mut magic = [0u8; 8];
    File::open(&inputs[0])?.read_exact_at(&mut magic, 0)?;
    // Codec is irrelevant to concat (blocks are copied verbatim); only the encrypted
    // flag matters, since it sets the directory-entry width (nonce).
    let (_, encrypted) = magic_kind(&magic)
        .ok_or_else(|| anyhow!("concat_block_files: bad magic in {}", inputs[0].display()))?;
    let magic_len = magic.len() as u64;

    let mut out = File::create(out_path.as_ref())
        .with_context(|| format!("create {}", out_path.as_ref().display()))?;
    out.write_all(&magic)?;
    let mut out_pos = magic_len;
    let mut dir_bytes: Vec<u8> = Vec::new();
    let mut block_count: u64 = 0;
    let mut total_records: u64 = 0;

    for inp in inputs {
        let mut f = File::open(inp).with_context(|| format!("open {}", inp.display()))?;
        let mut m = [0u8; 8];
        f.read_exact_at(&mut m, 0)?;
        if m != magic {
            bail!(
                "concat_block_files: mixed magics ({} differs)",
                inp.display()
            );
        }
        let len = f.metadata()?.len();
        let mut footer = [0u8; FOOTER_LEN as usize];
        f.read_exact_at(&mut footer, len - FOOTER_LEN)?;
        let mut fr = &footer[..];
        let dir_offset = fr.read_u64::<LittleEndian>()?;
        let dir_len = fr.read_u64::<LittleEndian>()?;
        let blocks = fr.read_u64::<LittleEndian>()?;

        // Copy the block region [8, dir_offset) verbatim, tracking where it lands.
        let blocks_base_out = out_pos;
        let region = dir_offset - magic_len;
        f.seek(SeekFrom::Start(magic_len))?;
        let copied = std::io::copy(&mut (&f).take(region), &mut out)
            .with_context(|| format!("copy blocks of {}", inp.display()))?;
        if copied != region {
            bail!(
                "concat_block_files: short copy of {} ({copied} of {region})",
                inp.display()
            );
        }
        out_pos += region;

        // Rewrite each directory entry's offset into the output's coordinate space.
        let mut db = vec![0u8; dir_len as usize];
        f.read_exact_at(&mut db, dir_offset)?;
        let mut dr = &db[..];
        for _ in 0..blocks {
            let offset = dr.read_u64::<LittleEndian>()?;
            let comp_len = dr.read_u32::<LittleEndian>()?;
            let raw_len = dr.read_u32::<LittleEndian>()?;
            let rec_count = dr.read_u32::<LittleEndian>()?;
            dir_bytes.write_u64::<LittleEndian>(blocks_base_out + (offset - magic_len))?;
            dir_bytes.write_u32::<LittleEndian>(comp_len)?;
            dir_bytes.write_u32::<LittleEndian>(raw_len)?;
            dir_bytes.write_u32::<LittleEndian>(rec_count)?;
            if encrypted {
                let mut nonce = [0u8; NONCE_LEN];
                dr.read_exact(&mut nonce)?;
                dir_bytes.write_all(&nonce)?;
            }
            block_count += 1;
            total_records += rec_count as u64;
        }
    }

    let dir_offset_out = out_pos;
    out.write_all(&dir_bytes)?;
    let mut footer = Vec::with_capacity(FOOTER_LEN as usize);
    footer.write_u64::<LittleEndian>(dir_offset_out)?;
    footer.write_u64::<LittleEndian>(dir_bytes.len() as u64)?;
    footer.write_u64::<LittleEndian>(block_count)?;
    out.write_all(&footer)?;
    out.sync_all().context("fsync concat block file")?;
    Ok(total_records)
}

/// Reader holding the resident block directory; fetches blocks positionally
/// from the backing object (a local file's `pread`, or a remote range read).
pub struct BlockFileReader {
    src: Arc<dyn RandomReadAt>,
    dir: Vec<DirEntry>,
    /// Prefix sums of records-per-block: `block_start[i]` is the global index of
    /// block `i`'s first record. Length `dir.len() + 1`; the last entry is the
    /// total record count. Resident, `O(num_blocks)` — tiny.
    block_start: Vec<u64>,
    /// Per-generation cipher, set iff the file is encrypted. Refused at open if
    /// the file is encrypted and no key was supplied (absent-key refusal).
    cipher: Option<Arc<BlockCipher>>,
    /// Block-body codec, from the file magic. [`BlockCodec::Raw`] skips the zstd
    /// decompress on every block read (bare `pread` + slice).
    codec: BlockCodec,
}

impl BlockFileReader {
    /// Open a plaintext block file. Refuses an encrypted file (no key supplied).
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cipher(path, None)
    }

    /// Open a block file by path (local filesystem), validating the magic and
    /// loading the block directory. A convenience wrapper over [`open_src`] for
    /// callers (tests, tools) that hold a path; the serve path opens through an
    /// [`ObjectStore`](crate::store::ObjectStore) and calls [`open_src`].
    ///
    /// [`open_src`]: BlockFileReader::open_src
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let src = Arc::new(FileObject::open(path.as_ref())?);
        Self::open_src(src, cipher)
    }

    /// Open a block file from any positional-read source (local file or remote
    /// object), validating the magic and loading the block directory.
    /// An encrypted file requires `cipher = Some(..)`; an encrypted file opened
    /// without a key is refused with a clear error rather than returning garbage.
    pub fn open_src(src: Arc<dyn RandomReadAt>, cipher: Option<Arc<BlockCipher>>) -> Result<Self> {
        let len = src.len();
        if len < BLOCKFILE_MAGIC.len() as u64 + FOOTER_LEN {
            bail!("block file too short to be valid");
        }

        let mut magic = [0u8; 8];
        src.read_exact_at(&mut magic, 0)?;
        let (codec, encrypted) =
            magic_kind(&magic).ok_or_else(|| anyhow!("bad block file magic"))?;
        // Bind the cipher to what the file actually is: an encrypted file with no
        // key is refused; a plaintext file ignores any key it was handed.
        let cipher = if encrypted {
            match cipher {
                Some(c) => Some(c),
                None => bail!("block file is encrypted but no key was supplied"),
            }
        } else {
            None
        };

        let mut footer = [0u8; FOOTER_LEN as usize];
        src.read_exact_at(&mut footer, len - FOOTER_LEN)?;
        let mut fr = &footer[..];
        let dir_offset = fr.read_u64::<LittleEndian>()?;
        let dir_len = fr.read_u64::<LittleEndian>()?;
        let block_count = fr.read_u64::<LittleEndian>()?;

        // The footer is plaintext even for an encrypted file (the cipher covers the blocks and
        // the directory body, not the footer that locates them), so `dir_offset`/`dir_len`/
        // `block_count` are unauthenticated on-disk `u64`s on every configuration. Sizing
        // `vec![0u8; dir_len]` straight off one is an allocator abort on a forged length,
        // before the short read that would have caught it. The directory must lie inside the
        // file — check the claim first, then the block ceiling.
        if dir_offset.saturating_add(dir_len) > len {
            return Err(DecodeRejected::OutOfFile {
                what: "blockfile directory",
                offset: dir_offset,
                len: dir_len,
                file_len: len,
            }
            .into());
        }
        codec::check_stored_len(dir_len as usize)?;
        let mut dir_bytes = vec![0u8; dir_len as usize];
        src.read_exact_at(&mut dir_bytes, dir_offset)?;
        let mut dr = &dir_bytes[..];
        // A directory entry costs ≥20 bytes (u64 offset ‖ u32 comp_len ‖ u32 raw_len ‖ u32
        // rec_count; the nonce only adds to that), so reserve no more than the directory body
        // could hold — the loop below errors on the first short read.
        let cap = capacity_for(block_count as usize, dir_bytes.len(), 20);
        let mut dir = Vec::with_capacity(cap);
        let mut block_start = Vec::with_capacity(cap + 1);
        let mut acc = 0u64;
        for _ in 0..block_count {
            let offset = dr.read_u64::<LittleEndian>()?;
            let comp_len = dr.read_u32::<LittleEndian>()?;
            let raw_len = dr.read_u32::<LittleEndian>()?;
            let rec_count = dr.read_u32::<LittleEndian>()?;
            let nonce = if encrypted {
                let mut n = [0u8; NONCE_LEN];
                dr.read_exact(&mut n)?;
                Some(n)
            } else {
                None
            };
            block_start.push(acc);
            acc += rec_count as u64;
            dir.push(DirEntry {
                offset,
                comp_len,
                raw_len,
                rec_count,
                nonce,
            });
        }
        block_start.push(acc);
        Ok(Self {
            src,
            dir,
            block_start,
            cipher,
            codec,
        })
    }

    /// Number of blocks in the file.
    pub fn num_blocks(&self) -> usize {
        self.dir.len()
    }

    /// Total number of records across all blocks.
    pub fn total_records(&self) -> u64 {
        *self.block_start.last().unwrap_or(&0)
    }

    /// The block container codec (from the file magic) — lets a store built on this reader
    /// derive its record encoding without a separate flag (e.g. `node_labels.blk` treats a
    /// `Raw` container as bitmask records, a `Zstd` container as varint records).
    pub fn codec(&self) -> BlockCodec {
        self.codec
    }

    /// Map a global record index (0-based, in append order) to its location.
    pub fn locate(&self, global: u64) -> Result<RecordLoc> {
        if global >= self.total_records() {
            bail!(
                "record index {global} out of range (have {})",
                self.total_records()
            );
        }
        // block_start is sorted ascending; find the block whose range contains `global`.
        let block = match self.block_start.binary_search(&global) {
            Ok(b) => b,      // exact match: first record of block b
            Err(b) => b - 1, // between starts: belongs to the previous block
        };
        Ok(RecordLoc {
            block: BlockId(block as u32),
            slot: (global - self.block_start[block]) as u32,
        })
    }

    /// Read the `global`-th record (append order) directly.
    pub fn read_record_global(&self, global: u64) -> Result<Vec<u8>> {
        let loc = self.locate(global)?;
        self.read_record(loc)
    }

    /// Decrypt-then-decompress a block's stored (on-disk) bytes into its raw form.
    /// An encrypted file's stored bytes are the compressed block sealed with the
    /// AEAD, so we unseal before inflating.
    fn decode_stored(&self, e: &DirEntry, stored: Vec<u8>) -> Result<Vec<u8>> {
        let comp = match (&self.cipher, &e.nonce) {
            (Some(cipher), Some(nonce)) => cipher.decrypt(nonce, &stored)?,
            _ => stored,
        };
        // Raw blocks are stored verbatim (the unseal above already yielded the raw
        // bytes) — no decompress pass, which is the whole point of the Raw codec. zstd's
        // frame decode used to validate the raw length implicitly; the Raw path has no such
        // check, so verify the stored length matches the directory's `raw_len` — otherwise a
        // truncated/corrupt block would reach `parse_block`/`read_record` with an offset table
        // that overruns the data, panicking the query thread instead of surfacing a
        // recoverable error (the degree-column fault handler turns `Err` into a CSR fallback).
        match self.codec {
            BlockCodec::Zstd => codec::decompress(&comp, e.raw_len as usize),
            BlockCodec::Raw => {
                if comp.len() != e.raw_len as usize {
                    bail!(
                        "raw block is {} bytes, directory says {}",
                        comp.len(),
                        e.raw_len
                    );
                }
                Ok(comp)
            }
        }
    }

    /// Read and decompress a whole block by id.
    pub fn read_block(&self, block: BlockId) -> Result<Vec<u8>> {
        let e = self
            .dir
            .get(block.index())
            .with_context(|| format!("block {} out of range", block.0))?;
        // `comp_len` is an unvalidated on-disk `u32` — a forged directory could ask for a
        // 4 GiB buffer for a block the file does not even contain. Check the claim first.
        codec::check_stored_len(e.comp_len as usize)?;
        let mut stored = vec![0u8; e.comp_len as usize];
        self.src.read_exact_at(&mut stored, e.offset)?;
        self.decode_stored(e, stored)
    }

    /// Read a single record by location (decompresses its block, copies the slot).
    pub fn read_record(&self, loc: RecordLoc) -> Result<Vec<u8>> {
        let raw = self.read_block(loc.block)?;
        let (offsets, data) = parse_block(&raw)?;
        // `parse_block` validates the offset table against `data`; `record_from_block` re-checks
        // the slot is in range. This used to re-implement both checks inline — one copy now.
        let rec = record_from_block(&offsets, data, loc.slot)
            .with_context(|| format!("block {}", loc.block.0))?;
        Ok(rec.to_vec())
    }

    /// Read-ahead window: how many blocks a whole-file scan fetches concurrently.
    /// Over a remote backend this many round-trips overlap; resident memory is
    /// bounded to this many blocks (not the whole object), so a large store does
    /// not balloon RSS. On a local file the batch reads serially (no benefit, no
    /// harm). 16 hides typical S3 RTT without an aggressive preload.
    const SCAN_READAHEAD: usize = 16;

    /// Visit every block once, in ascending order, decompressed exactly once,
    /// using a bounded concurrent read-ahead so a remote backend overlaps its
    /// fetch round-trips. `f` receives `(block_index, raw_block_bytes)`.
    pub fn for_each_block(&self, f: impl FnMut(usize, &[u8]) -> Result<()>) -> Result<()> {
        self.for_each_block_in(0, self.dir.len(), f)
    }

    /// [`for_each_block`](BlockFileReader::for_each_block) restricted to blocks
    /// `[b_lo, b_hi)`. Disjoint block ranges share no state, so callers may drive
    /// several concurrently over one reader.
    pub fn for_each_block_in(
        &self,
        b_lo: usize,
        b_hi: usize,
        mut f: impl FnMut(usize, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let n = b_hi.min(self.dir.len());
        let mut bi = b_lo;
        while bi < n {
            let hi = (bi + Self::SCAN_READAHEAD).min(n);
            // Fetch this window's stored bytes — concurrently on a remote backend
            // (overlapping RTTs), serially on a local file.
            let ranges: Vec<(u64, u64)> = self.dir[bi..hi]
                .iter()
                .map(|e| (e.offset, e.comp_len as u64))
                .collect();
            let stored_batch = self.src.read_ranges(&ranges)?;
            for (k, stored) in stored_batch.into_iter().enumerate() {
                let idx = bi + k;
                let raw = self.decode_stored(&self.dir[idx], stored)?;
                f(idx, &raw)?;
            }
            bi = hi;
        }
        Ok(())
    }

    /// Visit every record once, in ascending global order, decompressing each
    /// block a single time. `f` borrows each record (no per-record allocation).
    ///
    /// Use this for O(n) whole-file scans (e.g. building label / reltype
    /// postings at generation open). The per-record [`read_record_global`] path
    /// re-decompresses a record's *entire* block on every call, so scanning all
    /// N records that way does O(records-per-block) redundant zstd work per
    /// block — quadratic-feeling on a large store. This pass is O(total bytes)
    /// and uses a bounded concurrent read-ahead (see [`for_each_block`]) so a
    /// remote backend overlaps its fetch round-trips.
    ///
    /// [`for_each_block`]: BlockFileReader::for_each_block
    pub fn for_each_record(&self, f: impl FnMut(u64, &[u8]) -> Result<()>) -> Result<()> {
        self.for_each_record_in(0, self.total_records(), f)
    }

    /// [`for_each_record`](BlockFileReader::for_each_record) restricted to global
    /// record indices `[lo, hi)`, decompressing only the blocks that hold them.
    ///
    /// This is the primitive a *parallel* whole-file sweep is built from: shard the
    /// record space into contiguous ranges and give each worker one. Blocks straddling
    /// a range boundary are decompressed by both neighbours (at most one block of
    /// duplicated work per boundary); everything else is decompressed exactly once, so
    /// the total zstd work is the same as the serial scan. Records outside `[lo, hi)`
    /// are skipped without being handed to `f`, so each record is visited by exactly
    /// one worker.
    pub fn for_each_record_in(
        &self,
        lo: u64,
        hi: u64,
        mut f: impl FnMut(u64, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let total = self.total_records();
        let (lo, hi) = (lo.min(total), hi.min(total));
        if lo >= hi {
            return Ok(());
        }
        let b_lo = self.locate(lo)?.block.index();
        // `hi` is exclusive, so the last block we need is the one holding `hi - 1`.
        let b_hi = self.locate(hi - 1)?.block.index() + 1;
        self.for_each_block_in(b_lo, b_hi, |bi, raw| {
            let (offsets, data) = parse_block(raw)?;
            let start = self.block_start[bi];
            for slot in 0..self.dir[bi].rec_count {
                let global = start + slot as u64;
                if global < lo {
                    continue;
                }
                if global >= hi {
                    break;
                }
                let rec = record_from_block(&offsets, data, slot)?;
                f(global, rec)?;
            }
            Ok(())
        })
    }
}

/// What [`check_offset_table`] labels itself as in a [`DecodeRejected`].
const OFFSET_TABLE: &str = "block";

/// Reject a slot-offset table that is not a partition of the `data_len`-byte data region.
///
/// [`BlockFileWriter::take_raw_block`] is the format's *only* block constructor, and it emits
/// exactly one shape: `cur_offsets` starts as `[0]` and every `add` pushes `cur_data.len()`,
/// so a legitimate table always starts at 0, never decreases, and ends at exactly the data
/// length. Nothing in the decode path pads that region — `decode_stored` unseals to the exact
/// plaintext, zstd decompresses to the directory's `raw_len`, and the `Raw` codec asserts the
/// stored length matches — so this is the encoder's whole contract, not an approximation of it.
///
/// It is checked **here**, once, because [`parse_block`] hands `(offsets, data)` to four callers
/// that then index the table by hand (`pq.rs`'s `load_resident`, `merge_build.rs`'s `next_raw`,
/// `extsort.rs`'s run reader, and [`read_record`]) — and a bad entry in any of them is a slice
/// out of bounds, i.e. a **panic at generation open**, not a recoverable error. Validating at the
/// single parse point makes `start <= end <= data.len()` true *by construction* for every table
/// `parse_block` returns, so those callers cannot reintroduce the bug by forgetting: monotonicity
/// gives `offsets[i] <= offsets[i+1]`, and the terminal `== data_len` caps the whole run.
///
/// [`read_record`]: BlockFileReader::read_record
/// [`BlockFileWriter::take_raw_block`]: BlockFileWriter
fn check_offset_table(offsets: &[u32], data_len: usize) -> Result<()> {
    let reject = |reason, slot: usize, start: u32, end: u32| {
        Err(DecodeRejected::BlockOffsetTable {
            what: OFFSET_TABLE,
            reason,
            slot,
            start,
            end,
            data_len,
        }
        .into())
    };
    // `parse_block` always reads `count + 1` entries, so the table is never empty; guard anyway
    // rather than index a slice whose construction this function does not own.
    let Some((&last, head)) = offsets.split_last() else {
        return reject("empty table", 0, 0, 0);
    };
    if offsets[0] != 0 {
        return reject("first slot does not start at 0", 0, offsets[0], last);
    }
    for (slot, pair) in offsets.windows(2).enumerate() {
        let (start, end) = (pair[0], pair[1]);
        if start > end {
            return reject("offsets decrease", slot, start, end);
        }
    }
    // Caps the run: the table is monotone by here, so every `end` is now `<= last == data_len`.
    if last as usize != data_len {
        return reject(
            "last slot does not end at the data length",
            head.len(),
            last,
            last,
        );
    }
    Ok(())
}

/// Parse a decompressed block into its `(count+1)` slot offsets and the record
/// data region. Exposed so callers that already hold a cached decompressed block
/// can slice records without a second decode.
///
/// The returned table is validated against the data region (see [`check_offset_table`]), so a
/// caller may slice `data[offsets[i]..offsets[i+1]]` for any `i < offsets.len() - 1` without
/// re-checking. [`record_from_block`] is still the tidier way to say that.
pub fn parse_block(raw: &[u8]) -> Result<(Vec<u32>, &[u8])> {
    let mut r = raw;
    let count = r.read_u32::<LittleEndian>()? as usize;
    // Bound the header against the block length *before* allocating, so a corrupt `count`
    // (e.g. from a truncated raw block) can neither over-allocate nor make `&raw[header_len..]`
    // slice out of bounds — it returns an error instead of panicking.
    let header_len = 4 + (count + 1) * 4;
    if raw.len() < header_len {
        bail!(
            "block header ({header_len} B) exceeds block length ({} B)",
            raw.len()
        );
    }
    let mut offsets = Vec::with_capacity(count + 1);
    for _ in 0..=count {
        offsets.push(r.read_u32::<LittleEndian>()?);
    }
    let data = &raw[header_len..];
    check_offset_table(&offsets, data.len())?;
    Ok((offsets, data))
}

/// Slice the `slot`-th record out of an already-parsed block.
pub fn record_from_block<'a>(offsets: &[u32], data: &'a [u8], slot: u32) -> Result<&'a [u8]> {
    let slot = slot as usize;
    if slot + 1 >= offsets.len() {
        bail!("record slot out of range");
    }
    let start = offsets[slot] as usize;
    let end = offsets[slot + 1] as usize;
    if start > end || end > data.len() {
        bail!(
            "record slot {slot} range {start}..{end} out of block data ({} B)",
            data.len()
        );
    }
    Ok(&data[start..end])
}

/// Byte range of the `slot`-th record within the **whole** decompressed block
/// (header included), reading only `count` and the two bracketing slot offsets —
/// no offset-table allocation. A cache holder that keeps the `Arc`-owned block can
/// slice the record straight out by this range, so a scan touching N records pays
/// neither a per-record `to_vec()` copy nor a per-record `parse_block` allocation.
/// Bounds-checked against the block length so a corrupt block errors instead of
/// panicking.
pub fn record_range_in_block(raw: &[u8], slot: u32) -> Result<std::ops::Range<usize>> {
    let mut hdr = raw;
    let count = hdr.read_u32::<LittleEndian>()? as usize;
    let slot = slot as usize;
    if slot + 1 > count {
        bail!("record slot out of range");
    }
    let header_len = 4 + (count + 1) * 4;
    if raw.len() < header_len {
        bail!("truncated block header");
    }
    let off = |i: usize| -> usize {
        let at = 4 + i * 4;
        u32::from_le_bytes([raw[at], raw[at + 1], raw[at + 2], raw[at + 3]]) as usize
    };
    let start = header_len + off(slot);
    let end = header_len + off(slot + 1);
    if start > end || end > raw.len() {
        bail!("record range out of bounds");
    }
    Ok(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    // TEMPORARY (red observation, HIK-140): the pre-fix shims. `for_file` does not exist
    // yet, so a file cipher *is* the generation cipher — which is exactly the bug.
    fn gen_cipher(master: &[u8], salt: &[u8]) -> Arc<BlockCipher> {
        Arc::new(BlockCipher::from_master(master, salt))
    }
    fn file_cipher(c: &Arc<BlockCipher>, _name: &str) -> Arc<BlockCipher> {
        c.clone()
    }

    // HIK-80: the block-file footer is plaintext even for an encrypted file, and `dir_len` — an
    // on-disk `u64` — sized the directory read buffer directly. Forging it is an allocator abort
    // at generation open. (Not one of the sites the ticket named; found in the sweep.)
    #[test]
    fn forged_footer_dir_len_is_refused_at_open() {
        let path = tmp("forged_dir_len");
        let mut w = BlockFileWriter::create(&path, 512, 3).unwrap();
        for i in 0..100u32 {
            w.append_record(format!("rec-{i}").as_bytes()).unwrap();
        }
        w.finish().unwrap();
        assert!(BlockFileReader::open(&path).is_ok());

        // `dir_len` is the second u64 of the 24-byte footer.
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        let dir_len_at = n - FOOTER_LEN as usize + 8;
        bytes[dir_len_at..dir_len_at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let err = match BlockFileReader::open(&path) {
            Ok(_) => panic!("a forged dir_len must be refused"),
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
        std::env::temp_dir().join(format!("slater_bf_{}_{}", std::process::id(), name))
    }

    #[test]
    fn concat_preserves_records_in_order() {
        // Three block files with multi-block content; concat must read back as the
        // exact concatenation of their records, in order.
        let mut parts = Vec::new();
        let mut expected: Vec<Vec<u8>> = Vec::new();
        for (pi, base) in [(0u32, 0u32), (1, 137), (2, 999)].iter() {
            let p = tmp(&format!("part{pi}"));
            let mut w = BlockFileWriter::create(&p, 512, 3).unwrap();
            for i in 0..200u32 {
                let rec =
                    format!("p{pi}-r{}-{}", base + i, "z".repeat((i % 30) as usize)).into_bytes();
                w.append_record(&rec).unwrap();
                expected.push(rec);
            }
            w.finish().unwrap();
            parts.push(p);
        }
        let out = tmp("concat");
        let total = concat_block_files(&out, &parts).unwrap();
        assert_eq!(total, expected.len() as u64);

        let r = BlockFileReader::open(&out).unwrap();
        assert_eq!(r.total_records(), expected.len() as u64);
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(&r.read_record_global(i as u64).unwrap(), want, "record {i}");
        }
        for p in &parts {
            let _ = std::fs::remove_file(p);
        }
        let _ = std::fs::remove_file(&out);
    }

    #[test]
    fn many_records_roundtrip_across_blocks() {
        let path = tmp("many");
        // Small target forces multiple blocks.
        let mut w = BlockFileWriter::create(&path, 1024, 3).unwrap();
        let mut locs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..500u32 {
            let rec = format!("record-{i}-{}", "x".repeat((i % 40) as usize)).into_bytes();
            locs.push(w.append_record(&rec).unwrap());
            expected.push(rec);
        }
        let nblocks = w.finish().unwrap();
        assert!(
            nblocks > 1,
            "expected the small target to span multiple blocks"
        );

        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.num_blocks() as u64, nblocks);
        for (loc, want) in locs.iter().zip(&expected) {
            assert_eq!(&r.read_record(*loc).unwrap(), want);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn global_record_addressing_matches_append_order() {
        let path = tmp("global");
        let mut w = BlockFileWriter::create(&path, 512, 3).unwrap();
        let mut expected = Vec::new();
        for i in 0..300u32 {
            let rec = format!("n{i}:{}", "y".repeat((i % 30) as usize)).into_bytes();
            w.append_record(&rec).unwrap();
            expected.push(rec);
        }
        w.finish().unwrap();

        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.total_records(), expected.len() as u64);
        // Every node id resolves to the record appended for it, no side table.
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(&r.read_record_global(i as u64).unwrap(), want);
        }
        assert!(r.locate(expected.len() as u64).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn record_range_in_block_matches_record_from_block() {
        // The allocation-free range helper must slice the exact same bytes as the
        // parse_block + record_from_block path, for every slot in a real block.
        let path = tmp("range");
        let mut w = BlockFileWriter::create(&path, 1024, 3).unwrap();
        let mut locs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..200u32 {
            let rec = format!("r{i}:{}", "q".repeat((i % 50) as usize)).into_bytes();
            locs.push(w.append_record(&rec).unwrap());
            expected.push(rec);
        }
        w.finish().unwrap();

        let r = BlockFileReader::open(&path).unwrap();
        for (loc, want) in locs.iter().zip(&expected) {
            let raw = r.read_block(loc.block).unwrap();
            let range = record_range_in_block(&raw, loc.slot).unwrap();
            assert_eq!(&raw[range], &want[..]);
        }
        // Out-of-range slot errors instead of panicking.
        let raw0 = r.read_block(locs[0].block).unwrap();
        assert!(record_range_in_block(&raw0, u32::MAX).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_file_opens_with_zero_blocks() {
        let path = tmp("empty");
        let w = BlockFileWriter::create(&path, 256 * 1024, 3).unwrap();
        let n = w.finish().unwrap();
        assert_eq!(n, 0);
        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.num_blocks(), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_bad_magic() {
        let path = tmp("badmagic");
        std::fs::write(&path, vec![0u8; 64]).unwrap();
        assert!(BlockFileReader::open(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encrypted_records_roundtrip_across_blocks() {
        use crate::crypto::BlockCipher;
        let path = tmp("enc_many");
        let cipher = Arc::new(BlockCipher::from_master(b"master-key", &[7u8; 32]));
        // Small target forces multiple blocks, each with its own nonce.
        let mut w =
            BlockFileWriter::create_with_cipher(&path, 1024, 3, Some(cipher.clone())).unwrap();
        let mut locs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..500u32 {
            let rec = format!("secret-{i}-{}", "z".repeat((i % 40) as usize)).into_bytes();
            locs.push(w.append_record(&rec).unwrap());
            expected.push(rec);
        }
        let nblocks = w.finish().unwrap();
        assert!(nblocks > 1, "small target should span multiple blocks");

        // The right key reads every record back.
        let r = BlockFileReader::open_with_cipher(&path, Some(cipher)).unwrap();
        assert_eq!(r.num_blocks() as u64, nblocks);
        for (loc, want) in locs.iter().zip(&expected) {
            assert_eq!(&r.read_record(*loc).unwrap(), want);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn corrupt_block_offsets_error_not_panic() {
        // A corrupt `count` must not over-allocate or slice out of bounds.
        let mut bad_hdr = Vec::new();
        bad_hdr.extend_from_slice(&100_000u32.to_le_bytes()); // claims 100k records
        bad_hdr.extend_from_slice(&[0u8; 8]); // ...but the block is tiny
        assert!(parse_block(&bad_hdr).is_err());
        // An offset table whose ranges overrun the data region errors, not panics.
        let offsets = [0u32, 50]; // end 50 > data len 10
        let data = [0u8; 10];
        assert!(record_from_block(&offsets, &data, 0).is_err());
    }

    /// Assemble a block image by hand: `count ‖ offsets ‖ data`. The writer can only ever
    /// emit well-formed tables, so a forged image is the only way to reach the rejections
    /// below.
    fn raw_block(offsets: &[u32], data: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&((offsets.len() - 1) as u32).to_le_bytes());
        for o in offsets {
            b.extend_from_slice(&o.to_le_bytes());
        }
        b.extend_from_slice(data);
        b
    }

    fn rejects_offset_table(offsets: &[u32], data: &[u8]) -> String {
        let err = parse_block(&raw_block(offsets, data))
            .expect_err("forged offset table must be refused");
        // Branch on the error *type*, never its text (CONTRIBUTING.md).
        match err.downcast_ref::<DecodeRejected>() {
            Some(DecodeRejected::BlockOffsetTable { reason, .. }) => reason.to_string(),
            other => panic!("expected DecodeRejected::BlockOffsetTable, got {other:?}"),
        }
    }

    /// `parse_block` hands `(offsets, data)` to callers that index the table by hand
    /// (`pq.rs`'s `load_resident`, `merge_build.rs`'s `next_raw`, `extsort.rs`'s run reader),
    /// so an unvalidated entry is a **slice-out-of-bounds panic at generation open**, not a
    /// recoverable error. Validating here makes `start <= end <= data.len()` true by
    /// construction for all of them. HIK-128.
    #[test]
    fn forged_offset_table_is_refused_at_parse() {
        let data = [7u8; 9];
        // An offset running past the end of the data region.
        assert_eq!(
            rejects_offset_table(&[0, 3, 6, 999], &data),
            "last slot does not end at the data length"
        );
        // `start > end` — the slice that panics with "start > end" rather than an index
        // overrun. Slot 0 is deliberately well-formed, so this is the monotonicity check
        // firing and not an earlier one.
        assert_eq!(
            rejects_offset_table(&[0, 3, 2, 9], &data),
            "offsets decrease"
        );
        // A table that does not start at 0 leaves a prefix of the data unreachable.
        assert_eq!(
            rejects_offset_table(&[1, 3, 6, 9], &data),
            "first slot does not start at 0"
        );
        // A terminal that stops short is equally not a partition of the region.
        assert_eq!(
            rejects_offset_table(&[0, 3, 6, 8], &data),
            "last slot does not end at the data length"
        );
    }

    /// The guard must not be a no-op: a well-formed table still parses, and every slot still
    /// slices back to the exact bytes the writer put in it.
    #[test]
    fn well_formed_offset_table_still_parses() {
        let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9];
        let block = raw_block(&[0, 3, 6, 9], &data);
        let (offsets, out) = parse_block(&block).unwrap();
        assert_eq!(offsets, vec![0, 3, 6, 9]);
        assert_eq!(out, &data);
        assert_eq!(record_from_block(&offsets, out, 0).unwrap(), &[1, 2, 3]);
        assert_eq!(record_from_block(&offsets, out, 2).unwrap(), &[7, 8, 9]);
        // Empty records are legal (`start == end`) and must not be read as a decrease.
        let block = raw_block(&[0, 0, 9], &data);
        let (offsets, out) = parse_block(&block).unwrap();
        assert_eq!(record_from_block(&offsets, out, 0).unwrap(), b"");
        assert_eq!(record_from_block(&offsets, out, 1).unwrap(), &data);
        // A single-record block, and the degenerate empty block.
        assert!(parse_block(&raw_block(&[0, 9], &data)).is_ok());
        assert!(parse_block(&raw_block(&[0], &[])).is_ok());
    }

    #[test]
    fn raw_codec_roundtrips_and_skips_compression() {
        // Raw blocks are stored verbatim: comp_len == raw_len for every block, and the
        // records read back byte-identical with no zstd pass.
        let path = tmp("raw_many");
        let mut w =
            BlockFileWriter::create_with_codec(&path, 1024, BlockCodec::Raw, 3, None).unwrap();
        let mut locs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..300u32 {
            // Highly compressible payload — zstd *would* shrink it, so equal lengths
            // prove the codec was genuinely skipped.
            let rec = vec![(i % 251) as u8; 40 + (i % 30) as usize];
            locs.push(w.append_record(&rec).unwrap());
            expected.push(rec);
        }
        let nblocks = w.finish().unwrap();
        assert!(nblocks > 1, "small target should span multiple blocks");

        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.codec, BlockCodec::Raw);
        for e in &r.dir {
            assert_eq!(e.comp_len, e.raw_len, "raw blocks are stored uncompressed");
        }
        for (loc, want) in locs.iter().zip(&expected) {
            assert_eq!(&r.read_record(*loc).unwrap(), want);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn raw_encrypted_roundtrips() {
        use crate::crypto::BlockCipher;
        let path = tmp("raw_enc");
        let cipher = Arc::new(BlockCipher::from_master(b"master-key", &[9u8; 32]));
        let mut w = BlockFileWriter::create_with_codec(
            &path,
            1024,
            BlockCodec::Raw,
            3,
            Some(cipher.clone()),
        )
        .unwrap();
        let mut locs = Vec::new();
        let mut expected = Vec::new();
        for i in 0..400u32 {
            let rec = format!("raw-secret-{i}-{}", "y".repeat((i % 35) as usize)).into_bytes();
            locs.push(w.append_record(&rec).unwrap());
            expected.push(rec);
        }
        let nblocks = w.finish().unwrap();
        assert!(nblocks > 1);

        let r = BlockFileReader::open_with_cipher(&path, Some(cipher)).unwrap();
        assert_eq!(r.codec, BlockCodec::Raw);
        // Sealed raw block counts ciphertext = raw + 16-byte tag.
        for e in &r.dir {
            assert_eq!(e.comp_len, e.raw_len + 16, "sealed raw = raw + AEAD tag");
        }
        for (loc, want) in locs.iter().zip(&expected) {
            assert_eq!(&r.read_record(*loc).unwrap(), want);
        }
        // Absent key on a raw+enc file is still refused.
        assert!(BlockFileReader::open(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn encrypted_block_bytes_are_not_plaintext_on_disk() {
        use crate::crypto::BlockCipher;
        let path = tmp("enc_ondisk");
        let cipher = Arc::new(BlockCipher::from_master(b"k", &[1u8; 32]));
        let mut w = BlockFileWriter::create_with_cipher(&path, 4096, 3, Some(cipher)).unwrap();
        let marker = b"PLAINTEXT-MARKER-camelid-camelid";
        w.append_record(marker).unwrap();
        w.finish().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            !bytes.windows(marker.len()).any(|w| w == marker),
            "the plaintext marker must not appear in the encrypted file"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn wrong_key_and_absent_key_are_refused() {
        use crate::crypto::BlockCipher;
        let path = tmp("enc_refuse");
        let right = Arc::new(BlockCipher::from_master(b"right", &[3u8; 32]));
        let mut w =
            BlockFileWriter::create_with_cipher(&path, 4096, 3, Some(right.clone())).unwrap();
        w.append_record(b"sensitive payload here").unwrap();
        w.finish().unwrap();

        // Absent key: refused at open with a clear error (not a panic).
        let err = match BlockFileReader::open(&path) {
            Ok(_) => panic!("expected an absent-key refusal"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("encrypted but no key"));

        // Wrong key: opens (the directory is plaintext) but a block read fails
        // cleanly at the AEAD tag check.
        let wrong = Arc::new(BlockCipher::from_master(b"wrong", &[3u8; 32]));
        let r = BlockFileReader::open_with_cipher(&path, Some(wrong)).unwrap();
        let err = r.read_record_global(0).unwrap_err();
        assert!(err.to_string().contains("wrong key"));
        let _ = std::fs::remove_file(&path);
    }

    /// `for_each_record_in` is what makes a parallel whole-file sweep possible, so
    /// the contract it has to keep is: partition the record space into contiguous
    /// ranges, and every record is visited by exactly one range, in ascending order.
    /// Boundaries deliberately fall inside blocks here (records per block ≫ 1).
    #[test]
    fn ranged_record_scan_partitions_the_file_exactly_once() {
        let path = tmp("range_scan");
        let n = 500u64;
        let mut w = BlockFileWriter::create(&path, 256, 3).unwrap();
        for i in 0..n {
            w.append_record(format!("rec{i:04}").as_bytes()).unwrap();
        }
        let blocks = w.finish().unwrap();
        assert!(blocks > 4, "want a multi-block file, got {blocks}");

        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.total_records(), n);

        // Four contiguous ranges whose bounds do not align to block starts.
        let bounds = [0u64, 137, 250, 401, n];
        let mut seen: Vec<u64> = Vec::new();
        for w in bounds.windows(2) {
            let (lo, hi) = (w[0], w[1]);
            let mut local = Vec::new();
            r.for_each_record_in(lo, hi, |g, rec| {
                assert_eq!(rec, format!("rec{g:04}").as_bytes(), "wrong record at {g}");
                local.push(g);
                Ok(())
            })
            .unwrap();
            assert_eq!(local, (lo..hi).collect::<Vec<_>>(), "range [{lo},{hi})");
            seen.extend(local);
        }
        assert_eq!(seen, (0..n).collect::<Vec<_>>(), "not an exact partition");

        // Degenerate and clamped ranges are silent no-ops, not errors.
        let mut hits = 0;
        r.for_each_record_in(10, 10, |_, _| {
            hits += 1;
            Ok(())
        })
        .unwrap();
        r.for_each_record_in(n, n + 50, |_, _| {
            hits += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(hits, 0);
        let _ = std::fs::remove_file(&path);
    }

    /// The parallel seal pipeline drains in block order, so a file written through it
    /// must read back with its records in append order and its blocks intact — even
    /// when workers finish out of order (many small blocks make that likely).
    #[test]
    fn parallel_sealing_preserves_record_order() {
        let path = tmp("seal_order");
        let n = 5_000u64;
        let mut w = BlockFileWriter::create(&path, 512, 3).unwrap();
        for i in 0..n {
            w.append_record(format!("{i:08}").as_bytes()).unwrap();
        }
        let blocks = w.finish().unwrap();
        assert!(blocks > 50, "want many blocks so seals race, got {blocks}");

        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.total_records(), n);
        let mut next = 0u64;
        r.for_each_record(|g, rec| {
            assert_eq!(g, next);
            assert_eq!(rec, format!("{g:08}").as_bytes());
            next += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(next, n);
        let _ = std::fs::remove_file(&path);
    }

    /// The parallel concat rebuilds each directory entry, and an encrypted entry is
    /// wider by a 24-byte per-block nonce. Get that stride wrong and every block after
    /// the first decrypts to garbage — so the encrypted path gets its own round-trip.
    #[test]
    fn concat_preserves_encrypted_records_and_their_nonces() {
        use crate::crypto::BlockCipher;
        let cipher = Arc::new(BlockCipher::from_master(b"concat key", &[7u8; 32]));
        let mut parts = Vec::new();
        let mut expected: Vec<Vec<u8>> = Vec::new();
        for pi in 0..3u32 {
            let p = tmp(&format!("enc_part{pi}"));
            let mut w =
                BlockFileWriter::create_with_cipher(&p, 512, 3, Some(cipher.clone())).unwrap();
            for i in 0..150u32 {
                let rec = format!("e{pi}-{i}-{}", "y".repeat((i % 40) as usize)).into_bytes();
                w.append_record(&rec).unwrap();
                expected.push(rec);
            }
            w.finish().unwrap();
            parts.push(p);
        }
        let out = tmp("enc_concat");
        let total = concat_block_files(&out, &parts).unwrap();
        assert_eq!(total, expected.len() as u64);

        let r = BlockFileReader::open_with_cipher(&out, Some(cipher)).unwrap();
        assert_eq!(r.total_records(), expected.len() as u64);
        for (i, want) in expected.iter().enumerate() {
            assert_eq!(&r.read_record_global(i as u64).unwrap(), want, "record {i}");
        }
        for p in &parts {
            let _ = std::fs::remove_file(p);
        }
        let _ = std::fs::remove_file(&out);
    }

    /// `(dir_offset, dir_len, block_count)` from a block file's plaintext footer.
    fn read_footer(bytes: &[u8]) -> (usize, usize, usize) {
        let n = bytes.len();
        let f = &bytes[n - FOOTER_LEN as usize..];
        (
            u64::from_le_bytes(f[0..8].try_into().unwrap()) as usize,
            u64::from_le_bytes(f[8..16].try_into().unwrap()) as usize,
            u64::from_le_bytes(f[16..24].try_into().unwrap()) as usize,
        )
    }

    /// HIK-140: a sealed block carries its ordinal as AEAD associated data, so an
    /// attacker who cannot forge the tag still cannot *move* a block to another
    /// ordinal in the same file. The move is a directory-entry swap — location,
    /// lengths and nonce travel with the block, so the tag itself still verifies;
    /// only the AAD binding refuses it.
    #[test]
    fn sealed_block_refuses_to_open_at_another_ordinal() {
        use crate::crypto::AeadRejected;
        let path = tmp("relocate_ordinal");
        let cipher = gen_cipher(b"master-key", &[3u8; 32]);
        let mut w = BlockFileWriter::create_with_cipher(
            &path,
            8, // tiny target ⇒ one record per block
            3,
            Some(file_cipher(&cipher, "relocate.blk")),
        )
        .unwrap();
        for i in 0..4u32 {
            w.append_record(format!("block-payload-{i}").as_bytes())
                .unwrap();
        }
        let nblocks = w.finish().unwrap();
        assert_eq!(nblocks, 4, "one record per block");

        // Swap directory entries 0 and 1: block 1's ciphertext is now presented as
        // block 0 (and vice versa), with its own nonce.
        let mut bytes = std::fs::read(&path).unwrap();
        let (dir_offset, _, count) = read_footer(&bytes);
        assert_eq!(count, 4);
        let e = DIR_ENTRY_LEN_ENC;
        let first: Vec<u8> = bytes[dir_offset..dir_offset + e].to_vec();
        bytes.copy_within(dir_offset + e..dir_offset + 2 * e, dir_offset);
        bytes[dir_offset + e..dir_offset + 2 * e].copy_from_slice(&first);
        std::fs::write(&path, &bytes).unwrap();

        let r =
            BlockFileReader::open_with_cipher(&path, Some(file_cipher(&cipher, "relocate.blk")))
                .unwrap();
        let err = r
            .read_block(BlockId(0))
            .expect_err("a block sealed at ordinal 1 must not open at ordinal 0");
        assert_eq!(
            err.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch),
            "relocation must fail in the AEAD, not later in the decode: {err:#}"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// HIK-140: a sealed block is bound to the *file* it was written into, so bytes
    /// lifted out of one `.blk` and pasted into another `.blk` of the same generation
    /// — same key, same ordinal — do not open.
    #[test]
    fn sealed_block_refuses_to_open_in_another_file() {
        use crate::crypto::AeadRejected;
        let gen = gen_cipher(b"master-key", &[4u8; 32]);
        let a_path = tmp("lift_src");
        let b_path = tmp("lift_dst");
        for (path, name, rec) in [
            (&a_path, "node_props.blk", b"payload-A-0123456789"),
            (&b_path, "edge_props.blk", b"payload-B-0123456789"),
        ] {
            let mut w =
                BlockFileWriter::create_with_cipher(path, 4096, 3, Some(file_cipher(&gen, name)))
                    .unwrap();
            w.append_record(rec).unwrap();
            assert_eq!(w.finish().unwrap(), 1);
        }

        // Rebuild `edge_props.blk` around `node_props.blk`'s sealed block 0: same
        // ordinal, same generation key, same nonce — only the file differs.
        let a = std::fs::read(&a_path).unwrap();
        let (a_dir, _, a_count) = read_footer(&a);
        assert_eq!(a_count, 1);
        let a_entry = &a[a_dir..a_dir + DIR_ENTRY_LEN_ENC];
        let a_block = &a[8..a_dir]; // magic(8) .. directory
        let mut forged = Vec::new();
        forged.extend_from_slice(&a[0..8]); // same magic (both encrypted, zstd)
        forged.extend_from_slice(a_block);
        let dir_off = forged.len() as u64;
        forged.extend_from_slice(a_entry); // offset 8 is still correct
        forged.extend_from_slice(&dir_off.to_le_bytes());
        forged.extend_from_slice(&(DIR_ENTRY_LEN_ENC as u64).to_le_bytes());
        forged.extend_from_slice(&1u64.to_le_bytes());
        std::fs::write(&b_path, &forged).unwrap();

        let r =
            BlockFileReader::open_with_cipher(&b_path, Some(file_cipher(&gen, "edge_props.blk")))
                .unwrap();
        let err = r
            .read_record_global(0)
            .expect_err("a block sealed for node_props.blk must not open in edge_props.blk");
        assert_eq!(
            err.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch),
            "cross-file relocation must fail in the AEAD: {err:#}"
        );
        let _ = std::fs::remove_file(&a_path);
        let _ = std::fs::remove_file(&b_path);
    }

    #[test]
    fn plaintext_file_opens_with_a_key_present() {
        use crate::crypto::BlockCipher;
        // A key supplied for a plaintext file is simply ignored — encryption is
        // optional, so a plaintext generation keeps opening even under a key.
        let path = tmp("plain_with_key");
        let mut w = BlockFileWriter::create(&path, 4096, 3).unwrap();
        w.append_record(b"plain record").unwrap();
        w.finish().unwrap();
        let cipher = Arc::new(BlockCipher::from_master(b"unused", &[9u8; 32]));
        let r = BlockFileReader::open_with_cipher(&path, Some(cipher)).unwrap();
        assert_eq!(&r.read_record_global(0).unwrap(), b"plain record");
        let _ = std::fs::remove_file(&path);
    }
}
