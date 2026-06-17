// SPDX-License-Identifier: Apache-2.0
//! Generic external sort: run-formation up to a byte budget, then a binary k-way
//! merge. The single bounded-memory primitive the external builder leans on —
//! used to order edges by source/destination node-id (for streaming CSR) and to
//! order range-index `(value, id)` pairs (for streaming ISAM), none of which fit
//! in RAM at graph scale.
//!
//! A caller pushes records; whenever the in-RAM buffer reaches the budget it is
//! sorted and spilled to a **run file** (a plain [`crate::blockfile`] block
//! container — already streaming, compressed and self-describing, so no new
//! on-disk format is needed). `sorted()` then returns an iterator that merges all
//! runs with a min-heap, holding one decoded record + one decompressed block per
//! run resident (`O(#runs)`); at a multi-GB budget even a 766M-edge sort forms
//! only single-digit runs, so the merge is a single pass.
//!
//! Run files are transient and **never encrypted** — they live under a build-local
//! temp dir and are unlinked when the sort is done (or the sorter is dropped).

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use anyhow::{anyhow, Context, Result};

use crate::blockfile::{parse_block, BlockFileReader, BlockFileWriter};
use crate::ids::BlockId;

/// Target raw block size for spilled run files. Independent of the generation's
/// block size — runs are transient scratch, not part of the published image.
const RUN_BLOCK_BYTES: usize = 256 * 1024;

/// Globally-unique suffix so concurrent/sequential sorters never collide on a run
/// path within one process.
static SORTER_SEQ: AtomicU64 = AtomicU64::new(0);

// ── parallel run-formation (Option A) ────────────────────────────────────────
//
// `spill_run` — sort a full in-RAM buffer, zstd-compress it, write the run file —
// is the CPU-heavy part of the external sort, and it ran inline on the single
// thread that pushes records (so emit.topology sat at ~1 core). We move it onto a
// shared, bounded worker pool: the push thread hands off a full buffer and keeps
// filling the next, while up to N buffers sort+compress in parallel. The k-way
// merge in `sorted()` is unchanged; correctness is unaffected because `cmp_key` is
// a total order, so the merged output is identical regardless of the order runs
// complete in.
//
// Memory faithfulness: each sorter splits its byte budget across (max_inflight + 1)
// smaller buffers, so peak resident bytes per sorter stay ≈ `budget_bytes` — just
// more, smaller runs (a slightly larger but still single-pass merge heap).

/// One queued spill: sort + write, executed on a pool worker.
type SpillJob = Box<dyn FnOnce() + Send + 'static>;

/// Process-wide worker pool that executes spill jobs. Lazily started; sized by
/// `SLATER_EXTSORT_SPILL_THREADS` (default: online cores, capped at 16). Bounding
/// the thread count here means N concurrent ExtSorters don't each spawn their own
/// pool — they all submit into this one.
static SPILL_POOL: OnceLock<Sender<SpillJob>> = OnceLock::new();

/// Caller-configured spill-worker cap (`0` = unset). Set once by
/// [`configure_spill_threads`] before any sorter is used.
static CONFIGURED_SPILL_THREADS: AtomicUsize = AtomicUsize::new(0);

/// Set the spill-worker cap (e.g. from `--threads`). Must be called before the
/// first `ExtSorter`; later calls are ignored once the pool has started. The
/// `SLATER_EXTSORT_SPILL_THREADS` env var still overrides this.
pub fn configure_spill_threads(n: usize) {
    CONFIGURED_SPILL_THREADS.store(n.max(1), AtomicOrdering::Relaxed);
}

/// Resolved spill-worker count. `1` means "spill inline" (the original behaviour,
/// kept as an escape hatch). Precedence: env override → configured cap → cores.
fn spill_threads() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("SLATER_EXTSORT_SPILL_THREADS")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or_else(
                || match CONFIGURED_SPILL_THREADS.load(AtomicOrdering::Relaxed) {
                    0 => std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(4),
                    n => n,
                },
            )
            .clamp(1, 64)
    })
}

fn spill_pool() -> &'static Sender<SpillJob> {
    SPILL_POOL.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<SpillJob>();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..spill_threads() {
            let rx = Arc::clone(&rx);
            std::thread::Builder::new()
                .name("slater-extsort-spill".into())
                .spawn(move || loop {
                    // Hold the lock only to dequeue; run the job unlocked.
                    let job = { rx.lock().unwrap().recv() };
                    match job {
                        Ok(job) => job(),
                        Err(_) => break, // sender dropped (never, in practice)
                    }
                })
                .expect("spawn extsort spill worker");
        }
        tx
    })
}

/// Shared completion state for one sorter's in-flight spills.
struct SpillState {
    /// Spills submitted but not yet finished (queued or running).
    pending: Mutex<usize>,
    cv: Condvar,
    /// Completed run files tagged with their dispatch index. Sorted back into
    /// dispatch order before merging so the run sequence — and thus the merge's
    /// tie-break for any equal-keyed records — is identical to the old inline path,
    /// regardless of the order workers finish in.
    runs: Mutex<Vec<(u64, PathBuf)>>,
    /// First error a worker hit, surfaced on the next push / on `sorted()`.
    err: Mutex<Option<anyhow::Error>>,
}

impl SpillState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(0),
            cv: Condvar::new(),
            runs: Mutex::new(Vec::new()),
            err: Mutex::new(None),
        })
    }
    /// Block until every submitted spill has finished.
    fn drain(&self) {
        let mut p = self.pending.lock().unwrap();
        while *p > 0 {
            p = self.cv.wait(p).unwrap();
        }
    }
    fn take_err(&self) -> Option<anyhow::Error> {
        self.err.lock().unwrap().take()
    }
}

/// Sort a buffer by key and write it as one run file. The unit of pool work.
fn write_run<R: SortRecord>(mut buf: Vec<R>, path: &Path, level: i32) -> Result<()> {
    buf.sort_by(|a, b| a.cmp_key(b));
    let mut w = BlockFileWriter::create(path, RUN_BLOCK_BYTES, level)?;
    let mut enc = Vec::new();
    for rec in &buf {
        enc.clear();
        rec.encode(&mut enc);
        w.append_record(&enc)?;
    }
    w.finish()?;
    Ok(())
}

/// A record that can be spilled to a run and merged back by a total-order key.
///
/// `cmp_key` must be a **total** order (embed any tiebreaker, e.g. the id, so two
/// distinct records never compare `Equal`) — the merge is only as deterministic
/// as this comparator.
pub trait SortRecord: Sized {
    /// Append this record's self-delimiting bytes to `buf`.
    fn encode(&self, buf: &mut Vec<u8>);
    /// Decode one record from the front of `r` (the whole slice is one record).
    fn decode(r: &mut &[u8]) -> Result<Self>;
    /// Total ordering key.
    fn cmp_key(&self, other: &Self) -> Ordering;
    /// Cheap upper-ish estimate of the encoded size, for budgeting. An
    /// over-estimate only spills a little sooner; it never affects correctness.
    fn size_hint(&self) -> usize;
}

/// External sorter: feed records with [`ExtSorter::push`], then [`ExtSorter::sorted`].
pub struct ExtSorter<R: SortRecord + Send + 'static> {
    temp_dir: PathBuf,
    /// Per-buffer spill threshold (the budget split across in-flight buffers).
    buf_threshold: usize,
    /// Max spills outstanding at once (`1` ⇒ inline, original behaviour).
    max_inflight: usize,
    level: i32,
    seq: u64,
    buf: Vec<R>,
    buf_bytes: usize,
    /// Monotonic run index for unique run-file names (workers complete out of order).
    next_run: u64,
    state: Arc<SpillState>,
    /// Set once `sorted()` has moved the run paths out, so `Drop` doesn't re-clean.
    consumed: bool,
}

impl<R: SortRecord + Send + 'static> ExtSorter<R> {
    /// Create a sorter spilling under `temp_dir`, holding at most `budget_bytes` of
    /// records resident. `zstd_level` compresses the run files. Run formation
    /// (sort + compress + write) runs on the shared spill pool; the budget is split
    /// across the in-flight buffers so peak resident bytes stay ≈ `budget_bytes`.
    pub fn new(temp_dir: &Path, budget_bytes: usize, zstd_level: i32) -> Result<Self> {
        std::fs::create_dir_all(temp_dir)
            .with_context(|| format!("create extsort temp dir {}", temp_dir.display()))?;
        let max_inflight = spill_threads();
        let budget = budget_bytes.max(1);
        // Split the budget across (in-flight + the one being filled). With inline
        // spilling (max_inflight == 1) this is budget/2; round up and never zero.
        let buf_threshold = (budget / (max_inflight + 1)).max(1);
        Ok(Self {
            temp_dir: temp_dir.to_path_buf(),
            buf_threshold,
            max_inflight,
            level: zstd_level,
            seq: SORTER_SEQ.fetch_add(1, AtomicOrdering::Relaxed),
            buf: Vec::new(),
            buf_bytes: 0,
            next_run: 0,
            state: SpillState::new(),
            consumed: false,
        })
    }

    /// Push one record; spills a sorted run when the buffer reaches its threshold.
    pub fn push(&mut self, rec: R) -> Result<()> {
        self.buf_bytes += rec.size_hint();
        self.buf.push(rec);
        if self.buf_bytes >= self.buf_threshold {
            self.spill_run()?;
        }
        Ok(())
    }

    fn run_path(&mut self) -> (u64, PathBuf) {
        let idx = self.next_run;
        self.next_run += 1;
        let path = self.temp_dir.join(format!(
            "slater_extsort_{}_{}_{}.run",
            std::process::id(),
            self.seq,
            idx
        ));
        (idx, path)
    }

    /// Hand the current buffer off to the spill pool (or write it inline when
    /// `max_inflight == 1`). Surfaces any prior worker error eagerly so a failing
    /// build aborts promptly instead of at `sorted()`.
    fn spill_run(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if let Some(e) = self.state.take_err() {
            return Err(e);
        }
        let (idx, path) = self.run_path();
        let buf = std::mem::take(&mut self.buf);
        self.buf_bytes = 0;

        if self.max_inflight <= 1 {
            // Inline: original single-threaded behaviour.
            write_run(buf, &path, self.level)?;
            self.state.runs.lock().unwrap().push((idx, path));
            return Ok(());
        }

        // Backpressure: block until fewer than `max_inflight` spills are outstanding,
        // so the number of resident buffers (and thus memory) stays bounded.
        {
            let mut p = self.state.pending.lock().unwrap();
            while *p >= self.max_inflight {
                p = self.state.cv.wait(p).unwrap();
            }
            *p += 1;
        }
        let state = Arc::clone(&self.state);
        let level = self.level;
        spill_pool()
            .send(Box::new(move || {
                match write_run(buf, &path, level) {
                    Ok(()) => state.runs.lock().unwrap().push((idx, path)),
                    Err(e) => {
                        let mut slot = state.err.lock().unwrap();
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                    }
                }
                let mut p = state.pending.lock().unwrap();
                *p -= 1;
                state.cv.notify_all();
            }))
            .map_err(|_| anyhow!("extsort spill pool unavailable"))?;
        Ok(())
    }

    /// Finish input and return a merging iterator over all records in key order.
    /// Spills the tail buffer, waits for every in-flight run to land, then merges.
    pub fn sorted(mut self) -> Result<SortedIter<R>> {
        self.spill_run()?;
        self.state.drain();
        if let Some(e) = self.state.take_err() {
            return Err(e);
        }
        self.consumed = true;
        let mut runs = std::mem::take(&mut *self.state.runs.lock().unwrap());
        // Restore dispatch order so the merge is deterministic for equal keys.
        runs.sort_by_key(|(idx, _)| *idx);
        SortedIter::open(runs.into_iter().map(|(_, p)| p).collect())
    }
}

impl<R: SortRecord + Send + 'static> Drop for ExtSorter<R> {
    fn drop(&mut self) {
        // Only meaningful if `sorted()` was never called (e.g. an error aborted the
        // build). Wait for any in-flight spills to finish so their files are closed,
        // then unlink every run.
        if self.consumed {
            return;
        }
        self.state.drain();
        for (_, p) in self.state.runs.lock().unwrap().iter() {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// One run's read cursor: yields its records in stored (already-sorted) order,
/// decompressing one block at a time.
struct RunCursor<R: SortRecord> {
    reader: BlockFileReader,
    block: usize,
    slot: u32,
    cur: Vec<u8>,      // current decompressed block bytes
    offsets: Vec<u32>, // slot offsets into `data`
    data_start: usize, // where the record region begins in `cur`
    _marker: std::marker::PhantomData<R>,
}

impl<R: SortRecord> RunCursor<R> {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BlockFileReader::open(path)?,
            block: 0,
            slot: 0,
            cur: Vec::new(),
            offsets: Vec::new(),
            data_start: 0,
            _marker: std::marker::PhantomData,
        })
    }

    /// Load `self.block` into `cur`/`offsets`. Returns false at end of file.
    fn load_block(&mut self) -> Result<bool> {
        loop {
            if self.block >= self.reader.num_blocks() {
                return Ok(false);
            }
            let raw = self.reader.read_block(BlockId(self.block as u32))?;
            let (offsets, data) = parse_block(&raw)?;
            if offsets.len() <= 1 {
                // Empty block; skip to the next.
                self.block += 1;
                continue;
            }
            self.data_start = raw.len() - data.len();
            self.offsets = offsets;
            self.cur = raw;
            self.slot = 0;
            return Ok(true);
        }
    }

    fn next(&mut self) -> Result<Option<R>> {
        loop {
            if (self.slot as usize) + 1 >= self.offsets.len() {
                self.block += 1;
                if !self.load_block()? {
                    return Ok(None);
                }
                continue;
            }
            let s = self.slot as usize;
            let start = self.data_start + self.offsets[s] as usize;
            let end = self.data_start + self.offsets[s + 1] as usize;
            self.slot += 1;
            let mut rec = &self.cur[start..end];
            return Ok(Some(R::decode(&mut rec)?));
        }
    }
}

/// Min-heap entry; ordered so `BinaryHeap` (a max-heap) pops the smallest key.
struct HeapItem<R: SortRecord> {
    rec: R,
    run: usize,
}

impl<R: SortRecord> PartialEq for HeapItem<R> {
    fn eq(&self, other: &Self) -> bool {
        self.rec.cmp_key(&other.rec) == Ordering::Equal
    }
}
impl<R: SortRecord> Eq for HeapItem<R> {}
impl<R: SortRecord> PartialOrd for HeapItem<R> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<R: SortRecord> Ord for HeapItem<R> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed: a smaller key is "greater" so it sits at the heap top.
        other.rec.cmp_key(&self.rec)
    }
}

/// Iterator merging the spilled runs in key order. Unlinks the run files on drop.
pub struct SortedIter<R: SortRecord> {
    cursors: Vec<RunCursor<R>>,
    heap: std::collections::BinaryHeap<HeapItem<R>>,
    runs: Vec<PathBuf>,
}

impl<R: SortRecord> SortedIter<R> {
    fn open(runs: Vec<PathBuf>) -> Result<Self> {
        let mut cursors = Vec::with_capacity(runs.len());
        let mut heap = std::collections::BinaryHeap::new();
        for (i, path) in runs.iter().enumerate() {
            let mut c = RunCursor::open(path)?;
            c.load_block()?;
            if let Some(rec) = c.next()? {
                heap.push(HeapItem { rec, run: i });
            }
            cursors.push(c);
        }
        Ok(Self {
            cursors,
            heap,
            runs,
        })
    }
}

impl<R: SortRecord> Iterator for SortedIter<R> {
    type Item = Result<R>;

    fn next(&mut self) -> Option<Result<R>> {
        let HeapItem { rec, run } = self.heap.pop()?;
        match self.cursors[run].next() {
            Ok(Some(next)) => self.heap.push(HeapItem { rec: next, run }),
            Ok(None) => {}
            Err(e) => return Some(Err(e)),
        }
        Some(Ok(rec))
    }
}

impl<R: SortRecord> Drop for SortedIter<R> {
    fn drop(&mut self) {
        // Close run files (drop cursors) before unlinking — tidy on every FS.
        self.cursors.clear();
        for p in &self.runs {
            let _ = std::fs::remove_file(p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{read_uvarint, write_uvarint};

    /// A (key,payload) record; cmp_key is a total order on (key, payload).
    struct KV {
        key: u64,
        payload: u64,
    }
    impl SortRecord for KV {
        fn encode(&self, buf: &mut Vec<u8>) {
            write_uvarint(buf, self.key);
            write_uvarint(buf, self.payload);
        }
        fn decode(r: &mut &[u8]) -> Result<Self> {
            let key = read_uvarint(r)?;
            let payload = read_uvarint(r)?;
            Ok(KV { key, payload })
        }
        fn cmp_key(&self, other: &Self) -> Ordering {
            self.key
                .cmp(&other.key)
                .then(self.payload.cmp(&other.payload))
        }
        fn size_hint(&self) -> usize {
            16
        }
    }

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "slater_extsort_test_{}_{}",
            std::process::id(),
            name
        ))
    }

    #[test]
    fn sorts_and_is_a_permutation_across_many_runs() {
        let dir = tmp("perm");
        let _ = std::fs::remove_dir_all(&dir);
        // A deterministic LCG so the input order is scrambled but reproducible.
        let mut state = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state >> 16
        };
        let n = 50_000u64;
        // Tiny budget (~a few KB of records) forces many runs.
        let mut s = ExtSorter::<KV>::new(&dir, 4096, 1).unwrap();
        let mut input = Vec::new();
        for i in 0..n {
            let key = next() % 1000; // many duplicate keys
            s.push(KV { key, payload: i }).unwrap();
            input.push((key, i));
        }
        let it = s.sorted().unwrap();
        let got: Vec<(u64, u64)> = it
            .map(|r| r.unwrap())
            .map(|kv| (kv.key, kv.payload))
            .collect();

        // Globally sorted by (key, payload).
        assert_eq!(got.len(), n as usize);
        for w in got.windows(2) {
            assert!(w[0] <= w[1], "not sorted at {:?} {:?}", w[0], w[1]);
        }
        // A permutation of the input (same multiset).
        let mut a = input.clone();
        a.sort_unstable();
        assert_eq!(got, a);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_files_are_deleted_when_iterator_drops() {
        let dir = tmp("cleanup");
        let _ = std::fs::remove_dir_all(&dir);
        let mut s = ExtSorter::<KV>::new(&dir, 256, 1).unwrap();
        for i in 0..2000u64 {
            s.push(KV {
                key: 2000 - i,
                payload: i,
            })
            .unwrap();
        }
        let count_runs = |dir: &Path| {
            std::fs::read_dir(dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().map(|x| x == "run").unwrap_or(false))
                .count()
        };
        {
            // Hold the iterator alive across the check (consuming it via `map`
            // would drop it first). Run files exist while it lives…
            let mut it = s.sorted().unwrap();
            assert!(count_runs(&dir) > 0, "expected spilled run files to exist");
            // Iterate by reference so `it` stays alive to the end of the block (the
            // run files exist only while it does); a by-value `for` would drop it early.
            for r in it.by_ref() {
                r.unwrap();
            }
        }
        // …and are gone once it drops.
        let runs: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "run").unwrap_or(false))
            .collect();
        assert!(runs.is_empty(), "run files should be unlinked on drop");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_sorter_yields_nothing() {
        let dir = tmp("empty");
        let _ = std::fs::remove_dir_all(&dir);
        let s = ExtSorter::<KV>::new(&dir, 4096, 1).unwrap();
        let mut it = s.sorted().unwrap();
        assert!(it.next().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
