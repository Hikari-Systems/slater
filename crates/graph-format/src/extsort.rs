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
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use anyhow::{Context, Result};

use crate::blockfile::{parse_block, BlockFileReader, BlockFileWriter};
use crate::ids::BlockId;

/// Target raw block size for spilled run files. Independent of the generation's
/// block size — runs are transient scratch, not part of the published image.
const RUN_BLOCK_BYTES: usize = 256 * 1024;

/// Globally-unique suffix so concurrent/sequential sorters never collide on a run
/// path within one process.
static SORTER_SEQ: AtomicU64 = AtomicU64::new(0);

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
pub struct ExtSorter<R: SortRecord> {
    temp_dir: PathBuf,
    budget_bytes: usize,
    level: i32,
    seq: u64,
    buf: Vec<R>,
    buf_bytes: usize,
    runs: Vec<PathBuf>,
}

impl<R: SortRecord> ExtSorter<R> {
    /// Create a sorter spilling under `temp_dir`, forming runs of at most
    /// `budget_bytes` of in-RAM records. `zstd_level` compresses the run files.
    pub fn new(temp_dir: &Path, budget_bytes: usize, zstd_level: i32) -> Result<Self> {
        std::fs::create_dir_all(temp_dir)
            .with_context(|| format!("create extsort temp dir {}", temp_dir.display()))?;
        Ok(Self {
            temp_dir: temp_dir.to_path_buf(),
            budget_bytes: budget_bytes.max(1),
            level: zstd_level,
            seq: SORTER_SEQ.fetch_add(1, AtomicOrdering::Relaxed),
            buf: Vec::new(),
            buf_bytes: 0,
            runs: Vec::new(),
        })
    }

    /// Push one record; spills a sorted run when the buffer reaches the budget.
    pub fn push(&mut self, rec: R) -> Result<()> {
        self.buf_bytes += rec.size_hint();
        self.buf.push(rec);
        if self.buf_bytes >= self.budget_bytes {
            self.spill_run()?;
        }
        Ok(())
    }

    fn spill_run(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.buf.sort_by(|a, b| a.cmp_key(b));
        let path = self.temp_dir.join(format!(
            "slater_extsort_{}_{}_{}.run",
            std::process::id(),
            self.seq,
            self.runs.len()
        ));
        let mut w = BlockFileWriter::create(&path, RUN_BLOCK_BYTES, self.level)?;
        let mut enc = Vec::new();
        for rec in &self.buf {
            enc.clear();
            rec.encode(&mut enc);
            w.append_record(&enc)?;
        }
        w.finish()?;
        self.buf.clear();
        self.buf_bytes = 0;
        self.runs.push(path);
        Ok(())
    }

    /// Finish input and return a merging iterator over all records in key order.
    /// The tail buffer is spilled first so every record reaches the merge the same
    /// way (no special in-RAM-vs-on-disk path).
    pub fn sorted(mut self) -> Result<SortedIter<R>> {
        self.spill_run()?;
        let runs = std::mem::take(&mut self.runs);
        SortedIter::open(runs)
    }
}

impl<R: SortRecord> Drop for ExtSorter<R> {
    fn drop(&mut self) {
        // Only fires if `sorted()` was never called (e.g. an error aborted the
        // build); otherwise `runs` was moved out and this removes nothing.
        for p in &self.runs {
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
