// SPDX-License-Identifier: Apache-2.0
//! Chunk-lazy residency for the dense per-node degree column (`node_degrees.blk`).
//!
//! The column stores every node's exact out- and in-degree as a run of fixed
//! [`DEGREES_PER_RECORD`]-wide records (see [`graph_format::nodedegree`]). Loading it whole
//! costs `node_count × 8` bytes resident (~733 MB on the 91.6M-node graph) even for a query
//! that never sums degrees or touches only part of the id space. This holder keeps the block
//! reader instead and **faults a chunk on first touch**, retaining only the id-range chunks a
//! query actually reads; cold chunks free on the idle-TTL sweep like the block cache, without
//! routing through — and thrashing — the shared `BlockCache`.
//!
//! A chunk fault is one ~1 MiB zstd decompress amortised over 262 K ids (~0.5–1.5 ms on fs,
//! ~10–100 ms per range-GET on an object store), and the whole out-column is only ~350 chunks
//! — so worst-case (touch everything) is a few hundred ms of faults, versus the 0.8 s the
//! degree-sum count fast path takes over the resident column. For latency-critical or
//! object-store deployments the `pinned` mode prefaults every chunk at open and never evicts.
//!
//! Thread-safety / hot path: the accessor takes `&self` and is called from the parallel
//! degree-sum walk (millions of scattered lookups over the ~350 chunks). Faults run **off the
//! lock** — decompress into an `Arc<[u32]>`, then a brief write lock stores it (a concurrent
//! fault of the same chunk just discards the loser; the bytes are identical). Hits take a
//! shared read lock and stamp a per-chunk atomic `last_used`, so parallel readers never
//! serialise. A fault I/O error surfaces as `None` (identical to "no column"): the caller
//! then falls back to the CSR leading-count peek, the same answer, just slower.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use graph_format::blockfile::BlockFileReader;
use graph_format::nodedegree::{read_degree_chunk, records_per_half, DEGREES_PER_RECORD};
use tracing::warn;

/// Residency policy for the dense degree column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DegreeResidency {
    /// Chunk-lazy: fault a chunk on first touch, free cold chunks on the idle-TTL sweep.
    #[default]
    Lazy,
    /// Pinned: prefault every chunk at open and never evict (today's eager behaviour) — for
    /// latency-critical / object-store deployments that must avoid mid-query range-GET faults.
    Pinned,
}

/// One direction's lazily-faulted chunks. Slots are `None` until touched; each carries a
/// monotonic `last_used` (nanos since the column's `created` base) stamped on every access so
/// the idle sweep can drop cold chunks.
struct Half {
    /// `slots[c]` is chunk `c`'s decoded degrees, or `None` if not resident.
    slots: RwLock<Vec<Option<Arc<[u32]>>>>,
    /// Per-chunk last-touch, nanos since `DegreeColumn::created`. Parallel to `slots`; read/
    /// written under a shared read lock (atomic), so hot lookups never take the write lock.
    last_used: Vec<AtomicU64>,
}

impl Half {
    fn new(chunks: usize) -> Self {
        Self {
            slots: RwLock::new(vec![None; chunks]),
            last_used: (0..chunks).map(|_| AtomicU64::new(0)).collect(),
        }
    }
}

/// Chunk-lazy dense degree column. Retains the `node_degrees.blk` reader (with its cipher) and
/// materialises out/in chunks on demand.
pub struct DegreeColumn {
    reader: BlockFileReader,
    node_count: usize,
    /// Records per direction (`node_count.div_ceil(DEGREES_PER_RECORD)`).
    per_half: usize,
    out: Half,
    in_: Half,
    /// Monotonic base for `last_used` deltas — all `Instant`s share the process clock, so
    /// `now.saturating_duration_since(created)` is comparable across the accessor and the sweep.
    created: Instant,
    /// `Pinned` ⇒ never evict; `Lazy` ⇒ cold chunks freed by the idle sweep.
    residency: DegreeResidency,
}

impl DegreeColumn {
    /// Open the column over an already-opened `node_degrees.blk` reader. Validates the record
    /// count up-front (same fail-fast as the eager `read_node_degrees`) so a malformed column
    /// refuses at generation open, not mid-query. `Pinned` prefaults every chunk here.
    pub fn open(
        reader: BlockFileReader,
        node_count: usize,
        residency: DegreeResidency,
    ) -> Result<Self> {
        let per_half = records_per_half(node_count);
        let total = reader.total_records() as usize;
        anyhow::ensure!(
            total == per_half * 2,
            "node_degrees.blk has {total} records, expected {} for {node_count} nodes",
            per_half * 2
        );
        let col = Self {
            reader,
            node_count,
            per_half,
            out: Half::new(per_half),
            in_: Half::new(per_half),
            created: Instant::now(),
            residency,
        };
        if residency == DegreeResidency::Pinned {
            col.prefault_all()?;
        }
        Ok(col)
    }

    /// Fault every chunk (both halves) resident — the pinned path. Errors propagate so a
    /// broken column fails the open rather than silently degrading later.
    fn prefault_all(&self) -> Result<()> {
        for outgoing in [true, false] {
            let half = if outgoing { &self.out } else { &self.in_ };
            for chunk in 0..self.per_half {
                let decoded: Arc<[u32]> =
                    read_degree_chunk(&self.reader, self.per_half, outgoing, chunk)?.into();
                half.slots.write().unwrap()[chunk] = Some(decoded);
            }
        }
        Ok(())
    }

    /// Exact out-degree of dense node `node`, or `None` when out of range or a chunk fault
    /// failed (caller falls back to the CSR leading-count peek — identical answer).
    pub fn out_degree(&self, node: u64) -> Option<u32> {
        self.degree(node, true)
    }

    /// Exact in-degree — counterpart of [`Self::out_degree`].
    pub fn in_degree(&self, node: u64) -> Option<u32> {
        self.degree(node, false)
    }

    fn degree(&self, node: u64, outgoing: bool) -> Option<u32> {
        let idx = node as usize;
        if idx >= self.node_count {
            return None;
        }
        let chunk = idx / DEGREES_PER_RECORD;
        let off = idx % DEGREES_PER_RECORD;
        let half = if outgoing { &self.out } else { &self.in_ };
        let decoded = self.chunk(half, outgoing, chunk)?;
        // Stamp last-touch (Relaxed: eviction only needs approximate recency).
        half.last_used[chunk].store(self.now_nanos(), Ordering::Relaxed);
        decoded.get(off).copied()
    }

    /// Return chunk `c` of `half`, faulting it if cold. The fault decompresses off the lock;
    /// the write lock is held only to store (a concurrent winner is kept, our copy discarded).
    fn chunk(&self, half: &Half, outgoing: bool, c: usize) -> Option<Arc<[u32]>> {
        if let Some(hit) = half.slots.read().unwrap()[c].clone() {
            return Some(hit);
        }
        let decoded: Arc<[u32]> = match read_degree_chunk(&self.reader, self.per_half, outgoing, c)
        {
            Ok(v) => v.into(),
            Err(e) => {
                warn!(error = %e, chunk = c, outgoing, "degree-column chunk fault failed; falling back to CSR peek");
                return None;
            }
        };
        let mut slots = half.slots.write().unwrap();
        // Double-check: another thread may have faulted this chunk while we decoded.
        if let Some(hit) = slots[c].clone() {
            return Some(hit);
        }
        slots[c] = Some(decoded.clone());
        Some(decoded)
    }

    fn now_nanos(&self) -> u64 {
        Instant::now()
            .saturating_duration_since(self.created)
            .as_nanos() as u64
    }

    /// Drop chunk slots untouched for at least `ttl` (both halves). No-op under `Pinned`.
    /// Returns the number of chunks freed. `now` is the sweep's `Instant::now()`.
    pub fn evict_expired(&self, now: Instant, ttl: Duration) -> usize {
        if self.residency == DegreeResidency::Pinned {
            return 0;
        }
        let now_nanos = now.saturating_duration_since(self.created).as_nanos() as u64;
        let ttl_nanos = ttl.as_nanos() as u64;
        let mut freed = 0;
        for half in [&self.out, &self.in_] {
            let mut slots = half.slots.write().unwrap();
            for (c, slot) in slots.iter_mut().enumerate() {
                if slot.is_none() {
                    continue;
                }
                let last = half.last_used[c].load(Ordering::Relaxed);
                if now_nanos.saturating_sub(last) > ttl_nanos {
                    *slot = None;
                    freed += 1;
                }
            }
        }
        freed
    }

    /// Number of chunks currently resident across both halves — for tests / diagnostics.
    pub fn resident_chunks(&self) -> usize {
        [&self.out, &self.in_]
            .iter()
            .map(|h| {
                h.slots
                    .read()
                    .unwrap()
                    .iter()
                    .filter(|s| s.is_some())
                    .count()
            })
            .sum()
    }

    /// Records per direction — for tests.
    pub fn records_per_half(&self) -> usize {
        self.per_half
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::nodedegree::write_node_degrees;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_degcol_{}_{}", std::process::id(), name))
    }

    fn build(n: usize) -> (std::path::PathBuf, Vec<u32>, Vec<u32>) {
        let out: Vec<u32> = (0..n as u32).map(|i| i.wrapping_mul(2654435761)).collect();
        let inn: Vec<u32> = (0..n as u32).map(|i| (i % 5) + 1).collect();
        let path = tmp(&format!("col{n}.blk"));
        write_node_degrees(&path, &out, &inn, 1 << 16, 3, None).unwrap();
        (path, out, inn)
    }

    #[test]
    fn parity_across_random_ids_lazy() {
        let n = 3 * DEGREES_PER_RECORD + 123;
        let (path, out, inn) = build(n);
        let reader = BlockFileReader::open(&path).unwrap();
        let col = DegreeColumn::open(reader, n, DegreeResidency::Lazy).unwrap();

        // Deterministic scatter across id space (no rng in tests).
        let ids = (0..500).map(|k| (k * 2654435761usize) % n).chain([
            0,
            n - 1,
            DEGREES_PER_RECORD,
            DEGREES_PER_RECORD - 1,
            2 * DEGREES_PER_RECORD,
        ]);
        for id in ids {
            assert_eq!(col.out_degree(id as u64), Some(out[id]), "out id {id}");
            assert_eq!(col.in_degree(id as u64), Some(inn[id]), "in id {id}");
        }
        // Out of range ⇒ None (matches the eager `.get()`), no fault.
        assert_eq!(col.out_degree(n as u64), None);
        assert_eq!(col.in_degree((n + 1000) as u64), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn only_touched_chunks_resident() {
        let n = 4 * DEGREES_PER_RECORD + 7;
        let (path, out, _) = build(n);
        let reader = BlockFileReader::open(&path).unwrap();
        let col = DegreeColumn::open(reader, n, DegreeResidency::Lazy).unwrap();
        assert_eq!(col.records_per_half(), 5);
        assert_eq!(
            col.resident_chunks(),
            0,
            "nothing faulted before first touch"
        );

        // Touch one id in out-chunk 2 only.
        let id = 2 * DEGREES_PER_RECORD + 10;
        assert_eq!(col.out_degree(id as u64), Some(out[id]));
        assert_eq!(
            col.resident_chunks(),
            1,
            "one lookup materialises one chunk"
        );

        // A second id in the same chunk faults nothing further.
        assert_eq!(col.out_degree((id + 1) as u64), Some(out[id + 1]));
        assert_eq!(col.resident_chunks(), 1);

        // In-degree of the same id is a different half ⇒ its own chunk.
        col.in_degree(id as u64);
        assert_eq!(col.resident_chunks(), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn idle_chunks_evicted_hot_survive_refault_same() {
        let n = 3 * DEGREES_PER_RECORD + 9;
        let (path, out, _) = build(n);
        let reader = BlockFileReader::open(&path).unwrap();
        let col = DegreeColumn::open(reader, n, DegreeResidency::Lazy).unwrap();

        // Fault out-chunk 0; touching stamps last_used ≈ now.
        let id = 5usize;
        assert_eq!(col.out_degree(id as u64), Some(out[id]));
        assert_eq!(col.resident_chunks(), 1);

        // A huge TTL right after touch ⇒ still hot, nothing freed.
        assert_eq!(
            col.evict_expired(Instant::now(), Duration::from_secs(3600)),
            0
        );
        assert_eq!(col.resident_chunks(), 1);

        // A zero TTL ⇒ any touched chunk is now idle "past" the window ⇒ freed.
        assert_eq!(col.evict_expired(Instant::now(), Duration::ZERO), 1);
        assert_eq!(col.resident_chunks(), 0);

        // Re-fault after eviction returns the same value (and re-materialises the chunk).
        assert_eq!(col.out_degree(id as u64), Some(out[id]));
        assert_eq!(col.resident_chunks(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pinned_prefaults_everything() {
        let n = 3 * DEGREES_PER_RECORD + 1;
        let (path, out, inn) = build(n);
        let reader = BlockFileReader::open(&path).unwrap();
        let col = DegreeColumn::open(reader, n, DegreeResidency::Pinned).unwrap();
        assert_eq!(col.resident_chunks(), 2 * col.records_per_half());
        // Values still correct, and eviction is a no-op under pinned.
        assert_eq!(col.out_degree(0), Some(out[0]));
        assert_eq!(col.in_degree((n - 1) as u64), Some(inn[n - 1]));
        assert_eq!(col.evict_expired(Instant::now(), Duration::ZERO), 0);
        assert_eq!(col.resident_chunks(), 2 * col.records_per_half());
        let _ = std::fs::remove_file(&path);
    }
}
