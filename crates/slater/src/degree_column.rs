// SPDX-License-Identifier: Apache-2.0
//! Chunk-lazy residency for the dense per-node degree column (`node_degrees.blk`).
//!
//! The column stores every node's exact out- and in-degree as a run of fixed
//! [`DEGREES_PER_RECORD`]-wide records, each a compact per-chunk Elias–Fano encoding (see
//! [`graph_format::degree_ef`]). Loading it whole even for a query that never sums degrees or
//! touches only part of the id space is wasteful. This holder keeps the block reader instead
//! and **faults a chunk on first touch**, retaining only the id-range chunks a query actually
//! reads; cold chunks free on the idle-TTL sweep like the block cache, without routing through
//! — and thrashing — the shared `BlockCache`.
//!
//! A chunk fault is one raw `pread` (~164 KB, no zstd) + EF parse amortised over 262 K ids
//! (tens of µs on fs, ~10–100 ms per range-GET on an object store), and the whole out-column
//! is only ~350 chunks. The resident form is EF (or `Constant`), ~6× smaller than a dense
//! `u32` chunk, so the working set stays cache-friendly for the degree-sum count fast path.
//! For latency-critical or object-store deployments the `pinned` mode prefaults every chunk at
//! open and never evicts.
//!
//! Thread-safety / hot path: the accessor takes `&self` and is called from the parallel
//! degree-sum walk (millions of scattered lookups over the ~350 chunks). Faults run **off the
//! lock** — decode into an `Arc<DegreeChunk>`, then a brief write lock stores it (a concurrent
//! fault of the same chunk just discards the loser; the values are identical). Hits take a
//! shared read lock and only set a coarse per-chunk CLOCK `referenced` bit (recency is tracked
//! CLOCK-style, not with a per-lookup wall-clock read + shared `last_used` store), so parallel
//! readers never serialise on a shared timestamp cache line. A fault I/O error surfaces as
//! `None` (identical to "no column"): the caller then falls back to the CSR leading-count peek,
//! the same answer, just slower.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use graph_format::blockfile::BlockFileReader;
use graph_format::degree_ef::DegreeChunk;
use graph_format::nodedegree::{read_degree_chunk, records_per_half, DEGREES_PER_RECORD};
use tracing::warn;

/// Default soft byte budget for the chunk-lazy degree column when the caller does not
/// configure one (`cache.degreeColumnBytes` unset). Generous enough that a degree-sum walk
/// over a graph of tens of millions of nodes stays fully resident, while bounding the
/// pathological whole-column case (~733 MB at 91.6M nodes) so `lazy` cannot silently grow to
/// `pinned` when the idle-TTL sweep is disabled. Like the block/vector/result pools this is a
/// *cap*, not a reservation. `pinned` deployments that want the whole column resident set
/// `degreeColumn=pinned` (the budget is then ignored).
pub const DEFAULT_BUDGET_BYTES: usize = 256 * 1024 * 1024;

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

/// One direction's lazily-faulted chunks. Slots are `None` until touched. Recency is tracked
/// CLOCK-style: the hot lookup path only sets a cheap per-chunk `referenced` bit, while
/// `last_used` is stamped on fault and re-stamped by the idle sweep (and consulted, coldest
/// first, by the byte-budget pressure evictor) — so a lookup never reads the wall clock nor
/// churns a shared timestamp atomic.
struct Half {
    /// `slots[c]` is chunk `c`'s decoded degrees (compact [`DegreeChunk`]), or `None` if
    /// not resident.
    slots: RwLock<Vec<Option<Arc<DegreeChunk>>>>,
    /// Per-chunk last-refresh, nanos since `DegreeColumn::created`. Stamped when a chunk is
    /// faulted in and re-stamped by the idle sweep for any chunk touched since the last sweep;
    /// it is *not* written on the hot lookup path (that recency lives in `referenced`). Parallel
    /// to `slots`; orders eviction candidates coldest-first.
    last_used: Vec<AtomicU64>,
    /// Per-chunk CLOCK reference bit, set on every lookup (coarsely — a no-op once already set)
    /// and cleared by the idle sweep *and* by the byte-budget evictor's first pass (each grants
    /// a set-bit chunk one second chance). Parallel to `slots`. This is the *only* write the hot
    /// lookup path makes, replacing a per-lookup clock read + timestamp store.
    referenced: Vec<AtomicBool>,
}

impl Half {
    fn new(chunks: usize) -> Self {
        Self {
            slots: RwLock::new(vec![None; chunks]),
            last_used: (0..chunks).map(|_| AtomicU64::new(0)).collect(),
            referenced: (0..chunks).map(|_| AtomicBool::new(false)).collect(),
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
    /// `Pinned` ⇒ never evict; `Lazy` ⇒ cold chunks freed by the idle sweep *and* by budget
    /// pressure (see `budget_bytes`).
    residency: DegreeResidency,
    /// Soft cap on total resident chunk bytes across both halves, enforced on **every fault**
    /// (independent of the idle-TTL sweep) by evicting the coldest resident chunks until back
    /// within budget. This is the pressure path that keeps `Lazy` bounded even when
    /// `cacheTtlMs <= 0` disables the sweep. `0` ⇒ uncapped (grows to the whole column — an
    /// explicit operator opt-out). Ignored under `Pinned`, which keeps the whole column resident.
    budget_bytes: usize,
    /// Running sum of resident chunks' `DegreeChunk::resident_bytes()` across both halves. Every
    /// mutation happens while holding the owning half's `slots` write lock, so it stays
    /// consistent with the slots (the atomic only lets the hot read path and `resident_bytes()`
    /// observe it without a lock).
    resident_bytes: AtomicUsize,
}

impl DegreeColumn {
    /// Open the column over an already-opened `node_degrees.blk` reader. Validates the record
    /// count up-front (same fail-fast as the eager `read_node_degrees`) so a malformed column
    /// refuses at generation open, not mid-query. `Pinned` prefaults every chunk here.
    ///
    /// `budget_bytes` caps the resident bytes under `Lazy` (evicting the coldest chunks on each
    /// fault to stay within it — a memory bound that holds even when the idle-TTL sweep is
    /// disabled); `0` means uncapped. It is ignored under `Pinned`. See [`DEFAULT_BUDGET_BYTES`].
    pub fn open(
        reader: BlockFileReader,
        node_count: usize,
        residency: DegreeResidency,
        budget_bytes: usize,
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
            budget_bytes,
            resident_bytes: AtomicUsize::new(0),
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
                let decoded = Arc::new(read_degree_chunk(
                    &self.reader,
                    self.per_half,
                    outgoing,
                    chunk,
                )?);
                self.resident_bytes
                    .fetch_add(decoded.resident_bytes(), Ordering::Relaxed);
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
        // Coarse CLOCK reference: set the bit (skip the store once already set, so a hot chunk's
        // cache line stays clean for concurrent readers). No wall-clock read and no shared
        // `last_used` store on the hot path — the idle sweep and the byte-budget evictor convert
        // this bit into a second chance / a `last_used` refresh.
        let refd = &half.referenced[chunk];
        if !refd.load(Ordering::Relaxed) {
            refd.store(true, Ordering::Relaxed);
        }
        decoded.degree_at(off)
    }

    /// Return chunk `c` of `half`, faulting it if cold. The fault decodes off the lock; the
    /// write lock is held only to store (a concurrent winner is kept, our copy discarded). After
    /// a fault actually stores a new chunk, the budget is re-enforced (pressure eviction).
    fn chunk(&self, half: &Half, outgoing: bool, c: usize) -> Option<Arc<DegreeChunk>> {
        if let Some(hit) = half.slots.read().unwrap()[c].clone() {
            return Some(hit);
        }
        let decoded = match read_degree_chunk(&self.reader, self.per_half, outgoing, c) {
            Ok(v) => Arc::new(v),
            Err(e) => {
                warn!(error = %e, chunk = c, outgoing, "degree-column chunk fault failed; falling back to CSR peek");
                return None;
            }
        };
        {
            let mut slots = half.slots.write().unwrap();
            // Double-check: another thread may have faulted this chunk while we decoded. If so,
            // keep the winner and discard our copy — and skip budget enforcement (the winner's
            // own fault already enforced it).
            if let Some(hit) = slots[c].clone() {
                return Some(hit);
            }
            self.resident_bytes
                .fetch_add(decoded.resident_bytes(), Ordering::Relaxed);
            slots[c] = Some(decoded.clone());
        }
        // Stamp the just-faulted chunk's fault time and mark it referenced *before* enforcing
        // the budget, so the pressure sweep never evicts the very chunk we are about to return
        // (it is also explicitly protected below — no evict-then-refault thrash, no
        // use-after-evict). A fresh chunk starts a full TTL window and a set reference bit. A
        // reader still holding a returned `Arc` after an eviction keeps its bytes alive by
        // refcount; a re-fault decodes byte-identically from the same file.
        half.last_used[c].store(self.now_nanos(), Ordering::Relaxed);
        half.referenced[c].store(true, Ordering::Relaxed);
        self.enforce_budget(outgoing, c);
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
                // CLOCK second chance: a chunk touched since the last sweep clears its reference
                // bit and refreshes `last_used` to this sweep's `now`, surviving this pass. Only
                // a chunk untouched across a whole sweep interval *and* aged past the TTL is
                // evicted.
                if half.referenced[c].swap(false, Ordering::Relaxed) {
                    half.last_used[c].store(now_nanos, Ordering::Relaxed);
                    continue;
                }
                let last = half.last_used[c].load(Ordering::Relaxed);
                if now_nanos.saturating_sub(last) > ttl_nanos {
                    if let Some(chunk) = slot.take() {
                        self.resident_bytes
                            .fetch_sub(chunk.resident_bytes(), Ordering::Relaxed);
                    }
                    freed += 1;
                }
            }
        }
        freed
    }

    /// Evict resident chunks until total resident bytes are back within `budget_bytes`, using
    /// CLOCK second-chance eviction across both halves. Called on every fault, so `Lazy` stays
    /// bounded even when the idle-TTL sweep is disabled (`cacheTtlMs <= 0`). No-op under `Pinned`
    /// or an uncapped (`0`) budget. `(protect_outgoing, protect_chunk)` names the just-faulted
    /// chunk, which is excluded from eviction — guaranteeing the fault's own chunk survives (the
    /// keep-at-least-one floor) and never thrashes.
    ///
    /// Recency is read from the CLOCK `referenced` bit, not from `last_used`: since the hot
    /// lookup path no longer stamps `last_used` (it sets the bit instead), a purely
    /// `last_used`-ordered evictor would evict an actively-read-but-old chunk while keeping a
    /// cold-but-recently-faulted one. Instead, a chunk whose bit is set earns one second chance
    /// (its bit is cleared and it is spared this call); a chunk whose bit is clear is evicted.
    /// `last_used` only orders candidates coldest-first within each phase.
    fn enforce_budget(&self, protect_outgoing: bool, protect_chunk: usize) {
        if self.budget_bytes == 0 || self.residency == DegreeResidency::Pinned {
            return;
        }
        if self.resident_bytes.load(Ordering::Relaxed) <= self.budget_bytes {
            return;
        }
        // Snapshot resident, non-protected chunks (coldest `last_used` first) off the write
        // locks. The snapshot may be raced by a concurrent fault/evict, so `evict_chunk`
        // re-checks the slot under the write lock before dropping it.
        let mut cands: Vec<(u64, bool, usize)> = Vec::new();
        for outgoing in [true, false] {
            let half = if outgoing { &self.out } else { &self.in_ };
            let slots = half.slots.read().unwrap();
            for (c, slot) in slots.iter().enumerate() {
                if slot.is_none() || (outgoing == protect_outgoing && c == protect_chunk) {
                    continue;
                }
                cands.push((half.last_used[c].load(Ordering::Relaxed), outgoing, c));
            }
        }
        cands.sort_unstable_by_key(|&(last_used, _, _)| last_used);

        // First pass (the CLOCK hand): spare chunks hit since they were faulted/last swept
        // (reference bit set — clear it and defer them), evict cold chunks (bit clear) coldest
        // first. This is what keeps a hot chunk resident under pressure and evicts a cold one,
        // even when the hot chunk's `last_used` is the older of the two.
        let mut spared: Vec<(bool, usize)> = Vec::with_capacity(cands.len());
        for &(_, outgoing, c) in &cands {
            if self.resident_bytes.load(Ordering::Relaxed) <= self.budget_bytes {
                return;
            }
            let half = if outgoing { &self.out } else { &self.in_ };
            if half.referenced[c].swap(false, Ordering::Relaxed) {
                spared.push((outgoing, c));
                continue;
            }
            self.evict_chunk(half, c);
        }

        // Second pass: evicting the cold chunks alone did not free enough, so the byte budget —
        // a hard bound that must hold even when every resident chunk is hot — forces us to
        // reclaim the spared chunks too, still coldest-first. Their bits were cleared in the
        // first pass, so no chunk gets more than one second chance: `Σ resident ≤ budget` is
        // restored and the sweep terminates regardless of concurrent re-referencing.
        for (outgoing, c) in spared {
            if self.resident_bytes.load(Ordering::Relaxed) <= self.budget_bytes {
                return;
            }
            let half = if outgoing { &self.out } else { &self.in_ };
            self.evict_chunk(half, c);
        }
    }

    /// Drop resident chunk `c` of `half` under its write lock, debiting its resident bytes.
    /// Re-checks the slot (the enforce-budget candidate snapshot is taken off-lock, so a
    /// concurrent fault/evict may have changed it) and is a no-op if the slot is already empty.
    fn evict_chunk(&self, half: &Half, c: usize) {
        let mut slots = half.slots.write().unwrap();
        if let Some(chunk) = slots[c].take() {
            self.resident_bytes
                .fetch_sub(chunk.resident_bytes(), Ordering::Relaxed);
        }
    }

    /// Total resident chunk bytes across both halves — for tests / diagnostics.
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes.load(Ordering::Relaxed)
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
    use graph_format::degree_ef::DegreeCodecOpts;
    use graph_format::nodedegree::write_node_degrees;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_degcol_{}_{}", std::process::id(), name))
    }

    fn build(n: usize) -> (std::path::PathBuf, Vec<u32>, Vec<u32>) {
        let out: Vec<u32> = (0..n as u32).map(|i| i.wrapping_mul(2654435761)).collect();
        let inn: Vec<u32> = (0..n as u32).map(|i| (i % 5) + 1).collect();
        let path = tmp(&format!("col{n}.blk"));
        write_node_degrees(&path, &out, &inn, 1 << 16, DegreeCodecOpts::default(), None).unwrap();
        (path, out, inn)
    }

    #[test]
    fn parity_across_random_ids_lazy() {
        let n = 3 * DEGREES_PER_RECORD + 123;
        let (path, out, inn) = build(n);
        let reader = BlockFileReader::open(&path).unwrap();
        // Uncapped (0): every touched chunk stays resident, so parity is exercised without
        // pressure eviction perturbing which chunks are present.
        let col = DegreeColumn::open(reader, n, DegreeResidency::Lazy, 0).unwrap();

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
        let col = DegreeColumn::open(reader, n, DegreeResidency::Lazy, 0).unwrap();
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
        // Uncapped (0): isolate the idle-TTL path from budget pressure.
        let col = DegreeColumn::open(reader, n, DegreeResidency::Lazy, 0).unwrap();

        // Fault out-chunk 0; the fault stamps last_used and sets the CLOCK reference bit.
        let id = 5usize;
        assert_eq!(col.out_degree(id as u64), Some(out[id]));
        assert_eq!(col.resident_chunks(), 1);

        // Deterministic, clock-independent sweep instants (all safely after `created`) so the
        // second sweep's `now` is strictly after the first's — the CLOCK second chance refreshes
        // `last_used` on the first sweep, so a bare `Instant::now()` pair could tie under load.
        let base = Instant::now();

        // A huge TTL right after touch ⇒ referenced ⇒ CLOCK second chance, nothing freed (but
        // the reference bit is now cleared).
        assert_eq!(
            col.evict_expired(base + Duration::from_millis(10), Duration::from_secs(3600)),
            0
        );
        assert_eq!(col.resident_chunks(), 1);

        // A later zero-TTL sweep ⇒ bit clear (not re-touched) and aged past the window ⇒ freed.
        assert_eq!(
            col.evict_expired(base + Duration::from_millis(20), Duration::ZERO),
            1
        );
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
        // A deliberately tiny budget (1 byte): `Pinned` must ignore it entirely — the whole
        // column stays prefaulted and never evicts, budget pressure or not.
        let col = DegreeColumn::open(reader, n, DegreeResidency::Pinned, 1).unwrap();
        assert_eq!(col.resident_chunks(), 2 * col.records_per_half());
        assert!(col.resident_bytes() > 0);
        // Values still correct, and eviction is a no-op under pinned.
        assert_eq!(col.out_degree(0), Some(out[0]));
        assert_eq!(col.in_degree((n - 1) as u64), Some(inn[n - 1]));
        assert_eq!(col.evict_expired(Instant::now(), Duration::ZERO), 0);
        assert_eq!(
            col.resident_chunks(),
            2 * col.records_per_half(),
            "pinned ignores the byte budget"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn budget_pressure_evicts_without_ttl_sweep() {
        // The reported condition: cacheTtlMs <= 0, so the idle-TTL sweep is never spawned and
        // `evict_expired` is never called. Pre-fix the lazy column had no other reclamation
        // path, so faulted chunks accumulated to the whole column. With the budget-pressure
        // path, faulting past the budget evicts the coldest chunks on the fault path itself.
        let n = 8 * DEGREES_PER_RECORD + 3; // 9 chunks per half
        let (path, out, _) = build(n);

        // Measure one chunk's resident footprint with an uncapped probe column.
        let probe = DegreeColumn::open(
            BlockFileReader::open(&path).unwrap(),
            n,
            DegreeResidency::Lazy,
            0,
        )
        .unwrap();
        assert_eq!(probe.out_degree(0), Some(out[0]));
        let one_chunk = probe.resident_bytes();
        assert!(
            one_chunk > 0,
            "a faulted chunk should have nonzero footprint"
        );

        // Budget with room for ~3 of these chunks.
        let budget = one_chunk * 3;
        let reader = BlockFileReader::open(&path).unwrap();
        let col = DegreeColumn::open(reader, n, DegreeResidency::Lazy, budget).unwrap();

        // Touch every out-chunk in order — WITHOUT ever calling evict_expired (sweep disabled).
        for c in 0..col.records_per_half() {
            let id = c * DEGREES_PER_RECORD;
            assert_eq!(col.out_degree(id as u64), Some(out[id]), "chunk {c}");
        }

        // Pressure kept us bounded: neither resident bytes nor chunk count grew to the whole
        // column, even though the TTL sweep never ran.
        assert!(
            col.resident_bytes() <= budget,
            "resident {} exceeded budget {} despite pressure eviction",
            col.resident_bytes(),
            budget
        );
        assert!(
            col.resident_chunks() < col.records_per_half(),
            "eviction did not happen without a TTL sweep: {} of {} chunks resident",
            col.resident_chunks(),
            col.records_per_half()
        );

        // Byte-identical after churn: read the whole out-column back (re-faulting every evicted
        // chunk) and confirm every value matches the source degrees.
        for (id, &deg) in out.iter().enumerate() {
            assert_eq!(col.out_degree(id as u64), Some(deg), "refault id {id}");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn touched_chunk_gets_a_clock_second_chance() {
        // The hot lookup path no longer stamps a wall-clock timestamp; it sets a coarse CLOCK
        // reference bit. The idle sweep clears the bit and grants a second chance, so a chunk
        // touched since the last sweep survives one eviction pass. Pre-fix (a plain last_used
        // stamp), a zero-TTL sweep evicted it on the *first* pass.
        // (Distinct `n` from the other tests so its temp file never collides under parallelism.)
        let n = 3 * DEGREES_PER_RECORD + 13;
        let (path, out, _) = build(n);
        let reader = BlockFileReader::open(&path).unwrap();
        // Uncapped (0): isolate the idle-TTL / CLOCK path from budget pressure.
        let col = DegreeColumn::open(reader, n, DegreeResidency::Lazy, 0).unwrap();

        let id = 5usize;
        assert_eq!(col.out_degree(id as u64), Some(out[id]));
        assert_eq!(col.resident_chunks(), 1);

        // Deterministic, monotonic sweep instants (the second strictly after the first, since the
        // second chance refreshes `last_used` on the first sweep).
        let base = Instant::now();

        // First zero-TTL sweep: referenced since the fault ⇒ CLOCK second chance, survives.
        assert_eq!(
            col.evict_expired(base + Duration::from_millis(10), Duration::ZERO),
            0
        );
        assert_eq!(col.resident_chunks(), 1);

        // No further touch: the next sweep finds the bit clear and evicts.
        assert_eq!(
            col.evict_expired(base + Duration::from_millis(20), Duration::ZERO),
            1
        );
        assert_eq!(col.resident_chunks(), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn budget_pressure_spares_hot_chunk_evicts_cold_by_reference_bit() {
        // The reconciliation of HIK-94 (byte-budget pressure eviction) with HIK-100 (the hot
        // path stamps a CLOCK reference bit instead of `last_used`). A naive union would keep
        // HIK-94's pure `last_used`-ordered evictor while HIK-100 stops updating `last_used` on
        // hits — so pressure eviction would pick victims by *fault time*, blind to the reference
        // bit, and could evict a hot, actively-read chunk while keeping a cold one. This test
        // constructs exactly that trap and proves the reconciled evictor avoids it: the hot
        // chunk (older `last_used`, reference bit set) survives, the cold chunk (newer
        // `last_used`, reference bit clear) is the victim.
        let n = 3 * DEGREES_PER_RECORD + 5; // 4 out-chunks
        let (path, out, _) = build(n);

        let hot = 0usize; // chunk 0
        let cold = 1usize; // chunk 1
        let trigger = 2usize; // chunk 2
        let hot_id = (hot * DEGREES_PER_RECORD) as u64;
        let cold_id = (cold * DEGREES_PER_RECORD) as u64;
        let trigger_id = (trigger * DEGREES_PER_RECORD) as u64;

        // Measure the exact resident footprint of the three chunks (their EF sizes differ). A
        // budget one byte under all three forces enforce_budget to evict exactly one chunk;
        // evicting the (smaller-or-equal) cold chunk always drops back under budget, so the
        // choice of victim — not the arithmetic — is what this test pins down.
        let probe = DegreeColumn::open(
            BlockFileReader::open(&path).unwrap(),
            n,
            DegreeResidency::Lazy,
            0,
        )
        .unwrap();
        assert_eq!(probe.out_degree(hot_id), Some(out[hot_id as usize]));
        assert_eq!(probe.out_degree(cold_id), Some(out[cold_id as usize]));
        assert_eq!(probe.out_degree(trigger_id), Some(out[trigger_id as usize]));
        let three = probe.resident_bytes();
        assert!(three > 1);
        let budget = three - 1;

        let reader = BlockFileReader::open(&path).unwrap();
        let col = DegreeColumn::open(reader, n, DegreeResidency::Lazy, budget).unwrap();

        // Deterministic, clock-independent `now`s (all safely after `created`).
        let base = Instant::now();
        let huge = Duration::from_secs(3600);

        // Fault HOT, then a huge-TTL sweep clears its bit and stamps last_used = base+10ms
        // (referenced ⇒ second chance, nothing freed). HOT is now bit-clear, last_used old.
        assert_eq!(col.out_degree(hot_id), Some(out[hot_id as usize]));
        assert_eq!(col.evict_expired(base + Duration::from_millis(10), huge), 0);

        // Fault COLD (bit set), then a second huge-TTL sweep: COLD is referenced ⇒ its bit is
        // cleared and last_used refreshed to base+20ms (newer than HOT's); HOT is *not*
        // referenced (bit already clear, not re-hit) ⇒ its last_used is left untouched (old).
        assert_eq!(col.out_degree(cold_id), Some(out[cold_id as usize]));
        assert_eq!(col.evict_expired(base + Duration::from_millis(20), huge), 0);

        // Re-hit HOT: the hot path sets its reference bit (but does NOT touch last_used, which
        // stays the *older* base+10ms). COLD keeps its clear bit and the *newer* base+20ms.
        assert_eq!(col.out_degree(hot_id), Some(out[hot_id as usize]));

        // State now: HOT = (bit set, last_used older); COLD = (bit clear, last_used newer).
        // A pure last_used LRU evictor would evict HOT (older) — the naive-merge bug.
        assert!(col.out.slots.read().unwrap()[hot].is_some());
        assert!(col.out.slots.read().unwrap()[cold].is_some());
        assert_eq!(col.resident_chunks(), 2);

        // Fault the trigger ⇒ three resident, over the two-chunk budget ⇒ enforce_budget runs.
        assert_eq!(col.out_degree(trigger_id), Some(out[trigger_id as usize]));

        // The reconciliation: HOT survives (reference bit granted it a second chance), COLD is
        // evicted (bit clear), even though COLD's last_used is newer. The trigger it protected
        // stays. Byte budget still honoured.
        assert!(
            col.out.slots.read().unwrap()[hot].is_some(),
            "the actively-hit chunk was evicted — enforce_budget ignored the reference bit"
        );
        assert!(
            col.out.slots.read().unwrap()[cold].is_none(),
            "the cold chunk survived — enforce_budget evicted by fault time, not recency"
        );
        assert!(col.out.slots.read().unwrap()[trigger].is_some());
        assert_eq!(col.resident_chunks(), 2);
        assert!(col.resident_bytes() <= budget);

        // Values remain correct (re-faulting the evicted cold chunk is byte-identical).
        assert_eq!(col.out_degree(hot_id), Some(out[hot_id as usize]));
        assert_eq!(col.out_degree(cold_id), Some(out[cold_id as usize]));
        let _ = std::fs::remove_file(&path);
    }
}
