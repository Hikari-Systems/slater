// SPDX-License-Identifier: Apache-2.0
//! The per-graph single-writer that fronts the writable layer.
//!
//! [`DeltaWriter`] owns a graph's durability floor ([`WalSink`]) and its
//! authoritative in-RAM [`Memtable`], and publishes an immutable read snapshot
//! that queries overlay through [`MergedView`](crate::read_view::MergedView). It is
//! the runtime home of the write flow the plan describes:
//!
//! ```text
//! parse → resolve business key to a core dense id (ISAM) → WAL append+commit
//!       → memtable apply → publish snapshot
//! ```
//!
//! # One writer, many readers
//! Every mutation is serialised behind [`DeltaWriter::inner`] (a `Mutex`), so the
//! memtable has exactly one writer — matching the discipline
//! [`slater_delta::memtable`] is built for. Readers never touch that lock: they
//! take a cheap `Arc<Memtable>` clone from the published `RwLock<Arc<Memtable>>`
//! guard (the same shape the generation guard uses for `Arc<Generation>`), so a
//! query pins a consistent delta for its whole life and the writer can move on.
//!
//! # Durability = the commit barrier
//! [`DeltaWriter::write`] returns only after [`WalSink::commit`] has fsynced the
//! record, so a `Seq` handed back to the caller is durable: the Bolt `SUCCESS` the
//! server sends afterwards therefore implies durability (D44). Phase 1c commits one
//! record per call; group-commit batching (many records, one fsync) is a later
//! throughput optimisation the segment format already supports.
//!
//! # Core-generation binding
//! The resolved dense-id read index ([`Memtable::by_dense`]) is only valid against
//! the *core generation the writes were resolved against* — dense ids are permuted
//! by every rebuild. [`DeltaWriter::core_uuid`] records that generation so the
//! server can fail safe (serve the pure core) rather than overlay a delta onto a
//! generation whose dense ids no longer line up. Re-resolving a live delta across a
//! hot-reload swap is out of scope for Phase 1c — consolidation (Phase 1d) is the
//! sanctioned path that folds the delta into a fresh core.
//!
//! # A panic under the writer lock must not end the graph
//! The writer lock is held across code that *can* panic — [`Memtable::apply`], the L0
//! encoders/mergers, and (in [`DeltaWriter::retire`]) a **caller-supplied** `resolve`
//! closure. A `std` lock poisons on a panic-while-held, so an `.expect()` at every
//! acquisition would turn one panic into a permanent, per-graph write outage: every
//! later write, flush, compaction and republish would panic on the poisoned lock until
//! the process restarted (see the `compact_l0` `unreachable!` that shipped as a real
//! outage). Two invariants remove that failure mode, and both are load-bearing:
//!
//! 1. **Every mutating critical section is panic-*atomic*.** Each is written as
//!    *prepare* — all fallible and panic-prone work on **locals**, touching nothing the
//!    lock protects — followed by *install*, a run of moves (assignments,
//!    `Vec::insert`/`splice`/`clear`) which cannot unwind. A panic therefore always
//!    leaves [`WriterInner`] exactly as it was before the call, and the published
//!    snapshot (only ever *assigned* an already-built value) with it. Keep new code in
//!    that shape: nothing that can panic may run between the first and last write to
//!    `WriterInner`.
//! 2. **Acquisition never panics.** Because (1) makes a poison flag carry no
//!    information — the state behind it is intact by construction — [`DeltaWriter`]
//!    takes its guards through [`lock_writer`]/[`read_lock`]/[`write_lock`], which
//!    recover the guard from a `PoisonError` and clear the flag.
//!
//! The panic itself still unwinds, loudly, to whoever asked for the write: the one
//! query (or one background flush task) fails and is logged, and the graph keeps
//! serving reads *and* writes. That is the trade — a broken invariant surfaces as a
//! failed op rather than a process abort — and it is sound only while (1) holds.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::{Context, Result};
use graph_format::blockcache::BlockCache as GfBlockCache;
use graph_format::ids::{Generation as GenId, Value};
use slater_delta::l0_offheap::{merge_run, write_segment};
use slater_delta::{
    replay_dir, DeltaSnapshot, L0Reader, L0Segment, LevelRead, Memtable, OpResolution, Seq, WalOp,
    WalRecord, WalSink,
};

/// Block target size + zstd level for an off-heap L0 segment's payload sections. Small
/// blocks (as for range indexes) keep a cold point read's one-time decode cheap; a delta
/// is small relative to the core anyway.
const OFFHEAP_L0_BLOCK_BYTES: usize = 16 * 1024;
const OFFHEAP_L0_ZSTD_LEVEL: i32 = 3;

/// A sealed L0 level held by the writer — either **resident** (the whole flushed memtable
/// in RAM) or **off-heap** (a directory of block files whose payloads page through the
/// shared [`BlockCache`](GfBlockCache); only a compact index is resident). The writer
/// dispatches over this so reads, publish, freeze and retire are format-agnostic; the one
/// consumer that needs a resident memtable — L0→L0 compaction — is skipped in the off-heap
/// mode (consolidation bounds the level count instead). See D54.
enum L0Level {
    Resident(L0Segment),
    OffHeap {
        reader: Arc<L0Reader>,
        /// The segment **directory** (retire deletes it; compaction is off here).
        dir: PathBuf,
        /// On-disk size — a conservative stand-in for the resident footprint in the
        /// total-delta accounting (the off-heap resident share is far smaller).
        bytes: u64,
    },
}

impl L0Level {
    /// This level as a `dyn LevelRead` for a transient read (bases, born-resolve) — a
    /// borrow, so no `Arc` clone.
    fn as_level(&self) -> &dyn LevelRead {
        match self {
            L0Level::Resident(s) => s.memtable().as_ref(),
            L0Level::OffHeap { reader, .. } => reader.as_ref(),
        }
    }

    /// An owned `Arc<dyn LevelRead>` for the published [`DeltaSnapshot`] / freeze.
    fn level_arc(&self) -> Arc<dyn LevelRead> {
        match self {
            L0Level::Resident(s) => s.memtable().clone() as Arc<dyn LevelRead>,
            L0Level::OffHeap { reader, .. } => reader.clone() as Arc<dyn LevelRead>,
        }
    }

    /// The on-disk path retire deletes (a file for resident, a directory for off-heap).
    fn path(&self) -> &Path {
        match self {
            L0Level::Resident(s) => s.path(),
            L0Level::OffHeap { dir, .. } => dir,
        }
    }

    /// Approximate resident/total-accounting size in bytes.
    fn bytes(&self) -> u64 {
        match self {
            L0Level::Resident(s) => s.memtable().bytes() as u64,
            L0Level::OffHeap { bytes, .. } => *bytes,
        }
    }

    /// The resident memtable behind a [`L0Level::Resident`] — for resident L0→L0
    /// compaction (`merge_levels`); `None` for an off-heap level. A stack can hold both
    /// formats at once (the flush flag changed between runs; segments reload in the format
    /// they were written), so this is fallible rather than a panic.
    fn resident_memtable(&self) -> Option<&Arc<Memtable>> {
        match self {
            L0Level::Resident(s) => Some(s.memtable()),
            L0Level::OffHeap { .. } => None,
        }
    }

    /// The off-heap reader behind a [`L0Level::OffHeap`] — for the disk-native streaming
    /// compaction ([`merge_run`]); `None` for a resident level.
    fn offheap_reader(&self) -> Option<Arc<L0Reader>> {
        match self {
            L0Level::OffHeap { reader, .. } => Some(reader.clone()),
            L0Level::Resident(_) => None,
        }
    }

    /// Whether this level is an off-heap (directory) segment — the key compaction groups
    /// a run by, since a run merges in **its own** format, not the writer's current flag.
    fn is_off_heap(&self) -> bool {
        matches!(self, L0Level::OffHeap { .. })
    }
}

/// A frozen delta handed to consolidation: an immutable snapshot of every
/// committed write (the active memtable **and** every sealed L0 level), plus the
/// on-disk segments those writes live in. The snapshot feeds the merged-view dump;
/// the segments are deleted by [`DeltaWriter::retire`] once the fresh generation is
/// published (see Phase 1d / 4c in `docs/WRITABLE-PROGRESS.md`).
pub struct Frozen {
    /// The active memtable at freeze time — the newest delta level the dump folds in.
    pub snapshot: Arc<Memtable>,
    /// The sealed L0 levels at freeze time, **newest first** — the dump folds these
    /// beneath the active memtable via [`DeltaSnapshot::with_levels`]. Each is a
    /// `dyn LevelRead` (resident or off-heap), read through the merged view.
    pub l0: Vec<Arc<dyn LevelRead>>,
    /// Every committed WAL segment the frozen delta represents. `retire` removes
    /// exactly these; any segment opened after the freeze (post-freeze writes) is
    /// left untouched.
    pub consumed: Vec<PathBuf>,
    /// The L0 segment files the frozen delta represents — folded into the new core by
    /// the consolidation, so `retire` deletes exactly these once the swap is live.
    pub consumed_l0: Vec<PathBuf>,
}

/// Serialised writer state — reached only under the `Mutex`, never by readers.
struct WriterInner {
    dir: PathBuf,
    /// The segment currently open for appends.
    sink: WalSink,
    /// The authoritative active memtable, shared by `Arc` with the published snapshot's
    /// newest level.
    ///
    /// **Never mutated in place — only replaced wholesale.** A write clones it, applies
    /// the whole batch to that private copy, and installs the result with one move
    /// (copy-on-write), so a panic inside [`Memtable::apply`] cannot leave a half-applied
    /// batch behind (the module-level panic-atomicity invariant). Sharing the `Arc` with
    /// the published snapshot keeps the cost at exactly one deep clone per batch — the
    /// clone `published_snapshot` used to make anyway — and makes `freeze` free.
    mem: Arc<Memtable>,
    /// The sealed L0 levels beneath the active memtable, **newest first** (empty on the
    /// common no-flush path). Each flush prepends one; consolidation clears them. A level
    /// is resident or off-heap per [`WriterInner::off_heap`].
    l0: Vec<L0Level>,
    /// The last sequence number assigned (0 before the first write).
    seq: Seq,
    /// Read (and write) sealed L0 levels off-heap through [`WriterInner::block_cache`]
    /// (Phase C / D54). Set from `delta.offHeapL0` at open; when false everything is the
    /// resident single-file path exactly as before.
    off_heap: bool,
    /// The server's shared block cache off-heap L0 segments page through. `None` on the
    /// resident path and for non-server openers.
    block_cache: Option<Arc<GfBlockCache>>,
}

/// The per-graph writable-layer writer. Cheap to clone-share behind an `Arc`.
pub struct DeltaWriter {
    graph: String,
    /// The core generation the delta's dense ids were resolved against. Interior
    /// mutable because [`DeltaWriter::retire`] re-binds it to the freshly
    /// consolidated generation without reopening the writer.
    core_uuid: RwLock<GenId>,
    /// Single-writer serialisation of every mutation.
    inner: Mutex<WriterInner>,
    /// The published immutable read snapshot: the active memtable **and** the sealed L0
    /// levels, folded atomically (readers clone the whole [`DeltaSnapshot`], so a flush
    /// that moves data from the memtable into a new L0 level can never split a read's
    /// view — see [`Self::republish`]).
    published: RwLock<DeltaSnapshot>,
    /// Bumps on every published change; folded into the result-cache key so an
    /// overlaid result is invalidated by the next write (see `server::result_key`).
    epoch: AtomicU64,
    /// Set for the whole duration of a consolidation (freeze → build → swap →
    /// retire). While set, [`Self::flush_to_l0`]/[`Self::compact_l0`] are no-ops: a
    /// flush/compaction between freeze and retire would add an L0 segment that retire
    /// (which clears the whole stack) would then drop, losing its writes. Writes
    /// themselves continue landing in the memtable + WAL and survive the build (Phase
    /// 4a); the memtable simply grows until retire, bounded by the 4d-ii hard cap.
    consolidating: AtomicBool,
}

/// Take the writer mutex, **recovering** it if a previous holder panicked.
///
/// A poisoned lock here says only "someone panicked while holding this" — it says nothing
/// about the state behind it, and by the module's panic-atomicity invariant that state is
/// the pre-panic one, intact. Propagating the poison (`.expect()`) would therefore convert
/// a single panicking op into a permanent per-graph write outage for no safety gain; so the
/// guard is recovered with [`PoisonError::into_inner`] and the flag cleared, and the next
/// write proceeds against the state the panicking one never managed to change.
fn lock_writer(m: &Mutex<WriterInner>) -> MutexGuard<'_, WriterInner> {
    let guard = m.lock().unwrap_or_else(PoisonError::into_inner);
    m.clear_poison();
    guard
}

/// Take a read guard, recovering from a poisoned `RwLock` — see [`lock_writer`]. (An
/// `RwLock` only poisons on a panic under its *write* guard; the writer's write sections
/// are single moves, so this is belt-and-braces — but no acquisition in this file may be a
/// panic site.)
fn read_lock<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    let guard = l.read().unwrap_or_else(PoisonError::into_inner);
    l.clear_poison();
    guard
}

/// Take a write guard, recovering from a poisoned `RwLock` — see [`read_lock`].
fn write_lock<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    let guard = l.write().unwrap_or_else(PoisonError::into_inner);
    l.clear_poison();
    guard
}

impl DeltaWriter {
    /// Open (or re-open) the writer for `graph`, whose WAL segments live under
    /// `dir`. Replays every committed record into the authoritative memtable —
    /// resolving each business key to its current-core dense id via `resolve`
    /// (`None` for a key absent from the core, i.e. a delta-born node) — then opens
    /// a *fresh* segment after the highest existing one so no committed segment is
    /// ever truncated. `core_uuid` is the generation the dense ids resolve against.
    pub fn open(
        dir: impl AsRef<Path>,
        graph: &str,
        core_uuid: GenId,
        core_node_count: u64,
        core_edge_count: u64,
        resolve: impl Fn(&WalOp) -> OpResolution,
    ) -> Result<Self> {
        Self::open_with_cache(
            dir,
            graph,
            core_uuid,
            core_node_count,
            core_edge_count,
            false,
            None,
            resolve,
        )
    }

    /// [`Self::open`] with the off-heap-L0 knob and the shared block cache (Phase C /
    /// D54). `off_heap` controls only what a *flush* writes; existing sealed segments are
    /// reloaded in whichever format they were written (a directory = off-heap, a file =
    /// resident), so the two never require the flag to agree with the disk. An off-heap
    /// segment needs `cache`; finding one with `cache == None` is a clear error.
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_cache(
        dir: impl AsRef<Path>,
        graph: &str,
        core_uuid: GenId,
        core_node_count: u64,
        core_edge_count: u64,
        off_heap: bool,
        cache: Option<Arc<GfBlockCache>>,
        resolve: impl Fn(&WalOp) -> OpResolution,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();

        // Reload the sealed L0 segments first (Phase 4c). They stack past the core: the
        // active memtable resumes past every level's synthetic id space, so a WAL-tail
        // born node never collides with an already-flushed one. `l0` ends **newest
        // first** (the read-stack order); the bases are the max over levels (= the
        // newest level's `base + born`, since bases stack monotonically).
        let l0_dir = dir.join("l0");
        let mut l0: Vec<L0Level> = Vec::new();
        let mut node_base = core_node_count;
        let mut edge_base = core_edge_count;
        for (_, path) in l0_segment_paths_sorted(&l0_dir)? {
            let level = open_l0_level(&path, cache.as_ref())
                .with_context(|| format!("reload L0 segment {path:?}"))?;
            let lv = level.as_level();
            node_base = node_base.max(lv.synthetic_base() + lv.born_count());
            edge_base = edge_base.max(lv.edge_synthetic_base() + lv.born_edge_count());
            l0.push(level);
        }
        l0.reverse(); // ascending on disk (oldest→newest) → newest-first read order

        // Replay the live WAL tail (writes since the last flush rotated the WAL) into a
        // fresh active memtable rebased past all L0 levels. A born key that is Absent from
        // the core is resolved against the L0 levels first (Phase 4c-B) so a re-`MERGE` of
        // an already-flushed born node reuses its synthetic id rather than duplicating it.
        let mut mem = Memtable::with_bases(node_base, edge_base);
        let replay = replay_dir(&dir).with_context(|| format!("replay WAL dir {dir:?}"))?;
        for rec in &replay.records {
            let res = resolve_with_l0(&rec.op, resolve(&rec.op), &l0);
            mem.apply(&rec.op, res);
        }

        let next_segment = next_segment_number(&dir)?;
        let sink = WalSink::create(&dir, next_segment)
            .with_context(|| format!("open WAL segment {next_segment} under {dir:?}"))?;

        let mem = Arc::new(mem);
        let published = published_snapshot(&mem, &l0);
        Ok(Self {
            graph: graph.to_string(),
            core_uuid: RwLock::new(core_uuid),
            inner: Mutex::new(WriterInner {
                dir,
                sink,
                mem,
                l0,
                seq: replay.last_seq,
                off_heap,
                block_cache: cache,
            }),
            published: RwLock::new(published),
            epoch: AtomicU64::new(1),
            consolidating: AtomicBool::new(false),
        })
    }

    /// The graph this writer serves.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Claim the exclusive right to consolidate this graph, returning `false` if a
    /// consolidation is already running (the caller must not proceed). Held until
    /// [`Self::end_consolidation`]; while held, auto flush/compaction is suppressed so
    /// nothing mutates the L0 stack across the freeze→retire window (Phase 4d-ii).
    pub fn begin_consolidation(&self) -> bool {
        self.consolidating
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Release the consolidation claim taken by [`Self::begin_consolidation`].
    pub fn end_consolidation(&self) {
        self.consolidating.store(false, Ordering::Release);
    }

    /// Whether a consolidation is currently in flight for this graph.
    pub fn is_consolidating(&self) -> bool {
        self.consolidating.load(Ordering::Acquire)
    }

    /// The core generation the delta's dense ids were resolved against. The server
    /// overlays this delta only on a generation with this UUID.
    pub fn core_uuid(&self) -> GenId {
        *read_lock(&self.core_uuid)
    }

    /// A consistent immutable snapshot of the **active memtable** — one `Arc` clone, no
    /// writer contention. Used by the writer's single-memtable diagnostics and tests; a
    /// read overlay wants [`Self::delta_snapshot`] (which also carries the L0 levels).
    pub fn snapshot(&self) -> Arc<Memtable> {
        read_lock(&self.published).active_memtable().clone()
    }

    /// The full published delta — the active memtable **and** every sealed L0 level,
    /// folded atomically. A query pins this for its whole life; the read overlay
    /// (`server::delta_for_read`) builds its `MergedView` from it.
    pub fn delta_snapshot(&self) -> DeltaSnapshot {
        read_lock(&self.published).clone()
    }

    /// The synthetic dense id of a delta-born node with this business identity that is
    /// resident in a **sealed L0 level** — the write path's Phase 4c-B born-resolution
    /// hook. A re-`MERGE` of a node already flushed to L0 must reuse this id rather than
    /// allocate a duplicate; the active memtable resolves its own born nodes through
    /// [`Memtable::upsert_node`] idempotency, so only the L0 levels are consulted here.
    pub fn born_synthetic_for_identity(
        &self,
        label: &str,
        key: &str,
        value: &Value,
    ) -> Option<u64> {
        read_lock(&self.published)
            .l0_levels()
            .iter()
            .find_map(|m| m.born_synthetic_for_identity(label, key, value))
    }

    /// The synthetic dense id of a delta-born node with this business identity, resolved
    /// across the **whole** delta (active memtable + every L0 level) — the DELETE write
    /// path's born-resolution hook. Unlike [`Self::born_synthetic_for_identity`] (L0
    /// only, for MERGE create reuse), this also consults the active memtable, so a born
    /// node deleted before it is ever flushed is found; and returning the id lets
    /// `execute_write` plant the tombstone's `by_dense` mapping, suppressing a node
    /// already flushed to an L0 level on read.
    pub fn born_synthetic_in_delta(&self, label: &str, key: &str, value: &Value) -> Option<u64> {
        read_lock(&self.published).born_synthetic_for_identity(label, key, value)
    }

    /// Publish `mem ⊕ l0` as one atomic [`DeltaSnapshot`], so a lock-free reader never
    /// observes a half-applied flush (data in neither or both of the memtable and a new
    /// L0 level). Called under the writer lock after every state change.
    ///
    /// Part of every caller's *install* phase: the snapshot is fully built before the
    /// guard is taken, and the guard only ever sees a move-assign, so publishing cannot
    /// panic (and cannot publish a half-built delta).
    fn republish(&self, inner: &WriterInner) {
        let published = published_snapshot(&inner.mem, &inner.l0);
        *write_lock(&self.published) = published;
    }

    /// The current delta epoch (monotonic; bumps on every published write).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Durably apply one write: fold it into a private copy of the authoritative memtable,
    /// append the record, commit (fsync — the ack barrier), then install the new memtable
    /// and publish the new snapshot. Returns the durable sequence number. `resolved` is the
    /// caller's resolved dense-id context ([`OpResolution`]) — a `None` endpoint marks a
    /// delta-born node/edge (Phase 2/3). Same three-phase shape (and the same failure and
    /// panic atomicity) as [`Self::write_batch`], for a single op.
    pub fn write(&self, op: WalOp, resolved: OpResolution) -> Result<Seq> {
        let mut inner = lock_writer(&self.inner);

        // --- prepare: fold into a private copy; `inner` is untouched, so a panic in
        //     `apply` leaves the writer exactly as it was.
        let mut mem = (*inner.mem).clone();
        mem.apply(&op, resolved);

        // --- durably commit the record (the ack barrier). On failure the copy is dropped:
        //     nothing applied, nothing published.
        let seq = inner.seq.next();
        let rec = WalRecord { seq, op };
        inner.sink.append(&rec).context("append WAL record")?;
        inner.sink.commit(seq).context("commit WAL batch")?;
        inner.seq = seq;

        // --- install: moves only. Publish the new delta (active memtable ⊕ unchanged L0
        //     levels), then bump the epoch so readers keying on it see the new state
        //     (publish-before-bump: an observer that reads the higher epoch also sees the
        //     swapped-in snapshot).
        inner.mem = Arc::new(mem);
        self.republish(&inner);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(seq)
    }

    /// Durably apply a **batch** of writes under a single group commit — the fix for the
    /// one-fsync-per-statement cost that dominates bulk-write throughput. It folds every op
    /// into the memtable, appends every record, then does **one** `commit` (a single fsync —
    /// the ack barrier for the whole batch) and **one** publish + epoch bump. Returns the
    /// durable sequence number of the last op (the whole batch is durable once this returns).
    ///
    /// **Atomic on failure *and* on panic.** The batch is folded into a **private copy** of
    /// the memtable, which is installed only once every record is durable — so if any append
    /// or the commit fails, no op is applied or published (and the un-committed records are
    /// dropped on replay: no commit marker), and if [`Memtable::apply`] *panics* the WAL has
    /// not been touched either. Either way the batch is rejected whole, never half-applied,
    /// and the writer is left usable (module invariant 1). An empty batch is a no-op.
    ///
    /// The copy is not an extra cost: `published_snapshot` deep-cloned the memtable on every
    /// write in any case, and this copy *is* the one it publishes.
    pub fn write_batch(&self, ops: &[(WalOp, OpResolution)]) -> Result<Seq> {
        let mut inner = lock_writer(&self.inner);
        if ops.is_empty() {
            return Ok(inner.seq);
        }
        // 1. Prepare: fold every op into a private copy of the authoritative memtable.
        let mut mem = (*inner.mem).clone();
        for (op, resolved) in ops {
            mem.apply(op, *resolved);
        }
        // 2. Append every record (fast; no fsync yet).
        let mut last = inner.seq;
        for (op, _) in ops {
            let seq = inner.seq.next();
            let rec = WalRecord {
                seq,
                op: op.clone(),
            };
            inner.sink.append(&rec).context("append WAL record")?;
            inner.seq = seq;
            last = seq;
        }
        // 3. One commit = one fsync for the whole batch (the ack barrier).
        inner.sink.commit(last).context("commit WAL batch")?;
        // 4. Install (moves only) + one publish + epoch bump for the batch
        //    (publish-before-bump, as in `write`).
        inner.mem = Arc::new(mem);
        self.republish(&inner);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(last)
    }

    /// Begin a consolidation: seal the WAL segment currently open for appends, open
    /// a fresh one for any subsequent write, and hand back an immutable snapshot of
    /// the committed delta together with the segments that snapshot lives in.
    ///
    /// Freeze is *non-destructive* — the authoritative memtable and the published
    /// read snapshot are untouched, so reads keep overlaying the delta and a
    /// consolidation that fails (or crashes) before publishing loses nothing: the
    /// sealed segments are still on disk and replay the writes. [`Self::retire`] is
    /// the only step that discards them, and only after the fresh generation is live.
    ///
    /// Phase 1 runs consolidation on the single-writer path (the caller does not
    /// admit concurrent writes during the build), so the fresh segment stays empty
    /// until `retire`; the freeze-to-a-live-memtable "writes never block" behaviour
    /// is Phase 4 admission control.
    pub fn freeze(&self) -> Result<Frozen> {
        let mut inner = lock_writer(&self.inner);
        // Everything committed so far — the active memtable, the sealed L0 levels, and
        // the WAL segment about to be sealed — is the frozen delta. Capture the paths
        // before opening the fresh segment so the fresh one is never in the consumed set.
        let consumed = wal_segment_paths(&inner.dir)?;
        let consumed_l0: Vec<PathBuf> = inner.l0.iter().map(|s| s.path().to_path_buf()).collect();
        let next = next_segment_number(&inner.dir)?;
        let fresh = WalSink::create(&inner.dir, next).with_context(|| {
            format!("open post-freeze WAL segment {next} under {:?}", inner.dir)
        })?;
        let old = std::mem::replace(&mut inner.sink, fresh);
        old.seal().context("seal WAL segment at freeze")?;
        // The memtable is shared, never mutated in place (the write path replaces it
        // wholesale), so the frozen snapshot is a plain `Arc` clone — no deep copy.
        let snapshot = Arc::clone(&inner.mem);
        let l0: Vec<Arc<dyn LevelRead>> = inner.l0.iter().map(|s| s.level_arc()).collect();
        Ok(Frozen {
            snapshot,
            l0,
            consumed,
            consumed_l0,
        })
    }

    /// Flush the active memtable to a new immutable **L0 segment** on disk, bounding
    /// resident delta size without a full core rebuild (Phase 4c-B). A no-op (returns
    /// `false`) when the memtable is empty.
    ///
    /// Under the writer lock: seal the memtable to `<wal_dir>/l0/<n>.l0` (fsync-durable),
    /// prepend it to the L0 read stack, reset the active memtable **rebased past every
    /// level** (both the node and edge synthetic id spaces), seal + rotate the WAL, and
    /// delete the pre-flush WAL segments (their writes now live in the durable L0 file).
    /// The new levels are published atomically, so a concurrent reader never sees the
    /// flushed data in neither or both of the memtable and the new L0 level.
    ///
    /// All of the panic-prone work (the L0 encode) runs *before* the first write to
    /// `WriterInner`, so a panic in the encoder leaves the writer untouched and usable —
    /// see the module's panic-atomicity invariant. Keep it that way.
    pub fn flush_to_l0(&self) -> Result<bool> {
        let mut inner = lock_writer(&self.inner);
        // Checked under the lock so it serialises with freeze/retire: a consolidation
        // must not have an L0 segment appear between its freeze and its retire.
        if self.consolidating.load(Ordering::Acquire) {
            return Ok(false);
        }
        if inner.mem.is_empty() {
            return Ok(false);
        }

        // 1. Seal the active memtable to a fresh, content-checked L0 segment (fsync-durable) —
        //    a resident single file, or (off-heap) a directory of block files.
        let l0_dir = inner.dir.join("l0");
        std::fs::create_dir_all(&l0_dir)
            .with_context(|| format!("create L0 directory {l0_dir:?}"))?;
        let n = next_l0_number(&l0_dir)?;
        let path = l0_dir.join(format!("{n:010}.l0"));
        let level = if inner.off_heap {
            let cache = inner
                .block_cache
                .clone()
                .context("off-heap L0 flush requires a block cache")?;
            let data = inner.mem.to_segment_data();
            write_segment(
                &data,
                &path,
                new_segment_scope(),
                OFFHEAP_L0_BLOCK_BYTES,
                OFFHEAP_L0_ZSTD_LEVEL,
            )
            .with_context(|| format!("write off-heap L0 segment {path:?}"))?;
            open_l0_level(&path, Some(&cache))
                .with_context(|| format!("reopen off-heap L0 segment {path:?}"))?
        } else {
            L0Segment::write(&inner.mem, &path)
                .with_context(|| format!("write L0 segment {path:?}"))?;
            L0Level::Resident(
                L0Segment::open(&path).with_context(|| format!("reopen L0 segment {path:?}"))?,
            )
        };

        // 2. Rebase the active memtable past every level (the flushed one is the newest
        //    L0, so the next born id starts at its base + its born count).
        let node_base = inner.mem.synthetic_base() + inner.mem.born_count();
        let edge_base = inner.mem.edge_synthetic_base() + inner.mem.born_edge_count();

        // 3. Rotate the WAL: the flushed writes now live in the L0 file, so seal the
        //    current segment and open a fresh one before deleting the consumed segments.
        let consumed = wal_segment_paths(&inner.dir)?;
        let next = next_segment_number(&inner.dir)?;
        let fresh = WalSink::create(&inner.dir, next)
            .with_context(|| format!("open post-flush WAL segment {next} under {:?}", inner.dir))?;
        let old = std::mem::replace(&mut inner.sink, fresh);
        old.seal().context("seal WAL segment at flush")?;

        inner.mem = Arc::new(Memtable::with_bases(node_base, edge_base));
        inner.l0.insert(0, level); // newest-first

        // 4. The flushed writes are durable in the L0 file (fsynced above), so the
        //    pre-flush WAL segments can go.
        for p in &consumed {
            remove_if_present(p)?;
        }

        // 5. Publish the reset memtable + grown L0 stack atomically.
        self.republish(&inner);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(true)
    }

    /// Compact a **size-tier** of the sealed L0 stack into one merged segment (Phase
    /// 4d-i) — the cheap, O(delta), no-core-rebuild tier that lets the layer sustain
    /// write volume. Rather than merge *all* levels (the first-cut policy), it selects a
    /// contiguous run of similar-sized levels ([`select_compaction_run`]) and merges only
    /// that run (reclaiming overwritten patches + shadowed tombstones and collapsing read
    /// fan-out within the tier), leaving differently-sized levels alone so a large level is
    /// never repeatedly rewritten with tiny new ones (write amplification). The merge runs
    /// in the run's own format: **resident** levels fold in RAM via
    /// [`Memtable::merge_levels`]; **off-heap** levels are merged by
    /// [`merge_run`](slater_delta::l0_offheap::merge_run), a disk-native streaming k-way
    /// merge over the sorted on-disk sections that never holds the merged payloads resident
    /// (RSS bounded to a block window — see D54). A no-op (returns `false`) with fewer than
    /// two levels, or
    /// when no two adjacent levels are same-tier (a healthy size ladder). This is
    /// self-balancing: equal-sized flushes form same-tier runs that merge, and the merged
    /// results are themselves same-tier and merge in turn, so fan-out stays bounded.
    ///
    /// **Mixed-format stacks.** The stack can hold both formats at once: `delta.offHeapL0`
    /// only decides what a *flush* writes, while sealed segments reload in whichever format
    /// they were written, so flipping the flag leaves off-heap directory levels under (or
    /// over) resident file levels. The run is therefore selected within a *contiguous
    /// same-format span* ([`select_compaction_run_in_stack`]) and merged in **that run's**
    /// format — never in whatever the flag currently says. Each tier keeps compacting
    /// across a flag flip, and no format ever reaches the other's merge path.
    ///
    /// **Number-vs-stack-order reconciliation.** Merging a *partial* run leaves several
    /// L0 files, so their on-disk numbers must still agree with age/born-id-base order
    /// (`open` sorts by number). The merged segment therefore **reuses the run's oldest
    /// (minimum) file number** — the oldest run member's slot, whose file number and
    /// born-id base are both the run's minimum, which is exactly the merged segment's base
    /// (`merge_levels` keeps the oldest level's base). Reusing that slot keeps number
    /// order == base order with no change to `open`. The active memtable and core are
    /// untouched, so every born id (and any dense id already handed to a reader) stays
    /// valid.
    ///
    /// Crash posture matches the first-cut merge-all policy: publish-before-delete
    /// protects live readers, but a crash between writing the merged file and deleting the
    /// run's newer members would leave both on disk (a redundant born-id range) until the
    /// next compaction — a pre-existing limitation, not worsened here.
    ///
    /// The whole merge (the panic-prone part — this is where the `unreachable!` that took a
    /// graph's write path down with it used to live) runs before the stack is spliced, so a
    /// panic in it leaves `WriterInner` untouched and the writer usable. Keep it that way.
    pub fn compact_l0(&self) -> Result<bool> {
        let mut inner = lock_writer(&self.inner);
        // As with `flush_to_l0`: never mutate the L0 stack during a consolidation.
        if self.consolidating.load(Ordering::Acquire) {
            return Ok(false);
        }
        if inner.l0.len() < 2 {
            return Ok(false);
        }

        // Pick a contiguous run of similar-sized levels **of one format** (over the
        // newest-first stack): a mixed stack still compacts, each format tier on its own.
        let sizes: Vec<u64> = inner.l0.iter().map(|s| s.bytes()).collect();
        let formats: Vec<bool> = inner.l0.iter().map(|s| s.is_off_heap()).collect();
        let Some((start, end)) = select_compaction_run_in_stack(&sizes, &formats) else {
            return Ok(false); // a healthy size ladder — nothing same-tier to merge
        };

        // Reuse the run's OLDEST slot (`inner.l0[end - 1]`): its number and born-id base are
        // the run's minimum, matching the merged segment's base, so on-disk number order
        // stays == age/base order without reconciling in `open`. The run's newer members are
        // deleted after publishing.
        let oldest_path = inner.l0[end - 1].path().to_path_buf();
        let consumed: Vec<PathBuf> = inner.l0[start..end - 1]
            .iter()
            .map(|s| s.path().to_path_buf())
            .collect();

        // Merge the run (newest-first) into one equivalent segment, in **the run's own**
        // format (every member shares it — the selection groups by format, and the reused
        // oldest slot is that format's on-disk shape): off-heap **streams** the sorted
        // on-disk runs (RSS bounded to a block window, D54), resident folds in RAM via
        // `merge_levels`.
        let merged_level = if formats[start] {
            let cache = inner
                .block_cache
                .clone()
                .context("off-heap L0 compaction requires a block cache")?;
            let Some(run) = inner.l0[start..end]
                .iter()
                .map(|s| s.offheap_reader())
                .collect::<Option<Vec<Arc<L0Reader>>>>()
            else {
                return Ok(false); // unreachable by construction — never merge cross-format
            };
            merge_run(
                &run,
                &oldest_path,
                new_segment_scope(),
                OFFHEAP_L0_BLOCK_BYTES,
                OFFHEAP_L0_ZSTD_LEVEL,
            )
            .with_context(|| format!("merge off-heap L0 run into {oldest_path:?}"))?;
            open_l0_level(&oldest_path, Some(&cache))
                .with_context(|| format!("reopen merged off-heap L0 segment {oldest_path:?}"))?
        } else {
            let Some(run) = inner.l0[start..end]
                .iter()
                .map(|s| s.resident_memtable().map(Arc::as_ref))
                .collect::<Option<Vec<&Memtable>>>()
            else {
                return Ok(false); // unreachable by construction — never merge cross-format
            };
            let merged = Memtable::merge_levels(&run);
            L0Segment::write(&merged, &oldest_path)
                .with_context(|| format!("write compacted L0 segment {oldest_path:?}"))?;
            L0Level::Resident(
                L0Segment::open(&oldest_path)
                    .with_context(|| format!("reopen compacted L0 segment {oldest_path:?}"))?,
            )
        };

        // Splice the run out, in place, for the single merged segment (stack stays
        // newest-first + number-ordered).
        inner.l0.splice(start..end, std::iter::once(merged_level));

        // Publish the collapsed stack, then delete the consumed files (durable in the
        // merged segment) — publish-before-delete so a reader never loses the data.
        self.republish(&inner);
        for p in &consumed {
            remove_if_present(p)?;
        }
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(true)
    }

    /// Complete a consolidation: delete the `consumed` (pre-freeze) WAL segments —
    /// their writes now live in the freshly built core — then **rebuild** the live
    /// memtable by replaying the surviving *post-freeze* segments against the new core
    /// (re-based on `new_core_node_count`/`new_core_edge_count` so delta-born ids start
    /// past the new core), and re-bind the writer to `new_core_uuid` so subsequent
    /// writes resolve their business keys against the new generation.
    ///
    /// # Post-freeze writes are carried forward (Phase 4a)
    /// A write that arrives between [`Self::freeze`] and this call lands in the fresh
    /// segment freeze opened — which is **not** in `consumed`. Rather than discard it
    /// (the Phase 1 behaviour, safe only because it forbade concurrent writes during a
    /// build), retire re-applies it: it deletes the consumed set, then replays every
    /// remaining segment through `resolve` (each committed record is durable — `commit`
    /// fsyncs — so the still-open segment's tail replays fine). `resolve` is bound to the
    /// *new* core, so each post-freeze business key re-resolves against the freshly built
    /// generation — a pre-freeze delta-born node (a synthetic id) that consolidation
    /// folded into the new core is thereby re-bound to its now-real dense id. This is what
    /// lets an automatic consolidation fire while writes continue.
    ///
    /// # Ordering
    /// A lock-free reader must never overlay a stale delta on the new core: the rebuilt
    /// snapshot is published *before* the core UUID is re-bound, so any reader that
    /// observes `core_uuid == new_core_uuid` also observes the rebuilt (re-resolved)
    /// overlay. A reader straddling the swap may momentarily fall back to the pure new
    /// core (which already holds the pre-freeze writes) — a benign visibility blip; the
    /// post-freeze writes themselves are durable in the surviving segments.
    pub fn retire(
        &self,
        consumed: &[PathBuf],
        consumed_l0: &[PathBuf],
        new_core_uuid: GenId,
        new_core_node_count: u64,
        new_core_edge_count: u64,
        resolve: impl Fn(&WalOp) -> OpResolution,
    ) -> Result<()> {
        let mut inner = lock_writer(&self.inner);
        // The consumed WAL segments' + L0 levels' writes are now in the new core — drop
        // them. The currently-open (post-freeze) WAL segment is never in `consumed`
        // (freeze rotated to it), so it survives and keeps taking appends after this
        // rebuild. Every L0 level present at freeze was folded into the new core, so the
        // whole stack retires; 4c-B does not admit a flush during a consolidation (that
        // in-flight guard is 4d), so the stack at retire is exactly `consumed_l0`. This
        // must precede the replay below (which reads the WAL directory and must see only
        // the surviving segments); it changes nothing the writer lock protects.
        for path in consumed {
            remove_if_present(path)?;
        }
        for path in consumed_l0 {
            remove_if_present(path)?;
        }

        // Prepare — on locals. Rebuild the live memtable from the surviving (post-freeze)
        // WAL segments, each write re-resolved against the new core. Re-base the synthetic
        // id spaces on the freshly built core: its node/edge counts now include the
        // folded-in delta-born entities (including any that were flushed to an L0 level), so
        // a post-freeze born id starts past them and a post-freeze re-write of a folded born
        // key re-resolves to its now-real dense id.
        //
        // `resolve` is **caller code invoked under the writer lock**, and `apply` can panic,
        // so this is the writer's most exposed panic site — which is exactly why the L0 stack
        // is cleared in the install phase *below* rather than here: a panic leaves
        // `WriterInner` (memtable, L0 stack, seq) exactly as it was and `core_uuid` still
        // bound to the old core, so the writer stays usable, the server keeps failing safe to
        // the pure new core (the same posture as a retire that returns `Err`), and the retire
        // is retryable — `remove_if_present` tolerates the already-deleted paths.
        let mut mem = Memtable::with_bases(new_core_node_count, new_core_edge_count);
        let replay = replay_dir(&inner.dir)
            .with_context(|| format!("replay post-freeze WAL dir {:?}", inner.dir))?;
        for rec in &replay.records {
            mem.apply(&rec.op, resolve(&rec.op));
        }

        // Install — moves only. Publish the rebuilt overlay first (no L0 now), then re-bind
        // the core UUID (see the ordering note above). The seq counter stays monotonic from
        // the replayed high-water mark.
        inner.l0.clear();
        inner.seq = replay.last_seq;
        inner.mem = Arc::new(mem);
        self.republish(&inner);
        *write_lock(&self.core_uuid) = new_core_uuid;
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Re-bind the writer to a new core generation whose **dense id space is identical** to
    /// the current one — a T3 segment compaction (Phase 5) merges a contiguous run of
    /// immutable upper segments into one, reorganising the stack without renumbering any id,
    /// touching the base, or folding the delta. So — unlike [`Self::retire`] — the memtable,
    /// WAL and L0 levels are preserved untouched (no WAL replay, no synthetic-id rebase, no
    /// L0 clear); only the served set uuid the delta records changes. The caller MUST hold
    /// the consolidation guard and MUST have verified the new core preserves
    /// `extents().total()` for both id spaces (the resolved dense ids stay valid only under
    /// that invariant). The lock order (`inner` → `core_uuid`) mirrors [`Self::retire`], and
    /// the epoch bump invalidates any delta-overlaid result-cache entry keyed on the old set.
    pub fn rebind_core_uuid(&self, new_core_uuid: GenId) {
        // Serialise against an in-flight write so a mutation never straddles the rebind.
        let _inner = lock_writer(&self.inner);
        *write_lock(&self.core_uuid) = new_core_uuid;
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }

    /// Number of distinct node identities currently carrying a delta (diagnostics).
    pub fn node_delta_count(&self) -> usize {
        self.snapshot().node_delta_count()
    }

    /// Approximate resident **active-memtable** size in bytes — checked against the
    /// memtable→L0 flush cap (a full memtable flushes; the L0 levels don't count here).
    pub fn bytes(&self) -> usize {
        lock_writer(&self.inner).mem.bytes()
    }

    /// Approximate resident size of the **whole** delta (active memtable + every L0
    /// level) — checked against the total-delta soft/hard caps (Phase 4d).
    pub fn total_bytes(&self) -> usize {
        let inner = lock_writer(&self.inner);
        inner.mem.bytes() + inner.l0.iter().map(|s| s.bytes() as usize).sum::<usize>()
    }

    /// The number of sealed L0 levels currently overlaid (diagnostics / tests).
    pub fn l0_len(&self) -> usize {
        lock_writer(&self.inner).l0.len()
    }

    /// Total changed-entity count across the whole delta (nodes + edges, summed over
    /// every level). Compared against a fraction of the core's entity count to decide
    /// when to fire a background consolidation (Phase 4d-ii-b) — an over-estimate when
    /// an entity is touched in several levels, which only makes the trigger fire a
    /// little sooner (safe).
    pub fn delta_entity_count(&self) -> usize {
        let s = read_lock(&self.published);
        s.node_delta_count() + s.edge_delta_count()
    }

    /// The directory holding this graph's WAL segments.
    pub fn wal_dir(&self) -> PathBuf {
        lock_writer(&self.inner).dir.clone()
    }
}

/// Every `*.wal` segment file currently under `dir` (unordered). A missing
/// directory yields an empty list. Used by [`DeltaWriter::freeze`] to record which
/// segments a frozen snapshot consumes.
fn wal_segment_paths(dir: &Path) -> Result<Vec<PathBuf>> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("list WAL dir {dir:?}")),
    };
    let mut out = Vec::new();
    for entry in rd {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("wal") {
            out.push(path);
        }
    }
    Ok(out)
}

/// The next unused segment number under `dir`: one past the highest `NNNN.wal`, or
/// 0 if the directory has none. Opening a fresh number guarantees an existing
/// committed segment is never truncated by [`WalSink::create`].
fn next_segment_number(dir: &Path) -> Result<u64> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e).with_context(|| format!("list WAL dir {dir:?}")),
    };
    let mut max: Option<u64> = None;
    for entry in rd {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wal") {
            continue;
        }
        if let Some(n) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
        {
            max = Some(max.map_or(n, |m| m.max(n)));
        }
    }
    Ok(match max {
        Some(m) => m + 1,
        None => 0,
    })
}

/// Build the atomic published [`DeltaSnapshot`] from the active memtable and the L0
/// stack (newest-first): share the memtable (it is immutable once installed — the write
/// path replaces it wholesale rather than mutating it) and gather the L0 segments'
/// immutable memtable handles.
fn published_snapshot(mem: &Arc<Memtable>, l0: &[L0Level]) -> DeltaSnapshot {
    let levels: Vec<Arc<dyn LevelRead>> = l0.iter().map(|s| s.level_arc()).collect();
    DeltaSnapshot::with_levels(Arc::clone(mem), levels)
}

/// Open a sealed L0 segment at `path` in whichever format it was written: a **directory**
/// is off-heap (its payloads page through the shared `cache`), a **file** is resident.
/// Finding an off-heap segment with no `cache` is a clear configuration error.
fn open_l0_level(path: &Path, cache: Option<&Arc<GfBlockCache>>) -> Result<L0Level> {
    if path.is_dir() {
        let cache = cache
            .cloned()
            .context("off-heap L0 segment on disk but no block cache configured")?;
        // The cache scope is read from the segment's meta (persisted, fresh per write), so
        // a compaction that reuses a directory can't collide with stale cached blocks.
        let reader = Arc::new(L0Reader::open(path, cache)?);
        let bytes = dir_size(path);
        Ok(L0Level::OffHeap {
            reader,
            dir: path.to_path_buf(),
            bytes,
        })
    } else {
        Ok(L0Level::Resident(L0Segment::open(path)?))
    }
}

/// A fresh, globally-unique cache scope for a newly written off-heap segment, persisted in
/// its meta so a reopen reads it back. Fresh per write (even when a compaction reuses a
/// directory), and disjoint from the columnar keys (which scope on the generation UUID).
fn new_segment_scope() -> u128 {
    uuid::Uuid::new_v4().as_u128()
}

/// Total on-disk size of a segment directory (sum of its file lengths).
fn dir_size(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

/// Refine a base (core-only) resolution against the sealed L0 levels: a node/endpoint
/// key that the core reports Absent (`None`) but that is a **delta-born** node resident
/// in an L0 level resolves to that born node's existing synthetic id, so a re-`MERGE`
/// on the WAL-tail replay path reuses it rather than allocating a duplicate (Phase
/// 4c-B). Mirrors the live write path's `DeltaWriter::born_synthetic_for_identity`.
fn resolve_with_l0(op: &WalOp, base: OpResolution, l0: &[L0Level]) -> OpResolution {
    let born = |(label, key, value): (&str, &str, &Value)| {
        l0.iter()
            .find_map(|s| s.as_level().born_synthetic_for_identity(label, key, value))
    };
    match base {
        OpResolution::Node(None) => OpResolution::Node(op.node_key().and_then(born)),
        OpResolution::Edge { src, dst, edge_id } => {
            let (s_key, _reltype, d_key) = op.edge_keys().expect("edge op has edge keys");
            // `edge_id` (a core-edge-patch resolution) is already bound to the core, not
            // to an L0 level, so it passes through unchanged; only the born-endpoint
            // fallback consults L0.
            OpResolution::Edge {
                src: src.or_else(|| born(s_key)),
                dst: dst.or_else(|| born(d_key)),
                edge_id,
            }
        }
        other => other,
    }
}

/// Remove `path`, tolerating an already-absent path (idempotent cleanup). Handles both a
/// resident segment **file** and an off-heap segment **directory**.
fn remove_if_present(path: &Path) -> Result<()> {
    let res = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    match res {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("remove {path:?}")),
    }
}

/// Every `*.l0` segment under `l0_dir` as `(number, path)`, **sorted ascending** by
/// number (oldest→newest). A missing directory yields an empty list.
fn l0_segment_paths_sorted(l0_dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let rd = match std::fs::read_dir(l0_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("list L0 dir {l0_dir:?}")),
    };
    let mut out = Vec::new();
    for entry in rd {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("l0") {
            continue;
        }
        if let Some(n) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
        {
            out.push((n, path));
        }
    }
    out.sort_by_key(|(n, _)| *n);
    Ok(out)
}

/// The next unused L0 segment number under `l0_dir`: one past the highest, or 0 when
/// none exist. Monotonic across the writer's life so an L0 file is never overwritten
/// by a flush. (Compaction deliberately *reuses* the run's oldest number — see
/// [`DeltaWriter::compact_l0`].)
fn next_l0_number(l0_dir: &Path) -> Result<u64> {
    Ok(match l0_segment_paths_sorted(l0_dir)?.last() {
        Some((n, _)) => n + 1,
        None => 0,
    })
}

/// Levels within this byte-size factor of each other are the **same tier** and worth
/// compacting together; a level larger than `RATIO×` its neighbours is a different tier
/// (merging it in would be write amplification — rewriting a big level for a few small
/// writes). A first-cut ratio; not yet configurable.
const SIZE_TIER_RATIO: u64 = 4;

/// Choose a contiguous run of similar-sized L0 levels to compact, as a half-open range
/// over the newest-first stack sizes. Returns the **longest** maximal run whose byte
/// sizes are all within [`SIZE_TIER_RATIO`]× of the run's smallest (length ≥ 2); ties
/// break to the **oldest** run (largest start index). `None` when no adjacent pair is
/// same-tier — a healthy size ladder that needs no compaction. Pure + deterministic.
fn select_compaction_run(sizes: &[u64]) -> Option<(usize, usize)> {
    let n = sizes.len();
    let mut best: Option<(usize, usize)> = None;
    let mut i = 0;
    while i < n {
        // Extend a maximal run `[i, j)` whose min/max stay within RATIO. A zero-byte
        // level (only inert tombstones) is treated as size 1 so it never divides by zero
        // and always joins a neighbour tier.
        let mut lo = sizes[i].max(1);
        let mut hi = sizes[i].max(1);
        let mut j = i + 1;
        while j < n {
            let s = sizes[j].max(1);
            let nlo = lo.min(s);
            let nhi = hi.max(s);
            if nhi > nlo.saturating_mul(SIZE_TIER_RATIO) {
                break;
            }
            lo = nlo;
            hi = nhi;
            j += 1;
        }
        let len = j - i;
        if len >= 2 {
            // `>=` so a later (older) run of equal length wins the tie → oldest-first.
            let take = best.map_or(true, |(bs, be)| len >= be - bs);
            if take {
                best = Some((i, j));
            }
        }
        i = j.max(i + 1);
    }
    best
}

/// Choose the compaction run over a **mixed-format** stack: [`select_compaction_run`]'s
/// size-tier selection, restricted to a contiguous run of levels sharing one format
/// (`off_heap[i]`). A run must be single-format because the two formats have different
/// merge paths (in-RAM `merge_levels` vs streaming `merge_run`) and different on-disk
/// shapes (file vs directory) — and a stack *is* mixed whenever the `delta.offHeapL0` flag
/// changes between runs, since sealed segments reload in the format they were written.
/// Restricting to a contiguous span keeps the run contiguous in the stack, so compaction's
/// splice + oldest-file-number reuse (number order == born-id-base order) is unchanged.
/// Same tie-break as [`select_compaction_run`] (longest run; tie → oldest), and identical
/// to it on a single-format stack. Pure + deterministic.
fn select_compaction_run_in_stack(sizes: &[u64], off_heap: &[bool]) -> Option<(usize, usize)> {
    debug_assert_eq!(sizes.len(), off_heap.len());
    let n = sizes.len();
    let mut best: Option<(usize, usize)> = None;
    let mut span_start = 0;
    while span_start < n {
        let mut span_end = span_start + 1;
        while span_end < n && off_heap[span_end] == off_heap[span_start] {
            span_end += 1;
        }
        if let Some((s, e)) = select_compaction_run(&sizes[span_start..span_end]) {
            let (s, e) = (span_start + s, span_start + e);
            // `>=` so a later (older) span's equal-length run wins the tie → oldest-first,
            // matching `select_compaction_run`'s within-span tie-break.
            if best.map_or(true, |(bs, be)| e - s >= be - bs) {
                best = Some((s, e));
            }
        }
        span_start = span_end;
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::ids::Value;

    fn upsert(label: &str, key: &str, value: Value, patches: &[(&str, Value)]) -> WalOp {
        WalOp::UpsertNode {
            label: label.into(),
            key: key.into(),
            value,
            patches: patches
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    /// A resolver that maps the fixture's tickers to fixed dense ids.
    fn resolve_ticker(op: &WalOp) -> OpResolution {
        let (_, _, value) = op.node_key().expect("fixture uses node ops only");
        OpResolution::Node(match value {
            Value::Str(s) if s == "A" => Some(10),
            Value::Str(s) if s == "B" => Some(20),
            _ => None,
        })
    }

    /// Sugar for a resolved node write in the tests.
    fn node(id: u64) -> OpResolution {
        OpResolution::Node(Some(id))
    }

    fn tmp(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("slater_dw_{tag}_{}", std::process::id()))
    }

    fn gf_cache() -> Arc<GfBlockCache> {
        Arc::new(GfBlockCache::new(1 << 20))
    }

    fn open_with_flag(dir: &Path, cache: Arc<GfBlockCache>, off_heap: bool) -> DeltaWriter {
        DeltaWriter::open_with_cache(
            dir,
            "g",
            GenId(uuid::Uuid::nil()),
            100,
            0,
            off_heap,
            Some(cache),
            resolve_ticker,
        )
        .unwrap()
    }

    fn open_offheap(dir: &Path, cache: Arc<GfBlockCache>) -> DeltaWriter {
        open_with_flag(dir, cache, true)
    }

    /// An off-heap flush writes a **directory** segment whose reads (core patch + a
    /// delta-born node) survive a reopen and match the resident semantics; compaction is a
    /// no-op in off-heap mode.
    #[test]
    fn offheap_flush_writes_a_directory_and_reads_survive_reopen() {
        let dir = tmp("offheap_flush");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = gf_cache();
        {
            let w = open_offheap(&dir, cache.clone());
            w.write(
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("A".into()),
                    &[("price", Value::Int(10))],
                ),
                node(10),
            )
            .unwrap();
            // A born node (resolver returns None for "C" → synthetic id 100).
            w.write(
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("C".into()),
                    &[("price", Value::Int(30))],
                ),
                OpResolution::Node(None),
            )
            .unwrap();

            assert!(w.flush_to_l0().unwrap(), "memtable had writes to flush");
            assert_eq!(w.l0_len(), 1);
            // The on-disk segment is a directory (off-heap), not a file.
            let seg = dir.join("l0").join("0000000000.l0");
            assert!(seg.is_dir(), "off-heap flush writes a directory segment");
            // Compaction is disabled in off-heap mode.
            assert!(!w.compact_l0().unwrap(), "off-heap L0 does no compaction");

            // Reads come from the off-heap level now (memtable was reset by the flush).
            let snap = w.delta_snapshot();
            assert_eq!(
                snap.node_patch(10)
                    .and_then(|d| d.patches.get("price").cloned()),
                Some(Value::Int(10)),
            );
            assert_eq!(snap.born_ids_with_label("Company"), vec![100]);
            assert_eq!(
                snap.node_identity_by_dense(100),
                Some((
                    "Company".to_string(),
                    "ticker".to_string(),
                    Value::Str("C".into())
                )),
            );
        }
        // Reopen off-heap: the directory segment reloads and the reads still hold.
        let w2 = open_offheap(&dir, cache);
        assert_eq!(w2.l0_len(), 1);
        let snap = w2.delta_snapshot();
        assert_eq!(
            snap.node_patch(10)
                .and_then(|d| d.patches.get("price").cloned()),
            Some(Value::Int(10)),
        );
        assert_eq!(snap.born_ids_with_label("Company"), vec![100]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Freezing an off-heap writer captures the sealed level as a `dyn LevelRead`, so the
    /// consolidation dump reads its writes through the merged view (no resident memtable).
    #[test]
    fn offheap_freeze_captures_levels_for_the_dump() {
        let dir = tmp("offheap_freeze");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = gf_cache();
        let w = open_offheap(&dir, cache);
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(7))],
            ),
            node(10),
        )
        .unwrap();
        assert!(w.flush_to_l0().unwrap());
        let frozen = w.freeze().unwrap();
        assert_eq!(frozen.l0.len(), 1, "the sealed off-heap level is captured");
        assert_eq!(frozen.consumed_l0.len(), 1);
        // The captured level reads through LevelRead (as the dump's merged view would).
        let stack = DeltaSnapshot::with_levels(frozen.snapshot.clone(), frozen.l0.clone());
        assert_eq!(
            stack
                .node_patch(10)
                .and_then(|d| d.patches.get("price").cloned()),
            Some(Value::Int(7)),
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Off-heap L0→L0 compaction (D54) streams the sealed on-disk segments through a merge
    /// and collapses the same-tier run into one, preserving every read (cross-level core
    /// patch fold + disjoint born nodes), durable across a reopen.
    #[test]
    fn offheap_compaction_streams_and_collapses_the_stack() {
        let dir = tmp("offheap_compact");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = gf_cache();
        {
            let w = open_offheap(&dir, cache.clone());
            // Three same-tier flushes: each re-patches core 10 (folds newest-wins) and adds
            // a distinct born node (disjoint synthetic ids 100, 101, 102).
            for i in 0..3u64 {
                w.write(
                    upsert(
                        "Company",
                        "ticker",
                        Value::Str("A".into()),
                        &[("price", Value::Int(i as i64))],
                    ),
                    node(10),
                )
                .unwrap();
                w.write(
                    upsert(
                        "Company",
                        "ticker",
                        Value::Str(format!("C{i}")),
                        &[("n", Value::Int(i as i64))],
                    ),
                    OpResolution::Node(None),
                )
                .unwrap();
                assert!(w.flush_to_l0().unwrap());
            }
            assert_eq!(w.l0_len(), 3);
            let before_born = w.delta_snapshot().born_ids_with_label("Company");
            assert_eq!(before_born, vec![100, 101, 102]);

            // Compact the same-tier run → collapses the stack.
            assert!(w.compact_l0().unwrap(), "same-tier run compacts");
            assert!(w.l0_len() < 3, "compaction collapsed the stack");
            let snap = w.delta_snapshot();
            assert_eq!(
                snap.node_patch(10)
                    .and_then(|d| d.patches.get("price").cloned()),
                Some(Value::Int(2)),
                "newest core-10 patch wins after the merge",
            );
            assert_eq!(
                snap.born_ids_with_label("Company"),
                before_born,
                "born ids preserved",
            );
            assert_eq!(
                snap.node_identity_by_dense(100),
                Some((
                    "Company".to_string(),
                    "ticker".to_string(),
                    Value::Str("C0".into()),
                )),
            );
        }
        // Reopen off-heap: the merged segment reloads and reads still hold.
        let w2 = open_offheap(&dir, cache);
        let snap = w2.delta_snapshot();
        assert_eq!(
            snap.node_patch(10)
                .and_then(|d| d.patches.get("price").cloned()),
            Some(Value::Int(2)),
        );
        assert_eq!(snap.born_ids_with_label("Company"), vec![100, 101, 102]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// HIK-82. Flipping `delta.offHeapL0` off leaves a **mixed-format** stack (sealed
    /// segments reload in whichever format they were written), and a write-triggered
    /// compaction must not blow up on it: before the fix the resident branch called
    /// `resident_memtable()` on an off-heap level and hit `unreachable!` *while holding the
    /// writer mutex*, poisoning it (every later write/freeze/flush then panicked too).
    /// Compaction must instead pick a single-format run and merge it in that run's own
    /// format — so both tiers keep collapsing across the flag flip, reads are preserved and
    /// the writer stays usable.
    #[test]
    fn compact_l0_on_a_mixed_format_stack_compacts_without_panicking() {
        let dir = tmp("mixed_compact");
        let _ = std::fs::remove_dir_all(&dir);
        let cache = gf_cache();

        // Two off-heap (directory) levels: each re-patches core node A (dense 10) and adds
        // one born node (synthetic 100, 101).
        {
            let w = open_offheap(&dir, cache.clone());
            for i in 0..2u64 {
                w.write(
                    upsert(
                        "Company",
                        "ticker",
                        Value::Str("A".into()),
                        &[("price", Value::Int(i as i64))],
                    ),
                    node(10),
                )
                .unwrap();
                w.write(
                    upsert(
                        "Company",
                        "ticker",
                        Value::Str(format!("C{i}")),
                        &[("n", Value::Int(i as i64))],
                    ),
                    OpResolution::Node(None),
                )
                .unwrap();
                assert!(w.flush_to_l0().unwrap());
            }
            assert_eq!(w.l0_len(), 2);
        }

        // The operator flips `offHeapL0` off and restarts. The two directory segments
        // reload as off-heap levels; the next flush writes a *resident* file on top of them
        // — a mixed stack.
        let w = open_with_flag(&dir, cache.clone(), false);
        assert_eq!(
            w.l0_len(),
            2,
            "the off-heap levels reload under the flag flip"
        );
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(2))],
            ),
            node(10),
        )
        .unwrap();
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("C2".into()),
                &[("n", Value::Int(2))],
            ),
            OpResolution::Node(None),
        )
        .unwrap();
        assert!(w.flush_to_l0().unwrap());
        assert_eq!(w.l0_len(), 3);
        {
            let inner = w.inner.lock().expect("delta writer lock");
            let formats: Vec<bool> = inner.l0.iter().map(|s| s.is_off_heap()).collect();
            assert_eq!(
                formats,
                vec![false, true, true],
                "mixed stack, newest-first: a resident file over two off-heap directories",
            );
        }

        let reads = |w: &DeltaWriter| {
            let s = w.delta_snapshot();
            (
                s.born_ids_with_label("Company"),
                s.node_patch(10)
                    .and_then(|d| d.patches.get("price").cloned()),
                s.node_patch(100).and_then(|d| d.patches.get("n").cloned()),
                s.node_patch(101).and_then(|d| d.patches.get("n").cloned()),
                s.node_patch(102).and_then(|d| d.patches.get("n").cloned()),
            )
        };
        let before = reads(&w);
        assert_eq!(
            before,
            (
                vec![100, 101, 102],
                Some(Value::Int(2)),
                Some(Value::Int(0)),
                Some(Value::Int(1)),
                Some(Value::Int(2))
            ),
        );

        // The off-heap tier compacts on its own (streamed through `merge_run`) — no panic,
        // no poisoned lock, and the resident level on top is left alone.
        assert!(w.compact_l0().unwrap(), "the off-heap tier compacts");
        assert_eq!(w.l0_len(), 2, "the two off-heap levels merged into one");
        assert_eq!(reads(&w), before, "reads unchanged by the mixed compaction");
        {
            let inner = w.inner.lock().expect("delta writer lock");
            let formats: Vec<bool> = inner.l0.iter().map(|s| s.is_off_heap()).collect();
            assert_eq!(
                formats,
                vec![false, true],
                "the merged level kept the run's own (off-heap) format",
            );
        }

        // The writer is still healthy after the compaction (the lock was never poisoned):
        // another write + flush lands a second resident level, and the *resident* tier now
        // compacts — in its own format — over the surviving off-heap level.
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("C3".into()),
                &[("n", Value::Int(3))],
            ),
            OpResolution::Node(None),
        )
        .unwrap();
        assert!(w.flush_to_l0().unwrap());
        assert_eq!(w.l0_len(), 3);
        assert!(w.compact_l0().unwrap(), "the resident tier compacts");
        assert_eq!(w.l0_len(), 2);
        {
            let inner = w.inner.lock().expect("delta writer lock");
            let formats: Vec<bool> = inner.l0.iter().map(|s| s.is_off_heap()).collect();
            assert_eq!(formats, vec![false, true], "still one level of each format");
        }
        let after = reads(&w);
        assert_eq!(
            after,
            (
                vec![100, 101, 102, 103],
                Some(Value::Int(2)),
                Some(Value::Int(0)),
                Some(Value::Int(1)),
                Some(Value::Int(2))
            ),
        );
        assert_eq!(
            w.delta_snapshot()
                .node_patch(103)
                .and_then(|d| d.patches.get("n").cloned()),
            Some(Value::Int(3)),
        );
        drop(w);

        // Durable + correctly ordered across a reopen: the merged segments reused their
        // run's oldest file number, so on-disk number order still matches born-id order.
        let w2 = open_with_flag(&dir, cache, false);
        assert_eq!(w2.l0_len(), 2);
        assert_eq!(reads(&w2), after, "mixed-compacted reads survive a reopen");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_publishes_snapshot_and_bumps_epoch() {
        let dir = tmp("publish");
        let _ = std::fs::remove_dir_all(&dir);
        let w =
            DeltaWriter::open(&dir, "g", GenId(uuid::Uuid::nil()), 100, 0, resolve_ticker).unwrap();
        assert_eq!(w.snapshot().node_delta_count(), 0);
        let e0 = w.epoch();

        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(10))],
            ),
            node(10),
        )
        .unwrap();

        let snap = w.snapshot();
        let d = snap.node_patch(10).expect("resolved by dense id");
        assert_eq!(d.patches.get("price"), Some(&Value::Int(10)));
        assert!(w.epoch() > e0, "epoch bumps on write");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopen_replays_committed_writes() {
        let dir = tmp("reopen");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let w = DeltaWriter::open(&dir, "g", GenId(uuid::Uuid::nil()), 100, 0, resolve_ticker)
                .unwrap();
            w.write(
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("A".into()),
                    &[("price", Value::Int(11))],
                ),
                node(10),
            )
            .unwrap();
            w.write(
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("B".into()),
                    &[("price", Value::Int(22))],
                ),
                node(20),
            )
            .unwrap();
        }
        // A fresh writer over the same dir must rebuild the same memtable from the WAL.
        let w2 =
            DeltaWriter::open(&dir, "g", GenId(uuid::Uuid::nil()), 100, 0, resolve_ticker).unwrap();
        let snap = w2.snapshot();
        assert_eq!(snap.node_delta_count(), 2);
        assert_eq!(
            snap.node_patch(10).unwrap().patches.get("price"),
            Some(&Value::Int(11))
        );
        assert_eq!(
            snap.node_patch(20).unwrap().patches.get("price"),
            Some(&Value::Int(22))
        );
        // The reopened writer appends to a fresh segment, leaving the sealed one intact.
        w2.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(99))],
            ),
            node(10),
        )
        .unwrap();
        let w3 =
            DeltaWriter::open(&dir, "g", GenId(uuid::Uuid::nil()), 100, 0, resolve_ticker).unwrap();
        assert_eq!(
            w3.snapshot().node_patch(10).unwrap().patches.get("price"),
            Some(&Value::Int(99)),
            "last-writer-wins survives across segments"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_batch_group_commits_and_survives_reopen() {
        let dir = tmp("batch");
        let _ = std::fs::remove_dir_all(&dir);
        let core = GenId(uuid::Uuid::from_u128(77));
        let w = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();

        // An empty batch is a no-op that returns the current seq without publishing.
        let e0 = w.epoch();
        assert_eq!(w.write_batch(&[]).unwrap(), Seq(0));
        assert_eq!(w.epoch(), e0, "empty batch does not bump the epoch");

        // A batch: patch core node 10, and create two born nodes (synthetic 100, 101).
        let batch = vec![
            (
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("A".into()),
                    &[("price", Value::Int(1))],
                ),
                node(10),
            ),
            (
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("X".into()),
                    &[("price", Value::Int(2))],
                ),
                OpResolution::Node(None),
            ),
            (
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("Y".into()),
                    &[("price", Value::Int(3))],
                ),
                OpResolution::Node(None),
            ),
        ];
        let last = w.write_batch(&batch).unwrap();
        assert_eq!(last, Seq(3), "three ops advance the seq by three");
        assert_eq!(w.epoch(), e0 + 1, "the whole batch is ONE published epoch");

        let read = |w: &DeltaWriter| {
            let s = w.delta_snapshot();
            (
                s.born_count(),
                s.node_patch(10)
                    .and_then(|d| d.patches.get("price").cloned()),
                s.node_patch(100)
                    .and_then(|d| d.patches.get("price").cloned()),
                s.node_patch(101)
                    .and_then(|d| d.patches.get("price").cloned()),
            )
        };
        let want = (
            2,
            Some(Value::Int(1)),
            Some(Value::Int(2)),
            Some(Value::Int(3)),
        );
        assert_eq!(
            read(&w),
            want,
            "every op in the batch is applied + published"
        );

        // The single group commit (one commit marker for seq 3) replays the whole batch.
        drop(w);
        let w2 = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();
        assert_eq!(
            read(&w2),
            want,
            "the batched writes are durable across a reopen"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn freeze_captures_snapshot_and_retire_resets_against_new_core() {
        let dir = tmp("freeze");
        let _ = std::fs::remove_dir_all(&dir);
        let old_core = GenId(uuid::Uuid::from_u128(1));
        let new_core = GenId(uuid::Uuid::from_u128(2));
        let w = DeltaWriter::open(&dir, "g", old_core, 100, 0, resolve_ticker).unwrap();
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(42))],
            ),
            node(10),
        )
        .unwrap();

        // Freeze: the snapshot captures the committed write, and the consumed set is
        // the sealed segment(s) — while a fresh, empty segment is now open.
        let frozen = w.freeze().unwrap();
        assert_eq!(
            frozen.snapshot.node_patch(10).unwrap().patches.get("price"),
            Some(&Value::Int(42))
        );
        assert!(
            !frozen.consumed.is_empty(),
            "freeze records consumed segments"
        );
        for p in &frozen.consumed {
            assert!(
                p.exists(),
                "consumed segment still on disk until retire: {p:?}"
            );
        }
        // The live overlay is untouched by a freeze (reads keep seeing the delta).
        assert_eq!(
            w.snapshot().node_patch(10).unwrap().patches.get("price"),
            Some(&Value::Int(42))
        );
        assert_eq!(w.core_uuid(), old_core);
        let epoch_before = w.epoch();

        // Retire against the new core: consumed segments gone, overlay empty, rebind.
        // No post-freeze write here, so the replayed post-freeze segment is empty and the
        // rebuilt overlay is empty.
        w.retire(
            &frozen.consumed,
            &frozen.consumed_l0,
            new_core,
            100,
            0,
            resolve_ticker,
        )
        .unwrap();
        for p in &frozen.consumed {
            assert!(!p.exists(), "retire deletes the consumed segment: {p:?}");
        }
        assert_eq!(w.snapshot().node_delta_count(), 0, "delta retired");
        assert!(w.snapshot().node_patch(10).is_none());
        assert_eq!(w.core_uuid(), new_core, "writer re-bound to the new core");
        assert!(w.epoch() > epoch_before, "retire bumps the epoch");

        // A reopen after retire replays nothing (the only remaining segment is the
        // fresh empty one), so the writer comes up clean against the new core.
        let reopened = DeltaWriter::open(&dir, "g", new_core, 100, 0, resolve_ticker).unwrap();
        assert_eq!(reopened.snapshot().node_delta_count(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn writes_during_consolidation_survive() {
        // A write that arrives after freeze but before retire must be carried forward,
        // re-resolved against the new core — not discarded (the Phase 4a fix).
        let dir = tmp("concurrent_write");
        let _ = std::fs::remove_dir_all(&dir);
        let old_core = GenId(uuid::Uuid::from_u128(10));
        let new_core = GenId(uuid::Uuid::from_u128(11));
        let w = DeltaWriter::open(&dir, "g", old_core, 100, 0, resolve_ticker).unwrap();

        // Pre-freeze write on core node A (dense 10).
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(1))],
            ),
            node(10),
        )
        .unwrap();

        let frozen = w.freeze().unwrap();

        // Post-freeze write on core node B (dense 20) — lands in the fresh segment.
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("B".into()),
                &[("price", Value::Int(2))],
            ),
            node(20),
        )
        .unwrap();

        // Consolidation folded A's patch into a new core with permuted dense ids; the
        // new-core resolver maps the business keys to their new ids (A→30, B→40).
        let resolve_new = |op: &WalOp| -> OpResolution {
            let (_, _, value) = op.node_key().expect("node ops only");
            OpResolution::Node(match value {
                Value::Str(s) if s == "A" => Some(30),
                Value::Str(s) if s == "B" => Some(40),
                _ => None,
            })
        };
        w.retire(
            &frozen.consumed,
            &frozen.consumed_l0,
            new_core,
            100,
            0,
            resolve_new,
        )
        .unwrap();

        // A's patch now lives in the new core (gone from the delta); B was carried
        // forward and re-resolved onto its new dense id.
        let snap = w.snapshot();
        assert_eq!(
            snap.node_delta_count(),
            1,
            "only the post-freeze write remains"
        );
        assert!(snap.node_patch(10).is_none(), "A folded into the new core");
        assert_eq!(
            snap.node_patch(40).unwrap().patches.get("price"),
            Some(&Value::Int(2)),
            "B carried forward, re-resolved to its new-core dense id"
        );
        assert_eq!(w.core_uuid(), new_core);

        // Durable: a reopen against the new core replays the surviving segment and
        // recovers B at the same new dense id.
        let reopened = DeltaWriter::open(&dir, "g", new_core, 100, 0, resolve_new).unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .node_patch(40)
                .unwrap()
                .patches
                .get("price"),
            Some(&Value::Int(2)),
            "post-freeze write survives a reopen after retire"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn post_freeze_write_reresolves_a_born_node_to_the_new_core() {
        // A node created (MERGE) pre-freeze takes a synthetic id; consolidation folds it
        // into the new core as a real node. A post-freeze patch on it must re-resolve to
        // that real dense id at retire.
        let dir = tmp("born_reresolve");
        let _ = std::fs::remove_dir_all(&dir);
        let old_core = GenId(uuid::Uuid::from_u128(20));
        let new_core = GenId(uuid::Uuid::from_u128(21));
        let w = DeltaWriter::open(&dir, "g", old_core, 100, 0, resolve_ticker).unwrap();

        // Pre-freeze: MERGE-create born node C (absent from the old core → synthetic id 100).
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("C".into()),
                &[("price", Value::Int(7))],
            ),
            OpResolution::Node(None),
        )
        .unwrap();
        assert_eq!(w.snapshot().synthetic_base(), 100);
        assert_eq!(
            w.snapshot().node_patch(100).unwrap().patches.get("price"),
            Some(&Value::Int(7)),
            "born node sits at the synthetic id"
        );

        let frozen = w.freeze().unwrap();

        // Post-freeze: patch C again. Against the old core it is still born (id 100).
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("C".into()),
                &[("price", Value::Int(9))],
            ),
            OpResolution::Node(None),
        )
        .unwrap();

        // Consolidation folded C into the new core at a real dense id (50); node_count grew
        // to 101. The new-core resolver now finds C.
        let resolve_new = |op: &WalOp| -> OpResolution {
            let (_, _, value) = op.node_key().expect("node ops only");
            OpResolution::Node(match value {
                Value::Str(s) if s == "C" => Some(50),
                _ => None,
            })
        };
        w.retire(
            &frozen.consumed,
            &frozen.consumed_l0,
            new_core,
            101,
            0,
            resolve_new,
        )
        .unwrap();

        let snap = w.snapshot();
        assert_eq!(snap.node_delta_count(), 1);
        assert!(
            snap.node_patch(100).is_none(),
            "no longer born — folded into the core"
        );
        assert_eq!(
            snap.node_patch(50).unwrap().patches.get("price"),
            Some(&Value::Int(9)),
            "post-freeze patch re-resolved onto the now-real dense id"
        );
        assert_eq!(
            snap.synthetic_base(),
            101,
            "synthetic space re-based past the grown core"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn freeze_without_retire_keeps_the_write_durable() {
        // Models a consolidation that fails/crashes after freeze but before publish:
        // the sealed segment must still replay the committed write (no loss).
        let dir = tmp("freeze_crash");
        let _ = std::fs::remove_dir_all(&dir);
        let core = GenId(uuid::Uuid::from_u128(3));
        let w = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("B".into()),
                &[("price", Value::Int(7))],
            ),
            node(20),
        )
        .unwrap();
        let _frozen = w.freeze().unwrap(); // freeze, then "crash" (drop, no retire)
        drop(w);

        let reopened = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .node_patch(20)
                .unwrap()
                .patches
                .get("price"),
            Some(&Value::Int(7)),
            "a frozen-but-not-retired write survives a reopen"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn flush_to_l0_seals_memtable_and_reopen_reloads_l0() {
        let dir = tmp("flush_reload");
        let _ = std::fs::remove_dir_all(&dir);
        let core = GenId(uuid::Uuid::from_u128(30));
        let w = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();

        // Empty flush is a no-op.
        assert!(!w.flush_to_l0().unwrap(), "nothing to flush");
        assert_eq!(w.l0_len(), 0);

        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(11))],
            ),
            node(10),
        )
        .unwrap();
        assert!(
            !w.snapshot().is_empty(),
            "write lands in the active memtable"
        );

        // Flush spills the memtable to an L0 level and resets the active memtable empty.
        assert!(w.flush_to_l0().unwrap());
        assert_eq!(w.l0_len(), 1);
        assert!(
            w.snapshot().is_empty(),
            "active memtable freed by the flush"
        );
        assert!(
            w.snapshot().node_patch(10).is_none(),
            "the write no longer lives in the active memtable"
        );
        // …but the full delta still overlays it (from the L0 level).
        assert_eq!(
            w.delta_snapshot()
                .node_patch(10)
                .unwrap()
                .patches
                .get("price"),
            Some(&Value::Int(11)),
            "the flushed write reads back through the L0 level"
        );

        // A reopen reloads the L0 segment before replaying the (now-empty) WAL tail.
        let w2 = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();
        assert_eq!(w2.l0_len(), 1, "reopen reloads the L0 segment");
        assert_eq!(
            w2.delta_snapshot()
                .node_patch(10)
                .unwrap()
                .patches
                .get("price"),
            Some(&Value::Int(11)),
            "the flushed write survives a reopen via the L0 file"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn remerge_of_a_flushed_born_node_reuses_its_synthetic_id() {
        // Phase 4c-B write-path born resolution: a MERGE-born node flushed to an L0
        // level, re-MERGE'd afterwards, must reuse its synthetic id — not duplicate.
        let dir = tmp("flush_born_reuse");
        let _ = std::fs::remove_dir_all(&dir);
        let core = GenId(uuid::Uuid::from_u128(31));
        let w = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();

        // MERGE-create born node C (absent from the core → synthetic id 100).
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("C".into()),
                &[("price", Value::Int(7))],
            ),
            OpResolution::Node(None),
        )
        .unwrap();
        assert!(w.flush_to_l0().unwrap());
        assert_eq!(w.l0_len(), 1);
        assert_eq!(w.delta_snapshot().born_count(), 1);

        // The writer resolves the flushed born key to its existing synthetic id — this is
        // exactly what `execute_write`'s MERGE-Absent branch consults.
        let reused = w.born_synthetic_for_identity("Company", "ticker", &Value::Str("C".into()));
        assert_eq!(reused, Some(100), "re-MERGE resolves to the L0 born id");

        // Re-MERGE with that resolution → patches the existing born node, no duplicate.
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("C".into()),
                &[("price", Value::Int(9))],
            ),
            OpResolution::Node(reused),
        )
        .unwrap();
        let snap = w.delta_snapshot();
        assert_eq!(snap.born_count(), 1, "no duplicate born node allocated");
        assert_eq!(
            snap.node_patch(100).unwrap().patches.get("price"),
            Some(&Value::Int(9)),
            "the newer patch wins over the flushed value"
        );

        // A reopen reproduces the resolution: the WAL-tail re-MERGE re-resolves against
        // the reloaded L0 level, so the born id stays 100 (no duplicate on replay).
        let w2 = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();
        let snap2 = w2.delta_snapshot();
        assert_eq!(
            snap2.born_count(),
            1,
            "reopen does not duplicate the born node"
        );
        assert_eq!(
            snap2.node_patch(100).unwrap().patches.get("price"),
            Some(&Value::Int(9)),
            "the re-MERGE patch survives a reopen"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compact_l0_collapses_the_stack_preserving_reads() {
        let dir = tmp("compact");
        let _ = std::fs::remove_dir_all(&dir);
        let core = GenId(uuid::Uuid::from_u128(40));
        let w = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();

        // Nothing to compact yet.
        assert!(!w.compact_l0().unwrap());

        // Segment 0: born node C (resolve None → synthetic 100).
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("C".into()),
                &[("price", Value::Int(1))],
            ),
            OpResolution::Node(None),
        )
        .unwrap();
        assert!(w.flush_to_l0().unwrap());

        // Segment 1: patch core node A (dense 10) + born node D (synthetic 101).
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(5))],
            ),
            node(10),
        )
        .unwrap();
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("D".into()),
                &[("price", Value::Int(2))],
            ),
            OpResolution::Node(None),
        )
        .unwrap();
        assert!(w.flush_to_l0().unwrap());
        assert_eq!(w.l0_len(), 2, "two L0 levels before compaction");

        let reads = |w: &DeltaWriter| {
            let s = w.delta_snapshot();
            (
                s.born_count(),
                s.synthetic_base(),
                s.node_patch(100)
                    .and_then(|d| d.patches.get("price").cloned()),
                s.node_patch(10)
                    .and_then(|d| d.patches.get("price").cloned()),
                s.node_patch(101)
                    .and_then(|d| d.patches.get("price").cloned()),
            )
        };
        let before = reads(&w);
        assert_eq!(
            before,
            (
                2,
                100,
                Some(Value::Int(1)),
                Some(Value::Int(5)),
                Some(Value::Int(2))
            )
        );

        // Compact: one merged level, identical reads.
        assert!(w.compact_l0().unwrap());
        assert_eq!(w.l0_len(), 1, "stack collapsed to one level");
        assert_eq!(reads(&w), before, "reads unchanged by compaction");

        // Reopen reloads the single compacted segment.
        let w2 = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();
        assert_eq!(w2.l0_len(), 1);
        assert_eq!(reads(&w2), before, "compacted reads survive a reopen");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn select_compaction_run_picks_a_same_size_tier() {
        // Fewer than two, or no adjacent same-tier pair (a clean size ladder) → nothing.
        assert_eq!(select_compaction_run(&[]), None);
        assert_eq!(select_compaction_run(&[100]), None);
        assert_eq!(
            select_compaction_run(&[1000, 200, 40, 8]),
            None,
            "size ladder"
        );
        // A run of equal-sized levels merges wholesale.
        assert_eq!(select_compaction_run(&[100, 100, 100]), Some((0, 3)));
        // A big level (newest) is left out; the trailing similar-sized run merges.
        assert_eq!(select_compaction_run(&[10_000, 100, 120, 90]), Some((1, 4)));
        // Two same-tier runs of equal length → the OLDEST (largest start) wins the tie.
        assert_eq!(
            select_compaction_run(&[100, 100, 9_000, 50, 60]),
            Some((3, 5))
        );
        // Within-ratio (4×) is same tier; just over is not.
        assert_eq!(select_compaction_run(&[100, 400]), Some((0, 2)));
        assert_eq!(select_compaction_run(&[100, 401]), None);
    }

    #[test]
    fn select_compaction_run_in_stack_never_crosses_a_format_boundary() {
        // A single-format stack selects exactly as the format-blind selector does.
        for sizes in [
            vec![],
            vec![100],
            vec![1000, 200, 40, 8],
            vec![100, 100, 100],
            vec![10_000, 100, 120, 90],
            vec![100, 100, 9_000, 50, 60],
        ] {
            for off_heap in [false, true] {
                let flags = vec![off_heap; sizes.len()];
                assert_eq!(
                    select_compaction_run_in_stack(&sizes, &flags),
                    select_compaction_run(&sizes),
                    "single-format stack {sizes:?} (off_heap={off_heap})",
                );
            }
        }
        // The tiers are same-size across the boundary, but a run may never span it: the
        // longest single-format run wins (here the two off-heap levels).
        assert_eq!(
            select_compaction_run_in_stack(&[100, 100, 100], &[false, true, true]),
            Some((1, 3)),
        );
        // Equal-length runs either side of the boundary → the OLDEST (largest start) wins,
        // matching the format-blind tie-break.
        assert_eq!(
            select_compaction_run_in_stack(&[100, 100, 100, 100], &[false, false, true, true]),
            Some((2, 4)),
        );
        // A same-tier pair that only exists *across* the boundary is not a run — the
        // formats have different merge paths, so nothing is compacted.
        assert_eq!(
            select_compaction_run_in_stack(&[100, 100], &[false, true]),
            None,
        );
        // A lone level of the other format between two runs does not join either.
        assert_eq!(
            select_compaction_run_in_stack(&[100, 100, 100], &[true, false, true]),
            None,
        );
    }

    #[test]
    fn compact_l0_merges_only_the_matching_size_tier() {
        // Three L0 levels: two small (same tier) beneath one much larger level. Size-
        // tiered compaction merges only the two small ones, leaving the large level and
        // every born id intact — a *partial* compaction, not merge-all.
        let dir = tmp("compact_partial");
        let _ = std::fs::remove_dir_all(&dir);
        let core = GenId(uuid::Uuid::from_u128(41));
        let w = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();

        // Level 0 (oldest): one born node — synthetic id 100.
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("p", Value::Int(1))],
            ),
            OpResolution::Node(None),
        )
        .unwrap();
        assert!(w.flush_to_l0().unwrap());
        // Level 1: one born node — synthetic id 101.
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("B".into()),
                &[("p", Value::Int(2))],
            ),
            OpResolution::Node(None),
        )
        .unwrap();
        assert!(w.flush_to_l0().unwrap());
        // Level 2 (newest): many born nodes, so this level is a different (larger) tier.
        for i in 0..64u64 {
            w.write(
                upsert(
                    "Company",
                    "ticker",
                    Value::Str(format!("big{i}")),
                    &[("p", Value::Int(i as i64))],
                ),
                OpResolution::Node(None),
            )
            .unwrap();
        }
        assert!(w.flush_to_l0().unwrap());
        assert_eq!(w.l0_len(), 3, "three levels before compaction");

        // Snapshot the reads that must be preserved: the two small levels' born nodes and
        // a couple from the big level, plus the born count.
        let reads = |w: &DeltaWriter| {
            let s = w.delta_snapshot();
            (
                s.born_count(),
                s.node_patch(100).and_then(|d| d.patches.get("p").cloned()),
                s.node_patch(101).and_then(|d| d.patches.get("p").cloned()),
                s.node_patch(102).and_then(|d| d.patches.get("p").cloned()),
                s.node_patch(165).and_then(|d| d.patches.get("p").cloned()),
            )
        };
        let before = reads(&w);
        assert_eq!(
            before,
            (
                66,
                Some(Value::Int(1)),
                Some(Value::Int(2)),
                Some(Value::Int(0)),
                Some(Value::Int(63))
            ),
            "2 small born + 64 big born = 66; ids 100/101 small, 102.. big"
        );

        // Partial compaction: the two small levels merge, the big one is untouched.
        assert!(w.compact_l0().unwrap());
        assert_eq!(w.l0_len(), 2, "only the small tier merged (not merge-all)");
        assert_eq!(
            reads(&w),
            before,
            "reads unchanged by the partial compaction"
        );

        // A second compaction now sees the merged small level and the big level — either
        // same-tier (merge) or not; either way reads stay identical and born ids hold.
        let _ = w.compact_l0().unwrap();
        assert_eq!(
            reads(&w),
            before,
            "reads unchanged after a further compaction"
        );

        // Durable + correctly ordered across a reopen (the merged segment reused the
        // run's oldest file number, so number order still matches born-id base order).
        let w2 = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();
        assert_eq!(
            reads(&w2),
            before,
            "partial-compacted reads survive a reopen"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn consolidation_guard_suppresses_flush_and_compact() {
        let dir = tmp("guard");
        let _ = std::fs::remove_dir_all(&dir);
        let core = GenId(uuid::Uuid::from_u128(41));
        let w = DeltaWriter::open(&dir, "g", core, 100, 0, resolve_ticker).unwrap();

        // Build two L0 segments so both flush and compact would have work to do.
        for v in ["A", "B"] {
            w.write(
                upsert(
                    "Company",
                    "ticker",
                    Value::Str(v.into()),
                    &[("p", Value::Int(1))],
                ),
                node(if v == "A" { 10 } else { 20 }),
            )
            .unwrap();
            w.flush_to_l0().unwrap();
        }
        assert_eq!(w.l0_len(), 2);
        // A live write in the active memtable.
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("p", Value::Int(2))],
            ),
            node(10),
        )
        .unwrap();
        assert!(!w.snapshot().is_empty());

        // Claim consolidation: flush + compact become no-ops (the stack must not change
        // across a freeze→retire window).
        assert!(w.begin_consolidation());
        assert!(!w.begin_consolidation(), "a second claim is refused");
        assert!(
            !w.flush_to_l0().unwrap(),
            "flush suppressed while consolidating"
        );
        assert!(
            !w.compact_l0().unwrap(),
            "compaction suppressed while consolidating"
        );
        assert_eq!(w.l0_len(), 2, "L0 stack untouched");
        assert!(!w.snapshot().is_empty(), "memtable not flushed");

        // Release: maintenance works again.
        w.end_consolidation();
        assert!(w.flush_to_l0().unwrap(), "flush resumes after release");
        assert!(w.compact_l0().unwrap(), "compaction resumes after release");
        assert_eq!(w.l0_len(), 1, "three segments compacted to one");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A panic **while the writer lock is held** must not take the graph's write path down
    /// with it, and must not leave a torn delta behind.
    ///
    /// `retire` calls the caller's `resolve` closure under the writer mutex, so a panicking
    /// resolver is a real panic-while-held — the same shape as the `compact_l0`
    /// `unreachable!` that shipped as a permanent per-graph write outage. Before the fix
    /// this test dies twice over: the `std` mutex is **poisoned**, so the very next
    /// `write()` panics on `.expect("delta writer lock")` (and so does every later write,
    /// flush, compaction and `l0_len()` — until the process restarts); and `retire` had
    /// already cleared the L0 stack *before* running the panicking replay, so simply making
    /// the lock non-poisoning (e.g. `parking_lot`) would instead let the next write publish
    /// a delta whose sealed L0 level has silently vanished. Both are asserted here.
    #[test]
    fn panic_under_the_writer_lock_leaves_the_graph_writable_and_untorn() {
        let dir = tmp("panic_under_lock");
        let _ = std::fs::remove_dir_all(&dir);
        let old_core = GenId(uuid::Uuid::from_u128(30));
        let new_core = GenId(uuid::Uuid::from_u128(31));
        let w = DeltaWriter::open(&dir, "g", old_core, 100, 0, resolve_ticker).unwrap();

        // A (dense 10) is written and flushed, so its patch lives **only** in the sealed L0
        // level — a reader that loses the L0 stack loses this write.
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(1))],
            ),
            node(10),
        )
        .unwrap();
        assert!(w.flush_to_l0().unwrap(), "memtable had writes to flush");
        assert_eq!(w.l0_len(), 1);

        let frozen = w.freeze().unwrap();

        // A post-freeze write, so retire's replay has a record to hand to `resolve`.
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("B".into()),
                &[("price", Value::Int(2))],
            ),
            node(20),
        )
        .unwrap();
        let epoch_before = w.epoch();

        // Retire with a resolver that panics — i.e. a panic inside the writer's critical
        // section, holding the mutex.
        let boom = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            w.retire(
                &frozen.consumed,
                &frozen.consumed_l0,
                new_core,
                100,
                0,
                |_op: &WalOp| -> OpResolution { panic!("resolver blew up under the writer lock") },
            )
        }));
        assert!(boom.is_err(), "the resolver panicked under the writer lock");

        // 1. The graph is still WRITABLE. (Pre-fix: this panics on the poisoned mutex.)
        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("B".into()),
                &[("price", Value::Int(3))],
            ),
            node(20),
        )
        .expect("a panic under the lock must not end the graph's write path");
        assert!(w.epoch() > epoch_before, "the recovered write published");

        // 2. Nothing is TORN. The failed retire installed none of its state, so the writer
        //    still holds the pre-retire delta — the L0 stack included — and the write above
        //    republished *that*, not a half-retired one. (This is what a bare `parking_lot`
        //    swap would break: the pre-fix `retire` clears `l0` before the panicking replay,
        //    so the write above would have published a delta with no L0 level and A's patch
        //    would be gone.)
        assert_eq!(w.l0_len(), 1, "the L0 stack survived the panicking retire");
        let snap = w.delta_snapshot();
        assert_eq!(
            snap.node_patch(10)
                .and_then(|d| d.patches.get("price").cloned()),
            Some(Value::Int(1)),
            "the L0-only write is still readable after a panic under the lock",
        );
        assert_eq!(
            snap.node_patch(20)
                .and_then(|d| d.patches.get("price").cloned()),
            Some(Value::Int(3)),
            "the post-panic write is readable",
        );
        assert_eq!(
            w.core_uuid(),
            old_core,
            "a retire that panicked did not re-bind the core",
        );

        // 3. Every other lock-taking read path still works (nothing is poisoned).
        assert!(w.bytes() > 0);
        assert!(w.total_bytes() > 0);
        assert!(w.delta_entity_count() > 0);
        assert_eq!(w.wal_dir(), dir);

        // 4. The retire is retryable — a clean resolver completes it (the deleted consumed
        //    paths are tolerated), so the graph converges rather than needing a restart.
        w.retire(
            &frozen.consumed,
            &frozen.consumed_l0,
            new_core,
            100,
            0,
            resolve_ticker,
        )
        .expect("retire is retryable after a panicking one");
        assert_eq!(w.core_uuid(), new_core);
        assert_eq!(w.l0_len(), 0, "the retry retired the L0 stack");

        // 5. …and so does maintenance.
        assert!(w.flush_to_l0().is_ok(), "flush works after the panic");
        assert!(w.compact_l0().is_ok(), "compaction works after the panic");
        assert!(w.freeze().is_ok(), "freeze works after the panic");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The memtable fold is copy-on-write, so a batch is applied whole or not at all: the
    /// published snapshot never shows a half-applied batch, and the memtable the writer
    /// keeps is the one it published.
    #[test]
    fn a_batch_is_applied_whole_and_shares_the_published_memtable() {
        let dir = tmp("batch_cow");
        let _ = std::fs::remove_dir_all(&dir);
        let w =
            DeltaWriter::open(&dir, "g", GenId(uuid::Uuid::nil()), 100, 0, resolve_ticker).unwrap();
        w.write_batch(&[
            (
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("A".into()),
                    &[("price", Value::Int(1))],
                ),
                node(10),
            ),
            (
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("B".into()),
                    &[("price", Value::Int(2))],
                ),
                node(20),
            ),
        ])
        .unwrap();

        let snap = w.snapshot();
        assert_eq!(
            snap.node_patch(10)
                .and_then(|d| d.patches.get("price").cloned()),
            Some(Value::Int(1)),
        );
        assert_eq!(
            snap.node_patch(20)
                .and_then(|d| d.patches.get("price").cloned()),
            Some(Value::Int(2)),
        );
        // The writer installed the very `Arc` it published (no second deep clone).
        assert!(Arc::ptr_eq(&snap, &lock_writer(&w.inner).mem));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn next_segment_number_advances_past_existing() {
        let dir = tmp("segno");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(next_segment_number(&dir).unwrap(), 0); // missing dir
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("0000000000.wal"), b"x").unwrap();
        std::fs::write(dir.join("0000000003.wal"), b"x").unwrap();
        std::fs::write(dir.join("notes.txt"), b"x").unwrap();
        assert_eq!(next_segment_number(&dir).unwrap(), 4);
        std::fs::remove_dir_all(&dir).ok();
    }
}
