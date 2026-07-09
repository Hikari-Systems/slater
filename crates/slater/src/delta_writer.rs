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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{Context, Result};
use graph_format::ids::{Generation as GenId, Value};
use slater_delta::{
    replay_dir, DeltaSnapshot, L0Segment, LevelRead, Memtable, OpResolution, Seq, WalOp, WalRecord,
    WalSink,
};

/// A frozen delta handed to consolidation: an immutable snapshot of every
/// committed write (the active memtable **and** every sealed L0 level), plus the
/// on-disk segments those writes live in. The snapshot feeds the merged-view dump;
/// the segments are deleted by [`DeltaWriter::retire`] once the fresh generation is
/// published (see Phase 1d / 4c in `docs/WRITABLE-PROGRESS.md`).
pub struct Frozen {
    /// The active memtable at freeze time — the newest delta level the dump folds in.
    pub snapshot: Arc<Memtable>,
    /// The sealed L0 levels at freeze time, **newest first** — the dump folds these
    /// beneath the active memtable via [`DeltaSnapshot::with_levels`].
    pub l0: Vec<Arc<Memtable>>,
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
    /// The authoritative active memtable; the published snapshot's newest level is a
    /// clone of it.
    mem: Memtable,
    /// The sealed L0 segments beneath the active memtable, **newest first** (empty on
    /// the common no-flush path). Each flush prepends one; consolidation clears them.
    l0: Vec<L0Segment>,
    /// The last sequence number assigned (0 before the first write).
    seq: Seq,
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
        let dir = dir.as_ref().to_path_buf();

        // Reload the sealed L0 segments first (Phase 4c). They stack past the core: the
        // active memtable resumes past every level's synthetic id space, so a WAL-tail
        // born node never collides with an already-flushed one. `l0` ends **newest
        // first** (the read-stack order); the bases are the max over levels (= the
        // newest level's `base + born`, since bases stack monotonically).
        let l0_dir = dir.join("l0");
        let mut l0: Vec<L0Segment> = Vec::new();
        let mut node_base = core_node_count;
        let mut edge_base = core_edge_count;
        for (_, path) in l0_segment_paths_sorted(&l0_dir)? {
            let seg =
                L0Segment::open(&path).with_context(|| format!("reload L0 segment {path:?}"))?;
            let m = seg.memtable();
            node_base = node_base.max(m.synthetic_base() + m.born_count());
            edge_base = edge_base.max(m.edge_synthetic_base() + m.born_edge_count());
            l0.push(seg);
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
        *self.core_uuid.read().expect("delta core-uuid lock")
    }

    /// A consistent immutable snapshot of the **active memtable** — one `Arc` clone, no
    /// writer contention. Used by the writer's single-memtable diagnostics and tests; a
    /// read overlay wants [`Self::delta_snapshot`] (which also carries the L0 levels).
    pub fn snapshot(&self) -> Arc<Memtable> {
        self.published
            .read()
            .expect("delta snapshot lock")
            .active_memtable()
            .clone()
    }

    /// The full published delta — the active memtable **and** every sealed L0 level,
    /// folded atomically. A query pins this for its whole life; the read overlay
    /// (`server::delta_for_read`) builds its `MergedView` from it.
    pub fn delta_snapshot(&self) -> DeltaSnapshot {
        self.published.read().expect("delta snapshot lock").clone()
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
        self.published
            .read()
            .expect("delta snapshot lock")
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
        self.published
            .read()
            .expect("delta snapshot lock")
            .born_synthetic_for_identity(label, key, value)
    }

    /// Publish `mem ⊕ l0` as one atomic [`DeltaSnapshot`], so a lock-free reader never
    /// observes a half-applied flush (data in neither or both of the memtable and a new
    /// L0 level). Called under the writer lock after every state change.
    fn republish(&self, inner: &WriterInner) {
        let published = published_snapshot(&inner.mem, &inner.l0);
        *self.published.write().expect("delta snapshot lock") = published;
    }

    /// The current delta epoch (monotonic; bumps on every published write).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Durably apply one write: append the record, commit (fsync — the ack
    /// barrier), fold it into the authoritative memtable, and publish the new
    /// snapshot. Returns the durable sequence number. `resolved` is the caller's
    /// resolved dense-id context ([`OpResolution`]) — a `None` endpoint marks a
    /// delta-born node/edge (Phase 2/3).
    pub fn write(&self, op: WalOp, resolved: OpResolution) -> Result<Seq> {
        let mut inner = self.inner.lock().expect("delta writer lock");
        let seq = inner.seq.next();
        let rec = WalRecord { seq, op };
        inner.sink.append(&rec).context("append WAL record")?;
        inner.sink.commit(seq).context("commit WAL batch")?;
        inner.seq = seq;
        inner.mem.apply(&rec.op, resolved);
        // Publish the new delta (active memtable ⊕ unchanged L0 levels), then bump the
        // epoch so readers keying on it see the new state (publish-before-bump: an
        // observer that reads the higher epoch also sees the swapped-in snapshot).
        self.republish(&inner);
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(seq)
    }

    /// Durably apply a **batch** of writes under a single group commit — the fix for the
    /// one-fsync-per-statement cost that dominates bulk-write throughput. It appends every
    /// record, then does **one** `commit` (a single fsync — the ack barrier for the whole
    /// batch), folds all ops into the memtable, and does **one** publish + epoch bump.
    /// Returns the durable sequence number of the last op (the whole batch is durable
    /// once this returns).
    ///
    /// **Atomic on failure:** the memtable is applied only *after* the commit fsync
    /// succeeds, so if any append or the commit fails, no op in the batch is applied or
    /// published, and the un-committed records are dropped on replay (no commit marker) —
    /// the batch is rejected whole, never half-applied. An empty batch is a no-op.
    pub fn write_batch(&self, ops: &[(WalOp, OpResolution)]) -> Result<Seq> {
        let mut inner = self.inner.lock().expect("delta writer lock");
        if ops.is_empty() {
            return Ok(inner.seq);
        }
        // 1. Append every record (fast; no fsync yet).
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
        // 2. One commit = one fsync for the whole batch (the ack barrier).
        inner.sink.commit(last).context("commit WAL batch")?;
        // 3. Fold every op into the authoritative memtable (only after the fsync).
        for (op, resolved) in ops {
            inner.mem.apply(op, *resolved);
        }
        // 4. One publish + epoch bump for the batch (publish-before-bump, as in `write`).
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
        let mut inner = self.inner.lock().expect("delta writer lock");
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
        let snapshot = Arc::new(inner.mem.clone());
        let l0: Vec<Arc<Memtable>> = inner.l0.iter().map(|s| s.memtable().clone()).collect();
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
    pub fn flush_to_l0(&self) -> Result<bool> {
        let mut inner = self.inner.lock().expect("delta writer lock");
        // Checked under the lock so it serialises with freeze/retire: a consolidation
        // must not have an L0 segment appear between its freeze and its retire.
        if self.consolidating.load(Ordering::Acquire) {
            return Ok(false);
        }
        if inner.mem.is_empty() {
            return Ok(false);
        }

        // 1. Seal the active memtable to a fresh, content-checked L0 file (fsync-durable).
        let l0_dir = inner.dir.join("l0");
        std::fs::create_dir_all(&l0_dir)
            .with_context(|| format!("create L0 directory {l0_dir:?}"))?;
        let n = next_l0_number(&l0_dir)?;
        let path = l0_dir.join(format!("{n:010}.l0"));
        L0Segment::write(&inner.mem, &path)
            .with_context(|| format!("write L0 segment {path:?}"))?;
        let seg = L0Segment::open(&path).with_context(|| format!("reopen L0 segment {path:?}"))?;

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

        inner.mem = Memtable::with_bases(node_base, edge_base);
        inner.l0.insert(0, seg); // newest-first

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
    /// that run via [`Memtable::merge_levels`] (reclaiming overwritten patches + shadowed
    /// tombstones and collapsing read fan-out within the tier), leaving differently-sized
    /// levels alone so a large level is never repeatedly rewritten with tiny new ones
    /// (write amplification). A no-op (returns `false`) with fewer than two levels, or
    /// when no two adjacent levels are same-tier (a healthy size ladder). This is
    /// self-balancing: equal-sized flushes form same-tier runs that merge, and the merged
    /// results are themselves same-tier and merge in turn, so fan-out stays bounded.
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
    pub fn compact_l0(&self) -> Result<bool> {
        let mut inner = self.inner.lock().expect("delta writer lock");
        // As with `flush_to_l0`: never mutate the L0 stack during a consolidation.
        if self.consolidating.load(Ordering::Acquire) {
            return Ok(false);
        }
        if inner.l0.len() < 2 {
            return Ok(false);
        }

        // Pick a contiguous run of similar-sized levels (over the newest-first stack).
        let sizes: Vec<u64> = inner
            .l0
            .iter()
            .map(|s| s.memtable().bytes() as u64)
            .collect();
        let Some((start, end)) = select_compaction_run(&sizes) else {
            return Ok(false); // a healthy size ladder — nothing same-tier to merge
        };

        // Merge the run (newest-first) into one equivalent segment.
        let run: Vec<&Memtable> = inner.l0[start..end]
            .iter()
            .map(|s| s.memtable().as_ref())
            .collect();
        let merged = Memtable::merge_levels(&run);

        // Reuse the run's OLDEST file slot (`inner.l0[end - 1]`) — its number and born-id
        // base are the run's minimum, matching the merged segment's base, so on-disk
        // number order stays == age/base order without reconciling in `open`.
        let oldest_path = inner.l0[end - 1].path().to_path_buf();
        L0Segment::write(&merged, &oldest_path)
            .with_context(|| format!("write compacted L0 segment {oldest_path:?}"))?;
        let seg = L0Segment::open(&oldest_path)
            .with_context(|| format!("reopen compacted L0 segment {oldest_path:?}"))?;

        // The run's newer members (all but the oldest slot we overwrote) are now folded
        // into the merged segment; delete them after publishing.
        let consumed: Vec<PathBuf> = inner.l0[start..end - 1]
            .iter()
            .map(|s| s.path().to_path_buf())
            .collect();

        // Splice the run out, in place, for the single merged segment (stack stays
        // newest-first + number-ordered).
        inner.l0.splice(start..end, std::iter::once(seg));

        // Publish the collapsed stack, then delete the consumed files (durable in the
        // merged file) — publish-before-delete so a reader never loses the data.
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
        let mut inner = self.inner.lock().expect("delta writer lock");
        // The consumed WAL segments' + L0 levels' writes are now in the new core — drop
        // them. The currently-open (post-freeze) WAL segment is never in `consumed`
        // (freeze rotated to it), so it survives and keeps taking appends after this
        // rebuild. Every L0 level present at freeze was folded into the new core, so the
        // whole stack retires; 4c-B does not admit a flush during a consolidation (that
        // in-flight guard is 4d), so the stack at retire is exactly `consumed_l0`.
        for path in consumed {
            remove_if_present(path)?;
        }
        for path in consumed_l0 {
            remove_if_present(path)?;
        }
        inner.l0.clear();

        // Rebuild the live memtable from the surviving (post-freeze) WAL segments, each
        // write re-resolved against the new core. Re-base the synthetic id spaces on the
        // freshly built core: its node/edge counts now include the folded-in delta-born
        // entities (including any that were flushed to an L0 level), so a post-freeze
        // born id starts past them and a post-freeze re-write of a folded born key
        // re-resolves to its now-real dense id.
        let mut mem = Memtable::with_bases(new_core_node_count, new_core_edge_count);
        let replay = replay_dir(&inner.dir)
            .with_context(|| format!("replay post-freeze WAL dir {:?}", inner.dir))?;
        for rec in &replay.records {
            mem.apply(&rec.op, resolve(&rec.op));
        }
        inner.seq = replay.last_seq;
        inner.mem = mem;

        // Publish the rebuilt overlay first (no L0 now), then re-bind the core UUID (see
        // the ordering note above). The seq counter stays monotonic from the replayed
        // high-water mark.
        self.republish(&inner);
        *self.core_uuid.write().expect("delta core-uuid lock") = new_core_uuid;
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Number of distinct node identities currently carrying a delta (diagnostics).
    pub fn node_delta_count(&self) -> usize {
        self.snapshot().node_delta_count()
    }

    /// Approximate resident **active-memtable** size in bytes — checked against the
    /// memtable→L0 flush cap (a full memtable flushes; the L0 levels don't count here).
    pub fn bytes(&self) -> usize {
        self.inner.lock().expect("delta writer lock").mem.bytes()
    }

    /// Approximate resident size of the **whole** delta (active memtable + every L0
    /// level) — checked against the total-delta soft/hard caps (Phase 4d).
    pub fn total_bytes(&self) -> usize {
        let inner = self.inner.lock().expect("delta writer lock");
        inner.mem.bytes() + inner.l0.iter().map(|s| s.memtable().bytes()).sum::<usize>()
    }

    /// The number of sealed L0 levels currently overlaid (diagnostics / tests).
    pub fn l0_len(&self) -> usize {
        self.inner.lock().expect("delta writer lock").l0.len()
    }

    /// Total changed-entity count across the whole delta (nodes + edges, summed over
    /// every level). Compared against a fraction of the core's entity count to decide
    /// when to fire a background consolidation (Phase 4d-ii-b) — an over-estimate when
    /// an entity is touched in several levels, which only makes the trigger fire a
    /// little sooner (safe).
    pub fn delta_entity_count(&self) -> usize {
        let s = self.published.read().expect("delta snapshot lock");
        s.node_delta_count() + s.edge_delta_count()
    }

    /// The directory holding this graph's WAL segments.
    pub fn wal_dir(&self) -> PathBuf {
        self.inner.lock().expect("delta writer lock").dir.clone()
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
/// stack (newest-first): clone the memtable into a fresh level and gather the L0
/// segments' immutable memtable handles.
fn published_snapshot(mem: &Memtable, l0: &[L0Segment]) -> DeltaSnapshot {
    let mem = Arc::new(mem.clone());
    let levels: Vec<Arc<dyn LevelRead>> = l0
        .iter()
        .map(|s| s.memtable().clone() as Arc<dyn LevelRead>)
        .collect();
    DeltaSnapshot::with_levels(mem, levels)
}

/// Refine a base (core-only) resolution against the sealed L0 levels: a node/endpoint
/// key that the core reports Absent (`None`) but that is a **delta-born** node resident
/// in an L0 level resolves to that born node's existing synthetic id, so a re-`MERGE`
/// on the WAL-tail replay path reuses it rather than allocating a duplicate (Phase
/// 4c-B). Mirrors the live write path's `DeltaWriter::born_synthetic_for_identity`.
fn resolve_with_l0(op: &WalOp, base: OpResolution, l0: &[L0Segment]) -> OpResolution {
    let born = |(label, key, value): (&str, &str, &Value)| {
        l0.iter()
            .find_map(|s| s.memtable().born_synthetic_for_identity(label, key, value))
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

/// Remove `path`, tolerating an already-absent file (idempotent cleanup).
fn remove_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("remove file {path:?}")),
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
