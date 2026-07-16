// SPDX-License-Identifier: Apache-2.0
//! The RW-index **lifecycle**: how [`graph_format::rwvamana::RwVamana`] is kept in step with
//! the write delta, and how a query is guaranteed to see an index that matches the delta it
//! is reading (FreshDiskANN T0, HIK-112).
//!
//! # The temp index is a cache of the delta. The delta is the durable thing.
//!
//! Nothing here is persisted, and nothing here needs to be. **No WAL format change, no new
//! on-disk state, no recovery path.** The delta is already WAL-backed and size-bounded; this
//! index is a pure function of it, so a crash rebuilds it by re-reading the replayed delta.
//! It is not even rebuilt at startup — the first KNN query that wants it builds it. Someone
//! will eventually want to persist it: don't.
//!
//! # Why it replaces a brute force
//!
//! `exec::apply_vector_call`'s delta arm used to rebuild a `ResidentMatrix` over the *entire*
//! delta on **every query** and scan it. That was deliberate, not an oversight: caching one
//! matrix per level would grow `VectorIndexCache`'s pinned set without bound (matrices are
//! charged to the budget but never evicted — the Σ-over-levels pinning trap, D63). This index
//! is not in that pool at all: it is derived state bounded by the delta, with its own
//! accounting and its own `maxVectors` valve.
//!
//! # The atomicity argument — read this before touching anything below
//!
//! A query pins a `(DeltaSnapshot, epoch)` **pair** for its whole life ([`crate::server`]'s
//! `ReadOverlay`, taken under one lock). The index it uses is advanced to **exactly** that
//! epoch, re-resolving each changed node id **against the query's own pinned snapshot**. So
//! the index at cut `E` is a *pure function of the snapshot at `E`* — it cannot observe a
//! delta level whose index it lacks, because it is *derived from* that level.
//!
//! Three rules keep that airtight, and each of them is load-bearing:
//!
//! 1. **The index serves a query only when `index_epoch == E`.** Behind (`< E`) → advance,
//!    under the write guard, from the [`TouchedJournal`]. Ahead (`> E`, another query
//!    overtook us) → **fall back to the brute-forced matrix**; the same answer, a different
//!    cost. There is no "state as of an epoch I never saw": a node touched at epochs 50 *and*
//!    80 has exactly one resolvable state per snapshot, and we only ever hold one snapshot.
//! 2. **Journal entries for `≤ E` are written before `E` is published**, and are immutable
//!    once written, so a reader holding `E` always finds them. If the journal has been
//!    trimmed below the index's epoch — or the generation changed under a consolidation,
//!    which rebases every dense id — the index **rebuilds from the snapshot**, whole. Never
//!    partially.
//! 3. **The cache is keyed by [`GenId`].** A consolidation publishes a new generation, which
//!    gets a fresh, empty holder; the old one is dropped. That is what makes `retire`'s dense
//!    id rebase safe *by construction* rather than by remembering to invalidate.
//!
//! # The seal window (and why there isn't one)
//!
//! HIK-112 warns of an interleaving where a vector lives in a sealed L0 level whose index has
//! been dropped but whose core segment is not yet published — vectors vanishing from KNN with
//! nothing to say so. In *this* ladder that window is not reachable, and it is worth writing
//! down why, because the next person will re-derive the fear:
//!
//! * `DeltaWriter::flush_to_l0` moves the active memtable into `DeltaSnapshot::l0` and resets
//!   the memtable — but `l0` is part of the **same published snapshot**, and the delta arm
//!   resolves through `DeltaSnapshot::node_patch`, which folds `mem ⊕ every L0` newest-wins.
//!   A seal does not change what the delta says about any node id, so the index needs no
//!   change either: `flush_to_l0` journals an **empty** touched set, deliberately.
//! * `compact_l0` merges L0 levels into one. Same fold, same answer, same empty journal entry.
//! * The L0 → core-segment publish (`Server::flush_graph_to_segment`) publishes the segment
//!   **before** it retires the delta, and while both exist the delta's `superseded()`
//!   suppresses the segment arm. The vectors are in two levels for a moment; never in none.
//!
//! An index over `mem ⊕ L0` therefore closes the window by construction — there is nothing to
//! synchronise across a seal. `rw_index_ladder_survives_a_seal` pins that.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use anyhow::Result;

use graph_format::ids::Generation as GenId;
use graph_format::manifest::VectorIndexDesc;
use graph_format::rwvamana::RwVamana;

/// What the **write delta** says about one node's embedding for one vector index — the exact
/// three-way contract `exec::vector_levels`' delta half computes, and the only thing this
/// module needs to know about the delta.
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaVector {
    /// The delta gives this node a vector. It supersedes whatever the levels below hold.
    Set(Vec<f32>),
    /// The delta **took the embedding away** (`REMOVE n.embedding`, a `SET n = {…}` that
    /// dropped it, an overwrite with a non-vector). It supersedes the levels below with
    /// *nothing* — which is why absence cannot express it and it needs its own state (D12).
    Gone,
    /// The delta took the node **out of the index's scope** (`REMOVE n:Label`) without touching
    /// the embedding value. Identical to [`Self::Gone`] for *this* module — a scan must suppress
    /// the levels below either way, and the node is not in the index — but the two are different
    /// facts about the vector, and a consolidation must tell them apart or it destroys a vector
    /// HIK-118 promised a later `SET n:Label` would restore (HIK-122). See
    /// [`exec::VectorLevel::out_of_scope`](crate::exec::VectorLevel::out_of_scope).
    OutOfScope,
    /// The delta says nothing about this node's embedding, *or* the node is tombstoned, *or*
    /// it does not carry the index's label and never did. It contributes nothing and supersedes
    /// nothing — whatever a lower level holds still stands.
    Silent,
}

// ── Config ─────────────────────────────────────────────────────────────────────

/// The three safety valves. A recall or memory regression in production is then one config
/// flip from neutralised, and every fallback path is the brute-forced matrix that shipped
/// before this index existed — the same results, a different cost.
#[derive(Debug, Clone, Copy)]
pub struct RwIndexConfig {
    /// Kill switch. Off ⇒ every delta arm brute-forces, exactly as it did pre-HIK-112.
    pub enabled: bool,
    /// Below this many touched delta nodes, don't even build: a linear scan of a resident
    /// matrix beats a graph walk at small `n`, and a proximity graph's recall at tiny `n` is
    /// not free.
    pub min_vectors: usize,
    /// Above this many **slots** (rows, dead ones included — an update appends and never
    /// reclaims), refuse the index and brute-force.
    ///
    /// It bounds the resident set, but what really sizes it is the **rebuild**: re-inserting the
    /// whole delta costs ~2 ms/vector at dim 768 (measured — `graph_format::rwvamana`), on the
    /// read path, under the write guard. See `config::default_rw_max_vectors`.
    pub max_vectors: usize,
}

impl Default for RwIndexConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_vectors: 2_000,
            max_vectors: 50_000,
        }
    }
}

// ── The touched-id journal ─────────────────────────────────────────────────────

/// Per-epoch dense node ids the writer touched — the *only* thing `DeltaWriter` has to know
/// about vectors, which is to say nothing at all.
///
/// Maintaining the index inside the writer would need a node's **effective label set** — core
/// labels (a block read through a `Generation`) ∪ `labels_added` ∖ `labels_removed` — and the
/// writer holds neither a generation nor a `ReadView`. Handing it one means it holds a
/// generation that swaps under it. So the writer publishes only what it already knows (a
/// handful of `u64`s per commit) and every vector decision stays on the read side, where the
/// `ReadView` lives.
///
/// The journal is **append-only and monotone in epoch**, and each entry is written *before*
/// its epoch is published — so a reader holding epoch `E` can always resolve `(from, E]`, or
/// learn definitively that it cannot (the range fell off the back) and rebuild whole.
#[derive(Debug, Default)]
pub struct TouchedJournal {
    inner: Mutex<JournalInner>,
}

#[derive(Debug, Default)]
struct JournalInner {
    /// `(epoch, touched dense node ids)`, ascending by epoch, contiguous.
    entries: VecDeque<(u64, Vec<u64>)>,
    /// The smallest `from` [`TouchedJournal::since`] can still serve. Raised when an entry is
    /// trimmed off the back, and reset outright by a consolidation.
    floor: u64,
    /// Retained ids, for the trim bound.
    ids: usize,
}

/// Retention bounds. Generous — an index that falls this far behind is one that has not been
/// queried in a very long time, and a full rebuild for it is correct anyway.
const JOURNAL_MAX_ENTRIES: usize = 8_192;
const JOURNAL_MAX_IDS: usize = 1 << 20;

impl TouchedJournal {
    /// A journal for a writer whose first published epoch is `epoch` (no entry — nothing has
    /// been touched yet, so `since(epoch, epoch)` is trivially empty).
    pub fn new(epoch: u64) -> Self {
        Self {
            inner: Mutex::new(JournalInner {
                entries: VecDeque::new(),
                floor: epoch,
                ids: 0,
            }),
        }
    }

    /// Record the ids one published epoch touched. Called **under the writer lock, before the
    /// snapshot for `epoch` is published** — that ordering is what rule 2 in the module doc
    /// rests on.
    ///
    /// A structural change that leaves `mem ⊕ L0` unchanged (a seal, a compaction) records an
    /// **empty** list rather than nothing at all: the epoch still bumps, so the journal must
    /// still be able to answer for it.
    pub fn record(&self, epoch: u64, ids: Vec<u64>) {
        let mut g = lock(&self.inner);
        g.ids += ids.len();
        g.entries.push_back((epoch, ids));
        while g.entries.len() > JOURNAL_MAX_ENTRIES
            || (g.ids > JOURNAL_MAX_IDS && g.entries.len() > 1)
        {
            let Some((e, dropped)) = g.entries.pop_front() else {
                break;
            };
            g.ids -= dropped.len();
            // Having dropped `e`, the oldest we still hold is `e + 1`, so the oldest `from`
            // we can serve is exactly `e`.
            g.floor = e;
        }
    }

    /// Forget everything: a consolidation rebuilt the delta against a **new core**, rebasing
    /// every dense id, so no id below is meaningful any more. (The index cache is keyed by
    /// `GenId` and would rebuild regardless — this is the belt to that pair of braces.)
    pub fn reset(&self, epoch: u64) {
        let mut g = lock(&self.inner);
        g.entries.clear();
        g.ids = 0;
        g.floor = epoch;
    }

    /// Every dense node id touched in `(from, to]`, or `None` when the journal cannot cover
    /// the range — in which case the caller must rebuild from the snapshot rather than guess.
    ///
    /// Ids may repeat (a node touched at several epochs); the caller re-resolves each against
    /// one snapshot, so a repeat is idempotent.
    pub fn since(&self, from: u64, to: u64) -> Option<Vec<u64>> {
        if to == from {
            return Some(Vec::new());
        }
        if to < from {
            return None; // the caller is *ahead* of us: it must not use this index at all
        }
        let g = lock(&self.inner);
        if from < g.floor {
            return None; // trimmed off the back
        }
        let mut out = Vec::new();
        let mut expect = from + 1;
        for (e, ids) in &g.entries {
            if *e <= from {
                continue;
            }
            if *e > to {
                break;
            }
            if *e != expect {
                return None; // a gap — refuse rather than silently skip an epoch's writes
            }
            expect += 1;
            out.extend_from_slice(ids);
        }
        (expect == to + 1).then_some(out)
    }
}

// ── One index ──────────────────────────────────────────────────────────────────

/// The RW-index for one `(label, property)` vector index, plus the bookkeeping that makes it
/// answerable: which delta epoch it is an exact function of, and which node ids the delta
/// **supersedes** at lower levels.
pub struct RwVectorIndex {
    graph: RwVamana,
    /// Node ids whose lower-level entry the delta invalidates — every id the delta gives a
    /// vector *or* takes one away from. This is `exec::VectorLevel::superseded()` for the
    /// delta, maintained incrementally so the query need not re-walk the delta to compute it.
    /// Getting it wrong is not a slow query, it is a **duplicate or a missing node** in the
    /// merged top-k (see `vector::merge_topk`).
    superseded: HashSet<u64>,
    /// The delta epoch this index exactly reflects. `None` = never built, or poisoned by a
    /// failed advance (a half-advanced index must never be served: it holds *some* ids at a
    /// newer epoch than its own).
    epoch: Option<u64>,
    /// Sticky for this generation: the delta outgrew `maxVectors`. It only shrinks at a
    /// consolidation, which mints a new `GenId` and hence a fresh holder — so re-checking
    /// per query would just rebuild-and-refuse forever.
    refused: bool,
}

impl RwVectorIndex {
    fn new(desc: &VectorIndexDesc) -> Self {
        Self {
            graph: RwVamana::new(desc.dim as usize, desc.metric),
            superseded: HashSet::new(),
            epoch: None,
            refused: false,
        }
    }

    /// An empty index over the same vectors — the shape a rebuild starts from.
    fn new_like(other: &Self) -> Self {
        Self {
            graph: RwVamana::new(other.graph.dim(), other.graph.metric()),
            superseded: HashSet::new(),
            epoch: None,
            refused: false,
        }
    }

    /// The delta epoch this index exactly reflects, if any.
    pub fn epoch(&self) -> Option<u64> {
        self.epoch
    }

    /// The nodes the delta supersedes at every level below it — the suppression set the
    /// segment and base arms scan with.
    pub fn superseded(&self) -> &HashSet<u64> {
        &self.superseded
    }

    pub fn graph(&self) -> &RwVamana {
        &self.graph
    }

    /// Live vectors the delta contributes.
    pub fn live_count(&self) -> usize {
        self.graph.live_count()
    }

    /// Resident footprint. Charged **separately** from `vectorCacheBytes`: this is derived
    /// state bounded by the delta, not a cache of the core.
    pub fn resident_bytes(&self) -> usize {
        self.graph.resident_bytes()
            + self.superseded.len() * 2 * std::mem::size_of::<u64>()
            + std::mem::size_of::<Self>()
    }

    /// Fold one node's delta state in. The whole state machine, in one place:
    ///
    /// | the delta says | the graph | `superseded` |
    /// |---|---|---|
    /// | the delta says              | the graph                                 | `superseded` |
    /// |---|---|---|
    /// | [`DeltaVector::Set`]        | insert (delete-then-insert on a re-embed) | **in** |
    /// | [`DeltaVector::Gone`]       | remove (the slot stays a waypoint)        | **in** |
    /// | [`DeltaVector::OutOfScope`] | remove (the slot stays a waypoint)        | **in** |
    /// | [`DeltaVector::Silent`]     | remove                                    | **out** |
    ///
    /// `Gone`/`OutOfScope` vs `Silent` is the *sharp* distinction, and it is decided upstream by
    /// [`exec::delta_vector_for`]:
    /// * `Gone` — the delta **took the embedding away**: `REMOVE n.embedding`, a `SET n = {…}`
    ///   that dropped it, an overwrite with a non-vector. The node had a place in the index and
    ///   lost it, so the vector a level below still holds must be suppressed — superseded **in**.
    /// * `OutOfScope` — `REMOVE n:Label` left the index's scope (HIK-116). Same suppression, for
    ///   the same reason; the value is untouched, which only a consolidation cares about
    ///   (HIK-122), so this module folds it exactly like `Gone`.
    /// * `Silent` — the delta has **nothing to say** about a node's membership: it never
    ///   carried the label, or it was only patched on an unrelated property. Leaving it in
    ///   `superseded` would suppress the base's perfectly good vector for a node the delta was
    ///   never about — superseded **out**.
    ///
    /// All three non-`Set` states remove from the graph (a suppressed node stays a navigable
    /// waypoint, never an emitted hit); they differ only in `superseded`. Getting the split
    /// wrong is silent either way — a `Gone` misfiled as `Silent` leaves a de-labelled node
    /// scoring at its stale vector (the HIK-116 bug); the reverse hides a live base vector.
    fn apply(&mut self, id: u64, says: DeltaVector) -> Result<()> {
        match says {
            DeltaVector::Set(v) => {
                self.graph.insert(id, &v)?;
                self.superseded.insert(id);
            }
            DeltaVector::Gone | DeltaVector::OutOfScope => {
                self.graph.remove(id);
                self.superseded.insert(id);
            }
            DeltaVector::Silent => {
                self.graph.remove(id);
                self.superseded.remove(&id);
            }
        }
        Ok(())
    }
}

// ── The cache ──────────────────────────────────────────────────────────────────

/// Per-generation RW indexes, one `RwLock` per `(label, property)`.
///
/// Keyed by [`GenId`], which is what makes a consolidation safe: it publishes a new
/// generation, the new generation gets a fresh empty holder, and the stale one (whose dense
/// ids the rebase invalidated) is simply never consulted again.
///
/// At most [`MAX_LIVE_GENERATIONS`] holders are retained — the newest, plus the one the
/// queries still in flight across a swap are reading. A dropped holder's memory goes back
/// immediately unless such a query still holds its `Arc`, which is exactly right.
#[derive(Default)]
pub struct RwIndexCache {
    /// Insertion-ordered, newest **last**. Two entries, so a `Vec` beats a map.
    gens: Mutex<Vec<(GenId, Arc<GenIndexes>)>>,
}

/// The served generation, plus the one a query straddling a swap is still reading.
const MAX_LIVE_GENERATIONS: usize = 2;

/// One `(label, property)` index, shared: the writer of an *advance* takes the write guard,
/// every query takes the read guard.
pub type SharedIndex = Arc<RwLock<RwVectorIndex>>;

#[derive(Default)]
pub struct GenIndexes {
    per_index: Mutex<HashMap<(String, String), SharedIndex>>,
}

/// The outcome of asking for an index: either one that **exactly matches** the query's delta
/// epoch, or a reason to brute-force.
pub enum RwLookup {
    /// Advanced to the query's epoch. Serve from it (re-checking the epoch under the read
    /// guard — see [`read_at_epoch`]).
    Ready(SharedIndex),
    /// Brute-force this query's delta arm. Not an error — the fallback is the pre-HIK-112
    /// path, which produces the same answer.
    BruteForce,
}

/// Everything [`RwIndexCache::ensure`] needs about *which* index, at *which* cut, under *which*
/// valves. Bundled so the call is one context plus the two closures that reach into the
/// caller's snapshot, rather than eight positional arguments.
pub struct EnsureCtx<'a> {
    /// The generation the delta's dense ids are resolved against — the cache key, and what
    /// makes a consolidation's id rebase safe by construction.
    pub gen: GenId,
    pub desc: &'a VectorIndexDesc,
    /// The delta epoch of the caller's pinned snapshot. The index is advanced to **exactly**
    /// this, and served only at it.
    pub epoch: u64,
    pub cfg: &'a RwIndexConfig,
    pub journal: &'a TouchedJournal,
}

impl RwIndexCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resident bytes across every generation's indexes — for `/health`.
    pub fn resident_bytes(&self) -> usize {
        let gens: Vec<Arc<GenIndexes>> = lock(&self.gens).iter().map(|(_, g)| g.clone()).collect();
        gens.iter()
            .flat_map(|g| {
                let per: Vec<_> = lock(&g.per_index).values().cloned().collect();
                per.into_iter()
            })
            .map(|ix| read_lock(&ix).resident_bytes())
            .sum()
    }

    /// Drop every generation's indexes except `keep` — a retired generation's index must not
    /// sit resident forever. A query already holding the `Arc` keeps working (and is reading a
    /// snapshot of that generation, so it *should*).
    pub fn retain(&self, keep: GenId) {
        lock(&self.gens).retain(|(g, _)| *g == keep);
    }

    /// Drop everything (a graph was unserved).
    pub fn clear(&self) {
        lock(&self.gens).clear();
    }

    /// The delta epoch the `(gen, label, property)` index currently stands at, or `None` if it
    /// has never been built (or is poisoned, or refused).
    ///
    /// An index **serves** a query iff this equals the query's epoch — so this is also the only
    /// way to tell, from outside, that a query was answered by the index rather than by the
    /// brute-force fallback. Without it a test asserting "the answer is right" passes whether
    /// the index worked or was never consulted at all, which is no test.
    pub fn index_epoch(&self, gen: GenId, label: &str, property: &str) -> Option<u64> {
        let holder = lock(&self.gens)
            .iter()
            .find(|(g, _)| *g == gen)
            .map(|(_, h)| h.clone())?;
        let ix = lock(&holder.per_index)
            .get(&(label.to_string(), property.to_string()))
            .cloned()?;
        let epoch = read_lock(&ix).epoch;
        epoch
    }

    fn index_for(&self, gen: GenId, desc: &VectorIndexDesc) -> SharedIndex {
        let holder = {
            let mut gens = lock(&self.gens);
            match gens.iter().find(|(g, _)| *g == gen) {
                Some((_, h)) => h.clone(),
                None => {
                    let h = Arc::new(GenIndexes::default());
                    gens.push((gen, h.clone()));
                    // Evict the oldest holder. Bounded rather than cleared-on-swap because
                    // nothing here is told about swaps — and it does not need to be: an index
                    // is only ever consulted under the `GenId` it was built for.
                    while gens.len() > MAX_LIVE_GENERATIONS {
                        gens.remove(0);
                    }
                    h
                }
            }
        };
        let key = (desc.label.clone(), desc.property.clone());
        let mut per = lock(&holder.per_index);
        per.entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(RwVectorIndex::new(desc))))
            .clone()
    }

    /// Bring the `(gen, desc)` index to **exactly** `epoch`, or say why it cannot be used.
    ///
    /// * `candidates` — every dense node id the delta touches, for a rebuild from scratch
    ///   (`DeltaSnapshot::node_dense_ids`). Read lazily: an incremental advance never asks.
    /// * `resolve` — what the delta says about one node id, **resolved against the caller's
    ///   pinned snapshot**. This is the function that makes the index a pure function of that
    ///   snapshot, and it is the caller's because only the caller has a `ReadView`.
    ///
    /// On any resolve/insert error the index is left **poisoned** (`epoch = None`), never
    /// half-advanced: a half-advanced index holds some ids at a newer epoch than its own, and
    /// serving that is a silently-wrong top-k.
    pub fn ensure(
        &self,
        ctx: EnsureCtx<'_>,
        candidates: impl FnOnce() -> Vec<u64>,
        resolve: impl Fn(u64) -> Result<DeltaVector>,
    ) -> Result<RwLookup> {
        let EnsureCtx {
            gen,
            desc,
            epoch,
            cfg,
            journal,
        } = ctx;
        if !cfg.enabled {
            return Ok(RwLookup::BruteForce);
        }
        let ix = self.index_for(gen, desc);

        // Fast path: already exactly at our epoch. Taken under a *read* guard, so concurrent
        // queries at the same epoch do not serialise on a writer lock they do not need.
        //
        // The `min_vectors` gate is applied HERE too, not only on the build path below. It is a
        // *recall* floor, not just a speed heuristic: a Vamana graph over a handful of vectors
        // has genuinely worse recall than a linear scan, so serving a stamped-but-tiny index
        // would be a silent recall regression — and without this check the second query at an
        // epoch whose first query built-then-declined the index would do exactly that.
        {
            let g = read_lock(&ix);
            if g.refused || (g.epoch == Some(epoch) && g.live_count() < cfg.min_vectors) {
                return Ok(RwLookup::BruteForce);
            }
            if g.epoch == Some(epoch) {
                return Ok(RwLookup::Ready(ix.clone()));
            }
        }

        {
            let mut g = write_lock(&ix);
            // Re-check: another query may have advanced (or refused) it while we swapped guards.
            if g.refused {
                return Ok(RwLookup::BruteForce);
            }
            match g.epoch {
                Some(e) if e == epoch => return Ok(RwLookup::Ready(ix.clone())),
                // **Ahead of us.** Another query, holding a newer snapshot, advanced past our
                // epoch. We must not read it: it holds vectors our snapshot has not committed.
                // Brute-force instead — correct, just slower.
                Some(e) if e > epoch => return Ok(RwLookup::BruteForce),
                // Behind: try the journal. A gap (trimmed, or a generation reset) ⇒ rebuild.
                Some(e) => match journal.since(e, epoch) {
                    Some(ids) => advance(&mut g, epoch, &ids, cfg, resolve)?,
                    None => rebuild(&mut g, epoch, &candidates(), cfg, resolve)?,
                },
                None => rebuild(&mut g, epoch, &candidates(), cfg, resolve)?,
            }
            if g.refused || g.live_count() < cfg.min_vectors {
                return Ok(RwLookup::BruteForce);
            }
        }
        Ok(RwLookup::Ready(ix.clone()))
    }
}

/// Advance an index from its current epoch to `epoch` by re-resolving only the touched ids.
///
/// The ids come from the journal; each is re-resolved against the **caller's snapshot**, so
/// the result is the snapshot's state, not a replay of intermediate ones. An id touched at
/// several epochs in the range resolves once (idempotently) to its state at `epoch`.
fn advance(
    ix: &mut RwVectorIndex,
    epoch: u64,
    ids: &[u64],
    cfg: &RwIndexConfig,
    resolve: impl Fn(u64) -> Result<DeltaVector>,
) -> Result<()> {
    // Poison first: if any resolve below fails (or the thread unwinds), the index is left
    // unbuildable rather than half-advanced, and the next query rebuilds it whole.
    ix.epoch = None;
    let mut seen: HashSet<u64> = HashSet::with_capacity(ids.len());
    for &id in ids {
        if !seen.insert(id) {
            continue;
        }
        ix.apply(id, resolve(id)?)?;
    }
    if ix.graph.slots() > cfg.max_vectors {
        refuse(ix);
        return Ok(());
    }
    ix.epoch = Some(epoch);
    Ok(())
}

/// Rebuild an index from scratch over every id the delta touches. The startup path (no
/// persistence — see the module doc), the post-consolidation path, and the "the journal could
/// not cover the gap" path, all in one.
fn rebuild(
    ix: &mut RwVectorIndex,
    epoch: u64,
    candidates: &[u64],
    cfg: &RwIndexConfig,
    resolve: impl Fn(u64) -> Result<DeltaVector>,
) -> Result<()> {
    // Refuse *before* the O(n · L · R) build, not after: a delta that outgrew the valve would
    // otherwise be rebuilt-and-refused on every single query.
    if candidates.len() > cfg.max_vectors {
        let mut fresh = RwVectorIndex::new_like(ix);
        refuse(&mut fresh);
        *ix = fresh;
        return Ok(());
    }
    // Build into a *fresh* index and swap it in wholesale: a rebuild that fails part-way must
    // not leave the old index mutated (it would then be neither the old epoch nor the new).
    let mut fresh = RwVectorIndex::new_like(ix);
    for &id in candidates {
        fresh.apply(id, resolve(id)?)?;
    }
    // The candidate count is an upper bound on rows, so this can only fire if a *re-embed*
    // pushed the slot count past the valve. Refuse rather than serve an oversized index.
    if fresh.graph.slots() > cfg.max_vectors {
        refuse(&mut fresh);
    } else {
        fresh.epoch = Some(epoch);
    }
    *ix = fresh;
    Ok(())
}

/// Give up on this index for the life of the generation, and hand the memory back.
fn refuse(ix: &mut RwVectorIndex) {
    *ix = RwVectorIndex::new_like(ix);
    ix.refused = true;
}

// ── Lock helpers ───────────────────────────────────────────────────────────────
//
// A panic under one of these guards must not take the graph's KNN path down for the rest of
// the process: the index is a *cache*, so the worst a recovered guard can serve is a stale
// state — and a stale state cannot be served anyway (the epoch check refuses it). Mirrors
// `delta_writer`'s poison recovery for exactly the reason documented there.

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read_lock<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_lock<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(PoisonError::into_inner)
}

/// Take the read guard a [`RwLookup::Ready`] promises, re-checking the epoch under it.
///
/// The promise was made under a guard we then dropped, so re-check: between `ensure` and here
/// another query may have advanced the index past our epoch. This is the second half of rule
/// 1 — without it, `ensure`'s "exactly at `E`" guarantee is a TOCTOU.
pub fn read_at_epoch(ix: &SharedIndex, epoch: u64) -> Option<RwLockReadGuard<'_, RwVectorIndex>> {
    let g = read_lock(ix);
    (g.epoch == Some(epoch)).then_some(g)
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::manifest::{AnnMode, Metric};

    fn desc(label: &str, prop: &str, dim: u32) -> VectorIndexDesc {
        VectorIndexDesc {
            label: label.into(),
            property: prop.into(),
            dim,
            metric: Metric::L2,
            count: 0,
            first_record: 0,
            mode: AnnMode::BruteForce,
        }
    }

    fn cfg() -> RwIndexConfig {
        RwIndexConfig {
            enabled: true,
            min_vectors: 0,
            max_vectors: 1_000_000,
        }
    }

    fn ctx<'a>(
        gen: GenId,
        desc: &'a VectorIndexDesc,
        epoch: u64,
        cfg: &'a RwIndexConfig,
        journal: &'a TouchedJournal,
    ) -> EnsureCtx<'a> {
        EnsureCtx {
            gen,
            desc,
            epoch,
            cfg,
            journal,
        }
    }

    // ── the journal ────────────────────────────────────────────────────────────

    #[test]
    fn journal_covers_a_contiguous_range_and_refuses_a_gap() {
        let j = TouchedJournal::new(1);
        j.record(2, vec![10, 11]);
        j.record(3, vec![]); // a seal: the epoch bumps, the delta's content does not
        j.record(4, vec![12]);

        assert_eq!(j.since(1, 4), Some(vec![10, 11, 12]));
        assert_eq!(j.since(3, 4), Some(vec![12]));
        assert_eq!(j.since(4, 4), Some(vec![]), "already there");
        // Asking for an epoch the journal has not seen: refuse, do not silently under-report.
        assert_eq!(j.since(1, 9), None);
        // Asking to go *backwards* is a caller that is ahead of us: never serviceable.
        assert_eq!(j.since(4, 2), None);
    }

    #[test]
    fn journal_trim_refuses_rather_than_losing_writes() {
        // The trap: a journal that quietly forgets an epoch's ids would let an index "advance"
        // over a gap and never learn about the vectors written in it — a silent KNN miss.
        let j = TouchedJournal::new(1);
        for e in 2..=(JOURNAL_MAX_ENTRIES as u64 + 5) {
            j.record(e, vec![e]);
        }
        let last = JOURNAL_MAX_ENTRIES as u64 + 5;
        assert_eq!(
            j.since(1, last),
            None,
            "trimmed range must refuse, not skip"
        );
        // The retained tail still serves.
        assert_eq!(j.since(last - 1, last), Some(vec![last]));
    }

    #[test]
    fn journal_reset_invalidates_everything_before_it() {
        let j = TouchedJournal::new(1);
        j.record(2, vec![7]);
        assert_eq!(j.since(1, 2), Some(vec![7]));
        // A consolidation rebased every dense id: nothing below is meaningful.
        j.reset(9);
        assert_eq!(j.since(1, 9), None);
        assert_eq!(j.since(9, 9), Some(vec![]));
    }

    // ── the index state machine ────────────────────────────────────────────────

    /// A delta resolver over a hand-written table — independently-derived truth for what the
    /// delta says, with no dependency on `exec`'s resolution at all.
    fn table(entries: &[(u64, DeltaVector)]) -> impl Fn(u64) -> Result<DeltaVector> + '_ {
        move |id| {
            Ok(entries
                .iter()
                .find(|(i, _)| *i == id)
                .map(|(_, s)| s.clone())
                .unwrap_or(DeltaVector::Silent))
        }
    }

    #[test]
    fn superseded_holds_embedded_and_un_embedded_ids_and_nothing_else() {
        // `superseded` is what suppresses the base and segment arms. An id wrongly in it makes
        // a live base vector vanish; an id wrongly out of it makes a stale base vector come
        // back beside the delta's — a duplicate node id in the merged top-k.
        let cache = RwIndexCache::new();
        let g = GenId(uuid::Uuid::nil());
        let d = desc("Doc", "emb", 2);
        let j = TouchedJournal::new(1);
        let says = [
            (1u64, DeltaVector::Set(vec![1.0, 0.0])),
            (2, DeltaVector::Gone),
            (3, DeltaVector::Silent),
        ];

        let lk = cache
            .ensure(ctx(g, &d, 1, &cfg(), &j), || vec![1, 2, 3], table(&says))
            .unwrap();
        let RwLookup::Ready(ix) = lk else {
            panic!("expected an index")
        };
        let ix = read_at_epoch(&ix, 1).unwrap();
        let mut sup: Vec<u64> = ix.superseded().iter().copied().collect();
        sup.sort_unstable();
        assert_eq!(
            sup,
            vec![1, 2],
            "embedded (1) and un-embedded (2); NOT silent (3)"
        );
        assert_eq!(ix.live_count(), 1, "only the embedded id has a vector");
        assert!(ix.graph().contains(1));
        assert!(!ix.graph().contains(2));
    }

    /// A node that stops carrying the index's label (`REMOVE n:Label`) goes `Silent`. It must
    /// leave the graph **and** leave `superseded` — otherwise the delta keeps suppressing a
    /// base vector for a node it no longer says anything about, and that node vanishes from
    /// KNN entirely.
    #[test]
    fn a_node_that_goes_silent_leaves_the_graph_and_the_suppression_set() {
        let cache = RwIndexCache::new();
        let g = GenId(uuid::Uuid::nil());
        let d = desc("Doc", "emb", 2);
        let j = TouchedJournal::new(1);

        let embedded = [(5u64, DeltaVector::Set(vec![1.0, 0.0]))];
        let lk = cache
            .ensure(ctx(g, &d, 1, &cfg(), &j), || vec![5], table(&embedded))
            .unwrap();
        assert!(matches!(lk, RwLookup::Ready(_)));

        // Epoch 2 touches node 5, and now the delta says nothing about its embedding.
        j.record(2, vec![5]);
        let silent: [(u64, DeltaVector); 0] = [];
        let lk = cache
            .ensure(ctx(g, &d, 2, &cfg(), &j), || vec![5], table(&silent))
            .unwrap();
        let RwLookup::Ready(ix) = lk else {
            panic!("expected an index")
        };
        let ix = read_at_epoch(&ix, 2).unwrap();
        assert_eq!(ix.live_count(), 0);
        assert!(
            ix.superseded().is_empty(),
            "a silent node must suppress nothing: {:?}",
            ix.superseded()
        );
    }

    /// The overtake. A query pinned at epoch `E` must **never** read an index that has been
    /// advanced past `E` by another query — that index holds vectors `E`'s snapshot has not
    /// committed, and the suppression sets would not match them.
    #[test]
    fn a_query_behind_the_index_falls_back_rather_than_reading_the_future() {
        let cache = RwIndexCache::new();
        let g = GenId(uuid::Uuid::nil());
        let d = desc("Doc", "emb", 2);
        let j = TouchedJournal::new(1);
        j.record(2, vec![1]);

        let at2 = [(1u64, DeltaVector::Set(vec![1.0, 0.0]))];
        assert!(matches!(
            cache
                .ensure(ctx(g, &d, 2, &cfg(), &j), || vec![1], table(&at2))
                .unwrap(),
            RwLookup::Ready(_)
        ));
        // A straggler still holding the epoch-1 snapshot.
        assert!(
            matches!(
                cache
                    .ensure(ctx(g, &d, 1, &cfg(), &j), Vec::new, table(&at2))
                    .unwrap(),
                RwLookup::BruteForce
            ),
            "a query behind the index must brute-force, not read the future"
        );
        // And the guard re-check is the second half of it (the Arc could be handed out and the
        // index advanced before the read guard is taken).
        let ix = cache.index_for(g, &d);
        assert!(read_at_epoch(&ix, 1).is_none());
        assert!(read_at_epoch(&ix, 2).is_some());
    }

    #[test]
    fn a_failed_resolve_poisons_the_index_rather_than_half_advancing_it() {
        let cache = RwIndexCache::new();
        let g = GenId(uuid::Uuid::nil());
        let d = desc("Doc", "emb", 2);
        let j = TouchedJournal::new(1);

        let boom = |id: u64| -> Result<DeltaVector> {
            if id == 2 {
                anyhow::bail!("block read failed");
            }
            Ok(DeltaVector::Set(vec![1.0, 0.0]))
        };
        assert!(cache
            .ensure(ctx(g, &d, 1, &cfg(), &j), || vec![1, 2, 3], boom)
            .is_err());
        // Whatever it managed to apply, it must not be *serveable*: an index that holds id 1
        // but not id 3, stamped at epoch 1, is a silently-truncated KNN.
        let ix = cache.index_for(g, &d);
        assert_eq!(
            read_lock(&ix).epoch(),
            None,
            "must be poisoned, not stamped"
        );
        assert!(read_at_epoch(&ix, 1).is_none());
    }

    #[test]
    fn max_vectors_refuses_before_paying_for_the_build_and_stays_refused() {
        let cache = RwIndexCache::new();
        let g = GenId(uuid::Uuid::nil());
        let d = desc("Doc", "emb", 2);
        let j = TouchedJournal::new(1);
        let cfg = RwIndexConfig {
            enabled: true,
            min_vectors: 0,
            max_vectors: 4,
        };
        let says: Vec<(u64, DeltaVector)> = (0..10)
            .map(|i| (i, DeltaVector::Set(vec![i as f32, 0.0])))
            .collect();

        // The candidate count alone (10 > 4) refuses — `resolve` is never called, so a build
        // that would have cost O(n · L · R) is not paid for and then thrown away.
        let calls = std::cell::Cell::new(0usize);
        let counted = |id: u64| {
            calls.set(calls.get() + 1);
            table(&says)(id)
        };
        assert!(matches!(
            cache
                .ensure(ctx(g, &d, 1, &cfg, &j), || (0..10).collect(), counted)
                .unwrap(),
            RwLookup::BruteForce
        ));
        assert_eq!(calls.get(), 0, "refused before resolving anything");

        // And it stays refused for the life of the generation — otherwise every query would
        // rebuild-and-refuse.
        j.record(2, vec![0]);
        assert!(matches!(
            cache
                .ensure(ctx(g, &d, 2, &cfg, &j), || (0..10).collect(), table(&says))
                .unwrap(),
            RwLookup::BruteForce
        ));
    }

    #[test]
    fn min_vectors_brute_forces_without_refusing_so_a_growing_delta_gets_an_index() {
        let cache = RwIndexCache::new();
        let g = GenId(uuid::Uuid::nil());
        let d = desc("Doc", "emb", 2);
        let j = TouchedJournal::new(1);
        let cfg = RwIndexConfig {
            enabled: true,
            min_vectors: 3,
            max_vectors: 1000,
        };
        let small = [(1u64, DeltaVector::Set(vec![1.0, 0.0]))];
        assert!(matches!(
            cache
                .ensure(ctx(g, &d, 1, &cfg, &j), || vec![1], table(&small))
                .unwrap(),
            RwLookup::BruteForce
        ));

        // A SECOND query at the same (below-floor) epoch must ALSO brute-force. The first query
        // built the index and stamped it at epoch 1, so this exercises the *fast* path — which
        // had, before the fix, no `min_vectors` gate and would have served the tiny (low-recall)
        // index the first query correctly declined.
        assert!(matches!(
            cache
                .ensure(ctx(g, &d, 1, &cfg, &j), || vec![1], table(&small))
                .unwrap(),
            RwLookup::BruteForce
        ));

        // The delta grows past the floor: now it must get an index (min is not sticky).
        j.record(2, vec![2, 3]);
        let big: Vec<(u64, DeltaVector)> = (1..=3)
            .map(|i| (i, DeltaVector::Set(vec![i as f32, 0.0])))
            .collect();
        assert!(matches!(
            cache
                .ensure(ctx(g, &d, 2, &cfg, &j), || vec![1, 2, 3], table(&big))
                .unwrap(),
            RwLookup::Ready(_)
        ));
    }

    #[test]
    fn the_kill_switch_never_builds_anything() {
        let cache = RwIndexCache::new();
        let g = GenId(uuid::Uuid::nil());
        let d = desc("Doc", "emb", 2);
        let j = TouchedJournal::new(1);
        let cfg = RwIndexConfig {
            enabled: false,
            ..RwIndexConfig::default()
        };
        assert!(matches!(
            cache
                .ensure(
                    ctx(g, &d, 1, &cfg, &j),
                    || panic!("must not read candidates"),
                    |_| panic!("must not resolve"),
                )
                .unwrap(),
            RwLookup::BruteForce
        ));
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn a_generation_swap_gets_a_fresh_holder() {
        // A consolidation rebases every dense id. Keying the cache by `GenId` is what makes
        // that safe without anyone having to remember to invalidate.
        let cache = RwIndexCache::new();
        let d = desc("Doc", "emb", 2);
        let j = TouchedJournal::new(1);
        let old = GenId(uuid::Uuid::from_u128(1));
        let new = GenId(uuid::Uuid::from_u128(2));
        let says = [(1u64, DeltaVector::Set(vec![1.0, 0.0]))];

        cache
            .ensure(ctx(old, &d, 1, &cfg(), &j), || vec![1], table(&says))
            .unwrap();
        assert!(read_lock(&cache.index_for(old, &d)).epoch().is_some());
        // The new generation's holder knows nothing — it must rebuild, not inherit.
        assert!(read_lock(&cache.index_for(new, &d)).epoch().is_none());

        cache.retain(new);
        assert!(
            read_lock(&cache.index_for(old, &d)).epoch().is_none(),
            "the retired generation's index must be dropped"
        );
    }
}
