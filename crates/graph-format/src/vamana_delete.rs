// SPDX-License-Identifier: Apache-2.0
//! FreshVamana's `Delete` — **delete consolidation** of a base Vamana graph.
//!
//! A tombstoned record is a **hole**: its `.pq` id is [`HOLE`], so it is never emitted, but
//! it keeps its out-edges and stays a navigational waypoint (`vamana::beam_search`'s
//! `emit → None` contract). That is *correct* — pruning it from the walk would disconnect
//! whatever lies behind it — but it is not *free*: every beam search that expands a hole
//! pays **one block read for a record that can never be returned**, forever, and the holes
//! occupy beam slots that live candidates wanted. As the dead fraction grows, IO and recall
//! both decay, and nothing recovers them short of a full rebuild.
//!
//! This pass patches the dead records **out of the adjacency**. Afterwards **no reachable
//! node's adjacency names a hole**, so the dead records cost *zero* query IO — they are
//! simply never walked to again. It is the pass that makes vector deletes sustainable.
//!
//! ```text
//! dead    = { ordinals whose .pq id is HOLE } ∪ { extra tombstones }
//! patched = { i : !dead[i] } ∪ { medoid }          # everything a search can still reach
//!
//! for p in layout order:                           # sequential block reads
//!     if p ∉ patched:        emit unchanged        # an abandoned hole — unreachable now
//!     if adj[p] ∩ dead == ∅: emit unchanged        # the overwhelming majority
//!     else:
//!         C = live nodes reachable from p through the dead region   (the splice)
//!         adj[p] = robust_prune_over(p, C, alpha, r, points)
//! ```
//!
//! Splicing a deleted neighbour's own out-neighbours into `p`'s candidate pool and then
//! robust-pruning back to `R` is what preserves navigability through the region the dead
//! node used to bridge.
//!
//! # Output: holes, never compaction
//!
//! Deleted records stay at their layout ordinal, with their `.pq` id set to [`HOLE`]. A
//! layout ordinal *is* a record position and every adjacency entry in every record is an
//! ordinal, so compacting one record would renumber the global ordinal space and invalidate
//! the whole file — an O(N) rewrite of a ~370 GB file to reclaim a few per cent. Holes cost
//! only dead disk space, reclaimed at the next *full* rebuild.
//!
//! # The trap: never orphan the medoid
//!
//! [`crate::manifest::AnnMode::Vamana::medoid`] is the fixed entry point of **every** beam
//! search. A *deleted* medoid is fine — sentinel id, never emitted, still a waypoint. What
//! must never happen is this pass splicing away *its* out-edges: the entry point is then
//! isolated, every search expands exactly one node, and recall for the whole index silently
//! goes to **zero** — no error, no panic, every query still returning *something*.
//!
//! The entire defence is the `∪ { medoid }` term in `patched`: the medoid is never
//! *abandoned*, so its adjacency is rewritten (cleaned of holes) rather than left to rot,
//! and it always keeps out-edges. Note what is **not** special-cased: edges *to* a dead
//! medoid **are** spliced out of live adjacency, and the medoid **is** spliced *through*
//! like any other dead node.
//!
//! * Removing an in-edge to the medoid is free. `beam_search` seeds its beam with the medoid
//!   and expands it **first, always**, so it is in the expanded set before any neighbour list
//!   is even read: an edge to it can never cause a fetch and can never help navigation. The
//!   slot it wastes is better spent on a real neighbour. Keeping such an edge would also
//!   leave a live node pointing at a hole, which is the one thing this pass exists to
//!   prevent.
//! * Splicing *through* the medoid is likewise safe, and skipping it is not: a node whose
//!   only neighbour was the dead medoid would be left a **dead end**. Orphaning the medoid
//!   is the hazard; inheriting its out-neighbours (which robust-prune then filters) is not.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};

use crate::crypto::BlockCipher;
use crate::manifest::Metric;
use crate::pq::{ann_point, sq_l2, PqReader, PqWriter, ResidentPq, HOLE};
use crate::vamana::{robust_prune_over, PointSet, VamanaIndex, VamanaReader, VamanaWriter};

/// The scalar shape of one delete-consolidation pass.
#[derive(Debug, Clone, Copy)]
pub struct ConsolidateOpts {
    /// The index's fixed entry point (its layout ordinal). Never orphaned — see the module
    /// doc.
    pub medoid: VamanaIndex,
    /// Out-degree bound `R` — the same one the graph was built with.
    pub r: usize,
    /// Robust-prune long-edge factor — the same one the graph was built with.
    pub alpha: f32,
    /// The index's metric. With [`Self::max_norm`] and [`Self::space_dim`] it defines the
    /// **ANN space** ([`ann_point`]): the transform under which squared-L2 ranks the way the
    /// metric does. The `.vamana` stores **raw** vectors, and robust-prune's domination test
    /// is only sound over a true metric, so every distance this pass takes is measured in
    /// that space — exactly as the original build measured it.
    pub metric: Metric,
    /// `M = max‖x‖` over the indexed set (the dot/MIPS augmentation constant), from the
    /// MANIFEST. Read only for [`Metric::Dot`].
    pub max_norm: f64,
    /// The dimension of the ANN space — [`crate::pq::ann_pq_params`]'s `dim`, i.e. the
    /// `.pq` codebook's own `dim`. (Equal to the vector dim except for dot, which carries an
    /// extra subspace for the norm augmentation.)
    pub space_dim: usize,
    /// LRU capacity, in **records**. Mandatory, not an optimisation: `|C| ≤ R·(R+1)` and
    /// robust-prune walks the whole pool once per chosen neighbour, so an uncached pass
    /// would re-read the same records hundreds of times. See [`recommended_cache_records`].
    pub cache_records: usize,
}

/// The LRU capacity a pass over out-degree `r` wants: enough to hold a whole candidate pool
/// (`|C| ≤ r·(r+1)`) plus slack, so one `robust_prune_over` never evicts a record it is
/// still walking. Sizing below this is *correct* but re-reads the pool on every iteration.
pub fn recommended_cache_records(r: usize) -> usize {
    r.saturating_mul(r.saturating_add(1))
        .saturating_add(64)
        .max(1024)
}

/// What one pass did. `patched + unchanged == records`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ConsolidateStats {
    /// Records in the `.vamana` — holes included. Unchanged by the pass (no compaction).
    pub records: u64,
    /// Records that are holes after the pass (the `dead` set).
    pub dead: u64,
    /// Records that are **not** holes — the new `live_count` for the MANIFEST.
    pub live: u64,
    /// Records whose adjacency was recomputed (they named at least one hole).
    pub patched: u64,
    /// Records emitted byte-for-byte unchanged.
    pub unchanged: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

// ── The disk point set ─────────────────────────────────────────────────────────

/// One cached record: its raw stored vector (what gets re-emitted), the same vector mapped
/// into the ANN space (what every distance is measured in), and its out-neighbours (what the
/// splice walks).
///
/// Both vectors are held rather than deriving one from the other on each touch: `ann_point`
/// allocates and, for cosine, normalises, and the pass takes O(r·|C|) distances per patched
/// node. For cosine and L2 this doubles the per-record footprint, which is a trade the
/// offline builder can afford (the whole cache is `cache_records` records, not the index).
struct CachedNode {
    raw: Vec<f32>,
    ann: Vec<f32>,
    nbrs: Vec<VamanaIndex>,
}

/// An exact LRU over decoded records, keyed by layout ordinal.
///
/// Hand-rolled: the workspace has no LRU crate, and `graph-format` forbids `unsafe`, so the
/// usual intrusive-list trick is out. A monotonic touch counter into a `BTreeMap` gives an
/// exact LRU in O(log n) per touch with no unsafe and no eviction scan.
struct Lru {
    capacity: usize,
    /// ordinal → (last-touch tick, record)
    map: HashMap<VamanaIndex, (u64, Rc<CachedNode>)>,
    /// last-touch tick → ordinal (the eviction order; the first key is the LRU victim)
    order: BTreeMap<u64, VamanaIndex>,
    tick: u64,
}

impl Lru {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: HashMap::new(),
            order: BTreeMap::new(),
            tick: 0,
        }
    }

    fn get(&mut self, i: VamanaIndex) -> Option<Rc<CachedNode>> {
        let (old_tick, node) = self.map.get(&i).map(|(t, n)| (*t, n.clone()))?;
        self.order.remove(&old_tick);
        self.tick += 1;
        self.order.insert(self.tick, i);
        self.map.insert(i, (self.tick, node.clone()));
        Some(node)
    }

    fn put(&mut self, i: VamanaIndex, node: Rc<CachedNode>) {
        if let Some((old_tick, _)) = self.map.remove(&i) {
            self.order.remove(&old_tick);
        }
        while self.map.len() >= self.capacity {
            // `pop_first` on the tick order is the least-recently-used record.
            let Some((_, victim)) = self.order.pop_first() else {
                break;
            };
            self.map.remove(&victim);
        }
        self.tick += 1;
        self.order.insert(self.tick, i);
        self.map.insert(i, (self.tick, node));
    }
}

/// A [`PointSet`] over a `.vamana` on disk, behind the LRU.
///
/// The whole pass reads records through this one seam — the sequential emit walk, the
/// splice's dead-neighbour lookups, and robust-prune's distances alike — so a record read
/// once is paid for once. Layout order is BFS-from-medoid, so a node's neighbours are
/// *nearby in the file* and the LRU hits hard.
///
/// Not `Sync` (the cache is a `RefCell`): the pass is a single sequential walk, by design —
/// it is the sequential read order that makes the layout locality pay.
pub struct CachedPoints<'a> {
    reader: &'a VamanaReader,
    metric: Metric,
    max_norm: f64,
    space_dim: usize,
    records: usize,
    cache: RefCell<Lru>,
    hits: Cell<u64>,
    misses: Cell<u64>,
}

impl<'a> CachedPoints<'a> {
    pub fn new(reader: &'a VamanaReader, opts: &ConsolidateOpts) -> Self {
        Self {
            reader,
            metric: opts.metric,
            max_norm: opts.max_norm,
            space_dim: opts.space_dim,
            records: reader.len() as usize,
            cache: RefCell::new(Lru::new(opts.cache_records)),
            hits: Cell::new(0),
            misses: Cell::new(0),
        }
    }

    /// Record reads that the LRU served.
    pub fn hits(&self) -> u64 {
        self.hits.get()
    }

    /// Record reads that went to the block file — the pass's real IO.
    pub fn misses(&self) -> u64 {
        self.misses.get()
    }

    fn node(&self, i: VamanaIndex) -> Result<Rc<CachedNode>> {
        if let Some(n) = self.cache.borrow_mut().get(i) {
            self.hits.set(self.hits.get() + 1);
            return Ok(n);
        }
        self.misses.set(self.misses.get() + 1);
        let rec = self
            .reader
            .node(i)
            .with_context(|| format!("read .vamana record {i}"))?;
        // A neighbour ordinal is untrusted on-disk data. Reject a forged one here, once, with
        // a message that names the record — rather than letting it index a `dead[]` slice out
        // of bounds (a panic) deep inside the splice.
        for &nb in &rec.neighbours {
            ensure!(
                (nb as usize) < self.records,
                ".vamana record {i} names neighbour ordinal {nb}, but the file holds only {} \
                 records",
                self.records
            );
        }
        let ann = ann_point(self.metric, &rec.vector, self.max_norm, self.space_dim)
            .with_context(|| format!("map .vamana record {i} into the ANN space"))?;
        let node = Rc::new(CachedNode {
            raw: rec.vector,
            ann,
            nbrs: rec.neighbours,
        });
        self.cache.borrow_mut().put(i, node.clone());
        Ok(node)
    }
}

impl PointSet for CachedPoints<'_> {
    fn len(&self) -> usize {
        self.records
    }

    fn dist(&self, a: VamanaIndex, b: VamanaIndex) -> Result<f64> {
        let va = self.node(a)?;
        let vb = self.node(b)?;
        Ok(sq_l2(&va.ann, &vb.ann))
    }
}

// ── The pass ───────────────────────────────────────────────────────────────────

/// The splice: every **live** node reachable from `p` through the dead region.
///
/// The paper's formula follows exactly one hop — `⋃_{v ∈ adj[p] ∩ D} adj[v] \ D` — which is
/// wrong on a *chain* of deleted nodes. For `p → v → w` with **both** `v` and `w` dead,
/// `adj[v] \ D` drops `w` and never follows it: if `adj[p] = {v}` the candidate pool comes
/// out **empty** and `p` is left with no out-edges at all, even though a live node sits one
/// hop further on. So this walks the dead region breadth-first and collects the live nodes on
/// its boundary.
///
/// Bounded: it stops expanding once `r` dead records have been expanded **and** it has at
/// least one candidate — which keeps `|C| ≤ r·(r+1)` (the paper's bound) in every realistic
/// case. It keeps walking only while the pool is still empty, because an empty pool means a
/// dead-end record, and the only honest reason to accept one is that **no live node is
/// reachable from `p` at all**.
///
/// The result is sorted and deduped, so it does not depend on the traversal order — which is
/// what keeps the emitted `.vamana` byte-deterministic.
fn splice_candidates(
    p: VamanaIndex,
    nbrs: &[VamanaIndex],
    dead: &[bool],
    points: &CachedPoints,
    r: usize,
) -> Result<Vec<VamanaIndex>> {
    let mut cands: Vec<VamanaIndex> = Vec::new();
    let mut queue: VecDeque<VamanaIndex> = VecDeque::new();
    let mut queued: HashSet<VamanaIndex> = HashSet::new();
    let mut push = |nb: VamanaIndex, cands: &mut Vec<VamanaIndex>, queue: &mut VecDeque<_>| {
        if nb == p {
            return;
        }
        if dead[nb as usize] {
            if queued.insert(nb) {
                queue.push_back(nb);
            }
        } else {
            cands.push(nb);
        }
    };
    for &nb in nbrs {
        push(nb, &mut cands, &mut queue);
    }

    let mut expansions = 0usize;
    while let Some(v) = queue.pop_front() {
        let node = points.node(v)?;
        expansions += 1;
        for &nb in &node.nbrs {
            push(nb, &mut cands, &mut queue);
        }
        if expansions >= r && !cands.is_empty() {
            break;
        }
    }

    cands.sort_unstable();
    cands.dedup();
    Ok(cands)
}

/// Run FreshVamana's `Delete` over one `.vamana`, writing the patched graph to `out`.
///
/// `dead[i]` marks layout ordinal `i` as a tombstoned hole; it must have one entry per
/// record. The record count and every ordinal are **preserved** — deleted records keep their
/// slot (holes, never compaction), so the caller's `.pq` stays in lockstep and the MANIFEST's
/// `count` is unchanged. Only `live_count` moves.
///
/// The caller must also rewrite the `.pq` so its id column agrees with `dead` — see
/// [`rewrite_pq_holes`], or use [`consolidate_index_files`], which does both.
///
/// Errors (rather than emitting a graph that would fail silently) if the medoid can reach no
/// live record: that is an orphaned entry point, and it takes recall for the whole index to
/// zero without a single error at query time.
pub fn consolidate_deletes(
    reader: &VamanaReader,
    dead: &[bool],
    opts: &ConsolidateOpts,
    out: &mut VamanaWriter,
) -> Result<ConsolidateStats> {
    let records = reader.len();
    ensure!(
        records <= u32::MAX as u64,
        "a .vamana with {records} records exceeds the u32 layout-ordinal space"
    );
    let n = records as usize;
    ensure!(
        dead.len() == n,
        "the dead set has {} entries but the .vamana holds {n} records — they index each \
         other by position",
        dead.len()
    );
    ensure!(opts.r >= 1, "the out-degree bound R must be at least 1");
    let mut stats = ConsolidateStats {
        records,
        ..Default::default()
    };
    if n == 0 {
        return Ok(stats);
    }
    let medoid = opts.medoid;
    ensure!(
        (medoid as usize) < n,
        "medoid layout ordinal {medoid} is out of range — the .vamana holds {n} records"
    );

    let live = dead.iter().filter(|d| !**d).count();
    stats.live = live as u64;
    stats.dead = (n - live) as u64;
    // Nothing is live. There is no live node left to reference a hole, so the invariant this
    // pass exists to establish already holds — vacuously — and rewriting adjacency here could
    // only orphan the entry point of an index that has nothing to serve anyway. Emit
    // unchanged: the generation still opens, and every query correctly returns nothing.
    let all_dead = live == 0;

    let points = CachedPoints::new(reader, opts);
    for p in 0..n {
        let p = p as VamanaIndex;
        let node = points.node(p)?;
        // A hole that is not the medoid is *abandoned*: once the pass finishes, no reachable
        // node names it, so its record can never be read again. Rewriting it would be pure
        // churn (and it is the one record whose adjacency is allowed to name holes).
        let reachable = !dead[p as usize] || p == medoid;
        let names_a_hole = node.nbrs.iter().any(|&nb| dead[nb as usize]);
        if all_dead || !reachable || !names_a_hole {
            out.append(&node.raw, &node.nbrs)?;
            stats.unchanged += 1;
            continue;
        }

        let cands = splice_candidates(p, &node.nbrs, dead, &points, opts.r)?;
        let pruned = robust_prune_over(p, &cands, opts.alpha, opts.r, &points)?;
        if pruned.is_empty() && p == medoid {
            // The entry point can reach no live record — every node reachable from it is
            // dead. Emitting this would orphan the medoid: every search would expand exactly
            // one node and return garbage, with no error anywhere. The open-path validator
            // refuses such an index; refuse to *write* it, and say what to do instead.
            bail!(
                "delete consolidation would orphan the Vamana entry point: no live record is \
                 reachable from medoid layout ordinal {medoid} — every node it can reach is a \
                 hole. The index needs a full rebuild, not a delete consolidation."
            );
        }
        // A live node with an empty pool is a *dead end*, not a hazard: it names no hole (the
        // invariant holds), it is still emitted if a search reaches it, and the search simply
        // does not walk on from it. It can only happen when every node reachable from it is
        // dead, which is exactly when there is nothing to walk on to.
        out.append(&node.raw, &pruned)?;
        stats.patched += 1;
    }

    stats.cache_hits = points.hits();
    stats.cache_misses = points.misses();
    Ok(stats)
}

/// Rewrite a `.pq` so its id column agrees with `dead`: ordinal `i` keeps its dense node id
/// unless `dead[i]`, in which case it becomes the [`HOLE`] sentinel.
///
/// The **codes are carried over untouched**, and that is deliberate: a hole keeps its PQ
/// codes so the beam can still estimate a distance for it. (After a consolidation nothing
/// reaches a hole, so this is belt-and-braces — but a hole's codes are also what a *future*
/// insert into that free slot would overwrite, and a `.pq` record must exist for every
/// `.vamana` record either way: the two files index each other by position.)
pub fn rewrite_pq_holes(pq: &ResidentPq, dead: &[bool], out: &mut PqWriter) -> Result<()> {
    ensure!(
        pq.len() == dead.len(),
        "the dead set has {} entries but the .pq holds {} records",
        dead.len(),
        pq.len()
    );
    for (i, &is_dead) in dead.iter().enumerate() {
        let id = if is_dead { HOLE } else { pq.node_ids[i] };
        out.append_codes(id, pq.codes_of(i))?;
    }
    Ok(())
}

/// Everything a pass over one index's two files needs, beyond the paths.
pub struct ConsolidateIndex<'a> {
    /// The index's entry point (`AnnMode::Vamana::medoid`).
    pub medoid: VamanaIndex,
    /// `R` and `alpha`, as the graph was built with (`AnnMode::Vamana`).
    pub r: usize,
    pub alpha: f32,
    pub metric: Metric,
    /// `AnnMode::Vamana::max_norm`.
    pub max_norm: f64,
    /// **Additional** dead layout ordinals, beyond the holes the `.pq` already names. The
    /// union of the two is the dead set. (A caller that has already marked its deletes as
    /// holes passes an empty slice.)
    pub tombstoned: &'a [VamanaIndex],
    pub vamana_block_bytes: usize,
    pub pq_block_bytes: usize,
    pub zstd_level: i32,
    pub cipher: Option<Arc<BlockCipher>>,
}

/// Run the pass over one index's `.vamana` + `.pq`, writing a new, consistent pair.
///
/// The two output files are written in lockstep: same record count, same layout ordinals,
/// with `dead = { the .pq's existing holes } ∪ { cfg.tombstoned }` patched out of every
/// reachable node's adjacency and marked [`HOLE`] in the new id column. The returned
/// [`ConsolidateStats::live`] is the new `AnnMode::Vamana::live_count`; `count` (the record
/// count) does not change.
///
/// Writes to fresh paths rather than in place — a delete consolidation that fails part-way
/// (an orphaned medoid, a short read) must not leave a half-patched graph behind. The caller
/// publishes by rename.
pub fn consolidate_index_files(
    vamana_in: &Path,
    pq_in: &Path,
    vamana_out: &Path,
    pq_out: &Path,
    cfg: &ConsolidateIndex,
) -> Result<ConsolidateStats> {
    let reader = VamanaReader::open_with_cipher(vamana_in, cfg.cipher.clone())
        .with_context(|| format!("open {}", vamana_in.display()))?;
    let pq = PqReader::open_with_cipher(pq_in, cfg.cipher.clone())
        .with_context(|| format!("open {}", pq_in.display()))?
        .load_resident()
        .with_context(|| format!("load {}", pq_in.display()))?;
    let n = reader.len() as usize;
    ensure!(
        pq.len() == n,
        "vector index is inconsistent: {} holds {n} records but {} holds {} — they are \
         written in lockstep and index each other by position",
        vamana_in.display(),
        pq_in.display(),
        pq.len()
    );

    // The dead set: the holes the `.pq` already names, plus any the caller adds.
    let mut dead: Vec<bool> = (0..n).map(|i| pq.is_hole(i)).collect();
    for &t in cfg.tombstoned {
        ensure!(
            (t as usize) < n,
            "tombstoned layout ordinal {t} is out of range — the index holds {n} records"
        );
        dead[t as usize] = true;
    }

    let opts = ConsolidateOpts {
        medoid: cfg.medoid,
        r: cfg.r,
        alpha: cfg.alpha,
        metric: cfg.metric,
        max_norm: cfg.max_norm,
        // The codebook's own dimension is the ANN space's dimension, by construction
        // (`ann_pq_params`) — and it is the one the graph was actually built in, so read it
        // from the file rather than re-deriving it from the MANIFEST.
        space_dim: pq.codebook.params.dim as usize,
        cache_records: recommended_cache_records(cfg.r),
    };

    let mut vw = VamanaWriter::create_with_cipher(
        vamana_out,
        cfg.vamana_block_bytes,
        cfg.zstd_level,
        cfg.cipher.clone(),
    )
    .with_context(|| format!("create {}", vamana_out.display()))?;
    let stats = consolidate_deletes(&reader, &dead, &opts, &mut vw)?;
    vw.finish()?;

    let mut pw = PqWriter::create_with_cipher(
        pq_out,
        &pq.codebook,
        cfg.pq_block_bytes,
        cfg.zstd_level,
        cfg.cipher.clone(),
    )
    .with_context(|| format!("create {}", pq_out.display()))?;
    rewrite_pq_holes(&pq, &dead, &mut pw)?;
    pw.finish()?;

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pq::{normalise, train_codebooks, Lcg, PqParams};
    use crate::vamana::{bfs_order, build_vamana};
    use std::path::PathBuf;

    const BLOCK: usize = 4096;
    const LEVEL: i32 = 3;

    /// A per-test scratch dir. Named after the test (tests run concurrently in one process,
    /// so the pid alone would collide).
    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("slater_vamdel_{}_{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn unit_vectors(dim: usize, n: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = Lcg(seed);
        (0..n)
            .map(|_| {
                let v: Vec<f32> = (0..dim).map(|_| (rng.next_f64() as f32) - 0.5).collect();
                normalise(&v)
            })
            .collect()
    }

    /// A built-and-laid-out index on disk, with everything the pass needs to run over it.
    struct Fixture {
        dir: PathBuf,
        vamana: PathBuf,
        pq: PathBuf,
        n: usize,
        medoid: VamanaIndex,
        r: usize,
        /// `raw[layout_ordinal]` — the vectors, in layout order.
        raw: Vec<Vec<f32>>,
    }

    /// Build a real Vamana graph over `n` unit vectors, lay it out BFS-from-medoid, and write
    /// the `.vamana` + `.pq` pair exactly as `slater-build` does.
    fn build_fixture(name: &str, n: usize, dim: usize, r: usize) -> Fixture {
        let dir = scratch(name);
        let vectors = unit_vectors(dim, n, 0x51a7_e000_0000_0001);
        let g = build_vamana(&vectors, r, 1.2).unwrap();
        let order = bfs_order(&g);
        let mut new_of = vec![0u32; order.len()];
        for (new_idx, &old) in order.iter().enumerate() {
            new_of[old as usize] = new_idx as u32;
        }
        let medoid = new_of[g.medoid as usize];

        let vamana = dir.join("i.vamana");
        let pq = dir.join("i.pq");
        let mut vw = VamanaWriter::create_with_cipher(&vamana, BLOCK, LEVEL, None).unwrap();
        let mut raw = Vec::with_capacity(n);
        for &old in &order {
            let nbrs: Vec<u32> = g.adjacency[old as usize]
                .iter()
                .map(|&j| new_of[j as usize])
                .collect();
            vw.append(&vectors[old as usize], &nbrs).unwrap();
            raw.push(vectors[old as usize].clone());
        }
        vw.finish().unwrap();

        let params = PqParams::new(dim as u32, 4, 8).unwrap();
        let cb = train_codebooks(&vectors, params, 10).unwrap();
        let mut pw = PqWriter::create_with_cipher(&pq, &cb, BLOCK, LEVEL, None).unwrap();
        for (i, v) in raw.iter().enumerate() {
            // The dense node id is deliberately *not* the layout ordinal, so a test that
            // confused the two would fail.
            pw.append_codes(1000 + i as u64, &cb.encode(v).unwrap())
                .unwrap();
        }
        pw.finish().unwrap();

        Fixture {
            dir,
            vamana,
            pq,
            n,
            medoid,
            r,
            raw,
        }
    }

    impl Fixture {
        fn opts(&self, cache_records: usize) -> ConsolidateOpts {
            ConsolidateOpts {
                medoid: self.medoid,
                r: self.r,
                alpha: 1.2,
                metric: Metric::Cosine,
                max_norm: 1.0,
                space_dim: self.raw[0].len(),
                cache_records,
            }
        }

        /// Run the pass over `dead`, returning the patched adjacency of every ordinal (read
        /// back off disk) plus the stats.
        fn run(&self, tag: &str, dead: &[bool]) -> (Vec<Vec<VamanaIndex>>, ConsolidateStats) {
            let (adj, stats, _) = self.run_with_cache(tag, dead, recommended_cache_records(self.r));
            (adj, stats)
        }

        fn run_with_cache(
            &self,
            tag: &str,
            dead: &[bool],
            cache_records: usize,
        ) -> (Vec<Vec<VamanaIndex>>, ConsolidateStats, Vec<u8>) {
            let out = self.dir.join(format!("{tag}.vamana"));
            let reader = VamanaReader::open_with_cipher(&self.vamana, None).unwrap();
            let mut w = VamanaWriter::create_with_cipher(&out, BLOCK, LEVEL, None).unwrap();
            let stats =
                consolidate_deletes(&reader, dead, &self.opts(cache_records), &mut w).unwrap();
            w.finish().unwrap();
            let r = VamanaReader::open_with_cipher(&out, None).unwrap();
            let adj: Vec<Vec<VamanaIndex>> = (0..self.n)
                .map(|i| r.node(i as u32).unwrap().neighbours)
                .collect();
            let bytes = std::fs::read(&out).unwrap();
            (adj, stats, bytes)
        }

        /// The invariant the whole slice exists to establish, checked exhaustively: after the
        /// pass, **no node a search can reach names a hole**. That is every live node — and
        /// the medoid, which is reachable by definition whether it is live or not.
        fn assert_no_reachable_node_references_a_hole(
            &self,
            adj: &[Vec<VamanaIndex>],
            dead: &[bool],
        ) {
            for (p, nbrs) in adj.iter().enumerate() {
                let reachable = !dead[p] || p as VamanaIndex == self.medoid;
                if !reachable {
                    continue;
                }
                for &nb in nbrs {
                    assert!(
                        !dead[nb as usize],
                        "reachable node {p} (live={}, medoid={}) still references hole {nb} \
                         after the pass — every search that reaches {p} pays a block read for a \
                         record that can never be returned",
                        !dead[p],
                        p as VamanaIndex == self.medoid,
                    );
                }
            }
        }
    }

    /// The structural invariant, over a real graph, for dead sets from sparse to extreme —
    /// and over *every* ordinal, not a sample. Includes the case where the medoid itself is
    /// dead.
    #[test]
    fn no_live_node_references_a_hole() {
        let f = build_fixture("invariant", 400, 16, 12);
        for (i, &frac) in [0.05f64, 0.2, 0.5, 0.8].iter().enumerate() {
            let mut rng = Lcg(0xdead_0000_0000_0001 + i as u64);
            let dead: Vec<bool> = (0..f.n).map(|_| rng.next_f64() < frac).collect();
            let live = dead.iter().filter(|d| !**d).count();
            assert!(live > 0, "the fixture must leave something live");
            let (adj, stats) = f.run(&format!("frac{i}"), &dead);
            f.assert_no_reachable_node_references_a_hole(&adj, &dead);
            assert_eq!(stats.records, f.n as u64);
            assert_eq!(stats.live, live as u64);
            assert_eq!(stats.patched + stats.unchanged, f.n as u64);
            // Degree stays bounded by R.
            for (p, nbrs) in adj.iter().enumerate() {
                assert!(nbrs.len() <= f.r, "node {p} has degree {}", nbrs.len());
            }
        }
    }

    /// The adversary the one-hop formula fails: a **chain** of deleted nodes. `p → v → w`,
    /// with `v` and `w` both dead and the only live node beyond `w`. The paper's
    /// `⋃_{v ∈ adj[p] ∩ D} adj[v] \ D` drops `w` without following it, so `p`'s pool comes out
    /// empty and `p` is left a dead end — while `x`, one hop further, was reachable all along.
    ///
    /// Truth here is hand-derived from a graph laid out by hand: points on a line, so which
    /// candidate robust-prune keeps is not in doubt.
    #[test]
    fn a_chain_of_deleted_nodes_is_spliced_through_not_dropped() {
        let dir = scratch("chain");
        let path = dir.join("chain.vamana");
        // 0 = medoid/entry (live) → 1 (dead) → 2 (dead) → 3 (live).
        // The ONLY route from 0 to 3 runs through two dead nodes in a row.
        let vectors: Vec<Vec<f32>> = vec![
            vec![0.0f32, 0.0],
            vec![1.0, 0.0],
            vec![2.0, 0.0],
            vec![3.0, 0.0],
        ];
        let adjacency: Vec<Vec<VamanaIndex>> = vec![vec![1], vec![2], vec![3], vec![]];
        let mut w = VamanaWriter::create_with_cipher(&path, BLOCK, LEVEL, None).unwrap();
        for (v, nbrs) in vectors.iter().zip(&adjacency) {
            w.append(v, nbrs).unwrap();
        }
        w.finish().unwrap();

        let dead = vec![false, true, true, false];
        let opts = ConsolidateOpts {
            medoid: 0,
            r: 4,
            alpha: 1.2,
            metric: Metric::L2,
            max_norm: 0.0,
            space_dim: 2,
            cache_records: 64,
        };
        let out = dir.join("out.vamana");
        let reader = VamanaReader::open_with_cipher(&path, None).unwrap();
        let mut vw = VamanaWriter::create_with_cipher(&out, BLOCK, LEVEL, None).unwrap();
        consolidate_deletes(&reader, &dead, &opts, &mut vw).unwrap();
        vw.finish().unwrap();

        let r = VamanaReader::open_with_cipher(&out, None).unwrap();
        assert_eq!(
            r.node(0).unwrap().neighbours,
            vec![3],
            "the entry point must be spliced THROUGH the chain 1→2 onto the live node 3; a \
             one-hop splice leaves it with no out-edges and the index unnavigable"
        );
        assert!(!r
            .node(0)
            .unwrap()
            .neighbours
            .iter()
            .any(|&nb| dead[nb as usize]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A node whose **entire** neighbourhood is deleted, with live nodes behind them: it must
    /// come out pointing at those live nodes, not at the holes and not at nothing.
    #[test]
    fn a_node_whose_whole_neighbourhood_is_dead_is_repointed_at_the_live_nodes_behind_it() {
        let dir = scratch("allnbrsdead");
        let path = dir.join("g.vamana");
        //     0 (live, medoid)
        //    / \
        //   1   2   (both dead — 0's ENTIRE neighbourhood)
        //   |   |
        //   3   4   (live)
        let vectors: Vec<Vec<f32>> = vec![
            vec![0.0f32, 0.0],
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![2.0, 0.0],
            vec![0.0, 2.0],
        ];
        let adjacency: Vec<Vec<VamanaIndex>> =
            vec![vec![1, 2], vec![0, 3], vec![0, 4], vec![1], vec![2]];
        let mut w = VamanaWriter::create_with_cipher(&path, BLOCK, LEVEL, None).unwrap();
        for (v, nbrs) in vectors.iter().zip(&adjacency) {
            w.append(v, nbrs).unwrap();
        }
        w.finish().unwrap();

        let dead = vec![false, true, true, false, false];
        let opts = ConsolidateOpts {
            medoid: 0,
            r: 4,
            alpha: 1.2,
            metric: Metric::L2,
            max_norm: 0.0,
            space_dim: 2,
            cache_records: 64,
        };
        let out = dir.join("out.vamana");
        let reader = VamanaReader::open_with_cipher(&path, None).unwrap();
        let mut vw = VamanaWriter::create_with_cipher(&out, BLOCK, LEVEL, None).unwrap();
        consolidate_deletes(&reader, &dead, &opts, &mut vw).unwrap();
        vw.finish().unwrap();

        let r = VamanaReader::open_with_cipher(&out, None).unwrap();
        let mut got = r.node(0).unwrap().neighbours;
        got.sort_unstable();
        assert_eq!(
            got,
            vec![3, 4],
            "0's whole neighbourhood was deleted; it must inherit the live nodes behind both \
             holes, not be left empty"
        );
        // 3 and 4 pointed only at holes; each must now point at what lay behind them (0 is
        // excluded — a node is never its own neighbour).
        assert_eq!(r.node(3).unwrap().neighbours, vec![0]);
        assert_eq!(r.node(4).unwrap().neighbours, vec![0]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **The silent killer.** The medoid is the fixed entry point of every beam search. If the
    /// pass ever splices *its* out-edges away, every query expands one node and returns
    /// garbage — no error, no panic, recall for the whole index at zero.
    ///
    /// Attack it from both sides: a medoid that is itself deleted (so a pass that treated it
    /// as "just another hole" would abandon its adjacency), and a medoid whose entire
    /// neighbourhood is deleted (so a pass that spliced without following the chain would
    /// leave it empty).
    #[test]
    fn delete_consolidation_never_orphans_the_medoid() {
        let f = build_fixture("medoid", 300, 16, 10);

        // (a) The medoid is deleted, along with a third of the index.
        let mut rng = Lcg(0x0bad_beef_0000_0001);
        let mut dead: Vec<bool> = (0..f.n).map(|_| rng.next_f64() < 0.33).collect();
        dead[f.medoid as usize] = true;
        let (adj, stats) = f.run("dead_medoid", &dead);
        assert!(
            !adj[f.medoid as usize].is_empty(),
            "the medoid is deleted, but it is still the ENTRY POINT — orphaning its adjacency \
             takes recall for the whole index to zero, silently"
        );
        f.assert_no_reachable_node_references_a_hole(&adj, &dead);
        assert_eq!(stats.live, dead.iter().filter(|d| !**d).count() as u64);

        // (b) The medoid is live, but every one of its neighbours is deleted.
        let reader = VamanaReader::open_with_cipher(&f.vamana, None).unwrap();
        let medoid_nbrs = reader.node(f.medoid).unwrap().neighbours;
        assert!(!medoid_nbrs.is_empty());
        let mut dead = vec![false; f.n];
        for &nb in &medoid_nbrs {
            dead[nb as usize] = true;
        }
        let (adj, _) = f.run("medoid_nbrs_dead", &dead);
        assert!(
            !adj[f.medoid as usize].is_empty(),
            "the medoid's whole neighbourhood was deleted — it must be re-pointed at the live \
             nodes behind them, never left empty"
        );
        f.assert_no_reachable_node_references_a_hole(&adj, &dead);

        // (c) Both: the medoid AND its whole neighbourhood deleted.
        dead[f.medoid as usize] = true;
        let (adj, _) = f.run("both", &dead);
        assert!(!adj[f.medoid as usize].is_empty());
        f.assert_no_reachable_node_references_a_hole(&adj, &dead);
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// An index in which the entry point can reach **no** live record is unservable: a beam
    /// search expands one node and returns nothing useful. Emitting it would be the silent
    /// failure. Refuse, loudly, and say what to do.
    #[test]
    fn an_entry_point_that_can_reach_nothing_live_is_refused_not_silently_emitted() {
        let dir = scratch("orphan");
        let path = dir.join("g.vamana");
        // 0 (medoid) → 1 → 2, all reachable-from-0 nodes dead. 3 is live but has no in-edge
        // from the reachable region at all, so no splice can save the entry point.
        let vectors: Vec<Vec<f32>> = vec![
            vec![0.0f32, 0.0],
            vec![1.0, 0.0],
            vec![2.0, 0.0],
            vec![9.0, 9.0],
        ];
        let adjacency: Vec<Vec<VamanaIndex>> = vec![vec![1], vec![2], vec![1], vec![0]];
        let mut w = VamanaWriter::create_with_cipher(&path, BLOCK, LEVEL, None).unwrap();
        for (v, nbrs) in vectors.iter().zip(&adjacency) {
            w.append(v, nbrs).unwrap();
        }
        w.finish().unwrap();

        let dead = vec![true, true, true, false];
        let opts = ConsolidateOpts {
            medoid: 0,
            r: 4,
            alpha: 1.2,
            metric: Metric::L2,
            max_norm: 0.0,
            space_dim: 2,
            cache_records: 64,
        };
        let reader = VamanaReader::open_with_cipher(&path, None).unwrap();
        let mut vw =
            VamanaWriter::create_with_cipher(dir.join("out.vamana"), BLOCK, LEVEL, None).unwrap();
        let err = consolidate_deletes(&reader, &dead, &opts, &mut vw).unwrap_err();
        assert!(
            format!("{err:#}").contains("orphan"),
            "expected a loud refusal, got: {err:#}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `D` = everything. Nothing is live, so no live node can reference a hole and there is
    /// nothing to patch. The pass must be a no-op — in particular it must not orphan the
    /// entry point of an index the generation still has to be able to *open*.
    #[test]
    fn a_wholly_dead_index_is_a_no_op_not_an_orphaned_medoid() {
        let f = build_fixture("alldead", 120, 8, 8);
        let dead = vec![true; f.n];
        let (adj, stats) = f.run("alldead", &dead);
        assert_eq!(stats.live, 0);
        assert_eq!(stats.patched, 0, "nothing live ⇒ nothing to patch");
        assert_eq!(stats.unchanged, f.n as u64);
        assert!(
            !adj[f.medoid as usize].is_empty(),
            "the generation open path refuses an index whose medoid has no out-edges"
        );
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// `D` = ∅. The pass must emit the graph it was given, byte for byte — a delete
    /// consolidation with nothing to delete must not perturb the content hash.
    #[test]
    fn an_empty_dead_set_reproduces_the_input_exactly() {
        let f = build_fixture("nodead", 200, 16, 10);
        let dead = vec![false; f.n];
        let (_, stats, bytes) = f.run_with_cache("nodead", &dead, 1024);
        assert_eq!(stats.patched, 0);
        assert_eq!(stats.unchanged, f.n as u64);
        assert_eq!(
            bytes,
            std::fs::read(&f.vamana).unwrap(),
            "an empty dead set must reproduce the input .vamana byte for byte"
        );
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// Byte-determinism: the generation content hash is computed over this file, so the same
    /// input must give the same bytes — and the LRU is a **cache**, so its capacity must not
    /// change the output. A capacity of 1 evicts between every single distance call; if the
    /// result differed from a capacity that holds the whole pool, the cache would be part of
    /// the algorithm rather than an optimisation over it.
    #[test]
    fn the_output_is_byte_deterministic_and_independent_of_the_cache() {
        let f = build_fixture("determinism", 250, 16, 12);
        let mut rng = Lcg(0x5eed_0000_0000_0007);
        let dead: Vec<bool> = (0..f.n).map(|_| rng.next_f64() < 0.35).collect();

        let (_, s1, a) = f.run_with_cache("det_a", &dead, recommended_cache_records(f.r));
        let (_, s2, b) = f.run_with_cache("det_b", &dead, recommended_cache_records(f.r));
        assert_eq!(a, b, "the same input must give the same .vamana bytes");
        assert_eq!(s1.patched, s2.patched);

        let (_, s3, c) = f.run_with_cache("det_c", &dead, 1);
        assert_eq!(
            a, c,
            "the LRU is a cache, not part of the algorithm: capacity must not change the output"
        );
        // ...and it really is doing something: a capacity of 1 re-reads everything.
        assert!(
            s3.cache_misses > s1.cache_misses,
            "a 1-record cache must miss far more than a pool-sized one ({} vs {})",
            s3.cache_misses,
            s1.cache_misses
        );
        assert!(
            s1.cache_hits > s1.cache_misses,
            "the LRU must actually hit on a BFS-ordered layout: {} hits vs {} misses",
            s1.cache_hits,
            s1.cache_misses
        );
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// The trivially-small index that `build_vamana` short-circuits into a complete graph
    /// (`n <= r + 1`): every node names every other, so *every* live node names a hole the
    /// moment anything is deleted. The pass must still hold the invariant.
    #[test]
    fn the_complete_graph_short_circuit_survives_deletes() {
        let f = build_fixture("complete", 8, 8, 16); // n=8 <= r+1=17 ⇒ complete graph
        let reader = VamanaReader::open_with_cipher(&f.vamana, None).unwrap();
        assert_eq!(
            reader.node(0).unwrap().neighbours.len(),
            7,
            "the fixture must actually be the complete-graph short-circuit"
        );
        let mut dead = vec![false; f.n];
        dead[1] = true;
        dead[2] = true;
        dead[f.medoid as usize] = true;
        let (adj, _) = f.run("complete", &dead);
        f.assert_no_reachable_node_references_a_hole(&adj, &dead);
        assert!(!adj[f.medoid as usize].is_empty());
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// The file-level entry point: the two files come out in lockstep — same record count,
    /// the dead ordinals marked `HOLE` in the id column, every surviving id and its codes
    /// carried across untouched — and `live` is the new `live_count`.
    #[test]
    fn consolidate_index_files_keeps_the_two_files_in_lockstep() {
        let f = build_fixture("files", 200, 16, 10);
        let vam_out = f.dir.join("out.vamana");
        let pq_out = f.dir.join("out.pq");
        // Ordinals 3, 4 and the medoid are tombstoned by the caller; nothing is a hole yet.
        let tombstoned = vec![3u32, 4, f.medoid];
        let stats = consolidate_index_files(
            &f.vamana,
            &f.pq,
            &vam_out,
            &pq_out,
            &ConsolidateIndex {
                medoid: f.medoid,
                r: f.r,
                alpha: 1.2,
                metric: Metric::Cosine,
                max_norm: 1.0,
                tombstoned: &tombstoned,
                vamana_block_bytes: BLOCK,
                pq_block_bytes: BLOCK,
                zstd_level: LEVEL,
                cipher: None,
            },
        )
        .unwrap();

        let dead: Vec<bool> = (0..f.n).map(|i| tombstoned.contains(&(i as u32))).collect();
        assert_eq!(stats.records, f.n as u64);
        assert_eq!(stats.live, (f.n - 3) as u64);
        assert_eq!(stats.dead, 3);

        let out_pq = PqReader::open_with_cipher(&pq_out, None)
            .unwrap()
            .load_resident()
            .unwrap();
        let in_pq = PqReader::open_with_cipher(&f.pq, None)
            .unwrap()
            .load_resident()
            .unwrap();
        assert_eq!(
            out_pq.len(),
            f.n,
            "holes, not compaction: the record count is fixed"
        );
        assert_eq!(out_pq.live_count(), f.n - 3);
        for (i, &is_dead) in dead.iter().enumerate() {
            assert_eq!(
                out_pq.is_hole(i),
                is_dead,
                "ordinal {i}: the .pq id column must agree with the dead set"
            );
            if !is_dead {
                assert_eq!(
                    out_pq.node_ids[i],
                    1000 + i as u64,
                    "a live id must survive"
                );
            }
            // A hole keeps its codes — they are what a future insert into the slot overwrites.
            assert_eq!(out_pq.codes_of(i), in_pq.codes_of(i));
        }

        let out_v = VamanaReader::open_with_cipher(&vam_out, None).unwrap();
        assert_eq!(out_v.len(), f.n as u64);
        for i in 0..f.n {
            let node = out_v.node(i as u32).unwrap();
            assert_eq!(
                node.vector, f.raw[i],
                "the raw vector must survive the pass"
            );
            if !dead[i] || i as u32 == f.medoid {
                for &nb in &node.neighbours {
                    assert!(!dead[nb as usize], "ordinal {i} still names hole {nb}");
                }
            }
        }
        assert!(!out_v.node(f.medoid).unwrap().neighbours.is_empty());
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// A forged neighbour ordinal (an on-disk `.vamana` is untrusted) must be rejected with a
    /// message, not indexed out of bounds into the dead set.
    #[test]
    fn a_forged_neighbour_ordinal_is_rejected() {
        let dir = scratch("forged");
        let path = dir.join("g.vamana");
        let mut w = VamanaWriter::create_with_cipher(&path, BLOCK, LEVEL, None).unwrap();
        w.append(&[0.0f32, 0.0], &[1]).unwrap();
        w.append(&[1.0f32, 0.0], &[999]).unwrap(); // out of range
        w.finish().unwrap();

        let reader = VamanaReader::open_with_cipher(&path, None).unwrap();
        let mut vw =
            VamanaWriter::create_with_cipher(dir.join("out.vamana"), BLOCK, LEVEL, None).unwrap();
        let err = consolidate_deletes(
            &reader,
            &[false, false],
            &ConsolidateOpts {
                medoid: 0,
                r: 4,
                alpha: 1.2,
                metric: Metric::L2,
                max_norm: 0.0,
                space_dim: 2,
                cache_records: 8,
            },
            &mut vw,
        )
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("neighbour ordinal 999"),
            "{err:#}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The LRU's eviction order really is least-recently-used, and a re-touch renews an entry.
    /// (Hand-derived: capacity 2, insert A then B, touch A, insert C ⇒ B is the victim.)
    #[test]
    fn the_lru_evicts_the_least_recently_used_record() {
        let node = |x: f32| {
            Rc::new(CachedNode {
                raw: vec![x],
                ann: vec![x],
                nbrs: vec![],
            })
        };
        let mut lru = Lru::new(2);
        lru.put(1, node(1.0));
        lru.put(2, node(2.0));
        assert!(lru.get(1).is_some(), "1 is resident");
        lru.put(3, node(3.0)); // evicts the LRU — which is 2, because 1 was just touched
        assert!(lru.get(1).is_some(), "1 was re-touched, so it must survive");
        assert!(lru.get(3).is_some());
        assert!(lru.get(2).is_none(), "2 was the least recently used");
        assert_eq!(lru.map.len(), 2, "the capacity is a hard bound");
    }
}
