// SPDX-License-Identifier: Apache-2.0
//! The **RW-index**: a mutable, wholly in-memory Vamana graph over a *bounded, fresh* set
//! of vectors (FreshDiskANN's `RW-Index`, T0).
//!
//! # This is a cache. The delta is the durable thing.
//!
//! Say it once, loudly, because the shape of the code invites the opposite conclusion:
//! **nothing here is persisted, and nothing here needs to be.** An `RwVamana` is *derived
//! state* over `slater`'s write delta, which is itself WAL-backed and size-bounded. A crash
//! loses the graph and nothing else: the delta replays from its WAL, and the index is
//! rebuilt from the replayed delta by re-inserting each vector. There is no on-disk format,
//! no version, no recovery path — and there must not be one. If you find yourself adding a
//! `RwVamana::write`, stop: you are persisting a cache of something already durable.
//!
//! # What it is
//!
//! The three Vamana construction primitives ([`greedy_search_over`], [`robust_prune_over`],
//! [`insert_point`]) and the search ([`beam_search`]) already live in [`crate::vamana`],
//! generic over a [`PointSet`] and an [`AdjRead`]/[`AdjWrite`]. This module is the *third*
//! arm that plugs into them (after the offline slab build and the on-disk reader): points in
//! RAM, adjacency in RAM, both mutable.
//!
//! * [`RwVamana::insert`] — **an update is a delete plus an insert**, exactly as FreshDiskANN
//!   does it. If the node id already occupies a slot, that slot is marked dead and a *new*
//!   row is appended; the old row keeps its edges and stays a navigational waypoint. Anything
//!   less returns the node **twice, at two different scores**.
//! * [`RwVamana::remove`] — mark the slot dead. It **never touches the adjacency**. Pruning a
//!   deleted node out of the graph would disconnect whatever sits behind it and silently cost
//!   recall on the *live* nodes; the paper's consolidation pass is what eventually reclaims
//!   those edges, and here that pass is simply "the delta is consolidated away".
//! * [`RwVamana::search`] — [`beam_search`], verbatim. `estimate == exact`: the vectors are
//!   right there in RAM, so there is no PQ (the set is bounded — PQ would buy nothing but
//!   error). The D26 tie-break (ascending node id) and the exact re-rank come free.
//!
//! # The construction space (cosine/L2 augmented; dot is IP-native — HIK-137)
//!
//! Robust prune's domination test is only sound over a true metric, so a **cosine or L2** graph
//! is built in the metric's **ANN space** ([`crate::pq::ann_point`]): cosine ⇒ unit vectors
//! (D29), L2 ⇒ identity. Those two arms are unchanged.
//!
//! **Dot is built IP-native** (HIK-137 phase 3), exactly as the offline base build is
//! ([`crate::vamana::build_vamana_ip`]): closeness is the **raw** inner product, so
//! [`PointSet::dist`] returns `−⟨a,b⟩` with no norm augmentation, an insert selects the top-R by
//! IP ([`crate::vamana::insert_point_ip`] / `ip_prune_over`) instead of the α-domination robust
//! prune, and the entry point is the **highest-norm** row (the natural IP hub), tracked
//! incrementally as rows arrive. This retires the whole `max_norm`/augmentation machinery for Dot
//! — there is no moving `M` to keep coherent — which is what makes the old `max_norm`-carry
//! distortion (a growing `M` staling earlier rows' augmentation) *moot* rather than merely bounded.
//!
//! Adjacency chosen while the graph was smaller stays valid — it is a navigational structure,
//! not an answer — and every emitted score comes from the caller's **exact** re-rank of the
//! raw vector under the true metric, so the scores this arm feeds `slater`'s `merge_topk` are
//! on the same scale as every other arm's (the D29/HIK-109 invariant).
//!
//! # What an insert actually costs (measured, because the estimates in the air are wrong)
//!
//! One [`RwVamana::insert`] is a greedy search plus up to `R + 1` robust prunes — about **23 000
//! distance computations** at `R = 32`, `L = 64`, and the *back-link re-prune* is ~80 % of them.
//! Measured on a 20 000-vector, 768-dim cosine index (release, `opt-level = "s"`):
//!
//! | | per insert | 20 k build |
//! |---|---|---|
//! | scalar f64 [`crate::pq::sq_l2`] | 11.8 ms | 235 s |
//! | `f32x8` [`l2_sq_simd`] (shipped) | **2.1 ms** | **42 s** |
//!
//! That is in line with what FreshDiskANN reports, and it is ~40× the "≈50 µs" the ticket
//! guessed. It matters, because a *rebuild* (first query after a restart, or after the touched
//! journal gaps) re-inserts the whole delta on the read path: **budget ~2 ms × vectors**, and
//! size `vectorQuery.rwIndex.maxVectors` accordingly. It is not free, and pretending otherwise
//! is how a 30-second first query gets shipped.
//!
//! A beam **search** over that index is ~2 ms (200 queries, k=10, L=64) — and a buffer-filling
//! `fetch` (HIK-108 deferred the question) saves **nothing measurable**: 406 ms vs 411 ms over
//! 200 searches, i.e. inside the run-to-run noise. The clone is a `Vec<f32>` per *expansion*,
//! and expansions are dominated by the distance computation over the same bytes. Not worth the
//! API churn; the `fetch` signature stays as HIK-108 left it.
//!
//! # Navigating by the exact score is the same walk
//!
//! [`beam_search`] wants an `estimate` for navigation and an `exact` for ranking; here they
//! are the same closure. That is not a shortcut, it is an identity: for a *fixed* query the
//! exact distance and the construction-space distance induce the **same order** on candidates —
//! cosine (`‖q̂ − x̂‖² = 2 − 2cos` vs `1 − cos`), L2 (identical), dot (both are the raw
//! `−⟨q, x⟩` — the IP-native build measures exactly what the caller re-ranks with). Ranking by
//! one *is* ranking by the other, so the beam expands exactly the nodes the construction-space
//! estimate would have expanded.

use std::collections::HashMap;

use anyhow::{bail, Result};
use wide::f32x8;

use crate::manifest::Metric;
use crate::pq::{normalise_into, require_finite};
use crate::vamana::{
    beam_search, insert_point, insert_point_ip, BeamParams, Expanded, InsertParams, SearchHit,
    VamanaIndex,
};

/// Out-degree bound `R`. FreshDiskANN's RW index is small and fully resident, so a modest
/// degree is plenty; it also bounds `resident_bytes` at `slots × R × 4` for the adjacency.
pub const RW_R: usize = 32;
/// Robust-prune long-edge factor.
pub const RW_ALPHA: f32 = 1.2;
/// Search-list size during construction — wider than `R` for better candidates.
pub const RW_L_BUILD: usize = 64;

/// The resident points, split out from the graph so an insert can hold `&points` and
/// `&mut adjacency` at once (the borrow [`insert_point`] forces, and the one thing about
/// this file that *will* bite whoever refactors it).
struct Points {
    metric: Metric,
    dim: usize,
    /// Row-major `slots × dim` in **point space**: unit-normalised for cosine (D29), raw for
    /// L2 and dot. For dot the row is the raw vector and the IP build measures `−⟨a,b⟩` over it
    /// directly — there is no augmented row (HIK-137).
    data: Vec<f32>,
    /// `‖x‖²` of the raw vector, per slot. Doubles as the slot count ([`PointSet::len`]) and,
    /// for [`Metric::Dot`], selects the highest-norm IP entry point ([`RwVamana::medoid`]).
    norm2: Vec<f64>,
}

/// Squared-L2 between two rows, f32 SIMD with a scalar tail.
///
/// **Not** [`crate::pq::sq_l2`], deliberately, and the reason is worth spelling out. That one
/// accumulates in scalar f64 and feeds `build_vamana`, whose adjacency is an input to the
/// generation content hash — it cannot be touched. This one feeds *only* the RW index, which
/// is derived state with no hash and no on-disk form, so it is free to be fast.
///
/// And it has to be. One `insert` runs a greedy search plus up to `R + 1` robust prunes, which
/// is ~45 000 distance computations at `R = 32`, `L = 64`. Under the workspace's
/// `opt-level = "s"` release profile (which largely suppresses autovectorisation) a scalar f64
/// loop makes that **11.8 ms per vector at dim 768** — a 16 k-vector delta rebuild would take
/// *three minutes*, on the first query after a restart. Explicit `f32x8` brings it to ~2 ms.
///
/// The f32 reduction is a navigation-only quantity: it decides which edges robust-prune keeps,
/// never a reported score (every emitted score comes from the caller's exact f64 re-rank of the
/// raw vector). `slater::vector` makes the identical trade for the identical reason.
#[inline]
fn l2_sq_simd(a: &[f32], b: &[f32]) -> f64 {
    let mut acc = f32x8::ZERO;
    let mut ar = a.chunks_exact(8);
    let mut br = b.chunks_exact(8);
    for (ac, bc) in ar.by_ref().zip(br.by_ref()) {
        let av = f32x8::from(<[f32; 8]>::try_from(ac).unwrap());
        let bv = f32x8::from(<[f32; 8]>::try_from(bc).unwrap());
        let d = av - bv;
        acc = d.mul_add(d, acc);
    }
    let mut sum = acc.reduce_add();
    for (x, y) in ar.remainder().iter().zip(br.remainder()) {
        let d = x - y;
        sum += d * d;
    }
    sum as f64
}

impl Points {
    #[inline]
    fn row(&self, i: VamanaIndex) -> &[f32] {
        let i = i as usize;
        &self.data[i * self.dim..(i + 1) * self.dim]
    }

    /// Raw inner product `⟨row(a), row(b)⟩`, f32 SIMD with a scalar tail — the IP-native Dot
    /// closeness. Same navigation-only f32 trade as [`l2_sq_simd`] (every reported score is the
    /// caller's exact f64 re-rank).
    #[inline]
    fn ip(&self, a: VamanaIndex, b: VamanaIndex) -> f64 {
        let (x, y) = (self.row(a), self.row(b));
        let mut acc = f32x8::ZERO;
        let mut xr = x.chunks_exact(8);
        let mut yr = y.chunks_exact(8);
        for (xc, yc) in xr.by_ref().zip(yr.by_ref()) {
            let xv = f32x8::from(<[f32; 8]>::try_from(xc).unwrap());
            let yv = f32x8::from(<[f32; 8]>::try_from(yc).unwrap());
            acc = xv.mul_add(yv, acc);
        }
        let mut sum = acc.reduce_add();
        for (p, q) in xr.remainder().iter().zip(yr.remainder()) {
            sum += p * q;
        }
        sum as f64
    }
}

impl crate::vamana::PointSet for Points {
    fn len(&self) -> usize {
        self.norm2.len()
    }

    fn dist(&self, a: VamanaIndex, b: VamanaIndex) -> Result<f64> {
        Ok(match self.metric {
            // Cosine rows are unit vectors and L2 rows are the metric's own space, so the
            // stored squared-L2 *is* the ANN-space distance.
            Metric::Cosine | Metric::L2 => l2_sq_simd(self.row(a), self.row(b)),
            // Dot: IP-native (HIK-137). Closeness is the raw inner product, maximised, so the
            // min-based Vamana primitives descend on `−⟨a,b⟩`. No norm augmentation.
            Metric::Dot => -self.ip(a, b),
        })
    }
}

/// A mutable in-memory Vamana graph over a bounded, fresh set of vectors, keyed by **dense
/// node id**. See the module doc — most of the contract lives there.
pub struct RwVamana {
    points: Points,
    /// Out-neighbours per slot. `Vec<Vec<VamanaIndex>>` already implements
    /// [`AdjRead`](crate::vamana::AdjRead) + [`AdjWrite`](crate::vamana::AdjWrite).
    adjacency: Vec<Vec<VamanaIndex>>,
    /// The dense node id each slot holds. A dead slot keeps its id (it is never emitted, so
    /// the id is only ever read for a live slot) — clearing it would buy nothing.
    node_id: Vec<u64>,
    /// Dead slots: superseded by a re-embed, or removed outright. Never emitted; always
    /// navigated through.
    dead: Vec<bool>,
    /// Live node id → its slot. The *only* place a node id maps to a slot, so a re-embed
    /// cannot leave two live slots for one id.
    slot_of: HashMap<u64, VamanaIndex>,
    /// The beam's entry point. For cosine/L2 it is recomputed as the centroid-closest slot as
    /// the set grows ([`Self::maybe_remedoid`]); for dot it is the **highest-norm** slot, the
    /// natural IP hub, tracked incrementally on every insert (HIK-137). May be a dead slot — a
    /// dead slot is a perfectly good waypoint.
    medoid: VamanaIndex,
    /// Running sum of the point-space rows, for the medoid recompute. Over *every* slot,
    /// dead included: an entry point does not have to be live, and tracking live-only would
    /// need a subtract-on-delete that buys nothing.
    sum: Vec<f64>,
    /// Slot count at which the medoid is next recomputed (doubling, so the O(slots · dim)
    /// recompute amortises to O(dim) per insert).
    remedoid_at: usize,
    /// Reused generation-stamped scratch for the construction searches. Allocated once and
    /// reset in O(1) by [`Expanded::Stamps`]; the generation survives across inserts, which is
    /// exactly the long-lived-writer case [`Expanded`]'s wrap guard exists for.
    stamps: Vec<u32>,
    stamp_gen: u32,
}

impl RwVamana {
    /// An empty index over `dim`-dimensional vectors under `metric`.
    pub fn new(dim: usize, metric: Metric) -> Self {
        Self {
            points: Points {
                metric,
                dim,
                data: Vec::new(),
                norm2: Vec::new(),
            },
            adjacency: Vec::new(),
            node_id: Vec::new(),
            dead: Vec::new(),
            slot_of: HashMap::new(),
            medoid: 0,
            sum: vec![0.0; dim],
            remedoid_at: 64,
            stamps: Vec::new(),
            stamp_gen: 0,
        }
    }

    pub fn dim(&self) -> usize {
        self.points.dim
    }

    pub fn metric(&self) -> Metric {
        self.points.metric
    }

    /// Live vectors — the number a search can emit.
    pub fn live_count(&self) -> usize {
        self.slot_of.len()
    }

    /// Rows held, dead ones included. This is what [`Self::resident_bytes`] scales with, and
    /// therefore what a `maxVectors` valve must bound: an update appends a row and never
    /// reclaims the old one until the delta is consolidated away.
    pub fn slots(&self) -> usize {
        self.node_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slot_of.is_empty()
    }

    /// Whether `node_id` currently has a live vector here.
    pub fn contains(&self, node_id: u64) -> bool {
        self.slot_of.contains_key(&node_id)
    }

    /// The live node ids, in no particular order.
    pub fn live_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.slot_of.keys().copied()
    }

    /// Insert (or **re-embed**) `node_id`'s vector.
    ///
    /// An update is a delete plus an insert: the id's previous slot — if any — is marked dead
    /// *before* the new row is appended, so the id occupies exactly one live slot afterwards.
    /// A failure to do that is not a crash, it is the same node coming back twice at two
    /// different distances.
    pub fn insert(&mut self, node_id: u64, vector: &[f32]) -> Result<()> {
        if vector.len() != self.points.dim {
            bail!(
                "vector for node {node_id} has dimension {} but the index is {}-dimensional",
                vector.len(),
                self.points.dim
            );
        }
        // A distinct entry point (the delta write path): a NaN/±inf here poisons the resident
        // data buffer *immediately*, before any consolidation, and no downstream guard catches
        // it (`highest_norm` selection and the IP dot silently order a NaN by `total_cmp`).
        // Reject up front (HIK-134).
        require_finite(vector)?;
        // Delete-then-insert. Order matters only for clarity — the new slot is appended
        // below, so it cannot be the one we just killed.
        if let Some(old) = self.slot_of.remove(&node_id) {
            self.dead[old as usize] = true;
        }

        let slot = self.node_id.len() as VamanaIndex;
        let norm2: f64 = vector.iter().map(|&x| (x as f64) * (x as f64)).sum();
        match self.points.metric {
            // The one normalisation (D29 / HIK-109) — appended straight into the contiguous
            // buffer. A zero vector stays zero, which scores as distance 1 (never NaN).
            Metric::Cosine => normalise_into(vector, &mut self.points.data),
            Metric::L2 | Metric::Dot => self.points.data.extend_from_slice(vector),
        }
        self.points.norm2.push(norm2);
        self.node_id.push(node_id);
        self.dead.push(false);
        self.adjacency.push(Vec::new());
        for (acc, &x) in self.sum.iter_mut().zip(self.points.row(slot)) {
            *acc += x as f64;
        }
        self.slot_of.insert(node_id, slot);

        // The first point is the whole graph: it is its own entry, with no edges to prune.
        if slot == 0 {
            self.medoid = 0;
            self.remedoid_at = 64;
            return Ok(());
        }

        self.stamps.resize(self.node_id.len(), 0);
        // Three disjoint field borrows: `&mut adjacency`, `&points`, `&mut stamps`. This split
        // is the whole reason `Points` is its own struct.
        let mut expanded = Expanded::Stamps {
            buf: &mut self.stamps,
            gen: self.stamp_gen,
        };
        // Dot is IP-native: weave the point in by the top-R-by-IP rule (HIK-137). Cosine/L2 keep
        // the α-domination robust-prune insert (`Points::dist` gives the right space for each).
        let res = match self.points.metric {
            Metric::Dot => insert_point_ip(
                slot,
                &mut self.adjacency,
                &self.points,
                self.medoid,
                RW_R,
                RW_L_BUILD,
                &mut expanded,
            ),
            Metric::Cosine | Metric::L2 => insert_point(
                slot,
                &mut self.adjacency,
                &self.points,
                InsertParams {
                    medoid: self.medoid,
                    alpha: RW_ALPHA,
                    r: RW_R,
                    l_build: RW_L_BUILD,
                },
                &mut expanded,
            ),
        };
        // Carry the generation forward, so the next insert does not re-read this search's
        // stamps as its own. (`Expanded` bumps it internally; it is by-value, so read it back.)
        if let Expanded::Stamps { gen, .. } = &expanded {
            self.stamp_gen = *gen;
        }
        res?;

        // Entry point: dot rides the highest-norm hub, tracked incrementally (the just-inserted
        // slot becomes the entry iff it is strictly the longest — ties keep the earliest, which
        // matches the offline `highest_norm_entry`). Cosine/L2 use the centroid remedoid.
        match self.points.metric {
            Metric::Dot => {
                if self.points.norm2[slot as usize] > self.points.norm2[self.medoid as usize] {
                    self.medoid = slot;
                }
            }
            Metric::Cosine | Metric::L2 => self.maybe_remedoid(),
        }
        Ok(())
    }

    /// Un-embed `node_id`: mark its slot dead.
    ///
    /// **The adjacency is deliberately untouched.** The slot stays in the graph as a
    /// navigational waypoint; [`beam_search`] still expands it and still pushes its
    /// neighbours, it is simply never emitted. Pruning it instead would disconnect the region
    /// only reachable through it — a recall loss on the *live* nodes, with nothing to show
    /// for it.
    ///
    /// A no-op for an id this index does not hold (a node whose embedding only ever lived in
    /// the base is removed from *nothing* here).
    pub fn remove(&mut self, node_id: u64) {
        if let Some(slot) = self.slot_of.remove(&node_id) {
            self.dead[slot as usize] = true;
        }
    }

    /// The `k` nearest **live** neighbours of `query`, ascending by exact distance, ties broken
    /// by ascending node id (D26).
    ///
    /// * `exact` — the caller's exact metric distance from the query to a stored row. It is
    ///   used for *both* the ranking and the navigation (see the module doc: for a fixed query
    ///   they induce the same order). Passing the caller's own scorer is what keeps this arm's
    ///   scores on the same scale as every other arm feeding `merge_topk`.
    /// * `live` — a further suppression gate on the dense node id (tombstoned in the delta,
    ///   no longer carrying the index's label, …). A suppressed node is still *expanded* — the
    ///   waypoint contract again.
    ///
    /// Fewer than `k` hits come back if the neighbourhood is mostly dead; widen `beam_width`
    /// to compensate.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        beam_width: usize,
        exact: impl Fn(&[f32]) -> f32,
        live: impl Fn(u64) -> Result<bool>,
    ) -> Result<Vec<SearchHit>> {
        if query.len() != self.points.dim {
            bail!(
                "query vector has dimension {} but the index is {}-dimensional",
                query.len(),
                self.points.dim
            );
        }
        if k == 0 || self.node_id.is_empty() {
            return Ok(Vec::new());
        }
        beam_search(
            BeamParams {
                medoid: self.medoid,
                beam_width,
                k,
                num_nodes: self.node_id.len(),
            },
            |i| exact(self.points.row(i)),
            |i| {
                Ok((
                    self.points.row(i).to_vec(),
                    self.adjacency[i as usize].clone(),
                ))
            },
            &exact,
            |i| {
                if self.dead[i as usize] {
                    return Ok(None);
                }
                let id = self.node_id[i as usize];
                Ok(live(id)?.then_some(id))
            },
        )
    }

    /// The resident footprint. Bounded by the delta that feeds it: `slots × (dim · 4 + R · 4 +
    /// …)`, and `slots` is capped by the caller's `maxVectors` valve.
    pub fn resident_bytes(&self) -> usize {
        let f32s = self.points.data.len() * std::mem::size_of::<f32>();
        let norms = self.points.norm2.len() * std::mem::size_of::<f64>();
        let ids = self.node_id.len() * std::mem::size_of::<u64>();
        let dead = self.dead.len();
        let adj: usize = self
            .adjacency
            .iter()
            .map(|v| {
                v.capacity() * std::mem::size_of::<VamanaIndex>() + std::mem::size_of::<Vec<u32>>()
            })
            .sum();
        // A `HashMap` entry is the pair plus hashbrown's control byte and load-factor slack;
        // 2× the pair is the conventional, deliberately conservative estimate.
        let map = self.slot_of.len()
            * 2
            * (std::mem::size_of::<u64>() + std::mem::size_of::<VamanaIndex>());
        let scratch = self.stamps.len() * std::mem::size_of::<u32>()
            + self.sum.len() * std::mem::size_of::<f64>();
        f32s + norms + ids + dead + adj + map + scratch + std::mem::size_of::<Self>()
    }

    /// Recompute the entry point as the slot closest to the running centroid, on a doubling
    /// schedule so the O(slots · dim) scan amortises to O(dim) per insert.
    ///
    /// A fixed first-inserted entry point is not good enough: the first vector into a fresh
    /// delta is an arbitrary one, and if it is an outlier every beam starts its walk from the
    /// edge of the cloud. That does not error — it just quietly costs recall.
    fn maybe_remedoid(&mut self) {
        let n = self.node_id.len();
        if n < self.remedoid_at {
            return;
        }
        self.remedoid_at = n.saturating_mul(2);
        let inv = 1.0 / n as f64;
        let mean: Vec<f32> = self.sum.iter().map(|&x| (x * inv) as f32).collect();
        let mut best = (f64::INFINITY, self.medoid);
        for i in 0..n as VamanaIndex {
            let d = l2_sq_simd(self.points.row(i), &mean);
            if d < best.0 {
                best = (d, i);
            }
        }
        self.medoid = best.1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pq::Lcg;

    fn unit_vectors(dim: usize, n: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = Lcg(seed);
        (0..n)
            .map(|_| (0..dim).map(|_| (rng.next_f64() as f32) - 0.5).collect())
            .collect()
    }

    /// The exact cosine distance, computed independently of anything under test (this is the
    /// same shape as `slater::vector::distance`, but it is a *test's* definition of truth —
    /// the production scorer is passed in by the caller, so the index has none of its own).
    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let mut dot = 0.0f64;
        let (mut na, mut nb) = (0.0f64, 0.0f64);
        for (x, y) in a.iter().zip(b) {
            dot += *x as f64 * *y as f64;
            na += *x as f64 * *x as f64;
            nb += *y as f64 * *y as f64;
        }
        if na == 0.0 || nb == 0.0 {
            return 1.0;
        }
        (1.0 - dot / (na.sqrt() * nb.sqrt())) as f32
    }

    fn l2sq(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    fn neg_dot(a: &[f32], b: &[f32]) -> f32 {
        -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>()
    }

    /// Brute force over an independently-maintained live set — the ground truth for recall.
    /// Ties broken exactly as [`beam_search`] does (D26: score, then ascending node id).
    fn brute(
        live: &[(u64, Vec<f32>)],
        query: &[f32],
        k: usize,
        d: fn(&[f32], &[f32]) -> f32,
    ) -> Vec<u64> {
        let mut scored: Vec<(f32, u64)> = live.iter().map(|(id, v)| (d(query, v), *id)).collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(_, id)| id).collect()
    }

    fn always_live(_: u64) -> Result<bool> {
        Ok(true)
    }

    /// The headline test. 20k inserts, 5k updates (re-embeds), 3k deletes, then recall@10
    /// against a brute force over an independently-tracked **live** set.
    ///
    /// The bar is deliberately high (0.95): the index is small and `L` is wide, so a
    /// mediocre-but-not-broken graph still clears 0.9. A back-link bug — `insert_point`'s
    /// symmetric edge never written, say — shows up as recall in the 0.5–0.8 range, which a
    /// lower bar would wave through.
    #[test]
    fn rw_index_recall_vs_exact_brute_force() {
        let dim = 24;
        let mut rw = RwVamana::new(dim, Metric::Cosine);
        let vecs = unit_vectors(dim, 20_000, 0x5eed_1234_9876_0001);

        // The independently-maintained truth: id → its current live vector.
        let mut live: HashMap<u64, Vec<f32>> = HashMap::new();
        for (i, v) in vecs.iter().enumerate() {
            let id = i as u64;
            rw.insert(id, v).unwrap();
            live.insert(id, v.clone());
        }
        assert_eq!(rw.live_count(), 20_000);

        // 5k updates: every 4th id gets a *new* vector. The old row must stop being emitted.
        let fresh = unit_vectors(dim, 5_000, 0xabcd_ef01_2345_6789);
        for (j, v) in fresh.iter().enumerate() {
            let id = (j * 4) as u64;
            rw.insert(id, v).unwrap();
            live.insert(id, v.clone());
        }
        assert_eq!(
            rw.live_count(),
            20_000,
            "an update must not add a live vector"
        );
        assert_eq!(
            rw.slots(),
            25_000,
            "an update appends a row and deads the old"
        );

        // 3k deletes, chosen to overlap the updated set (ids 0, 4, 8, … are both).
        for j in 0..3_000u64 {
            let id = j * 6 + 1;
            rw.remove(id);
            live.remove(&id);
        }
        assert_eq!(rw.live_count(), live.len());

        let truth: Vec<(u64, Vec<f32>)> = live.into_iter().collect();
        let queries = unit_vectors(dim, 40, 0x1111_2222_3333_4444);
        let k = 10;
        let mut total = 0.0f64;
        for q in &queries {
            let hits = rw.search(q, k, 64, |v| cosine(q, v), always_live).unwrap();
            let got: std::collections::HashSet<u64> = hits.iter().map(|h| h.node_id).collect();
            assert_eq!(got.len(), hits.len(), "a node id came back twice: {hits:?}");
            let want = brute(&truth, q, k, cosine);
            let found = want.iter().filter(|id| got.contains(id)).count();
            total += found as f64 / k as f64;
        }
        let recall = total / queries.len() as f64;
        assert!(
            recall >= 0.95,
            "recall@{k} was {recall:.3}, expected >= 0.95 — a back-link or prune bug"
        );
    }

    /// An update must replace, not duplicate: the id appears **once**, at the **new**
    /// distance. The failure mode is silent — two rows for one id, both live, and the merge
    /// downstream cannot tell which is stale.
    #[test]
    fn rw_index_update_returns_the_new_vector_exactly_once() {
        let mut rw = RwVamana::new(2, Metric::L2);
        // A little cloud so the graph is not the degenerate complete one.
        for i in 0..40u64 {
            let a = (i as f32) * 0.25;
            rw.insert(i + 100, &[a, 10.0]).unwrap();
        }
        // Node 7 starts far from the origin, then is re-embedded right on top of it.
        rw.insert(7, &[50.0, 50.0]).unwrap();
        rw.insert(7, &[0.0, 0.0]).unwrap();

        let query = [0.0f32, 0.0];
        let hits = rw
            .search(&query, 5, 64, |v| l2sq(&query, v), always_live)
            .unwrap();
        let ids: Vec<u64> = hits.iter().map(|h| h.node_id).collect();
        assert_eq!(
            ids.iter().filter(|&&id| id == 7).count(),
            1,
            "node 7 came back {ids:?} — the superseded slot was still live"
        );
        let hit = hits.iter().find(|h| h.node_id == 7).unwrap();
        assert_eq!(hit.exact, 0.0, "node 7 must score at its NEW vector (0,0)");
        assert_eq!(hits[0].node_id, 7, "the re-embedded node is the nearest");
    }

    /// A deleted node stays a **waypoint**. The bridge here is the only path from the entry
    /// point to what lies behind it: delete it, and if the adjacency were pruned the node
    /// behind it would vanish from every search — silently, with full-looking results.
    ///
    /// Built as a line so the graph is forced: 0 (entry) — 1 (bridge) — 2 (the prize), with
    /// each point only near its neighbours.
    #[test]
    fn rw_index_delete_is_a_navigable_waypoint() {
        let mut rw = RwVamana::new(2, Metric::L2);
        rw.insert(900, &[9.0, 0.0]).unwrap(); // slot 0 — the entry point
        rw.insert(901, &[1.0, 0.0]).unwrap(); // slot 1 — the bridge
        rw.insert(902, &[0.0, 0.0]).unwrap(); // slot 2 — the prize
                                              // Force the line topology: 0 ↔ 1 ↔ 2, and nothing else. (A 3-node insert would give a
                                              // complete graph, which cannot express "the sole bridge".)
        rw.adjacency = vec![vec![1], vec![0, 2], vec![1]];
        rw.medoid = 0;

        rw.remove(901);
        assert!(!rw.contains(901));

        let query = [0.0f32, 0.0];
        let hits = rw
            .search(&query, 3, 8, |v| l2sq(&query, v), always_live)
            .unwrap();
        let ids: Vec<u64> = hits.iter().map(|h| h.node_id).collect();
        assert!(
            !ids.contains(&901),
            "the deleted bridge must not be emitted: {ids:?}"
        );
        assert_eq!(
            ids,
            vec![902, 900],
            "the deleted bridge must still be WALKED THROUGH to reach 902"
        );
    }

    /// `resident_bytes` must track the rows actually held — including the dead ones an update
    /// leaves behind, which is exactly what a `maxVectors` valve has to bound.
    #[test]
    fn rw_index_resident_bytes_bounded() {
        let dim = 64;
        let mut rw = RwVamana::new(dim, Metric::Cosine);
        assert!(rw.resident_bytes() < 4096, "an empty index is ~free");
        let vecs = unit_vectors(dim, 2_000, 0x9999_8888_7777_6666);
        for (i, v) in vecs.iter().enumerate() {
            rw.insert(i as u64, v).unwrap();
        }
        let after_inserts = rw.resident_bytes();

        // Floor: the rows themselves. Ceiling: rows + adjacency at full degree + the id/dead/
        // norm/map/scratch overheads, with generous slack. Anything outside means the footprint
        // is not what the `maxVectors` bound is computed against.
        let rows = 2_000 * dim * 4;
        let adj = 2_000 * RW_R * 4;
        assert!(after_inserts > rows, "{after_inserts} <= rows {rows}");
        assert!(
            after_inserts < rows + adj + 2_000 * 128 + 65_536,
            "{after_inserts} is more than the row + adjacency + overhead bound"
        );

        // 500 re-embeds add 500 rows and reclaim none — the growth must show.
        for i in 0..500u64 {
            rw.insert(i, &vecs[(i as usize + 7) % 2_000]).unwrap();
        }
        assert_eq!(rw.slots(), 2_500);
        assert_eq!(rw.live_count(), 2_000);
        assert!(
            rw.resident_bytes() > after_inserts + 500 * dim * 4,
            "an update's appended row must be charged"
        );

        // Deletes reclaim nothing either — the slot stays a waypoint, rows and edges and all.
        // The valve must know that, or it will believe a delete-heavy delta shrank the index.
        let before_deletes = rw.resident_bytes();
        for i in 0..500u64 {
            rw.remove(i);
        }
        assert_eq!(rw.live_count(), 1_500);
        assert_eq!(rw.slots(), 2_500, "a delete reclaims no row");
        assert!(
            rw.resident_bytes() > before_deletes * 9 / 10,
            "a delete freed a slot map entry and nothing else; {} vs {before_deletes}",
            rw.resident_bytes()
        );
        assert!(
            rw.resident_bytes() > 2_500 * dim * 4,
            "the dead rows are still charged"
        );
    }

    /// The dot/MIPS arm is **IP-native** (HIK-137): the graph is built over raw inner product
    /// with no norm augmentation, so the moving-`M` hazard that used to threaten this arm cannot
    /// exist — there is no `M`. This test pins the two things that must instead hold: (a) MIPS
    /// recall against an independent brute-force IP truth, and (b) the entry point rides the
    /// **highest-norm** row.
    ///
    /// The vectors go in with norms ramping 1 → 8 in ascending insert order, so the highest-norm
    /// row is the last-inserted slot — the incremental entry-point tracking must land there.
    #[test]
    fn rw_index_dot_is_ip_native_and_enters_at_the_highest_norm() {
        let dim = 8;
        let n = 400;
        let mut rw = RwVamana::new(dim, Metric::Dot);
        let mut live: Vec<(u64, Vec<f32>)> = Vec::new();
        for (i, v) in unit_vectors(dim, n, 0x0dd_ba11_0000_1234)
            .iter()
            .enumerate()
        {
            // Norms ramp 1 → 8 in insert order, so the last-inserted row is the longest.
            let scale = 1.0 + 7.0 * (i as f32) / (n as f32);
            let nrm = (v.iter().map(|x| x * x).sum::<f32>()).sqrt().max(1e-6);
            let scaled: Vec<f32> = v.iter().map(|x| x * scale / nrm).collect();
            rw.insert(i as u64, &scaled).unwrap();
            live.push((i as u64, scaled));
        }

        // (b) The IP entry point is the highest-norm slot — here the last one inserted.
        assert_eq!(
            rw.medoid,
            (n - 1) as VamanaIndex,
            "the dot entry point must ride the highest-norm row, not a centroid medoid"
        );

        // (a) Recall@k vs the independent brute-force IP top-k. The bar is high: IP-native
        // construction recovers essentially all of it on this benign norm spread, so a regression
        // to the old augmented build (which craters on MIPS-hard data) shows up immediately.
        let k = 5;
        let queries = unit_vectors(dim, 20, 0xfeed_0000_0000_0001);
        let mut total = 0.0f64;
        for q in &queries {
            let hits = rw.search(q, k, 64, |v| neg_dot(q, v), always_live).unwrap();
            let got: std::collections::HashSet<u64> = hits.iter().map(|h| h.node_id).collect();
            let want = brute(&live, q, k, neg_dot);
            total += want.iter().filter(|id| got.contains(id)).count() as f64 / k as f64;
        }
        let recall = total / queries.len() as f64;
        assert!(
            recall >= 0.9,
            "IP-native MIPS recall@{k} was {recall:.3} — the dot delta graph is not navigating by raw IP"
        );
    }

    /// A MIPS-hard delta: one heavy-norm outlier plus a cloud, exactly the shape that craters an
    /// augmented (L2-reduced) build. IP-native construction must still recover the true IP top-k.
    /// This is the delta twin of the base build's adversarial (Pareto) fixture.
    #[test]
    fn rw_index_dot_ip_native_survives_a_heavy_norm_outlier() {
        let dim = 16;
        let n = 600;
        let mut rw = RwVamana::new(dim, Metric::Dot);
        let mut live: Vec<(u64, Vec<f32>)> = Vec::new();
        for (i, v) in unit_vectors(dim, n, 0xa5a5_0f0f_1234_5678)
            .iter()
            .enumerate()
        {
            // Most rows are unit-ish; every 97th is a ~30× outlier — a high-norm vector is "near"
            // almost every query under IP, the navigation hazard the augmented build fails.
            let scale = if i.is_multiple_of(97) { 30.0 } else { 1.0 };
            let nrm = (v.iter().map(|x| x * x).sum::<f32>()).sqrt().max(1e-6);
            let scaled: Vec<f32> = v.iter().map(|x| x * scale / nrm).collect();
            rw.insert(i as u64, &scaled).unwrap();
            live.push((i as u64, scaled));
        }
        let k = 10;
        let queries = unit_vectors(dim, 30, 0x1357_9bdf_0000_0002);
        let mut total = 0.0f64;
        for q in &queries {
            let hits = rw.search(q, k, 64, |v| neg_dot(q, v), always_live).unwrap();
            let got: std::collections::HashSet<u64> = hits.iter().map(|h| h.node_id).collect();
            let want = brute(&live, q, k, neg_dot);
            total += want.iter().filter(|id| got.contains(id)).count() as f64 / k as f64;
        }
        let recall = total / queries.len() as f64;
        assert!(
            recall >= 0.9,
            "IP-native MIPS recall@{k} on a heavy-norm-outlier delta was {recall:.3}"
        );
    }

    /// Edge cases the ticket named: an empty index, the very first insert (the medoid
    /// bootstrap — there is no graph to search yet), and removing an id that was never here.
    #[test]
    fn rw_index_empty_first_insert_and_absent_remove() {
        let mut rw = RwVamana::new(3, Metric::Cosine);
        assert!(rw.is_empty());
        let q = [1.0f32, 0.0, 0.0];
        assert!(rw
            .search(&q, 5, 32, |v| cosine(&q, v), always_live)
            .unwrap()
            .is_empty());

        // Removing an id the index never held is a no-op, not a panic: a node whose embedding
        // only ever lived in the base is "removed" from nothing here.
        rw.remove(42);
        assert!(rw.is_empty());

        // The first insert bootstraps the medoid with no greedy search to run.
        rw.insert(42, &[1.0, 0.0, 0.0]).unwrap();
        let hits = rw
            .search(&q, 5, 32, |v| cosine(&q, v), always_live)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, 42);

        // Re-removing, then re-inserting, the same id.
        rw.remove(42);
        assert!(rw.is_empty());
        rw.remove(42); // idempotent
        assert!(rw
            .search(&q, 5, 32, |v| cosine(&q, v), always_live)
            .unwrap()
            .is_empty());
        rw.insert(42, &[0.0, 1.0, 0.0]).unwrap();
        let hits = rw
            .search(&q, 5, 32, |v| cosine(&q, v), always_live)
            .unwrap();
        assert_eq!(
            hits.len(),
            1,
            "a resurrected id must come back exactly once"
        );
        assert_eq!(hits[0].node_id, 42);
        assert_eq!(rw.slots(), 2, "the dead row plus the resurrected one");
    }

    /// The `live` gate suppresses without pruning — same contract as a delete, but decided by
    /// the *caller* (a delta tombstone, or a node that no longer carries the index's label).
    #[test]
    fn rw_index_live_gate_suppresses_but_still_navigates() {
        let mut rw = RwVamana::new(2, Metric::L2);
        rw.insert(900, &[9.0, 0.0]).unwrap();
        rw.insert(901, &[1.0, 0.0]).unwrap();
        rw.insert(902, &[0.0, 0.0]).unwrap();
        rw.adjacency = vec![vec![1], vec![0, 2], vec![1]];
        rw.medoid = 0;

        let query = [0.0f32, 0.0];
        let hits = rw
            .search(&query, 3, 8, |v| l2sq(&query, v), |id| Ok(id != 901))
            .unwrap();
        assert_eq!(
            hits.iter().map(|h| h.node_id).collect::<Vec<_>>(),
            vec![902, 900],
            "a caller-suppressed bridge must still be walked through"
        );
    }

    /// A vector of exactly zero has no direction. It must land in the index as a finite row
    /// (not `NaN` — `total_cmp` *orders* a NaN rather than rejecting it, so it would silently
    /// take a top-k slot) and score as maximally distant under cosine.
    #[test]
    fn rw_index_zero_vector_is_finite_and_maximally_distant() {
        let mut rw = RwVamana::new(2, Metric::Cosine);
        rw.insert(1, &[0.0, 0.0]).unwrap();
        rw.insert(2, &[3.0, 4.0]).unwrap();
        assert!(rw.points.data.iter().all(|x| x.is_finite()));

        let q = [3.0f32, 4.0];
        let hits = rw
            .search(&q, 2, 16, |v| cosine(&q, v), always_live)
            .unwrap();
        assert_eq!(hits[0].node_id, 2);
        assert!(hits[0].exact.abs() < 1e-6);
        assert_eq!(hits[1].node_id, 1);
        assert!((hits[1].exact - 1.0).abs() < 1e-6, "zero row ⇒ distance 1");
    }

    #[test]
    fn rw_index_rejects_a_dimension_mismatch() {
        let mut rw = RwVamana::new(3, Metric::Cosine);
        let err = rw.insert(1, &[1.0, 0.0]).unwrap_err();
        assert!(err.to_string().contains("dimension"), "{err}");
        rw.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        let q = [1.0f32, 0.0];
        let err = rw
            .search(&q, 1, 8, |v| cosine(&q, v), always_live)
            .unwrap_err();
        assert!(err.to_string().contains("dimension"), "{err}");
    }

    #[test]
    fn rw_index_rejects_a_nonfinite_component() {
        // The delta write path is a distinct entry point (HIK-134): a NaN/±inf here poisons the
        // resident buffer immediately, and `max_norm2.max(norm2)` silently drops the NaN so no
        // later guard catches it. Assert the *typed* finiteness error, and that the insert did
        // not partially mutate the index (a clean insert afterwards still works).
        let mut rw = RwVamana::new(3, Metric::Cosine);
        for bad in [
            [f32::NAN, 0.0, 0.0],
            [0.0, f32::INFINITY, 0.0],
            [0.0, 0.0, f32::NEG_INFINITY],
        ] {
            let err = rw.insert(1, &bad).unwrap_err();
            assert!(
                err.downcast_ref::<crate::pq::NonFiniteEmbedding>()
                    .is_some(),
                "must be the typed finiteness error, got: {err}"
            );
        }
        assert!(rw.is_empty(), "a rejected insert must not add a slot");
        rw.insert(1, &[1.0, 0.0, 0.0]).unwrap();
        assert_eq!(
            rw.live_count(),
            1,
            "a finite insert still works after rejection"
        );
    }
}
