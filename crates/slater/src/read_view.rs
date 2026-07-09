// SPDX-License-Identifier: Apache-2.0
//! The [`ReadView`] read surface and the [`MergedView`] delta overlay.
//!
//! The executor ([`crate::exec::Engine`]) and planner ([`crate::plan`]) read the
//! graph through a fixed set of methods that used to be inherent on
//! [`Generation`]. `ReadView` lifts that surface into a trait so the *same*
//! executor can run over either:
//!
//! - a bare [`Generation`] (the read-only path, and every test) — the trait is an
//!   identity pass-through to the inherent methods; or
//! - a [`MergedView`], which overlays the writable layer's [`DeltaSnapshot`] on
//!   top of an immutable core generation.
//!
//! # DESIGN: storage-reader overlay, not executor-level merge
//! The overlay lives *below* the executor's read surface (option A in
//! `docs/WRITABLE-PLAN.md`): the executor's logic is unchanged, and the merge is
//! confined to `MergedView`'s method bodies. `ReadView: Send + Sync` so a
//! `&dyn ReadView` can be handed to the rayon fan-out readers exactly as
//! `&Generation` was.
//!
//! # Empty-delta fast path
//! Phase 0 only ever constructs [`MergedView::read_only`] (an empty delta) and the
//! overlay methods short-circuit on [`DeltaSnapshot::is_empty`], so a graph with no
//! writes pays only a single predictable branch over the pure-core path. Property,
//! tombstone and topology overlays are layered into the method bodies in later
//! phases; the trait surface does not change.

use anyhow::Result;
use graph_format::columns::PropsReader;
use graph_format::ids::{Generation as GenId, NodeId, Value};
use graph_format::isam::IsamReader;
use graph_format::manifest::Manifest;
use graph_format::nodelabels::NodeLabelsReader;
use graph_format::topology::TopologyReader;
use graph_format::vectors::VectorStoreReader;
use slater_delta::DeltaSnapshot;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use crate::generation::{Generation, RelEndpointSide, VamanaIndex};

/// The graph read surface the executor and planner depend on.
///
/// Every method mirrors an inherent method of [`Generation`]; the two extra
/// methods, [`ReadView::delta`] and [`ReadView::core_generation`], expose the
/// overlay handle and the underlying immutable core to the merge logic.
pub trait ReadView: Send + Sync {
    // ── Identity / metadata ────────────────────────────────────────────────
    fn uuid(&self) -> GenId;
    fn manifest(&self) -> &Manifest;
    fn node_count(&self) -> u64;
    fn edge_count(&self) -> u64;

    /// The number of nodes a read actually **sees**: the core's nodes, plus the delta's
    /// born ones, minus every row the delta suppresses.
    ///
    /// Deliberately distinct from [`Self::node_count`], which is the dense-id **scan
    /// bound** (`core + every born id`, tombstoned or not) and so over-counts a delta
    /// with deletes. Answering `count(*)` from this keeps it a metadata read on both the
    /// pure-core and the merged path — see [`MergedView::live_node_count`].
    fn live_node_count(&self) -> u64;
    /// As [`Self::live_node_count`], restricted to nodes carrying `label_id`. Fallible
    /// because the merged path reads the labels of suppressed **core** rows (nodes may
    /// carry several labels, while a delta identity records only the matched one).
    fn live_label_node_count(&self, label_id: u32) -> Result<u64>;

    /// Live `labels(n)[0]` groups — `(first-label name, count)`, with `None` naming the
    /// zero-label bucket. Delta-born nodes carry exactly one label (their `MERGE` named
    /// it), which may be a label the core never defined, so the merged groups are keyed
    /// by name rather than by core label id.
    fn live_first_label_groups(&self) -> Result<Vec<(Option<String>, u64)>>;

    /// The live merged edge count, or `None` when the delta makes it uncomputable from
    /// counters (see [`DeltaSnapshot::edge_counts_are_exact`]) and the caller must fall
    /// back to full execution.
    ///
    /// Deleting a **node** silently kills its incident edges (the executor drops an edge
    /// whose endpoint is suppressed), so the merged count subtracts the degree of every
    /// tombstoned node. That is O(1) when nothing is deleted and O(Σ degree of the deleted
    /// nodes) otherwise — proportional to the delta's blast radius, never to the graph.
    fn live_edge_count(&self) -> Result<Option<u64>>;

    /// Live `type(r)` groups — `(reltype name, count)` — or `None` when uncomputable, on
    /// the same terms as [`Self::live_edge_count`].
    fn live_reltype_edge_groups(&self) -> Result<Option<Vec<(String, u64)>>>;

    // ── Symbol-table lookups ───────────────────────────────────────────────
    fn label_id(&self, name: &str) -> Option<u32>;
    fn reltype_id(&self, name: &str) -> Option<u32>;
    fn property_key_id(&self, name: &str) -> Option<u32>;
    fn label_name(&self, id: u32) -> Option<&str>;
    fn reltype_name(&self, id: u32) -> Option<&str>;
    fn property_key_name(&self, id: u32) -> Option<&str>;

    // ── Readers ─────────────────────────────────────────────────────────────
    fn node_props(&self) -> &PropsReader;
    fn node_labels(&self) -> &NodeLabelsReader;
    fn edge_props(&self) -> &PropsReader;
    fn topology(&self) -> &TopologyReader;
    fn vectors(&self) -> &VectorStoreReader;
    fn range_index(&self, name: &str) -> Option<&IsamReader>;
    fn property_histogram(&self, name: &str) -> Option<&[(Value, u64)]>;
    fn vamana_index(&self, label: &str, property: &str) -> Option<&VamanaIndex>;

    // ── Counts / marginals ──────────────────────────────────────────────────
    fn label_node_count(&self, label_id: u32) -> u64;
    fn reltype_edge_count(&self, reltype_id: u32) -> u64;
    fn first_label_count(&self, label_id: u32) -> u64;
    fn has_first_label_counts(&self) -> bool;
    fn first_labelled_node_count(&self) -> u64;
    fn src_label_reltype_count(&self, src_label_id: u32, reltype_id: u32) -> Option<u64>;
    fn reltype_tgt_label_count(&self, reltype_id: u32, tgt_label_id: u32) -> Option<u64>;
    fn schema_triple_count(
        &self,
        src_label_id: u32,
        reltype_id: u32,
        tgt_label_id: u32,
    ) -> Option<u64>;
    fn has_reltype_postings(&self) -> bool;
    fn reltype_source_count(&self, reltype_id: u32) -> u64;
    fn reltype_target_count(&self, reltype_id: u32) -> u64;

    // ── Scans ────────────────────────────────────────────────────────────────
    fn collect_nodes_with_label(&self, label_id: u32) -> Result<Vec<u64>>;
    fn collect_endpoint_nodes_for_reltypes(
        &self,
        reltype_ids: &[u32],
        side: RelEndpointSide,
    ) -> Result<Vec<u64>>;

    // ── Overlay handles ──────────────────────────────────────────────────────
    /// The delta layers captured for this view (empty for a bare [`Generation`]).
    fn delta(&self) -> &DeltaSnapshot;
    /// The immutable core generation underneath, when the concrete type is needed
    /// (cache keying already goes through [`ReadView::uuid`]).
    fn core_generation(&self) -> &Generation;
}

/// The process-wide empty delta, handed out by a bare [`Generation`]'s
/// [`ReadView::delta`] so the read-only path allocates nothing per view.
fn empty_delta() -> &'static DeltaSnapshot {
    static EMPTY: OnceLock<DeltaSnapshot> = OnceLock::new();
    EMPTY.get_or_init(DeltaSnapshot::empty)
}

// A bare generation is the identity `ReadView`: every method forwards to the
// inherent method (qualified as `Generation::…` so the forward can never be
// mistaken for a recursive trait call), the delta is always empty, and the core
// is the generation itself.
impl ReadView for Generation {
    fn uuid(&self) -> GenId {
        Generation::uuid(self)
    }
    fn manifest(&self) -> &Manifest {
        Generation::manifest(self)
    }
    fn node_count(&self) -> u64 {
        Generation::node_count(self)
    }
    fn edge_count(&self) -> u64 {
        Generation::edge_count(self)
    }
    /// A pure core has no delta: every node is live.
    fn live_node_count(&self) -> u64 {
        Generation::node_count(self)
    }
    fn live_label_node_count(&self, label_id: u32) -> Result<u64> {
        Ok(Generation::label_node_count(self, label_id))
    }
    fn live_first_label_groups(&self) -> Result<Vec<(Option<String>, u64)>> {
        let mut groups: Vec<(Option<String>, u64)> = Vec::new();
        for lid in 0..Generation::manifest(self).labels.len() as u32 {
            let c = Generation::first_label_count(self, lid);
            if c > 0 {
                let name = Generation::label_name(self, lid).unwrap_or("").to_string();
                groups.push((Some(name), c));
            }
        }
        let null_count = Generation::node_count(self)
            .saturating_sub(Generation::first_labelled_node_count(self));
        if null_count > 0 {
            groups.push((None, null_count));
        }
        Ok(groups)
    }
    fn live_edge_count(&self) -> Result<Option<u64>> {
        Ok(Some(Generation::edge_count(self)))
    }
    fn live_reltype_edge_groups(&self) -> Result<Option<Vec<(String, u64)>>> {
        let mut groups = Vec::new();
        for t in 0..Generation::manifest(self).reltypes.len() as u32 {
            let c = Generation::reltype_edge_count(self, t);
            if c > 0 {
                groups.push((
                    Generation::reltype_name(self, t).unwrap_or("").to_string(),
                    c,
                ));
            }
        }
        Ok(Some(groups))
    }
    fn label_id(&self, name: &str) -> Option<u32> {
        Generation::label_id(self, name)
    }
    fn reltype_id(&self, name: &str) -> Option<u32> {
        Generation::reltype_id(self, name)
    }
    fn property_key_id(&self, name: &str) -> Option<u32> {
        Generation::property_key_id(self, name)
    }
    fn label_name(&self, id: u32) -> Option<&str> {
        Generation::label_name(self, id)
    }
    fn reltype_name(&self, id: u32) -> Option<&str> {
        Generation::reltype_name(self, id)
    }
    fn property_key_name(&self, id: u32) -> Option<&str> {
        Generation::property_key_name(self, id)
    }
    fn node_props(&self) -> &PropsReader {
        Generation::node_props(self)
    }
    fn node_labels(&self) -> &NodeLabelsReader {
        Generation::node_labels(self)
    }
    fn edge_props(&self) -> &PropsReader {
        Generation::edge_props(self)
    }
    fn topology(&self) -> &TopologyReader {
        Generation::topology(self)
    }
    fn vectors(&self) -> &VectorStoreReader {
        Generation::vectors(self)
    }
    fn range_index(&self, name: &str) -> Option<&IsamReader> {
        Generation::range_index(self, name)
    }
    fn property_histogram(&self, name: &str) -> Option<&[(Value, u64)]> {
        Generation::property_histogram(self, name)
    }
    fn vamana_index(&self, label: &str, property: &str) -> Option<&VamanaIndex> {
        Generation::vamana_index(self, label, property)
    }
    fn label_node_count(&self, label_id: u32) -> u64 {
        Generation::label_node_count(self, label_id)
    }
    fn reltype_edge_count(&self, reltype_id: u32) -> u64 {
        Generation::reltype_edge_count(self, reltype_id)
    }
    fn first_label_count(&self, label_id: u32) -> u64 {
        Generation::first_label_count(self, label_id)
    }
    fn has_first_label_counts(&self) -> bool {
        Generation::has_first_label_counts(self)
    }
    fn first_labelled_node_count(&self) -> u64 {
        Generation::first_labelled_node_count(self)
    }
    fn src_label_reltype_count(&self, src_label_id: u32, reltype_id: u32) -> Option<u64> {
        Generation::src_label_reltype_count(self, src_label_id, reltype_id)
    }
    fn reltype_tgt_label_count(&self, reltype_id: u32, tgt_label_id: u32) -> Option<u64> {
        Generation::reltype_tgt_label_count(self, reltype_id, tgt_label_id)
    }
    fn schema_triple_count(
        &self,
        src_label_id: u32,
        reltype_id: u32,
        tgt_label_id: u32,
    ) -> Option<u64> {
        Generation::schema_triple_count(self, src_label_id, reltype_id, tgt_label_id)
    }
    fn has_reltype_postings(&self) -> bool {
        Generation::has_reltype_postings(self)
    }
    fn reltype_source_count(&self, reltype_id: u32) -> u64 {
        Generation::reltype_source_count(self, reltype_id)
    }
    fn reltype_target_count(&self, reltype_id: u32) -> u64 {
        Generation::reltype_target_count(self, reltype_id)
    }
    fn collect_nodes_with_label(&self, label_id: u32) -> Result<Vec<u64>> {
        Generation::collect_nodes_with_label(self, label_id)
    }
    fn collect_endpoint_nodes_for_reltypes(
        &self,
        reltype_ids: &[u32],
        side: RelEndpointSide,
    ) -> Result<Vec<u64>> {
        Generation::collect_endpoint_nodes_for_reltypes(self, reltype_ids, side)
    }
    fn delta(&self) -> &DeltaSnapshot {
        empty_delta()
    }
    fn core_generation(&self) -> &Generation {
        self
    }
}

/// A read view that overlays a [`DeltaSnapshot`] on an immutable core generation.
///
/// Pinned as a consistent `(core, delta)` tuple for a query's whole life so a
/// mid-query freeze/swap cannot split its view. Phase 0 constructs it only via
/// [`MergedView::read_only`] (empty delta); the accessor forwards below are the
/// stable seam that later phases thread their overlay logic through.
pub struct MergedView<'g> {
    core: &'g Generation,
    delta: DeltaSnapshot,
}

impl<'g> MergedView<'g> {
    /// Overlay `delta` on `core`.
    pub fn new(core: &'g Generation, delta: DeltaSnapshot) -> Self {
        Self { core, delta }
    }

    /// A view with an empty delta — behaviourally identical to reading `core`
    /// directly, but exercising the same `MergedView` code path production uses.
    pub fn read_only(core: &'g Generation) -> Self {
        Self {
            core,
            delta: DeltaSnapshot::empty(),
        }
    }

    /// Edges killed because an endpoint was deleted, bucketed by reltype name and split
    /// into core edges and delta-born ones. A `DELETE n` tombstones only the node, but the
    /// executor drops any edge whose endpoint fails the liveness check, so a live edge
    /// count has to account for them.
    ///
    /// Each dead edge is counted **once**: the outgoing pass over a tombstoned node claims
    /// every edge leaving it, and the incoming pass skips an edge whose source is itself
    /// tombstoned (already claimed). That also makes a self-loop on a dead node count once.
    ///
    /// Cost is O(Σ degree of the tombstoned nodes) — nothing at all when the delta has no
    /// deletes, and always proportional to the delta's blast radius rather than the graph.
    fn edges_lost_to_node_tombstones(&self) -> Result<LostEdges> {
        let mut lost = LostEdges::default();
        let suppressed = self.delta.effective_tombstoned_ids();
        if suppressed.is_empty() {
            return Ok(lost);
        }
        let core_count = self.core.node_count();
        let dead: HashSet<u64> = suppressed.iter().copied().collect();

        for &dense in suppressed {
            // Core adjacency exists only for core nodes.
            if dense < core_count {
                for a in self.core.topology().outgoing(NodeId(dense))? {
                    let name = self.core.reltype_name(a.reltype).unwrap_or("").to_string();
                    *lost.core.entry(name).or_default() += 1;
                }
                for a in self.core.topology().incoming(NodeId(dense))? {
                    if dead.contains(&(a.neighbour.index() as u64)) {
                        continue; // claimed by that source's outgoing pass
                    }
                    let name = self.core.reltype_name(a.reltype).unwrap_or("").to_string();
                    *lost.core.entry(name).or_default() += 1;
                }
            }
            // Delta-born edges incident to this dead node (either endpoint may be born).
            for e in self.delta.out_edges(dense) {
                if e.edge_id.is_some() && !e.tombstoned {
                    *lost.born.entry(e.reltype).or_default() += 1;
                }
            }
            for e in self.delta.in_edges(dense) {
                if e.edge_id.is_some() && !e.tombstoned && !dead.contains(&e.other) {
                    *lost.born.entry(e.reltype).or_default() += 1;
                }
            }
        }
        Ok(lost)
    }
}

/// Per-reltype tallies of edges removed by node tombstones, kept apart because the core
/// and born terms are added back from different counters.
#[derive(Default)]
struct LostEdges {
    core: HashMap<String, u64>,
    born: HashMap<String, u64>,
}

// The overlay view. Phase 0 forwards every accessor to the core; the delta is
// carried and exposed via `delta()` for the merge logic that later phases add.
impl ReadView for MergedView<'_> {
    fn uuid(&self) -> GenId {
        self.core.uuid()
    }
    fn manifest(&self) -> &Manifest {
        self.core.manifest()
    }
    fn node_count(&self) -> u64 {
        // Delta-born nodes (Phase 2c) occupy synthetic dense ids past the core count,
        // so the merged count includes them. A full scan (`0..node_count`) therefore
        // yields the core ids then the synthetic ones; tombstone suppression drops any
        // deleted id (core or born) at the scan boundary.
        self.core.node_count() + self.delta.born_count()
    }
    fn edge_count(&self) -> u64 {
        // Delta-born edges (Phase 3) occupy synthetic dense edge ids past the core
        // count, so the merged count includes them (matching the node overlay above).
        self.core.edge_count() + self.delta.born_edge_count()
    }
    /// `core + born − suppressed`, all three terms O(1)-or-O(#tombstones):
    /// `born_count` is a per-level counter and the suppressed set is the delta's
    /// (small) tombstone set folded newest-wins. Never touches the core's node blocks,
    /// so `count(*)` over a written graph stays a metadata read rather than the
    /// 91.6M-row scan the pure-core fast path used to be traded for.
    fn live_node_count(&self) -> u64 {
        (self.core.node_count() + self.delta.born_count())
            .saturating_sub(self.delta.effective_tombstoned_ids().len() as u64)
    }

    fn live_label_node_count(&self, label_id: u32) -> Result<u64> {
        let Some(label) = self.core.label_name(label_id) else {
            return Ok(0);
        };
        let core_count = self.core.node_count();
        // Suppressed rows carrying this label. A **core** id's labels come from the core
        // (a node may carry several, while the delta identity records only the label the
        // write matched on); a **born** id carries exactly the one label its `MERGE`
        // named, recoverable from its delta identity.
        let mut suppressed = 0u64;
        for &dense in self.delta.effective_tombstoned_ids() {
            let carries = if dense < core_count {
                self.core.node_labels().labels(dense)?.contains(&label_id)
            } else {
                self.delta
                    .node_identity_by_dense(dense)
                    .is_some_and(|(l, _, _)| l == label)
            };
            if carries {
                suppressed += 1;
            }
        }
        Ok(
            (self.core.label_node_count(label_id) + self.delta.born_count_with_label(label))
                .saturating_sub(suppressed),
        )
    }

    fn live_first_label_groups(&self) -> Result<Vec<(Option<String>, u64)>> {
        let core_count = self.core.node_count();
        // Suppressed rows, bucketed by the first label they *had*. A core row's labels come
        // from the core (it may carry several); a born row carries exactly the one its
        // `MERGE` named.
        let mut suppressed: HashMap<Option<String>, i64> = HashMap::new();
        for &dense in self.delta.effective_tombstoned_ids() {
            let first: Option<String> = if dense < core_count {
                self.core
                    .node_labels()
                    .labels(dense)?
                    .first()
                    .and_then(|&lid| self.core.label_name(lid))
                    .map(str::to_string)
            } else {
                self.delta.node_identity_by_dense(dense).map(|(l, _, _)| l)
            };
            *suppressed.entry(first).or_default() += 1;
        }
        let born: HashMap<String, u64> = self
            .delta
            .born_labels()
            .into_iter()
            .map(|l| {
                let n = self.delta.born_count_with_label(&l);
                (l, n)
            })
            .collect();

        // Emit in core label-id order, then delta-only labels by name, then the null
        // bucket — a deterministic order the metadata result builder can rely on.
        let mut out: Vec<(Option<String>, u64)> = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();
        for lid in 0..self.core.manifest().labels.len() as u32 {
            let name = self.core.label_name(lid).unwrap_or("").to_string();
            let key = Some(name.clone());
            let live = self.core.first_label_count(lid) as i64
                + born.get(&name).copied().unwrap_or(0) as i64
                - suppressed.get(&key).copied().unwrap_or(0);
            seen.insert(self.core.label_name(lid).unwrap_or(""));
            if live > 0 {
                out.push((key, live as u64));
            }
        }
        let mut extra: Vec<&String> = born.keys().filter(|l| !seen.contains(l.as_str())).collect();
        extra.sort();
        for name in extra {
            let key = Some(name.clone());
            let live = born[name] as i64 - suppressed.get(&key).copied().unwrap_or(0);
            if live > 0 {
                out.push((key, live as u64));
            }
        }
        let null_live = core_count.saturating_sub(self.core.first_labelled_node_count()) as i64
            - suppressed.get(&None).copied().unwrap_or(0);
        if null_live > 0 {
            out.push((None, null_live as u64));
        }
        Ok(out)
    }

    fn live_edge_count(&self) -> Result<Option<u64>> {
        if !self.delta.edge_counts_are_exact() {
            return Ok(None);
        }
        let lost = self.edges_lost_to_node_tombstones()?;
        let dead: u64 = lost.core.values().chain(lost.born.values()).sum();
        Ok(Some(
            (self.core.edge_count() + self.delta.born_edge_count()).saturating_sub(dead),
        ))
    }

    fn live_reltype_edge_groups(&self) -> Result<Option<Vec<(String, u64)>>> {
        if !self.delta.edge_counts_are_exact() {
            return Ok(None);
        }
        let lost = self.edges_lost_to_node_tombstones()?;
        let dead = |name: &str| -> i64 {
            (lost.core.get(name).copied().unwrap_or(0) + lost.born.get(name).copied().unwrap_or(0))
                as i64
        };
        let born: HashMap<String, u64> = self
            .delta
            .born_edge_reltypes()
            .into_iter()
            .map(|t| {
                let n = self.delta.born_edge_count_with_reltype(&t);
                (t, n)
            })
            .collect();

        // Core reltype-id order, then delta-only reltypes by name (see the label groups).
        let mut out: Vec<(String, u64)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for t in 0..self.core.manifest().reltypes.len() as u32 {
            let name = self.core.reltype_name(t).unwrap_or("").to_string();
            let live = self.core.reltype_edge_count(t) as i64
                + born.get(&name).copied().unwrap_or(0) as i64
                - dead(&name);
            seen.insert(name.clone());
            if live > 0 {
                out.push((name, live as u64));
            }
        }
        let mut extra: Vec<&String> = born.keys().filter(|t| !seen.contains(*t)).collect();
        extra.sort();
        for name in extra {
            let live = born[name] as i64 - dead(name);
            if live > 0 {
                out.push((name.clone(), live as u64));
            }
        }
        Ok(Some(out))
    }
    fn label_id(&self, name: &str) -> Option<u32> {
        self.core.label_id(name)
    }
    fn reltype_id(&self, name: &str) -> Option<u32> {
        self.core.reltype_id(name)
    }
    fn property_key_id(&self, name: &str) -> Option<u32> {
        self.core.property_key_id(name)
    }
    fn label_name(&self, id: u32) -> Option<&str> {
        self.core.label_name(id)
    }
    fn reltype_name(&self, id: u32) -> Option<&str> {
        self.core.reltype_name(id)
    }
    fn property_key_name(&self, id: u32) -> Option<&str> {
        self.core.property_key_name(id)
    }
    fn node_props(&self) -> &PropsReader {
        self.core.node_props()
    }
    fn node_labels(&self) -> &NodeLabelsReader {
        self.core.node_labels()
    }
    fn edge_props(&self) -> &PropsReader {
        self.core.edge_props()
    }
    fn topology(&self) -> &TopologyReader {
        self.core.topology()
    }
    fn vectors(&self) -> &VectorStoreReader {
        self.core.vectors()
    }
    fn range_index(&self, name: &str) -> Option<&IsamReader> {
        self.core.range_index(name)
    }
    fn property_histogram(&self, name: &str) -> Option<&[(Value, u64)]> {
        self.core.property_histogram(name)
    }
    fn vamana_index(&self, label: &str, property: &str) -> Option<&VamanaIndex> {
        self.core.vamana_index(label, property)
    }
    fn label_node_count(&self, label_id: u32) -> u64 {
        self.core.label_node_count(label_id)
    }
    fn reltype_edge_count(&self, reltype_id: u32) -> u64 {
        self.core.reltype_edge_count(reltype_id)
    }
    fn first_label_count(&self, label_id: u32) -> u64 {
        self.core.first_label_count(label_id)
    }
    fn has_first_label_counts(&self) -> bool {
        self.core.has_first_label_counts()
    }
    fn first_labelled_node_count(&self) -> u64 {
        self.core.first_labelled_node_count()
    }
    fn src_label_reltype_count(&self, src_label_id: u32, reltype_id: u32) -> Option<u64> {
        self.core.src_label_reltype_count(src_label_id, reltype_id)
    }
    fn reltype_tgt_label_count(&self, reltype_id: u32, tgt_label_id: u32) -> Option<u64> {
        self.core.reltype_tgt_label_count(reltype_id, tgt_label_id)
    }
    fn schema_triple_count(
        &self,
        src_label_id: u32,
        reltype_id: u32,
        tgt_label_id: u32,
    ) -> Option<u64> {
        self.core
            .schema_triple_count(src_label_id, reltype_id, tgt_label_id)
    }
    fn has_reltype_postings(&self) -> bool {
        self.core.has_reltype_postings()
    }
    fn reltype_source_count(&self, reltype_id: u32) -> u64 {
        self.core.reltype_source_count(reltype_id)
    }
    fn reltype_target_count(&self, reltype_id: u32) -> u64 {
        self.core.reltype_target_count(reltype_id)
    }
    fn collect_nodes_with_label(&self, label_id: u32) -> Result<Vec<u64>> {
        self.core.collect_nodes_with_label(label_id)
    }
    fn collect_endpoint_nodes_for_reltypes(
        &self,
        reltype_ids: &[u32],
        side: RelEndpointSide,
    ) -> Result<Vec<u64>> {
        self.core
            .collect_endpoint_nodes_for_reltypes(reltype_ids, side)
    }
    fn delta(&self) -> &DeltaSnapshot {
        &self.delta
    }
    fn core_generation(&self) -> &Generation {
        self.core
    }
}
