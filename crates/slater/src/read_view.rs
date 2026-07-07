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
use graph_format::ids::{Generation as GenId, Value};
use graph_format::isam::IsamReader;
use graph_format::manifest::Manifest;
use graph_format::nodelabels::NodeLabelsReader;
use graph_format::topology::TopologyReader;
use graph_format::vectors::VectorStoreReader;
use slater_delta::DeltaSnapshot;
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
