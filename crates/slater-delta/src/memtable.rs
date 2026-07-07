// SPDX-License-Identifier: Apache-2.0
//! The in-RAM memtable and the read-side [`DeltaSnapshot`].
//!
//! This is the build-time overlay fold (`slater-build`'s `overlay::apply_patches`
//! / `merge_build::fold_node_props`, last-writer-wins per key plus a net-new
//! tombstone) promoted from a build artefact to a runtime one.
//!
//! The memtable is keyed by canonical business identity ([`crate::identity`]), not
//! dense id. It is written by a **single writer** (draining a channel on one
//! thread) and published to readers as an immutable [`DeltaSnapshot`] via an
//! `ArcSwap` in the server, so readers never block the writer and vice versa — it
//! is deliberately **not** a concurrent/lock-free structure (writes are not being
//! optimised).
//!
//! Phase 0 wires the always-empty snapshot and the zero-cost `is_empty` fast path;
//! property-patch and tombstone mutation land in Phase 1/2. Patches are held in a
//! `BTreeMap` so the serialised order into an L0 segment / consolidation input is
//! deterministic.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use graph_format::ids::Value;

use crate::identity::{EdgeIdentity, NodeIdentity, SymbolId};

/// Per-node delta: property patches to fold last-writer-wins over the core row,
/// plus a tombstone flag that suppresses the core row entirely on read.
#[derive(Debug, Clone, Default)]
pub struct NodeDelta {
    /// Property patches keyed by delta-local property-key id, last value wins.
    pub patches: BTreeMap<SymbolId, Value>,
    /// If set, the node is deleted: the core row is suppressed on read and
    /// dropped at consolidation.
    pub tombstoned: bool,
}

/// Per-edge delta, mirroring [`NodeDelta`] for relationship records.
#[derive(Debug, Clone, Default)]
pub struct EdgeDelta {
    pub patches: BTreeMap<SymbolId, Value>,
    pub tombstoned: bool,
}

impl NodeDelta {
    /// Whether this delta carries any information (a bare, empty, un-tombstoned
    /// delta is meaningless and should never be stored).
    pub fn is_meaningful(&self) -> bool {
        self.tombstoned || !self.patches.is_empty()
    }
}

/// The single-writer in-RAM memtable.
#[derive(Debug, Default)]
pub struct Memtable {
    nodes: HashMap<Vec<u8>, NodeDelta>,
    edges: HashMap<Vec<u8>, EdgeDelta>,
    /// Running resident-size estimate, checked against the memtable byte budget.
    bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    /// No node or edge deltas — the reader can skip the overlay entirely.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty()
    }

    /// Current resident-size estimate in bytes.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Look up a node delta by business identity.
    pub fn lookup_node(&self, id: &NodeIdentity) -> Option<&NodeDelta> {
        self.nodes.get(&id.canonical_key())
    }

    /// Look up an edge delta by business identity.
    pub fn lookup_edge(&self, id: &EdgeIdentity) -> Option<&EdgeDelta> {
        self.edges.get(&id.canonical_key())
    }

    /// Number of distinct node identities carrying a delta.
    pub fn node_delta_count(&self) -> usize {
        self.nodes.len()
    }
}

/// An immutable, read-side handle over the delta layers, captured at query start.
///
/// A query pins one `DeltaSnapshot` for its whole life (alongside the core `Arc`),
/// so a mid-query freeze/swap cannot split its view. Phase 0 only ever hands out
/// [`DeltaSnapshot::empty`]; later phases wrap the live memtable (and, from Phase
/// 4, the sealed L0 segments) behind the same handle.
#[derive(Debug, Clone)]
pub struct DeltaSnapshot {
    mem: Arc<Memtable>,
}

impl DeltaSnapshot {
    /// The canonical empty delta — the zero-cost read fast path.
    pub fn empty() -> Self {
        Self {
            mem: Arc::new(Memtable::new()),
        }
    }

    /// Wrap an immutable memtable snapshot.
    pub fn from_memtable(mem: Arc<Memtable>) -> Self {
        Self { mem }
    }

    /// Whether this snapshot overlays nothing — the reader's fast-path predicate.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.mem.is_empty()
    }

    /// Resolve a node delta, newest layer wins. (Phase 0: memtable only.)
    #[inline]
    pub fn lookup_node(&self, id: &NodeIdentity) -> Option<&NodeDelta> {
        self.mem.lookup_node(id)
    }

    /// Resolve an edge delta, newest layer wins. (Phase 0: memtable only.)
    #[inline]
    pub fn lookup_edge(&self, id: &EdgeIdentity) -> Option<&EdgeDelta> {
        self.mem.lookup_edge(id)
    }

    /// Count of delta-born-or-patched node identities, for scan-range planning.
    pub fn node_delta_count(&self) -> usize {
        self.mem.node_delta_count()
    }
}

impl Default for DeltaSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_is_empty_and_misses() {
        let snap = DeltaSnapshot::empty();
        assert!(snap.is_empty());
        let id = NodeIdentity::new(0, 0, Value::Int(1));
        assert!(snap.lookup_node(&id).is_none());
        assert_eq!(snap.node_delta_count(), 0);
    }

    #[test]
    fn node_delta_meaningfulness() {
        let mut d = NodeDelta::default();
        assert!(!d.is_meaningful());
        d.patches.insert(0, Value::Int(5));
        assert!(d.is_meaningful());
        let t = NodeDelta {
            patches: BTreeMap::new(),
            tombstoned: true,
        };
        assert!(t.is_meaningful());
    }
}
