// SPDX-License-Identifier: Apache-2.0
//! The in-RAM memtable and the read-side [`DeltaSnapshot`].
//!
//! This is the build-time overlay fold (`slater-build`'s `overlay::apply_patches`
//! / `merge_build::fold_node_props`, last-writer-wins per key plus a net-new
//! tombstone) promoted from a build artefact to a runtime one.
//!
//! The memtable is authoritatively keyed by canonical **business identity**
//! ([`crate::identity`]), never dense id — dense ids are per-generation and the
//! `cluster` phase permutes them. It is written by a **single writer** and
//! published to readers as an immutable [`DeltaSnapshot`] (via an `ArcSwap` in the
//! server), so readers never block the writer and vice versa — it is deliberately
//! **not** a concurrent/lock-free structure (writes are not being optimised).
//!
//! # Two id spaces, deliberately
//! - **Identity** `(label, key-property)` is interned to compact delta-local
//!   [`SymbolId`]s (the memtable owns the [`Interner`]) so `canonical_key` dedup is
//!   cheap; the identity is stored beside each delta so consolidation can recover
//!   the names.
//! - **Patch properties** are held by **name** (`BTreeMap<String, Value>`): the
//!   executor materialises a node's properties into a name-keyed record (core
//!   `key_id → name` happens at decode), so the read overlay folds patches in
//!   name-space with no interner round-trip. `BTreeMap` keeps the serialised order
//!   deterministic for L0 / consolidation.
//!
//! # Reads are O(1) via a resolved dense-id index
//! Existing-core nodes are also indexed by their **current-core dense id**
//! ([`Memtable::by_dense`]): the writer resolves each write's business key to a
//! dense id once (an ISAM probe, done on the `slater` side and passed in as
//! `resolved`), so a node read consults `resolved[dense_id]` directly rather than
//! reconstructing a node's business key from its dense id. The index is
//! core-generation-specific and is rebuilt-empty after a consolidation swap (which
//! retires the delta). Phase 1 assumes one business identity per node (multi-key
//! aliasing of the same physical node is out of scope until a later phase).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use graph_format::ids::Value;

use crate::identity::NodeIdentity;
use crate::interner::Interner;
use crate::wal::WalOp;

/// Per-node delta: property patches to fold last-writer-wins over the core row,
/// plus a tombstone flag that suppresses the core row entirely on read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NodeDelta {
    /// Property patches keyed by property **name**, last value wins.
    pub patches: BTreeMap<String, Value>,
    /// If set, the node is deleted: the core row is suppressed on read and dropped
    /// at consolidation. (Wired in Phase 2.)
    pub tombstoned: bool,
}

/// Per-edge delta, mirroring [`NodeDelta`] for relationship records. (Phase 3.)
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EdgeDelta {
    pub patches: BTreeMap<String, Value>,
    pub tombstoned: bool,
}

impl NodeDelta {
    /// Whether this delta carries any information (a bare, empty, un-tombstoned
    /// delta is meaningless and should never be stored).
    pub fn is_meaningful(&self) -> bool {
        self.tombstoned || !self.patches.is_empty()
    }
}

/// A stored node entry: the recoverable business identity plus its folded delta.
#[derive(Debug, Clone)]
struct NodeEntry {
    identity: NodeIdentity,
    delta: NodeDelta,
}

/// The single-writer in-RAM memtable.
///
/// `Clone` is how the writer publishes an immutable read snapshot: after a commit
/// it clones the authoritative table into a fresh `Arc<Memtable>` and swaps it in
/// (writes are deliberately un-optimised in Phase 1 — a per-commit clone is the
/// simplest correct publish).
#[derive(Debug, Default, Clone)]
pub struct Memtable {
    /// Delta-local interner for identity symbols (label, key-property names).
    interner: Interner,
    /// Authoritative store, keyed by canonical business identity.
    nodes: HashMap<Vec<u8>, NodeEntry>,
    edges: HashMap<Vec<u8>, EdgeDelta>,
    /// Read index: current-core dense id → the node's canonical identity key.
    by_dense: HashMap<u64, Vec<u8>>,
    /// Running resident-size estimate (approximate, monotonically conservative),
    /// checked against the memtable byte budget.
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

    /// Number of distinct node identities carrying a delta.
    pub fn node_delta_count(&self) -> usize {
        self.nodes.len()
    }

    /// Overwrite (last-writer-wins) `patches` onto the node identified by
    /// `(label, key, value)`. `resolved` is the node's current-core dense id (an
    /// ISAM probe on the `slater` side); `None` marks a delta-born node (Phase 2).
    ///
    /// Shared by live writes and WAL replay so the two paths cannot diverge.
    pub fn upsert_node(
        &mut self,
        label: &str,
        key: &str,
        value: Value,
        resolved: Option<u64>,
        patches: impl IntoIterator<Item = (String, Value)>,
    ) {
        let identity = NodeIdentity::new(
            self.interner.intern(label),
            self.interner.intern(key),
            value,
        );
        let ck = identity.canonical_key();
        let is_new = !self.nodes.contains_key(&ck);
        if is_new {
            self.bytes += ck.len() + std::mem::size_of::<NodeEntry>();
        }
        let entry = self.nodes.entry(ck.clone()).or_insert_with(|| NodeEntry {
            identity,
            delta: NodeDelta::default(),
        });
        // An upsert resurrects a tombstoned node (last-writer-wins at node level).
        entry.delta.tombstoned = false;
        for (name, val) in patches {
            self.bytes += name.len() + value_size(&val);
            entry.delta.patches.insert(name, val);
        }
        if let Some(dense) = resolved {
            self.by_dense.insert(dense, ck);
        }
    }

    /// Tombstone the node identified by `(label, key, value)`: reads suppress the
    /// core row and it is dropped at consolidation. `resolved` is the node's
    /// current-core dense id (an ISAM probe on the `slater` side); `None` for a
    /// business key absent from the core (a harmless no-op tombstone until Phase 2
    /// delta-born nodes). A tombstone drops any prior patches — a deleted node
    /// carries no properties — and wins last-writer-wins with [`Self::upsert_node`].
    ///
    /// Shared by live writes and WAL replay so the two paths cannot diverge.
    pub fn delete_node(&mut self, label: &str, key: &str, value: Value, resolved: Option<u64>) {
        let identity = NodeIdentity::new(
            self.interner.intern(label),
            self.interner.intern(key),
            value,
        );
        let ck = identity.canonical_key();
        if !self.nodes.contains_key(&ck) {
            self.bytes += ck.len() + std::mem::size_of::<NodeEntry>();
        }
        let entry = self.nodes.entry(ck.clone()).or_insert_with(|| NodeEntry {
            identity,
            delta: NodeDelta::default(),
        });
        entry.delta.tombstoned = true;
        entry.delta.patches.clear();
        if let Some(dense) = resolved {
            self.by_dense.insert(dense, ck);
        }
    }

    /// Apply a decoded WAL operation, given the business key's resolved
    /// current-core dense id (`None` for a delta-born node). The single path shared
    /// by live writes and WAL replay, so the two can never diverge.
    pub fn apply(&mut self, op: &WalOp, resolved: Option<u64>) {
        match op {
            WalOp::UpsertNode {
                label,
                key,
                value,
                patches,
            } => self.upsert_node(label, key, value.clone(), resolved, patches.iter().cloned()),
            WalOp::DeleteNode { label, key, value } => {
                self.delete_node(label, key, value.clone(), resolved)
            }
        }
    }

    /// Look up a node delta by its current-core dense id (the read path).
    pub fn node_patch(&self, dense_id: u64) -> Option<&NodeDelta> {
        let ck = self.by_dense.get(&dense_id)?;
        self.nodes.get(ck).map(|e| &e.delta)
    }

    /// Look up a node delta by business identity (uses this memtable's interner;
    /// intended for tests and same-memtable probes).
    pub fn lookup_node(&self, id: &NodeIdentity) -> Option<&NodeDelta> {
        self.nodes.get(&id.canonical_key()).map(|e| &e.delta)
    }

    /// Iterate stored nodes as `(label, key, value, delta)` with identity names
    /// recovered — the consolidation input (Phase 1d emits these as `MERGE` text).
    pub fn iter_nodes(&self) -> impl Iterator<Item = (&str, &str, &Value, &NodeDelta)> {
        self.nodes.values().map(move |e| {
            let label = self.interner.name(e.identity.label).unwrap_or("");
            let key = self.interner.name(e.identity.key).unwrap_or("");
            (label, key, &e.identity.value, &e.delta)
        })
    }
}

/// Rough resident size of a value, for the budget estimate.
fn value_size(v: &Value) -> usize {
    match v {
        Value::Null | Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 8,
        Value::Str(s) => s.len(),
        Value::List(items) => items.iter().map(value_size).sum::<usize>() + 8,
        Value::Vector(f) => f.len() * 4,
    }
}

/// An immutable, read-side handle over the delta layers, captured at query start.
///
/// A query pins one `DeltaSnapshot` for its whole life (alongside the core `Arc`),
/// so a mid-query freeze/swap cannot split its view. Phase 0 handed out only
/// [`DeltaSnapshot::empty`]; from Phase 1 the writer publishes a live memtable
/// snapshot (and, from Phase 4, the sealed L0 segments) behind the same handle.
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

    /// Resolve a node delta by current-core dense id (newest layer wins; Phase 0/1:
    /// memtable only).
    #[inline]
    pub fn node_patch(&self, dense_id: u64) -> Option<&NodeDelta> {
        self.mem.node_patch(dense_id)
    }

    /// Whether the core node `dense_id` is tombstoned by the delta (deleted): reads
    /// must suppress it (Phase 2). `false` for an absent or merely property-patched
    /// node.
    #[inline]
    pub fn is_tombstoned(&self, dense_id: u64) -> bool {
        self.node_patch(dense_id).is_some_and(|nd| nd.tombstoned)
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
        assert!(snap.node_patch(0).is_none());
        assert_eq!(snap.node_delta_count(), 0);
    }

    #[test]
    fn node_delta_meaningfulness() {
        let mut d = NodeDelta::default();
        assert!(!d.is_meaningful());
        d.patches.insert("x".into(), Value::Int(5));
        assert!(d.is_meaningful());
        let t = NodeDelta {
            patches: BTreeMap::new(),
            tombstoned: true,
        };
        assert!(t.is_meaningful());
    }

    #[test]
    fn upsert_folds_last_writer_wins_and_indexes_by_dense() {
        let mut m = Memtable::new();
        m.upsert_node(
            "Company",
            "ticker",
            Value::Str("A".into()),
            Some(42),
            [("price".to_string(), Value::Int(10))],
        );
        // Overwrite the same node's price; add a second property.
        m.upsert_node(
            "Company",
            "ticker",
            Value::Str("A".into()),
            Some(42),
            [
                ("price".to_string(), Value::Int(11)),
                ("sector".to_string(), Value::Str("Tech".into())),
            ],
        );
        assert_eq!(m.node_delta_count(), 1);
        let d = m.node_patch(42).expect("resolved by dense id");
        assert_eq!(d.patches.get("price"), Some(&Value::Int(11))); // last writer wins
        assert_eq!(d.patches.get("sector"), Some(&Value::Str("Tech".into())));
        assert!(!m.is_empty());
        assert!(m.bytes() > 0);
    }

    #[test]
    fn delete_tombstones_and_upsert_resurrects_last_writer_wins() {
        let mut m = Memtable::new();
        // Patch then delete: the node is tombstoned and its patches are dropped.
        m.upsert_node(
            "Company",
            "ticker",
            Value::Str("A".into()),
            Some(7),
            [("price".to_string(), Value::Int(10))],
        );
        m.delete_node("Company", "ticker", Value::Str("A".into()), Some(7));
        let d = m.node_patch(7).expect("tombstone is a stored delta");
        assert!(d.tombstoned, "delete tombstones the node");
        assert!(d.patches.is_empty(), "a deleted node carries no properties");
        assert_eq!(m.node_delta_count(), 1);

        // A later upsert on the same key resurrects it (last-writer-wins).
        m.upsert_node(
            "Company",
            "ticker",
            Value::Str("A".into()),
            Some(7),
            [("price".to_string(), Value::Int(20))],
        );
        let d = m.node_patch(7).expect("resurrected");
        assert!(!d.tombstoned, "an upsert clears the tombstone");
        assert_eq!(d.patches.get("price"), Some(&Value::Int(20)));
    }

    #[test]
    fn apply_delete_matches_direct_delete() {
        // The WAL-replay path (`apply`) and the direct call must not diverge.
        let mut viae = Memtable::new();
        viae.apply(
            &WalOp::DeleteNode {
                label: "L".into(),
                key: "k".into(),
                value: Value::Int(3),
            },
            Some(9),
        );
        let mut direct = Memtable::new();
        direct.delete_node("L", "k", Value::Int(3), Some(9));
        assert_eq!(viae.node_patch(9), direct.node_patch(9));
        assert!(viae.node_patch(9).unwrap().tombstoned);
    }

    #[test]
    fn distinct_identities_are_separate_nodes() {
        let mut m = Memtable::new();
        m.upsert_node("Company", "ticker", Value::Str("A".into()), Some(1), []);
        m.upsert_node("Company", "ticker", Value::Str("B".into()), Some(2), []);
        // Type-exact: Int(1) is a different node from Str("A").
        m.upsert_node("Company", "id", Value::Int(1), Some(3), []);
        assert_eq!(m.node_delta_count(), 3);
        assert!(m.node_patch(1).is_some());
        assert!(m.node_patch(2).is_some());
        assert!(m.node_patch(3).is_some());
        assert!(m.node_patch(99).is_none());
    }

    #[test]
    fn iter_nodes_recovers_identity_names() {
        let mut m = Memtable::new();
        m.upsert_node(
            "Company",
            "ticker",
            Value::Str("A".into()),
            Some(1),
            [("price".to_string(), Value::Int(10))],
        );
        let rows: Vec<_> = m.iter_nodes().collect();
        assert_eq!(rows.len(), 1);
        let (label, key, value, delta) = &rows[0];
        assert_eq!(*label, "Company");
        assert_eq!(*key, "ticker");
        assert_eq!(**value, Value::Str("A".into()));
        assert_eq!(delta.patches.get("price"), Some(&Value::Int(10)));
    }

    #[test]
    fn lookup_node_by_identity_matches_upsert() {
        let mut m = Memtable::new();
        m.upsert_node(
            "L",
            "k",
            Value::Int(7),
            Some(5),
            [("p".to_string(), Value::Bool(true))],
        );
        // Rebuild the same identity through the memtable's interner.
        let id = NodeIdentity::new(
            m.interner.intern("L"),
            m.interner.intern("k"),
            Value::Int(7),
        );
        assert!(m.lookup_node(&id).is_some());
    }
}
