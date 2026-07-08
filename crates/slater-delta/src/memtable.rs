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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use graph_format::ids::Value;
use graph_format::wire::{read_uvarint, read_value, write_uvarint, write_value};

use crate::identity::{EdgeIdentity, NodeIdentity, SymbolId};
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

/// Per-edge delta, mirroring [`NodeDelta`] for relationship records (Phase 3):
/// property patches (reserved — the write grammar creates topology only for now)
/// plus a tombstone flag that suppresses the edge on traversal.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EdgeDelta {
    pub patches: BTreeMap<String, Value>,
    pub tombstoned: bool,
}

/// A stored edge entry: the recoverable business identity, both endpoints resolved
/// to current-core-or-synthetic dense ids (the traversal read index keys on these),
/// and the folded delta.
#[derive(Debug, Clone)]
struct EdgeEntry {
    identity: EdgeIdentity,
    /// The source endpoint's dense id (core-resolved or a synthetic born-node id).
    src_dense: u64,
    /// The destination endpoint's dense id (core-resolved or a synthetic born-node id).
    dst_dense: u64,
    /// For a **delta-born** edge (a `MERGE`-created relationship absent from the core)
    /// its allocated synthetic dense edge id in `[edge_synthetic_base, …)`. `None` for
    /// a tombstone-only entry that merely suppresses an existing **core** edge (which
    /// keeps its own core edge id).
    synthetic_edge: Option<u64>,
    delta: EdgeDelta,
}

/// A read-side view of one delta edge incident to a queried node, handed to the
/// traversal overlay. `reltype` is the relationship-type **name** (the reader maps
/// it to a core reltype id); `other` is the neighbour endpoint's dense id (the dst
/// for an outgoing read, the src for an incoming one); `edge_id` is the synthetic
/// dense edge id for a born edge, `None` when the entry only tombstones a core edge.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaEdge {
    pub other: u64,
    pub reltype: String,
    pub edge_id: Option<u64>,
    pub tombstoned: bool,
}

/// The caller-resolved dense-id context an op needs to build the read index — the
/// `slater` side computes it (ISAM probes) and hands it to [`Memtable::apply`] so the
/// pure-`slater-delta` memtable never touches the core. A node op carries the single
/// business key's dense id; an edge op carries each endpoint's. `None` marks a
/// business key absent from the core (a delta-born node, or an endpoint to find/create
/// among the born nodes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpResolution {
    Node(Option<u64>),
    Edge { src: Option<u64>, dst: Option<u64> },
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
    /// For a **delta-born** node (a business key absent from the core, created by a
    /// `MERGE` write, Phase 2c) its allocated synthetic dense id in
    /// `[synthetic_base, synthetic_base + born_count)`. `None` for a core-resolved
    /// node (its current-core dense id lives in [`Memtable::by_dense`] only).
    synthetic: Option<u64>,
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
    /// Authoritative node store, keyed by canonical business identity.
    nodes: HashMap<Vec<u8>, NodeEntry>,
    /// Authoritative edge store, keyed by canonical edge business identity
    /// (`(src, reltype, dst)`).
    edges: HashMap<Vec<u8>, EdgeEntry>,
    /// Outgoing traversal read index: `src dense id → the edge identity keys` incident
    /// as a source (append order = allocation order = deterministic).
    out_adj: HashMap<u64, Vec<Vec<u8>>>,
    /// Incoming traversal read index: `dst dense id → the edge identity keys` incident
    /// as a destination.
    in_adj: HashMap<u64, Vec<Vec<u8>>>,
    /// Read index: dense id → the node's canonical identity key. Holds both
    /// current-core dense ids (property/tombstone deltas on existing nodes) and the
    /// synthetic dense ids of delta-born nodes, so a read resolves either uniformly.
    by_dense: HashMap<u64, Vec<u8>>,
    /// Base of the synthetic dense-id space = the core generation's `node_count` at
    /// open time. A delta-born node's id is `synthetic_base + born.len()` at the
    /// moment it is allocated, so the ids are `[synthetic_base, synthetic_base +
    /// born_count)` and never collide with a core dense id.
    synthetic_base: u64,
    /// Base of the synthetic **edge** dense-id space = the core generation's
    /// `edge_count` at open time, so a born edge's id never collides with a core edge
    /// id (which `rel_record` reads by id).
    edge_synthetic_base: u64,
    /// Delta-born canonical identity keys in **allocation order** (index = the id's
    /// offset from `synthetic_base`). Allocation order is WAL-replay order, so the
    /// synthetic id a business key receives is deterministic across a reopen.
    born: Vec<Vec<u8>>,
    /// Delta-born **edge** identity keys in allocation order (index = the id's offset
    /// from `edge_synthetic_base`); a tombstone-only entry is *not* pushed here (it
    /// allocates no synthetic edge id).
    born_edges: Vec<Vec<u8>>,
    /// Running resident-size estimate (approximate, monotonically conservative),
    /// checked against the memtable byte budget.
    bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    /// A memtable whose delta-born nodes allocate synthetic dense ids starting at
    /// `base` — the core generation's `node_count`, so the synthetic space begins
    /// exactly past the last core dense id. Born edges start at id 0 (use
    /// [`Self::with_bases`] to seed a non-zero core `edge_count`).
    pub fn with_synthetic_base(base: u64) -> Self {
        Self::with_bases(base, 0)
    }

    /// A memtable seeded with both synthetic bases — the core generation's
    /// `node_count` and `edge_count` — so delta-born nodes and edges take ids past the
    /// last core node / core edge respectively. The writer constructs the memtable
    /// this way (from the generation it resolves against) before replaying the WAL.
    pub fn with_bases(node_base: u64, edge_base: u64) -> Self {
        Self {
            synthetic_base: node_base,
            edge_synthetic_base: edge_base,
            ..Self::default()
        }
    }

    /// The base of the synthetic dense-id space (the core `node_count` at open).
    pub fn synthetic_base(&self) -> u64 {
        self.synthetic_base
    }

    /// The base of the synthetic edge dense-id space (the core `edge_count` at open).
    pub fn edge_synthetic_base(&self) -> u64 {
        self.edge_synthetic_base
    }

    /// The number of delta-born edges (each holds one synthetic dense edge id). The
    /// merged `edge_count` is `core.edge_count() + born_edge_count()`.
    pub fn born_edge_count(&self) -> u64 {
        self.born_edges.len() as u64
    }

    /// The number of delta-born nodes (each holds one synthetic dense id). The merged
    /// `node_count` is `core.node_count() + born_count()`.
    pub fn born_count(&self) -> u64 {
        self.born.len() as u64
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
    /// ISAM probe on the `slater` side); `None` marks a **delta-born** node, which is
    /// allocated a synthetic dense id in `[synthetic_base, …)` the first time it is
    /// seen (Phase 2c). This is the `MERGE` create-or-patch path — a resolved key
    /// patches the existing core node, an absent key creates a new one.
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
            synthetic: None,
        });
        // An upsert resurrects a tombstoned node (last-writer-wins at node level).
        entry.delta.tombstoned = false;
        for (name, val) in patches {
            self.bytes += name.len() + value_size(&val);
            entry.delta.patches.insert(name, val);
        }
        let already_synthetic = entry.synthetic;
        match resolved {
            Some(dense) => {
                self.by_dense.insert(dense, ck);
            }
            // Delta-born: allocate one synthetic dense id per identity, once. A later
            // upsert of the same key reuses it (and never re-pushes into `born`), so
            // the synthetic id is stable for the delta's whole life.
            None if already_synthetic.is_none() => {
                let dense = self.synthetic_base + self.born.len() as u64;
                self.born.push(ck.clone());
                self.by_dense.insert(dense, ck.clone());
                self.nodes
                    .get_mut(&ck)
                    .expect("entry just inserted")
                    .synthetic = Some(dense);
            }
            None => {}
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
            synthetic: None,
        });
        entry.delta.tombstoned = true;
        entry.delta.patches.clear();
        if let Some(dense) = resolved {
            self.by_dense.insert(dense, ck);
        }
    }

    /// Apply a decoded WAL operation, given the caller-resolved dense-id context
    /// ([`OpResolution`]). The single path shared by live writes and WAL replay, so
    /// the two can never diverge. The op kind and the resolution kind must agree
    /// (both node, or both edge) — a mismatch is a caller bug.
    pub fn apply(&mut self, op: &WalOp, res: OpResolution) {
        match (op, res) {
            (
                WalOp::UpsertNode {
                    label,
                    key,
                    value,
                    patches,
                },
                OpResolution::Node(resolved),
            ) => self.upsert_node(label, key, value.clone(), resolved, patches.iter().cloned()),
            (WalOp::DeleteNode { label, key, value }, OpResolution::Node(resolved)) => {
                self.delete_node(label, key, value.clone(), resolved)
            }
            (
                WalOp::UpsertEdge {
                    src_label,
                    src_key,
                    src_value,
                    reltype,
                    dst_label,
                    dst_key,
                    dst_value,
                    patches,
                },
                OpResolution::Edge { src, dst },
            ) => self.upsert_edge(
                src_label,
                src_key,
                src_value.clone(),
                reltype,
                dst_label,
                dst_key,
                dst_value.clone(),
                src,
                dst,
                patches.iter().cloned(),
            ),
            (
                WalOp::DeleteEdge {
                    src_label,
                    src_key,
                    src_value,
                    reltype,
                    dst_label,
                    dst_key,
                    dst_value,
                },
                OpResolution::Edge { src, dst },
            ) => self.delete_edge(
                src_label,
                src_key,
                src_value.clone(),
                reltype,
                dst_label,
                dst_key,
                dst_value.clone(),
                src,
                dst,
            ),
            (op, res) => {
                unreachable!("WalOp/OpResolution kind mismatch: {op:?} vs {res:?}")
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

    /// Recover the `(label, key-property, key-value)` business identity of the node
    /// at `dense_id` (either a core-resolved or a synthetic id). The read overlay
    /// uses this to materialise a delta-born node's label + business-key property,
    /// neither of which is stored as a patch. `None` if `dense_id` carries no delta.
    pub fn node_identity_by_dense(&self, dense_id: u64) -> Option<(&str, &str, &Value)> {
        let ck = self.by_dense.get(&dense_id)?;
        let e = self.nodes.get(ck)?;
        Some((
            self.interner.name(e.identity.label).unwrap_or(""),
            self.interner.name(e.identity.key).unwrap_or(""),
            &e.identity.value,
        ))
    }

    /// The synthetic dense id of the **delta-born** node with this business identity,
    /// if this memtable holds it as a born node (not a core-resolved patch/tombstone).
    /// `None` when the identity is absent here or is a core-resolved entry.
    ///
    /// The write path uses this to resolve a re-`MERGE` of a node already flushed to an
    /// L0 level to its **existing** synthetic id (Phase 4c-B) rather than allocate a
    /// duplicate. Non-mutating: it resolves the label/key names through this memtable's
    /// interner without interning (a name absent from the interner can't identify a
    /// stored node, so it short-circuits to `None`).
    pub fn born_synthetic_for_identity(
        &self,
        label: &str,
        key: &str,
        value: &Value,
    ) -> Option<u64> {
        let l = self.interner.get(label)?;
        let k = self.interner.get(key)?;
        let ck = NodeIdentity::new(l, k, value.clone()).canonical_key();
        self.nodes.get(&ck).and_then(|e| e.synthetic)
    }

    /// The synthetic dense ids of every delta-born node carrying `label` (ascending
    /// by allocation order). A label scan appends these to the core hits; tombstoned
    /// entries are included and dropped by the caller's tombstone suppression, so the
    /// contract matches the core scan (which likewise leaves suppression to the read).
    pub fn born_ids_with_label(&self, label: &str) -> Vec<u64> {
        let mut out = Vec::new();
        for ck in &self.born {
            if let Some(e) = self.nodes.get(ck) {
                if self.interner.name(e.identity.label) == Some(label) {
                    if let Some(dense) = e.synthetic {
                        out.push(dense);
                    }
                }
            }
        }
        out
    }

    /// The value a delta-born node's entry `e` presents for the indexed property
    /// `prop`, matching the read overlay's precedence (a patch wins over the
    /// business key — see `node_prop_par`): the patch value if present, else the
    /// business-key value when `prop` *is* the key property, else `None` (the node
    /// carries no such property and so is absent from the index).
    fn born_index_value<'a>(&'a self, e: &'a NodeEntry, prop: &str) -> Option<&'a Value> {
        if let Some(v) = e.delta.patches.get(prop) {
            return Some(v);
        }
        if self.interner.name(e.identity.key) == Some(prop) {
            return Some(&e.identity.value);
        }
        None
    }

    /// The synthetic dense ids of delta-born nodes carrying `label` whose indexed
    /// property `prop` satisfies `pred` (ascending by allocation order). The
    /// range-index overlay (Phase 2d) appends these to the core ISAM hits — a
    /// created node is otherwise invisible to an indexed key seek. Tombstoned
    /// entries are included and dropped by the caller's tombstone suppression, so
    /// the contract matches [`Self::born_ids_with_label`].
    fn born_ids_in_index(
        &self,
        label: &str,
        prop: &str,
        pred: impl Fn(&Value) -> bool,
    ) -> Vec<u64> {
        let mut out = Vec::new();
        for ck in &self.born {
            let Some(e) = self.nodes.get(ck) else {
                continue;
            };
            if self.interner.name(e.identity.label) != Some(label) {
                continue;
            }
            let Some(dense) = e.synthetic else { continue };
            if let Some(v) = self.born_index_value(e, prop) {
                if pred(v) {
                    out.push(dense);
                }
            }
        }
        out
    }

    /// Delta-born nodes carrying `label` whose indexed property `prop` equals `key`
    /// (by [`Value::cmp_key`], the total order the ISAM uses) — the `RangeEq`
    /// overlay (Phase 2d).
    pub fn born_ids_in_index_eq(&self, label: &str, prop: &str, key: &Value) -> Vec<u64> {
        use std::cmp::Ordering;
        self.born_ids_in_index(label, prop, |v| v.cmp_key(key) == Ordering::Equal)
    }

    /// Delta-born nodes carrying `label` whose indexed property `prop` falls in the
    /// `[lo, hi]` range with per-bound inclusivity (a `None` bound is unbounded on
    /// that side) — the `RangeRange` overlay (Phase 2d). Comparison is
    /// [`Value::cmp_key`], matching [`graph_format::isam`]'s `lookup_range`.
    pub fn born_ids_in_index_range(
        &self,
        label: &str,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Vec<u64> {
        use std::cmp::Ordering;
        self.born_ids_in_index(label, prop, |v| {
            let above_lo = match lo {
                None => true,
                Some(lo) => match v.cmp_key(lo) {
                    Ordering::Greater => true,
                    Ordering::Equal => lo_inclusive,
                    Ordering::Less => false,
                },
            };
            let below_hi = match hi {
                None => true,
                Some(hi) => match v.cmp_key(hi) {
                    Ordering::Less => true,
                    Ordering::Equal => hi_inclusive,
                    Ordering::Greater => false,
                },
            };
            above_lo && below_hi
        })
    }

    /// Resolve an edge endpoint to a dense id, **creating a delta-born node** if the
    /// business key is absent from the core and not already born (the `MERGE`-edge
    /// endpoint-create path). A `Some(resolved)` from the caller's ISAM probe wins; a
    /// `None` finds the existing born node or allocates a fresh one (no patches, no
    /// tombstone touch).
    fn endpoint_dense_or_create(
        &mut self,
        label: &str,
        key: &str,
        value: Value,
        resolved: Option<u64>,
    ) -> u64 {
        if let Some(dense) = resolved {
            return dense;
        }
        let identity = NodeIdentity::new(
            self.interner.intern(label),
            self.interner.intern(key),
            value,
        );
        let ck = identity.canonical_key();
        if let Some(dense) = self.nodes.get(&ck).and_then(|e| e.synthetic) {
            return dense;
        }
        let dense = self.synthetic_base + self.born.len() as u64;
        self.bytes += ck.len() + std::mem::size_of::<NodeEntry>();
        self.born.push(ck.clone());
        self.by_dense.insert(dense, ck.clone());
        self.nodes.insert(
            ck,
            NodeEntry {
                identity,
                delta: NodeDelta::default(),
                synthetic: Some(dense),
            },
        );
        dense
    }

    /// Resolve an edge endpoint to a dense id **without creating** one (the delete
    /// path): the caller's ISAM `resolved` wins, else an existing born node's
    /// synthetic id, else `None` — an endpoint that exists nowhere means there is no
    /// edge to tombstone.
    fn born_endpoint_dense(
        &mut self,
        label: &str,
        key: &str,
        value: Value,
        resolved: Option<u64>,
    ) -> Option<u64> {
        if resolved.is_some() {
            return resolved;
        }
        let identity = NodeIdentity::new(
            self.interner.intern(label),
            self.interner.intern(key),
            value,
        );
        let ck = identity.canonical_key();
        self.nodes.get(&ck).and_then(|e| e.synthetic)
    }

    /// Create (or resurrect + patch) the relationship `(src) -[reltype]-> (dst)`,
    /// resolving both endpoints via the caller's ISAM probes (`None` = create/find a
    /// delta-born endpoint). A brand-new edge identity is allocated a synthetic dense
    /// edge id and indexed into the outgoing/incoming adjacency; re-`MERGE`-ing the
    /// same identity reuses it (idempotent, matching [`Self::upsert_node`]). Shared by
    /// live writes and WAL replay.
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_edge(
        &mut self,
        src_label: &str,
        src_key: &str,
        src_value: Value,
        reltype: &str,
        dst_label: &str,
        dst_key: &str,
        dst_value: Value,
        src_resolved: Option<u64>,
        dst_resolved: Option<u64>,
        patches: impl IntoIterator<Item = (String, Value)>,
    ) {
        let src_dense =
            self.endpoint_dense_or_create(src_label, src_key, src_value.clone(), src_resolved);
        let dst_dense =
            self.endpoint_dense_or_create(dst_label, dst_key, dst_value.clone(), dst_resolved);
        let identity = EdgeIdentity::new(
            NodeIdentity::new(
                self.interner.intern(src_label),
                self.interner.intern(src_key),
                src_value,
            ),
            self.interner.intern(reltype),
            NodeIdentity::new(
                self.interner.intern(dst_label),
                self.interner.intern(dst_key),
                dst_value,
            ),
        );
        let eck = identity.canonical_key();
        if !self.edges.contains_key(&eck) {
            self.bytes += eck.len() + std::mem::size_of::<EdgeEntry>();
            let synthetic = self.edge_synthetic_base + self.born_edges.len() as u64;
            self.born_edges.push(eck.clone());
            self.out_adj.entry(src_dense).or_default().push(eck.clone());
            self.in_adj.entry(dst_dense).or_default().push(eck.clone());
            self.edges.insert(
                eck.clone(),
                EdgeEntry {
                    identity,
                    src_dense,
                    dst_dense,
                    synthetic_edge: Some(synthetic),
                    delta: EdgeDelta::default(),
                },
            );
        }
        let entry = self.edges.get_mut(&eck).expect("edge entry just ensured");
        entry.delta.tombstoned = false; // last-writer-wins resurrect
        let mut added = 0usize;
        for (name, val) in patches {
            added += name.len() + value_size(&val);
            entry.delta.patches.insert(name, val);
        }
        self.bytes += added;
    }

    /// Tombstone the relationship `(src) -[reltype]-> (dst)`: reads suppress it and
    /// consolidation drops it. A tombstone of a **core** edge stores a new entry
    /// (`synthetic_edge = None`) keyed by the resolved endpoint dense ids so the read
    /// overlay can match it against a core adjacency record; a tombstone of a
    /// delta-born edge flips its existing entry (and clears its patches). If either
    /// endpoint exists nowhere (no core node, no born node) there is no edge to
    /// delete — a no-op. Shared by live writes and WAL replay.
    #[allow(clippy::too_many_arguments)]
    pub fn delete_edge(
        &mut self,
        src_label: &str,
        src_key: &str,
        src_value: Value,
        reltype: &str,
        dst_label: &str,
        dst_key: &str,
        dst_value: Value,
        src_resolved: Option<u64>,
        dst_resolved: Option<u64>,
    ) {
        let Some(src_dense) =
            self.born_endpoint_dense(src_label, src_key, src_value.clone(), src_resolved)
        else {
            return;
        };
        let Some(dst_dense) =
            self.born_endpoint_dense(dst_label, dst_key, dst_value.clone(), dst_resolved)
        else {
            return;
        };
        let identity = EdgeIdentity::new(
            NodeIdentity::new(
                self.interner.intern(src_label),
                self.interner.intern(src_key),
                src_value,
            ),
            self.interner.intern(reltype),
            NodeIdentity::new(
                self.interner.intern(dst_label),
                self.interner.intern(dst_key),
                dst_value,
            ),
        );
        let eck = identity.canonical_key();
        if !self.edges.contains_key(&eck) {
            self.bytes += eck.len() + std::mem::size_of::<EdgeEntry>();
            self.out_adj.entry(src_dense).or_default().push(eck.clone());
            self.in_adj.entry(dst_dense).or_default().push(eck.clone());
            self.edges.insert(
                eck,
                EdgeEntry {
                    identity,
                    src_dense,
                    dst_dense,
                    synthetic_edge: None, // tombstone-only of a core edge
                    delta: EdgeDelta {
                        patches: BTreeMap::new(),
                        tombstoned: true,
                    },
                },
            );
        } else {
            let entry = self.edges.get_mut(&eck).expect("edge present");
            entry.delta.tombstoned = true;
            entry.delta.patches.clear();
        }
    }

    /// The delta edges incident to `node` as a **source** (outgoing). The reader folds
    /// these onto the core outgoing adjacency: a born edge is appended, a tombstoned
    /// one suppresses the matching core edge (see [`DeltaEdge`]).
    pub fn out_edges(&self, node: u64) -> Vec<DeltaEdge> {
        self.edges_for(&self.out_adj, node, true)
    }

    /// The delta edges incident to `node` as a **destination** (incoming).
    pub fn in_edges(&self, node: u64) -> Vec<DeltaEdge> {
        self.edges_for(&self.in_adj, node, false)
    }

    fn edges_for(
        &self,
        adj: &HashMap<u64, Vec<Vec<u8>>>,
        node: u64,
        outgoing: bool,
    ) -> Vec<DeltaEdge> {
        let Some(cks) = adj.get(&node) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(cks.len());
        for eck in cks {
            let Some(e) = self.edges.get(eck) else {
                continue;
            };
            let other = if outgoing { e.dst_dense } else { e.src_dense };
            out.push(DeltaEdge {
                other,
                reltype: self
                    .interner
                    .name(e.identity.reltype)
                    .unwrap_or("")
                    .to_string(),
                edge_id: e.synthetic_edge,
                tombstoned: e.delta.tombstoned,
            });
        }
        out
    }

    /// Iterate stored edges as `(src_label, src_key, src_value, reltype, dst_label,
    /// dst_key, dst_value, delta)` with identity names recovered — the consolidation
    /// input (Phase 3d emits born edges as `MERGE` text and drops tombstoned ones).
    #[allow(clippy::type_complexity)]
    pub fn iter_edges(
        &self,
    ) -> impl Iterator<Item = (&str, &str, &Value, &str, &str, &str, &Value, &EdgeDelta)> {
        self.edges.values().map(move |e| {
            let name = |s| self.interner.name(s).unwrap_or("");
            (
                name(e.identity.src.label),
                name(e.identity.src.key),
                &e.identity.src.value,
                name(e.identity.reltype),
                name(e.identity.dst.label),
                name(e.identity.dst.key),
                &e.identity.dst.value,
                &e.delta,
            )
        })
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

    /// Serialise the whole memtable to a self-describing byte image — the body of an
    /// **L0 delta segment** (Phase 4b). The image is complete: it carries the interner
    /// name table (so identities' delta-local [`SymbolId`]s round-trip), every folded
    /// node/edge entry, and the derived read indexes (`by_dense`, `out_adj`, `in_adj`)
    /// and born-order vectors verbatim, so [`Memtable::deserialise`] reconstructs a
    /// byte-for-byte equivalent memtable that answers every read identically. The
    /// serialised order is deterministic: `BTreeMap` patches keep property order, and
    /// entries are emitted sorted by canonical key so two equal memtables serialise to
    /// identical bytes (a determinism-golden property).
    ///
    /// Format is versioned but **not** back-compatible (zero legacy installs — an L0
    /// segment lives only between a flush and the next consolidation), so it may change
    /// freely; a version mismatch is a hard error on load.
    pub fn serialise(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, L0_FORMAT_VERSION);

        // Interner name table (id == index).
        let names = self.interner.names();
        write_uvarint(&mut buf, names.len() as u64);
        for n in names {
            w_str(&mut buf, n);
        }

        write_uvarint(&mut buf, self.synthetic_base);
        write_uvarint(&mut buf, self.edge_synthetic_base);
        write_uvarint(&mut buf, self.bytes as u64);

        // Nodes, sorted by canonical key for deterministic output.
        let mut nodes: Vec<(&Vec<u8>, &NodeEntry)> = self.nodes.iter().collect();
        nodes.sort_by(|a, b| a.0.cmp(b.0));
        write_uvarint(&mut buf, nodes.len() as u64);
        for (ck, e) in nodes {
            w_bytes(&mut buf, ck);
            w_node_identity(&mut buf, &e.identity);
            w_delta(&mut buf, &e.delta.patches, e.delta.tombstoned);
            w_opt_u64(&mut buf, e.synthetic);
        }

        // Edges, sorted by canonical key.
        let mut edges: Vec<(&Vec<u8>, &EdgeEntry)> = self.edges.iter().collect();
        edges.sort_by(|a, b| a.0.cmp(b.0));
        write_uvarint(&mut buf, edges.len() as u64);
        for (ck, e) in edges {
            w_bytes(&mut buf, ck);
            w_node_identity(&mut buf, &e.identity.src);
            write_uvarint(&mut buf, e.identity.reltype as u64);
            w_node_identity(&mut buf, &e.identity.dst);
            write_uvarint(&mut buf, e.src_dense);
            write_uvarint(&mut buf, e.dst_dense);
            w_opt_u64(&mut buf, e.synthetic_edge);
            w_delta(&mut buf, &e.delta.patches, e.delta.tombstoned);
        }

        w_dense_index(&mut buf, &self.by_dense);
        w_adj(&mut buf, &self.out_adj);
        w_adj(&mut buf, &self.in_adj);
        w_key_vec(&mut buf, &self.born);
        w_key_vec(&mut buf, &self.born_edges);
        buf
    }

    /// Reconstruct a memtable from a [`Memtable::serialise`] image. Every field is
    /// restored verbatim, so the result answers all reads identically to the original.
    pub fn deserialise(mut bytes: &[u8]) -> anyhow::Result<Self> {
        let r = &mut bytes;
        let version = read_uvarint(r)?;
        if version != L0_FORMAT_VERSION {
            anyhow::bail!(
                "unsupported L0 segment version {version} (expected {L0_FORMAT_VERSION})"
            );
        }

        let n_names = read_uvarint(r)? as usize;
        let mut names = Vec::with_capacity(n_names);
        for _ in 0..n_names {
            names.push(r_str(r)?);
        }
        let interner = Interner::from_names(names);

        let synthetic_base = read_uvarint(r)?;
        let edge_synthetic_base = read_uvarint(r)?;
        let bytes_est = read_uvarint(r)? as usize;

        let n_nodes = read_uvarint(r)? as usize;
        let mut nodes = HashMap::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            let ck = r_bytes(r)?;
            let identity = r_node_identity(r)?;
            let (patches, tombstoned) = r_delta(r)?;
            let synthetic = r_opt_u64(r)?;
            nodes.insert(
                ck,
                NodeEntry {
                    identity,
                    delta: NodeDelta {
                        patches,
                        tombstoned,
                    },
                    synthetic,
                },
            );
        }

        let n_edges = read_uvarint(r)? as usize;
        let mut edges = HashMap::with_capacity(n_edges);
        for _ in 0..n_edges {
            let ck = r_bytes(r)?;
            let src = r_node_identity(r)?;
            let reltype = read_uvarint(r)? as SymbolId;
            let dst = r_node_identity(r)?;
            let src_dense = read_uvarint(r)?;
            let dst_dense = read_uvarint(r)?;
            let synthetic_edge = r_opt_u64(r)?;
            let (patches, tombstoned) = r_delta(r)?;
            edges.insert(
                ck,
                EdgeEntry {
                    identity: EdgeIdentity { src, reltype, dst },
                    src_dense,
                    dst_dense,
                    synthetic_edge,
                    delta: EdgeDelta {
                        patches,
                        tombstoned,
                    },
                },
            );
        }

        let by_dense = r_dense_index(r)?;
        let out_adj = r_adj(r)?;
        let in_adj = r_adj(r)?;
        let born = r_key_vec(r)?;
        let born_edges = r_key_vec(r)?;

        if !r.is_empty() {
            anyhow::bail!("L0 segment has {} trailing bytes", r.len());
        }
        Ok(Self {
            interner,
            nodes,
            edges,
            out_adj,
            in_adj,
            by_dense,
            synthetic_base,
            edge_synthetic_base,
            born,
            born_edges,
            bytes: bytes_est,
        })
    }
}

/// L0 segment body format version (see [`Memtable::serialise`]).
const L0_FORMAT_VERSION: u64 = 1;

fn w_str(buf: &mut Vec<u8>, s: &str) {
    write_uvarint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

fn r_str(r: &mut &[u8]) -> anyhow::Result<String> {
    let len = read_uvarint(r)? as usize;
    if r.len() < len {
        anyhow::bail!("L0 string truncated");
    }
    let (s, rest) = r.split_at(len);
    *r = rest;
    String::from_utf8(s.to_vec()).map_err(|_| anyhow::anyhow!("L0 string not utf-8"))
}

fn w_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    write_uvarint(buf, b.len() as u64);
    buf.extend_from_slice(b);
}

fn r_bytes(r: &mut &[u8]) -> anyhow::Result<Vec<u8>> {
    let len = read_uvarint(r)? as usize;
    if r.len() < len {
        anyhow::bail!("L0 byte string truncated");
    }
    let (b, rest) = r.split_at(len);
    *r = rest;
    Ok(b.to_vec())
}

fn w_opt_u64(buf: &mut Vec<u8>, v: Option<u64>) {
    match v {
        Some(x) => {
            buf.push(1);
            write_uvarint(buf, x);
        }
        None => buf.push(0),
    }
}

fn r_opt_u64(r: &mut &[u8]) -> anyhow::Result<Option<u64>> {
    match read_u8(r)? {
        0 => Ok(None),
        1 => Ok(Some(read_uvarint(r)?)),
        t => anyhow::bail!("L0 bad Option tag {t}"),
    }
}

fn read_u8(r: &mut &[u8]) -> anyhow::Result<u8> {
    let (&b, rest) = r
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("L0 truncated"))?;
    *r = rest;
    Ok(b)
}

fn w_node_identity(buf: &mut Vec<u8>, id: &NodeIdentity) {
    write_uvarint(buf, id.label as u64);
    write_uvarint(buf, id.key as u64);
    write_value(buf, &id.value);
}

fn r_node_identity(r: &mut &[u8]) -> anyhow::Result<NodeIdentity> {
    let label = read_uvarint(r)? as SymbolId;
    let key = read_uvarint(r)? as SymbolId;
    let value = read_value(r)?;
    Ok(NodeIdentity { label, key, value })
}

fn w_delta(buf: &mut Vec<u8>, patches: &BTreeMap<String, Value>, tombstoned: bool) {
    buf.push(tombstoned as u8);
    write_uvarint(buf, patches.len() as u64);
    for (k, v) in patches {
        w_str(buf, k);
        write_value(buf, v);
    }
}

fn r_delta(r: &mut &[u8]) -> anyhow::Result<(BTreeMap<String, Value>, bool)> {
    let tombstoned = read_u8(r)? != 0;
    let n = read_uvarint(r)? as usize;
    let mut patches = BTreeMap::new();
    for _ in 0..n {
        let k = r_str(r)?;
        let v = read_value(r)?;
        patches.insert(k, v);
    }
    Ok((patches, tombstoned))
}

fn w_dense_index(buf: &mut Vec<u8>, idx: &HashMap<u64, Vec<u8>>) {
    let mut pairs: Vec<(&u64, &Vec<u8>)> = idx.iter().collect();
    pairs.sort_by_key(|(id, _)| **id);
    write_uvarint(buf, pairs.len() as u64);
    for (id, ck) in pairs {
        write_uvarint(buf, *id);
        w_bytes(buf, ck);
    }
}

fn r_dense_index(r: &mut &[u8]) -> anyhow::Result<HashMap<u64, Vec<u8>>> {
    let n = read_uvarint(r)? as usize;
    let mut idx = HashMap::with_capacity(n);
    for _ in 0..n {
        let id = read_uvarint(r)?;
        let ck = r_bytes(r)?;
        idx.insert(id, ck);
    }
    Ok(idx)
}

fn w_adj(buf: &mut Vec<u8>, adj: &HashMap<u64, Vec<Vec<u8>>>) {
    let mut pairs: Vec<(&u64, &Vec<Vec<u8>>)> = adj.iter().collect();
    pairs.sort_by_key(|(id, _)| **id);
    write_uvarint(buf, pairs.len() as u64);
    for (id, cks) in pairs {
        write_uvarint(buf, *id);
        write_uvarint(buf, cks.len() as u64);
        for ck in cks {
            w_bytes(buf, ck);
        }
    }
}

fn r_adj(r: &mut &[u8]) -> anyhow::Result<HashMap<u64, Vec<Vec<u8>>>> {
    let n = read_uvarint(r)? as usize;
    let mut adj = HashMap::with_capacity(n);
    for _ in 0..n {
        let id = read_uvarint(r)?;
        let m = read_uvarint(r)? as usize;
        let mut cks = Vec::with_capacity(m);
        for _ in 0..m {
            cks.push(r_bytes(r)?);
        }
        adj.insert(id, cks);
    }
    Ok(adj)
}

fn w_key_vec(buf: &mut Vec<u8>, v: &[Vec<u8>]) {
    write_uvarint(buf, v.len() as u64);
    for ck in v {
        w_bytes(buf, ck);
    }
}

fn r_key_vec(r: &mut &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
    let n = read_uvarint(r)? as usize;
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        v.push(r_bytes(r)?);
    }
    Ok(v)
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
/// snapshot behind this handle, and from Phase 4c the sealed **L0 segments** stack
/// beneath it: reads fold `memtable ⊕ L0*` (newest wins) over the core.
///
/// # Multi-level fold (Phase 4c)
/// The active [`mem`](Self::mem) is the newest level; [`l0`](Self::l0) holds the
/// sealed segments **newest-first**. Precedence is **last-writer-wins across levels**:
/// - **node patches** on a *core* dense id may split across levels (a node patched
///   before a flush, patched again after) — they merge **per-property, newer wins**;
///   a tombstone clears+deletes, a newer upsert resurrects (LSM tombstone semantics).
///   [`node_patch`](Self::node_patch) therefore returns an **owned** merged delta.
/// - **synthetic (delta-born) ids** are disjoint across levels (each level keeps its
///   own stacked `synthetic_base`), so born-id sets simply **union**; counts **sum**;
///   `synthetic_base`/`edge_synthetic_base` are the **min** (= the core count).
/// - **edges** union then dedup by `(reltype, neighbour)` newest-wins, so a born edge
///   flushed to L0 and later deleted surfaces once, tombstoned.
///
/// [`is_empty`](Self::is_empty) is `mem` empty **and** every L0 empty, so the
/// zero-cost read fast path (the overwhelming common no-write case) is preserved — an
/// empty `l0` vector makes the extra check a no-op.
#[derive(Debug, Clone)]
pub struct DeltaSnapshot {
    /// The live (active) memtable — the newest level.
    mem: Arc<Memtable>,
    /// Sealed, immutable L0 segments (as reloaded memtables), **newest first**. Empty
    /// on the common no-flush path.
    l0: Vec<Arc<Memtable>>,
}

impl DeltaSnapshot {
    /// The canonical empty delta — the zero-cost read fast path.
    pub fn empty() -> Self {
        Self {
            mem: Arc::new(Memtable::new()),
            l0: Vec::new(),
        }
    }

    /// Wrap a single live memtable snapshot with no sealed L0 levels (Phases 1–3).
    pub fn from_memtable(mem: Arc<Memtable>) -> Self {
        Self {
            mem,
            l0: Vec::new(),
        }
    }

    /// Stack sealed L0 segments (as reloaded memtables, **newest first**) beneath the
    /// active memtable (Phase 4c). The writer publishes the flushed segments here so a
    /// read folds `mem ⊕ L0*`.
    pub fn with_levels(mem: Arc<Memtable>, l0: Vec<Arc<Memtable>>) -> Self {
        Self { mem, l0 }
    }

    /// The active (newest, writable) memtable level — the writer's `snapshot()` returns
    /// this for its single-memtable accessors and tests.
    #[inline]
    pub fn active_memtable(&self) -> &Arc<Memtable> {
        &self.mem
    }

    /// The sealed L0 segments beneath the active memtable, **newest first** (empty on
    /// the common no-flush path). The writer folds born-id resolution over these.
    #[inline]
    pub fn l0_levels(&self) -> &[Arc<Memtable>] {
        &self.l0
    }

    /// The delta levels in **newest-first** precedence order (active memtable, then the
    /// L0 segments newest→oldest) — first hit wins for tombstone/identity/edge-dedup
    /// precedence.
    fn levels_newest_first(&self) -> impl Iterator<Item = &Memtable> {
        std::iter::once(self.mem.as_ref()).chain(self.l0.iter().map(Arc::as_ref))
    }

    /// The delta levels in **oldest-first** order (oldest L0 segment … active memtable)
    /// — the fold order for [`node_patch`](Self::node_patch) (newer property writes
    /// overlay older ones) and the emission order for born-id unions (their stacked
    /// synthetic-id ranges then come out ascending, matching the core scan order).
    fn levels_oldest_first(&self) -> impl Iterator<Item = &Memtable> {
        self.l0
            .iter()
            .rev()
            .map(Arc::as_ref)
            .chain(std::iter::once(self.mem.as_ref()))
    }

    /// Whether this snapshot overlays nothing — the reader's fast-path predicate. With
    /// no L0 levels (the common case) this is a single memtable check.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.mem.is_empty() && self.l0.iter().all(|m| m.is_empty())
    }

    /// Resolve a node delta by current-core dense id, folded newest-wins across all
    /// levels (Phase 4c). A *core* dense id's patches may be split across levels — they
    /// merge per-property (newer wins), a tombstone clears+deletes and a newer upsert
    /// resurrects; a *synthetic* id lives in one level, so the fold returns its entry
    /// unchanged. Returns an **owned** delta (the merge cannot borrow a single level).
    pub fn node_patch(&self, dense_id: u64) -> Option<NodeDelta> {
        // Fast path: no L0 — clone the single memtable's entry (one allocation, only
        // for a genuinely patched node) to honour the owned contract.
        if self.l0.is_empty() {
            return self.mem.node_patch(dense_id).cloned();
        }
        let mut acc: Option<NodeDelta> = None;
        for level in self.levels_oldest_first() {
            let Some(nd) = level.node_patch(dense_id) else {
                continue;
            };
            match &mut acc {
                None => acc = Some(nd.clone()),
                Some(a) => {
                    if nd.tombstoned {
                        // A newer delete removes the node and everything patched below
                        // it (a tombstone carries no properties of its own).
                        a.patches.clear();
                        a.tombstoned = true;
                    } else {
                        // A newer upsert resurrects (if a tombstone sat below it, that
                        // tombstone already cleared the accumulated patches) and its
                        // property patches win over any surviving older ones.
                        a.tombstoned = false;
                        for (k, v) in &nd.patches {
                            a.patches.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }
        acc
    }

    /// Whether the core node `dense_id` is tombstoned by the delta (deleted): reads
    /// must suppress it (Phase 2). Folded newest-wins across levels — the newest level
    /// that touches the id decides (a delete over an older write deletes; a re-`MERGE`
    /// over an older delete resurrects). Reads only the tombstone flags, so it never
    /// clones the merged patch set (the hot suppression-filter path).
    #[inline]
    pub fn is_tombstoned(&self, dense_id: u64) -> bool {
        for level in self.levels_newest_first() {
            if let Some(nd) = level.node_patch(dense_id) {
                return nd.tombstoned;
            }
        }
        false
    }

    /// Count of delta-born-or-patched node identities, for scan-range planning. Summed
    /// across levels — an over-estimate when a core node is patched in several levels,
    /// which is fine for a planning bound.
    pub fn node_delta_count(&self) -> usize {
        self.levels_newest_first()
            .map(Memtable::node_delta_count)
            .sum()
    }

    /// The base of the synthetic dense-id space (the core `node_count` this delta was
    /// opened against): an id `>= synthetic_base` is a delta-born node whose reads
    /// route to the delta only, never a core block. It is the **min** across levels
    /// (older levels have lower bases; the oldest is the core count).
    #[inline]
    pub fn synthetic_base(&self) -> u64 {
        self.levels_newest_first()
            .map(Memtable::synthetic_base)
            .min()
            .unwrap_or(0)
    }

    /// The number of delta-born nodes overlaid — the merged `node_count` is
    /// `core.node_count() + born_count()`. Summed across levels (born-id ranges are
    /// disjoint and stacked past the core count).
    #[inline]
    pub fn born_count(&self) -> u64 {
        self.levels_newest_first().map(Memtable::born_count).sum()
    }

    /// Recover a node's `(label, key, key-value)` business identity by dense id — the
    /// material a delta-born node's label + business-key property are read from. The
    /// newest level touching the id answers (a synthetic id lives in exactly one; a
    /// core id's identity is level-invariant).
    #[inline]
    pub fn node_identity_by_dense(&self, dense_id: u64) -> Option<(&str, &str, &Value)> {
        self.levels_newest_first()
            .find_map(|m| m.node_identity_by_dense(dense_id))
    }

    /// The synthetic dense ids of delta-born nodes carrying `label`, appended to a
    /// core label scan (tombstone suppression happens in the caller). Unioned across
    /// levels oldest-first, so the ids come out ascending (stacked synthetic ranges).
    #[inline]
    pub fn born_ids_with_label(&self, label: &str) -> Vec<u64> {
        if self.l0.is_empty() {
            return self.mem.born_ids_with_label(label);
        }
        self.levels_oldest_first()
            .flat_map(|m| m.born_ids_with_label(label))
            .collect()
    }

    /// Delta-born nodes for the `RangeEq` overlay: those carrying `label` whose
    /// indexed property `prop` equals `key` (Phase 2d; tombstone suppression in the
    /// caller). Unioned across levels oldest-first.
    #[inline]
    pub fn born_ids_in_index_eq(&self, label: &str, prop: &str, key: &Value) -> Vec<u64> {
        if self.l0.is_empty() {
            return self.mem.born_ids_in_index_eq(label, prop, key);
        }
        self.levels_oldest_first()
            .flat_map(|m| m.born_ids_in_index_eq(label, prop, key))
            .collect()
    }

    /// Delta-born nodes for the `RangeRange` overlay: those carrying `label` whose
    /// indexed property `prop` falls in `[lo, hi]` with per-bound inclusivity
    /// (Phase 2d; tombstone suppression in the caller). Unioned across levels
    /// oldest-first.
    #[inline]
    pub fn born_ids_in_index_range(
        &self,
        label: &str,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Vec<u64> {
        if self.l0.is_empty() {
            return self.mem.born_ids_in_index_range(
                label,
                prop,
                lo,
                lo_inclusive,
                hi,
                hi_inclusive,
            );
        }
        self.levels_oldest_first()
            .flat_map(|m| {
                m.born_ids_in_index_range(label, prop, lo, lo_inclusive, hi, hi_inclusive)
            })
            .collect()
    }

    /// The base of the synthetic edge dense-id space (the core `edge_count` this delta
    /// was opened against): an edge id `>= edge_synthetic_base` is a delta-born edge
    /// whose `rel_record` routes to the delta, never a core edge block (Phase 3). The
    /// **min** across levels (= the core count), mirroring [`synthetic_base`](Self::synthetic_base).
    #[inline]
    pub fn edge_synthetic_base(&self) -> u64 {
        self.levels_newest_first()
            .map(Memtable::edge_synthetic_base)
            .min()
            .unwrap_or(0)
    }

    /// The number of delta-born edges overlaid — the merged `edge_count` is
    /// `core.edge_count() + born_edge_count()`. Summed across levels.
    #[inline]
    pub fn born_edge_count(&self) -> u64 {
        self.levels_newest_first()
            .map(Memtable::born_edge_count)
            .sum()
    }

    /// The delta edges outgoing from `node` (Phase 3 traversal overlay): born edges to
    /// append, tombstoned edges to suppress from the core adjacency. Merged newest-wins
    /// across levels (Phase 4c) — see [`merge_edges`](Self::merge_edges).
    #[inline]
    pub fn out_edges(&self, node: u64) -> Vec<DeltaEdge> {
        self.merge_edges(node, true)
    }

    /// The delta edges incoming to `node` (Phase 3 traversal overlay).
    #[inline]
    pub fn in_edges(&self, node: u64) -> Vec<DeltaEdge> {
        self.merge_edges(node, false)
    }

    /// Fold the delta edges incident to `node` in one direction across levels, keyed by
    /// edge identity `(reltype, neighbour)` (unique for a fixed node + direction), with
    /// the **newest** level's entry winning. So a born edge flushed to an L0 level and
    /// later deleted in a newer level surfaces once, tombstoned — the traversal overlay
    /// then suppresses it, never double-counting or resurrecting it. Output order is
    /// deterministic (newest level's edges first, in that level's own order).
    fn merge_edges(&self, node: u64, outgoing: bool) -> Vec<DeltaEdge> {
        let read = |m: &Memtable| {
            if outgoing {
                m.out_edges(node)
            } else {
                m.in_edges(node)
            }
        };
        if self.l0.is_empty() {
            return read(&self.mem);
        }
        let mut seen: HashSet<(String, u64)> = HashSet::new();
        let mut out = Vec::new();
        for level in self.levels_newest_first() {
            for e in read(level) {
                if seen.insert((e.reltype.clone(), e.other)) {
                    out.push(e);
                }
            }
        }
        out
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
            OpResolution::Node(Some(9)),
        );
        let mut direct = Memtable::new();
        direct.delete_node("L", "k", Value::Int(3), Some(9));
        assert_eq!(viae.node_patch(9), direct.node_patch(9));
        assert!(viae.node_patch(9).unwrap().tombstoned);
    }

    #[test]
    fn delta_born_nodes_allocate_stable_synthetic_ids() {
        // Core has 100 nodes; delta-born nodes take ids 100, 101, … in first-seen
        // order, and re-upserting a born key keeps its id (never re-allocates).
        let mut m = Memtable::with_synthetic_base(100);
        assert_eq!(m.synthetic_base(), 100);
        m.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            None,
            [("age".to_string(), Value::Int(40))],
        );
        m.upsert_node("Person", "name", Value::Str("Erin".into()), None, []);
        assert_eq!(m.born_count(), 2);
        // First born → 100, second → 101.
        let dave = 100;
        let erin = 101;
        assert_eq!(
            m.node_patch(dave).unwrap().patches.get("age"),
            Some(&Value::Int(40))
        );
        assert!(m.node_patch(erin).unwrap().patches.is_empty());
        assert_eq!(
            m.node_identity_by_dense(dave),
            Some(("Person", "name", &Value::Str("Dave".into())))
        );

        // Re-upsert Dave with a new property: same synthetic id, no new born slot.
        m.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            None,
            [("city".to_string(), Value::Str("NYC".into()))],
        );
        assert_eq!(m.born_count(), 2, "re-upsert does not allocate a new id");
        assert_eq!(
            m.node_patch(dave).unwrap().patches.get("city"),
            Some(&Value::Str("NYC".into()))
        );
        assert_eq!(
            m.node_patch(dave).unwrap().patches.get("age"),
            Some(&Value::Int(40))
        );
    }

    #[test]
    fn born_ids_with_label_filters_and_survives_delete() {
        let mut m = Memtable::with_synthetic_base(10);
        m.upsert_node("Person", "name", Value::Str("Dave".into()), None, []);
        m.upsert_node("Company", "ticker", Value::Str("ZZZ".into()), None, []);
        m.upsert_node("Person", "name", Value::Str("Erin".into()), None, []);
        assert_eq!(m.born_ids_with_label("Person"), vec![10, 12]);
        assert_eq!(m.born_ids_with_label("Company"), vec![11]);

        // Deleting a born node keeps it in the label list (the caller suppresses the
        // tombstone) but marks it tombstoned.
        m.delete_node("Person", "name", Value::Str("Dave".into()), None);
        assert_eq!(m.born_ids_with_label("Person"), vec![10, 12]);
        assert!(m.node_patch(10).unwrap().tombstoned);
    }

    #[test]
    fn born_index_overlay_eq_and_range() {
        // Core has 10 nodes; born People with an indexed `age`. Some carry `age` as
        // a patch, one as… well, `age` is not the business key here (name is), so it
        // must come from a patch to be indexed.
        let mut m = Memtable::with_synthetic_base(10);
        m.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            None,
            [("age".to_string(), Value::Int(40))],
        ); // id 10
        m.upsert_node(
            "Person",
            "name",
            Value::Str("Erin".into()),
            None,
            [("age".to_string(), Value::Int(25))],
        ); // id 11
        m.upsert_node("Person", "name", Value::Str("Fry".into()), None, []); // id 12, no age
        m.upsert_node(
            "Company",
            "ticker",
            Value::Str("ZZZ".into()),
            None,
            [("age".to_string(), Value::Int(40))],
        ); // id 13, wrong label

        // Equality on the *business key* property (name) reads from identity.
        assert_eq!(
            m.born_ids_in_index_eq("Person", "name", &Value::Str("Dave".into())),
            vec![10]
        );
        // Equality on a patched property; label-filtered (Company excluded).
        assert_eq!(
            m.born_ids_in_index_eq("Person", "age", &Value::Int(40)),
            vec![10]
        );
        // A node without the indexed property (Fry) never appears.
        assert_eq!(
            m.born_ids_in_index_eq("Person", "age", &Value::Int(99)),
            Vec::<u64>::new()
        );

        // Range [25, 40] inclusive → Erin(25) and Dave(40), ascending by id.
        assert_eq!(
            m.born_ids_in_index_range(
                "Person",
                "age",
                Some(&Value::Int(25)),
                true,
                Some(&Value::Int(40)),
                true
            ),
            vec![10, 11]
        );
        // Exclusive low bound drops Erin(25).
        assert_eq!(
            m.born_ids_in_index_range("Person", "age", Some(&Value::Int(25)), false, None, true),
            vec![10]
        );
        // Unbounded → both aged People (Fry has no age).
        assert_eq!(
            m.born_ids_in_index_range("Person", "age", None, true, None, true),
            vec![10, 11]
        );
    }

    #[test]
    fn born_index_overlay_patch_wins_over_business_key() {
        // A patch on the business-key property overrides the identity value for the
        // index, matching the read overlay's precedence.
        let mut m = Memtable::with_synthetic_base(0);
        m.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            None,
            [("name".to_string(), Value::Str("Dan".into()))],
        );
        assert_eq!(
            m.born_ids_in_index_eq("Person", "name", &Value::Str("Dan".into())),
            vec![0]
        );
        assert!(m
            .born_ids_in_index_eq("Person", "name", &Value::Str("Dave".into()))
            .is_empty());
    }

    #[test]
    fn born_index_overlay_includes_tombstoned_for_caller_suppression() {
        // A tombstoned born node still surfaces (the caller's suppression drops it),
        // matching `born_ids_with_label`. Its patches are cleared, so only a
        // business-key seek can still match it.
        let mut m = Memtable::with_synthetic_base(0);
        m.upsert_node("Person", "name", Value::Str("Dave".into()), None, []);
        m.delete_node("Person", "name", Value::Str("Dave".into()), None);
        assert_eq!(
            m.born_ids_in_index_eq("Person", "name", &Value::Str("Dave".into())),
            vec![0]
        );
        assert!(m.node_patch(0).unwrap().tombstoned);
    }

    #[test]
    fn born_id_allocation_is_replay_order_deterministic() {
        // Applying the same op sequence twice (the live path vs. a WAL replay) yields
        // identical synthetic ids, because allocation follows first-seen order.
        let ops = [
            ("Person", "name", Value::Str("A".into())),
            ("Person", "name", Value::Str("B".into())),
            ("Person", "name", Value::Str("A".into())), // repeat: reuses A's id
            ("Person", "name", Value::Str("C".into())),
        ];
        let build = || {
            let mut m = Memtable::with_synthetic_base(5);
            for (l, k, v) in &ops {
                m.upsert_node(l, k, v.clone(), None, []);
            }
            m
        };
        let a = build();
        let b = build();
        assert_eq!(a.born_ids_with_label("Person"), vec![5, 6, 7]);
        assert_eq!(
            a.born_ids_with_label("Person"),
            b.born_ids_with_label("Person")
        );
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

    // ── Edge overlay (Phase 3a) ────────────────────────────────────────────────

    fn edge_between(
        m: &Memtable,
        node: u64,
        outgoing: bool,
        reltype: &str,
        other: u64,
    ) -> Option<DeltaEdge> {
        let edges = if outgoing {
            m.out_edges(node)
        } else {
            m.in_edges(node)
        };
        edges
            .into_iter()
            .find(|e| e.reltype == reltype && e.other == other)
    }

    #[test]
    fn upsert_edge_between_core_nodes_is_born_and_indexed_both_ways() {
        // Two core nodes (dense 10, 20); a MERGE edge between them takes a synthetic
        // edge id past the core edge_count and is visible outgoing from 10 / incoming
        // to 20.
        let mut m = Memtable::with_bases(100, 500);
        m.upsert_edge(
            "Company",
            "ticker",
            Value::Str("A".into()),
            "OWNS",
            "Drug",
            "id",
            Value::Int(7),
            Some(10),
            Some(20),
            [],
        );
        assert_eq!(m.born_edge_count(), 1);
        let out = m.out_edges(10);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].other, 20);
        assert_eq!(out[0].reltype, "OWNS");
        assert_eq!(out[0].edge_id, Some(500)); // first born edge = edge_synthetic_base
        assert!(!out[0].tombstoned);
        let inc = m.in_edges(20);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].other, 10);
        assert_eq!(inc[0].edge_id, Some(500));
        // Not visible from the wrong ends.
        assert!(m.out_edges(20).is_empty());
        assert!(m.in_edges(10).is_empty());
        assert!(!m.is_empty());
    }

    #[test]
    fn upsert_edge_is_idempotent_by_identity() {
        // Re-MERGE-ing the same edge identity reuses the synthetic id and does not
        // duplicate the adjacency entry.
        let mut m = Memtable::with_bases(0, 0);
        let mk = |m: &mut Memtable| {
            m.upsert_edge(
                "L",
                "k",
                Value::Int(1),
                "R",
                "L",
                "k",
                Value::Int(2),
                Some(1),
                Some(2),
                [],
            );
        };
        mk(&mut m);
        mk(&mut m);
        assert_eq!(m.born_edge_count(), 1);
        assert_eq!(m.out_edges(1).len(), 1);
        assert_eq!(m.out_edges(1)[0].edge_id, Some(0));
    }

    #[test]
    fn upsert_edge_creates_born_endpoint_nodes() {
        // A MERGE edge whose endpoints are absent from the core (resolved None)
        // creates delta-born nodes for them, allocating synthetic node ids in
        // first-seen order; the edge then points at those synthetic ids.
        let mut m = Memtable::with_bases(100, 0);
        m.upsert_edge(
            "Person",
            "name",
            Value::Str("Ann".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Bob".into()),
            None,
            None,
            [],
        );
        assert_eq!(m.born_count(), 2, "both endpoints born");
        // Ann → 100, Bob → 101 (first-seen order).
        let out = m.out_edges(100);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].other, 101);
        assert_eq!(
            m.node_identity_by_dense(100),
            Some(("Person", "name", &Value::Str("Ann".into())))
        );
        assert_eq!(
            m.node_identity_by_dense(101),
            Some(("Person", "name", &Value::Str("Bob".into())))
        );
    }

    #[test]
    fn delete_core_edge_stores_tombstone_only_entry() {
        // Deleting an edge between two core nodes with no prior born edge stores a
        // tombstone-only entry (no synthetic edge id) so the reader can suppress the
        // matching core adjacency record.
        let mut m = Memtable::with_bases(100, 500);
        m.delete_edge(
            "Company",
            "ticker",
            Value::Str("A".into()),
            "OWNS",
            "Drug",
            "id",
            Value::Int(7),
            Some(10),
            Some(20),
        );
        assert_eq!(m.born_edge_count(), 0, "a tombstone allocates no edge id");
        let e = edge_between(&m, 10, true, "OWNS", 20).expect("tombstone indexed outgoing");
        assert!(e.tombstoned);
        assert_eq!(e.edge_id, None);
        assert!(edge_between(&m, 20, false, "OWNS", 10).unwrap().tombstoned);
    }

    #[test]
    fn merge_then_delete_edge_tombstones_the_born_edge() {
        let mut m = Memtable::with_bases(0, 0);
        m.upsert_edge(
            "L",
            "k",
            Value::Int(1),
            "R",
            "L",
            "k",
            Value::Int(2),
            Some(1),
            Some(2),
            [],
        );
        m.delete_edge(
            "L",
            "k",
            Value::Int(1),
            "R",
            "L",
            "k",
            Value::Int(2),
            Some(1),
            Some(2),
        );
        let e = edge_between(&m, 1, true, "R", 2).expect("edge present");
        assert!(e.tombstoned, "the born edge is now tombstoned");
        assert_eq!(m.born_edge_count(), 1, "no new edge id from the delete");
    }

    #[test]
    fn delete_edge_with_absent_endpoint_is_a_noop() {
        // Neither endpoint exists (not core, not born) → nothing to tombstone.
        let mut m = Memtable::with_bases(100, 0);
        m.delete_edge(
            "L",
            "k",
            Value::Int(1),
            "R",
            "L",
            "k",
            Value::Int(2),
            None,
            None,
        );
        assert!(m.is_empty(), "no edge, no phantom born node");
    }

    #[test]
    fn upsert_edge_resurrects_a_tombstoned_edge() {
        let mut m = Memtable::with_bases(0, 0);
        let mk_del = |m: &mut Memtable| {
            m.delete_edge(
                "L",
                "k",
                Value::Int(1),
                "R",
                "L",
                "k",
                Value::Int(2),
                Some(1),
                Some(2),
            );
        };
        mk_del(&mut m);
        assert!(edge_between(&m, 1, true, "R", 2).unwrap().tombstoned);
        m.upsert_edge(
            "L",
            "k",
            Value::Int(1),
            "R",
            "L",
            "k",
            Value::Int(2),
            Some(1),
            Some(2),
            [],
        );
        assert!(!edge_between(&m, 1, true, "R", 2).unwrap().tombstoned);
    }

    #[test]
    fn apply_edge_ops_match_direct_calls() {
        // The WAL-replay path (`apply` + `OpResolution::Edge`) must not diverge from a
        // direct call.
        let op = WalOp::UpsertEdge {
            src_label: "L".into(),
            src_key: "k".into(),
            src_value: Value::Int(1),
            reltype: "R".into(),
            dst_label: "L".into(),
            dst_key: "k".into(),
            dst_value: Value::Int(2),
            patches: vec![],
        };
        let mut viae = Memtable::with_bases(0, 0);
        viae.apply(
            &op,
            OpResolution::Edge {
                src: Some(1),
                dst: Some(2),
            },
        );
        let mut direct = Memtable::with_bases(0, 0);
        direct.upsert_edge(
            "L",
            "k",
            Value::Int(1),
            "R",
            "L",
            "k",
            Value::Int(2),
            Some(1),
            Some(2),
            [],
        );
        assert_eq!(viae.out_edges(1), direct.out_edges(1));
    }

    #[test]
    fn iter_edges_recovers_identity_names() {
        let mut m = Memtable::with_bases(0, 0);
        m.upsert_edge(
            "Company",
            "ticker",
            Value::Str("A".into()),
            "OWNS",
            "Drug",
            "id",
            Value::Int(7),
            Some(1),
            Some(2),
            [],
        );
        let rows: Vec<_> = m.iter_edges().collect();
        assert_eq!(rows.len(), 1);
        let (sl, sk, sv, rt, dl, dk, dv, delta) = &rows[0];
        assert_eq!((*sl, *sk), ("Company", "ticker"));
        assert_eq!(**sv, Value::Str("A".into()));
        assert_eq!(*rt, "OWNS");
        assert_eq!((*dl, *dk), ("Drug", "id"));
        assert_eq!(**dv, Value::Int(7));
        assert!(!delta.tombstoned);
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

    // ── Multi-level read merge (Phase 4c-A) ────────────────────────────────────
    //
    // These stack two memtables into a `DeltaSnapshot` directly (an older "L0"
    // segment beneath the live one) — no flush machinery is needed to exercise the
    // fold; that wiring is 4c-B.

    /// Build a `DeltaSnapshot` from a live memtable and older L0 levels (newest first).
    fn snap(mem: Memtable, l0: Vec<Memtable>) -> DeltaSnapshot {
        DeltaSnapshot::with_levels(Arc::new(mem), l0.into_iter().map(Arc::new).collect())
    }

    #[test]
    fn snapshot_multi_level_node_patch_merges_per_property_newest_wins() {
        // Core node dense id 5: patched `age=40` before a flush (the L0 level), then
        // `age=41` + `city=NYC` after (the live memtable). The merge unions the
        // properties with the newer level winning per key.
        let mut old = Memtable::new();
        old.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            Some(5),
            [("age".to_string(), Value::Int(40))],
        );
        let mut mem = Memtable::new();
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            Some(5),
            [
                ("age".to_string(), Value::Int(41)),
                ("city".to_string(), Value::Str("NYC".into())),
            ],
        );
        let s = snap(mem, vec![old]);
        let nd = s.node_patch(5).expect("merged patch");
        assert!(!nd.tombstoned);
        assert_eq!(nd.patches.get("age"), Some(&Value::Int(41))); // newer level wins
        assert_eq!(nd.patches.get("city"), Some(&Value::Str("NYC".into())));
        assert!(!s.is_tombstoned(5));
    }

    #[test]
    fn snapshot_multi_level_delete_in_newest_level_wins() {
        // L0 upserts `age=40`; the live level deletes the same core node. The newer
        // delete wins: the merged delta is a tombstone carrying no properties.
        let mut old = Memtable::new();
        old.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            Some(5),
            [("age".to_string(), Value::Int(40))],
        );
        let mut mem = Memtable::new();
        mem.delete_node("Person", "name", Value::Str("Dave".into()), Some(5));
        let s = snap(mem, vec![old]);
        assert!(s.is_tombstoned(5));
        let nd = s.node_patch(5).expect("tombstone is a delta");
        assert!(nd.tombstoned);
        assert!(
            nd.patches.is_empty(),
            "a deleted node carries no properties"
        );
    }

    #[test]
    fn snapshot_multi_level_re_merge_shadows_older_tombstone() {
        // L0 tombstones the node; the live level re-`MERGE`s it with a fresh property.
        // The newer upsert resurrects it (LSM tombstone semantics): not tombstoned, and
        // the older `age` patch is gone (the tombstone below cleared it).
        let mut old = Memtable::new();
        old.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            Some(5),
            [("age".to_string(), Value::Int(40))],
        );
        old.delete_node("Person", "name", Value::Str("Dave".into()), Some(5));
        let mut mem = Memtable::new();
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            Some(5),
            [("city".to_string(), Value::Str("NYC".into()))],
        );
        let s = snap(mem, vec![old]);
        assert!(
            !s.is_tombstoned(5),
            "the newer re-MERGE resurrects the node"
        );
        let nd = s.node_patch(5).expect("resurrected delta");
        assert!(!nd.tombstoned);
        assert_eq!(nd.patches.get("city"), Some(&Value::Str("NYC".into())));
        assert!(
            !nd.patches.contains_key("age"),
            "the tombstone below the re-MERGE cleared the older patch"
        );
    }

    #[test]
    fn snapshot_multi_level_born_ids_union_across_levels() {
        // Core has 100 nodes. A born Person landed in L0 (id 100); after the flush the
        // live memtable is rebased to 101 and a second born Person lands (id 101). The
        // snapshot unions the born ids (ascending), sums the count, and routes each
        // synthetic id to the level that owns it.
        let mut old = Memtable::with_synthetic_base(100);
        old.upsert_node(
            "Person",
            "name",
            Value::Str("Ann".into()),
            None,
            [("age".to_string(), Value::Int(30))],
        ); // id 100
        let mut mem = Memtable::with_synthetic_base(101);
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Bob".into()),
            None,
            [("age".to_string(), Value::Int(31))],
        ); // id 101
        let s = snap(mem, vec![old]);
        assert_eq!(s.born_count(), 2);
        assert_eq!(s.synthetic_base(), 100, "min across levels = core count");
        assert_eq!(s.born_ids_with_label("Person"), vec![100, 101]);
        // Each synthetic id resolves against its owning level only.
        assert_eq!(
            s.node_identity_by_dense(100),
            Some(("Person", "name", &Value::Str("Ann".into())))
        );
        assert_eq!(
            s.node_identity_by_dense(101),
            Some(("Person", "name", &Value::Str("Bob".into())))
        );
        assert_eq!(
            s.node_patch(100).unwrap().patches.get("age"),
            Some(&Value::Int(30))
        );
        assert_eq!(
            s.node_patch(101).unwrap().patches.get("age"),
            Some(&Value::Int(31))
        );
    }

    #[test]
    fn snapshot_multi_level_born_index_overlay_unions() {
        // Born People carrying an indexed `age` split across two levels — an index seek
        // must find both.
        let mut old = Memtable::with_synthetic_base(100);
        old.upsert_node(
            "Person",
            "name",
            Value::Str("Ann".into()),
            None,
            [("age".to_string(), Value::Int(30))],
        ); // id 100
        let mut mem = Memtable::with_synthetic_base(101);
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Bob".into()),
            None,
            [("age".to_string(), Value::Int(40))],
        ); // id 101
        let s = snap(mem, vec![old]);
        assert_eq!(
            s.born_ids_in_index_eq("Person", "age", &Value::Int(30)),
            vec![100]
        );
        assert_eq!(
            s.born_ids_in_index_eq("Person", "age", &Value::Int(40)),
            vec![101]
        );
        assert_eq!(
            s.born_ids_in_index_range("Person", "age", Some(&Value::Int(30)), true, None, true),
            vec![100, 101]
        );
    }

    #[test]
    fn snapshot_multi_level_edges_merge_last_writer_wins() {
        // L0 born two edges from core node 10: (10)-OWNS->(20) and (10)-FOO->(30). The
        // live level deletes the OWNS edge (tombstone-only entry). The merge keeps one
        // entry per (reltype, neighbour), newest winning: OWNS surfaces tombstoned, FOO
        // stays a live born edge.
        let mut old = Memtable::with_bases(100, 500);
        old.upsert_edge(
            "Company",
            "ticker",
            Value::Str("A".into()),
            "OWNS",
            "Drug",
            "id",
            Value::Int(20),
            Some(10),
            Some(20),
            [],
        );
        old.upsert_edge(
            "Company",
            "ticker",
            Value::Str("A".into()),
            "FOO",
            "Drug",
            "id",
            Value::Int(30),
            Some(10),
            Some(30),
            [],
        );
        // Live level is rebased past L0's two born edges (base 502).
        let mut mem = Memtable::with_bases(100, 502);
        mem.delete_edge(
            "Company",
            "ticker",
            Value::Str("A".into()),
            "OWNS",
            "Drug",
            "id",
            Value::Int(20),
            Some(10),
            Some(20),
        );
        let s = snap(mem, vec![old]);
        assert_eq!(s.born_edge_count(), 2, "both born edges counted (from L0)");
        assert_eq!(
            s.edge_synthetic_base(),
            500,
            "min across levels = core count"
        );
        let out = s.out_edges(10);
        assert_eq!(out.len(), 2, "one entry per (reltype, neighbour)");
        let owns = out.iter().find(|e| e.reltype == "OWNS").unwrap();
        assert!(
            owns.tombstoned,
            "the newer delete wins over the older born edge"
        );
        let foo = out.iter().find(|e| e.reltype == "FOO").unwrap();
        assert!(!foo.tombstoned);
        assert_eq!(foo.edge_id, Some(501));
        // Incoming reads fold identically.
        assert!(s.in_edges(20)[0].tombstoned);
        assert!(!s.in_edges(30)[0].tombstoned);
    }

    #[test]
    fn snapshot_is_empty_folds_across_levels() {
        // No levels, or all-empty levels, is empty; a non-empty L0 under an empty live
        // memtable is not.
        assert!(DeltaSnapshot::empty().is_empty());
        assert!(snap(Memtable::new(), vec![Memtable::new()]).is_empty());
        let mut old = Memtable::new();
        old.upsert_node("L", "k", Value::Int(1), Some(0), []);
        let s = snap(Memtable::new(), vec![old]);
        assert!(!s.is_empty(), "a non-empty L0 keeps the snapshot non-empty");
        assert_eq!(
            s.node_patch(0).unwrap().patches.len(),
            0,
            "the sole (empty) patch still resolves through the L0 level"
        );
    }

    #[test]
    fn snapshot_single_level_matches_the_memtable() {
        // With no L0 the snapshot is a thin clone-on-read wrapper over one memtable.
        let mut mem = Memtable::new();
        mem.upsert_node(
            "L",
            "k",
            Value::Int(1),
            Some(3),
            [("p".to_string(), Value::Bool(true))],
        );
        let arc = Arc::new(mem);
        let s = DeltaSnapshot::from_memtable(arc.clone());
        assert_eq!(s.node_patch(3), arc.node_patch(3).cloned());
        assert!(!s.is_empty());
    }

    #[test]
    fn born_synthetic_for_identity_resolves_only_born_nodes() {
        // The write path (Phase 4c-B) resolves a re-MERGE of an already-flushed born
        // node to its existing synthetic id; a core-resolved patch and an unknown key
        // both resolve to `None`.
        let mut m = Memtable::with_synthetic_base(100);
        // A born node (absent from the core) → synthetic id 100.
        m.upsert_node(
            "Person",
            "name",
            Value::Str("Zoe".into()),
            None,
            [("age".to_string(), Value::Int(9))],
        );
        // A core-resolved patch on dense id 5 — not a born node.
        m.upsert_node("Person", "name", Value::Str("Al".into()), Some(5), []);

        assert_eq!(
            m.born_synthetic_for_identity("Person", "name", &Value::Str("Zoe".into())),
            Some(100),
            "the born node resolves to its synthetic id"
        );
        assert_eq!(
            m.born_synthetic_for_identity("Person", "name", &Value::Str("Al".into())),
            None,
            "a core-resolved node is not born"
        );
        assert_eq!(
            m.born_synthetic_for_identity("Person", "name", &Value::Str("Nobody".into())),
            None,
            "an unknown key resolves to None"
        );
        assert_eq!(
            m.born_synthetic_for_identity("Ghost", "name", &Value::Str("Zoe".into())),
            None,
            "a label absent from the interner short-circuits to None"
        );
    }
}
