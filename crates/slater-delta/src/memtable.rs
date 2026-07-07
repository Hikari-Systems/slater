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

use crate::identity::{EdgeIdentity, NodeIdentity};
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

    /// The base of the synthetic dense-id space (the core `node_count` this delta was
    /// opened against): an id `>= synthetic_base` is a delta-born node whose reads
    /// route to the delta only, never a core block.
    #[inline]
    pub fn synthetic_base(&self) -> u64 {
        self.mem.synthetic_base()
    }

    /// The number of delta-born nodes overlaid — the merged `node_count` is
    /// `core.node_count() + born_count()`.
    #[inline]
    pub fn born_count(&self) -> u64 {
        self.mem.born_count()
    }

    /// Recover a node's `(label, key, key-value)` business identity by dense id — the
    /// material a delta-born node's label + business-key property are read from.
    #[inline]
    pub fn node_identity_by_dense(&self, dense_id: u64) -> Option<(&str, &str, &Value)> {
        self.mem.node_identity_by_dense(dense_id)
    }

    /// The synthetic dense ids of delta-born nodes carrying `label`, appended to a
    /// core label scan (tombstone suppression happens in the caller).
    #[inline]
    pub fn born_ids_with_label(&self, label: &str) -> Vec<u64> {
        self.mem.born_ids_with_label(label)
    }

    /// Delta-born nodes for the `RangeEq` overlay: those carrying `label` whose
    /// indexed property `prop` equals `key` (Phase 2d; tombstone suppression in the
    /// caller).
    #[inline]
    pub fn born_ids_in_index_eq(&self, label: &str, prop: &str, key: &Value) -> Vec<u64> {
        self.mem.born_ids_in_index_eq(label, prop, key)
    }

    /// Delta-born nodes for the `RangeRange` overlay: those carrying `label` whose
    /// indexed property `prop` falls in `[lo, hi]` with per-bound inclusivity
    /// (Phase 2d; tombstone suppression in the caller).
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
        self.mem
            .born_ids_in_index_range(label, prop, lo, lo_inclusive, hi, hi_inclusive)
    }

    /// The base of the synthetic edge dense-id space (the core `edge_count` this delta
    /// was opened against): an edge id `>= edge_synthetic_base` is a delta-born edge
    /// whose `rel_record` routes to the delta, never a core edge block (Phase 3).
    #[inline]
    pub fn edge_synthetic_base(&self) -> u64 {
        self.mem.edge_synthetic_base()
    }

    /// The number of delta-born edges overlaid — the merged `edge_count` is
    /// `core.edge_count() + born_edge_count()`.
    #[inline]
    pub fn born_edge_count(&self) -> u64 {
        self.mem.born_edge_count()
    }

    /// The delta edges outgoing from `node` (Phase 3 traversal overlay): born edges to
    /// append, tombstoned edges to suppress from the core adjacency.
    #[inline]
    pub fn out_edges(&self, node: u64) -> Vec<DeltaEdge> {
        self.mem.out_edges(node)
    }

    /// The delta edges incoming to `node` (Phase 3 traversal overlay).
    #[inline]
    pub fn in_edges(&self, node: u64) -> Vec<DeltaEdge> {
        self.mem.in_edges(node)
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
}
