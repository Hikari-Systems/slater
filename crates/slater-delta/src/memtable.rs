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
    /// a tombstone-only or patch-only entry over an existing **core** edge (which keeps
    /// its own core edge id — see `core_edge`).
    synthetic_edge: Option<u64>,
    /// For an entry that overlays an existing **core** edge (a `SET r.p` in-place patch,
    /// carried in [`Memtable::by_edge_id`]) the resolved core edge id. `None` for a
    /// delta-born edge (which uses `synthetic_edge`) and for a tombstone-only entry
    /// (matched against the core adjacency by identity, not by id). When both are `None`
    /// the entry only suppresses a core edge; a patch overlay always sets this.
    core_edge: Option<u64>,
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
    /// An edge op's endpoint dense ids, plus `edge_id`: `Some(core_edge_id)` when the
    /// op patches an already-existing **core** edge in place (the caller resolved the
    /// core edge id against the current core), `None` for a delta-born edge create or
    /// any delete (which is resolved by identity). Distinguishing the two is what routes
    /// [`Memtable::apply`] to `patch_core_edge` versus `upsert_edge`.
    Edge {
        src: Option<u64>,
        dst: Option<u64>,
        edge_id: Option<u64>,
    },
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
    /// Read index: **core** edge id → the edge identity key of its in-place property
    /// patch (a `SET r.p` on an existing core edge). Only genuine core edge ids
    /// (`< edge_synthetic_base`) appear here, so it never collides with a born edge id;
    /// the edge-property overlay ([`Memtable::edge_delta_by_id`]) consults it to fold a
    /// core edge's patched properties over its stored ones. Delta-born edges are not
    /// listed (they read through `born_edges`).
    by_edge_id: HashMap<u64, Vec<u8>>,
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

    /// Number of distinct edge identities carrying a delta.
    pub fn edge_delta_count(&self) -> usize {
        self.edges.len()
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
                OpResolution::Edge { src, dst, edge_id },
            ) => match edge_id {
                // A resolved core edge id ⇒ patch an existing core edge in place.
                Some(cid) => self.patch_core_edge(
                    src_label,
                    src_key,
                    src_value.clone(),
                    reltype,
                    dst_label,
                    dst_key,
                    dst_value.clone(),
                    src,
                    dst,
                    cid,
                    patches.iter().cloned(),
                ),
                // No core edge ⇒ a delta-born edge (create-if-absent / re-MERGE).
                None => self.upsert_edge(
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
            },
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
                OpResolution::Edge { src, dst, .. },
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
        self.born_ids_in_index(label, prop, |v| {
            value_in_range(v, lo, lo_inclusive, hi, hi_inclusive)
        })
    }

    /// Core dense ids (`dense < synthetic_base`) carrying `label` that carry a **patch
    /// on the indexed property `prop`** — the candidate set for the "moved indexed
    /// value" overlay. A core node whose indexed property is patched is still listed at
    /// its *original* value in the core ISAM, so a range seek must reconsider these
    /// against their patched value. Value-agnostic: the caller ([`DeltaSnapshot`]) tests
    /// the *merged* (newest-wins) patched value, so a node patched across several levels
    /// is judged once by its newest value. A tombstone clears the patches, so a deleted
    /// node never appears here (and is suppressed separately anyway).
    pub fn core_ids_patched_on_index(&self, label: &str, prop: &str) -> Vec<u64> {
        let mut out = Vec::new();
        for (&dense, ck) in &self.by_dense {
            if dense >= self.synthetic_base {
                continue; // a born node — handled by `born_ids_in_index_*`
            }
            let Some(e) = self.nodes.get(ck) else {
                continue;
            };
            if self.interner.name(e.identity.label) != Some(label) {
                continue;
            }
            if e.delta.patches.contains_key(prop) {
                out.push(dense);
            }
        }
        out
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
                    core_edge: None,
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
                    core_edge: None,      // matched by adjacency, not by id
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

    /// Patch an existing **core** edge's properties in place: `MERGE (a)-[r:R]->(b) SET
    /// r.p = …` where `(a)-[:R]->(b)` already exists in the core (the caller resolved its
    /// `core_edge_id`). The patch is stored on a `synthetic_edge = None` entry keyed by
    /// the edge identity and indexed under the core edge id in [`Self::by_edge_id`], so
    /// the edge-property overlay folds it over the core edge's stored properties. A
    /// **core edge is not born and not tombstoned by a patch**, so — unlike `upsert_edge`
    /// and `delete_edge` — this does *not* touch the born-edge vector or the
    /// outgoing/incoming adjacency indexes (traversal reads the edge from the core; only
    /// its properties are overlaid). Re-patching folds last-writer-wins and resurrects a
    /// prior tombstone (LWW). Shared by live writes and WAL replay.
    #[allow(clippy::too_many_arguments)]
    pub fn patch_core_edge(
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
        core_edge_id: u64,
        patches: impl IntoIterator<Item = (String, Value)>,
    ) {
        // Both endpoints are core (a core edge exists between them), so the resolved ids
        // are supplied; `endpoint_dense_or_create` returns them without allocating.
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
            self.edges.insert(
                eck.clone(),
                EdgeEntry {
                    identity,
                    src_dense,
                    dst_dense,
                    synthetic_edge: None,
                    core_edge: Some(core_edge_id),
                    delta: EdgeDelta::default(),
                },
            );
        }
        // Index (idempotently) under the core edge id so a property read by id resolves
        // it, then fold the patches last-writer-wins (clearing a prior tombstone).
        self.by_edge_id.insert(core_edge_id, eck.clone());
        let entry = self.edges.get_mut(&eck).expect("edge entry just ensured");
        entry.core_edge = Some(core_edge_id);
        entry.delta.tombstoned = false;
        let mut added = 0usize;
        for (name, val) in patches {
            added += name.len() + value_size(&val);
            entry.delta.patches.insert(name, val);
        }
        self.bytes += added;
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

    /// The folded delta of the edge with dense id `edge_id`, if this level carries one.
    /// For a **delta-born** edge (`edge_id >= edge_synthetic_base`) this is the entry
    /// this level owns (born-id ranges are disjoint + stacked across levels). For a
    /// **core** edge id (`< edge_synthetic_base`) it is this level's in-place property
    /// patch, if any ([`Self::by_edge_id`]). `None` when this level neither owns the born
    /// id nor patched the core edge. The read overlay reads an edge's **properties**
    /// through this (edge-property overlay), mirroring [`node_patch`](Self::node_patch).
    fn edge_delta_by_id(&self, edge_id: u64) -> Option<&EdgeDelta> {
        let base = self.edge_synthetic_base;
        if edge_id < base {
            // A **core** edge id: return this level's in-place property patch if it has
            // one (`by_edge_id` holds only genuine core edge ids, so a born edge id from
            // an older level — also `< base` — misses here and is owned by that level).
            let eck = self.by_edge_id.get(&edge_id)?;
            return self.edges.get(eck).map(|e| &e.delta);
        }
        let eck = self.born_edges.get((edge_id - base) as usize)?;
        self.edges.get(eck).map(|e| &e.delta)
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

    /// Fold a **contiguous, stacked run** of L0 levels (newest-first) into one
    /// equivalent memtable — the L0→L0 compaction primitive (Phase 4d-i). The result
    /// answers every read identically to
    /// [`DeltaSnapshot::with_levels`](DeltaSnapshot) over the same run, but as a single
    /// resident level: it reclaims the space of overwritten patches and shadowed
    /// tombstones, and (crucially) collapses the read fan-out (per-read work grows with
    /// level count). It touches no core and no WAL — L0 is already post-WAL.
    ///
    /// **Synthetic ids are preserved.** The inputs' born-id ranges are disjoint and
    /// stacked (`base_{k+1} = base_k + born_k`), so the merged level keeps the oldest
    /// input's base and every born node/edge its exact id — the active memtable above
    /// the stack, and any dense id already handed to a reader, stay valid. This is why
    /// the caller must pass a **contiguous** run (a `debug_assert` checks the born-id
    /// tiling). The merge is done by replaying the newest-wins folded state through the
    /// ordinary [`Self::upsert_node`]/[`Self::delete_node`]/[`Self::upsert_edge`]/
    /// [`Self::delete_edge`] paths (born entities in ascending id order, endpoints
    /// resolved explicitly so none is re-allocated), so allocation + byte accounting
    /// reuse the single tested code path rather than duplicating it.
    pub fn merge_levels(newest_first: &[&Memtable]) -> Memtable {
        let Some((oldest, _)) = newest_first.split_last() else {
            return Memtable::default();
        };
        let base_node = oldest.synthetic_base;
        let base_edge = oldest.edge_synthetic_base;

        // Fold every level newest-wins, keyed by an **interner-independent** identity key
        // (names + type-exact value bytes), so entries from levels with different local
        // symbol tables combine correctly.
        let mut fnodes: HashMap<Vec<u8>, FoldedNode> = HashMap::new();
        let mut fedges: HashMap<Vec<u8>, FoldedEdge> = HashMap::new();
        for seg in newest_first.iter().rev() {
            // Nodes with a dense id (core-resolved or born). Entries absent from
            // `by_dense` are inert no-op tombstones (a delete of a key absent from the
            // core and never born) — they suppress nothing, so they are dropped.
            for (&dense, ck) in &seg.by_dense {
                let Some(entry) = seg.nodes.get(ck) else {
                    continue;
                };
                let label = seg.interner.name(entry.identity.label).unwrap_or("");
                let key = seg.interner.name(entry.identity.key).unwrap_or("");
                let nk = node_name_key(label, key, &entry.identity.value);
                let f = fnodes.entry(nk).or_insert_with(|| FoldedNode {
                    label: label.to_string(),
                    key: key.to_string(),
                    value: entry.identity.value.clone(),
                    patches: BTreeMap::new(),
                    tombstoned: false,
                    dense,
                });
                f.dense = dense;
                fold_delta(&mut f.patches, &mut f.tombstoned, &entry.delta);
            }
            for entry in seg.edges.values() {
                let sl = seg.interner.name(entry.identity.src.label).unwrap_or("");
                let sk = seg.interner.name(entry.identity.src.key).unwrap_or("");
                let rt = seg.interner.name(entry.identity.reltype).unwrap_or("");
                let dl = seg.interner.name(entry.identity.dst.label).unwrap_or("");
                let dk = seg.interner.name(entry.identity.dst.key).unwrap_or("");
                let ek = edge_name_key(
                    sl,
                    sk,
                    &entry.identity.src.value,
                    rt,
                    dl,
                    dk,
                    &entry.identity.dst.value,
                );
                let f = fedges.entry(ek).or_insert_with(|| FoldedEdge {
                    src_label: sl.to_string(),
                    src_key: sk.to_string(),
                    src_value: entry.identity.src.value.clone(),
                    reltype: rt.to_string(),
                    dst_label: dl.to_string(),
                    dst_key: dk.to_string(),
                    dst_value: entry.identity.dst.value.clone(),
                    patches: BTreeMap::new(),
                    tombstoned: false,
                    src_dense: entry.src_dense,
                    dst_dense: entry.dst_dense,
                    born_edge_id: None,
                    core_edge_id: None,
                });
                f.src_dense = entry.src_dense;
                f.dst_dense = entry.dst_dense;
                if f.born_edge_id.is_none() {
                    f.born_edge_id = entry.synthetic_edge;
                }
                if f.core_edge_id.is_none() {
                    f.core_edge_id = entry.core_edge;
                }
                fold_delta_edge(&mut f.patches, &mut f.tombstoned, &entry.delta);
            }
        }

        let mut m = Memtable::with_bases(base_node, base_edge);

        // Born nodes first, in ascending synthetic-id order, so `upsert_node(None)`
        // re-allocates the identical ids off the (shared) oldest base.
        let mut born: Vec<&FoldedNode> = fnodes.values().filter(|f| f.dense >= base_node).collect();
        born.sort_by_key(|f| f.dense);
        for (i, f) in born.iter().enumerate() {
            debug_assert_eq!(
                f.dense,
                base_node + i as u64,
                "born node ids must tile [base, base+n) — run not contiguous?"
            );
            m.upsert_node(&f.label, &f.key, f.value.clone(), None, f.patches.clone());
            if f.tombstoned {
                m.delete_node(&f.label, &f.key, f.value.clone(), None);
            }
        }
        // Core-resolved nodes (patched / tombstoned existing core rows), in dense-id
        // order so the merged memtable is deterministic (dense is unique per core node).
        let mut core: Vec<&FoldedNode> = fnodes.values().filter(|f| f.dense < base_node).collect();
        core.sort_by_key(|f| f.dense);
        for f in core {
            if f.tombstoned {
                m.delete_node(&f.label, &f.key, f.value.clone(), Some(f.dense));
            } else {
                m.upsert_node(
                    &f.label,
                    &f.key,
                    f.value.clone(),
                    Some(f.dense),
                    f.patches.clone(),
                );
            }
        }

        // Born edges in ascending synthetic-edge-id order (endpoints resolved so none is
        // re-allocated), then the core-edge tombstone-only entries.
        let mut born_e: Vec<&FoldedEdge> = fedges
            .values()
            .filter(|f| f.born_edge_id.is_some())
            .collect();
        born_e.sort_by_key(|f| f.born_edge_id.unwrap());
        for (i, f) in born_e.iter().enumerate() {
            debug_assert_eq!(
                f.born_edge_id.unwrap(),
                base_edge + i as u64,
                "born edge ids must tile [base, base+n) — run not contiguous?"
            );
            m.upsert_edge(
                &f.src_label,
                &f.src_key,
                f.src_value.clone(),
                &f.reltype,
                &f.dst_label,
                &f.dst_key,
                f.dst_value.clone(),
                Some(f.src_dense),
                Some(f.dst_dense),
                f.patches.clone(),
            );
            if f.tombstoned {
                m.delete_edge(
                    &f.src_label,
                    &f.src_key,
                    f.src_value.clone(),
                    &f.reltype,
                    &f.dst_label,
                    &f.dst_key,
                    f.dst_value.clone(),
                    Some(f.src_dense),
                    Some(f.dst_dense),
                );
            }
        }
        // Core-edge overlay entries (no born id): a tombstone that suppresses a core
        // edge, or an in-place property patch, in endpoint/reltype order for determinism.
        let mut core_e: Vec<&FoldedEdge> = fedges
            .values()
            .filter(|f| f.born_edge_id.is_none())
            .collect();
        core_e.sort_by(|a, b| {
            (a.src_dense, a.dst_dense, &a.reltype).cmp(&(b.src_dense, b.dst_dense, &b.reltype))
        });
        for f in core_e {
            if f.tombstoned {
                m.delete_edge(
                    &f.src_label,
                    &f.src_key,
                    f.src_value.clone(),
                    &f.reltype,
                    &f.dst_label,
                    &f.dst_key,
                    f.dst_value.clone(),
                    Some(f.src_dense),
                    Some(f.dst_dense),
                );
            } else {
                // A live `born_edge_id`-less entry is an in-place core-edge property
                // patch, so it must carry the core edge id it was resolved against.
                let cid = f
                    .core_edge_id
                    .expect("a non-tombstoned core-edge entry carries its core edge id");
                m.patch_core_edge(
                    &f.src_label,
                    &f.src_key,
                    f.src_value.clone(),
                    &f.reltype,
                    &f.dst_label,
                    &f.dst_key,
                    f.dst_value.clone(),
                    Some(f.src_dense),
                    Some(f.dst_dense),
                    cid,
                    f.patches.clone(),
                );
            }
        }
        m
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
            w_opt_u64(&mut buf, e.core_edge);
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
            let core_edge = r_opt_u64(r)?;
            let (patches, tombstoned) = r_delta(r)?;
            edges.insert(
                ck,
                EdgeEntry {
                    identity: EdgeIdentity { src, reltype, dst },
                    src_dense,
                    dst_dense,
                    synthetic_edge,
                    core_edge,
                    delta: EdgeDelta {
                        patches,
                        tombstoned,
                    },
                },
            );
        }
        // Rebuild the core-edge patch index from the entries (a `core_edge` id → its key)
        // rather than serialising it — the entries are authoritative and the map derives
        // deterministically from them.
        let by_edge_id: HashMap<u64, Vec<u8>> = edges
            .iter()
            .filter_map(|(ck, e)| e.core_edge.map(|cid| (cid, ck.clone())))
            .collect();

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
            by_edge_id,
            synthetic_base,
            edge_synthetic_base,
            born,
            born_edges,
            bytes: bytes_est,
        })
    }
}

/// The newest-wins fold of one node identity across a run of L0 levels
/// ([`Memtable::merge_levels`]). Identity is carried as recoverable names + value so it
/// is interner-independent; `dense` is the stable core-or-synthetic id.
struct FoldedNode {
    label: String,
    key: String,
    value: Value,
    patches: BTreeMap<String, Value>,
    tombstoned: bool,
    dense: u64,
}

/// The newest-wins fold of one edge identity across a run of L0 levels. `born_edge_id`
/// is `Some` iff the edge was ever born (carries a synthetic edge id); when it is `None`
/// the entry overlays a **core** edge — a tombstone-only suppression, or an in-place
/// property patch (`core_edge_id` is `Some`), or both.
struct FoldedEdge {
    src_label: String,
    src_key: String,
    src_value: Value,
    reltype: String,
    dst_label: String,
    dst_key: String,
    dst_value: Value,
    patches: BTreeMap<String, Value>,
    tombstoned: bool,
    src_dense: u64,
    dst_dense: u64,
    born_edge_id: Option<u64>,
    core_edge_id: Option<u64>,
}

/// Fold one level's node/edge delta onto an accumulator newest-wins (the accumulator
/// is the *older* state, `src` the newer): a newer tombstone clears the patches and
/// tombstones; a newer upsert resurrects and its properties win per key. Matches
/// [`DeltaSnapshot::node_patch`]'s across-levels merge.
fn fold_delta(patches: &mut BTreeMap<String, Value>, tombstoned: &mut bool, src: &NodeDelta) {
    if src.tombstoned {
        patches.clear();
        *tombstoned = true;
    } else {
        *tombstoned = false;
        for (k, v) in &src.patches {
            patches.insert(k.clone(), v.clone());
        }
    }
}

/// Overload of [`fold_delta`] for an [`EdgeDelta`] (identical LWW semantics).
fn fold_delta_edge(patches: &mut BTreeMap<String, Value>, tombstoned: &mut bool, src: &EdgeDelta) {
    if src.tombstoned {
        patches.clear();
        *tombstoned = true;
    } else {
        *tombstoned = false;
        for (k, v) in &src.patches {
            patches.insert(k.clone(), v.clone());
        }
    }
}

/// An **interner-independent** identity key for a node: length-prefixed label + key
/// names followed by the type-exact value bytes. Two levels with different local symbol
/// tables produce the same key for the same business identity, so the fold combines
/// them correctly.
fn node_name_key(label: &str, key: &str, value: &Value) -> Vec<u8> {
    let mut b = Vec::with_capacity(label.len() + key.len() + 8);
    write_uvarint(&mut b, label.len() as u64);
    b.extend_from_slice(label.as_bytes());
    write_uvarint(&mut b, key.len() as u64);
    b.extend_from_slice(key.as_bytes());
    write_value(&mut b, value);
    b
}

/// An interner-independent identity key for an edge: `src ‖ reltype ‖ dst`.
#[allow(clippy::too_many_arguments)]
fn edge_name_key(
    src_label: &str,
    src_key: &str,
    src_value: &Value,
    reltype: &str,
    dst_label: &str,
    dst_key: &str,
    dst_value: &Value,
) -> Vec<u8> {
    let mut b = node_name_key(src_label, src_key, src_value);
    write_uvarint(&mut b, reltype.len() as u64);
    b.extend_from_slice(reltype.as_bytes());
    b.extend_from_slice(&node_name_key(dst_label, dst_key, dst_value));
    b
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

/// Whether `v` falls in `[lo, hi]` with per-bound inclusivity (a `None` bound is
/// unbounded on that side), by [`Value::cmp_key`] — the ISAM total order used by
/// `graph_format::isam`'s `lookup_range`. Shared by the delta-born and moved-core
/// range-index overlays.
fn value_in_range(
    v: &Value,
    lo: Option<&Value>,
    lo_inclusive: bool,
    hi: Option<&Value>,
    hi_inclusive: bool,
) -> bool {
    use std::cmp::Ordering;
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
/// The per-level read surface a [`DeltaSnapshot`] folds over. Each sealed delta level
/// answers these — a resident [`Memtable`] today, an off-heap paged segment tomorrow —
/// so the snapshot dispatches uniformly over `&dyn LevelRead` without caring where a
/// level's bytes live.
///
/// **Every return is owned.** An off-heap level serves a read from a decompressed block
/// that may be evicted the instant the call returns, so it can never hand back a borrow
/// into its storage. The resident [`Memtable`] impl clones on the two hot value-returning
/// accessors ([`node_patch_owned`](LevelRead::node_patch_owned),
/// [`edge_delta_owned`](LevelRead::edge_delta_owned)); the tombstone-suppression hot path
/// reads a single flag ([`node_tombstoned`](LevelRead::node_tombstoned)) and never clones a
/// patch set. `Send + Sync` so a published [`DeltaSnapshot`] stays shareable across tasks.
pub trait LevelRead: std::fmt::Debug + Send + Sync {
    fn is_empty(&self) -> bool;
    fn node_delta_count(&self) -> usize;
    fn edge_delta_count(&self) -> usize;
    fn synthetic_base(&self) -> u64;
    fn edge_synthetic_base(&self) -> u64;
    fn born_count(&self) -> u64;
    fn born_edge_count(&self) -> u64;
    /// Owned node delta by dense id (the snapshot merges owned deltas across levels).
    fn node_patch_owned(&self, dense_id: u64) -> Option<NodeDelta>;
    /// The tombstone flag alone for `dense_id`, or `None` if this level does not touch
    /// it — the hot suppression-filter path, so it never clones the patch set.
    fn node_tombstoned(&self, dense_id: u64) -> Option<bool>;
    /// Owned `(label, key, key-value)` business identity by dense id.
    fn node_identity_owned(&self, dense_id: u64) -> Option<(String, String, Value)>;
    fn born_ids_with_label(&self, label: &str) -> Vec<u64>;
    fn born_synthetic_for_identity(&self, label: &str, key: &str, value: &Value) -> Option<u64>;
    fn born_ids_in_index_eq(&self, label: &str, prop: &str, key: &Value) -> Vec<u64>;
    fn born_ids_in_index_range(
        &self,
        label: &str,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Vec<u64>;
    fn core_ids_patched_on_index(&self, label: &str, prop: &str) -> Vec<u64>;
    fn out_edges(&self, node: u64) -> Vec<DeltaEdge>;
    fn in_edges(&self, node: u64) -> Vec<DeltaEdge>;
    /// Owned edge delta by dense edge id.
    fn edge_delta_owned(&self, edge_id: u64) -> Option<EdgeDelta>;
}

/// The resident level: a [`Memtable`] answers every accessor from its in-RAM maps,
/// cloning only where the trait's owned contract requires it (the borrow-returning
/// inherent methods stay for the same-memtable fast paths and tests).
impl LevelRead for Memtable {
    fn is_empty(&self) -> bool {
        Memtable::is_empty(self)
    }
    fn node_delta_count(&self) -> usize {
        Memtable::node_delta_count(self)
    }
    fn edge_delta_count(&self) -> usize {
        Memtable::edge_delta_count(self)
    }
    fn synthetic_base(&self) -> u64 {
        Memtable::synthetic_base(self)
    }
    fn edge_synthetic_base(&self) -> u64 {
        Memtable::edge_synthetic_base(self)
    }
    fn born_count(&self) -> u64 {
        Memtable::born_count(self)
    }
    fn born_edge_count(&self) -> u64 {
        Memtable::born_edge_count(self)
    }
    fn node_patch_owned(&self, dense_id: u64) -> Option<NodeDelta> {
        self.node_patch(dense_id).cloned()
    }
    fn node_tombstoned(&self, dense_id: u64) -> Option<bool> {
        self.node_patch(dense_id).map(|nd| nd.tombstoned)
    }
    fn node_identity_owned(&self, dense_id: u64) -> Option<(String, String, Value)> {
        self.node_identity_by_dense(dense_id)
            .map(|(l, k, v)| (l.to_string(), k.to_string(), v.clone()))
    }
    fn born_ids_with_label(&self, label: &str) -> Vec<u64> {
        Memtable::born_ids_with_label(self, label)
    }
    fn born_synthetic_for_identity(&self, label: &str, key: &str, value: &Value) -> Option<u64> {
        Memtable::born_synthetic_for_identity(self, label, key, value)
    }
    fn born_ids_in_index_eq(&self, label: &str, prop: &str, key: &Value) -> Vec<u64> {
        Memtable::born_ids_in_index_eq(self, label, prop, key)
    }
    fn born_ids_in_index_range(
        &self,
        label: &str,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Vec<u64> {
        Memtable::born_ids_in_index_range(self, label, prop, lo, lo_inclusive, hi, hi_inclusive)
    }
    fn core_ids_patched_on_index(&self, label: &str, prop: &str) -> Vec<u64> {
        Memtable::core_ids_patched_on_index(self, label, prop)
    }
    fn out_edges(&self, node: u64) -> Vec<DeltaEdge> {
        Memtable::out_edges(self, node)
    }
    fn in_edges(&self, node: u64) -> Vec<DeltaEdge> {
        Memtable::in_edges(self, node)
    }
    fn edge_delta_owned(&self, edge_id: u64) -> Option<EdgeDelta> {
        self.edge_delta_by_id(edge_id).cloned()
    }
}

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
    /// Sealed, immutable L0 levels, **newest first**. Empty on the common no-flush path.
    /// Each is a `dyn LevelRead` — a resident [`Memtable`] today, an off-heap paged
    /// segment tomorrow — so the fold below dispatches without caring where its bytes live.
    l0: Vec<Arc<dyn LevelRead>>,
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
    pub fn with_levels(mem: Arc<Memtable>, l0: Vec<Arc<dyn LevelRead>>) -> Self {
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
    pub fn l0_levels(&self) -> &[Arc<dyn LevelRead>] {
        &self.l0
    }

    /// The delta levels in **newest-first** precedence order (active memtable, then the
    /// L0 segments newest→oldest) — first hit wins for tombstone/identity/edge-dedup
    /// precedence.
    fn levels_newest_first(&self) -> impl Iterator<Item = &dyn LevelRead> {
        std::iter::once(self.mem.as_ref() as &dyn LevelRead)
            .chain(self.l0.iter().map(|a| a.as_ref()))
    }

    /// The delta levels in **oldest-first** order (oldest L0 segment … active memtable)
    /// — the fold order for [`node_patch`](Self::node_patch) (newer property writes
    /// overlay older ones) and the emission order for born-id unions (their stacked
    /// synthetic-id ranges then come out ascending, matching the core scan order).
    fn levels_oldest_first(&self) -> impl Iterator<Item = &dyn LevelRead> {
        self.l0
            .iter()
            .rev()
            .map(|a| a.as_ref())
            .chain(std::iter::once(self.mem.as_ref() as &dyn LevelRead))
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
            let Some(nd) = level.node_patch_owned(dense_id) else {
                continue;
            };
            match &mut acc {
                None => acc = Some(nd),
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
                        for (k, v) in nd.patches {
                            a.patches.insert(k, v);
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
            if let Some(t) = level.node_tombstoned(dense_id) {
                return t;
            }
        }
        false
    }

    /// Count of delta-born-or-patched node identities, for scan-range planning. Summed
    /// across levels — an over-estimate when a core node is patched in several levels,
    /// which is fine for a planning bound.
    pub fn node_delta_count(&self) -> usize {
        self.levels_newest_first()
            .map(LevelRead::node_delta_count)
            .sum()
    }

    /// Count of edge identities carrying a delta across all levels (summed; an
    /// over-estimate when the same edge is touched in several levels — fine as a
    /// planning/threshold magnitude). Pairs with [`Self::node_delta_count`].
    pub fn edge_delta_count(&self) -> usize {
        self.levels_newest_first()
            .map(LevelRead::edge_delta_count)
            .sum()
    }

    /// The base of the synthetic dense-id space (the core `node_count` this delta was
    /// opened against): an id `>= synthetic_base` is a delta-born node whose reads
    /// route to the delta only, never a core block. It is the **min** across levels
    /// (older levels have lower bases; the oldest is the core count).
    #[inline]
    pub fn synthetic_base(&self) -> u64 {
        self.levels_newest_first()
            .map(LevelRead::synthetic_base)
            .min()
            .unwrap_or(0)
    }

    /// The number of delta-born nodes overlaid — the merged `node_count` is
    /// `core.node_count() + born_count()`. Summed across levels (born-id ranges are
    /// disjoint and stacked past the core count).
    #[inline]
    pub fn born_count(&self) -> u64 {
        self.levels_newest_first().map(LevelRead::born_count).sum()
    }

    /// Recover a node's `(label, key, key-value)` business identity by dense id — the
    /// material a delta-born node's label + business-key property are read from. The
    /// newest level touching the id answers (a synthetic id lives in exactly one; a
    /// core id's identity is level-invariant).
    #[inline]
    pub fn node_identity_by_dense(&self, dense_id: u64) -> Option<(String, String, Value)> {
        self.levels_newest_first()
            .find_map(|m| m.node_identity_owned(dense_id))
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

    /// The synthetic dense id of a delta-born node with this business identity, resolved
    /// across **every** level (active memtable, then L0 newest→oldest). A born identity
    /// is allocated in exactly one level and its synthetic id is stable across a flush,
    /// so the first level that carries it decides. The DELETE write path uses this to
    /// tombstone a born node by its business key; passing the resolved id as the
    /// delete's dense-id context plants the tombstone's `by_dense` mapping so a node
    /// already **flushed** to L0 (whose live entry sits in an L0 level, not the active
    /// tombstone) is still suppressed on read. Distinct from the create-side MERGE reuse
    /// path, which consults only the L0 levels (the active memtable's `upsert_node`
    /// idempotency covers its own born nodes there).
    pub fn born_synthetic_for_identity(
        &self,
        label: &str,
        key: &str,
        value: &Value,
    ) -> Option<u64> {
        self.levels_newest_first()
            .find_map(|m| m.born_synthetic_for_identity(label, key, value))
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

    // ── Moved-indexed-value overlay ────────────────────────────────────────────
    // A *core* node whose indexed property is patched keeps its **original** value in
    // the core ISAM, so a range seek reads a stale membership: it is found at the old
    // value and missed at the new one. These accessors relocate it — a seek drops a
    // core hit whose patched value moved out of the predicate (`core_hit_survives_*`)
    // and adds a core node whose patched value moved into it (`moved_core_ids_in_index_*`).
    // The value *read back* is already corrected by the property overlay; this fixes
    // only index *membership*. Born nodes are covered by `born_ids_in_index_*`; these
    // deal exclusively with core dense ids (`< synthetic_base`).

    /// The **merged** patched value a core node presents for the indexed property
    /// `prop` (newest level wins per property), or `None` if it is not patched on
    /// `prop` (its core ISAM value stands) or is tombstoned (suppressed separately).
    fn patched_index_value(&self, dense: u64, prop: &str) -> Option<Value> {
        self.node_patch(dense)
            .filter(|nd| !nd.tombstoned)
            .and_then(|nd| nd.patches.get(prop).cloned())
    }

    /// Whether a core ISAM hit at an **equality** seek on `prop` survives the overlay.
    /// A node whose patched value for `prop` moved *away* from `key` is dropped (the
    /// ISAM still lists it at its stale original value); an unpatched hit always survives.
    pub fn core_hit_survives_eq(&self, dense: u64, prop: &str, key: &Value) -> bool {
        match self.patched_index_value(dense, prop) {
            Some(v) => v.cmp_key(key) == std::cmp::Ordering::Equal,
            None => true,
        }
    }

    /// Whether a core ISAM hit at a **range** seek on `prop` survives the overlay — its
    /// patched value stays within `[lo, hi]`. An unpatched hit always survives.
    #[allow(clippy::too_many_arguments)]
    pub fn core_hit_survives_range(
        &self,
        dense: u64,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> bool {
        match self.patched_index_value(dense, prop) {
            Some(v) => value_in_range(&v, lo, lo_inclusive, hi, hi_inclusive),
            None => true,
        }
    }

    /// Core dense ids whose **patched** indexed value for `prop` satisfies `pred` —
    /// nodes relocated *into* a range seek by a property patch (the core ISAM lists them
    /// at their old value). Candidates are unioned across levels (patched on `prop`,
    /// carrying `label`) and each is judged by its *merged* (newest-wins) value, so a
    /// node patched across levels is decided once. Ascending (`BTreeSet`).
    fn moved_core_ids_in_index(
        &self,
        label: &str,
        prop: &str,
        pred: impl Fn(&Value) -> bool,
    ) -> Vec<u64> {
        let mut cand = std::collections::BTreeSet::new();
        for level in self.levels_newest_first() {
            cand.extend(level.core_ids_patched_on_index(label, prop));
        }
        cand.into_iter()
            .filter(|&d| self.patched_index_value(d, prop).is_some_and(|v| pred(&v)))
            .collect()
    }

    /// Core nodes relocated *into* an **equality** seek: patched indexed value == `key`.
    #[inline]
    pub fn moved_core_ids_in_index_eq(&self, label: &str, prop: &str, key: &Value) -> Vec<u64> {
        self.moved_core_ids_in_index(label, prop, |v| v.cmp_key(key) == std::cmp::Ordering::Equal)
    }

    /// Core nodes relocated *into* a **range** seek: patched indexed value in `[lo, hi]`.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn moved_core_ids_in_index_range(
        &self,
        label: &str,
        prop: &str,
        lo: Option<&Value>,
        lo_inclusive: bool,
        hi: Option<&Value>,
        hi_inclusive: bool,
    ) -> Vec<u64> {
        self.moved_core_ids_in_index(label, prop, |v| {
            value_in_range(v, lo, lo_inclusive, hi, hi_inclusive)
        })
    }

    /// The base of the synthetic edge dense-id space (the core `edge_count` this delta
    /// was opened against): an edge id `>= edge_synthetic_base` is a delta-born edge
    /// whose `rel_record` routes to the delta, never a core edge block (Phase 3). The
    /// **min** across levels (= the core count), mirroring [`synthetic_base`](Self::synthetic_base).
    #[inline]
    pub fn edge_synthetic_base(&self) -> u64 {
        self.levels_newest_first()
            .map(LevelRead::edge_synthetic_base)
            .min()
            .unwrap_or(0)
    }

    /// The number of delta-born edges overlaid — the merged `edge_count` is
    /// `core.edge_count() + born_edge_count()`. Summed across levels.
    #[inline]
    pub fn born_edge_count(&self) -> u64 {
        self.levels_newest_first()
            .map(LevelRead::born_edge_count)
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
        let read = |m: &dyn LevelRead| {
            if outgoing {
                m.out_edges(node)
            } else {
                m.in_edges(node)
            }
        };
        if self.l0.is_empty() {
            return read(self.mem.as_ref());
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

    // ── Edge-property overlay ──────────────────────────────────────────────────
    // Two cases, both resolved by dense edge id:
    //   * A **delta-born** edge (`MERGE (a)-[r:R]->(b) SET r.p = …`, id `>= core
    //     edge_count`) carries its properties in the delta, not in any core edge-props
    //     record. Its id is owned by exactly one level (born-id ranges are disjoint), so
    //     the fold below reduces to that single level.
    //   * A **core** edge (id `< core edge_count`) patched in place by `SET r.p` on an
    //     existing edge. Its patches may be split across several L0 levels (patch, flush,
    //     patch again), so they fold newest-wins per property, and the read overlay lays
    //     the result over the core edge's stored properties.
    // A tombstoned edge is suppressed on traversal, so its (cleared) properties are
    // normally never asked for; it reads as empty here defensively.

    /// The value the delta presents for property `prop` of the edge with dense id
    /// `edge_id` (born or patched-core), or `None` if the delta carries no such patch —
    /// in which case the caller falls back to the core edge-props record. Folded
    /// newest-first: the newest level that patches `prop` wins, and a newer tombstone
    /// stops the search (the edge is deleted, so no older patch survives).
    pub fn edge_patch_value(&self, edge_id: u64, prop: &str) -> Option<Value> {
        for m in self.levels_newest_first() {
            let Some(d) = m.edge_delta_owned(edge_id) else {
                continue;
            };
            if d.tombstoned {
                return None; // the newest touch is a delete
            }
            if let Some(v) = d.patches.get(prop) {
                return Some(v.clone());
            }
            // This level touches the edge but not this property — keep looking older.
        }
        None
    }

    /// Every property patch of the edge with dense id `edge_id` (name → value), folded
    /// newest-wins across levels. Empty if it carries no properties or is tombstoned.
    /// Used to materialise a born edge's full property set (`RETURN r`), to overlay a
    /// patched core edge's full record, and to carry either through consolidation.
    pub fn edge_patches(&self, edge_id: u64) -> BTreeMap<String, Value> {
        let mut merged: BTreeMap<String, Value> = BTreeMap::new();
        for m in self.levels_oldest_first() {
            let Some(d) = m.edge_delta_owned(edge_id) else {
                continue;
            };
            if d.tombstoned {
                // A newer delete clears everything patched below it.
                merged.clear();
            } else {
                for (k, v) in &d.patches {
                    merged.insert(k.clone(), v.clone());
                }
            }
        }
        merged
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
                edge_id: None,
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
        DeltaSnapshot::with_levels(
            Arc::new(mem),
            l0.into_iter()
                .map(|m| Arc::new(m) as Arc<dyn LevelRead>)
                .collect(),
        )
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
            Some((
                "Person".to_string(),
                "name".to_string(),
                Value::Str("Ann".into())
            ))
        );
        assert_eq!(
            s.node_identity_by_dense(101),
            Some((
                "Person".to_string(),
                "name".to_string(),
                Value::Str("Bob".into())
            ))
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
    fn moved_indexed_value_relocates_a_patched_core_node() {
        // A *core* node (dense id 5, core has 10 nodes) whose indexed `age` is patched to
        // 99. The overlay must relocate it: found at the new value, dropped at the old.
        let mut mem = Memtable::with_synthetic_base(10);
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Alice".into()),
            Some(5),
            [("age".to_string(), Value::Int(99))],
        );
        let s = snap(mem, vec![]);

        // It is a candidate patched on `age`, and moves *into* an eq/range seek at 99.
        assert_eq!(
            s.moved_core_ids_in_index_eq("Person", "age", &Value::Int(99)),
            vec![5]
        );
        assert!(s
            .moved_core_ids_in_index_eq("Person", "age", &Value::Int(30))
            .is_empty());
        assert_eq!(
            s.moved_core_ids_in_index_range(
                "Person",
                "age",
                Some(&Value::Int(50)),
                true,
                None,
                true
            ),
            vec![5]
        );

        // A hit survives an eq/range seek only at its patched value; a stale hit moves out.
        assert!(s.core_hit_survives_eq(5, "age", &Value::Int(99)));
        assert!(!s.core_hit_survives_eq(5, "age", &Value::Int(30)));
        assert!(s.core_hit_survives_range(5, "age", Some(&Value::Int(50)), true, None, true));
        assert!(!s.core_hit_survives_range(5, "age", None, true, Some(&Value::Int(50)), true));

        // An unpatched core hit (a different id) always survives and is not a candidate.
        assert!(s.core_hit_survives_eq(6, "age", &Value::Int(30)));
        // A different label does not match.
        assert!(s
            .moved_core_ids_in_index_eq("Company", "age", &Value::Int(99))
            .is_empty());
        // A different (unpatched) property is not relocated.
        assert!(s
            .moved_core_ids_in_index_eq("Person", "salary", &Value::Int(99))
            .is_empty());
    }

    #[test]
    fn moved_indexed_value_uses_the_merged_value_across_levels() {
        // Core node 5 patched `age=30` in an older L0 level, then `age=99` in the newer
        // active memtable. The overlay must judge it by the *merged* (newest) value 99.
        let mut older = Memtable::with_synthetic_base(10);
        older.upsert_node(
            "Person",
            "name",
            Value::Str("Alice".into()),
            Some(5),
            [("age".to_string(), Value::Int(30))],
        );
        let mut newer = Memtable::with_synthetic_base(10);
        newer.upsert_node(
            "Person",
            "name",
            Value::Str("Alice".into()),
            Some(5),
            [("age".to_string(), Value::Int(99))],
        );
        let s = snap(newer, vec![older]);

        // Moves into a seek at the newest value 99, not the shadowed 30.
        assert_eq!(
            s.moved_core_ids_in_index_eq("Person", "age", &Value::Int(99)),
            vec![5]
        );
        assert!(s
            .moved_core_ids_in_index_eq("Person", "age", &Value::Int(30))
            .is_empty());
        assert!(s.core_hit_survives_eq(5, "age", &Value::Int(99)));
        assert!(!s.core_hit_survives_eq(5, "age", &Value::Int(30)));
    }

    /// Helper: MERGE the born edge Ann-KNOWS->Bob (endpoints core nodes 0/1) with `patches`.
    fn merge_ann_knows_bob(m: &mut Memtable, patches: Vec<(String, Value)>) {
        m.upsert_edge(
            "Person",
            "name",
            Value::Str("Ann".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Bob".into()),
            Some(0),
            Some(1),
            patches,
        );
    }

    #[test]
    fn edge_properties_read_back_through_the_overlay() {
        // Core has 10 nodes / 5 edges; a born edge takes edge id `edge_synthetic_base` = 5.
        let mut mem = Memtable::with_bases(10, 5);
        merge_ann_knows_bob(&mut mem, vec![("since".into(), Value::Int(2020))]);
        let s = snap(mem, vec![]);

        assert_eq!(s.edge_patch_value(5, "since"), Some(Value::Int(2020)));
        assert_eq!(s.edge_patch_value(5, "weight"), None);
        assert_eq!(s.edge_patches(5).len(), 1);
        assert_eq!(s.edge_patches(5).get("since"), Some(&Value::Int(2020)));
        // A core edge id (`< base`) carries no delta patches.
        assert_eq!(s.edge_patch_value(4, "since"), None);
        assert!(s.edge_patches(4).is_empty());
    }

    #[test]
    fn edge_properties_patch_then_tombstone() {
        let mut mem = Memtable::with_bases(10, 5);
        merge_ann_knows_bob(&mut mem, vec![("since".into(), Value::Int(2020))]);
        // Re-MERGE with new values patches the same born edge in place (idempotent id).
        merge_ann_knows_bob(
            &mut mem,
            vec![
                ("since".into(), Value::Int(2021)),
                ("weight".into(), Value::Int(3)),
            ],
        );
        let s = snap(mem.clone(), vec![]);
        assert_eq!(s.edge_patch_value(5, "since"), Some(Value::Int(2021)));
        assert_eq!(s.edge_patch_value(5, "weight"), Some(Value::Int(3)));

        // Deleting the edge clears its properties (and suppresses it on traversal).
        mem.delete_edge(
            "Person",
            "name",
            Value::Str("Ann".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Bob".into()),
            Some(0),
            Some(1),
        );
        let s = snap(mem, vec![]);
        assert!(
            s.edge_patches(5).is_empty(),
            "a tombstoned edge reads no props"
        );
        assert_eq!(s.edge_patch_value(5, "since"), None);
    }

    #[test]
    fn edge_properties_resolve_from_the_owning_level() {
        // The born edge (id 5) lives in an older L0 level; the active memtable is empty.
        // `edge_patch_value`/`edge_patches` must find the owning level.
        let mut older = Memtable::with_bases(10, 5);
        merge_ann_knows_bob(&mut older, vec![("since".into(), Value::Int(2020))]);
        let s = snap(Memtable::with_bases(10, 6), vec![older]);
        assert_eq!(s.edge_patch_value(5, "since"), Some(Value::Int(2020)));
        assert_eq!(s.edge_patches(5).get("since"), Some(&Value::Int(2020)));
    }

    /// Patch the **core** edge with id 3 (base 5, so `3 < base` is a core edge).
    fn patch_core_ann_knows_bob(
        m: &mut Memtable,
        core_edge_id: u64,
        patches: Vec<(String, Value)>,
    ) {
        m.patch_core_edge(
            "Person",
            "name",
            Value::Str("Ann".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Bob".into()),
            Some(0),
            Some(1),
            core_edge_id,
            patches,
        );
    }

    #[test]
    fn core_edge_patch_reads_back_and_folds_across_levels() {
        // A single level: patch core edge id 3, read it back; an unpatched core edge (id
        // 2) carries no delta (the reader falls back to the core value).
        let mut mem = Memtable::with_bases(10, 5);
        patch_core_ann_knows_bob(&mut mem, 3, vec![("since".into(), Value::Int(2020))]);
        let s = snap(mem.clone(), vec![]);
        assert_eq!(s.edge_patch_value(3, "since"), Some(Value::Int(2020)));
        assert_eq!(s.edge_patch_value(3, "weight"), None);
        assert_eq!(s.edge_patches(3).get("since"), Some(&Value::Int(2020)));
        assert_eq!(s.edge_patch_value(2, "since"), None, "unpatched core edge");
        assert!(s.edge_patches(2).is_empty());

        // Re-patch the same core edge id in place (idempotent — no new entry, LWW props).
        patch_core_ann_knows_bob(
            &mut mem,
            3,
            vec![
                ("since".into(), Value::Int(2021)),
                ("weight".into(), Value::Int(5)),
            ],
        );
        assert_eq!(mem.edge_delta_count(), 1, "re-patch reuses the one entry");
        let s = snap(mem, vec![]);
        assert_eq!(s.edge_patch_value(3, "since"), Some(Value::Int(2021)));
        assert_eq!(s.edge_patch_value(3, "weight"), Some(Value::Int(5)));

        // Two levels: an older patch {since, note} under a newer patch {since}. The
        // properties fold per-key newest-wins (2021 beats 2020; `note` survives).
        let mut older = Memtable::with_bases(10, 5);
        patch_core_ann_knows_bob(
            &mut older,
            3,
            vec![
                ("since".into(), Value::Int(2020)),
                ("note".into(), Value::Str("hi".into())),
            ],
        );
        let mut newer = Memtable::with_bases(10, 5);
        patch_core_ann_knows_bob(&mut newer, 3, vec![("since".into(), Value::Int(2021))]);
        let s = snap(newer, vec![older]);
        assert_eq!(s.edge_patch_value(3, "since"), Some(Value::Int(2021)));
        assert_eq!(
            s.edge_patch_value(3, "note"),
            Some(Value::Str("hi".into())),
            "an older-level property the newer level did not touch survives"
        );
        let all = s.edge_patches(3);
        assert_eq!(all.get("since"), Some(&Value::Int(2021)));
        assert_eq!(all.get("note"), Some(&Value::Str("hi".into())));
    }

    #[test]
    fn core_edge_patch_survives_serialise_roundtrip() {
        // The `by_edge_id` index is rebuilt from the persisted `core_edge` field, so a
        // patched core edge reads identically after a serialise → deserialise round-trip.
        let mut mem = Memtable::with_bases(10, 5);
        patch_core_ann_knows_bob(
            &mut mem,
            3,
            vec![
                ("since".into(), Value::Int(2020)),
                ("weight".into(), Value::Int(9)),
            ],
        );
        let back = Memtable::deserialise(&mem.serialise()).unwrap();
        let s = snap(back, vec![]);
        assert_eq!(s.edge_patch_value(3, "since"), Some(Value::Int(2020)));
        assert_eq!(s.edge_patch_value(3, "weight"), Some(Value::Int(9)));
        assert_eq!(s.edge_patches(3).len(), 2);
    }

    #[test]
    fn merge_levels_preserves_a_core_edge_patch() {
        // Two stacked levels each patch core edge id 3; `merge_levels` folds them into one
        // level that reads identically to the multi-level snapshot fold.
        let mut older = Memtable::with_bases(10, 5);
        patch_core_ann_knows_bob(
            &mut older,
            3,
            vec![
                ("since".into(), Value::Int(2020)),
                ("note".into(), Value::Str("hi".into())),
            ],
        );
        let mut newer = Memtable::with_bases(10, 5);
        patch_core_ann_knows_bob(&mut newer, 3, vec![("since".into(), Value::Int(2021))]);

        let merged = Memtable::merge_levels(&[&newer, &older]);
        let via_merge = snap(merged, vec![]);
        let via_fold = snap(newer, vec![older]);
        assert_eq!(
            via_merge.edge_patches(3),
            via_fold.edge_patches(3),
            "merge_levels matches the snapshot fold for a patched core edge"
        );
        assert_eq!(
            via_merge.edge_patch_value(3, "since"),
            Some(Value::Int(2021))
        );
        assert_eq!(
            via_merge.edge_patch_value(3, "note"),
            Some(Value::Str("hi".into()))
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

    /// A three-level stacked run — a core patch that is re-patched then deleted, a core
    /// tombstone, born nodes across all levels, a born node re-MERGE'd in a newer level,
    /// and a born edge later deleted — exercising every fold path.
    fn stacked_run() -> Vec<Memtable> {
        // Oldest (L0_0): base nodes 100, edges 500.
        let mut l0 = Memtable::with_bases(100, 500);
        l0.upsert_node(
            "Person",
            "name",
            Value::Str("Al".into()),
            Some(5),
            [("age".into(), Value::Int(40))],
        );
        l0.delete_node("Person", "name", Value::Str("Bob".into()), Some(7));
        l0.upsert_node(
            "Person",
            "name",
            Value::Str("Ann".into()),
            None,
            [("age".into(), Value::Int(1))],
        ); // 100
        l0.upsert_node("Company", "ticker", Value::Str("BBB".into()), None, []); // 101
        l0.upsert_edge(
            "Person",
            "name",
            Value::Str("Ann".into()),
            "KNOWS",
            "Company",
            "ticker",
            Value::Str("BBB".into()),
            Some(100),
            Some(101),
            [],
        ); // edge 500

        // Middle (L0_1): base nodes 102, edges 501.
        let mut l1 = Memtable::with_bases(102, 501);
        l1.upsert_node(
            "Person",
            "name",
            Value::Str("Al".into()),
            Some(5),
            [
                ("age".into(), Value::Int(41)),
                ("city".into(), Value::Str("NYC".into())),
            ],
        );
        l1.upsert_node(
            "Person",
            "name",
            Value::Str("Ann".into()),
            Some(100),
            [("age".into(), Value::Int(2))],
        ); // re-MERGE born A
        l1.upsert_node("Person", "name", Value::Str("Cy".into()), None, []); // 102
        l1.delete_edge(
            "Person",
            "name",
            Value::Str("Ann".into()),
            "KNOWS",
            "Company",
            "ticker",
            Value::Str("BBB".into()),
            Some(100),
            Some(101),
        ); // tombstone the core-of-this-run edge 500
        l1.upsert_edge(
            "Person",
            "name",
            Value::Str("Cy".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Ann".into()),
            Some(102),
            Some(100),
            [],
        ); // edge 501

        // Newest (L0_2): base nodes 103, edges 502.
        let mut l2 = Memtable::with_bases(103, 502);
        l2.delete_node("Person", "name", Value::Str("Al".into()), Some(5)); // tombstone core 5
        l2.upsert_node(
            "Company",
            "ticker",
            Value::Str("DDD".into()),
            None,
            [("rank".into(), Value::Int(9))],
        ); // 103

        vec![l2, l1, l0] // newest-first
    }

    /// Normalise a delta-edge list to its **observable** form for equivalence checks:
    /// a tombstoned edge's `edge_id` is never materialised (it only suppresses a core
    /// edge by `(reltype, neighbour)`), so it is masked to `None` — the one field where
    /// a born-then-deleted edge's merged canonical form legitimately differs from the
    /// multi-level stack's shadowing tombstone. Result is sorted for order-independence.
    fn norm_edges(v: Vec<DeltaEdge>) -> Vec<(String, u64, bool, Option<u64>)> {
        let mut out: Vec<(String, u64, bool, Option<u64>)> = v
            .into_iter()
            .map(|e| {
                let id = if e.tombstoned { None } else { e.edge_id };
                (e.reltype, e.other, e.tombstoned, id)
            })
            .collect();
        out.sort();
        out
    }

    #[test]
    fn merge_levels_matches_the_snapshot_fold() {
        let run = stacked_run();
        let refs: Vec<&Memtable> = run.iter().collect();
        let merged = Memtable::merge_levels(&refs);

        let stack = DeltaSnapshot::with_levels(
            Arc::new(run[0].clone()),
            run[1..]
                .iter()
                .map(|m| Arc::new(m.clone()) as Arc<dyn LevelRead>)
                .collect(),
        );
        let merged_snap = DeltaSnapshot::from_memtable(Arc::new(merged));

        // Bases + counts.
        assert_eq!(stack.synthetic_base(), merged_snap.synthetic_base());
        assert_eq!(
            stack.edge_synthetic_base(),
            merged_snap.edge_synthetic_base()
        );
        assert_eq!(stack.born_count(), merged_snap.born_count());
        assert_eq!(stack.born_edge_count(), merged_snap.born_edge_count());

        let hi = stack.synthetic_base() + stack.born_count();
        for id in 0..hi {
            assert_eq!(
                stack.node_patch(id),
                merged_snap.node_patch(id),
                "node_patch({id})"
            );
            assert_eq!(
                stack.is_tombstoned(id),
                merged_snap.is_tombstoned(id),
                "is_tombstoned({id})"
            );
            assert_eq!(
                norm_edges(stack.out_edges(id)),
                norm_edges(merged_snap.out_edges(id)),
                "out_edges({id})"
            );
            assert_eq!(
                norm_edges(stack.in_edges(id)),
                norm_edges(merged_snap.in_edges(id)),
                "in_edges({id})"
            );
        }
        for lbl in ["Person", "Company"] {
            assert_eq!(
                stack.born_ids_with_label(lbl),
                merged_snap.born_ids_with_label(lbl),
                "born_ids_with_label({lbl})"
            );
        }

        // Concrete spot-checks of the headline folds.
        assert!(
            merged_snap.is_tombstoned(5),
            "core 5 deleted in the newest level"
        );
        assert!(
            merged_snap.is_tombstoned(7),
            "core 7 deleted in the oldest level"
        );
        assert_eq!(
            merged_snap.node_patch(100).unwrap().patches.get("age"),
            Some(&Value::Int(2)),
            "born A's newer age wins"
        );
        assert_eq!(merged_snap.born_ids_with_label("Company"), vec![101, 103]);
    }

    #[test]
    fn merge_levels_is_deterministic() {
        let run = stacked_run();
        let refs: Vec<&Memtable> = run.iter().collect();
        assert_eq!(
            Memtable::merge_levels(&refs).serialise(),
            Memtable::merge_levels(&refs).serialise(),
            "equal runs merge to byte-identical segments"
        );
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
