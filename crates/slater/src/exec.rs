// SPDX-License-Identifier: Apache-2.0
//! Volcano-style executor: an AST [`Query`] → result rows, pulled from the
//! immutable [`Generation`](crate::generation::Generation) through the
//! decompressed-block [`BlockCache`].
//!
//! The executor is the consumer of everything M4 built before it: it scans
//! candidate nodes via the [`plan`](crate::plan) module's chosen strategy (range
//! index / label postings / full scan), traverses the CSR for relationship
//! patterns (fixed and variable length, with type alternation and direction),
//! filters and projects with a full expression evaluator over [`Value`], and folds
//! `WITH`/aggregation/`ORDER BY`/`SKIP`/`LIMIT`/`DISTINCT`/`UNION`/map projection on
//! top.
//!
//! Records are read **through the block cache** (D18): each typed reader exposes
//! its underlying `BlockFileReader` and a public record decoder, so the executor
//! routes every node/edge/topology read through `BlockCache::record` and slices the
//! record out of a (possibly already-resident) decompressed block — no second
//! decompress, hot blocks stay warm across a query and across connections.
//!
//! Memory stays flat in the graph's size: candidate generation is the only place a
//! large id set is materialised, and that is bounded by index/label selectivity;
//! the per-row state is a handful of bound ids, and block residency is capped by
//! the cache budget. Result size is bounded by `max_rows`, traversal time by an
//! optional wall-clock deadline, and intermediate collections (comprehensions,
//! `UNWIND`, concatenation, aggregate buffers, varlen paths) by an optional
//! query-wide element budget (`query.maxIntermediate`).
//
// The tokio Bolt listener that drives this (decoding RUN/PULL and PackStream-
// encoding the rows) is the next M4 increment; allow dead_code until it lands.
#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Result};

use crate::algo;
use crate::cache::{BlockCache, FileKind, VectorIndexCache};
use crate::generation::RelEndpointSide;
use crate::parser::ast::*;
use crate::plan::{choose_node_scan, index_for, is_id_anchored, maybe_rel_type_scan, NodeScan};
use crate::read_view::ReadView;
use crate::rwindex::{
    self, DeltaVector, EnsureCtx, RwIndexCache, RwIndexConfig, RwLookup, SharedIndex,
    TouchedJournal,
};
use crate::temporal::{self, TKind};
use crate::vector;
use graph_format::blockfile::BlockFileReader;
use graph_format::ids::{EdgeId, NodeId, Value};
use graph_format::manifest::{AnnMode, AnnNav, EntityKind, VectorIndexDesc};
use graph_format::postings::EndpointPostingIter;
use graph_format::pq::{AdcTable, ResidentPq};
use graph_format::segvamana::SegmentVamanaIndex;
use graph_format::vamana::{self, beam_search};
use graph_format::vectors::{self, VectorEntry};
use graph_format::{columns, nodelabels, topology};
use rayon::prelude::*;
use slater_scalar::ArithmeticOverflow;

// The `Engine` executor's methods live in these child modules, split out of this
// file by concern (each is a `use super::*` + one `impl Engine` block; a pure
// relocation). The struct, its fields, and the shared free helpers stay here.
mod access;
mod driver;
mod eval;
mod knn;
mod matchclause;
mod proc;
mod project;
mod scan;
#[cfg(test)]
mod tests;
mod traverse;

/// Unbounded variable-length expansion (`*` / `*n..`) is capped at this many hops,
/// so a runaway traversal on a densely connected graph cannot blow up. Explicit
/// upper bounds (`*1..3`) are honoured exactly; only the open-ended case is capped.
const MAX_VARLEN_HOPS: u32 = 15;

/// A GQL quantified path group `((…)){m,n}` is desugared into the union of its
/// fixed-length expansions (one ordinary pattern per repetition count). This caps
/// the total hops a single group may unroll to, so `{1,1000}` over a multi-hop
/// inner pattern can't generate an enormous pattern set.
const QUANT_MAX_UNROLL: usize = 32;

/// User-supplied regex patterns (`=~`, `string.matchRegEx`, `string.replaceRegEx`)
/// are rejected past this many bytes: no legitimate query pattern approaches 1 KiB,
/// and the cap bounds compile time before the size limits below even apply.
const MAX_REGEX_PATTERN_BYTES: usize = 1024;

/// Compiled-NFA size cap (the regex crate default is 10 MiB). Bounds both the
/// compile cost and the per-match cost of pathological patterns like nested
/// bounded repetitions (`(a{100}){100}…`).
const REGEX_SIZE_LIMIT: usize = 1 << 20;

/// Lazy-DFA cache cap per compiled regex (crate default 2 MiB).
const REGEX_DFA_SIZE_LIMIT: usize = 1 << 20;

/// Distinct patterns cached per query; past this, patterns still compile (bounded
/// by the limits above) but are not retained. The cache exists to kill the
/// per-row recompile of a constant pattern, so one entry is the common case.
const REGEX_CACHE_MAX: usize = 64;

/// One fully-resolved traversal step: the edge id, the neighbour reached, the
/// relationship type, and the edge's *stored* direction (`start`→`end`, which is
/// the true src→dst regardless of the direction the pattern walked it in). Carried
/// so a bound relationship can be materialised as a Bolt `Relationship` with
/// correct endpoints and type without a second lookup.
#[derive(Clone)]
pub(crate) struct Hop {
    edge: u64,
    neighbour: u64,
    reltype: u32,
    start: u64,
    end: u64,
}

/// A relationship-type constraint resolved once before a traversal's per-edge loop
/// (see [`Engine::expand_one_hop`]). The common positive shapes pre-resolve to a flat
/// reltype-id set so the hot loop is a plain integer membership test; only a boolean
/// type expression (`&`/`!`) carries the AST through for per-edge evaluation.
enum TypeFilter<'a> {
    AnyOf(Vec<u32>),
    Expr(&'a LabelExpr),
}

impl Hop {
    /// The runtime relationship value this hop binds.
    fn as_rel(&self) -> Val {
        Val::Rel {
            id: self.edge,
            start: self.start,
            end: self.end,
            reltype: self.reltype,
        }
    }
}

/// One in-flight branch of the parallel chain walk ([`Engine::expand_chain_par`]):
/// the node reached so far, the bindings accumulated along the way (a shared
/// [`Frame`] — the sequential walk's mutate-in-place map can't be shared across a
/// live breadth of branches, but an `Arc`-linked frame can, so a branch that binds
/// no new variable just bumps the parent's refcount instead of cloning the map),
/// and the path hops (tracked only when the pattern binds a path variable, else
/// left empty to avoid the clone).
pub(crate) struct ChainBranch {
    cur: u64,
    binding: std::sync::Arc<Frame>,
    walk: Vec<Hop>,
}

/// An immutable, structurally-shared variable→value scope for the parallel chain
/// walk. Each hop that binds a variable layers a small `delta` over an `Arc` to its
/// parent, so sibling branches that share a prefix share that prefix's storage
/// (O(1) `Arc` clone instead of copying the whole inherited map per neighbour). A
/// hop that binds nothing reuses the parent frame outright. The owned
/// `HashMap<String, Val>` every consumer downstream of the walk expects is produced
/// once, at the leaf, by [`Frame::flatten`].
pub(crate) struct Frame {
    parent: Option<std::sync::Arc<Frame>>,
    /// Variables bound *at this layer* (≤ 2 in the walk: a rel var and a next-node
    /// var). Searched last-first so a later write shadows an earlier one in-layer.
    delta: Vec<(Box<str>, Val)>,
}

impl Frame {
    /// A root frame wrapping the anchor's inherited binding (one allocation per
    /// anchor; thereafter the walk only layers small deltas).
    fn root(base: &HashMap<String, Val>) -> std::sync::Arc<Frame> {
        std::sync::Arc::new(Frame {
            parent: None,
            delta: base
                .iter()
                .map(|(k, v)| (k.as_str().into(), v.clone()))
                .collect(),
        })
    }

    /// The value bound to `name`, searching this layer (last write wins) before
    /// recursing to the parent so a child frame shadows its parent — the same
    /// precedence [`Frame::flatten`] reproduces.
    fn get(&self, name: &str) -> Option<&Val> {
        self.delta
            .iter()
            .rev()
            .find(|(k, _)| &**k == name)
            .map(|(_, v)| v)
            .or_else(|| self.parent.as_ref().and_then(|p| p.get(name)))
    }

    /// Materialise the whole chain into an owned map. Root-first so children
    /// overwrite parents on a name clash — matching [`Scope::collect_into`] and the
    /// sequential walk's LIFO restore (deepest binding wins).
    fn flatten(&self) -> HashMap<String, Val> {
        let mut out = HashMap::new();
        self.collect_into(&mut out);
        out
    }

    fn collect_into(&self, out: &mut HashMap<String, Val>) {
        if let Some(p) = &self.parent {
            p.collect_into(out);
        }
        for (k, v) in &self.delta {
            out.insert(k.to_string(), v.clone());
        }
    }
}

// ── Runtime value ─────────────────────────────────────────────────────────────

/// A value flowing through the executor. Extends the stored [`Value`] with the
/// graph-reference and map kinds an evaluator produces (`Node`/`Rel` are bound
/// dense ids resolved lazily against the generation; `Map` backs map projection
/// and map literals).
#[derive(Debug, Clone)]
pub enum Val {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<Val>),
    Vector(Vec<f32>),
    Map(Vec<(String, Val)>),
    Node(u64),
    /// A bound relationship: its dense edge id plus the endpoints and type captured
    /// at traversal time. `start`/`end` are the relationship's *stored* direction
    /// (src→dst), independent of which way the pattern walked it, so a Bolt
    /// `Relationship` (and `type()`/`startNode()`) reports the true graph direction.
    Rel {
        id: u64,
        start: u64,
        end: u64,
        reltype: u32,
    },
    /// A path: an alternating node/relationship sequence `n0, r0, n1, r1, …, nk`.
    /// `nodes` holds the `k+1` node ids in walk order (with repeats for a path
    /// that revisits a node); `rels` holds the `k` relationship values
    /// (`Val::Rel`, each carrying its *stored* src→dst direction) in walk order.
    /// Constructor/compute-only — paths are never stored or decoded from disk.
    Path {
        nodes: Vec<u64>,
        rels: Vec<Val>,
    },
    /// A geographic point (FalkorDB `T_POINT`). FalkorDB only constructs WGS-84
    /// lat/lon points (no Cartesian/x-y form, no SRID parameter); the SRID is
    /// always 4326, emitted at wire-encode time. Stored as `f64`; FalkorDB keeps
    /// `f32` internally, but its tests assert coordinates only to 1e-5 and
    /// distances to a 10% tolerance, so the wider precision is observationally
    /// equivalent. Constructor/compute-only — points are never decoded from disk.
    Point {
        latitude: f64,
        longitude: f64,
    },
    /// A temporal value. Like FalkorDB, every temporal is a single `time_t`
    /// (whole seconds since the Unix epoch, UTC) plus this type tag — see
    /// [`crate::temporal`] for the full model. Constructor/compute-only (the
    /// on-disk format cannot store temporals), so they are never decoded from a
    /// node property.
    /// `date()` — seconds at UTC midnight of the day.
    Date(i64),
    /// `localtime()` — seconds since midnight, `[0, 86400)`.
    Time(i64),
    /// `localdatetime()` — seconds since the epoch.
    DateTime(i64),
    /// `duration()` — the `time_t` of *epoch + duration*.
    Duration(i64),
}

/// Project a runtime [`Val`] back to a planning [`Value`], for the subset that can
/// key a range index. Returns `None` for runtime-only shapes (nodes, rels, paths,
/// maps, points, temporals) the on-disk index can never hold — the planner then
/// drops that `$param` predicate and falls back to a scan. The inverse of
/// [`Val::from_value`].
pub(crate) fn val_to_value(v: &Val) -> Option<Value> {
    Some(match v {
        Val::Null => Value::Null,
        Val::Bool(b) => Value::Bool(*b),
        Val::Int(i) => Value::Int(*i),
        Val::Float(f) => Value::Float(*f),
        Val::Str(s) => Value::Str(s.clone()),
        Val::Vector(xs) => Value::Vector(xs.clone()),
        Val::List(xs) => Value::List(xs.iter().map(val_to_value).collect::<Option<Vec<_>>>()?),
        Val::Map(_)
        | Val::Node(_)
        | Val::Rel { .. }
        | Val::Path { .. }
        | Val::Point { .. }
        | Val::Date(_)
        | Val::Time(_)
        | Val::DateTime(_)
        | Val::Duration(_) => return None,
    })
}

/// Project every argument to an on-disk [`Value`], or `None` if any is a
/// runtime-only shape. Used to decide whether a pure scalar call can be delegated
/// to the shared [`slater_scalar`] evaluator (which is keyed on `Value`).
fn try_all_values(args: &[Val]) -> Option<Vec<Value>> {
    args.iter().map(val_to_value).collect()
}

/// Project the index-keyable scalars of a per-row binding into a planning map, so
/// the planner can turn `MATCH (n:L {p: w})` / `WHERE n.p = w` — where `w` was
/// bound at runtime by `UNWIND`/`WITH`/a prior `MATCH` — into a per-row index seek
/// instead of a label scan. Non-keyable values (nodes, maps, temporals) are
/// dropped by [`val_to_value`], so the predicate falls back to a scan exactly as an
/// unbound variable would. The map is tiny (a handful of in-scope columns).
fn bound_scalars(binding: &HashMap<String, Val>) -> HashMap<String, Value> {
    binding
        .iter()
        .filter_map(|(k, v)| val_to_value(v).map(|val| (k.clone(), val)))
        .collect()
}

/// Does the anchor `start` key its index off a column already in `cols` — an
/// inline prop `{p: w}` or a `WHERE start.p <op> w` whose value is a bound column?
/// If so the chosen scan depends on the row and must be re-planned per row; if not,
/// it can be planned once and reused (the streamed-MATCH fast path). Sound either
/// way — a false positive only costs the per-row planning; `node_ok` re-filters.
fn anchor_correlated(start: &NodePat, where_: Option<&Expr>, cols: &[String]) -> bool {
    let is_col = |name: &str| cols.iter().any(|c| c == name);
    // Inline `{prop: w}` where `w` is an in-scope column.
    for (_, e) in &start.props {
        if let Expr::Var(name) = e {
            if is_col(name) {
                return true;
            }
        }
    }
    // `WHERE start.prop <op> w` (or mirrored) where `w` is an in-scope column.
    if let (Some(var), Some(w)) = (start.var.as_deref(), where_) {
        return where_anchor_uses_col(w, var, cols);
    }
    false
}

/// Recurse the top-level `AND`s of a `WHERE`, looking for a comparison between
/// `var.prop` and a bound column `Expr::Var(c)` (c ∈ `cols`) — the shape that
/// resolves to a per-row index seek.
fn where_anchor_uses_col(expr: &Expr, var: &str, cols: &[String]) -> bool {
    let is_anchor_prop = |e: &Expr| matches!(e, Expr::Property(base, _) if matches!(&**base, Expr::Var(v) if v == var));
    let is_col = |e: &Expr| matches!(e, Expr::Var(c) if cols.iter().any(|x| x == c));
    match expr {
        Expr::And(parts) => parts.iter().any(|p| where_anchor_uses_col(p, var, cols)),
        Expr::Compare(_, l, r) => {
            (is_anchor_prop(l) && is_col(r)) || (is_col(l) && is_anchor_prop(r))
        }
        _ => false,
    }
}

/// Edges per chunk handed to a [`for_each_adj_overlaid`] visitor. Bounds a streamed
/// hub's working set to O(chunk) neighbours regardless of degree; the collecting
/// [`read_adj_overlaid`] wrapper is chunk-agnostic (it concatenates every chunk).
const ADJ_STREAM_CHUNK: usize = 8192;

/// Effective (upper-bound) degree at or above which a node is routed into the streaming
/// reader instead of the whole-adjacency materialise — so a hub never inflates a wide
/// parallel gather. Must be ≥ the build-side hub-degree sidecar floor so the sidecar
/// holds an exact degree for every node a query might stream (slice 3).
const ADJ_STREAM_THRESHOLD: u64 = 8192;

/// Stream node `node`'s overlaid adjacency in one direction, invoking `emit` with
/// `chunk`-sized `&[Adj]` slices instead of materialising the whole neighbour list.
/// This is the **single** implementation of the core→segment→delta adjacency fold;
/// [`read_adj_overlaid`] is exactly this visitor collected into a `Vec`, so the two
/// are byte-for-byte identical (same edges, same order).
///
/// Why streaming: the core CSR record is the only unbounded part of a hub node's
/// adjacency (out-degree in the millions), and it decodes edge-by-edge via
/// [`topology::decode_adj_into`] — so a hub is walked at O(chunk) resident neighbours
/// plus the two bounded overlays, never the full multi-GB list at once. The segment
/// and delta overlays are bounded (segment count × fence-gated fragment; byte-capped
/// delta) and are prepared once, up front.
///
/// The fold, reproduced exactly (see the deleted `overlay_segment_adj`/`overlay_adj`):
///  1. **Segment overlay** (skipped for a singleton stack): fold each upper segment's
///     fence-gated adjacency fragment oldest→newest. A `removed` entry suppresses the
///     matching `edge_id`; a born entry appends. `core_removed` unions every segment's
///     removals — correct for **core** edges, which precede every segment. `seg_born`
///     carries the born edges surviving all *later* segments' removals, in append order.
///  2. **Delta overlay** (skipped for an empty delta): `suppress` holds the
///     `(reltype, neighbour)` pairs a delta tombstone drops; `delta_born` the born
///     edges; and a tombstoned **neighbour node** drops any edge to it (the traversal
///     side of a node `DELETE`). A born edge whose reltype is absent from the core is
///     skipped (it cannot be an `Adj`).
///  3. **Emit order**: surviving core edges, then surviving segment-born, then surviving
///     delta-born — the exact order the materialised fold produced.
///
/// A **delta-born** node (id ≥ core node count) has no core record, so only its segment-
/// and delta-born edges remain.
fn for_each_adj_overlaid(
    gen: &dyn ReadView,
    cache: &BlockCache,
    node: u64,
    outgoing: bool,
    reltypes: Option<&[u32]>,
    chunk: usize,
    emit: &mut dyn FnMut(&[topology::Adj]) -> Result<()>,
) -> Result<()> {
    // --- Prepare the bounded segment overlay once (was `overlay_segment_adj`). ---
    // `core_removed`: edge-ids any segment removes — applies to the core list, whose
    // edges precede every segment, so a union is exact. `seg_born`: born edges surviving
    // every *later* segment's removals, in the order the fold would append them.
    let stack = gen.core_stack();
    let mut core_removed: HashSet<u64> = HashSet::new();
    let mut seg_born: Vec<topology::Adj> = Vec::new();
    if !stack.is_singleton() {
        for seg in stack.segments() {
            let r = &seg.reader;
            let frag = if outgoing {
                if !r.may_hold_out_adj(node) {
                    continue;
                }
                r.out_adj(node)?
            } else {
                if !r.may_hold_in_adj(node) {
                    continue;
                }
                r.in_adj(node)?
            };
            if frag.is_empty() {
                continue;
            }
            let mut seg_removed: HashSet<u64> = HashSet::new();
            let mut this_born: Vec<topology::Adj> = Vec::new();
            for e in frag {
                if e.removed {
                    seg_removed.insert(e.edge_id);
                } else if let Some(rt) = gen.reltype_id(&e.reltype) {
                    this_born.push(topology::Adj {
                        reltype: rt,
                        neighbour: NodeId(e.other),
                        edge: EdgeId(e.edge_id),
                    });
                }
            }
            if !seg_removed.is_empty() {
                // This segment's removals suppress born edges from earlier segments only
                // (this_born is appended after), matching the incremental per-segment fold.
                seg_born.retain(|a| !seg_removed.contains(&a.edge.0));
                core_removed.extend(seg_removed.iter().copied());
            }
            seg_born.extend(this_born);
        }
    }

    // --- Prepare the bounded delta overlay once (was `overlay_adj`). ---
    let delta = gen.delta();
    let has_delta = !delta.is_empty();
    let mut suppress: HashSet<(u32, u64)> = HashSet::new();
    let mut delta_born: Vec<topology::Adj> = Vec::new();
    if has_delta {
        let deltas = if outgoing {
            delta.out_edges(node)
        } else {
            delta.in_edges(node)
        };
        for e in deltas {
            let Some(rt) = gen.reltype_id(&e.reltype) else {
                continue;
            };
            if e.tombstoned {
                suppress.insert((rt, e.other));
            } else if let Some(eid) = e.edge_id {
                delta_born.push(topology::Adj {
                    reltype: rt,
                    neighbour: NodeId(e.other),
                    edge: EdgeId(eid),
                });
            }
        }
    }
    // Delta filter applied to core + segment-born edges: drop a delta-suppressed
    // `(reltype, neighbour)` or an edge to a tombstoned neighbour node. On an empty
    // delta the whole overlay is skipped, exactly as the materialised fast path did.
    let delta_keep = |a: &topology::Adj| -> bool {
        !has_delta
            || (!suppress.contains(&(a.reltype, a.neighbour.0))
                && !delta.is_tombstoned(a.neighbour.0))
    };

    // Chunked emit: buffer survivors and flush a full `chunk`; a scoped closure so the
    // final partial flush can run after the borrow of `emit`/`buf` is released.
    let mut buf: Vec<topology::Adj> = Vec::new();
    {
        let mut push = |a: topology::Adj| -> Result<()> {
            // Test-only seam: count every surviving edge handed toward the visitor. A
            // short-circuit probe (`has_incident_edge`/`find_outgoing_edge`, chunk 1) increments
            // this once and stops; the materialising readers increment it once per edge. The
            // regression test asserts the probe walks O(1) edges, not the whole hub adjacency.
            #[cfg(test)]
            ADJ_VISIT_COUNT.with(|c| c.set(c.get() + 1));
            buf.push(a);
            if buf.len() >= chunk {
                emit(&buf)?;
                buf.clear();
            }
            Ok(())
        };
        // 1. Surviving core edges, streamed edge-by-edge (never held whole for a hub).
        if node < gen.core_generation().node_count() {
            let topo = gen.topology();
            let global = if outgoing {
                topo.outgoing_global(NodeId(node))
            } else {
                topo.incoming_global(NodeId(node))
            };
            let rec = cache.record(topo.inner(), gen.uuid(), FileKind::Topology, global)?;
            let mut core_visit = |a: topology::Adj| -> Result<()> {
                if !core_removed.contains(&a.edge.0) && delta_keep(&a) {
                    push(a)?;
                }
                Ok(())
            };
            // Typed expand pushes the reltype set into the decoder, which skips a whole
            // non-matching reltype run without decoding its neighbour bytes (the hub win);
            // an untyped read decodes every run.
            match reltypes {
                Some(rts) => topology::decode_adj_into_filtered(
                    &rec,
                    outgoing,
                    |rt| rts.contains(&rt),
                    &mut core_visit,
                )?,
                None => topology::decode_adj_into(&rec, outgoing, &mut core_visit)?,
            }
        }
        // Segment/delta-born edges are small and uncompressed; apply the same reltype filter
        // in the fold rather than in the (already-materialised) fragment.
        let rt_keep = |a: &topology::Adj| reltypes.is_none_or(|rts| rts.contains(&a.reltype));
        // 2. Surviving segment-born edges (delta filter applies, as they sit in the list).
        for a in &seg_born {
            if rt_keep(a) && delta_keep(a) {
                push(*a)?;
            }
        }
        // 3. Surviving delta-born edges (tombstoned-neighbour filter only, matching the fold).
        for a in &delta_born {
            if rt_keep(a) && !delta.is_tombstoned(a.neighbour.0) {
                push(*a)?;
            }
        }
    }
    if !buf.is_empty() {
        emit(&buf)?;
    }
    Ok(())
}

/// Read node `node`'s adjacency in one direction (`outgoing` = out-edges) through the
/// (Sync) block cache and fold the edge delta over it. A **delta-born** node has no
/// core topology record, so its core adjacency is empty and only its born edges
/// remain. The single reader behind the sequential [`Engine::outgoing`]/[`incoming`]
/// and the parallel [`hops_par`]/[`neighbours_par`], so every traversal path applies
/// the identical overlay. Collects [`for_each_adj_overlaid`] — the one fold — so it is
/// byte-for-byte the streamed neighbour list.
fn read_adj_overlaid(
    gen: &dyn ReadView,
    cache: &BlockCache,
    node: u64,
    outgoing: bool,
) -> Result<Vec<topology::Adj>> {
    read_adj_overlaid_filtered(gen, cache, node, outgoing, None)
}

/// [`read_adj_overlaid`] restricted to a reltype set (`None` = all). The set is pushed into the
/// core CSR decode so a typed expand skips non-matching reltype runs without decoding them.
fn read_adj_overlaid_filtered(
    gen: &dyn ReadView,
    cache: &BlockCache,
    node: u64,
    outgoing: bool,
    reltypes: Option<&[u32]>,
) -> Result<Vec<topology::Adj>> {
    let mut out = Vec::new();
    for_each_adj_overlaid(
        gen,
        cache,
        node,
        outgoing,
        reltypes,
        ADJ_STREAM_CHUNK,
        &mut |c| {
            out.extend_from_slice(c);
            Ok(())
        },
    )?;
    Ok(out)
}

#[cfg(test)]
thread_local! {
    /// Per-thread count of surviving adjacency edges handed to a [`for_each_adj_overlaid`]
    /// visitor. The short-circuit-probe regression test resets it, runs a probe, and asserts
    /// it advanced by O(1) rather than the node's full degree. Thread-local so parallel tests
    /// don't clobber each other's count — the single-node reader runs entirely on the calling
    /// thread (no fanout), so the count is exact.
    pub(crate) static ADJ_VISIT_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Zero-sized sentinel raised from a [`for_each_adj_overlaid`] `emit` callback to stop the
/// stream at the first edge of interest — the existence-probe / first-match short-circuit.
/// It is caught and swallowed by [`any_adj_overlaid`] / [`find_outgoing_edge_overlaid`]
/// (matched by *type* via `downcast_ref`, never by message), so it never escapes as a real
/// error. Using the stream's own early-return keeps a hub's multi-million-edge adjacency from
/// being decoded or materialised just to answer "is there at least one edge?".
#[derive(Debug)]
struct AdjScanStop;

impl std::fmt::Display for AdjScanStop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("adjacency scan short-circuited")
    }
}

impl std::error::Error for AdjScanStop {}

/// Does node `node` have **any** surviving (post-overlay) edge in direction `outgoing`?
/// Streams the overlaid adjacency one edge at a time and stops at the first survivor via the
/// [`AdjScanStop`] sentinel — so a hub node is never walked to completion nor materialised into
/// a `Vec`. Overlay-exact: it sees a delta-born edge and drops a delta-tombstoned one, because
/// it shares the single [`for_each_adj_overlaid`] fold with the materialising readers.
fn any_adj_overlaid(
    gen: &dyn ReadView,
    cache: &BlockCache,
    node: u64,
    outgoing: bool,
) -> Result<bool> {
    let mut found = false;
    let r = for_each_adj_overlaid(gen, cache, node, outgoing, None, 1, &mut |batch| {
        if !batch.is_empty() {
            found = true;
            return Err(AdjScanStop.into());
        }
        Ok(())
    });
    match r {
        Ok(()) => Ok(found),
        Err(e) if e.downcast_ref::<AdjScanStop>().is_some() => Ok(true),
        Err(e) => Err(e),
    }
}

/// The edge id of the first surviving (post-overlay) `src -[reltype]-> dst` out-edge, or `None`.
/// Pushes the reltype set into the CSR decode (a typed run skip) and stops at the first matching
/// neighbour via the [`AdjScanStop`] sentinel — so finding one edge of a hub source never
/// materialises its whole out-adjacency. Overlay-exact via the shared fold; the caller controls
/// which edges are in scope by choosing the view (an empty-delta view sees core edges only).
fn find_outgoing_edge_overlaid(
    gen: &dyn ReadView,
    cache: &BlockCache,
    src: u64,
    reltype: u32,
    dst: u64,
) -> Result<Option<u64>> {
    let mut hit: Option<u64> = None;
    let rts = [reltype];
    let r = for_each_adj_overlaid(gen, cache, src, true, Some(&rts), 1, &mut |batch| {
        if let Some(a) = batch
            .iter()
            .find(|a| a.reltype == reltype && a.neighbour.0 == dst)
        {
            hit = Some(a.edge.0);
            return Err(AdjScanStop.into());
        }
        Ok(())
    });
    match r {
        Ok(()) => Ok(hit),
        Err(e) if e.downcast_ref::<AdjScanStop>().is_some() => Ok(hit),
        Err(e) => Err(e),
    }
}

/// Thread-safe single-node neighbour read for the parallel `shortestPath()` BFS:
/// read+decode `node`'s adjacency in direction `dir` through the (Sync) block cache,
/// keeping neighbours whose reltype passes `type_ids` (`None` = any type). Run
/// off-thread by the rayon frontier expansion — it touches only the Sync `gen` +
/// `cache`, never the executor's interior-mutable state, so it carries no rel-property
/// predicate (the caller restricts the parallel path to property-free patterns).
fn neighbours_par(
    gen: &dyn ReadView,
    cache: &BlockCache,
    node: u64,
    dir: Direction,
    type_ids: Option<&[u32]>,
) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    let mut take = |outgoing: bool| -> Result<()> {
        // Push the reltype set into the decode so a typed expand skips non-matching runs.
        for a in read_adj_overlaid_filtered(gen, cache, node, outgoing, type_ids)? {
            if type_ids.is_none_or(|ids| ids.contains(&a.reltype)) {
                out.push(a.neighbour.0);
            }
        }
        Ok(())
    };
    match dir {
        Direction::Outgoing => take(true)?,
        Direction::Incoming => take(false)?,
        Direction::Undirected => {
            take(true)?;
            take(false)?;
        }
    }
    Ok(out)
}

/// Resolve a relationship pattern's type constraint once, before a per-edge loop.
/// The common positive shapes (untyped, `:T`, `:T1|T2`) collapse to a flat
/// reltype-id set for a plain integer membership test; only a genuine boolean type
/// expression (`&`/`!`) keeps the AST for per-edge evaluation. Used by both
/// [`Engine::expand_with_dir`] and the parallel [`hops_par`] reader; touches only
/// the (Sync) symbol table, so it is safe to call before fanning out.
fn resolve_type_filter<'a>(gen: &dyn ReadView, rel: &'a RelPat) -> Option<TypeFilter<'a>> {
    rel.type_expr.as_ref().map(|e| match e.positive_atoms() {
        Some(names) => TypeFilter::AnyOf(names.iter().filter_map(|t| gen.reltype_id(t)).collect()),
        None => TypeFilter::Expr(e),
    })
}

/// Thread-safe single-node hop expansion behind the parallel multi-hop walk
/// (Task 9): read+decode `node`'s adjacency in direction `dir` through the (Sync)
/// block cache, yielding one [`Hop`] per edge whose type passes `tf` (`None` = any
/// type). Mirrors [`Engine::expand_with_dir`] **minus** the relationship-property
/// predicate (`rel_ok`): the parallel path is gated to property-free rels (see
/// [`Engine::chain_parallelizable`]), so no `!Sync` per-edge evaluation is needed.
/// Outgoing edges precede incoming for an undirected hop, and `start`/`end` carry
/// the stored src→dst direction — both exactly as the sequential reader, so the hop
/// order (and thus the emitted-row order) is identical.
fn hops_par(
    gen: &dyn ReadView,
    cache: &BlockCache,
    node: u64,
    dir: Direction,
    tf: Option<&TypeFilter>,
) -> Result<Vec<Hop>> {
    // A positive `:T1|T2` filter pushes into the decode (skips non-matching reltype runs); a
    // boolean `Expr` cannot, so it stays a post-decode per-edge check below.
    let rt_ids = match tf {
        Some(TypeFilter::AnyOf(ids)) => Some(ids.as_slice()),
        _ => None,
    };
    let mut sources: Vec<(Vec<topology::Adj>, bool)> = Vec::new();
    match dir {
        Direction::Outgoing => sources.push((
            read_adj_overlaid_filtered(gen, cache, node, true, rt_ids)?,
            false,
        )),
        Direction::Incoming => sources.push((
            read_adj_overlaid_filtered(gen, cache, node, false, rt_ids)?,
            true,
        )),
        Direction::Undirected => {
            sources.push((
                read_adj_overlaid_filtered(gen, cache, node, true, rt_ids)?,
                false,
            ));
            sources.push((
                read_adj_overlaid_filtered(gen, cache, node, false, rt_ids)?,
                true,
            ));
        }
    }
    let mut out = Vec::new();
    for (adjs, incoming) in sources {
        for a in adjs {
            match tf {
                None => {}
                Some(TypeFilter::AnyOf(ids)) => {
                    if !ids.contains(&a.reltype) {
                        continue;
                    }
                }
                Some(TypeFilter::Expr(e))
                    if !e.eval(&|name| gen.reltype_id(name) == Some(a.reltype)) =>
                {
                    continue;
                }
                Some(TypeFilter::Expr(_)) => {}
            }
            let (start, end) = if incoming {
                (a.neighbour.0, node)
            } else {
                (node, a.neighbour.0)
            };
            out.push(Hop {
                edge: a.edge.0,
                neighbour: a.neighbour.0,
                reltype: a.reltype,
                start,
                end,
            });
        }
    }
    Ok(out)
}

/// Stream node `node`'s type-filtered hops in `chunk`-sized batches, reproducing
/// [`hops_par`]'s edge order (outgoing before incoming for an undirected hop) **without
/// materialising the whole hop list** — the hub-node counterpart of `hops_par`, built on
/// [`for_each_adj_overlaid`]. Like `hops_par` it applies only the type filter (the
/// parallel walk is gated to relationship-property-free patterns); a caller that needs
/// the relationship-property predicate (`rel_ok`) applies it per emitted hop. Routing a
/// hub through this bounds its live buffer to `O(chunk)` hops instead of its full degree.
fn for_each_hop_overlaid(
    gen: &dyn ReadView,
    cache: &BlockCache,
    node: u64,
    dir: Direction,
    tf: Option<&TypeFilter>,
    chunk: usize,
    emit: &mut dyn FnMut(&[Hop]) -> Result<()>,
) -> Result<()> {
    // (outgoing, incoming) sources in `hops_par` order: outgoing edges precede incoming
    // for an undirected hop, and `incoming` flips start/end to the stored src→dst sense.
    let dirs: &[(bool, bool)] = match dir {
        Direction::Outgoing => &[(true, false)],
        Direction::Incoming => &[(false, true)],
        Direction::Undirected => &[(true, false), (false, true)],
    };
    let rt_ids = match tf {
        Some(TypeFilter::AnyOf(ids)) => Some(ids.as_slice()),
        _ => None,
    };
    let mut buf: Vec<Hop> = Vec::new();
    for &(outgoing, incoming) in dirs {
        for_each_adj_overlaid(gen, cache, node, outgoing, rt_ids, chunk, &mut |adjs| {
            for a in adjs {
                let keep = match tf {
                    None => true,
                    Some(TypeFilter::AnyOf(ids)) => ids.contains(&a.reltype),
                    Some(TypeFilter::Expr(e)) => {
                        e.eval(&|name| gen.reltype_id(name) == Some(a.reltype))
                    }
                };
                if !keep {
                    continue;
                }
                let (start, end) = if incoming {
                    (a.neighbour.0, node)
                } else {
                    (node, a.neighbour.0)
                };
                buf.push(Hop {
                    edge: a.edge.0,
                    neighbour: a.neighbour.0,
                    reltype: a.reltype,
                    start,
                    end,
                });
                if buf.len() >= chunk {
                    emit(&buf)?;
                    buf.clear();
                }
            }
            Ok(())
        })?;
    }
    if !buf.is_empty() {
        emit(&buf)?;
    }
    Ok(())
}

/// Minimum frontier size below which a parallel multi-hop level's adjacency reads
/// run sequentially — the rayon fan-out overhead isn't worth it for a narrow
/// frontier (the same threshold the shortestPath frontier uses). Above it, the
/// per-node reads gather on the shared fanout pool.
const EXPAND_PAR_MIN: usize = 64;

/// Branch-flush size for the parallel chain walk ([`Engine::par_walk`]): the
/// next-hop frontier is recursed depth-first once it reaches `EXPAND_BATCH`
/// branches, so live branch memory stays `O(EXPAND_BATCH × chain length)` instead
/// of the chain's exponential fan-out. (Bounds *branch* count; the read buffer is
/// bounded separately by [`EXPAND_READ_CHUNK`].)
const EXPAND_BATCH: usize = 512;

/// Node-chunk size for the parallel chain walk's adjacency reads: a chunk's edges
/// gather into one buffer that is freed before the next chunk reads, bounding live
/// read memory to `O(EXPAND_READ_CHUNK × degree)` — one chunk's worth. Decoupled
/// from [`EXPAND_BATCH`] because a *branch* is tiny but a high-degree node's
/// *adjacency* is not: reading a whole 512-branch frontier of hubs at once buffers
/// tens of millions of edges, where the sequential walk holds only one node's. Set
/// to [`EXPAND_PAR_MIN`] so each chunk is exactly at the pool's fan-out threshold.
const EXPAND_READ_CHUNK: usize = EXPAND_PAR_MIN;

/// Read one vector-index record `global` from `vectors.f32.blk` **through the block
/// cache** (D18), decoding its dense node id + full-precision vector. The Sync reader
/// behind the parallel brute-force kNN gather — it takes only `&Generation`/`&BlockCache`
/// (both `Send + Sync`) so it can run off-thread; mirrors [`Engine::vector_group`]'s
/// per-record body.
fn read_vector(gen: &dyn ReadView, cache: &BlockCache, global: u64) -> Result<VectorEntry> {
    let rec = cache.record(gen.vectors().inner(), gen.uuid(), FileKind::Vectors, global)?;
    vectors::decode_vector(&rec)
}

/// Minimum vector-group / candidate count below which brute-force kNN reads and
/// scoring run sequentially — the rayon fan-out overhead isn't worth it for a small
/// group (and the live estate is entirely below the ANN threshold, so most groups are
/// small). Above it, both the candidate reads and the distance/top-k scan parallelize.
const KNN_PAR_MIN: usize = 256;

/// Thread-safe read+decode of node `id`'s resident label-id set through the (Sync)
/// block cache. The free-fn body behind [`Engine::node_label_ids`] so the parallel
/// anchor filter ([`node_ok_par`], Task 10) can read labels off-thread.
fn node_label_ids_par(gen: &dyn ReadView, cache: &BlockCache, id: u64) -> Result<Vec<u32>> {
    // A delta-born node (Phase 2c) carries the single label of its business identity
    // and has no core label record — resolve the label name from the delta and map it
    // through the core symbol table (the write path requires the label to pre-exist).
    // The id-threshold compare gates the core block read; the label overlay below still
    // applies to a born node's identity label.
    let mut ids: Vec<u32> = if let Some(row) = gen.core_stack().resolve_node_row(id)? {
        // A segment carries a full row for `id` (an override of a base node, or a
        // segment-born node): its label set replaces the base's, mapped through the core
        // symbol table (labels must pre-exist, as for a delta-born identity label). A
        // tombstone row contributes no labels.
        if row.tombstoned {
            Vec::new()
        } else {
            row.labels.iter().filter_map(|l| gen.label_id(l)).collect()
        }
    } else if id >= gen.core_generation().node_count() {
        gen.delta()
            .node_identity_by_dense(id)
            .and_then(|(label, _, _)| gen.label_id(&label))
            .into_iter()
            .collect()
    } else {
        let rec = cache.record(
            gen.node_labels().inner(),
            gen.uuid(),
            FileKind::NodeLabels,
            id,
        )?;
        nodelabels::decode_labels(&rec, gen.node_labels().bitmask())?
    };
    // Label mutation overlay (Stage 5): fold out `REMOVE n:Label` drops and union in
    // `SET n:Label` additions. Labels are held by name; map through the core symbol
    // table. The empty-delta fast path skips this entirely.
    let delta = gen.delta();
    if !delta.is_empty() {
        if let Some(nd) = delta.node_patch(id) {
            if !nd.labels_removed.is_empty() {
                let removed: Vec<u32> = nd
                    .labels_removed
                    .iter()
                    .filter_map(|l| gen.label_id(l))
                    .collect();
                ids.retain(|x| !removed.contains(x));
            }
            for l in &nd.labels_added {
                if let Some(lid) = gen.label_id(l) {
                    if !ids.contains(&lid) {
                        ids.push(lid);
                    }
                }
            }
        }
    }
    Ok(ids)
}

/// D12: an *indexed* embedding is routed **out** of the column store at build time, so a
/// column read of one yields `Null` from the core — it is served by the KNN path, not by
/// `RETURN n.embedding`. A delta- or segment-written embedding, though, *does* still sit
/// in the node's property map: that map is what carries it through the WAL, the L0, the
/// T2 flush and the consolidation rebuild, so it is deliberately not stripped at write
/// time. Suppress it on the way out instead, or one query would answer `Null` for a
/// core-resident node and a vector for a freshly-written one — the same graph giving two
/// answers depending on which level the node happens to live in.
///
/// Cheap by construction: a non-vector value returns before touching the manifest, and a
/// graph that declares no vector index never resolves labels. An *unindexed* inline
/// vector reads back verbatim, exactly as it does from the core.
fn suppress_indexed_vector(
    gen: &dyn ReadView,
    cache: &BlockCache,
    id: u64,
    key: &str,
    v: Val,
) -> Result<Val> {
    if !matches!(v, Val::Vector(_)) {
        return Ok(v);
    }
    let indexed: Vec<&str> = gen
        .manifest()
        .vector_indexes
        .iter()
        .filter(|d| d.property == key)
        .map(|d| d.label.as_str())
        .collect();
    if indexed.is_empty() {
        return Ok(v);
    }
    for lid in node_label_ids_par(gen, cache, id)? {
        if gen.label_name(lid).is_some_and(|n| indexed.contains(&n)) {
            return Ok(Val::Null);
        }
    }
    Ok(v)
}

/// What **one level above the base** says about a vector index — the write-visibility
/// primitive the base's sealed, immutable index cannot provide.
#[derive(Default)]
pub struct VectorLevel {
    /// `(node_id, vector)` for every node this level embeds or re-embeds. The entry any
    /// level *below* it holds for that node is stale and must be suppressed.
    pub entries: Vec<graph_format::vectors::VectorEntry>,
    /// Nodes whose embedding this level **took away** — `REMOVE n.embedding`, a `SET n = {…}`
    /// that dropped it, or an overwrite with a non-vector value. Every level below must be
    /// suppressed with *nothing* put in its place: the node is simply no longer in the index.
    pub removed: Vec<u64>,
    /// Nodes this level took **out of the index's scope** (`REMOVE n:Label`) while leaving the
    /// embedding *value* untouched (D64). Every level below is suppressed exactly as `removed`
    /// suppresses it — the node is not in the index — but the two are **not** the same fact,
    /// and a consolidation is where the difference bites (HIK-122).
    ///
    /// A `removed` node's vector is gone because the user deleted it. An `out_of_scope` node's
    /// vector is *retained*: HIK-118 promises that a later `SET n:Label` puts the node back in
    /// scope and scores it again, and `flush_segment` records a **label** removal rather than a
    /// value one precisely to keep that promise across a flush. A consolidation that treats the
    /// two alike destroys the vector and makes the promise a lie — so it must *move* these
    /// vectors to the column store (the canonical out-of-scope representation, and the one a
    /// fresh build produces), not drop them. See [`VectorLevels::out_of_scope`].
    pub out_of_scope: Vec<u64>,
}

impl VectorLevel {
    /// Every node whose *lower*-level entry this level invalidates — re-embedded, un-embedded,
    /// or taken out of scope. A lower level must suppress these in its **scan**, not in the
    /// merge afterwards (see [`vector::merge_topk`]).
    ///
    /// All three channels suppress; only [`Self::removed`] *deletes*. A caller that acts on the
    /// suppression (a scan) wants this; a caller that acts on the deletion (a consolidation)
    /// must ask the channels apart.
    pub fn superseded(&self) -> HashSet<u64> {
        self.entries
            .iter()
            .map(|e| e.node_id)
            .chain(self.removed.iter().copied())
            .chain(self.out_of_scope.iter().copied())
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.removed.is_empty() && self.out_of_scope.is_empty()
    }
}

/// The write ladder's two mutable tiers, resolved **per level** for one vector index.
///
/// The base's index is sealed at build time; everything above it lives here. The levels are
/// kept apart rather than flattened because each one has a *different* set of levels above
/// it, and therefore a different suppression set:
///
/// ```text
/// base_live     = suppress( delta.superseded ∪ segments.superseded ∪ tombstones )
/// segments_live = suppress( delta.superseded ∪ tombstones )
/// delta_live    = suppress( tombstones )                      // nothing is above the delta
/// ```
///
/// A flattened overlay can only express the first line. That is enough while the levels
/// above the base are brute-forced through a single matrix (each node appears in it exactly
/// once, newest-wins), and it stops being enough the moment a level gets an index of its own
/// and scans itself: level *i* still physically holds the vector that level *i+1* superseded,
/// and suppressing it with the *global* set would also drop the newer entry that replaced it.
/// Suppression is per level, and it happens in that level's **scan** — never in the merge, which
/// [deliberately does not dedup](vector::merge_topk): a stale duplicate that reaches the merge
/// has already been able to take one of the `k` slots and evict a live candidate, so the k-th
/// neighbour goes missing rather than merely being misordered. Silently.
#[derive(Default)]
pub struct VectorLevels {
    /// Newest: the write delta (memtable + sealed L0). Nothing is above it.
    pub delta: VectorLevel,
    /// The core segments, folded newest-wins **across segments only** — the write delta is
    /// deliberately *not* applied, so this level holds what the segments themselves say, even
    /// where the delta has since superseded it. Suppressed by `delta.superseded()`.
    pub segments: VectorLevel,
}

impl VectorLevels {
    /// Every node whose **base** entry some level above it invalidates. The union, because
    /// the base is below both.
    pub fn superseded(&self) -> HashSet<u64> {
        let mut s = self.segments.superseded();
        s.extend(self.delta.superseded());
        s
    }

    pub fn is_empty(&self) -> bool {
        self.delta.is_empty() && self.segments.is_empty()
    }

    /// Every node some level above the base took **out of the index's scope** without deleting
    /// its embedding, and that no level has since put back (a re-embed or a value removal is
    /// the newer, winning fact about it).
    ///
    /// The consolidation's rescue set (HIK-122). These nodes must not be in the rebuilt index —
    /// the base arm's `live` gate does not re-check scope, so the base index is trusted to hold
    /// only in-scope nodes — but their vectors must survive as **column** values. Where a level
    /// carries the vector inline, the dump's property walk already does that; where it does not,
    /// the only copy is the one D12 routed out of the props record into the base index, and the
    /// consolidation has to move it back.
    pub fn out_of_scope(&self) -> HashSet<u64> {
        let newer = self.delta.superseded();
        let mut s: HashSet<u64> = self
            .segments
            .out_of_scope
            .iter()
            .copied()
            .filter(|id| !newer.contains(id))
            .collect();
        s.extend(self.delta.out_of_scope.iter().copied());
        s
    }

    /// The levels flattened newest-wins into the effective `(node_id, vector)` set above the
    /// base — the delta's entries, plus a segment entry for every node the delta did not
    /// supersede. The **consolidation dump**'s view: it rewrites the graph, so it wants one
    /// vector per node, not a scan of each level.
    ///
    /// Derived from the same two levels the KNN path reads, so the dump cannot disagree with
    /// the query about which vector wins — a disagreement would drop a vector on the floor,
    /// and only on consolidation.
    pub fn into_effective_entries(self) -> Vec<graph_format::vectors::VectorEntry> {
        let newer = self.delta.superseded();
        let mut out = self.delta.entries;
        out.extend(
            self.segments
                .entries
                .into_iter()
                .filter(|e| !newer.contains(&e.node_id)),
        );
        out
    }
}

/// What one level says about node `id`'s embedding for a vector index.
enum LevelSays {
    /// The level carries an embedding: it supersedes every level below.
    Vector(Vec<f32>),
    /// The level took the embedding away — `REMOVE`d, dropped by a `SET n = {…}` replace, or
    /// overwritten with a value that is not a vector. Every level below is suppressed with
    /// nothing in its place.
    Gone,
    /// The level says nothing about this node's embedding: whatever is below it stands (an
    /// older segment's, or the base's — which a column read cannot even see, per D12).
    Nothing,
}

/// Split the delta and the core segments into per-level [`VectorLevels`] for one vector index.
///
/// Two consumers, and they must agree or a vector goes missing: the KNN path scans each level
/// against the base's own arm, and the consolidation dump carries them so a rebuild does not
/// drop a freshly-written embedding on the floor. Both derive from *this* fold — see
/// [`VectorLevels::into_effective_entries`].
///
/// # Why removals need their own channel
/// An indexed embedding is routed **out** of the column store (D12), so a node's property
/// record never held one — which means "this row has no embedding" is ambiguous. A node
/// whose embedding was `REMOVE`d and a node merely flushed for an unrelated reason
/// (`SET n.age = 99`) produce byte-identical rows, and both read back as `Null`. Value
/// absence therefore cannot express a removal, and without an explicit record the removed
/// node's stale base vector keeps scoring forever. The delta says so via `NodeDelta`; a
/// segment says so via its `vec.meta` sidecar ([`graph_format::segvectors`]).
///
/// # Cost
/// O(vectors touched), not O(graph) and not O(segment): the segment candidates are exactly the
/// ids the sidecars name (a large segment that embedded three nodes contributes three
/// candidates, not its whole row count) and the delta's are the nodes it touched. Each level
/// resolves *itself* — the segments through [`CoreStack::resolve_node_row`](crate::segstack::CoreStack::resolve_node_row),
/// which is already delta-free, and the delta through its own `NodeDelta`.
pub fn vector_levels(
    gen: &dyn ReadView,
    cache: &BlockCache,
    desc: &VectorIndexDesc,
) -> Result<VectorLevels> {
    if gen.delta().is_empty() && gen.core_stack().is_singleton() {
        return Ok(VectorLevels::default());
    }
    Ok(VectorLevels {
        delta: delta_level(gen, cache, desc)?,
        segments: segment_level(gen, cache, desc)?,
    })
}

/// Is node `id` deleted anywhere above the base? A deleted node takes its embedding with it,
/// and its tombstone already suppresses it on every arm — it needs no vector removal on top.
fn vector_dead(gen: &dyn ReadView, id: u64) -> Result<bool> {
    let stack = gen.core_stack();
    Ok(gen.delta().is_tombstoned(id) || (!stack.is_singleton() && stack.is_node_tombstoned(id)?))
}

/// Does node `id` **effectively** carry the index's label? The index is scoped to a label, and
/// a write can add one (`SET n:Label`) or take one away, so this resolves the effective label
/// set rather than trusting the base's. It is a property of the node, not of a level, so every
/// level asks the same question.
fn vector_indexed(
    gen: &dyn ReadView,
    cache: &BlockCache,
    id: u64,
    desc: &VectorIndexDesc,
) -> Result<bool> {
    Ok(node_label_ids_par(gen, cache, id)?
        .into_iter()
        .any(|l| gen.label_name(l).is_some_and(|n| n == desc.label)))
}

/// **The one definition of what the write delta says about one node's embedding.**
///
/// The RW-index ([`crate::rwindex`]) is an incrementally-maintained cache of exactly this
/// function over the delta's touched ids, and [`delta_level`] below is the same function
/// applied to all of them at once. They must not drift: the index's `superseded` set is what
/// suppresses the base and segment arms, so a disagreement is a duplicate or a missing node in
/// the merged top-k, not a slow query. One definition, two callers.
pub(crate) fn delta_vector_for(
    gen: &dyn ReadView,
    cache: &BlockCache,
    id: u64,
    desc: &VectorIndexDesc,
) -> Result<DeltaVector> {
    if vector_dead(gen, id)? {
        return Ok(DeltaVector::Silent);
    }
    if !vector_indexed(gen, cache, id, desc)? {
        // The node is not in the index's scope. Two ways to get here and they are *not* the
        // same fact: a node that simply never carried the index's label has nothing below to
        // supersede (`Silent` — the base's vector, if any, is another node's business); but a
        // node whose effective label set dropped the label **because this delta removed it**
        // (`REMOVE n:Label`) has *left* a scope it was in, so the vector some level below still
        // holds must be suppressed — `OutOfScope` (superseded **in**), exactly as a value
        // removal is. The index is scope-defined by the label, so leaving the label leaves the
        // index. D12 routes an indexed embedding out of the row, so absence cannot express this;
        // the delta's `labels_removed` is the channel (the segment's is its sidecar —
        // `segment_level`).
        //
        // It is `OutOfScope` and not `Gone` because the embedding *value* is untouched (D64):
        // every scan treats the two alike, but a consolidation must not delete this vector
        // (HIK-122) — it has to move it to the column store.
        let left_scope = gen
            .delta()
            .node_patch(id)
            .is_some_and(|nd| nd.labels_removed.contains(&desc.label));
        if !left_scope {
            return Ok(DeltaVector::Silent);
        }
        // Leaving the scope is not the only thing this delta may have done. Ask what it says
        // about the *value* too, because one write can do both — `MATCH (n) SET n = {name:'x'}
        // REMOVE n:Doc` drops the embedding *and* the label — and a deletion is the stronger
        // fact: `OutOfScope` promises the value survives, so filing a deleted one under it
        // would have the consolidation rescue the vector the user just threw away, back into
        // the column store, where `RETURN n.embedding` would then hand it out again.
        return Ok(match delta_says(gen, id, desc) {
            LevelSays::Gone => DeltaVector::Gone,
            LevelSays::Vector(_) | LevelSays::Nothing => DeltaVector::OutOfScope,
        });
    }
    Ok(match delta_says(gen, id, desc) {
        LevelSays::Vector(v) => DeltaVector::Set(v),
        LevelSays::Gone => DeltaVector::Gone,
        // The delta says nothing about the *value* — but it may still have changed the node's
        // membership. `SET n:Label` moves a node **into** the index's scope, and while it was
        // out of scope its embedding was an ordinary column value (that is the canonical
        // out-of-scope form: D12 only routes an embedding out of the row for a node that is
        // *in* scope at build time, and the base index only ever holds in-scope nodes). So no
        // level below has an entry for it, and nothing would ever score it again — while
        // `suppress_indexed_vector` starts answering `Null` for the column read the moment the
        // label lands. The vector would be reachable by no query at all.
        //
        // Materialise it here, which is the mirror of the `left_scope` arm above: entering the
        // scope is the delta's own fact about this node's membership, so the delta is the level
        // that must carry the vector. `node_prop_raw` is the *unsuppressed* read — the value
        // really is in the column store — and `Silent` still covers the ordinary case of a node
        // that has no embedding at all.
        LevelSays::Nothing => {
            let entered_scope = gen
                .delta()
                .node_patch(id)
                .is_some_and(|nd| nd.labels_added.contains(&desc.label));
            match entered_scope.then(|| node_prop_raw(gen, cache, id, &desc.property)) {
                Some(Ok(Val::Vector(v))) => DeltaVector::Set(v),
                Some(Err(e)) => return Err(e),
                _ => DeltaVector::Silent,
            }
        }
    })
}

/// The delta level, brute-forced: [`delta_vector_for`] over every node the delta touched.
///
/// This is the O(delta) walk — a label resolve (which reads a block) and a vector clone per
/// touched node, on **every query** — that the RW-index exists to replace. It stays as the
/// fallback (`vectorQuery.rwIndex.enabled = false`, a delta below `minVectors` or above
/// `maxVectors`, a query whose epoch the index has already run past), and as the consolidation
/// dump's view, which wants entries rather than a graph.
pub fn delta_level(
    gen: &dyn ReadView,
    cache: &BlockCache,
    desc: &VectorIndexDesc,
) -> Result<VectorLevel> {
    let mut level = VectorLevel::default();
    if gen.delta().is_empty() {
        return Ok(level);
    }
    for id in gen.delta().node_dense_ids() {
        match delta_vector_for(gen, cache, id, desc)? {
            DeltaVector::Set(v) => level.entries.push(graph_format::vectors::VectorEntry {
                node_id: id,
                vector: v,
            }),
            DeltaVector::Gone => level.removed.push(id),
            DeltaVector::OutOfScope => level.out_of_scope.push(id),
            DeltaVector::Silent => {}
        }
    }
    Ok(level)
}

/// The segments level: what the core segments say, with the delta deliberately **not** applied.
///
/// Candidates are exactly the ids the segments' `vec.meta` sidecars name for this index — a
/// large segment that embedded three nodes contributes three candidates, not its row count, and
/// a segment with no sidecar contributes nothing at all. Unaffected by the RW-index: this level
/// is bounded by the sidecars, not by the delta.
pub fn segment_level(
    gen: &dyn ReadView,
    cache: &BlockCache,
    desc: &VectorIndexDesc,
) -> Result<VectorLevel> {
    let mut segments = VectorLevel::default();
    let stack = gen.core_stack();
    if stack.is_singleton() {
        return Ok(segments);
    }
    let mut ids = Vec::new();
    for seg in stack.segments() {
        if let Some(v) = &seg.vectors {
            ids.extend_from_slice(v.ids(&desc.label, &desc.property));
            ids.extend_from_slice(v.label_removals(&desc.label, &desc.property));
            ids.extend_from_slice(v.value_removals(&desc.label, &desc.property));
        }
    }
    ids.sort_unstable();
    ids.dedup();
    for id in ids {
        if vector_dead(gen, id)? {
            // A deleted node takes its embedding with it; its tombstone already suppresses it.
            continue;
        }
        if !vector_indexed(gen, cache, id, desc)? {
            // A candidate the sidecars name — so a level at or below still physically holds a
            // vector for it — is no longer in the index's scope (its effective label set
            // dropped the label). It must supersede that vector, not vanish: this is the same
            // silent hole as a value removal, one step over. A `continue` here (the old code)
            // swallows the sidecar's own removal, so a *consolidation* — which reads this fold,
            // not the raw sidecar union the KNN read path uses — resurfaces the vector. The
            // delta is not consulted here (it may be empty post-flush; the fact lives in the
            // node's effective label set), so any out-of-scope candidate has left the scope.
            // The candidate set is the sidecar ids ∪ removals, so this stays O(vectors touched).
            //
            // `out_of_scope`, not `removed` (HIK-122): the sidecar's `label_removals` channel
            // exists precisely because the embedding *value* survives a de-labelling (D64), and
            // filing it as a deletion is what let a consolidation destroy it. `segment_says` is
            // still asked, because a value removal at this level is the newer, winning fact and
            // really is a deletion — the node left the scope *and* the user deleted the vector.
            match segment_says(gen, id, desc)? {
                // The value is gone on its own account, labels aside. A real deletion.
                LevelSays::Gone => segments.removed.push(id),
                // `Vector` — the level's row carries the embedding inline, so the dump's
                // property walk already lands it in the column store (the node is out of scope,
                // so `suppress_indexed_vectors_named` leaves it alone). `Nothing` — no level
                // above the base holds it, so the only copy is the one D12 routed *out* of the
                // props record into the base index, and the consolidation must move it back.
                // Either way the node is not in the index and every level below is suppressed.
                LevelSays::Vector(_) | LevelSays::Nothing => segments.out_of_scope.push(id),
            }
            continue;
        }
        match segment_says(gen, id, desc)? {
            LevelSays::Vector(v) => segments.entries.push(graph_format::vectors::VectorEntry {
                node_id: id,
                vector: v,
            }),
            LevelSays::Gone => segments.removed.push(id),
            LevelSays::Nothing => {}
        }
    }
    Ok(segments)
}

/// What the **write delta** says about node `id`'s embedding — the delta alone, with no core
/// or segment fallback (that is the whole point of the level split).
fn delta_says(gen: &dyn ReadView, id: u64, desc: &VectorIndexDesc) -> LevelSays {
    let Some(nd) = gen.delta().node_patch(id) else {
        return LevelSays::Nothing;
    };
    match nd.patches.get(&desc.property) {
        Some(Value::Vector(v)) => LevelSays::Vector(v.clone()),
        // The delta named the property but not with a vector (`SET n.embedding = 5`, which the
        // write path admits — `validate_vector_dims` only constrains a `Value::Vector`). The
        // newest level says this node has no embedding, so it has none: leaving the level below
        // to keep scoring its stale vector is exactly the silent wrongness a removal exists to
        // prevent.
        Some(_) => LevelSays::Gone,
        // A replace-all that re-set the embedding is not a removal — but it took the `Vector`
        // arm above, so reaching here means it really is gone.
        None if nd.replaced || nd.removed.contains(&desc.property) => LevelSays::Gone,
        None => LevelSays::Nothing,
    }
}

/// What the **core segments** say about node `id`'s embedding, folded newest-wins across
/// segments only — the write delta above them is deliberately not applied.
///
/// The vector itself rides the node's row ([`graph_format::segvectors`]: `Value::Vector` is a
/// first-class wire type, so a fragment would be a second copy), and a flush writes the full
/// *effective* row, so the newest segment carrying the id already holds the newest vector —
/// no cross-segment fold. What the rows cannot express is a removal, and that is what the
/// `vec.meta` sidecars are for.
fn segment_says(gen: &dyn ReadView, id: u64, desc: &VectorIndexDesc) -> Result<LevelSays> {
    let stack = gen.core_stack();
    if let Some(row) = stack.resolve_node_row(id)? {
        // A tombstone row is the segments deleting the node, not un-embedding it; the caller
        // already dropped tombstoned candidates.
        if !row.tombstoned {
            match row.props.iter().find(|(k, _)| k == &desc.property) {
                Some((_, Value::Vector(v))) => return Ok(LevelSays::Vector(v.clone())),
                // Named, but not a vector — see `delta_says`.
                Some(_) => return Ok(LevelSays::Gone),
                // Absent, which is ambiguous: this row may be a node flushed for an unrelated
                // reason that still holds the base's (D12-routed, row-invisible) vector. Only
                // the sidecar below can tell.
                None => {}
            }
        }
    }
    // Newest segment first: a later re-embed would have been caught by the row read above, so a
    // segment that names this id in its removals is the last word on it.
    //
    // Only a **value** removal makes it `Gone` here (HIK-118) — this function is about the
    // embedding *value*, and says nothing about scope. For a node that is currently **in** scope
    // (`vector_indexed`), a `label_removal` naming this id has already been un-done by a
    // re-label — the node left and came back, and its base/older vector is the live one again.
    // For a node currently **out** of scope, the caller (`segment_level`) has already decided
    // that from the effective label set and only wants to know whether the value itself survived.
    // A value removal is different from both: the value is gone, so it stays `Gone` regardless of
    // labels. Treating a `label_removal` as `Gone` would drop the vector on a *consolidation*
    // only, silently — which is exactly HIK-122.
    let removed = stack.segments().iter().rev().any(|seg| {
        seg.vectors.as_ref().is_some_and(|v| {
            v.value_removals(&desc.label, &desc.property)
                .binary_search(&id)
                .is_ok()
        })
    });
    Ok(if removed {
        LevelSays::Gone
    } else {
        LevelSays::Nothing
    })
}

/// Thread-safe read of node `id`'s value for property `key` (or `Null` if absent),
/// decoding only the requested key from the cached record. The free-fn body behind
/// [`Engine::node_prop`]; used by the parallel anchor filter ([`node_ok_par`]).
///
/// A *column* read, so D12 applies: an indexed embedding reads as `Null`. The vector
/// paths (KNN, the consolidation dump) want the embedding itself and read through
/// [`node_prop_raw`] instead.
fn node_prop_par(gen: &dyn ReadView, cache: &BlockCache, id: u64, key: &str) -> Result<Val> {
    let v = node_prop_raw(gen, cache, id, key)?;
    suppress_indexed_vector(gen, cache, id, key, v)
}

/// The value actually stored at the winning level for `(id, key)` — delta patch over
/// segment row over base record — with no D12 suppression. See [`node_prop_par`].
fn node_prop_raw(gen: &dyn ReadView, cache: &BlockCache, id: u64, key: &str) -> Result<Val> {
    // Writable-layer overlay (Phase 1c): a live delta patch on this property wins
    // last-writer-wins over the core value, and can introduce a property the core
    // never had. The empty-delta fast path skips the whole probe.
    let delta = gen.delta();
    if !delta.is_empty() {
        if let Some(nd) = delta.node_patch(id) {
            if let Some(v) = nd.patches.get(key) {
                return Ok(Val::from_value(v.clone()));
            }
            // Under a replace-all, or when this property was `REMOVE`d, the core value is
            // gone. The anchor business key is the exception — it survives, seeded from
            // the delta identity — so never read a core block for a dropped property.
            if nd.replaced || nd.removed.contains(key) {
                if let Some((_, kname, kval)) = delta.node_identity_by_dense(id) {
                    if kname.as_str() == key {
                        return Ok(Val::from_value(kval));
                    }
                }
                return Ok(Val::Null);
            }
        }
    }
    // Core stack (below the delta, above the base): a segment full row wins over the base.
    // The delta patch/replace/remove above already took precedence, so reaching here means
    // the delta did not decide this key.
    if let Some(row) = gen.core_stack().resolve_node_row(id)? {
        if row.tombstoned {
            return Ok(Val::Null);
        }
        return Ok(row
            .props
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| Val::from_value(v.clone()))
            .unwrap_or(Val::Null));
    }
    // A delta-born node (Phase 2c) with no segment row has no core row: its only non-patch
    // property is the business key, recovered from the delta identity. Never read a core
    // block for a synthetic id.
    if !gen.delta().is_empty() && id >= gen.core_generation().node_count() {
        if let Some((_, kname, kval)) = gen.delta().node_identity_by_dense(id) {
            if kname.as_str() == key {
                return Ok(Val::from_value(kval));
            }
        }
        return Ok(Val::Null);
    }
    let Some(key_id) = gen.property_key_id(key) else {
        return Ok(Val::Null);
    };
    let rec = cache.record(
        gen.node_props().inner(),
        gen.uuid(),
        FileKind::NodeProps,
        id,
    )?;
    Ok(columns::decode_one(&rec, key_id)?
        .map(Val::from_value)
        .unwrap_or(Val::Null))
}

/// Last-writer-wins insert of `(name, value)` into a name-space property list: an
/// existing entry for `name` is overwritten, otherwise the pair is appended. The
/// overlay fold ([`Engine::overlay_node_props`]) uses this for both patched and
/// business-key properties.
fn overlay_named(named: &mut NamedProps, name: &str, value: Val) {
    if let Some(slot) = named.iter_mut().find(|(k, _)| k == name) {
        slot.1 = value;
    } else {
        named.push((name.to_string(), value));
    }
}

/// Thread-safe counterpart to [`Engine::node_ok`] for the parallel anchor filter
/// (Task 10): whether node `id` satisfies the anchor's `label_expr` and inline
/// properties, touching only the Sync `gen`/`cache`. Inline property **values**
/// (`wants`) are pre-evaluated once single-threaded by the caller against the row
/// binding — they don't depend on `id`, and their evaluation may route through the
/// !Sync executor (`eval`/`regex_cache`), which workers must not — so only the
/// per-candidate column/label reads and the `loose_eq` comparison run here.
/// `guaranteed` lists label ids the anchor scan already proved (see
/// [`Engine::scan_guaranteed_labels`]); they skip the label-record decode exactly as
/// the sequential path does, so the accept/reject decision is byte-for-byte identical.
fn node_ok_par(
    gen: &dyn ReadView,
    cache: &BlockCache,
    id: u64,
    label_expr: Option<&LabelExpr>,
    wants: &[(&str, Val)],
    guaranteed: &[u32],
) -> Result<bool> {
    if let Some(expr) = label_expr {
        if let Some(atom) = expr.as_single_atom() {
            match gen.label_id(atom) {
                Some(lid) if guaranteed.contains(&lid) => {}
                Some(lid) => {
                    if !node_label_ids_par(gen, cache, id)?.contains(&lid) {
                        return Ok(false);
                    }
                }
                None => return Ok(false),
            }
        } else {
            let have = node_label_ids_par(gen, cache, id)?;
            let ok = expr.eval(&|name| {
                gen.label_id(name)
                    .is_some_and(|lid| guaranteed.contains(&lid) || have.contains(&lid))
            });
            if !ok {
                return Ok(false);
            }
        }
    }
    for (k, want) in wants {
        let got = node_prop_par(gen, cache, id, k)?;
        if got.loose_eq(want) != Some(true) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Thread-safe edge-property read — the free-fn body of [`Engine::edge_prop`],
/// touching only the Sync `gen`/`cache`. Used by [`property_val`] (and thus the
/// parallel aggregation precompute, Task 12) and by `Engine::edge_prop`, so the two
/// stay byte-for-byte identical.
fn edge_prop_par(gen: &dyn ReadView, cache: &BlockCache, id: u64, key: &str) -> Result<Val> {
    if !gen.delta().is_empty() {
        // A delta patch on this key wins over any core/segment value (`SET r.p`, or a
        // delta-born edge whose properties live only in the delta).
        if let Some(v) = gen.delta().edge_patch_value(id, key) {
            return Ok(Val::from_value(v));
        }
    }
    // Core stack: a segment full row for the edge wins over the base.
    if let Some(row) = gen.core_stack().resolve_edge_row(id)? {
        if row.tombstoned {
            return Ok(Val::Null);
        }
        return Ok(row
            .props
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| Val::from_value(v.clone()))
            .unwrap_or(Val::Null));
    }
    // A delta-born edge with no segment row has no core record — only the delta (already
    // consulted above) holds its properties, so any other key is absent.
    if !gen.delta().is_empty() && id >= gen.core_generation().edge_count() {
        return Ok(Val::Null);
    }
    let Some(key_id) = gen.property_key_id(key) else {
        return Ok(Val::Null);
    };
    let rec = cache.record(
        gen.edge_props().inner(),
        gen.uuid(),
        FileKind::EdgeProps,
        id,
    )?;
    Ok(columns::decode_one(&rec, key_id)?
        .map(Val::from_value)
        .unwrap_or(Val::Null))
}

/// Thread-safe property access — the free-fn body of [`Engine::property`], reading
/// only the Sync `gen`/`cache`. Node/Rel reads route through the block cache; the
/// Map/Point/temporal/Null arms are pure value logic. `Engine::property` delegates
/// here, and the parallel aggregation precompute ([`eval_simple`], Task 12) calls it
/// directly, so the two paths produce identical `Val`s for the same input.
fn property_val(gen: &dyn ReadView, cache: &BlockCache, base: &Val, key: &str) -> Result<Val> {
    match base {
        Val::Node(id) => node_prop_par(gen, cache, *id, key),
        Val::Rel { id, .. } => edge_prop_par(gen, cache, *id, key),
        Val::Map(m) => Ok(m
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or(Val::Null)),
        Val::Point {
            latitude,
            longitude,
        } => Ok(match key {
            "latitude" => Val::Float(*latitude),
            "longitude" => Val::Float(*longitude),
            _ => Val::Null,
        }),
        Val::Date(s) => temporal::date_component(*s, key, false)
            .map(Val::Int)
            .ok_or_else(|| anyhow::anyhow!("unknown date component {key}")),
        Val::Time(s) => temporal::time_component(*s, key)
            .map(Val::Int)
            .ok_or_else(|| anyhow::anyhow!("unknown time component {key}")),
        Val::DateTime(s) => temporal::date_component(*s, key, true)
            .map(Val::Int)
            .ok_or_else(|| anyhow::anyhow!("unknown datetime component {key}")),
        Val::Duration(s) => temporal::duration_component(*s, key)
            .map(Val::Float)
            .ok_or_else(|| anyhow::anyhow!("unknown duration component {key}")),
        Val::Null => Ok(Val::Null),
        other => bail!("type {} has no property '{key}'", other.to_display()),
    }
}

/// Whether `expr` can be evaluated over a single row using only the Sync
/// `gen`/`cache`/`params` — no `!Sync` executor state (no regex `=~`, no budget
/// `charge`, no nested matching). Exactly the forms [`eval_simple`] handles: a
/// bound variable, a literal, a parameter, or a one-level property read `var.key`.
/// The parallel aggregation precompute (Task 12) is gated on every group key and
/// aggregate argument being simple-readable.
fn simple_readable(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Param(_) | Expr::Var(_) => true,
        Expr::Property(base, _) => matches!(&**base, Expr::Var(_)),
        _ => false,
    }
}

/// Evaluate a [`simple_readable`] expression over one row (`cols`/`row`) using only
/// the Sync `gen`/`cache`/`params` — the worker-thread counterpart to the restricted
/// slice of [`Engine::eval`] that the parallel aggregation precompute needs. It
/// shares [`property_val`] with `Engine::eval`, and its variable / literal /
/// parameter arms mirror `Engine::eval` exactly, so it returns a byte-for-byte
/// identical [`Val`] for the same expression and row. A non-simple form is a caller
/// bug (the call site is gated by [`simple_readable`]) and bails rather than panics.
fn eval_simple(
    gen: &dyn ReadView,
    cache: &BlockCache,
    params: &HashMap<String, Val>,
    cols: &[String],
    row: &[Val],
    expr: &Expr,
) -> Result<Val> {
    match expr {
        Expr::Literal(v) => Ok(Val::from_value(v.clone())),
        Expr::Param(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("parameter ${name} was not supplied")),
        Expr::Var(name) => cols
            .iter()
            .position(|c| c == name)
            .map(|i| row[i].clone())
            .ok_or_else(|| anyhow::anyhow!("variable '{name}' is not in scope")),
        Expr::Property(base, key) => {
            let b = eval_simple(gen, cache, params, cols, row, base)?;
            property_val(gen, cache, &b, key)
        }
        _ => bail!("eval_simple called on a non-simple expression"),
    }
}

/// One projected output item in the parallel aggregation plan ([`Engine::project_aggregated_par`]).
/// `slot` indexes the per-row precomputed value buffer (`cells[row][slot]`).
pub(crate) enum AggItem {
    /// A non-aggregate (grouping-key) item; its value is `cells[row][slot]`.
    Group { slot: usize },
    /// `count(*)` — the group's row count, no per-row read.
    CountStar,
    /// A single-argument aggregate (`count`/`sum`/`avg`/`min`/`max`/`collect`/
    /// `stdev`/`stdevp`, optionally `DISTINCT`); its argument is `cells[row][slot]`.
    Agg {
        name: String,
        distinct: bool,
        slot: usize,
    },
}

/// Plan a [`project_aggregated`](Engine::project_aggregated) over the parallel
/// precompute path, or return `None` to fall back to the sequential body. Eligible
/// iff every output item is either a [`simple_readable`] grouping expression or a
/// **bare** aggregate call (not an aggregate nested in a larger expression) over a
/// simple-readable argument — `count(*)` or a single-argument aggregate. Two-argument
/// `percentile*` is excluded. Returns the list of per-row slot expressions (group
/// keys and aggregate arguments, in first-seen order) and the per-item plan.
fn plan_par_aggregation(items: &[(Expr, String)]) -> Option<(Vec<&Expr>, Vec<AggItem>)> {
    let mut slots: Vec<&Expr> = Vec::new();
    let mut plan: Vec<AggItem> = Vec::with_capacity(items.len());
    for (e, _) in items {
        if contains_aggregate(e) {
            // Only a bare aggregate function call (e.g. `count(n.x)`), never an
            // aggregate inside an expression (`count(n.x) + 1`): the bare form lets
            // the merge use the computed value directly, mirroring `eval` returning
            // the cursor value for an aggregate-function node.
            let Expr::Function {
                name,
                distinct,
                args,
            } = e
            else {
                return None;
            };
            if !is_aggregate(name) {
                return None;
            }
            let lname = name.to_lowercase();
            match args {
                FuncArgs::Star => {
                    if lname != "count" {
                        return None;
                    }
                    plan.push(AggItem::CountStar);
                }
                FuncArgs::Args(a) => {
                    // Single-argument aggregates only (excludes `percentile*`, which
                    // take a second constant argument handled by `compute_aggregate`).
                    if a.len() != 1
                        || matches!(lname.as_str(), "percentilecont" | "percentiledisc")
                        || !simple_readable(&a[0])
                    {
                        return None;
                    }
                    let slot = slots.len();
                    slots.push(&a[0]);
                    plan.push(AggItem::Agg {
                        name: lname,
                        distinct: *distinct,
                        slot,
                    });
                }
            }
        } else {
            if !simple_readable(e) {
                return None;
            }
            let slot = slots.len();
            slots.push(e);
            plan.push(AggItem::Group { slot });
        }
    }
    Some((slots, plan))
}

/// Reduce a group's collected (null-dropped, optionally deduped) values to the
/// aggregate result. Shared by the sequential [`Engine::compute_aggregate`] and the
/// parallel-precompute path (Task 12) so both produce identical results. The
/// two-argument `percentile*` aggregates are handled by the caller (they carry a
/// constant percentile), so they never reach here.
fn reduce_agg(lname: &str, vals: Vec<Val>) -> Result<Val> {
    Ok(match lname {
        "count" => Val::Int(vals.len() as i64),
        "collect" => Val::List(vals),
        "sum" => sum(&vals)?,
        "avg" => avg(&vals)?,
        "min" => vals
            .into_iter()
            .reduce(|a, b| if a.cmp_total(&b).is_le() { a } else { b })
            .unwrap_or(Val::Null),
        "max" => vals
            .into_iter()
            .reduce(|a, b| if a.cmp_total(&b).is_ge() { a } else { b })
            .unwrap_or(Val::Null),
        "stdev" => std_dev(&vals, true)?,
        "stdevp" => std_dev(&vals, false)?,
        other => bail!("unknown aggregate function '{other}'"),
    })
}

/// Minimum input-row count below which [`project_aggregated`](Engine::project_aggregated)
/// runs the sequential per-row eval — the rayon fan-out overhead isn't worth it for a
/// small table. Above it (with a fanout pool and an eligible shape), the group-key and
/// aggregate-argument reads gather on the shared fanout pool.
const AGG_PAR_MIN: usize = 64;

/// Minimum scanned-candidate count below which the anchor `node_ok` filter (Task 10)
/// runs sequentially — the rayon fan-out overhead isn't worth it for a narrow scan.
/// Above it, the per-candidate label/property reads gather on the shared fanout pool.
const SCAN_PAR_MIN: usize = 64;

/// Minimum selected-node count below which `algo.*` subgraph construction
/// ([`Engine::build_view`], Task 11) reads node adjacency sequentially — the rayon
/// fan-out overhead isn't worth it for a tiny view. Above it, the per-node
/// out-adjacency reads gather on the shared fanout pool.
const BUILD_VIEW_PAR_MIN: usize = 64;

/// Node batch size for [`Engine::build_view`]'s chunked adjacency gather: the view is
/// filled a chunk at a time so the retained edge count is charged against
/// `maxIntermediate` incrementally and the deadline is checked as it fills, rather
/// than materialising the whole subgraph's adjacency in one uninterruptible gather.
/// Large enough that the per-chunk pool dispatch stays amortised.
const BUILD_VIEW_CHUNK: usize = 65_536;

/// First window an anchor sweep ([`CandidateStream`]) pulls from the id space, in dense
/// node ids. Deliberately small: a pushed `LIMIT 1` must not walk further into the id
/// space than it has to.
const CAND_WINDOW_MIN: u64 = 1_024;

/// Largest window an anchor sweep pulls (ids). The window ramps ×8 from
/// [`CAND_WINDOW_MIN`] to this, so an *uncapped* sweep amortises the per-window record
/// locate + block decompress back to the cost of the old single-pass scan, while the
/// scan's whole resident footprint stays bounded at 64 K ids — 512 KB — instead of one
/// `Vec<u64>` over the entire id space (733 MB on the 91.6M-node graph).
const CAND_WINDOW_MAX: u64 = 65_536;

/// Where a [`CandidateStream`]'s ids come from.
enum CandidateSrc<'a> {
    /// Ids the caller already holds (a candidate set hoisted across input rows) — handed
    /// out in windows without being copied, and already tombstone-suppressed.
    Ready(&'a [u64]),
    /// Ids this scan materialised: an id seek, an index lookup, a reltype endpoint
    /// posting, or a label scan whose segment/delta overlay needs the sorted union.
    /// Already tombstone-suppressed. Bounded by the query (a seek, a posting) rather than
    /// by the graph, which is why it may be built in one go.
    Owned(Vec<u64>),
    /// A lazy sweep of `next..end` over the dense id space: the whole space
    /// (`NodeScan::AllNodes`) when `label` is `None`, else the nodes carrying `label`
    /// (`NodeScan::LabelScan` over a pure core with no delta), decoded from the
    /// node-label column window by window.
    Sweep {
        label: Option<u32>,
        next: u64,
        end: u64,
    },
    /// A lazy **k-way merge** of several ascending id sources over the dense id space,
    /// produced window by window (HIK-104). Used where the eager path needed a *sorted union*
    /// across the base generation and the delta/segment overlays — `LabelScan` under a segment
    /// stack or write delta, and every `RelTypeScan` — which a single window cursor cannot
    /// emit. Each source is individually ascending+distinct; the merge partitions the id space
    /// into the *same* ramping windows as [`CandidateSrc::Sweep`] and, per window `[lo, hi)`,
    /// pulls only the ids each source has in that range, then `sort`+`dedup`s that bounded
    /// buffer — reproducing the eager `sort_unstable`+`dedup` exactly while keeping the resident
    /// footprint to one window. `next..end` is the undecoded id range.
    Merge {
        srcs: Vec<MergeSrc>,
        next: u64,
        end: u64,
    },
}

/// One ascending, distinct source of an anchor k-way merge ([`CandidateSrc::Merge`]).
pub(crate) enum MergeSrc {
    /// A write-bounded, already ascending+deduped id list — the segment/delta overlay of a
    /// label scan (stack label-carriers ∪ delta born/added-label ids), or one segment's
    /// endpoint posting slice. `pos` is the next unread index.
    Mat { ids: Vec<u64>, pos: usize },
    /// A lazy sweep of the base node-label column: ascending ids carrying `label`, decoded a
    /// window at a time, **minus** `exclude` (ids the segment stack overrides — their label
    /// membership is decided by a `Mat` overlay instead, mirroring `fold_label_scan`'s
    /// `retain`). The column has records only below `col_end`; higher ids are born ids the
    /// overlay supplies.
    LabelCol {
        label: u32,
        exclude: HashSet<u64>,
        col_end: u64,
    },
    /// A lazy walk of a base endpoint posting, owning only the compressed Elias–Fano form
    /// (a few bits/id, not the expanded 733 MB `Vec`). `head` is the value already pulled from
    /// the cursor that straddles into a later window (`>= hi`), held until its window arrives.
    Posting {
        iter: EndpointPostingIter,
        head: Option<u64>,
    },
}

/// A lazy, bounded-window cursor over an anchor scan's candidate node ids, drained with
/// [`Engine::next_candidates`]. The reason it exists: the anchor scan used to collect the
/// *entire* id space into one `Vec<u64>` before a single row was produced, so a pushed
/// `LIMIT` could only truncate the row loop that walked it — the 733 MB allocation had
/// already happened.
pub(crate) struct CandidateStream<'a> {
    src: CandidateSrc<'a>,
    /// Cursor into a `Ready`/`Owned` id list; unused by a sweep.
    pos: usize,
    /// The current window's ids (a sweep only). Reused across windows.
    buf: Vec<u64>,
    /// Ids the next sweep window covers; ramps to [`CAND_WINDOW_MAX`].
    window: u64,
}

impl<'a> CandidateStream<'a> {
    fn new(src: CandidateSrc<'a>) -> Self {
        Self {
            src,
            pos: 0,
            buf: Vec::new(),
            window: CAND_WINDOW_MIN,
        }
    }
    /// A lazy sweep of `0..end`, restricted to `label`'s nodes when set.
    fn sweep(label: Option<u32>, end: u64) -> Self {
        Self::new(CandidateSrc::Sweep {
            label,
            next: 0,
            end,
        })
    }
    /// A lazy k-way merge of `srcs` over the dense id space `0..end`.
    fn merge(srcs: Vec<MergeSrc>, end: u64) -> Self {
        Self::new(CandidateSrc::Merge { srcs, next: 0, end })
    }
    /// Ids this scan materialised (already tombstone-suppressed).
    fn owned(ids: Vec<u64>) -> Self {
        Self::new(CandidateSrc::Owned(ids))
    }
    /// Ids the caller holds (already tombstone-suppressed).
    fn ready(ids: &'a [u64]) -> Self {
        Self::new(CandidateSrc::Ready(ids))
    }
    /// The single node an already-bound anchor variable pins. Not a scan: nothing to
    /// suppress, since the binding came from a scan that already did.
    fn single(id: u64) -> Self {
        Self::new(CandidateSrc::Owned(vec![id]))
    }

    /// An **upper bound** on the ids this stream can yield — exact for a materialised
    /// source, the swept id range for a sweep (a label sweep yields no more than the ids
    /// it walks). Used only to decide whether the pooled anchor prefilter is worth arming.
    fn upper_bound(&self) -> usize {
        match &self.src {
            CandidateSrc::Ready(ids) => ids.len(),
            CandidateSrc::Owned(ids) => ids.len(),
            CandidateSrc::Sweep { next, end, .. } => end.saturating_sub(*next) as usize,
            // The id space the merge can still sweep — an upper bound on the distinct ids it
            // can yield (each is a node id in `next..end`), which is all the prefilter gate
            // needs.
            CandidateSrc::Merge { next, end, .. } => end.saturating_sub(*next) as usize,
        }
    }
}

/// The next `CAND_WINDOW_MAX` ids of a materialised candidate list, advancing `pos`.
fn slice_window<'s>(ids: &'s [u64], pos: &mut usize) -> Option<&'s [u64]> {
    let lo = (*pos).min(ids.len());
    let hi = lo.saturating_add(CAND_WINDOW_MAX as usize).min(ids.len());
    *pos = hi;
    (lo < hi).then(|| &ids[lo..hi])
}

/// Map `f` over `items` on the shared fanout pool (or sequentially when the pool is
/// absent or `items` is smaller than `min_batch`), preserving input order. `f` must
/// read only Sync state (&Generation/&BlockCache) — never the !Sync Engine.
fn par_gather<I: Sync, T: Send>(
    pool: Option<&rayon::ThreadPool>,
    items: &[I],
    min_batch: usize,
    f: impl Fn(&I) -> Result<T> + Sync + Send,
) -> Result<Vec<T>> {
    match pool {
        Some(p) if items.len() >= min_batch => p.install(|| items.par_iter().map(&f).collect()),
        _ => items.iter().map(&f).collect(),
    }
}

/// Assemble the walk-order node path `src → … → meet → … → dst` from a bidirectional
/// search's two neighbour maps: `fpar` is `node -> (predecessor toward src, depth)` and
/// `bpar` is `node -> (successor toward dst, depth)`.
fn bidir_node_path(
    src: u64,
    dst: u64,
    meet: u64,
    fpar: &HashMap<u64, (u64, u32)>,
    bpar: &HashMap<u64, (u64, u32)>,
) -> Vec<u64> {
    let mut nodes = vec![meet];
    let mut cur = meet;
    while cur != src {
        cur = fpar.get(&cur).expect("forward chain to src").0;
        nodes.push(cur);
    }
    nodes.reverse(); // [src, …, meet]
    let mut cur = meet;
    while cur != dst {
        cur = bpar.get(&cur).expect("backward chain to dst").0;
        nodes.push(cur);
    }
    nodes // [src, …, meet, …, dst]
}

impl Val {
    fn from_value(v: Value) -> Val {
        match v {
            Value::Null => Val::Null,
            Value::Bool(b) => Val::Bool(b),
            Value::Int(i) => Val::Int(i),
            Value::Float(f) => Val::Float(f),
            Value::Str(s) => Val::Str(s),
            Value::List(xs) => Val::List(xs.into_iter().map(Val::from_value).collect()),
            Value::Vector(v) => Val::Vector(v),
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Val::Null => 0,
            Val::Bool(_) => 1,
            Val::Int(_) | Val::Float(_) => 2,
            Val::Str(_) => 3,
            Val::List(_) => 4,
            Val::Vector(_) => 5,
            Val::Map(_) => 6,
            Val::Node(_) => 7,
            Val::Rel { .. } => 8,
            Val::Path { .. } => 9,
            Val::Point { .. } => 10,
            Val::Date(_) => 11,
            Val::Time(_) => 12,
            Val::DateTime(_) => 13,
            Val::Duration(_) => 14,
        }
    }

    /// A deterministic total order over runtime values, used by `ORDER BY`,
    /// `DISTINCT` and aggregation grouping. Numbers compare numerically; `NaN`
    /// sorts deterministically (`total_cmp`); cross-type falls back to type rank.
    fn cmp_total(&self, other: &Val) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        use Val::*;
        fn num(v: &Val) -> Option<f64> {
            match v {
                Int(i) => Some(*i as f64),
                Float(f) => Some(*f),
                _ => None,
            }
        }
        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Bool(a), Bool(b)) => a.cmp(b),
            (Str(a), Str(b)) => a.cmp(b),
            (Node(a), Node(b)) => a.cmp(b),
            (Rel { id: a, .. }, Rel { id: b, .. }) => a.cmp(b),
            (
                Path {
                    nodes: na,
                    rels: ra,
                },
                Path {
                    nodes: nb,
                    rels: rb,
                },
            ) => na.cmp(nb).then_with(|| {
                ra.iter()
                    .zip(rb)
                    .map(|(x, y)| x.cmp_total(y))
                    .find(|o| *o != Ordering::Equal)
                    .unwrap_or_else(|| ra.len().cmp(&rb.len()))
            }),
            // FalkorDB orders points by longitude first, then latitude (value.c
            // T_POINT case): `lon_diff != 0 ? lon_diff : lat_diff`.
            (
                Point {
                    latitude: la,
                    longitude: lo_a,
                },
                Point {
                    latitude: lb,
                    longitude: lo_b,
                },
            ) => lo_a.total_cmp(lo_b).then_with(|| la.total_cmp(lb)),
            // Temporals compare by their underlying `time_t` (FalkorDB compares
            // `datetimeval`); cross-type falls through to the rank ordering.
            (Date(a), Date(b))
            | (Time(a), Time(b))
            | (DateTime(a), DateTime(b))
            | (Duration(a), Duration(b)) => a.cmp(b),
            (List(a), List(b)) => a
                .iter()
                .zip(b)
                .map(|(x, y)| x.cmp_total(y))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len())),
            (Vector(a), Vector(b)) => a
                .iter()
                .zip(b)
                .map(|(x, y)| x.total_cmp(y))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len())),
            (Map(a), Map(b)) => a
                .iter()
                .zip(b)
                .map(|((ka, va), (kb, vb))| ka.cmp(kb).then_with(|| va.cmp_total(vb)))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len())),
            _ => match (num(self), num(other)) {
                (Some(a), Some(b)) => a.total_cmp(&b),
                _ => self.rank().cmp(&other.rank()),
            },
        }
    }

    /// Cypher `=`/`<>` equality: three-valued. `None` means the comparison is
    /// `null` (an operand was `null`); `Some(b)` is a definite result. Numbers
    /// compare across `Int`/`Float`; differing types are unequal (not null).
    fn loose_eq(&self, other: &Val) -> Option<bool> {
        use Val::*;
        fn num(v: &Val) -> Option<f64> {
            match v {
                Int(i) => Some(*i as f64),
                Float(f) => Some(*f),
                _ => None,
            }
        }
        if matches!(self, Null) || matches!(other, Null) {
            return None;
        }
        if let (Some(a), Some(b)) = (num(self), num(other)) {
            return Some(a == b);
        }
        Some(match (self, other) {
            (Bool(a), Bool(b)) => a == b,
            (Str(a), Str(b)) => a == b,
            (Node(a), Node(b)) => a == b,
            (Rel { id: a, .. }, Rel { id: b, .. }) => a == b,
            // Path equality: same node-id sequence and same relationship sequence
            // (FalkorDB `Path_eq` — endpoints + edges in order).
            (
                Path {
                    nodes: na,
                    rels: ra,
                },
                Path {
                    nodes: nb,
                    rels: rb,
                },
            ) => {
                na == nb
                    && ra.len() == rb.len()
                    && ra.iter().zip(rb).all(|(x, y)| x.loose_eq(y) == Some(true))
            }
            (Vector(a), Vector(b)) => a == b,
            (
                Point {
                    latitude: la,
                    longitude: lo_a,
                },
                Point {
                    latitude: lb,
                    longitude: lo_b,
                },
            ) => la == lb && lo_a == lo_b,
            // Same-type temporals are equal iff their `time_t` matches; a
            // temporal vs a different temporal type is unequal (the `_` arm).
            (Date(a), Date(b))
            | (Time(a), Time(b))
            | (DateTime(a), DateTime(b))
            | (Duration(a), Duration(b)) => a == b,
            (List(a), List(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.loose_eq(y) == Some(true))
            }
            (Map(a), Map(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b)
                        .all(|((ka, va), (kb, vb))| ka == kb && va.loose_eq(vb) == Some(true))
            }
            _ => false,
        })
    }

    fn as_num(&self) -> Option<f64> {
        match self {
            Val::Int(i) => Some(*i as f64),
            Val::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Render for string concatenation / `toString`.
    fn to_display(&self) -> String {
        match self {
            Val::Null => "null".into(),
            Val::Bool(b) => b.to_string(),
            Val::Int(i) => i.to_string(),
            Val::Float(f) => f.to_string(),
            Val::Str(s) => s.clone(),
            // FalkorDB `value.c` T_POINT: `point({latitude: %f, longitude: %f})`,
            // C `%f` ⇒ 6 fractional digits.
            Val::Point {
                latitude,
                longitude,
            } => format!("point({{latitude: {latitude:.6}, longitude: {longitude:.6}}})"),
            // Temporals render via their dedicated calendar formatters (Date
            // `YYYY-MM-DD`, Time `HH:MM:SS`, DateTime `…T…`, Duration `PnYnMnD…`).
            // All are integer-based — no f64 formatting — so the double-precision
            // `toString` concern does not arise here.
            Val::Date(s) => temporal::date_to_string(*s),
            Val::Time(s) => temporal::time_to_string(*s),
            Val::DateTime(s) => temporal::datetime_to_string(*s),
            Val::Duration(s) => temporal::duration_to_string(*s),
            other => format!("{other:?}"),
        }
    }
}

/// A property map resolved to named runtime values — what a Bolt `Node`/
/// `Relationship` structure carries (see [`Engine::node_record`]).
pub type NamedProps = Vec<(String, Val)>;

/// A grouping/distinct key: a row of values with a total order.
#[derive(Clone)]
struct GroupKey(Vec<Val>);
impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for GroupKey {}
impl PartialOrd for GroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for GroupKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .iter()
            .zip(&other.0)
            .map(|(a, b)| a.cmp_total(b))
            .find(|o| *o != std::cmp::Ordering::Equal)
            .unwrap_or_else(|| self.0.len().cmp(&other.0.len()))
    }
}

// ── Scopes ──────────────────────────────────────────────────────────────────

/// A variable→value lookup for expression evaluation. Several backings exist so
/// the same evaluator serves a matcher's binding map, a projected output row, and
/// list-comprehension element bindings without copying.
pub(crate) enum Scope<'a> {
    Empty,
    Map(&'a HashMap<String, Val>),
    Row(&'a [String], &'a [Val]),
    /// A shared chain-walk [`Frame`] (read without flattening it to a map).
    Frame(&'a Frame),
    /// Parent scope with one extra binding layered on top (list predicates).
    With(&'a Scope<'a>, &'a str, &'a Val),
    /// Two scopes; the first wins on a name clash (`ORDER BY` alias over input).
    Merge(&'a Scope<'a>, &'a Scope<'a>),
}

impl<'a> Scope<'a> {
    fn get(&self, name: &str) -> Option<Val> {
        match self {
            Scope::Empty => None,
            Scope::Map(m) => m.get(name).cloned(),
            Scope::Frame(f) => f.get(name).cloned(),
            Scope::Row(cols, row) => cols.iter().position(|c| c == name).map(|i| row[i].clone()),
            Scope::With(parent, n, v) => {
                if *n == name {
                    Some((*v).clone())
                } else {
                    parent.get(name)
                }
            }
            Scope::Merge(a, b) => a.get(name).or_else(|| b.get(name)),
        }
    }

    /// Flatten the scope chain into a name→value map. Used to seed the recursive
    /// matcher (which consumes a `HashMap`) for a pattern comprehension. Shadowing
    /// follows `get`: a `With` binding and the first arm of a `Merge` win over what
    /// they layer on top of, so they are inserted last.
    fn to_binding(&self) -> HashMap<String, Val> {
        let mut out = HashMap::new();
        self.collect_into(&mut out);
        out
    }

    fn collect_into(&self, out: &mut HashMap<String, Val>) {
        match self {
            Scope::Empty => {}
            Scope::Map(m) => {
                for (k, v) in m.iter() {
                    out.insert(k.clone(), v.clone());
                }
            }
            Scope::Row(cols, row) => {
                for (c, v) in cols.iter().zip(row.iter()) {
                    out.insert(c.clone(), v.clone());
                }
            }
            Scope::Frame(f) => f.collect_into(out),
            // Parent first, then the layered binding wins on a name clash.
            Scope::With(parent, n, v) => {
                parent.collect_into(out);
                out.insert((*n).to_string(), (*v).clone());
            }
            // `get` prefers `a`, so write `b` first and let `a` overwrite.
            Scope::Merge(a, b) => {
                b.collect_into(out);
                a.collect_into(out);
            }
        }
    }
}

// ── Public result ─────────────────────────────────────────────────────────────

/// The result of a query: named columns and their rows.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Val>>,
}

/// An intermediate relation: in-scope variable names and their bound rows.
pub(crate) struct Table {
    cols: Vec<String>,
    rows: Vec<Vec<Val>>,
}

impl Table {
    /// The seed relation: a single empty row, which `MATCH` expands.
    fn singleton() -> Table {
        Table {
            cols: Vec::new(),
            rows: vec![Vec::new()],
        }
    }
}

/// Typed executor limit violations (deadline, per-query / server-wide intermediate
/// budget, shortestPath node cap). Each variant's `Display` reproduces the exact text
/// the executor previously `bail!`ed, so message-based assertions still hold — but
/// callers such as the diagnostics failure classifier now branch on the **type**
/// (`downcast_ref::<ExecLimit>()`) instead of matching substrings, per the house
/// typed-errors standard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ExecLimit {
    #[error("query exceeded its time limit")]
    Deadline,
    #[error(
        "query exceeded the intermediate result budget of {0} elements (query.maxIntermediate)"
    )]
    IntermediateBudget(u64),
    #[error("server-wide intermediate budget of {0} elements exhausted (query.maxIntermediateGlobal) — too many concurrent memory-heavy queries")]
    GlobalBudget(u64),
    #[error("shortestPath exceeded the node cap of {0} (query.maxShortestPathExplore)")]
    ShortestPathCap(u64),
}

// ── Server-wide intermediate budget ─────────────────────────────────────────

/// Process-wide ceiling on the **sum** of all in-flight queries' intermediate
/// element charges.
///
/// The per-query [`Engine::with_max_intermediate`] budget bounds *one* query; it
/// cannot bound the aggregate, so `N` concurrent memory-heavy queries each
/// charging up to their per-query cap multiply into `N × maxIntermediate` —
/// enough to OOM a bounded-memory deployment even though every individual query
/// is within its limit. This guard closes that gap: every `Engine` charges its
/// intermediate elements against this shared counter as well as its own, and a
/// charge that would push the global total past [`limit`](Self::limit) fails the
/// query with a clean, retryable error instead of growing the heap.
///
/// Held as `Arc<GlobalIntermediateBudget>` and shared by every per-query engine.
/// A `limit` of 0 disables the guard (the counter is never touched).
pub struct GlobalIntermediateBudget {
    in_use: AtomicU64,
    peak: AtomicU64,
    limit: u64,
}

impl GlobalIntermediateBudget {
    pub fn new(limit: u64) -> Self {
        Self {
            in_use: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            limit,
        }
    }
    /// The configured ceiling (`query.maxIntermediateGlobal`); 0 = disabled.
    pub fn limit(&self) -> u64 {
        self.limit
    }
    /// Live sum of all in-flight queries' charged intermediate elements.
    pub fn in_use(&self) -> u64 {
        self.in_use.load(std::sync::atomic::Ordering::Relaxed)
    }
    /// High-water mark of [`in_use`](Self::in_use) since start.
    pub fn peak(&self) -> u64 {
        self.peak.load(std::sync::atomic::Ordering::Relaxed)
    }
    /// Charge `n` elements; returns `false` if the new global total exceeds the
    /// limit (the caller then rejects the query). The `n` stays added either way —
    /// the query refunds its whole charge on completion via [`release`](Self::release),
    /// so a rejected query's transient charge is reclaimed when it unwinds.
    fn try_charge(&self, n: u64) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        if self.limit == 0 {
            return true;
        }
        let now = self.in_use.fetch_add(n, Relaxed) + n;
        self.peak.fetch_max(now, Relaxed);
        now <= self.limit
    }
    /// Refund `n` elements when a query finishes (success or failure).
    fn release(&self, n: u64) {
        if self.limit == 0 || n == 0 {
            return;
        }
        self.in_use
            .fetch_sub(n, std::sync::atomic::Ordering::Relaxed);
    }
}

// ── Engine ─────────────────────────────────────────────────────────────────

/// Everything the delta's RW-index arm needs, carried as one unit so the epoch cannot get
/// separated from the index it cuts.
pub struct RwArm<'g> {
    /// Per-generation index holder (see [`crate::rwindex::RwIndexCache`]).
    indexes: &'g RwIndexCache,
    /// The writer's per-epoch touched-id journal — how an index catches up without re-walking
    /// the whole delta.
    journal: Arc<TouchedJournal>,
    /// The epoch of the delta snapshot this query pinned. The index is used **only** at
    /// exactly this cut.
    epoch: u64,
    cfg: RwIndexConfig,
}

/// Per-query execution context over one generation and its block cache.
pub struct Engine<'g, V: ReadView> {
    gen: &'g V,
    cache: &'g BlockCache,
    /// The vector-index pool, needed only by the `AnnMode::Vamana` arm. The
    /// brute-force arm and all non-vector queries leave it `None`.
    vec_cache: Option<&'g VectorIndexCache>,
    /// The FreshDiskANN RW-index over the write delta ([`crate::rwindex`]) — the delta arm of
    /// `db.idx.vector.queryNodes`. `None` on a read-only estate (no writable layer): the delta
    /// is then empty and there is nothing for it to index.
    rw: Option<RwArm<'g>>,
    params: HashMap<String, Val>,
    /// The subset of `params` that can key an index, projected to `Value` once so
    /// the planner can resolve `$param` predicates without re-converting per call
    /// (see `choose_node_scan`). Non-keyable params (nodes, maps, temporals) are
    /// simply absent — the planner then drops that predicate and falls back.
    plan_params: HashMap<String, Value>,
    max_rows: usize,
    deadline: Option<Instant>,
    /// Beam-search list size `L` for the Vamana arm (config `vectorQuery.beamWidth`).
    beam_width: usize,
    /// Beam-search list size `L` for the **per-segment** read-only temp indexes (HIK-113,
    /// config `vectorQuery.tempBeamWidth`). Temp indexes are small and a heavily-superseded
    /// level can under-return, so a wider `L` here is cheap insurance. `0` ⇒ use `beam_width`.
    temp_beam_width: usize,
    /// Query-wide intermediate-element budget (config `query.maxIntermediate`);
    /// 0 disables. Charged by every operation that materialises a collection
    /// (comprehensions, UNWIND, list concat, aggregate buffers, varlen paths), so
    /// a query cannot grow unbounded memory inside the `timeout_ms` window.
    max_intermediate: u64,
    budget_used: Cell<u64>,
    /// Transient walk-work budget (config `query.maxScan`); 0 disables. Charged only by
    /// the count-pushdown chain walk (adjacency reads + per-row tallies that retain
    /// nothing), routed via [`charge_walk`](Self::charge_walk). Unlike `max_intermediate`
    /// it holds no memory, so it does not touch the server-wide aggregate — it is a
    /// runaway-work backstop, with `timeout_ms` the primary governor. Per-query, touched
    /// only on the calling thread, like `budget_used`.
    max_scan: u64,
    scan_used: Cell<u64>,
    /// Dense node ids the anchor scans of this run have produced — the id space they
    /// actually touched, *not* a budget (an anchor scan is charged to neither: a point
    /// lookup must stay ~free, and `max_scan` meters walk work). It exists because
    /// "how much of the graph did the anchor scan walk?" is the only way to see, from the
    /// outside, that a pushed `LIMIT` really does stop the scan rather than truncate a row
    /// loop over an already-materialised id space. Reset per [`run`](Self::run); read via
    /// [`anchor_ids_scanned`](Self::anchor_ids_scanned).
    scanned_ids: Cell<u64>,
    /// Count-pushdown accumulator. `Some(n)` ⇒ the chain-walk emit leaves tally a
    /// completed row here and skip materialising it (the `RETURN count(*)` fast
    /// path); `None` ⇒ normal row-building. Per-query, touched only on the calling
    /// thread (the walk's parallelism is confined to `par_gather`), like
    /// `budget_used`.
    count_acc: Cell<Option<u64>>,
    /// Degree-sum terminal (count fast path): when set, the chain walk stops one hop
    /// short and, instead of expanding the final relationship, adds each penultimate
    /// frontier node's **effective out/in degree** to `count_acc` — turning a k-hop
    /// `count(endpoint)` into a (k-1)-hop walk plus an O(1)-per-node degree lookup, so
    /// the widest final hop (the hubs) is never materialised. Armed only when the
    /// pattern's final hop is a plain, unfiltered, count-only edge over a homogeneous
    /// graph with no pending node-deletes (see [`Self::degree_terminal_dir`]).
    degree_terminal: Cell<bool>,
    /// Server-wide intermediate budget shared across every concurrent query
    /// (`query.maxIntermediateGlobal`); `None` ⇒ no global guard. Charged in
    /// lock-step with `budget_used`; `global_charged` is this query's running
    /// contribution to it, refunded in full when the query ends (see `run`/`Drop`).
    global_budget: Option<&'g GlobalIntermediateBudget>,
    global_charged: Cell<u64>,
    /// Optional cap on how many nodes a single `shortestPath()` global-visited BFS
    /// may discover (config `query.maxShortestPathExplore`); 0 = unlimited. Distinct
    /// from `max_intermediate` on purpose: the BFS holds an O(V) working set with a
    /// small (compacted) constant, so its natural ceiling is the node count; this is
    /// a *dedicated* safety valve for tiny-memory deployments that does not loosen
    /// every other query's intermediate budget. Unlimited by default preserves the
    /// "AnyShortest always succeeds in O(V+E)" guarantee.
    max_shortest_path_explore: u64,
    /// Optional shared worker pool for per-query parallelism (shortestPath frontier
    /// expansion, multi-hop expansion, brute-force kNN, anchor scans, …;
    /// `query.maxFanout` > 1). `None` ⇒ sequential. Only I/O-bound Sync reads run on
    /// it; all mutation of the executor's interior-mutable caches stays on the calling
    /// thread, so they are never touched off-thread.
    fanout_pool: Option<std::sync::Arc<rayon::ThreadPool>>,
    /// Effective-degree at or above which a node's adjacency is **streamed** in bounded
    /// chunks rather than materialised whole (see [`Self::is_hub`]) — the hub cut-off that
    /// keeps a high-degree node from inflating a wide parallel gather. Defaults to
    /// [`ADJ_STREAM_THRESHOLD`]; the server sets it from `query.adjStreamThreshold`.
    adj_stream_threshold: u64,
    /// Edges per chunk handed to the streaming adjacency reader (bounds a streamed hub's
    /// live buffer). Defaults to [`ADJ_STREAM_CHUNK`]; the server sets it from
    /// `query.adjStreamChunk`.
    adj_stream_chunk: usize,
    /// Compiled user regexes, keyed by the final compile string. Engines live for
    /// one query, so this exists to stop `=~` recompiling its pattern per row.
    regex_cache: RefCell<HashMap<String, regex::Regex>>,
}

impl<V: ReadView> Drop for Engine<'_, V> {
    /// Backstop for the global-budget refund if `run` is bypassed or unwound by a
    /// panic mid-query; after a normal `run` the charge is already 0, so this is a
    /// no-op on the hot path.
    fn drop(&mut self) {
        self.release_global();
    }
}

impl<'g, V: ReadView> Engine<'g, V> {
    pub fn new(gen: &'g V, cache: &'g BlockCache) -> Self {
        Self {
            gen,
            cache,
            vec_cache: None,
            rw: None,
            params: HashMap::new(),
            plan_params: HashMap::new(),
            max_rows: usize::MAX,
            deadline: None,
            beam_width: 64,
            temp_beam_width: 0,
            max_intermediate: 0,
            budget_used: Cell::new(0),
            max_scan: 0,
            scan_used: Cell::new(0),
            scanned_ids: Cell::new(0),
            count_acc: Cell::new(None),
            degree_terminal: Cell::new(false),
            global_budget: None,
            global_charged: Cell::new(0),
            max_shortest_path_explore: 0,
            fanout_pool: None,
            adj_stream_threshold: ADJ_STREAM_THRESHOLD,
            adj_stream_chunk: ADJ_STREAM_CHUNK,
            regex_cache: RefCell::new(HashMap::new()),
        }
    }

    /// Set the hub-streaming degree cut-off and chunk size (config
    /// `query.adjStreamThreshold` / `query.adjStreamChunk`). A `chunk` of 0 is clamped to
    /// 1 so the streamer always makes progress. Defaults are [`ADJ_STREAM_THRESHOLD`] /
    /// [`ADJ_STREAM_CHUNK`].
    pub fn with_adj_stream(mut self, threshold: u64, chunk: usize) -> Self {
        self.adj_stream_threshold = threshold;
        self.adj_stream_chunk = chunk.max(1);
        self
    }

    /// Override just the hub-streaming degree cut-off (tests exercise the streaming route
    /// on a small fixture by lowering it).
    #[cfg(test)]
    pub fn with_adj_stream_threshold(mut self, threshold: u64) -> Self {
        self.adj_stream_threshold = threshold;
        self
    }

    /// Supply the vector-index pool so `AnnMode::Vamana` indexes can be served.
    pub fn with_vector_cache(mut self, vec_cache: &'g VectorIndexCache, beam_width: usize) -> Self {
        self.vec_cache = Some(vec_cache);
        self.beam_width = beam_width.max(1);
        self
    }

    /// Set the per-segment temp-index beam width (`vectorQuery.tempBeamWidth`). `0` leaves it
    /// tracking `beam_width`.
    pub fn with_temp_beam_width(mut self, temp_beam_width: usize) -> Self {
        self.temp_beam_width = temp_beam_width;
        self
    }

    /// The beam width the per-segment temp indexes search at — `tempBeamWidth` if configured,
    /// else the base `beam_width`.
    fn temp_beam_width(&self) -> usize {
        if self.temp_beam_width == 0 {
            self.beam_width
        } else {
            self.temp_beam_width
        }
    }

    /// Supply the RW-index arm: the per-generation index holder, the writer's touched-id
    /// journal, and **the epoch of the delta snapshot this engine is reading**.
    ///
    /// The epoch must come from the *same* atomic read as the delta (`DeltaWriter::
    /// delta_snapshot_at`). If it does not, the index can be advanced past the delta the query
    /// is actually overlaying — extra nodes, mismatched suppression, no error. See
    /// [`crate::rwindex`].
    pub fn with_rw_index(
        mut self,
        indexes: &'g RwIndexCache,
        journal: Arc<TouchedJournal>,
        epoch: u64,
        cfg: RwIndexConfig,
    ) -> Self {
        self.rw = Some(RwArm {
            indexes,
            journal,
            epoch,
            cfg,
        });
        self
    }

    pub fn with_params(mut self, params: HashMap<String, Val>) -> Self {
        self.plan_params = params
            .iter()
            .filter_map(|(k, v)| val_to_value(v).map(|vv| (k.clone(), vv)))
            .collect();
        self.params = params;
        self
    }

    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = max_rows;
        self
    }

    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Cap the total number of intermediate elements a query may materialise
    /// (config `query.maxIntermediate`); 0 disables the budget.
    pub fn with_max_intermediate(mut self, max_intermediate: u64) -> Self {
        self.max_intermediate = max_intermediate;
        self
    }

    /// Cap the transient walk-work a count-pushdown traversal may do (config
    /// `query.maxScan`); 0 disables it. Memory-safe to set high — see [`charge_walk`].
    pub fn with_max_scan(mut self, max_scan: u64) -> Self {
        self.max_scan = max_scan;
        self
    }

    /// Share the process-wide intermediate budget so this query's charges also
    /// count against the server-wide ceiling (`query.maxIntermediateGlobal`).
    /// Without it the query is bounded only by its per-query `maxIntermediate`.
    pub fn with_global_budget(mut self, budget: &'g GlobalIntermediateBudget) -> Self {
        self.global_budget = Some(budget);
        self
    }

    /// Cap the number of nodes a `shortestPath()` global-visited BFS may discover
    /// (config `query.maxShortestPathExplore`); 0 = unlimited (the default, which
    /// preserves the always-succeeds guarantee).
    pub fn with_max_shortest_path_explore(mut self, cap: u64) -> Self {
        self.max_shortest_path_explore = cap;
        self
    }

    /// Supply the shared worker pool for per-query parallelism (shortestPath frontier
    /// expansion, multi-hop expansion, brute-force kNN, anchor scans, …;
    /// `query.maxFanout` > 1). `None` keeps queries sequential.
    pub fn with_fanout_pool(mut self, pool: Option<std::sync::Arc<rayon::ThreadPool>>) -> Self {
        self.fanout_pool = pool;
        self
    }

    /// Total query cost charged by the last [`run`](Self::run): the sum of the
    /// *retained* intermediate elements (`budget_used`, gated by
    /// `query.maxIntermediate`) and the *transient* walk elements
    /// (`scan_used`, gated by `query.maxScan`). A materialising query charges the
    /// former; a count-pushdown walk the latter — so the sum is the engine's
    /// single "elements touched" work figure. Reset at the start of each `run`.
    pub fn cost(&self) -> u64 {
        self.budget_used.get().saturating_add(self.scan_used.get())
    }

    /// Dense node ids the anchor scans of the last [`run`](Self::run) produced — how much
    /// of the id space the scan actually walked. Bounded by a pushed `LIMIT`, because the
    /// scan is a stream ([`Engine::candidate_stream`]) and not a `Vec` over the whole graph.
    /// Reset at the start of each `run`.
    pub fn anchor_ids_scanned(&self) -> u64 {
        self.scanned_ids.get()
    }

    // ── Cached record reads (D18) ───────────────────────────────────────────
}

// ── Free helpers ──────────────────────────────────────────────────────────────

pub(crate) enum BoolOp {
    And,
    Or,
    Xor,
}

/// One row's `ORDER BY` sort key: each key expression's value paired with its
/// sort direction.
type SortKey = Vec<(Val, SortDir)>;

/// A cursor over a group's precomputed aggregate values; `eval` advances it once
/// per aggregate-function node it visits, in the same order as `collect_aggregates`.
pub(crate) struct AggCursor {
    vals: Vec<Val>,
    cur: std::cell::Cell<usize>,
}
impl AggCursor {
    fn new(vals: Vec<Val>) -> Self {
        Self {
            vals,
            cur: std::cell::Cell::new(0),
        }
    }
    fn next(&self) -> Val {
        let i = self.cur.get();
        self.cur.set(i + 1);
        self.vals.get(i).cloned().unwrap_or(Val::Null)
    }
}

/// The procedures slater can answer — `CALL dbms.procedures()` self-report. Every
/// one is read-only (slater is a read-only engine). Includes both the procedures
/// dispatched through the query engine (Phase 11) and those answered pre-parse from
/// the manifest (`db.labels`, `db.indexes`, …) and at the server level
/// (`dbms.components`): all are callable, so all are reported.
const SLATER_PROCEDURES: &[&str] = &[
    "algo.BFS",
    "algo.HarmonicCentrality",
    "algo.WCC",
    "algo.betweenness",
    "algo.labelPropagation",
    "algo.pageRank",
    "db.constraints",
    "db.idx.vector.queryNodes",
    "db.indexes",
    "db.labels",
    "db.meta.stats",
    "db.propertyKeys",
    "db.relationshipTypes",
    "dbms.components",
    "dbms.functions",
    "dbms.procedures",
];

/// Parsed `algo.*` config: `(label-id filter, reltype-id filter, raw config map)`,
/// where each filter is `None` for "all". Returned by [`Engine::parse_algo_config`].
type AlgoConfig = (Option<Vec<u32>>, Option<Vec<u32>>, Vec<(String, Val)>);

/// A filtered subgraph view for `algo.*` procedures (built by
/// [`Engine::build_view`]): the selected dense node ids in `nodes` (ascending) and
/// directed out-adjacency `out`, as 0-based indices into `nodes`.
pub(crate) struct GraphView {
    nodes: Vec<u64>,
    out: Vec<Vec<usize>>,
}

impl GraphView {
    /// The directed edges as `(from, to)` index pairs — the undirected view used by
    /// WCC's union-find.
    fn undirected_edges(&self) -> Vec<(usize, usize)> {
        let mut e = Vec::new();
        for (i, adj) in self.out.iter().enumerate() {
            for &j in adj {
                e.push((i, j));
            }
        }
        e
    }

    /// Symmetric adjacency lists (each directed edge contributes both directions) —
    /// the undirected neighbourhood used by CDLP label propagation.
    fn undirected_adj(&self) -> Vec<Vec<usize>> {
        let mut u = vec![Vec::new(); self.nodes.len()];
        for (i, adj) in self.out.iter().enumerate() {
            for &j in adj {
                u[i].push(j);
                u[j].push(i);
            }
        }
        u
    }
}

/// Whether a (lowercased) procedure name is an `algo.*` graph algorithm dispatched
/// through [`Engine::apply_algo_call`].
fn is_algo_proc(name: &str) -> bool {
    matches!(
        name,
        "algo.bfs"
            | "algo.wcc"
            | "algo.pagerank"
            | "algo.harmoniccentrality"
            | "algo.betweenness"
            | "algo.labelpropagation"
    )
}

/// The canonical output column names of an `algo.*` procedure, in the order its
/// rows carry them.
fn algo_outputs(lname: &str) -> &'static [&'static str] {
    match lname {
        "algo.bfs" => &["nodes", "edges"],
        "algo.wcc" => &["node", "componentId"],
        "algo.pagerank" => &["node", "score"],
        "algo.harmoniccentrality" => &["node", "score", "reachable"],
        "algo.betweenness" => &["node", "score"],
        "algo.labelpropagation" => &["node", "communityId"],
        _ => &[],
    }
}

/// Case-insensitive lookup into a `Val::Map`'s key/value pairs.
fn map_get_ci<'a>(map: &'a [(String, Val)], key: &str) -> Option<&'a Val> {
    map.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v)
}

/// Map a per-node grouping (each node's `roots[i]` is an arbitrary-but-stable group
/// representative) to a canonical group id per node: the smallest dense node id in
/// the group. Used for WCC `componentId` / CDLP `communityId`.
fn canonical_group_ids(nodes: &[u64], roots: &[usize]) -> Vec<i64> {
    let mut min_id: HashMap<usize, u64> = HashMap::new();
    for (i, &r) in roots.iter().enumerate() {
        let id = nodes[i];
        min_id
            .entry(r)
            .and_modify(|m| {
                if id < *m {
                    *m = id;
                }
            })
            .or_insert(id);
    }
    roots.iter().map(|r| min_id[r] as i64).collect()
}

/// The scalar + aggregate functions slater implements (lowercased canonical names),
/// self-reported by `CALL dbms.functions()`. Hand-maintained to mirror the
/// `call_function` match arms + [`is_aggregate`]; doubles as the roadmap's coverage
/// gate against FalkorDB's 144-entry `builtin_funcs.gperf`. Add a name here whenever
/// a new function arm lands.
const IMPLEMENTED_FUNCTIONS: &[&str] = &[
    // aggregates
    "avg",
    "collect",
    "count",
    "max",
    "min",
    "percentilecont",
    "percentiledisc",
    "stdev",
    "stdevp",
    "sum",
    // numeric / trig
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "ceil",
    "cos",
    "cot",
    "degrees",
    "e",
    "exp",
    "floor",
    "haversin",
    "log",
    "log10",
    "pi",
    "pow",
    "radians",
    "round",
    "sign",
    "sin",
    "sqrt",
    "tan",
    // string
    "left",
    "ltrim",
    "replace",
    "reverse",
    "right",
    "rtrim",
    "split",
    "string.join",
    "string.matchregex",
    "string.replaceregex",
    "substring",
    "tolower",
    "toupper",
    "trim",
    "lower",
    "upper",
    // conversion
    "toboolean",
    "tobooleanlist",
    "tobooleanornull",
    "tofloat",
    "tofloatlist",
    "tofloatornull",
    "tointeger",
    "tointegerlist",
    "tointegerornull",
    "tostring",
    "tostringlist",
    "tostringornull",
    // list
    "head",
    "keys",
    "last",
    "list.dedup",
    "list.insert",
    "list.insertlistelements",
    "list.remove",
    "list.sort",
    "range",
    "size",
    "tail",
    // predicate / type
    "coalesce",
    "exists",
    "isempty",
    "type",
    "typeof",
    // entity / path
    "endnode",
    "haslabels",
    "id",
    "indegree",
    "labels",
    "length",
    "nodes",
    "outdegree",
    "properties",
    "relationships",
    "startnode",
    // vector
    "similarity",
    "vec.cosinedistance",
    "vec.cosinesimilarity",
    "vec.euclideandistance",
    "vecf32",
    // spatial
    "distance",
    "point",
    // temporal
    "date",
    "duration",
    "localdatetime",
    "localtime",
    "timestamp",
    // non-deterministic (clock / RNG)
    "rand",
    "randomuuid",
];

/// `CALL dbms.procedures()` — `[name, mode]` rows, one per [`SLATER_PROCEDURES`].
fn slater_procedures() -> (Vec<String>, Vec<Vec<Val>>) {
    let cols = vec!["name".to_string(), "mode".to_string()];
    let rows = SLATER_PROCEDURES
        .iter()
        .map(|n| vec![Val::Str(n.to_string()), Val::Str("READ".to_string())])
        .collect();
    (cols, rows)
}

/// `CALL dbms.functions()` — one row per [`IMPLEMENTED_FUNCTIONS`] with FalkorDB's
/// 7-column output shape. `name` and `aggregation` are exact; the type-signature
/// columns (`return_type`, `arguments`) are reported as the permissive `"Any"`
/// rather than reproducing FalkorDB's per-function `SIType` strings (slater carries
/// no static type descriptors). `internal`/`reducible`/`variable_len` are `false`.
fn slater_functions() -> (Vec<String>, Vec<Vec<Val>>) {
    let cols = [
        "name",
        "return_type",
        "arguments",
        "internal",
        "reducible",
        "aggregation",
        "variable_len",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let rows = IMPLEMENTED_FUNCTIONS
        .iter()
        .map(|n| {
            vec![
                Val::Str(n.to_string()),
                Val::Str("Any".to_string()),
                Val::List(Vec::new()),
                Val::Bool(false),
                Val::Bool(false),
                Val::Bool(is_aggregate(n)),
                Val::Bool(false),
            ]
        })
        .collect();
    (cols, rows)
}

fn is_aggregate(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "collect"
            | "stdev"
            | "stdevp"
            | "percentilecont"
            | "percentiledisc"
    )
}

/// Whether an expression contains an aggregate function anywhere (so the
/// projection must group). Does not descend *into* an aggregate's arguments —
/// nested aggregates are not permitted.
fn contains_aggregate(e: &Expr) -> bool {
    let mut found = false;
    walk_non_agg(e, &mut |f| {
        if let Expr::Function { name, .. } = f {
            if is_aggregate(name) {
                found = true;
            }
        }
    });
    found
}

/// Collect aggregate-function nodes in pre-order, not recursing into their args.
fn collect_aggregates<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Function { name, .. } = e {
        if is_aggregate(name) {
            out.push(e);
            return;
        }
    }
    for c in children(e) {
        collect_aggregates(c, out);
    }
}

/// Visit `e` and its descendants, but treat an aggregate function as a leaf (do
/// not descend into its arguments).
fn walk_non_agg(e: &Expr, f: &mut impl FnMut(&Expr)) {
    f(e);
    if let Expr::Function { name, .. } = e {
        if is_aggregate(name) {
            return;
        }
    }
    for c in children(e) {
        walk_non_agg(c, f);
    }
}

/// The immediate sub-expressions of `e`.
fn children(e: &Expr) -> Vec<&Expr> {
    match e {
        Expr::Literal(_) | Expr::Param(_) | Expr::Var(_) => vec![],
        Expr::Property(b, _) => vec![b],
        Expr::Index(b, i) => vec![b, i],
        Expr::Slice { base, from, to } => {
            let mut v = vec![base.as_ref()];
            if let Some(f) = from {
                v.push(f);
            }
            if let Some(t) = to {
                v.push(t);
            }
            v
        }
        Expr::HasLabels(b, _) => vec![b],
        Expr::Neg(e) | Expr::Not(e) => vec![e],
        Expr::And(xs) | Expr::Or(xs) | Expr::Xor(xs) | Expr::List(xs) => xs.iter().collect(),
        Expr::Arith(_, l, r)
        | Expr::Compare(_, l, r)
        | Expr::StringOp(_, l, r)
        | Expr::In(l, r) => vec![l, r],
        Expr::IsNull(e, _) => vec![e],
        Expr::Case {
            subject,
            whens,
            els,
        } => {
            let mut v: Vec<&Expr> = Vec::new();
            if let Some(s) = subject {
                v.push(s);
            }
            for (c, t) in whens {
                v.push(c);
                v.push(t);
            }
            if let Some(e) = els {
                v.push(e);
            }
            v
        }
        Expr::Function { args, .. } => match args {
            FuncArgs::Star => vec![],
            FuncArgs::Args(a) => a.iter().collect(),
        },
        Expr::Map(entries) => entries.iter().map(|(_, e)| e).collect(),
        Expr::MapProjection { items, .. } => items
            .iter()
            .filter_map(|i| match i {
                MapProjItem::Literal(_, e) => Some(e),
                _ => None,
            })
            .collect(),
        Expr::ListPredicate {
            list, predicate, ..
        } => {
            let mut v = vec![list.as_ref()];
            if let Some(p) = predicate {
                v.push(p);
            }
            v
        }
        Expr::ListComprehension {
            list,
            predicate,
            projection,
            ..
        } => {
            let mut v = vec![list.as_ref()];
            if let Some(p) = predicate {
                v.push(p);
            }
            if let Some(p) = projection {
                v.push(p);
            }
            v
        }
        Expr::PatternComprehension {
            predicate,
            projection,
            ..
        } => {
            let mut v = vec![projection.as_ref()];
            if let Some(p) = predicate {
                v.push(p);
            }
            v
        }
        Expr::Reduce {
            acc_init,
            list,
            body,
            ..
        } => vec![acc_init.as_ref(), list.as_ref(), body.as_ref()],
        // The pattern's own inline exprs (prop maps) are not walked, matching
        // `PatternComprehension`; an EXISTS inner WHERE is.
        Expr::PatternPredicate(_) => vec![],
        Expr::Exists { predicate, .. } => predicate.iter().map(|p| p.as_ref()).collect(),
        // The inner pattern's endpoints reference bound vars, not sub-expressions
        // an aggregate could hide in (mirrors `PatternPredicate`).
        Expr::ShortestPath(_) => vec![],
    }
}

/// Interpret a value as a three-valued boolean: `Some(b)` or `None` (null/non-bool).
fn three_valued(v: &Val) -> Option<bool> {
    match v {
        Val::Bool(b) => Some(*b),
        _ => None,
    }
}

/// Whether a value is definitely TRUE (the predicate-pass test).
fn truthy(v: &Val) -> bool {
    matches!(v, Val::Bool(true))
}

/// Executor-internal view of a GQL path restrictor (`Pattern.restrictor`), with the
/// *absence* of a restrictor folded onto `Trail` — slater's historical edge-unique
/// variable-length behaviour — so `None` and explicit `TRAIL` run the identical
/// code path and existing queries are unaffected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalkMode {
    Walk,
    Trail,
    Acyclic,
    Simple,
}

fn walk_mode(r: Option<PathRestrictor>) -> WalkMode {
    match r {
        None | Some(PathRestrictor::Trail) => WalkMode::Trail,
        Some(PathRestrictor::Walk) => WalkMode::Walk,
        Some(PathRestrictor::Acyclic) => WalkMode::Acyclic,
        Some(PathRestrictor::Simple) => WalkMode::Simple,
    }
}

fn varlen_bounds(vl: &VarLength) -> (u32, u32) {
    let min = vl.min.unwrap_or(1);
    // Do NOT clamp `max` up to `min`: an explicitly inverted range (`*5..3`, or an
    // open `*20..` whose min exceeds the default hop cap) is an *empty* range, not a
    // single length-`min` walk. Every consumer (`varlen`, `select_paths`,
    // `shortestPath`) already yields nothing when `max < min`, so returning the raw
    // bounds is correct — the old `.max(min)` silently rewrote `5..3` into `5..5`.
    let max = vl.max.unwrap_or(MAX_VARLEN_HOPS);
    (min, max)
}

/// Undo a scoped `HashMap::insert` on a traversal binding frame: restore the
/// value the key held before this branch overwrote it, or remove the key if it
/// was absent. Paired with each frame insert in [`Engine::expand_chain`] /
/// [`Engine::match_single_pattern`], this gives the per-branch isolation the old
/// per-hop `binding.clone()` provided — without cloning the whole map per branch
/// (root cause 6). Restoring in LIFO order is correct even if two inserts in the
/// same hop alias the same key (a rel var and node var sharing a name).
fn restore_binding(binding: &mut HashMap<String, Val>, key: String, prev: Option<Val>) {
    match prev {
        Some(v) => {
            binding.insert(key, v);
        }
        None => {
            binding.remove(&key);
        }
    }
}

/// Reverse a relationship chain so its last node becomes the anchor: walk the
/// nodes back-to-front and flip each relationship's direction. The matched edge
/// set and every node/relationship variable binding are preserved — only the
/// traversal order changes — so results are identical (the caller guarantees no
/// path variable and no variable-length hop, where order would otherwise matter).
fn reverse_pattern(p: &Pattern) -> Pattern {
    let mut nodes: Vec<NodePat> = Vec::with_capacity(p.rels.len() + 1);
    nodes.push(p.start.clone());
    for (_, n) in &p.rels {
        nodes.push(n.clone());
    }
    let new_start = nodes.last().unwrap().clone();
    let mut new_rels = Vec::with_capacity(p.rels.len());
    for i in (0..p.rels.len()).rev() {
        let mut rel = p.rels[i].0.clone();
        rel.dir = flip_dir(rel.dir);
        new_rels.push((rel, nodes[i].clone()));
    }
    Pattern {
        path_var: None,
        start: new_start,
        rels: new_rels,
        segments: None,
        // Reversal only fires for restrictor-free, varlen-free patterns
        // (`maybe_reroot` bails on any `var_length`), so there is no restrictor to
        // carry; a restrictor pattern always has a variable-length relationship.
        restrictor: p.restrictor,
        // A selected pattern is routed to `apply_match_selected` and never reaches
        // `maybe_reroot`, so there is no selector to carry here.
        selector: None,
    }
}

/// Desugar a pattern that may contain GQL quantified groups into one or more
/// ordinary (`segments: None`) patterns whose union is equivalent. A pattern with
/// no groups returns `[clone]`. Each quantified group `((inner)){m,n}` contributes
/// one alternative per repetition count `k ∈ [m, n]`; the alternatives across all
/// segments are combined as a cartesian product so a pattern with two groups yields
/// every (k₁, k₂) length pairing.
///
/// Only finite, `m ≥ 1` bounds are supported for now; unbounded (`+`, `*`, `{m,}`)
/// and zero-length (`{0,n}`) groups are rejected with a clear message rather than
/// silently mishandled.
fn expand_quantified_pattern(p: &Pattern) -> Result<Vec<Pattern>> {
    let Some(segments) = &p.segments else {
        return Ok(vec![p.clone()]);
    };
    // Alternative `rels` chains accumulated left-to-right across segments.
    let mut chains: Vec<Vec<(RelPat, NodePat)>> = vec![Vec::new()];
    for seg in segments {
        let seg_alts: Vec<Vec<(RelPat, NodePat)>> = match seg {
            Segment::Hop(rel, node) => vec![vec![(rel.clone(), node.clone())]],
            Segment::Quantified {
                inner,
                bounds,
                exit,
            } => {
                let min = bounds.min.unwrap_or(0);
                let Some(max) = bounds.max else {
                    bail!(
                        "an unbounded quantified path pattern ('+', '*' or '{{m,}}') is not yet \
                         supported; use a finite upper bound such as {{1,5}}"
                    );
                };
                if min < 1 {
                    bail!(
                        "a quantified path pattern with a lower bound below 1 ('{{0,n}}', '*') \
                         is not yet supported; use {{1,n}}"
                    );
                }
                if max < min {
                    bail!("quantified path pattern upper bound {max} is below lower bound {min}");
                }
                if (max as usize).saturating_mul(inner.len()) > QUANT_MAX_UNROLL {
                    bail!(
                        "quantified path pattern unrolls to more than {QUANT_MAX_UNROLL} hops; \
                         tighten the bounds"
                    );
                }
                (min..=max).map(|k| repeat_inner(inner, k, exit)).collect()
            }
        };
        let mut next = Vec::with_capacity(chains.len() * seg_alts.len());
        for c in &chains {
            for a in &seg_alts {
                let mut nc = c.clone();
                nc.extend(a.iter().cloned());
                next.push(nc);
            }
        }
        chains = next;
    }
    Ok(chains
        .into_iter()
        .map(|rels| Pattern {
            path_var: None,
            start: p.start.clone(),
            rels,
            segments: None,
            // A restrictor or selector over a quantified group is rejected at
            // lowering, so a segment-bearing pattern never carries one to desugar.
            restrictor: None,
            selector: None,
        })
        .collect())
}

/// `k` (≥1) copies of a quantified group's inner relationship chain, with every
/// intermediate node and relationship variable anonymised (group-internal bindings
/// aren't exposed) and the final node replaced by `exit` (the node written after
/// the group). Node labels/properties on inner nodes are preserved so per-hop
/// constraints still apply; only the variable name is dropped.
fn repeat_inner(inner: &[(RelPat, NodePat)], k: u32, exit: &NodePat) -> Vec<(RelPat, NodePat)> {
    let total = inner.len() * k as usize;
    let mut out = Vec::with_capacity(total);
    for _copy in 0..k {
        for (rel, node) in inner {
            let mut rel = rel.clone();
            rel.var = None;
            let is_last = out.len() + 1 == total;
            let node = if is_last {
                exit.clone()
            } else {
                NodePat {
                    var: None,
                    label_expr: node.label_expr.clone(),
                    props: node.props.clone(),
                }
            };
            out.push((rel, node));
        }
    }
    out
}

/// Cartesian product of per-pattern alternatives: given each source pattern's list
/// of desugared expansions, produce every conjunctive pattern-list (one expansion
/// chosen per source pattern). With no quantified patterns each inner list is a
/// singleton, so the result is the single original pattern-list.
fn cartesian_patterns(alts: &[Vec<Pattern>]) -> Vec<Vec<Pattern>> {
    let mut combos: Vec<Vec<Pattern>> = vec![Vec::new()];
    for alt in alts {
        let mut next = Vec::with_capacity(combos.len() * alt.len().max(1));
        for c in &combos {
            for p in alt {
                let mut nc = c.clone();
                nc.push(p.clone());
                next.push(nc);
            }
        }
        combos = next;
    }
    combos
}

fn flip_dir(d: Direction) -> Direction {
    match d {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Undirected => Direction::Undirected,
    }
}

/// Resolve a (possibly negative) index against a collection length.
/// Resolve a `[start..end]` slice into a sub-slice, mirroring FalkorDB `AR_SLICE`:
/// negative bounds count from the end (a start below 0 clamps to 0, an end above
/// the length clamps to the length), and a non-positive width yields `&[]`.
fn slice_range<T>(xs: &[T], start: i64, end: i64) -> &[T] {
    let len = xs.len() as i64;
    // A negative bound counts from the end: `len - |bound|`, which is just
    // `len + bound`. Saturating, because `|i64::MIN|` is not an `i64` — `.abs()`
    // panicked on `xs[-9223372036854775808..]` in a debug build and wrapped in a
    // release one. Saturation is exactly right *here* (unlike in `arith`, where a
    // saturated answer would be a lie): both bounds are immediately clamped into
    // `0..=len` anyway, so a bound that saturates lands on the same clamp it would
    // have reached with infinite-precision arithmetic.
    let mut s = if start < 0 {
        len.saturating_add(start)
    } else {
        start
    };
    if s < 0 {
        s = 0;
    }
    let mut e = if end < 0 {
        len.saturating_add(end)
    } else {
        end
    };
    if e > len {
        e = len;
    }
    if e <= s {
        return &[];
    }
    &xs[s as usize..e as usize]
}

fn list_index(len: usize, i: i64) -> Option<usize> {
    // Saturating for the same reason as `slice_range`: `len as i64 + i64::MIN`
    // overflows, and an out-of-range index is `None` either way — so saturating to
    // `i64::MIN` reaches the same `None` without wrapping (release) or panicking
    // (debug). `xs[-9223372036854775808]` is simply out of range.
    let idx = if i < 0 {
        (len as i64).saturating_add(i)
    } else {
        i
    };
    if idx < 0 || idx as usize >= len {
        None
    } else {
        Some(idx as usize)
    }
}

fn is_temporal(v: &Val) -> bool {
    matches!(
        v,
        Val::Date(_) | Val::Time(_) | Val::DateTime(_) | Val::Duration(_)
    )
}

/// The underlying `time_t` of any temporal `Val` (0 for non-temporals).
fn temporal_secs(v: &Val) -> i64 {
    match v {
        Val::Date(s) | Val::Time(s) | Val::DateTime(s) | Val::Duration(s) => *s,
        _ => 0,
    }
}

/// Wrap a `time_t` back into the non-duration temporal `Val` of kind `k`.
fn rewrap_temporal(k: TKind, s: i64) -> Val {
    match k {
        TKind::Date => Val::Date(s),
        TKind::Time => Val::Time(s),
        TKind::DateTime => Val::DateTime(s),
    }
}

fn temporal_kind(v: &Val) -> Option<TKind> {
    match v {
        Val::Date(_) => Some(TKind::Date),
        Val::Time(_) => Some(TKind::Time),
        Val::DateTime(_) => Some(TKind::DateTime),
        _ => None,
    }
}

/// `+`/`-` where at least one operand is a temporal. Only `temporal ± duration`
/// and `duration ± duration` are defined (FalkorDB `temporal_arithmetic.c`).
fn temporal_arith(op: BinOp, a: &Val, b: &Val) -> Result<Val> {
    match op {
        BinOp::Add => match (a, b) {
            (Val::Duration(x), Val::Duration(y)) => {
                Ok(Val::Duration(temporal::add_durations(*x, *y, false)?))
            }
            // temporal + duration (either order)
            (Val::Duration(d), t) | (t, Val::Duration(d)) if temporal_kind(t).is_some() => {
                let k = temporal_kind(t).unwrap();
                Ok(rewrap_temporal(
                    k,
                    temporal::add_duration(k, temporal_secs(t), *d, false),
                ))
            }
            _ => bail!("'{}' and '{}' cannot be added", type_name(a), type_name(b)),
        },
        BinOp::Sub => match (a, b) {
            (Val::Duration(x), Val::Duration(y)) => {
                Ok(Val::Duration(temporal::add_durations(*x, *y, true)?))
            }
            // temporal - duration only (duration - temporal is invalid)
            (t, Val::Duration(d)) if temporal_kind(t).is_some() => {
                let k = temporal_kind(t).unwrap();
                Ok(rewrap_temporal(
                    k,
                    temporal::add_duration(k, temporal_secs(t), *d, true),
                ))
            }
            _ => bail!(
                "'{}' and '{}' cannot be subtracted",
                type_name(a),
                type_name(b)
            ),
        },
        _ => bail!(
            "operator not supported between '{}' and '{}'",
            type_name(a),
            type_name(b)
        ),
    }
}

// ── Temporal constructor map extraction (FalkorDB time_funcs.c) ──────────────

/// `_get_component`: a case-insensitive integer component in `[min, max]`.
/// `Ok(None)` = absent; `Err` = wrong type or out of range.
fn temporal_int(m: &[(String, Val)], key: &str, min: i64, max: i64) -> Result<Option<i64>> {
    match m.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)) {
        None => Ok(None),
        Some((_, Val::Int(v))) => {
            if *v < min || *v > max {
                bail!("Invalid value for {key} (valid values {min} - {max})");
            }
            Ok(Some(*v))
        }
        Some(_) => bail!("{key} must be an integer value"),
    }
}

/// `_duration_get_component`: a case-insensitive numeric (int or float) component.
fn temporal_num(m: &[(String, Val)], key: &str) -> Result<Option<f64>> {
    match m.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)) {
        None => Ok(None),
        Some((_, v)) => match v.as_num() {
            Some(n) => Ok(Some(n)),
            None => bail!("{key} must be a numerical value"),
        },
    }
}

/// `date({...})` (FalkorDB `AR_DATE` map branch): build from y/m/d, ISO week, or
/// quarter. `year` is mandatory; week and quarter forms are mutually exclusive
/// with the plain month/day form.
fn build_date(m: &[(String, Val)]) -> Result<Val> {
    let year = temporal_int(m, "year", -999_999_999, 999_999_999)?
        .ok_or_else(|| anyhow::anyhow!("year must be specified"))?;
    let quarter = temporal_int(m, "quarter", 1, 4)?;
    let day_of_quarter = temporal_int(m, "dayOfQuarter", 1, 92)?;
    let month = temporal_int(m, "month", 1, 12)?;
    let week = temporal_int(m, "week", 1, 53)?;
    let day = temporal_int(m, "day", 1, 31)?;
    let day_of_week = temporal_int(m, "dayOfWeek", 1, 7)?;

    let recognized = 1 + [quarter, day_of_quarter, month, week, day, day_of_week]
        .iter()
        .filter(|c| c.is_some())
        .count();
    if m.len() > recognized {
        bail!("date components map contains an unknown key");
    }

    let secs = if let Some(week) = week {
        temporal::date_from_week(year as i32, week, day_of_week.unwrap_or(1))
    } else if quarter.is_some() || day_of_quarter.is_some() {
        temporal::date_from_quarter(
            year as i32,
            quarter.unwrap_or(1),
            day_of_quarter.unwrap_or(1),
        )
    } else {
        temporal::date_from_components(year as i32, month.unwrap_or(1) as u32, day.unwrap_or(1))
    };
    Ok(Val::Date(secs))
}

/// `localtime({...})` (FalkorDB `AR_LOCALTIME` map branch). `hour` is mandatory;
/// sub-second fields are validated but dropped (whole-second storage).
fn build_time(m: &[(String, Val)]) -> Result<Val> {
    let hour = temporal_int(m, "hour", 0, 23)?;
    let minute = temporal_int(m, "minute", 0, 59)?;
    let second = temporal_int(m, "second", 0, 59)?;
    let milli = temporal_int(m, "millisecond", 0, 999)?;
    let micro = temporal_int(m, "microsecond", 0, 999_999)?;
    let nano = temporal_int(m, "nanosecond", 0, 999_999_999)?;

    let recognized = [hour, minute, second, milli, micro, nano]
        .iter()
        .filter(|c| c.is_some())
        .count();
    let hour = hour.ok_or_else(|| anyhow::anyhow!("hour must be specified"))?;
    if minute.is_none() && second.is_some() {
        bail!("second cannot be specified without minute");
    }
    if m.len() > recognized {
        bail!("datetime components map contains an unknown key");
    }
    Ok(Val::Time(temporal::time_from_components(
        hour,
        minute.unwrap_or(0),
        second.unwrap_or(0),
    )))
}

/// `localdatetime({...})` (FalkorDB `AR_LOCALDATETIME` map branch) — a date
/// (y/m/d, ISO week, or quarter) plus an optional clock offset.
fn build_datetime(m: &[(String, Val)]) -> Result<Val> {
    let year = temporal_int(m, "year", -999_999_999, 999_999_999)?
        .ok_or_else(|| anyhow::anyhow!("year must be specified"))?;
    let quarter = temporal_int(m, "quarter", 1, 4)?;
    let day_of_quarter = temporal_int(m, "dayOfQuarter", 1, 92)?;
    let month = temporal_int(m, "month", 1, 12)?;
    let week = temporal_int(m, "week", 1, 53)?;
    let day = temporal_int(m, "day", 1, 31)?;
    let day_of_week = temporal_int(m, "dayOfWeek", 1, 7)?;
    let hour = temporal_int(m, "hour", 0, 23)?;
    let minute = temporal_int(m, "minute", 0, 59)?;
    let second = temporal_int(m, "second", 0, 59)?;
    let milli = temporal_int(m, "millisecond", 0, 999)?;
    let micro = temporal_int(m, "microsecond", 0, 999_999)?;
    let nano = temporal_int(m, "nanosecond", 0, 999_999_999)?;

    let recognized = 1 + [
        quarter,
        day_of_quarter,
        month,
        week,
        day,
        day_of_week,
        hour,
        minute,
        second,
        milli,
        micro,
        nano,
    ]
    .iter()
    .filter(|c| c.is_some())
    .count();
    if m.len() > recognized {
        bail!("datetime components map contains an unknown key");
    }

    let hms = (hour.unwrap_or(0), minute.unwrap_or(0), second.unwrap_or(0));
    let secs = if let Some(week) = week {
        temporal::datetime_from_week(year as i32, week, day_of_week.unwrap_or(1), hms)
    } else if quarter.is_some() || day_of_quarter.is_some() {
        temporal::datetime_from_quarter(
            year as i32,
            quarter.unwrap_or(1),
            day_of_quarter.unwrap_or(1),
            hms,
        )
    } else {
        temporal::datetime_from_components(
            year as i32,
            month.unwrap_or(1) as u32,
            day.unwrap_or(1),
            hms,
        )
    };
    Ok(Val::DateTime(secs))
}

/// `duration({...})` (FalkorDB `AR_DURATION` map branch) — any of
/// years/months/weeks/days/hours/minutes/seconds (numeric, fractional allowed).
fn build_duration(m: &[(String, Val)]) -> Result<Val> {
    let years = temporal_num(m, "years")?;
    let months = temporal_num(m, "months")?;
    let weeks = temporal_num(m, "weeks")?;
    let days = temporal_num(m, "days")?;
    let hours = temporal_num(m, "hours")?;
    let minutes = temporal_num(m, "minutes")?;
    let seconds = temporal_num(m, "seconds")?;

    let recognized = [years, months, weeks, days, hours, minutes, seconds]
        .iter()
        .filter(|c| c.is_some())
        .count();
    if m.len() > recognized {
        bail!("datetime components map contains an unknown key");
    }
    Ok(Val::Duration(temporal::duration_to_timet(
        years.unwrap_or(0.0),
        months.unwrap_or(0.0),
        weeks.unwrap_or(0.0),
        days.unwrap_or(0.0),
        hours.unwrap_or(0.0),
        minutes.unwrap_or(0.0),
        seconds.unwrap_or(0.0),
    )?))
}

/// The Cypher source spelling of a binary operator, for error messages.
fn binop_symbol(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        BinOp::Pow => "^",
    }
}

fn arith(op: BinOp, a: Val, b: Val) -> Result<Val> {
    if matches!(a, Val::Null) || matches!(b, Val::Null) {
        return Ok(Val::Null);
    }
    // Temporal arithmetic (FalkorDB `SIValue_Add`/`Subtract` + `temporal_arithmetic.c`):
    // `temporal ± duration → temporal`, `duration ± duration → duration`; every
    // other temporal combination is rejected (FalkorDB instead silently coerces
    // the `time_t` to a number — we error, which is friendlier and untested).
    if is_temporal(&a) || is_temporal(&b) {
        return temporal_arith(op, &a, &b);
    }
    // String concatenation and list construction for `+`.
    if let BinOp::Add = op {
        match (&a, &b) {
            (Val::Str(_), _) | (_, Val::Str(_)) => {
                return Ok(Val::Str(format!("{}{}", a.to_display(), b.to_display())))
            }
            (Val::List(xs), Val::List(ys)) => {
                let mut v = xs.clone();
                v.extend(ys.clone());
                return Ok(Val::List(v));
            }
            (Val::List(xs), _) => {
                let mut v = xs.clone();
                v.push(b);
                return Ok(Val::List(v));
            }
            (_, Val::List(ys)) => {
                let mut v = vec![a];
                v.extend(ys.clone());
                return Ok(Val::List(v));
            }
            _ => {}
        }
    }
    // Pure integer arithmetic stays integer; any float operand promotes.
    if let (Val::Int(x), Val::Int(y)) = (&a, &b) {
        let (x, y) = (*x, *y);
        // Exponentiation always yields a Float, even for integer operands
        // (`2 ^ 3` = 8.0), matching Neo4j — so it cannot overflow an `i64`.
        if let BinOp::Pow = op {
            return Ok(Val::Float((x as f64).powf(y as f64)));
        }
        // Everything else is `checked_*`: an integer result Cypher's `i64` cannot
        // represent is a clean `ArithmeticOverflow`, never a wrapped value (the
        // release profile carries no `overflow-checks`) and never a panic. Kept in
        // lockstep with `eval_binop` in `slater-scalar` (shared with the builder).
        let checked = match op {
            BinOp::Add => x.checked_add(y),
            BinOp::Sub => x.checked_sub(y),
            BinOp::Mul => x.checked_mul(y),
            BinOp::Div => {
                if y == 0 {
                    bail!("integer division by zero");
                }
                // `i64::MIN / -1` is the one overflowing division, and it panics even
                // in release (Rust always checks division overflow) — so this arm is a
                // liveness fix: `RETURN -9223372036854775808 / -1` took the process
                // down rather than returning a wrong answer.
                x.checked_div(y)
            }
            BinOp::Mod => {
                if y == 0 {
                    bail!("integer modulo by zero");
                }
                x.checked_rem(y)
            }
            BinOp::Pow => unreachable!("Pow returns Float above"),
        };
        return match checked {
            Some(v) => Ok(Val::Int(v)),
            None => bail!(ArithmeticOverflow::binary(binop_symbol(op), x, y)),
        };
    }
    match (a.as_num(), b.as_num()) {
        (Some(x), Some(y)) => Ok(Val::Float(match op {
            BinOp::Add => x + y,
            BinOp::Sub => x - y,
            BinOp::Mul => x * y,
            BinOp::Div => x / y,
            BinOp::Mod => x % y,
            BinOp::Pow => x.powf(y),
        })),
        _ => bail!(
            "cannot apply arithmetic to {} and {}",
            a.to_display(),
            b.to_display()
        ),
    }
}

fn compare(op: CmpOp, a: &Val, b: &Val) -> Val {
    match op {
        CmpOp::Eq => a.loose_eq(b).map(Val::Bool).unwrap_or(Val::Null),
        CmpOp::Ne => a.loose_eq(b).map(|e| Val::Bool(!e)).unwrap_or(Val::Null),
        _ => {
            if matches!(a, Val::Null) || matches!(b, Val::Null) {
                return Val::Null;
            }
            let Some(ord) = comparable(a, b) else {
                return Val::Null;
            };
            use std::cmp::Ordering::*;
            Val::Bool(match op {
                CmpOp::Lt => ord == Less,
                CmpOp::Le => ord != Greater,
                CmpOp::Gt => ord == Greater,
                CmpOp::Ge => ord != Less,
                _ => unreachable!(),
            })
        }
    }
}

/// Ordering for `<`/`>` etc — only for like-typed, ordered operands; otherwise
/// `None` (→ the comparison is `null`).
fn comparable(a: &Val, b: &Val) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Val::Int(_) | Val::Float(_), Val::Int(_) | Val::Float(_)) => Some(a.cmp_total(b)),
        (Val::Str(x), Val::Str(y)) => Some(x.cmp(y)),
        (Val::Bool(x), Val::Bool(y)) => Some(x.cmp(y)),
        // Temporals are ordered only against the same temporal type (FalkorDB
        // `SI_VALUES_ARE_COMPARABLE`); a mixed pair yields `null`.
        (Val::Date(x), Val::Date(y))
        | (Val::Time(x), Val::Time(y))
        | (Val::DateTime(x), Val::DateTime(y))
        | (Val::Duration(x), Val::Duration(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

// ── User-supplied regexes (`=~` / `string.matchRegEx` / `string.replaceRegEx`) ──
//
// Patterns are length-capped, built with explicit NFA / lazy-DFA size limits, and
// cached per query so a constant pattern compiles once rather than once per row.
// The regex crate is an RE2-style linear-time engine (no backtracking), so with
// compile cost and automaton size bounded, match time is bounded too.

// string.join(list, delimiter = '') -> string. Null list -> null; every list
// element must be a string (mirrors FalkorDB AR_JOIN).
fn string_join(args: &[Val]) -> Result<Val> {
    let list = match args.first() {
        Some(Val::List(xs)) => xs,
        Some(Val::Null) | None => return Ok(Val::Null),
        Some(other) => bail!("string.join() needs a list, got {}", other.to_display()),
    };
    let delim = match args.get(1) {
        None => "",
        Some(Val::Str(d)) => d.as_str(),
        Some(other) => bail!(
            "Type mismatch: expected String but was {}",
            type_name(other)
        ),
    };
    let mut parts = Vec::with_capacity(list.len());
    for v in list {
        match v {
            Val::Str(s) => parts.push(s.as_str()),
            other => bail!(
                "Type mismatch: expected String but was {}",
                type_name(other)
            ),
        }
    }
    Ok(Val::Str(parts.join(delim)))
}

fn in_list(needle: &Val, haystack: &Val) -> Val {
    match haystack {
        Val::Null => Val::Null,
        Val::List(xs) => {
            let mut saw_null = false;
            for x in xs {
                match needle.loose_eq(x) {
                    Some(true) => return Val::Bool(true),
                    Some(false) => {}
                    None => saw_null = true,
                }
            }
            if saw_null {
                Val::Null
            } else {
                Val::Bool(false)
            }
        }
        _ => Val::Null,
    }
}

/// `sum()` over integers stays an integer and **errors** if the total leaves `i64`.
///
/// It does not promote to `f64` on overflow (FalkorDB does; Neo4j raises "long
/// overflow"). Promotion would make the *result type* depend on the data — the same
/// query returning a Bolt Integer today and a Float tomorrow because one more row
/// landed — and `f64` is exact only to 2^53, so a promoted total past 2^63 is still
/// the wrong number, merely wrong with a decimal point. A total that overflows was
/// garbage before this change (it wrapped, silently, in release); failing loudly can
/// only replace a wrong answer, never a right one.
fn sum(vals: &[Val]) -> Result<Val> {
    if vals.iter().all(|v| matches!(v, Val::Int(_))) {
        let mut s = 0i64;
        for v in vals {
            if let Val::Int(i) = v {
                s = match s.checked_add(*i) {
                    Some(v) => v,
                    None => bail!(ArithmeticOverflow::aggregate("sum()", s, *i)),
                };
            }
        }
        return Ok(Val::Int(s));
    }
    let mut s = 0f64;
    for v in vals {
        match v.as_num() {
            Some(x) => s += x,
            None => bail!("sum() needs numbers, got {}", v.to_display()),
        }
    }
    Ok(Val::Float(s))
}

fn avg(vals: &[Val]) -> Result<Val> {
    if vals.is_empty() {
        return Ok(Val::Null);
    }
    let mut s = 0f64;
    for v in vals {
        match v.as_num() {
            Some(x) => s += x,
            None => bail!("avg() needs numbers, got {}", v.to_display()),
        }
    }
    Ok(Val::Float(s / vals.len() as f64))
}

/// Coerce a null-dropped value list to `f64`s, erroring on any non-number.
fn agg_nums(vals: &[Val], fname: &str) -> Result<Vec<f64>> {
    let mut xs = Vec::with_capacity(vals.len());
    for v in vals {
        match v.as_num() {
            Some(x) => xs.push(x),
            None => bail!("{fname}() needs numbers, got {}", v.to_display()),
        }
    }
    Ok(xs)
}

/// Standard deviation over null-dropped numerics. `sampled` selects the divisor:
/// `n - 1` for `stDev` (sample), `n` for `stDevP` (population). Mirrors
/// FalkorDB's `StDevGenericFinalize` — empty input (or a single value in the
/// sampled case) yields `0.0`.
fn std_dev(vals: &[Val], sampled: bool) -> Result<Val> {
    let xs = agg_nums(vals, "stDev")?;
    let count = xs.len();
    let divisor = count.saturating_sub(sampled as usize);
    if count == 0 || divisor == 0 {
        return Ok(Val::Float(0.0));
    }
    let mean = xs.iter().sum::<f64>() / count as f64;
    // (x - mean)(x + mean) = x² - mean², summed = Σ(x - mean)²; matches FalkorDB.
    let sum: f64 = xs.iter().map(|&x| (x - mean) * (x + mean)).sum();
    Ok(Val::Float((sum / divisor as f64).sqrt()))
}

/// Sort null-dropped numerics ascending; returns `None` (→ NULL result) if empty.
fn sorted_nums(vals: &[Val], fname: &str) -> Result<Option<Vec<f64>>> {
    let mut xs = agg_nums(vals, fname)?;
    if xs.is_empty() {
        return Ok(None);
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Ok(Some(xs))
}

/// `percentileCont(value, p)` — linear interpolation between closest ranks.
/// Mirrors FalkorDB `PercContFinalize`.
fn percentile_cont(vals: &[Val], p: f64) -> Result<Val> {
    let Some(xs) = sorted_nums(vals, "percentileCont")? else {
        return Ok(Val::Null);
    };
    let count = xs.len();
    let float_idx = p * (count - 1) as f64;
    let int_val = float_idx.floor();
    let fraction = float_idx - int_val;
    let index = int_val as usize;
    if fraction == 0.0 {
        return Ok(Val::Float(xs[index]));
    }
    Ok(Val::Float(
        xs[index] * (1.0 - fraction) + xs[index + 1] * fraction,
    ))
}

/// `percentileDisc(value, p)` — nearest-rank (no interpolation).
/// Mirrors FalkorDB `PercDiscFinalize`.
fn percentile_disc(vals: &[Val], p: f64) -> Result<Val> {
    let Some(xs) = sorted_nums(vals, "percentileDisc")? else {
        return Ok(Val::Null);
    };
    let idx = if p > 0.0 {
        (p * xs.len() as f64).ceil() as usize - 1
    } else {
        0
    };
    Ok(Val::Float(xs[idx]))
}

/// FalkorDB's value-type name, as returned by `typeOf()` (`SIType_ToString`).
/// Great-circle distance in metres between two WGS-84 lat/lon points, via the
/// haversine formula (FalkorDB `AR_DISTANCE`, `point_funcs.c`). Uses the same
/// Earth radius (6 378 140 m) FalkorDB does; FalkorDB accumulates in `f32` while
/// this stays in `f64`, but its `test_point.py` asserts distances only to a 10%
/// tolerance, so the difference is immaterial.
fn haversine_metres(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS: f64 = 6_378_140.0;
    let to_rad = |d: f64| d * std::f64::consts::PI / 180.0;
    let (phi1, phi2) = (to_rad(lat1), to_rad(lat2));
    let dphi = phi2 - phi1;
    let dlambda = to_rad(lon2) - to_rad(lon1);
    // a = sin²(Δφ/2) + cos φ1 · cos φ2 · sin²(Δλ/2)
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlambda / 2.0).sin().powi(2);
    // c = 2 · atan2(√a, √(1−a)); d = R · c
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS * c
}

/// A uniform random `f64` in `[0, 1)` for `rand()`.
///
/// `rand`'s `StandardUniform` for `f64` is exactly the construction this needs:
/// 53 uniformly random bits scaled by `2^-53`, so the result is uniform over
/// `[0, 1)` and **1.0 is unreachable** — which is what `rand()`'s contract, and
/// the `(0.0..1.0)` assertion in `rand_is_uniform_over_unit_interval`, require.
///
/// `rand::rng()` is a `ThreadRng`: a thread-local ChaCha12 CSPRNG seeded once
/// from the OS, so a per-row `rand()` costs no entropy syscall and no lock.
///
/// This deliberately owns no generator of its own. It used to (HIK-102 retired a
/// hand-rolled SplitMix64), and before that it sliced bits straight out of a v4
/// UUID — which is the bug worth remembering: a UUID's 128 bits are *not* all
/// random. The version nibble (byte 6) and the RFC-4122 variant bits (top two of
/// byte 8) are fixed. Taking the **low** 64 bits started at that variant byte, so
/// after `>> 11` the two most-significant mantissa bits were always `1` then `0`,
/// confining every draw to `[0.5, 0.75)` and making `WHERE rand() < 0.1`
/// unsatisfiable (HIK-74). Do not reintroduce either shortcut.
fn random_f64() -> f64 {
    use rand::Rng as _;
    rand::rng().random::<f64>()
}

/// Milliseconds since the Unix epoch for `timestamp()` (FalkorDB's `time_t`-ms).
fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn type_name(v: &Val) -> &'static str {
    match v {
        Val::Null => "Null",
        Val::Bool(_) => "Boolean",
        Val::Int(_) => "Integer",
        Val::Float(_) => "Float",
        Val::Str(_) => "String",
        Val::List(_) => "List",
        Val::Vector(_) => "Vectorf32",
        Val::Map(_) => "Map",
        Val::Node(_) => "Node",
        Val::Rel { .. } => "Edge",
        Val::Path { .. } => "Path",
        Val::Point { .. } => "Point",
        // `localtime`→`Time`, `localdatetime`→`Datetime` (FalkorDB collapses the
        // Local* enum variants onto these in `SIType_ToString`).
        Val::Date(_) => "Date",
        Val::Time(_) => "Time",
        Val::DateTime(_) => "Datetime",
        Val::Duration(_) => "Duration",
    }
}

/// Extract one embedding component from a query value: it must be a number (the
/// `vecf32`/`queryNodes` type rule) **and** finite. A `NaN`/`±inf` component survives
/// `f64::max`, collapses the norm augmentation to `0.0` while riding verbatim into the
/// raw coordinates, and is *ordered largest* by `total_cmp` — so it silently poisons the
/// index and the KNN order. The finiteness decision goes through the one shared gate
/// (`graph_format::pq::finite_f32`) so no ingest/query site can drift (HIK-134). The
/// typed [`graph_format::pq::NonFiniteEmbedding`] propagates as the `anyhow` root, so
/// callers branch on the type, never the message.
fn embed_component(index: usize, x: &Val, ctx: &str) -> Result<f32> {
    let f = x
        .as_num()
        .ok_or_else(|| anyhow::anyhow!("{ctx} elements must be numbers, got {}", x.to_display()))?;
    Ok(graph_format::pq::finite_f32(index, f as f32)?)
}

/// Coerce a value to a vector for the similarity functions: a `Vector` directly, or a
/// list of numbers (the shape an inlined literal / `$param` takes). `Ok(None)` is *not a
/// vector* (a non-list, non-vector value — the caller decides NULL vs type error);
/// `Err` is a list carrying a non-finite component, rejected through the shared gate so
/// this stays on the same finiteness path as every other ingest site (HIK-134).
fn as_vector(v: &Val) -> Result<Option<Vec<f32>>> {
    match v {
        Val::Vector(xs) => Ok(Some(xs.clone())),
        Val::List(xs) => xs
            .iter()
            .enumerate()
            .map(|(i, x)| embed_component(i, x, "vector"))
            .collect::<Result<Vec<f32>>>()
            .map(Some),
        _ => Ok(None),
    }
}

fn cmp_sort_keys(a: &[(Val, SortDir)], b: &[(Val, SortDir)]) -> std::cmp::Ordering {
    for ((va, dir), (vb, _)) in a.iter().zip(b) {
        let mut ord = va.cmp_total(vb);
        if matches!(dir, SortDir::Desc) {
            ord = ord.reverse();
        }
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn dedup_rows(rows: &mut Vec<Vec<Val>>) {
    let mut seen: BTreeSet<GroupKey> = BTreeSet::new();
    rows.retain(|r| seen.insert(GroupKey(r.clone())));
}

fn dedup_vals(vals: &mut Vec<Val>) {
    let mut seen: BTreeSet<GroupKey> = BTreeSet::new();
    vals.retain(|v| seen.insert(GroupKey(vec![v.clone()])));
}

/// Resolve a (possibly negative) list index to a forward offset, mirroring
/// FalkorDB `list_funcs.c normalize_index`. With `inclusive` the valid range
/// gains one slot at the end (so `list.insert` can append). Returns `None` when
/// the index is out of bounds.
fn normalize_index(idx: i64, len: usize, inclusive: bool) -> Option<usize> {
    let alen = len as i64 + if inclusive { 1 } else { 0 };
    if (idx < 0 && idx + alen < 0) || (idx > 0 && idx >= alen) {
        return None;
    }
    Some((if idx < 0 { alen + idx } else { idx }) as usize)
}

/// Whether `xs` already holds a value equal (by total order) to `v`.
fn list_contains(xs: &[Val], v: &Val) -> bool {
    xs.iter()
        .any(|x| x.cmp_total(v) == std::cmp::Ordering::Equal)
}

/// A mandatory integer argument (FalkorDB `SI_GET_NUMERIC`, so a float truncates).
fn num_i64(v: Option<&Val>) -> Result<i64> {
    match v {
        Some(Val::Int(i)) => Ok(*i),
        Some(Val::Float(f)) => Ok(*f as i64),
        _ => bail!("expected an integer index argument"),
    }
}

/// `list.sort(list, ascending = true)`: a sorted copy under the total order.
fn list_sort(args: &[Val]) -> Result<Val> {
    let mut xs = match args.first() {
        Some(Val::List(xs)) => xs.clone(),
        Some(Val::Null) | None => return Ok(Val::Null),
        Some(other) => bail!("list.sort() needs a list, got {}", other.to_display()),
    };
    let ascending = args.get(1).map(truthy).unwrap_or(true);
    xs.sort_by(|a, b| {
        let o = a.cmp_total(b);
        if ascending {
            o
        } else {
            o.reverse()
        }
    });
    Ok(Val::List(xs))
}

/// `list.remove(list, idx, count = 1)`: drop up to `count` consecutive elements
/// starting at `idx`. Out-of-bounds index or non-positive count returns the list
/// unchanged.
fn list_remove(args: &[Val]) -> Result<Val> {
    let xs = match args.first() {
        Some(Val::List(xs)) => xs.clone(),
        Some(Val::Null) | None => return Ok(Val::Null),
        Some(other) => bail!("list.remove() needs a list, got {}", other.to_display()),
    };
    let index = num_i64(args.get(1))?;
    let count = match args.get(2) {
        Some(v) => num_i64(Some(v))?,
        None => 1,
    };
    if count <= 0 {
        return Ok(Val::List(xs));
    }
    let Some(idx) = normalize_index(index, xs.len(), false) else {
        return Ok(Val::List(xs));
    };
    let count = (count as usize).min(xs.len() - idx);
    let mut out = Vec::with_capacity(xs.len() - count);
    out.extend_from_slice(&xs[..idx]);
    out.extend_from_slice(&xs[idx + count..]);
    Ok(Val::List(out))
}

/// `list.insert(list, idx, val, dups = true)`: insert one value at `idx`. A NULL
/// value, an out-of-bounds index, or (when `dups` is false) an already-present
/// value all return the list unchanged.
fn list_insert(args: &[Val]) -> Result<Val> {
    let xs = match args.first() {
        Some(Val::List(xs)) => xs.clone(),
        Some(Val::Null) | None => return Ok(Val::Null),
        Some(other) => bail!("list.insert() needs a list, got {}", other.to_display()),
    };
    let val = args.get(2).cloned().unwrap_or(Val::Null);
    if matches!(val, Val::Null) {
        return Ok(Val::List(xs));
    }
    let index = num_i64(args.get(1))?;
    let Some(idx) = normalize_index(index, xs.len(), true) else {
        return Ok(Val::List(xs));
    };
    let allow_dups = args.get(3).map(truthy).unwrap_or(true);
    if !allow_dups && list_contains(&xs, &val) {
        return Ok(Val::List(xs));
    }
    let mut out = Vec::with_capacity(xs.len() + 1);
    out.extend_from_slice(&xs[..idx]);
    out.push(val);
    out.extend_from_slice(&xs[idx..]);
    Ok(Val::List(out))
}

/// `list.insertListElements(list, list2, idx, dups = true)`: splice `list2` into
/// `list` at `idx`. A NULL second list or out-of-bounds index returns the first
/// list unchanged; with `dups` false, `list2` is deduped and elements already in
/// `list` are skipped.
fn list_insert_elements(args: &[Val]) -> Result<Val> {
    let a = match args.first() {
        Some(Val::List(xs)) => xs.clone(),
        Some(Val::Null) | None => return Ok(Val::Null),
        Some(other) => bail!(
            "list.insertListElements() needs a list, got {}",
            other.to_display()
        ),
    };
    let mut b = match args.get(1) {
        Some(Val::List(xs)) => xs.clone(),
        Some(Val::Null) | None => return Ok(Val::List(a)),
        Some(other) => bail!(
            "list.insertListElements() needs a list as its second argument, got {}",
            other.to_display()
        ),
    };
    let index = num_i64(args.get(2))?;
    let Some(idx) = normalize_index(index, a.len(), true) else {
        return Ok(Val::List(a));
    };
    let allow_dups = args.get(3).map(truthy).unwrap_or(true);
    if !allow_dups {
        dedup_vals(&mut b);
        b.retain(|v| !list_contains(&a, v));
    }
    let mut out = Vec::with_capacity(a.len() + b.len());
    out.extend_from_slice(&a[..idx]);
    out.extend(b);
    out.extend_from_slice(&a[idx..]);
    Ok(Val::List(out))
}

/// Materialise a [`Val::Path`] from a start node and the flattened hop sequence
/// walked to reach the end. Nodes are `start` followed by each hop's neighbour
/// (in walk order, so an intermediate node revisited by a bidirectional pattern
/// appears more than once); relationships are each hop carrying its stored
/// src→dst direction.
fn make_path(start: u64, hops: &[Hop]) -> Val {
    let mut nodes = Vec::with_capacity(hops.len() + 1);
    nodes.push(start);
    let mut rels = Vec::with_capacity(hops.len());
    for h in hops {
        rels.push(h.as_rel());
        nodes.push(h.neighbour);
    }
    Val::Path { nodes, rels }
}

/// Variables a pattern introduces that are not already in `existing`, in order.
fn collect_pattern_vars(p: &Pattern, existing: &[String], out: &mut Vec<String>) {
    let add = |v: &Option<String>, out: &mut Vec<String>| {
        if let Some(name) = v {
            if !existing.contains(name) && !out.contains(name) {
                out.push(name.clone());
            }
        }
    };
    add(&p.path_var, out);
    add(&p.start.var, out);
    for (rel, node) in &p.rels {
        add(&rel.var, out);
        add(&node.var, out);
    }
}

/// A best-effort column name for an unaliased projection item (Cypher uses the
/// source text; we reconstruct a close approximation).
fn expr_name(e: &Expr) -> String {
    match e {
        Expr::Var(v) => v.clone(),
        Expr::Property(b, k) => format!("{}.{}", expr_name(b), k),
        Expr::Param(p) => format!("${p}"),
        Expr::Function { name, args, .. } => match args {
            FuncArgs::Star => format!("{name}(*)"),
            FuncArgs::Args(a) => {
                format!(
                    "{name}({})",
                    a.iter().map(expr_name).collect::<Vec<_>>().join(", ")
                )
            }
        },
        Expr::Literal(v) => Val::from_value(v.clone()).to_display(),
        _ => "expr".to_string(),
    }
}

/// Is `e` a non-DISTINCT `count(*)`, or `count(var)` where `var` is the anchor
/// node's variable? Used by the Stage-3 count fast path (see `try_count_fast_path`).
fn is_count_of(e: &Expr, var: Option<&str>) -> bool {
    let Expr::Function {
        name,
        distinct: false,
        args,
    } = e
    else {
        return false;
    };
    if !name.eq_ignore_ascii_case("count") {
        return false;
    }
    match args {
        FuncArgs::Star => true,
        FuncArgs::Args(a) if a.len() == 1 => {
            matches!(&a[0], Expr::Var(v) if Some(v.as_str()) == var)
        }
        FuncArgs::Args(_) => false,
    }
}

/// Is `e` a non-DISTINCT `type(<relvar>)` — the reltype group key of the
/// relationship-metadata fast path?
fn is_type_of(e: &Expr, relvar: Option<&str>) -> bool {
    matches!(e,
        Expr::Function { name, distinct: false, args: FuncArgs::Args(a) }
            if name.eq_ignore_ascii_case("type")
                && a.len() == 1
                && matches!(&a[0], Expr::Var(v) if Some(v.as_str()) == relvar))
}

/// Is `e` exactly `labels(<nodevar>)[0]` — the first-label group key of the
/// label-metadata fast path? Only a literal `0` index qualifies.
fn is_first_label_of(e: &Expr, nodevar: Option<&str>) -> bool {
    let Expr::Index(base, idx) = e else {
        return false;
    };
    if !matches!(idx.as_ref(), Expr::Literal(Value::Int(0))) {
        return false;
    }
    matches!(base.as_ref(),
        Expr::Function { name, distinct: false, args: FuncArgs::Args(a) }
            if name.eq_ignore_ascii_case("labels")
                && a.len() == 1
                && matches!(&a[0], Expr::Var(v) if Some(v.as_str()) == nodevar))
}

/// Is `e` a non-DISTINCT `count(*)`, or `count(v)` where `v` is any of `bound`
/// (the variables a non-OPTIONAL MATCH binds, so `v` is never null and the count
/// equals `count(*)`)? Used by the multi-hop count-walk fast path
/// (see `try_count_walk_fast_path`).
fn is_count_star_or_var(e: &Expr, bound: &[String]) -> bool {
    let Expr::Function {
        name,
        distinct: false,
        args,
    } = e
    else {
        return false;
    };
    if !name.eq_ignore_ascii_case("count") {
        return false;
    }
    match args {
        FuncArgs::Star => true,
        FuncArgs::Args(a) if a.len() == 1 => {
            matches!(&a[0], Expr::Var(v) if bound.iter().any(|b| b == v))
        }
        FuncArgs::Args(_) => false,
    }
}

/// If `e` is a bare property access `var.p` on the anchor node's variable,
/// return the property name `p`. Used by the Stage-7 grouped-index fast path.
fn node_property(e: &Expr, var: Option<&str>) -> Option<String> {
    match e {
        Expr::Property(base, p) => match base.as_ref() {
            Expr::Var(v) if Some(v.as_str()) == var => Some(p.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// If `e` is `count(DISTINCT var.p)` on the anchor node's variable, return the
/// property name `p` (see `try_grouped_index_fast_path`).
fn count_distinct_property(e: &Expr, var: Option<&str>) -> Option<String> {
    let Expr::Function {
        name,
        distinct: true,
        args: FuncArgs::Args(a),
    } = e
    else {
        return None;
    };
    if !name.eq_ignore_ascii_case("count") || a.len() != 1 {
        return None;
    }
    node_property(&a[0], var)
}
