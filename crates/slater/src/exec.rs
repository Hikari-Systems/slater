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
struct Hop {
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
struct ChainBranch {
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
struct Frame {
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
enum AggItem {
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
enum MergeSrc {
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
struct CandidateStream<'a> {
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
enum Scope<'a> {
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
struct Table {
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

    fn node_props(&self, id: u64) -> Result<Vec<(u32, Value)>> {
        // A delta-born node (Phase 2c) has no core props record; its properties are
        // the delta patches plus the business key, folded in by `overlay_node_props`.
        if id >= self.gen.core_generation().node_count() {
            return Ok(Vec::new());
        }
        let rec = self.cache.record(
            self.gen.node_props().inner(),
            self.gen.uuid(),
            FileKind::NodeProps,
            id,
        )?;
        columns::decode_props(&rec)
    }

    fn edge_props(&self, id: u64) -> Result<Vec<(u32, Value)>> {
        let delta = self.gen.delta();
        // A delta-born edge's properties live entirely in the delta overlay. Map each
        // patch name to its property-key id; a name absent from the core symbol table has
        // no id and is dropped from this id-keyed view (still readable by name via
        // `RETURN r.p`).
        if !delta.is_empty() && id >= self.gen.core_generation().edge_count() {
            let mut out = Vec::new();
            for (name, value) in delta.edge_patches(id) {
                if let Some(kid) = self.gen.property_key_id(&name) {
                    out.push((kid, value));
                }
            }
            return Ok(out);
        }
        let rec = self.cache.record(
            self.gen.edge_props().inner(),
            self.gen.uuid(),
            FileKind::EdgeProps,
            id,
        )?;
        let mut props = columns::decode_props(&rec)?;
        // A **core** edge patched in place: fold its delta patches over the core record
        // (replace an existing key, append a new one), mirroring the node patch overlay.
        if !delta.is_empty() {
            for (name, value) in delta.edge_patches(id) {
                if let Some(kid) = self.gen.property_key_id(&name) {
                    match props.iter_mut().find(|(k, _)| *k == kid) {
                        Some(slot) => slot.1 = value,
                        None => props.push((kid, value)),
                    }
                }
            }
        }
        Ok(props)
    }

    fn node_label_ids(&self, id: u64) -> Result<Vec<u32>> {
        node_label_ids_par(self.gen, self.cache, id)
    }

    fn outgoing(&self, id: u64) -> Result<Vec<topology::Adj>> {
        read_adj_overlaid(self.gen, self.cache, id, true)
    }

    fn incoming(&self, id: u64) -> Result<Vec<topology::Adj>> {
        read_adj_overlaid(self.gen, self.cache, id, false)
    }

    /// Read a vector index group `[first_record, first_record + count)` from
    /// `vectors.f32.blk` **through the block cache** (D18) — the brute-force KNN
    /// candidate set. Each record decodes to its dense node id + full-precision
    /// vector; the group is contiguous (D10), so this touches only that index's
    /// blocks and they stay warm for repeat queries. When a fanout pool is
    /// configured and the group is at least [`KNN_PAR_MIN`], the per-record reads
    /// (cache lookup + zstd decode) gather in parallel, preserving record order.
    /// What each level above the base holds for `desc`, resolved per level — see the free fn
    /// [`vector_levels`].
    pub(crate) fn vector_levels(&self, desc: &VectorIndexDesc) -> Result<VectorLevels> {
        vector_levels(self.gen, self.cache, desc)
    }

    pub(crate) fn segment_level(&self, desc: &VectorIndexDesc) -> Result<VectorLevel> {
        segment_level(self.gen, self.cache, desc)
    }

    /// The full-precision embeddings the **sealed base index** holds for `wanted`, keyed by node
    /// id. Ids the base does not index are simply absent.
    ///
    /// The one read that recovers a vector D12 routed *out* of the props record: for a node that
    /// was in the index's scope at build time, this is the only copy in the generation, and no
    /// column read can see it. The consolidation needs it to move a de-labelled node's embedding
    /// back into the column store (HIK-122); nothing on the query path does, because the KNN arms
    /// scan the index rather than probe it by id.
    ///
    /// **Batched on purpose.** Neither arm can seek by node id — the brute-force store is in
    /// build-scan order and the `.pq` layout map is unsorted — so a probe is a scan of the index,
    /// and probing per candidate would make a bulk `REMOVE n:Doc` quadratic (candidates × index)
    /// at consolidation time. One pass per index serves the whole set, and an empty `wanted`
    /// (overwhelmingly the common case) reads nothing at all.
    pub(crate) fn base_index_vectors(
        &self,
        desc: &VectorIndexDesc,
        wanted: &HashSet<u64>,
    ) -> Result<HashMap<u64, Vec<f32>>> {
        let mut out = HashMap::new();
        if wanted.is_empty() {
            return Ok(out);
        }
        match desc.mode {
            // `vectors.f32.blk` holds the group at `[first_record, first_record + count)`, in
            // build-scan order.
            AnnMode::BruteForce => {
                for r in desc.first_record..desc.first_record + desc.count {
                    let e = read_vector(self.gen, self.cache, r)?;
                    if wanted.contains(&e.node_id) {
                        out.insert(e.node_id, e.vector);
                    }
                }
            }
            // The `.pq` side table is the layout→id map (v8: the `.vamana` record is pure
            // geometry), and the `.vamana` record's stored vector is **raw** — the ANN-space
            // transform is a navigation device applied at search time, never at rest — so it
            // is the embedding the user wrote. `node_ids` is already resident, so only the
            // matching records are read.
            AnnMode::Vamana { .. } => {
                let Some(ix) = self.gen.vamana_index(&desc.label, &desc.property) else {
                    return Ok(out);
                };
                for (ord, node_id) in ix.pq.node_ids.iter().enumerate() {
                    if !wanted.contains(node_id) {
                        continue;
                    }
                    let v = ix
                        .reader
                        .node(ord as graph_format::vamana::VamanaIndex)?
                        .vector;
                    out.insert(*node_id, v);
                }
            }
        }
        Ok(out)
    }

    pub(crate) fn delta_level(&self, desc: &VectorIndexDesc) -> Result<VectorLevel> {
        delta_level(self.gen, self.cache, desc)
    }

    /// The RW-index for `desc`, advanced to **this query's delta epoch**, or `None` to
    /// brute-force the delta arm.
    ///
    /// The index is a pure function of the query's *own* pinned snapshot: every id the journal
    /// reports as changed is re-resolved through [`delta_vector_for`] against `self.gen`. That
    /// is what makes it impossible for the index to describe a delta the query is not reading —
    /// and the returned epoch is carried back so the caller can re-check it under the read guard
    /// (another query may advance the index between here and there).
    fn rw_delta_index(&self, desc: &VectorIndexDesc) -> Result<Option<(SharedIndex, u64)>> {
        let Some(arm) = &self.rw else {
            return Ok(None);
        };
        // An empty delta has nothing to index, and a read-only estate must pay nothing.
        if self.gen.delta().is_empty() {
            return Ok(None);
        }
        let lookup = arm.indexes.ensure(
            EnsureCtx {
                gen: self.gen.uuid(),
                desc,
                epoch: arm.epoch,
                cfg: &arm.cfg,
                journal: &arm.journal,
            },
            || self.gen.delta().node_dense_ids(),
            |id| delta_vector_for(self.gen, self.cache, id, desc),
        )?;
        Ok(match lookup {
            RwLookup::Ready(ix) => Some((ix, arm.epoch)),
            RwLookup::BruteForce => None,
        })
    }

    pub(crate) fn vector_group(&self, first_record: u64, count: u64) -> Result<Vec<VectorEntry>> {
        let ids: Vec<u64> = (first_record..first_record + count).collect();
        let (gen, cache) = (self.gen, self.cache);
        par_gather(self.fanout_pool.as_deref(), &ids, KNN_PAR_MIN, |&g| {
            read_vector(gen, cache, g)
        })
    }

    /// A node's value for property `key`, or `Null` if absent. (An embedding
    /// routed out to the vector store reads as `Null` here — vector *values* are
    /// served by the M5 KNN/`similarity()` path, not by a column read.)
    fn node_prop(&self, id: u64, key: &str) -> Result<Val> {
        // Decode only the requested key from the cached record, skipping the
        // values of the others (root cause 5): a single-property read no longer
        // allocates a `Vec<(u32, Value)>` nor decodes every other value.
        node_prop_par(self.gen, self.cache, id, key)
    }

    /// The value actually stored at the winning level for `(id, key)`, with **no** D12
    /// suppression — see [`node_prop_raw`]. The vector paths want the embedding itself; every
    /// other read wants [`Self::node_prop`].
    pub(crate) fn node_prop_raw(&self, id: u64, key: &str) -> Result<Val> {
        node_prop_raw(self.gen, self.cache, id, key)
    }

    fn edge_prop(&self, id: u64, key: &str) -> Result<Val> {
        edge_prop_par(self.gen, self.cache, id, key)
    }

    /// Resolve a node's label names and named properties — the material a Bolt
    /// `Node` structure carries. Reads route through the block cache like any other
    /// record access, so encoding a returned node reuses already-resident blocks.
    pub fn node_record(&self, id: u64) -> Result<(Vec<String>, NamedProps)> {
        let labels: Vec<String> = self
            .node_label_ids(id)?
            .into_iter()
            .filter_map(|l| self.gen.label_name(l).map(|s| s.to_string()))
            .collect();
        let mut props = self.core_named_props(id)?;
        self.overlay_node_props(id, &mut props);
        self.suppress_indexed_vectors_named(&labels, &mut props);
        Ok((labels, props))
    }

    /// Strip every *indexed* embedding from a node's name-space property map — the
    /// whole-map twin of [`suppress_indexed_vector`], and for the same reason (D12: a
    /// column read of an indexed embedding yields `Null` from the core, so it must yield
    /// `Null` from the delta and the segments too).
    ///
    /// This is also what keeps a delta-written embedding out of the **column store** at
    /// consolidation: the dumper walks node properties through this fold, so an indexed
    /// vector never reaches `intern_props`. It rides the dump's dedicated vector stream
    /// instead. The T2 flush is unaffected — it builds its rows straight from the
    /// memtable, not through the `ReadView`, so the vector still reaches the segment.
    fn suppress_indexed_vectors_named(&self, labels: &[String], named: &mut NamedProps) {
        if !named.iter().any(|(_, v)| matches!(v, Val::Vector(_))) {
            return;
        }
        let indexes = &self.gen.manifest().vector_indexes;
        named.retain(|(k, v)| {
            !(matches!(v, Val::Vector(_))
                && indexes
                    .iter()
                    .any(|d| &d.property == k && labels.contains(&d.label)))
        });
    }

    /// Node `id`'s **core-stack** properties in name space (below the delta): the winning
    /// segment full row when one carries the id, else the base record mapped to names.
    /// Resolving in name space (rather than through the id-keyed [`Self::node_props`])
    /// preserves a segment property whose key is not in the base symbol table. The caller
    /// folds the delta overlay on top ([`Self::overlay_node_props`]).
    fn core_named_props(&self, id: u64) -> Result<NamedProps> {
        if let Some(row) = self.gen.core_stack().resolve_node_row(id)? {
            if row.tombstoned {
                return Ok(Vec::new());
            }
            return Ok(row
                .props
                .into_iter()
                .map(|(k, v)| (k, Val::from_value(v)))
                .collect());
        }
        Ok(self
            .node_props(id)?
            .into_iter()
            .map(|(kid, v)| (self.key_name(kid), Val::from_value(v)))
            .collect())
    }

    /// Edge `id`'s effective properties in name space: the winning segment full row folded
    /// under the delta's edge patches, else the base record (via [`Self::edge_props`], which
    /// already folds patches). The edge analogue of [`Self::core_named_props`].
    fn core_named_edge_props(&self, id: u64) -> Result<NamedProps> {
        if let Some(row) = self.gen.core_stack().resolve_edge_row(id)? {
            if row.tombstoned {
                return Ok(Vec::new());
            }
            let mut out: NamedProps = row
                .props
                .into_iter()
                .map(|(k, v)| (k, Val::from_value(v)))
                .collect();
            // A delta patch on a segment-carried edge wins last-writer-wins.
            let delta = self.gen.delta();
            if !delta.is_empty() {
                for (name, value) in delta.edge_patches(id) {
                    overlay_named(&mut out, &name, Val::from_value(value));
                }
            }
            return Ok(out);
        }
        Ok(self
            .edge_props(id)?
            .into_iter()
            .map(|(kid, v)| (self.key_name(kid), Val::from_value(v)))
            .collect())
    }

    /// Fold the live delta's property patches for node `id` onto `named` (the core
    /// props already resolved into name-space), last-writer-wins: a patched name
    /// replaces the core value, a new name is appended. The empty-delta fast path
    /// (the overwhelming common case) returns immediately. Phase 1c overlays property
    /// overwrites; Phase 2c also seeds a delta-born node's business-key property.
    fn overlay_node_props(&self, id: u64, named: &mut NamedProps) {
        let delta = self.gen.delta();
        if delta.is_empty() {
            return;
        }
        let nd = delta.node_patch(id);
        let replaced = nd.as_ref().is_some_and(|d| d.replaced);
        let born = id >= self.gen.core_generation().node_count();
        // A `SET n = {map}` replace-all discards every core-derived property.
        if replaced {
            named.clear();
        }
        // Seed the anchor business-key property from the delta identity (it is never
        // stored as a patch) when the core props are not its source of truth: a
        // delta-born node has no core row, and a replaced node just dropped it.
        if born || replaced {
            if let Some((_, kname, kval)) = delta.node_identity_by_dense(id) {
                overlay_named(named, &kname, Val::from_value(kval));
            }
        }
        let Some(nd) = nd else {
            return;
        };
        // Fold out removed properties (a no-op after a replace-all, which already
        // cleared them). The anchor key is never in `removed` (the writer forbids it).
        for name in &nd.removed {
            named.retain(|(k, _)| k.as_str() != name.as_str());
        }
        for (name, value) in &nd.patches {
            overlay_named(named, name, Val::from_value(value.clone()));
        }
    }

    /// The outgoing adjacency of node `id` (dst, reltype, edge id) — the edge-walk
    /// surface the consolidation serialiser ([`crate::consolidate`]) uses to emit
    /// every edge exactly once (from its source). Overlays the edge delta (Phase 3):
    /// a delta-born node's edges are its born out-edges, a tombstoned edge (or an edge
    /// to a tombstoned node) is dropped — so a rebuild carries the writes forward.
    pub fn outgoing_adj(&self, id: u64) -> Result<Vec<topology::Adj>> {
        self.outgoing(id)
    }

    /// The incoming adjacency of node `id` (the mirror of [`Self::outgoing_adj`]) —
    /// the edges whose destination is `id`. Overlay-aware in the same way: a
    /// delta-born in-edge is included, an edge the delta tombstones (or an edge from a
    /// tombstoned node) is dropped. Used by the DELETE-conformance incident-degree
    /// check, which must see relationships in *both* directions.
    pub fn incoming_adj(&self, id: u64) -> Result<Vec<topology::Adj>> {
        self.incoming(id)
    }

    /// Does node `id` have **any** incident relationship (outgoing or incoming) in the
    /// overlaid view? The existence half of [`Self::outgoing_adj`]/[`Self::incoming_adj`]:
    /// it short-circuits on the first surviving edge instead of materialising the whole
    /// adjacency `Vec`, so the DELETE-conformance check on a high-degree hub stops at edge 1
    /// rather than decoding (and allocating) its millions of neighbours. Overlay-exact — it
    /// sees a delta-born edge and drops a delta-tombstoned one, exactly as the collecting
    /// readers, because it shares the one [`for_each_adj_overlaid`] fold.
    pub fn has_incident_edge(&self, id: u64) -> Result<bool> {
        Ok(any_adj_overlaid(self.gen, self.cache, id, true)?
            || any_adj_overlaid(self.gen, self.cache, id, false)?)
    }

    /// The edge id of the first `src -[reltype]-> dst` out-edge in the overlaid view, or
    /// `None`. The existence-resolving analogue of a filtered [`Self::outgoing_adj`] `find`:
    /// it pushes the reltype into the CSR decode and stops at the first matching neighbour,
    /// so it never materialises a hub source's out-adjacency to locate one edge. Which edges
    /// are in scope follows the view — over an empty-delta view it returns the core edge id
    /// only (the `MERGE`-idempotency / core-edge-patch resolver's requirement).
    pub fn find_outgoing_edge(&self, src: u64, reltype: u32, dst: u64) -> Result<Option<u64>> {
        find_outgoing_edge_overlaid(self.gen, self.cache, src, reltype, dst)
    }

    /// Resolve a relationship's type name and named properties — the material a
    /// Bolt `Relationship` structure carries.
    pub fn rel_record(&self, id: u64, reltype: u32) -> Result<(String, NamedProps)> {
        let type_name = self.gen.reltype_name(reltype).unwrap_or("").to_string();
        let props = self.core_named_edge_props(id)?;
        Ok((type_name, props))
    }

    fn key_name(&self, kid: u32) -> String {
        self.gen.property_key_name(kid).unwrap_or("?").to_string()
    }

    /// Raw (undecoded) `node_labels.blk` record for a **core** node, read through the
    /// block cache. The bytes are the canonical [`nodelabels::encode_labels_record`]
    /// layout in the core generation's label ids — the consolidation dump byte-copies
    /// them for untouched nodes, skipping decode + re-encode. Caller guarantees
    /// `id < core_generation().node_count()`.
    pub fn raw_node_labels(&self, id: u64) -> Result<crate::cache::BlockRecord> {
        self.cache.record(
            self.gen.node_labels().inner(),
            self.gen.uuid(),
            FileKind::NodeLabels,
            id,
        )
    }

    /// Raw (undecoded) `node_props.blk` record for a **core** node (see
    /// [`Self::raw_node_labels`]). Caller guarantees `id < core node count`.
    pub fn raw_node_props(&self, id: u64) -> Result<crate::cache::BlockRecord> {
        self.cache.record(
            self.gen.node_props().inner(),
            self.gen.uuid(),
            FileKind::NodeProps,
            id,
        )
    }

    /// Raw (undecoded) `edge_props.blk` record for a **core** edge (see
    /// [`Self::raw_node_labels`]). Caller guarantees `id < core edge count`.
    pub fn raw_edge_props(&self, id: u64) -> Result<crate::cache::BlockRecord> {
        self.cache.record(
            self.gen.edge_props().inner(),
            self.gen.uuid(),
            FileKind::EdgeProps,
            id,
        )
    }

    fn check_deadline(&self) -> Result<()> {
        if let Some(d) = self.deadline {
            if Instant::now() >= d {
                return Err(ExecLimit::Deadline.into());
            }
        }
        Ok(())
    }

    /// Charge `n` elements against the query-wide intermediate budget. Called by
    /// every operation that materialises a collection, so cumulative (not just
    /// peak) allocation is bounded — geometric growth like `reduce(acc + acc)`
    /// trips the budget on an early iteration.
    fn charge(&self, n: u64) -> Result<()> {
        // Per-query budget (config `query.maxIntermediate`; 0 disables).
        if self.max_intermediate != 0 {
            let used = self.budget_used.get().saturating_add(n);
            self.budget_used.set(used);
            if used > self.max_intermediate {
                return Err(ExecLimit::IntermediateBudget(self.max_intermediate).into());
            }
        }
        // Server-wide budget (config `query.maxIntermediateGlobal`; 0 disables) —
        // the aggregate guard a per-query cap cannot provide. Charged even when the
        // per-query budget is off, and refunded in full when the query ends.
        if let Some(g) = self.global_budget {
            if g.limit() != 0 {
                self.global_charged
                    .set(self.global_charged.get().saturating_add(n));
                if !g.try_charge(n) {
                    return Err(ExecLimit::GlobalBudget(g.limit()).into());
                }
            }
        }
        Ok(())
    }

    /// Charge `n` *transient* walk elements against the scan budget (config
    /// `query.maxScan`; 0 disables). Cumulative like [`charge`](Self::charge) so a
    /// geometric blow-up trips early, but — unlike `charge` — it touches neither the
    /// retained per-query budget nor the server-wide aggregate: count-pushdown work
    /// holds no memory, so there is nothing for a concurrent query to compete over.
    fn charge_scan(&self, n: u64) -> Result<()> {
        if self.max_scan != 0 {
            let used = self.scan_used.get().saturating_add(n);
            self.scan_used.set(used);
            if used > self.max_scan {
                bail!(
                    "query exceeded the scan budget of {} elements (query.maxScan)",
                    self.max_scan
                );
            }
        }
        Ok(())
    }

    /// Charge `n` chain-walk elements, routed by retention. In count-pushdown mode
    /// (`count_acc` set) the walk tallies and discards every row, frees each adjacency
    /// buffer per chunk, and holds only a structurally bounded frontier — nothing is
    /// retained, so the charge is transient ([`charge_scan`](Self::charge_scan)). In
    /// row-building mode the same elements materialise, so it is the retained
    /// [`charge`](Self::charge). This is the split that lets a memory-flat `count(*)`
    /// run to the timeout without being gated by the tight memory budget, while a
    /// materialising walk stays bounded exactly as before.
    fn charge_walk(&self, n: u64) -> Result<()> {
        if self.count_acc.get().is_some() {
            self.charge_scan(n)
        } else {
            self.charge(n)
        }
    }

    /// In count-pushdown mode (`count_acc` set), tally one completed row and return
    /// `true` so the caller skips materialising it. `false` in normal row-building
    /// mode. Charging is unchanged and happens at the call site either way, so the
    /// intermediate budget bounds a counted walk exactly as it bounds a materialised
    /// one.
    fn count_tally(&self) -> bool {
        match self.count_acc.get() {
            Some(n) => {
                self.count_acc.set(Some(n + 1));
                true
            }
            None => false,
        }
    }

    /// Refund this query's whole global-budget charge. Idempotent: a second call
    /// (e.g. `Drop` after `run` already released) refunds nothing.
    fn release_global(&self) {
        if let Some(g) = self.global_budget {
            g.release(self.global_charged.replace(0));
        }
    }

    // ── Entry point ───────────────────────────────────────────────────────

    /// Execute a (possibly `UNION`ed) query. Always refunds this query's
    /// server-wide budget charge before returning, on success or failure.
    pub fn run(&self, q: &Query) -> Result<QueryResult> {
        let r = self.run_inner(q);
        self.release_global();
        r
    }

    fn run_inner(&self, q: &Query) -> Result<QueryResult> {
        self.budget_used.set(0); // per-run budget; engines may be reused
        self.scan_used.set(0);
        self.scanned_ids.set(0);
        self.global_charged.set(0);
        let mut result = self.run_single(&q.head)?;
        for (union_all, part) in &q.tail {
            let next = self.run_single(part)?;
            if next.columns.len() != result.columns.len() {
                bail!("all parts of a UNION must return the same number of columns");
            }
            self.charge(next.rows.len() as u64)?; // UNION cross-branch buildup
            result.rows.extend(next.rows);
            if !union_all {
                self.charge(result.rows.len() as u64)?; // DISTINCT `seen` set
                dedup_rows(&mut result.rows);
            }
        }
        if result.rows.len() > self.max_rows {
            bail!(
                "query produced {} rows, exceeding the limit of {}",
                result.rows.len(),
                self.max_rows
            );
        }
        Ok(result)
    }

    fn run_single(&self, sq: &SingleQuery) -> Result<QueryResult> {
        // The count / whole-graph-metadata fast paths answer from the immutable core's
        // resident marginals and range indexes without materialising rows — but a live
        // delta can change those answers (a tombstone removes a node from a count/label
        // enumeration; a property patch on an indexed key moves it in the index).
        //
        // The bare `count(*)` path is delta-aware and always runs: the delta carries an
        // O(1) born tally per level and a small suppressed-id set, so the merged count is
        // still a metadata read. The rest need a pure core; with any delta present they
        // fall through to full execution, where `scan_candidates` suppresses tombstones
        // and the property overlay corrects patched values. The empty delta is the
        // overwhelming common case, so read-only performance is intact.
        // Stage 3: a bare `MATCH (n[:L][{p: v}]) RETURN count(*)|count(n)` from
        // resident metadata / a single index lookup, skipping materialisation.
        // Only reachable here (top-level / UNION part), where the seed is the
        // empty singleton — a `CALL { … }` subquery seeds outer rows via
        // `run_single_seeded` and never takes this path, so the count is always
        // over the whole match.
        //
        // This one is **delta-aware** (`live_node_count` / `live_label_node_count` net
        // out the delta's born and suppressed rows), so it survives a non-empty delta —
        // without it, a single `MERGE` would turn a whole-graph `count(*)` into a full
        // scan of the core. The inline-property variant still needs a pure core (see
        // the guard in `try_count_fast_path`).
        if let Some((columns, row)) = self.try_count_fast_path(sq)? {
            return Ok(QueryResult {
                columns,
                rows: vec![row],
            });
        }
        // Stage E: the bare whole-graph edge count `MATCH ()-[r]->() RETURN count(*)`,
        // from resident counts rather than an expansion. Delta-aware; it must precede
        // Stage B, which would otherwise walk every edge.
        if let Some((columns, row)) = self.try_edge_count_fast_path(sq)? {
            return Ok(QueryResult {
                columns,
                rows: vec![row],
            });
        }
        // Stage M: a whole-graph label/reltype *metadata* enumeration or grouped count —
        // `MATCH ()-[r]->() RETURN [DISTINCT] type(r) [, count(*)]` and `MATCH (n) RETURN
        // [DISTINCT] labels(n)[0] [, count(*)]` (plus the labelled schema-marginal
        // variants) — answered from resident metadata with zero block reads, instead of
        // materialising every binding. Both are delta-aware: they net the delta's born
        // rows in and its suppressed rows out, and decline the shapes they cannot answer
        // exactly over a delta (the labelled endpoint cube, an undirected hop).
        if let Some(res) = self.try_reltype_meta_fast_path(sq)? {
            return Ok(res);
        }
        if let Some(res) = self.try_label_meta_fast_path(sq)? {
            return Ok(res);
        }
        // The grouped-index fast path walks the base range index / histograms directly, which
        // are not segment-aware, so it is only sound over a singleton set; a stacked set falls
        // through to full execution (segment-aware via the scan / adjacency seams).
        if self.gen.delta().is_empty() && self.gen.core_stack().is_singleton() {
            // Stage 7: `MATCH (n:L) RETURN n.p, count(*)` (group-by an indexed prop)
            // and `RETURN count(DISTINCT n.p)` are answered from the range index over
            // (L, p) — one sequential index walk, no per-node property decode.
            if let Some(res) = self.try_grouped_index_fast_path(sq)? {
                return Ok(res);
            }
            // Stage B: `MATCH (…)-[…]->(…) [WHERE …] RETURN count(*)|count(v)` — a
            // multi-hop count walks but counts during expansion instead of
            // materialising the row set (the fanout RSS peak).
            if let Some(res) = self.try_count_walk_fast_path(sq)? {
                return Ok(res);
            }
        }
        self.run_single_seeded(sq, Table::singleton())
    }

    /// Recognise a single-node `count` aggregate that can be answered without
    /// materialising rows, returning the single result `(columns, row)` or `None`
    /// when any guard fails (the caller then executes normally).
    ///
    /// Guards: exactly one MATCH reading clause, non-OPTIONAL, no WHERE, one
    /// single-node pattern (no rels); the RETURN is non-DISTINCT, no
    /// ORDER BY/SKIP/LIMIT/`*`, and its items are exactly one `count(*)`/`count(n)`
    /// (n the pattern's variable) plus any number of **constant** items
    /// (`$param`/literal — the benchmark appends `… , $k AS k` to bust the result
    /// cache). A constant item is a single grouping key with one group, so the
    /// count is still over the whole match.
    ///
    /// The count itself: no inline props → `node_count()` (0 labels) or
    /// `label_node_count(L)` (1 label); a single indexed-equality inline prop
    /// whose index covers exactly the pattern's label+prop → that index's
    /// `lookup_eq` length. Anything else (multi-label, residual props, non-index
    /// props, a non-constant extra projection) falls back.
    fn try_count_fast_path(&self, sq: &SingleQuery) -> Result<Option<(Vec<String>, Vec<Val>)>> {
        // A stacked set answers `count(*)` / `count(n:L)` from the summed segment marginals
        // (`live_node_count` / `live_label_node_count`); decline to full execution when any
        // segment's marginals are not provably exact.
        if !self.gen.core_stack().marginals_exact() {
            return Ok(None);
        }
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if !pat.rels.is_empty() || pat.segments.is_some() {
            return Ok(None); // single-node patterns only
        }
        let node = &pat.start;

        let body = &sq.ret.body;
        if sq.ret.distinct
            || body.star
            || body.items.is_empty()
            || !body.order_by.is_empty()
            || body.skip.is_some()
            || body.limit.is_some()
        {
            return Ok(None);
        }
        // Exactly one item must be the count; every other item must be a constant
        // (a single, constant grouping key → one group).
        let mut count_idx = None;
        for (i, it) in body.items.iter().enumerate() {
            if is_count_of(&it.expr, node.var.as_deref()) {
                if count_idx.is_some() {
                    return Ok(None); // two counts — not our shape
                }
                count_idx = Some(i);
            } else if !matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                return Ok(None); // a non-constant projection ⇒ grouping/other agg
            }
        }
        let Some(count_idx) = count_idx else {
            return Ok(None);
        };

        // Compute the match count. The no-inline-prop shapes read the *live* counts, so
        // they hold over a merged view too (born rows added, suppressed rows netted out).
        let count: i64 = if node.props.is_empty() {
            match &node.label_expr {
                None => self.gen.live_node_count() as i64,
                // A lone positive atom is a single label posting; any boolean /
                // multi-label expression has no single-posting count — fall back.
                Some(e) => match e.as_single_atom() {
                    Some(l) => match self.gen.label_id(l) {
                        // `live_label_node_count` is exact under a label overlay (Stage 5),
                        // so no fall-back-to-scan is needed here.
                        Some(lid) => self.gen.live_label_node_count(lid)? as i64,
                        // A label the core never defined can still have delta-born nodes
                        // (a `MERGE` may introduce a brand-new label), and those have no
                        // core label id to count against — fall back to full execution.
                        None if !self.gen.delta().is_empty() => return Ok(None),
                        None => 0,
                    },
                    None => return Ok(None),
                },
            }
        } else if !self.gen.delta().is_empty() {
            // Inline props over a delta: the index-length shortcut below ignores born
            // rows, moved indexed values and tombstones. It is also cheap to execute
            // normally (an indexed seek, not a scan), so just fall back.
            return Ok(None);
        } else {
            // Inline props: only an exact single indexed-equality is safe (the scan
            // result then needs no residual filtering, so its length is the count).
            let scan = choose_node_scan(self.gen, node, None, &self.plan_params, &HashMap::new());
            let NodeScan::RangeEq { ref index, .. } = scan else {
                return Ok(None);
            };
            let covers = node.props.len() == 1
                && self
                    .gen
                    .manifest()
                    .range_indexes
                    .iter()
                    .find(|ri| &ri.name == index && ri.entity == EntityKind::Node)
                    .is_some_and(|ri| {
                        // The RangeEq scan fully determines membership only when no
                        // label residual remains: either no label constraint, or a
                        // single positive atom that *is* the index's label. A boolean
                        // or multi-label expression would need re-checking, so bail.
                        node.props[0].0 == ri.property
                            && match &node.label_expr {
                                None => true,
                                Some(e) => {
                                    e.as_single_atom().map(String::as_str)
                                        == Some(ri.label_or_type.as_str())
                                }
                            }
                    });
            if !covers {
                return Ok(None);
            }
            self.scan_candidates(&scan)?.len() as i64
        };

        Ok(Some(self.count_row(sq, count_idx, count)?))
    }

    /// Build the single output row of a count fast path: `count` in its column, every
    /// other (constant) projection evaluated against an empty scope.
    fn count_row(
        &self,
        sq: &SingleQuery,
        count_idx: usize,
        count: i64,
    ) -> Result<(Vec<String>, Vec<Val>)> {
        let body = &sq.ret.body;
        let empty: HashMap<String, Val> = HashMap::new();
        let mut columns = Vec::with_capacity(body.items.len());
        let mut row = Vec::with_capacity(body.items.len());
        for (i, it) in body.items.iter().enumerate() {
            columns.push(it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)));
            if i == count_idx {
                row.push(Val::Int(count));
            } else {
                row.push(self.eval(&it.expr, &Scope::Map(&empty), None)?);
            }
        }
        Ok((columns, row))
    }

    /// Recognise a whole-graph relationship-type metadata query and answer it from
    /// resident metadata (the per-reltype edge counts / edge-schema marginals),
    /// touching no blocks. Handles the enumeration `MATCH ()-[r]->() RETURN DISTINCT
    /// type(r)` and the grouped count `RETURN type(r), count(*)`, plus the
    /// source/target-labelled marginals `(:A)-[r]->()` / `()-[r]->(:B)` (in either
    /// arrow direction) when the generation carries the schema marginals.
    ///
    /// Declines (→ `None`, the matcher runs) on anything that makes it more than a
    /// whole-graph metadata question: a WHERE, a rel-type filter or rel property, an
    /// endpoint property or boolean/multi-label expr, both endpoints labelled (the
    /// full cube — currently unbuilt), an undirected relationship (the `2·edge −
    /// self_loop` semantics are deferred to a parity-checked follow-up), any extra
    /// non-constant projection, additional pattern segments, or ORDER BY/SKIP/LIMIT.
    fn try_reltype_meta_fast_path(&self, sq: &SingleQuery) -> Result<Option<QueryResult>> {
        // ---- pattern shape ----
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if pat.segments.is_some() || pat.selector.is_some() || pat.restrictor.is_some() {
            return Ok(None);
        }
        if pat.rels.len() != 1 {
            return Ok(None); // exactly one relationship (a whole-graph edge scan)
        }
        let (rel, right) = &pat.rels[0];
        let left = &pat.start;
        // The relationship must be an unfiltered single hop, bound to a variable so
        // `type(r)` can reference it.
        if rel.type_expr.is_some() || !rel.props.is_empty() || rel.var_length.is_some() {
            return Ok(None);
        }
        let Some(relvar) = rel.var.as_deref() else {
            return Ok(None);
        };
        // Endpoints carry no properties.
        if !left.props.is_empty() || !right.props.is_empty() {
            return Ok(None);
        }
        // A single node variable reused on both endpoints (`(a)-[r]->(a)`) constrains
        // the edge to a self-loop — that is not a whole-graph metadata question, so
        // decline and let the matcher handle it.
        if let (Some(lv), Some(rv)) = (left.var.as_deref(), right.var.as_deref()) {
            if lv == rv {
                return Ok(None);
            }
        }
        // Each endpoint is bare (no constraint) or a single positive label atom.
        let atom = |n: &NodePat| -> Result<Option<Option<String>>> {
            match &n.label_expr {
                None => Ok(Some(None)),
                Some(e) => match e.as_single_atom() {
                    Some(l) => Ok(Some(Some(l.clone()))),
                    None => Ok(None), // boolean / multi-label ⇒ decline
                },
            }
        };
        let (Some(left_label), Some(right_label)) = (atom(left)?, atom(right)?) else {
            return Ok(None);
        };

        // ---- projection shape ----
        let Some((key_idx, count_idx)) =
            self.classify_meta_projection(sq, |e| is_type_of(e, Some(relvar)), relvar)
        else {
            return Ok(None);
        };

        // ---- resolve the metadata source and compute per-reltype counts ----
        // Each endpoint resolves to: `None` ⇒ bare (no label); `Some(None)` ⇒ labelled
        // but the label is absent from the graph (matches nothing); `Some(Some(id))` ⇒
        // labelled with a known id.
        let left_id = left_label.map(|n| self.gen.label_id(&n));
        let right_id = right_label.map(|n| self.gen.label_id(&n));

        // With a live delta *or* a core segment stack, the base's resident schema marginals
        // no longer describe the graph. The whole-graph `type(r)` shape is still answerable
        // from the summed edge counters (`live_reltype_edge_groups`); the labelled-endpoint
        // cube and the undirected doubling are not, so they decline and the matcher runs.
        if !self.gen.delta().is_empty() || !self.gen.core_stack().is_singleton() {
            if left_id.is_some() || right_id.is_some() || matches!(rel.dir, Direction::Undirected) {
                return Ok(None);
            }
            let Some(live) = self.gen.live_reltype_edge_groups()? else {
                return Ok(None);
            };
            let groups: Vec<(Val, u64)> = live.into_iter().map(|(n, c)| (Val::Str(n), c)).collect();
            return Ok(Some(
                self.build_meta_result(sq, key_idx, count_idx, groups)?,
            ));
        }
        // Edges of type `t` whose source satisfies `src` and target satisfies `tgt`,
        // read from the resident whole-graph counts / schema marginals / cube. `None`
        // ⇒ the required marginal is not present in this generation (⇒ decline).
        let g = |src: Option<Option<u32>>, tgt: Option<Option<u32>>, t: u32| -> Option<u64> {
            Some(match (src, tgt) {
                (None, None) => self.gen.reltype_edge_count(t),
                (Some(None), _) | (_, Some(None)) => 0,
                (Some(Some(a)), None) => self.gen.src_label_reltype_count(a, t)?,
                (None, Some(Some(b))) => self.gen.reltype_tgt_label_count(t, b)?,
                (Some(Some(a)), Some(Some(b))) => self.gen.schema_triple_count(a, t, b)?,
            })
        };
        // Map the pattern's directionality onto the source/target axes. An outgoing
        // arrow binds left→source, right→target; incoming is the mirror. An undirected
        // relationship matches each edge in *both* orientations, so its count is the
        // sum over both axis assignments — which counts a self-loop twice and handles
        // a labelled endpoint "on either end" without any inclusion-exclusion.
        let count_for = |t: u32| -> Option<u64> {
            match rel.dir {
                Direction::Outgoing => g(left_id, right_id, t),
                Direction::Incoming => g(right_id, left_id, t),
                Direction::Undirected => Some(g(left_id, right_id, t)? + g(right_id, left_id, t)?),
            }
        };

        let n = self.gen.manifest().reltypes.len();
        let mut groups: Vec<(Val, u64)> = Vec::new();
        for t in 0..n as u32 {
            let Some(c) = count_for(t) else {
                return Ok(None); // marginal not present in this generation
            };
            if c > 0 {
                let name = self.gen.reltype_name(t).unwrap_or("").to_string();
                groups.push((Val::Str(name), c));
            }
        }
        Ok(Some(
            self.build_meta_result(sq, key_idx, count_idx, groups)?,
        ))
    }

    /// Recognise a whole-graph edge count — `MATCH ()-[r]->() RETURN count(*)` — and
    /// answer it from resident counts instead of walking the adjacency.
    ///
    /// Without this, the bare (ungrouped) edge count has no fast path at all: the grouped
    /// `RETURN type(r), count(*)` form is answered from the manifest, but dropping the
    /// group key sent the query to a full expansion — 96 s and a `maxScan` breach on a
    /// 1.5B-edge core. Merged views answer from the delta's edge counters, declining when
    /// those cannot be exact (see [`ReadView::live_edge_count`]).
    ///
    /// Declines on: a WHERE, a rel-type filter / rel property / variable length, any
    /// endpoint label or property, a self-loop pattern `(a)-[r]->(a)`, an undirected
    /// relationship (each edge would match in both orientations), extra pattern segments,
    /// a non-constant extra projection, or DISTINCT / ORDER BY / SKIP / LIMIT.
    fn try_edge_count_fast_path(
        &self,
        sq: &SingleQuery,
    ) -> Result<Option<(Vec<String>, Vec<Val>)>> {
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if pat.segments.is_some() || pat.selector.is_some() || pat.restrictor.is_some() {
            return Ok(None);
        }
        if pat.rels.len() != 1 {
            return Ok(None);
        }
        let (rel, right) = &pat.rels[0];
        let left = &pat.start;
        if rel.type_expr.is_some() || !rel.props.is_empty() || rel.var_length.is_some() {
            return Ok(None);
        }
        // An undirected hop matches each edge in both orientations — not a plain count.
        if matches!(rel.dir, Direction::Undirected) {
            return Ok(None);
        }
        // Whole graph: both endpoints unconstrained, and not the same variable (which
        // would restrict the match to self-loops).
        if left.label_expr.is_some()
            || right.label_expr.is_some()
            || !left.props.is_empty()
            || !right.props.is_empty()
        {
            return Ok(None);
        }
        if let (Some(lv), Some(rv)) = (left.var.as_deref(), right.var.as_deref()) {
            if lv == rv {
                return Ok(None);
            }
        }

        let body = &sq.ret.body;
        if sq.ret.distinct
            || body.star
            || body.items.is_empty()
            || !body.order_by.is_empty()
            || body.skip.is_some()
            || body.limit.is_some()
        {
            return Ok(None);
        }
        let mut count_idx = None;
        for (i, it) in body.items.iter().enumerate() {
            if is_count_of(&it.expr, rel.var.as_deref()) {
                if count_idx.is_some() {
                    return Ok(None);
                }
                count_idx = Some(i);
            } else if !matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                return Ok(None);
            }
        }
        let Some(count_idx) = count_idx else {
            return Ok(None);
        };
        let Some(count) = self.gen.live_edge_count()? else {
            return Ok(None); // the delta cannot answer it exactly — run the matcher
        };
        Ok(Some(self.count_row(sq, count_idx, count as i64)?))
    }

    /// Recognise a whole-graph `labels(n)[0]` metadata query and answer it from the
    /// resident first-label counts, touching no blocks. Handles `MATCH (n) RETURN
    /// DISTINCT labels(n)[0]` and `RETURN labels(n)[0], count(*)`. Requires the
    /// generation's `first_label_counts` (so first-label semantics are reproduced
    /// exactly, even with multi-label nodes); the null bucket (zero-label nodes) is
    /// `node_count − Σ first_label_counts`. Declines on any node label/property
    /// constraint, a WHERE, a non-`[0]` index, extra non-constant projection,
    /// `count(DISTINCT …)`, or ORDER BY/SKIP/LIMIT.
    fn try_label_meta_fast_path(&self, sq: &SingleQuery) -> Result<Option<QueryResult>> {
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if !pat.rels.is_empty() || pat.segments.is_some() {
            return Ok(None); // single-node pattern only
        }
        let node = &pat.start;
        if node.label_expr.is_some() || !node.props.is_empty() {
            return Ok(None); // whole-graph: no endpoint constraint
        }
        let Some(nodevar) = node.var.as_deref() else {
            return Ok(None);
        };
        // Requires exact first-label counts — otherwise first-label semantics can't
        // be reproduced from per-label occurrence counts under multi-label nodes.
        if !self.gen.has_first_label_counts() {
            return Ok(None);
        }
        // A core segment carries per-label *occurrence* deltas, not first-label deltas, so a
        // stacked set cannot reproduce `labels(n)[0]` groups from marginals — decline to full
        // execution (which reads the effective rows and is segment-aware).
        if !self.gen.core_stack().is_singleton() {
            return Ok(None);
        }

        let Some((key_idx, count_idx)) =
            self.classify_meta_projection(sq, |e| is_first_label_of(e, Some(nodevar)), nodevar)
        else {
            return Ok(None);
        };

        // Live groups: the core's first-label marginals, plus the delta's born nodes,
        // minus its suppressed rows. Zero-label nodes project `labels(n)[0] == null`.
        let groups: Vec<(Val, u64)> = self
            .gen
            .live_first_label_groups()?
            .into_iter()
            .map(|(name, c)| (name.map_or(Val::Null, Val::Str), c))
            .collect();
        Ok(Some(
            self.build_meta_result(sq, key_idx, count_idx, groups)?,
        ))
    }

    /// Shared projection guard for the metadata fast paths. Returns
    /// `Some((key_idx, count_idx))` when the RETURN is exactly one group key (matched
    /// by `is_key`) plus, for a grouped count, one `count(*)`/`count(var)` and any
    /// number of constant items — and the DISTINCT flag is consistent with that
    /// shape (enumeration must be DISTINCT; a grouped count must not be). `None`
    /// otherwise (the caller then declines). A trailing `ORDER BY` / `SKIP` / `LIMIT`
    /// is permitted — it is applied to the finished metadata rows in
    /// [`Self::build_meta_result`], exactly as the general path would.
    fn classify_meta_projection(
        &self,
        sq: &SingleQuery,
        is_key: impl Fn(&Expr) -> bool,
        countvar: &str,
    ) -> Option<(usize, Option<usize>)> {
        let body = &sq.ret.body;
        if body.star || body.items.is_empty() {
            return None;
        }
        let mut key_idx = None;
        let mut count_idx = None;
        for (i, it) in body.items.iter().enumerate() {
            if is_key(&it.expr) {
                if key_idx.is_some() {
                    return None;
                }
                key_idx = Some(i);
            } else if is_count_of(&it.expr, Some(countvar)) {
                if count_idx.is_some() {
                    return None;
                }
                count_idx = Some(i);
            } else if matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                // a constant grouping-neutral column (one group) — allowed
            } else {
                return None;
            }
        }
        let key_idx = key_idx?;
        // Enumeration (no count) must be DISTINCT, else it is a per-edge/-node
        // projection, not a metadata question. A grouped count must not be DISTINCT.
        match count_idx {
            None if !sq.ret.distinct => return None,
            Some(_) if sq.ret.distinct => return None,
            _ => {}
        }
        Some((key_idx, count_idx))
    }

    /// Assemble the single-column-per-projection-item result of a metadata fast path
    /// from the computed `(key_value, count)` groups, honouring the original item
    /// order (group key, optional count aggregate, and any constant items).
    fn build_meta_result(
        &self,
        sq: &SingleQuery,
        key_idx: usize,
        count_idx: Option<usize>,
        groups: Vec<(Val, u64)>,
    ) -> Result<QueryResult> {
        let items = &sq.ret.body.items;
        let columns: Vec<String> = items
            .iter()
            .map(|it| it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)))
            .collect();
        let empty: HashMap<String, Val> = HashMap::new();
        let mut rows = Vec::with_capacity(groups.len());
        for (key, count) in groups {
            let mut row = Vec::with_capacity(items.len());
            for (i, it) in items.iter().enumerate() {
                if i == key_idx {
                    row.push(key.clone());
                } else if Some(i) == count_idx {
                    row.push(Val::Int(count as i64));
                } else {
                    row.push(self.eval(&it.expr, &Scope::Map(&empty), None)?);
                }
            }
            rows.push(row);
        }
        // Apply any trailing ORDER BY / SKIP / LIMIT over the finished rows using the
        // same routine as the general projection, so an ordered/limited metadata query
        // is byte-identical to the scan (the rows are already the scan's answer).
        let rows = self.order_skip_limit_no_input(&sq.ret.body, &columns, rows)?;
        Ok(QueryResult { columns, rows })
    }

    /// Recognise a bare `RETURN count(*) | count(v)` over a single non-OPTIONAL
    /// `MATCH` (optional `WHERE`) whose pattern has relationships, and answer it by
    /// **counting matched rows during expansion** instead of materialising every
    /// completed binding. This is the multi-hop / WHERE sibling of
    /// [`Self::try_count_fast_path`] (which answers single-node counts from
    /// metadata) — it still walks, but never builds the row set, so a high-degree
    /// hub `count(*)` runs in O(1) result memory instead of the
    /// `query.maxIntermediate`-bounded `Vec<HashMap>` that is the fanout RSS peak.
    ///
    /// Guards (anything else returns `None` → the materialising path runs, still
    /// correct): one MATCH reading clause, non-OPTIONAL, no quantified/selector/
    /// restrictor pattern, at least one relationship; the RETURN is non-`*`, has no
    /// ORDER BY/SKIP/LIMIT, and its items are exactly one `count(*)`/`count(v)` (with
    /// `v` a variable this MATCH binds — always non-null on a completed non-OPTIONAL
    /// match, so `count(v) == count(*)`) plus any number of **constant** items.
    /// `count(DISTINCT …)`, `count(expr)`, a second aggregate, a grouping item, or a
    /// trailing clause all fall back (pushdown would miscount).
    fn try_count_walk_fast_path(&self, sq: &SingleQuery) -> Result<Option<QueryResult>> {
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        // OPTIONAL emits outer-join rows whose pattern vars are null: count(v) would
        // then skip them (≠ count(*)) and count(*) over a no-match seed is 1, not 0.
        if m.optional {
            return Ok(None);
        }
        if m.patterns
            .iter()
            .any(|p| p.segments.is_some() || p.selector.is_some() || p.restrictor.is_some())
        {
            return Ok(None);
        }
        // Must actually walk — a pure single-node count is the metadata fast path's job.
        if m.patterns.iter().all(|p| p.rels.is_empty()) {
            return Ok(None);
        }

        let body = &sq.ret.body;
        if body.star
            || body.items.is_empty()
            || !body.order_by.is_empty()
            || body.skip.is_some()
            || body.limit.is_some()
        {
            return Ok(None);
        }

        // Variables this MATCH binds; each is non-null on a completed non-OPTIONAL
        // match, so `count(v)` over any of them equals `count(*)`.
        let mut bound: Vec<String> = Vec::new();
        for p in &m.patterns {
            collect_pattern_vars(p, &[], &mut bound);
        }

        // Exactly one count item; every other item a constant (one group).
        let mut count_idx = None;
        for (i, it) in body.items.iter().enumerate() {
            if is_count_star_or_var(&it.expr, &bound) {
                if count_idx.is_some() {
                    return Ok(None); // two counts — not our shape
                }
                count_idx = Some(i);
            } else if !matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                return Ok(None); // grouping key / other aggregate
            }
        }
        let Some(count_idx) = count_idx else {
            return Ok(None);
        };

        let n = self.count_match(m)?;

        // One output row: the count in its column, constants evaluated.
        let empty: HashMap<String, Val> = HashMap::new();
        let mut columns = Vec::with_capacity(body.items.len());
        let mut row = Vec::with_capacity(body.items.len());
        for (i, it) in body.items.iter().enumerate() {
            columns.push(it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)));
            if i == count_idx {
                row.push(Val::Int(n as i64));
            } else {
                row.push(self.eval(&it.expr, &Scope::Map(&empty), None)?);
            }
        }
        Ok(Some(QueryResult {
            columns,
            rows: vec![row],
        }))
    }

    /// Count the rows a non-OPTIONAL `MATCH` produces.
    ///
    /// For a single pattern with no `WHERE`, drive the ordinary matcher with the
    /// count accumulator armed: the chain-walk leaves tally completed rows and never
    /// materialise them (`out` stays empty), so a high-degree-hub `count(*)` runs in
    /// O(1) result memory — the fanout RSS win. Charging is unchanged, so the
    /// intermediate budget still bounds the walk exactly as before.
    ///
    /// A `WHERE` (the survivor filter is applied at the `match_patterns` terminal,
    /// after `match_single_pattern` has produced the rows) or a multi-pattern
    /// conjunction falls back to the materialising path — correct, just without the
    /// memory win (these are not the fanout-count hot shape).
    fn count_match(&self, m: &MatchClause) -> Result<u64> {
        if m.patterns.len() == 1 && m.where_.is_none() {
            debug_assert!(
                self.count_acc.get().is_none(),
                "count_match is not re-entrant"
            );
            self.count_acc.set(Some(0));
            let mut sink: Vec<HashMap<String, Val>> = Vec::new();
            let res =
                self.match_single_pattern(&m.patterns[0], &HashMap::new(), None, &mut sink, None);
            let n = self.count_acc.replace(None).unwrap_or(0);
            res?;
            debug_assert!(sink.is_empty(), "count-pushdown must not materialise rows");
            return Ok(n);
        }
        let table = self.apply_match(Table::singleton(), m, None)?;
        Ok(table.rows.len() as u64)
    }

    /// Recognise a single-node aggregation whose grouping/distinct key is an
    /// *indexed* property, and answer it from the range index instead of decoding
    /// the property from every node record. Returns the full `QueryResult` or
    /// `None` when any guard fails (the caller then executes normally).
    ///
    /// Guards mirror [`Self::try_count_fast_path`]: exactly one non-OPTIONAL
    /// `MATCH`, no `WHERE`, one single-node pattern (no rels), exactly one label,
    /// no inline props; the `RETURN` is non-DISTINCT and not `*`. The grouped /
    /// aggregated property must be a bare `n.p` with an open range index. Two
    /// shapes are recognised (anything else falls back):
    ///   - **group-by**: one `n.p` item + one `count(*)`/`count(n)` + any
    ///     constants → one row per distinct value of `p`, plus a null group for
    ///     nodes lacking `p` (`count(*)`/`count(n)` include nulls; `n` is never
    ///     null, so they agree).
    ///   - **distinct-count**: one `count(DISTINCT n.p)` + any constants, no
    ///     grouping item → a single row; the count is the number of distinct keys
    ///     (the index omits nulls, which `count(DISTINCT …)` also excludes).
    ///
    /// `ORDER BY`/`SKIP`/`LIMIT` are applied to the (small) grouped output via
    /// [`Self::order_skip_limit_no_input`].
    fn try_grouped_index_fast_path(&self, sq: &SingleQuery) -> Result<Option<QueryResult>> {
        if sq.reading.len() != 1 {
            return Ok(None);
        }
        let Clause::Match(m) = &sq.reading[0] else {
            return Ok(None);
        };
        if m.optional || m.where_.is_some() || m.patterns.len() != 1 {
            return Ok(None);
        }
        let pat = &m.patterns[0];
        if !pat.rels.is_empty() || pat.segments.is_some() {
            return Ok(None); // single-node patterns only
        }
        let node = &pat.start;
        if !node.props.is_empty() {
            return Ok(None); // an inline prop is an extra equality filter
        }
        let Some(label) = node.label_expr.as_ref().and_then(|e| e.as_single_atom()) else {
            return Ok(None); // exactly one positive label (null-group denominator is exact)
        };
        let var = node.var.as_deref();

        let body = &sq.ret.body;
        // `sq.ret.distinct` is intentionally NOT a guard: `lower_return_clause`
        // sets it by scanning the clause text for the word "distinct", so
        // `RETURN count(DISTINCT n.p)` reports `ret.distinct = true` even though
        // there is no `RETURN DISTINCT`. For both shapes here the output rows are
        // unique by grouping key (the null group's key is distinct too), so a
        // final-row `DISTINCT` dedup is always a no-op — safe to ignore either way.
        if body.star || body.items.is_empty() {
            return Ok(None);
        }

        // Classify each RETURN item: a grouping property `n.p`, the (single)
        // count aggregate, or a constant. Anything else ⇒ fall back.
        let mut group_prop: Option<(usize, String)> = None;
        let mut count_plain: Option<usize> = None;
        let mut count_distinct: Option<(usize, String)> = None;
        for (i, it) in body.items.iter().enumerate() {
            if let Some(p) = node_property(&it.expr, var) {
                if group_prop.is_some() {
                    return Ok(None); // more than one grouping key
                }
                group_prop = Some((i, p));
            } else if is_count_of(&it.expr, var) {
                if count_plain.is_some() || count_distinct.is_some() {
                    return Ok(None);
                }
                count_plain = Some(i);
            } else if let Some(p) = count_distinct_property(&it.expr, var) {
                if count_plain.is_some() || count_distinct.is_some() {
                    return Ok(None);
                }
                count_distinct = Some((i, p));
            } else if !matches!(it.expr, Expr::Param(_) | Expr::Literal(_)) {
                return Ok(None); // a non-constant, non-{group,count} projection
            }
        }

        // Resolve the indexed property, the count column, and (group-by only) the
        // grouping column. Mixed shapes (e.g. `n.p, count(DISTINCT n.p)`) bail.
        let (prop, group_i, count_i, is_distinct) = match (group_prop, count_plain, count_distinct)
        {
            (Some((gi, p)), Some(ci), None) => (p, Some(gi), ci, false),
            (None, None, Some((ci, p))) => (p, None, ci, true),
            _ => return Ok(None),
        };

        let Some(idx_name) = index_for(self.gen, std::slice::from_ref(label), &prop) else {
            return Ok(None); // no open range index over (label, prop)
        };
        let reader = self
            .gen
            .range_index(&idx_name)
            .expect("index_for only returns open indexes");
        // Prefer the build-time histogram (O(distinct)); it is byte-identical to
        // `distinct_key_counts` (derived from this very index), so the answer is the
        // same. Absent (over the cardinality cap / pre-v3 generation) ⇒ walk the
        // index, exactly as before.
        let groups = match self.gen.property_histogram(&idx_name) {
            Some(h) => h.to_vec(),
            None => reader.distinct_key_counts()?,
        };

        let columns: Vec<String> = body
            .items
            .iter()
            .map(|it| it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)))
            .collect();
        let empty: HashMap<String, Val> = HashMap::new();

        let out_rows: Vec<Vec<Val>> = if is_distinct {
            // Single row: distinct non-null values = number of index keys.
            let n = groups.len() as i64;
            let mut r = Vec::with_capacity(body.items.len());
            for (i, it) in body.items.iter().enumerate() {
                r.push(if i == count_i {
                    Val::Int(n)
                } else {
                    self.eval(&it.expr, &Scope::Map(&empty), None)?
                });
            }
            vec![r]
        } else {
            let group_i = group_i.expect("group-by shape has a grouping item");
            let Some(lid) = self.gen.label_id(label) else {
                return Ok(None);
            };
            let total = self.gen.label_node_count(lid);
            let indexed: u64 = groups.iter().map(|(_, n)| *n).sum();
            let null_count = total.saturating_sub(indexed);

            let row_for = |gval: Val, count: i64| -> Result<Vec<Val>> {
                let mut r = Vec::with_capacity(body.items.len());
                for (i, it) in body.items.iter().enumerate() {
                    if i == group_i {
                        r.push(gval.clone());
                    } else if i == count_i {
                        r.push(Val::Int(count));
                    } else {
                        r.push(self.eval(&it.expr, &Scope::Map(&empty), None)?);
                    }
                }
                Ok(r)
            };

            let mut rows = Vec::with_capacity(groups.len() + 1);
            for (k, n) in groups {
                rows.push(row_for(Val::from_value(k), n as i64)?);
            }
            // Nodes of `label` that lack `prop` form the null group.
            if null_count > 0 {
                rows.push(row_for(Val::Null, null_count as i64)?);
            }
            rows
        };

        let rows = self.order_skip_limit_no_input(body, &columns, out_rows)?;
        Ok(Some(QueryResult { columns, rows }))
    }

    /// Apply a projection body's `ORDER BY` → `SKIP` → `LIMIT` to already-
    /// projected `rows` whose columns are `cols`. `ORDER BY` keys reference the
    /// projected aliases only — the aggregated / fast-path case, where there is no
    /// 1:1 input table to merge in (cf. the `with_input` branch in [`Self::project`]).
    fn order_skip_limit_no_input(
        &self,
        body: &ProjectionBody,
        cols: &[String],
        mut rows: Vec<Vec<Val>>,
    ) -> Result<Vec<Vec<Val>>> {
        if !body.order_by.is_empty() {
            // The `keyed` buffer clones every row plus its sort keys, so charge the
            // row count before building it (a large ORDER BY is otherwise uncharged).
            self.charge(rows.len() as u64)?;
            let mut keyed: Vec<(SortKey, Vec<Val>)> = Vec::with_capacity(rows.len());
            for r in rows {
                let scope = Scope::Row(cols, &r);
                let mut keys = Vec::with_capacity(body.order_by.len());
                for (e, dir) in &body.order_by {
                    keys.push((self.eval(e, &scope, None)?, *dir));
                }
                keyed.push((keys, r));
            }
            keyed.sort_by(|a, b| cmp_sort_keys(&a.0, &b.0));
            rows = keyed.into_iter().map(|(_, r)| r).collect();
        }
        if let Some(skip) = &body.skip {
            let n = self.eval_count(skip)?;
            rows = rows.into_iter().skip(n).collect();
        }
        if let Some(limit) = &body.limit {
            let n = self.eval_count(limit)?;
            rows.truncate(n);
        }
        Ok(rows)
    }

    /// Row cap a final `RETURN` lets us push into the last MATCH (root cause 6 —
    /// "buffer all paths"). When the projection is a plain 1:1 map — no
    /// aggregation, no `DISTINCT`, no `ORDER BY` — with a `LIMIT`, only the first
    /// `SKIP + LIMIT` matched rows (in match-emit order) can ever survive, so the
    /// match may stop the moment it has produced that many. Returns `None` when any
    /// of those needs the full set (aggregation/`DISTINCT`/`ORDER BY`, or no
    /// `LIMIT`). The pushdown is exact: stopping early yields the *same* prefix of
    /// rows that buffering-then-truncating does, since nothing between the match and
    /// the limit reorders or drops rows. `LIMIT`/`SKIP` are constant expressions
    /// (Cypher forbids row variables there), so evaluating them here is safe.
    fn projection_row_cap(&self, body: &ProjectionBody, distinct: bool) -> Result<Option<usize>> {
        let Some(limit) = &body.limit else {
            return Ok(None);
        };
        if distinct || !body.order_by.is_empty() {
            return Ok(None);
        }
        if body.items.iter().any(|it| contains_aggregate(&it.expr)) {
            return Ok(None);
        }
        let n = self.eval_count(limit)?;
        let skip = match &body.skip {
            Some(s) => self.eval_count(s)?,
            None => 0,
        };
        Ok(Some(n.saturating_add(skip)))
    }

    /// Run a single query part starting from `seed` instead of the empty singleton.
    /// A top-level query seeds the singleton; a `CALL { … }` subquery seeds the
    /// imported outer variables (one row) so the inner clauses can reference them.
    fn run_single_seeded(&self, sq: &SingleQuery, seed: Table) -> Result<QueryResult> {
        // A pushable `RETURN … LIMIT n` caps only the LAST reading clause feeding
        // the final 1:1 projection — earlier clauses may be filtered or expanded
        // downstream, so capping them could under-produce.
        let cap = self.projection_row_cap(&sq.ret.body, sq.ret.distinct)?;
        let last = sq.reading.len();
        let mut table = seed;
        for (i, clause) in sq.reading.iter().enumerate() {
            let clause_cap = if i + 1 == last { cap } else { None };
            match clause {
                Clause::Match(m) => table = self.apply_match(table, m, clause_cap)?,
                Clause::With(w) => {
                    table = self.project(table, &w.body, w.distinct, w.where_.as_ref())?
                }
                Clause::VectorCall(vc) => table = self.apply_vector_call(table, vc)?,
                Clause::Call(cc) => table = self.apply_call(table, cc)?,
                Clause::CallSubquery(cs) => table = self.apply_call_subquery(table, cs)?,
                Clause::Unwind(uc) => table = self.apply_unwind(table, uc)?,
            }
        }
        let table = self.project(table, &sq.ret.body, sq.ret.distinct, None)?;
        Ok(QueryResult {
            columns: table.cols,
            rows: table.rows,
        })
    }

    // ── MATCH ────────────────────────────────────────────────────────────

    fn apply_match(&self, table: Table, m: &MatchClause, cap: Option<usize>) -> Result<Table> {
        // PR 3: a shortest-path selector (`ANY SHORTEST` / `ALL SHORTEST` /
        // `SHORTEST k`) drives a dedicated search between the pattern's endpoints
        // rather than the ordinary matcher, so route it out first. A selector must be
        // the sole pattern in its clause (comma-joined conjunctions alongside a
        // selector are not yet supported).
        if m.patterns.iter().any(|p| p.selector.is_some()) {
            if m.patterns.len() != 1 {
                bail!(
                    "a path selector (ANY/ALL SHORTEST or SHORTEST k) must be the only \
                     pattern in its MATCH clause"
                );
            }
            return self.apply_match_selected(table, m, cap);
        }
        // PR 2: a path restrictor is honoured only where `varlen` owns the
        // uniqueness scope — a variable-length relationship. Reject it on any other
        // pattern (a node-only or fixed-hop chain) rather than silently ignoring it,
        // so the user gets a clear message instead of unrestricted results. A
        // restrictor over a quantified group is already rejected at lowering.
        for p in &m.patterns {
            if p.restrictor.is_some() && !p.rels.iter().any(|(r, _)| r.var_length.is_some()) {
                bail!(
                    "a path restrictor (WALK/TRAIL/ACYCLIC/SIMPLE) currently requires a \
                     variable-length relationship, e.g. MATCH TRAIL (a)-[:R*]->(b)"
                );
            }
        }
        // Stage 5: a single non-optional node-only pattern (no relationships, no
        // path variable, fresh-scan anchor) streams candidates straight into rows,
        // skipping the per-row `HashMap` binding the general matcher builds (root
        // cause 4).
        if let Some(t) = self.try_stream_match(&table, m, cap)? {
            return Ok(t);
        }
        // GQL quantified path patterns (`((…)){m,n}`) take a separate path that
        // desugars each group into the union of its fixed-length expansions. The
        // common (quantifier-free) case stays on the hot path below untouched.
        if m.patterns.iter().any(|p| p.segments.is_some()) {
            return self.apply_match_quantified(table, m, cap);
        }
        // Variables this clause newly introduces, appended to the scope in order.
        let mut new_vars: Vec<String> = Vec::new();
        for p in &m.patterns {
            collect_pattern_vars(p, &table.cols, &mut new_vars);
        }
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        let mut out_rows = Vec::new();
        for row in &table.rows {
            // Stage 6: stop once a pushed `LIMIT` is satisfied — the cumulative cap
            // across all seed rows. The per-seed match is also capped at the rows
            // still needed, so a single seed expanding millions of paths halts early.
            if cap.is_some_and(|c| out_rows.len() >= c) {
                break;
            }
            self.check_deadline()?;
            let mut seed: HashMap<String, Val> = HashMap::with_capacity(table.cols.len());
            for (c, v) in table.cols.iter().zip(row) {
                seed.insert(c.clone(), v.clone());
            }
            let mut matches: Vec<HashMap<String, Val>> = Vec::new();
            let remaining = cap.map(|c| c.saturating_sub(out_rows.len()));
            self.match_patterns(
                &m.patterns,
                0,
                seed,
                m.where_.as_ref(),
                &mut matches,
                remaining,
            )?;

            if matches.is_empty() && m.optional {
                let mut r = row.clone();
                r.extend(std::iter::repeat_n(Val::Null, new_vars.len()));
                out_rows.push(r);
            } else {
                for b in matches {
                    let mut r = row.clone();
                    for v in &new_vars {
                        r.push(b.get(v).cloned().unwrap_or(Val::Null));
                    }
                    out_rows.push(r);
                }
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// `MATCH` containing one or more GQL quantified path patterns
    /// (`((…)){m,n}`). Each source pattern is desugared into the union of its
    /// fixed-length expansions (`expand_quantified_pattern`); the cartesian product
    /// of the per-pattern alternatives gives the conjunctive pattern-lists to run.
    /// Every alternative introduces the same named variables (boundary nodes only —
    /// group-internal nodes/relationships are anonymised), so the output column set
    /// is well defined. Each expansion is an ordinary (`segments: None`) pattern, so
    /// it reuses the full matcher, including edge-uniqueness, `node_ok`, the
    /// intermediate budget, and the deadline.
    ///
    /// Semantics: as with Cypher variable-length, one row is emitted per matching
    /// path, so two repetition counts that bind the same boundary nodes produce two
    /// rows (add `DISTINCT` to collapse them) — exactly what `-[*1..2]-` does.
    fn apply_match_quantified(
        &self,
        table: Table,
        m: &MatchClause,
        cap: Option<usize>,
    ) -> Result<Table> {
        let alts: Vec<Vec<Pattern>> = m
            .patterns
            .iter()
            .map(expand_quantified_pattern)
            .collect::<Result<_>>()?;
        let combos = cartesian_patterns(&alts);
        debug_assert!(
            !combos.is_empty(),
            "every quantified group has ≥1 expansion"
        );

        // New variables are identical across combos by construction; derive from the
        // first so the column layout matches every expansion.
        let mut new_vars: Vec<String> = Vec::new();
        for p in &combos[0] {
            collect_pattern_vars(p, &table.cols, &mut new_vars);
        }
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        let mut out_rows = Vec::new();
        for row in &table.rows {
            if cap.is_some_and(|c| out_rows.len() >= c) {
                break;
            }
            self.check_deadline()?;
            let mut seed: HashMap<String, Val> = HashMap::with_capacity(table.cols.len());
            for (c, v) in table.cols.iter().zip(row) {
                seed.insert(c.clone(), v.clone());
            }
            // Accumulate all expansions' matches for this seed row before emitting,
            // so OPTIONAL's "no match" test sees every alternative.
            let mut matches: Vec<HashMap<String, Val>> = Vec::new();
            let remaining = cap.map(|c| c.saturating_sub(out_rows.len()));
            for combo in &combos {
                if remaining.is_some_and(|r| matches.len() >= r) {
                    break;
                }
                self.match_patterns(
                    combo,
                    0,
                    seed.clone(),
                    m.where_.as_ref(),
                    &mut matches,
                    remaining,
                )?;
            }

            if matches.is_empty() && m.optional {
                let mut r = row.clone();
                r.extend(std::iter::repeat_n(Val::Null, new_vars.len()));
                out_rows.push(r);
            } else {
                for b in matches {
                    if cap.is_some_and(|c| out_rows.len() >= c) {
                        break;
                    }
                    let mut r = row.clone();
                    for v in &new_vars {
                        r.push(b.get(v).cloned().unwrap_or(Val::Null));
                    }
                    out_rows.push(r);
                }
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// `MATCH` carrying a GQL shortest-path selector (`ANY SHORTEST` / `ALL SHORTEST`
    /// / `SHORTEST k`). The pattern is a single relationship between two endpoints;
    /// for every endpoint pair (each side either already bound, or scanned and
    /// filtered by its node pattern) the selector picks shortest connecting paths via
    /// the shared BFS core [`select_paths`] — the same core `shortestPath()` uses.
    /// Each chosen path becomes one output row binding the endpoints, the (list-
    /// valued) relationship variable and any path variable; the clause `WHERE` is
    /// applied per row, exactly as the ordinary matcher does.
    ///
    /// Scope (PR 3): a selector requires a single-relationship pattern (like
    /// `shortestPath()`), carries no relationship property filter, and cannot yet be
    /// combined with a path restrictor — those are rejected with a clear message. A
    /// selector over a quantified group is already rejected at lowering.
    fn apply_match_selected(
        &self,
        table: Table,
        m: &MatchClause,
        cap: Option<usize>,
    ) -> Result<Table> {
        let p = &m.patterns[0];
        let selector = p.selector.expect("routed here only for a selected pattern");
        if p.restrictor.is_some() {
            bail!(
                "combining a path selector with a path restrictor \
                 (WALK/TRAIL/ACYCLIC/SIMPLE) is not yet supported"
            );
        }
        if p.rels.len() != 1 {
            bail!(
                "a path selector (ANY/ALL SHORTEST or SHORTEST k) currently requires a \
                 single relationship, e.g. MATCH ANY SHORTEST (a)-[:R*]->(b)"
            );
        }
        let (rel, end) = &p.rels[0];
        if !rel.props.is_empty() {
            bail!("filters on relationships under a path selector are not supported");
        }
        let (min, max) = match &rel.var_length {
            Some(vl) => varlen_bounds(vl),
            None => (1, 1),
        };

        let mut new_vars: Vec<String> = Vec::new();
        collect_pattern_vars(p, &table.cols, &mut new_vars);
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        let mut out_rows = Vec::new();
        for row in &table.rows {
            if cap.is_some_and(|c| out_rows.len() >= c) {
                break;
            }
            self.check_deadline()?;
            let mut seed: HashMap<String, Val> = HashMap::with_capacity(table.cols.len());
            for (c, v) in table.cols.iter().zip(row) {
                seed.insert(c.clone(), v.clone());
            }

            // Endpoint candidates: a bound endpoint is its single node; a free one is
            // scanned and filtered by its node pattern's labels/inline props.
            let srcs = self.endpoint_candidates(&p.start, &seed, m.where_.as_ref())?;
            let dsts = self.endpoint_candidates(end, &seed, m.where_.as_ref())?;

            // Bound the |srcs|×|dsts| search fan-out: with two free endpoints this launches a
            // separate shortest-path search for every (src, dst) pair — quadratic in the scanned
            // id space. Charge the product up front. It is self-scaling: a bound endpoint is a
            // single candidate, so this only bites when *both* endpoints are free and large (the
            // pathological case), and it trips the standard `maxIntermediate` budget before the
            // searches run rather than after they have burned the graph.
            self.charge(srcs.len().saturating_mul(dsts.len()) as u64)?;

            let mut matches: Vec<HashMap<String, Val>> = Vec::new();
            for &src in &srcs {
                for &dst in &dsts {
                    for hops in self.select_paths(src, dst, rel, (min, max), selector)? {
                        let mut b = seed.clone();
                        if let Some(v) = &p.start.var {
                            b.insert(v.clone(), Val::Node(src));
                        }
                        // A shared endpoint variable (e.g. `(a)-[*]->(a)`) must agree:
                        // skip the pair when the end node would contradict a binding
                        // the start (or seed) already fixed.
                        if let Some(v) = &end.var {
                            if let Some(existing) = b.get(v) {
                                if existing.loose_eq(&Val::Node(dst)) != Some(true) {
                                    continue;
                                }
                            } else {
                                b.insert(v.clone(), Val::Node(dst));
                            }
                        }
                        if let Some(v) = &rel.var {
                            let rels = Val::List(hops.iter().map(Hop::as_rel).collect());
                            b.insert(v.clone(), rels);
                        }
                        if let Some(pv) = &p.path_var {
                            b.insert(pv.clone(), make_path(src, &hops));
                        }
                        if let Some(w) = m.where_.as_ref() {
                            if !truthy(&self.eval(w, &Scope::Map(&b), None)?) {
                                continue;
                            }
                        }
                        matches.push(b);
                    }
                }
            }

            if matches.is_empty() && m.optional {
                let mut r = row.clone();
                r.extend(std::iter::repeat_n(Val::Null, new_vars.len()));
                out_rows.push(r);
            } else {
                for b in matches {
                    if cap.is_some_and(|c| out_rows.len() >= c) {
                        break;
                    }
                    let mut r = row.clone();
                    for v in &new_vars {
                        r.push(b.get(v).cloned().unwrap_or(Val::Null));
                    }
                    out_rows.push(r);
                }
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// Candidate node ids for one endpoint of a selected pattern. A variable already
    /// bound to a node (by the seed/an earlier clause) is that single node; bound to
    /// a non-node it cannot match (empty). A free endpoint is scanned with the usual
    /// planner strategy and filtered by `node_ok` (its labels + inline props), so an
    /// endpoint like `(b:Person)` only contributes `:Person` nodes.
    fn endpoint_candidates(
        &self,
        node: &NodePat,
        binding: &HashMap<String, Val>,
        where_: Option<&Expr>,
    ) -> Result<Vec<u64>> {
        match node.var.as_deref().and_then(|v| binding.get(v)) {
            Some(Val::Node(id)) => Ok(vec![*id]),
            Some(_) => Ok(Vec::new()),
            None => {
                let bound = bound_scalars(binding);
                let scan = choose_node_scan(self.gen, node, where_, &self.plan_params, &bound);
                let guaranteed = self.scan_guaranteed_labels(&scan);
                // Streamed: only the survivors are retained, never the candidate set as
                // well (an endpoint of a quantified pattern can be a full-width scan).
                let mut out = Vec::new();
                let mut stream = self.candidate_stream(&scan)?;
                while let Some(batch) = self.next_candidates(&mut stream)? {
                    for &c in batch {
                        if self.node_ok(c, node, &Scope::Map(binding), &guaranteed)? {
                            out.push(c);
                        }
                    }
                }
                Ok(out)
            }
        }
    }

    /// Shared shortest-path BFS core driving both `shortestPath()` and the GQL path
    /// selectors. Between two concrete nodes `src`/`dst` it returns the chosen paths
    /// as hop-lists in walk (start→end) order:
    /// - `AnyShortest` → at most one shortest path;
    /// - `AllShortest` → every path of the single minimum length;
    /// - `ShortestK(k)` → up to `k` paths in non-decreasing length order.
    ///
    /// `AnyShortest` (the `shortestPath()` case) needs just one path, so it runs a
    /// single global-`visited` BFS with a back-pointer map ([`Self::any_shortest_path`]):
    /// each node is enqueued at most once (frontier ≤ |V|, work `O(V+E)`), BFS first
    /// reaches every node along a shortest path, and the reconstructed walk is
    /// automatically simple.
    ///
    /// `AllShortest`/`ShortestK` can have exponentially many shortest paths, so they
    /// keep the loopless simple-path search below: each frontier entry carries its own
    /// cloned `visited` set, so a node reachable by many prefixes is re-enqueued once
    /// per prefix. On a hub-dense small-world graph that frontier explodes, so the
    /// per-layer `maxIntermediate` charge is its backstop (it rejects the blow-up
    /// instead of OOMing). Paths are loopless (no node repeats), bounding the walk on a
    /// cyclic graph; every entry in a BFS layer has the same hop count, so paths
    /// surface in non-decreasing length order — the property those selectors rely on.
    /// `min`/`max` are the relationship's length bounds (a fixed hop is `(1, 1)`);
    /// `min == 0` with coincident endpoints admits the empty path.
    fn select_paths(
        &self,
        src: u64,
        dst: u64,
        rel: &RelPat,
        bounds: (u32, u32),
        selector: PathSelector,
    ) -> Result<Vec<Vec<Hop>>> {
        let (min, max) = bounds;
        let empty = HashMap::new();
        if matches!(selector, PathSelector::AnyShortest) {
            return self.any_shortest_path(src, dst, rel, bounds);
        }
        let want = match selector {
            PathSelector::AnyShortest => 1,
            PathSelector::ShortestK(k) => k as usize,
            PathSelector::AllShortest => usize::MAX,
        };
        let mut results: Vec<Vec<Hop>> = Vec::new();

        // min == 0 admits the empty (single-node) path when the endpoints coincide.
        if min == 0 && src == dst {
            results.push(Vec::new());
            if results.len() >= want {
                return Ok(results);
            }
        }
        if max == 0 {
            return Ok(results);
        }

        // Each frontier entry carries its own loopless `visited` set so sibling
        // branches stay simple independently. (node, path so far, visited nodes).
        let mut frontier: Vec<(u64, Vec<Hop>, HashSet<u64>)> =
            vec![(src, Vec::new(), HashSet::from([src]))];
        let mut depth = 0u32;
        // `AllShortest`: once `dst` is first reached, its layer is the minimum length;
        // after that layer is fully processed no further shortest path can appear.
        let mut found_min = false;
        while !frontier.is_empty() && depth < max {
            self.check_deadline()?;
            let mut next = Vec::new();
            for (node, path, visited) in &frontier {
                for hop in self.expand_one_hop(*node, rel, &empty)? {
                    let nb = hop.neighbour;
                    if visited.contains(&nb) {
                        continue; // loopless: never revisit a node on this path
                    }
                    if nb == dst {
                        // A connecting path ends here; a loopless path is never
                        // extended past its destination.
                        let len = path.len() as u32 + 1;
                        if len >= min {
                            let mut hops = path.clone();
                            hops.push(hop);
                            self.charge(hops.len() as u64 + 1)?;
                            results.push(hops);
                            found_min = true;
                            if results.len() >= want {
                                return Ok(results);
                            }
                        }
                        continue;
                    }
                    // Charge this live branch *before* cloning its path + visited set.
                    // Each branch carries a cloned `Vec<Hop>` + cloned `HashSet<u64>`, so
                    // on a hub-dense small-world graph a single layer's frontier can
                    // explode to millions of entries. Charging the whole layer only
                    // *after* it is materialised lets that one layer exhaust RSS before
                    // the budget ever trips (it OOM-killed the capped container); charging
                    // per branch trips the standard `maxIntermediate` budget mid-layer,
                    // before the clones accumulate. The charge is **proportional to the
                    // branch's clone size** (`path.len()+1`, mirroring the result charge
                    // above) — a fixed `charge(1)` under-counted a deep branch by a factor
                    // of its depth, so the budget only tripped long after the O(depth)
                    // clones had accumulated. Only emitted results are charged elsewhere.
                    self.charge(path.len() as u64 + 1)?;
                    let mut npath = path.clone();
                    npath.push(hop);
                    let mut nvisited = visited.clone();
                    nvisited.insert(nb);
                    next.push((nb, npath, nvisited));
                }
            }
            // `AllShortest` stops after the first dst-bearing layer; the others stop
            // only on `want`/exhaustion (handled above and by the loop condition).
            if found_min && matches!(selector, PathSelector::AllShortest) {
                return Ok(results);
            }
            frontier = next;
            depth += 1;
        }
        Ok(results)
    }

    /// `ANY SHORTEST` / `shortestPath()`: one shortest path between `src` and `dst`,
    /// via **bidirectional** BFS — a forward search from `src` along the pattern
    /// direction and a backward search from `dst` along the *reverse* direction,
    /// expanding the smaller frontier each step until the two search spheres meet.
    ///
    /// Why bidirectional: on a small-world / scale-free graph the k-hop ball grows
    /// roughly exponentially in k, so a one-sided BFS to depth `max` can touch a large
    /// fraction of a giant component (≈766 M edge reads on full Wikidata → minutes,
    /// I/O-bound). Meeting in the middle replaces one depth-`max` ball with two
    /// depth-`max/2` balls — exponentially less work *and* memory. Each side keeps a
    /// dense bitset `visited` (≈ node_count/8 bytes) plus a `node -> (neighbour, depth)`
    /// map; the discovering `Hop`s are re-derived from the CSR during reconstruction,
    /// so the resident structures stay small. The deadline is checked *within* a level
    /// (every few thousand expansions) so a runaway search aborts at `timeoutMs` rather
    /// than overrunning between levels. The optional, dedicated `maxShortestPathExplore`
    /// cap (0 = unlimited) bounds the total nodes either search may hold, independent of
    /// the shared `maxIntermediate` budget — preserving the always-succeeds guarantee
    /// by default.
    ///
    /// `max` caps the *total* path length; `min` filters the result. `min == 0` with
    /// coincident endpoints admits the empty path. (For `shortestPath()` `min ∈ {0,1}`,
    /// so the discovered shortest distance always meets it whenever a path exists.)
    fn any_shortest_path(
        &self,
        src: u64,
        dst: u64,
        rel: &RelPat,
        bounds: (u32, u32),
    ) -> Result<Vec<Vec<Hop>>> {
        let (min, max) = bounds;
        let empty = HashMap::new();

        // min == 0 admits the empty (single-node) path when the endpoints coincide.
        if min == 0 && src == dst {
            return Ok(vec![Vec::new()]);
        }
        if max == 0 {
            return Ok(Vec::new());
        }

        let node_count = self.gen.node_count();
        let words = node_count.div_ceil(64) as usize;
        let (mut fvis, mut bvis) = (vec![0u64; words], vec![0u64; words]);
        let bit = |v: &[u64], id: u64| (v[(id >> 6) as usize] >> (id & 63)) & 1 != 0;
        let set = |v: &mut [u64], id: u64| v[(id >> 6) as usize] |= 1u64 << (id & 63);
        set(&mut fvis, src);
        set(&mut bvis, dst);

        let cap = self.max_shortest_path_explore;
        let mut discovered: u64 = 2; // the two endpoints are already held resident
        if cap != 0 && discovered > cap {
            return Err(ExecLimit::ShortestPathCap(cap).into());
        }

        // `node -> (neighbour toward the seed, depth from the seed)`.
        let mut fpar: HashMap<u64, (u64, u32)> = HashMap::new();
        let mut bpar: HashMap<u64, (u64, u32)> = HashMap::new();
        let (mut ffront, mut bfront) = (vec![src], vec![dst]);
        let (mut fdepth, mut bdepth) = (0u32, 0u32);
        let fdir = rel.dir;
        let bdir = match rel.dir {
            Direction::Outgoing => Direction::Incoming,
            Direction::Incoming => Direction::Outgoing,
            Direction::Undirected => Direction::Undirected,
        };
        // Parallel frontier expansion is sound only for a property-free pattern whose
        // type constraint is a flat reltype-id set (or absent): the off-thread worker
        // reads adjacency but must not evaluate a rel predicate (that would touch the
        // executor's interior-mutable state). Anything richer expands sequentially.
        let type_ids: Option<Vec<u32>> = rel.type_expr.as_ref().and_then(|e| {
            e.positive_atoms().map(|names| {
                names
                    .iter()
                    .filter_map(|t| self.gen.reltype_id(t))
                    .collect()
            })
        });
        let fast = rel.props.is_empty() && (rel.type_expr.is_none() || type_ids.is_some());
        let mut best: Option<(u32, u64)> = None; // (total length, meeting node)
        let mut since_check = 0u64;
        const SP_PAR_MIN_FRONTIER: usize = 64; // below this, the pool overhead isn't worth it

        loop {
            self.check_deadline()?;
            let combined = fdepth + bdepth;
            let bound = best.map(|(b, _)| b.min(max)).unwrap_or(max);
            // No future meeting can be shorter than `combined + 1`, so once the two
            // radii sum to the best-so-far (or `max`) we are done.
            if combined >= bound || ffront.is_empty() || bfront.is_empty() {
                break;
            }

            // Expand whichever frontier is smaller (the bidirectional speed-up).
            let forward = ffront.len() <= bfront.len();
            let (front, dir, depth) = if forward {
                (&ffront, fdir, fdepth + 1)
            } else {
                (&bfront, bdir, bdepth + 1)
            };

            // Gather the level's neighbours per frontier node. The I/O-bound adjacency
            // reads (CSR block fetch + zstd decompress, released from the cache mutex)
            // overlap across the pool; all `visited`/`parent`/meeting mutation happens
            // single-threaded in the merge below.
            let expansions: Vec<(u64, Vec<u64>)> = if fast {
                let tids = type_ids.as_deref();
                let (gen, cache) = (self.gen, self.cache);
                par_gather(
                    self.fanout_pool.as_deref(),
                    front,
                    SP_PAR_MIN_FRONTIER,
                    |&node| neighbours_par(gen, cache, node, dir, tids).map(|nbs| (node, nbs)),
                )?
            } else {
                let mut v = Vec::with_capacity(front.len());
                for &node in front {
                    let nbs = self
                        .expand_with_dir(node, rel, dir, &empty)?
                        .into_iter()
                        .map(|h| h.neighbour)
                        .collect();
                    v.push((node, nbs));
                }
                v
            };

            let mut next = Vec::new();
            for (node, nbs) in expansions {
                for nb in nbs {
                    let (mine, theirs) = if forward {
                        (&mut fvis, &bvis)
                    } else {
                        (&mut bvis, &fvis)
                    };
                    if bit(mine, nb) {
                        continue; // already on a ≤-length shortest path this side
                    }
                    set(mine, nb);
                    discovered += 1;
                    if cap != 0 && discovered > cap {
                        return Err(ExecLimit::ShortestPathCap(cap).into());
                    }
                    if forward {
                        fpar.insert(nb, (node, depth));
                    } else {
                        bpar.insert(nb, (node, depth));
                    }
                    if bit(theirs, nb) {
                        // The other search already reached `nb`: its depth there is the
                        // seed (0) or the recorded value.
                        let other = if forward { &bpar } else { &fpar };
                        let other_seed = if forward { dst } else { src };
                        let od = if nb == other_seed {
                            0
                        } else {
                            other.get(&nb).map(|&(_, d)| d).unwrap_or(0)
                        };
                        let total = depth + od;
                        if total >= min && best.map(|(b, _)| total < b).unwrap_or(true) {
                            best = Some((total, nb));
                        }
                    }
                    next.push(nb);
                    since_check += 1;
                    if since_check >= 4096 {
                        self.check_deadline()?;
                        since_check = 0;
                    }
                }
            }
            if forward {
                ffront = next;
                fdepth = depth;
            } else {
                bfront = next;
                bdepth = depth;
            }
        }

        match best {
            Some((total, meet)) if total >= min && total <= max => {
                let nodes = bidir_node_path(src, dst, meet, &fpar, &bpar);
                Ok(vec![self.reconstruct_from_node_path(&nodes, rel)?])
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Rebuild the hop sequence for a node path (consecutive ids in walk order),
    /// re-deriving each [`Hop`] from the CSR — the bidirectional search stored only
    /// neighbour ids to keep its working set small. For each step `u -> v` we take the
    /// first pattern-typed edge from `u` that lands on `v`; any such edge is a valid
    /// shortest-path edge (parallel multi-edges are interchangeable for `AnyShortest`).
    fn reconstruct_from_node_path(&self, nodes: &[u64], rel: &RelPat) -> Result<Vec<Hop>> {
        let empty = HashMap::new();
        let mut hops = Vec::with_capacity(nodes.len().saturating_sub(1));
        for w in nodes.windows(2) {
            let (u, v) = (w[0], w[1]);
            let hop = self
                .expand_with_dir(u, rel, rel.dir, &empty)?
                .into_iter()
                .find(|h| h.neighbour == v)
                .expect("path edge re-derivable from the CSR");
            hops.push(hop);
        }
        Ok(hops)
    }

    /// Stream a single node-only `MATCH` (one pattern, no relationships, no path
    /// variable, anchor not already bound) directly into output rows, returning
    /// the new table or `None` when the pattern needs the general matcher.
    ///
    /// The general path materialises a `Vec<HashMap<String, Val>>` — one cloned
    /// binding map per matched row (root cause 4). For a bare label/index scan the
    /// only new binding is the anchor node, so we append `Val::Node(id)` to a clone
    /// of the input row and skip the map entirely. The anchor scan is chosen once
    /// (parameter/`WHERE`-aware, like the general path), `node_ok` enforces the
    /// pattern's labels/inline props, and the clause `WHERE` is re-evaluated per
    /// emitted row against the full row scope — identical semantics to
    /// `match_patterns`, including row order and the per-row intermediate charge.
    fn try_stream_match(
        &self,
        table: &Table,
        m: &MatchClause,
        cap: Option<usize>,
    ) -> Result<Option<Table>> {
        if m.optional || m.patterns.len() != 1 {
            return Ok(None);
        }
        let p = &m.patterns[0];
        if !p.rels.is_empty() || p.path_var.is_some() || p.segments.is_some() {
            return Ok(None);
        }
        let start = &p.start;
        // An already-bound anchor is a single concrete node, handled by the general
        // matcher's bound-anchor branch; only a fresh scan streams here.
        if let Some(v) = &start.var {
            if table.cols.contains(v) {
                return Ok(None);
            }
        }

        // A *correlated* anchor keys its index off a column already in `table`
        // (e.g. `UNWIND $ids AS w MATCH (n:L {p: w})`, or `WHERE n.p = w`): the seek
        // depends on the row, so the scan must move inside the loop. When the anchor
        // is uncorrelated we plan once and reuse it for every row — today's fast path.
        let correlated = anchor_correlated(start, m.where_.as_ref(), &table.cols);
        let hoisted = if correlated {
            None
        } else {
            let scan = choose_node_scan(
                self.gen,
                start,
                m.where_.as_ref(),
                &self.plan_params,
                &HashMap::new(),
            );
            let guaranteed = self.scan_guaranteed_labels(&scan);
            Some((guaranteed, scan))
        };
        // A *multi-row* input against an uncorrelated anchor is a cartesian product: every
        // input row revisits every candidate, so derive the ids once and replay them rather
        // than re-running the sweep per row (which would re-read the label column once per
        // row). A single input row — the seed, or a `WITH` that collapsed to one — streams,
        // so `LIMIT` short-circuits the scan: the case the bounded-memory invariant turns on.
        let shared: Option<Vec<u64>> = match &hoisted {
            Some((_, scan)) if table.rows.len() > 1 => Some(self.scan_candidates(scan)?),
            _ => None,
        };

        let mut out_cols = table.cols.clone();
        if let Some(v) = &start.var {
            out_cols.push(v.clone());
        }

        let mut out_rows = Vec::new();
        'outer: for in_row in &table.rows {
            self.check_deadline()?;
            // Binding for inline-prop evaluation in `node_ok`, built once per input
            // row (the anchor's own var is intentionally absent, as in the general
            // path). Typically one row — the singleton seed — so one map per query.
            let in_binding: HashMap<String, Val> = table
                .cols
                .iter()
                .cloned()
                .zip(in_row.iter().cloned())
                .collect();
            // The hoisted plan (streamed, or replayed from the shared set), or a per-row
            // index seek keyed by this row's scalars.
            let per_row;
            let (guaranteed, mut stream): (&[u32], CandidateStream) = match (&hoisted, &shared) {
                (Some((g, _)), Some(ids)) => (g, CandidateStream::ready(ids)),
                (Some((g, scan)), None) => (g, self.candidate_stream(scan)?),
                (None, _) => {
                    let bound = bound_scalars(&in_binding);
                    let scan = choose_node_scan(
                        self.gen,
                        start,
                        m.where_.as_ref(),
                        &self.plan_params,
                        &bound,
                    );
                    per_row = self.scan_guaranteed_labels(&scan);
                    let stream = self.candidate_stream(&scan)?;
                    (&per_row, stream)
                }
            };
            while let Some(batch) = self.next_candidates(&mut stream)? {
                for &c in batch {
                    // Stage 6: honour a pushed `LIMIT` (no ORDER BY/aggregation/DISTINCT)
                    // so a bare `MATCH (n:L) … LIMIT k` scans only k matching nodes — and,
                    // now that the scan is a stream, stops producing candidates too.
                    if cap.is_some_and(|cc| out_rows.len() >= cc) {
                        break 'outer;
                    }
                    if !self.node_ok(c, start, &Scope::Map(&in_binding), guaranteed)? {
                        continue;
                    }
                    let mut row = in_row.clone();
                    if start.var.is_some() {
                        row.push(Val::Node(c));
                    }
                    if let Some(w) = m.where_.as_ref() {
                        if !truthy(&self.eval(w, &Scope::Row(&out_cols, &row), None)?) {
                            continue;
                        }
                    }
                    self.charge(1)?;
                    out_rows.push(row);
                }
            }
        }
        Ok(Some(Table {
            cols: out_cols,
            rows: out_rows,
        }))
    }

    // ── UNWIND ───────────────────────────────────────────────────────────────

    /// Multiply each input row by the elements of the list `uc.expr` evaluates to,
    /// binding each element to `uc.var`. Matching FalkorDB's `op_unwind` (`_initList`):
    /// a list expands element-wise, NULL and the empty list emit zero rows, and any
    /// other scalar is wrapped as a single-element list (one row) — a deliberate
    /// FalkorDB divergence from Neo4j (which errors on `UNWIND 5`).
    fn apply_unwind(&self, table: Table, uc: &UnwindClause) -> Result<Table> {
        let mut out_cols = table.cols.clone();
        // The alias is a fresh binding appended after the input columns. (A name
        // clash with an existing column would shadow it on read; the eu-ai-act
        // service never re-uses an in-scope name here.)
        out_cols.push(uc.var.clone());

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let scope = Scope::Row(&table.cols, row);
            let items = match self.eval(&uc.expr, &scope, None)? {
                Val::List(xs) => xs,
                Val::Null => continue,  // null → zero rows
                scalar => vec![scalar], // scalar → wrap as [scalar] → one row
            };
            for item in items {
                let mut r = row.clone();
                r.push(item);
                self.charge(r.len() as u64)?;
                out_rows.push(r);
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    // ── CALL <metadata procedure> (Phase 11) ────────────────────────────────

    /// Run a read-only metadata procedure and bind its outputs into the table.
    /// The procedures take no arguments and produce rows independent of the input
    /// bindings, so the result is the input table × the procedure rows, projected
    /// to the `YIELD`ed columns (or all outputs when `YIELD` is absent) with the
    /// optional `YIELD … WHERE` applied. Mirrors [`Self::apply_vector_call`]'s
    /// binding/`WHERE` handling.
    fn apply_call(&self, table: Table, cc: &CallClause) -> Result<Table> {
        let lname = cc.name.to_ascii_lowercase();
        // algo.* graph-algorithm procedures take arguments (which may reference bound
        // variables) and compute their rows from the graph, so they follow the
        // per-row model of `apply_vector_call`, not the input-independent path below.
        if is_algo_proc(&lname) {
            return self.apply_algo_call(table, cc, &lname);
        }
        if !cc.args.is_empty() {
            bail!("{}() takes no arguments", cc.name);
        }
        let (out_names, proc_rows) = self.procedure_rows(&lname)?;

        // (output index, bound name) pairs: YIELD selects/reorders/aliases; a bare
        // call binds every output under its own name.
        let bindings: Vec<(usize, String)> = if cc.yields.is_empty() {
            out_names
                .iter()
                .enumerate()
                .map(|(i, n)| (i, n.clone()))
                .collect()
        } else {
            let mut v = Vec::with_capacity(cc.yields.len());
            for (output, bound) in &cc.yields {
                let idx = out_names
                    .iter()
                    .position(|n| n.eq_ignore_ascii_case(output))
                    .ok_or_else(|| anyhow::anyhow!("{}() does not yield '{output}'", cc.name))?;
                v.push((idx, bound.clone()));
            }
            v
        };

        let mut out_cols = table.cols.clone();
        out_cols.extend(bindings.iter().map(|(_, b)| b.clone()));

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            for prow in &proc_rows {
                let mut r = row.clone();
                for (idx, _) in &bindings {
                    r.push(prow[*idx].clone());
                }
                if let Some(w) = &cc.where_ {
                    let scope = Scope::Row(&out_cols, &r);
                    if three_valued(&self.eval(w, &scope, None)?) != Some(true) {
                        continue;
                    }
                }
                out_rows.push(r);
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    // ── algo.* graph-algorithm procedures (Phase 13) ─────────────────────────

    /// Per-row dispatch for an `algo.*` procedure: evaluate the arguments against
    /// each input row (they may reference bound variables, e.g. `algo.BFS(a, …)`),
    /// compute the procedure's rows from the graph, then cross-product input rows ×
    /// proc rows and bind the YIELD outputs. Mirrors [`Self::apply_vector_call`]'s
    /// per-row binding/`WHERE` handling; the proc rows carry every output in its
    /// canonical order, so a partial YIELD just selects a subset.
    fn apply_algo_call(&self, table: Table, cc: &CallClause, lname: &str) -> Result<Table> {
        let out_names = algo_outputs(lname);

        // (output index, bound name) pairs: YIELD selects/reorders/aliases; a bare
        // call binds every output under its own name (case-insensitive match).
        let bindings: Vec<(usize, String)> = if cc.yields.is_empty() {
            out_names
                .iter()
                .enumerate()
                .map(|(i, n)| (i, n.to_string()))
                .collect()
        } else {
            let mut v = Vec::with_capacity(cc.yields.len());
            for (output, bound) in &cc.yields {
                let idx = out_names
                    .iter()
                    .position(|n| n.eq_ignore_ascii_case(output))
                    .ok_or_else(|| anyhow::anyhow!("{}() does not yield '{output}'", cc.name))?;
                v.push((idx, bound.clone()));
            }
            v
        };

        let mut out_cols = table.cols.clone();
        out_cols.extend(bindings.iter().map(|(_, b)| b.clone()));

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let scope = Scope::Row(&table.cols, row);
            let args: Vec<Val> = cc
                .args
                .iter()
                .map(|e| self.eval(e, &scope, None))
                .collect::<Result<_>>()?;
            let proc_rows = self.algo_rows(lname, &args)?;
            for prow in &proc_rows {
                let mut r = row.clone();
                for (idx, _) in &bindings {
                    r.push(prow[*idx].clone());
                }
                if let Some(w) = &cc.where_ {
                    let row_scope = Scope::Row(&out_cols, &r);
                    if three_valued(&self.eval(w, &row_scope, None)?) != Some(true) {
                        continue;
                    }
                }
                // Each retained output row is charged, so an N-row input table crossed
                // with a per-row `algo.*` accumulates against the cumulative budget
                // rather than materialising for free.
                self.charge(1)?;
                out_rows.push(r);
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// Compute the rows of an `algo.*` procedure for one set of evaluated arguments.
    /// Each row carries every output of the procedure in canonical order (see
    /// [`algo_outputs`]).
    fn algo_rows(&self, lname: &str, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        match lname {
            "algo.bfs" => self.algo_bfs(args),
            "algo.wcc" => self.algo_components(args),
            "algo.pagerank" => self.algo_pagerank(args),
            "algo.harmoniccentrality" => self.algo_harmonic(args),
            "algo.betweenness" => self.algo_betweenness(args),
            "algo.labelpropagation" => self.algo_labelprop(args),
            other => bail!("unknown procedure '{other}'"),
        }
    }

    /// `algo.BFS(source, maxLevel, relationshipType)` — single-source BFS, yielding
    /// one row `[nodes, edges]` of the reachable nodes (excluding the source) and the
    /// tree edge that first reached each. `maxLevel <= 0` is unlimited; a positive
    /// value caps the BFS depth. A NULL source, an unknown relationship type, or an
    /// unreachable source all produce **zero** rows (FalkorDB emits nothing).
    fn algo_bfs(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        if args.len() != 3 {
            bail!("algo.BFS expects 3 arguments (source, maxLevel, relationshipType)");
        }
        let source = match &args[0] {
            Val::Node(id) => *id,
            Val::Null => return Ok(Vec::new()),
            other => bail!("algo.BFS source must be a node, got {}", other.to_display()),
        };
        let max_level = match &args[1] {
            Val::Int(n) => *n,
            other => bail!(
                "algo.BFS maxLevel must be an integer, got {}",
                other.to_display()
            ),
        };
        let reltype: Option<u32> = match &args[2] {
            Val::Null => None,
            Val::Str(s) => match self.gen.reltype_id(s) {
                Some(id) => Some(id),
                None => return Ok(Vec::new()),
            },
            other => bail!(
                "algo.BFS relationshipType must be a string or null, got {}",
                other.to_display()
            ),
        };
        let unlimited = max_level <= 0;

        let mut visited = std::collections::HashSet::new();
        visited.insert(source);
        let mut queue = std::collections::VecDeque::new();
        queue.push_back((source, 0i64));
        let mut nodes: Vec<Val> = Vec::new();
        let mut edges: Vec<Val> = Vec::new();
        // `nodes`/`edges`/`visited` grow over the reachable subgraph, so the loop is
        // both memory- and time-bounded here: `charge` caps the retained `Val`s
        // against `maxIntermediate` (two per discovered node — one `Val::Node`, one
        // `Val::Rel`), and a per-pop `check_deadline` makes a runaway
        // `algo.BFS(src, 0, NULL)` abort at `timeoutMs` rather than materialising tens
        // of millions of rows uninterruptibly. The deadline read is dwarfed by the
        // per-node `outgoing` block fetch, so an unconditional check is cheap enough
        // to guarantee prompt cancellation.
        while let Some((node, lvl)) = queue.pop_front() {
            self.check_deadline()?;
            if !unlimited && lvl >= max_level {
                continue;
            }
            for a in self.outgoing(node)? {
                if let Some(rt) = reltype {
                    if a.reltype != rt {
                        continue;
                    }
                }
                let nb = a.neighbour.0;
                if visited.insert(nb) {
                    // Charge before pushing so growth is bounded at the point of
                    // allocation, not after the whole subgraph is already resident.
                    self.charge(2)?;
                    nodes.push(Val::Node(nb));
                    edges.push(Val::Rel {
                        id: a.edge.0,
                        start: node,
                        end: nb,
                        reltype: a.reltype,
                    });
                    queue.push_back((nb, lvl + 1));
                }
            }
        }
        if nodes.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![vec![Val::List(nodes), Val::List(edges)]])
    }

    /// `algo.WCC([config])` — weakly-connected components; one row `[node,
    /// componentId]` per selected node. `componentId` is the smallest dense node id
    /// in the component (a stable canonical representative).
    fn algo_components(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        let (labels, rels, _) = self.parse_algo_config("WCC", args, &[])?;
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let roots = algo::wcc(view.nodes.len(), &view.undirected_edges(), &|| {
            self.check_deadline()
        })?;
        let group_id = canonical_group_ids(&view.nodes, &roots);
        Ok(view
            .nodes
            .iter()
            .enumerate()
            .map(|(i, &id)| vec![Val::Node(id), Val::Int(group_id[i])])
            .collect())
    }

    /// `algo.pageRank(label, relationshipType)` — PageRank over the (optionally
    /// label/reltype filtered) subgraph; one row `[node, score]` per selected node.
    /// The two arguments are scalar `string|null` (not a config map).
    fn algo_pagerank(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        if args.len() != 2 {
            bail!("algo.pageRank expects 2 arguments (label, relationshipType)");
        }
        let labels = self.scalar_label_filter("pageRank", &args[0])?;
        let rels = self.scalar_reltype_filter("pageRank", &args[1])?;
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let scores = algo::pagerank(view.nodes.len(), &view.out, &|| self.check_deadline())?;
        Ok(view
            .nodes
            .iter()
            .zip(scores)
            .map(|(&id, s)| vec![Val::Node(id), Val::Float(s)])
            .collect())
    }

    /// `algo.HarmonicCentrality([config])` — harmonic closeness; one row `[node,
    /// score, reachable]` per selected node.
    fn algo_harmonic(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        let (labels, rels, _) = self.parse_algo_config("HarmonicCentrality", args, &[])?;
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let hc = algo::harmonic(view.nodes.len(), &view.out, &|| self.check_deadline())?;
        Ok(view
            .nodes
            .iter()
            .zip(hc)
            .map(|(&id, (score, reach))| {
                vec![Val::Node(id), Val::Float(score), Val::Int(reach as i64)]
            })
            .collect())
    }

    /// `algo.betweenness([config])` — Brandes betweenness; one row `[node, score]`
    /// per selected node. `samplingSize`/`samplingSeed` are validated but ignored
    /// (the full exact betweenness is computed; see [`algo::betweenness`]).
    fn algo_betweenness(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        let (labels, rels, map) =
            self.parse_algo_config("betweenness", args, &["samplingSize", "samplingSeed"])?;
        if let Some(v) = map_get_ci(&map, "samplingSize") {
            if !matches!(v, Val::Int(n) if *n > 0) {
                bail!("betweenness configuration, 'samplingSize' should be a positive integer");
            }
        }
        if let Some(v) = map_get_ci(&map, "samplingSeed") {
            if !matches!(v, Val::Int(_)) {
                bail!("betweenness configuration, 'samplingSeed' should be an integer");
            }
        }
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let cb = algo::betweenness(view.nodes.len(), &view.out, &|| self.check_deadline())?;
        Ok(view
            .nodes
            .iter()
            .zip(cb)
            .map(|(&id, s)| vec![Val::Node(id), Val::Float(s)])
            .collect())
    }

    /// `algo.labelPropagation([config])` — CDLP community detection; one row `[node,
    /// communityId]` per selected node. `communityId` is the smallest dense node id
    /// in the community. `maxIterations` (default 10) caps the propagation rounds.
    fn algo_labelprop(&self, args: &[Val]) -> Result<Vec<Vec<Val>>> {
        let (labels, rels, map) =
            self.parse_algo_config("labelPropagation", args, &["maxIterations"])?;
        let mut max_iter = 10usize;
        if let Some(v) = map_get_ci(&map, "maxIterations") {
            match v {
                Val::Int(n) if *n > 0 => max_iter = *n as usize,
                _ => bail!(
                    "labelPropagation configuration, 'maxIterations' should be a positive integer"
                ),
            }
        }
        let view = self.build_view(labels.as_deref(), rels.as_deref())?;
        let comm = algo::cdlp(view.nodes.len(), &view.undirected_adj(), max_iter, &|| {
            self.check_deadline()
        })?;
        let group_id = canonical_group_ids(&view.nodes, &comm);
        Ok(view
            .nodes
            .iter()
            .enumerate()
            .map(|(i, &id)| vec![Val::Node(id), Val::Int(group_id[i])])
            .collect())
    }

    /// Parse the shared `algo.*` config-map argument (WCC / centrality / community
    /// procs). `args` holds 0 or 1 evaluated arguments; 0 args or a NULL argument is
    /// an empty config. `extra` lists the proc-specific keys permitted beyond
    /// `nodeLabels`/`relationshipTypes`. Returns the resolved label / reltype id
    /// filters (`None` = "all") plus the raw map for proc-specific keys. Unknown
    /// labels / reltypes are ignored (mirrors FalkorDB); unknown *keys* error.
    fn parse_algo_config(&self, proc: &str, args: &[Val], extra: &[&str]) -> Result<AlgoConfig> {
        let map: Vec<(String, Val)> = match args {
            [] | [Val::Null] => Vec::new(),
            [Val::Map(m)] => m.clone(),
            [_] => bail!("invalid {proc} configuration"),
            _ => bail!("{proc} takes at most one configuration argument"),
        };
        for (k, _) in &map {
            let known = k.eq_ignore_ascii_case("nodeLabels")
                || k.eq_ignore_ascii_case("relationshipTypes")
                || extra.iter().any(|e| e.eq_ignore_ascii_case(k));
            if !known {
                bail!("{proc} configuration contains unknown key '{k}'");
            }
        }
        let labels = match map_get_ci(&map, "nodeLabels") {
            None => None,
            Some(v) => Some(self.resolve_name_filter(proc, "nodeLabels", v, true)?),
        };
        let rels = match map_get_ci(&map, "relationshipTypes") {
            None => None,
            Some(v) => Some(self.resolve_name_filter(proc, "relationshipTypes", v, false)?),
        };
        Ok((labels, rels, map))
    }

    /// Resolve a config `nodeLabels` / `relationshipTypes` value (must be an array of
    /// strings) to dense label / reltype ids, silently dropping names that don't
    /// exist in the schema.
    fn resolve_name_filter(
        &self,
        proc: &str,
        key: &str,
        v: &Val,
        is_label: bool,
    ) -> Result<Vec<u32>> {
        let items = match v {
            Val::List(xs) => xs,
            _ => bail!("{proc} configuration, '{key}' should be an array of strings"),
        };
        let mut ids = Vec::new();
        for it in items {
            let name = match it {
                Val::Str(s) => s,
                _ => bail!("{proc} configuration, '{key}' should be an array of strings"),
            };
            let id = if is_label {
                self.gen.label_id(name)
            } else {
                self.gen.reltype_id(name)
            };
            if let Some(id) = id {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    /// Resolve a scalar `string|null` label argument (algo.pageRank's first arg) to a
    /// single-label filter; `null` → `None` (all nodes). An unknown label yields an
    /// empty selection.
    fn scalar_label_filter(&self, proc: &str, v: &Val) -> Result<Option<Vec<u32>>> {
        match v {
            Val::Null => Ok(None),
            Val::Str(s) => Ok(Some(self.gen.label_id(s).into_iter().collect())),
            other => bail!(
                "algo.{proc} label must be a string or null, got {}",
                other.to_display()
            ),
        }
    }

    /// Resolve a scalar `string|null` relationship-type argument (algo.pageRank's
    /// second arg) to a single-reltype filter; `null` → `None` (all edges).
    fn scalar_reltype_filter(&self, proc: &str, v: &Val) -> Result<Option<Vec<u32>>> {
        match v {
            Val::Null => Ok(None),
            Val::Str(s) => Ok(Some(self.gen.reltype_id(s).into_iter().collect())),
            other => bail!(
                "algo.{proc} relationshipType must be a string or null, got {}",
                other.to_display()
            ),
        }
    }

    /// Materialise the filtered subgraph an `algo.*` procedure runs over: the
    /// selected dense node ids (ascending) plus directed out-adjacency as 0-based
    /// indices into that node list. `labels = None` selects every node; otherwise the
    /// union of nodes carrying any listed label. `rels = None` keeps every edge;
    /// otherwise only edges of a listed type. An edge is kept only when both
    /// endpoints are in the selected node set.
    fn build_view(&self, labels: Option<&[u32]>, rels: Option<&[u32]>) -> Result<GraphView> {
        // Route the node selection through `scan_candidates` so the view is built over the
        // *effective* estate: segment-born and delta-born nodes carrying a selected label are
        // included, tombstoned (segment or delta) ids are dropped, and a segment override that
        // changed a node's labels re-decides its membership. (This also folds the write-delta,
        // which the pre-segmented-core `build_view` ignored.)
        let nodes: Vec<u64> = match labels {
            None => self.scan_candidates(&NodeScan::AllNodes)?,
            Some(lbls) => {
                let mut set = std::collections::BTreeSet::new();
                for &l in lbls {
                    for nid in self.scan_candidates(&NodeScan::LabelScan { label_id: l })? {
                        set.insert(nid);
                    }
                }
                set.into_iter().collect()
            }
        };
        // The view (`nodes`, `pos`, and the `out` adjacency) is retained for the whole
        // algorithm run, so it is charged against `maxIntermediate` — an unfiltered
        // `algo.*` over a 91.6M-node store would otherwise OOM building the view before
        // any algorithm ran, ignoring the memory budget entirely. Charge the node-sized
        // structures first (`nodes` + `pos` + the `out` outer vec), so a huge selection
        // trips the budget before a single adjacency block is read.
        self.charge(nodes.len() as u64)?;
        let pos: HashMap<u64, usize> = nodes.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        // Each selected node's out-adjacency read is independent and touches only the
        // Sync cache, so gather the reads on the shared fanout pool (Task 11).
        // `neighbours_par` keeps the stored edge order and applies the same rel-type
        // filter, so mapping each neighbour through `pos` (single-threaded, `pos` is
        // shared read-only) yields the same 0-based index the sequential build did —
        // byte-for-byte identical node list + `out`. The gather is chunked so the
        // retained edge count is charged incrementally and the deadline is observed as
        // the view fills, bounding both memory and time before the algorithm starts;
        // concatenating chunks in order keeps the output identical to a single gather.
        let (gen, cache) = (self.gen, self.cache);
        let mut out: Vec<Vec<usize>> = Vec::with_capacity(nodes.len());
        for chunk in nodes.chunks(BUILD_VIEW_CHUNK) {
            self.check_deadline()?;
            let adj: Vec<Vec<u64>> = par_gather(
                self.fanout_pool.as_deref(),
                chunk,
                BUILD_VIEW_PAR_MIN,
                |&id| neighbours_par(gen, cache, id, Direction::Outgoing, rels),
            )?;
            let mut edges = 0u64;
            for nbs in &adj {
                let mapped: Vec<usize> = nbs.iter().filter_map(|nb| pos.get(nb).copied()).collect();
                edges += mapped.len() as u64;
                out.push(mapped);
            }
            // The retained `out` adjacency is walk work materialised for the run.
            self.charge(edges)?;
        }
        Ok(GraphView { nodes, out })
    }

    /// The fixed output columns and rows for a metadata procedure (lowercased name).
    fn procedure_rows(&self, name: &str) -> Result<(Vec<String>, Vec<Vec<Val>>)> {
        match name {
            // Slater enforces no constraints, so this is always empty — but with the
            // FalkorDB `db.constraints` output shape so a YIELD over it still binds.
            "db.constraints" => Ok((
                ["type", "label", "properties", "entitytype", "status"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                Vec::new(),
            )),
            "db.meta.stats" => Ok(self.meta_stats()),
            "dbms.procedures" => Ok(slater_procedures()),
            "dbms.functions" => Ok(slater_functions()),
            other => bail!("unknown procedure '{other}'"),
        }
    }

    /// `CALL db.meta.stats()` — schema/stat counts from the manifest plus the
    /// per-label / per-reltype count maps (ported from FalkorDB `proc_meta_stats.c`).
    /// All counts come from the resident per-label / per-reltype count maps
    /// (`label_node_count`, `reltype_edge_count`) — no graph scan.
    fn meta_stats(&self) -> (Vec<String>, Vec<Vec<Val>>) {
        let m = self.gen.manifest();
        // Live (delta- and stack-aware) counts. `live_label_node_count` / `live_node_count`
        // always sum; the edge/reltype live counts decline (→ base) when a segment's
        // marginals are not exact. All equal the base marginals for a singleton + empty
        // delta, so a pure-core `db.meta.stats()` is unchanged.
        let labels: Vec<(String, Val)> = m
            .labels
            .iter()
            .map(|l| {
                let cnt = self
                    .gen
                    .label_id(l)
                    .map(|id| self.gen.live_label_node_count(id).unwrap_or(0))
                    .unwrap_or(0);
                (l.clone(), Val::Int(cnt as i64))
            })
            .collect();
        let live_rt: Option<HashMap<String, u64>> = self
            .gen
            .live_reltype_edge_groups()
            .ok()
            .flatten()
            .map(|g| g.into_iter().collect());
        let reltypes: Vec<(String, Val)> = m
            .reltypes
            .iter()
            .map(|t| {
                let cnt = match &live_rt {
                    Some(map) => map.get(t).copied().unwrap_or(0),
                    None => self
                        .gen
                        .reltype_id(t)
                        .map(|id| self.gen.reltype_edge_count(id))
                        .unwrap_or(0),
                };
                (t.clone(), Val::Int(cnt as i64))
            })
            .collect();
        let node_count = self.gen.live_node_count();
        let edge_count = self
            .gen
            .live_edge_count()
            .ok()
            .flatten()
            .unwrap_or(m.edge_count);
        let cols = [
            "labels",
            "relTypes",
            "relCount",
            "nodeCount",
            "labelCount",
            "relTypeCount",
            "propertyKeyCount",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let row = vec![
            Val::Map(labels),
            Val::Map(reltypes),
            Val::Int(edge_count as i64),
            Val::Int(node_count as i64),
            Val::Int(m.labels.len() as i64),
            Val::Int(m.reltypes.len() as i64),
            Val::Int(m.property_keys.len() as i64),
        ];
        (cols, vec![row])
    }

    // ── CALL { … } subquery (Phase 12) ───────────────────────────────────────

    /// Run a correlated `CALL { … }` subquery: the inner query is executed once
    /// per outer row with its imported variables seeded, and the results are
    /// concatenated back. A returning subquery multiplies the outer cardinality by
    /// its result rows (each output row is `outer_row ++ inner_row`); a unit
    /// (`RETURN`-less) subquery passes the outer rows through unchanged (in a
    /// read-only engine it has no observable effect). Mirrors FalkorDB's
    /// `op_apply` + `op_argument`.
    fn apply_call_subquery(&self, table: Table, cs: &CallSubqueryClause) -> Result<Table> {
        // Unit subquery: run the inner clauses per row to surface any errors, then
        // emit the outer row unchanged (cardinality preserved).
        if !cs.returning {
            for row in &table.rows {
                self.check_deadline()?;
                self.run_subquery_for_row(cs, &table.cols, row)?;
            }
            return Ok(table);
        }

        let mut out_cols: Option<Vec<String>> = None;
        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let inner = self.run_subquery_for_row(cs, &table.cols, row)?;
            if out_cols.is_none() {
                out_cols = Some(self.subquery_out_cols(&table.cols, &inner.columns)?);
            }
            // Charge the cross-row buildup: one output row per (outer × inner) pair.
            self.charge(inner.rows.len() as u64)?;
            for irow in inner.rows {
                let mut r = row.clone();
                r.extend(irow);
                out_rows.push(r);
            }
        }
        // With no outer rows the inner never ran; derive the output schema from the
        // inner RETURN's projection names so the result still has correct columns.
        let cols = match out_cols {
            Some(c) => c,
            None => {
                let inner_cols: Vec<String> = cs
                    .inner
                    .head
                    .ret
                    .body
                    .items
                    .iter()
                    .map(|it| it.alias.clone().unwrap_or_else(|| expr_name(&it.expr)))
                    .collect();
                self.subquery_out_cols(&table.cols, &inner_cols)?
            }
        };
        Ok(Table {
            cols,
            rows: out_rows,
        })
    }

    /// Combine the outer columns with the subquery's returned columns, rejecting a
    /// returned name that is already bound in the outer scope (FalkorDB
    /// "Variable `x` already declared in outer scope").
    fn subquery_out_cols(&self, outer: &[String], inner: &[String]) -> Result<Vec<String>> {
        let mut cols = outer.to_vec();
        for ic in inner {
            if outer.iter().any(|c| c == ic) {
                bail!("Variable `{ic}` already declared in outer scope");
            }
            cols.push(ic.clone());
        }
        Ok(cols)
    }

    /// Execute the inner subquery (all `UNION` branches) for one outer row, each
    /// branch seeded with the variables it imports.
    fn run_subquery_for_row(
        &self,
        cs: &CallSubqueryClause,
        outer_cols: &[String],
        outer_row: &[Val],
    ) -> Result<QueryResult> {
        let head_seed = self.subquery_seed(&cs.imports[0], outer_cols, outer_row)?;
        let mut result = self.run_single_seeded(&cs.inner.head, head_seed)?;
        for (i, (union_all, part)) in cs.inner.tail.iter().enumerate() {
            let seed = self.subquery_seed(&cs.imports[i + 1], outer_cols, outer_row)?;
            let next = self.run_single_seeded(part, seed)?;
            if next.columns.len() != result.columns.len() {
                bail!("all branches of a CALL {{}} UNION must return the same number of columns");
            }
            self.charge(next.rows.len() as u64)?; // CALL{} UNION cross-branch buildup
            result.rows.extend(next.rows);
            if !*union_all {
                self.charge(result.rows.len() as u64)?; // DISTINCT `seen` set
                dedup_rows(&mut result.rows);
            }
        }
        Ok(result)
    }

    /// Build the one-row seed table that imports the requested outer variables into
    /// a subquery branch. `Imports::None` seeds the empty singleton (the subquery
    /// sees no outer variables); a named import that is not bound outside errors.
    fn subquery_seed(
        &self,
        imp: &Imports,
        outer_cols: &[String],
        outer_row: &[Val],
    ) -> Result<Table> {
        match imp {
            Imports::None => Ok(Table::singleton()),
            Imports::All => Ok(Table {
                cols: outer_cols.to_vec(),
                rows: vec![outer_row.to_vec()],
            }),
            Imports::Named(names) => {
                let mut row = Vec::with_capacity(names.len());
                for n in names {
                    let idx = outer_cols
                        .iter()
                        .position(|c| c == n)
                        .ok_or_else(|| anyhow::anyhow!("variable '{n}' is not in scope"))?;
                    row.push(outer_row[idx].clone());
                }
                Ok(Table {
                    cols: names.clone(),
                    rows: vec![row],
                })
            }
        }
    }

    // ── CALL db.idx.vector.queryNodes (brute-force KNN) ──────────────────────

    /// Expand each input row with the `k` nearest neighbours from the named vector
    /// index, binding the `YIELD` outputs (`node`, `score`). The candidate set is
    /// the index group read through the block cache; scoring/selection is the pure
    /// [`vector::brute_force_knn`] over it (D26 — `score` is the distance, ascending).
    fn apply_vector_call(&self, table: Table, vc: &VectorCallClause) -> Result<Table> {
        let (ord, desc) = self
            .gen
            .manifest()
            .vector_indexes
            .iter()
            .enumerate()
            .find(|(_, d)| d.label == vc.label && d.property == vc.property)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no vector index on (:{} {{{}}}) — db.idx.vector.queryNodes needs one",
                    vc.label,
                    vc.property
                )
            })?;
        // Capture the small descriptor bits so the per-row loop does not hold the
        // manifest borrow (it also calls `self` methods to read candidates). `ord`
        // (the index's position) keys its resident matrix in the vector-index pool.
        let ord = ord as u32;
        let metric = desc.metric;
        let dim = desc.dim as usize;
        let first_record = desc.first_record;
        let count = desc.count;
        let mode = desc.mode.clone();
        let desc = desc.clone();

        // The levels above the base carry vectors the sealed base index cannot: a node
        // embedded since the build has no entry in it at all, and a node *re*-embedded
        // since the build has a stale one.
        //
        // Kept as *separate levels* rather than one flattened overlay, because each level has
        // a different set of levels above it and therefore a different suppression set (see
        // `VectorLevels`).
        //
        // The **segments** level is served *per segment* (HIK-113): a segment whose live
        // embedded set crossed the floor at flush/merge carries its own sealed Vamana index and
        // is beam-searched; a smaller (or pre-feature) segment is brute-forced over its own
        // sidecar ids. Each segment suppresses everything *newer* (the delta ∪ every newer
        // segment) in its own scan — `superseded_above` — folded once per query in
        // [`Self::segments_knn`] inside the per-row loop below.
        //
        // The base's suppression set is the sidecar union over every segment: the base sits below
        // all of them, and a base entry any segment supersedes must lose in the base's own scan.
        // A re-embed (`ids`) and a **value** removal always suppress. A **label** removal is
        // suppressed *conditionally* (HIK-118): it means the node left the index's scope, but a
        // later `SET n:Doc` (in the delta or a newer segment) puts it back — and then the base's
        // vector is the live one and must score again. So a `label_removal` id suppresses the base
        // only while the node is **not** currently in scope. `vector_indexed` resolves the
        // effective label set (the same resolve the fold uses), built once per query here — not
        // per row — and reuses the label reads already on the hot path.
        let mut above_base_segments: HashSet<u64> = HashSet::new();
        if !self.gen.core_stack().is_singleton() {
            for seg in self.gen.core_stack().segments() {
                if let Some(v) = &seg.vectors {
                    above_base_segments.extend(v.ids(&desc.label, &desc.property).iter().copied());
                    above_base_segments.extend(
                        v.value_removals(&desc.label, &desc.property)
                            .iter()
                            .copied(),
                    );
                    for &id in v.label_removals(&desc.label, &desc.property) {
                        if !vector_indexed(self.gen, self.cache, id, &desc)? {
                            above_base_segments.insert(id);
                        }
                    }
                }
            }
        }

        // The **delta** level is served by the FreshDiskANN RW-index (`crate::rwindex`) — an
        // in-memory Vamana over the fresh set, advanced to this query's delta epoch and read
        // at exactly that cut. It replaces what used to be an O(delta) `ResidentMatrix`
        // allocate-and-normalise on the hot path of *every single query* — ~300 MB at 768 dim
        // over a 10⁵-vector overlay.
        //
        // Caching a matrix per level instead was never an option: the vector pool charges
        // `matrix_bytes` to its budget and never evicts it, so a per-level matrix would grow
        // the pinned set without bound as segments accumulate (the Σ-over-levels pinning trap,
        // D63). The RW-index is not in that pool at all — it is derived state bounded by the
        // delta, with its own `maxVectors` valve.
        //
        // `RwLookup::BruteForce` (kill switch off, delta below `minVectors` or above
        // `maxVectors`, or an index another query has already advanced *past* our epoch) falls
        // back to exactly the gather-and-scan that shipped before: the same answer, a
        // different cost.
        let rw_ix = self.rw_delta_index(&desc)?;
        let rw = rw_ix
            .as_ref()
            .and_then(|(ix, epoch)| rwindex::read_at_epoch(ix, *epoch));
        // Only when the index is not serving do we pay for the delta walk.
        let brute_delta = match &rw {
            Some(_) => None,
            None => Some(self.delta_level(&desc)?),
        };

        // Everything *newer* than the segments — the delta's suppression set, from whichever
        // arm is serving it. The RW-index maintains it incrementally, so the fast path never
        // re-walks the delta to compute it.
        let owned_above_segments = brute_delta.as_ref().map(|l| l.superseded());
        let above_segments: &HashSet<u64> = match (&owned_above_segments, &rw) {
            (Some(s), _) => s,
            (None, Some(ix)) => ix.superseded(),
            (None, None) => unreachable!("one of the two delta arms is always present"),
        };
        let matrix_of = |entries: Vec<VectorEntry>| -> Result<Option<vector::ResidentMatrix>> {
            if entries.is_empty() {
                return Ok(None);
            }
            Ok(Some(vector::ResidentMatrix::from_entries(
                dim, metric, entries,
            )?))
        };
        let delta_matrix = match brute_delta {
            Some(l) => matrix_of(l.entries)?,
            None => None,
        };

        // A vector index is built over the **base** generation and is immutable, so a
        // node deleted since the build is still in it. The delta/stack tombstones are
        // the only place that delete can take effect on this path — without this the
        // KNN arms would hand back a deleted node as a live `Val::Node` (every other
        // read path already suppresses them; see `suppress_tombstoned_in_place`).
        //
        // A node a *newer* level re-embedded is suppressed for the same reason: this level's
        // vector for it is stale. It must lose in this level's **scan**, not in the merge
        // afterwards — `merge_topk` cannot drop it late without risking the k-th slot (see
        // there). Which levels are "newer" is what differs per arm, and it is the whole point
        // of the split: the base is below both levels, the segments are below only the delta,
        // and nothing at all is above the delta.
        let delta = self.gen.delta();
        let stack = self.gen.core_stack();
        let tombstoned = |id: u64| -> Result<bool> {
            Ok(delta.is_tombstoned(id)
                || (!stack.is_singleton() && stack.is_node_tombstoned(id)?))
        };
        // `above_base` = everything either level above the base supersedes. Tested as two set
        // probes rather than a materialised union, so the delta's half can stay borrowed from
        // the RW-index instead of being cloned per query.
        let live_fn = |id: u64| -> Result<bool> {
            Ok(!above_base_segments.contains(&id)
                && !above_segments.contains(&id)
                && !tombstoned(id)?)
        };
        // Nothing sits above the delta, so its only suppression is the tombstone. Both delta
        // arms already drop tombstoned nodes as they are built (`delta_vector_for` resolves
        // them to `Silent`), so this is defence in depth — but the delta *is* a scanned level
        // like any other, and the RW-index needs a `live` gate anyway to keep a suppressed node
        // a navigable waypoint rather than pruning it from the walk.
        let delta_live_fn = |id: u64| -> Result<bool> { Ok(!tombstoned(id)?) };
        // A pure-core generation with an empty delta can have no tombstones and no overlay,
        // so the read-only estate pays nothing at all for this.
        let live: Option<vector::LivePredicate> = if delta.is_empty() && stack.is_singleton() {
            None
        } else {
            Some(&live_fn)
        };

        // The bound YIELD names introduced into scope, in YIELD order.
        let mut new_vars: Vec<String> = Vec::new();
        for (_, bound) in &vc.yields {
            if !table.cols.contains(bound) && !new_vars.contains(bound) {
                new_vars.push(bound.clone());
            }
        }
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        // The brute-force arm prefers the resident, pre-decoded matrix (decode +
        // normalize once per generation, then scan resident memory — no per-query
        // gather/allocation). It falls back to the up-front gather when no vector
        // pool is wired or the matrix would not fit the vector-index budget. The
        // Vamana arm navigates per query and reads nothing here.
        let matrix = match (&mode, self.vec_cache) {
            (AnnMode::BruteForce, Some(pool)) => {
                let expected = count as usize * dim * std::mem::size_of::<f32>()
                    + count as usize * std::mem::size_of::<u64>();
                pool.matrix_or(self.gen.uuid(), ord, expected, || {
                    vector::ResidentMatrix::from_entries(
                        dim,
                        metric,
                        self.vector_group(first_record, count)?,
                    )
                })?
            }
            _ => None,
        };
        let entries = match (&mode, &matrix) {
            (AnnMode::BruteForce, None) => Some(self.vector_group(first_record, count)?),
            _ => None,
        };

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let scope = Scope::Row(&table.cols, row);
            let k = match self.eval(&vc.k, &scope, None)? {
                Val::Int(n) if n >= 0 => n as usize,
                other => bail!(
                    "db.idx.vector.queryNodes k must be a non-negative integer, got {}",
                    other.to_display()
                ),
            };
            let query = self.eval_query_vector(&vc.query_vec, &scope)?;
            if query.len() != dim {
                bail!(
                    "query vector has dimension {} but the (:{} {{{}}}) index is {}-dimensional",
                    query.len(),
                    vc.label,
                    vc.property,
                    dim
                );
            }
            // Both arms produce the same `score` (the metric distance, ascending) —
            // brute force scans every candidate exactly; Vamana navigates by PQ in
            // resident memory and re-ranks the beam exactly (D32).
            let neighbours = match &mode {
                AnnMode::BruteForce => match &matrix {
                    Some(m) => vector::brute_force_knn_matrix_par(
                        self.fanout_pool.as_deref(),
                        m,
                        &query,
                        k,
                        KNN_PAR_MIN,
                        live,
                    )?,
                    None => vector::brute_force_knn_par(
                        self.fanout_pool.as_deref(),
                        entries.as_ref().unwrap(),
                        &query,
                        k,
                        metric,
                        KNN_PAR_MIN,
                        live,
                    )?,
                },
                AnnMode::Vamana { medoid, nav, .. } => {
                    self.vamana_knn(vc, *medoid, *nav, metric, &query, k, live)?
                }
            };
            // Fold the levels above the base in. Each level has already suppressed — in its own
            // scan/walk — every node a *newer* level supersedes, so the merge is a straight
            // scored fold. See `vector::merge_topk` for why it must not dedup here instead.
            let scan = |m: &Option<vector::ResidentMatrix>,
                        live: vector::LivePredicate|
             -> Result<Vec<vector::Neighbour>> {
                match m {
                    None => Ok(Vec::new()),
                    Some(m) => vector::brute_force_knn_matrix_par(
                        self.fanout_pool.as_deref(),
                        m,
                        &query,
                        k,
                        KNN_PAR_MIN,
                        Some(live),
                    ),
                }
            };
            // The delta arm: the RW-index's beam walk, or the gathered brute force. The
            // **exact** scorer is the same `vector::distance` every other arm re-ranks with
            // (D32/D29), so all three levels' scores are on one scale and `merge_topk`
            // interleaves them correctly rather than silently.
            let fresh = match &rw {
                Some(ix) => ix
                    .graph()
                    .search(
                        &query,
                        k,
                        self.beam_width,
                        |v| vector::distance(metric, &query, v) as f32,
                        delta_live_fn,
                    )?
                    .into_iter()
                    .map(|h| vector::Neighbour {
                        node_id: h.node_id,
                        score: h.exact as f64,
                    })
                    .collect(),
                None => scan(&delta_matrix, &delta_live_fn)?,
            };
            // The **segments** level: one beam per sealed segment, brute force per unsealed one,
            // each suppressed by everything newer than it (`superseded_above`).
            let segs = self.segments_knn(&desc, &query, k, above_segments, &tombstoned)?;
            let neighbours = if segs.is_empty() && fresh.is_empty() {
                neighbours
            } else {
                vector::merge_topk([neighbours, segs, fresh], k)
            };
            // A node id can reach the merged top-k from at most one level: the level that
            // holds its *effective* vector. A duplicate is therefore always a suppression
            // bug — and a silent one, because the stale copy has already taken a slot from a
            // live candidate by the time anyone could notice.
            debug_assert_eq!(
                neighbours
                    .iter()
                    .map(|n| n.node_id)
                    .collect::<HashSet<_>>()
                    .len(),
                neighbours.len(),
                "duplicate node id in the merged top-k — a level failed to suppress: {neighbours:?}"
            );
            for nb in neighbours {
                let mut r = row.clone();
                for bound in &new_vars {
                    let output = vc
                        .yields
                        .iter()
                        .find(|(_, b)| b == bound)
                        .map(|(o, _)| o.as_str())
                        .unwrap_or("");
                    r.push(match output {
                        "node" => Val::Node(nb.node_id),
                        "score" => Val::Float(nb.score),
                        _ => Val::Null,
                    });
                }
                // Apply the optional YIELD ... WHERE over the yielded row.
                if let Some(w) = &vc.where_ {
                    let row_scope = Scope::Row(&out_cols, &r);
                    if three_valued(&self.eval(w, &row_scope, None)?) != Some(true) {
                        continue;
                    }
                }
                out_rows.push(r);
            }
        }
        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// The `AnnMode::Vamana` arm: a greedy beam search over the disk-native graph,
    /// navigating by the **resident PQ estimate** (in memory, no IO) and reading
    /// full vectors + adjacency only for the frontier through the vector-index pool
    /// (coalesced by block), re-ranking the beam by the **exact** metric distance so
    /// the returned `score` matches the brute-force contract (D32). The resident set
    /// is PQ codes only — never a full in-memory graph.
    #[allow(clippy::too_many_arguments)]
    fn vamana_knn(
        &self,
        vc: &VectorCallClause,
        medoid: u64,
        nav: AnnNav,
        metric: graph_format::manifest::Metric,
        query: &[f32],
        k: usize,
        live: Option<vector::LivePredicate>,
    ) -> Result<Vec<vector::Neighbour>> {
        let (label, property) = (vc.label.as_str(), vc.property.as_str());
        let pool = self.vec_cache.ok_or_else(|| {
            anyhow::anyhow!("vector-index cache is not configured; cannot serve a Vamana index")
        })?;
        let index = self.gen.vamana_index(label, property).ok_or_else(|| {
            anyhow::anyhow!("Vamana index files for (:{label} {{{property}}}) are not open")
        })?;
        // The base's index is keyed in the pool by the generation uuid; `None`-live means every
        // node is live (a pure-core estate with no overlay).
        self.beam_over_index(
            pool,
            self.gen.uuid(),
            index.ord,
            index.reader.inner(),
            &index.pq,
            medoid,
            nav,
            metric,
            query,
            k,
            self.beam_width,
            |id| match live {
                Some(f) => f(id),
                None => Ok(true),
            },
        )
    }

    /// One beam search over a sealed Vamana/PQ index — the base's or a **segment's** (HIK-113).
    /// The only differences between the two callers are which files back it (`reader`/`resident`/
    /// `medoid`), the pool key (`gen_id` — a generation uuid for the base, a **segment uuid** for
    /// a segment; the two spaces cannot collide), and the beam width. Everything the search
    /// itself does — PQ-estimated navigation, one coalesced block read per expansion, the exact
    /// re-rank under the true metric, the `HOLE` + `live` suppression that keeps a dead node a
    /// navigable waypoint, the D26 node-id tie-break — is identical, so it lives here once.
    #[allow(clippy::too_many_arguments)]
    fn beam_over_index(
        &self,
        pool: &VectorIndexCache,
        gen_id: graph_format::ids::Generation,
        ord: u32,
        reader: &BlockFileReader,
        resident: &ResidentPq,
        medoid: u64,
        nav: AnnNav,
        metric: graph_format::manifest::Metric,
        query: &[f32],
        k: usize,
        beam_width: usize,
        live: impl Fn(u64) -> Result<bool>,
    ) -> Result<Vec<vector::Neighbour>> {
        // The **record** count, holes included — a hole is a legal, navigable neighbour, so
        // this (never the live count) bounds-checks a neighbour ordinal. Using the live count
        // here would reject valid ordinals and silently cut recall.
        let n = resident.len();
        if n == 0 || k == 0 {
            return Ok(Vec::new());
        }
        // The shared (base + sealed-segment) navigator choke: refuse an `InnerProduct` discriminator
        // on a non-Dot index before dispatching. The base index is also checked at generation open
        // (`validate_vamana_index`), but a sealed **segment** carries its own `nav` and has no
        // open-time metric context (`SegmentVamanaSet::open_if_present_via` never sees the metric —
        // it lives in the base descriptor), so a forged `nav: inner_product` on a cosine/L2 segment
        // would otherwise reach `AdcTable::new_ip` here and mis-navigate. Fail closed instead
        // (HIK-137 phase 4).
        nav.check_metric(metric, "vector index navigation")?;
        // PQ navigates in the space the codebook was trained in. HIK-137: an `InnerProduct` index
        // was trained on the RAW vectors and is navigated by the IP-ADC estimate (−⟨q, x̂⟩) with the
        // raw query — NO `ann_query` augmentation. `Augmented` (cosine/L2/legacy-Dot) is unchanged:
        // it maps the query into the L2-reduced ANN space and navigates by squared-L2 ADC.
        let adc = match nav {
            AnnNav::InnerProduct => AdcTable::new_ip(&resident.codebook, query)?,
            AnnNav::Augmented => {
                let qn = graph_format::pq::ann_query(
                    metric,
                    query,
                    resident.codebook.params.dim as usize,
                )?;
                AdcTable::new(&resident.codebook, &qn)?
            }
        };
        let hits = beam_search(
            vamana::BeamParams {
                medoid: medoid as u32,
                beam_width,
                k,
                num_nodes: n,
            },
            |i| adc.estimate(resident.codes_of(i as usize)),
            |i| {
                // One coalesced block read per expansion (cached in the vector pool).
                let rec = pool.record(reader, gen_id, ord, i as u64)?;
                let node = vamana::decode_node(&rec)?;
                Ok((node.vector, node.neighbours))
            },
            |v| vector::distance(metric, query, v) as f32,
            |i| {
                let node_id = resident.node_ids[i as usize];
                if node_id == graph_format::pq::HOLE {
                    return Ok(None);
                }
                if live(node_id)? {
                    Ok(Some(node_id))
                } else {
                    Ok(None)
                }
            },
        )?;
        Ok(hits
            .into_iter()
            .map(|h| vector::Neighbour {
                node_id: h.node_id,
                score: h.exact as f64,
            })
            .collect())
    }

    /// The **segments** level of a KNN read (HIK-113): every core segment, folded
    /// **newest → oldest**, each contributing its live embeddings suppressed by everything above
    /// it — the delta (`above_segments`) plus every *newer* segment. A segment that sealed a
    /// Vamana index is beam-searched; one that did not (below the floor, or pre-feature, or a
    /// deleted/corrupt pair) is brute-forced over its own sidecar ids. `None` sealed index ⇒
    /// brute force is the whole compatibility story.
    ///
    /// The accumulator `acc` is exactly `superseded_above(i)` at segment `i`: it starts at the
    /// delta's suppression set and grows by each visited (newer) segment's `ids ∪ removals`.
    /// Because every older segment suppresses an id a newer one touched, a node reaches the
    /// merged top-k from at most one segment (the newest that still holds it live) — which is
    /// what keeps `merge_topk`'s no-dedup fold correct.
    fn segments_knn(
        &self,
        desc: &VectorIndexDesc,
        query: &[f32],
        k: usize,
        above_segments: &HashSet<u64>,
        tombstoned: &impl Fn(u64) -> Result<bool>,
    ) -> Result<Vec<vector::Neighbour>> {
        let stack = self.gen.core_stack();
        if stack.is_singleton() {
            return Ok(Vec::new());
        }
        let (label, property) = (desc.label.as_str(), desc.property.as_str());
        let segs = stack.segments();
        let mut acc: HashSet<u64> = above_segments.clone();
        let mut out: Vec<vector::Neighbour> = Vec::new();
        // Newest → oldest: `segs` is oldest→newest, so iterate in reverse.
        for seg in segs.iter().rev() {
            let Some(sidecar) = &seg.vectors else {
                continue;
            };
            let ids = sidecar.ids(label, property);
            let label_removals = sidecar.label_removals(label, property);
            let value_removals = sidecar.value_removals(label, property);
            if ids.is_empty() && label_removals.is_empty() && value_removals.is_empty() {
                continue;
            }
            // This segment's live gate: suppress everything newer (`acc`), the tombstoned, and
            // any node that no longer effectively carries the index's label. `acc` is read here
            // and only extended *after* the segment is processed, so the two never overlap.
            let live = |id: u64| -> Result<bool> {
                Ok(!acc.contains(&id)
                    && !tombstoned(id)?
                    && vector_indexed(self.gen, self.cache, id, desc)?)
            };
            match seg
                .vector_graph
                .as_ref()
                .and_then(|g| g.get(label, property))
            {
                Some(ix) => out.extend(self.segment_vamana_knn(
                    seg.manifest.segment_uuid,
                    ix,
                    desc.metric,
                    query,
                    k,
                    live,
                )?),
                None => {
                    // Brute force this segment's *own* embeddings (its rows carry the vector).
                    // Apply the live gate while gathering — a brute force has no navigation, so a
                    // suppressed node is simply excluded (no waypoint to preserve), and
                    // pre-filtering keeps the gathered set the exact live set the scan ranks.
                    let mut entries: Vec<VectorEntry> = Vec::new();
                    for &id in ids {
                        if !live(id)? {
                            continue;
                        }
                        if let Some(row) = seg.reader.node_row(id)? {
                            if row.tombstoned {
                                continue;
                            }
                            if let Some((_, Value::Vector(v))) =
                                row.props.iter().find(|(k, _)| k == &desc.property)
                            {
                                entries.push(VectorEntry {
                                    node_id: id,
                                    vector: v.clone(),
                                });
                            }
                        }
                    }
                    if !entries.is_empty() {
                        out.extend(vector::brute_force_knn_par(
                            self.fanout_pool.as_deref(),
                            &entries,
                            query,
                            k,
                            desc.metric,
                            KNN_PAR_MIN,
                            None,
                        )?);
                    }
                }
            }
            // `acc` is only read through `live` above (a shared borrow NLL ends at its last use),
            // so it is free to grow here: fold this segment's touched ids into the suppression
            // set for every older segment. A re-embed (`ids`) and a **value** removal always
            // suppress an older level's entry. A **label** removal is conditional (HIK-118): if
            // the node is back in scope, an *older segment* may still hold its live vector (the
            // re-label did not move it), and that vector must surface — so a re-labelled id does
            // not enter `acc`. When it is still out of scope, the `live` gate's `vector_indexed`
            // check would exclude it anyway; adding it to `acc` keeps the suppression explicit and
            // costs nothing.
            acc.extend(ids.iter().copied());
            acc.extend(value_removals.iter().copied());
            for &id in label_removals {
                if !vector_indexed(self.gen, self.cache, id, desc)? {
                    acc.insert(id);
                }
            }
        }
        Ok(out)
    }

    /// A beam search over one segment's sealed Vamana index, keyed in the vector-index pool by
    /// the **segment uuid** (in the `gen` slot) + the segment-local ordinal. See
    /// [`Self::beam_over_index`].
    fn segment_vamana_knn(
        &self,
        seg_uuid: graph_format::ids::Generation,
        ix: &SegmentVamanaIndex,
        metric: graph_format::manifest::Metric,
        query: &[f32],
        k: usize,
        live: impl Fn(u64) -> Result<bool>,
    ) -> Result<Vec<vector::Neighbour>> {
        let pool = self.vec_cache.ok_or_else(|| {
            anyhow::anyhow!("vector-index cache is not configured; cannot serve a segment index")
        })?;
        self.beam_over_index(
            pool,
            seg_uuid,
            ix.ord,
            ix.reader.inner(),
            &ix.pq,
            ix.medoid,
            // HIK-137 phase 3: a Dot segment seals IP-native and carries `nav: InnerProduct` on its
            // `SealedVamanaMeta`; dispatch on it so the segment beam navigates by the IP-ADC estimate,
            // exactly as the base does. A cosine/L2 (or legacy) segment is `Augmented`.
            ix.nav,
            metric,
            query,
            k,
            self.temp_beam_width(),
            live,
        )
    }

    /// Evaluate an expression that must produce a query vector: a `vecf32([...])`
    /// literal, a stored `Vector`, or a list of numbers (a `$param` arrives as a
    /// list). Anything else is a type error.
    fn eval_query_vector(&self, e: &Expr, scope: &Scope) -> Result<Vec<f32>> {
        match self.eval(e, scope, None)? {
            Val::Vector(v) => Ok(v),
            Val::List(xs) => xs
                .iter()
                .enumerate()
                .map(|(i, x)| embed_component(i, x, "query vector"))
                .collect(),
            other => bail!(
                "query vector must be a vecf32([...]) literal or numeric list, got {}",
                other.to_display()
            ),
        }
    }

    /// Match `patterns[idx..]` against `binding`, applying the clause `WHERE` once
    /// every pattern is bound, collecting completed bindings.
    fn match_patterns(
        &self,
        patterns: &[Pattern],
        idx: usize,
        binding: HashMap<String, Val>,
        where_: Option<&Expr>,
        out: &mut Vec<HashMap<String, Val>>,
        cap: Option<usize>,
    ) -> Result<()> {
        if cap.is_some_and(|c| out.len() >= c) {
            return Ok(());
        }
        if idx == patterns.len() {
            if let Some(w) = where_ {
                if !truthy(&self.eval(w, &Scope::Map(&binding), None)?) {
                    return Ok(());
                }
            }
            // Charging each emitted binding bounds dense-graph materialisation
            // (plain MATCH and pattern comprehensions alike) by the query budget.
            self.charge(1)?;
            out.push(binding);
            return Ok(());
        }
        // The pushed cap (Stage 6) bounds this pattern's own expansion only when it
        // is the LAST pattern AND there is no residual WHERE — then each emitted
        // binding becomes exactly one output row (1:1), so the expansion needs at
        // most `cap - out.len()` rows. Otherwise downstream patterns/WHERE may drop
        // or multiply rows, so the per-pattern walk stays uncapped (only the `out`
        // accumulation below stops early).
        let sp_cap = if idx + 1 == patterns.len() && where_.is_none() {
            cap.map(|c| c.saturating_sub(out.len()))
        } else {
            None
        };
        let mut partial = Vec::new();
        self.match_single_pattern(&patterns[idx], &binding, where_, &mut partial, sp_cap)?;
        for b in partial {
            if cap.is_some_and(|c| out.len() >= c) {
                break;
            }
            self.match_patterns(patterns, idx + 1, b, where_, out, cap)?;
        }
        Ok(())
    }

    fn match_single_pattern(
        &self,
        pattern: &Pattern,
        binding: &HashMap<String, Val>,
        where_: Option<&Expr>,
        out: &mut Vec<HashMap<String, Val>>,
        cap: Option<usize>,
    ) -> Result<()> {
        // If the start anchor would be a full scan but the pattern's *end* node is
        // id-seekable, match the reversed pattern so the seekable node leads. This
        // is what turns Memgraph Lab's `MATCH (m)-[r]->(n) WHERE id(n) = X`
        // neighbourhood-expansion (id pinned on the far end) from a full edge scan
        // into a seek + one-hop walk. Reversal preserves the binding set exactly
        // (same vars, same edges, flipped traversal direction) and the full WHERE
        // is re-checked downstream, so it cannot change results.
        let rerooted = self.maybe_reroot(pattern, binding, where_);
        let pattern = rerooted.as_ref().unwrap_or(pattern);
        let start = &pattern.start;
        // `guaranteed` are the anchor labels the chosen scan already proves for every
        // candidate, so `node_ok` can skip re-decoding a label record for them
        // (root cause 2). Only the scanned branch yields guarantees; an already-bound
        // anchor is a single node we still verify in full.
        let (mut candidates, guaranteed): (CandidateStream, Vec<u32>) =
            match start.var.as_deref().and_then(|v| binding.get(v)) {
                Some(Val::Node(id)) => (CandidateStream::single(*id), Vec::new()),
                Some(_) => return Ok(()), // bound to a non-node → cannot match
                None => {
                    // The anchor is the only place the planner picks a scan strategy.
                    // Scalars already bound for this row let an anchor keyed by a
                    // bound variable (`{p: w}` / `WHERE n.p = w`) seek the index.
                    let bound = bound_scalars(binding);
                    let scan = choose_node_scan(self.gen, start, where_, &self.plan_params, &bound);
                    // If the (post-reroot) pattern's first hop is a required,
                    // fixed-length typed edge, drive from that reltype's endpoint
                    // posting instead of a label/full scan — skipping the nodes
                    // that have no such edge. Sound for any context (incl.
                    // OPTIONAL): only reached for an unbound anchor, and an
                    // edgeless anchor yields no row under either plan, so the
                    // matched set is identical. See `maybe_rel_type_scan`.
                    let scan = maybe_rel_type_scan(self.gen, &scan, pattern).unwrap_or(scan);
                    let guaranteed = self.scan_guaranteed_labels(&scan);
                    (self.candidate_stream(&scan)?, guaranteed)
                }
            };
        // One mutable frame for the whole anchor loop. Each candidate binds the
        // anchor var in place, expands, then restores it — instead of cloning the
        // inherited scope per candidate, and (in `expand_chain`) per hop per
        // neighbour. `node_ok` still sees the pre-anchor scope (the anchor's own
        // var intentionally absent, as before), since `frame` is restored to the
        // base binding between candidates.
        // The chain shape (rels, props, var-length) is the same for every anchor, so
        // decide once whether each anchor's expansion uses the parallel breadth-first
        // walk (Task 9) or the sequential depth-first one. A pushed `LIMIT` (`cap`)
        // disables it: the breadth-first walk would eagerly read a whole hop level
        // before the cap could stop it, over-reading a high-degree frontier the
        // depth-first early-exit would have skipped — so capped chains stay sequential
        // (the plan's early-exit rule). Uncapped chains (counts, aggregates, DISTINCT,
        // un-LIMITed returns) genuinely need the whole neighbourhood, so the parallel
        // reads are pure overlap with no wasted work.
        let parallel = cap.is_none() && self.chain_parallelizable(pattern);
        // Task 10: when the anchor is a scan wide enough to be worth it and `node_ok`
        // actually reads a per-candidate label/property record, evaluate that filter
        // across the shared fanout pool up front, then expand only the survivors in
        // input order. The inline-prop *values* (`wants`) don't depend on the
        // candidate, so they are evaluated once here (single-threaded — they may route
        // through the !Sync evaluator) and the workers do only Sync label/column reads
        // + `loose_eq`. Gated to uncapped scans: a pushed `LIMIT` would over-read the
        // whole candidate set before the cap could stop the scan (the plan's early-exit
        // rule), so capped scans keep the inline per-candidate filter with its break.
        // The scan is a stream now, so the filter runs a window at a time (each window is
        // ≫ `SCAN_PAR_MIN`, so it still fans out) rather than over one giant `Vec`; the
        // width gate reads the stream's upper bound instead of a materialised length.
        let prefilter = cap.is_none()
            && self.fanout_pool.is_some()
            && candidates.upper_bound() >= SCAN_PAR_MIN
            && self.anchor_filter_reads(start, &guaranteed);
        // Candidate-independent, so evaluated once for the whole scan (single-threaded —
        // it may route through the !Sync evaluator); the pool workers then do only Sync
        // label/column reads + `loose_eq`.
        let wants: Vec<(&str, Val)> = if prefilter {
            start
                .props
                .iter()
                .map(|(k, e)| Ok((k.as_str(), self.eval(e, &Scope::Map(binding), None)?)))
                .collect::<Result<_>>()?
        } else {
            Vec::new()
        };
        // Degree-sum terminal: in count-pushdown mode (armed by `count_match`, no WHERE),
        // if this post-reroot pattern's final hop qualifies, arm the walk to add each
        // penultimate node's effective degree instead of expanding the last relationship.
        // Checked on the *post-reroot* pattern, so a rerooted chain (whose new terminal is
        // the filtered original anchor) declines automatically. Restored after the loop.
        let degree_term = self.count_acc.get().is_some()
            && where_.is_none()
            && self.degree_terminal_dir(pattern).is_some();
        let prev_degree_term = self.degree_terminal.replace(degree_term);
        let mut frame = binding.clone();
        let mut walk = Vec::new();
        // Filter verdicts for the current window (`prefilter` only), reused across windows.
        let mut pass: Vec<bool> = Vec::new();
        'scan: while let Some(batch) = self.next_candidates(&mut candidates)? {
            if prefilter {
                let (gen, cache) = (self.gen, self.cache);
                let label_expr = start.label_expr.as_ref();
                pass = par_gather(self.fanout_pool.as_deref(), batch, SCAN_PAR_MIN, |&c| {
                    node_ok_par(gen, cache, c, label_expr, &wants, &guaranteed)
                })?;
            }
            for (i, &c) in batch.iter().enumerate() {
                // Stage 6: once a pushed `LIMIT` is met, stop scanning anchors — the
                // remaining candidates can only add rows the projection would truncate,
                // and the stream stops producing them.
                if cap.is_some_and(|cc| out.len() >= cc) {
                    break 'scan;
                }
                // Already filtered in parallel above when `prefilter`; otherwise check the
                // anchor's labels/inline props inline (with the loop's early-exit break).
                if prefilter {
                    if !pass[i] {
                        continue;
                    }
                } else if !self.node_ok(c, start, &Scope::Map(&frame), &guaranteed)? {
                    continue;
                }
                let prev = start
                    .var
                    .as_ref()
                    .map(|v| (v.clone(), frame.insert(v.clone(), Val::Node(c))));
                if parallel {
                    self.expand_chain_par(pattern, c, &frame, out, cap)?;
                } else {
                    debug_assert!(walk.is_empty());
                    self.expand_chain(pattern, 0, c, &mut frame, c, &mut walk, out, cap)?;
                }
                if let Some((v, old)) = prev {
                    restore_binding(&mut frame, v, old);
                }
            }
        }
        self.degree_terminal.set(prev_degree_term);
        Ok(())
    }

    /// Decide whether to match `pattern` reversed so a concrete (single-candidate)
    /// **end** node leads instead of a full-scan start. Returns the reversed pattern
    /// when it would help, else `None` (match the original). Reversal preserves the
    /// binding set exactly (same vars, same edges, flipped traversal direction) and
    /// the full WHERE is re-checked downstream, so it cannot change results.
    ///
    /// Common preconditions: the pattern has at least one relationship and **no**
    /// variable-length hop (a reversed `*` walk could reorder a returned
    /// relationship list); it has no path variable (reversal would reverse the
    /// path); and the **start** is a fresh scan (an already-bound start leads with a
    /// concrete node, so reversal could only lose that).
    ///
    /// Two cases re-root:
    /// - **(1) end already bound** by an outer `MATCH`/`WITH` to a concrete node —
    ///   lead with that node and walk its reverse adjacency, instead of full-scanning
    ///   the start label once per bound end row. This is the eu-ai-act §P1
    ///   reverse-traversal case: `… MATCH (c:Chunk)-[:SOURCED_FROM]->(b)` with `b`
    ///   bound went from a seek to an O(|Chunk|)-per-row scan without it.
    /// - **(2) end id-anchored by `WHERE`** (`… WHERE id(end) = X`, the start *not*
    ///   anchored) — seek the end and walk back, turning a full edge scan into a
    ///   seek + one-hop (Memgraph Lab neighbourhood expansion).
    fn maybe_reroot(
        &self,
        pattern: &Pattern,
        binding: &HashMap<String, Val>,
        where_: Option<&Expr>,
    ) -> Option<Pattern> {
        if pattern.path_var.is_some() || pattern.rels.is_empty() {
            return None;
        }
        if pattern.rels.iter().any(|(r, _)| r.var_length.is_some()) {
            return None;
        }
        let unbound = |v: Option<&String>| v.is_none_or(|name| !binding.contains_key(name));
        if !unbound(pattern.start.var.as_ref()) {
            return None;
        }
        let end = &pattern.rels.last().unwrap().1;
        let end_var = end.var.as_deref()?;
        // (1) End node already bound to a concrete node — lead with it.
        if matches!(binding.get(end_var), Some(Val::Node(_))) {
            return Some(reverse_pattern(pattern));
        }
        // (2) End must otherwise be a fresh scan target id-anchored by WHERE.
        if !unbound(end.var.as_ref()) {
            return None;
        }
        let where_ = where_?;
        let start_anchored = pattern
            .start
            .var
            .as_deref()
            .is_some_and(|v| is_id_anchored(where_, v));
        if start_anchored || !is_id_anchored(where_, end_var) {
            return None;
        }
        Some(reverse_pattern(pattern))
    }

    /// Whether `pattern`'s chain qualifies for the parallel breadth-first expansion
    /// ([`Self::expand_chain_par`], Task 9): a fanout pool is configured and the
    /// pattern is a plain (non-quantified) chain of at least one **fixed-length,
    /// property-free** relationship. Property-bearing rels need `rel_ok` (which calls
    /// the `!Sync` evaluator) and variable-length rels recurse through `varlen`
    /// (which charges the budget mid-recursion); both stay on the sequential
    /// [`Self::expand_chain`] path. Node-side labels/props are unrestricted — they are
    /// re-checked single-threaded in the merge.
    fn chain_parallelizable(&self, pattern: &Pattern) -> bool {
        self.fanout_pool.is_some()
            && pattern.segments.is_none()
            && !pattern.rels.is_empty()
            && pattern
                .rels
                .iter()
                .all(|(r, _)| r.var_length.is_none() && r.props.is_empty())
    }

    /// A conservative **upper bound** on node `node`'s overlaid degree in direction
    /// `dir` — used to route a hub into the streaming reader *before* its adjacency is
    /// materialised. It never under-counts a real hub: the core term is exact, the
    /// segment-born and delta-born terms are added, and deletions/tombstones (which only
    /// *reduce* degree) are ignored — so an over-estimate at worst over-streams, never
    /// OOMs by mistaking a hub for a normal node. A tombstoned node has degree 0.
    ///
    /// The core term is an O(1), zero-I/O lookup in the build-side hub-degree sidecar
    /// (`hub_degrees.blk`) when present; a generation built before the sidecar falls back
    /// to reading the record's leading edge count (one cached block). The segment-born
    /// and delta-born terms are always the same bounded reads.
    fn effective_degree_ub(&self, node: u64, dir: Direction) -> Result<u64> {
        let gen = self.gen;
        if gen.delta().is_tombstoned(node) {
            return Ok(0);
        }
        let one = |outgoing: bool| -> Result<u64> {
            // Core degree. A delta-born id (≥ core node count) has no core record ⇒ 0.
            // With the hub-degree sidecar (new builds): an O(1) lookup — exact for a
            // listed hub, else the node is below the build floor, so its UB is `floor-1`
            // (never under-counts). Without a sidecar (older generation): read the
            // record's leading edge count (one cached block, no full decode).
            let core = if node >= gen.core_generation().node_count() {
                0
            } else {
                let cg = gen.core_generation();
                match cg.hub_degree_floor() {
                    Some(floor) => {
                        let listed = if outgoing {
                            cg.core_out_degree_if_hub(node)
                        } else {
                            cg.core_in_degree_if_hub(node)
                        };
                        listed.unwrap_or(floor.saturating_sub(1) as u64)
                    }
                    None => {
                        let topo = gen.topology();
                        let global = if outgoing {
                            topo.outgoing_global(NodeId(node))
                        } else {
                            topo.incoming_global(NodeId(node))
                        };
                        let rec = self.cache.record(
                            topo.inner(),
                            gen.uuid(),
                            FileKind::Topology,
                            global,
                        )?;
                        topology::adj_count(&rec)?
                    }
                }
            };
            // Segment-born upper bound: the O(#segments), zero-I/O per-segment hub-degree
            // delta fold (Component 2). `max(0, Δ)` — a net-negative segment contribution
            // (more removed than born) is treated as 0, so the bound never deflates below
            // the core term. Zero for a singleton stack.
            let stack = gen.core_stack();
            let seg_delta = if outgoing {
                stack.hub_out_degree_delta(node)
            } else {
                stack.hub_in_degree_delta(node)
            };
            let seg = seg_delta.max(0) as u64;
            // Delta-born (bounded by the byte-capped delta): count live born edges.
            let delta = gen.delta();
            let dlt = if delta.is_empty() {
                0
            } else {
                let edges = if outgoing {
                    delta.out_edges(node)
                } else {
                    delta.in_edges(node)
                };
                edges
                    .iter()
                    .filter(|e| e.edge_id.is_some() && !e.tombstoned)
                    .count() as u64
            };
            Ok(core + seg + dlt)
        };
        Ok(match dir {
            Direction::Outgoing => one(true)?,
            Direction::Incoming => one(false)?,
            Direction::Undirected => one(true)? + one(false)?,
        })
    }

    /// Whether node `node` should be streamed rather than materialised for a hop in
    /// direction `dir` — its [`Self::effective_degree_ub`] is at/above the engine's
    /// `adj_stream_threshold`.
    fn is_hub(&self, node: u64, dir: Direction) -> Result<bool> {
        Ok(self.effective_degree_ub(node, dir)? >= self.adj_stream_threshold)
    }

    /// The **exact** count of `node`'s incident edges in `dir` — the degree-sum terminal's
    /// per-node contribution (see [`Self::degree_terminal`]). Composed from the maintained
    /// degree marginals, never a full adjacency read: core degree (O(1) from the hub-degree
    /// sidecar, else the CSR record's leading count), plus each segment's fence-gated
    /// fragment (born − removed), plus the live delta (born − suppressed).
    ///
    /// Exact **only** under the [`Self::degree_terminal_dir`] preconditions — a homogeneous
    /// final hop (so every incident edge counts) and no pending live node-deletes (so no
    /// non-local tombstone correction is owed). The caller guarantees both before arming.
    fn effective_incident_count(&self, node: u64, dir: Direction) -> Result<u64> {
        match dir {
            Direction::Outgoing => self.directed_edge_count(node, true),
            Direction::Incoming => self.directed_edge_count(node, false),
            Direction::Undirected => Ok(
                self.directed_edge_count(node, true)? + self.directed_edge_count(node, false)?
            ),
        }
    }

    /// Exact effective out-degree (`outgoing`) or in-degree of `node`, composed across the
    /// write path. See [`Self::effective_incident_count`] for the exactness preconditions.
    fn directed_edge_count(&self, node: u64, outgoing: bool) -> Result<u64> {
        let gen = self.gen;
        let cg = gen.core_generation();
        let mut deg: i64 = 0;
        // Core: exact out/in degree. Consult the **pinned** hub sidecar first (O(1), few MB,
        // always resident, covers exactly the mega-hubs) so a hub's degree — which dominates
        // count magnitude — never faults a chunk of the chunk-lazy dense column; then the dense
        // per-node column (O(1) on a resident chunk, else one ~1 MiB chunk fault covering the
        // next 262 K ids) for the long tail; then the record's leading edge count (one cached
        // block, no decode) for a generation with neither. All three are the exact core degree
        // — the sidecar and dense column agree on a listed hub — so the order is answer-neutral,
        // only cheaper. 0 for a delta-born id.
        if node < cg.node_count() {
            let listed = if outgoing {
                cg.core_out_degree_if_hub(node)
            } else {
                cg.core_in_degree_if_hub(node)
            };
            deg += match listed {
                Some(d) => d as i64,
                None => {
                    let dense = if outgoing {
                        cg.node_out_degree(node)
                    } else {
                        cg.node_in_degree(node)
                    };
                    match dense {
                        Some(d) => d as i64,
                        None => {
                            let topo = gen.topology();
                            let global = if outgoing {
                                topo.outgoing_global(NodeId(node))
                            } else {
                                topo.incoming_global(NodeId(node))
                            };
                            let rec = self.cache.record(
                                topo.inner(),
                                gen.uuid(),
                                FileKind::Topology,
                                global,
                            )?;
                            topology::adj_count(&rec)? as i64
                        }
                    }
                }
            };
        }
        // Segments: fence-gated fragment, born (+1) − removed (−1). Bounded per node; an
        // untouched node skips the segment via its O(1) presence fence. Exact composition.
        let stack = gen.core_stack();
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
                for e in frag {
                    if e.removed {
                        deg -= 1;
                    } else {
                        deg += 1;
                    }
                }
            }
        }
        // Live delta: born edge (+1), suppressed core edge (−1). Node-tombstones are ruled
        // out upfront (`degree_terminal_dir`), so no edge-to-deleted-node correction is owed.
        let delta = gen.delta();
        if !delta.is_empty() {
            let edges = if outgoing {
                delta.out_edges(node)
            } else {
                delta.in_edges(node)
            };
            for e in edges {
                if e.tombstoned {
                    deg -= 1;
                } else if e.edge_id.is_some() {
                    deg += 1;
                }
            }
        }
        Ok(deg.max(0) as u64)
    }

    /// Whether `pattern`'s final hop is a plain, unfiltered `count`-only edge that can be
    /// answered by summing effective degree over the penultimate frontier instead of
    /// expanding — returning that hop's [`Direction`] when so, else `None` (walk normally).
    ///
    /// Requires: an ordinary fixed-length chain (no path var / quantified segments /
    /// selector / restrictor / variable-length hop); the final relationship carries no
    /// property predicate and its type filter counts **every** incident edge (untyped, or
    /// the graph has exactly one reltype the filter accepts, so degree == matching count);
    /// the final node is unfiltered and not a back-reference to an earlier-bound variable
    /// (which would restrict which endpoints count); and no live node-delete is pending
    /// (which would make a maintained degree non-exact — see [`Self::directed_edge_count`]).
    /// Intermediate hops may be typed/filtered — they are walked normally; only the final
    /// hop is replaced.
    fn degree_terminal_dir(&self, pattern: &Pattern) -> Option<Direction> {
        if pattern.path_var.is_some()
            || pattern.segments.is_some()
            || pattern.selector.is_some()
            || pattern.restrictor.is_some()
            || pattern.rels.is_empty()
            || pattern.rels.iter().any(|(r, _)| r.var_length.is_some())
        {
            return None;
        }
        let (last_rel, last_node) = pattern.rels.last().unwrap();
        if !last_rel.props.is_empty() {
            return None;
        }
        // Final-hop type must count every incident edge: untyped, or a single-reltype graph
        // whose lone type the filter accepts (then out-degree == the matching-edge count).
        match &last_rel.type_expr {
            None => {}
            Some(e) => {
                let reltypes = &self.gen.manifest().reltypes;
                if reltypes.len() != 1 || !e.eval(&|name| name == reltypes[0]) {
                    return None;
                }
            }
        }
        // Final node unfiltered and a fresh endpoint (no cycle constraint).
        if last_node.label_expr.is_some() || !last_node.props.is_empty() {
            return None;
        }
        if let Some(v) = last_node.var.as_deref() {
            let bound_earlier = pattern.start.var.as_deref() == Some(v)
                || pattern.rels[..pattern.rels.len() - 1]
                    .iter()
                    .any(|(r, n)| r.var.as_deref() == Some(v) || n.var.as_deref() == Some(v));
            if bound_earlier {
                return None;
            }
        }
        if self.gen.delta().has_tombstones() {
            return None;
        }
        Some(last_rel.dir)
    }

    /// The per-neighbour merge body of [`Self::par_walk`], factored out so both the
    /// parallel-gathered normal nodes and the sequentially-**streamed** hub nodes share
    /// it verbatim: `node_ok`, the next-var equality guard, the (structurally shared)
    /// binding layer, the path-walk track, and the `EXPAND_BATCH`-bounded depth-first
    /// flush into the next hop. Consumes `hop` (it moves into the branch's `walk`).
    #[allow(clippy::too_many_arguments)]
    fn walk_merge_hop(
        &self,
        pattern: &Pattern,
        i: usize,
        start: u64,
        rel: &RelPat,
        next: &NodePat,
        track_walk: bool,
        b: &ChainBranch,
        hop: Hop,
        pending: &mut Vec<ChainBranch>,
        out: &mut Vec<HashMap<String, Val>>,
    ) -> Result<()> {
        let nb = hop.neighbour;
        if !self.node_ok(nb, next, &Scope::Frame(&b.binding), &[])? {
            return Ok(());
        }
        if let Some(v) = &next.var {
            if let Some(existing) = b.binding.get(v) {
                if existing.loose_eq(&Val::Node(nb)) != Some(true) {
                    return Ok(());
                }
            }
        }
        // Relationship-uniqueness (openCypher relationship-isomorphism): a multi-hop
        // chain must not reuse an edge already bound earlier in the same walk. `track_walk`
        // is set for such chains (see `par_walk`), so `b.walk` holds the prior hops; the
        // scan is O(chain length). This rejects exactly the hops the sequential
        // `expand_chain` rejects, so leaf order and the charge sequence stay identical.
        if track_walk && b.walk.iter().any(|h| h.edge == hop.edge) {
            return Ok(());
        }
        // Structural share: a hop that binds no variable carries the parent frame
        // unchanged (an `Arc` bump); a binding hop layers a small delta over it.
        let binding = if rel.var.is_none() && next.var.is_none() {
            b.binding.clone()
        } else {
            let mut delta: Vec<(Box<str>, Val)> = Vec::with_capacity(2);
            if let Some(v) = &rel.var {
                delta.push((v.as_str().into(), hop.as_rel()));
            }
            if let Some(v) = &next.var {
                delta.push((v.as_str().into(), Val::Node(nb)));
            }
            std::sync::Arc::new(Frame {
                parent: Some(b.binding.clone()),
                delta,
            })
        };
        let walk = if track_walk {
            let mut w = b.walk.clone();
            w.push(hop);
            w
        } else {
            Vec::new()
        };
        pending.push(ChainBranch {
            cur: nb,
            binding,
            walk,
        });
        // Flush a full batch into the next hop immediately (depth-first on an in-order
        // prefix) so the live frontier never exceeds one batch.
        if pending.len() >= EXPAND_BATCH {
            let batch = std::mem::take(pending);
            self.par_walk(pattern, i + 1, start, batch, out)?;
        }
        Ok(())
    }

    /// Parallel counterpart to [`Self::expand_chain`] for a fixed-length,
    /// property-free chain (gated by [`Self::chain_parallelizable`]). Walks the chain
    /// from anchor `cur` in **bounded breadth batches** ([`Self::par_walk`]): each
    /// batch's adjacency reads gather on the shared fanout pool ([`hops_par`]), then
    /// the merge runs **single-threaded in input order** — `node_ok` + next-var
    /// binding checks, the intermediate budget `charge()`, and the path binding. Only
    /// the adjacency I/O overlaps.
    ///
    /// Batches are expanded depth-first (an in-order prefix of the frontier, fully
    /// expanded before the next), so the emitted rows, their order, and the charge
    /// sequence are byte-for-byte identical to the sequential depth-first walk — while
    /// live memory stays bounded by `EXPAND_BATCH × chain length` instead of the whole
    /// exponential frontier. That bound is what keeps a dense chain failing *cleanly*
    /// at `maxIntermediate` (charged at completion, exactly as `expand_chain`) rather
    /// than ballooning RSS before the first charge.
    fn expand_chain_par(
        &self,
        pattern: &Pattern,
        cur: u64,
        base: &HashMap<String, Val>,
        out: &mut Vec<HashMap<String, Val>>,
        cap: Option<usize>,
    ) -> Result<()> {
        // Only entered for uncapped chains (see `match_single_pattern`): a pushed
        // `LIMIT` would over-read the breadth batches, so it routes to the sequential
        // early-exit path instead.
        debug_assert!(
            cap.is_none(),
            "expand_chain_par must not be used with a pushed cap"
        );
        let init = vec![ChainBranch {
            cur,
            binding: Frame::root(base),
            walk: Vec::new(),
        }];
        self.par_walk(pattern, 0, cur, init, out)
    }

    /// Expand hop `i` of the chain for the in-order branch `frontier`, recursing into
    /// hop `i+1`. See [`Self::expand_chain_par`] for the invariants. `start` is the
    /// anchor node (constant down the recursion, for `make_path`). Reads the frontier
    /// in [`EXPAND_READ_CHUNK`]-node chunks (parallel adjacency reads, freed per
    /// chunk), builds the next-level branches in order, and recurses depth-first as
    /// soon as a batch of [`EXPAND_BATCH`] accumulates — bounding both the read buffer
    /// and the live frontier while preserving depth-first leaf order.
    fn par_walk(
        &self,
        pattern: &Pattern,
        i: usize,
        start: u64,
        frontier: Vec<ChainBranch>,
        out: &mut Vec<HashMap<String, Val>>,
    ) -> Result<()> {
        // Degree-sum terminal: at the last hop, add each penultimate node's effective
        // degree to the count instead of expanding the final (widest) relationship. Armed
        // only in count mode over a qualifying pattern (see `degree_terminal_dir`).
        if self.degree_terminal.get() && i + 1 == pattern.rels.len() {
            let dir = pattern.rels[i].0.dir;
            for b in &frontier {
                self.check_deadline()?;
                // One unit of walk-work per penultimate node — the degree lookup that
                // replaces expanding its final edges. The count itself (`d`) is *not*
                // charged: the fast path's whole point is to tally a huge final hop in
                // O(1), so the traversal to build this frontier is what `maxScan` bounds.
                self.charge_walk(1)?;
                let d = self.effective_incident_count(b.cur, dir)?;
                let n = self.count_acc.get().unwrap_or(0);
                self.count_acc.set(Some(n + d));
            }
            return Ok(());
        }
        if i == pattern.rels.len() {
            // Completion: charge + emit each branch in order (mirrors `expand_chain`'s
            // terminal — one intermediate per emitted row, path bound if requested).
            for b in frontier {
                self.charge_walk(1)?;
                // Count-pushdown: tally the row and skip building it (no flatten, no
                // alloc) — the whole point of the fast path.
                if self.count_tally() {
                    continue;
                }
                // The owned map every downstream consumer expects is built here, once
                // per completed row — the only flatten in the walk.
                let mut binding = b.binding.flatten();
                if let Some(pv) = &pattern.path_var {
                    binding.insert(pv.clone(), make_path(start, &b.walk));
                }
                out.push(binding);
            }
            return Ok(());
        }
        let (gen, cache) = (self.gen, self.cache);
        let (rel, next) = &pattern.rels[i];
        let tf = resolve_type_filter(gen, rel);
        let dir = rel.dir;
        // Track the per-branch walk when a path variable needs it, OR when the chain
        // has more than one relationship — a multi-hop chain needs the prior hops'
        // edge ids to enforce relationship-uniqueness in `walk_merge_hop`. A single-hop
        // chain has no earlier edge to collide with, so it stays allocation-free.
        let track_walk = pattern.path_var.is_some() || pattern.rels.len() > 1;
        let mut pending: Vec<ChainBranch> = Vec::new();
        // Read in small node-chunks, not the whole frontier at once: a chunk's
        // adjacency buffer (`neigh`) is freed before the next is read, so live read
        // memory stays `O(EXPAND_READ_CHUNK × degree)` — one chunk's worth — instead
        // of the whole frontier's edges. Without this, a frontier of high-degree hubs
        // materialises tens of millions of edges in a single buffer (the sequential
        // walk only ever holds one node's adjacency). The chunk is ≥ [`EXPAND_PAR_MIN`]
        // so each read still fans out across the pool.
        for chunk in frontier.chunks(EXPAND_READ_CHUNK) {
            self.check_deadline()?;
            // Route hub nodes out of the wide parallel gather: a hub's multi-million-edge
            // adjacency materialised inside `par_gather` (up to a whole chunk of them at
            // once) is the fan-out OOM. Decide per node from the cheap upper-bound degree
            // probe; gather the *normal* nodes in parallel as before, and stream each hub
            // sequentially in bounded chunks (its live buffer stays `O(ADJ_STREAM_CHUNK)`).
            let hub: Vec<bool> = chunk
                .iter()
                .map(|b| self.is_hub(b.cur, dir))
                .collect::<Result<_>>()?;
            let normal_nodes: Vec<u64> = chunk
                .iter()
                .zip(&hub)
                .filter(|(_, &h)| !h)
                .map(|(b, _)| b.cur)
                .collect();
            let neigh = par_gather(
                self.fanout_pool.as_deref(),
                &normal_nodes,
                EXPAND_PAR_MIN,
                |&n| hops_par(gen, cache, n, dir, tf.as_ref()),
            )?;
            // Charge the gathered normal hops against the intermediate budget (root cause
            // 2b), mirroring the sequential `expand_one_hop`. `hops_par` runs on the rayon
            // pool where `self.charge` (a non-`Sync` `Cell`) cannot be touched, so the
            // charge stays here on the calling thread once the buffer is materialised.
            // Streamed hub hops are charged per streamed chunk below, as they are produced.
            let produced: u64 = neigh.iter().map(|h| h.len() as u64).sum();
            self.charge_walk(produced)?;
            // Merge in input order — `walk_merge_hop` is the shared per-neighbour body.
            // Normal nodes consume their gathered hops (in chunk order); hub nodes stream.
            let mut normal = neigh.into_iter();
            for (b, &is_hub_node) in chunk.iter().zip(&hub) {
                if is_hub_node {
                    for_each_hop_overlaid(
                        gen,
                        cache,
                        b.cur,
                        dir,
                        tf.as_ref(),
                        self.adj_stream_chunk,
                        &mut |hops| {
                            self.charge_walk(hops.len() as u64)?;
                            for hop in hops {
                                self.walk_merge_hop(
                                    pattern,
                                    i,
                                    start,
                                    rel,
                                    next,
                                    track_walk,
                                    b,
                                    hop.clone(),
                                    &mut pending,
                                    out,
                                )?;
                            }
                            Ok(())
                        },
                    )?;
                } else {
                    let hops = normal.next().expect("one gather result per normal node");
                    for hop in hops {
                        self.walk_merge_hop(
                            pattern,
                            i,
                            start,
                            rel,
                            next,
                            track_walk,
                            b,
                            hop,
                            &mut pending,
                            out,
                        )?;
                    }
                }
            }
        }
        if !pending.is_empty() {
            self.par_walk(pattern, i + 1, start, pending, out)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // recursive walk: scratch path buffer + start anchor
    fn expand_chain(
        &self,
        pattern: &Pattern,
        i: usize,
        cur: u64,
        binding: &mut HashMap<String, Val>,
        start: u64,
        walk: &mut Vec<Hop>,
        out: &mut Vec<HashMap<String, Val>>,
        cap: Option<usize>,
    ) -> Result<()> {
        // Mutate-in-place binding frame (root cause 6): rather than `binding.clone()`
        // per neighbour per hop, each branch inserts its hop's rel/next bindings,
        // recurses, then restores them on backtrack via `restore_binding`. The only
        // remaining clone is one per *completed* row (when pushing into `out`),
        // which is unavoidable and matches the streaming-scan path. `walk` (the path
        // scratch) and the var-length `used` set already use the same push/pop
        // discipline, so siblings stay isolated.
        //
        // Stage 6: `cap` is the number of rows the final `LIMIT` still needs (only
        // set when the projection is a 1:1 map with no ORDER BY/aggregation/DISTINCT
        // and no residual WHERE — see `match_patterns`). Once `out` reaches it we
        // unwind without expanding further, so a query like the 3-hop that would
        // otherwise buffer ~28k paths for `LIMIT 100` stops after 100.
        if cap.is_some_and(|c| out.len() >= c) {
            return Ok(());
        }
        // Degree-sum terminal (uncapped count fast path): at the last hop, add `cur`'s
        // effective degree to the count instead of expanding the final relationship. See
        // [`Self::degree_terminal`] / [`Self::par_walk`]'s matching short-circuit.
        if self.degree_terminal.get() && i + 1 == pattern.rels.len() {
            // One unit of walk-work (the degree lookup); the count is tallied in O(1) and
            // deliberately not charged to `maxScan` (see `par_walk`'s matching hook).
            self.charge_walk(1)?;
            let d = self.effective_incident_count(cur, pattern.rels[i].0.dir)?;
            let n = self.count_acc.get().unwrap_or(0);
            self.count_acc.set(Some(n + d));
            return Ok(());
        }
        if i == pattern.rels.len() {
            // Charge each completed binding. `match_single_pattern` buffers the whole
            // single-pattern result set here (its `partial` vector) *before* the
            // cross-pattern join re-charges it at the `match_patterns` terminal, so a
            // dense expansion (e.g. every `:LINK` edge over a 1M-node graph) must trip
            // the budget here — otherwise `partial` balloons RSS to an OOM before the
            // charged terminal is ever reached. The double count over the two buffers
            // mirrors their genuine combined peak (conservative on purpose).
            self.charge_walk(1)?;
            // Count-pushdown: tally the row and skip building it (no clone).
            if self.count_tally() {
                return Ok(());
            }
            if let Some(pv) = &pattern.path_var {
                // Bind the path for this completed walk, snapshot the row, then
                // restore so sibling branches don't inherit a stale path value.
                let prev = binding.insert(pv.clone(), make_path(start, walk));
                out.push(binding.clone());
                restore_binding(binding, pv.clone(), prev);
            } else {
                out.push(binding.clone());
            }
            return Ok(());
        }
        self.check_deadline()?;
        let (rel, next) = &pattern.rels[i];
        match &rel.var_length {
            None if cap.is_none() && self.is_hub(cur, rel.dir)? => {
                // Hub source on an uncapped chain: stream its adjacency in bounded chunks
                // rather than materialise the whole hop list (bounds even a lone 10M-edge
                // hub at fan=1). `for_each_hop_overlaid` applies only the type filter, so
                // the relationship-property predicate (`rel_ok`) and the intermediate
                // charge are applied per hop here — matching `expand_one_hop` exactly. The
                // in-place binding/walk push→recurse→restore is unchanged. Gated to
                // `cap.is_none()`: a pushed LIMIT keeps the early-exit materialise path.
                let (gen, cache) = (self.gen, self.cache);
                let tf = resolve_type_filter(gen, rel);
                for_each_hop_overlaid(
                    gen,
                    cache,
                    cur,
                    rel.dir,
                    tf.as_ref(),
                    self.adj_stream_chunk,
                    &mut |hops| {
                        for hop in hops {
                            if !self.rel_ok(hop.edge, rel, binding)? {
                                continue;
                            }
                            self.charge_walk(1)?;
                            let nb = hop.neighbour;
                            if !self.node_ok(nb, next, &Scope::Map(binding), &[])? {
                                continue;
                            }
                            if let Some(v) = &next.var {
                                if let Some(existing) = binding.get(v) {
                                    if existing.loose_eq(&Val::Node(nb)) != Some(true) {
                                        continue;
                                    }
                                }
                            }
                            // Relationship-uniqueness (openCypher relationship-isomorphism):
                            // an edge already bound earlier in this chain cannot be reused
                            // (e.g. an undirected 2-hop bouncing back over the same edge).
                            // `walk` holds this walk's prior hops; scan is O(chain length).
                            if walk.iter().any(|h| h.edge == hop.edge) {
                                continue;
                            }
                            let prev_rel = rel
                                .var
                                .as_ref()
                                .map(|v| (v.clone(), binding.insert(v.clone(), hop.as_rel())));
                            let prev_next = next
                                .var
                                .as_ref()
                                .map(|v| (v.clone(), binding.insert(v.clone(), Val::Node(nb))));
                            walk.push(hop.clone());
                            self.expand_chain(pattern, i + 1, nb, binding, start, walk, out, cap)?;
                            walk.pop();
                            if let Some((v, prev)) = prev_next {
                                restore_binding(binding, v, prev);
                            }
                            if let Some((v, prev)) = prev_rel {
                                restore_binding(binding, v, prev);
                            }
                        }
                        Ok(())
                    },
                )?;
            }
            None => {
                for hop in self.expand_one_hop(cur, rel, binding)? {
                    if cap.is_some_and(|c| out.len() >= c) {
                        break;
                    }
                    let nb = hop.neighbour;
                    if !self.node_ok(nb, next, &Scope::Map(binding), &[])? {
                        continue;
                    }
                    if let Some(v) = &next.var {
                        if let Some(existing) = binding.get(v) {
                            if existing.loose_eq(&Val::Node(nb)) != Some(true) {
                                continue;
                            }
                        }
                    }
                    // Relationship-uniqueness (openCypher relationship-isomorphism): skip a
                    // hop whose edge is already bound earlier in this chain. `walk` holds
                    // this walk's prior hops; the scan is O(chain length).
                    if walk.iter().any(|h| h.edge == hop.edge) {
                        continue;
                    }
                    let prev_rel = rel
                        .var
                        .as_ref()
                        .map(|v| (v.clone(), binding.insert(v.clone(), hop.as_rel())));
                    let prev_next = next
                        .var
                        .as_ref()
                        .map(|v| (v.clone(), binding.insert(v.clone(), Val::Node(nb))));
                    walk.push(hop);
                    self.expand_chain(pattern, i + 1, nb, binding, start, walk, out, cap)?;
                    walk.pop();
                    // Restore LIFO so an aliasing rel/next var name unwinds correctly.
                    if let Some((v, prev)) = prev_next {
                        restore_binding(binding, v, prev);
                    }
                    if let Some((v, prev)) = prev_rel {
                        restore_binding(binding, v, prev);
                    }
                }
            }
            Some(vl) => {
                let (min, max) = varlen_bounds(vl);
                let mode = walk_mode(pattern.restrictor);
                let mut paths: Vec<(Vec<Hop>, u64)> = Vec::new();
                // Seed the trail's used-edge set with the edges already bound by the
                // fixed prefix of this chain, so relationship-uniqueness holds across the
                // fixed→var-length boundary (a `*` segment can't re-walk an earlier hop's
                // edge). Only consulted in Trail mode; varlen only removes edges it itself
                // inserts, so these seeds persist for the whole segment.
                let mut used: HashSet<u64> = walk.iter().map(|h| h.edge).collect();
                // `visited` (node-uniqueness for ACYCLIC/SIMPLE) is seeded with the
                // walk's start node so a hop back to it is detected — rejected by
                // ACYCLIC, allowed once as the closing endpoint by SIMPLE.
                let mut visited = HashSet::new();
                if matches!(mode, WalkMode::Acyclic | WalkMode::Simple) {
                    visited.insert(cur);
                }
                let mut path = Vec::new();
                self.varlen(
                    cur,
                    cur,
                    rel,
                    (min, max),
                    mode,
                    &mut path,
                    &mut used,
                    &mut visited,
                    &mut paths,
                    binding,
                )?;
                for (hops, endnode) in paths {
                    if cap.is_some_and(|c| out.len() >= c) {
                        break;
                    }
                    if !self.node_ok(endnode, next, &Scope::Map(binding), &[])? {
                        continue;
                    }
                    if let Some(v) = &next.var {
                        if let Some(existing) = binding.get(v) {
                            if existing.loose_eq(&Val::Node(endnode)) != Some(true) {
                                continue;
                            }
                        }
                    }
                    let prev_rel = rel.var.as_ref().map(|v| {
                        let rels = Val::List(hops.iter().map(Hop::as_rel).collect());
                        (v.clone(), binding.insert(v.clone(), rels))
                    });
                    let prev_next = next
                        .var
                        .as_ref()
                        .map(|v| (v.clone(), binding.insert(v.clone(), Val::Node(endnode))));
                    let n = hops.len();
                    walk.extend(hops);
                    self.expand_chain(pattern, i + 1, endnode, binding, start, walk, out, cap)?;
                    walk.truncate(walk.len() - n);
                    if let Some((v, prev)) = prev_next {
                        restore_binding(binding, v, prev);
                    }
                    if let Some((v, prev)) = prev_rel {
                        restore_binding(binding, v, prev);
                    }
                }
            }
        }
        Ok(())
    }

    /// Depth-first variable-length expansion, emitting `(path_edges, end_node)` for
    /// every path whose length is in `[min, max]`. `mode` (the GQL path restrictor,
    /// `WalkMode::Trail` by default) governs node/edge reuse within the walk:
    /// - `Walk` — no restriction (repeated nodes and edges allowed). Bounded only by
    ///   `max` (`MAX_VARLEN_HOPS` for an open `*`), the intermediate budget and the
    ///   deadline, since a cycle would otherwise expand without limit.
    /// - `Trail` — no repeated edge (the historical default for `*`); tracked in
    ///   `used`.
    /// - `Acyclic` — no repeated node at all (endpoints included); tracked in
    ///   `visited`, which the caller seeds with the start node.
    /// - `Simple` — no repeated node *except* the two endpoints may coincide (a
    ///   single closed cycle); a hop back to the start node is emitted but not
    ///   extended, so the start can never become an interior repeat.
    ///
    /// Node-uniqueness implies edge-uniqueness, so `Acyclic`/`Simple` need only the
    /// `visited` set and leave `used` untouched; `Trail` uses only `used`. This keeps
    /// each mode's per-hop work minimal and the `Trail`/default path byte-for-byte as
    /// before.
    #[allow(clippy::too_many_arguments)] // recursive DFS: scratch buffers + scope
    fn varlen(
        &self,
        start: u64,
        node: u64,
        rel: &RelPat,
        bounds: (u32, u32),
        mode: WalkMode,
        path: &mut Vec<Hop>,
        used: &mut HashSet<u64>,
        visited: &mut HashSet<u64>,
        out: &mut Vec<(Vec<Hop>, u64)>,
        binding: &HashMap<String, Val>,
    ) -> Result<()> {
        let (min, max) = bounds;
        if path.len() as u32 >= min {
            // Each emission clones the hop vector, so charge by path length: on a
            // dense graph the depth cap alone still permits an enormous result set.
            self.charge(path.len() as u64 + 1)?;
            out.push((path.clone(), node));
        }
        if path.len() as u32 >= max {
            return Ok(());
        }
        self.check_deadline()?;
        let track_edges = matches!(mode, WalkMode::Trail);
        let track_nodes = matches!(mode, WalkMode::Acyclic | WalkMode::Simple);
        for hop in self.expand_one_hop(node, rel, binding)? {
            let edge = hop.edge;
            let nb = hop.neighbour;
            // SIMPLE alone permits the one repeat that closes the walk at its start;
            // it is emitted but never extended (extending would repeat the start as
            // an interior node).
            let mut close_only = false;
            match mode {
                WalkMode::Walk => {}
                WalkMode::Trail => {
                    if used.contains(&edge) {
                        continue;
                    }
                }
                WalkMode::Acyclic => {
                    if visited.contains(&nb) {
                        continue;
                    }
                }
                WalkMode::Simple => {
                    if visited.contains(&nb) {
                        if nb != start {
                            continue;
                        }
                        close_only = true;
                    }
                }
            }
            if track_edges {
                used.insert(edge);
            }
            // `insert` returns false (so `inserted` stays false) when the node is
            // already present — e.g. the SIMPLE close-the-cycle hop back to `start`,
            // which the caller pre-seeded — so we never wrongly remove it on unwind.
            let inserted = track_nodes && visited.insert(nb);
            path.push(hop);
            if close_only {
                if path.len() as u32 >= min {
                    self.charge(path.len() as u64 + 1)?;
                    out.push((path.clone(), nb));
                }
            } else {
                self.varlen(
                    start, nb, rel, bounds, mode, path, used, visited, out, binding,
                )?;
            }
            path.pop();
            if track_edges {
                used.remove(&edge);
            }
            if inserted {
                visited.remove(&nb);
            }
        }
        Ok(())
    }

    /// One traversal step from `node`: edges matching the pattern's direction,
    /// type alternation and relationship property predicates, each resolved to a
    /// [`Hop`] (edge, neighbour, type, and stored src→dst endpoints).
    ///
    /// Charges the produced hops via [`charge_walk`](Self::charge_walk) — the retained
    /// `maxIntermediate` budget in row-building mode, the transient `maxScan` budget in
    /// count-pushdown mode where the adjacency Vec is read-then-discarded (root cause 2b):
    /// expanding a hub reads its whole adjacency and builds one `Hop` per matching
    /// edge — a `Vec<Hop>` that, summed over a depth-first chain walk, is the bulk
    /// of an expansion-heavy query's transient allocation. Without this charge the
    /// terminal `charge(1)` per *completed* row only trips once millions of heavy
    /// binding rows have already materialised (the 2b OOM); charging per produced
    /// hop trips a hub expansion immediately, before those rows accumulate. The
    /// charge is cumulative (never refunded within the query), so a fan-out that
    /// re-expands the same hub at every branch is bounded by total work, not peak.
    ///
    /// Kept on the budgeted traversal wrapper rather than [`Self::expand_with_dir`]
    /// itself: the latter is also the reader for `shortestPath()` reconstruction and
    /// its sequential fallback, which are bounded by the dedicated
    /// `maxShortestPathExplore` cap and must stay independent of `maxIntermediate`.
    fn expand_one_hop(
        &self,
        node: u64,
        rel: &RelPat,
        binding: &HashMap<String, Val>,
    ) -> Result<Vec<Hop>> {
        let hops = self.expand_with_dir(node, rel, rel.dir, binding)?;
        self.charge_walk(hops.len() as u64)?;
        Ok(hops)
    }

    /// As [`Self::expand_one_hop`] but with an explicit traversal `dir`, overriding
    /// `rel.dir`. The bidirectional `shortestPath()` search uses this to walk the
    /// *reverse* of the pattern direction outward from `dst` (so an `(a)-[:T]->(b)`
    /// search expands `dst` over incoming `:T` edges). Type alternation and the
    /// relationship property predicate are still taken from `rel`.
    fn expand_with_dir(
        &self,
        node: u64,
        rel: &RelPat,
        dir: Direction,
        binding: &HashMap<String, Val>,
    ) -> Result<Vec<Hop>> {
        // Resolve the relationship-type constraint once, before the per-edge loop.
        // The overwhelmingly common shapes — untyped, a single `:T`, or a `:T1|T2`
        // alternation — collapse to a flat reltype-id set so the hot loop stays a
        // plain `ids.contains` integer test, exactly as before GQL. Only a genuine
        // boolean type expression (`&`/`!`) falls to per-edge evaluation.
        let type_filter = resolve_type_filter(self.gen, rel);
        // (adjacency list, `incoming`) — for an incoming edge the stored direction
        // is neighbour→node, so start/end are swapped relative to an outgoing one.
        let mut sources: Vec<(Vec<topology::Adj>, bool)> = Vec::new();
        match dir {
            Direction::Outgoing => sources.push((self.outgoing(node)?, false)),
            Direction::Incoming => sources.push((self.incoming(node)?, true)),
            Direction::Undirected => {
                sources.push((self.outgoing(node)?, false));
                sources.push((self.incoming(node)?, true));
            }
        }
        let mut out = Vec::new();
        for (adjs, incoming) in sources {
            for a in adjs {
                match &type_filter {
                    None => {}
                    Some(TypeFilter::AnyOf(ids)) => {
                        if !ids.contains(&a.reltype) {
                            continue;
                        }
                    }
                    // A relationship carries exactly one type, so evaluate the
                    // expression over the singleton present-set {this edge's type}.
                    Some(TypeFilter::Expr(e))
                        if !e.eval(&|name| self.gen.reltype_id(name) == Some(a.reltype)) =>
                    {
                        continue;
                    }
                    Some(TypeFilter::Expr(_)) => {}
                }
                if !self.rel_ok(a.edge.0, rel, binding)? {
                    continue;
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

    /// Candidate node ids for a chosen scan strategy, as a **lazy** bounded-window
    /// stream (drain it with [`Engine::next_candidates`]).
    ///
    /// The two full-width sweeps — `AllNodes` and a plain `LabelScan`, the only strategies
    /// whose size is the *graph's*, not the query's — are produced a window at a time and
    /// never materialised. A pushed `LIMIT` therefore stops the scan itself, instead of
    /// merely truncating a row loop over an already-built `Vec` of the whole id space
    /// (733 MB of `Vec<u64>` on the 91.6M-node graph, allocated before the first row and
    /// charged to no budget — the defect this replaces). The scan's resident footprint is
    /// now one window, [`CAND_WINDOW_MAX`] ids, whatever the graph's size.
    ///
    /// Every other strategy is bounded by an index seek or a precomputed posting and is
    /// materialised eagerly, exactly as before, then tombstone-suppressed. Anchor scans are
    /// deliberately charged to neither budget (a point lookup must stay ~free — see
    /// `shortest_path_any_succeeds_under_tiny_budget` — and `maxScan` meters *walk* work —
    /// see `hub_count_one_hop_answered_by_degree_terminal`); what they touch is instead
    /// counted in [`Engine::anchor_ids_scanned`].
    ///
    /// A `LabelScan` is only lazy over a *pure core with no delta*: with a segment stack
    /// or a write delta, membership is a sorted union of base ∪ segment-born ∪ delta-born
    /// ids (a node can gain or lose the label above the base), which cannot be produced in
    /// ascending order window-by-window — so that shape keeps the eager fold.
    fn candidate_stream<'c>(&self, scan: &NodeScan) -> Result<CandidateStream<'c>> {
        let ids = match scan {
            // The dense id space, produced lazily. `node_count` is the *scan bound* (base
            // + every segment's born band + the delta's born ids), and per-window
            // tombstone suppression drops deleted ids exactly as the eager sweep did — so
            // this arm is correct for every stack/delta shape.
            NodeScan::AllNodes => {
                return Ok(CandidateStream::sweep(None, self.gen.node_count()));
            }
            // Pure core, no delta: re-derive the label's ids window-by-window from the
            // node-label column (the same single pass `collect_nodes_with_label` makes,
            // just resumable).
            NodeScan::LabelScan { label_id }
                if self.gen.core_stack().is_singleton() && self.gen.delta().is_empty() =>
            {
                return Ok(CandidateStream::sweep(
                    Some(*label_id),
                    self.gen.node_count(),
                ));
            }
            // Already bounds-checked + deduped by the planner; yield as-is. An
            // empty list is a seek that matched no node.
            NodeScan::IdSeek { ids } => ids.clone(),
            NodeScan::RangeEq { index, key } => {
                let mut ids = self
                    .gen
                    .range_index(index)
                    .expect("planner only picks open indexes")
                    .lookup_eq(key)?;
                // Core stack (below the delta): suppress base hits the segments supersede,
                // union the segments' matching born/patched ids, then restore ascending
                // order for the delta overlay below.
                let stack = self.gen.core_stack();
                if !stack.is_singleton() {
                    if let Some((label, prop)) = self.node_index_label_prop(index) {
                        stack.fold_index_eq(&mut ids, label, prop, key)?;
                        ids.sort_unstable();
                        ids.dedup();
                    }
                }
                let delta = self.gen.delta();
                if !delta.is_empty() {
                    if let Some((label, prop)) = self.node_index_label_prop(index) {
                        // Moved-indexed-value overlay: a core node whose indexed property
                        // was patched is still listed at its *old* value in the ISAM.
                        // Drop hits whose patched value moved out of the seek, and add
                        // core nodes whose patched value moved in (inserted in sorted
                        // position so the ascending order holds).
                        ids.retain(|&x| delta.core_hit_survives_eq(x, prop, key));
                        for id in delta.moved_core_ids_in_index_eq(label, prop, key) {
                            if let Err(pos) = ids.binary_search(&id) {
                                ids.insert(pos, id);
                            }
                        }
                        // Delta-born nodes (Phase 2c) are not in the core ISAM — append
                        // the synthetic ids whose indexed property equals `key`, so a
                        // created node is found by an equality seek (Phase 2d). Born ids
                        // sort after every core id, so the ascending order holds.
                        // Tombstoned ids are dropped by the suppression below.
                        ids.extend(delta.born_ids_in_index_eq(label, prop, key));
                    }
                }
                ids
            }
            NodeScan::RangeRange { index, lo, hi } => {
                let mut ids = self
                    .gen
                    .range_index(index)
                    .expect("planner only picks open indexes")
                    .lookup_range(
                        lo.as_ref().map(|(v, _)| v),
                        lo.as_ref().map(|(_, i)| *i).unwrap_or(true),
                        hi.as_ref().map(|(v, _)| v),
                        hi.as_ref().map(|(_, i)| *i).unwrap_or(true),
                    )?;
                // Core stack index fragments (below the delta), mirroring `RangeEq`.
                let stack = self.gen.core_stack();
                if !stack.is_singleton() {
                    if let Some((label, prop)) = self.node_index_label_prop(index) {
                        stack.fold_index_range(
                            &mut ids,
                            label,
                            prop,
                            lo.as_ref().map(|(v, _)| v),
                            lo.as_ref().map(|(_, i)| *i).unwrap_or(true),
                            hi.as_ref().map(|(v, _)| v),
                            hi.as_ref().map(|(_, i)| *i).unwrap_or(true),
                        )?;
                        ids.sort_unstable();
                        ids.dedup();
                    }
                }
                // Mirrors the `RangeEq` overlay above: relocate patched core nodes in
                // the range index, then append matching delta-born nodes (Phase 2d).
                let delta = self.gen.delta();
                if !delta.is_empty() {
                    if let Some((label, prop)) = self.node_index_label_prop(index) {
                        let lo_v = lo.as_ref().map(|(v, _)| v);
                        let lo_i = lo.as_ref().map(|(_, i)| *i).unwrap_or(true);
                        let hi_v = hi.as_ref().map(|(v, _)| v);
                        let hi_i = hi.as_ref().map(|(_, i)| *i).unwrap_or(true);
                        ids.retain(|&x| {
                            delta.core_hit_survives_range(x, prop, lo_v, lo_i, hi_v, hi_i)
                        });
                        for id in
                            delta.moved_core_ids_in_index_range(label, prop, lo_v, lo_i, hi_v, hi_i)
                        {
                            if let Err(pos) = ids.binary_search(&id) {
                                ids.insert(pos, id);
                            }
                        }
                        ids.extend(
                            delta.born_ids_in_index_range(label, prop, lo_v, lo_i, hi_v, hi_i),
                        );
                    }
                }
                ids
            }
            // LabelScan under a segment stack or a write delta (HIK-104). Membership is a
            // sorted union of the base label column, the stack's re-decided carriers and the
            // delta's born/added-label ids — which the eager path built into one `Vec` (the
            // 733 MB base collect) before the first row. Stream it as a k-way merge instead:
            // the base column is a lazy `LabelCol` source (minus the stack-overridden ids),
            // the write-bounded overlay is one `Mat` source, and per-window `sort`+`dedup`
            // reproduces the eager union exactly, one window resident.
            NodeScan::LabelScan { label_id } => {
                let stack = self.gen.core_stack();
                let delta = self.gen.delta();
                let label = self.gen.label_name(*label_id).map(str::to_string);
                // Stack overlay: `exclude` are the ids whose base membership the stack
                // overrides (removed from the base sweep), `carriers` the subset whose
                // effective row still carries the label (re-added) — the exact split
                // `fold_label_scan` performs.
                let mut exclude: HashSet<u64> = HashSet::new();
                let mut overlay: Vec<u64> = Vec::new();
                if !stack.is_singleton() {
                    if let Some(label) = label.as_deref() {
                        if let Some((touched, carriers)) = stack.label_scan_overlay(label)? {
                            exclude = touched.into_iter().collect();
                            overlay = carriers;
                        }
                    }
                }
                // Delta-born nodes (Phase 2c) are not in the base label column; ids that
                // *gained* the label via `SET n:Label` are re-added here too. A core node
                // that *dropped* the label stays in the base sweep and is re-checked and
                // rejected by `node_ok` (the scan is not trusted to prove the label — see
                // `scan_guaranteed_labels`); tombstoned ids are dropped by per-window
                // suppression. The empty-delta fast path skips the lookup entirely.
                if !delta.is_empty() {
                    if let Some(label) = label.as_deref() {
                        overlay.extend(delta.born_ids_with_label(label));
                        overlay.extend(delta.ids_with_added_label(label));
                    }
                }
                overlay.sort_unstable();
                overlay.dedup();
                let mut srcs = vec![MergeSrc::LabelCol {
                    label: *label_id,
                    exclude,
                    col_end: self.gen.node_label_column_len(),
                }];
                if !overlay.is_empty() {
                    srcs.push(MergeSrc::Mat {
                        ids: overlay,
                        pos: 0,
                    });
                }
                return Ok(CandidateStream::merge(srcs, self.gen.node_count()));
            }
            // Distinct edge-having endpoint nodes for the typed first hop (HIK-104). The
            // base posting expands to the whole endpoint set (733 MB for a dense reltype),
            // so stream it: each base posting is a lazy `Posting` cursor over the compressed
            // Elias–Fano form, each segment's endpoint slice a write-bounded `Mat` source, and
            // the k-way merge produces the ascending+deduped union a window at a time. Postings
            // carry no removals — a superset stays correct because the first hop re-filters by
            // reltype (and per-window suppression drops deleted nodes).
            NodeScan::RelTypeScan {
                reltype_ids, side, ..
            } => {
                let mut srcs: Vec<MergeSrc> = self
                    .gen
                    .endpoint_posting_cursors(reltype_ids, *side)?
                    .into_iter()
                    .map(|mut iter| {
                        let head = iter.next();
                        MergeSrc::Posting { iter, head }
                    })
                    .collect();
                let stack = self.gen.core_stack();
                if !stack.is_singleton() {
                    for seg in stack.segments() {
                        let Some(post) = &seg.postings else { continue };
                        for &rt in reltype_ids {
                            let Some(name) = self.gen.reltype_name(rt) else {
                                continue;
                            };
                            let mut seg_ids: Vec<u64> = Vec::new();
                            if matches!(side, RelEndpointSide::Source | RelEndpointSide::Either) {
                                seg_ids.extend_from_slice(post.src_ids(name));
                            }
                            if matches!(side, RelEndpointSide::Target | RelEndpointSide::Either) {
                                seg_ids.extend_from_slice(post.tgt_ids(name));
                            }
                            if !seg_ids.is_empty() {
                                // A segment's src and tgt slices are each ascending+distinct,
                                // but their concatenation (Either side) is not — normalise so
                                // the merge's per-source ascending invariant holds.
                                seg_ids.sort_unstable();
                                seg_ids.dedup();
                                srcs.push(MergeSrc::Mat {
                                    ids: seg_ids,
                                    pos: 0,
                                });
                            }
                        }
                    }
                }
                return Ok(CandidateStream::merge(srcs, self.gen.node_count()));
            }
        };
        let mut ids = ids;
        self.suppress_tombstoned_in_place(&mut ids)?;
        self.scanned_ids
            .set(self.scanned_ids.get().saturating_add(ids.len() as u64));
        Ok(CandidateStream::owned(ids))
    }

    /// The next window of candidate ids from `s`, or `None` once the scan is exhausted.
    /// The slice borrows the stream and stays valid until the next call.
    ///
    /// A lazy window is decoded, tombstone-suppressed and counted into
    /// [`Engine::anchor_ids_scanned`] as it is produced — so a consumer that stops early
    /// (a pushed `LIMIT`) leaves the rest of the id space untouched. A window that
    /// suppression (or the label filter) empties is skipped, not yielded, so a caller never
    /// mistakes it for the end of the scan.
    fn next_candidates<'s>(&self, s: &'s mut CandidateStream<'_>) -> Result<Option<&'s [u64]>> {
        let CandidateStream {
            src,
            pos,
            buf,
            window,
        } = s;
        let (label, next, end) = match src {
            // Materialised sources are handed out in windows too, so a capped consumer
            // walks no further into them than it must. They are already suppressed and
            // counted, so no per-window work is done here.
            CandidateSrc::Ready(ids) => return Ok(slice_window(ids, pos)),
            CandidateSrc::Owned(ids) => return Ok(slice_window(ids, pos)),
            CandidateSrc::Merge { srcs, next, end } => {
                return self.next_merge_window(srcs, next, end, buf, window);
            }
            CandidateSrc::Sweep { label, next, end } => (label, next, end),
        };
        buf.clear();
        while *next < *end {
            let (lo, hi) = (*next, (*next + *window).min(*end));
            *next = hi;
            // Ramp the window: the first one is small so a `LIMIT 1` touches ~1 K ids,
            // then it grows to `CAND_WINDOW_MAX` so an uncapped sweep amortises the
            // per-window block locate/decompress back to the old single-pass cost.
            *window = window.saturating_mul(8).min(CAND_WINDOW_MAX);
            // A long sweep is now interruptible: the eager collect ran to completion before
            // the executor could look at the clock again.
            self.check_deadline()?;
            self.scanned_ids
                .set(self.scanned_ids.get().saturating_add(hi - lo));
            match label {
                None => buf.extend(lo..hi),
                Some(l) => {
                    let labels = self.gen.node_labels();
                    let (bitmask, want) = (labels.bitmask(), *l);
                    labels.inner().for_each_record_in(lo, hi, |node_id, rec| {
                        if graph_format::nodelabels::decode_labels(rec, bitmask)?.contains(&want) {
                            buf.push(node_id);
                        }
                        Ok(())
                    })?;
                }
            }
            self.suppress_tombstoned_in_place(buf)?;
            if !buf.is_empty() {
                return Ok(Some(buf.as_slice()));
            }
        }
        Ok(None)
    }

    /// Drain the next non-empty window of a k-way anchor merge ([`CandidateSrc::Merge`],
    /// HIK-104). It partitions the id space into the *same* ramping windows a sweep uses;
    /// per window `[lo, hi)` it pulls only the ids each source has in range — the base label
    /// column decoded lazily (minus the stack-overridden ids), each base endpoint posting
    /// walked from its compressed Elias–Fano cursor, and the write-bounded overlays advanced
    /// by index — then `sort_unstable`+`dedup`s that bounded buffer. Every id falls in exactly
    /// one window, so this reproduces the eager path's global `sort`+`dedup` ordering and
    /// cross-source dedup exactly while the resident footprint stays one window. Tombstones are
    /// suppressed per window (a `SET`-dropped label is left to `node_ok`, as before), the
    /// id-space frontier walked is counted into [`Engine::anchor_ids_scanned`] so a pushed
    /// `LIMIT` stops the merge, and empty windows are skipped rather than yielded.
    fn next_merge_window<'b>(
        &self,
        srcs: &mut [MergeSrc],
        next: &mut u64,
        end: &mut u64,
        buf: &'b mut Vec<u64>,
        window: &mut u64,
    ) -> Result<Option<&'b [u64]>> {
        buf.clear();
        while *next < *end {
            let (lo, hi) = (*next, (*next + *window).min(*end));
            *next = hi;
            *window = window.saturating_mul(8).min(CAND_WINDOW_MAX);
            self.check_deadline()?;
            self.scanned_ids
                .set(self.scanned_ids.get().saturating_add(hi - lo));
            for src in srcs.iter_mut() {
                match src {
                    // Write-bounded, already ascending+distinct: emit its ids below `hi`. The
                    // cursor is monotone, so everything left is `>= lo` already.
                    MergeSrc::Mat { ids, pos } => {
                        while *pos < ids.len() && ids[*pos] < hi {
                            buf.push(ids[*pos]);
                            *pos += 1;
                        }
                    }
                    // Ascending Elias–Fano posting cursor: emit its ids below `hi`, keeping the
                    // straddling head for the window it belongs to.
                    MergeSrc::Posting { iter, head } => {
                        while let Some(h) = *head {
                            if h < hi {
                                buf.push(h);
                                *head = iter.next();
                            } else {
                                break;
                            }
                        }
                    }
                    // The base label column, decoded lazily over this window (clamped to the
                    // column's records; higher ids are born ids the overlay supplies), keeping
                    // the label's carriers that the stack does not override.
                    MergeSrc::LabelCol {
                        label,
                        exclude,
                        col_end,
                    } => {
                        let chi = hi.min(*col_end);
                        if lo < chi {
                            let labels = self.gen.node_labels();
                            let (bitmask, want) = (labels.bitmask(), *label);
                            labels.inner().for_each_record_in(lo, chi, |node_id, rec| {
                                if graph_format::nodelabels::decode_labels(rec, bitmask)?
                                    .contains(&want)
                                    && !exclude.contains(&node_id)
                                {
                                    buf.push(node_id);
                                }
                                Ok(())
                            })?;
                        }
                    }
                }
            }
            buf.sort_unstable();
            buf.dedup();
            self.suppress_tombstoned_in_place(buf)?;
            if !buf.is_empty() {
                return Ok(Some(buf.as_slice()));
            }
        }
        Ok(None)
    }

    /// Candidate node ids for a chosen scan strategy, **materialised**. The eager
    /// counterpart of [`Engine::candidate_stream`], for the consumers that genuinely need
    /// the whole set at once: the `algo.*` subgraph view (it builds an index over it), the
    /// indexed count fast path (it wants only the length), a candidate set hoisted across
    /// many input rows, and the scan-seam tests.
    fn scan_candidates(&self, scan: &NodeScan) -> Result<Vec<u64>> {
        let mut s = self.candidate_stream(scan)?;
        // An eagerly-materialised source is already the answer.
        if let CandidateSrc::Owned(ids) = s.src {
            return Ok(ids);
        }
        let mut out = Vec::new();
        while let Some(batch) = self.next_candidates(&mut s)? {
            out.extend_from_slice(batch);
        }
        Ok(out)
    }

    /// Drop candidate dense ids a deletion has tombstoned — the delta's (Phase 2) *and* the
    /// core stack's (a flush that deleted a node): a deleted node must never bind as an
    /// anchor. The pure-core singleton with an empty delta returns the input untouched, so
    /// the read-only path pays nothing. In place, so a scan window is filtered without a
    /// second allocation.
    fn suppress_tombstoned_in_place(&self, ids: &mut Vec<u64>) -> Result<()> {
        let delta = self.gen.delta();
        let stack = self.gen.core_stack();
        if delta.is_empty() && stack.is_singleton() {
            return Ok(());
        }
        let (mut keep, mut i) = (0usize, 0usize);
        while i < ids.len() {
            let id = ids[i];
            i += 1;
            if delta.is_tombstoned(id) || (!stack.is_singleton() && stack.is_node_tombstoned(id)?) {
                continue;
            }
            ids[keep] = id;
            keep += 1;
        }
        ids.truncate(keep);
        Ok(())
    }

    /// The `(label, property)` a node range index is defined on, for the delta-born
    /// overlay (Phase 2d): a born node enters index `index` only if it carries
    /// `label` and its `property` value satisfies the seek. `None` if the name is
    /// not an open node range index.
    fn node_index_label_prop(&self, index: &str) -> Option<(&str, &str)> {
        self.gen
            .manifest()
            .range_indexes
            .iter()
            .find(|ri| ri.name == index && ri.entity == EntityKind::Node)
            .map(|ri| (ri.label_or_type.as_str(), ri.property.as_str()))
    }

    /// The label ids a chosen anchor scan already proves every candidate carries,
    /// so `node_ok` can skip re-decoding a label record for them (root cause 2). A
    /// `LabelScan` proves its label; a range-index scan proves the (node) label the
    /// index is defined on — a node only enters that index if it carries that label.
    /// Id seeks and full scans prove nothing.
    fn scan_guaranteed_labels(&self, scan: &NodeScan) -> Vec<u32> {
        match scan {
            // A label scan proves its label — unless a label mutation is present, in
            // which case a scanned candidate may have dropped it (Stage 5); force
            // `node_ok` to re-check by proving nothing.
            NodeScan::LabelScan { label_id } => {
                if self.gen.delta().has_label_overlay() {
                    Vec::new()
                } else {
                    vec![*label_id]
                }
            }
            NodeScan::RangeEq { index, .. } | NodeScan::RangeRange { index, .. } => self
                .gen
                .manifest()
                .range_indexes
                .iter()
                .find(|ri| &ri.name == index && ri.entity == EntityKind::Node)
                .and_then(|ri| self.gen.label_id(&ri.label_or_type))
                .into_iter()
                .collect(),
            NodeScan::IdSeek { .. } | NodeScan::AllNodes => Vec::new(),
            // The posting proves an *edge*, not a label; carry the anchor's lone
            // required label (lifted from the replaced LabelScan) so `node_ok`
            // still skips that label record, but re-checks anything else.
            NodeScan::RelTypeScan {
                guaranteed_label, ..
            } => guaranteed_label.iter().copied().collect(),
        }
    }

    /// Whether [`Self::node_ok`] would read a per-candidate label or property record
    /// for the anchor `start` — i.e. whether a parallel filter over many scanned
    /// candidates (Task 10) is worth the fan-out. Returns false when the filter is
    /// constant or already proven by the scan: no labels and no inline props, a single
    /// label atom the scan already guaranteed, or an unknown single label (which
    /// rejects every candidate with no record read at all).
    fn anchor_filter_reads(&self, start: &NodePat, guaranteed: &[u32]) -> bool {
        if !start.props.is_empty() {
            return true;
        }
        match &start.label_expr {
            None => false,
            Some(expr) => match expr.as_single_atom() {
                Some(atom) => match self.gen.label_id(atom) {
                    Some(lid) => !guaranteed.contains(&lid),
                    None => false,
                },
                None => true,
            },
        }
    }

    /// Whether node `id` satisfies a node pattern's labels and inline properties.
    /// Inline property values are evaluated against `binding` so a value bound
    /// earlier (e.g. by a `WITH`, or an earlier node/rel in the pattern) resolves,
    /// making `(b {id: x})` behave exactly like `(b) WHERE b.id = x`.
    ///
    /// `guaranteed` lists label ids the caller's anchor scan already proved for `id`
    /// (see [`scan_guaranteed_labels`]); those are skipped so the common
    /// label-scan/index-scan path never decodes a label record. Downstream
    /// (traversal) callers pass `&[]` — their candidates carry no such proof.
    fn node_ok(&self, id: u64, pat: &NodePat, scope: &Scope, guaranteed: &[u32]) -> Result<bool> {
        if let Some(expr) = &pat.label_expr {
            // Fast path, byte-for-byte as cheap as the pre-GQL single-label check: a
            // lone positive atom `(:Person)` the anchor scan already proved needs no
            // label record at all. This call is hot (one per candidate per hop), so
            // the common case must never touch the label record or, when guaranteed,
            // even the symbol table beyond one lookup.
            if let Some(atom) = expr.as_single_atom() {
                match self.gen.label_id(atom) {
                    Some(lid) if guaranteed.contains(&lid) => {}
                    Some(lid) => {
                        if !self.node_label_ids(id)?.contains(&lid) {
                            return Ok(false);
                        }
                    }
                    None => return Ok(false), // unknown label, single atom ⇒ no match
                }
            } else {
                // A boolean label expression (`&`/`|`/`!`, parens): decode the resident
                // labels once and evaluate as plain set membership. Anchor-proven
                // labels are folded into the present-predicate so a guaranteed atom
                // still counts without re-decoding. An atom naming an unknown label is
                // simply absent — so `!Unknown` holds and `Unknown` fails, the sound
                // set-logic answer.
                let have = self.node_label_ids(id)?;
                let ok = expr.eval(&|name| {
                    self.gen
                        .label_id(name)
                        .is_some_and(|lid| guaranteed.contains(&lid) || have.contains(&lid))
                });
                if !ok {
                    return Ok(false);
                }
            }
        }
        for (k, e) in &pat.props {
            let want = self.eval(e, scope, None)?;
            let got = self.node_prop(id, k)?;
            if got.loose_eq(&want) != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether edge `id` satisfies a relationship pattern's inline properties.
    /// Values are evaluated against `binding` (see [`node_ok`]).
    fn rel_ok(&self, id: u64, rel: &RelPat, binding: &HashMap<String, Val>) -> Result<bool> {
        for (k, e) in &rel.props {
            let want = self.eval(e, &Scope::Map(binding), None)?;
            let got = self.edge_prop(id, k)?;
            if got.loose_eq(&want) != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // ── Projection (RETURN / WITH) ──────────────────────────────────────────

    fn project(
        &self,
        table: Table,
        body: &ProjectionBody,
        distinct: bool,
        post_where: Option<&Expr>,
    ) -> Result<Table> {
        // Expand `*` to the in-scope variables, then the explicit items.
        let mut items: Vec<(Expr, String)> = Vec::new();
        if body.star {
            for c in &table.cols {
                items.push((Expr::Var(c.clone()), c.clone()));
            }
        }
        for it in &body.items {
            let name = it.alias.clone().unwrap_or_else(|| expr_name(&it.expr));
            items.push((it.expr.clone(), name));
        }
        let out_cols: Vec<String> = items.iter().map(|(_, n)| n.clone()).collect();

        let aggregating = items.iter().any(|(e, _)| contains_aggregate(e));
        let mut out_rows = if aggregating {
            self.project_aggregated(&table, &items)?
        } else {
            self.project_simple(&table, &items)?
        };

        if distinct {
            self.charge(out_rows.len() as u64)?; // DISTINCT `seen` set
            dedup_rows(&mut out_rows);
        }

        // WITH ... WHERE (HAVING-style) filters the projected rows.
        if let Some(w) = post_where {
            let mut kept = Vec::new();
            for r in out_rows {
                if truthy(&self.eval(w, &Scope::Row(&out_cols, &r), None)?) {
                    kept.push(r);
                }
            }
            out_rows = kept;
        }

        // ORDER BY then SKIP then LIMIT. ORDER BY keys may reference the projected
        // aliases and (for a non-aggregated projection) the input row vars; the
        // alias wins on a clash.
        if !body.order_by.is_empty() {
            // The `keyed` buffer clones every row plus its sort keys, so charge the
            // row count before building it (a large ORDER BY is otherwise uncharged).
            self.charge(out_rows.len() as u64)?;
            let with_input = !aggregating && out_rows.len() == table.rows.len();
            let mut keyed: Vec<(SortKey, Vec<Val>)> = Vec::with_capacity(out_rows.len());
            for (i, r) in out_rows.into_iter().enumerate() {
                let out_scope = Scope::Row(&out_cols, &r);
                let mut keys = Vec::with_capacity(body.order_by.len());
                for (e, dir) in &body.order_by {
                    let v = if with_input {
                        let in_scope = Scope::Row(&table.cols, &table.rows[i]);
                        let merged = Scope::Merge(&out_scope, &in_scope);
                        self.eval(e, &merged, None)?
                    } else {
                        self.eval(e, &out_scope, None)?
                    };
                    keys.push((v, *dir));
                }
                keyed.push((keys, r));
            }
            keyed.sort_by(|a, b| cmp_sort_keys(&a.0, &b.0));
            out_rows = keyed.into_iter().map(|(_, r)| r).collect();
        }

        if let Some(skip) = &body.skip {
            let n = self.eval_count(skip)?;
            out_rows = out_rows.into_iter().skip(n).collect();
        }
        if let Some(limit) = &body.limit {
            let n = self.eval_count(limit)?;
            out_rows.truncate(n);
        }

        Ok(Table {
            cols: out_cols,
            rows: out_rows,
        })
    }

    fn project_simple(&self, table: &Table, items: &[(Expr, String)]) -> Result<Vec<Vec<Val>>> {
        let mut out = Vec::with_capacity(table.rows.len());
        for row in &table.rows {
            let scope = Scope::Row(&table.cols, row);
            let mut r = Vec::with_capacity(items.len());
            for (e, _) in items {
                r.push(self.eval(e, &scope, None)?);
            }
            out.push(r);
        }
        Ok(out)
    }

    fn project_aggregated(&self, table: &Table, items: &[(Expr, String)]) -> Result<Vec<Vec<Val>>> {
        // Parallel fast path (Task 12): when every group key and aggregate argument
        // is `simple_readable` (Sync-evaluable: a var, literal, param, or `var.key`),
        // a fanout pool is configured, and the table is large enough, precompute the
        // per-row reads on the pool and reduce single-threaded. The grouping order,
        // budget charges and results are byte-for-byte identical to the sequential
        // body below — only the property reads move off-thread.
        if self.fanout_pool.is_some() && table.rows.len() >= AGG_PAR_MIN {
            if let Some((slots, plan)) = plan_par_aggregation(items) {
                if !slots.is_empty() {
                    return self.project_aggregated_par(table, items.len(), &slots, &plan);
                }
            }
        }
        // Grouping key = the values of the non-aggregating items, per row.
        let group_item: Vec<bool> = items.iter().map(|(e, _)| contains_aggregate(e)).collect();
        let mut groups: BTreeMap<GroupKey, Vec<usize>> = BTreeMap::new();
        for (ri, row) in table.rows.iter().enumerate() {
            let scope = Scope::Row(&table.cols, row);
            let mut key = Vec::new();
            for ((e, _), is_agg) in items.iter().zip(&group_item) {
                if !is_agg {
                    key.push(self.eval(e, &scope, None)?);
                }
            }
            // Charge each newly-created group: the `groups` map grows with the
            // distinct-key cardinality, which is otherwise uncharged (the per-row
            // index list is bounded by the already-charged input rows).
            let key = GroupKey(key);
            if !groups.contains_key(&key) {
                self.charge(1)?;
            }
            groups.entry(key).or_default().push(ri);
        }
        // An aggregation with no rows and no grouping keys still yields one row
        // (e.g. `RETURN count(*)` over an empty match → 0).
        if groups.is_empty() && !group_item.iter().any(|g| !g) {
            groups.insert(GroupKey(Vec::new()), Vec::new());
        }

        let mut out = Vec::with_capacity(groups.len());
        for (_, indices) in groups {
            // Representative row for grouping-key (non-agg) sub-expressions.
            let rep: &[Val] = indices.first().map(|&i| &table.rows[i][..]).unwrap_or(&[]);
            let rep_scope = Scope::Row(&table.cols, rep);
            let mut r = Vec::with_capacity(items.len());
            for (e, _) in items {
                if contains_aggregate(e) {
                    let mut aggs = Vec::new();
                    collect_aggregates(e, &mut aggs);
                    let mut vals = Vec::with_capacity(aggs.len());
                    for a in &aggs {
                        vals.push(self.compute_aggregate(a, table, &indices)?);
                    }
                    let cursor = AggCursor::new(vals);
                    r.push(self.eval(e, &rep_scope, Some(&cursor))?);
                } else {
                    r.push(self.eval(e, &rep_scope, None)?);
                }
            }
            out.push(r);
        }
        Ok(out)
    }

    /// Parallel counterpart to [`project_aggregated`](Self::project_aggregated) for the
    /// `simple_readable` shape (see [`plan_par_aggregation`]). The per-row group-key and
    /// aggregate-argument reads gather on the shared fanout pool (each touching only the
    /// Sync `gen`/`cache`); grouping, budget charges and the final reduction stay
    /// single-threaded in input order, so the output and the charge sequence are
    /// byte-for-byte identical to the sequential body.
    fn project_aggregated_par(
        &self,
        table: &Table,
        item_count: usize,
        slots: &[&Expr],
        plan: &[AggItem],
    ) -> Result<Vec<Vec<Val>>> {
        // Precompute, on the pool, the value of every slot expression for every row.
        // Capture only Sync state (never `&self`, which is `!Sync`).
        let gen = self.gen;
        let cache = self.cache;
        let params = &self.params;
        let cols = &table.cols;
        let cells: Vec<Vec<Val>> = par_gather(
            self.fanout_pool.as_deref(),
            &table.rows,
            AGG_PAR_MIN,
            |row| {
                slots
                    .iter()
                    .map(|e| eval_simple(gen, cache, params, cols, row, e))
                    .collect::<Result<Vec<_>>>()
            },
        )?;

        // Grouping: the key is the non-aggregate (Group) slots, in item order.
        let group_slots: Vec<usize> = plan
            .iter()
            .filter_map(|p| match p {
                AggItem::Group { slot } => Some(*slot),
                _ => None,
            })
            .collect();
        let has_group_item = !group_slots.is_empty();
        let mut groups: BTreeMap<GroupKey, Vec<usize>> = BTreeMap::new();
        for (ri, row) in cells.iter().enumerate() {
            let key = GroupKey(group_slots.iter().map(|&s| row[s].clone()).collect());
            if !groups.contains_key(&key) {
                self.charge(1)?; // charge each newly-created group (mirrors sequential)
            }
            groups.entry(key).or_default().push(ri);
        }
        // An aggregation with no rows and no grouping keys still yields one row.
        if groups.is_empty() && !has_group_item {
            groups.insert(GroupKey(Vec::new()), Vec::new());
        }

        let mut out = Vec::with_capacity(groups.len());
        for (_, indices) in groups {
            let mut r = Vec::with_capacity(item_count);
            for p in plan {
                match p {
                    // Grouping-key item: take the representative row's value (the
                    // first index — a Group item only exists when every group is
                    // non-empty, so `indices[0]` is always present).
                    AggItem::Group { slot } => r.push(cells[indices[0]][*slot].clone()),
                    AggItem::CountStar => r.push(Val::Int(indices.len() as i64)),
                    AggItem::Agg {
                        name,
                        distinct,
                        slot,
                    } => {
                        // Mirror `compute_aggregate`: drop nulls, charging each kept
                        // value in index order; for DISTINCT charge the dedup set, then
                        // reduce with the shared `reduce_agg`.
                        let mut vals = Vec::new();
                        for &i in &indices {
                            let v = cells[i][*slot].clone();
                            if !matches!(v, Val::Null) {
                                self.charge(1)?;
                                vals.push(v);
                            }
                        }
                        if *distinct {
                            self.charge(vals.len() as u64)?;
                            dedup_vals(&mut vals);
                        }
                        r.push(reduce_agg(name, vals)?);
                    }
                }
            }
            out.push(r);
        }
        Ok(out)
    }

    fn compute_aggregate(&self, agg: &Expr, table: &Table, indices: &[usize]) -> Result<Val> {
        let Expr::Function {
            name,
            distinct,
            args,
        } = agg
        else {
            bail!("internal: compute_aggregate on a non-function");
        };
        let lname = name.to_lowercase();

        // count(*) needs no argument.
        if lname == "count" {
            if let FuncArgs::Star = args {
                return Ok(Val::Int(indices.len() as i64));
            }
        }
        let args_slice = match args {
            FuncArgs::Args(a) => a.as_slice(),
            FuncArgs::Star => bail!("aggregate {name} expects an argument"),
        };

        // `percentileCont`/`percentileDisc` are two-arg aggregates: the first arg
        // is collected per row, the second is a constant percentile in [0, 1]
        // that FalkorDB reads once on the first invocation. Extract it before the
        // per-row loop (evaluated against a representative row of the group).
        let is_percentile = matches!(lname.as_str(), "percentilecont" | "percentiledisc");
        let percentile = if is_percentile {
            if args_slice.len() != 2 {
                bail!("{name}() expects exactly two arguments");
            }
            let pscope = match indices.first() {
                Some(&i) => Scope::Row(&table.cols, &table.rows[i]),
                None => Scope::Empty,
            };
            let p = match self.eval(&args_slice[1], &pscope, None)?.as_num() {
                Some(p) => p,
                None => bail!("{name}() percentile must be a number"),
            };
            if !(0.0..=1.0).contains(&p) {
                bail!("Percentile value must be in the range 0 to 1, got {p}");
            }
            Some(p)
        } else {
            if args_slice.len() != 1 {
                bail!("aggregate {name} expects exactly one argument");
            }
            None
        };
        let arg = &args_slice[0];

        // Evaluate the argument over the group's rows, dropping nulls. The buffer
        // is materialised for every aggregate (not just collect), so it charges
        // the intermediate budget.
        let mut vals = Vec::new();
        for &i in indices {
            let scope = Scope::Row(&table.cols, &table.rows[i]);
            let v = self.eval(arg, &scope, None)?;
            if !matches!(v, Val::Null) {
                self.charge(1)?;
                vals.push(v);
            }
        }
        if *distinct {
            self.charge(vals.len() as u64)?; // DISTINCT-aggregate `seen` set
            dedup_vals(&mut vals);
        }

        // `percentile*` carry the constant percentile; every other aggregate's
        // value→result reduction is shared with the parallel path via `reduce_agg`.
        match lname.as_str() {
            "percentilecont" => percentile_cont(&vals, percentile.unwrap()),
            "percentiledisc" => percentile_disc(&vals, percentile.unwrap()),
            other => reduce_agg(other, vals),
        }
    }

    /// Evaluate a constant `SKIP`/`LIMIT` expression to a non-negative count.
    fn eval_count(&self, e: &Expr) -> Result<usize> {
        match self.eval(e, &Scope::Empty, None)? {
            Val::Int(n) if n >= 0 => Ok(n as usize),
            other => bail!("SKIP/LIMIT must be a non-negative integer, got {other:?}"),
        }
    }

    // ── Expression evaluation ───────────────────────────────────────────────

    fn eval(&self, expr: &Expr, scope: &Scope, aggs: Option<&AggCursor>) -> Result<Val> {
        match expr {
            Expr::Literal(v) => Ok(Val::from_value(v.clone())),
            Expr::Param(name) => self
                .params
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("parameter ${name} was not supplied")),
            Expr::Var(name) => scope
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("variable '{name}' is not in scope")),
            Expr::Property(base, key) => {
                let b = self.eval(base, scope, aggs)?;
                self.property(&b, key)
            }
            Expr::Index(base, idx) => {
                let b = self.eval(base, scope, aggs)?;
                let i = self.eval(idx, scope, aggs)?;
                self.index(&b, &i)
            }
            Expr::Slice { base, from, to } => {
                let b = self.eval(base, scope, aggs)?;
                // Absent bounds default to 0 / INT32_MAX, mirroring FalkorDB's
                // slice AST construction; an explicit NULL bound yields NULL.
                let f = match from {
                    Some(e) => self.eval(e, scope, aggs)?,
                    None => Val::Int(0),
                };
                let t = match to {
                    Some(e) => self.eval(e, scope, aggs)?,
                    None => Val::Int(i32::MAX as i64),
                };
                self.slice(&b, &f, &t)
            }
            Expr::HasLabels(base, labels) => {
                let b = self.eval(base, scope, aggs)?;
                self.has_labels(&b, labels)
            }
            Expr::Neg(e) => match self.eval(e, scope, aggs)? {
                // `-i64::MIN` has no `i64` value (the two's-complement range is
                // asymmetric), so negation is checked like every other integer op —
                // it wrapped back to `i64::MIN` in release before this.
                Val::Int(i) => match i.checked_neg() {
                    Some(v) => Ok(Val::Int(v)),
                    None => bail!(ArithmeticOverflow::unary("-", i)),
                },
                Val::Float(f) => Ok(Val::Float(-f)),
                Val::Null => Ok(Val::Null),
                other => bail!("cannot negate {}", other.to_display()),
            },
            Expr::Not(e) => Ok(match three_valued(&self.eval(e, scope, aggs)?) {
                Some(b) => Val::Bool(!b),
                None => Val::Null,
            }),
            Expr::And(parts) => self.fold_bool(parts, scope, aggs, BoolOp::And),
            Expr::Or(parts) => self.fold_bool(parts, scope, aggs, BoolOp::Or),
            Expr::Xor(parts) => self.fold_bool(parts, scope, aggs, BoolOp::Xor),
            Expr::Arith(op, l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                let v = arith(*op, a, b)?;
                // List concatenation is the only arithmetic that materialises a
                // collection; charging every temp defeats geometric growth like
                // `reduce(acc = [0], x IN range(1, 60) | acc + acc)`.
                if let Val::List(xs) = &v {
                    self.charge(xs.len() as u64)?;
                }
                Ok(v)
            }
            Expr::Compare(op, l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                Ok(compare(*op, &a, &b))
            }
            Expr::StringOp(op, l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                self.string_op(*op, &a, &b)
            }
            Expr::In(l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                Ok(in_list(&a, &b))
            }
            Expr::IsNull(e, negated) => {
                let v = self.eval(e, scope, aggs)?;
                let is_null = matches!(v, Val::Null);
                Ok(Val::Bool(if *negated { !is_null } else { is_null }))
            }
            Expr::Case {
                subject,
                whens,
                els,
            } => self.eval_case(subject, whens, els, scope, aggs),
            Expr::Function {
                name,
                distinct,
                args,
            } => {
                if is_aggregate(name) {
                    return match aggs {
                        Some(cursor) => Ok(cursor.next()),
                        None => bail!("aggregation '{name}' is not allowed here"),
                    };
                }
                let arg_vals = match args {
                    FuncArgs::Star => bail!("'{name}(*)' is only valid for count"),
                    FuncArgs::Args(a) => a
                        .iter()
                        .map(|e| self.eval(e, scope, aggs))
                        .collect::<Result<Vec<_>>>()?,
                };
                self.call_function(name, *distinct, arg_vals)
            }
            Expr::List(items) => Ok(Val::List(
                items
                    .iter()
                    .map(|e| self.eval(e, scope, aggs))
                    .collect::<Result<_>>()?,
            )),
            Expr::Map(entries) => {
                let mut m = Vec::with_capacity(entries.len());
                for (k, e) in entries {
                    m.push((k.clone(), self.eval(e, scope, aggs)?));
                }
                Ok(Val::Map(m))
            }
            Expr::MapProjection { var, items } => self.eval_map_projection(var, items, scope, aggs),
            Expr::ListPredicate {
                quant,
                var,
                list,
                predicate,
            } => self.eval_list_predicate(*quant, var, list, predicate.as_deref(), scope, aggs),
            Expr::ListComprehension {
                var,
                list,
                predicate,
                projection,
            } => self.eval_list_comprehension(
                var,
                list,
                predicate.as_deref(),
                projection.as_deref(),
                scope,
                aggs,
            ),
            Expr::PatternComprehension {
                pattern,
                predicate,
                projection,
            } => self.eval_pattern_comprehension(
                pattern,
                predicate.as_deref(),
                projection,
                scope,
                aggs,
            ),
            Expr::Reduce {
                acc_var,
                acc_init,
                var,
                list,
                body,
            } => self.eval_reduce(acc_var, acc_init, var, list, body, scope, aggs),
            Expr::PatternPredicate(pattern) => {
                // True iff the pattern, seeded by the current bindings, has ≥1
                // match (FalkorDB `op_semi_apply`; the negated form `NOT (…)` is
                // anti-semi-apply via the surrounding `Expr::Not`). No early-exit:
                // all matches are collected, then emptiness is tested.
                let seed = scope.to_binding();
                let mut bindings = Vec::new();
                self.match_single_pattern(pattern, &seed, None, &mut bindings, None)?;
                Ok(Val::Bool(!bindings.is_empty()))
            }
            Expr::Exists {
                patterns,
                predicate,
            } => {
                // `match_patterns` seeds from the outer bindings, chains the
                // comma-separated patterns, and applies the inner WHERE once every
                // pattern is bound — exactly the semi-apply existence test.
                let seed = scope.to_binding();
                let mut bindings = Vec::new();
                self.match_patterns(patterns, 0, seed, predicate.as_deref(), &mut bindings, None)?;
                Ok(Val::Bool(!bindings.is_empty()))
            }
            Expr::ShortestPath(pattern) => self.eval_shortest_path(pattern, scope),
        }
    }

    /// Evaluate `shortestPath((a)-[*]->(b))` against the current scope: a BFS over
    /// the traversal adjacency from the bound source to the bound destination,
    /// returning the first (hence shortest) connecting [`Val::Path`], or `Val::Null`
    /// when none exists. Mirrors FalkorDB's validation of the wrapped pattern.
    fn eval_shortest_path(&self, pattern: &Pattern, scope: &Scope) -> Result<Val> {
        // The inner pattern must be a single variable-length relationship with no
        // property filter, between two endpoints already bound to nodes.
        if pattern.rels.len() != 1 {
            bail!("shortestPath requires a path containing a single relationship");
        }
        let (rel, end) = &pattern.rels[0];
        if !rel.props.is_empty() {
            bail!("filters on relationships in shortestPath are not supported");
        }
        let (min, max) = match &rel.var_length {
            Some(vl) => varlen_bounds(vl),
            None => (1, 1),
        };
        if min > 1 {
            bail!("shortestPath does not support a minimal length different from 0 or 1");
        }
        let bound_node = |var: Option<&str>| -> Result<u64> {
            match var.and_then(|v| scope.get(v)) {
                Some(Val::Node(id)) => Ok(id),
                _ => bail!("A shortestPath requires bound nodes"),
            }
        };
        let src = bound_node(pattern.start.var.as_deref())?;
        let dst = bound_node(end.var.as_deref())?;
        // FalkorDB orients the returned path from the relationship arrow's tail to
        // its head. The shared core walks the syntactic start→end (using the
        // pattern's direction); for an incoming pattern `(b)<-[*]-(a)` the arrow tail
        // is the end node, so the result is reversed into arrow order. (Undirected
        // keeps start→end order.)
        let reverse = matches!(rel.dir, Direction::Incoming);

        // Delegate to the shared selector core: `shortestPath()` is exactly
        // `ANY SHORTEST` between two bound nodes — one loopless shortest path, or none.
        let Some(hops) = self
            .select_paths(src, dst, rel, (min, max), PathSelector::AnyShortest)?
            .into_iter()
            .next()
        else {
            return Ok(Val::Null);
        };
        let path = make_path(src, &hops);
        if reverse {
            if let Val::Path {
                mut nodes,
                mut rels,
            } = path
            {
                nodes.reverse();
                rels.reverse();
                return Ok(Val::Path { nodes, rels });
            }
        }
        Ok(path)
    }

    fn fold_bool(
        &self,
        parts: &[Expr],
        scope: &Scope,
        aggs: Option<&AggCursor>,
        op: BoolOp,
    ) -> Result<Val> {
        let mut saw_null = false;
        let mut acc = matches!(op, BoolOp::And); // identity: AND=true, OR/XOR=false
        for p in parts {
            match three_valued(&self.eval(p, scope, aggs)?) {
                Some(b) => match op {
                    BoolOp::And => {
                        if !b {
                            return Ok(Val::Bool(false));
                        }
                    }
                    BoolOp::Or => {
                        if b {
                            return Ok(Val::Bool(true));
                        }
                    }
                    BoolOp::Xor => acc ^= b,
                },
                None => saw_null = true,
            }
        }
        if saw_null {
            return Ok(Val::Null);
        }
        Ok(Val::Bool(acc))
    }

    // Point coordinate read (FalkorDB `Point_GetCoordinate`): only
    // `latitude`/`longitude` resolve; any other key yields NULL. Temporal component
    // access (FalkorDB `entity_funcs.c` → `*_getComponent`): an unknown component is
    // an *error* (unlike Point/Map, which yield NULL). The body lives in the Sync
    // free fn [`property_val`] so the parallel aggregation precompute can share it.
    fn property(&self, base: &Val, key: &str) -> Result<Val> {
        property_val(self.gen, self.cache, base, key)
    }

    fn index(&self, base: &Val, idx: &Val) -> Result<Val> {
        match (base, idx) {
            (Val::Null, _) | (_, Val::Null) => Ok(Val::Null),
            (Val::List(xs), Val::Int(i)) => Ok(list_index(xs.len(), *i)
                .map(|n| xs[n].clone())
                .unwrap_or(Val::Null)),
            (Val::Vector(xs), Val::Int(i)) => Ok(list_index(xs.len(), *i)
                .map(|n| Val::Float(xs[n] as f64))
                .unwrap_or(Val::Null)),
            (Val::Map(m), Val::Str(k)) => Ok(m
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
                .unwrap_or(Val::Null)),
            _ => bail!(
                "cannot index {} with {}",
                base.to_display(),
                idx.to_display()
            ),
        }
    }

    /// `base[from..to]` slice. Mirrors FalkorDB `AR_SLICE`: any NULL operand
    /// yields NULL; a negative bound counts from the end (clamped into range); a
    /// non-positive width yields an empty result. Extends FalkorDB (arrays only)
    /// to strings, slicing by Unicode scalar value.
    fn slice(&self, base: &Val, from: &Val, to: &Val) -> Result<Val> {
        if matches!(base, Val::Null) || matches!(from, Val::Null) || matches!(to, Val::Null) {
            return Ok(Val::Null);
        }
        let start = num_i64(Some(from))?;
        let end = num_i64(Some(to))?;
        match base {
            Val::List(xs) => Ok(Val::List(slice_range(xs, start, end).to_vec())),
            Val::Vector(xs) => Ok(Val::List(
                slice_range(xs, start, end)
                    .iter()
                    .map(|f| Val::Float(*f as f64))
                    .collect(),
            )),
            Val::Str(s) => {
                let chars: Vec<char> = s.chars().collect();
                Ok(Val::Str(slice_range(&chars, start, end).iter().collect()))
            }
            other => bail!("cannot slice {}", other.to_display()),
        }
    }

    fn has_labels(&self, base: &Val, labels: &[String]) -> Result<Val> {
        match base {
            Val::Null => Ok(Val::Null),
            Val::Node(id) => {
                let have = self.node_label_ids(*id)?;
                for l in labels {
                    match self.gen.label_id(l) {
                        Some(lid) if have.contains(&lid) => {}
                        _ => return Ok(Val::Bool(false)),
                    }
                }
                Ok(Val::Bool(true))
            }
            other => bail!("cannot test labels on {}", other.to_display()),
        }
    }

    fn eval_case(
        &self,
        subject: &Option<Box<Expr>>,
        whens: &[(Expr, Expr)],
        els: &Option<Box<Expr>>,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        match subject {
            // Simple form: CASE x WHEN v THEN ... — compare x to each v.
            Some(subj) => {
                let s = self.eval(subj, scope, aggs)?;
                for (v, then) in whens {
                    let cand = self.eval(v, scope, aggs)?;
                    if s.loose_eq(&cand) == Some(true) {
                        return self.eval(then, scope, aggs);
                    }
                }
            }
            // Searched form: CASE WHEN cond THEN ... — first true condition.
            None => {
                for (cond, then) in whens {
                    if truthy(&self.eval(cond, scope, aggs)?) {
                        return self.eval(then, scope, aggs);
                    }
                }
            }
        }
        match els {
            Some(e) => self.eval(e, scope, aggs),
            None => Ok(Val::Null),
        }
    }

    fn eval_map_projection(
        &self,
        var: &str,
        items: &[MapProjItem],
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let base = scope
            .get(var)
            .ok_or_else(|| anyhow::anyhow!("variable '{var}' is not in scope"))?;
        let mut out: Vec<(String, Val)> = Vec::new();
        for item in items {
            match item {
                MapProjItem::AllProps => {
                    for (k, v) in self.all_properties(&base)? {
                        out.push((k, v));
                    }
                }
                MapProjItem::Property(p) => out.push((p.clone(), self.property(&base, p)?)),
                MapProjItem::Literal(k, e) => out.push((k.clone(), self.eval(e, scope, aggs)?)),
            }
        }
        Ok(Val::Map(out))
    }

    fn all_properties(&self, base: &Val) -> Result<Vec<(String, Val)>> {
        match base {
            Val::Node(id) => {
                // Core-stack row (segment or base) in name space, then the delta overlay.
                let mut out = self.core_named_props(*id)?;
                self.overlay_node_props(*id, &mut out);
                let labels: Vec<String> = self
                    .node_label_ids(*id)?
                    .into_iter()
                    .filter_map(|l| self.gen.label_name(l).map(|s| s.to_string()))
                    .collect();
                self.suppress_indexed_vectors_named(&labels, &mut out);
                Ok(out)
            }
            // `core_named_edge_props` already folds the segment row and the delta patches.
            Val::Rel { id, .. } => self.core_named_edge_props(*id),
            Val::Map(m) => Ok(m.clone()),
            Val::Null => Ok(Vec::new()),
            other => bail!("type {} has no properties", other.to_display()),
        }
    }

    fn eval_list_predicate(
        &self,
        quant: Quantifier,
        var: &str,
        list: &Expr,
        predicate: Option<&Expr>,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let items = match self.eval(list, scope, aggs)? {
            Val::List(xs) => xs,
            Val::Null => return Ok(Val::Null),
            other => bail!("a list predicate needs a list, got {}", other.to_display()),
        };
        let mut true_count = 0usize;
        let mut any_false = false;
        let mut saw_null = false;
        for item in &items {
            let inner = Scope::With(scope, var, item);
            let v = match predicate {
                Some(p) => self.eval(p, &inner, aggs)?,
                None => item.clone(),
            };
            match three_valued(&v) {
                Some(true) => true_count += 1,
                Some(false) => any_false = true,
                None => saw_null = true,
            }
        }
        Ok(match quant {
            // any: a definite true wins; else null if any null; else false.
            Quantifier::Any => {
                if true_count > 0 {
                    Val::Bool(true)
                } else if saw_null {
                    Val::Null
                } else {
                    Val::Bool(false)
                }
            }
            // all: a definite false wins; else null if any null; else true.
            Quantifier::All => {
                if any_false {
                    Val::Bool(false)
                } else if saw_null {
                    Val::Null
                } else {
                    Val::Bool(true)
                }
            }
            // none: a definite true → false; else null if any null; else true.
            Quantifier::None => {
                if true_count > 0 {
                    Val::Bool(false)
                } else if saw_null {
                    Val::Null
                } else {
                    Val::Bool(true)
                }
            }
            Quantifier::Single => Val::Bool(true_count == 1),
        })
    }

    /// Evaluate `[var IN list WHERE predicate | projection]`. Mirrors
    /// [`Self::eval_list_predicate`] element binding: iterate the source list with
    /// `var` layered onto the scope, keep elements whose predicate is definitely
    /// true (a NULL predicate excludes, like FalkorDB's three-valued filter), and
    /// project each survivor (defaulting to the bound element). A NULL source list
    /// yields NULL.
    fn eval_list_comprehension(
        &self,
        var: &str,
        list: &Expr,
        predicate: Option<&Expr>,
        projection: Option<&Expr>,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let items = match self.eval(list, scope, aggs)? {
            Val::List(xs) => xs,
            Val::Null => return Ok(Val::Null),
            other => bail!(
                "a list comprehension needs a list, got {}",
                other.to_display()
            ),
        };
        let mut out = Vec::new();
        for item in &items {
            let inner = Scope::With(scope, var, item);
            if let Some(p) = predicate {
                if !truthy(&self.eval(p, &inner, aggs)?) {
                    continue;
                }
            }
            self.charge(1)?;
            out.push(match projection {
                Some(e) => self.eval(e, &inner, aggs)?,
                None => item.clone(),
            });
        }
        Ok(Val::List(out))
    }

    /// Evaluate `reduce(acc = init, var IN list | body)`. Mirrors FalkorDB
    /// `AR_REDUCE`: a NULL list yields NULL; otherwise fold `body` over the list,
    /// threading the accumulator (seeded from `init`) and binding `var` to each
    /// element. Both bindings shadow the surrounding scope only inside `body`.
    #[allow(clippy::too_many_arguments)]
    fn eval_reduce(
        &self,
        acc_var: &str,
        acc_init: &Expr,
        var: &str,
        list: &Expr,
        body: &Expr,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let items = match self.eval(list, scope, aggs)? {
            Val::List(xs) => xs,
            Val::Null => return Ok(Val::Null),
            other => bail!("reduce() needs a list, got {}", other.to_display()),
        };
        let mut acc = self.eval(acc_init, scope, aggs)?;
        for item in &items {
            let acc_scope = Scope::With(scope, acc_var, &acc);
            let inner = Scope::With(&acc_scope, var, item);
            acc = self.eval(body, &inner, aggs)?;
        }
        Ok(acc)
    }

    /// Evaluate `[pattern WHERE predicate | projection]`. The pattern is matched
    /// against the surrounding scope (its already-bound nodes seed the traversal),
    /// the optional `WHERE` filters matches, and `projection` is collected per
    /// match in match order. New pattern variables stay local to each match and do
    /// not leak to the outer row; an empty match set yields `[]`. This is the
    /// observable equivalent of FalkorDB's correlated collect sub-plan.
    fn eval_pattern_comprehension(
        &self,
        pattern: &Pattern,
        predicate: Option<&Expr>,
        projection: &Expr,
        scope: &Scope,
        aggs: Option<&AggCursor>,
    ) -> Result<Val> {
        let seed = scope.to_binding();
        let mut bindings = Vec::new();
        self.match_single_pattern(pattern, &seed, predicate, &mut bindings, None)?;
        let mut out = Vec::with_capacity(bindings.len());
        for b in bindings {
            out.push(self.eval(projection, &Scope::Map(&b), aggs)?);
        }
        Ok(Val::List(out))
    }

    fn call_function(&self, name: &str, _distinct: bool, args: Vec<Val>) -> Result<Val> {
        let n = name.to_lowercase();
        let a0 = |i: usize| args.get(i).cloned().unwrap_or(Val::Null);
        Ok(match n.as_str() {
            "coalesce" => args
                .into_iter()
                .find(|v| !matches!(v, Val::Null))
                .unwrap_or(Val::Null),
            // Pure, deterministic scalar functions whose result depends only on
            // their argument values are single-sourced in the `slater-scalar`
            // crate (shared with the offline builder). Convert the runtime args to
            // on-disk `Value`s and delegate; a runtime-only argument (node / map /
            // path / temporal) can never satisfy these string/numeric/conversion
            // functions and yields NULL — exactly the old `str_fn`/`num_fn`
            // behaviour for a non-scalar argument.
            "tolower" | "lower" | "toupper" | "upper" | "trim" | "ltrim" | "rtrim"
            | "tointeger" | "tointegerornull" | "tofloat" | "tofloatornull" | "toboolean"
            | "tobooleanornull" | "abs" | "ceil" | "floor" | "round" | "sqrt" | "log" | "log10"
            | "exp" | "e" | "pi" | "pow" | "sign" | "sin" | "cos" | "tan" | "cot" | "asin"
            | "acos" | "atan" | "atan2" | "degrees" | "radians" | "haversin" => {
                match try_all_values(&args) {
                    Some(vs) => Val::from_value(
                        slater_scalar::eval_pure(&n, &vs)?
                            .expect("listed name is handled by slater-scalar"),
                    ),
                    None => Val::Null,
                }
            }
            "reverse" => match a0(0) {
                Val::Str(s) => Val::Str(s.chars().rev().collect()),
                Val::List(mut xs) => {
                    xs.reverse();
                    Val::List(xs)
                }
                Val::Null => Val::Null,
                other => bail!(
                    "reverse() needs a string or list, got {}",
                    other.to_display()
                ),
            },
            // `length(path)` is the relationship count (FalkorDB `AR_PATH_LENGTH`);
            // `size`/`length` over a collection/string is the element/char count.
            "size" | "length" => match a0(0) {
                Val::List(xs) => Val::Int(xs.len() as i64),
                Val::Vector(xs) => Val::Int(xs.len() as i64),
                Val::Str(s) => Val::Int(s.chars().count() as i64),
                Val::Map(m) => Val::Int(m.len() as i64),
                Val::Path { rels, .. } => Val::Int(rels.len() as i64),
                Val::Null => Val::Null,
                other => bail!(
                    "{n}() needs a collection or string, got {}",
                    other.to_display()
                ),
            },
            // nodes(path)/relationships(path): the path's node / relationship
            // sequence as a list (FalkorDB `AR_NODES`/`AR_RELATIONSHIPS`).
            "nodes" => match a0(0) {
                Val::Path { nodes, .. } => Val::List(nodes.into_iter().map(Val::Node).collect()),
                Val::Null => Val::Null,
                other => bail!("nodes() needs a path, got {}", other.to_display()),
            },
            "relationships" => match a0(0) {
                Val::Path { rels, .. } => Val::List(rels),
                Val::Null => Val::Null,
                other => bail!("relationships() needs a path, got {}", other.to_display()),
            },
            "head" => match a0(0) {
                Val::List(xs) => xs.into_iter().next().unwrap_or(Val::Null),
                Val::Null => Val::Null,
                other => bail!("head() needs a list, got {}", other.to_display()),
            },
            "last" => match a0(0) {
                Val::List(xs) => xs.into_iter().last().unwrap_or(Val::Null),
                Val::Null => Val::Null,
                other => bail!("last() needs a list, got {}", other.to_display()),
            },
            // `tostring`/`toString` and the `*OrNull` variant. FalkorDB's plain
            // `toString` errors on a non-convertible type while `toStringOrNull`
            // yields NULL; our renderer never errors, so the two coincide here.
            // `toString`/`toStringOrNull`: the renderer never errors, so the two
            // coincide. Kept here (not delegated) because a runtime-only argument
            // (node / temporal / point) must render via `Val::to_display`, not NULL.
            "tostring" | "tostringornull" => match a0(0) {
                Val::Null => Val::Null,
                v => match val_to_value(&v) {
                    Some(vv) => Val::from_value(
                        slater_scalar::eval_pure("tostring", &[vv])?.expect("handled"),
                    ),
                    None => Val::Str(v.to_display()),
                },
            },
            // left/right: the n leftmost/rightmost characters. n must be >= 0;
            // when the string is shorter than n the whole string is returned.
            "left" => self.left_right(&args, true)?,
            "right" => self.left_right(&args, false)?,
            // typeOf — FalkorDB's value-type name (SIType_ToString).
            "typeof" => Val::Str(type_name(&a0(0)).to_string()),
            // isEmpty — empty string / list / map. NULL argument → NULL.
            "isempty" => match a0(0) {
                Val::Str(s) => Val::Bool(s.is_empty()),
                Val::List(xs) => Val::Bool(xs.is_empty()),
                Val::Map(m) => Val::Bool(m.is_empty()),
                Val::Null => Val::Null,
                other => bail!(
                    "isEmpty() needs a string, list or map, got {}",
                    other.to_display()
                ),
            },
            "exists" => Val::Bool(!matches!(a0(0), Val::Null)),
            "substring" => self.substring(&args)?,
            "split" => match (a0(0), a0(1)) {
                (Val::Str(s), Val::Str(sep)) => {
                    Val::List(s.split(&sep).map(|p| Val::Str(p.to_string())).collect())
                }
                (Val::Null, _) | (_, Val::Null) => Val::Null,
                _ => bail!("split() needs two strings"),
            },
            "replace" => match (a0(0), a0(1), a0(2)) {
                (Val::Str(s), Val::Str(a), Val::Str(b)) => Val::Str(s.replace(&a, &b)),
                (Val::Null, _, _) => Val::Null,
                _ => bail!("replace() needs three strings"),
            },
            "string.join" => string_join(&args)?,
            "string.matchregex" => self.match_regex(&a0(0), &a0(1))?,
            "string.replaceregex" => {
                let repl = if args.len() >= 3 {
                    a0(2)
                } else {
                    Val::Str(String::new())
                };
                self.replace_regex(&a0(0), &a0(1), &repl)?
            }
            "range" => self.range_fn(&args)?,
            "keys" => Val::List(
                self.all_properties(&a0(0))?
                    .into_iter()
                    .map(|(k, _)| Val::Str(k))
                    .collect(),
            ),
            "properties" => Val::Map(self.all_properties(&a0(0))?),
            "labels" => match a0(0) {
                Val::Node(id) => Val::List(
                    self.node_label_ids(id)?
                        .into_iter()
                        .filter_map(|l| self.gen.label_name(l).map(|s| Val::Str(s.to_string())))
                        .collect(),
                ),
                Val::Null => Val::Null,
                other => bail!("labels() needs a node, got {}", other.to_display()),
            },
            "id" => match a0(0) {
                Val::Node(id) | Val::Rel { id, .. } => Val::Int(id as i64),
                Val::Null => Val::Null,
                other => bail!(
                    "id() needs a node or relationship, got {}",
                    other.to_display()
                ),
            },
            "type" => match a0(0) {
                Val::Rel { reltype, .. } => self
                    .gen
                    .reltype_name(reltype)
                    .map(|s| Val::Str(s.to_string()))
                    .unwrap_or(Val::Null),
                Val::Null => Val::Null,
                other => bail!("type() needs a relationship, got {}", other.to_display()),
            },
            // startNode/endNode return the stored-direction endpoints carried on
            // the relationship value (src→dst), so no re-traversal is needed. Match
            // FalkorDB: NULL argument → NULL; a non-relationship is an error.
            "startnode" => match a0(0) {
                Val::Rel { start, .. } => Val::Node(start),
                Val::Null => Val::Null,
                other => bail!(
                    "startNode() needs a relationship, got {}",
                    other.to_display()
                ),
            },
            "endnode" => match a0(0) {
                Val::Rel { end, .. } => Val::Node(end),
                Val::Null => Val::Null,
                other => bail!("endNode() needs a relationship, got {}", other.to_display()),
            },
            // Build a first-class vector from a list of numbers — the inlined
            // `vecf32([...])` form drivers send (and the query-vector argument of
            // db.idx.vector.queryNodes). Round-trips a `Vector` unchanged.
            "vecf32" => match a0(0) {
                Val::Vector(v) => Val::Vector(v),
                Val::List(xs) => Val::Vector(
                    xs.iter()
                        .enumerate()
                        .map(|(i, x)| embed_component(i, x, "vecf32()"))
                        .collect::<Result<_>>()?,
                ),
                Val::Null => Val::Null,
                other => bail!(
                    "vecf32() needs a list of numbers, got {}",
                    other.to_display()
                ),
            },
            // Cosine similarity of two vectors, in [-1, 1] (higher = more similar).
            // Complements the KNN `score`, which is the distance `1 - similarity`
            // (D26). Accepts vectors or numeric lists.
            "similarity" | "vec.cosinesimilarity" => match (as_vector(&a0(0))?, as_vector(&a0(1))?)
            {
                (Some(a), Some(b)) if a.len() == b.len() => {
                    Val::Float(vector::cosine_similarity(&a, &b))
                }
                (Some(a), Some(b)) => bail!(
                    "similarity() needs equal-length vectors ({} vs {})",
                    a.len(),
                    b.len()
                ),
                _ => Val::Null,
            },
            // Euclidean / cosine *distance* between two vectors (FalkorDB
            // vector_funcs.c). NULL operand → NULL; a dimension mismatch or a
            // non-vector operand is an error, matching FalkorDB's "Vector
            // dimension mismatch" / type-mismatch behaviour. Accept vectors or
            // numeric lists, like `similarity`.
            "vec.euclideandistance" | "vec.cosinedistance" => {
                let (x, y) = (a0(0), a0(1));
                if matches!(x, Val::Null) || matches!(y, Val::Null) {
                    Val::Null
                } else {
                    let a = as_vector(&x)?.ok_or_else(|| {
                        anyhow::anyhow!("{n}() needs vectors, got {}", x.to_display())
                    })?;
                    let b = as_vector(&y)?.ok_or_else(|| {
                        anyhow::anyhow!("{n}() needs vectors, got {}", y.to_display())
                    })?;
                    if a.len() != b.len() {
                        bail!("Vector dimension mismatch, {} != {}", a.len(), b.len());
                    }
                    if n == "vec.euclideandistance" {
                        Val::Float(vector::euclidean_distance(&a, &b))
                    } else {
                        Val::Float(vector::cosine_distance(&a, &b))
                    }
                }
            }
            // ── Point / geo functions (FalkorDB point_funcs.c) ──────────────
            // point({latitude, longitude}): a WGS-84 geographic point. FalkorDB
            // accepts ONLY the lat/lon map form (no Cartesian x/y, no SRID arg);
            // the map must have exactly those two numeric keys, latitude in
            // [-90,90] and longitude in [-180,180]. NULL map → NULL.
            "point" => match a0(0) {
                Val::Null => Val::Null,
                Val::Map(m) => {
                    if m.len() != 2 {
                        bail!("A point map should have 2 elements, latitude and longitude");
                    }
                    let get = |k: &str| m.iter().find(|(key, _)| key == k).map(|(_, v)| v);
                    let lat = get("latitude").ok_or_else(|| {
                        anyhow::anyhow!("Did not find 'latitude' value in point map")
                    })?;
                    let lon = get("longitude").ok_or_else(|| {
                        anyhow::anyhow!("Did not find 'longitude' value in point map")
                    })?;
                    let (latitude, longitude) = match (lat.as_num(), lon.as_num()) {
                        (Some(a), Some(b)) => (a, b),
                        _ => bail!(
                            "'latitude' and 'longitude' values in point map were not both valid numerics"
                        ),
                    };
                    if !(-90.0..=90.0).contains(&latitude) {
                        bail!("latitude should be within the -90 to 90 range");
                    }
                    if !(-180.0..=180.0).contains(&longitude) {
                        bail!("longitude should be within the -180 to 180 range");
                    }
                    Val::Point {
                        latitude,
                        longitude,
                    }
                }
                other => bail!("point() expects a map, got {}", other.to_display()),
            },
            // distance(p1, p2): great-circle distance in metres (haversine over the
            // WGS-84 sphere, FalkorDB `AR_DISTANCE`). NULL operand → NULL.
            "distance" => match (a0(0), a0(1)) {
                (Val::Null, _) | (_, Val::Null) => Val::Null,
                (
                    Val::Point {
                        latitude: la,
                        longitude: lo_a,
                    },
                    Val::Point {
                        latitude: lb,
                        longitude: lo_b,
                    },
                ) => Val::Float(haversine_metres(la, lo_a, lb, lo_b)),
                (a, b) => bail!(
                    "distance() needs two points, got {} and {}",
                    a.to_display(),
                    b.to_display()
                ),
            },
            // ── Temporal constructors (FalkorDB time_funcs.c) ───────────────
            // Each takes a string (ISO-8601) or a component map; a bad string →
            // NULL, NULL arg → NULL. A no-arg call would be the wall-clock `now`,
            // which is out of scope (non-deterministic) → NULL. `timestamp()`
            // shipped in Phase 1 as an Int and is unchanged.
            "date" => match a0(0) {
                Val::Null => Val::Null,
                Val::Str(s) => temporal::date_from_string(&s)
                    .map(Val::Date)
                    .unwrap_or(Val::Null),
                Val::Map(m) => build_date(&m)?,
                other => bail!("date() expects a string or map, got {}", other.to_display()),
            },
            "localtime" => match a0(0) {
                Val::Null => Val::Null,
                Val::Str(s) => temporal::time_from_string(&s)
                    .map(Val::Time)
                    .unwrap_or(Val::Null),
                Val::Map(m) => build_time(&m)?,
                other => bail!(
                    "localtime() expects a string or map, got {}",
                    other.to_display()
                ),
            },
            "localdatetime" => match a0(0) {
                Val::Null => Val::Null,
                Val::Str(s) => temporal::datetime_from_string(&s)
                    .map(Val::DateTime)
                    .unwrap_or(Val::Null),
                Val::Map(m) => build_datetime(&m)?,
                other => bail!(
                    "localdatetime() expects a string or map, got {}",
                    other.to_display()
                ),
            },
            "duration" => match a0(0) {
                Val::Null => Val::Null,
                Val::Str(s) => temporal::duration_from_string(&s)?
                    .map(Val::Duration)
                    .unwrap_or(Val::Null),
                Val::Map(m) => build_duration(&m)?,
                other => bail!(
                    "duration() expects a string or map, got {}",
                    other.to_display()
                ),
            },
            // ── Non-deterministic builtins (wall-clock / RNG) ────────────────
            // These read the clock or an entropy source, so `parser::is_nondeterministic`
            // marks any query calling them non-cacheable (server.rs `run_query`
            // skips the result-cache get + insert) — otherwise a cache hit would
            // replay a stale value.
            // `rand()` → uniform double in [0,1) (FalkorDB `AR_RAND`: rand()/RAND_MAX).
            "rand" => Val::Float(random_f64()),
            // `randomUUID()` → a fresh RFC-4122 v4 UUID string (FalkorDB `AR_RANDOMUUID`).
            "randomuuid" => Val::Str(uuid::Uuid::new_v4().to_string()),
            // `timestamp()` → milliseconds since the Unix epoch (FalkorDB `AR_TIMESTAMP`).
            "timestamp" => Val::Int(now_millis()),
            // ── List functions (FalkorDB list_funcs.c) ──────────────────────
            // tail: all but the first element. NULL → NULL.
            "tail" => match a0(0) {
                Val::Null => Val::Null,
                Val::List(xs) => Val::List(xs.into_iter().skip(1).collect()),
                other => bail!("tail() needs a list, got {}", other.to_display()),
            },
            // list.dedup: drop later duplicates, preserving first-seen order.
            "list.dedup" => match a0(0) {
                Val::Null => Val::Null,
                Val::List(mut xs) => {
                    dedup_vals(&mut xs);
                    Val::List(xs)
                }
                other => bail!("list.dedup() needs a list, got {}", other.to_display()),
            },
            // list.sort(list, ascending = true): sorted copy by total order.
            "list.sort" => list_sort(&args)?,
            // list.remove(list, idx, count = 1): drop up-to-`count` elements.
            "list.remove" => list_remove(&args)?,
            // list.insert(list, idx, val, dups = true): insert one element.
            "list.insert" => list_insert(&args)?,
            // list.insertListElements(list, list2, idx, dups = true): splice a list.
            "list.insertlistelements" => list_insert_elements(&args)?,
            // Element-wise conversion lists; each element goes through the
            // matching `*OrNull` scalar (NULL on failure). NULL list → NULL.
            "tobooleanlist" => self.to_type_list(&a0(0), "toboolean")?,
            "tofloatlist" => self.to_type_list(&a0(0), "tofloat")?,
            "tointegerlist" => self.to_type_list(&a0(0), "tointeger")?,
            "tostringlist" => self.to_type_list(&a0(0), "tostring")?,
            // ── Entity functions (FalkorDB entity_funcs.c) ──────────────────
            // hasLabels(node, [labels]): node carries ALL given labels. The
            // operator form `n:Label` is handled separately by `Expr::HasLabels`.
            "haslabels" => match a0(0) {
                Val::Null => Val::Null,
                Val::Node(id) => {
                    let labels = match a0(1) {
                        Val::List(xs) => xs,
                        Val::Null => return Ok(Val::Null),
                        other => bail!(
                            "hasLabels() needs a list of label strings, got {}",
                            other.to_display()
                        ),
                    };
                    let have = self.node_label_ids(id)?;
                    let mut res = true;
                    for l in labels {
                        let name = match l {
                            Val::Str(s) => s,
                            other => bail!(
                                "hasLabels() labels must be strings, got {}",
                                other.to_display()
                            ),
                        };
                        match self.gen.label_id(&name) {
                            Some(lid) if have.contains(&lid) => {}
                            _ => {
                                res = false;
                                break;
                            }
                        }
                    }
                    Val::Bool(res)
                }
                other => bail!("hasLabels() needs a node, got {}", other.to_display()),
            },
            // indegree/outdegree(node, [types…]): count edges in one direction,
            // optionally restricted to the given relationship type(s) (passed as
            // varargs strings or a single array of strings).
            "indegree" => self.node_degree(&args, true)?,
            "outdegree" => self.node_degree(&args, false)?,
            other => bail!("unknown function '{other}'"),
        })
    }

    /// Element-wise list conversion shared by `to{Boolean,Float,Integer,String}List`:
    /// run each element through the named `*OrNull`-style scalar arm. A NULL list
    /// yields NULL (FalkorDB `_AR_TOTYPELIST`).
    fn to_type_list(&self, v: &Val, conv: &str) -> Result<Val> {
        match v {
            Val::Null => Ok(Val::Null),
            Val::List(xs) => {
                let mut out = Vec::with_capacity(xs.len());
                for x in xs {
                    out.push(self.call_function(conv, false, vec![x.clone()])?);
                }
                Ok(Val::List(out))
            }
            other => bail!("{conv}List() needs a list, got {}", other.to_display()),
        }
    }

    /// `indegree`/`outdegree`: count a node's edges in one direction, optionally
    /// filtered to specific relationship types. Mirrors FalkorDB `_AR_NodeDegree`:
    /// a NULL node yields NULL; type filters may be varargs strings or one array.
    fn node_degree(&self, args: &[Val], incoming: bool) -> Result<Val> {
        let dir = if incoming { "indegree" } else { "outdegree" };
        let id = match args.first() {
            Some(Val::Node(id)) => *id,
            Some(Val::Null) | None => return Ok(Val::Null),
            Some(other) => bail!("{dir}() needs a node, got {}", other.to_display()),
        };
        // Collect the (deduplicated) relationship-type filter, if any.
        let mut names: Vec<String> = Vec::new();
        if args.len() > 1 {
            let push = |names: &mut Vec<String>, v: &Val| -> Result<()> {
                match v {
                    Val::Str(s) => {
                        if !names.contains(s) {
                            names.push(s.clone());
                        }
                        Ok(())
                    }
                    other => bail!("{dir}() types must be strings, got {}", other.to_display()),
                }
            };
            match &args[1] {
                Val::List(xs) => {
                    for x in xs {
                        push(&mut names, x)?;
                    }
                }
                _ => {
                    for a in &args[1..] {
                        push(&mut names, a)?;
                    }
                }
            }
        }
        let adjs = if incoming {
            self.incoming(id)?
        } else {
            self.outgoing(id)?
        };
        let count = if args.len() > 1 {
            let type_ids: Vec<u32> = names
                .iter()
                .filter_map(|t| self.gen.reltype_id(t))
                .collect();
            adjs.iter()
                .filter(|a| type_ids.contains(&a.reltype))
                .count()
        } else {
            adjs.len()
        };
        Ok(Val::Int(count as i64))
    }

    fn substring(&self, args: &[Val]) -> Result<Val> {
        let s = match args.first() {
            Some(Val::Str(s)) => s,
            Some(Val::Null) | None => return Ok(Val::Null),
            Some(other) => bail!("substring() needs a string, got {}", other.to_display()),
        };
        let chars: Vec<char> = s.chars().collect();
        let start = match args.get(1) {
            Some(Val::Int(i)) if *i >= 0 => (*i as usize).min(chars.len()),
            _ => bail!("substring() start must be a non-negative integer"),
        };
        let end = match args.get(2) {
            Some(Val::Int(len)) if *len >= 0 => (start + *len as usize).min(chars.len()),
            None => chars.len(),
            _ => bail!("substring() length must be a non-negative integer"),
        };
        Ok(Val::Str(chars[start..end].iter().collect()))
    }

    /// `left(s, n)` / `right(s, n)`: the n leftmost (or rightmost) characters.
    /// NULL string → NULL; `n` must be a non-negative integer; an `n` past the
    /// string length returns the whole string (matching FalkorDB AR_LEFT/AR_RIGHT).
    fn left_right(&self, args: &[Val], from_left: bool) -> Result<Val> {
        let s = match args.first() {
            Some(Val::Str(s)) => s,
            Some(Val::Null) | None => return Ok(Val::Null),
            Some(other) => bail!(
                "{}() needs a string, got {}",
                if from_left { "left" } else { "right" },
                other.to_display()
            ),
        };
        let n = match args.get(1) {
            Some(Val::Int(i)) if *i >= 0 => *i as usize,
            _ => bail!("length must be a non-negative integer"),
        };
        let chars: Vec<char> = s.chars().collect();
        if n >= chars.len() {
            return Ok(Val::Str(s.clone()));
        }
        let slice = if from_left {
            &chars[..n]
        } else {
            &chars[chars.len() - n..]
        };
        Ok(Val::Str(slice.iter().collect()))
    }

    fn range_fn(&self, args: &[Val]) -> Result<Val> {
        let int = |v: Option<&Val>, d: i64| match v {
            Some(Val::Int(i)) => Ok(*i),
            None => Ok(d),
            _ => bail!("range() bounds must be integers"),
        };
        let start = int(args.first(), 0)?;
        let end = int(args.get(1), 0)?;
        let step = int(args.get(2), 1)?;
        if step == 0 {
            bail!("range() step must be non-zero");
        }
        // Bound the result *before* allocating. A naive loop over `range(0,
        // i64::MAX)` allocates until it OOMs, and the unchecked `i += step` it used
        // to do wraps on overflow into an infinite loop (the per-query deadline is
        // not consulted inside this tight loop). Compute the element count in i128
        // (so `end - start` cannot overflow) and refuse anything past the guardrail.
        // 1M × ~48 B/element ≈ 48 MB — the lone guard when `query.maxIntermediate`
        // is disabled, so it is sized for the 100–200 MB deployment envelope.
        const MAX_RANGE_LEN: i128 = 1_000_000;
        let count: i128 = {
            let (s, e, st) = (start as i128, end as i128, step as i128);
            if (st > 0 && s > e) || (st < 0 && s < e) {
                0
            } else {
                (e - s) / st + 1
            }
        };
        if count > MAX_RANGE_LEN {
            bail!("range() would produce {count} elements, exceeding the limit of {MAX_RANGE_LEN}");
        }
        // Also charge the query-wide budget before allocating, so repeated
        // near-limit ranges cannot stack up to unbounded memory.
        self.charge(count as u64)?;
        let mut out = Vec::with_capacity(count as usize);
        let mut i = start;
        // Inclusive of `end`, matching Cypher.
        while (step > 0 && i <= end) || (step < 0 && i >= end) {
            out.push(Val::Int(i));
            // Stop cleanly at the i64 boundary instead of wrapping into an infinite
            // loop (the count guard above already bounds the iteration otherwise).
            match i.checked_add(step) {
                Some(n) => i = n,
                None => break,
            }
        }
        Ok(Val::List(out))
    }
}

// ── Free helpers ──────────────────────────────────────────────────────────────

enum BoolOp {
    And,
    Or,
    Xor,
}

/// One row's `ORDER BY` sort key: each key expression's value paired with its
/// sort direction.
type SortKey = Vec<(Val, SortDir)>;

/// A cursor over a group's precomputed aggregate values; `eval` advances it once
/// per aggregate-function node it visits, in the same order as `collect_aggregates`.
struct AggCursor {
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
struct GraphView {
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
enum WalkMode {
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
impl<'g, V: ReadView> Engine<'g, V> {
    /// Compile `pattern`, or fetch it from the per-query cache. `anchored` wraps
    /// it as `\A(?:…)\z` so `=~` requires the entire subject to match —
    /// openCypher / FalkorDB `=~` semantics; the unanchored form scans for every
    /// non-overlapping match anywhere in the subject.
    fn compiled_regex(&self, pattern: &str, anchored: bool) -> Result<regex::Regex> {
        if pattern.len() > MAX_REGEX_PATTERN_BYTES {
            bail!(
                "regex pattern is {} bytes, exceeding the limit of {MAX_REGEX_PATTERN_BYTES}",
                pattern.len()
            );
        }
        let key = if anchored {
            format!(r"\A(?:{pattern})\z")
        } else {
            pattern.to_string()
        };
        if let Some(re) = self.regex_cache.borrow().get(&key) {
            return Ok(re.clone());
        }
        let re = regex::RegexBuilder::new(&key)
            .size_limit(REGEX_SIZE_LIMIT)
            .dfa_size_limit(REGEX_DFA_SIZE_LIMIT)
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid regex: {e}"))?;
        let mut cache = self.regex_cache.borrow_mut();
        if cache.len() < REGEX_CACHE_MAX {
            cache.insert(key, re.clone());
        }
        Ok(re)
    }

    fn string_op(&self, op: StrOp, a: &Val, b: &Val) -> Result<Val> {
        let (s, t) = match (a, b) {
            (Val::Str(s), Val::Str(t)) => (s, t),
            // `=~` against a null operand is null (three-valued); so are the others.
            _ => return Ok(Val::Null),
        };
        Ok(Val::Bool(match op {
            StrOp::StartsWith => s.starts_with(t.as_str()),
            StrOp::EndsWith => s.ends_with(t.as_str()),
            StrOp::Contains => s.contains(t.as_str()),
            // `=~` is a full-match: the whole string must match the pattern,
            // mirroring FalkorDB's `str_MatchRegex` (anchored at both ends).
            StrOp::Regex => self.compiled_regex(t, true)?.is_match(s),
        }))
    }

    // string.matchRegEx(str, regex) -> list of [full_match, group1, …] per match.
    // A null operand yields an empty list; non-participating groups become "".
    fn match_regex(&self, s: &Val, pat: &Val) -> Result<Val> {
        let (s, pat) = match (s, pat) {
            (Val::Str(s), Val::Str(p)) => (s, p),
            (Val::Null, _) | (_, Val::Null) => return Ok(Val::List(vec![])),
            (Val::Str(_), other) | (other, _) => bail!(
                "Type mismatch: expected String or Null but was {}",
                type_name(other)
            ),
        };
        let re = self.compiled_regex(pat, false)?;
        let mut out = Vec::new();
        for caps in re.captures_iter(s) {
            let row = caps
                .iter()
                .map(|g| Val::Str(g.map_or("", |m| m.as_str()).to_string()))
                .collect();
            out.push(Val::List(row));
        }
        Ok(Val::List(out))
    }

    // string.replaceRegEx(str, regex, replacement = '') -> string. Any null operand
    // yields null; the replacement is inserted literally (no `$group` expansion).
    fn replace_regex(&self, s: &Val, pat: &Val, repl: &Val) -> Result<Val> {
        let (s, pat, repl) = match (s, pat, repl) {
            (Val::Str(s), Val::Str(p), Val::Str(r)) => (s, p, r),
            (Val::Null, _, _) | (_, Val::Null, _) | (_, _, Val::Null) => return Ok(Val::Null),
            (Val::Str(_), Val::Str(_), other) | (Val::Str(_), other, _) | (other, _, _) => bail!(
                "Type mismatch: expected String or Null but was {}",
                type_name(other)
            ),
        };
        let re = self.compiled_regex(pat, false)?;
        Ok(Val::Str(
            re.replace_all(s, regex::NoExpand(repl)).into_owned(),
        ))
    }
}

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

#[cfg(test)]
mod tests;
