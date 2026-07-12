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
use std::time::Instant;

use anyhow::{bail, Result};

use crate::algo;
use crate::cache::{BlockCache, FileKind, VectorIndexCache};
use crate::generation::RelEndpointSide;
use crate::parser::ast::*;
use crate::plan::{choose_node_scan, index_for, is_id_anchored, maybe_rel_type_scan, NodeScan};
use crate::read_view::ReadView;
use crate::temporal::{self, TKind};
use crate::vector;
use graph_format::ids::{EdgeId, NodeId, Value};
use graph_format::manifest::{AnnMode, EntityKind};
use graph_format::pq::AdcTable;
use graph_format::vamana::{self, beam_search};
use graph_format::vectors::{self, VectorEntry};
use graph_format::{columns, nodelabels, topology};
use rayon::prelude::*;

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
            topology::decode_adj_into(&rec, |a| {
                if !core_removed.contains(&a.edge.0) && delta_keep(&a) {
                    push(a)?;
                }
                Ok(())
            })?;
        }
        // 2. Surviving segment-born edges (delta filter applies, as they sit in the list).
        for a in &seg_born {
            if delta_keep(a) {
                push(*a)?;
            }
        }
        // 3. Surviving delta-born edges (tombstoned-neighbour filter only, matching the fold).
        for a in &delta_born {
            if !delta.is_tombstoned(a.neighbour.0) {
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
    let mut out = Vec::new();
    for_each_adj_overlaid(gen, cache, node, outgoing, ADJ_STREAM_CHUNK, &mut |c| {
        out.extend_from_slice(c);
        Ok(())
    })?;
    Ok(out)
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
        for a in read_adj_overlaid(gen, cache, node, outgoing)? {
            if type_ids.map_or(true, |ids| ids.contains(&a.reltype)) {
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
    let mut sources: Vec<(Vec<topology::Adj>, bool)> = Vec::new();
    match dir {
        Direction::Outgoing => sources.push((read_adj_overlaid(gen, cache, node, true)?, false)),
        Direction::Incoming => sources.push((read_adj_overlaid(gen, cache, node, false)?, true)),
        Direction::Undirected => {
            sources.push((read_adj_overlaid(gen, cache, node, true)?, false));
            sources.push((read_adj_overlaid(gen, cache, node, false)?, true));
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
    let mut buf: Vec<Hop> = Vec::new();
    for &(outgoing, incoming) in dirs {
        for_each_adj_overlaid(gen, cache, node, outgoing, chunk, &mut |adjs| {
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
        nodelabels::decode_labels(&rec)?
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

/// Thread-safe read of node `id`'s value for property `key` (or `Null` if absent),
/// decoding only the requested key from the cached record. The free-fn body behind
/// [`Engine::node_prop`]; used by the parallel anchor filter ([`node_ok_par`]).
fn node_prop_par(gen: &dyn ReadView, cache: &BlockCache, id: u64, key: &str) -> Result<Val> {
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

/// Per-query execution context over one generation and its block cache.
pub struct Engine<'g, V: ReadView> {
    gen: &'g V,
    cache: &'g BlockCache,
    /// The vector-index pool, needed only by the `AnnMode::Vamana` arm. The
    /// brute-force arm and all non-vector queries leave it `None`.
    vec_cache: Option<&'g VectorIndexCache>,
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
            params: HashMap::new(),
            plan_params: HashMap::new(),
            max_rows: usize::MAX,
            deadline: None,
            beam_width: 64,
            max_intermediate: 0,
            budget_used: Cell::new(0),
            max_scan: 0,
            scan_used: Cell::new(0),
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
    fn vector_group(&self, first_record: u64, count: u64) -> Result<Vec<VectorEntry>> {
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

    fn edge_prop(&self, id: u64, key: &str) -> Result<Val> {
        edge_prop_par(self.gen, self.cache, id, key)
    }

    /// Resolve a node's label names and named properties — the material a Bolt
    /// `Node` structure carries. Reads route through the block cache like any other
    /// record access, so encoding a returned node reuses already-resident blocks.
    pub fn node_record(&self, id: u64) -> Result<(Vec<String>, NamedProps)> {
        let labels = self
            .node_label_ids(id)?
            .into_iter()
            .filter_map(|l| self.gen.label_name(l).map(|s| s.to_string()))
            .collect();
        let mut props = self.core_named_props(id)?;
        self.overlay_node_props(id, &mut props);
        Ok((labels, props))
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
                bail!("query exceeded its time limit");
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
                bail!(
                    "query exceeded the intermediate result budget of {} elements (query.maxIntermediate)",
                    self.max_intermediate
                );
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
                    bail!(
                        "server-wide intermediate budget of {} elements exhausted \
                         (query.maxIntermediateGlobal) — too many concurrent memory-heavy queries",
                        g.limit()
                    );
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
                r.extend(std::iter::repeat(Val::Null).take(new_vars.len()));
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
                r.extend(std::iter::repeat(Val::Null).take(new_vars.len()));
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
                r.extend(std::iter::repeat(Val::Null).take(new_vars.len()));
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
                let mut out = Vec::new();
                for c in self.scan_candidates(&scan)? {
                    if self.node_ok(c, node, &Scope::Map(binding), &guaranteed)? {
                        out.push(c);
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
                    // before the clones accumulate. Only emitted results are charged
                    // elsewhere (above).
                    self.charge(1)?;
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
            bail!("shortestPath exceeded the node cap of {cap} (query.maxShortestPathExplore)");
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
                        bail!(
                            "shortestPath exceeded the node cap of {cap} \
                             (query.maxShortestPathExplore)"
                        );
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
            let candidates = self.scan_candidates(&scan)?;
            Some((guaranteed, candidates))
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
            // The hoisted plan, or a per-row index seek keyed by this row's scalars.
            let per_row;
            let (guaranteed, candidates): (&[u32], &[u64]) = match &hoisted {
                Some((g, c)) => (g, c),
                None => {
                    let bound = bound_scalars(&in_binding);
                    let scan = choose_node_scan(
                        self.gen,
                        start,
                        m.where_.as_ref(),
                        &self.plan_params,
                        &bound,
                    );
                    let guaranteed = self.scan_guaranteed_labels(&scan);
                    let candidates = self.scan_candidates(&scan)?;
                    per_row = (guaranteed, candidates);
                    (&per_row.0, &per_row.1)
                }
            };
            for &c in candidates {
                // Stage 6: honour a pushed `LIMIT` (no ORDER BY/aggregation/DISTINCT)
                // so a bare `MATCH (n:L) … LIMIT k` scans only k matching nodes.
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
        while let Some((node, lvl)) = queue.pop_front() {
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
        let roots = algo::wcc(view.nodes.len(), &view.undirected_edges());
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
        let scores = algo::pagerank(view.nodes.len(), &view.out);
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
        let hc = algo::harmonic(view.nodes.len(), &view.out);
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
        let cb = algo::betweenness(view.nodes.len(), &view.out);
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
        let comm = algo::cdlp(view.nodes.len(), &view.undirected_adj(), max_iter);
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
        let pos: HashMap<u64, usize> = nodes.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        // Each selected node's out-adjacency read is independent and touches only the
        // Sync cache, so gather the reads on the shared fanout pool (Task 11).
        // `neighbours_par` keeps the stored edge order and applies the same rel-type
        // filter, so mapping each neighbour through `pos` (single-threaded, `pos` is
        // shared read-only) yields the same 0-based index the sequential build did —
        // byte-for-byte identical node list + `out`.
        let (gen, cache) = (self.gen, self.cache);
        let adj: Vec<Vec<u64>> = par_gather(
            self.fanout_pool.as_deref(),
            &nodes,
            BUILD_VIEW_PAR_MIN,
            |&id| neighbours_par(gen, cache, id, Direction::Outgoing, rels),
        )?;
        let out: Vec<Vec<usize>> = adj
            .iter()
            .map(|nbs| nbs.iter().filter_map(|nb| pos.get(nb).copied()).collect())
            .collect();
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
                    )?,
                    None => vector::brute_force_knn_par(
                        self.fanout_pool.as_deref(),
                        entries.as_ref().unwrap(),
                        &query,
                        k,
                        metric,
                        KNN_PAR_MIN,
                    )?,
                },
                AnnMode::Vamana { medoid, .. } => {
                    self.vamana_knn(&vc.label, &vc.property, *medoid, metric, &query, k)?
                }
            };
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
    fn vamana_knn(
        &self,
        label: &str,
        property: &str,
        medoid: u64,
        metric: graph_format::manifest::Metric,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<vector::Neighbour>> {
        let pool = self.vec_cache.ok_or_else(|| {
            anyhow::anyhow!("vector-index cache is not configured; cannot serve a Vamana index")
        })?;
        let index = self.gen.vamana_index(label, property).ok_or_else(|| {
            anyhow::anyhow!("Vamana index files for (:{label} {{{property}}}) are not open")
        })?;
        let resident = &index.pq;
        let n = resident.len();
        if n == 0 || k == 0 {
            return Ok(Vec::new());
        }

        // PQ navigates in the normalised space the codebook was trained in (D29).
        let qn = normalise(query);
        let adc = AdcTable::new(&resident.codebook, &qn)?;

        let gen_id = self.gen.uuid();
        let reader = index.reader.inner();
        let ord = index.ord;
        let hits = beam_search(
            medoid as u32,
            self.beam_width,
            k,
            n,
            |i| adc.estimate(resident.codes_of(i as usize)),
            |i| {
                // One coalesced block read per expansion (cached in the vector pool).
                let rec = pool.record(reader, gen_id, ord, i as u64)?;
                let node = vamana::decode_node(&rec)?;
                Ok((node.vector, node.neighbours))
            },
            // Exact re-rank uses the original query (cosine is scale-invariant, so
            // the normalised stored vectors give the same distance).
            |v| vector::distance(metric, query, v) as f32,
        )?;
        Ok(hits
            .into_iter()
            .map(|h| vector::Neighbour {
                node_id: resident.node_ids[h.index as usize],
                score: h.exact as f64,
            })
            .collect())
    }

    /// Evaluate an expression that must produce a query vector: a `vecf32([...])`
    /// literal, a stored `Vector`, or a list of numbers (a `$param` arrives as a
    /// list). Anything else is a type error.
    fn eval_query_vector(&self, e: &Expr, scope: &Scope) -> Result<Vec<f32>> {
        match self.eval(e, scope, None)? {
            Val::Vector(v) => Ok(v),
            Val::List(xs) => xs
                .iter()
                .map(|x| {
                    x.as_num().map(|f| f as f32).ok_or_else(|| {
                        anyhow::anyhow!(
                            "query vector elements must be numbers, got {}",
                            x.to_display()
                        )
                    })
                })
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
        let (candidates, guaranteed): (Vec<u64>, Vec<u32>) =
            match start.var.as_deref().and_then(|v| binding.get(v)) {
                Some(Val::Node(id)) => (vec![*id], Vec::new()),
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
                    (self.scan_candidates(&scan)?, guaranteed)
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
        let prefilter = cap.is_none()
            && self.fanout_pool.is_some()
            && candidates.len() >= SCAN_PAR_MIN
            && self.anchor_filter_reads(start, &guaranteed);
        let candidates: Vec<u64> = if prefilter {
            let wants: Vec<(&str, Val)> = start
                .props
                .iter()
                .map(|(k, e)| Ok((k.as_str(), self.eval(e, &Scope::Map(binding), None)?)))
                .collect::<Result<_>>()?;
            let (gen, cache) = (self.gen, self.cache);
            let label_expr = start.label_expr.as_ref();
            let pass = par_gather(
                self.fanout_pool.as_deref(),
                &candidates,
                SCAN_PAR_MIN,
                |&c| node_ok_par(gen, cache, c, label_expr, &wants, &guaranteed),
            )?;
            candidates
                .into_iter()
                .zip(pass)
                .filter_map(|(c, ok)| ok.then_some(c))
                .collect()
        } else {
            candidates
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
        for c in candidates {
            // Stage 6: once a pushed `LIMIT` is met, stop scanning anchors — the
            // remaining candidates can only add rows the projection would truncate.
            if cap.is_some_and(|cc| out.len() >= cc) {
                break;
            }
            // Already filtered in parallel above when `prefilter`; otherwise check the
            // anchor's labels/inline props inline (with the loop's early-exit break).
            if !prefilter && !self.node_ok(c, start, &Scope::Map(&frame), &guaranteed)? {
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
        let unbound = |v: Option<&String>| v.map_or(true, |name| !binding.contains_key(name));
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
        let track_walk = pattern.path_var.is_some();
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
                let mut used = HashSet::new();
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

    /// Candidate node ids for a chosen scan strategy.
    fn scan_candidates(&self, scan: &NodeScan) -> Result<Vec<u64>> {
        let ids = match scan {
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
            NodeScan::LabelScan { label_id } => {
                let mut ids = self.gen.collect_nodes_with_label(*label_id)?;
                // Core stack: a segment full row can add or drop a label (or tombstone the
                // node), so every stack-touched id's membership is recomputed from its
                // effective row, and born ids carrying the label are added.
                let stack = self.gen.core_stack();
                let label = self.gen.label_name(*label_id).map(str::to_string);
                if !stack.is_singleton() {
                    if let Some(label) = label.as_deref() {
                        stack.fold_label_scan(&mut ids, label)?;
                    }
                }
                // Delta-born nodes (Phase 2c) are not in the core label postings —
                // append the synthetic ids carrying this label so a created node shows
                // up in a label scan. Stage 5 also appends core/born ids that *gained*
                // this label via `SET n:Label`; a core node that *dropped* it stays in
                // the postings but is re-checked and rejected by `node_ok` (the scan is
                // no longer trusted to prove the label — see `scan_guaranteed_labels`).
                // Tombstoned ids are dropped by the suppression below. The empty-delta
                // fast path skips the lookup entirely.
                let delta = self.gen.delta();
                if !delta.is_empty() {
                    if let Some(label) = label.as_deref() {
                        ids.extend(delta.born_ids_with_label(label));
                        ids.extend(delta.ids_with_added_label(label));
                    }
                }
                if !stack.is_singleton() || !delta.is_empty() {
                    ids.sort_unstable();
                    ids.dedup();
                }
                ids
            }
            NodeScan::AllNodes => (0..self.gen.node_count()).collect(),
            // Distinct edge-having endpoint nodes for the typed first hop (the
            // precomputed posting). Ascending+deduped, same contract as a label
            // scan; the first hop re-filters by reltype so this only narrows.
            NodeScan::RelTypeScan {
                reltype_ids, side, ..
            } => {
                let mut ids = self
                    .gen
                    .collect_endpoint_nodes_for_reltypes(reltype_ids, *side)?;
                // Union each segment's endpoint driving set for these reltypes. Postings
                // carry no removals — a superset stays correct because the first hop
                // re-filters by reltype (and `suppress_tombstoned` drops deleted nodes).
                let stack = self.gen.core_stack();
                if !stack.is_singleton() {
                    for seg in stack.segments() {
                        let Some(post) = &seg.postings else { continue };
                        for &rt in reltype_ids {
                            let Some(name) = self.gen.reltype_name(rt) else {
                                continue;
                            };
                            if matches!(side, RelEndpointSide::Source | RelEndpointSide::Either) {
                                ids.extend_from_slice(post.src_ids(name));
                            }
                            if matches!(side, RelEndpointSide::Target | RelEndpointSide::Either) {
                                ids.extend_from_slice(post.tgt_ids(name));
                            }
                        }
                    }
                    ids.sort_unstable();
                    ids.dedup();
                }
                ids
            }
        };
        self.suppress_tombstoned(ids)
    }

    /// Drop candidate dense ids a deletion has tombstoned — the delta's (Phase 2) *and* the
    /// core stack's (a flush that deleted a node): a deleted node must never bind as an
    /// anchor. The pure-core singleton with an empty delta returns the input untouched, so
    /// the read-only path pays nothing.
    fn suppress_tombstoned(&self, ids: Vec<u64>) -> Result<Vec<u64>> {
        let delta = self.gen.delta();
        let stack = self.gen.core_stack();
        if delta.is_empty() && stack.is_singleton() {
            return Ok(ids);
        }
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if delta.is_tombstoned(id) {
                continue;
            }
            if !stack.is_singleton() && stack.is_node_tombstoned(id)? {
                continue;
            }
            out.push(id);
        }
        Ok(out)
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
                Val::Int(i) => Ok(Val::Int(-i)),
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
                        .map(|x| {
                            x.as_num().map(|f| f as f32).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "vecf32() elements must be numbers, got {}",
                                    x.to_display()
                                )
                            })
                        })
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
            "similarity" | "vec.cosinesimilarity" => match (as_vector(&a0(0)), as_vector(&a0(1))) {
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
                    let a = as_vector(&x).ok_or_else(|| {
                        anyhow::anyhow!("{n}() needs vectors, got {}", x.to_display())
                    })?;
                    let b = as_vector(&y).ok_or_else(|| {
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
                Val::Str(s) => temporal::duration_from_string(&s)
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

/// L2-normalise a query vector to unit length (the cosine PQ space — D29). A zero
/// vector is returned unchanged.
fn normalise(v: &[f32]) -> Vec<f32> {
    let norm: f64 = v
        .iter()
        .map(|&x| (x as f64) * (x as f64))
        .sum::<f64>()
        .sqrt();
    if norm == 0.0 {
        return v.to_vec();
    }
    v.iter().map(|&x| (x as f64 / norm) as f32).collect()
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
    let max = vl.max.unwrap_or(MAX_VARLEN_HOPS).max(min);
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
    let mut s = if start < 0 { len - start.abs() } else { start };
    if s < 0 {
        s = 0;
    }
    let mut e = if end < 0 { len - end.abs() } else { end };
    if e > len {
        e = len;
    }
    if e <= s {
        return &[];
    }
    &xs[s as usize..e as usize]
}

fn list_index(len: usize, i: i64) -> Option<usize> {
    let idx = if i < 0 { len as i64 + i } else { i };
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
                Ok(Val::Duration(temporal::add_durations(*x, *y, false)))
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
                Ok(Val::Duration(temporal::add_durations(*x, *y, true)))
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
    )))
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
        return Ok(match op {
            BinOp::Add => Val::Int(x + y),
            BinOp::Sub => Val::Int(x - y),
            BinOp::Mul => Val::Int(x * y),
            BinOp::Div => {
                if y == 0 {
                    bail!("integer division by zero");
                }
                Val::Int(x / y)
            }
            BinOp::Mod => {
                if y == 0 {
                    bail!("integer modulo by zero");
                }
                Val::Int(x % y)
            }
            // Exponentiation always yields a Float, even for integer operands
            // (`2 ^ 3` = 8.0), matching Neo4j.
            BinOp::Pow => Val::Float((x as f64).powf(y as f64)),
        });
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

fn sum(vals: &[Val]) -> Result<Val> {
    if vals.iter().all(|v| matches!(v, Val::Int(_))) {
        let mut s = 0i64;
        for v in vals {
            if let Val::Int(i) = v {
                s += i;
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

/// A uniform random `f64` in `[0, 1)` for `rand()`. Drawn from the same
/// CSPRNG that backs `uuid`'s v4 generator (so no extra dependency): 53 high
/// bits of a fresh UUID divided by `2^53` give a correctly-distributed double.
fn random_f64() -> f64 {
    let bits = (uuid::Uuid::new_v4().as_u128() as u64) >> 11; // keep 53 bits
    (bits as f64) / ((1u64 << 53) as f64)
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

/// Coerce a value to a vector for the similarity functions: a `Vector` directly,
/// or a list of numbers (the shape an inlined literal / `$param` takes).
fn as_vector(v: &Val) -> Option<Vec<f32>> {
    match v {
        Val::Vector(xs) => Some(xs.clone()),
        Val::List(xs) => xs.iter().map(|x| x.as_num().map(|f| f as f32)).collect(),
        _ => None,
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
mod tests {
    use super::*;
    use crate::generation::Generation;
    use crate::parser;
    use crate::testgen;
    use graph_format::ids::Generation as GenId;

    /// The writable-layer read overlay (Phase 1c): a delta patch on an existing
    /// node's property overrides the core value last-writer-wins, a delta patch on
    /// a *new* property name appears, and both the all-props path (`node_record` /
    /// `properties()`) and the single-prop path (`n.key`) reflect it.
    #[test]
    fn delta_overlay_folds_node_property_patches() {
        use crate::read_view::MergedView;
        use slater_delta::{DeltaSnapshot, Memtable};
        use std::sync::Arc;

        let (root, graph, _) = testgen::write_basic("delta_overlay_unit");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        // Patch node 0 (Alice :Person, age=30): overwrite `age`, add new `rating`.
        let mut mem = Memtable::new();
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Alice".into()),
            Some(0),
            [
                ("age".to_string(), Value::Int(99)),
                ("rating".to_string(), Value::Str("AAA".into())),
            ],
        );
        let delta = DeltaSnapshot::from_memtable(Arc::new(mem));
        let view = MergedView::new(&gen, delta);

        // All-props path: node_record reflects the overwrite and the new property.
        let engine = Engine::new(&view, &cache);
        let (_labels, props) = engine.node_record(0).unwrap();
        let age = props.iter().find(|(k, _)| k == "age").map(|(_, v)| v);
        assert!(
            matches!(age, Some(Val::Int(99))),
            "age overwritten: {props:?}"
        );
        let rating = props.iter().find(|(k, _)| k == "rating").map(|(_, v)| v);
        assert!(
            matches!(rating, Some(Val::Str(s)) if s == "AAA"),
            "new property present: {props:?}"
        );
        // An unpatched node is untouched by the overlay.
        let (_l, p1) = engine.node_record(1).unwrap();
        let age1 = p1.iter().find(|(k, _)| k == "age").map(|(_, v)| v);
        assert!(
            matches!(age1, Some(Val::Int(25))),
            "node 1 untouched: {p1:?}"
        );

        // Single-prop path: `n.age` / `n.rating` read through the overlay too.
        let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age, n.rating").unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(99)), "n.age via overlay");
        assert!(
            matches!(&res.rows[0][1], Val::Str(s) if s == "AAA"),
            "n.rating via overlay"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// Stack a single upper core segment over a `write_basic` base and repoint `current`
    /// at a set that lists it. The segment overrides base node 0 (full-row replace: keeps
    /// `name`, changes `age` 30→99, adds a non-core-symbol prop `mood`, drops `city`/`team`),
    /// tombstones base node 2, births node 5 (`:Person {name:'Zed', age:50}`) and edge 5
    /// (`(0)-[:KNOWS {since:2099}]->(5)`). Returns `(root, graph, set_uuid)`.
    fn write_basic_with_segment(tag: &str) -> (std::path::PathBuf, String, uuid::Uuid) {
        use graph_format::manifest::FileEntry;
        use graph_format::segindex::{write_index_fragments, IndexSpec};
        use graph_format::segmanifest::{
            DirtyIndex, SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION,
        };
        use graph_format::segment::{AdjEdge, EdgeRow, NodeRow, SegmentWriter};
        use graph_format::segpostings::{write_posting_fragments, PostingSpec};
        use graph_format::setmanifest::{SegmentRef, SetManifest};

        let (root, graph, base_uuid) = testgen::write_basic(tag);
        let seg_uuid = uuid::Uuid::from_u128(0x5_5e60_0000_0000_0000_0000_0000_0001);
        let set_uuid = uuid::Uuid::from_u128(0x5_5e70_0000_0000_0000_0000_0000_0001);

        let seg_dir = root
            .join(&graph)
            .join("segments")
            .join(seg_uuid.to_string());
        std::fs::create_dir_all(seg_dir.parent().unwrap()).unwrap();
        let mut w = SegmentWriter::create(&seg_dir, 0x22, 4096, 3).unwrap();
        // Nodes pushed in ascending dense-id order: override(0), tombstone(2), born(5).
        w.push_node(
            0,
            &NodeRow {
                labels: vec!["Person".into()],
                props: vec![
                    ("name".into(), Value::Str("Alice".into())),
                    ("age".into(), Value::Int(99)),
                    ("mood".into(), Value::Str("calm".into())),
                ],
                tombstoned: false,
            },
        )
        .unwrap();
        w.push_node(2, &NodeRow::tombstone()).unwrap();
        w.push_node(
            5,
            &NodeRow {
                labels: vec!["Person".into()],
                props: vec![
                    ("name".into(), Value::Str("Zed".into())),
                    ("age".into(), Value::Int(50)),
                ],
                tombstoned: false,
            },
        )
        .unwrap();
        w.push_edge(
            5,
            &EdgeRow {
                src: 0,
                dst: 5,
                reltype: "KNOWS".into(),
                props: vec![("since".into(), Value::Int(2099))],
                tombstoned: false,
            },
        )
        .unwrap();
        // Adjacency fragments: born edge 5 (0→5 KNOWS) on both endpoints, and a removal of
        // base edge 4 (0→2 KNOWS) from node 0's outgoing list.
        w.push_adj_out(
            0,
            &[
                AdjEdge {
                    other: 2,
                    reltype: "KNOWS".into(),
                    edge_id: 4,
                    removed: true,
                },
                AdjEdge {
                    other: 5,
                    reltype: "KNOWS".into(),
                    edge_id: 5,
                    removed: false,
                },
            ],
        )
        .unwrap();
        w.push_adj_in(
            5,
            &[AdjEdge {
                other: 0,
                reltype: "KNOWS".into(),
                edge_id: 5,
                removed: false,
            }],
        )
        .unwrap();
        w.finish().unwrap();

        // Index fragments: the born/patched (value, id) pairs this segment carries, plus the
        // removal sidecar of base ids whose indexed value it supersedes (node 0's age moved
        // 30→99, node 2 tombstoned). name: node 0 keeps "Alice", so only Carol(2) is removed.
        write_index_fragments(
            &seg_dir,
            &[
                IndexSpec {
                    label: "Person".into(),
                    prop: "age".into(),
                    entries: vec![(Value::Int(99), 0), (Value::Int(50), 5)],
                    removals: vec![0, 2],
                },
                IndexSpec {
                    label: "Person".into(),
                    prop: "name".into(),
                    entries: vec![(Value::Str("Zed".into()), 5)],
                    removals: vec![2],
                },
            ],
            4096,
            3,
            None,
        )
        .unwrap();
        // Endpoint driving sets: the born edge 0-[:KNOWS]->5.
        write_posting_fragments(
            &seg_dir,
            &[PostingSpec {
                reltype: "KNOWS".into(),
                src_ids: vec![0],
                tgt_ids: vec![5],
            }],
        )
        .unwrap();

        let mut m = SegmentManifest {
            magic: SEGMENT_MAGIC.into(),
            version: SEGMENT_MANIFEST_VERSION,
            segment_uuid: GenId(seg_uuid),
            base: GenId(base_uuid),
            created_unix: 0,
            node_band: (5, 6), // one born node id
            edge_band: (5, 6), // one born edge id
            content_hash: String::new(),
            encryption: None,
            node_count_delta: 0, // +1 born (5), -1 tombstoned (2)
            edge_count_delta: 0, // +1 born (e5), -1 removed (e4)
            reltype_edge_deltas: vec![("KNOWS".into(), 0)], // KNOWS: +e5 -e4
            label_node_deltas: vec![("Person".into(), 0)],
            hub_degree_out_deltas: vec![],
            hub_degree_in_deltas: vec![],
            marginals_exact: true,
            dirty_indexes: vec![
                DirtyIndex {
                    label: "Person".into(),
                    property: "age".into(),
                    fragment: "idx_0.isam".into(),
                },
                DirtyIndex {
                    label: "Person".into(),
                    property: "name".into(),
                    fragment: "idx_1.isam".into(),
                },
            ],
            label_membership_touch: None,
            mac: None,
            files: vec![FileEntry {
                name: "node.blk".into(),
                bytes: 0,
                blake3: "aa".into(),
                sha256: None,
                crc32c: None,
            }],
        };
        m.set_content_hash();
        m.write_to_dir(&seg_dir).unwrap();

        let sets = root.join(&graph).join("sets");
        std::fs::create_dir_all(&sets).unwrap();
        let mut set = SetManifest::singleton(GenId(base_uuid), 0);
        set.set_uuid = GenId(set_uuid);
        set.segments = vec![SegmentRef::from_manifest(&m)];
        std::fs::write(
            sets.join(format!("{set_uuid}.json")),
            set.to_bytes().unwrap(),
        )
        .unwrap();
        std::fs::write(root.join(&graph).join("current"), set_uuid.to_string()).unwrap();
        (root, graph, set_uuid)
    }

    fn prop<'a>(props: &'a NamedProps, key: &str) -> Option<&'a Val> {
        props.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// Slice 1 parity oracle: the pre-streaming **materialised** adjacency fold
    /// (core read → per-segment fragment fold → delta fold), reproduced verbatim so the
    /// streaming [`for_each_adj_overlaid`] can be checked byte-for-byte against it. This is
    /// the frozen behaviour of the old `read_adj_overlaid` before it became a `collect`.
    #[cfg(test)]
    fn materialised_adj_fold(
        gen: &dyn ReadView,
        cache: &BlockCache,
        node: u64,
        outgoing: bool,
    ) -> Vec<topology::Adj> {
        // core
        let mut core = if node >= gen.core_generation().node_count() {
            Vec::new()
        } else {
            let topo = gen.topology();
            let global = if outgoing {
                topo.outgoing_global(NodeId(node))
            } else {
                topo.incoming_global(NodeId(node))
            };
            let rec = cache
                .record(topo.inner(), gen.uuid(), FileKind::Topology, global)
                .unwrap();
            topology::decode_adj(&rec).unwrap()
        };
        // per-segment fold, oldest→newest
        let stack = gen.core_stack();
        if !stack.is_singleton() {
            for seg in stack.segments() {
                let r = &seg.reader;
                let frag = if outgoing {
                    if !r.may_hold_out_adj(node) {
                        continue;
                    }
                    r.out_adj(node).unwrap()
                } else {
                    if !r.may_hold_in_adj(node) {
                        continue;
                    }
                    r.in_adj(node).unwrap()
                };
                if frag.is_empty() {
                    continue;
                }
                let mut removed: HashSet<u64> = HashSet::new();
                let mut born: Vec<topology::Adj> = Vec::new();
                for e in frag {
                    if e.removed {
                        removed.insert(e.edge_id);
                    } else if let Some(rt) = gen.reltype_id(&e.reltype) {
                        born.push(topology::Adj {
                            reltype: rt,
                            neighbour: NodeId(e.other),
                            edge: EdgeId(e.edge_id),
                        });
                    }
                }
                if !removed.is_empty() {
                    core.retain(|a| !removed.contains(&a.edge.0));
                }
                core.extend(born);
            }
        }
        // delta fold
        let delta = gen.delta();
        if !delta.is_empty() {
            let deltas = if outgoing {
                delta.out_edges(node)
            } else {
                delta.in_edges(node)
            };
            let mut suppress: HashSet<(u32, u64)> = HashSet::new();
            let mut born: Vec<topology::Adj> = Vec::new();
            for e in deltas {
                let Some(rt) = gen.reltype_id(&e.reltype) else {
                    continue;
                };
                if e.tombstoned {
                    suppress.insert((rt, e.other));
                } else if let Some(eid) = e.edge_id {
                    born.push(topology::Adj {
                        reltype: rt,
                        neighbour: NodeId(e.other),
                        edge: EdgeId(eid),
                    });
                }
            }
            core.retain(|a| {
                !suppress.contains(&(a.reltype, a.neighbour.0))
                    && !delta.is_tombstoned(a.neighbour.0)
            });
            for a in born {
                if !delta.is_tombstoned(a.neighbour.0) {
                    core.push(a);
                }
            }
        }
        core
    }

    /// Slice 1: the streaming [`for_each_adj_overlaid`] reproduces the materialised
    /// core→segment→delta fold **byte-for-byte** — same edges, same order — across
    /// core-only / segment / delta / tombstone / node-delete fixtures, and the result is
    /// invariant to the emit `chunk` size (chunk boundaries never reorder or drop edges).
    /// [`read_adj_overlaid`] (now a `collect`) is asserted equal to the oracle too.
    #[test]
    fn for_each_adj_overlaid_byte_parity() {
        use crate::read_view::MergedView;
        use slater_delta::{DeltaSnapshot, Memtable};
        use std::sync::Arc;

        // Every node/direction: collect wrapper == oracle, and every chunk size streams the
        // same sequence with no empty/over-cap chunk.
        let check = |view: &MergedView, cache: &BlockCache, max_node: u64| {
            for node in 0..=max_node {
                for outgoing in [true, false] {
                    let want = materialised_adj_fold(view, cache, node, outgoing);
                    let got = read_adj_overlaid(view, cache, node, outgoing).unwrap();
                    assert_eq!(got, want, "collect parity node={node} out={outgoing}");
                    for chunk in [1usize, 2, 3, 8192] {
                        let mut streamed = Vec::new();
                        for_each_adj_overlaid(view, cache, node, outgoing, chunk, &mut |c| {
                            assert!(!c.is_empty(), "empty chunk node={node} chunk={chunk}");
                            assert!(c.len() <= chunk, "over-cap chunk node={node} chunk={chunk}");
                            streamed.extend_from_slice(c);
                            Ok(())
                        })
                        .unwrap();
                        assert_eq!(
                            streamed, want,
                            "stream parity node={node} out={outgoing} chunk={chunk}"
                        );
                    }
                }
            }
        };

        // A: core-only — singleton stack + empty delta (both streaming fast paths).
        {
            let (root, graph, _) = testgen::write_basic("adj_stream_core");
            let gen = Generation::open(&root, &graph).unwrap();
            let cache = BlockCache::new(1 << 20);
            let view = MergedView::read_only(&gen);
            check(&view, &cache, 4);
            std::fs::remove_dir_all(&root).ok();
        }

        // B: one upper segment, empty delta — segment fold with a removed + born fragment.
        {
            let (root, graph, _) = write_basic_with_segment("adj_stream_seg");
            let gen = Generation::open(&root, &graph).unwrap();
            let cache = BlockCache::new(1 << 20);
            let view = MergedView::read_only(&gen);
            // Sanity: node 0's out list lost base e4 and gained segment e5 (fold is non-trivial).
            let out0 = read_adj_overlaid(&view, &cache, 0, true).unwrap();
            assert!(!out0.iter().any(|a| a.edge.0 == 4), "segment removed e4");
            assert!(out0.iter().any(|a| a.edge.0 == 5), "segment born e5");
            check(&view, &cache, 6);
            std::fs::remove_dir_all(&root).ok();
        }

        // C: segment + rich delta — born edge, edge suppression, and a node delete.
        {
            let (root, graph, _) = write_basic_with_segment("adj_stream_seg_delta");
            let gen = Generation::open(&root, &graph).unwrap();
            let cache = BlockCache::new(1 << 20);
            let mut mem = Memtable::new();
            // Register both endpoints so the edge delete resolves core dense ids.
            mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
            mem.upsert_node("Person", "name", Value::Str("Bob".into()), Some(1), []);
            // Delta-born out-edge 0→3 (Acme) KNOWS.
            mem.upsert_edge(
                "Person",
                "name",
                Value::Str("Alice".into()),
                "KNOWS",
                "Company",
                "name",
                Value::Str("Acme".into()),
                Some(0),
                Some(3),
                [],
            );
            // Suppress base edge e0 (0→1 KNOWS).
            mem.delete_edge(
                "Person",
                "name",
                Value::Str("Alice".into()),
                "KNOWS",
                "Person",
                "name",
                Value::Str("Bob".into()),
                Some(0),
                Some(1),
            );
            // Node delete: Globex (4) — drops any edge whose neighbour is 4.
            mem.delete_node("Company", "name", Value::Str("Globex".into()), Some(4));
            let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
            // Sanity: the delta branches are actually live (else the oracle would trivially agree).
            assert!(view.delta().is_tombstoned(4), "node 4 tombstoned in delta");
            let knows = gen.reltype_id("KNOWS").unwrap();
            let out0 = read_adj_overlaid(&view, &cache, 0, true).unwrap();
            // e0 (0-[:KNOWS]->1) is delta-suppressed; the delta-born 0-[:KNOWS]->3 is present.
            // (Check by neighbour, not edge id — a bare Memtable numbers born ids from 0.)
            assert!(
                !out0
                    .iter()
                    .any(|a| a.reltype == knows && a.neighbour.0 == 1),
                "delta suppressed e0 (0->1 KNOWS)"
            );
            assert!(
                out0.iter()
                    .any(|a| a.reltype == knows && a.neighbour.0 == 3),
                "delta-born 0->3 KNOWS present"
            );
            check(&view, &cache, 6);
            std::fs::remove_dir_all(&root).ok();
        }

        // D: delta only, no segments — empty stack fast path with a live delta + node delete.
        {
            let (root, graph, _) = testgen::write_basic("adj_stream_delta_only");
            let gen = Generation::open(&root, &graph).unwrap();
            let cache = BlockCache::new(1 << 20);
            let mut mem = Memtable::new();
            mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
            mem.upsert_edge(
                "Person",
                "name",
                Value::Str("Alice".into()),
                "KNOWS",
                "Company",
                "name",
                Value::Str("Acme".into()),
                Some(0),
                Some(3),
                [],
            );
            mem.delete_node("Person", "name", Value::Str("Carol".into()), Some(2));
            let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
            assert!(view.delta().is_tombstoned(2), "node 2 tombstoned in delta");
            check(&view, &cache, 4);
            std::fs::remove_dir_all(&root).ok();
        }
    }

    /// Slice 2: the streamed hop reader [`for_each_hop_overlaid`] yields the **same hops
    /// in the same order** as the materialising [`hops_par`] — for every direction and a
    /// range of type filters (untyped, a `:KNOWS` set, an empty set) — over core /
    /// segment / segment+delta fixtures. This is the guarantee the hub routing rests on:
    /// swapping a hub's materialise for a stream cannot change the traversal's result.
    #[test]
    fn for_each_hop_overlaid_matches_hops_par() {
        use crate::read_view::MergedView;
        use slater_delta::{DeltaSnapshot, Memtable};
        use std::sync::Arc;

        // Hop has no PartialEq — compare by its full tuple projection.
        let key = |h: &Hop| (h.edge, h.neighbour, h.reltype, h.start, h.end);
        let check = |view: &MergedView, cache: &BlockCache, knows: u32, max_node: u64| {
            let tfs: Vec<Option<TypeFilter>> = vec![
                None,
                Some(TypeFilter::AnyOf(vec![knows])),
                Some(TypeFilter::AnyOf(vec![])),
            ];
            for node in 0..=max_node {
                for dir in [
                    Direction::Outgoing,
                    Direction::Incoming,
                    Direction::Undirected,
                ] {
                    for tf in &tfs {
                        let want = hops_par(view, cache, node, dir, tf.as_ref()).unwrap();
                        // A small chunk (3) forces multi-chunk streaming across boundaries.
                        let mut got = Vec::new();
                        for_each_hop_overlaid(view, cache, node, dir, tf.as_ref(), 3, &mut |c| {
                            got.extend_from_slice(c);
                            Ok(())
                        })
                        .unwrap();
                        assert_eq!(
                            got.iter().map(key).collect::<Vec<_>>(),
                            want.iter().map(key).collect::<Vec<_>>(),
                            "hop parity node={node} dir={dir:?} tf={tf:?}",
                            tf = tf.as_ref().map(|_| "some")
                        );
                    }
                }
            }
        };

        // Core-only.
        {
            let (root, graph, _) = testgen::write_basic("hop_stream_core");
            let gen = Generation::open(&root, &graph).unwrap();
            let cache = BlockCache::new(1 << 20);
            let knows = gen.reltype_id("KNOWS").unwrap();
            let view = MergedView::read_only(&gen);
            check(&view, &cache, knows, 4);
            std::fs::remove_dir_all(&root).ok();
        }
        // Segment + delta (born edge, edge-delete, node-delete) — the full overlay.
        {
            let (root, graph, _) = write_basic_with_segment("hop_stream_seg_delta");
            let gen = Generation::open(&root, &graph).unwrap();
            let cache = BlockCache::new(1 << 20);
            let knows = gen.reltype_id("KNOWS").unwrap();
            let mut mem = Memtable::new();
            mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
            mem.upsert_node("Person", "name", Value::Str("Bob".into()), Some(1), []);
            mem.upsert_edge(
                "Person",
                "name",
                Value::Str("Alice".into()),
                "KNOWS",
                "Company",
                "name",
                Value::Str("Acme".into()),
                Some(0),
                Some(3),
                [],
            );
            mem.delete_edge(
                "Person",
                "name",
                Value::Str("Alice".into()),
                "KNOWS",
                "Person",
                "name",
                Value::Str("Bob".into()),
                Some(0),
                Some(1),
            );
            mem.delete_node("Company", "name", Value::Str("Globex".into()), Some(4));
            let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
            check(&view, &cache, knows, 6);
            std::fs::remove_dir_all(&root).ok();
        }
    }

    /// Degree-sum terminal count fast path: a k-hop `count(endpoint)` answered by summing
    /// effective degree over the penultimate frontier must equal the materialising walk —
    /// across 1/2/3-hop, undirected, an anchor scan, and a live delta of edge writes — and
    /// it must actually engage (not silently decline and pass via the walk). Node-deletes
    /// and non-qualifying shapes decline to the walk, still correct.
    #[test]
    fn degree_terminal_count_matches_walk() {
        use crate::read_view::MergedView;
        use slater_delta::{DeltaSnapshot, Memtable};
        use std::sync::Arc;

        fn pattern_of(q: &str) -> crate::parser::ast::Pattern {
            let ast = parser::parse(q).unwrap();
            let crate::parser::ast::Clause::Match(m) = &ast.head.reading[0] else {
                panic!("not a match: {q}");
            };
            m.patterns[0].clone()
        }
        let count = |view: &MergedView, cache: &BlockCache, q: &str| -> i64 {
            let ast = parser::parse(q).unwrap();
            match Engine::new(view, cache).run(&ast).unwrap().rows[0][0] {
                Val::Int(n) => n,
                ref v => panic!("count not int: {v:?}"),
            }
        };
        let rows = |view: &MergedView, cache: &BlockCache, q: &str| -> usize {
            let ast = parser::parse(q).unwrap();
            Engine::new(view, cache).run(&ast).unwrap().rows.len()
        };

        // Untyped final hops qualify even on write_basic's two-reltype graph (total degree
        // == matching count). Fast `count(m)` must equal the materialised `RETURN m` rows.
        let (root, graph, _) = testgen::write_basic("degterm");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        {
            let view = MergedView::read_only(&gen);
            let eng = Engine::new(&view, &cache);
            let cases = [
                (
                    "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN count(m)",
                    "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN m",
                ),
                (
                    "MATCH (a:Person {name:'Alice'})-[]->()-[]->(m) RETURN count(m)",
                    "MATCH (a:Person {name:'Alice'})-[]->()-[]->(m) RETURN m",
                ),
                (
                    "MATCH (a:Person {name:'Alice'})-[]->()-[]->()-[]->(m) RETURN count(m)",
                    "MATCH (a:Person {name:'Alice'})-[]->()-[]->()-[]->(m) RETURN m",
                ),
                (
                    "MATCH (a:Person)-[]->(m) RETURN count(m)",
                    "MATCH (a:Person)-[]->(m) RETURN m",
                ),
                (
                    "MATCH (a:Person {name:'Alice'})-[]-(m) RETURN count(m)",
                    "MATCH (a:Person {name:'Alice'})-[]-(m) RETURN m",
                ),
            ];
            for (fast, refq) in cases {
                assert!(
                    eng.degree_terminal_dir(&pattern_of(fast)).is_some(),
                    "degree terminal must engage for `{fast}`"
                );
                assert_eq!(
                    count(&view, &cache, fast) as usize,
                    rows(&view, &cache, refq),
                    "count mismatch for `{fast}`"
                );
            }
            // Shapes that must decline (→ walk): typed final hop on a multi-reltype graph,
            // a filtered final node, a var-length hop, a path variable, a back-reference.
            for q in [
                "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(m) RETURN count(m)",
                "MATCH (a:Person {name:'Alice'})-[]->(m:Company) RETURN count(m)",
                "MATCH (a:Person {name:'Alice'})-[*1..2]->(m) RETURN count(m)",
                "MATCH p=(a:Person {name:'Alice'})-[]->(m) RETURN count(m)",
                "MATCH (a:Person {name:'Alice'})-[]->(a) RETURN count(a)",
            ] {
                assert!(
                    eng.degree_terminal_dir(&pattern_of(q)).is_none(),
                    "degree terminal must decline for `{q}`"
                );
            }
        }

        // Live delta of edge writes: the composed degree must reflect the born edges.
        {
            let mut mem = Memtable::new();
            for k in 0..3 {
                mem.upsert_edge(
                    "Person",
                    "name",
                    Value::Str("Alice".into()),
                    "KNOWS",
                    "Person",
                    "name",
                    Value::Str(format!("newpal{k}")),
                    Some(0),
                    None,
                    [],
                );
            }
            let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
            let eng = Engine::new(&view, &cache);
            let q = "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN count(m)";
            assert!(eng.degree_terminal_dir(&pattern_of(q)).is_some());
            assert_eq!(
                count(&view, &cache, q) as usize,
                rows(
                    &view,
                    &cache,
                    "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN m"
                ),
                "delta-composed count must match the walk"
            );
        }

        // Pending node-delete ⇒ decline (non-local), but the walk still counts correctly.
        {
            let mut mem = Memtable::new();
            mem.delete_node("Company", "name", Value::Str("Globex".into()), Some(4));
            let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
            let eng = Engine::new(&view, &cache);
            let q = "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN count(m)";
            assert!(
                eng.degree_terminal_dir(&pattern_of(q)).is_none(),
                "a pending node-delete must decline the degree terminal"
            );
            assert_eq!(
                count(&view, &cache, q) as usize,
                rows(
                    &view,
                    &cache,
                    "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN m"
                ),
            );
        }
        std::fs::remove_dir_all(&root).ok();
    }

    /// Slice 2: the hub routing probe [`Engine::effective_degree_ub`] is a **safe upper
    /// bound** — it never under-counts a real hub, so no hub is ever mistaken for a normal
    /// node and materialised. For every non-delta-tombstoned node the bound is ≥ the
    /// actual overlaid degree (out+in for undirected); a delta-tombstoned node reports 0
    /// (the documented "deleted, never expanded" contract).
    #[test]
    fn effective_degree_ub_never_undercounts() {
        use crate::read_view::MergedView;
        use slater_delta::{DeltaSnapshot, Memtable};
        use std::sync::Arc;

        let actual = |view: &MergedView, cache: &BlockCache, node: u64, dir: Direction| -> u64 {
            let deg = |outgoing: bool| {
                read_adj_overlaid(view, cache, node, outgoing)
                    .unwrap()
                    .len() as u64
            };
            match dir {
                Direction::Outgoing => deg(true),
                Direction::Incoming => deg(false),
                Direction::Undirected => deg(true) + deg(false),
            }
        };
        let check = |view: &MergedView, cache: &BlockCache, max_node: u64| {
            let engine = Engine::new(view, cache);
            for node in 0..=max_node {
                for dir in [
                    Direction::Outgoing,
                    Direction::Incoming,
                    Direction::Undirected,
                ] {
                    let ub = engine.effective_degree_ub(node, dir).unwrap();
                    if view.delta().is_tombstoned(node) {
                        assert_eq!(ub, 0, "delta-tombstoned node {node} probes to 0");
                    } else {
                        let got = actual(view, cache, node, dir);
                        assert!(
                            ub >= got,
                            "under-count node={node} dir={dir:?}: ub={ub} < actual={got}"
                        );
                    }
                }
            }
        };

        // Core-only: the bound is exact (no deletions to over-count).
        {
            let (root, graph, _) = testgen::write_basic("ub_core");
            let gen = Generation::open(&root, &graph).unwrap();
            let cache = BlockCache::new(1 << 20);
            let view = MergedView::read_only(&gen);
            check(&view, &cache, 4);
            std::fs::remove_dir_all(&root).ok();
        }
        // Core + delta with a born edge, an edge-delete, and a node-delete: core and delta
        // terms are exact, so the bound stays ≥ actual. (A *segment*-born edge below the
        // build floor is a documented, harmless under-count — the sidecar records only
        // `|Δ| >= floor` — so it is covered separately by
        // `segment_degree_delta_feeds_the_hub_probe`, not here.)
        {
            let (root, graph, _) = testgen::write_basic("ub_delta");
            let gen = Generation::open(&root, &graph).unwrap();
            let cache = BlockCache::new(1 << 20);
            let mut mem = Memtable::new();
            mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
            mem.upsert_node("Person", "name", Value::Str("Bob".into()), Some(1), []);
            mem.upsert_edge(
                "Person",
                "name",
                Value::Str("Alice".into()),
                "KNOWS",
                "Company",
                "name",
                Value::Str("Acme".into()),
                Some(0),
                Some(3),
                [],
            );
            mem.delete_edge(
                "Person",
                "name",
                Value::Str("Alice".into()),
                "KNOWS",
                "Person",
                "name",
                Value::Str("Bob".into()),
                Some(0),
                Some(1),
            );
            mem.delete_node("Company", "name", Value::Str("Globex".into()), Some(4));
            let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
            assert!(view.delta().is_tombstoned(4));
            check(&view, &cache, 4);
            std::fs::remove_dir_all(&root).ok();
        }
    }

    /// Slice 3: with a hub-degree sidecar present, [`Engine::effective_degree_ub`] takes
    /// its core term from the O(1) sidecar lookup — exact for a listed hub, `floor-1` for
    /// a node below the floor — instead of reading the record's leading count. Attaches a
    /// hand-written `hub_degrees.blk` to a `write_basic` fixture (node 0 out-degree 3;
    /// node 2 in-degree 2) and re-seals the manifest, then checks the accessors and probe.
    #[test]
    fn effective_degree_ub_uses_hub_sidecar() {
        use crate::read_view::MergedView;
        use graph_format::integrity::{content_hash, hash_file};
        use graph_format::manifest::{FileEntry, HubDegreeDesc, Manifest};

        let (root, graph, uuid) = testgen::write_basic("hub_sidecar_reader");
        let gendir = root.join(&graph).join(uuid.to_string());
        // write_basic: node 0 out-edges e0→1, e2→3, e4→2 (out-degree 3); node 2 in-edges
        // e1(1→2), e4(0→2) (in-degree 2). Floor 2 ⇒ out-hub {0:3}, in-hub {2:2}.
        graph_format::hubdegree::write_hub_degrees(
            gendir.join("hub_degrees.blk"),
            &[(0, 3)],
            &[(2, 2)],
            4096,
            3,
            None,
        )
        .unwrap();

        // Re-seal the (plaintext, MAC-less) manifest: add the file to the inventory,
        // recompute the content hash, and record the descriptor.
        let mut m = Manifest::read_from_dir(&gendir).unwrap();
        let p = gendir.join("hub_degrees.blk");
        m.files.push(FileEntry {
            name: "hub_degrees.blk".into(),
            bytes: std::fs::metadata(&p).unwrap().len(),
            blake3: hash_file(&p).unwrap(),
            sha256: None,
            crc32c: None,
        });
        m.files.sort_by(|a, b| a.name.cmp(&b.name));
        let inv: Vec<(String, String)> = m
            .files
            .iter()
            .map(|f| (f.name.clone(), f.blake3.clone()))
            .collect();
        m.content_hash = content_hash(&inv);
        m.hub_degrees = Some(HubDegreeDesc {
            floor: 2,
            out_hubs: 1,
            in_hubs: 1,
        });
        m.write_to_dir(&gendir).unwrap();

        let gen = Generation::open(&root, &graph).unwrap();
        assert_eq!(gen.hub_degree_floor(), Some(2));
        assert_eq!(gen.core_out_degree_if_hub(0), Some(3));
        assert_eq!(gen.core_out_degree_if_hub(1), None, "out-degree 1 < floor");
        assert_eq!(gen.core_in_degree_if_hub(2), Some(2));
        assert_eq!(gen.core_in_degree_if_hub(0), None);

        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);
        // Empty delta/segments ⇒ the UB is exactly the sidecar core term.
        assert_eq!(
            engine.effective_degree_ub(0, Direction::Outgoing).unwrap(),
            3
        );
        // Node 1 is not listed out ⇒ UB = floor-1 = 1 (never under-counts its real 1).
        assert_eq!(
            engine.effective_degree_ub(1, Direction::Outgoing).unwrap(),
            1
        );
        assert_eq!(
            engine.effective_degree_ub(2, Direction::Incoming).unwrap(),
            2
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// Slice 5: `directed_edge_count` consults the pinned hub sidecar *before* the chunk-lazy
    /// dense column, so a mega-hub's degree is answered from the resident sidecar and faults no
    /// dense chunk. Builds a `write_basic` fixture with BOTH `hub_degrees.blk` (floor 2 ⇒ out-hub
    /// {0:3}) and the dense `node_degrees.blk`, then asserts: a hub lookup returns the exact
    /// degree with zero resident chunks; a non-hub lookup (below the floor) does fault its chunk.
    #[test]
    fn hub_lookup_skips_dense_chunk_fault() {
        use crate::read_view::MergedView;
        use graph_format::integrity::{content_hash, hash_file};
        use graph_format::manifest::{FileEntry, HubDegreeDesc, Manifest};

        let (root, graph, uuid) = testgen::write_basic("hub_before_dense");
        let gendir = root.join(&graph).join(uuid.to_string());
        // write_basic degrees: out=[3,1,1,0,0], in=[0,1,2,1,1] over 5 nodes.
        graph_format::hubdegree::write_hub_degrees(
            gendir.join("hub_degrees.blk"),
            &[(0, 3)],
            &[(2, 2)],
            4096,
            3,
            None,
        )
        .unwrap();
        graph_format::nodedegree::write_node_degrees(
            gendir.join("node_degrees.blk"),
            &[3, 1, 1, 0, 0],
            &[0, 1, 2, 1, 1],
            4096,
            3,
            None,
        )
        .unwrap();

        // Re-seal the plaintext manifest: add both files to the inventory, record the sidecar
        // descriptor, and recompute the content hash.
        let mut m = Manifest::read_from_dir(&gendir).unwrap();
        for name in ["hub_degrees.blk", "node_degrees.blk"] {
            let p = gendir.join(name);
            m.files.push(FileEntry {
                name: name.into(),
                bytes: std::fs::metadata(&p).unwrap().len(),
                blake3: hash_file(&p).unwrap(),
                sha256: None,
                crc32c: None,
            });
        }
        m.files.sort_by(|a, b| a.name.cmp(&b.name));
        let inv: Vec<(String, String)> = m
            .files
            .iter()
            .map(|f| (f.name.clone(), f.blake3.clone()))
            .collect();
        m.content_hash = content_hash(&inv);
        m.hub_degrees = Some(HubDegreeDesc {
            floor: 2,
            out_hubs: 1,
            in_hubs: 1,
        });
        m.write_to_dir(&gendir).unwrap();

        let gen = Generation::open(&root, &graph).unwrap();
        assert_eq!(gen.degree_column_resident_chunks(), Some(0), "cold at open");
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);

        // Node 0 is an out-hub ⇒ answered by the sidecar, exact, no dense chunk faulted.
        assert_eq!(engine.directed_edge_count(0, true).unwrap(), 3);
        assert_eq!(
            gen.degree_column_resident_chunks(),
            Some(0),
            "hub answered from the sidecar must not fault a dense chunk"
        );

        // Node 1 (out-degree 1 < floor) is not a hub ⇒ falls through to the dense column,
        // which faults its chunk. Value is exact.
        assert_eq!(engine.directed_edge_count(1, true).unwrap(), 1);
        assert_eq!(
            gen.degree_column_resident_chunks(),
            Some(1),
            "a non-hub lookup faults the dense chunk"
        );
        // Node 2 in-degree 2 is an in-hub ⇒ sidecar again, no new (in-half) chunk faulted.
        assert_eq!(engine.directed_edge_count(2, false).unwrap(), 2);
        assert_eq!(gen.degree_column_resident_chunks(), Some(1));

        std::fs::remove_dir_all(&root).ok();
    }

    /// Slice 4: a flush that borns many edges from one node records that node's out-degree
    /// delta in the segment manifest (`|Δ| >= floor`), the `CoreStack` fold sums it, and
    /// `effective_degree_ub` adds it to the core term — the O(#segments) segment path of the
    /// hub probe, end to end (write → flush → segment manifest → fold → probe).
    #[test]
    fn segment_degree_delta_feeds_the_hub_probe() {
        use crate::cache::VectorIndexCache;
        use crate::config::DeltaConfig;
        use crate::read_view::MergedView;
        use crate::server::{execute_edge_write, Graphs};
        use std::collections::HashMap;

        let floor = graph_format::hubdegree::DEFAULT_HUB_DEGREE_FLOOR as u64;
        let born = floor + 6; // 1030 born out-edges from Alice ⇒ Δ = 1030 >= floor
        let (root, graph, _) = testgen::write_basic("seg_degree_delta");
        let wal = root.join("_wal");
        let cfg = DeltaConfig {
            enabled: true,
            wal_dir: wal.to_string_lossy().into_owned(),
            memtable_bytes: 256 << 20,
            l0_compaction_trigger: 0,
            segment_flush_bytes: 0,
            max_upper_segments: 0,
            delta_core_percent: 0,
            delta_hard_bytes: 0,
            consolidate_window: String::new(),
            builder_bin: "slater-build".to_string(),
            off_heap_l0: false,
            segment_gc_grace_secs: 0,
        };
        let vc = VectorIndexCache::new(1 << 20);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs.enable_writable_layer(&cfg, &root, None).unwrap();
        {
            let gen = graphs.get(&graph).unwrap();
            let writer = graphs.writer(&graph).unwrap();
            for k in 0..born {
                let q = format!(
                    "MERGE (a:Person {{name:'Alice'}})-[:KNOWS]->(c:Person {{name:'hubleaf{k}'}})"
                );
                match parser::parse_statement(&q).unwrap() {
                    parser::ast::Statement::WriteEdge(w) => {
                        execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                    }
                    other => panic!("expected an edge write, got {other:?}"),
                }
            }
        }
        graphs
            .flush_graph_to_segment(&graph, &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes to a segment");

        let gen = graphs.get(&graph).unwrap();
        assert_eq!(gen.stack().segments().len(), 1);
        // The segment manifest records Alice (node 0) with the exact out-degree delta.
        let out_deltas = &gen.stack().segments()[0].manifest.hub_degree_out_deltas;
        assert_eq!(
            out_deltas.iter().find(|(id, _)| *id == 0).map(|(_, d)| *d),
            Some(born as i64),
            "segment out-degree delta for Alice: {out_deltas:?}"
        );
        // The fold sums it; the probe adds it to the (block-peek) core term (no core sidecar
        // on this fixture): core out-degree 3 + segment Δ = 3 + born.
        assert_eq!(gen.stack().hub_out_degree_delta(0), born as i64);
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(gen.as_ref());
        let engine = Engine::new(&view, &cache);
        assert_eq!(
            engine.effective_degree_ub(0, Direction::Outgoing).unwrap(),
            3 + born,
        );
        // With a low stream threshold the node is now a hub via the segment delta alone.
        let hub_engine = Engine::new(&view, &cache).with_adj_stream_threshold(floor);
        assert!(hub_engine.is_hub(0, Direction::Outgoing).unwrap());
        std::fs::remove_dir_all(&root).ok();
    }

    /// A segment full row overrides/extends the base node reads it carries, births new
    /// entities, and tombstones nodes — through both `node_record` (all-props) and the
    /// single-property path. This is the read oracle for slice 3.2.
    #[test]
    fn segment_full_row_overrides_and_extends_reads() {
        use crate::read_view::MergedView;
        let (root, graph, set_uuid) = write_basic_with_segment("seg_full_row_reads");
        let gen = Generation::open(&root, &graph).unwrap();
        assert_eq!(gen.uuid(), GenId(set_uuid));
        assert_eq!(gen.stack().segments().len(), 1);
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);

        // Overridden node 0: full-row replace — age 30→99, new `mood`, and `city`/`team` gone.
        let (labels0, p0) = engine.node_record(0).unwrap();
        assert_eq!(labels0, vec!["Person".to_string()]);
        assert!(matches!(prop(&p0, "name"), Some(Val::Str(s)) if s == "Alice"));
        assert!(matches!(prop(&p0, "age"), Some(Val::Int(99))), "{p0:?}");
        assert!(matches!(prop(&p0, "mood"), Some(Val::Str(s)) if s == "calm"));
        assert!(
            prop(&p0, "city").is_none(),
            "full-row replace drops base props: {p0:?}"
        );
        assert!(prop(&p0, "team").is_none(), "{p0:?}");
        // Single-property path agrees, including the non-core-symbol key `mood`.
        assert!(matches!(engine.node_prop(0, "age").unwrap(), Val::Int(99)));
        assert!(matches!(engine.node_prop(0, "mood").unwrap(), Val::Str(s) if s == "calm"));
        assert!(matches!(engine.node_prop(0, "city").unwrap(), Val::Null));

        // Born node 5.
        let (labels5, p5) = engine.node_record(5).unwrap();
        assert_eq!(labels5, vec!["Person".to_string()]);
        assert!(matches!(prop(&p5, "name"), Some(Val::Str(s)) if s == "Zed"));
        assert!(matches!(engine.node_prop(5, "age").unwrap(), Val::Int(50)));

        // Tombstoned node 2: no labels, no props.
        let (labels2, p2) = engine.node_record(2).unwrap();
        assert!(
            labels2.is_empty() && p2.is_empty(),
            "tombstoned: {labels2:?} {p2:?}"
        );

        // Untouched base node 1 reads straight from the base.
        let (_l1, p1) = engine.node_record(1).unwrap();
        assert!(matches!(prop(&p1, "age"), Some(Val::Int(25))));
        assert!(matches!(prop(&p1, "city"), Some(Val::Str(s)) if s == "London"));

        // Born edge 5 resolves its full row; base edge 0 is untouched.
        let knows = gen.reltype_id("KNOWS").unwrap();
        let (t5, ep5) = engine.rel_record(5, knows).unwrap();
        assert_eq!(t5, "KNOWS");
        assert!(
            matches!(prop(&ep5, "since"), Some(Val::Int(2099))),
            "{ep5:?}"
        );
        assert!(matches!(
            engine.edge_prop(5, "since").unwrap(),
            Val::Int(2099)
        ));
        let (_t0, ep0) = engine.rel_record(0, knows).unwrap();
        assert!(
            matches!(prop(&ep0, "since"), Some(Val::Int(2020))),
            "{ep0:?}"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// The write-delta sits above the segment stack: a delta patch wins over a segment full
    /// row (delta > segment > base), for both the all-props and single-property paths.
    #[test]
    fn delta_wins_over_segment_full_row() {
        use crate::read_view::MergedView;
        use slater_delta::{DeltaSnapshot, Memtable};
        use std::sync::Arc;

        let (root, graph, _) = write_basic_with_segment("seg_delta_precedence");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        // Patch node 0 (already segment-overridden to age 99): the delta sets age 7.
        let mut mem = Memtable::new();
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Alice".into()),
            Some(0),
            [("age".to_string(), Value::Int(7))],
        );
        let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let engine = Engine::new(&view, &cache);

        let (_l0, p0) = engine.node_record(0).unwrap();
        assert!(
            matches!(prop(&p0, "age"), Some(Val::Int(7))),
            "delta wins: {p0:?}"
        );
        // The segment's other props still show through where the delta is silent.
        assert!(matches!(prop(&p0, "mood"), Some(Val::Str(s)) if s == "calm"));
        assert!(matches!(engine.node_prop(0, "age").unwrap(), Val::Int(7)));
        std::fs::remove_dir_all(&root).ok();
    }

    /// A segment's adjacency fragments fold over the base neighbour list: a `removed` entry
    /// suppresses a base edge, a born entry appends one, and an untouched node reads its base
    /// adjacency unchanged (its fence skips the segment). The read oracle for slice 3.3.
    #[test]
    fn segment_adjacency_fragments_merge_over_base() {
        use crate::read_view::MergedView;
        let (root, graph, _) = write_basic_with_segment("seg_adjacency");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);
        let knows = gen.reltype_id("KNOWS").unwrap();
        let works = gen.reltype_id("WORKS_AT").unwrap();

        let triples = |adj: &[topology::Adj]| -> Vec<(u64, u32, u64)> {
            let mut v: Vec<_> = adj
                .iter()
                .map(|a| (a.neighbour.0, a.reltype, a.edge.0))
                .collect();
            v.sort();
            v
        };

        // Base node 0 out-edges: →1 (KNOWS e0), →3 (WORKS_AT e2), →2 (KNOWS e4). The segment
        // removes e4 and adds e5 (→5 KNOWS).
        assert_eq!(
            triples(&engine.outgoing(0).unwrap()),
            vec![(1, knows, 0), (3, works, 2), (5, knows, 5)],
        );
        // Incoming to born node 5 is the born edge alone (no base row for a synthetic id).
        assert_eq!(triples(&engine.incoming(5).unwrap()), vec![(0, knows, 5)]);
        // A node with no fragment in the segment reads its base adjacency unchanged.
        assert_eq!(
            triples(&engine.outgoing(1).unwrap()),
            vec![(2, knows, 1)], // base edge e1: 1→2 KNOWS
        );

        // Under a delta that adds one more out-edge from node 0, all three layers compose.
        use slater_delta::{DeltaSnapshot, Memtable};
        use std::sync::Arc;
        let mut mem = Memtable::new();
        mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
        // A second, delta-born out-edge from node 0: 0→3 (Acme) KNOWS.
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Alice".into()),
            "KNOWS",
            "Company",
            "name",
            Value::Str("Acme".into()),
            Some(0),
            Some(3),
            [],
        );
        let dview = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let deng = Engine::new(&dview, &cache);
        let out0 = deng.outgoing(0).unwrap();
        // base e0(→1), base e2(→3 WORKS_AT), segment e5(→5), delta born(→3 KNOWS); e4 gone.
        assert_eq!(out0.len(), 4, "{:?}", triples(&out0));
        assert!(out0
            .iter()
            .any(|a| a.neighbour.0 == 5 && a.reltype == knows));
        assert!(
            !out0.iter().any(|a| a.edge.0 == 4),
            "removed edge stays gone under a delta"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// The scan_candidates seam merges segment index fragments (base hits minus removals ∪
    /// the segments' matching born/patched ids), recomputes label membership over segment
    /// full rows, and unions endpoint postings — with tombstoned nodes suppressed. The read
    /// oracle for slice 3.4.
    #[test]
    fn segment_index_label_and_reltype_scans_merge() {
        use crate::plan::NodeScan;
        use crate::read_view::MergedView;
        let (root, graph, _) = write_basic_with_segment("seg_scans");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);

        let eq = |age: i64| -> Vec<u64> {
            let mut v = engine
                .scan_candidates(&NodeScan::RangeEq {
                    index: "node_Person_age".into(),
                    key: Value::Int(age),
                })
                .unwrap();
            v.sort_unstable();
            v
        };
        // Node 0's age moved 30→99 (found at 99, gone at 30); node 5 born at 50; node 2
        // (age 40) tombstoned, so its stale base entry is suppressed by the removal sidecar.
        assert_eq!(eq(99), vec![0]);
        assert_eq!(eq(30), Vec::<u64>::new());
        assert_eq!(eq(50), vec![5]);
        assert_eq!(eq(40), Vec::<u64>::new());
        assert_eq!(eq(25), vec![1]); // untouched base node Bob

        // Range: age >= 45 → the moved node 0 (99) and born node 5 (50); base 30/25/40 excluded.
        let mut rng = engine
            .scan_candidates(&NodeScan::RangeRange {
                index: "node_Person_age".into(),
                lo: Some((Value::Int(45), true)),
                hi: None,
            })
            .unwrap();
        rng.sort_unstable();
        assert_eq!(rng, vec![0, 5]);

        // Label scan: Person = {Alice(0, overridden, still Person), Bob(1), Zed(5, born)};
        // Carol(2) tombstoned and dropped.
        let person = gen.label_id("Person").unwrap();
        let mut labs = engine
            .scan_candidates(&NodeScan::LabelScan { label_id: person })
            .unwrap();
        labs.sort_unstable();
        assert_eq!(labs, vec![0, 1, 5]);
        // (RelTypeScan's segment-posting union is exercised in
        // `segment_reltype_scan_unions_postings`, which uses a base fixture carrying the
        // endpoint postings a `RelTypeScan` requires.)

        std::fs::remove_dir_all(&root).ok();
    }

    /// Stack a **births-only** segment (no tombstones/removals, so its marginals are trivially
    /// self-consistent) over a `write_basic` base: born node 5 (`:Person {name:'Zed'}`) and
    /// born edge 5 (`(0)-[:KNOWS]->(5)`) with adjacency. Returns `(root, graph, seg_uuid)`.
    fn write_basic_with_born_segment(tag: &str) -> (std::path::PathBuf, String, uuid::Uuid) {
        use graph_format::manifest::FileEntry;
        use graph_format::segmanifest::{SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION};
        use graph_format::segment::{AdjEdge, EdgeRow, NodeRow, SegmentWriter};
        use graph_format::setmanifest::{SegmentRef, SetManifest};

        let (root, graph, base_uuid) = testgen::write_basic(tag);
        let seg_uuid = uuid::Uuid::from_u128(0x5_5eb0_0000_0000_0000_0000_0000_0001);
        let set_uuid = uuid::Uuid::from_u128(0x5_5eb1_0000_0000_0000_0000_0000_0001);
        let seg_dir = root
            .join(&graph)
            .join("segments")
            .join(seg_uuid.to_string());
        std::fs::create_dir_all(seg_dir.parent().unwrap()).unwrap();
        let mut w = SegmentWriter::create(&seg_dir, 0x44, 4096, 3).unwrap();
        w.push_node(
            5,
            &NodeRow {
                labels: vec!["Person".into()],
                props: vec![("name".into(), Value::Str("Zed".into()))],
                tombstoned: false,
            },
        )
        .unwrap();
        w.push_adj_out(
            0,
            &[AdjEdge {
                other: 5,
                reltype: "KNOWS".into(),
                edge_id: 5,
                removed: false,
            }],
        )
        .unwrap();
        w.push_adj_in(
            5,
            &[AdjEdge {
                other: 0,
                reltype: "KNOWS".into(),
                edge_id: 5,
                removed: false,
            }],
        )
        .unwrap();
        w.push_edge(
            5,
            &EdgeRow {
                src: 0,
                dst: 5,
                reltype: "KNOWS".into(),
                props: vec![],
                tombstoned: false,
            },
        )
        .unwrap();
        w.finish().unwrap();

        let mut m = SegmentManifest {
            magic: SEGMENT_MAGIC.into(),
            version: SEGMENT_MANIFEST_VERSION,
            segment_uuid: GenId(seg_uuid),
            base: GenId(base_uuid),
            created_unix: 0,
            node_band: (5, 6),
            edge_band: (5, 6),
            content_hash: String::new(),
            encryption: None,
            node_count_delta: 1,
            edge_count_delta: 1,
            reltype_edge_deltas: vec![("KNOWS".into(), 1)],
            label_node_deltas: vec![("Person".into(), 1)],
            hub_degree_out_deltas: vec![],
            hub_degree_in_deltas: vec![],
            marginals_exact: true,
            dirty_indexes: vec![],
            label_membership_touch: None,
            mac: None,
            files: vec![FileEntry {
                name: "node.blk".into(),
                bytes: 0,
                blake3: "aa".into(),
                sha256: None,
                crc32c: None,
            }],
        };
        m.set_content_hash();
        m.write_to_dir(&seg_dir).unwrap();
        let sets = root.join(&graph).join("sets");
        std::fs::create_dir_all(&sets).unwrap();
        let mut set = SetManifest::singleton(GenId(base_uuid), 0);
        set.set_uuid = GenId(set_uuid);
        set.segments = vec![SegmentRef::from_manifest(&m)];
        std::fs::write(
            sets.join(format!("{set_uuid}.json")),
            set.to_bytes().unwrap(),
        )
        .unwrap();
        std::fs::write(root.join(&graph).join("current"), set_uuid.to_string()).unwrap();
        (root, graph, seg_uuid)
    }

    /// Whole-graph counts are answered from the summed segment marginals (node/label/edge/
    /// reltype), and a segment whose marginals are not exact declines to full execution —
    /// which is segment-aware and yields the same answer. The read oracle for slice 3.5.
    #[test]
    fn segment_marginals_answer_counts_and_decline_when_inexact() {
        use crate::read_view::MergedView;
        use graph_format::segmanifest::SegmentManifest;
        let (root, graph, seg_uuid) = write_basic_with_born_segment("seg_counts");
        let seg_dir = root
            .join(&graph)
            .join("segments")
            .join(seg_uuid.to_string());
        let cache = BlockCache::new(1 << 20);

        let count = |view: &MergedView, q: &str| -> i64 {
            let res = Engine::new(view, &cache)
                .run(&parser::parse(q).unwrap())
                .unwrap();
            match res.rows[0][0] {
                Val::Int(n) => n,
                ref v => panic!("expected Int, got {v:?}"),
            }
        };
        let reltype_groups = |view: &MergedView| -> Vec<(String, i64)> {
            let res = Engine::new(view, &cache)
                .run(&parser::parse("MATCH ()-[r]->() RETURN type(r), count(*)").unwrap())
                .unwrap();
            let mut g: Vec<(String, i64)> = res
                .rows
                .iter()
                .map(|r| match (&r[0], &r[1]) {
                    (Val::Str(s), Val::Int(c)) => (s.clone(), *c),
                    other => panic!("{other:?}"),
                })
                .collect();
            g.sort();
            g
        };

        // Live estate = base 5 nodes + Zed(5); base 5 edges + e5. Answered from marginals.
        let gen = Generation::open(&root, &graph).unwrap();
        {
            let view = MergedView::read_only(&gen);
            assert_eq!(count(&view, "MATCH (n) RETURN count(*)"), 6);
            assert_eq!(count(&view, "MATCH (n:Person) RETURN count(*)"), 4); // + Zed
            assert_eq!(count(&view, "MATCH (n:Company) RETURN count(*)"), 2); // untouched
            assert_eq!(count(&view, "MATCH ()-[r]->() RETURN count(*)"), 6);
            // KNOWS = e0,e1,e4,e5 = 4; WORKS_AT = e2,e3 = 2.
            assert_eq!(
                reltype_groups(&view),
                vec![("KNOWS".to_string(), 4), ("WORKS_AT".to_string(), 2)]
            );
        }

        // Flip the segment's marginals to inexact: the count fast paths must decline and full
        // execution (segment-aware) must still return the same answers.
        let mut m = SegmentManifest::read_from_dir(&seg_dir).unwrap();
        m.marginals_exact = false;
        m.write_to_dir(&seg_dir).unwrap();
        let gen2 = Generation::open(&root, &graph).unwrap();
        let view2 = MergedView::read_only(&gen2);
        assert_eq!(
            count(&view2, "MATCH (n) RETURN count(*)"),
            6,
            "decline → full exec"
        );
        assert_eq!(count(&view2, "MATCH (n:Person) RETURN count(*)"), 4);
        assert_eq!(count(&view2, "MATCH ()-[r]->() RETURN count(*)"), 6);
        assert_eq!(
            reltype_groups(&view2),
            vec![("KNOWS".to_string(), 4), ("WORKS_AT".to_string(), 2)]
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// A `RelTypeScan` unions each segment's endpoint driving set over the base postings
    /// (over-inclusion is safe — the first hop re-filters by reltype). Uses a base fixture
    /// that carries the endpoint postings a `RelTypeScan` needs.
    #[test]
    fn segment_reltype_scan_unions_postings() {
        use crate::plan::NodeScan;
        use crate::read_view::MergedView;
        use graph_format::manifest::FileEntry;
        use graph_format::segmanifest::{SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION};
        use graph_format::segment::{NodeRow, SegmentWriter};
        use graph_format::segpostings::{write_posting_fragments, PostingSpec};
        use graph_format::setmanifest::{SegmentRef, SetManifest};

        let (root, graph) = testgen::write_rel_sparse("seg_reltype_scan");
        let base_uuid = Generation::current_uuid(&root, &graph).unwrap();
        let seg_uuid = uuid::Uuid::from_u128(0x5_5e60_0000_0000_0000_0000_0000_0009);
        let set_uuid = uuid::Uuid::from_u128(0x5_5e70_0000_0000_0000_0000_0000_0009);

        // A segment that births node 6 (:N) with a new outgoing T-edge, so its endpoint
        // posting adds node 6 to T's source driving set (base T sources are {0,1}).
        let seg_dir = root
            .join(&graph)
            .join("segments")
            .join(seg_uuid.to_string());
        std::fs::create_dir_all(seg_dir.parent().unwrap()).unwrap();
        let mut w = SegmentWriter::create(&seg_dir, 0x33, 4096, 3).unwrap();
        w.push_node(
            6,
            &NodeRow {
                labels: vec!["N".into()],
                props: vec![("name".into(), Value::Str("g".into()))],
                tombstoned: false,
            },
        )
        .unwrap();
        w.finish().unwrap();
        write_posting_fragments(
            &seg_dir,
            &[PostingSpec {
                reltype: "T".into(),
                src_ids: vec![6],
                tgt_ids: vec![],
            }],
        )
        .unwrap();

        let mut m = SegmentManifest {
            magic: SEGMENT_MAGIC.into(),
            version: SEGMENT_MANIFEST_VERSION,
            segment_uuid: GenId(seg_uuid),
            base: GenId(base_uuid),
            created_unix: 0,
            node_band: (6, 7),
            edge_band: (3, 3),
            content_hash: String::new(),
            encryption: None,
            node_count_delta: 1,
            edge_count_delta: 0,
            reltype_edge_deltas: vec![],
            label_node_deltas: vec![("N".into(), 1)],
            hub_degree_out_deltas: vec![],
            hub_degree_in_deltas: vec![],
            marginals_exact: true,
            dirty_indexes: vec![],
            label_membership_touch: None,
            mac: None,
            files: vec![FileEntry {
                name: "node.blk".into(),
                bytes: 0,
                blake3: "aa".into(),
                sha256: None,
                crc32c: None,
            }],
        };
        m.set_content_hash();
        m.write_to_dir(&seg_dir).unwrap();
        let sets = root.join(&graph).join("sets");
        std::fs::create_dir_all(&sets).unwrap();
        let mut set = SetManifest::singleton(GenId(base_uuid), 0);
        set.set_uuid = GenId(set_uuid);
        set.segments = vec![SegmentRef::from_manifest(&m)];
        std::fs::write(
            sets.join(format!("{set_uuid}.json")),
            set.to_bytes().unwrap(),
        )
        .unwrap();
        std::fs::write(root.join(&graph).join("current"), set_uuid.to_string()).unwrap();

        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);
        let t = gen.reltype_id("T").unwrap();
        let mut srcs = engine
            .scan_candidates(&NodeScan::RelTypeScan {
                reltype_ids: vec![t],
                side: RelEndpointSide::Source,
                guaranteed_label: None,
            })
            .unwrap();
        srcs.sort_unstable();
        assert_eq!(
            srcs,
            vec![0, 1, 6],
            "base T sources {{0,1}} ∪ segment {{6}}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// `algo.*` procedures build their subgraph view over the *effective* estate: the
    /// label-filtered node set now includes a segment-born node carrying the label (it went
    /// through the base label postings only before slice 3.6's fix). Regression guard for the
    /// adversarial-review finding.
    #[test]
    fn algo_view_includes_segment_born_labelled_node() {
        use crate::read_view::MergedView;
        let (root, graph, _) = write_basic_with_born_segment("seg_algo_view");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);

        // Base :Person = {Alice, Bob, Carol}; the segment births Zed (:Person). The WCC view
        // over :Person must span all four, so the row count is 4, not the base-only 3.
        let res = engine
            .run(
                &parser::parse(
                    "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node, componentId \
                     RETURN count(*)",
                )
                .unwrap(),
            )
            .unwrap();
        assert!(
            matches!(res.rows[0][0], Val::Int(4)),
            "{:?}",
            res.rows[0][0]
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// A stacked set opens and answers queries identically through a non-filesystem backend
    /// (mem store), exercising the store-native segment reader path end-to-end (the segments
    /// live on the same object store as the base). Conformance for slice 3.6.
    #[test]
    fn stacked_set_opens_and_reads_over_mem_store() {
        use crate::read_view::MergedView;
        use graph_format::store::mem::MemObjectStore;
        use graph_format::store::ObjectStore;

        fn load_tree(store: &MemObjectStore, root: &std::path::Path, dir: &std::path::Path) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    load_tree(store, root, &path);
                } else {
                    let key = path
                        .strip_prefix(root)
                        .unwrap()
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    store
                        .put(&key, &std::fs::read(&path).unwrap(), None)
                        .unwrap();
                }
            }
        }

        let (root, graph, _) = write_basic_with_born_segment("seg_mem_store");
        let mem = MemObjectStore::new();
        load_tree(&mem, &root, &root);

        let gen = Generation::open_with_store(&mem, &graph, None).unwrap();
        assert_eq!(
            gen.stack().segments().len(),
            1,
            "segment loaded via the mem store"
        );
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);

        // Born node 5 reads its full row through the store; whole-graph count is marginal-summed.
        let (labels, props) = engine.node_record(5).unwrap();
        assert_eq!(labels, vec!["Person".to_string()]);
        assert!(matches!(prop(&props, "name"), Some(Val::Str(s)) if s == "Zed"));
        let res = engine
            .run(&parser::parse("MATCH (n) RETURN count(*)").unwrap())
            .unwrap();
        assert!(matches!(res.rows[0][0], Val::Int(6)));
        // Its born adjacency resolves too.
        let knows = gen.reltype_id("KNOWS").unwrap();
        assert!(engine
            .incoming(5)
            .unwrap()
            .iter()
            .any(|a| a.neighbour.0 == 0 && a.reltype == knows));

        std::fs::remove_dir_all(&root).ok();
    }

    /// Every pure scalar function delegated to `slater-scalar` must still be
    /// advertised by `CALL dbms.functions()` (the registry the planner validates
    /// against), so the extraction did not silently drop a name.
    #[test]
    fn pure_functions_are_advertised() {
        for name in slater_scalar::PURE_FUNCTIONS {
            assert!(
                IMPLEMENTED_FUNCTIONS.contains(name),
                "slater-scalar advertises `{name}` but IMPLEMENTED_FUNCTIONS does not"
            );
        }
    }

    /// Smoke-test the delegation path: a scalar call routes through `slater-scalar`
    /// and a `coalesce` over a runtime-only `Val` still uses the local fallback.
    #[test]
    fn scalar_delegation_and_runtime_fallback() {
        let (root, graph, _) = testgen::write_basic("scalar_delegation");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(64 << 20);
        let eng = Engine::new(&gen, &cache);
        // delegated to slater-scalar (compare via to_display — Val is not PartialEq)
        assert_eq!(
            eng.call_function("toUpper", false, vec![Val::Str("ab".into())])
                .unwrap()
                .to_display(),
            "AB"
        );
        assert_eq!(
            eng.call_function("round", false, vec![Val::Float(2.5)])
                .unwrap()
                .to_display(),
            "3"
        );
        // coalesce with a runtime-only first arg keeps the local fallback (returns
        // the node, which has no `Value` projection)
        assert!(matches!(
            eng.call_function("coalesce", false, vec![Val::Node(7), Val::Null])
                .unwrap(),
            Val::Node(7)
        ));
    }

    /// Open the shared fixture and run `q`, returning the result.
    fn run(root_tag: &str, q: &str) -> (std::path::PathBuf, QueryResult) {
        let (root, graph, _) = testgen::write_basic(root_tag);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let ast = parser::parse(q).unwrap();
        let res = engine.run(&ast).unwrap();
        (root, res)
    }

    /// Single-column results as a sorted Vec of display strings, for order-free
    /// assertions.
    fn col0(res: &QueryResult) -> Vec<String> {
        let mut v: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
        v.sort();
        v
    }

    /// All rows as display strings, sorted, for order-free whole-result equality.
    fn rows_disp(res: &QueryResult) -> Vec<Vec<String>> {
        let mut v: Vec<Vec<String>> = res
            .rows
            .iter()
            .map(|r| r.iter().map(|c| c.to_display()).collect())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn power_operator_and_float_literals_eval() {
        // `^` always yields a Float, even for integer operands (Neo4j semantics),
        // and the new float lexis (`1e3`, `.5`) evaluates to the right numbers.
        let (root, res) = run(
            "exec_pow",
            "RETURN 2 ^ 3 AS a, 2 ^ 10 AS b, -2 ^ 2 AS c, 2 ^ 3 ^ 2 AS d, \
             1e3 AS e, .5 AS f, 4 ^ 0.5 AS g",
        );
        let r = &res.rows[0];
        let f = |v: &Val| match v {
            Val::Float(x) => *x,
            other => panic!("expected Float, got {other:?}"),
        };
        assert_eq!(f(&r[0]), 8.0);
        assert_eq!(f(&r[1]), 1024.0);
        assert_eq!(f(&r[2]), 4.0); // (-2) ^ 2
        assert_eq!(f(&r[3]), 64.0); // (2 ^ 3) ^ 2, left-assoc
        assert_eq!(f(&r[4]), 1000.0);
        assert_eq!(f(&r[5]), 0.5);
        assert_eq!(f(&r[6]), 2.0); // 4 ^ 0.5 == sqrt(4)
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn trailing_semicolon_is_accepted() {
        let (root, res) = run("exec_semi", "MATCH (n) RETURN count(*) AS c;");
        assert!(matches!(res.rows[0][0], Val::Int(5)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn all_nodes_scan_counts() {
        let (root, res) = run("exec_count_all", "MATCH (n) RETURN count(*) AS c");
        assert_eq!(res.columns, vec!["c"]);
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(5)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn label_scan_with_projection() {
        let (root, res) = run("exec_label", "MATCH (n:Person) RETURN n.name AS name");
        assert_eq!(res.columns, vec!["name"]);
        assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn label_count_uses_fast_path() {
        // Stage 3: `MATCH (n:Person) RETURN count(*)` reads the label posting length
        // (3 Person nodes in the fixture) without materialising rows.
        let (root, res) = run("exec_count_label", "MATCH (n:Person) RETURN count(*) AS c");
        assert_eq!(res.columns, vec!["c"]);
        assert!(
            matches!(res.rows[0][0], Val::Int(3)),
            "{:?}",
            res.rows[0][0]
        );
        let _ = std::fs::remove_dir_all(&root);

        // count(n) over the same pattern is identical.
        let (root, res) = run(
            "exec_count_label_n",
            "MATCH (n:Person) RETURN count(n) AS c",
        );
        assert!(matches!(res.rows[0][0], Val::Int(3)));
        let _ = std::fs::remove_dir_all(&root);

        // An unknown label counts zero (not an error, not a full scan).
        let (root, res) = run("exec_count_unknown", "MATCH (n:Nope) RETURN count(*) AS c");
        assert!(matches!(res.rows[0][0], Val::Int(0)));
        let _ = std::fs::remove_dir_all(&root);
    }

    // ---- whole-graph label/reltype metadata fast paths (Stage M) ----

    /// Open the richer metadata fixture (multi-label node, no-label node, self-loop).
    fn meta_gen(tag: &str) -> (std::path::PathBuf, Generation) {
        let (root, graph, _) = testgen::write_meta(tag);
        let gen = Generation::open(&root, &graph).unwrap();
        (root, gen)
    }

    #[test]
    fn meta_reltype_enumeration_and_grouped_counts() {
        let (root, gen) = meta_gen("meta_reltype");
        let cache = BlockCache::new(1 << 20);
        let eng = Engine::new(&gen, &cache);
        let run = |q: &str| eng.run(&parser::parse(q).unwrap()).unwrap();

        // A1 — DISTINCT type(r): the reltype list.
        let a1 = run("MATCH ()-[r]->() RETURN DISTINCT type(r) AS t");
        assert_eq!(a1.columns, vec!["t"]);
        assert_eq!(col0(&a1), vec!["KNOWS", "OWNS", "WORKS_AT"]);

        // B1 — type(r), count(*): edges per reltype (KNOWS 2, WORKS_AT 2, OWNS 1).
        let b1 = run("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c");
        assert_eq!(
            rows_disp(&b1),
            vec![
                vec!["KNOWS".to_string(), "2".to_string()],
                vec!["OWNS".to_string(), "1".to_string()],
                vec!["WORKS_AT".to_string(), "2".to_string()],
            ]
        );

        // Reverse arrow gives the same totals; count(r) == count(*).
        let b1r = run("MATCH ()<-[r]-() RETURN type(r) AS t, count(r) AS c");
        assert_eq!(rows_disp(&b1r), rows_disp(&b1));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn meta_first_label_enumeration_and_counts() {
        let (root, gen) = meta_gen("meta_label");
        let cache = BlockCache::new(1 << 20);
        let eng = Engine::new(&gen, &cache);
        let run = |q: &str| eng.run(&parser::parse(q).unwrap()).unwrap();

        // A2 — DISTINCT labels(n)[0]: includes the null bucket (the label-less node).
        let a2 = run("MATCH (n) RETURN DISTINCT labels(n)[0] AS l");
        assert_eq!(col0(&a2), vec!["Admin", "Company", "Person", "null"]);

        // B2 — labels(n)[0], count(*): Person 2 (Alice+Bob first-label), Admin 1
        // (Carol), Company 1 (Acme), null 1 (Ghost).
        let b2 = run("MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c");
        assert_eq!(
            rows_disp(&b2),
            vec![
                vec!["Admin".to_string(), "1".to_string()],
                vec!["Company".to_string(), "1".to_string()],
                vec!["Person".to_string(), "2".to_string()],
                vec!["null".to_string(), "1".to_string()],
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn meta_fast_paths_match_the_scan() {
        // Every fast-pathed form must equal the general matcher on the same query;
        // appending an always-true WHERE forces the matcher (its independent truth).
        let (root, gen) = meta_gen("meta_parity");
        let cache = BlockCache::new(1 << 20);
        let eng = Engine::new(&gen, &cache);
        let parity = |fast: &str, slow: &str| {
            let f = eng.run(&parser::parse(fast).unwrap()).unwrap();
            let s = eng.run(&parser::parse(slow).unwrap()).unwrap();
            assert_eq!(f.columns, s.columns, "columns: {fast}");
            assert_eq!(rows_disp(&f), rows_disp(&s), "rows: {fast} vs {slow}");
        };
        // bare enumerations + counts, both arrow directions + undirected
        parity(
            "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t",
            "MATCH ()-[r]->() WHERE 1 = 1 RETURN DISTINCT type(r) AS t",
        );
        parity(
            "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c",
            "MATCH ()-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        // undirected: each edge matches in both orientations (self-loops counted
        // twice), so the fast path returns 2× the directed count — verified equal to
        // the matcher.
        parity(
            "MATCH ()-[r]-() RETURN type(r) AS t, count(*) AS c",
            "MATCH ()-[r]-() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        parity(
            "MATCH ()-[r]-() RETURN DISTINCT type(r) AS t",
            "MATCH ()-[r]-() WHERE 1 = 1 RETURN DISTINCT type(r) AS t",
        );
        parity(
            "MATCH (n) RETURN DISTINCT labels(n)[0] AS l",
            "MATCH (n) WHERE 1 = 1 RETURN DISTINCT labels(n)[0] AS l",
        );
        parity(
            "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c",
            "MATCH (n) WHERE 1 = 1 RETURN labels(n)[0] AS l, count(*) AS c",
        );
        // labelled schema marginals: source-, target-, reverse-arrow-, multi-label.
        parity(
            "MATCH (:Person)-[r]->() RETURN type(r) AS t, count(*) AS c",
            "MATCH (:Person)-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        parity(
            "MATCH ()-[r]->(:Company) RETURN type(r) AS t, count(*) AS c",
            "MATCH ()-[r]->(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        parity(
            "MATCH ()<-[r]-(:Person) RETURN type(r) AS t, count(*) AS c",
            "MATCH ()<-[r]-(:Person) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        parity(
            "MATCH (:Admin)-[r]->() RETURN type(r) AS t, count(*) AS c",
            "MATCH (:Admin)-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        // both-endpoints-labelled (the full schema-triple cube), grouped + fully
        // specified, including a multi-label endpoint.
        parity(
            "MATCH (:Person)-[r]->(:Company) RETURN type(r) AS t, count(*) AS c",
            "MATCH (:Person)-[r]->(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        parity(
            "MATCH (:Company)-[r]->(:Company) RETURN type(r) AS t, count(*) AS c",
            "MATCH (:Company)-[r]->(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        parity(
            "MATCH (:Admin)-[r]->(:Company) RETURN DISTINCT type(r) AS t",
            "MATCH (:Admin)-[r]->(:Company) WHERE 1 = 1 RETURN DISTINCT type(r) AS t",
        );
        // undirected with a labelled endpoint — src+tgt marginal (one end) and
        // triple+mirror (both ends), verified equal to the matcher.
        parity(
            "MATCH (:Person)-[r]-() RETURN type(r) AS t, count(*) AS c",
            "MATCH (:Person)-[r]-() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        parity(
            "MATCH ()-[r]-(:Company) RETURN type(r) AS t, count(*) AS c",
            "MATCH ()-[r]-(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        parity(
            "MATCH (:Person)-[r]-(:Company) RETURN type(r) AS t, count(*) AS c",
            "MATCH (:Person)-[r]-(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn meta_order_by_skip_limit() {
        // A trailing ORDER BY / SKIP / LIMIT is applied to the finished metadata rows,
        // order-identically to the matcher (compared without re-sorting).
        let (root, gen) = meta_gen("meta_order");
        let cache = BlockCache::new(1 << 20);
        let eng = Engine::new(&gen, &cache);
        let run = |q: &str| eng.run(&parser::parse(q).unwrap()).unwrap();
        let disp = |res: &QueryResult| -> Vec<Vec<String>> {
            res.rows
                .iter()
                .map(|r| r.iter().map(|c| c.to_display()).collect())
                .collect()
        };
        // Total order (c desc, then key) so ties are deterministic across paths.
        let ordered_parity = |fast: &str, slow: &str| {
            let f = run(fast);
            let s = run(slow);
            assert_eq!(f.columns, s.columns, "cols: {fast}");
            assert_eq!(disp(&f), disp(&s), "ordered rows: {fast} vs {slow}");
        };
        ordered_parity(
            "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY c DESC, t",
            "MATCH ()-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c ORDER BY c DESC, t",
        );
        // LIMIT truncates after ordering: the single largest group.
        assert_eq!(
            disp(&run(
                "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY c DESC, t LIMIT 1"
            )),
            vec![vec!["KNOWS".to_string(), "2".to_string()]],
        );
        // SKIP + LIMIT on the label side.
        ordered_parity(
            "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c ORDER BY c DESC, l SKIP 1 LIMIT 2",
            "MATCH (n) WHERE 1 = 1 RETURN labels(n)[0] AS l, count(*) AS c ORDER BY c DESC, l SKIP 1 LIMIT 2",
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn meta_fast_path_reads_no_blocks_under_tiny_budget() {
        // The regression guard: with `maxIntermediate` far below the edge count the
        // metadata queries still SUCCEED (no materialisation), read zero blocks, and
        // charge no budget — while the scanning form of the same question trips.
        let (root, gen) = meta_gen("meta_perf");
        let cache = BlockCache::new(1 << 20);
        let eng = Engine::new(&gen, &cache).with_max_intermediate(1);
        for q in [
            "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t",
            "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c",
            "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c",
        ] {
            let before = cache.metrics().misses;
            let res = eng.run(&parser::parse(q).unwrap()).unwrap();
            assert!(!res.rows.is_empty(), "empty result for {q}");
            assert_eq!(cache.metrics().misses, before, "fast path read blocks: {q}");
            assert_eq!(eng.cost(), 0, "fast path charged budget: {q}");
        }
        // The materialising form of the same question DOES trip the tiny budget —
        // exactly the failure the fast path removes.
        let scan = eng.run(
            &parser::parse("MATCH ()-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c")
                .unwrap(),
        );
        assert!(scan.is_err(), "scan should trip maxIntermediate=1");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn meta_declines_still_correct() {
        // Each "do NOT fast-path" shape falls back to the matcher and stays correct.
        let (root, gen) = meta_gen("meta_decline");
        let cache = BlockCache::new(1 << 20);
        let eng = Engine::new(&gen, &cache);
        let rows = |q: &str| rows_disp(&eng.run(&parser::parse(q).unwrap()).unwrap());

        // rel-type filter.
        assert_eq!(
            rows("MATCH ()-[r:KNOWS]->() RETURN type(r) AS t, count(*) AS c"),
            vec![vec!["KNOWS".to_string(), "2".to_string()]],
        );
        // WHERE predicate.
        assert_eq!(
            rows("MATCH ()-[r]->() WHERE type(r) = 'KNOWS' RETURN type(r) AS t, count(*) AS c"),
            vec![vec!["KNOWS".to_string(), "2".to_string()]],
        );
        // count(DISTINCT …) — declines; here it equals count(*) (all edges distinct).
        assert_eq!(
            rows("MATCH ()-[r]->() RETURN type(r) AS t, count(DISTINCT r) AS c"),
            rows("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c"),
        );
        // a node variable reused on both endpoints `(a)-[r]->(a)` constrains a
        // self-loop, so the whole-graph counts must NOT be used — it declines and the
        // matcher returns only the self-loop (OWNS: Acme→Acme).
        assert_eq!(
            rows("MATCH (a)-[r]->(a) RETURN type(r) AS t, count(*) AS c"),
            vec![vec!["OWNS".to_string(), "1".to_string()]],
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn meta_where_clause_is_not_ignored() {
        // A WHERE narrows the match, so the whole-graph metadata counts would be
        // WRONG — the fast path must decline and the matcher return the *filtered*
        // answer. Each case is chosen so the correct answer DIFFERS from the
        // metadata count, proving the resident counts are not reused.
        let (root, gen) = meta_gen("meta_where");
        let cache = BlockCache::new(1 << 20);
        let eng = Engine::new(&gen, &cache);
        let rows = |q: &str| rows_disp(&eng.run(&parser::parse(q).unwrap()).unwrap());

        // Whole-graph baseline (fast path): KNOWS 2, WORKS_AT 2, OWNS 1.
        let base = rows("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c");
        assert_eq!(
            base,
            vec![
                vec!["KNOWS".to_string(), "2".to_string()],
                vec!["OWNS".to_string(), "1".to_string()],
                vec!["WORKS_AT".to_string(), "2".to_string()],
            ]
        );

        // WHERE on a source property → only Alice's out-edges (KNOWS 1, WORKS_AT 1).
        let by_src =
            rows("MATCH (a)-[r]->() WHERE a.name = 'Alice' RETURN type(r) AS t, count(*) AS c");
        assert_eq!(
            by_src,
            vec![
                vec!["KNOWS".to_string(), "1".to_string()],
                vec!["WORKS_AT".to_string(), "1".to_string()],
            ]
        );
        assert_ne!(
            by_src, base,
            "WHERE on source property must change the counts"
        );

        // WHERE that prunes an entire reltype group — OWNS must disappear, not be
        // reported with its metadata count of 1.
        let pruned =
            rows("MATCH ()-[r]->() WHERE type(r) <> 'OWNS' RETURN type(r) AS t, count(*) AS c");
        assert_eq!(
            pruned,
            vec![
                vec!["KNOWS".to_string(), "2".to_string()],
                vec!["WORKS_AT".to_string(), "2".to_string()],
            ]
        );
        assert!(
            !pruned.iter().any(|r| r[0] == "OWNS"),
            "WHERE must prune the OWNS group entirely"
        );

        // WHERE that matches nothing → zero rows, NOT the metadata counts.
        let none =
            rows("MATCH ()-[r]->() WHERE r.no_such_prop = 99 RETURN type(r) AS t, count(*) AS c");
        assert!(
            none.is_empty(),
            "a WHERE matching no edges must yield no rows"
        );

        // Label side: a WHERE on a node property → only the matching node's first
        // label (Bob → Person 1), not the whole-graph Person count of 2.
        let base_l = rows("MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c");
        let one = rows("MATCH (n) WHERE n.name = 'Bob' RETURN labels(n)[0] AS l, count(*) AS c");
        assert_eq!(one, vec![vec!["Person".to_string(), "1".to_string()]]);
        assert_ne!(
            one, base_l,
            "WHERE on a node property must change the counts"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn count_with_constant_extra_projection_fast_path() {
        // The benchmark appends `… , $k AS k` (a constant grouping key) to bust the
        // result cache. That is still a single group, so the fast path fires and the
        // extra column is carried through in order.
        let (root, res) = run(
            "exec_count_tag",
            "MATCH (n:Person) RETURN count(*) AS c, 7 AS k",
        );
        assert_eq!(res.columns, vec!["c", "k"]);
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(3)));
        assert!(matches!(res.rows[0][1], Val::Int(7)));
        let _ = std::fs::remove_dir_all(&root);

        // Order preserved when the tag precedes the count.
        let (root, res) = run("exec_count_tag2", "MATCH (n) RETURN 9 AS k, count(n) AS c");
        assert_eq!(res.columns, vec!["k", "c"]);
        assert!(matches!(res.rows[0][0], Val::Int(9)));
        assert!(matches!(res.rows[0][1], Val::Int(5)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn count_with_non_constant_extra_projection_falls_back() {
        // A second item that reads node data is a real grouping key — must NOT take
        // the fast path; group-by-city over the 3 Person nodes yields 2 rows.
        let (root, res) = run(
            "exec_count_group",
            "MATCH (n:Person) RETURN n.city AS city, count(*) AS c",
        );
        assert_eq!(res.rows.len(), 2, "{:?}", res.rows);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn count_with_where_still_correct() {
        // A residual WHERE disables the fast path; the answer must still be right
        // (2 of the 3 Person nodes have age >= 30 in the fixture: Alice 30, Carol 40;
        // Bob is 25).
        let (root, res) = run(
            "exec_count_where",
            "MATCH (n:Person) WHERE n.age >= 30 RETURN count(*) AS c",
        );
        assert!(
            matches!(res.rows[0][0], Val::Int(2)),
            "{:?}",
            res.rows[0][0]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn streaming_scan_where_and_property_projection() {
        // Stage 5: a single node-only MATCH streams without per-row HashMaps. A
        // WHERE filter that reads a property (city = 'London') keeps Alice + Bob,
        // and the projected property comes back correctly.
        let (root, res) = run(
            "exec_stream_where",
            "MATCH (n:Person) WHERE n.city = 'London' RETURN n.name AS name",
        );
        assert_eq!(res.columns, vec!["name"]);
        assert_eq!(col0(&res), vec!["Alice", "Bob"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn streaming_scan_group_by_property_aggregation() {
        // Aggregation over the streamed rows: group the 3 Person nodes by city
        // (London → 2, Paris → 1). Exercises the streaming match feeding
        // project_aggregated with a per-row property read.
        let (root, res) = run(
            "exec_stream_agg",
            "MATCH (n:Person) RETURN n.city AS city, count(*) AS c ORDER BY c DESC",
        );
        assert_eq!(res.columns, vec!["city", "c"]);
        assert_eq!(res.rows.len(), 2);
        assert_eq!(res.rows[0][0].to_display(), "London");
        assert!(matches!(res.rows[0][1], Val::Int(2)));
        assert_eq!(res.rows[1][0].to_display(), "Paris");
        assert!(matches!(res.rows[1][1], Val::Int(1)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn streaming_scan_inline_prop_filter() {
        // An inline property on the anchor (handled by node_ok in the streaming
        // path, not a residual WHERE) selects the single matching node.
        let (root, res) = run(
            "exec_stream_inline",
            "MATCH (n:Person {city: 'Paris'}) RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn grouped_index_distinct_count_fast_path() {
        // Stage 7: `count(DISTINCT n.p)` over an indexed property is the number of
        // distinct index keys. age has 3 distinct values; team has one ('Red'),
        // and the index omits Carol (no team) — DISTINCT also excludes null.
        let (root, res) = run(
            "exec_g_distinct_age",
            "MATCH (n:Person) RETURN count(DISTINCT n.age) AS c",
        );
        assert_eq!(res.columns, vec!["c"]);
        assert!(
            matches!(res.rows[0][0], Val::Int(3)),
            "{:?}",
            res.rows[0][0]
        );
        let _ = std::fs::remove_dir_all(&root);

        // With the cache-busting constant tail, and a single distinct value.
        let (root, res) = run(
            "exec_g_distinct_team",
            "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c, 7 AS k",
        );
        assert_eq!(res.columns, vec!["c", "k"]);
        assert!(
            matches!(res.rows[0][0], Val::Int(1)),
            "{:?}",
            res.rows[0][0]
        );
        assert!(matches!(res.rows[0][1], Val::Int(7)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn grouped_index_group_by_fast_path() {
        // Stage 7: group-by an indexed property reads (key, count) from the index.
        // team: Alice/Bob 'Red' (2) and Carol's missing team becomes a null group
        // (1). ORDER BY c DESC puts the larger group first.
        let (root, res) = run(
            "exec_g_groupby_team",
            "MATCH (n:Person) RETURN n.team AS t, count(*) AS c ORDER BY c DESC",
        );
        assert_eq!(res.columns, vec!["t", "c"]);
        assert_eq!(res.rows.len(), 2, "{:?}", res.rows);
        assert_eq!(res.rows[0][0].to_display(), "Red");
        assert!(matches!(res.rows[0][1], Val::Int(2)));
        assert!(matches!(res.rows[1][0], Val::Null), "{:?}", res.rows[1][0]);
        assert!(matches!(res.rows[1][1], Val::Int(1)));
        let _ = std::fs::remove_dir_all(&root);

        // All-distinct indexed property: one group of 1 per value (no null group,
        // every Person has an age). `count(n)` behaves like `count(*)` here.
        let (root, res) = run(
            "exec_g_groupby_age",
            "MATCH (n:Person) RETURN n.age AS a, count(n) AS c",
        );
        assert_eq!(
            rows_disp(&res),
            vec![
                vec!["25".to_string(), "1".to_string()],
                vec!["30".to_string(), "1".to_string()],
                vec!["40".to_string(), "1".to_string()],
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn grouped_index_matches_general_path() {
        // The fast path must return exactly what the general (materialise + group)
        // path does. A residual WHERE that keeps every row forces the general path;
        // both group-by team (incl. the null group) and distinct-count must agree.
        let (root, fast) = run(
            "exec_g_cmp_fast",
            "MATCH (n:Person) RETURN n.team AS t, count(*) AS c",
        );
        let _ = std::fs::remove_dir_all(&root);
        let (root, general) = run(
            "exec_g_cmp_gen",
            "MATCH (n:Person) WHERE n.age >= 0 RETURN n.team AS t, count(*) AS c",
        );
        assert_eq!(rows_disp(&fast), rows_disp(&general));
        let _ = std::fs::remove_dir_all(&root);

        let (root, fast) = run(
            "exec_g_cmp_fast_d",
            "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c",
        );
        let _ = std::fs::remove_dir_all(&root);
        let (root, general) = run(
            "exec_g_cmp_gen_d",
            "MATCH (n:Person) WHERE n.age >= 0 RETURN count(DISTINCT n.team) AS c",
        );
        assert_eq!(rows_disp(&fast), rows_disp(&general));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn grouped_index_fast_path_guards() {
        // Shapes the fast path must decline, each still answered correctly by the
        // general path.

        // (a) Residual WHERE: age >= 30 keeps Alice (Red) and Carol (null).
        let (root, res) = run(
            "exec_g_guard_where",
            "MATCH (n:Person) WHERE n.age >= 30 RETURN n.team AS t, count(*) AS c",
        );
        assert_eq!(
            rows_disp(&res),
            vec![
                vec!["Red".to_string(), "1".to_string()],
                vec!["null".to_string(), "1".to_string()],
            ]
        );
        let _ = std::fs::remove_dir_all(&root);

        // (b) A non-count aggregate (sum) over the grouping property.
        let (root, res) = run(
            "exec_g_guard_sum",
            "MATCH (n:Person) RETURN n.team AS t, sum(n.age) AS s",
        );
        // Red = Alice 30 + Bob 25 = 55; null group = Carol 40.
        assert_eq!(
            rows_disp(&res),
            vec![
                vec!["Red".to_string(), "55".to_string()],
                vec!["null".to_string(), "40".to_string()],
            ]
        );
        let _ = std::fs::remove_dir_all(&root);

        // (c) Two grouping keys (the second `node.prop` trips the >1-key guard).
        let (root, res) = run(
            "exec_g_guard_twokeys",
            "MATCH (n:Person) RETURN n.team AS t, n.city AS city, count(*) AS c",
        );
        // (Red, London) Alice+Bob = 2; (null, Paris) Carol = 1.
        assert_eq!(res.rows.len(), 2, "{:?}", res.rows);
        let _ = std::fs::remove_dir_all(&root);

        // (d) A non-indexed grouping property (city) — must fall back, still right.
        let (root, res) = run(
            "exec_g_guard_noindex",
            "MATCH (n:Person) RETURN n.city AS city, count(*) AS c ORDER BY c DESC",
        );
        assert_eq!(res.rows[0][0].to_display(), "London");
        assert!(matches!(res.rows[0][1], Val::Int(2)));
        assert_eq!(res.rows[1][0].to_display(), "Paris");
        assert!(matches!(res.rows[1][1], Val::Int(1)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn grouped_index_fast_path_fires_without_scanning() {
        // Proof the fast path actually *fires* (rather than just agreeing with the
        // general path): the index walk charges nothing to the intermediate budget,
        // so a budget far too small for a per-row scan still succeeds. The control —
        // the same query forced onto the general path by a residual WHERE — exhausts
        // that budget scanning the 3 Person rows.
        //
        // The `count(DISTINCT n.p)` shape also exercises the parser quirk where the
        // inner DISTINCT sets `ret.distinct`; the fast path must not be fooled into
        // declining.
        let res = run_budgeted(
            "exec_g_fire_distinct",
            2,
            "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c, 7 AS k",
        )
        .expect("distinct-count fast path must not scan");
        assert!(
            matches!(res.rows[0][0], Val::Int(1)),
            "{:?}",
            res.rows[0][0]
        );

        let res = run_budgeted(
            "exec_g_fire_group",
            2,
            "MATCH (n:Person) RETURN n.team AS t, count(*) AS c",
        )
        .expect("group-by fast path must not scan");
        assert_eq!(res.rows.len(), 2);

        // Control: forced onto the general (scanning) path, the same budget trips.
        let err = run_budgeted(
            "exec_g_fire_control",
            2,
            "MATCH (n:Person) WHERE n.age >= 0 RETURN count(DISTINCT n.team) AS c",
        );
        assert!(
            err.is_err(),
            "the general path must exhaust the tiny budget (proving the fast path \
             above genuinely avoided the scan)"
        );
    }

    #[test]
    fn grouped_index_histogram_matches_scan() {
        // Level-1 precompute correctness: a histogram-ON generation answers
        // group-by / count(DISTINCT) from `prop_hist.blk`; an otherwise-identical
        // histogram-OFF generation answers them by walking the ISAM. Every query
        // must return identical rows AND identical row order.
        let ordered = |res: &QueryResult| -> Vec<Vec<String>> {
            res.rows
                .iter()
                .map(|r| r.iter().map(|c| c.to_display()).collect())
                .collect()
        };
        let exec = |root: &std::path::Path, graph: &str, q: &str| -> QueryResult {
            let gen = Generation::open(root, graph).unwrap();
            let cache = BlockCache::new(1 << 20);
            let out = Engine::new(&gen, &cache)
                .run(&parser::parse(q).unwrap())
                .unwrap();
            out
        };

        let queries = [
            "MATCH (n:Person) RETURN n.team AS t, count(*) AS c ORDER BY c DESC",
            "MATCH (n:Person) RETURN n.team AS t, count(*) AS c",
            "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c",
            "MATCH (n:Person) RETURN n.age AS a, count(*) AS c ORDER BY a ASC",
            "MATCH (n:Person) RETURN count(DISTINCT n.age) AS c, 7 AS k",
        ];
        for (i, q) in queries.iter().enumerate() {
            let (root_off, g_off, _) = testgen::write_basic(&format!("exec_hist_off_{i}"));
            // The OFF generation carries no histogram → fallback (index walk).
            let gen_off = Generation::open(&root_off, &g_off).unwrap();
            assert!(gen_off.property_histogram("node_Person_team").is_none());
            drop(gen_off);
            let off = exec(&root_off, &g_off, q);
            let _ = std::fs::remove_dir_all(&root_off);

            let (root_on, g_on, _) =
                testgen::write_basic_with_histograms(&format!("exec_hist_on_{i}"));
            // The ON generation's histogram is byte-identical to the walk it replaces.
            let gen_on = Generation::open(&root_on, &g_on).unwrap();
            let hist = gen_on
                .property_histogram("node_Person_team")
                .expect("histogram present in the ON generation");
            let walk = gen_on
                .range_index("node_Person_team")
                .unwrap()
                .distinct_key_counts()
                .unwrap();
            assert_eq!(hist, walk.as_slice(), "histogram must equal the index walk");
            drop(gen_on);
            let on = exec(&root_on, &g_on, q);
            let _ = std::fs::remove_dir_all(&root_on);

            assert_eq!(on.columns, off.columns, "columns differ for `{q}`");
            assert_eq!(ordered(&on), ordered(&off), "rows/order differ for `{q}`");
        }
    }

    #[test]
    fn param_indexed_equality_count_fast_path() {
        // Stage 1 + 3: `{name: $n}` selects the name index and the count comes from
        // its `lookup_eq` length, not a label scan + materialise.
        let (root, graph, _) = testgen::write_basic("exec_count_param_idx");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let mut params = HashMap::new();
        params.insert("n".to_string(), Val::Str("Carol".into()));
        let engine = Engine::new(&gen, &cache).with_params(params);
        let ast = parser::parse("MATCH (n:Person {name: $n}) RETURN count(*) AS c").unwrap();
        let res = engine.run(&ast).unwrap();
        assert!(
            matches!(res.rows[0][0], Val::Int(1)),
            "{:?}",
            res.rows[0][0]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn range_index_equality_lookup() {
        let (root, res) = run(
            "exec_rangeeq",
            "MATCH (n:Person {name: 'Bob'}) RETURN n.age AS age",
        );
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(25)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn where_range_filter_and_order() {
        let (root, res) = run(
            "exec_range",
            "MATCH (n:Person) WHERE n.age >= 30 RETURN n.name AS name ORDER BY n.age DESC",
        );
        // Carol (40) then Alice (30).
        let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
        assert_eq!(names, vec!["Carol", "Alice"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn relationship_pattern_traversal() {
        let (root, res) = run(
            "exec_rel",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b",
        );
        let mut pairs: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("Alice".into(), "Bob".into()),
                ("Alice".into(), "Carol".into()),
                ("Bob".into(), "Carol".into()),
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn relationship_value_carries_type_and_stored_endpoints() {
        // Outgoing walk: r is the stored Alice(0)-[:KNOWS]->Bob(1) edge.
        let (root, res) = run(
            "exec_reltype",
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN type(r) AS t, r AS rel",
        );
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "KNOWS");
        match res.rows[0][1] {
            Val::Rel {
                start,
                end,
                reltype,
                ..
            } => {
                assert_eq!((start, end, reltype), (0, 1, 0));
            }
            ref other => panic!("expected a relationship, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);

        // Walking the SAME edge incoming must report the same stored direction
        // (start→end is src→dst, not the traversal direction).
        let (root, res) = run(
            "exec_reltype_in",
            "MATCH (b:Person {name: 'Bob'})<-[r:KNOWS]-(a) RETURN r AS rel",
        );
        assert_eq!(res.rows.len(), 1);
        match res.rows[0][0] {
            Val::Rel { start, end, .. } => assert_eq!((start, end), (0, 1)),
            ref other => panic!("expected a relationship, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn incoming_direction_traversal() {
        let (root, res) = run(
            "exec_incoming",
            "MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a.name AS a, b.name AS b",
        );
        // Reverse of the KNOWS edges: Bob<-Alice, Carol<-Bob, Carol<-Alice.
        let mut pairs: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("Bob".into(), "Alice".into()),
                ("Carol".into(), "Alice".into()),
                ("Carol".into(), "Bob".into()),
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn relationship_property_predicate() {
        let (root, res) = run(
            "exec_relprop",
            "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN a.name AS a, b.name AS b",
        );
        // Only the Alice-[:KNOWS {since:2020}]->Bob edge carries the property.
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "Alice");
        assert_eq!(res.rows[0][1].to_display(), "Bob");
        let _ = std::fs::remove_dir_all(&root);
    }

    // Inline property maps whose value is bound earlier (by a `WITH` or an earlier
    // node/rel) must resolve against the current scope — `(b {id: x})` behaves like
    // `(b) WHERE b.id = x`. This was the last eu-ai-act-data-service parity gap.

    #[test]
    fn inline_node_prop_resolves_variable_from_with() {
        // The exact reported gap: a WITH-bound value feeding a later inline map.
        let (root, res) = run(
            "exec_inline_with",
            "MATCH (n:Person {name:'Bob'}) WITH n.name AS who \
             MATCH (m:Person {name: who}) RETURN m.age AS age",
        );
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(25)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn inline_node_prop_joins_across_matches() {
        // baseId-style join: carry one node's property into another node's inline map.
        let (root, res) = run(
            "exec_inline_join",
            "MATCH (a:Person {name:'Alice'}) WITH a.city AS c \
             MATCH (p:Person {city: c}) RETURN p.name AS n",
        );
        // Alice and Bob are both in London.
        assert_eq!(col0(&res), vec!["Alice", "Bob"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn inline_rel_prop_resolves_variable() {
        // Variable value on a relationship inline map.
        let (root, res) = run(
            "exec_inline_rel",
            "WITH 2020 AS yr MATCH (a)-[r:KNOWS {since: yr}]->(b) \
             RETURN a.name AS a, b.name AS b",
        );
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "Alice");
        assert_eq!(res.rows[0][1].to_display(), "Bob");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn inline_node_prop_resolves_property_access() {
        // The value is a property access (`a.name`), not just a bare variable.
        let (root, res) = run(
            "exec_inline_propaccess",
            "MATCH (a:Person {name:'Bob'}) \
             MATCH (m:Person {name: a.name}) RETURN m.name AS n",
        );
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "Bob");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn inline_node_prop_literal_still_works() {
        // Regression guard: literal inline maps must keep matching after the change.
        let (root, res) = run(
            "exec_inline_literal",
            "MATCH (n:Person {name:'Bob'}) RETURN n.age AS age",
        );
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(25)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn variable_length_expansion() {
        let (root, res) = run(
            "exec_varlen",
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b) RETURN b.name AS name",
        );
        // 1 hop: Bob, Carol. 2 hops: Alice→Bob→Carol = Carol again.
        assert_eq!(col0(&res), vec!["Bob", "Carol", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn type_alternation() {
        let (root, res) = run(
            "exec_altern",
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS name",
        );
        // Alice KNOWS Bob, KNOWS Carol, WORKS_AT Acme.
        assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn with_aggregation_group_and_having() {
        let (root, res) = run(
            "exec_with",
            "MATCH (n:Person) WITH n.city AS city, count(*) AS c WHERE c > 1 RETURN city, c",
        );
        // London has 2 (Alice, Bob); Paris has 1 (filtered out).
        assert_eq!(res.columns, vec!["city", "c"]);
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "London");
        assert!(matches!(res.rows[0][1], Val::Int(2)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn distinct_and_aggregate_functions() {
        let (root, res) = run(
            "exec_aggs",
            "MATCH (n:Person) RETURN count(n) AS c, sum(n.age) AS total, avg(n.age) AS mean, min(n.age) AS lo, max(n.age) AS hi, collect(DISTINCT n.city) AS cities",
        );
        let r = &res.rows[0];
        assert!(matches!(r[0], Val::Int(3)));
        assert!(matches!(r[1], Val::Int(95))); // 30+25+40
        assert!(matches!(r[2], Val::Float(f) if (f - 95.0 / 3.0).abs() < 1e-9));
        assert!(matches!(r[3], Val::Int(25)));
        assert!(matches!(r[4], Val::Int(40)));
        match &r[5] {
            Val::List(xs) => {
                let mut cities: Vec<String> = xs.iter().map(|v| v.to_display()).collect();
                cities.sort();
                assert_eq!(cities, vec!["London", "Paris"]);
            }
            other => panic!("expected a list, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn distinct_projection() {
        let (root, res) = run(
            "exec_distinct",
            "MATCH (n:Person) RETURN DISTINCT n.city AS city",
        );
        assert_eq!(col0(&res), vec!["London", "Paris"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn skip_and_limit() {
        let (root, res) = run(
            "exec_skiplimit",
            "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name SKIP 1 LIMIT 1",
        );
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "Bob");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn map_projection() {
        let (root, res) = run(
            "exec_mapproj",
            "MATCH (n:Person {name: 'Alice'}) RETURN n {.name, .age} AS m",
        );
        match &res.rows[0][0] {
            Val::Map(m) => {
                assert_eq!(m[0].0, "name");
                assert_eq!(m[0].1.to_display(), "Alice");
                assert_eq!(m[1].0, "age");
                assert!(matches!(m[1].1, Val::Int(30)));
            }
            other => panic!("expected a map, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn case_and_list_predicate_and_in() {
        let (root, res) = run(
            "exec_case",
            "MATCH (n:Person) RETURN n.name AS name, CASE WHEN n.age >= 30 THEN 'senior' ELSE 'junior' END AS band ORDER BY n.name",
        );
        let bands: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        assert_eq!(
            bands,
            vec![
                ("Alice".into(), "senior".into()),
                ("Bob".into(), "junior".into()),
                ("Carol".into(), "senior".into()),
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn where_in_and_string_ops() {
        let (root, res) = run(
            "exec_in",
            "MATCH (n:Person) WHERE n.age IN [25, 40] AND n.name STARTS WITH 'C' RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn union_distinct_and_all() {
        let (root, res) = run(
            "exec_union",
            "MATCH (n:Person) RETURN n.name AS x UNION MATCH (c:Company) RETURN c.name AS x",
        );
        assert_eq!(res.columns, vec!["x"]);
        assert_eq!(col0(&res), vec!["Acme", "Alice", "Bob", "Carol", "Globex"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn optional_match_yields_nulls() {
        // Companies have no outgoing KNOWS, so the optional rel is null.
        let (root, res) = run(
            "exec_optional",
            "MATCH (n:Company) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n.name AS name, m AS friend ORDER BY n.name",
        );
        assert_eq!(res.rows.len(), 2);
        for r in &res.rows {
            assert!(matches!(r[1], Val::Null));
        }
        assert_eq!(res.rows[0][0].to_display(), "Acme");
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── Stage 6 — traversal-frame characterization ───────────────────────────
    // These lock the exact result set of the multi-hop / variable-length walk so
    // the mutate-in-place binding frame (replacing the per-hop `binding.clone()`)
    // is provably result-preserving. They pass on the pre-Stage-6 code and must
    // still pass byte-for-byte after the rewrite.

    #[test]
    fn frame_two_hop_chain_exact_rows() {
        // KNOWS Person→Person edges: Alice→Bob, Bob→Carol, Alice→Carol. The only
        // length-2 KNOWS chain is Alice→Bob→Carol (Carol has no outgoing KNOWS).
        let (root, res) = run(
            "exec_frame_2hop",
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             RETURN a.name AS a, b.name AS b, c.name AS c",
        );
        let rows: Vec<(String, String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display(), r[2].to_display()))
            .collect();
        assert_eq!(rows, vec![("Alice".into(), "Bob".into(), "Carol".into())]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn frame_three_hop_chain_exact_rows() {
        // Headline-shaped 3-hop: KNOWS, KNOWS, WORKS_AT. The only walk is
        // Alice→Bob→Carol→Globex (Carol WORKS_AT Globex).
        let (root, res) = run(
            "exec_frame_3hop",
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:WORKS_AT]->(d) \
             RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d",
        );
        let rows: Vec<(String, String, String, String)> = res
            .rows
            .iter()
            .map(|r| {
                (
                    r[0].to_display(),
                    r[1].to_display(),
                    r[2].to_display(),
                    r[3].to_display(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![(
                "Alice".into(),
                "Bob".into(),
                "Carol".into(),
                "Globex".into()
            )]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn frame_sibling_branch_binding_isolation() {
        // The specific frame risk: Alice has TWO KNOWS siblings (Bob, Carol). Only
        // the Bob branch extends (Bob→Carol); the Carol branch dead-ends. If a
        // sibling fails to restore the mid binding `b` on backtrack, the Carol
        // branch would leak `b = Bob` and fabricate rows. Exactly one row proves
        // each branch is isolated.
        let (root, res) = run(
            "exec_frame_sibling",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN b.name AS b, c.name AS c",
        );
        let rows: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        assert_eq!(rows, vec![("Bob".into(), "Carol".into())]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn frame_same_end_node_via_two_paths() {
        // Carol is reachable from Alice by two distinct KNOWS paths — direct
        // (Alice→Carol) and via Bob (Alice→Bob→Carol). Both must survive as
        // separate rows; the frame must not collapse or duplicate them.
        let (root, res) = run(
            "exec_frame_twopaths",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS*1..2]->(c:Person {name:'Carol'}) \
             RETURN c.name AS c",
        );
        assert_eq!(col0(&res), vec!["Carol", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn frame_undirected_traversal() {
        // Bob's KNOWS edges: incoming from Alice (e0), outgoing to Carol (e1).
        // Undirected sees both.
        let (root, res) = run(
            "exec_frame_undirected",
            "MATCH (a:Person {name:'Bob'})-[:KNOWS]-(x) RETURN x.name AS x",
        );
        assert_eq!(col0(&res), vec!["Alice", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn frame_where_references_mid_pattern_var() {
        // A WHERE on the mid node `b` (evaluated against the full row scope) keeps
        // only the chain through Bob.
        let (root, res) = run(
            "exec_frame_midwhere",
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE b.name = 'Bob' RETURN a.name AS a, c.name AS c",
        );
        let rows: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        assert_eq!(rows, vec![("Alice".into(), "Carol".into())]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn frame_multipattern_comma_join_shared_var() {
        // Two comma-joined patterns sharing `b`: pattern 1 binds b∈{Bob,Carol};
        // pattern 2 (b)-[:KNOWS]->(c) only extends from Bob.
        let (root, res) = run(
            "exec_frame_comma",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b), (b)-[:KNOWS]->(c) \
             RETURN b.name AS b, c.name AS c",
        );
        let rows: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        assert_eq!(rows, vec![("Bob".into(), "Carol".into())]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn frame_varlen_zero_length_includes_self() {
        // `*0..1`: zero hops binds the anchor itself (Alice); one hop adds its
        // KNOWS neighbours.
        let (root, res) = run(
            "exec_frame_varlen0",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS*0..1]->(b) RETURN b.name AS b",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn frame_varlen_relationship_uniqueness() {
        // Undirected `*2..2` from Bob must not reuse an edge within a path: the
        // walks are Bob-e0-Alice-e4-Carol and Bob-e1-Carol-e4-Alice. Reusing e0/e1
        // would step back to Bob — so a "Bob" in the result would mean uniqueness
        // is broken.
        let (root, res) = run(
            "exec_frame_unique",
            "MATCH (a:Person {name:'Bob'})-[:KNOWS*2..2]-(x) RETURN x.name AS x",
        );
        assert_eq!(col0(&res), vec!["Alice", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn frame_path_var_walk_order() {
        // The path scratch buffer must yield nodes/relationships in walk order
        // (Alice→Bob→Carol = ids 0,1,2; edges e0,e1 = ids 0,1) after the frame
        // push/pop rewrite.
        let (root, res) = run(
            "exec_frame_pathorder",
            "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN [n IN nodes(p) | id(n)] AS ns, [r IN relationships(p) | id(r)] AS rs",
        );
        assert_eq!(res.rows.len(), 1);
        assert_eq!(render(&res.rows[0][0]), "[0,1,2]");
        assert_eq!(render(&res.rows[0][1]), "[0,1]");
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── GQL quantified path patterns ─────────────────────────────────────────
    // Graph (write_basic): KNOWS = Alice→Bob, Bob→Carol, Alice→Carol;
    // WORKS_AT = Alice→Acme, Carol→Globex.

    /// Run a query against the basic fixture, returning the result or the error
    /// string (and always cleaning the fixture up).
    fn run_result(tag: &str, q: &str) -> std::result::Result<QueryResult, String> {
        let (root, graph, _) = testgen::write_basic(tag);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let out = parser::parse(q)
            .map_err(|e| e.to_string())
            .and_then(|ast| engine.run(&ast).map_err(|e| e.to_string()));
        let _ = std::fs::remove_dir_all(&root);
        out
    }

    /// Sorted first-column display strings for a query that must succeed.
    fn gql_col0(tag: &str, q: &str) -> Vec<String> {
        let mut v: Vec<String> = run_result(tag, q)
            .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"))
            .rows
            .iter()
            .map(|r| r[0].to_display())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn quantified_path_equals_varlength() {
        // The GQL group `((x)-[:KNOWS]->(y)){1,2}` is the cross-dialect equivalent
        // of Cypher's `-[:KNOWS*1..2]->`; both must yield the same multiset of end
        // nodes (Bob, Carol via 1 hop; Carol again via Alice→Bob→Carol).
        let gql = gql_col0(
            "exec_gql_q_vs_vl_g",
            "MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)){1,2} (b:Person) RETURN b.name AS b",
        );
        let cypher = gql_col0(
            "exec_gql_q_vs_vl_c",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS*1..2]->(b:Person) RETURN b.name AS b",
        );
        assert_eq!(gql, vec!["Bob", "Carol", "Carol"]);
        assert_eq!(gql, cypher, "GQL quantifier must match Cypher var-length");
    }

    #[test]
    fn quantified_exact_equals_fixed_varlength() {
        // `{2}` is exactly `*2..2`: the only 2-hop KNOWS path from Alice ends at Carol.
        let gql = gql_col0(
            "exec_gql_exact_g",
            "MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)){2} (b) RETURN b.name AS b",
        );
        let cypher = gql_col0(
            "exec_gql_exact_c",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS*2..2]->(b) RETURN b.name AS b",
        );
        assert_eq!(gql, vec!["Carol"]);
        assert_eq!(gql, cypher);
    }

    #[test]
    fn quantified_multi_hop_inner_matches_unrolled() {
        // A two-relationship inner sub-path repeated once equals the unrolled Cypher
        // chain `-[:KNOWS]->()-[:WORKS_AT]->()`: Alice→Carol→Globex (Bob has no
        // WORKS_AT edge).
        let gql = gql_col0(
            "exec_gql_multi_g",
            "MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)-[:WORKS_AT]->(z)){1} (b) RETURN b.name AS b",
        );
        let cypher = gql_col0(
            "exec_gql_multi_c",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->()-[:WORKS_AT]->(b) RETURN b.name AS b",
        );
        assert_eq!(gql, vec!["Globex"]);
        assert_eq!(gql, cypher);
    }

    #[test]
    fn quantified_dialect_switch_across_union() {
        // One query, two dialects: a Cypher branch UNIONed with a GQL branch. The
        // Cypher branch returns Alice's direct KNOWS (Bob, Carol); the GQL `{2}`
        // branch returns the 2-hop end (Carol); UNION de-dups to {Bob, Carol}.
        let rows = gql_col0(
            "exec_gql_union",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name AS b \
             UNION \
             MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)){2} (b) RETURN b.name AS b",
        );
        assert_eq!(rows, vec!["Bob", "Carol"]);
    }

    #[test]
    fn quantified_mixed_with_plain_hop() {
        // A plain Cypher hop and a GQL group in the SAME pattern: Alice -KNOWS-> m
        // then one more KNOWS to b. Only Alice→Bob→Carol qualifies (Carol has no
        // outgoing KNOWS), so b = Carol — same as the unrolled 2-hop Cypher chain.
        let gql = gql_col0(
            "exec_gql_mixed_g",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(m) ((x)-[:KNOWS]->(y)){1} (b) RETURN b.name AS b",
        );
        let cypher = gql_col0(
            "exec_gql_mixed_c",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->()-[:KNOWS]->(b) RETURN b.name AS b",
        );
        assert_eq!(gql, vec!["Carol"]);
        assert_eq!(gql, cypher);
    }

    #[test]
    fn quantified_count_bypasses_fast_path() {
        // `count(*)` over a quantified pattern must NOT take the single-node count
        // fast path (which keys off empty `rels`); the segments guard routes it to
        // the general matcher, counting all three matching paths.
        let res = run_result(
            "exec_gql_count",
            "MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)){1,2} (b) RETURN count(*) AS c",
        )
        .unwrap();
        assert!(
            matches!(res.rows[0][0], Val::Int(3)),
            "{:?}",
            res.rows[0][0]
        );
    }

    #[test]
    fn quantified_unbounded_rejected() {
        for q in [
            "MATCH (a) ((x)-[:KNOWS]->(y))+ (b) RETURN b",
            "MATCH (a) ((x)-[:KNOWS]->(y))* (b) RETURN b",
            "MATCH (a) ((x)-[:KNOWS]->(y)){1,} (b) RETURN b",
        ] {
            let e = run_result("exec_gql_unbounded", q).unwrap_err();
            assert!(
                e.contains("unbounded") || e.contains("lower bound"),
                "{q}: {e}"
            );
        }
    }

    #[test]
    fn quantified_zero_lower_bound_rejected() {
        let e = run_result(
            "exec_gql_zero",
            "MATCH (a) ((x)-[:KNOWS]->(y)){0,2} (b) RETURN b",
        )
        .unwrap_err();
        assert!(e.contains("lower bound below 1"), "{e}");
    }

    // ── GQL path restrictors (PR 2) ──────────────────────────────────────────
    // Run over the cyclic fixture (testgen::write_cycle): a→b→c→a triangle plus a
    // c→b chord. Over `(s{name:'a'})-[:R*1..4]->(x)` the four modes yield a distinct
    // number of paths — WALK 6, TRAIL 4, SIMPLE 3, ACYCLIC 2 — which is exactly what
    // sets them apart (see the fixture doc-comment for the per-length enumeration).

    /// Parse + run `q` against a fresh cycle fixture, returning the result or the
    /// error string, and always cleaning the fixture up.
    fn cycle_result(tag: &str, q: &str) -> std::result::Result<QueryResult, String> {
        let (root, graph) = testgen::write_cycle(tag);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let out = parser::parse(q)
            .map_err(|e| e.to_string())
            .and_then(|ast| engine.run(&ast).map_err(|e| e.to_string()));
        let _ = std::fs::remove_dir_all(&root);
        out
    }

    /// Sorted end-node names of `(s{name:'a'})-[<restrictor>:R*1..4]->(x)`, one entry
    /// per matched path (duplicates kept), for the given restrictor prefix.
    fn cycle_ends(tag: &str, restrictor: &str) -> Vec<String> {
        let q = format!("MATCH {restrictor} (s {{name:'a'}})-[:R*1..4]->(x) RETURN x.name AS n");
        let mut v: Vec<String> = cycle_result(tag, &q)
            .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"))
            .rows
            .iter()
            .map(|r| r[0].to_display())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn restrictors_distinguish_modes_on_cycle() {
        // The headline: each mode produces a different path multiset on the cycle.
        let walk = cycle_ends("exec_gql_r_walk", "WALK");
        let trail = cycle_ends("exec_gql_r_trail", "TRAIL");
        let simple = cycle_ends("exec_gql_r_simple", "SIMPLE");
        let acyclic = cycle_ends("exec_gql_r_acyclic", "ACYCLIC");

        // WALK reuses edges and nodes freely: every walk of length 1..4.
        assert_eq!(walk, vec!["a", "b", "b", "b", "c", "c"], "WALK");
        // TRAIL forbids edge reuse: drops the two length-4 walks that repeat an edge.
        assert_eq!(trail, vec!["a", "b", "b", "c"], "TRAIL");
        // SIMPLE forbids interior node repeats but lets the walk close at its start
        // `a`; the second visit to `b` (via the chord) is excluded.
        assert_eq!(simple, vec!["a", "b", "c"], "SIMPLE");
        // ACYCLIC forbids every node repeat, so the closing return to `a` is gone too.
        assert_eq!(acyclic, vec!["b", "c"], "ACYCLIC");

        // …and the counts are all distinct (6, 4, 3, 2).
        assert_eq!(
            (walk.len(), trail.len(), simple.len(), acyclic.len()),
            (6, 4, 3, 2)
        );
    }

    #[test]
    fn bare_star_equals_trail() {
        // Parity: a bare `*` (no restrictor) must be byte-for-byte today's behaviour,
        // which is edge-unique = TRAIL. So absence of a restrictor ≡ explicit TRAIL.
        let bare = cycle_ends("exec_gql_r_bare", "");
        let trail = cycle_ends("exec_gql_r_bare_trail", "TRAIL");
        assert_eq!(bare, trail, "bare * must equal explicit TRAIL");
        assert_eq!(bare, vec!["a", "b", "b", "c"]);
    }

    #[test]
    fn acyclic_excludes_start_that_simple_keeps() {
        // The one place SIMPLE and ACYCLIC differ on this graph is the cycle-closing
        // path a→b→c→a: SIMPLE keeps it (endpoints may coincide), ACYCLIC drops it.
        let simple = cycle_ends("exec_gql_r_se_simple", "SIMPLE");
        let acyclic = cycle_ends("exec_gql_r_se_acyclic", "ACYCLIC");
        assert!(
            simple.contains(&"a".to_string()),
            "SIMPLE keeps the closed cycle"
        );
        assert!(
            !acyclic.contains(&"a".to_string()),
            "ACYCLIC drops the closed cycle"
        );
    }

    #[test]
    fn restrictor_requires_variable_length() {
        // A restrictor is honoured only where `varlen` owns the uniqueness scope.
        // On a fixed hop or a node-only pattern it is rejected, not silently ignored.
        for q in [
            "MATCH TRAIL (s {name:'a'})-[:R]->(x) RETURN x",
            "MATCH WALK (n) RETURN n",
        ] {
            let e = cycle_result("exec_gql_r_novar", q).unwrap_err();
            assert!(e.contains("variable-length relationship"), "{q}: {e}");
        }
    }

    #[test]
    fn restrictor_over_quantified_group_rejected() {
        // The grammar accepts `TRAIL ((…)){m,n}` but lowering rejects it: the group
        // desugars into separate expansions that cannot share one uniqueness scope.
        let e = cycle_result(
            "exec_gql_r_quant",
            "MATCH TRAIL (s {name:'a'}) ((x)-[:R]->(y)){1,2} (z) RETURN z",
        )
        .unwrap_err();
        assert!(e.contains("restrictor") && e.contains("quantified"), "{e}");
    }

    // ── GQL shortest-path selectors (PR 3) ───────────────────────────────────
    // ANY/ALL SHORTEST and SHORTEST k share the BFS core `select_paths` with
    // `shortestPath()`. Parity is checked on the basic fixture; the multi-path
    // behaviours run over the diamond fixture (testgen::write_diamond), which has two
    // length-2 `s→t` paths (via `a`, via `b`) plus a length-3 detour `s→a→c→t`.

    /// Parse + run `q` against a fresh diamond fixture, returning the result or the
    /// error string, and always cleaning the fixture up.
    fn diamond_result(tag: &str, q: &str) -> std::result::Result<QueryResult, String> {
        let (root, graph) = testgen::write_diamond(tag);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let out = parser::parse(q)
            .map_err(|e| e.to_string())
            .and_then(|ast| engine.run(&ast).map_err(|e| e.to_string()));
        let _ = std::fs::remove_dir_all(&root);
        out
    }

    /// Sorted path lengths (`size(r)` per row) for a diamond query that must succeed.
    fn diamond_lengths(tag: &str, q: &str) -> Vec<i64> {
        let mut v: Vec<i64> = diamond_result(tag, q)
            .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"))
            .rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(i) => i,
                ref o => panic!("expected Int length, got {o:?}"),
            })
            .collect();
        v.sort();
        v
    }

    #[test]
    fn any_shortest_parity_with_shortest_path() {
        // ANY SHORTEST over a MATCH pattern agrees with the shortestPath() function on
        // the same endpoints: the single shortest KNOWS path Alice→Carol is the direct
        // 1-hop edge, and its node sequence is [Alice, Carol].
        let sel = run_result(
            "exec_gql_any_parity",
            "MATCH ANY SHORTEST p = (a:Person {name:'Alice'})-[:KNOWS*]->(c:Person {name:'Carol'}) \
             RETURN size(relationships(p)) AS l, [n IN nodes(p) | n.name] AS names",
        )
        .unwrap();
        assert_eq!(sel.rows.len(), 1, "one shortest path for the single pair");
        assert!(
            matches!(sel.rows[0][0], Val::Int(1)),
            "{:?}",
            sel.rows[0][0]
        );
        assert_eq!(render(&sel.rows[0][1]), "['Alice','Carol']");

        // The shortestPath() function returns the identical length on the same pair.
        let func = run_result(
            "exec_gql_any_parity_fn",
            "MATCH (a:Person {name:'Alice'}), (c:Person {name:'Carol'}) \
             RETURN length(shortestPath((a)-[:KNOWS*]->(c))) AS l",
        )
        .unwrap();
        assert!(matches!(func.rows[0][0], Val::Int(1)));
    }

    #[test]
    fn any_shortest_picks_one_of_the_ties() {
        // On the diamond, ANY SHORTEST returns exactly one s→t path, of length 2.
        let lens = diamond_lengths(
            "exec_gql_any_one",
            "MATCH ANY SHORTEST (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
        );
        assert_eq!(lens, vec![2], "a single shortest path");
    }

    #[test]
    fn all_shortest_returns_all_ties() {
        // ALL SHORTEST returns both length-2 paths (via `a`, via `b`) and not the
        // length-3 detour — every path of the minimum length, no more.
        let lens = diamond_lengths(
            "exec_gql_all_ties",
            "MATCH ALL SHORTEST (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
        );
        assert_eq!(lens, vec![2, 2], "two length-2 ties");

        // The two paths are distinct: their interior node is `a` in one, `b` in the
        // other.
        let res = diamond_result(
            "exec_gql_all_ties_nodes",
            "MATCH ALL SHORTEST p = (s {name:'s'})-[:R*]->(t {name:'t'}) \
             RETURN [n IN nodes(p) | n.name] AS names",
        )
        .unwrap();
        let mut names: Vec<String> = res.rows.iter().map(|r| render(&r[0])).collect();
        names.sort();
        assert_eq!(names, vec!["['s','a','t']", "['s','b','t']"]);
    }

    #[test]
    fn shortest_k_returns_k_in_length_order() {
        // SHORTEST 2 → the two length-2 ties.
        assert_eq!(
            diamond_lengths(
                "exec_gql_k2",
                "MATCH SHORTEST 2 (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
            ),
            vec![2, 2],
        );
        // SHORTEST 3 → the two ties plus the length-3 detour (k can pull in a longer
        // path once the shortest ones are spent).
        assert_eq!(
            diamond_lengths(
                "exec_gql_k3",
                "MATCH SHORTEST 3 (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
            ),
            vec![2, 2, 3],
        );
        // SHORTEST 4 cannot exceed the three loopless paths that exist.
        assert_eq!(
            diamond_lengths(
                "exec_gql_k4",
                "MATCH SHORTEST 4 (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
            ),
            vec![2, 2, 3],
        );
        // SHORTEST 1 ≡ ANY SHORTEST: a single shortest path.
        assert_eq!(
            diamond_lengths(
                "exec_gql_k1",
                "MATCH SHORTEST 1 (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
            ),
            vec![2],
        );
    }

    #[test]
    fn selector_applies_where_after_selection() {
        // Free endpoints ranging over every node, narrowed by a WHERE on their names:
        // only the s→t pairing survives, yielding the two shortest paths. This proves
        // the clause WHERE is applied per produced path, across the endpoint product.
        let lens = diamond_lengths(
            "exec_gql_sel_where",
            "MATCH ALL SHORTEST (x)-[r:R*]->(y) WHERE x.name = 's' AND y.name = 't' \
             RETURN size(r) AS l",
        );
        assert_eq!(lens, vec![2, 2]);

        // A WHERE that excludes every endpoint pair yields no rows.
        let none = diamond_result(
            "exec_gql_sel_where_empty",
            "MATCH ANY SHORTEST (x)-[r:R*]->(y) WHERE x.name = 't' AND y.name = 's' \
             RETURN size(r) AS l",
        )
        .unwrap();
        assert!(none.rows.is_empty(), "no t→s path exists");
    }

    #[test]
    fn selector_optional_emits_null_when_no_path() {
        // OPTIONAL MATCH with a selector keeps the driving row and null-fills when no
        // path connects the endpoints (t cannot reach s).
        let res = diamond_result(
            "exec_gql_sel_optional",
            "MATCH (a {name:'t'}) OPTIONAL MATCH ANY SHORTEST (a)-[r:R*]->(z {name:'s'}) \
             RETURN a.name AS a, r IS NULL AS no_path",
        )
        .unwrap();
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "t");
        assert!(matches!(res.rows[0][1], Val::Bool(true)));
    }

    #[test]
    fn selector_rejections() {
        // A multi-relationship selected pattern is out of scope (PR 3 covers a single
        // relationship, like shortestPath()).
        let e = diamond_result(
            "exec_gql_sel_multi",
            "MATCH ANY SHORTEST (s {name:'s'})-[:R]->(m)-[:R*]->(t {name:'t'}) RETURN t",
        )
        .unwrap_err();
        assert!(e.contains("single relationship"), "{e}");

        // A selector combined with a restrictor is not yet supported.
        let e = diamond_result(
            "exec_gql_sel_restr",
            "MATCH ANY SHORTEST TRAIL (s {name:'s'})-[:R*]->(t {name:'t'}) RETURN t",
        )
        .unwrap_err();
        assert!(e.contains("restrictor"), "{e}");

        // A selector over a quantified group is rejected at lowering.
        let e = diamond_result(
            "exec_gql_sel_quant",
            "MATCH ALL SHORTEST (s {name:'s'}) ((x)-[:R]->(y)){1,2} (t) RETURN t",
        )
        .unwrap_err();
        assert!(e.contains("selector") && e.contains("quantified"), "{e}");

        // A selector cannot share its clause with a comma-joined pattern.
        let e = diamond_result(
            "exec_gql_sel_multipat",
            "MATCH ANY SHORTEST (s {name:'s'})-[:R*]->(t {name:'t'}), (u) RETURN t",
        )
        .unwrap_err();
        assert!(e.contains("only") && e.contains("pattern"), "{e}");
    }

    // ── GQL label boolean expressions (PR 4) ─────────────────────────────────
    // The basic fixture has disjoint labels :Person (Alice, Bob, Carol) and
    // :Company (Acme, Globex), and rel-types KNOWS / WORKS_AT — enough to tell the
    // boolean forms apart on both nodes and relationships.

    #[test]
    fn label_boolean_node_cardinalities() {
        // OR unions the two label sets (all 5), NOT-Person leaves the 2 companies,
        // and AND is empty (no node carries both labels) — three distinct sets.
        assert_eq!(
            gql_col0(
                "exec_gql_label_or",
                "MATCH (n:Person|Company) RETURN n.name AS n"
            ),
            vec!["Acme", "Alice", "Bob", "Carol", "Globex"],
        );
        assert_eq!(
            gql_col0("exec_gql_label_not", "MATCH (n:!Person) RETURN n.name AS n"),
            vec!["Acme", "Globex"],
        );
        assert!(
            gql_col0(
                "exec_gql_label_and",
                "MATCH (n:Person&Company) RETURN n.name AS n"
            )
            .is_empty(),
            "no node carries both labels",
        );
    }

    #[test]
    fn colon_chain_lowers_to_and_not_or() {
        // Parity: `:Person:Company` is AND sugar, so it must give the SAME (empty)
        // result as `:Person&Company` — NOT the 5-row OR result. A regression that
        // lowered the colon chain to OR would surface here.
        let colon = gql_col0(
            "exec_gql_colon_and",
            "MATCH (n:Person:Company) RETURN n.name AS n",
        );
        let amp = gql_col0(
            "exec_gql_amp_and",
            "MATCH (n:Person&Company) RETURN n.name AS n",
        );
        assert!(colon.is_empty());
        assert_eq!(colon, amp);
    }

    #[test]
    fn label_boolean_reltype_cardinalities() {
        // Alice's out-edges: KNOWS→Bob, KNOWS→Carol, WORKS_AT→Acme. OR keeps all
        // three neighbours, NOT-KNOWS keeps just the WORKS_AT target, AND is empty
        // (an edge carries exactly one type).
        assert_eq!(
            gql_col0(
                "exec_gql_rel_or",
                "MATCH (a {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b",
            ),
            vec!["Acme", "Bob", "Carol"],
        );
        assert_eq!(
            gql_col0(
                "exec_gql_rel_not",
                "MATCH (a {name:'Alice'})-[:!KNOWS]->(b) RETURN b.name AS b",
            ),
            vec!["Acme"],
        );
        assert!(
            gql_col0(
                "exec_gql_rel_and",
                "MATCH (a {name:'Alice'})-[:KNOWS&WORKS_AT]->(b) RETURN b.name AS b",
            )
            .is_empty(),
            "an edge carries exactly one type",
        );
    }

    #[test]
    fn reltype_alternation_parity_with_single_types() {
        // `:KNOWS|WORKS_AT` (now an Or expression) must equal the union of the two
        // single-type traversals — the pre-GQL alternation behaviour, unchanged.
        let alt = gql_col0(
            "exec_gql_rel_alt",
            "MATCH (a {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b",
        );
        let knows = gql_col0(
            "exec_gql_rel_knows",
            "MATCH (a {name:'Alice'})-[:KNOWS]->(b) RETURN b.name AS b",
        );
        let works = gql_col0(
            "exec_gql_rel_works",
            "MATCH (a {name:'Alice'})-[:WORKS_AT]->(b) RETURN b.name AS b",
        );
        let mut union = [knows, works].concat();
        union.sort();
        assert_eq!(alt, union);
    }

    // ── GQL PR 5 — `FOR` is UNWIND ────────────────────────────────────────────

    #[test]
    fn for_and_unwind_produce_identical_rows() {
        // `FOR x IN list` lowers onto the same UnwindClause as `UNWIND list AS x`,
        // so the two must emit byte-for-byte identical result rows — confirming the
        // lowering reaches the unchanged executor path.
        let by_for = gql_col0("exec_gql_for", "FOR x IN [3, 1, 2] RETURN x ORDER BY x");
        let by_unwind = gql_col0(
            "exec_gql_unwind",
            "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x",
        );
        assert_eq!(by_for, by_unwind);
        assert_eq!(by_for, vec!["1", "2", "3"]);

        // FOR over a MATCH-produced list behaves exactly like UNWIND too — one row
        // per matched `b` (Alice KNOWS both Bob and Carol in the basic fixture).
        let for_match = gql_col0(
            "exec_gql_for_match",
            "MATCH (a {name:'Alice'})-[:KNOWS]->(b) FOR n IN [b.name] RETURN n",
        );
        assert_eq!(for_match, vec!["Bob", "Carol"]);
    }

    #[test]
    fn cast_executes_as_the_conversion_function() {
        // CAST lowers onto the to*/temporal functions, so it must compute exactly
        // what those functions do — confirming the lowering reaches the real path.
        assert_eq!(
            gql_col0("exec_gql_cast_int", "RETURN CAST('42' AS INTEGER) AS v"),
            gql_col0("exec_gql_toint", "RETURN toInteger('42') AS v"),
        );
        assert_eq!(
            gql_col0("exec_gql_cast_int2", "RETURN CAST('42' AS INTEGER) AS v"),
            vec!["42"],
        );
        // Float, string and boolean spellings all round-trip through their function.
        assert_eq!(
            gql_col0("exec_gql_cast_float", "RETURN CAST(3 AS FLOAT) AS v"),
            gql_col0("exec_gql_tofloat", "RETURN toFloat(3) AS v"),
        );
        assert_eq!(
            gql_col0("exec_gql_cast_bool", "RETURN CAST('true' AS BOOLEAN) AS v"),
            vec!["true"],
        );
        // A non-convertible value yields NULL, exactly like toInteger.
        assert_eq!(
            gql_col0("exec_gql_cast_null", "RETURN CAST('nope' AS INTEGER) AS v"),
            gql_col0("exec_gql_toint_null", "RETURN toInteger('nope') AS v"),
        );
    }

    // ── Stage 6 — LIMIT pushdown (early-stop) ────────────────────────────────
    // Pushing the LIMIT into the match must return the SAME prefix of rows (in
    // match-emit order) that buffering-then-truncating did — early-stop changes
    // *when* matching halts, never *which* rows come first.

    /// All rows of `q` as `(a, b)` display-string pairs, plus fixture cleanup.
    fn pairs(tag: &str, q: &str) -> Vec<(String, String)> {
        let (root, res) = run(tag, q);
        let v = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        let _ = std::fs::remove_dir_all(&root);
        v
    }

    #[test]
    fn limit_pushdown_traversal_returns_order_preserving_prefix() {
        let full = pairs(
            "exec_limit_full",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS a, b.name AS b",
        );
        assert!(full.len() >= 3, "{full:?}"); // Alice→Bob, Alice→Carol, Bob→Carol
        let limited = pairs(
            "exec_limit_2",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS a, b.name AS b LIMIT 2",
        );
        assert_eq!(limited.len(), 2);
        assert_eq!(limited.as_slice(), &full[..2]);
    }

    #[test]
    fn limit_pushdown_with_skip() {
        // SKIP s LIMIT n caps the match at s+n, then the projection drops s — the
        // single returned row must equal the unlimited row at index s.
        let full = pairs(
            "exec_skiplim_full",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS a, b.name AS b",
        );
        let limited = pairs(
            "exec_skiplim",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS a, b.name AS b SKIP 1 LIMIT 1",
        );
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0], full[1]);
    }

    #[test]
    fn limit_pushdown_streaming_scan_prefix() {
        // The node-only streaming path (try_stream_match) honours the cap too.
        let (root, full) = run(
            "exec_limit_stream_full",
            "MATCH (n:Person) RETURN n.name AS name",
        );
        let names_full = col0(&full); // sorted; just need the count
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(names_full.len(), 3);
        let (root, lim) = run(
            "exec_limit_stream",
            "MATCH (n:Person) RETURN n.name AS name LIMIT 2",
        );
        assert_eq!(lim.rows.len(), 2);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn limit_does_not_break_aggregation_or_order() {
        // The cap MUST be `None` when the projection aggregates or orders: the LIMIT
        // applies after the full group + sort, so all 3 Person rows must be seen.
        let (root, res) = run(
            "exec_limit_agg_guard",
            "MATCH (n:Person) RETURN n.city AS city, count(*) AS c ORDER BY c DESC LIMIT 1",
        );
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "London");
        assert!(
            matches!(res.rows[0][1], Val::Int(2)),
            "{:?}",
            res.rows[0][1]
        );
        let _ = std::fs::remove_dir_all(&root);

        // ORDER BY without aggregation also needs the full set before truncating.
        let (root, res) = run(
            "exec_limit_order_guard",
            "MATCH (n:Person) RETURN n.name AS name ORDER BY n.age DESC LIMIT 1",
        );
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "Carol"); // oldest at 40
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn reads_route_through_the_block_cache() {
        // A second identical run over the same cache must be served from resident
        // blocks (no new misses), proving the executor reads through the cache.
        let (root, graph, _) = testgen::write_basic("exec_cache");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let ast = parser::parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name").unwrap();

        engine.run(&ast).unwrap();
        let after_first = cache.metrics();
        assert!(
            after_first.misses > 0,
            "first run should populate the cache"
        );
        engine.run(&ast).unwrap();
        let after_second = cache.metrics();
        assert_eq!(
            after_second.misses, after_first.misses,
            "second run should hit the cache for every block"
        );
        assert!(after_second.hits > after_first.hits);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn parameter_substitution() {
        let (root, graph, _) = testgen::write_basic("exec_param");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let mut params = HashMap::new();
        params.insert("name".to_string(), Val::Str("Carol".into()));
        let engine = Engine::new(&gen, &cache).with_params(params);
        let ast =
            parser::parse("MATCH (n:Person) WHERE n.name = $name RETURN n.age AS age").unwrap();
        let res = engine.run(&ast).unwrap();
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(40)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn range_refuses_unbounded_span() {
        let (root, graph, _) = testgen::write_basic("exec_range_cap");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);

        // A full-i64 span would allocate until OOM, and the old unchecked `i += step`
        // wrapped past i64::MAX into an infinite loop. The element-count guard now
        // refuses it before allocating — a single cheap query no longer downs the server.
        let ast = parser::parse("RETURN range(0, 9223372036854775807)").unwrap();
        let err = engine
            .run(&ast)
            .expect_err("an unbounded range must be refused");
        assert!(
            format!("{err:#}").contains("range()"),
            "expected a range() limit error, got: {err:#}"
        );

        // A bounded range still materialises exactly.
        let ast = parser::parse("RETURN range(1, 5)").unwrap();
        let res = engine.run(&ast).unwrap();
        match &res.rows[0][0] {
            Val::List(xs) => assert_eq!(xs.len(), 5),
            other => panic!("expected a list, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn max_rows_limit_is_enforced() {
        let (root, graph, _) = testgen::write_basic("exec_maxrows");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache).with_max_rows(2);
        let ast = parser::parse("MATCH (n) RETURN n.name").unwrap();
        assert!(
            engine.run(&ast).is_err(),
            "5 rows should exceed the cap of 2"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── Regex limits + per-query intermediate budget (Tier-2 hardening) ──────

    /// Open the shared fixture with an intermediate-element budget set.
    fn budgeted_engine(
        root_tag: &str,
        budget: u64,
    ) -> (std::path::PathBuf, Generation, BlockCache, u64) {
        let (root, graph, _) = testgen::write_basic(root_tag);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        (root, gen, cache, budget)
    }

    /// Run `q` against the fixture with the given budget, returning the result.
    fn run_budgeted(root_tag: &str, budget: u64, q: &str) -> Result<QueryResult> {
        let (root, gen, cache, budget) = budgeted_engine(root_tag, budget);
        let engine = Engine::new(&gen, &cache).with_max_intermediate(budget);
        let ast = parser::parse(q).unwrap();
        let res = engine.run(&ast);
        let _ = std::fs::remove_dir_all(&root);
        res
    }

    /// Run `q` with the per-query budget OFF and a server-wide budget set. Asserts
    /// the universal invariant — every query refunds its whole global charge, so
    /// the live counter returns to zero — and returns `(result, peak_charge)`.
    fn run_global(root_tag: &str, global: u64, q: &str) -> (Result<QueryResult>, u64) {
        let (root, graph, _) = testgen::write_basic(root_tag);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let budget = GlobalIntermediateBudget::new(global);
        let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
        let ast = parser::parse(q).unwrap();
        let res = engine.run(&ast);
        let peak = budget.peak();
        assert_eq!(
            budget.in_use(),
            0,
            "every query must refund its whole global charge"
        );
        let _ = std::fs::remove_dir_all(&root);
        (res, peak)
    }

    /// Run `q` with BOTH the per-query and the server-wide budget set, so a test
    /// can assert which guard trips first. Also asserts the global refund invariant.
    fn run_both(root_tag: &str, per_query: u64, global: u64, q: &str) -> Result<QueryResult> {
        let (root, graph, _) = testgen::write_basic(root_tag);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let budget = GlobalIntermediateBudget::new(global);
        let engine = Engine::new(&gen, &cache)
            .with_max_intermediate(per_query)
            .with_global_budget(&budget);
        let ast = parser::parse(q).unwrap();
        let res = engine.run(&ast);
        assert_eq!(
            budget.in_use(),
            0,
            "query must refund its whole global charge"
        );
        let _ = std::fs::remove_dir_all(&root);
        res
    }

    /// True if `res` is the per-query budget error.
    fn is_per_query_budget_err(res: &Result<QueryResult>) -> bool {
        res.as_ref().err().is_some_and(|e| {
            format!("{e:#}").contains("intermediate result budget")
                && !format!("{e:#}").contains("server-wide")
        })
    }

    /// True if `res` is the server-wide budget error.
    fn is_global_budget_err(res: &Result<QueryResult>) -> bool {
        res.as_ref()
            .err()
            .is_some_and(|e| format!("{e:#}").contains("server-wide intermediate budget"))
    }

    /// True if `res` is the transient walk-work (`query.maxScan`) error — the budget a
    /// count-pushdown traversal charges instead of the retained `maxIntermediate`.
    fn is_scan_budget_err(res: &Result<QueryResult>) -> bool {
        res.as_ref()
            .err()
            .is_some_and(|e| format!("{e:#}").contains("scan budget"))
    }

    #[test]
    fn regex_pattern_length_is_capped() {
        // A pattern past MAX_REGEX_PATTERN_BYTES is refused before compilation.
        let long = "a".repeat(2 * MAX_REGEX_PATTERN_BYTES);
        let err = run_err("exec_regex_len", &format!("RETURN 'a' =~ '{long}'"));
        assert!(
            err.contains("regex pattern is"),
            "expected the pattern-length error, got: {err}"
        );
    }

    #[test]
    fn regex_size_limit_is_enforced() {
        // Well under the length cap in source bytes, but the compiled automaton
        // (a^100M via nested bounded repetition) blows the NFA size limit.
        let err = run_err(
            "exec_regex_size",
            "RETURN 'a' =~ '((((a{100}){100}){100}){100})'",
        );
        assert!(
            err.contains("Invalid regex"),
            "expected a size-limit compile error, got: {err}"
        );
    }

    #[test]
    fn regex_cache_compiles_once_per_query() {
        let (root, graph, _) = testgen::write_basic("exec_regex_cache");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        // `=~` evaluates once per Person row; the pattern must compile once.
        let ast = parser::parse("MATCH (n:Person) WHERE n.name =~ 'A.*' RETURN n.name").unwrap();
        let res = engine.run(&ast).unwrap();
        assert_eq!(col0(&res), vec!["Alice"]);
        assert_eq!(
            engine.regex_cache.borrow().len(),
            1,
            "one constant pattern should occupy exactly one cache slot"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn intermediate_budget_caps_comprehension() {
        // range(0, 100000) charges ~100k; the comprehension's output charges
        // another ~100k, so a 150k budget trips inside the comprehension itself.
        let err = run_budgeted(
            "exec_budget_comp",
            150_000,
            "RETURN [x IN range(0, 100000) | x]",
        )
        .expect_err("the comprehension must exceed the budget");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "expected the budget error, got: {err:#}"
        );
    }

    #[test]
    fn intermediate_budget_caps_concat_doubling() {
        // acc + acc doubles per iteration; charging every temp trips the budget
        // after ~12 iterations instead of allocating 2^30 elements.
        let err = run_budgeted(
            "exec_budget_concat",
            10_000,
            "RETURN size(reduce(acc = [0], x IN range(1, 30) | acc + acc))",
        )
        .expect_err("geometric list growth must exceed the budget");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "expected the budget error, got: {err:#}"
        );
    }

    #[test]
    fn intermediate_budget_caps_unwind() {
        // range(0, 1000) charges ~1k and fits; the UNWIND'd rows charge ~1k more
        // and trip a 1.5k budget inside apply_unwind.
        let err = run_budgeted(
            "exec_budget_unwind",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN count(x)",
        )
        .expect_err("the unwound rows must exceed the budget");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "expected the budget error, got: {err:#}"
        );
    }

    #[test]
    fn global_budget_bounds_concurrent_aggregate() {
        // The mechanism the per-query cap cannot provide: two "in-flight" queries
        // charging against one shared budget. Each is individually fine, but their
        // sum trips the ceiling — and the charge is held until each query refunds.
        let b = GlobalIntermediateBudget::new(1_000);
        assert!(b.try_charge(600), "query A within the ceiling");
        assert!(!b.try_charge(600), "query A+B exceed the ceiling");
        assert_eq!(b.in_use(), 1_200, "both charges live until refunded");
        b.release(600);
        assert_eq!(b.in_use(), 600);
        b.release(600);
        assert_eq!(b.in_use(), 0, "all refunded");
        assert_eq!(b.peak(), 1_200, "peak records the high-water");
    }

    #[test]
    fn global_budget_zero_disables() {
        let b = GlobalIntermediateBudget::new(0);
        assert!(b.try_charge(10_000_000), "a 0 limit never rejects");
        assert_eq!(b.in_use(), 0, "a disabled guard never accumulates");
    }

    #[test]
    fn global_budget_trips_with_per_query_off() {
        // Per-query budget disabled (0), but the server-wide guard still bounds the
        // query — and the distinct error names the global knob.
        let (root, graph, _) = testgen::write_basic("exec_global_solo");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let budget = GlobalIntermediateBudget::new(1_500);
        let engine = Engine::new(&gen, &cache)
            .with_max_intermediate(0)
            .with_global_budget(&budget);
        let ast = parser::parse("UNWIND range(0, 1000) AS x RETURN count(x)").unwrap();
        let err = engine
            .run(&ast)
            .expect_err("the global budget must trip with the per-query budget off");
        assert!(
            format!("{err:#}").contains("server-wide intermediate budget"),
            "expected the global-budget error, got: {err:#}"
        );
        assert_eq!(
            budget.in_use(),
            0,
            "a failed query refunds its whole charge"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn global_budget_refunds_after_successful_run() {
        let (root, graph, _) = testgen::write_basic("exec_global_refund");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let budget = GlobalIntermediateBudget::new(10_000);
        let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
        let ast = parser::parse("UNWIND range(0, 100) AS x RETURN count(x)").unwrap();
        engine.run(&ast).expect("well within the budget");
        assert_eq!(budget.in_use(), 0, "a finished query holds no charge");
        assert!(budget.peak() > 0, "it did draw on the budget mid-run");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn global_budget_rises_during_run_and_falls_after() {
        // Observe the live gauge from a second thread *while* a query executes: the
        // global charge must climb above zero during the run and return to zero
        // when it completes (the shared in-flight accounting, end to end).
        use std::sync::atomic::{AtomicBool, Ordering};
        let (root, graph, _) = testgen::write_basic("exec_global_inflight");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        // Generous ceiling so the query never trips the guard; it still charges
        // ~900k elements and holds them for the whole run, so the reader can see it
        // climb. (range() itself caps at 1M elements, so stay under that here.)
        let budget = GlobalIntermediateBudget::new(100_000_000);
        let done = AtomicBool::new(false);
        let mut max_live = 0u64;
        std::thread::scope(|s| {
            s.spawn(|| {
                let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
                let ast = parser::parse("UNWIND range(0, 900000) AS x RETURN count(x)").unwrap();
                engine.run(&ast).expect("within the budget");
                done.store(true, Ordering::Release);
            });
            // Sample the live gauge until the query thread signals completion,
            // yielding each iteration so the worker is not starved (the sampler
            // must not monopolise a constrained scheduler). The deadline is a
            // safety net so a stuck query fails the test rather than hanging it.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            while !done.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
                max_live = max_live.max(budget.in_use());
                std::thread::sleep(std::time::Duration::from_micros(50));
            }
        });
        assert!(
            max_live > 0,
            "the global charge must be observable above zero while the query runs"
        );
        assert_eq!(
            budget.in_use(),
            0,
            "the charge must fall back to zero once the query completes"
        );
        assert!(budget.peak() >= max_live, "peak tracks the live high-water");
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── Per-query budget across every materialising operation ────────────────

    #[test]
    fn intermediate_budget_caps_collect() {
        // collect() buffers all inputs; charging the buffer trips a tight budget.
        let err = run_budgeted(
            "exec_budget_collect",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN collect(x)",
        )
        .expect_err("the collect buffer must exceed the budget");
        assert!(format!("{err:#}").contains("intermediate result budget"));
    }

    #[test]
    fn intermediate_budget_caps_count_distinct() {
        // count(DISTINCT x) holds a `seen` set; charging it trips the budget.
        let err = run_budgeted(
            "exec_budget_distinct",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN count(DISTINCT x)",
        )
        .expect_err("the DISTINCT seen-set must exceed the budget");
        assert!(format!("{err:#}").contains("intermediate result budget"));
    }

    #[test]
    fn intermediate_budget_caps_order_by() {
        // ORDER BY clones every row plus its sort key into a buffer (charged).
        let err = run_budgeted(
            "exec_budget_order",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN x ORDER BY x",
        )
        .expect_err("the ORDER BY buffer must exceed the budget");
        assert!(format!("{err:#}").contains("intermediate result budget"));
    }

    #[test]
    fn intermediate_budget_caps_group_by() {
        // A distinct grouping key per row creates ~N groups; charging each group
        // (plus the unwound rows) trips the budget.
        let err = run_budgeted(
            "exec_budget_group",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN x AS g, count(*) AS n",
        )
        .expect_err("the group table must exceed the budget");
        assert!(format!("{err:#}").contains("intermediate result budget"));
    }

    #[test]
    fn intermediate_budget_caps_union() {
        // A UNION accumulates both branches (and a DISTINCT seen-set); a tight
        // budget trips while building it.
        let err = run_budgeted(
            "exec_budget_union",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN x \
             UNION UNWIND range(0, 1000) AS y RETURN y",
        )
        .expect_err("the UNION buildup must exceed the budget");
        assert!(format!("{err:#}").contains("intermediate result budget"));
    }

    #[test]
    fn intermediate_budget_zero_disables_the_cap() {
        // A 0 budget means unlimited: a large materialisation completes.
        let res = run_budgeted(
            "exec_budget_zero",
            0,
            "UNWIND range(0, 200000) AS x RETURN count(x)",
        )
        .expect("a 0 budget must not cap anything");
        assert_eq!(res.rows.len(), 1);
    }

    #[test]
    fn intermediate_budget_allows_within_limit() {
        // Comfortably under the cap → the query succeeds.
        let res = run_budgeted(
            "exec_budget_within",
            100_000,
            "UNWIND range(0, 1000) AS x RETURN count(x)",
        )
        .expect("a query within the budget must succeed");
        assert_eq!(res.rows.len(), 1);
    }

    #[test]
    fn intermediate_budget_threshold_passes_then_trips() {
        // The same materialisation passes under a generous cap and trips under a
        // tight one — the budget actually gates on the charged element count.
        run_budgeted(
            "exec_budget_thresh_ok",
            50_000,
            "RETURN [x IN range(0, 1000) | x]",
        )
        .expect("generous budget passes");
        let err = run_budgeted(
            "exec_budget_thresh_no",
            1_500,
            "RETURN [x IN range(0, 1000) | x]",
        )
        .expect_err("tight budget trips");
        assert!(format!("{err:#}").contains("intermediate result budget"));
    }

    // ── Server-wide budget across the same operations ────────────────────────

    #[test]
    fn global_budget_trips_on_comprehension() {
        let (res, _) = run_global("exec_g_comp", 1_500, "RETURN [x IN range(0, 1000) | x]");
        assert!(is_global_budget_err(&res), "got: {res:?}");
    }

    #[test]
    fn global_budget_trips_on_collect() {
        let (res, _) = run_global(
            "exec_g_collect",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN collect(x)",
        );
        assert!(is_global_budget_err(&res), "got: {res:?}");
    }

    #[test]
    fn global_budget_trips_on_count_distinct() {
        let (res, _) = run_global(
            "exec_g_distinct",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN count(DISTINCT x)",
        );
        assert!(is_global_budget_err(&res), "got: {res:?}");
    }

    #[test]
    fn global_budget_trips_on_order_by() {
        let (res, _) = run_global(
            "exec_g_order",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN x ORDER BY x",
        );
        assert!(is_global_budget_err(&res), "got: {res:?}");
    }

    #[test]
    fn global_budget_trips_on_union() {
        let (res, _) = run_global(
            "exec_g_union",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN x UNION UNWIND range(0, 1000) AS y RETURN y",
        );
        assert!(is_global_budget_err(&res), "got: {res:?}");
    }

    #[test]
    fn global_budget_allows_small_query() {
        let (res, peak) = run_global("exec_g_small", 100_000, "RETURN [x IN range(0, 50) | x]");
        assert!(res.is_ok(), "a small query must not trip: {res:?}");
        assert!(peak > 0, "it still drew on the budget");
    }

    #[test]
    fn global_budget_zero_completes_large() {
        // Per-query off and global 0 → no cap; a large materialisation completes
        // and the (disabled) counter never accumulates.
        let (res, peak) = run_global(
            "exec_g_zero",
            0,
            "UNWIND range(0, 200000) AS x RETURN count(x)",
        );
        assert!(res.is_ok(), "0 disables the guard: {res:?}");
        assert_eq!(peak, 0, "a disabled guard never accumulates");
    }

    #[test]
    fn global_budget_refunds_after_a_trip() {
        // run_global already asserts in_use == 0; make the failure path explicit.
        let (res, _) = run_global(
            "exec_g_refund_fail",
            1_500,
            "UNWIND range(0, 1000) AS x RETURN collect(x)",
        );
        assert!(is_global_budget_err(&res), "expected a trip: {res:?}");
    }

    // ── Interaction of the two budgets ───────────────────────────────────────

    #[test]
    fn per_query_budget_trips_first_when_tighter() {
        // Tighter per-query cap (1500) beneath a roomy global (10M) → the per-query
        // guard fires, named by its own error.
        let res = run_both(
            "exec_both_pq",
            1_500,
            10_000_000,
            "UNWIND range(0, 1000) AS x RETURN collect(x)",
        );
        assert!(is_per_query_budget_err(&res), "got: {res:?}");
    }

    #[test]
    fn global_budget_trips_first_when_tighter() {
        // Tighter global (1500) beneath a roomy per-query cap (10M) → the
        // server-wide guard fires, named by its own error.
        let res = run_both(
            "exec_both_g",
            10_000_000,
            1_500,
            "UNWIND range(0, 1000) AS x RETURN collect(x)",
        );
        assert!(is_global_budget_err(&res), "got: {res:?}");
    }

    #[test]
    fn both_budgets_off_completes_large() {
        let res = run_both(
            "exec_both_off",
            0,
            0,
            "UNWIND range(0, 200000) AS x RETURN count(x)",
        );
        assert!(res.is_ok(), "both budgets off → no cap: {res:?}");
    }

    // ── Expansion charge: a hub read must trip the budget (root cause 2b) ─────

    /// Few-thousand-edge hub; comfortably clears `EXPAND_PAR_MIN` (64) so the pooled
    /// reader fans out, and small enough to build in well under a millisecond.
    const HUB_N: u64 = 3_000;
    /// Far below `HUB_N`, so a single hub expansion (which charges ~`HUB_N`) trips it.
    const HUB_TIGHT: u64 = 100;
    /// Far above the whole star's cumulative charge (~a few × `HUB_N`), so a full
    /// expansion completes — the guard must bound hubs without over-charging.
    const HUB_GENEROUS: u64 = 10_000_000;

    /// Run `q` against an `n`-leaf hub fixture (see [`testgen::write_hub`]) with the
    /// given per-query and server-wide budgets (0 disables either), optionally behind
    /// a fanout pool so the parallel `expand_chain_par` path is exercised. Asserts the
    /// universal refund invariant and returns `(result, global_peak)`.
    fn run_hub(
        tag: &str,
        n: u64,
        per_query: u64,
        scan: u64,
        global: u64,
        with_pool: bool,
        q: &str,
    ) -> (Result<QueryResult>, u64) {
        let (root, graph) = testgen::write_hub(tag, n);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let budget = GlobalIntermediateBudget::new(global);
        let mut engine = Engine::new(&gen, &cache)
            .with_max_intermediate(per_query)
            .with_max_scan(scan)
            .with_global_budget(&budget);
        if with_pool {
            engine = engine.with_fanout_pool(Some(std::sync::Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(3)
                    .build()
                    .unwrap(),
            )));
        }
        let res = engine.run(&parser::parse(q).unwrap());
        let peak = budget.peak();
        assert_eq!(
            budget.in_use(),
            0,
            "every query must refund its whole global charge"
        );
        let _ = std::fs::remove_dir_all(&root);
        (res, peak)
    }

    // ── Per-query-type budget routing (the retention split) ───────────────────
    // The same hub adjacency read is charged against a *different* budget depending on
    // what the query does with the rows. `RETURN count(*)` is count-pushdown — it
    // retains nothing, so its reads charge the transient `maxScan` budget and never the
    // retained `maxIntermediate` nor the server-wide aggregate. A row-returning or
    // var-length traversal materialises, so the same reads charge `maxIntermediate`
    // (and the global budget). run_hub args: (tag, n, maxIntermediate, maxScan, global).

    #[test]
    fn hub_count_one_hop_answered_by_degree_terminal() {
        // The degree-sum terminal answers a 1-hop `count(neighbour)` from the hub's stored
        // out-degree in O(1) — it never walks the `HUB_N`-edge adjacency, so the tight scan
        // cap the old row-by-row walk tripped is no longer even approached. (The 2-hop
        // variant below still trips: building its penultimate frontier reads the hub.)
        let (res, _) = run_hub(
            "exec_hub_1hop_degterm",
            HUB_N,
            0,
            HUB_TIGHT,
            0,
            false,
            "MATCH (c:Hub)-[:LINK]->(x) RETURN count(x)",
        );
        let r = res.expect("degree terminal answers a 1-hop hub count without tripping maxScan");
        assert!(
            matches!(r.rows[0][0], Val::Int(n) if n == HUB_N as i64),
            "1-hop count == hub out-degree: {:?}",
            r.rows[0][0]
        );
    }

    #[test]
    fn hub_count_two_hop_trips_scan_budget() {
        let (res, _) = run_hub(
            "exec_hub_2hop_scan",
            HUB_N,
            0,
            HUB_TIGHT,
            0,
            false,
            "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN count(y)",
        );
        assert!(is_scan_budget_err(&res), "got: {res:?}");
    }

    #[test]
    fn hub_count_filtered_trips_scan_with_zero_rows() {
        // 2b for counts: `:Hub` matches only the centre, so every neighbour is rejected
        // and ZERO rows complete — yet the adjacency read still charges scan and trips.
        let (res, _) = run_hub(
            "exec_hub_filt_scan",
            HUB_N,
            0,
            HUB_TIGHT,
            0,
            false,
            "MATCH (c:Hub)-[:LINK]->(x:Hub) RETURN count(x)",
        );
        assert!(
            is_scan_budget_err(&res),
            "a filtered count read (no rows complete) must still trip maxScan: {res:?}"
        );
    }

    #[test]
    fn hub_count_ignores_retained_and_global_budgets() {
        // The crux of the split: with the retained *and* global budgets tight (well
        // below `HUB_N`) but scan generous, the count still completes with the right
        // answer — it draws neither — and never charges the server-wide aggregate.
        let (res, peak) = run_hub(
            "exec_hub_count_iso",
            HUB_N,
            HUB_TIGHT,
            HUB_GENEROUS,
            HUB_TIGHT,
            false,
            "MATCH (c:Hub)-[:LINK]->(x) RETURN count(x) AS n",
        );
        let res = res.expect("a count must not draw the retained/global budgets");
        assert_eq!(col0(&res), vec![HUB_N.to_string()]);
        assert!(
            peak < HUB_N,
            "count-pushdown must not charge the per-edge reads to the server-wide \
             aggregate: peak={peak}"
        );
    }

    #[test]
    fn hub_materialize_one_hop_trips_per_query_budget() {
        // Row-returning: the same read materialises, so it charges the retained budget.
        let (res, _) = run_hub(
            "exec_hub_1hop_pq",
            HUB_N,
            HUB_TIGHT,
            0,
            0,
            false,
            "MATCH (c:Hub)-[:LINK]->(x) RETURN x",
        );
        assert!(
            is_per_query_budget_err(&res),
            "a materialising hub read must trip maxIntermediate: {res:?}"
        );
    }

    #[test]
    fn hub_materialize_one_hop_trips_global_budget() {
        let (res, _) = run_hub(
            "exec_hub_1hop_g",
            HUB_N,
            0,
            0,
            HUB_TIGHT,
            false,
            "MATCH (c:Hub)-[:LINK]->(x) RETURN x",
        );
        assert!(
            is_global_budget_err(&res),
            "a materialising hub read must trip the server-wide budget: {res:?}"
        );
    }

    #[test]
    fn hub_materialize_two_hop_trips_per_query_budget() {
        let (res, _) = run_hub(
            "exec_hub_2hop_pq",
            HUB_N,
            HUB_TIGHT,
            0,
            0,
            false,
            "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN y",
        );
        assert!(is_per_query_budget_err(&res), "got: {res:?}");
    }

    #[test]
    fn hub_varlen_count_charges_retained_not_scan() {
        // The two-regime nuance the sweep found: a *var-length* `count(*)` still
        // materialises its per-node path set, so even under count-pushdown it charges
        // the retained budget (and trips it) — unlike a fixed-hop count, which is pure
        // scan. With scan disabled, the trip can only be the retained path materialise.
        let (res, _) = run_hub(
            "exec_hub_varlen_count",
            HUB_N,
            HUB_TIGHT,
            0,
            0,
            false,
            "MATCH (c:Hub)-[:LINK*1..2]->(x) RETURN count(*)",
        );
        assert!(
            is_per_query_budget_err(&res),
            "a var-length count materialises paths and must trip maxIntermediate: {res:?}"
        );
    }

    #[test]
    fn frame_get_flatten_shadowing() {
        // Pins the shadowing convention that makes the parallel walk match the
        // sequential LIFO oracle: a child frame shadows its parent, the last write in
        // a layer wins, and `flatten` (root-first) reproduces both.
        use std::sync::Arc;
        let mut base = HashMap::new();
        base.insert("a".to_string(), Val::Int(1));
        base.insert("b".to_string(), Val::Int(2));
        let root = Frame::root(&base);
        let child = Arc::new(Frame {
            parent: Some(root),
            delta: vec![("b".into(), Val::Int(20))],
        });
        let grand = Arc::new(Frame {
            parent: Some(child),
            delta: vec![("a".into(), Val::Int(100)), ("a".into(), Val::Int(101))],
        });
        assert!(
            matches!(grand.get("b"), Some(Val::Int(20))),
            "child shadows parent"
        );
        assert!(
            matches!(grand.get("a"), Some(Val::Int(101))),
            "last delta wins"
        );
        assert!(grand.get("c").is_none());
        let flat = grand.flatten();
        assert_eq!(flat.len(), 2);
        assert!(matches!(flat.get("a"), Some(Val::Int(101))));
        assert!(matches!(flat.get("b"), Some(Val::Int(20))));
    }

    #[test]
    fn count_pushdown_matches_materialized() {
        // The pushed-down `count(*)`/`count(v)` must equal the row count the
        // materialising path produces — across 1/2/3-hop, a constant co-item, and an
        // empty match. (write_basic: KNOWS Alice->Bob, Bob->Carol.)
        let count_of = |tag: &str, q: &str| -> i64 {
            match &run_budgeted(tag, 1_000_000, q).unwrap().rows[0][0] {
                Val::Int(n) => *n,
                o => panic!("count is not an Int: {o:?}"),
            }
        };
        let rows_of =
            |tag: &str, q: &str| -> usize { run_budgeted(tag, 1_000_000, q).unwrap().rows.len() };
        let cases = [
            (
                "MATCH (a:Person)-[:KNOWS]->(b) RETURN count(*) AS c",
                "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name AS b",
            ),
            (
                "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(c) AS c",
                "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name AS x",
            ),
            (
                "MATCH (a:Person)-[:KNOWS]->(b) RETURN count(*) AS c, 7 AS k",
                "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name AS b",
            ),
            (
                // empty: 3-hop KNOWS dead-ends at Carol.
                "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN count(*) AS c",
                "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN d.name AS d",
            ),
        ];
        for (cq, rq) in cases {
            assert_eq!(
                count_of("cpd_eq", cq) as usize,
                rows_of("cpd_eq", rq),
                "`{cq}`"
            );
        }
    }

    #[test]
    fn count_pushdown_falls_back_but_correct() {
        // Shapes that must NOT push down still return the correct count via the
        // materialising path.
        let count_of = |q: &str| -> i64 {
            match &run_budgeted("cpd_fb", 1_000_000, q).unwrap().rows[0][0] {
                Val::Int(n) => *n,
                o => panic!("count is not an Int: {o:?}"),
            }
        };
        // count(DISTINCT) — KNOWS targets {Bob, Carol} = 2 distinct (not pushed: needs
        // the value set), vs 3 total KNOWS edges (Alice->Bob, Bob->Carol, Alice->Carol).
        assert_eq!(
            count_of("MATCH (a:Person)-[:KNOWS]->(b) RETURN count(DISTINCT b) AS c"),
            2
        );
        // WHERE survivor filter — only Alice->Bob of the 3 KNOWS edges (falls back to
        // the materialising path, which applies WHERE).
        assert_eq!(
            count_of("MATCH (a:Person)-[:KNOWS]->(b) WHERE b.name = 'Bob' RETURN count(*) AS c"),
            1
        );
    }

    #[test]
    fn hub_small_expansion_succeeds_under_a_generous_budget() {
        // The guard must bound hubs without over-charging: a generous scan budget lets
        // the whole star expand and return the right count. A materialising run of the
        // same shape really draws the server-wide aggregate (≥ one charge per edge read).
        let (res, _) = run_hub(
            "exec_hub_small_ok",
            HUB_N,
            HUB_GENEROUS,
            HUB_GENEROUS,
            HUB_GENEROUS,
            false,
            "MATCH (c:Hub)-[:LINK]->(x) RETURN count(x) AS n",
        );
        let res = res.expect("a generous budget must let the hub expand");
        assert_eq!(col0(&res), vec![HUB_N.to_string()]);
        let (mat, peak) = run_hub(
            "exec_hub_small_mat",
            HUB_N,
            HUB_GENEROUS,
            0,
            HUB_GENEROUS,
            false,
            "MATCH (c:Hub)-[:LINK]->(x) RETURN x",
        );
        mat.expect("materialise under a generous budget");
        assert!(
            peak >= HUB_N,
            "a materialising expansion must charge the aggregate ≥ once per edge read: peak={peak}"
        );
    }

    #[test]
    fn hub_expansion_charge_on_parallel_path() {
        // The fanout pool routes a fixed multi-hop chain through `expand_chain_par`,
        // whose adjacency reads gather on rayon — where the per-query `Cell` charge
        // state cannot be touched. The charge is applied on the calling thread once the
        // buffer lands, so the pooled walk routes to the SAME budget as the sequential
        // one: a count trips `maxScan`, a materialising walk trips `maxIntermediate` /
        // the global budget, and under generous budgets both return the sequential
        // result. The hop-1 frontier (`HUB_N` leaves) clears `EXPAND_PAR_MIN`, so the
        // pooled reader truly fans out rather than degrading to a sequential read.
        let cq = "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN count(y) AS n";
        let mq = "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN y";
        // count-pushdown on the pooled path → scan budget.
        let (scan, _) = run_hub("exec_hub_par_scan", HUB_N, 0, HUB_TIGHT, 0, true, cq);
        assert!(
            is_scan_budget_err(&scan),
            "the pooled count must trip maxScan: {scan:?}"
        );
        // materialising on the pooled path → retained + global budgets.
        let (pq, _) = run_hub("exec_hub_par_pq", HUB_N, HUB_TIGHT, 0, 0, true, mq);
        assert!(
            is_per_query_budget_err(&pq),
            "the pooled materialising walk must trip maxIntermediate: {pq:?}"
        );
        let (g, _) = run_hub("exec_hub_par_g", HUB_N, 0, 0, HUB_TIGHT, true, mq);
        assert!(
            is_global_budget_err(&g),
            "the pooled materialising walk must trip the server-wide budget: {g:?}"
        );
        // Generous budgets: pooled and sequential counts agree exactly.
        let (par, _) = run_hub(
            "exec_hub_par_ok",
            HUB_N,
            HUB_GENEROUS,
            HUB_GENEROUS,
            HUB_GENEROUS,
            true,
            cq,
        );
        let (seq, _) = run_hub(
            "exec_hub_seq_ok",
            HUB_N,
            HUB_GENEROUS,
            HUB_GENEROUS,
            HUB_GENEROUS,
            false,
            cq,
        );
        let par = par.expect("pooled generous run");
        let seq = seq.expect("sequential generous run");
        assert_eq!(
            col0(&par),
            col0(&seq),
            "pooled and sequential expansions must agree"
        );
        assert_eq!(col0(&par), vec![HUB_N.to_string()]);
    }

    #[test]
    fn engine_is_not_sync_rayon_invariant() {
        // Compile-time guard-rail for the rayon-safety invariant. The entire argument
        // that `par_gather`/`par_walk` are race-free rests on `&Engine` never crossing a
        // thread boundary: the `Sync + Send` bound on `par_gather`'s closure can only
        // reject a closure that captures `&self` *because* `Engine` is `!Sync` (its
        // per-query `Cell`/`RefCell` charge state — `budget_used`, `scan_used`,
        // `count_acc`, `global_charged`, `regex_cache`). If a future change makes that
        // state `Sync` (e.g. swapping a `Cell` for an `Atomic` to "charge in parallel"),
        // the `AmbiguousIfSync` resolution below becomes ambiguous and this stops
        // compiling — forcing a deliberate re-read of `charge_walk` and the `par_gather`
        // contract before the invariant is weakened.
        trait AmbiguousIfSync<A> {
            fn _f() {}
        }
        impl<T: ?Sized> AmbiguousIfSync<()> for T {}
        impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}
        // Resolves to the blanket `()` impl unambiguously iff `Engine` is NOT `Sync`.
        let _ = <Engine<'static, Generation> as AmbiguousIfSync<_>>::_f;
    }

    // ── Engine reuse: the charge resets and refunds per run ───────────────────

    #[test]
    fn global_charge_resets_between_runs_on_a_reused_engine() {
        let (root, graph, _) = testgen::write_basic("exec_g_reuse");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let budget = GlobalIntermediateBudget::new(100_000);
        let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
        let ast = parser::parse("UNWIND range(0, 500) AS x RETURN count(x)").unwrap();
        for _ in 0..5 {
            engine.run(&ast).expect("within the budget");
            assert_eq!(budget.in_use(), 0, "each run fully refunds before the next");
        }
        // A reused engine that has succeeded many times still trips correctly when a
        // single run exceeds the budget (no stale carry-over inflating the charge).
        let big = parser::parse("UNWIND range(0, 200000) AS x RETURN collect(x)").unwrap();
        assert!(
            engine.run(&big).is_err(),
            "the oversized run must still trip"
        );
        assert_eq!(budget.in_use(), 0, "the tripped run also refunds");
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── GlobalIntermediateBudget mechanics ───────────────────────────────────

    #[test]
    fn global_budget_starts_at_zero() {
        let b = GlobalIntermediateBudget::new(1_000);
        assert_eq!(b.in_use(), 0);
        assert_eq!(b.peak(), 0);
        assert_eq!(b.limit(), 1_000);
    }

    #[test]
    fn global_budget_charge_to_exact_limit_then_trips() {
        let b = GlobalIntermediateBudget::new(1_000);
        assert!(
            b.try_charge(1_000),
            "charging exactly to the limit is allowed"
        );
        assert_eq!(b.in_use(), 1_000);
        assert!(!b.try_charge(1), "one element past the limit trips");
        b.release(1_001);
        assert_eq!(b.in_use(), 0);
    }

    #[test]
    fn global_budget_release_cycles_return_to_zero() {
        let b = GlobalIntermediateBudget::new(10_000);
        for _ in 0..1_000 {
            assert!(b.try_charge(7));
            b.release(7);
        }
        assert_eq!(b.in_use(), 0, "balanced charge/release nets to zero");
        assert!(b.peak() >= 7, "peak captured the per-cycle high-water");
    }

    #[test]
    fn varlen_charges_intermediate_budget() {
        // A tiny budget trips while materialising variable-length paths…
        let err = run_budgeted(
            "exec_budget_varlen_tiny",
            2,
            "MATCH (a)-[*1..3]->(b) RETURN count(*)",
        )
        .expect_err("varlen paths must charge the budget");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "expected the budget error, got: {err:#}"
        );
        // …and a generous budget leaves the same query untouched (no over-charge).
        let res = run_budgeted(
            "exec_budget_varlen_ok",
            1_000_000,
            "MATCH (a)-[*1..3]->(b) RETURN count(*)",
        )
        .expect("a generous budget must not affect the query");
        assert_eq!(res.rows.len(), 1);
    }

    #[test]
    fn correlated_unwind_seek_returns_right_rows() {
        // `UNWIND … AS w MATCH (n:Person {name:w})` keys the anchor off the per-row
        // scalar `w`. The planner now resolves it to a `node_Person_name` index seek
        // (see plan.rs `bound_scalar_*` tests); this proves the seek path is sound
        // end-to-end — the right rows, no more, no fewer.
        let (root, res) = run(
            "exec_correlated_unwind",
            "UNWIND ['Alice', 'Bob', 'Nobody'] AS w \
             MATCH (n:Person {name: w}) RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn correlated_where_seek_returns_right_rows() {
        // The `WHERE n.name = w` spelling resolves to the same per-row seek.
        let (root, res) = run(
            "exec_correlated_where",
            "UNWIND ['Carol', 'Bob'] AS w \
             MATCH (n:Person) WHERE n.name = w RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn distinct_charges_intermediate_budget() {
        // The `seen` set behind `RETURN DISTINCT` is charged: a budget that admits
        // the 3-row match (3) but not the DISTINCT pass (+3) trips; 1M is untouched.
        let err = run_budgeted(
            "exec_budget_distinct_tiny",
            5,
            "MATCH (n:Person) RETURN DISTINCT n.city",
        )
        .expect_err("DISTINCT must charge the budget");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "expected the budget error, got: {err:#}"
        );
        let res = run_budgeted(
            "exec_budget_distinct_ok",
            1_000_000,
            "MATCH (n:Person) RETURN DISTINCT n.city",
        )
        .expect("a generous budget must not affect the query");
        assert_eq!(res.rows.len(), 2); // London, Paris
    }

    #[test]
    fn order_by_charges_intermediate_budget() {
        // The `keyed` sort buffer clones every row; charged before it is built.
        let err = run_budgeted(
            "exec_budget_order_tiny",
            5,
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
        )
        .expect_err("ORDER BY must charge the budget");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "expected the budget error, got: {err:#}"
        );
        let res = run_budgeted(
            "exec_budget_order_ok",
            1_000_000,
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
        )
        .expect("a generous budget must not affect the query");
        assert_eq!(res.rows.len(), 3);
    }

    #[test]
    fn group_by_charges_intermediate_budget() {
        // Each distinct group costs one element; a budget that admits the match (3)
        // and the first group but not the second (Paris) trips.
        let err = run_budgeted(
            "exec_budget_group_tiny",
            4,
            "MATCH (n:Person) RETURN n.city, count(*)",
        )
        .expect_err("GROUP BY must charge the budget");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "expected the budget error, got: {err:#}"
        );
        let res = run_budgeted(
            "exec_budget_group_ok",
            1_000_000,
            "MATCH (n:Person) RETURN n.city, count(*)",
        )
        .expect("a generous budget must not affect the query");
        assert_eq!(res.rows.len(), 2); // {London: 2}, {Paris: 1}
    }

    #[test]
    fn all_shortest_frontier_charges_intermediate_budget() {
        // `ALL SHORTEST`/`SHORTEST k` keep the cloned-per-branch simple-path search
        // (the number of shortest paths can be exponential), whose BFS frontier is
        // charged per expansion layer so a hub-dense graph trips the budget mid-search
        // instead of ballooning RSS. The destination (a Company) is unreachable over
        // `:KNOWS`, so no *result* is ever charged — only the frontier — yet a tiny
        // budget still trips.
        let q = "MATCH (a:Person {name:'Alice'}), (z:Company {name:'Acme'}) \
                 MATCH ALL SHORTEST (a)-[:KNOWS*]->(z) RETURN count(*) AS c";
        let err = run_budgeted("exec_budget_allsp_tiny", 3, q)
            .expect_err("the ALL SHORTEST frontier must charge the budget");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "expected the budget error, got: {err:#}"
        );
        let res = run_budgeted("exec_budget_allsp_ok", 1_000_000, q)
            .expect("a generous budget must not affect the query");
        assert_eq!(col0(&res), vec!["0"]); // no KNOWS path Person→Company
    }

    #[test]
    fn shortest_path_any_succeeds_under_tiny_budget() {
        // `shortestPath()`/`ANY SHORTEST` now runs a single global-`visited` BFS that
        // enqueues each node at most once and charges no frontier, so it succeeds in
        // `O(V+E)` under a budget the old cloned-per-branch search would trip on (3 is
        // below where the frontier charge fired for `all_shortest_frontier_*`). The
        // unreachable-Company probe returns NULL cheaply; a reachable pair returns its
        // length.
        let unreachable = "MATCH (a:Person {name:'Alice'}), (z:Company {name:'Acme'}) \
                           RETURN shortestPath((a)-[:KNOWS*]->(z)) IS NULL AS np";
        let res = run_budgeted("exec_budget_anysp_unreach", 3, unreachable)
            .expect("the global-visited BFS must not charge the frontier");
        assert_eq!(col0(&res), vec!["true"]); // no KNOWS path Person→Company

        let reachable = "MATCH (a:Person {name:'Alice'}), (z:Person {name:'Carol'}) \
                         RETURN length(shortestPath((a)-[:KNOWS*]->(z))) AS l";
        let res = run_budgeted("exec_budget_anysp_reach", 3, reachable)
            .expect("the global-visited BFS must not charge the frontier");
        assert_eq!(col0(&res), vec!["1"]); // Alice-[:KNOWS]->Carol directly (e4)
    }

    #[test]
    fn shortest_path_explore_cap_bounds_the_bfs() {
        // The dedicated `maxShortestPathExplore` cap bounds the global-visited BFS
        // independently of `maxIntermediate`: the reachable pair the unlimited BFS
        // finds above fails *cleanly* (no panic, no OOM) once the discovery count
        // exceeds the cap, while the default (0 = unlimited) still succeeds and the
        // re-derived path keeps its correct length.
        let q = "MATCH (a:Person {name:'Alice'}), (z:Person {name:'Carol'}) \
                 RETURN length(shortestPath((a)-[:KNOWS*]->(z))) AS l";
        let (root, gen, cache, _) = budgeted_engine("exec_sp_explore_cap", 1_000_000);
        let err = Engine::new(&gen, &cache)
            .with_max_shortest_path_explore(1)
            .run(&parser::parse(q).unwrap())
            .expect_err("the explore cap must bound the BFS");
        assert!(
            format!("{err:#}").contains("maxShortestPathExplore"),
            "expected the explore-cap error, got: {err:#}"
        );
        let res = Engine::new(&gen, &cache)
            .with_max_shortest_path_explore(0)
            .run(&parser::parse(q).unwrap())
            .expect("the default unlimited cap must succeed");
        assert_eq!(col0(&res), vec!["1"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn shortest_path_meets_in_the_middle() {
        // A length-2 shortest path exercises the bidirectional search's meet-in-middle
        // and reconstruction *across* the meeting node — the endpoints share no direct
        // edge; the path is Acme -WORKS_AT- Alice -KNOWS- Bob (undirected, mixed type).
        let q = "MATCH (a:Company {name:'Acme'}), (b:Person {name:'Bob'}) \
                 RETURN length(shortestPath((a)-[*..6]-(b))) AS l";
        let res = run_budgeted("exec_sp_midmeet", 1_000_000, q).expect("a length-2 path exists");
        assert_eq!(col0(&res), vec!["2"]);
    }

    #[test]
    fn shortest_path_with_pool_is_correct() {
        // A pool-configured engine must return identical results to the sequential one
        // (the parallel frontier gather shares the same neighbour logic; the full-graph
        // benchmark exercises the large-frontier rayon branch).
        let (root, gen, cache, _) = budgeted_engine("exec_sp_pool", 1_000_000);
        let pool = std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let q = "MATCH (a:Company {name:'Acme'}), (b:Person {name:'Bob'}) \
                 RETURN length(shortestPath((a)-[*..6]-(b))) AS l";
        let res = Engine::new(&gen, &cache)
            .with_fanout_pool(Some(pool))
            .run(&parser::parse(q).unwrap())
            .expect("pool-configured shortestPath runs");
        assert_eq!(col0(&res), vec!["2"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Slice 2 integration: routing a hub through the streaming reader must return
    /// **identical** results to materialising it — for both the sequential (`expand_chain`)
    /// and pooled (`par_walk`) engines, over count / ordered-rows / undirected / path-var /
    /// relationship-property (`rel_ok`) shapes. Driven by a low `adj_stream_threshold` (2)
    /// so `write_basic`'s degree-3 anchor (Alice) streams while its degree-1 neighbours
    /// materialise — a genuine hub/normal mix in one frontier. Each query is run four ways
    /// (seq/pool × stream/materialise); all four must agree byte-for-byte.
    #[test]
    fn hub_streaming_matches_materialise() {
        let (root, gen, cache, _) = budgeted_engine("exec_hub_stream", 1_000_000);
        let pool = std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap(),
        );
        let disp = |r: &QueryResult| -> Vec<Vec<String>> {
            r.rows
                .iter()
                .map(|row| row.iter().map(|c| c.to_display()).collect())
                .collect()
        };
        let queries = [
            // 2-hop count — the count-pushdown terminal over a streamed hub frontier.
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(*)",
            // 2-hop ordered rows with rel + node vars bound (Alice is the streamed hub).
            "MATCH (a:Person)-[r1:KNOWS]->(b)-[r2:KNOWS]->(c) \
             RETURN a.name AS a, b.name AS b, c.name AS c ORDER BY a, b, c",
            // Type-alternation one-hop from the hub anchor (mixes KNOWS + WORKS_AT out-edges).
            "MATCH (a:Person {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b ORDER BY b",
            // Same, UNORDERED — locks row-order preservation (streamed hop order must equal
            // the materialised `hops_par` order, not merely the same set).
            "MATCH (a:Person {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b",
            // Undirected one-hop from the hub anchor (outgoing-then-incoming stream order).
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]-(x) RETURN x.name AS x ORDER BY x",
            // Path variable: the reconstructed path must match streamed vs materialised.
            "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN length(p) AS len, nodes(p) AS ns",
            // Relationship-property predicate: gated OUT of the parallel path (has props),
            // so this exercises `expand_chain`'s hub arm applying `rel_ok` per streamed hop.
            "MATCH (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b) RETURN b.name AS b ORDER BY b",
        ];
        for q in queries {
            let ast = parser::parse(q).unwrap();
            let run = |pool: Option<std::sync::Arc<rayon::ThreadPool>>, threshold: u64| {
                let mut e = Engine::new(&gen, &cache).with_adj_stream_threshold(threshold);
                if let Some(p) = pool {
                    e = e.with_fanout_pool(Some(p));
                }
                e.run(&ast)
                    .unwrap_or_else(|err| panic!("`{q}` (threshold {threshold}) failed: {err:#}"))
            };
            // Materialise baseline (threshold beyond any degree) on the sequential engine.
            let base = run(None, u64::MAX);
            let variants = [
                ("seq+stream", run(None, 2)),
                ("pool+materialise", run(Some(pool.clone()), u64::MAX)),
                ("pool+stream", run(Some(pool.clone()), 2)),
            ];
            for (tag, v) in &variants {
                assert_eq!(base.columns, v.columns, "columns differ ({tag}) for `{q}`");
                assert_eq!(disp(&base), disp(v), "rows differ ({tag}) for `{q}`");
            }
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn multi_hop_with_pool_matches_sequential() {
        // The parallel breadth-first chain expansion (`expand_chain_par`) must return
        // exactly the rows — and in the same order — as the sequential depth-first
        // walk, across fixed multi-hop chains, a path variable, a pushed LIMIT, and a
        // tight intermediate budget. The fixture frontier is below `EXPAND_PAR_MIN`, so
        // `par_gather` reads sequentially here; this pins `expand_chain_par`'s merge
        // (node_ok / next-var / charge / cap / path binding) against the DFS path,
        // while the full-Wikidata benchmark exercises the wide-frontier rayon branch.
        let (root, gen, cache, _) = budgeted_engine("exec_multihop_pool", 1_000_000);
        let pool = std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap(),
        );
        // Var-length is gated OUT of the parallel path, so a `*1..2` query is the
        // sequential walk under both engines — still asserted identical to lock the gate.
        let queries = [
            // 2-hop, ordered, with both rel and node vars bound.
            "MATCH (a:Person)-[r1:KNOWS]->(b)-[r2:KNOWS]->(c) \
             RETURN a.name AS a, b.name AS b, c.name AS c ORDER BY a, b, c",
            // 3-hop mixed types ending in WORKS_AT.
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:WORKS_AT]->(d) \
             RETURN a.name AS a, d.name AS d ORDER BY a, d",
            // Undirected one-hop from a pinned anchor (outgoing-then-incoming order).
            "MATCH (a:Person {name:'Bob'})-[:KNOWS]-(x) RETURN x.name AS x ORDER BY x",
            // Path variable: the bound path must reconstruct identically.
            "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN length(p) AS len, nodes(p) AS ns",
            // Type alternation + an anchor with no LIMIT/ORDER (pushed-cap off).
            "MATCH (a:Person {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b ORDER BY b",
            // Inline property on a non-anchor node — exercises `node_ok` reading the
            // shared `Scope::Frame` on the parallel walk (Bob KNOWS Carol).
            "MATCH (a:Person)-[:KNOWS]->(b {name:'Carol'}) RETURN a.name AS a ORDER BY a",
            // Pushed LIMIT on a 2-hop — gated to the sequential early-exit path under
            // both engines (a capped chain must not breadth-first over-read).
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name AS c LIMIT 1",
            // Variable-length — gated to the sequential path under both engines.
            "MATCH (a:Person {name:'Alice'})-[:KNOWS*1..2]->(b) RETURN b.name AS b ORDER BY b",
        ];
        for q in queries {
            let ast = parser::parse(q).unwrap();
            let seq = Engine::new(&gen, &cache)
                .run(&ast)
                .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
            let par = Engine::new(&gen, &cache)
                .with_fanout_pool(Some(pool.clone()))
                .run(&ast)
                .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
            // Whole-result equality preserving row order — the parallel walk must be
            // byte-for-byte identical, not merely the same set.
            let disp = |r: &QueryResult| -> Vec<Vec<String>> {
                r.rows
                    .iter()
                    .map(|row| row.iter().map(|c| c.to_display()).collect())
                    .collect()
            };
            assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
            assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
        }
        // A tight intermediate budget must trip at the same point under both engines:
        // the 2-hop chain emits 1 row (Alice→Bob→Carol), so a budget of 1 fits and 0
        // (with the count terminal) is irrelevant — use a chain that overflows a small
        // budget identically. Alice→Bob→Carol is the lone 2-hop KNOWS path; a budget
        // that the cross-pattern terminal also charges trips both engines alike.
        let q = "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a.name, c.name";
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(&gen, &cache).with_max_intermediate(1).run(&ast);
        let par = Engine::new(&gen, &cache)
            .with_max_intermediate(1)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast);
        match (&seq, &par) {
            (Ok(s), Ok(p)) => assert_eq!(s.rows.len(), p.rows.len(), "budget row count differs"),
            (Err(_), Err(_)) => {} // both trip the budget — consistent
            _ => panic!("budget behaviour differs: seq={seq:?}, par={par:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn aggregation_with_pool_matches_sequential() {
        // The parallel group-by / count(DISTINCT) precompute (Task 12) must produce the
        // same grouped output — same row order, same values — as the sequential per-row
        // eval. The wide fixture has 200 nodes (≥ AGG_PAR_MIN) with `team` ∈ {Red, Blue,
        // null} and unique `name`, so the pooled engine truly fans the property reads out
        // while the grouping/reduction stays single-threaded.
        let (root, graph) = testgen::write_wide("exec_aggregation", 200);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let pool = std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap(),
        );
        let disp = |r: &QueryResult| -> Vec<Vec<String>> {
            r.rows
                .iter()
                .map(|row| row.iter().map(|c| c.to_display()).collect())
                .collect()
        };
        let queries = [
            // Group-by a property + count(*) — the canonical shape.
            "MATCH (n) RETURN n.team AS t, count(*) AS c ORDER BY t",
            // count(DISTINCT n.p) — single row, no grouping item; nulls excluded.
            "MATCH (n) RETURN count(DISTINCT n.team) AS c",
            // Multiple aggregates over a group, incl. order-sensitive collect().
            "MATCH (n) RETURN n.team AS t, count(*) AS c, collect(n.name) AS names ORDER BY t",
            // min/max over a group (uses the cmp_total reduce path).
            "MATCH (n) RETURN n.team AS t, min(n.name) AS lo, max(n.name) AS hi ORDER BY t",
            // No grouping item, single-arg aggregate over the whole table.
            "MATCH (n) RETURN count(n.team) AS c",
            // A constant grouping item alongside the aggregate.
            "MATCH (n) RETURN n.team AS t, count(*) AS c, 1 AS one ORDER BY t",
        ];
        for q in queries {
            let ast = parser::parse(q).unwrap();
            let seq = Engine::new(&gen, &cache)
                .run(&ast)
                .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
            let par = Engine::new(&gen, &cache)
                .with_fanout_pool(Some(pool.clone()))
                .run(&ast)
                .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
            assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
            assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
        }

        // A `$param` grouping key exercises the Param arm of `eval_simple`.
        {
            let q = "MATCH (n) RETURN n.team AS t, count(*) AS c, $k AS k ORDER BY t";
            let ast = parser::parse(q).unwrap();
            let params = HashMap::from([("k".to_string(), Val::Int(7))]);
            let seq = Engine::new(&gen, &cache)
                .with_params(params.clone())
                .run(&ast)
                .unwrap();
            let par = Engine::new(&gen, &cache)
                .with_params(params)
                .with_fanout_pool(Some(pool.clone()))
                .run(&ast)
                .unwrap();
            assert_eq!(disp(&seq), disp(&par), "param rows differ for `{q}`");
        }

        // A tight intermediate budget must trip (or fit) at the same point under both
        // engines — the parallel path charges each new group and each aggregated value
        // in the same order as the sequential merge.
        let q = "MATCH (n) RETURN n.team AS t, count(*) AS c";
        let ast = parser::parse(q).unwrap();
        for budget in [1u64, 2, 3] {
            let seq = Engine::new(&gen, &cache)
                .with_max_intermediate(budget)
                .run(&ast);
            let par = Engine::new(&gen, &cache)
                .with_max_intermediate(budget)
                .with_fanout_pool(Some(pool.clone()))
                .run(&ast);
            match (&seq, &par) {
                (Ok(s), Ok(p)) => assert_eq!(disp(s), disp(p), "budget={budget} rows differ"),
                (Err(_), Err(_)) => {}
                _ => panic!("budget={budget} behaviour differs: seq={seq:?}, par={par:?}"),
            }
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn anchor_filter_with_pool_matches_sequential() {
        // The parallel anchor `node_ok` prefilter (Task 10) must keep exactly the
        // candidates — in the same order — that the sequential inline filter keeps,
        // across the shapes that make `node_ok` actually read a record: a label scan
        // with an inline property, a boolean label expression (full scan), an inline
        // property bound from a parameter, and a tight intermediate budget. The wide
        // fixture has 200 nodes (100 :Person / 100 :Company) so the candidate set
        // clears `SCAN_PAR_MIN` and the pooled engine truly fans the filter out.
        let (root, graph) = testgen::write_wide("exec_anchor_filter", 200);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let pool = std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap(),
        );
        let queries = [
            // Label scan (Person guaranteed) + inline prop → node_ok reads `team`.
            "MATCH (n:Person {team:'Red'}) RETURN n.name AS name ORDER BY name",
            // Boolean label expr → full scan + per-candidate label decode.
            "MATCH (n:Person|Company) RETURN n.name AS name ORDER BY name",
            // Negated label → full scan, keeps only the :Company half.
            "MATCH (n:!Person) RETURN n.name AS name ORDER BY name",
            // Inline prop with no matching value → every candidate rejected.
            "MATCH (n:Person {team:'Green'}) RETURN n.name AS name ORDER BY name",
            // Aggregate over the filtered set (uncapped, the prefilter's home turf).
            "MATCH (n:Person {team:'Blue'}) RETURN count(*) AS c",
        ];
        for q in queries {
            let ast = parser::parse(q).unwrap();
            let seq = Engine::new(&gen, &cache)
                .run(&ast)
                .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
            let par = Engine::new(&gen, &cache)
                .with_fanout_pool(Some(pool.clone()))
                .run(&ast)
                .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
            let disp = |r: &QueryResult| -> Vec<Vec<String>> {
                r.rows
                    .iter()
                    .map(|row| row.iter().map(|c| c.to_display()).collect())
                    .collect()
            };
            assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
            assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
        }
        // A tight intermediate budget must trip (or fit) at the same point under both
        // engines — the prefilter doesn't charge, so the single-threaded merge/terminal
        // still governs the budget identically.
        let q = "MATCH (n:Person|Company) RETURN n.name";
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(&gen, &cache)
            .with_max_intermediate(10)
            .run(&ast);
        let par = Engine::new(&gen, &cache)
            .with_max_intermediate(10)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast);
        match (&seq, &par) {
            (Ok(s), Ok(p)) => assert_eq!(s.rows.len(), p.rows.len(), "budget row count differs"),
            (Err(_), Err(_)) => {}
            _ => panic!("budget behaviour differs: seq={seq:?}, par={par:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn build_view_with_pool_matches_sequential() {
        // The parallel `algo.*` subgraph build (`build_view`, Task 11) must produce the
        // same view — hence identical algorithm output — as the sequential build. The
        // per-node adjacency reads gather on the pool while the pos-mapping/select merge
        // stays single-threaded, so node list + 0-based `out` are byte-for-byte identical.
        // Two fixtures: the small edge-bearing `write_basic` graph pins the merge with
        // real adjacency (below `BUILD_VIEW_PAR_MIN`, so `par_gather` reads sequentially),
        // and the 200-node `write_wide` graph clears the threshold so the pooled engine
        // truly fans the reads out (no edges → exercises the parallel read + empty merge).
        let pool = std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap(),
        );
        let disp = |r: &QueryResult| -> Vec<Vec<String>> {
            r.rows
                .iter()
                .map(|row| row.iter().map(|c| c.to_display()).collect())
                .collect()
        };
        let assert_par_eq = |gen: &Generation, cache: &BlockCache, q: &str| {
            let ast = parser::parse(q).unwrap();
            let seq = Engine::new(gen, cache)
                .run(&ast)
                .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
            let par = Engine::new(gen, cache)
                .with_fanout_pool(Some(pool.clone()))
                .run(&ast)
                .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
            assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
            assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
        };

        // Edge-bearing fixture: every algo proc shape, incl. rel-type and label filters.
        let (root, graph, _) = testgen::write_basic("exec_build_view_pool");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let queries = [
            "CALL algo.WCC() YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
            "CALL algo.WCC({relationshipTypes: ['KNOWS']}) YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
            "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
            "CALL algo.pageRank(NULL, NULL) YIELD node, score \
             RETURN node.name AS name, score ORDER BY name",
            "CALL algo.pageRank('Person', 'KNOWS') YIELD node, score \
             RETURN node.name AS name, score ORDER BY name",
            "CALL algo.betweenness() YIELD node, score RETURN node.name AS name, score ORDER BY name",
            "CALL algo.HarmonicCentrality({nodeLabels: ['Person'], relationshipTypes: ['KNOWS']}) \
             YIELD node, score, reachable RETURN node.name AS name, score, reachable ORDER BY name",
            "CALL algo.labelPropagation({relationshipTypes: ['KNOWS']}) YIELD node, communityId \
             RETURN node.name AS name, communityId ORDER BY name",
        ];
        for q in queries {
            assert_par_eq(&gen, &cache, q);
        }
        let _ = std::fs::remove_dir_all(&root);

        // Wide fixture (200 nodes ≥ BUILD_VIEW_PAR_MIN): the pooled build fans the
        // adjacency reads across rayon; pool and sequential must still match exactly.
        let (wroot, wgraph) = testgen::write_wide("exec_build_view_pool_wide", 200);
        let wgen = Generation::open(&wroot, &wgraph).unwrap();
        let wcache = BlockCache::new(1 << 20);
        assert_par_eq(
            &wgen,
            &wcache,
            "CALL algo.pageRank(NULL, NULL) YIELD node, score \
             RETURN node.name AS name, score ORDER BY name",
        );
        assert_par_eq(
            &wgen,
            &wcache,
            "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
        );
        let _ = std::fs::remove_dir_all(&wroot);
    }

    #[test]
    fn rel_match_buffer_charges_intermediate_budget() {
        // `match_single_pattern` buffers a *materialising* relationship pattern's whole
        // result set before the cross-pattern terminal charges it; without charging the
        // buffer a dense expansion (every `:LINK` edge over a 1M-node graph) OOMs the
        // process. A row-returning query (not count-pushdown — that retains nothing and
        // is bounded by `maxScan`) exercises this retained buffer: the fixture's 3 KNOWS
        // edges trip a retained budget of 2 and pass at 1M.
        let err = run_budgeted(
            "exec_budget_relmatch_tiny",
            2,
            "MATCH (a)-[:KNOWS]->(b) RETURN b.name AS b",
        )
        .expect_err("the relationship-match buffer must charge the retained budget");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "expected the budget error, got: {err:#}"
        );
        let res = run_budgeted(
            "exec_budget_relmatch_ok",
            1_000_000,
            "MATCH (a)-[:KNOWS]->(b) RETURN b.name AS b",
        )
        .expect("a generous budget must not affect the query");
        assert_eq!(res.rows.len(), 3, "3 KNOWS edges materialise 3 rows");
    }

    #[test]
    fn budget_resets_between_runs() {
        let (root, gen, cache, _) = budgeted_engine("exec_budget_reset", 0);
        let engine = Engine::new(&gen, &cache).with_max_intermediate(1_500);
        // Each run charges ~1k; without the per-run reset the second would trip.
        let ast = parser::parse("RETURN size(range(0, 1000))").unwrap();
        engine.run(&ast).expect("first run fits the budget");
        engine
            .run(&ast)
            .expect("the budget must reset between runs");
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── Vector KNN (M5) ──────────────────────────────────────────────────────

    /// The three Person embeddings in the fixture (see `testgen`), by node id.
    const FIXTURE_VECS: [(u64, [f32; 3]); 3] = [
        (0, [0.1, 0.2, 0.3]), // Alice
        (1, [0.2, 0.1, 0.0]), // Bob
        (2, [0.9, 0.8, 0.7]), // Carol
    ];

    /// Brute-force reference: cosine-distance to `query`, ascending, tie-break id.
    fn reference_knn(query: &[f32], k: usize) -> Vec<(u64, f64)> {
        let mut r: Vec<(u64, f64)> = FIXTURE_VECS
            .iter()
            .map(|(id, v)| (*id, 1.0 - vector::cosine_similarity(query, v)))
            .collect();
        r.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        r.truncate(k);
        r
    }

    #[test]
    fn vector_knn_returns_k_nearest_ordered_with_reference_scores() {
        // Query equals Alice's vector, so Alice (distance 0) is first, then Carol,
        // then Bob — exactly the brute-force reference order and scores.
        let (root, res) = run(
            "exec_knn_ref",
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node) AS id, score",
        );
        assert_eq!(res.columns, vec!["id", "score"]);
        let want = reference_knn(&[0.1, 0.2, 0.3], 2);
        assert_eq!(res.rows.len(), want.len());
        for (got, (wid, wscore)) in res.rows.iter().zip(&want) {
            let Val::Int(id) = got[0] else {
                panic!("id should be an integer, got {:?}", got[0]);
            };
            let Val::Float(score) = got[1] else {
                panic!("score should be a float, got {:?}", got[1]);
            };
            assert_eq!(id as u64, *wid);
            assert!(
                (score - wscore).abs() < 1e-6,
                "score {score} vs reference {wscore}"
            );
        }
        // First hit is the exact match: distance ~0.
        assert!(matches!(res.rows[0][0], Val::Int(0)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn vector_knn_with_pool_is_correct() {
        // A pool-configured engine returns the identical (id, score) kNN rows as the
        // sequential engine. The fixture group is tiny (below KNN_PAR_MIN), so this
        // pins the pool wiring + sequential-fallback path end to end; the `vector`
        // unit test exercises the rayon chunked read/score branch directly.
        let (root, graph, _) = testgen::write_basic("exec_knn_pool");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let pool = std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let q = "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.1, 0.2, 0.3])) \
                 YIELD node, score RETURN id(node) AS id, score";
        let res = Engine::new(&gen, &cache)
            .with_fanout_pool(Some(pool))
            .run(&parser::parse(q).unwrap())
            .expect("pool-configured kNN runs");
        let want = reference_knn(&[0.1, 0.2, 0.3], 3);
        assert_eq!(res.rows.len(), want.len());
        for (got, (wid, wscore)) in res.rows.iter().zip(&want) {
            let Val::Int(id) = got[0] else {
                panic!("id should be an integer, got {:?}", got[0]);
            };
            let Val::Float(score) = got[1] else {
                panic!("score should be a float, got {:?}", got[1]);
            };
            assert_eq!(id as u64, *wid);
            assert!(
                (score - wscore).abs() < 1e-6,
                "score {score} vs reference {wscore}"
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn vector_knn_yield_alias_and_node_projection() {
        // Carol's own vector → Carol is the single nearest neighbour; the yielded
        // node is a real Node we can project a property off.
        let (root, res) = run(
            "exec_knn_alias",
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 1, vecf32([0.9, 0.8, 0.7])) \
             YIELD node AS n, score AS s RETURN n.name AS name, s",
        );
        assert_eq!(res.columns, vec!["name", "s"]);
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0].to_display(), "Carol");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn vector_knn_yield_where_filters_rows() {
        // Ask for all three but keep only the (near-)exact match via YIELD ... WHERE.
        let (root, res) = run(
            "exec_knn_where",
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score WHERE score < 0.0001 RETURN id(node) AS id",
        );
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(0)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn vector_knn_unknown_index_is_an_error() {
        let (root, graph, _) = testgen::write_basic("exec_knn_noindex");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Company', 'embedding', 1, vecf32([0.1, 0.2, 0.3])) \
             YIELD node RETURN node",
        )
        .unwrap();
        let err = engine.run(&ast).err().unwrap();
        assert!(err.to_string().contains("no vector index"), "got: {err}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn vector_knn_dimension_mismatch_is_an_error() {
        let (root, graph, _) = testgen::write_basic("exec_knn_dim");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        // A 2-dim query against the 3-dim index.
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 1, vecf32([0.1, 0.2])) \
             YIELD node RETURN node",
        )
        .unwrap();
        let err = engine.run(&ast).err().unwrap();
        assert!(err.to_string().contains("dimension"), "got: {err}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn vector_knn_query_vector_from_parameter() {
        let (root, graph, _) = testgen::write_basic("exec_knn_param");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let mut params = HashMap::new();
        // A $param query vector arrives as a list of numbers.
        params.insert(
            "q".to_string(),
            Val::List(vec![Val::Float(0.9), Val::Float(0.8), Val::Float(0.7)]),
        );
        params.insert("k".to_string(), Val::Int(1));
        let engine = Engine::new(&gen, &cache).with_params(params);
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Person', 'embedding', $k, $q) \
             YIELD node, score RETURN id(node) AS id",
        )
        .unwrap();
        let res = engine.run(&ast).unwrap();
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(2)), "Carol is nearest");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn vector_knn_reads_route_through_the_block_cache() {
        let (root, graph, _) = testgen::write_basic("exec_knn_cache");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node)",
        )
        .unwrap();
        engine.run(&ast).unwrap();
        let after_first = cache.metrics();
        assert!(after_first.misses > 0, "first run populates the cache");
        engine.run(&ast).unwrap();
        let after_second = cache.metrics();
        assert_eq!(
            after_second.misses, after_first.misses,
            "the vector group should be served from resident blocks on the second run"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn similarity_and_vecf32_scalar_functions() {
        let (root, res) = run(
            "exec_similarity",
            "RETURN similarity(vecf32([1.0, 0.0]), vecf32([1.0, 0.0])) AS same, \
             similarity(vecf32([1.0, 0.0]), vecf32([0.0, 1.0])) AS orth",
        );
        let Val::Float(same) = res.rows[0][0] else {
            panic!("expected float");
        };
        let Val::Float(orth) = res.rows[0][1] else {
            panic!("expected float");
        };
        assert!((same - 1.0).abs() < 1e-9);
        assert!(orth.abs() < 1e-9);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase8_vector_distance_functions() {
        // Vectors ported from FalkorDB tests/flow/test_vecsim.py::test01_vector_distance.
        // euclidean([1,2],[2,3]) = sqrt(2); cosine = 1 - 8/sqrt(65).
        let (root, res) = run(
            "exec_p8_dist",
            "RETURN vec.euclideanDistance(vecf32([1.0, 2.0]), vecf32([2.0, 3.0])) AS e, \
             vec.cosineDistance(vecf32([1.0, 2.0]), vecf32([2.0, 3.0])) AS c, \
             vec.euclideanDistance(vecf32([1.0, 1.0]), vecf32([1.0, 1.0])) AS esame, \
             vec.cosineDistance(vecf32([1.0, 1.0]), vecf32([1.0, 1.0])) AS csame",
        );
        assert_float(&res.rows[0][0], 2.0_f64.sqrt());
        assert_float(&res.rows[0][1], 1.0 - 8.0 / 65.0_f64.sqrt());
        assert_float(&res.rows[0][2], 0.0);
        assert_float(&res.rows[0][3], 0.0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase8_vector_distance_null_propagates() {
        // A NULL operand → NULL (either side), for both functions.
        let (root, res) = run(
            "exec_p8_null",
            "RETURN vec.euclideanDistance(null, vecf32([1.0, 1.0])) AS a, \
             vec.euclideanDistance(vecf32([1.0, 1.0]), null) AS b, \
             vec.cosineDistance(null, null) AS c",
        );
        assert!(matches!(res.rows[0][0], Val::Null));
        assert!(matches!(res.rows[0][1], Val::Null));
        assert!(matches!(res.rows[0][2], Val::Null));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase8_vector_distance_errors() {
        // Dimension mismatch is an error (FalkorDB: "Vector dimension mismatch").
        let e = run_err(
            "exec_p8_dim",
            "RETURN vec.euclideanDistance(vecf32([1.0, 1.0]), vecf32([2.0, 2.0, 3.0])) AS d",
        );
        assert!(e.contains("dimension mismatch"), "got: {e}");
        // A non-vector operand is an error (FalkorDB: "Type mismatch"). Pass a
        // string directly (vecf32() would reject it first; the distance arm coerces
        // via as_vector and rejects a non-numeric scalar).
        let e = run_err(
            "exec_p8_type",
            "RETURN vec.cosineDistance([1.0, 1.0], 'foo') AS d",
        );
        assert!(e.contains("vectors"), "got: {e}");
    }

    // ── Phase 9 — Val::Point, point()/distance(), coordinate reads ────────────

    // point() construction + coordinate property reads (test_point.py
    // test_point_coordinates). FalkorDB stores f32; coordinates are asserted to
    // 1e-5. An unknown coordinate key yields NULL.
    #[test]
    fn phase9_point_construction_and_coordinates() {
        let (root, res) = run(
            "exec_p9_coords",
            "WITH point({latitude: 32.070794860, longitude: 34.820751118}) AS p \
             RETURN p.latitude AS lat, p.longitude AS lon, p.v AS missing, typeOf(p) AS t",
        );
        let r = &res.rows[0];
        match r[0] {
            Val::Float(x) => assert!((x - 32.070794860).abs() < 1e-5, "lat {x}"),
            ref o => panic!("expected float latitude, got {o:?}"),
        }
        match r[1] {
            Val::Float(x) => assert!((x - 34.820751118).abs() < 1e-5, "lon {x}"),
            ref o => panic!("expected float longitude, got {o:?}"),
        }
        assert!(matches!(r[2], Val::Null), "unknown key → NULL");
        assert_eq!(render(&r[3]), "'Point'");
        let _ = std::fs::remove_dir_all(&root);
    }

    // distance() haversine, in metres (test_point.py test_point_distance). The
    // FalkorDB suite tolerates 10% error; we assert the same vectors well within it.
    #[test]
    fn phase9_point_distance() {
        let (root, res) = run(
            "exec_p9_dist",
            "WITH point({latitude:32.070794860, longitude:34.820751118}) AS a, \
                  point({latitude:32.070109656, longitude:34.822351298}) AS b, \
                  point({latitude:30.621734079, longitude:-96.33775507}) AS c \
             RETURN distance(a, a) AS d0, distance(a, b) AS d160, distance(a, c) AS d_far",
        );
        let r = &res.rows[0];
        let f = |v: &Val| match v {
            Val::Float(x) => *x,
            o => panic!("expected float, got {o:?}"),
        };
        assert!(f(&r[0]).abs() < 1e-6, "same point → 0, got {}", f(&r[0]));
        let within = |got: f64, want: f64| {
            assert!((got - want).abs() <= 0.1 * want, "got {got}, want ~{want}")
        };
        within(f(&r[1]), 160.0);
        within(f(&r[2]), 11_352_120.0);
        let _ = std::fs::remove_dir_all(&root);
    }

    // Coordinate range validation + bad-key errors (test_point.py test_point_values).
    #[test]
    fn phase9_point_validation_errors() {
        for (tag, q, needle) in [
            (
                "exec_p9_lat_hi",
                "RETURN point({latitude:90.1, longitude:20}) AS p",
                "latitude should be within",
            ),
            (
                "exec_p9_lat_lo",
                "RETURN point({latitude:-90.1, longitude:20}) AS p",
                "latitude should be within",
            ),
            (
                "exec_p9_lon_hi",
                "RETURN point({latitude:10, longitude:180.1}) AS p",
                "longitude should be within",
            ),
            (
                "exec_p9_lon_lo",
                "RETURN point({latitude:10, longitude:-180.1}) AS p",
                "longitude should be within",
            ),
            (
                "exec_p9_one_key",
                "RETURN point({latitude:10}) AS p",
                "should have 2 elements",
            ),
            (
                "exec_p9_no_lat",
                "RETURN point({x:1, y:2}) AS p",
                "Did not find 'latitude'",
            ),
        ] {
            let e = run_err(tag, q);
            assert!(e.contains(needle), "query `{q}` → `{e}` (want `{needle}`)");
        }
    }

    // Ordering + equality. FalkorDB orders points by longitude then latitude
    // (test_point.py test_nested_point ORDER BY p), and equal points are `=`.
    #[test]
    fn phase9_point_ordering_and_equality() {
        let (root, res) = run(
            "exec_p9_order",
            "UNWIND [point({latitude:33, longitude:35}), \
                     point({latitude:32, longitude:31}), \
                     point({latitude:32, longitude:32}), \
                     point({latitude:31, longitude:32}), \
                     point({latitude:29, longitude:36})] AS p \
             WITH p ORDER BY p RETURN p.longitude AS lon, p.latitude AS lat",
        );
        let lons: Vec<f64> = res
            .rows
            .iter()
            .map(|r| match r[0] {
                Val::Float(x) => x,
                ref o => panic!("{o:?}"),
            })
            .collect();
        assert_eq!(lons, vec![31.0, 32.0, 32.0, 35.0, 36.0]);
        // The lon-32 tie breaks on latitude ascending (31 before 32).
        assert!(matches!(res.rows[1][1], Val::Float(x) if (x - 31.0).abs() < 1e-9));
        assert!(matches!(res.rows[2][1], Val::Float(x) if (x - 32.0).abs() < 1e-9));

        let (root2, eq) = run(
            "exec_p9_eq",
            "WITH point({latitude:32, longitude:34}) AS a, \
                  point({latitude:32, longitude:34}) AS b, \
                  point({latitude:32, longitude:35}) AS c \
             RETURN a = b AS same, a = c AS diff",
        );
        assert!(matches!(eq.rows[0][0], Val::Bool(true)));
        assert!(matches!(eq.rows[0][1], Val::Bool(false)));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&root2);
    }

    // NULL propagation + toString rendering (%f, 6 decimals — test_nested_point).
    #[test]
    fn phase9_point_null_and_tostring() {
        let (root, res) = run(
            "exec_p9_null_str",
            "RETURN point(null) AS np, distance(null, point({latitude:1, longitude:2})) AS nd, \
             toString(point({latitude:32, longitude:34})) AS s",
        );
        let r = &res.rows[0];
        assert!(matches!(r[0], Val::Null));
        assert!(matches!(r[1], Val::Null));
        assert_eq!(
            render(&r[2]),
            "'point({latitude: 32.000000, longitude: 34.000000})'"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase1_trig_and_angle_functions() {
        let (root, res) = run(
            "exec_p1_trig",
            "RETURN sin(0.0) AS s, cos(0.0) AS c, tan(0.0) AS t, \
             cot(0.7853981633974483) AS cot, asin(1.0) AS asin, acos(1.0) AS acos, \
             atan(1.0) AS atan, atan2(1.0, 1.0) AS atan2, \
             degrees(3.141592653589793) AS deg, radians(180.0) AS rad, \
             haversin(0.0) AS hav",
        );
        let f = |i: usize| match res.rows[0][i] {
            Val::Float(x) => x,
            _ => panic!("expected float at col {i}"),
        };
        let close = |a: f64, b: f64| assert!((a - b).abs() < 1e-9, "{a} != {b}");
        close(f(0), 0.0); // sin 0
        close(f(1), 1.0); // cos 0
        close(f(2), 0.0); // tan 0
        close(f(3), 1.0); // cot(pi/4)
        close(f(4), std::f64::consts::FRAC_PI_2); // asin 1
        close(f(5), 0.0); // acos 1
        close(f(6), std::f64::consts::FRAC_PI_4); // atan 1
        close(f(7), std::f64::consts::FRAC_PI_4); // atan2(1,1)
        close(f(8), 180.0); // degrees(pi)
        close(f(9), std::f64::consts::PI); // radians(180)
        close(f(10), 0.0); // haversin 0
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase1_left_right_and_isempty_typeof() {
        let (root, res) = run(
            "exec_p1_str",
            "RETURN left('muchacho', 4) AS l, right('muchacho', 4) AS r, \
             left('hi', 9) AS lover, right('hi', 9) AS rover, \
             isEmpty('') AS e1, isEmpty('x') AS e2, isEmpty([]) AS e3, \
             typeOf(1) AS t1, typeOf(1.5) AS t2, typeOf('a') AS t3, \
             typeOf(true) AS t4, typeOf([1]) AS t5, typeOf(null) AS t6",
        );
        let row = &res.rows[0];
        assert!(matches!(&row[0], Val::Str(s) if s == "much"));
        assert!(matches!(&row[1], Val::Str(s) if s == "acho"));
        assert!(matches!(&row[2], Val::Str(s) if s == "hi"));
        assert!(matches!(&row[3], Val::Str(s) if s == "hi"));
        assert!(matches!(row[4], Val::Bool(true)));
        assert!(matches!(row[5], Val::Bool(false)));
        assert!(matches!(row[6], Val::Bool(true)));
        assert!(matches!(&row[7], Val::Str(s) if s == "Integer"));
        assert!(matches!(&row[8], Val::Str(s) if s == "Float"));
        assert!(matches!(&row[9], Val::Str(s) if s == "String"));
        assert!(matches!(&row[10], Val::Str(s) if s == "Boolean"));
        assert!(matches!(&row[11], Val::Str(s) if s == "List"));
        assert!(matches!(&row[12], Val::Str(s) if s == "Null"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase1_ornull_conversions() {
        let (root, res) = run(
            "exec_p1_ornull",
            "RETURN toIntegerOrNull('7') AS i, toIntegerOrNull('x') AS i2, \
             toFloatOrNull('1.5') AS f, toFloatOrNull('x') AS f2, \
             toBooleanOrNull('true') AS b, toBooleanOrNull('x') AS b2, \
             toStringOrNull(42) AS s, toStringOrNull(null) AS s2",
        );
        let row = &res.rows[0];
        assert!(matches!(row[0], Val::Int(7)));
        assert!(matches!(row[1], Val::Null));
        assert!(matches!(row[2], Val::Float(x) if (x - 1.5).abs() < 1e-9));
        assert!(matches!(row[3], Val::Null));
        assert!(matches!(row[4], Val::Bool(true)));
        assert!(matches!(row[5], Val::Null));
        assert!(matches!(&row[6], Val::Str(s) if s == "42"));
        assert!(matches!(row[7], Val::Null));
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Canonical render of a value for order-sensitive list assertions (the
    /// `Val` enum derives no `PartialEq`). Mirrors Cypher literal syntax closely
    /// enough to read the expectations off the FalkorDB test vectors.
    #[cfg(test)]
    fn render(v: &Val) -> String {
        match v {
            Val::Null => "null".into(),
            Val::Bool(b) => b.to_string(),
            Val::Int(i) => i.to_string(),
            Val::Float(f) => f.to_string(),
            Val::Str(s) => format!("'{s}'"),
            Val::List(xs) => {
                let inner: Vec<String> = xs.iter().map(render).collect();
                format!("[{}]", inner.join(","))
            }
            other => format!("{other:?}"),
        }
    }

    // Phase 2 — list functions tail / list.* and the to*List family.
    #[test]
    fn phase2_tail_dedup_sort() {
        let (root, res) = run(
            "exec_p2_list_a",
            "RETURN tail([1,2,3]) AS t, tail([7]) AS t1, tail([]) AS te, \
             list.dedup([1,2,1,3,3,2]) AS d, list.dedup([3,[1,2],3,[1],[1,2]]) AS dn, \
             list.sort([3,1,2]) AS s, list.sort([1,3,2], false) AS sd, \
             list.sort([[4,5,6],[1,2,3]]) AS sl",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "[2,3]");
        assert_eq!(render(&r[1]), "[]");
        assert_eq!(render(&r[2]), "[]");
        assert_eq!(render(&r[3]), "[1,2,3]");
        assert_eq!(render(&r[4]), "[3,[1,2],[1]]");
        assert_eq!(render(&r[5]), "[1,2,3]");
        assert_eq!(render(&r[6]), "[3,2,1]");
        assert_eq!(render(&r[7]), "[[1,2,3],[4,5,6]]");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase2_list_remove() {
        // Vectors ported from FalkorDB tests/flow/test_list.py test09_remove.
        let (root, res) = run(
            "exec_p2_remove",
            "RETURN list.remove([1,2,3], 1, 2) AS a, list.remove([1,2,3,4], 1, 2) AS b, \
             list.remove([1,2,3], 2) AS c, list.remove([1,2,3,4], -1, 1) AS d, \
             list.remove([1,2,3,4], -4, 1) AS e, list.remove([1,2,3,4], -3, 5) AS f, \
             list.remove([1,2,3,4], -5, 5) AS g, list.remove([1,2,3,4], 4, 5) AS h, \
             list.remove([1,2,3], 1, 0) AS i, list.remove(null, 2) AS j",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "[1]");
        assert_eq!(render(&r[1]), "[1,4]");
        assert_eq!(render(&r[2]), "[1,2]");
        assert_eq!(render(&r[3]), "[1,2,3]");
        assert_eq!(render(&r[4]), "[2,3,4]");
        assert_eq!(render(&r[5]), "[1]");
        assert_eq!(render(&r[6]), "[1,2,3,4]"); // out-of-bound index → unchanged
        assert_eq!(render(&r[7]), "[1,2,3,4]");
        assert_eq!(render(&r[8]), "[1,2,3]"); // count 0 → unchanged
        assert_eq!(render(&r[9]), "null");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase2_list_insert_and_insert_elements() {
        // Vectors ported from FalkorDB test_list.py test11_insert / test12.
        let (root, res) = run(
            "exec_p2_insert",
            "RETURN list.insert([1,2,3], 0, 4) AS a, list.insert([1,2,3], 3, 4) AS b, \
             list.insert([1,2,3], -1, 4) AS c, list.insert([1,2,3], -3, 4) AS d, \
             list.insert([], 0, 4) AS e, list.insert(null, 2, 3) AS f, \
             list.insert([1,2,3], 0, 2, false) AS g, \
             list.insertListElements([1,2,3], [4,5,6], 0) AS h, \
             list.insertListElements([1,2,3], [4], -1) AS i, \
             list.insertListElements([1,2,3], [9,3,2,7], 0, false) AS j, \
             list.insertListElements([1,2,3], null, 1) AS k",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "[4,1,2,3]");
        assert_eq!(render(&r[1]), "[1,2,3,4]");
        assert_eq!(render(&r[2]), "[1,2,3,4]");
        assert_eq!(render(&r[3]), "[1,4,2,3]");
        assert_eq!(render(&r[4]), "[4]");
        assert_eq!(render(&r[5]), "null");
        assert_eq!(render(&r[6]), "[1,2,3]"); // dups=false + 2 already present → unchanged
        assert_eq!(render(&r[7]), "[4,5,6,1,2,3]");
        assert_eq!(render(&r[8]), "[1,2,3,4]"); // idx -1 with inclusive bounds → append
        assert_eq!(render(&r[9]), "[9,7,1,2,3]"); // dups dropped vs list1
        assert_eq!(render(&r[10]), "[1,2,3]"); // null list2 → unchanged
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase2_to_type_lists() {
        // Vectors ported from FalkorDB test_list.py test06–09.
        let (root, res) = run(
            "exec_p2_tolists",
            "RETURN toBooleanList(null) AS a, toBooleanList([null, null]) AS b, \
             toBooleanList(['abc', true, 'false', null, ['a','b']]) AS c, \
             toFloatList(['abc', 1.5, 7.0578, null, ['a','b']]) AS d, \
             toIntegerList(['abc', 7, '5', null, ['a','b']]) AS e, \
             toStringList([1, 2.5, 'x', null]) AS f",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "null");
        assert_eq!(render(&r[1]), "[null,null]");
        assert_eq!(render(&r[2]), "[null,true,false,null,null]");
        assert_eq!(render(&r[3]), "[null,1.5,7.0578,null,null]");
        assert_eq!(render(&r[4]), "[null,7,5,null,null]");
        assert_eq!(render(&r[5]), "['1','2.5','x',null]");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase2_entity_haslabels_and_degree() {
        // Fixture: Alice -KNOWS-> Bob, -WORKS_AT-> Acme, -KNOWS-> Carol;
        //          Bob -KNOWS-> Carol; Carol -WORKS_AT-> Globex.
        let (root, res) = run(
            "exec_p2_entity",
            "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}), \
                   (k:Company {name: 'Acme'}) \
             RETURN hasLabels(a, ['Person']) AS h1, hasLabels(a, ['Company']) AS h2, \
                    hasLabels(a, ['Person','Foo']) AS h3, hasLabels(k, ['Company']) AS h4, \
                    outdegree(a) AS od, outdegree(a, 'KNOWS') AS odk, \
                    outdegree(a, 'WORKS_AT') AS odw, outdegree(a, ['KNOWS','WORKS_AT']) AS oda, \
                    indegree(a) AS ai, indegree(c) AS ci, indegree(c, 'KNOWS') AS cik, \
                    indegree(c, 'WORKS_AT') AS ciw",
        );
        let r = &res.rows[0];
        assert!(matches!(r[0], Val::Bool(true)));
        assert!(matches!(r[1], Val::Bool(false)));
        assert!(matches!(r[2], Val::Bool(false)));
        assert!(matches!(r[3], Val::Bool(true)));
        assert!(matches!(r[4], Val::Int(3)));
        assert!(matches!(r[5], Val::Int(2)));
        assert!(matches!(r[6], Val::Int(1)));
        assert!(matches!(r[7], Val::Int(3)));
        assert!(matches!(r[8], Val::Int(0)));
        assert!(matches!(r[9], Val::Int(2)));
        assert!(matches!(r[10], Val::Int(2)));
        assert!(matches!(r[11], Val::Int(0)));
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── Phase 3: statistical aggregations ────────────────────────────────────

    /// A `Val::Float` close to `want` (FalkorDB returns doubles for these aggs).
    fn assert_float(v: &Val, want: f64) {
        match v {
            Val::Float(x) => assert!((x - want).abs() < 1e-9, "expected ~{want}, got {x}"),
            other => panic!("expected Float({want}), got {other:?}"),
        }
    }

    #[test]
    fn phase3_stdev_sample_and_population() {
        // Vectors ported from FalkorDB tests/flow/test_aggregation.py::test06_StDev.
        // Edge case: a single value has zero sample deviation.
        let (root, res) = run("exec_p3_stdev1", "RETURN stDev(5.1) AS s");
        assert_float(&res.rows[0][0], 0.0);
        let _ = std::fs::remove_dir_all(&root);

        // 1..10: sample variance = 82.5/9, population variance = 82.5/10.
        let (root, res) = run(
            "exec_p3_stdev2",
            "UNWIND [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] AS x \
             RETURN stDev(x) AS s, stDevP(x) AS sp",
        );
        assert_float(&res.rows[0][0], (82.5_f64 / 9.0).sqrt());
        assert_float(&res.rows[0][1], (82.5_f64 / 10.0).sqrt());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase3_percentile_cont() {
        // FalkorDB test04_percentileCont: linear interpolation over [2,4,6,8,10].
        let cases = [
            (0.0, 2.0),
            (0.1, 2.8),
            (0.33, 4.64),
            (0.5, 6.0),
            (1.0, 10.0),
        ];
        for (i, (p, want)) in cases.iter().enumerate() {
            let (root, res) = run(
                &format!("exec_p3_pcont_{i}"),
                &format!("UNWIND [2, 4, 6, 8, 10] AS x RETURN percentileCont(x, {p}) AS r"),
            );
            assert_float(&res.rows[0][0], *want);
            let _ = std::fs::remove_dir_all(&root);
        }
    }

    #[test]
    fn phase3_percentile_disc() {
        // FalkorDB test05_percentileDisc: nearest-rank over [2,4,6,8,10].
        let cases = [(0.0, 2.0), (0.1, 2.0), (0.33, 4.0), (0.5, 6.0), (1.0, 10.0)];
        for (i, (p, want)) in cases.iter().enumerate() {
            let (root, res) = run(
                &format!("exec_p3_pdisc_{i}"),
                &format!("UNWIND [2, 4, 6, 8, 10] AS x RETURN percentileDisc(x, {p}) AS r"),
            );
            assert_float(&res.rows[0][0], *want);
            let _ = std::fs::remove_dir_all(&root);
        }
        // p == 0 takes index 0 of the sorted values, regardless of input order.
        let (root, res) = run(
            "exec_p3_pdisc_zero",
            "UNWIND [0.5, 0, 1] AS x RETURN percentileDisc(x, 0) AS r",
        );
        assert_float(&res.rows[0][0], 0.0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase3_empty_aggregation_defaults() {
        // FalkorDB test01_empty_aggregation: with no rows and no grouping key, the
        // statistical aggregates still emit one row — stDev/stDevP→0, percentiles→null.
        let (root, res) = run(
            "exec_p3_empty",
            "MATCH (n) WHERE n.name = 'noneExisting' \
             RETURN stDev(n.v) AS a, stDevP(n.v) AS b, \
                    percentileDisc(n.v, 0.5) AS c, percentileCont(n.v, 0.5) AS d",
        );
        assert_eq!(res.rows.len(), 1);
        let r = &res.rows[0];
        assert_float(&r[0], 0.0);
        assert_float(&r[1], 0.0);
        assert!(matches!(r[2], Val::Null));
        assert!(matches!(r[3], Val::Null));
        let _ = std::fs::remove_dir_all(&root);
    }

    // log/log10/exp/e/pi/pow — the camelid §1 gap (TF-IDF scoring needs `log`).
    #[test]
    fn numeric_log_family_functions() {
        let (root, res) = run(
            "exec_logfns",
            "RETURN log(2.718281828459045) AS ln, log10(1000.0) AS l10, \
             exp(0.0) AS ex, e() AS e, pi() AS pi, pow(2.0, 10.0) AS p",
        );
        let f = |v: &Val| match v {
            Val::Float(x) => *x,
            other => panic!("expected float, got {other:?}"),
        };
        let r = &res.rows[0];
        assert!((f(&r[0]) - 1.0).abs() < 1e-12);
        assert!((f(&r[1]) - 3.0).abs() < 1e-12);
        assert!((f(&r[2]) - 1.0).abs() < 1e-12);
        assert!((f(&r[3]) - std::f64::consts::E).abs() < 1e-12);
        assert!((f(&r[4]) - std::f64::consts::PI).abs() < 1e-12);
        assert!((f(&r[5]) - 1024.0).abs() < 1e-9);
        let _ = std::fs::remove_dir_all(&root);
    }

    // FalkorDB parity: a non-positive argument to log yields the IEEE result
    // (-inf / NaN), not an error; NULL propagates as NULL.
    #[test]
    fn log_domain_and_null_match_falkordb() {
        let (root, res) = run(
            "exec_log_domain",
            "RETURN log(0.0) AS zero, log(-1.0) AS neg, log(null) AS nul",
        );
        let r = &res.rows[0];
        assert!(matches!(r[0], Val::Float(x) if x == f64::NEG_INFINITY));
        assert!(matches!(r[1], Val::Float(x) if x.is_nan()));
        assert!(matches!(r[2], Val::Null));
        let _ = std::fs::remove_dir_all(&root);
    }

    // eu-ai-act §P1: a relationship whose target node is already bound from a prior
    // MATCH must lead with that bound node (reverse adjacency), not full-scan the
    // start label once per bound row. We assert correctness here; the reroot in
    // `maybe_reroot` removes the O(|start-label|)-per-row blow-up.
    #[test]
    fn reverse_traversal_to_bound_node() {
        // Bob is reached by Alice and Carol via KNOWS. Bind Bob first, then match
        // the incoming KNOWS with the *source* unbound — the planner should reroot
        // to lead with Bob and walk reverse adjacency.
        let (root, res) = run(
            "exec_bound_end_reroot",
            "MATCH (b:Person {name:'Bob'}) \
             MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS nm ORDER BY nm",
        );
        let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
        assert_eq!(names, vec!["Alice"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The headline M7 test: a synthetic index far above the ANN threshold **and**
    /// far larger than the vector cache budget. The Vamana/PQ arm recovers most of
    /// the brute-force top-k while the vector-index pool stays bounded (resident PQ
    /// codes + only a handful of paged-in Vamana blocks — never the whole store).
    #[test]
    fn vamana_knn_matches_brute_force_with_bounded_vector_cache() {
        let fix = testgen::VamanaFixture {
            n: 2000,
            dim: 32,
            r: 24,
            alpha: 1.2,
            pq_subspaces: 8,
            pq_bits: 8,
            vector_block_size: 8192,
        };
        let (root, graph, raw) = testgen::write_vamana("exec_vamana_recall", &fix);
        let gen = Generation::open(&root, &graph).unwrap();
        let block_cache = BlockCache::new(1 << 20);

        // Budget = resident PQ codes + room for only ~8 of the 8 KiB Vamana blocks,
        // far below the full store, so the pool must page during the walk.
        let (ord, pq_bytes, blocks_total) = {
            let vi = gen.vamana_index("Doc", "embedding").unwrap();
            (
                vi.ord,
                vi.pq.resident_bytes(),
                vi.reader.inner().num_blocks(),
            )
        };
        let budget = pq_bytes + 64 * 1024;
        let vec_cache = VectorIndexCache::new(budget);
        vec_cache.pin(
            gen.uuid(),
            ord,
            gen.vamana_index("Doc", "embedding").unwrap().pq.clone(),
        );

        let k = 10;
        let queries = 20;
        let mut recall_sum = 0.0f64;
        for qi in 0..queries {
            // A query near a stored vector, lightly perturbed.
            let mut q = raw[(qi * 97) % fix.n].clone();
            q[0] += 0.05;

            // Brute-force ground truth (cosine over the raw vectors).
            let mut truth: Vec<(f64, u64)> = raw
                .iter()
                .enumerate()
                .map(|(i, v)| (1.0 - vector::cosine_similarity(&q, v), i as u64))
                .collect();
            truth.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let truth_k: std::collections::HashSet<u64> =
                truth.iter().take(k).map(|(_, id)| *id).collect();

            let mut params = HashMap::new();
            params.insert(
                "q".to_string(),
                Val::List(q.iter().map(|x| Val::Float(*x as f64)).collect()),
            );
            let engine = Engine::new(&gen, &block_cache)
                .with_vector_cache(&vec_cache, 96)
                .with_params(params);
            let ast = parser::parse(
                "CALL db.idx.vector.queryNodes('Doc', 'embedding', 10, $q) \
                 YIELD node, score RETURN id(node) AS id, score",
            )
            .unwrap();
            let res = engine.run(&ast).unwrap();
            assert!(res.rows.len() <= k);
            // Scores are ascending cosine distances (the brute-force contract).
            let mut prev = f64::NEG_INFINITY;
            let got: std::collections::HashSet<u64> = res
                .rows
                .iter()
                .map(|r| {
                    if let Val::Float(s) = r[1] {
                        assert!(s + 1e-6 >= prev, "scores must be ascending");
                        prev = s;
                    }
                    match r[0] {
                        Val::Int(n) => n as u64,
                        _ => panic!("id(node) should be an integer"),
                    }
                })
                .collect();
            let found = truth_k.iter().filter(|id| got.contains(id)).count();
            recall_sum += found as f64 / k as f64;
        }
        let recall = recall_sum / queries as f64;
        assert!(
            recall >= 0.8,
            "Vamana recall@{k} was {recall:.3}, expected ≥ 0.8"
        );

        // Bounded memory: the pool never grew past its budget (+ at most one
        // oversized block), and held only a fraction of the store's blocks.
        assert!(
            vec_cache.bytes() <= budget + fix.vector_block_size,
            "vector pool {} exceeded budget {}",
            vec_cache.bytes(),
            budget
        );
        assert!(
            vec_cache.block_count() < blocks_total,
            "paged in {} of {} blocks — the whole store should never be resident",
            vec_cache.block_count(),
            blocks_total
        );
        assert!(
            blocks_total > 16,
            "test needs the store to span many blocks"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    // ── id() seek pushdown — end-to-end correctness ────────────────────────────
    // Fixture ids: [0]Alice [1]Bob [2]Carol (Person), [3]Acme [4]Globex (Company).
    // Edges: Alice-KNOWS->Bob, Bob-KNOWS->Carol, Alice-WORKS_AT->Acme,
    //        Carol-WORKS_AT->Globex, Alice-KNOWS->Carol.

    #[test]
    fn id_seek_returns_the_one_node() {
        let (root, res) = run(
            "exec_id_seek",
            "MATCH (n) WHERE id(n) = 1 RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Bob"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_seek_drives_expansion_without_full_scan() {
        // Lab's neighbourhood-expansion shape. Anchor `n` is seeked to Alice(0),
        // then expanded — the result is exactly Alice's out-neighbours.
        let (root, res) = run(
            "exec_id_seek_expand",
            "MATCH (n)-[r]->(m) WHERE id(n) = 0 RETURN m.name AS name",
        );
        assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_seek_still_enforces_label() {
        // Node 0 is Alice (Person), not a Company → the residual label check on the
        // seeked candidate yields nothing.
        let (root, res) = run(
            "exec_id_seek_label",
            "MATCH (n:Company) WHERE id(n) = 0 RETURN n.name AS name",
        );
        assert!(res.rows.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_seek_still_enforces_extra_predicate() {
        // id(n)=0 seeks Alice, but the AND-ed name predicate is for Bob → empty.
        let (root, res) = run(
            "exec_id_seek_pred_no",
            "MATCH (n) WHERE id(n) = 0 AND n.name = 'Bob' RETURN n.name AS name",
        );
        assert!(res.rows.is_empty());
        // The matching companion: same id, the right name → one row.
        let (root2, res2) = run(
            "exec_id_seek_pred_yes",
            "MATCH (n) WHERE id(n) = 0 AND n.name = 'Alice' RETURN n.name AS name",
        );
        assert_eq!(col0(&res2), vec!["Alice"]);
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&root2);
    }

    #[test]
    fn id_under_or_returns_all_disjuncts() {
        // THE wrong-results guard: if the seek wrongly fired on the OR it would
        // return only one node. Both must come back.
        let (root, res) = run(
            "exec_id_or",
            "MATCH (n) WHERE id(n) = 0 OR id(n) = 2 RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_in_list_returns_each() {
        let (root, res) = run(
            "exec_id_in",
            "MATCH (n) WHERE id(n) IN [0, 2, 99] RETURN n.name AS name",
        );
        // 99 is out of range and contributes nothing.
        assert_eq!(col0(&res), vec!["Alice", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_out_of_range_returns_empty() {
        let (root, res) = run(
            "exec_id_oor",
            "MATCH (n) WHERE id(n) = 999 RETURN n.name AS name",
        );
        assert!(res.rows.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_negative_returns_empty() {
        let (root, res) = run(
            "exec_id_neg",
            "MATCH (n) WHERE id(n) = -5 RETURN n.name AS name",
        );
        assert!(res.rows.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_seek_with_disjunction_companion_predicate() {
        // `id(n) = 0 AND (name='Alice' OR name='Zzz')`: the seek narrows to Alice,
        // the parenthesised OR is re-checked as a residual → Alice stays.
        let (root, res) = run(
            "exec_id_and_or",
            "MATCH (n) WHERE id(n) = 0 AND (n.name = 'Alice' OR n.name = 'Zzz') RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── id() seek with anchor re-rooting (id on the far end of the traversal) ───

    #[test]
    fn id_on_end_reroots_outgoing_expansion() {
        // `(m)-[r]->(n) WHERE id(n)=1`: id is on the END node n (Bob). Re-rooting
        // seeks Bob and walks the edge backwards → m is whoever points to Bob: Alice.
        let (root, res) = run(
            "exec_reroot_out",
            "MATCH (m)-[r]->(n) WHERE id(n) = 1 RETURN m.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_on_end_reroots_incoming_expansion() {
        // `(m)<-[r]-(n) WHERE id(n)=0`: n is Alice; m is each of Alice's
        // out-neighbours (Bob, Acme, Carol) — same as a forward expansion from her.
        let (root, res) = run(
            "exec_reroot_in",
            "MATCH (m)<-[r]-(n) WHERE id(n) = 0 RETURN m.name AS name",
        );
        assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn reroot_matches_unrerooted_result_set() {
        // Both Bob and Alice point to Carol(2); re-rooting must find both.
        let (root, res) = run(
            "exec_reroot_multi",
            "MATCH (m)-[r]->(n) WHERE id(n) = 2 RETURN m.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn reroot_still_enforces_end_label() {
        // Acme(3) is a Company reached from Alice via WORKS_AT → one row.
        let (root, res) = run(
            "exec_reroot_label_ok",
            "MATCH (m)-[r]->(n:Company) WHERE id(n) = 3 RETURN m.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice"]);
        // Bob(1) is a Person, so the :Company constraint on the seeked end empties it.
        let (root2, res2) = run(
            "exec_reroot_label_no",
            "MATCH (m)-[r]->(n:Company) WHERE id(n) = 1 RETURN m.name AS name",
        );
        assert!(res2.rows.is_empty());
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&root2);
    }

    #[test]
    fn varlength_end_id_is_not_rerooted_but_correct() {
        // A `*` hop is excluded from re-rooting (order of a returned rel-list could
        // change); the result must still be correct via the normal scan. Paths
        // ending at Carol(2): Bob→Carol, Alice→Carol, Alice→Bob→Carol ⇒ {Alice,Bob}.
        let (root, res) = run(
            "exec_reroot_varlen",
            "MATCH (m)-[r*1..2]->(n) WHERE id(n) = 2 RETURN DISTINCT m.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── §1 list comprehension ──────────────────────────────────────────────

    /// Display a single-row, single-column list result as a Vec of display strings.
    fn list0(res: &QueryResult) -> Vec<String> {
        assert_eq!(res.rows.len(), 1, "expected exactly one row");
        match &res.rows[0][0] {
            Val::List(xs) => xs.iter().map(|v| v.to_display()).collect(),
            other => panic!("expected a list, got {other:?}"),
        }
    }

    #[test]
    fn list_comprehension_filter_keeps_non_null() {
        let (root, res) = run(
            "exec_listcomp_filter",
            "RETURN [x IN [1, null, 2] WHERE x IS NOT NULL] AS r",
        );
        assert_eq!(list0(&res), vec!["1", "2"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn list_comprehension_projection_only() {
        let (root, res) = run("exec_listcomp_map", "RETURN [x IN [1, 2, 3] | x * 2] AS r");
        assert_eq!(list0(&res), vec!["2", "4", "6"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn list_comprehension_filter_and_projection() {
        let (root, res) = run(
            "exec_listcomp_both",
            "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS r",
        );
        assert_eq!(list0(&res), vec!["4", "6"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn list_comprehension_then_index() {
        // The primary call site: extract the first non-`Concept` label.
        let (root, res) = run(
            "exec_listcomp_index",
            "RETURN [l IN ['Concept', 'Person'] WHERE l <> 'Concept'][0] AS r",
        );
        assert_eq!(res.rows[0][0].to_display(), "Person");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn list_comprehension_null_source_is_null() {
        let (root, res) = run(
            "exec_listcomp_null",
            "RETURN [x IN null WHERE x > 1 | x] AS r",
        );
        assert!(matches!(res.rows[0][0], Val::Null));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn list_comprehension_nested() {
        // Inner builds [0,2,4,6] (evens 0..6); outer keeps those whose double is
        // ≥ 4 and doubles them: 2→4, 4→8, 6→12.
        let (root, res) = run(
            "exec_listcomp_nested",
            "RETURN [e IN [n IN [0,1,2,3,4,5,6] WHERE n % 2 = 0] WHERE e * 2 >= 4 | e * 2] AS r",
        );
        assert_eq!(list0(&res), vec!["4", "8", "12"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn bare_membership_list_still_parses_as_list_literal() {
        // `[x IN list]` (no WHERE/`|`) must remain a one-element list literal whose
        // element is the membership test — NOT a comprehension.
        let (root, res) = run("exec_membership_literal", "RETURN [2 IN [1, 2, 3]] AS r");
        match &res.rows[0][0] {
            Val::List(xs) => {
                assert_eq!(xs.len(), 1);
                assert!(matches!(xs[0], Val::Bool(true)));
            }
            other => panic!("expected a one-element list, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── §2 pattern comprehension ────────────────────────────────────────────

    #[test]
    fn pattern_comprehension_degree_via_size() {
        // size([(n)-[:KNOWS]->(:Person) | 1]) — outgoing KNOWS degree per person.
        // Alice→{Bob,Carol}=2, Bob→{Carol}=1, Carol→{}=0.
        let (root, res) = run(
            "exec_patcomp_size",
            "MATCH (n:Person) RETURN n.name AS name, size([(n)-[:KNOWS]->(:Person) | 1]) AS deg ORDER BY name",
        );
        let got: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("Alice".into(), "2".into()),
                ("Bob".into(), "1".into()),
                ("Carol".into(), "0".into()),
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn pattern_comprehension_collects_neighbour_props() {
        // Alice knows Bob and Carol; the projection collects their names.
        let (root, res) = run(
            "exec_patcomp_names",
            "MATCH (n:Person {name: 'Alice'}) RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends",
        );
        let mut friends = list0(&res);
        friends.sort();
        assert_eq!(friends, vec!["Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn pattern_comprehension_empty_match_is_empty_list() {
        // Carol has no outgoing KNOWS edge → an empty list, not null.
        let (root, res) = run(
            "exec_patcomp_empty",
            "MATCH (n:Person {name: 'Carol'}) RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends",
        );
        match &res.rows[0][0] {
            Val::List(xs) => assert!(xs.is_empty()),
            other => panic!("expected an empty list, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── §3 UNWIND ───────────────────────────────────────────────────────────

    #[test]
    fn unwind_list_emits_one_row_per_element() {
        let (root, res) = run("exec_unwind_list", "UNWIND [1, 2, 3] AS x RETURN x");
        assert_eq!(col0(&res), vec!["1", "2", "3"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unwind_empty_and_null_emit_zero_rows() {
        let (root, res) = run("exec_unwind_empty", "UNWIND [] AS x RETURN x");
        assert!(res.rows.is_empty());
        let (root2, res2) = run("exec_unwind_null", "UNWIND null AS x RETURN x");
        assert!(res2.rows.is_empty());
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&root2);
    }

    #[test]
    fn unwind_scalar_wraps_as_single_row() {
        // FalkorDB divergence from Neo4j: a scalar unwinds to one row.
        let (root, res) = run("exec_unwind_scalar", "UNWIND 5 AS q RETURN q");
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(5)));
        let (root2, res2) = run("exec_unwind_scalar_str", "UNWIND 'abc' AS q RETURN q");
        assert_eq!(res2.rows.len(), 1);
        assert_eq!(res2.rows[0][0].to_display(), "abc");
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&root2);
    }

    #[test]
    fn unwind_null_element_is_a_real_row() {
        let (root, res) = run("exec_unwind_null_elem", "UNWIND [1, null, 2] AS x RETURN x");
        assert_eq!(res.rows.len(), 3);
        assert!(matches!(res.rows[1][0], Val::Null));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unwind_preserves_upstream_context() {
        // The original `l` column survives alongside the unwound `x` (TCK scenario:
        // UNWIND does not prune context).
        let (root, res) = run(
            "exec_unwind_ctx",
            "WITH [1, 2] AS l UNWIND l AS x RETURN l, x ORDER BY x",
        );
        assert_eq!(res.rows.len(), 2);
        // Each row keeps the full list in column 0 and one element in column 1.
        assert!(matches!(&res.rows[0][0], Val::List(xs) if xs.len() == 2));
        assert!(matches!(res.rows[0][1], Val::Int(1)));
        assert!(matches!(res.rows[1][1], Val::Int(2)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unwind_variable_length_relationship_list() {
        // §3+§4 combined: unwind a collected edge list, then read its endpoints.
        let (root, res) = run(
            "exec_unwind_rels",
            "MATCH (a)-[r*1..2]->(b) WITH r LIMIT 1 UNWIND r AS e RETURN type(e) AS t",
        );
        assert!(res
            .rows
            .iter()
            .all(|row| row[0].to_display() == "KNOWS" || row[0].to_display() == "WORKS_AT"));
        assert!(!res.rows.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── §4 startNode / endNode ──────────────────────────────────────────────

    #[test]
    fn start_and_end_node_match_walked_endpoints() {
        // For every KNOWS edge, startNode(e)==a and endNode(e)==b.
        let (root, res) = run(
            "exec_startend",
            "MATCH (a)-[e:KNOWS]->(b) RETURN a.name AS an, startNode(e).name AS sn, b.name AS bn, endNode(e).name AS en",
        );
        assert!(!res.rows.is_empty());
        for r in &res.rows {
            assert_eq!(r[0].to_display(), r[1].to_display(), "startNode mismatch");
            assert_eq!(r[2].to_display(), r[3].to_display(), "endNode mismatch");
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn start_node_of_null_is_null() {
        let (root, res) = run(
            "exec_startnull",
            "OPTIONAL MATCH (a:Person)-[e:NONEXISTENT]->(b) RETURN startNode(e) AS s LIMIT 1",
        );
        assert!(matches!(res.rows[0][0], Val::Null));
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Parse + run `q` expecting an engine error; returns the error text.
    fn run_err(root_tag: &str, q: &str) -> String {
        let (root, graph, _) = testgen::write_basic(root_tag);
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let ast = parser::parse(q).unwrap();
        let err = engine.run(&ast).expect_err("expected query error");
        let _ = std::fs::remove_dir_all(&root);
        err.to_string()
    }

    // Phase 4 — regex `=~` full-match operator (openCypher / FalkorDB
    // `str_MatchRegex`: the whole subject must match, anchored at both ends).
    #[test]
    fn phase4_regex_match_operator() {
        let (root, res) = run(
            "exec_p4_regex",
            "RETURN 'abc' =~ 'a.c' AS m1, 'abc' =~ 'a' AS m2, 'abc' =~ 'ab.*' AS m3, \
             'Hello World' =~ '.*World' AS m4, 'A' =~ 'a' AS m5, \
             null =~ 'a' AS m6, 'foo' =~ null AS m7",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "true"); // full match
        assert_eq!(render(&r[1]), "false"); // 'a' is not the whole 'abc'
        assert_eq!(render(&r[2]), "true");
        assert_eq!(render(&r[3]), "true");
        assert_eq!(render(&r[4]), "false"); // case-sensitive
        assert_eq!(render(&r[5]), "null"); // null subject -> null
        assert_eq!(render(&r[6]), "null"); // null pattern -> null
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase4_regex_invalid_pattern_errors() {
        let msg = run_err("exec_p4_badregex", "RETURN 'aa' =~ '('");
        assert!(msg.contains("Invalid regex"), "got: {msg}");
    }

    // Phase 4 — string.join (vectors ported from test_function_calls.py test89).
    #[test]
    fn phase4_string_join() {
        let (root, res) = run(
            "exec_p4_join",
            "RETURN string.join(['HELL','OW']) AS a, string.join(['HELL','OW'], ' ') AS b, \
             string.join(['HELL'], ' ') AS c, string.join(['HELL','OW','NOW'], ' ') AS d, \
             string.join([]) AS e, string.join([], '|') AS f, string.join(null, '') AS g",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "'HELLOW'");
        assert_eq!(render(&r[1]), "'HELL OW'");
        assert_eq!(render(&r[2]), "'HELL'");
        assert_eq!(render(&r[3]), "'HELL OW NOW'");
        assert_eq!(render(&r[4]), "''");
        assert_eq!(render(&r[5]), "''");
        assert_eq!(render(&r[6]), "null");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase4_string_join_type_mismatch_errors() {
        let msg = run_err("exec_p4_join_err", "RETURN string.join(['HELL', 2], ' ')");
        assert!(
            msg.contains("Type mismatch") && msg.contains("Integer"),
            "got: {msg}"
        );
    }

    // Phase 4 — string.matchRegEx (vectors ported from test_function_calls.py
    // test91). Unanchored scan; each match is [full, group1, …]; null -> [].
    #[test]
    fn phase4_string_matchregex() {
        let (root, res) = run(
            "exec_p4_matchregex",
            r"RETURN
                string.matchRegEx('blabla <header h1>txt1</header>', '<header (\w+)>(\w+)</header>') AS a,
                string.matchRegEx('blabla <header h1>txt1</header> blabla <header h2>txt2</header>', '<header (\w+)>(\w+)</header>') AS b,
                string.matchRegEx('aba', 'a') AS c,
                string.matchRegEx('', 'a') AS d,
                string.matchRegEx('bla', '(bla)(bal)') AS e,
                string.matchRegEx('bla9', '(bla)[(bal)9]') AS f,
                string.matchRegEx(null, 'bla') AS g,
                string.matchRegEx('bla', null) AS h",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "[['<header h1>txt1</header>','h1','txt1']]");
        assert_eq!(
            render(&r[1]),
            "[['<header h1>txt1</header>','h1','txt1'],['<header h2>txt2</header>','h2','txt2']]"
        );
        assert_eq!(render(&r[2]), "[['a'],['a']]");
        assert_eq!(render(&r[3]), "[]");
        assert_eq!(render(&r[4]), "[]");
        assert_eq!(render(&r[5]), "[['bla9','bla']]");
        assert_eq!(render(&r[6]), "[]");
        assert_eq!(render(&r[7]), "[]");
        let _ = std::fs::remove_dir_all(&root);
    }

    // Phase 4 — string.replaceRegEx (vectors ported from test_function_calls.py
    // test92). Literal replacement (no `$group` expansion); null operand -> null.
    #[test]
    fn phase4_string_replaceregex() {
        let (root, res) = run(
            "exec_p4_replaceregex",
            r"RETURN
                string.replaceRegEx('blabla <header h1>txt1</header>', '<header (\w+)>(\w+)</header>', 'hellow') AS a,
                string.replaceRegEx('blabla <header h1>txt1</header> blabla <header h2>txt2</header>', '<header (\w+)>(\w+)</header>', 'hellow') AS b,
                string.replaceRegEx('abc', '[b]') AS c,
                string.replaceRegEx('abc', '[b]', '55') AS d,
                string.replaceRegEx('abcb', '[b]', '') AS e,
                string.replaceRegEx('bbla', '[b]', 'bla') AS f,
                string.replaceRegEx('', '[b]', 'bla') AS g,
                string.replaceRegEx(null, 'bla') AS h,
                string.replaceRegEx('bla', null) AS i,
                string.replaceRegEx('bla', 'bla', null) AS j",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "'blabla hellow'");
        assert_eq!(render(&r[1]), "'blabla hellow blabla hellow'");
        assert_eq!(render(&r[2]), "'ac'");
        assert_eq!(render(&r[3]), "'a55c'");
        assert_eq!(render(&r[4]), "'ac'");
        assert_eq!(render(&r[5]), "'blablala'");
        assert_eq!(render(&r[6]), "''");
        assert_eq!(render(&r[7]), "null");
        assert_eq!(render(&r[8]), "null");
        assert_eq!(render(&r[9]), "null");
        let _ = std::fs::remove_dir_all(&root);
    }

    // Phase 5 — list slice `[i..j]` (vectors ported from TCK List2.feature and
    // FalkorDB `AR_SLICE`). Open ends, negative indices, empty/exceeding ranges.
    #[test]
    fn phase5_list_slice() {
        let (root, res) = run(
            "exec_p5_slice",
            "WITH [1,2,3,4,5] AS l5, [1,2,3] AS l3 RETURN \
             l5[1..3] AS a, l3[1..] AS b, l3[..2] AS c, l3[0..1] AS d, \
             l3[0..0] AS e, l3[-3..-1] AS f, l3[3..1] AS g, l3[-5..5] AS h, \
             l3[..] AS i",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "[2,3]");
        assert_eq!(render(&r[1]), "[2,3]");
        assert_eq!(render(&r[2]), "[1,2]");
        assert_eq!(render(&r[3]), "[1]");
        assert_eq!(render(&r[4]), "[]");
        assert_eq!(render(&r[5]), "[1,2]");
        assert_eq!(render(&r[6]), "[]");
        assert_eq!(render(&r[7]), "[1,2,3]");
        assert_eq!(render(&r[8]), "[1,2,3]");
        let _ = std::fs::remove_dir_all(&root);
    }

    // Phase 5 — slice null handling (test_list.py test03 + TCK List2 [9]): a NULL
    // list or any NULL bound yields NULL.
    #[test]
    fn phase5_slice_null() {
        let (root, res) = run(
            "exec_p5_slice_null",
            "WITH null AS n, [1,2,3] AS l RETURN \
             n[0..5] AS a, l[0..null] AS b, l[null..2] AS c, l[null..] AS d, n[..] AS e",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "null");
        assert_eq!(render(&r[1]), "null");
        assert_eq!(render(&r[2]), "null");
        assert_eq!(render(&r[3]), "null");
        assert_eq!(render(&r[4]), "null");
        let _ = std::fs::remove_dir_all(&root);
    }

    // Phase 5 — string slicing (Slater extension beyond FalkorDB's array-only
    // slice; slices by Unicode scalar value).
    #[test]
    fn phase5_string_slice() {
        let (root, res) = run(
            "exec_p5_str_slice",
            "WITH 'hello' AS s RETURN s[1..3] AS a, s[..2] AS b, s[2..] AS c, s[-2..] AS d",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "'el'");
        assert_eq!(render(&r[1]), "'he'");
        assert_eq!(render(&r[2]), "'llo'");
        assert_eq!(render(&r[3]), "'lo'");
        let _ = std::fs::remove_dir_all(&root);
    }

    // Phase 5 — reduce (vectors ported from FalkorDB test_reduce.py).
    #[test]
    fn phase5_reduce() {
        let (root, res) = run(
            "exec_p5_reduce",
            "RETURN \
             reduce(sum = 0, n in [1,2,3] | sum + n) AS a, \
             reduce(sum = 0, n in [1,2,3] | sum - n) AS b, \
             reduce(sum = 0, n in ['1','2','3'] | sum + toInteger(n)) AS c, \
             reduce(last = 0, n in [1,2,3] | n) AS d, \
             reduce(msg = 'hello ', c in ['w','o','r','l','d'] | msg + c) AS e, \
             reduce(arr = [1,2], n in [2,3] | arr + n) AS f, \
             reduce(sum = 1, n in [] | sum + n) AS g",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "6");
        assert_eq!(render(&r[1]), "-6");
        assert_eq!(render(&r[2]), "6");
        assert_eq!(render(&r[3]), "3");
        assert_eq!(render(&r[4]), "'hello world'");
        assert_eq!(render(&r[5]), "[1,2,2,3]");
        assert_eq!(render(&r[6]), "1");
        let _ = std::fs::remove_dir_all(&root);
    }

    // Phase 5 — reduce with carried/outer variables and nesting (test_reduce.py
    // test_variable_reduction / test_nested_reduction / test_multiple_reductions).
    #[test]
    fn phase5_reduce_variables_and_nesting() {
        let (root, res) = run(
            "exec_p5_reduce_vars",
            "WITH 1 AS base, [1,2,3] AS arr, -1 AS bias \
             RETURN reduce(sum = base, n in arr | sum + n + bias) AS a, \
             reduce(sum = reduce(x = 1, n in [1] | x + n), \
                    n in reduce(arr = [1], n in [2] | arr + n) | sum + n) AS b",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "4");
        assert_eq!(render(&r[1]), "5");
        let _ = std::fs::remove_dir_all(&root);

        let (root, res) = run(
            "exec_p5_reduce_multi",
            "UNWIND [[1,2,3],[4,5,6]] AS arr RETURN reduce(sum = 1, n in arr | sum + n) AS s",
        );
        assert_eq!(col0(&res), vec!["16", "7"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    // Phase 5 — reduce null/error handling (test_reduce.py test_null_reduction /
    // test_type_missmatch_reduction).
    #[test]
    fn phase5_reduce_null_and_errors() {
        let (root, res) = run(
            "exec_p5_reduce_null",
            "RETURN reduce(sum = null, n in [1,2,3] | sum + n) AS a, \
             reduce(sum = 1, n in null | sum + n) AS b, \
             reduce(sum = 1, n in [1,2,3] | sum + n + null) AS c",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "null");
        assert_eq!(render(&r[1]), "null");
        assert_eq!(render(&r[2]), "null");
        let _ = std::fs::remove_dir_all(&root);

        // 'a' * 1 is an invalid operation; '2' is not a list.
        assert!(run_err(
            "exec_p5_reduce_e1",
            "RETURN reduce(sum = 'a', n in [1,2,3] | sum * n)"
        )
        .contains("cannot apply arithmetic"));
        assert!(run_err(
            "exec_p5_reduce_e2",
            "RETURN reduce(sum = 1, n in 2 | sum + n)"
        )
        .contains("needs a list"));
        // A reduce missing its `| body` is a plain function call over the
        // would-be accumulator binding `sum`, which is unbound -> runtime error.
        assert!(
            run_err("exec_p5_reduce_e3", "RETURN reduce(sum = 0, n in [1,2,3])").contains("'sum'")
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── Phase 6 — pattern predicates & EXISTS { } ──────────────────────────────
    //
    // Vectors are adapted from FalkorDB/TCK `expressions/pattern/Pattern1.feature`
    // and `existentialSubqueries/ExistentialSubquery1.feature` onto the shared
    // read-only fixture (those scenarios use CREATE setup we cannot replay).
    // Fixture topology:
    //   Alice -KNOWS-> Bob, Bob -KNOWS-> Carol, Alice -KNOWS-> Carol,
    //   Alice -WORKS_AT-> Acme, Carol -WORKS_AT-> Globex.

    // Pattern1 [1]/[4]/[6]: any / typed-outgoing / typed-incoming connection.
    #[test]
    fn phase6_pattern_predicate_directions() {
        // Any outgoing edge — everyone with an out-edge (not the two companies).
        let (root, res) = run(
            "exec_p6_any_out",
            "MATCH (n) WHERE (n)-->() RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);

        // Outgoing KNOWS only (Carol's sole out-edge is WORKS_AT).
        let (root, res) = run(
            "exec_p6_knows_out",
            "MATCH (n) WHERE (n)-[:KNOWS]->() RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob"]);
        let _ = std::fs::remove_dir_all(&root);

        // Incoming KNOWS.
        let (root, res) = run(
            "exec_p6_knows_in",
            "MATCH (n) WHERE (n)<-[:KNOWS]-() RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    // Pattern1 [5]: undirected connection sees the edge from either end.
    #[test]
    fn phase6_pattern_predicate_undirected_and_label() {
        let (root, res) = run(
            "exec_p6_undirected",
            "MATCH (n) WHERE (n)-[:WORKS_AT]-() RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Acme", "Alice", "Carol", "Globex"]);
        let _ = std::fs::remove_dir_all(&root);

        // A label predicate on the far node restricts the match.
        let (root, res) = run(
            "exec_p6_label",
            "MATCH (n) WHERE (n)-[:WORKS_AT]->(:Company) RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    // Pattern1 [19]/[20]/[21]: negation, conjunction, disjunction of predicates.
    #[test]
    fn phase6_pattern_predicate_boolean_combinations() {
        // NOT — anti-semi-apply: the two companies have no out-edge.
        let (root, res) = run(
            "exec_p6_not",
            "MATCH (n) WHERE NOT (n)-->() RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Acme", "Globex"]);
        let _ = std::fs::remove_dir_all(&root);

        // Conjunction — only Alice both KNOWS-out and WORKS_AT-out.
        let (root, res) = run(
            "exec_p6_and",
            "MATCH (n) WHERE (n)-[:KNOWS]->() AND (n)-[:WORKS_AT]->() RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice"]);
        let _ = std::fs::remove_dir_all(&root);

        // Disjunction — WORKS_AT-out (Alice, Carol) OR KNOWS-in (Bob, Carol).
        let (root, res) = run(
            "exec_p6_or",
            "MATCH (n) WHERE (n)-[:WORKS_AT]->() OR (n)<-[:KNOWS]-() RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    // Pattern1 [14]: two bound endpoints — the predicate pins both sides.
    #[test]
    fn phase6_pattern_predicate_two_bound_nodes() {
        let (root, res) = run(
            "exec_p6_two_node",
            "MATCH (n), (m) WHERE (n)-[:KNOWS]->(m) RETURN n.name AS a, m.name AS b",
        );
        let mut pairs: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("Alice".into(), "Bob".into()),
                ("Alice".into(), "Carol".into()),
                ("Bob".into(), "Carol".into()),
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // ExistentialSubquery1 [1]/[3]: simple EXISTS, with and without a match.
    #[test]
    fn phase6_exists_simple() {
        let (root, res) = run(
            "exec_p6_exists_knows",
            "MATCH (n) WHERE EXISTS { (n)-[:KNOWS]->() } RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob"]);
        let _ = std::fs::remove_dir_all(&root);

        // A non-existent relationship type yields no matches → empty result.
        let (root, res) = run(
            "exec_p6_exists_none",
            "MATCH (n) WHERE EXISTS { (n)-[:NOSUCHREL]->() } RETURN n.name AS name",
        );
        assert!(res.rows.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    // ExistentialSubquery2 [1]: the explicit-MATCH inner form with a label.
    #[test]
    fn phase6_exists_with_match_keyword() {
        let (root, res) = run(
            "exec_p6_exists_match",
            "MATCH (n) WHERE EXISTS { MATCH (n)-[:WORKS_AT]->(:Company) } RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    // ExistentialSubquery1 [2]: inner WHERE correlating outer and inner bindings.
    #[test]
    fn phase6_exists_inner_where_correlated() {
        // Who points at someone older? Only Alice(30)->Bob(25) satisfies n.age >
        // m.age; Acme/Globex have no age so the comparison is NULL (excluded).
        let (root, res) = run(
            "exec_p6_exists_where",
            "MATCH (n) WHERE EXISTS { (n)-->(m) WHERE n.age > m.age } RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice"]);
        let _ = std::fs::remove_dir_all(&root);

        // Negated EXISTS — nodes with no outgoing KNOWS edge.
        let (root, res) = run(
            "exec_p6_not_exists",
            "MATCH (n) WHERE NOT EXISTS { (n)-[:KNOWS]->() } RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Acme", "Carol", "Globex"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── Phase 7 — Val::Path, path functions, shortestPath ────────────────────

    // `MATCH p=(…)-[…]->(…) RETURN p` binds a path; nodes()/length() read it back.
    // Vectors adapted from FalkorDB tests/flow/test_path.py (read-only fixture).
    #[test]
    fn phase7_path_binding_and_functions() {
        let (root, res) = run(
            "exec_p7_path_bind",
            "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b:Person) \
             RETURN [n IN nodes(p) | n.name] AS names, length(p) AS l ORDER BY b.name",
        );
        assert_eq!(res.columns, vec!["names", "l"]);
        assert_eq!(res.rows.len(), 2);
        assert_eq!(render(&res.rows[0][0]), "['Alice','Bob']");
        assert!(matches!(res.rows[0][1], Val::Int(1)));
        assert_eq!(render(&res.rows[1][0]), "['Alice','Carol']");
        assert!(matches!(res.rows[1][1], Val::Int(1)));
        let _ = std::fs::remove_dir_all(&root);
    }

    // A variable-length path binds every node along the walk (incl. intermediates).
    #[test]
    fn phase7_variable_length_path() {
        let (root, res) = run(
            "exec_p7_varlen_path",
            "MATCH p=(a:Person {name:'Alice'})-[:KNOWS*]->(b:Person) \
             RETURN [n IN nodes(p) | n.name] AS names ORDER BY length(p), b.name",
        );
        let got: Vec<String> = res.rows.iter().map(|r| render(&r[0])).collect();
        assert_eq!(
            got,
            vec![
                "['Alice','Bob']",
                "['Alice','Carol']",
                "['Alice','Bob','Carol']",
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // relationships(p) yields the edges in walk order; type()/id() read them.
    #[test]
    fn phase7_relationships_function() {
        let (root, res) = run(
            "exec_p7_rels_fn",
            "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'}) \
             RETURN [r IN relationships(p) | type(r)] AS types, \
                    [r IN relationships(p) | id(r)] AS ids",
        );
        assert_eq!(render(&res.rows[0][0]), "['KNOWS']");
        assert_eq!(render(&res.rows[0][1]), "[0]");
        let _ = std::fs::remove_dir_all(&root);
    }

    // Path equality/inequality filters (test_path.py test_path_comparison). Each of
    // the 3 KNOWS paths equals only itself, so `p1 = p2` keeps 3 of the 9 pairs.
    #[test]
    fn phase7_path_equality() {
        let (root, res) = run(
            "exec_p7_path_eq",
            "MATCH p1=(a:Person)-[:KNOWS]->(b:Person) \
             MATCH p2=(c:Person)-[:KNOWS]->(d:Person) WHERE p1 = p2 RETURN count(*) AS c",
        );
        assert!(matches!(res.rows[0][0], Val::Int(3)));
        let _ = std::fs::remove_dir_all(&root);

        let (root, res) = run(
            "exec_p7_path_neq",
            "MATCH p1=(a:Person)-[:KNOWS]->(b:Person) \
             MATCH p2=(c:Person)-[:KNOWS]->(d:Person) WHERE p1 <> p2 RETURN count(*) AS c",
        );
        assert!(matches!(res.rows[0][0], Val::Int(6)));
        let _ = std::fs::remove_dir_all(&root);
    }

    // shortestPath finds the fewest-hop route: Alice→Carol direct (e4), not via Bob.
    // A reversed pattern `(c)<-[*]-(a)` yields the same path (test_shortest_path.py).
    #[test]
    fn phase7_shortest_path() {
        let (root, res) = run(
            "exec_p7_sp",
            "MATCH (a:Person {name:'Alice'}), (c:Person {name:'Carol'}) \
             RETURN length(shortestPath((a)-[:KNOWS*]->(c))) AS l, \
                    [n IN nodes(shortestPath((a)-[:KNOWS*]->(c))) | n.name] AS names, \
                    [n IN nodes(shortestPath((c)<-[:KNOWS*]-(a))) | n.name] AS rev",
        );
        assert!(matches!(res.rows[0][0], Val::Int(1)));
        assert_eq!(render(&res.rows[0][1]), "['Alice','Carol']");
        assert_eq!(render(&res.rows[0][2]), "['Alice','Carol']");
        let _ = std::fs::remove_dir_all(&root);
    }

    // `*0..` admits the empty (single-node) path when src == dst; `*` (min 1) does
    // not, so a node with no cycle back to itself yields NULL (test05_min_hops).
    #[test]
    fn phase7_shortest_path_min_zero() {
        let (root, res) = run(
            "exec_p7_sp_zero",
            "MATCH (a:Person {name:'Alice'}) \
             RETURN length(shortestPath((a)-[:KNOWS*0..]->(a))) AS l, \
                    [n IN nodes(shortestPath((a)-[:KNOWS*0..]->(a))) | n.name] AS names, \
                    shortestPath((a)-[:KNOWS*]->(a)) IS NULL AS cyc_null",
        );
        assert!(matches!(res.rows[0][0], Val::Int(0)));
        assert_eq!(render(&res.rows[0][1]), "['Alice']");
        assert!(matches!(res.rows[0][2], Val::Bool(true)));
        let _ = std::fs::remove_dir_all(&root);
    }

    // No connecting path → NULL (Bob cannot reach Alice over KNOWS).
    #[test]
    fn phase7_shortest_path_no_path() {
        let (root, res) = run(
            "exec_p7_sp_none",
            "MATCH (a:Person {name:'Bob'}), (c:Person {name:'Alice'}) \
             RETURN shortestPath((a)-[:KNOWS*]->(c)) IS NULL AS np",
        );
        assert!(matches!(res.rows[0][0], Val::Bool(true)));
        let _ = std::fs::remove_dir_all(&root);
    }

    // shortestPath inside a WHERE filter (test07_shortestPath_in_filter): keep source
    // nodes that can reach Carol over KNOWS — Alice and Bob (Carol has no cycle).
    #[test]
    fn phase7_shortest_path_in_filter() {
        let (root, res) = run(
            "exec_p7_sp_filter",
            "MATCH (a:Person), (c:Person {name:'Carol'}) \
             WHERE length(shortestPath((a)-[:KNOWS*]->(c))) > 0 RETURN a.name AS n",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    // The wrapped-pattern restrictions FalkorDB enforces (test01_invalid_shortest_paths).
    #[test]
    fn phase7_shortest_path_errors() {
        let pre = "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Carol'}) RETURN ";
        let cases = [
            (
                "exec_p7_sp_e1",
                "shortestPath((a)-[:KNOWS*2..]->(b))",
                "minimal length",
            ),
            (
                "exec_p7_sp_e2",
                "shortestPath((a)-[:KNOWS]->()-[:KNOWS*]->(b))",
                "single relationship",
            ),
            (
                "exec_p7_sp_e3",
                "shortestPath((a)-[:KNOWS* {since:2020}]->(b))",
                "filters on relationships",
            ),
            (
                "exec_p7_sp_e4",
                "shortestPath((a)-[:KNOWS*]->())",
                "requires bound nodes",
            ),
        ];
        for (tag, sp, want) in cases {
            let msg = run_err(tag, &format!("{pre}{sp}"));
            assert!(msg.contains(want), "query `{sp}` → `{msg}` (want `{want}`)");
        }

        // An unbound endpoint variable is likewise rejected.
        let msg = run_err(
            "exec_p7_sp_e5",
            "MATCH (a:Person {name:'Alice'}) RETURN shortestPath((a)-[:KNOWS*]->(z))",
        );
        assert!(msg.contains("requires bound nodes"), "{msg}");
    }

    // ── Phase 11: metadata procedures (CALL dispatch) ────────────────────────
    // Vectors adapted from FalkorDB tests/flow/test_procedures.py (test11/test12)
    // onto the read-only fixture: Person(3)/Company(2) nodes, KNOWS(3)/WORKS_AT(2)
    // edges, 5 property keys.

    fn map_get<'a>(v: &'a Val, key: &str) -> &'a Val {
        match v {
            Val::Map(m) => m
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, val)| val)
                .unwrap_or_else(|| panic!("key {key:?} absent in {v:?}")),
            o => panic!("expected map, got {o:?}"),
        }
    }

    #[test]
    fn phase11_meta_stats_bare() {
        // A bare `CALL db.meta.stats()` (no YIELD/RETURN) returns every output.
        let (root, res) = run("exec_p11_meta", "CALL db.meta.stats()");
        assert_eq!(
            res.columns,
            vec![
                "labels",
                "relTypes",
                "relCount",
                "nodeCount",
                "labelCount",
                "relTypeCount",
                "propertyKeyCount"
            ]
        );
        assert_eq!(res.rows.len(), 1);
        let r = &res.rows[0];
        assert!(matches!(map_get(&r[0], "Person"), Val::Int(3)));
        assert!(matches!(map_get(&r[0], "Company"), Val::Int(2)));
        assert!(matches!(map_get(&r[1], "KNOWS"), Val::Int(3)));
        assert!(matches!(map_get(&r[1], "WORKS_AT"), Val::Int(2)));
        assert!(matches!(r[2], Val::Int(5)), "relCount");
        assert!(matches!(r[3], Val::Int(5)), "nodeCount");
        assert!(matches!(r[4], Val::Int(2)), "labelCount");
        assert!(matches!(r[5], Val::Int(2)), "relTypeCount");
        assert!(matches!(r[6], Val::Int(6)), "propertyKeyCount");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase11_meta_stats_yield_projection() {
        // YIELD selects/reorders outputs into a downstream pipeline.
        let (root, res) = run(
            "exec_p11_meta_yield",
            "CALL db.meta.stats() YIELD nodeCount, relCount, propertyKeyCount \
             RETURN propertyKeyCount AS pk, nodeCount AS n, relCount AS r",
        );
        assert_eq!(res.columns, vec!["pk", "n", "r"]);
        let r = &res.rows[0];
        assert!(matches!(r[0], Val::Int(6))); // propertyKeyCount (name/age/city/since/embedding/team)
        assert!(matches!(r[1], Val::Int(5)));
        assert!(matches!(r[2], Val::Int(5)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase11_dbms_procedures_yield_order() {
        // FalkorDB test11 form: YIELD mode, name RETURN mode, name ORDER BY name.
        let (root, res) = run(
            "exec_p11_procs",
            "CALL dbms.procedures() YIELD mode, name RETURN mode, name ORDER BY name",
        );
        assert_eq!(res.columns, vec!["mode", "name"]);
        // Every procedure is READ; names are sorted.
        let names: Vec<String> = res.rows.iter().map(|r| r[1].to_display()).collect();
        assert!(res.rows.iter().all(|r| r[0].to_display() == "READ"));
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted, "ORDER BY name");
        for want in [
            "db.constraints",
            "db.meta.stats",
            "dbms.functions",
            "dbms.procedures",
        ] {
            assert!(names.iter().any(|n| n == want), "missing {want}");
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase11_dbms_functions_aggregation_flag() {
        // FalkorDB test12 form (literals instead of $param): the aggregation flag
        // distinguishes aggregates from scalars.
        let (root, res) = run(
            "exec_p11_funcs",
            "CALL dbms.functions() YIELD name, aggregation \
             WHERE name IN ['avg', 'count', 'sin'] \
             RETURN name, aggregation ORDER BY name",
        );
        assert_eq!(res.columns, vec!["name", "aggregation"]);
        let got: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("avg".to_string(), "true".to_string()),
                ("count".to_string(), "true".to_string()),
                ("sin".to_string(), "false".to_string()),
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase11_dbms_functions_coverage_gate() {
        // The self-report is the coverage gate: a representative sample of the
        // functions landed through Phases 1–9 must be present.
        let (root, res) = run(
            "exec_p11_funcs_cov",
            "CALL dbms.functions() YIELD name RETURN name",
        );
        let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
        for want in [
            "sin",
            "tail",
            "point",
            "distance",
            "vec.euclideandistance",
            "tofloatornull",
            "percentilecont",
            "string.matchregex",
            "date",
            "duration",
        ] {
            assert!(
                names.iter().any(|n| n == want),
                "coverage gate missing {want}"
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase11_db_constraints_empty() {
        // slater enforces no constraints → empty result with the FalkorDB shape.
        let (root, res) = run(
            "exec_p11_constraints",
            "CALL db.constraints() YIELD type, label, properties, entitytype, status \
             RETURN type, label, properties, entitytype, status",
        );
        assert_eq!(
            res.columns,
            vec!["type", "label", "properties", "entitytype", "status"]
        );
        assert!(res.rows.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase11_call_unknown_yield_errors() {
        let (root, graph, _) = testgen::write_basic("exec_p11_badyield");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let ast = parser::parse("CALL db.meta.stats() YIELD bogus RETURN bogus").unwrap();
        let err = engine.run(&ast).unwrap_err().to_string();
        assert!(err.contains("does not yield 'bogus'"), "{err}");
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── Phase 12 — CALL { … } subquery ───────────────────────────────────────
    // Vectors adapted from FalkorDB `tests/flow/test_call_subquery.py` (test02–07,
    // test14, test17) onto the read-only fixture (Person Alice/Bob/Carol with
    // name/age/city; their CREATE-based setup is replayed as MATCH over the
    // fixture).

    #[test]
    fn phase12_simple_scan_return() {
        // test02: a plain scan-and-return subquery, with an outer RETURN over it.
        let (root, res) = run(
            "exec_p12_scan",
            "CALL { MATCH (n:Person {name: 'Alice'}) RETURN n } RETURN n.name AS name",
        );
        assert_eq!(res.columns, vec!["name"]);
        assert_eq!(col0(&res), vec!["Alice"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase12_importing_with_correlated() {
        // test04: import an outer variable with a leading `WITH` and reference it
        // inside; the subquery returns one row per outer row.
        let (root, res) = run(
            "exec_p12_import",
            "MATCH (p:Person) CALL { WITH p RETURN p.age AS age } \
             RETURN p.name AS name, age ORDER BY age ASC",
        );
        assert_eq!(res.columns, vec!["name", "age"]);
        let rows: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("Bob".into(), "25".into()),
                ("Alice".into(), "30".into()),
                ("Carol".into(), "40".into()),
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase12_cardinality_multiplication() {
        // test06: a returning subquery multiplies cardinality (2 outer × 3 inner =
        // 6 rows). The inner does not import `x`, so it is invisible inside.
        let (root, res) = run(
            "exec_p12_card",
            "UNWIND [1, 2] AS x CALL { UNWIND [10, 20, 30] AS y RETURN y } \
             RETURN x, y ORDER BY x ASC, y ASC",
        );
        assert_eq!(res.columns, vec!["x", "y"]);
        let rows: Vec<(i64, i64)> = res
            .rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (Val::Int(a), Val::Int(b)) => (*a, *b),
                _ => panic!("expected ints"),
            })
            .collect();
        assert_eq!(
            rows,
            vec![(1, 10), (1, 20), (1, 30), (2, 10), (2, 20), (2, 30)]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase12_correlated_filter_drops_rows() {
        // test03/test05: a returning subquery that yields nothing for an outer row
        // drops that row entirely (no input passthrough). 'Zztop' matches no node.
        let (root, res) = run(
            "exec_p12_drop",
            "UNWIND ['Alice', 'Zztop'] AS nm \
             CALL { WITH nm MATCH (p:Person {name: nm}) RETURN p } \
             RETURN p.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase12_optional_match_in_subquery() {
        // test07: OPTIONAL MATCH inside the subquery keeps the row with a null when
        // nothing matches, so cardinality is preserved per outer row.
        let (root, res) = run(
            "exec_p12_optional",
            "UNWIND [25, 99] AS a \
             CALL { WITH a OPTIONAL MATCH (p:Person {age: a}) RETURN p } \
             RETURN a, p.name AS name ORDER BY a ASC",
        );
        let rows: Vec<(String, String)> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        assert_eq!(
            rows,
            vec![("25".into(), "Bob".into()), ("99".into(), "null".into())]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase12_aggregation_in_subquery() {
        // test04/test17: a correlated aggregation. For each threshold `a`, count the
        // Persons with age >= a (Bob 25, Alice 30, Carol 40).
        let (root, res) = run(
            "exec_p12_agg",
            "UNWIND [25, 30] AS a \
             CALL { WITH a MATCH (p:Person) WHERE p.age >= a RETURN count(p) AS c } \
             RETURN a, c ORDER BY a ASC",
        );
        let rows: Vec<(i64, i64)> = res
            .rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (Val::Int(a), Val::Int(c)) => (*a, *c),
                _ => panic!("expected ints"),
            })
            .collect();
        assert_eq!(rows, vec![(25, 3), (30, 2)]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase12_nested_call_subquery() {
        // test14: a CALL {} directly inside another CALL {}.
        let (root, res) = run(
            "exec_p12_nested",
            "CALL { CALL { MATCH (p:Person {name: 'Bob'}) RETURN p } RETURN p } \
             RETURN p.name AS name",
        );
        assert_eq!(col0(&res), vec!["Bob"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase12_union_in_subquery() {
        // A UNION inside the subquery, each branch importing `p`. DISTINCT union of
        // Alice's name and city.
        let (root, res) = run(
            "exec_p12_union",
            "MATCH (p:Person {name: 'Alice'}) \
             CALL { WITH p RETURN p.name AS x UNION WITH p RETURN p.city AS x } \
             RETURN x ORDER BY x ASC",
        );
        assert_eq!(col0(&res), vec!["Alice", "London"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase12_unit_subquery_passthrough() {
        // A unit (RETURN-less) subquery preserves the outer cardinality: one outer
        // row stays one row even though the inner MATCH finds three Persons.
        let (root, res) = run(
            "exec_p12_unit",
            "WITH 1 AS a CALL { MATCH (p:Person) } RETURN a",
        );
        assert_eq!(res.columns, vec!["a"]);
        assert_eq!(res.rows.len(), 1);
        assert!(matches!(res.rows[0][0], Val::Int(1)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase12_non_imported_outer_var_is_invisible() {
        // test01: without a leading `WITH`, an outer variable is not visible inside.
        let err = run_err(
            "exec_p12_invisible",
            "WITH 1 AS a CALL { RETURN a AS b } RETURN b",
        );
        assert!(err.contains("'a' is not in scope"), "{err}");
    }

    #[test]
    fn phase12_import_undefined_errors() {
        // test01: importing a variable that does not exist outside is an error.
        let err = run_err(
            "exec_p12_undef",
            "CALL { WITH a RETURN 1 AS one } RETURN one",
        );
        assert!(err.contains("'a' is not in scope"), "{err}");
    }

    #[test]
    fn phase12_outer_scope_collision_errors() {
        // test01: a subquery may not return a name already bound in the outer scope.
        let err = run_err(
            "exec_p12_collision",
            "MATCH (p:Person {name: 'Alice'}) CALL { RETURN 1 AS p } RETURN p",
        );
        assert!(err.contains("already declared in outer scope"), "{err}");
    }

    // ── Phase 13: algo.* graph-algorithm procedures ──────────────────────────
    //
    // Tests run over the `write_basic` fixture (dense ids in brackets):
    //   [0]Alice [1]Bob [2]Carol :Person ; [3]Acme [4]Globex :Company
    //   Alice-KNOWS->Bob, Bob-KNOWS->Carol, Alice-KNOWS->Carol,
    //   Alice-WORKS_AT->Acme, Carol-WORKS_AT->Globex
    // FalkorDB's own algo tests use CREATE setups we can't replay, so the vectors
    // are adapted to this fixture; assertions follow the FalkorDB tests' style
    // (orderings, exact-0 for sinks, sum≈1) rather than exact LAGraph float values.

    #[test]
    fn phase13_bfs_all_reltypes_and_restricted() {
        // BFS from Alice over all relationship types reaches everyone but Alice.
        let (root, res) = run(
            "exec_p13_bfs_all",
            "MATCH (a:Person {name: 'Alice'}) \
             CALL algo.BFS(a, -1, NULL) YIELD nodes \
             UNWIND nodes AS n RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol", "Globex"]);

        // Restricted to KNOWS, only the two reachable Persons appear.
        let (_, res) = run(
            "exec_p13_bfs_knows",
            "MATCH (a:Person {name: 'Alice'}) \
             CALL algo.BFS(a, -1, 'KNOWS') YIELD nodes \
             UNWIND nodes AS n RETURN n.name AS name",
        );
        assert_eq!(col0(&res), vec!["Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase13_bfs_max_depth_and_edges() {
        // Depth 1 = direct neighbours only; edges parallel the nodes (each is the
        // tree edge that first reached the node).
        let (root, res) = run(
            "exec_p13_bfs_depth",
            "MATCH (a:Person {name: 'Alice'}) \
             CALL algo.BFS(a, 1, 'KNOWS') YIELD nodes, edges \
             RETURN [n IN nodes | n.name] AS ns, [e IN edges | type(e)] AS ts, size(edges) AS k",
        );
        assert_eq!(res.rows.len(), 1);
        // nodes are Bob and Carol (Alice's direct KNOWS neighbours)
        let Val::List(ns) = &res.rows[0][0] else {
            panic!("expected list");
        };
        let mut names: Vec<String> = ns.iter().map(|v| v.to_display()).collect();
        names.sort();
        assert_eq!(names, vec!["Bob", "Carol"]);
        // every tree edge is a KNOWS edge, one per reached node
        let Val::List(ts) = &res.rows[0][1] else {
            panic!("expected list");
        };
        assert!(ts.iter().all(|t| t.to_display() == "KNOWS"));
        assert!(matches!(res.rows[0][2], Val::Int(2)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase13_bfs_no_results_and_null_source() {
        // A sink node (Globex) reaches nothing → the CALL produces zero rows.
        let (root, res) = run(
            "exec_p13_bfs_sink",
            "MATCH (g:Company {name: 'Globex'}) \
             CALL algo.BFS(g, -1, NULL) YIELD nodes RETURN nodes",
        );
        assert_eq!(res.rows.len(), 0);

        // A missing relationship type → zero rows.
        let (_, res) = run(
            "exec_p13_bfs_missing_rel",
            "MATCH (a:Person {name: 'Alice'}) \
             CALL algo.BFS(a, -1, 'NOPE') YIELD nodes RETURN nodes",
        );
        assert_eq!(res.rows.len(), 0);

        // A NULL source (OPTIONAL MATCH with no hit) → zero rows, no error.
        let (_, res) = run(
            "exec_p13_bfs_null",
            "OPTIONAL MATCH (n:NoSuchLabel) \
             CALL algo.BFS(n, -1, NULL) YIELD nodes RETURN nodes",
        );
        assert_eq!(res.rows.len(), 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase13_wcc_components() {
        // All edges undirected → the whole graph is one component of 5.
        let (root, res) = run(
            "exec_p13_wcc_all",
            "CALL algo.WCC() YIELD node, componentId RETURN node.name AS name, componentId",
        );
        assert_eq!(res.rows.len(), 5);
        let cids: std::collections::HashSet<String> =
            res.rows.iter().map(|r| r[1].to_display()).collect();
        assert_eq!(cids.len(), 1, "one component over the full graph");

        // Restricted to KNOWS: the three Persons form one component; the two
        // Companies (no KNOWS edges) are isolated singletons → 3 components.
        let (_, res) = run(
            "exec_p13_wcc_knows",
            "CALL algo.WCC({relationshipTypes: ['KNOWS']}) YIELD node, componentId \
             RETURN node.name AS name, componentId",
        );
        assert_eq!(res.rows.len(), 5);
        let mut groups: std::collections::HashMap<String, Vec<String>> = Default::default();
        for r in &res.rows {
            groups
                .entry(r[1].to_display())
                .or_default()
                .push(r[0].to_display());
        }
        assert_eq!(groups.len(), 3, "Persons + 2 isolated Companies");
        // the Persons share one component
        let person_comp: Vec<_> = res
            .rows
            .iter()
            .filter(|r| ["Alice", "Bob", "Carol"].contains(&r[0].to_display().as_str()))
            .map(|r| r[1].to_display())
            .collect();
        assert!(
            person_comp.windows(2).all(|w| w[0] == w[1]),
            "Persons in one component"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase13_wcc_node_label_filter() {
        // nodeLabels=['Person'] selects only the three Persons, connected via KNOWS.
        let (root, res) = run(
            "exec_p13_wcc_person",
            "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node RETURN node.name AS name",
        );
        assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase13_pagerank_scores() {
        // Over the whole graph: 5 rows, scores positive and summing to ~1
        // (FalkorDB test_pagerank asserts exactly these structural properties).
        let (root, res) = run(
            "exec_p13_pagerank",
            "CALL algo.pageRank(NULL, NULL) YIELD node, score \
             RETURN node.name AS name, score",
        );
        assert_eq!(res.rows.len(), 5);
        let mut sum = 0.0;
        for r in &res.rows {
            let Val::Float(s) = r[1] else {
                panic!("score should be a float");
            };
            assert!(s > 0.0, "scores are positive");
            sum += s;
        }
        assert!((sum - 1.0).abs() < 1e-4, "scores sum to ~1, got {sum}");

        // Over the Person/KNOWS subgraph (Alice->Bob, Alice->Carol, Bob->Carol),
        // Carol — the sink all rank flows toward — scores highest of the three.
        let (_, res) = run(
            "exec_p13_pagerank_knows",
            "CALL algo.pageRank('Person', 'KNOWS') YIELD node, score \
             RETURN node.name AS name, score",
        );
        assert_eq!(res.rows.len(), 3);
        let scores: std::collections::HashMap<String, f64> = res
            .rows
            .iter()
            .map(|r| {
                let Val::Float(s) = r[1] else {
                    panic!("score should be a float");
                };
                (r[0].to_display(), s)
            })
            .collect();
        assert!(scores["Carol"] > scores["Alice"], "Carol > Alice");
        assert!(scores["Carol"] > scores["Bob"], "Carol > Bob");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase13_harmonic_centrality() {
        // Over the Person/KNOWS subgraph (Alice->Bob, Alice->Carol, Bob->Carol):
        //   Alice reaches Bob & Carol at d=1 → score 2.0, reachable 2
        //   Bob reaches Carol at d=1         → score 1.0, reachable 1
        //   Carol is a sink                  → score 0.0, reachable 0
        let (root, res) = run(
            "exec_p13_harmonic",
            "CALL algo.HarmonicCentrality({nodeLabels: ['Person'], relationshipTypes: ['KNOWS']}) \
             YIELD node, score, reachable \
             RETURN node.name AS name, score, reachable ORDER BY score DESC",
        );
        assert_eq!(res.rows.len(), 3);
        assert_eq!(res.rows[0][0].to_display(), "Alice");
        assert_float(&res.rows[0][1], 2.0);
        assert!(matches!(res.rows[0][2], Val::Int(2)));
        assert_eq!(res.rows[1][0].to_display(), "Bob");
        assert_float(&res.rows[1][1], 1.0);
        assert!(matches!(res.rows[1][2], Val::Int(1)));
        assert_eq!(res.rows[2][0].to_display(), "Carol");
        assert_float(&res.rows[2][1], 0.0);
        assert!(matches!(res.rows[2][2], Val::Int(0)));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase13_betweenness() {
        // Over the whole graph, only Carol lies on a shortest path between other
        // nodes (Alice->Globex and Bob->Globex both pass through Carol); every other
        // node has betweenness exactly 0.
        let (root, res) = run(
            "exec_p13_betweenness",
            "CALL algo.betweenness() YIELD node, score RETURN node.name AS name, score",
        );
        assert_eq!(res.rows.len(), 5);
        let scores: std::collections::HashMap<String, f64> = res
            .rows
            .iter()
            .map(|r| {
                let Val::Float(s) = r[1] else {
                    panic!("score should be a float");
                };
                (r[0].to_display(), s)
            })
            .collect();
        assert!(scores["Carol"] > 0.0, "Carol is on shortest paths");
        for name in ["Alice", "Bob", "Acme", "Globex"] {
            assert_eq!(scores[name], 0.0, "{name} is on no shortest path");
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase13_label_propagation() {
        // Over the KNOWS subgraph the three Persons form one community; the two
        // Companies (no KNOWS edges) stay in their own singleton communities.
        let (root, res) = run(
            "exec_p13_labelprop",
            "CALL algo.labelPropagation({relationshipTypes: ['KNOWS']}) \
             YIELD node, communityId RETURN node.name AS name, communityId",
        );
        assert_eq!(res.rows.len(), 5);
        let comm: std::collections::HashMap<String, String> = res
            .rows
            .iter()
            .map(|r| (r[0].to_display(), r[1].to_display()))
            .collect();
        assert_eq!(comm["Alice"], comm["Bob"]);
        assert_eq!(comm["Bob"], comm["Carol"]);
        assert_ne!(comm["Alice"], comm["Acme"]);
        assert_ne!(comm["Alice"], comm["Globex"]);
        assert_ne!(comm["Acme"], comm["Globex"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn phase13_algo_validation_errors() {
        // Unknown YIELD field.
        let e = run_err(
            "exec_p13_err_yield",
            "CALL algo.WCC() YIELD node, bogus RETURN node",
        );
        assert!(e.contains("does not yield 'bogus'"), "{e}");

        // Non-array nodeLabels.
        let e = run_err(
            "exec_p13_err_labels",
            "CALL algo.WCC({nodeLabels: 'Person'}) YIELD node RETURN node",
        );
        assert!(e.contains("should be an array of strings"), "{e}");

        // Unknown config key.
        let e = run_err(
            "exec_p13_err_key",
            "CALL algo.WCC({bogus: 1}) YIELD node RETURN node",
        );
        assert!(e.contains("unknown key"), "{e}");

        // Non-map config argument.
        let e = run_err(
            "exec_p13_err_cfg",
            "CALL algo.WCC('invalid') YIELD node RETURN node",
        );
        assert!(e.contains("invalid WCC configuration"), "{e}");

        // pageRank requires exactly two scalar arguments.
        let e = run_err(
            "exec_p13_err_pr_arity",
            "CALL algo.pageRank('Person') YIELD node RETURN node",
        );
        assert!(e.contains("expects 2 arguments"), "{e}");

        // betweenness sampling-size validation.
        let e = run_err(
            "exec_p13_err_sampling",
            "CALL algo.betweenness({samplingSize: -1}) YIELD node RETURN node",
        );
        assert!(e.contains("samplingSize"), "{e}");
    }

    // ── Phase 10 — temporal value types (date/localtime/localdatetime/duration) ──
    // Vectors ported from FalkorDB `tests/flow/test_temporal.py`. The inline `run`
    // harness has no params, so the `$map`/`$str` inputs become literal map/string
    // expressions in the query text.

    /// `localtime` from a map and from a string, its `.hour/.minute/.second`
    /// components, and `toString` (sub-second is dropped → `HH:MM:SS`).
    #[test]
    fn phase10_localtime_construction_and_components() {
        let (root, res) = run(
            "exec_p10_lt",
            "WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d \
             RETURN toString(d) AS s, d.hour AS h, d.minute AS mi, d.second AS se, typeOf(d) AS t",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "'12:31:14'");
        assert!(matches!(r[1], Val::Int(12)));
        assert!(matches!(r[2], Val::Int(31)));
        assert!(matches!(r[3], Val::Int(14)));
        assert_eq!(render(&r[4]), "'Time'");

        // String forms (compact + colon) and the trailing-fraction drop.
        let (root2, res2) = run(
            "exec_p10_lt_str",
            "RETURN toString(localtime('21')) AS a, toString(localtime('2140')) AS b, \
                    toString(localtime('214032')) AS c, toString(localtime('21:40:32.143')) AS e",
        );
        let r = &res2.rows[0];
        assert_eq!(render(&r[0]), "'21:00:00'");
        assert_eq!(render(&r[1]), "'21:40:00'");
        assert_eq!(render(&r[2]), "'21:40:32'");
        assert_eq!(render(&r[3]), "'21:40:32'");

        // toString round-trips back to an equal value.
        let (root3, res3) = run(
            "exec_p10_lt_rt",
            "WITH localtime({hour: 12, minute: 31, second: 14}) AS d \
             RETURN localtime(toString(d)) = d AS b",
        );
        assert!(matches!(res3.rows[0][0], Val::Bool(true)));
        for p in [root, root2, root3] {
            let _ = std::fs::remove_dir_all(&p);
        }
    }

    /// `date` from components (y/m/d, ISO week, quarter) and strings, its many
    /// components, and `toString` (`YYYY-MM-DD`).
    #[test]
    fn phase10_date_construction_and_components() {
        // Component-map and string constructions agree on the rendered date.
        let (root, res) = run(
            "exec_p10_date_build",
            "RETURN toString(date({year:1984})) AS a, \
                    toString(date({year:1984, month:10})) AS b, \
                    toString(date({year:1984, week:10})) AS c, \
                    toString(date({year:1984, month:10, day:11})) AS d, \
                    toString(date({year:1984, week:10, dayOfWeek:3})) AS e, \
                    toString(date({year:1984, quarter:3, dayOfQuarter:45})) AS f, \
                    toString(date({year:1984, quarter:3})) AS g, \
                    toString(date('2015202')) AS h, toString(date('2015-W30-2')) AS i, \
                    toString(date('20150721')) AS j",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "'1984-01-01'");
        assert_eq!(render(&r[1]), "'1984-10-01'");
        assert_eq!(render(&r[2]), "'1984-03-05'");
        assert_eq!(render(&r[3]), "'1984-10-11'");
        assert_eq!(render(&r[4]), "'1984-03-07'");
        assert_eq!(render(&r[5]), "'1984-08-14'");
        assert_eq!(render(&r[6]), "'1984-07-01'");
        assert_eq!(render(&r[7]), "'2015-07-21'"); // ordinal day 202
        assert_eq!(render(&r[8]), "'2015-07-21'"); // ISO week 30, Tue
        assert_eq!(render(&r[9]), "'2015-07-21'");

        // Components of date(1984-10-21) — incl. FalkorDB's quirky dayOfQuarter (23).
        let (root2, res2) = run(
            "exec_p10_date_comp",
            "WITH date({year: 1984, month:10, day:21}) AS d \
             RETURN d.year, d.quarter, d.month, d.week, d.day, d.dayOfWeek, \
                    d.dayOfQuarter, d.ordinalDay, typeOf(d)",
        );
        let r = &res2.rows[0];
        let ints: Vec<i64> = (0..8)
            .map(|i| match r[i] {
                Val::Int(v) => v,
                ref o => panic!("col {i}: expected int, got {o:?}"),
            })
            .collect();
        assert_eq!(ints, vec![1984, 4, 10, 42, 21, 0, 23, 295]);
        assert_eq!(render(&r[8]), "'Date'");
        for p in [root, root2] {
            let _ = std::fs::remove_dir_all(&p);
        }
    }

    /// `localdatetime` from components/strings, its `toString` (`…T…`), the
    /// ISO-week construction edge cases, and component access.
    #[test]
    fn phase10_localdatetime_construction_and_components() {
        let (root, res) = run(
            "exec_p10_ldt",
            "RETURN toString(localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond:645876123})) AS a, \
                    toString(localdatetime({year:1984, month:10, day:11, hour:12})) AS b, \
                    toString(localdatetime({year:1984})) AS c, \
                    toString(localdatetime({year:1918, week:1})) AS d, \
                    toString(localdatetime({year:1918, week:53})) AS e, \
                    toString(localdatetime('2025-02-18T12:34:56')) AS f, \
                    toString(localdatetime('20250218T123456')) AS g",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "'1984-10-11T12:31:14'");
        assert_eq!(render(&r[1]), "'1984-10-11T12:00:00'");
        assert_eq!(render(&r[2]), "'1984-01-01T00:00:00'");
        assert_eq!(render(&r[3]), "'1917-12-31T00:00:00'"); // ISO week 1 of 1918
        assert_eq!(render(&r[4]), "'1918-12-30T00:00:00'"); // lenient week 53
        assert_eq!(render(&r[5]), "'2025-02-18T12:34:56'");
        assert_eq!(render(&r[6]), "'2025-02-18T12:34:56'");

        // Components incl. clock parts + round-trip via toString.
        let (root2, res2) = run(
            "exec_p10_ldt_comp",
            "WITH localdatetime({year:1984, month:10, day:21, hour:10, minute:31, second:46}) AS d \
             RETURN d.year, d.quarter, d.month, d.week, d.day, d.ordinalDay, \
                    d.hour, d.minute, d.second, \
                    localdatetime(toString(d)) = d AS rt, typeOf(d) AS t",
        );
        let r = &res2.rows[0];
        let ints: Vec<i64> = (0..9)
            .map(|i| match r[i] {
                Val::Int(v) => v,
                ref o => panic!("col {i}: expected int, got {o:?}"),
            })
            .collect();
        assert_eq!(ints, vec![1984, 4, 10, 42, 21, 295, 10, 31, 46]);
        assert!(matches!(r[9], Val::Bool(true)), "toString round-trip");
        assert_eq!(render(&r[10]), "'Datetime'");
        for p in [root, root2] {
            let _ = std::fs::remove_dir_all(&p);
        }
    }

    /// `duration` from a map and ISO-8601 string, its components (weeks fold into
    /// days), and `toString`.
    #[test]
    fn phase10_duration_construction_and_components() {
        // Components: weeks fold into days (1 week + 4 days → 11 days, 0 weeks).
        let (root, res) = run(
            "exec_p10_dur_comp",
            "WITH duration({years:2, months:3, weeks:1, days:4, hours:5, minutes:22, seconds:7}) AS d \
             RETURN d.years, d.months, d.weeks, d.days, d.hours, d.minutes, d.seconds, typeOf(d) AS t",
        );
        let r = &res.rows[0];
        // Duration components are doubles (FalkorDB `SI_DoubleVal`) → render as ints.
        let got: Vec<String> = (0..7).map(|i| render(&r[i])).collect();
        assert_eq!(got, vec!["2", "3", "0", "11", "5", "22", "7"]);
        assert_eq!(render(&r[7]), "'Duration'");

        // String form + toString round-trips ('P1M' stays 'P1M').
        let (root2, res2) = run(
            "exec_p10_dur_str",
            "RETURN toString(duration('P1M')) AS a, \
                    toString(duration('P1Y2M3DT4H5M6S')) AS b, \
                    toString(duration({years:2, months:3, days:11, hours:5, minutes:22, seconds:7})) AS c",
        );
        let r = &res2.rows[0];
        assert_eq!(render(&r[0]), "'P1M'");
        assert_eq!(render(&r[1]), "'P1Y2M3DT4H5M6S'");
        assert_eq!(render(&r[2]), "'P2Y3M11DT5H22M7S'");
        for p in [root, root2] {
            let _ = std::fs::remove_dir_all(&p);
        }
    }

    /// Comparison operators over each temporal type (test_temporal.py *_compare).
    #[test]
    fn phase10_temporal_comparison() {
        let (root, res) = run(
            "exec_p10_cmp",
            "WITH date({year:1980, month:12, day:24}) AS d1, date({year:1984, month:10, day:11}) AS d2, \
                  localtime({hour:10, minute:35}) AS t1, localtime({hour:12, minute:31, second:14}) AS t2, \
                  duration({years:1, months:11}) AS u1, duration({years:1, months:10}) AS u2 \
             RETURN d1 < d2, d1 = d2, t1 < t2, t1 >= t2, u1 > u2, u1 = u2, \
                    d1 = d1, t2 = t2",
        );
        let r = &res.rows[0];
        let b: Vec<bool> = (0..8)
            .map(|i| match r[i] {
                Val::Bool(v) => v,
                ref o => panic!("col {i}: {o:?}"),
            })
            .collect();
        // d1<d2 T, d1=d2 F, t1<t2 T, t1>=t2 F, u1>u2 T, u1=u2 F, d1=d1 T, t2=t2 T
        assert_eq!(b, vec![true, false, true, false, true, false, true, true]);

        // Cross-type comparison (date vs duration) is `null`, not an error.
        let (root2, res2) = run(
            "exec_p10_cmp_x",
            "WITH date({year:2000, month:1, day:1}) AS d, duration({days:1}) AS u \
             RETURN d < u AS lt, d = u AS eq",
        );
        let r = &res2.rows[0];
        assert!(matches!(r[0], Val::Null), "date<duration → null");
        assert!(matches!(r[1], Val::Bool(false)), "date=duration → false");
        for p in [root, root2] {
            let _ = std::fs::remove_dir_all(&p);
        }
    }

    /// Temporal ± duration and duration ± duration (test_temporal.py
    /// test_duration_add + test_month_end_duration_arithmetic).
    #[test]
    fn phase10_temporal_arithmetic() {
        let (root, res) = run(
            "exec_p10_arith",
            "WITH duration({years:1, months:1, weeks:1, days:1, hours:1, minutes:32, seconds:10}) AS a, \
                  duration({years:2, months:2, weeks:2, days:2, hours:2, minutes:34, seconds:12}) AS b \
             RETURN toString(a + b) AS sum, toString(b - a) AS diff",
        );
        let r = &res.rows[0];
        assert_eq!(render(&r[0]), "'P3Y3M24DT4H6M22S'"); // 66 min normalises to 4h6m
        assert_eq!(render(&r[1]), "'P1Y1M8DT1H2M2S'");

        let (root2, res2) = run(
            "exec_p10_arith2",
            "RETURN toString(date({year:1984, month:10, day:21}) + duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1})) AS d, \
                    toString(duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1}) + date({year:1984, month:10, day:21})) AS d2, \
                    toString(localtime({hour:2, minute:34, second:32}) + duration({years:1, months:1, days:1, hours:1, minutes:35, seconds:35})) AS t, \
                    toString(localtime({hour:10, minute:30, second:10}) - duration({hours:2, minutes:40, seconds:30})) AS t2, \
                    toString(localdatetime({year:1984, month:10, day:21, hour:5, minute:30, second:10}) + duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1})) AS dt, \
                    toString(localdatetime({year:1984, month:10, day:21, hour:5, minute:30, second:10}) - duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1})) AS dt2",
        );
        let r = &res2.rows[0];
        assert_eq!(render(&r[0]), "'1985-11-22'"); // date + dur (clock parts ignored)
        assert_eq!(render(&r[1]), "'1985-11-22'"); // commutative
        assert_eq!(render(&r[2]), "'04:10:07'"); // time + dur (calendar parts ignored)
        assert_eq!(render(&r[3]), "'07:49:40'"); // time - dur
        assert_eq!(render(&r[4]), "'1985-11-22T06:31:11'");
        assert_eq!(render(&r[5]), "'1983-09-20T04:29:09'");

        // Month-end overflow normalises forward (Jan 31 + 1mo → Mar 02).
        let (root3, res3) = run(
            "exec_p10_arith_me",
            "RETURN toString(date('2024-01-31') + duration('P1M')) AS d, \
                    toString(localdatetime('2024-01-31T00:00:00') + duration('P1M')) AS l",
        );
        let r = &res3.rows[0];
        assert_eq!(render(&r[0]), "'2024-03-02'");
        assert_eq!(render(&r[1]), "'2024-03-02T00:00:00'");
        for p in [root, root2, root3] {
            let _ = std::fs::remove_dir_all(&p);
        }
    }

    /// Unsupported temporal arithmetic errors (duration − temporal is invalid),
    /// and `null`/unknown-component handling.
    #[test]
    fn phase10_temporal_errors_and_null() {
        for (tag, q) in [
            (
                "exec_p10_e1",
                "RETURN duration({days:1}) - date({year:1984, month:10, day:21})",
            ),
            (
                "exec_p10_e2",
                "RETURN duration({hours:2}) - localtime({hour:10, minute:30})",
            ),
            (
                "exec_p10_e3",
                "RETURN duration({days:1}) - localdatetime({year:1984})",
            ),
        ] {
            let e = run_err(tag, q);
            assert!(e.contains("cannot be subtracted"), "query `{q}` → `{e}`");
        }

        // Unknown component on a temporal is an error (unlike Point/Map → NULL).
        let e = run_err(
            "exec_p10_e_comp",
            "WITH date({year:2000, month:1, day:1}) AS d RETURN d.bogus",
        );
        assert!(e.contains("unknown date component"), "{e}");

        // NULL / bad-string inputs propagate to NULL.
        let (root, res) = run(
            "exec_p10_null",
            "RETURN date(null) AS a, localtime('nonsense') AS b, duration('not-a-duration') AS c",
        );
        let r = &res.rows[0];
        assert!(matches!(r[0], Val::Null));
        assert!(matches!(r[1], Val::Null));
        assert!(matches!(r[2], Val::Null));
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── Phase 1b — non-deterministic builtins (rand / randomUUID / timestamp) ──
    #[test]
    fn phase1b_nondeterministic_functions() {
        let (root, res) = run(
            "exec_p1b_fns",
            "RETURN rand() AS r, randomUUID() AS u, timestamp() AS t",
        );
        let r = &res.rows[0];
        match r[0] {
            Val::Float(x) => assert!((0.0..1.0).contains(&x), "rand() in [0,1): {x}"),
            ref o => panic!("rand() → {o:?}"),
        }
        match &r[1] {
            // RFC-4122 v4: 36 chars, 4 hyphens, version nibble '4'.
            Val::Str(s) => {
                assert_eq!(s.len(), 36, "uuid {s}");
                assert_eq!(s.matches('-').count(), 4, "uuid {s}");
                assert_eq!(s.as_bytes()[14], b'4', "v4 version nibble: {s}");
            }
            o => panic!("randomUUID() → {o:?}"),
        }
        match r[2] {
            // Milliseconds since the epoch — well past 2020 (1.6e12 ms).
            Val::Int(t) => assert!(t > 1_600_000_000_000, "timestamp() ms: {t}"),
            ref o => panic!("timestamp() → {o:?}"),
        }

        // Two randomUUID() calls in one row are distinct.
        let (root2, res2) = run(
            "exec_p1b_uuid2",
            "RETURN randomUUID() AS a, randomUUID() AS b",
        );
        let r = &res2.rows[0];
        assert_ne!(render(&r[0]), render(&r[1]), "two UUIDs differ");
        for p in [root, root2] {
            let _ = std::fs::remove_dir_all(&p);
        }
    }

    // ── relationship-type scan: identical results with the posting on vs off ───

    /// Run `q` over the sparse-reltype fixture and return the sorted display rows.
    /// `postings` toggles the endpoint postings: on ⇒ the planner drives typed
    /// first hops from the rel-type posting; off ⇒ the identical graph with no
    /// postings, so every query falls back to the label scan.
    fn rel_rows(tag: &str, q: &str, postings: bool) -> Vec<String> {
        let (root, graph) = if postings {
            testgen::write_rel_sparse(tag)
        } else {
            testgen::write_rel_sparse_no_postings(tag)
        };
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let engine = Engine::new(&gen, &cache);
        let res = parser::parse(q)
            .map_err(|e| e.to_string())
            .and_then(|ast| engine.run(&ast).map_err(|e| e.to_string()))
            .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"));
        let mut rows: Vec<String> = res
            .rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|v| v.to_display())
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect();
        rows.sort();
        let _ = std::fs::remove_dir_all(&root);
        rows
    }

    #[test]
    fn rel_type_scan_matches_label_scan_results() {
        // Every shape the rel-type scan can fire on must return byte-identical rows
        // to the label-scan plan over the same graph. The fixture: 6 :N nodes,
        // T-edges a->b, b->c (sources {a,b}, targets {b,c}), U-edge a->d.
        let cases = [
            // outgoing 1-hop
            "MATCH (a:N)-[:T]->(b) RETURN a.name AS x, b.name AS y",
            // outgoing 1-hop, unlabelled anchor (base AllNodes)
            "MATCH (a)-[:T]->(b) RETURN a.name AS x, b.name AS y",
            // incoming
            "MATCH (a:N)<-[:T]-(b) RETURN a.name AS x, b.name AS y",
            // undirected
            "MATCH (a:N)-[:T]-(b) RETURN a.name AS x, b.name AS y",
            // 2-hop
            "MATCH (a:N)-[:T]->(b)-[:T]->(c) RETURN c.name AS y",
            // with LIMIT (early-exit path)
            "MATCH (a:N)-[:T]->(b) RETURN b.name AS y LIMIT 1",
            // multi-type union
            "MATCH (a:N)-[:T|U]->(b) RETURN a.name AS x, b.name AS y",
            // count (uncapped, parallel-eligible)
            "MATCH (a:N)-[:T]->(b) RETURN count(*) AS n",
            // OPTIONAL with an unbound anchor: edgeless nodes must not change the
            // outcome — both plans yield the same matched set (and the same
            // null-row behaviour, driven by whether anything matched at all).
            "OPTIONAL MATCH (a:N)-[:T]->(b) RETURN a.name AS x, b.name AS y",
        ];
        for (i, q) in cases.iter().enumerate() {
            let on = rel_rows(&format!("exec_relscan_on_{i}"), q, true);
            let off = rel_rows(&format!("exec_relscan_off_{i}"), q, false);
            assert_eq!(on, off, "rel-scan vs label-scan mismatch for: {q}");
        }
    }

    #[test]
    fn rel_type_scan_concrete_rows() {
        // Pin the actual rows (not just on==off), so a bug that breaks *both*
        // plans identically can't hide. T-edges: a->b, b->c.
        let rows = rel_rows(
            "exec_relscan_concrete",
            "MATCH (a:N)-[:T]->(b) RETURN a.name, b.name",
            true,
        );
        assert_eq!(rows, vec!["a|b".to_string(), "b|c".to_string()]);
    }
}
