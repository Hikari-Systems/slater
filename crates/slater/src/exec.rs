// SPDX-License-Identifier: Apache-2.0
//! Volcano-style executor: an AST [`Query`] → result rows, pulled from the
//! immutable [`Generation`] through the decompressed-block [`BlockCache`].
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
//! optional wall-clock deadline.
//
// The tokio Bolt listener that drives this (decoding RUN/PULL and PackStream-
// encoding the rows) is the next M4 increment; allow dead_code until it lands.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Instant;

use anyhow::{bail, Result};

use crate::cache::{BlockCache, FileKind, VectorIndexCache};
use crate::generation::Generation;
use crate::parser::ast::*;
use crate::plan::{choose_node_scan, is_id_anchored, NodeScan};
use crate::vector;
use graph_format::ids::{NodeId, Value};
use graph_format::manifest::AnnMode;
use graph_format::pq::AdcTable;
use graph_format::vamana::{self, beam_search};
use graph_format::vectors::{self, VectorEntry};
use graph_format::{columns, nodelabels, topology};

/// Unbounded variable-length expansion (`*` / `*n..`) is capped at this many hops,
/// so a runaway traversal on a densely connected graph cannot blow up. Explicit
/// upper bounds (`*1..3`) are honoured exactly; only the open-ended case is capped.
const MAX_VARLEN_HOPS: u32 = 15;

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

// ── Engine ─────────────────────────────────────────────────────────────────

/// Per-query execution context over one generation and its block cache.
pub struct Engine<'g> {
    gen: &'g Generation,
    cache: &'g BlockCache,
    /// The vector-index pool, needed only by the `AnnMode::Vamana` arm. The
    /// brute-force arm and all non-vector queries leave it `None`.
    vec_cache: Option<&'g VectorIndexCache>,
    params: HashMap<String, Val>,
    max_rows: usize,
    deadline: Option<Instant>,
    /// Beam-search list size `L` for the Vamana arm (config `vectorQuery.beamWidth`).
    beam_width: usize,
}

impl<'g> Engine<'g> {
    pub fn new(gen: &'g Generation, cache: &'g BlockCache) -> Self {
        Self {
            gen,
            cache,
            vec_cache: None,
            params: HashMap::new(),
            max_rows: usize::MAX,
            deadline: None,
            beam_width: 64,
        }
    }

    /// Supply the vector-index pool so `AnnMode::Vamana` indexes can be served.
    pub fn with_vector_cache(mut self, vec_cache: &'g VectorIndexCache, beam_width: usize) -> Self {
        self.vec_cache = Some(vec_cache);
        self.beam_width = beam_width.max(1);
        self
    }

    pub fn with_params(mut self, params: HashMap<String, Val>) -> Self {
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

    // ── Cached record reads (D18) ───────────────────────────────────────────

    fn node_props(&self, id: u64) -> Result<Vec<(u32, Value)>> {
        let rec = self.cache.record(
            self.gen.node_props().inner(),
            self.gen.uuid(),
            FileKind::NodeProps,
            id,
        )?;
        columns::decode_props(&rec)
    }

    fn edge_props(&self, id: u64) -> Result<Vec<(u32, Value)>> {
        let rec = self.cache.record(
            self.gen.edge_props().inner(),
            self.gen.uuid(),
            FileKind::EdgeProps,
            id,
        )?;
        columns::decode_props(&rec)
    }

    fn node_label_ids(&self, id: u64) -> Result<Vec<u32>> {
        let rec = self.cache.record(
            self.gen.node_labels().inner(),
            self.gen.uuid(),
            FileKind::NodeLabels,
            id,
        )?;
        nodelabels::decode_labels(&rec)
    }

    fn outgoing(&self, id: u64) -> Result<Vec<topology::Adj>> {
        let topo = self.gen.topology();
        let global = topo.outgoing_global(NodeId(id));
        let rec = self
            .cache
            .record(topo.inner(), self.gen.uuid(), FileKind::Topology, global)?;
        topology::decode_adj(&rec)
    }

    fn incoming(&self, id: u64) -> Result<Vec<topology::Adj>> {
        let topo = self.gen.topology();
        let global = topo.incoming_global(NodeId(id));
        let rec = self
            .cache
            .record(topo.inner(), self.gen.uuid(), FileKind::Topology, global)?;
        topology::decode_adj(&rec)
    }

    /// Read a vector index group `[first_record, first_record + count)` from
    /// `vectors.f32.blk` **through the block cache** (D18) — the brute-force KNN
    /// candidate set. Each record decodes to its dense node id + full-precision
    /// vector; the group is contiguous (D10), so this touches only that index's
    /// blocks and they stay warm for repeat queries.
    fn vector_group(&self, first_record: u64, count: u64) -> Result<Vec<VectorEntry>> {
        let reader = self.gen.vectors().inner();
        let mut out = Vec::with_capacity(count as usize);
        for global in first_record..first_record + count {
            let rec = self
                .cache
                .record(reader, self.gen.uuid(), FileKind::Vectors, global)?;
            out.push(vectors::decode_vector(&rec)?);
        }
        Ok(out)
    }

    /// A node's value for property `key`, or `Null` if absent. (An embedding
    /// routed out to the vector store reads as `Null` here — vector *values* are
    /// served by the M5 KNN/`similarity()` path, not by a column read.)
    fn node_prop(&self, id: u64, key: &str) -> Result<Val> {
        let Some(key_id) = self.gen.property_key_id(key) else {
            return Ok(Val::Null);
        };
        for (k, v) in self.node_props(id)? {
            if k == key_id {
                return Ok(Val::from_value(v));
            }
        }
        Ok(Val::Null)
    }

    fn edge_prop(&self, id: u64, key: &str) -> Result<Val> {
        let Some(key_id) = self.gen.property_key_id(key) else {
            return Ok(Val::Null);
        };
        for (k, v) in self.edge_props(id)? {
            if k == key_id {
                return Ok(Val::from_value(v));
            }
        }
        Ok(Val::Null)
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
        let props = self
            .node_props(id)?
            .into_iter()
            .map(|(kid, v)| (self.key_name(kid), Val::from_value(v)))
            .collect();
        Ok((labels, props))
    }

    /// Resolve a relationship's type name and named properties — the material a
    /// Bolt `Relationship` structure carries.
    pub fn rel_record(&self, id: u64, reltype: u32) -> Result<(String, NamedProps)> {
        let type_name = self.gen.reltype_name(reltype).unwrap_or("").to_string();
        let props = self
            .edge_props(id)?
            .into_iter()
            .map(|(kid, v)| (self.key_name(kid), Val::from_value(v)))
            .collect();
        Ok((type_name, props))
    }

    fn key_name(&self, kid: u32) -> String {
        self.gen.property_key_name(kid).unwrap_or("?").to_string()
    }

    fn check_deadline(&self) -> Result<()> {
        if let Some(d) = self.deadline {
            if Instant::now() >= d {
                bail!("query exceeded its time limit");
            }
        }
        Ok(())
    }

    // ── Entry point ───────────────────────────────────────────────────────

    /// Execute a (possibly `UNION`ed) query.
    pub fn run(&self, q: &Query) -> Result<QueryResult> {
        let mut result = self.run_single(&q.head)?;
        for (union_all, part) in &q.tail {
            let next = self.run_single(part)?;
            if next.columns.len() != result.columns.len() {
                bail!("all parts of a UNION must return the same number of columns");
            }
            result.rows.extend(next.rows);
            if !union_all {
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
        self.run_single_seeded(sq, Table::singleton())
    }

    /// Run a single query part starting from `seed` instead of the empty singleton.
    /// A top-level query seeds the singleton; a `CALL { … }` subquery seeds the
    /// imported outer variables (one row) so the inner clauses can reference them.
    fn run_single_seeded(&self, sq: &SingleQuery, seed: Table) -> Result<QueryResult> {
        let mut table = seed;
        for clause in &sq.reading {
            match clause {
                Clause::Match(m) => table = self.apply_match(table, m)?,
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

    fn apply_match(&self, table: Table, m: &MatchClause) -> Result<Table> {
        // Variables this clause newly introduces, appended to the scope in order.
        let mut new_vars: Vec<String> = Vec::new();
        for p in &m.patterns {
            collect_pattern_vars(p, &table.cols, &mut new_vars);
        }
        let mut out_cols = table.cols.clone();
        out_cols.extend(new_vars.iter().cloned());

        let mut out_rows = Vec::new();
        for row in &table.rows {
            self.check_deadline()?;
            let mut seed: HashMap<String, Val> = HashMap::with_capacity(table.cols.len());
            for (c, v) in table.cols.iter().zip(row) {
                seed.insert(c.clone(), v.clone());
            }
            let mut matches: Vec<HashMap<String, Val>> = Vec::new();
            self.match_patterns(&m.patterns, 0, seed, m.where_.as_ref(), &mut matches)?;

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
        if !cc.args.is_empty() {
            bail!("{}() takes no arguments", cc.name);
        }
        let (out_names, proc_rows) = self.procedure_rows(&cc.name.to_ascii_lowercase())?;

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
    /// All counts come from resident indexes (`nodes_with_label`, `edges_with_reltype`)
    /// — no graph scan.
    fn meta_stats(&self) -> (Vec<String>, Vec<Vec<Val>>) {
        let m = self.gen.manifest();
        let labels: Vec<(String, Val)> = m
            .labels
            .iter()
            .map(|l| {
                let cnt = self
                    .gen
                    .label_id(l)
                    .map(|id| self.gen.nodes_with_label(id).len())
                    .unwrap_or(0);
                (l.clone(), Val::Int(cnt as i64))
            })
            .collect();
        let reltypes: Vec<(String, Val)> = m
            .reltypes
            .iter()
            .map(|t| {
                let cnt = self
                    .gen
                    .reltype_id(t)
                    .map(|id| self.gen.edges_with_reltype(id).len())
                    .unwrap_or(0);
                (t.clone(), Val::Int(cnt as i64))
            })
            .collect();
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
            Val::Int(m.edge_count as i64),
            Val::Int(m.node_count as i64),
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
            result.rows.extend(next.rows);
            if !*union_all {
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
        let desc = self
            .gen
            .manifest()
            .vector_indexes
            .iter()
            .find(|d| d.label == vc.label && d.property == vc.property)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no vector index on (:{} {{{}}}) — db.idx.vector.queryNodes needs one",
                    vc.label,
                    vc.property
                )
            })?;
        // Capture the small descriptor bits so the per-row loop does not hold the
        // manifest borrow (it also calls `self` methods to read candidates).
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

        // The brute-force arm reads the whole index group once, up front; the
        // Vamana arm navigates per query and reads nothing here.
        let entries = match mode {
            AnnMode::BruteForce => Some(self.vector_group(first_record, count)?),
            AnnMode::Vamana { .. } => None,
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
                AnnMode::BruteForce => {
                    vector::brute_force_knn(entries.as_ref().unwrap(), &query, k, metric)?
                }
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
    ) -> Result<()> {
        if idx == patterns.len() {
            if let Some(w) = where_ {
                if !truthy(&self.eval(w, &Scope::Map(&binding), None)?) {
                    return Ok(());
                }
            }
            out.push(binding);
            return Ok(());
        }
        let mut partial = Vec::new();
        self.match_single_pattern(&patterns[idx], &binding, where_, &mut partial)?;
        for b in partial {
            self.match_patterns(patterns, idx + 1, b, where_, out)?;
        }
        Ok(())
    }

    fn match_single_pattern(
        &self,
        pattern: &Pattern,
        binding: &HashMap<String, Val>,
        where_: Option<&Expr>,
        out: &mut Vec<HashMap<String, Val>>,
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
        let candidates: Vec<u64> = match start.var.as_deref().and_then(|v| binding.get(v)) {
            Some(Val::Node(id)) => vec![*id],
            Some(_) => return Ok(()), // bound to a non-node → cannot match
            None => {
                // The anchor is the only place the planner picks a scan strategy.
                let scan = choose_node_scan(self.gen, start, where_);
                self.scan_candidates(&scan)?
            }
        };
        for c in candidates {
            if !self.node_ok(c, start, binding)? {
                continue;
            }
            let mut b = binding.clone();
            if let Some(v) = &start.var {
                b.insert(v.clone(), Val::Node(c));
            }
            let mut path = Vec::new();
            self.expand_chain(pattern, 0, c, b, c, &mut path, out)?;
        }
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

    #[allow(clippy::too_many_arguments)] // recursive walk: scratch path buffer + start anchor
    fn expand_chain(
        &self,
        pattern: &Pattern,
        i: usize,
        cur: u64,
        binding: HashMap<String, Val>,
        start: u64,
        walk: &mut Vec<Hop>,
        out: &mut Vec<HashMap<String, Val>>,
    ) -> Result<()> {
        if i == pattern.rels.len() {
            let mut binding = binding;
            if let Some(pv) = &pattern.path_var {
                binding.insert(pv.clone(), make_path(start, walk));
            }
            out.push(binding);
            return Ok(());
        }
        self.check_deadline()?;
        let (rel, next) = &pattern.rels[i];
        match &rel.var_length {
            None => {
                for hop in self.expand_one_hop(cur, rel, &binding)? {
                    let nb = hop.neighbour;
                    if !self.node_ok(nb, next, &binding)? {
                        continue;
                    }
                    if let Some(v) = &next.var {
                        if let Some(existing) = binding.get(v) {
                            if existing.loose_eq(&Val::Node(nb)) != Some(true) {
                                continue;
                            }
                        }
                    }
                    let mut b = binding.clone();
                    if let Some(v) = &rel.var {
                        b.insert(v.clone(), hop.as_rel());
                    }
                    if let Some(v) = &next.var {
                        b.insert(v.clone(), Val::Node(nb));
                    }
                    walk.push(hop);
                    self.expand_chain(pattern, i + 1, nb, b, start, walk, out)?;
                    walk.pop();
                }
            }
            Some(vl) => {
                let (min, max) = varlen_bounds(vl);
                let mut paths: Vec<(Vec<Hop>, u64)> = Vec::new();
                let mut used = HashSet::new();
                let mut path = Vec::new();
                self.varlen(
                    cur,
                    rel,
                    (min, max),
                    &mut path,
                    &mut used,
                    &mut paths,
                    &binding,
                )?;
                for (hops, endnode) in paths {
                    if !self.node_ok(endnode, next, &binding)? {
                        continue;
                    }
                    if let Some(v) = &next.var {
                        if let Some(existing) = binding.get(v) {
                            if existing.loose_eq(&Val::Node(endnode)) != Some(true) {
                                continue;
                            }
                        }
                    }
                    let mut b = binding.clone();
                    if let Some(v) = &rel.var {
                        b.insert(v.clone(), Val::List(hops.iter().map(Hop::as_rel).collect()));
                    }
                    if let Some(v) = &next.var {
                        b.insert(v.clone(), Val::Node(endnode));
                    }
                    let n = hops.len();
                    walk.extend(hops);
                    self.expand_chain(pattern, i + 1, endnode, b, start, walk, out)?;
                    walk.truncate(walk.len() - n);
                }
            }
        }
        Ok(())
    }

    /// Depth-first variable-length expansion with relationship uniqueness (no edge
    /// reused within a path), emitting `(path_edges, end_node)` for every path
    /// whose length is in `[min, max]`.
    #[allow(clippy::too_many_arguments)] // recursive DFS: scratch buffers + scope
    fn varlen(
        &self,
        node: u64,
        rel: &RelPat,
        bounds: (u32, u32),
        path: &mut Vec<Hop>,
        used: &mut HashSet<u64>,
        out: &mut Vec<(Vec<Hop>, u64)>,
        binding: &HashMap<String, Val>,
    ) -> Result<()> {
        let (min, max) = bounds;
        if path.len() as u32 >= min {
            out.push((path.clone(), node));
        }
        if path.len() as u32 >= max {
            return Ok(());
        }
        self.check_deadline()?;
        for hop in self.expand_one_hop(node, rel, binding)? {
            if used.contains(&hop.edge) {
                continue;
            }
            let edge = hop.edge;
            let nb = hop.neighbour;
            used.insert(edge);
            path.push(hop);
            self.varlen(nb, rel, bounds, path, used, out, binding)?;
            path.pop();
            used.remove(&edge);
        }
        Ok(())
    }

    /// One traversal step from `node`: edges matching the pattern's direction,
    /// type alternation and relationship property predicates, each resolved to a
    /// [`Hop`] (edge, neighbour, type, and stored src→dst endpoints).
    fn expand_one_hop(
        &self,
        node: u64,
        rel: &RelPat,
        binding: &HashMap<String, Val>,
    ) -> Result<Vec<Hop>> {
        let type_ids: Option<Vec<u32>> = if rel.types.is_empty() {
            None
        } else {
            Some(
                rel.types
                    .iter()
                    .filter_map(|t| self.gen.reltype_id(t))
                    .collect(),
            )
        };
        // (adjacency list, `incoming`) — for an incoming edge the stored direction
        // is neighbour→node, so start/end are swapped relative to an outgoing one.
        let mut sources: Vec<(Vec<topology::Adj>, bool)> = Vec::new();
        match rel.dir {
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
                if let Some(ids) = &type_ids {
                    if !ids.contains(&a.reltype) {
                        continue;
                    }
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
        match scan {
            // Already bounds-checked + deduped by the planner; yield as-is. An
            // empty list is a seek that matched no node.
            NodeScan::IdSeek { ids } => Ok(ids.clone()),
            NodeScan::RangeEq { index, key } => self
                .gen
                .range_index(index)
                .expect("planner only picks open indexes")
                .lookup_eq(key),
            NodeScan::RangeRange { index, lo, hi } => self
                .gen
                .range_index(index)
                .expect("planner only picks open indexes")
                .lookup_range(
                    lo.as_ref().map(|(v, _)| v),
                    lo.as_ref().map(|(_, i)| *i).unwrap_or(true),
                    hi.as_ref().map(|(v, _)| v),
                    hi.as_ref().map(|(_, i)| *i).unwrap_or(true),
                ),
            NodeScan::LabelScan { label_id } => Ok(self.gen.nodes_with_label(*label_id).to_vec()),
            NodeScan::AllNodes => Ok((0..self.gen.node_count()).collect()),
        }
    }

    /// Whether node `id` satisfies a node pattern's labels and inline properties.
    /// Inline property values are evaluated against `binding` so a value bound
    /// earlier (e.g. by a `WITH`, or an earlier node/rel in the pattern) resolves,
    /// making `(b {id: x})` behave exactly like `(b) WHERE b.id = x`.
    fn node_ok(&self, id: u64, pat: &NodePat, binding: &HashMap<String, Val>) -> Result<bool> {
        if !pat.labels.is_empty() {
            let have = self.node_label_ids(id)?;
            for l in &pat.labels {
                match self.gen.label_id(l) {
                    Some(lid) if have.contains(&lid) => {}
                    _ => return Ok(false),
                }
            }
        }
        for (k, e) in &pat.props {
            let want = self.eval(e, &Scope::Map(binding), None)?;
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
            groups.entry(GroupKey(key)).or_default().push(ri);
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

        // Evaluate the argument over the group's rows, dropping nulls.
        let mut vals = Vec::new();
        for &i in indices {
            let scope = Scope::Row(&table.cols, &table.rows[i]);
            let v = self.eval(arg, &scope, None)?;
            if !matches!(v, Val::Null) {
                vals.push(v);
            }
        }
        if *distinct {
            dedup_vals(&mut vals);
        }

        Ok(match lname.as_str() {
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
            "percentilecont" => percentile_cont(&vals, percentile.unwrap())?,
            "percentiledisc" => percentile_disc(&vals, percentile.unwrap())?,
            other => bail!("unknown aggregate function '{other}'"),
        })
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
                arith(*op, a, b)
            }
            Expr::Compare(op, l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                Ok(compare(*op, &a, &b))
            }
            Expr::StringOp(op, l, r) => {
                let a = self.eval(l, scope, aggs)?;
                let b = self.eval(r, scope, aggs)?;
                string_op(*op, &a, &b)
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
                self.match_single_pattern(pattern, &seed, None, &mut bindings)?;
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
                self.match_patterns(patterns, 0, seed, predicate.as_deref(), &mut bindings)?;
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
        // its head. The BFS walks the syntactic start→end (using the pattern's
        // direction); for an incoming pattern `(b)<-[*]-(a)` the arrow tail is the
        // end node, so the result is reversed into arrow order. (Undirected keeps
        // start→end order.)
        let reverse = matches!(rel.dir, Direction::Incoming);

        // min == 0 admits the empty (single-node) path when src == dst.
        if min == 0 && src == dst {
            return Ok(Val::Path {
                nodes: vec![src],
                rels: Vec::new(),
            });
        }
        if max == 0 {
            return Ok(Val::Null);
        }

        // BFS by node, recording the predecessor edge to reconstruct the path. The
        // first time `dst` is dequeued (or reached) yields a shortest path; node
        // uniqueness (visited set) keeps it to simple, shortest paths.
        let empty = HashMap::new();
        let mut visited: HashSet<u64> = HashSet::new();
        visited.insert(src);
        // (node, predecessor hop into it). The root has no predecessor.
        let mut pred: HashMap<u64, Hop> = HashMap::new();
        let mut frontier = vec![src];
        let mut depth = 0u32;
        let mut found = false;
        'bfs: while !frontier.is_empty() && depth < max {
            self.check_deadline()?;
            let mut next = Vec::new();
            for &node in &frontier {
                for hop in self.expand_one_hop(node, rel, &empty)? {
                    let nb = hop.neighbour;
                    if visited.contains(&nb) {
                        continue;
                    }
                    visited.insert(nb);
                    pred.insert(nb, hop);
                    if nb == dst {
                        found = true;
                        break 'bfs;
                    }
                    next.push(nb);
                }
            }
            frontier = next;
            depth += 1;
        }
        if !found {
            return Ok(Val::Null);
        }
        // Walk predecessors back from dst to src, then reverse into walk order.
        let mut hops_rev: Vec<Hop> = Vec::new();
        let mut cur = dst;
        while cur != src {
            let hop = pred.get(&cur).expect("BFS predecessor recorded");
            // `cur` is `hop.neighbour`; the node we arrived from is the opposite
            // stored endpoint.
            cur = if hop.start == cur { hop.end } else { hop.start };
            hops_rev.push(hop.clone());
        }
        hops_rev.reverse();
        let path = make_path(src, &hops_rev);
        if reverse {
            if let Val::Path { mut nodes, mut rels } = path {
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

    fn property(&self, base: &Val, key: &str) -> Result<Val> {
        match base {
            Val::Node(id) => self.node_prop(*id, key),
            Val::Rel { id, .. } => self.edge_prop(*id, key),
            Val::Map(m) => Ok(m
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or(Val::Null)),
            // Point coordinate read (FalkorDB `Point_GetCoordinate`): only
            // `latitude`/`longitude` resolve; any other key yields NULL.
            Val::Point {
                latitude,
                longitude,
            } => Ok(match key {
                "latitude" => Val::Float(*latitude),
                "longitude" => Val::Float(*longitude),
                _ => Val::Null,
            }),
            Val::Null => Ok(Val::Null),
            other => bail!("type {} has no property '{key}'", other.to_display()),
        }
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
        let raw = match base {
            Val::Node(id) => self.node_props(*id)?,
            Val::Rel { id, .. } => self.edge_props(*id)?,
            Val::Map(m) => return Ok(m.clone()),
            Val::Null => return Ok(Vec::new()),
            other => bail!("type {} has no properties", other.to_display()),
        };
        let mut out = Vec::with_capacity(raw.len());
        for (kid, v) in raw {
            let name = self.gen.property_key_name(kid).unwrap_or("?").to_string();
            out.push((name, Val::from_value(v)));
        }
        Ok(out)
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
        self.match_single_pattern(pattern, &seed, predicate, &mut bindings)?;
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
            "tolower" | "lower" => str_fn(&a0(0), |s| s.to_lowercase()),
            "toupper" | "upper" => str_fn(&a0(0), |s| s.to_uppercase()),
            "trim" => str_fn(&a0(0), |s| s.trim().to_string()),
            "ltrim" => str_fn(&a0(0), |s| s.trim_start().to_string()),
            "rtrim" => str_fn(&a0(0), |s| s.trim_end().to_string()),
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
            "tostring" | "tostringornull" => match a0(0) {
                Val::Null => Val::Null,
                v => Val::Str(v.to_display()),
            },
            // `toInteger`/`toFloat`/`toBoolean` already return NULL on a failed
            // coercion, so the `*OrNull` forms (which never error) are aliases.
            "tointeger" | "tointegerornull" => match a0(0) {
                Val::Int(i) => Val::Int(i),
                Val::Float(f) => Val::Int(f as i64),
                Val::Str(s) => s.trim().parse::<i64>().map(Val::Int).unwrap_or(Val::Null),
                Val::Bool(b) => Val::Int(b as i64),
                _ => Val::Null,
            },
            "tofloat" | "tofloatornull" => match a0(0) {
                Val::Int(i) => Val::Float(i as f64),
                Val::Float(f) => Val::Float(f),
                Val::Str(s) => s.trim().parse::<f64>().map(Val::Float).unwrap_or(Val::Null),
                _ => Val::Null,
            },
            "toboolean" | "tobooleanornull" => match a0(0) {
                Val::Bool(b) => Val::Bool(b),
                Val::Str(s) => match s.trim().to_lowercase().as_str() {
                    "true" => Val::Bool(true),
                    "false" => Val::Bool(false),
                    _ => Val::Null,
                },
                _ => Val::Null,
            },
            "abs" => num_fn(&a0(0), |x| x.abs()),
            "ceil" => num_fn(&a0(0), |x| x.ceil()),
            "floor" => num_fn(&a0(0), |x| x.floor()),
            "round" => num_fn(&a0(0), |x| x.round()),
            "sqrt" => num_fn(&a0(0), |x| x.sqrt()),
            // Natural log / base-10 log. Like FalkorDB these wrap libm directly, so
            // a non-positive argument yields the IEEE result (`log(0) = -inf`,
            // `log(-1) = NaN`) rather than an error — matching its `AR_LOG`/`AR_LOG10`.
            "log" => num_fn(&a0(0), |x| x.ln()),
            "log10" => num_fn(&a0(0), |x| x.log10()),
            "exp" => num_fn(&a0(0), |x| x.exp()),
            "e" => Val::Float(std::f64::consts::E),
            "pi" => Val::Float(std::f64::consts::PI),
            "pow" => match (a0(0).as_num(), a0(1).as_num()) {
                (Some(b), Some(e)) => Val::Float(b.powf(e)),
                _ => Val::Null,
            },
            "sign" => num_fn(&a0(0), |x| x.signum().trunc()),
            // Trigonometric family — like FalkorDB these wrap libm directly and
            // return NULL for a non-numeric (incl. NULL) argument.
            "sin" => num_fn(&a0(0), |x| x.sin()),
            "cos" => num_fn(&a0(0), |x| x.cos()),
            "tan" => num_fn(&a0(0), |x| x.tan()),
            "cot" => num_fn(&a0(0), |x| x.cos() / x.sin()),
            "asin" => num_fn(&a0(0), |x| x.asin()),
            "acos" => num_fn(&a0(0), |x| x.acos()),
            "atan" => num_fn(&a0(0), |x| x.atan()),
            "atan2" => match (a0(0).as_num(), a0(1).as_num()) {
                (Some(y), Some(x)) => Val::Float(y.atan2(x)),
                _ => Val::Null,
            },
            "degrees" => num_fn(&a0(0), |x| x.to_degrees()),
            "radians" => num_fn(&a0(0), |x| x.to_radians()),
            // haversin(x) = (1 - cos x) / 2 (FalkorDB AR_HAVERSIN).
            "haversin" => num_fn(&a0(0), |x| (1.0 - x.cos()) / 2.0),
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
                other => bail!("isEmpty() needs a string, list or map, got {}", other.to_display()),
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
            "string.matchregex" => match_regex(&a0(0), &a0(1))?,
            "string.replaceregex" => {
                let repl = if args.len() >= 3 { a0(2) } else { Val::Str(String::new()) };
                replace_regex(&a0(0), &a0(1), &repl)?
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
            adjs.iter().filter(|a| type_ids.contains(&a.reltype)).count()
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
        let mut out = Vec::new();
        let mut i = start;
        // Inclusive of `end`, matching Cypher.
        while (step > 0 && i <= end) || (step < 0 && i >= end) {
            out.push(Val::Int(i));
            i += step;
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

/// The scalar + aggregate functions slater implements (lowercased canonical names),
/// self-reported by `CALL dbms.functions()`. Hand-maintained to mirror the
/// `call_function` match arms + [`is_aggregate`]; doubles as the roadmap's coverage
/// gate against FalkorDB's 144-entry `builtin_funcs.gperf`. Add a name here whenever
/// a new function arm lands.
const IMPLEMENTED_FUNCTIONS: &[&str] = &[
    // aggregates
    "avg", "collect", "count", "max", "min", "percentilecont", "percentiledisc",
    "stdev", "stdevp", "sum",
    // numeric / trig
    "abs", "acos", "asin", "atan", "atan2", "ceil", "cos", "cot", "degrees", "e",
    "exp", "floor", "haversin", "log", "log10", "pi", "pow", "radians", "round",
    "sign", "sin", "sqrt", "tan",
    // string
    "left", "ltrim", "replace", "reverse", "right", "rtrim", "split", "string.join",
    "string.matchregex", "string.replaceregex", "substring", "tolower", "toupper",
    "trim", "lower", "upper",
    // conversion
    "toboolean", "tobooleanlist", "tobooleanornull", "tofloat", "tofloatlist",
    "tofloatornull", "tointeger", "tointegerlist", "tointegerornull", "tostring",
    "tostringlist", "tostringornull",
    // list
    "head", "keys", "last", "list.dedup", "list.insert", "list.insertlistelements",
    "list.remove", "list.sort", "range", "size", "tail",
    // predicate / type
    "coalesce", "exists", "isempty", "type", "typeof",
    // entity / path
    "endnode", "haslabels", "id", "indegree", "labels", "length", "nodes",
    "outdegree", "properties", "relationships", "startnode",
    // vector
    "similarity", "vec.cosinedistance", "vec.cosinesimilarity",
    "vec.euclideandistance", "vecf32",
    // spatial
    "distance", "point",
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

fn varlen_bounds(vl: &VarLength) -> (u32, u32) {
    let min = vl.min.unwrap_or(1);
    let max = vl.max.unwrap_or(MAX_VARLEN_HOPS).max(min);
    (min, max)
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
    }
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

fn arith(op: BinOp, a: Val, b: Val) -> Result<Val> {
    if matches!(a, Val::Null) || matches!(b, Val::Null) {
        return Ok(Val::Null);
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
        });
    }
    match (a.as_num(), b.as_num()) {
        (Some(x), Some(y)) => Ok(Val::Float(match op {
            BinOp::Add => x + y,
            BinOp::Sub => x - y,
            BinOp::Mul => x * y,
            BinOp::Div => x / y,
            BinOp::Mod => x % y,
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
        _ => None,
    }
}

fn string_op(op: StrOp, a: &Val, b: &Val) -> Result<Val> {
    let (s, t) = match (a, b) {
        (Val::Str(s), Val::Str(t)) => (s, t),
        // `=~` against a null operand is null (three-valued); so are the others.
        _ => return Ok(Val::Null),
    };
    Ok(Val::Bool(match op {
        StrOp::StartsWith => s.starts_with(t.as_str()),
        StrOp::EndsWith => s.ends_with(t.as_str()),
        StrOp::Contains => s.contains(t.as_str()),
        // `=~` is a full-match: the whole string must match the pattern, mirroring
        // FalkorDB's `str_MatchRegex` (anchored at both ends). RE2 has no backrefs.
        StrOp::Regex => regex_full_match(t)?.is_match(s),
    }))
}

// Compile `pattern` anchored as a full-match (`\A(?:…)\z`) so `=~` requires the
// entire subject to match — openCypher / FalkorDB `=~` semantics.
fn regex_full_match(pattern: &str) -> Result<regex::Regex> {
    let anchored = format!(r"\A(?:{pattern})\z");
    regex::Regex::new(&anchored).map_err(|e| anyhow::anyhow!("Invalid regex: {e}"))
}

// Compile `pattern` for an unanchored scan (`string.matchRegEx`/`replaceRegEx`),
// which find every non-overlapping match anywhere in the subject.
fn regex_scan(pattern: &str) -> Result<regex::Regex> {
    regex::Regex::new(pattern).map_err(|e| anyhow::anyhow!("Invalid regex: {e}"))
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

// string.matchRegEx(str, regex) -> list of [full_match, group1, …] per match.
// A null operand yields an empty list; non-participating groups become "".
fn match_regex(s: &Val, pat: &Val) -> Result<Val> {
    let (s, pat) = match (s, pat) {
        (Val::Str(s), Val::Str(p)) => (s, p),
        (Val::Null, _) | (_, Val::Null) => return Ok(Val::List(vec![])),
        (Val::Str(_), other) | (other, _) => bail!(
            "Type mismatch: expected String or Null but was {}",
            type_name(other)
        ),
    };
    let re = regex_scan(pat)?;
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
fn replace_regex(s: &Val, pat: &Val, repl: &Val) -> Result<Val> {
    let (s, pat, repl) = match (s, pat, repl) {
        (Val::Str(s), Val::Str(p), Val::Str(r)) => (s, p, r),
        (Val::Null, _, _) | (_, Val::Null, _) | (_, _, Val::Null) => return Ok(Val::Null),
        (Val::Str(_), Val::Str(_), other)
        | (Val::Str(_), other, _)
        | (other, _, _) => bail!(
            "Type mismatch: expected String or Null but was {}",
            type_name(other)
        ),
    };
    let re = regex_scan(pat)?;
    Ok(Val::Str(re.replace_all(s, regex::NoExpand(repl)).into_owned()))
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
    Ok(Val::Float(xs[index] * (1.0 - fraction) + xs[index + 1] * fraction))
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
    let a =
        (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlambda / 2.0).sin().powi(2);
    // c = 2 · atan2(√a, √(1−a)); d = R · c
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS * c
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
    }
}

fn str_fn(v: &Val, f: impl Fn(&str) -> String) -> Val {
    match v {
        Val::Str(s) => Val::Str(f(s)),
        Val::Null => Val::Null,
        _ => Val::Null,
    }
}

fn num_fn(v: &Val, f: impl Fn(f64) -> f64) -> Val {
    match v.as_num() {
        Some(x) => Val::Float(f(x)),
        None => Val::Null,
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
    xs.iter().any(|x| x.cmp_total(v) == std::cmp::Ordering::Equal)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser;
    use crate::testgen;

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
            Val::Float(x) => assert!(
                (x - want).abs() < 1e-9,
                "expected ~{want}, got {x}"
            ),
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
        let cases = [(0.0, 2.0), (0.1, 2.8), (0.33, 4.64), (0.5, 6.0), (1.0, 10.0)];
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
        assert!(run_err("exec_p5_reduce_e1", "RETURN reduce(sum = 'a', n in [1,2,3] | sum * n)")
            .contains("cannot apply arithmetic"));
        assert!(run_err("exec_p5_reduce_e2", "RETURN reduce(sum = 1, n in 2 | sum + n)")
            .contains("needs a list"));
        // A reduce missing its `| body` is a plain function call over the
        // would-be accumulator binding `sum`, which is unbound -> runtime error.
        assert!(run_err("exec_p5_reduce_e3", "RETURN reduce(sum = 0, n in [1,2,3])")
            .contains("'sum'"));
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
        assert!(matches!(r[6], Val::Int(5)), "propertyKeyCount");
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
        assert!(matches!(r[0], Val::Int(5)));
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
        for want in ["db.constraints", "db.meta.stats", "dbms.functions", "dbms.procedures"] {
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
        let (root, res) = run("exec_p11_funcs_cov", "CALL dbms.functions() YIELD name RETURN name");
        let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
        for want in [
            "sin", "tail", "point", "distance", "vec.euclideandistance",
            "tofloatornull", "percentilecont", "string.matchregex",
        ] {
            assert!(names.iter().any(|n| n == want), "coverage gate missing {want}");
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
            vec![
                (1, 10),
                (1, 20),
                (1, 30),
                (2, 10),
                (2, 20),
                (2, 30)
            ]
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
        assert!(
            err.contains("already declared in outer scope"),
            "{err}"
        );
    }
}
