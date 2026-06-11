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
            (Vector(a), Vector(b)) => a == b,
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
        let mut table = Table::singleton();
        for clause in &sq.reading {
            match clause {
                Clause::Match(m) => table = self.apply_match(table, m)?,
                Clause::With(w) => {
                    table = self.project(table, &w.body, w.distinct, w.where_.as_ref())?
                }
                Clause::VectorCall(vc) => table = self.apply_vector_call(table, vc)?,
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
            self.expand_chain(pattern, 0, c, b, out)?;
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

    fn expand_chain(
        &self,
        pattern: &Pattern,
        i: usize,
        cur: u64,
        binding: HashMap<String, Val>,
        out: &mut Vec<HashMap<String, Val>>,
    ) -> Result<()> {
        if i == pattern.rels.len() {
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
                    self.expand_chain(pattern, i + 1, nb, b, out)?;
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
                    self.expand_chain(pattern, i + 1, endnode, b, out)?;
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
        let arg = match args {
            FuncArgs::Args(a) if a.len() == 1 => &a[0],
            _ => bail!("aggregate {name} expects exactly one argument"),
        };

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
                Ok(string_op(*op, &a, &b))
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
        }
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
            "size" | "length" => match a0(0) {
                Val::List(xs) => Val::Int(xs.len() as i64),
                Val::Vector(xs) => Val::Int(xs.len() as i64),
                Val::Str(s) => Val::Int(s.chars().count() as i64),
                Val::Map(m) => Val::Int(m.len() as i64),
                Val::Null => Val::Null,
                other => bail!(
                    "{n}() needs a collection or string, got {}",
                    other.to_display()
                ),
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
            "tostring" => match a0(0) {
                Val::Null => Val::Null,
                v => Val::Str(v.to_display()),
            },
            "tointeger" => match a0(0) {
                Val::Int(i) => Val::Int(i),
                Val::Float(f) => Val::Int(f as i64),
                Val::Str(s) => s.trim().parse::<i64>().map(Val::Int).unwrap_or(Val::Null),
                Val::Bool(b) => Val::Int(b as i64),
                _ => Val::Null,
            },
            "tofloat" => match a0(0) {
                Val::Int(i) => Val::Float(i as f64),
                Val::Float(f) => Val::Float(f),
                Val::Str(s) => s.trim().parse::<f64>().map(Val::Float).unwrap_or(Val::Null),
                _ => Val::Null,
            },
            "toboolean" => match a0(0) {
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
            other => bail!("unknown function '{other}'"),
        })
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

fn is_aggregate(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "count" | "sum" | "avg" | "min" | "max" | "collect"
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

fn string_op(op: StrOp, a: &Val, b: &Val) -> Val {
    match (a, b) {
        (Val::Str(s), Val::Str(t)) => Val::Bool(match op {
            StrOp::StartsWith => s.starts_with(t.as_str()),
            StrOp::EndsWith => s.ends_with(t.as_str()),
            StrOp::Contains => s.contains(t.as_str()),
        }),
        _ => Val::Null,
    }
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
}
