// SPDX-License-Identifier: Apache-2.0
//! Logical planning: choosing how to generate candidate rows for a node pattern.
//!
//! The interesting planning decision in a read-only store is *which structure to
//! sweep* for the anchor of a `MATCH` — a selective range-index lookup, the
//! inverted label postings, or (last resort) the whole node space. This module is
//! that decision and nothing more: it is a pure, side-effect-free function over the
//! pattern, the optional `WHERE`, and the open [`Generation`] (so it can see which
//! indexes and labels actually exist).
//!
//! Correctness does **not** depend on the planner being clever. The executor always
//! re-applies every label and property predicate to each candidate it is handed
//! (see `exec`), so a [`NodeScan`] only ever *narrows the candidate set* — picking
//! a worse strategy costs time, never correctness.
//!
//! The planner is **parameter-aware**: the RUN message carries the query's `$param`
//! bindings before planning, so a predicate like `{type: $t}` or `WHERE n.x = $v`
//! resolves its constant against the param map and selects an index just as a
//! literal would. A param that is absent or whose runtime type cannot key an index
//! simply does not contribute a predicate — the anchor falls back to a scan and the
//! executor filters it, exactly as before (this is what keeps it sound).
//
// Consumed by the executor (`exec`); the standalone planner is unit-tested here.
#![allow(dead_code)]

use crate::generation::Generation;
use crate::parser::ast::{CmpOp, Expr, FuncArgs, NodePat};
use graph_format::ids::Value;
use graph_format::manifest::EntityKind;
use std::collections::HashMap;

/// How the executor should generate candidate nodes for a single node pattern.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeScan {
    /// Direct seek to specific dense node ids — `id(n) = <int>` (or
    /// `id(n) IN [<int>, …]`) pinned the anchor. `ids` is already bounds-checked
    /// (every entry `< node_count`) and deduped, so the executor yields it as-is;
    /// an empty list means the requested id matches no node. This is the most
    /// selective strategy (O(1) per id), preferred over every index.
    IdSeek { ids: Vec<u64> },
    /// Equality lookup through a range (ISAM) index → the matching node ids.
    RangeEq { index: String, key: Value },
    /// Range lookup through a range (ISAM) index, with per-bound inclusivity.
    RangeRange {
        index: String,
        lo: Option<(Value, bool)>,
        hi: Option<(Value, bool)>,
    },
    /// Sweep the inverted postings of a single label (the most selective of the
    /// pattern's labels). Other labels/props become residual filters in `exec`.
    LabelScan { label_id: u32 },
    /// Full node sweep `0..node_count`. Residual filters in `exec`.
    AllNodes,
}

/// A constant predicate extracted on `var.<prop>` for planning.
enum Pred {
    Eq(Value),
    Lo(Value, bool),
    Hi(Value, bool),
}

/// Choose a candidate-generation strategy for `node`, the anchor of a pattern.
///
/// `var` is the node pattern's variable (predicates in `WHERE` reference it by
/// name); `where_` is the clause's optional predicate. Preference order:
/// range-index equality → range-index range → smallest label posting → full scan.
pub fn choose_node_scan(
    gen: &Generation,
    node: &NodePat,
    where_: Option<&Expr>,
    params: &HashMap<String, Value>,
) -> NodeScan {
    let Some(var) = node.var.as_deref() else {
        // An anonymous anchor can still be label/range-selective via its own
        // inline props, but it has no name for WHERE predicates to reference.
        return choose_from_preds(gen, node, &inline_preds(node, params));
    };

    // Highest-priority strategy: a direct id() seek. When a top-level `AND`
    // conjunct pins `id(var)` to concrete ids, the anchor is just those nodes — an
    // O(1) lookup that beats any index (and turns Memgraph Lab's
    // `MATCH (n)-[r]->(m) WHERE id(n) = X` neighbourhood-expansion from a full edge
    // scan into a seek). Soundness: a seek can only *narrow* candidates and the
    // executor re-checks the full `WHERE` on every binding, so the only hazard is
    // narrowing away a row a disjunction would have kept — which `id_seek_ids`
    // prevents by descending `AND` only (never `OR`).
    if let Some(w) = where_ {
        if let Some(ids) = id_seek_ids(w, var, gen.node_count()) {
            return NodeScan::IdSeek { ids };
        }
    }

    let mut preds = inline_preds(node, params);
    if let Some(w) = where_ {
        collect_where_preds(w, var, params, &mut preds);
    }
    choose_from_preds(gen, node, &preds)
}

/// If the top-level `AND`-conjuncts of `where_` pin `id(var)` to concrete node ids
/// — `id(var) = <int>` or `id(var) IN [<int>, …]` — return the in-bounds ids
/// (deduped, sorted). Returns `None` when no such conjunct exists so the caller
/// falls back to a normal scan.
///
/// Only descends `Expr::And` (never `Expr::Or`): an `id(var) = X` buried in a
/// disjunction does **not** constrain every result to id `X`, so seeking it would
/// drop valid rows. Every node satisfying the whole `WHERE` must satisfy each
/// `AND`-conjunct, so the union of the id-conjuncts' id-sets is always a superset
/// of the true result — a sound candidate set the executor then filters exactly.
/// A negative or out-of-range id matches no node and is simply dropped (an empty
/// `Some(vec![])` is a valid "seek that finds nothing", and faster than a scan).
fn id_seek_ids(expr: &Expr, var: &str, node_count: u64) -> Option<Vec<u64>> {
    let mut ids: Vec<u64> = Vec::new();
    let mut found = false;
    collect_id_eq(expr, var, &mut ids, &mut found);
    if !found {
        return None;
    }
    ids.retain(|&id| id < node_count);
    ids.sort_unstable();
    ids.dedup();
    Some(ids)
}

/// Does a top-level `AND`-conjunct of `where_` pin `id(var)` to concrete ids? The
/// executor uses this to decide whether to **re-root** a pattern onto an
/// id-seekable end node (so `MATCH (m)-[r]->(n) WHERE id(n) = X` seeks `n` and
/// walks the edge backwards instead of scanning every `m`). Cheaper than
/// [`id_seek_ids`] — no bounds resolution, just "is there an id constraint".
pub(crate) fn is_id_anchored(where_: &Expr, var: &str) -> bool {
    let mut ids = Vec::new();
    let mut found = false;
    collect_id_eq(where_, var, &mut ids, &mut found);
    found
}

/// Gather, from the top-level `AND`-conjuncts of `expr`, every concrete node id
/// that `id(var)` is constrained to. Sets `found` once any `id(var)` equality /
/// membership conjunct is seen (so a negative-only constraint still yields a
/// "seek that finds nothing" rather than a fallback scan).
fn collect_id_eq(expr: &Expr, var: &str, ids: &mut Vec<u64>, found: &mut bool) {
    match expr {
        Expr::And(parts) => {
            for p in parts {
                collect_id_eq(p, var, ids, found);
            }
        }
        // `id(var) = <int>` / `<int> = id(var)`.
        Expr::Compare(CmpOp::Eq, l, r) => {
            if let Some(i) = id_eq_operand(l, r, var) {
                *found = true;
                if i >= 0 {
                    ids.push(i as u64);
                }
            }
        }
        // `id(var) IN [<int>, …]` — a seek only when every element is a constant
        // integer (otherwise the full id-set is unknown → fall back to a scan).
        Expr::In(lhs, rhs) => {
            if is_id_of(lhs, var) {
                if let Expr::List(items) = &**rhs {
                    let consts: Option<Vec<i64>> = items.iter().map(const_int).collect();
                    if let Some(values) = consts {
                        *found = true;
                        ids.extend(values.into_iter().filter(|&i| i >= 0).map(|i| i as u64));
                    }
                }
            }
        }
        _ => {}
    }
}

/// For an `=` comparison, return the integer if exactly one side is `id(var)` and
/// the other a constant integer.
fn id_eq_operand(l: &Expr, r: &Expr, var: &str) -> Option<i64> {
    if is_id_of(l, var) {
        return const_int(r);
    }
    if is_id_of(r, var) {
        return const_int(l);
    }
    None
}

/// Evaluate a constant integer expression: an int literal, or a (possibly nested)
/// numeric negation of one. Needed because `-1` parses as `Neg(Literal(Int(1)))`,
/// not a negative literal. Overflow (`-i64::MIN`) yields `None` (no seek).
fn const_int(e: &Expr) -> Option<i64> {
    match e {
        Expr::Literal(Value::Int(i)) => Some(*i),
        Expr::Neg(inner) => const_int(inner).and_then(i64::checked_neg),
        _ => None,
    }
}

/// Is `e` exactly `id(var)` — the built-in `id` function (case-insensitive, not
/// `DISTINCT`) applied to the single variable `var`?
fn is_id_of(e: &Expr, var: &str) -> bool {
    matches!(
        e,
        Expr::Function { name, distinct: false, args: FuncArgs::Args(a) }
            if name.eq_ignore_ascii_case("id")
                && a.len() == 1
                && matches!(&a[0], Expr::Var(v) if v == var)
    )
}

/// Inline `{prop: literal}` / `{prop: $param}` entries are always equality
/// predicates (params resolved against the RUN bindings).
fn inline_preds(node: &NodePat, params: &HashMap<String, Value>) -> Vec<(String, Pred)> {
    node.props
        .iter()
        .filter_map(|(k, e)| resolve(e, params).map(|v| (k.clone(), Pred::Eq(v))))
        .collect()
}

fn choose_from_preds(gen: &Generation, node: &NodePat, preds: &[(String, Pred)]) -> NodeScan {
    // Prefer an equality lookup on any indexed property.
    for (prop, pred) in preds {
        if let Pred::Eq(v) = pred {
            if let Some(index) = index_for(gen, &node.labels, prop) {
                return NodeScan::RangeEq {
                    index,
                    key: v.clone(),
                };
            }
        }
    }
    // Else a range lookup, combining lo/hi bounds on a single indexed property.
    for (prop, _) in preds {
        if let Some(index) = index_for(gen, &node.labels, prop) {
            let mut lo = None;
            let mut hi = None;
            for (p, pred) in preds {
                if p != prop {
                    continue;
                }
                match pred {
                    Pred::Lo(v, incl) => lo = Some((v.clone(), *incl)),
                    Pred::Hi(v, incl) => hi = Some((v.clone(), *incl)),
                    Pred::Eq(_) => {}
                }
            }
            if lo.is_some() || hi.is_some() {
                return NodeScan::RangeRange { index, lo, hi };
            }
        }
    }
    // Else the smallest label posting, if the pattern names any label.
    let smallest = node
        .labels
        .iter()
        .filter_map(|l| gen.label_id(l))
        .min_by_key(|&id| gen.nodes_with_label(id).len());
    match smallest {
        Some(label_id) => NodeScan::LabelScan { label_id },
        None => NodeScan::AllNodes,
    }
}

/// Find an *open* range index over `(label ∈ labels, prop)` for a node entity.
pub(crate) fn index_for(gen: &Generation, labels: &[String], prop: &str) -> Option<String> {
    for ri in &gen.manifest().range_indexes {
        if ri.entity == EntityKind::Node
            && ri.property == prop
            && labels.iter().any(|l| l == &ri.label_or_type)
            && gen.range_index(&ri.name).is_some()
        {
            return Some(ri.name.clone());
        }
    }
    None
}

/// Flatten the top-level `AND`s of a `WHERE` and pull out constant predicates of
/// the form `var.prop <op> literal` (or the mirror image).
fn collect_where_preds(
    expr: &Expr,
    var: &str,
    params: &HashMap<String, Value>,
    out: &mut Vec<(String, Pred)>,
) {
    match expr {
        Expr::And(parts) => {
            for p in parts {
                collect_where_preds(p, var, params, out);
            }
        }
        Expr::Compare(op, l, r) => {
            if let Some((prop, val, flipped)) = compare_operands(l, r, var, params) {
                if let Some(pred) = pred_for(*op, val, flipped) {
                    out.push((prop, pred));
                }
            }
        }
        _ => {}
    }
}

/// Match `var.prop <op> const` / `const <op> var.prop` (where `const` is a literal
/// or a resolved `$param`), returning `(prop, value, flipped)` — `flipped` is true
/// when the property was on the right (so the comparison direction must mirror).
fn compare_operands(
    l: &Expr,
    r: &Expr,
    var: &str,
    params: &HashMap<String, Value>,
) -> Option<(String, Value, bool)> {
    if let (Some(prop), Some(v)) = (var_prop(l, var), resolve(r, params)) {
        return Some((prop, v, false));
    }
    if let (Some(v), Some(prop)) = (resolve(l, params), var_prop(r, var)) {
        return Some((prop, v, true));
    }
    None
}

fn pred_for(op: CmpOp, val: Value, flipped: bool) -> Option<Pred> {
    // When the property sat on the right, mirror the operator.
    let op = if flipped { mirror(op) } else { op };
    match op {
        CmpOp::Eq => Some(Pred::Eq(val)),
        CmpOp::Gt => Some(Pred::Lo(val, false)),
        CmpOp::Ge => Some(Pred::Lo(val, true)),
        CmpOp::Lt => Some(Pred::Hi(val, false)),
        CmpOp::Le => Some(Pred::Hi(val, true)),
        CmpOp::Ne => None,
    }
}

fn mirror(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::Le => CmpOp::Ge,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::Ge => CmpOp::Le,
        other => other,
    }
}

/// If `e` is `var.prop`, return `prop`.
fn var_prop(e: &Expr, var: &str) -> Option<String> {
    match e {
        Expr::Property(base, prop) => match &**base {
            Expr::Var(v) if v == var => Some(prop.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Resolve `e` to a constant planning value: a literal directly, or a `$param`
/// looked up in the RUN bindings. Any other expression (or an unbound param) is
/// `None`, so the predicate is dropped and the anchor falls back to a scan.
fn resolve(e: &Expr, params: &HashMap<String, Value>) -> Option<Value> {
    match e {
        Expr::Literal(v) => Some(v.clone()),
        Expr::Param(name) => params.get(name).cloned(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser;

    /// Pull the anchor node pattern and the (single) MATCH `WHERE` out of a query.
    fn anchor(q: &parser::ast::Query) -> (&NodePat, Option<&Expr>) {
        let parser::ast::Clause::Match(m) = &q.head.reading[0] else {
            panic!("expected a leading MATCH");
        };
        (&m.patterns[0].start, m.where_.as_ref())
    }

    fn plan_for(gen: &Generation, query: &str) -> NodeScan {
        let q = parser::parse(query).unwrap();
        let (node, where_) = anchor(&q);
        choose_node_scan(gen, node, where_, &HashMap::new())
    }

    fn plan_for_params(gen: &Generation, query: &str, params: &[(&str, Value)]) -> NodeScan {
        let q = parser::parse(query).unwrap();
        let (node, where_) = anchor(&q);
        let params: HashMap<String, Value> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        choose_node_scan(gen, node, where_, &params)
    }

    #[test]
    fn param_equality_on_indexed_property_picks_range_eq() {
        // `{name: $n}` and `WHERE n.name = $n` must select the index, not fall
        // back to a label scan — the param resolves against the RUN bindings.
        let (root, graph, _) = crate::testgen::write_basic("plan_param_eq");
        let gen = Generation::open(&root, &graph).unwrap();
        let want = NodeScan::RangeEq {
            index: "node_Person_name".into(),
            key: Value::Str("Carol".into()),
        };
        let inline = plan_for_params(
            &gen,
            "MATCH (n:Person {name: $n}) RETURN n",
            &[("n", Value::Str("Carol".into()))],
        );
        assert_eq!(inline, want);
        let where_ = plan_for_params(
            &gen,
            "MATCH (n:Person) WHERE n.name = $n RETURN n",
            &[("n", Value::Str("Carol".into()))],
        );
        assert_eq!(where_, want);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unbound_param_falls_back_to_label_scan() {
        // A param the RUN message never supplied contributes no predicate, so the
        // anchor degrades to a label scan (still correct — executor re-filters).
        let (root, graph, _) = crate::testgen::write_basic("plan_param_unbound");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for_params(&gen, "MATCH (n:Person) WHERE n.name = $n RETURN n", &[]);
        let person = gen.label_id("Person").unwrap();
        assert_eq!(scan, NodeScan::LabelScan { label_id: person });
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn inline_equality_on_indexed_property_picks_range_eq() {
        let (root, graph, _) = crate::testgen::write_basic("plan_inline_eq");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n:Person {name: 'Alice'}) RETURN n");
        assert_eq!(
            scan,
            NodeScan::RangeEq {
                index: "node_Person_name".into(),
                key: Value::Str("Alice".into())
            }
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn where_equality_on_indexed_property_picks_range_eq() {
        let (root, graph, _) = crate::testgen::write_basic("plan_where_eq");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n:Person) WHERE n.name = 'Bob' RETURN n");
        assert_eq!(
            scan,
            NodeScan::RangeEq {
                index: "node_Person_name".into(),
                key: Value::Str("Bob".into())
            }
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn range_predicate_on_indexed_property_picks_range_range() {
        // age is indexed in the richer fixture; a half-open range plans as a range.
        let (root, graph, _) = crate::testgen::write_basic("plan_range");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n:Person) WHERE n.age >= 30 RETURN n");
        assert_eq!(
            scan,
            NodeScan::RangeRange {
                index: "node_Person_age".into(),
                lo: Some((Value::Int(30), true)),
                hi: None,
            }
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unindexed_label_falls_back_to_smallest_label_posting() {
        let (root, graph, _) = crate::testgen::write_basic("plan_label");
        let gen = Generation::open(&root, &graph).unwrap();
        // Company has no indexed predicate here → label scan on Company.
        let scan = plan_for(&gen, "MATCH (n:Company) WHERE n.name = 'Acme' RETURN n");
        let company = gen.label_id("Company").unwrap();
        // Company has a name index too in the fixture? No — only Person. So this
        // is a label scan, not a range lookup.
        assert_eq!(scan, NodeScan::LabelScan { label_id: company });
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn no_label_no_index_falls_back_to_all_nodes() {
        let (root, graph, _) = crate::testgen::write_basic("plan_all");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n) RETURN n");
        assert_eq!(scan, NodeScan::AllNodes);
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── id() seek pushdown ─────────────────────────────────────────────────────

    #[test]
    fn id_equality_picks_id_seek() {
        let (root, graph, _) = crate::testgen::write_basic("plan_id_eq");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n) WHERE id(n) = 1 RETURN n");
        assert_eq!(scan, NodeScan::IdSeek { ids: vec![1] });
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_equality_flipped_operands_picks_id_seek() {
        let (root, graph, _) = crate::testgen::write_basic("plan_id_eq_flip");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n) WHERE 2 = id(n) RETURN n");
        assert_eq!(scan, NodeScan::IdSeek { ids: vec![2] });
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_function_is_case_insensitive() {
        let (root, graph, _) = crate::testgen::write_basic("plan_id_case");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n) WHERE ID(n) = 0 RETURN n");
        assert_eq!(scan, NodeScan::IdSeek { ids: vec![0] });
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_out_of_range_seeks_nothing() {
        // The fixture has 5 nodes (ids 0..4); 999 matches none → an empty seek
        // (still an IdSeek, not a scan — the answer is provably empty).
        let (root, graph, _) = crate::testgen::write_basic("plan_id_oor");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n) WHERE id(n) = 999 RETURN n");
        assert_eq!(scan, NodeScan::IdSeek { ids: vec![] });
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_negative_seeks_nothing() {
        let (root, graph, _) = crate::testgen::write_basic("plan_id_neg");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n) WHERE id(n) = -1 RETURN n");
        assert_eq!(scan, NodeScan::IdSeek { ids: vec![] });
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_seek_outranks_property_index() {
        // Both an indexed-name equality and an id() equality are present; the id
        // seek must win (it is the most selective).
        let (root, graph, _) = crate::testgen::write_basic("plan_id_vs_idx");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(
            &gen,
            "MATCH (n:Person) WHERE id(n) = 1 AND n.name = 'Alice' RETURN n",
        );
        assert_eq!(scan, NodeScan::IdSeek { ids: vec![1] });
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_in_list_picks_id_seek() {
        let (root, graph, _) = crate::testgen::write_basic("plan_id_in");
        let gen = Generation::open(&root, &graph).unwrap();
        // Out-of-range / duplicate ids are dropped; result is sorted + deduped.
        let scan = plan_for(&gen, "MATCH (n) WHERE id(n) IN [2, 0, 0, 99] RETURN n");
        assert_eq!(scan, NodeScan::IdSeek { ids: vec![0, 2] });
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_under_or_does_not_seek() {
        // CRITICAL guard: `id(n)=0 OR id(n)=2` does not constrain every row to one
        // id — seeking would drop the other. Must fall back to a (here: full) scan.
        let (root, graph, _) = crate::testgen::write_basic("plan_id_or");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n) WHERE id(n) = 0 OR id(n) = 2 RETURN n");
        assert_eq!(scan, NodeScan::AllNodes);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_of_other_variable_does_not_seek_anchor() {
        // The anchor is `n`; the predicate constrains `m`. The anchor must not be
        // seeked (it would be wrong) — it falls back to a scan.
        let (root, graph, _) = crate::testgen::write_basic("plan_id_otherv");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n)-[r]->(m) WHERE id(m) = 1 RETURN n");
        assert_eq!(scan, NodeScan::AllNodes);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_in_with_nonliteral_element_does_not_seek() {
        // A non-literal in the IN list means the id-set is unknown → no seek.
        let (root, graph, _) = crate::testgen::write_basic("plan_id_in_nonlit");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n) WHERE id(n) IN [1, n.age] RETURN n");
        assert_eq!(scan, NodeScan::AllNodes);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn id_inequality_does_not_seek() {
        // Only equality / IN seek; `id(n) > 1` is not a point lookup.
        let (root, graph, _) = crate::testgen::write_basic("plan_id_gt");
        let gen = Generation::open(&root, &graph).unwrap();
        let scan = plan_for(&gen, "MATCH (n) WHERE id(n) > 1 RETURN n");
        assert_eq!(scan, NodeScan::AllNodes);
        let _ = std::fs::remove_dir_all(&root);
    }
}
