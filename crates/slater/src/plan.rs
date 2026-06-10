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
//! a worse strategy costs time, never correctness. That is what lets the planner
//! plan on literals alone and ignore parameters: a parameterised predicate simply
//! falls back to a scan, and the executor filters it.
//
// Consumed by the executor (`exec`); the standalone planner is unit-tested here.
#![allow(dead_code)]

use crate::generation::Generation;
use crate::parser::ast::{CmpOp, Expr, NodePat};
use graph_format::ids::Value;
use graph_format::manifest::EntityKind;

/// How the executor should generate candidate nodes for a single node pattern.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeScan {
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
pub fn choose_node_scan(gen: &Generation, node: &NodePat, where_: Option<&Expr>) -> NodeScan {
    let Some(var) = node.var.as_deref() else {
        // An anonymous anchor can still be label/range-selective via its own
        // inline props, but it has no name for WHERE predicates to reference.
        return choose_from_preds(gen, node, &inline_preds(node));
    };

    let mut preds = inline_preds(node);
    if let Some(w) = where_ {
        collect_where_preds(w, var, &mut preds);
    }
    choose_from_preds(gen, node, &preds)
}

/// Inline `{prop: literal}` entries are always equality predicates.
fn inline_preds(node: &NodePat) -> Vec<(String, Pred)> {
    node.props
        .iter()
        .filter_map(|(k, e)| literal(e).map(|v| (k.clone(), Pred::Eq(v))))
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
fn index_for(gen: &Generation, labels: &[String], prop: &str) -> Option<String> {
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
fn collect_where_preds(expr: &Expr, var: &str, out: &mut Vec<(String, Pred)>) {
    match expr {
        Expr::And(parts) => {
            for p in parts {
                collect_where_preds(p, var, out);
            }
        }
        Expr::Compare(op, l, r) => {
            if let Some((prop, val, flipped)) = compare_operands(l, r, var) {
                if let Some(pred) = pred_for(*op, val, flipped) {
                    out.push((prop, pred));
                }
            }
        }
        _ => {}
    }
}

/// Match `var.prop <op> literal` / `literal <op> var.prop`, returning
/// `(prop, value, flipped)` where `flipped` is true when the property was on the
/// right (so the comparison direction must be mirrored).
fn compare_operands(l: &Expr, r: &Expr, var: &str) -> Option<(String, Value, bool)> {
    if let (Some(prop), Some(v)) = (var_prop(l, var), literal(r)) {
        return Some((prop, v, false));
    }
    if let (Some(v), Some(prop)) = (literal(l), var_prop(r, var)) {
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

/// If `e` is a literal, clone its value.
fn literal(e: &Expr) -> Option<Value> {
    match e {
        Expr::Literal(v) => Some(v.clone()),
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
        choose_node_scan(gen, node, where_)
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
}
