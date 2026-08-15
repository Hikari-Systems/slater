// SPDX-License-Identifier: Apache-2.0
//! Unit tests for the parent module (extracted verbatim from the inline
//! `mod tests`; a pure relocation, no test logic changed).

use super::*;
use crate::generation::Generation;
use crate::parser;
use crate::testgen;
use graph_format::ids::Generation as GenId;

mod aggregations;
mod budgets;
mod builtins;
mod dedup_and_pruning;
mod delta_and_segments;
mod expressions;
mod fulltext;
mod gql;
mod id_seek;
mod limit_pushdown;
mod patterns_and_paths;
mod points;
mod procedures;
mod query_basics;
mod reltype_scan;
mod temporal_values;
mod traversal_frames;
mod vector_delete;
mod vector_knn;

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

/// A `Val::Float` close to `want` (FalkorDB returns doubles for these aggs).
fn assert_float(v: &Val, want: f64) {
    match v {
        Val::Float(x) => assert!((x - want).abs() < 1e-9, "expected ~{want}, got {x}"),
        other => panic!("expected Float({want}), got {other:?}"),
    }
}

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
