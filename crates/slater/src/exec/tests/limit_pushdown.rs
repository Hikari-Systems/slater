// SPDX-License-Identifier: Apache-2.0
//! `limit_pushdown` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

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
    let ast = parser::parse("MATCH (n:Person) WHERE n.name = $name RETURN n.age AS age").unwrap();
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
