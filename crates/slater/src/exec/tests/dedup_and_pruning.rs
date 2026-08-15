// SPDX-License-Identifier: Apache-2.0
//! `dedup_and_pruning` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── HIK-218 step 2 — endpoint dedup must never change a result ───────────
// A variable-length walk may drop a path whose endpoint it already emitted, but
// only when two routes to that endpoint are indistinguishable in the output.
// These pin the cases the gate must REFUSE; each would otherwise lose rows.

/// The case the optimisation exists for: `DISTINCT` over the end node, no path
/// variable, no bound relationship, var-length last in the chain.
#[test]
fn distinct_endpoint_dedup_matches_the_undeduped_result() {
    let (root, res) = run(
        "exec_h218_distinct_ok",
        "MATCH (n:Person)-[:KNOWS*1..2]->(m:Person) RETURN DISTINCT m.name AS who ORDER BY who",
    );
    assert_eq!(col0(&res), vec!["Bob", "Carol"], "{:?}", col0(&res));
    let _ = std::fs::remove_dir_all(&root);
}

/// **Proof the dedup actually fires.** The result set cannot show it — the projection
/// would collapse these rows regardless — so this counts the paths dropped.
///
/// The diamond is `s -> {a,b,c} -> t`, so `*1..2` reaches `t` by three distinct routes.
/// Two of them are redundant under `DISTINCT` over the end node.
#[test]
fn the_endpoint_dedup_fires_on_converging_routes() {
    use crate::exec::traverse::VARLEN_DEDUP_SKIPS;
    let (root, graph) = testgen::write_diamond("exec_h218_diamond");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let run_q = |q: &str| -> (usize, u64) {
        VARLEN_DEDUP_SKIPS.with(|c| c.set(0));
        let engine = Engine::new(&gen, &cache);
        let ast = parser::parse(q).unwrap();
        let n = engine.run(&ast).unwrap().rows.len();
        (n, VARLEN_DEDUP_SKIPS.with(|c| c.get()))
    };

    // DISTINCT over the end node: the gate opens and the redundant routes to `t` drop.
    let (rows, skips) =
        run_q("MATCH (s:N {name: 's'})-[:R*1..2]->(m:N) RETURN DISTINCT m.name AS n ORDER BY n");
    assert_eq!(rows, 4, "expected s's reachable set {{a,b,c,t}}");
    assert!(
        skips > 0,
        "the dedup never fired — the optimisation is inert"
    );

    // Same walk without DISTINCT: the gate must stay shut, so nothing is dropped.
    let (rows_all, skips_all) =
        run_q("MATCH (s:N {name: 's'})-[:R*1..2]->(m:N) RETURN m.name AS n ORDER BY n");
    assert!(
        rows_all > rows,
        "non-DISTINCT must keep the duplicate paths"
    );
    assert_eq!(skips_all, 0, "the dedup fired without DISTINCT");

    let _ = std::fs::remove_dir_all(&root);
}

/// **Without** `DISTINCT`, Cypher emits one row per PATH. Deduping endpoints here
/// would silently drop rows, so the gate must refuse.
#[test]
fn a_non_distinct_varlen_still_emits_one_row_per_path() {
    let (root, res) = run(
        "exec_h218_non_distinct",
        "MATCH (n:Person)-[:KNOWS*1..2]->(m:Person) RETURN m.name AS who ORDER BY who",
    );
    // Alice->Bob, Alice->Carol, Bob->Carol (1-hop) + Alice->Bob->Carol (2-hop) = 4 paths.
    assert_eq!(
        res.rows.len(),
        4,
        "path multiplicity was lost: {:?}",
        col0(&res)
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A bound path variable makes the route itself observable, so two routes to one
/// endpoint are different rows even under `DISTINCT`.
#[test]
fn a_bound_path_var_refuses_endpoint_dedup() {
    let (root, res) = run(
        "exec_h218_path_var",
        "MATCH p = (n:Person)-[:KNOWS*1..2]->(m:Person) RETURN DISTINCT p",
    );
    assert_eq!(
        res.rows.len(),
        4,
        "distinct paths were collapsed by endpoint"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A bound relationship list likewise differs per route.
#[test]
fn a_bound_rel_var_refuses_endpoint_dedup() {
    let (root, res) = run(
        "exec_h218_rel_var",
        "MATCH (n:Person)-[r:KNOWS*1..2]->(m:Person) RETURN DISTINCT r",
    );
    assert_eq!(
        res.rows.len(),
        4,
        "distinct relationship lists were collapsed"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// An aggregate counts PATHS, so the gate must refuse even under `DISTINCT`.
#[test]
fn an_aggregate_refuses_endpoint_dedup() {
    let (root, res) = run(
        "exec_h218_aggregate",
        "MATCH (n:Person)-[:KNOWS*1..2]->(m:Person) RETURN DISTINCT count(*) AS c",
    );
    assert_eq!(
        res.rows[0][0].to_display(),
        "4",
        "path count was deduped away"
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── HIK-217 — seed pruning must not change any result ────────────────────
// `apply_match` seeds the matcher's binding with only the incoming columns the
// clause can actually read, rather than the whole relation. These pin the ways
// that could silently drop a value; every one of them would surface as `null`
// where a value belongs, which no pre-existing test would catch.

/// The load-bearing case: a carried column the MATCH does **not** reference is
/// pruned from the binding, yet must still reach the output. It does, because the
/// output prefix is rebuilt positionally from the input row and never from the
/// binding — which is precisely why pruning is safe.
#[test]
fn a_pruned_seed_column_is_still_returned() {
    let (root, res) = run(
        "exec_h217_pruned_returned",
        "MATCH (n:Person) WITH n, n.city AS city, n.age AS age \
         MATCH (n)-[:KNOWS]->(m:Person) \
         RETURN n.name AS who, city, age, m.name AS friend ORDER BY who, friend",
    );
    // Exactly the three Person->Person KNOWS edges. Asserting the count is what
    // makes this sensitive to *over*-pruning: dropping `n` would leave the second
    // MATCH to rebind it as a fresh variable and emit a cartesian product instead.
    assert_eq!(
        res.rows.len(),
        3,
        "seed pruning changed the match cardinality"
    );
    for r in &res.rows {
        assert!(
            matches!(r[1], Val::Str(_)),
            "a pruned column came back as {:?}",
            r[1]
        );
        assert!(
            matches!(r[2], Val::Int(_)),
            "a pruned column came back as {:?}",
            r[2]
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// A column referenced only by the clause's `WHERE` must be retained; pruning it
/// would make the predicate see `null` and silently drop every row.
#[test]
fn a_seed_column_referenced_only_in_where_is_retained() {
    let (root, res) = run(
        "exec_h217_where_ref",
        "MATCH (n:Person) WITH n, n.city AS home \
         MATCH (n)-[:KNOWS]->(m:Person) WHERE home = 'London' \
         RETURN n.name AS who ORDER BY who",
    );
    assert!(
        !res.rows.is_empty(),
        "WHERE saw null for a pruned column and dropped every row"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A column referenced only inside an inline property predicate must be retained —
/// `node_ok`/`rel_ok` evaluate those against the binding mid-walk, so the analysis
/// has to walk pattern props, not merely pattern variable names.
#[test]
fn a_seed_column_referenced_only_in_an_inline_prop_is_retained() {
    let (root, res) = run(
        "exec_h217_inline_prop",
        // The pattern must carry a relationship: a node-only pattern is taken by
        // `try_stream_match`, which bypasses `apply_match`'s seed entirely and would
        // make this test vacuous.
        "MATCH (n:Person) WITH n, n.name AS wanted \
         MATCH (a:Person)-[:KNOWS]->(b:Person {name: wanted}) \
         RETURN b.name AS got ORDER BY got",
    );
    assert!(
        !res.rows.is_empty(),
        "an inline property predicate saw null for a pruned column"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A column referenced only inside a nested pattern expression must be retained:
/// `PatternPredicate` / `PatternComprehension` / `EXISTS` seed their inner match
/// from the surrounding binding, so the analysis must recurse into them.
#[test]
fn a_seed_column_referenced_only_in_a_nested_pattern_is_retained() {
    let (root, res) = run(
        "exec_h217_nested_pattern",
        // Two things are load-bearing in this query. The outer pattern carries a
        // relationship, so `try_stream_match` does not take it. And the nested
        // pattern references the carried column as a *property value*, not as a node
        // variable: an unbound node variable would simply become a fresh binding and
        // match MORE, whereas an unbound property value is null and matches nothing —
        // so the test can actually tell the two apart.
        "MATCH (n:Person) WITH n, n.name AS nm \
         MATCH (x:Person)-[:KNOWS]->(y:Person) \
         WHERE (y)-[:KNOWS]->(:Person {name: nm}) \
         RETURN y.name AS friend ORDER BY friend",
    );
    assert!(
        !res.rows.is_empty(),
        "a nested pattern saw null for a pruned column and matched nothing"
    );
    let _ = std::fs::remove_dir_all(&root);
}
