// SPDX-License-Identifier: Apache-2.0
//! `gql` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

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
