// SPDX-License-Identifier: Apache-2.0
//! `patterns_and_paths` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

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
