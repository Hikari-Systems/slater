// SPDX-License-Identifier: Apache-2.0
//! `procedures` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

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

// ── HIK-88: algo.* must honour the memory budget and the query deadline ──────

#[test]
fn algo_bfs_charges_the_intermediate_budget() {
    // BFS from Alice reaches four nodes (Bob, Carol, Acme, Globex), charging two
    // elements per discovered node (one `Val::Node`, one `Val::Rel`) against
    // `maxIntermediate`. Pre-fix the loop grew `nodes`/`edges`/`visited` with no
    // `charge`, so it ran to completion regardless of the budget; now a tiny
    // budget trips before the whole reachable subgraph is materialised. The query
    // keeps the BFS result unexpanded (`RETURN size(nodes)`, no UNWIND) so only
    // the BFS's own charge — not downstream row-building — can trip the budget.
    let q = "MATCH (a:Person {name: 'Alice'}) \
                 CALL algo.BFS(a, 0, NULL) YIELD nodes RETURN size(nodes) AS k";
    let (root, gen, cache, _) = budgeted_engine("exec_algo_bfs_budget", 0);
    // A generous budget completes and reaches all four nodes.
    let res = Engine::new(&gen, &cache)
        .with_max_intermediate(1_000)
        .run(&parser::parse(q).unwrap())
        .expect("a generous budget lets the BFS finish");
    assert!(matches!(res.rows[0][0], Val::Int(4)));
    // A budget below the retained-node charge must abort with the budget error
    // rather than running the BFS to completion.
    let err = Engine::new(&gen, &cache)
        .with_max_intermediate(3)
        .run(&parser::parse(q).unwrap())
        .expect_err("a tiny budget must bound the BFS");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the intermediate-budget error, got: {err:#}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn algo_bfs_observes_the_deadline() {
    // The BFS pop loop now checks the deadline each iteration, so a runaway
    // `algo.BFS(src, 0, NULL)` aborts at `timeoutMs` instead of materialising the
    // whole reachable subgraph uninterruptibly.
    let q = "MATCH (a:Person {name: 'Alice'}) \
                 CALL algo.BFS(a, 0, NULL) YIELD nodes RETURN nodes";
    let (root, gen, cache, _) = budgeted_engine("exec_algo_bfs_deadline", 0);
    let res = Engine::new(&gen, &cache)
        .run(&parser::parse(q).unwrap())
        .expect("no deadline lets the BFS finish");
    assert_eq!(res.rows.len(), 1);
    let err = Engine::new(&gen, &cache)
        .with_deadline(Instant::now() - std::time::Duration::from_secs(1))
        .run(&parser::parse(q).unwrap())
        .expect_err("an elapsed deadline must abort the BFS");
    assert!(
        format!("{err:#}").contains("time limit"),
        "expected the deadline error, got: {err:#}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The whole `build_view`-backed family (every `algo.*` except BFS) exercised by
/// the budget / deadline guards, so a fix to BFS alone can't pass this.
const ALGO_VIEW_PROCS: [&str; 5] = [
    "CALL algo.WCC() YIELD node RETURN node",
    "CALL algo.pageRank(NULL, NULL) YIELD node RETURN node",
    "CALL algo.HarmonicCentrality() YIELD node RETURN node",
    "CALL algo.betweenness() YIELD node RETURN node",
    "CALL algo.labelPropagation() YIELD node RETURN node",
];

#[test]
fn algo_view_procs_charge_the_intermediate_budget() {
    // `build_view` materialises the whole selected subgraph (nodes + position map
    // + out-adjacency) before the algorithm runs. Pre-fix that ignored
    // `maxIntermediate` entirely — an OOM on a large store. Now it charges the
    // node count up front, so a budget below the 5-node fixture trips each proc.
    let (root, gen, cache, _) = budgeted_engine("exec_algo_view_budget", 0);
    for q in ALGO_VIEW_PROCS {
        let ast = parser::parse(q).unwrap();
        Engine::new(&gen, &cache)
            .with_max_intermediate(10_000)
            .run(&ast)
            .unwrap_or_else(|e| panic!("{q}: a generous budget should succeed: {e:#}"));
        let err = Engine::new(&gen, &cache)
            .with_max_intermediate(1)
            .run(&ast)
            .expect_err("a budget below the node count must bound the view");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "{q}: expected the intermediate-budget error, got: {err:#}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn algo_view_procs_observe_the_deadline() {
    // `build_view` checks the deadline as it fills, and each algorithm kernel is
    // threaded an interrupt it polls while working, so the `O(V·E)` centrality
    // procs abort at `timeoutMs` instead of wedging the connection.
    let (root, gen, cache, _) = budgeted_engine("exec_algo_view_deadline", 0);
    for q in ALGO_VIEW_PROCS {
        let ast = parser::parse(q).unwrap();
        Engine::new(&gen, &cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("{q}: no deadline should succeed: {e:#}"));
        let err = Engine::new(&gen, &cache)
            .with_deadline(Instant::now() - std::time::Duration::from_secs(1))
            .run(&ast)
            .expect_err("an elapsed deadline must abort the view proc");
        assert!(
            format!("{err:#}").contains("time limit"),
            "{q}: expected the deadline error, got: {err:#}"
        );
    }
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
