// SPDX-License-Identifier: Apache-2.0
//! `expressions` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

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
    assert!(run_err(
        "exec_p5_reduce_e1",
        "RETURN reduce(sum = 'a', n in [1,2,3] | sum * n)"
    )
    .contains("cannot apply arithmetic"));
    assert!(run_err(
        "exec_p5_reduce_e2",
        "RETURN reduce(sum = 1, n in 2 | sum + n)"
    )
    .contains("needs a list"));
    // A reduce missing its `| body` is a plain function call over the
    // would-be accumulator binding `sum`, which is unbound -> runtime error.
    assert!(run_err("exec_p5_reduce_e3", "RETURN reduce(sum = 0, n in [1,2,3])").contains("'sum'"));
    let _ = std::fs::remove_dir_all(&root);
}
