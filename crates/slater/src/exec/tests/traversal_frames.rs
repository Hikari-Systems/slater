// SPDX-License-Identifier: Apache-2.0
//! `traversal_frames` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Stage 6 — traversal-frame characterization ───────────────────────────
// These lock the exact result set of the multi-hop / variable-length walk so
// the mutate-in-place binding frame (replacing the per-hop `binding.clone()`)
// is provably result-preserving. They pass on the pre-Stage-6 code and must
// still pass byte-for-byte after the rewrite.

#[test]
fn frame_two_hop_chain_exact_rows() {
    // KNOWS Person→Person edges: Alice→Bob, Bob→Carol, Alice→Carol. The only
    // length-2 KNOWS chain is Alice→Bob→Carol (Carol has no outgoing KNOWS).
    let (root, res) = run(
        "exec_frame_2hop",
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             RETURN a.name AS a, b.name AS b, c.name AS c",
    );
    let rows: Vec<(String, String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display(), r[2].to_display()))
        .collect();
    assert_eq!(rows, vec![("Alice".into(), "Bob".into(), "Carol".into())]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_three_hop_chain_exact_rows() {
    // Headline-shaped 3-hop: KNOWS, KNOWS, WORKS_AT. The only walk is
    // Alice→Bob→Carol→Globex (Carol WORKS_AT Globex).
    let (root, res) = run(
        "exec_frame_3hop",
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:WORKS_AT]->(d) \
             RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d",
    );
    let rows: Vec<(String, String, String, String)> = res
        .rows
        .iter()
        .map(|r| {
            (
                r[0].to_display(),
                r[1].to_display(),
                r[2].to_display(),
                r[3].to_display(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![(
            "Alice".into(),
            "Bob".into(),
            "Carol".into(),
            "Globex".into()
        )]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_sibling_branch_binding_isolation() {
    // The specific frame risk: Alice has TWO KNOWS siblings (Bob, Carol). Only
    // the Bob branch extends (Bob→Carol); the Carol branch dead-ends. If a
    // sibling fails to restore the mid binding `b` on backtrack, the Carol
    // branch would leak `b = Bob` and fabricate rows. Exactly one row proves
    // each branch is isolated.
    let (root, res) = run(
        "exec_frame_sibling",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN b.name AS b, c.name AS c",
    );
    let rows: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(rows, vec![("Bob".into(), "Carol".into())]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_same_end_node_via_two_paths() {
    // Carol is reachable from Alice by two distinct KNOWS paths — direct
    // (Alice→Carol) and via Bob (Alice→Bob→Carol). Both must survive as
    // separate rows; the frame must not collapse or duplicate them.
    let (root, res) = run(
        "exec_frame_twopaths",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS*1..2]->(c:Person {name:'Carol'}) \
             RETURN c.name AS c",
    );
    assert_eq!(col0(&res), vec!["Carol", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_undirected_traversal() {
    // Bob's KNOWS edges: incoming from Alice (e0), outgoing to Carol (e1).
    // Undirected sees both.
    let (root, res) = run(
        "exec_frame_undirected",
        "MATCH (a:Person {name:'Bob'})-[:KNOWS]-(x) RETURN x.name AS x",
    );
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_where_references_mid_pattern_var() {
    // A WHERE on the mid node `b` (evaluated against the full row scope) keeps
    // only the chain through Bob.
    let (root, res) = run(
        "exec_frame_midwhere",
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE b.name = 'Bob' RETURN a.name AS a, c.name AS c",
    );
    let rows: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(rows, vec![("Alice".into(), "Carol".into())]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_multipattern_comma_join_shared_var() {
    // Two comma-joined patterns sharing `b`: pattern 1 binds b∈{Bob,Carol};
    // pattern 2 (b)-[:KNOWS]->(c) only extends from Bob.
    let (root, res) = run(
        "exec_frame_comma",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b), (b)-[:KNOWS]->(c) \
             RETURN b.name AS b, c.name AS c",
    );
    let rows: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(rows, vec![("Bob".into(), "Carol".into())]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_varlen_zero_length_includes_self() {
    // `*0..1`: zero hops binds the anchor itself (Alice); one hop adds its
    // KNOWS neighbours.
    let (root, res) = run(
        "exec_frame_varlen0",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS*0..1]->(b) RETURN b.name AS b",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_varlen_relationship_uniqueness() {
    // Undirected `*2..2` from Bob must not reuse an edge within a path: the
    // walks are Bob-e0-Alice-e4-Carol and Bob-e1-Carol-e4-Alice. Reusing e0/e1
    // would step back to Bob — so a "Bob" in the result would mean uniqueness
    // is broken.
    let (root, res) = run(
        "exec_frame_unique",
        "MATCH (a:Person {name:'Bob'})-[:KNOWS*2..2]-(x) RETURN x.name AS x",
    );
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_path_var_walk_order() {
    // The path scratch buffer must yield nodes/relationships in walk order
    // (Alice→Bob→Carol = ids 0,1,2; edges e0,e1 = ids 0,1) after the frame
    // push/pop rewrite.
    let (root, res) = run(
        "exec_frame_pathorder",
        "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN [n IN nodes(p) | id(n)] AS ns, [r IN relationships(p) | id(r)] AS rs",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(render(&res.rows[0][0]), "[0,1,2]");
    assert_eq!(render(&res.rows[0][1]), "[0,1]");
    let _ = std::fs::remove_dir_all(&root);
}
