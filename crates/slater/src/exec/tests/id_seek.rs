// SPDX-License-Identifier: Apache-2.0
//! `id_seek` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── id() seek pushdown — end-to-end correctness ────────────────────────────
// Fixture ids: [0]Alice [1]Bob [2]Carol (Person), [3]Acme [4]Globex (Company).
// Edges: Alice-KNOWS->Bob, Bob-KNOWS->Carol, Alice-WORKS_AT->Acme,
//        Carol-WORKS_AT->Globex, Alice-KNOWS->Carol.

#[test]
fn id_seek_returns_the_one_node() {
    let (root, res) = run(
        "exec_id_seek",
        "MATCH (n) WHERE id(n) = 1 RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_seek_drives_expansion_without_full_scan() {
    // Lab's neighbourhood-expansion shape. Anchor `n` is seeked to Alice(0),
    // then expanded — the result is exactly Alice's out-neighbours.
    let (root, res) = run(
        "exec_id_seek_expand",
        "MATCH (n)-[r]->(m) WHERE id(n) = 0 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_seek_still_enforces_label() {
    // Node 0 is Alice (Person), not a Company → the residual label check on the
    // seeked candidate yields nothing.
    let (root, res) = run(
        "exec_id_seek_label",
        "MATCH (n:Company) WHERE id(n) = 0 RETURN n.name AS name",
    );
    assert!(res.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_seek_still_enforces_extra_predicate() {
    // id(n)=0 seeks Alice, but the AND-ed name predicate is for Bob → empty.
    let (root, res) = run(
        "exec_id_seek_pred_no",
        "MATCH (n) WHERE id(n) = 0 AND n.name = 'Bob' RETURN n.name AS name",
    );
    assert!(res.rows.is_empty());
    // The matching companion: same id, the right name → one row.
    let (root2, res2) = run(
        "exec_id_seek_pred_yes",
        "MATCH (n) WHERE id(n) = 0 AND n.name = 'Alice' RETURN n.name AS name",
    );
    assert_eq!(col0(&res2), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

#[test]
fn id_under_or_returns_all_disjuncts() {
    // THE wrong-results guard: if the seek wrongly fired on the OR it would
    // return only one node. Both must come back.
    let (root, res) = run(
        "exec_id_or",
        "MATCH (n) WHERE id(n) = 0 OR id(n) = 2 RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_in_list_returns_each() {
    let (root, res) = run(
        "exec_id_in",
        "MATCH (n) WHERE id(n) IN [0, 2, 99] RETURN n.name AS name",
    );
    // 99 is out of range and contributes nothing.
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_out_of_range_returns_empty() {
    let (root, res) = run(
        "exec_id_oor",
        "MATCH (n) WHERE id(n) = 999 RETURN n.name AS name",
    );
    assert!(res.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_negative_returns_empty() {
    let (root, res) = run(
        "exec_id_neg",
        "MATCH (n) WHERE id(n) = -5 RETURN n.name AS name",
    );
    assert!(res.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_seek_with_disjunction_companion_predicate() {
    // `id(n) = 0 AND (name='Alice' OR name='Zzz')`: the seek narrows to Alice,
    // the parenthesised OR is re-checked as a residual → Alice stays.
    let (root, res) = run(
        "exec_id_and_or",
        "MATCH (n) WHERE id(n) = 0 AND (n.name = 'Alice' OR n.name = 'Zzz') RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ── id() seek with anchor re-rooting (id on the far end of the traversal) ───

#[test]
fn id_on_end_reroots_outgoing_expansion() {
    // `(m)-[r]->(n) WHERE id(n)=1`: id is on the END node n (Bob). Re-rooting
    // seeks Bob and walks the edge backwards → m is whoever points to Bob: Alice.
    let (root, res) = run(
        "exec_reroot_out",
        "MATCH (m)-[r]->(n) WHERE id(n) = 1 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_on_end_reroots_incoming_expansion() {
    // `(m)<-[r]-(n) WHERE id(n)=0`: n is Alice; m is each of Alice's
    // out-neighbours (Bob, Acme, Carol) — same as a forward expansion from her.
    let (root, res) = run(
        "exec_reroot_in",
        "MATCH (m)<-[r]-(n) WHERE id(n) = 0 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn reroot_matches_unrerooted_result_set() {
    // Both Bob and Alice point to Carol(2); re-rooting must find both.
    let (root, res) = run(
        "exec_reroot_multi",
        "MATCH (m)-[r]->(n) WHERE id(n) = 2 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn reroot_still_enforces_end_label() {
    // Acme(3) is a Company reached from Alice via WORKS_AT → one row.
    let (root, res) = run(
        "exec_reroot_label_ok",
        "MATCH (m)-[r]->(n:Company) WHERE id(n) = 3 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice"]);
    // Bob(1) is a Person, so the :Company constraint on the seeked end empties it.
    let (root2, res2) = run(
        "exec_reroot_label_no",
        "MATCH (m)-[r]->(n:Company) WHERE id(n) = 1 RETURN m.name AS name",
    );
    assert!(res2.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

#[test]
fn varlength_end_id_is_not_rerooted_but_correct() {
    // A `*` hop is excluded from re-rooting (order of a returned rel-list could
    // change); the result must still be correct via the normal scan. Paths
    // ending at Carol(2): Bob→Carol, Alice→Carol, Alice→Bob→Carol ⇒ {Alice,Bob}.
    let (root, res) = run(
        "exec_reroot_varlen",
        "MATCH (m)-[r*1..2]->(n) WHERE id(n) = 2 RETURN DISTINCT m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}
