// SPDX-License-Identifier: Apache-2.0
//! `reltype_scan` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── relationship-type scan: identical results with the posting on vs off ───

/// Run `q` over the sparse-reltype fixture and return the sorted display rows.
/// `postings` toggles the endpoint postings: on ⇒ the planner drives typed
/// first hops from the rel-type posting; off ⇒ the identical graph with no
/// postings, so every query falls back to the label scan.
fn rel_rows(tag: &str, q: &str, postings: bool) -> Vec<String> {
    let (root, graph) = if postings {
        testgen::write_rel_sparse(tag)
    } else {
        testgen::write_rel_sparse_no_postings(tag)
    };
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let res = parser::parse(q)
        .map_err(|e| e.to_string())
        .and_then(|ast| engine.run(&ast).map_err(|e| e.to_string()))
        .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"));
    let mut rows: Vec<String> = res
        .rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| v.to_display())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    rows.sort();
    let _ = std::fs::remove_dir_all(&root);
    rows
}

#[test]
fn rel_type_scan_matches_label_scan_results() {
    // Every shape the rel-type scan can fire on must return byte-identical rows
    // to the label-scan plan over the same graph. The fixture: 6 :N nodes,
    // T-edges a->b, b->c (sources {a,b}, targets {b,c}), U-edge a->d.
    let cases = [
        // outgoing 1-hop
        "MATCH (a:N)-[:T]->(b) RETURN a.name AS x, b.name AS y",
        // outgoing 1-hop, unlabelled anchor (base AllNodes)
        "MATCH (a)-[:T]->(b) RETURN a.name AS x, b.name AS y",
        // incoming
        "MATCH (a:N)<-[:T]-(b) RETURN a.name AS x, b.name AS y",
        // undirected
        "MATCH (a:N)-[:T]-(b) RETURN a.name AS x, b.name AS y",
        // 2-hop
        "MATCH (a:N)-[:T]->(b)-[:T]->(c) RETURN c.name AS y",
        // with LIMIT (early-exit path)
        "MATCH (a:N)-[:T]->(b) RETURN b.name AS y LIMIT 1",
        // multi-type union
        "MATCH (a:N)-[:T|U]->(b) RETURN a.name AS x, b.name AS y",
        // count (uncapped, parallel-eligible)
        "MATCH (a:N)-[:T]->(b) RETURN count(*) AS n",
        // OPTIONAL with an unbound anchor: edgeless nodes must not change the
        // outcome — both plans yield the same matched set (and the same
        // null-row behaviour, driven by whether anything matched at all).
        "OPTIONAL MATCH (a:N)-[:T]->(b) RETURN a.name AS x, b.name AS y",
    ];
    for (i, q) in cases.iter().enumerate() {
        let on = rel_rows(&format!("exec_relscan_on_{i}"), q, true);
        let off = rel_rows(&format!("exec_relscan_off_{i}"), q, false);
        assert_eq!(on, off, "rel-scan vs label-scan mismatch for: {q}");
    }
}

#[test]
fn rel_type_scan_concrete_rows() {
    // Pin the actual rows (not just on==off), so a bug that breaks *both*
    // plans identically can't hide. T-edges: a->b, b->c.
    let rows = rel_rows(
        "exec_relscan_concrete",
        "MATCH (a:N)-[:T]->(b) RETURN a.name, b.name",
        true,
    );
    assert_eq!(rows, vec!["a|b".to_string(), "b|c".to_string()]);
}

/// HIK-147 adversarial: the parameterised seek must agree with the literal
/// spelling **exactly**, including on the writable layer, where an `IdSeek` is a
/// narrowing the un-fixed param path never performed. Two ways this could go
/// wrong that a plan-level test cannot see: a delta-**born** id sits above the
/// core's node count (an over-tight bounds check would turn it into a provably-
/// empty seek and silently lose the row), and a **tombstoned** id is still inside
/// the scan bound (an unsuppressed seek would resurrect a deleted node that the
/// old scan path correctly hid).
#[test]
fn param_id_seek_agrees_with_the_scan_on_a_written_delta() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph, _) = testgen::write_basic("exec_param_id_delta");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // Born node (a fresh identity: no `resolved` core id) + a tombstone on core
    // node 1 (Bob). The synthetic base is the core's node count, so the born id
    // lands just past the last core id — exactly as the server seeds it at open.
    let mut mem = Memtable::with_synthetic_base(gen.node_count());
    mem.upsert_node(
        "Person",
        "name",
        Value::Str("Dave".into()),
        None,
        [("name".to_string(), Value::Str("Dave".into()))],
    );
    mem.delete_node("Person", "name", Value::Str("Bob".into()), Some(1));
    let born: Vec<u64> = mem.born_ids_with_label("Person");
    assert_eq!(born.len(), 1, "one born node");
    let born_id = born[0];
    let delta = DeltaSnapshot::from_memtable(Arc::new(mem));
    let view = MergedView::new(&gen, delta);

    let names = |id: i64| -> Vec<String> {
        let params: HashMap<String, Val> = [("p".to_string(), Val::Int(id))].into_iter().collect();
        let engine = Engine::new(&view, &cache).with_params(params);
        let via_param = engine
            .run(&parser::parse("MATCH (n) WHERE id(n) = $p RETURN n.name AS nm").unwrap())
            .unwrap();
        let engine = Engine::new(&view, &cache);
        let via_literal = engine
            .run(
                &parser::parse(&format!("MATCH (n) WHERE id(n) = {id} RETURN n.name AS nm"))
                    .unwrap(),
            )
            .unwrap();
        let p: Vec<String> = via_param.rows.iter().map(|r| r[0].to_display()).collect();
        let l: Vec<String> = via_literal.rows.iter().map(|r| r[0].to_display()).collect();
        assert_eq!(p, l, "param and literal spellings must agree for id {id}");
        p
    };

    assert_eq!(names(0), vec!["Alice"], "an ordinary core node");
    assert!(names(1).is_empty(), "a tombstoned node must stay deleted");
    assert_eq!(
        names(born_id as i64),
        vec!["Dave"],
        "a delta-born id is inside the scan bound and must be found"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-147 adversarial: the re-root the parameterised anchor now unlocks reverses
/// the pattern, so it must not change *which* rows come back on a fixture where
/// direction matters (the star fixture is symmetric and would hide a flipped
/// edge). A chain has exactly one predecessor and one successor per node.
#[test]
fn param_id_reroot_preserves_direction_and_multi_hop_results() {
    let (root, graph) = testgen::write_chain("exec_param_id_reroot_dir", 12);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let both = |q_param: &str, q_lit: &str, id: i64| -> Vec<String> {
        let params: HashMap<String, Val> = [("x".to_string(), Val::Int(id))].into_iter().collect();
        let engine = Engine::new(&gen, &cache).with_params(params);
        let p = engine.run(&parser::parse(q_param).unwrap()).unwrap();
        let engine = Engine::new(&gen, &cache);
        let l = engine.run(&parser::parse(q_lit).unwrap()).unwrap();
        let pv: Vec<String> = p.rows.iter().map(|r| r[0].to_display()).collect();
        let lv: Vec<String> = l.rows.iter().map(|r| r[0].to_display()).collect();
        assert_eq!(pv, lv, "param and literal must agree: {q_param}");
        pv
    };

    // One hop: only the *predecessor* of n5, never the successor.
    assert_eq!(
        both(
            "MATCH (m)-[:R]->(n) WHERE id(n) = $x RETURN m.name AS nm",
            "MATCH (m)-[:R]->(n) WHERE id(n) = 5 RETURN m.name AS nm",
            5,
        ),
        vec!["n4"]
    );
    // Two hops re-rooted from the far end.
    assert_eq!(
        both(
            "MATCH (a)-[:R]->(b)-[:R]->(c) WHERE id(c) = $x RETURN a.name AS nm",
            "MATCH (a)-[:R]->(b)-[:R]->(c) WHERE id(c) = 5 RETURN a.name AS nm",
            5,
        ),
        vec!["n3"]
    );
    // The reverse spelling of the same pattern must still see the successor.
    assert_eq!(
        both(
            "MATCH (m)<-[:R]-(n) WHERE id(n) = $x RETURN m.name AS nm",
            "MATCH (m)<-[:R]-(n) WHERE id(n) = 5 RETURN m.name AS nm",
            5,
        ),
        vec!["n6"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-150 (a): on a multigraph, **one** `DELETE r` suppresses **every** parallel core edge
/// of that reltype to that neighbour — the delta keys an edge by `(src, reltype, dst)` and
/// the read overlay honours that identity semantics (`exec.rs`'s `suppress` set), which
/// `flush_segment.rs` documents as deliberate.
///
/// A maintained degree that decrements once per tombstone therefore disagrees with what the
/// adjacency overlay actually emits, by (parallel multiplicity − 1).
///
/// Asserted against `outgoing_adj().len()` — the overlay's *own* answer — rather than a
/// hand-computed number, because the claim is precisely that the two disagree. A hard-coded
/// expectation would only be testing my arithmetic.
#[test]
fn a_parallel_edge_delete_makes_the_maintained_degree_disagree_with_the_overlay() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph) = testgen::write_multigraph("degree_parallel");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // Sanity on the fixture itself: a has 4 outgoing edges, 3 of them parallel to b.
    let clean = MergedView::read_only(&gen);
    assert_eq!(
        Engine::new(&clean, &cache).outgoing_adj(0).unwrap().len(),
        4
    );

    // One `DELETE r` on the (a)-[:R]->(b) identity.
    let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
    mem.delete_edge(
        "N",
        "name",
        Value::Str("a".into()),
        "R",
        "N",
        "name",
        Value::Str("b".into()),
        Some(0),
        Some(1),
    );
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let eng = Engine::new(&view, &cache);

    // The overlay drops all three parallel edges, leaving only a→c.
    let overlaid = eng.outgoing_adj(0).unwrap().len() as u64;
    assert_eq!(overlaid, 1, "identity semantics drop every parallel edge");

    // The maintained degree must not claim otherwise. Before HIK-150 it answered 3
    // (4 − 1), and nothing declined the fast path over it.
    match eng.directed_edge_count(0, true) {
        Ok(deg) => assert_eq!(
            deg, overlaid,
            "maintained out-degree disagrees with the adjacency overlay"
        ),
        Err(e) => assert!(
            e.downcast_ref::<crate::exec::DegreeNotExact>().is_some(),
            "a refusal must be the typed precondition error, got: {e:#}"
        ),
    }
    std::fs::remove_dir_all(&root).ok();
}

/// HIK-150 (b): `delete_edge` inserts a tombstone unconditionally — it never checks that a
/// core edge with that identity exists. Decrementing per tombstone therefore under-counts a
/// node whose delete matched nothing.
///
/// `b` has exactly one real outgoing edge (b→c); deleting the edge (b)-[:R]->(a), which the
/// core does not contain, must not change its degree.
#[test]
fn deleting_an_edge_that_does_not_exist_must_not_lower_the_degree() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph) = testgen::write_multigraph("degree_absent");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
    mem.delete_edge(
        "N",
        "name",
        Value::Str("b".into()),
        "R",
        "N",
        "name",
        Value::Str("a".into()), // no such core edge
        Some(1),
        Some(0),
    );
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let eng = Engine::new(&view, &cache);

    let overlaid = eng.outgoing_adj(1).unwrap().len() as u64;
    assert_eq!(
        overlaid, 1,
        "b→c is untouched by a delete that matched nothing"
    );

    // Before HIK-150 this answered 0 (1 − 1).
    match eng.directed_edge_count(1, true) {
        Ok(deg) => assert_eq!(
            deg, overlaid,
            "a delete that matched no core edge must not decrement the degree"
        ),
        Err(e) => assert!(
            e.downcast_ref::<crate::exec::DegreeNotExact>().is_some(),
            "a refusal must be the typed precondition error, got: {e:#}"
        ),
    }
    std::fs::remove_dir_all(&root).ok();
}

/// HIK-150: with a live edge delete, `degree_terminal_dir` must **decline** — and the query
/// must still answer correctly by walking.
///
/// This is the guard that keeps the two halves of the fix honest together. Refusing inside
/// `directed_edge_count` alone would turn a wrong answer into a failed query; declining here
/// is what keeps the query working at all.
#[test]
fn a_live_edge_delete_declines_the_degree_terminal_fast_path() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    fn pattern_of(q: &str) -> crate::parser::ast::Pattern {
        let ast = parser::parse(q).unwrap();
        let crate::parser::ast::Clause::Match(m) = &ast.head.reading[0] else {
            panic!("not a match: {q}");
        };
        m.patterns[0].clone()
    }

    let (root, graph) = testgen::write_multigraph("degterm_edge_tomb");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let q = "MATCH (n:N)-[]->(m) RETURN count(m)";

    // Baseline: no delete ⇒ the fast path arms, and it is right (5 core edges).
    let clean = MergedView::read_only(&gen);
    assert!(
        Engine::new(&clean, &cache)
            .degree_terminal_dir(&pattern_of(q))
            .is_some(),
        "the fast path must still arm on a delta with no deletes"
    );
    let ast = parser::parse(q).unwrap();
    assert!(matches!(
        Engine::new(&clean, &cache).run(&ast).unwrap().rows[0][0],
        Val::Int(5)
    ));

    // One `DELETE r` ⇒ decline, and the walked answer is the overlay's (3 parallel edges
    // suppressed by the single identity tombstone, leaving a→c and b→c).
    let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
    mem.delete_edge(
        "N",
        "name",
        Value::Str("a".into()),
        "R",
        "N",
        "name",
        Value::Str("b".into()),
        Some(0),
        Some(1),
    );
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let eng = Engine::new(&view, &cache);
    assert!(
        eng.degree_terminal_dir(&pattern_of(q)).is_none(),
        "a live edge delete must decline the maintained-degree fast path"
    );
    assert!(
        matches!(eng.run(&ast).unwrap().rows[0][0], Val::Int(2)),
        "the walked count must be the overlay's own answer"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// HIK-151: a live delta must not disable the degree-sum count walk (Stage B).
///
/// Stage B sat inside a gate written for Stage 7 — `delta().is_empty() &&
/// core_stack().is_singleton()` — whose justification (the grouped-index path walks the base
/// range index and histograms, which are not segment-aware) is about Stage 7 alone. Stage B
/// walks through the ordinary segment- and delta-aware seams and needs none of it, so one
/// `MERGE` turned the 91.6M 3-hop hub count back from 0.8 s into a full walk.
///
/// Observed through `ADJ_VISIT_COUNT` rather than a timer: with Stage B armed, the final hop
/// is answered from maintained degrees and its edges are never handed to
/// `for_each_adj_overlaid` at all. The delta here is a **property patch**, so the graph's
/// topology — and therefore the correct answer and the correct visit count — is identical
/// with and without it. Any difference is the gate, not the data.
#[test]
fn a_live_delta_must_not_disable_the_degree_sum_count_walk() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph) = testgen::write_diamond("stageb_live_delta");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let q = "MATCH (x)-[]->()-[]->(z) RETURN count(*)";
    let visits = || ADJ_VISIT_COUNT.with(|c| c.get());
    let reset = || ADJ_VISIT_COUNT.with(|c| c.set(0));

    let run = |view: &MergedView| -> (i64, u64) {
        reset();
        let ast = parser::parse(q).unwrap();
        let r = Engine::new(view, &cache).run(&ast).unwrap();
        let Val::Int(n) = r.rows[0][0] else {
            panic!("count not int")
        };
        (n, visits())
    };

    // Cold graph: the fast path arms, so the final hop costs no adjacency visits.
    let cold = MergedView::read_only(&gen);
    let (n_cold, v_cold) = run(&cold);

    // One property patch — no topology change whatsoever, but the delta is now non-empty.
    let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
    mem.upsert_node(
        "N",
        "name",
        Value::Str("s".into()),
        Some(0),
        [("touched".to_string(), Value::Int(1))],
    );
    let live = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let (n_live, v_live) = run(&live);

    assert_eq!(
        n_live, n_cold,
        "a property patch cannot change an edge count"
    );
    assert_eq!(
        v_live, v_cold,
        "one MERGE must not disable the degree-sum count fast path: the live delta walked \
         {v_live} adjacency edges where the cold graph walked {v_cold}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// HIK-151: with Stage B now reachable on a live graph, its answer must equal the
/// materialising walk's answer in every delta shape it does not decline.
///
/// Both sides are computed on the **same** view — `count(*)` (Stage B) against
/// `RETURN z` row count (full materialisation) — so neither is a hand-computed constant and
/// a shared misunderstanding of the fixture cannot make both wrong in the same direction.
///
/// The four shapes: cold; a delta-born edge; a delta-born edge plus a **node tombstone**
/// (which `degree_terminal_dir` declines, so Stage B still counts but walks the last hop);
/// and an **edge tombstone** (declined for the reason HIK-150 established).
#[test]
fn the_count_walk_agrees_with_the_materialising_walk_on_a_live_delta() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph) = testgen::write_diamond("stageb_agreement");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let check = |view: &MergedView, what: &str| {
        let counted = {
            let ast = parser::parse("MATCH (x)-[]->()-[]->(z) RETURN count(*)").unwrap();
            match Engine::new(view, &cache).run(&ast).unwrap().rows[0][0] {
                Val::Int(n) => n as usize,
                ref v => panic!("count not int: {v:?}"),
            }
        };
        let materialised = {
            let ast = parser::parse("MATCH (x)-[]->()-[]->(z) RETURN z").unwrap();
            Engine::new(view, &cache).run(&ast).unwrap().rows.len()
        };
        assert_eq!(
            counted, materialised,
            "{what}: the count walk and the materialising walk must agree"
        );
        counted
    };

    // (a) cold.
    let cold = MergedView::read_only(&gen);
    let n_cold = check(&cold, "cold");
    assert!(n_cold > 0, "the fixture must produce rows to compare");

    // (b) a delta-born edge — the born term of the maintained degree.
    let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
    mem.upsert_edge(
        "N",
        "name",
        Value::Str("t".into()), // node 4
        "R",
        "N",
        "name",
        Value::Str("s".into()), // node 0
        Some(4),
        Some(0),
        [],
    );
    let born = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let n_born = check(&born, "delta-born edge");
    assert!(
        n_born > n_cold,
        "a born edge closing the diamond must add rows ({n_born} vs {n_cold})"
    );

    // (c) a born edge plus a node tombstone — `degree_terminal_dir` declines, so the final
    //     hop is walked; the count must still agree.
    let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
    mem.upsert_edge(
        "N",
        "name",
        Value::Str("t".into()),
        "R",
        "N",
        "name",
        Value::Str("s".into()),
        Some(4),
        Some(0),
        [],
    );
    mem.delete_node("N", "name", Value::Str("b".into()), Some(2));
    let tombstoned = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    check(&tombstoned, "born edge + node tombstone");

    // (d) an edge tombstone — declined for HIK-150's reason; the count must still agree.
    let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
    mem.delete_edge(
        "N",
        "name",
        Value::Str("s".into()),
        "R",
        "N",
        "name",
        Value::Str("a".into()),
        Some(0),
        Some(1),
    );
    let edge_tomb = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    check(&edge_tomb, "edge tombstone");

    std::fs::remove_dir_all(&root).ok();
}
