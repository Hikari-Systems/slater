// SPDX-License-Identifier: Apache-2.0
//! `compaction` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// Phase 5 slice 5.1: **T3 segment compaction**. Two flushes stack two upper segments;
/// `compact_graph_segments` folds them into one merged segment that reads **identically** to
/// the run it replaces — a births-only pair, a base-node indexed patch (index-removal carry),
/// and a cross-segment node-row override (newest-wins) all resolve the same before and after,
/// the stack shrinks to one segment, the id space is preserved, the delta is rebound, and the
/// merged data survives a reopen.
#[test]
fn compact_segments_folds_a_run_into_one() {
    // `write_basic`: Alice/Bob/Carol :Person (name+age indexed, ages 30/25/40),
    // Acme/Globex :Company, base edge Alice-KNOWS->Bob among others.
    let (root, _g, _u) = testgen::write_basic("compact_seg_e2e");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let base_uuid = graphs.get("people").unwrap().uuid();

    let write = |graphs: &Graphs, q: &str| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => {
                execute_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            _ => panic!("expected a write: {q}"),
        }
    };
    let q = |graphs: &Graphs, query: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let ast = parser::parse(query).unwrap();
        let r = Engine::new(&view, &cache).run(&ast).unwrap();
        r
    };

    // Flush 1: a born node (Dave, indexed name+age), a born edge (Dave-KNOWS->Alice), and a
    // base-node **indexed** patch (Carol's age 40→99 — a below-run index removal + entry).
    write(&graphs, "MATCH (n:Person {name:'Carol'}) SET n.age = 99");
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(
        &graphs,
        "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Alice'})",
    );
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("flush 1 is non-empty");

    // Flush 2: another born node (Frank) and a **cross-segment override** of the same base
    // node's indexed age (Carol 99→77). Carol is a base node, so the write path re-resolves
    // her by key in both flushes; the merge must newest-wins her row (77) and suppress both
    // the base value (40) and segment 1's intermediate value (99) in the index. (Note that a
    // just-flushed *born* key like Dave cannot be re-patched by the write path until Phase 6
    // makes resolve segment-aware — see the plan's 4.1 note (e) — so the override targets the
    // base node Carol, which resolves in both flushes.)
    write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
    write(&graphs, "MATCH (n:Person {name:'Carol'}) SET n.age = 77");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("flush 2 is non-empty");

    let pre = graphs.get("people").unwrap();
    assert_eq!(
        pre.stack().segments().len(),
        2,
        "two upper segments stacked"
    );
    let old_node_total = pre.stack().extents().nodes.total();
    let old_edge_total = pre.stack().extents().edges.total();
    drop(pre);

    // The battery of probes that must read identically before and after the compaction.
    // `Val` has no `PartialEq`, so each probe result is captured as its debug string.
    let probe = |graphs: &Graphs| -> Vec<String> {
        let s = |v: &Val| format!("{v:?}");
        // A one-row scalar, or a marker for the row count when the seek should be empty.
        let scalar = |r: &QueryResult| r.rows.first().map_or("∅".into(), |row| s(&row[0]));
        // The reverse-adjacency probe onto the base node Alice (its row count matters too).
        let rev = q(
            graphs,
            "MATCH (b:Person {name:'Alice'})<-[:KNOWS]-(a) RETURN a.name",
        );
        vec![
            // 1. cross-segment override of a base node: Carol seeks by her newest age (77);
            //    the base value (40) and segment 1's intermediate value (99) are suppressed.
            scalar(&q(graphs, "MATCH (n:Person {age:77}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:99}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:40}) RETURN n.name")),
            s(&q(graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age").rows[0][0]),
            // 2. born nodes, each by its born age index.
            scalar(&q(graphs, "MATCH (n:Person {age:50}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:70}) RETURN n.name")),
            // 3. node count over summed marginals (3 base Person + Dave + Frank).
            s(&q(graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0]),
            // 4. born edge traverses to its base target (forward) …
            s(&q(
                graphs,
                "MATCH (a:Person {name:'Dave'})-[:KNOWS]->(b) RETURN b.name",
            )
            .rows[0][0]),
            // 5. … and reverse (incoming KNOWS onto Alice — only the born edge).
            format!("rev_rows={}", rev.rows.len()),
            scalar(&rev),
        ]
    };
    let before = probe(&graphs);
    // Sanity-check the ground truth so a bug can't make before==after both wrong.
    assert_eq!(
        before[0], "Str(\"Carol\")",
        "Carol by newest indexed age 77"
    );
    assert_eq!(before[1], "∅", "segment-1 intermediate age 99 suppressed");
    assert_eq!(before[2], "∅", "base age 40 suppressed");
    assert_eq!(before[3], "Int(77)", "Carol newest age");
    assert_eq!(before[4], "Str(\"Dave\")", "Dave by born age 50");
    assert_eq!(before[5], "Str(\"Frank\")", "Frank by born age 70");
    assert_eq!(before[6], "Int(5)", "3 base Person + 2 born");
    assert_eq!(before[7], "Str(\"Alice\")", "forward edge target");
    assert_eq!(before[8], "rev_rows=1", "one incoming KNOWS on Alice");
    assert_eq!(before[9], "Str(\"Dave\")", "reverse edge source");

    // Compact the run [0, 2) into one segment.
    let set_uuid = graphs
        .compact_graph_segments("people", &vc, &root, 0, 2)
        .unwrap();

    let post = graphs.get("people").unwrap();
    assert_eq!(post.uuid(), set_uuid, "served the compacted set");
    assert_eq!(post.base_uuid(), base_uuid, "base preserved by compaction");
    assert_eq!(
        post.stack().segments().len(),
        1,
        "run folded into one segment"
    );
    assert_eq!(
        post.stack().extents().nodes.total(),
        old_node_total,
        "node id space invariant under compaction"
    );
    assert_eq!(
        post.stack().extents().edges.total(),
        old_edge_total,
        "edge id space invariant under compaction"
    );
    drop(post);

    // The writer is rebound to the new set (ids unchanged, delta preserved).
    assert_eq!(
        graphs.writer("people").unwrap().core_uuid(),
        set_uuid,
        "delta rebound to the compacted set"
    );

    // Every probe reads identically through the merged segment.
    assert_eq!(probe(&graphs), before, "compaction preserves every read");

    // Reopen from disk: the compacted set + merged segment reload and survive. Re-enable
    // the writable layer so the shared probe closure has a writer (the delta was retired by
    // the flushes and rebound by the compaction, so it reloads empty).
    drop(graphs);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    assert_eq!(
        graphs.get("people").unwrap().uuid(),
        set_uuid,
        "reopen names the compacted set"
    );
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        1,
        "merged segment reloaded"
    );
    assert_eq!(
        probe(&graphs),
        before,
        "reopened compaction preserves every read"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 5 slice 5.3 (admission policy): the size-tiered auto entry point
/// [`Graphs::compact_graph_segments_auto`] admits a compaction only when the stack exceeds
/// `max_upper_segments`, and then folds the selected run through the same T3 writer. Three
/// similarly-sized flushes stack three segments; `auto` with a threshold ≥ 3 (or 0) is a
/// no-op, while a threshold of 2 admits and — the three being one tier — folds the whole run
/// into one. Every read is identical across the no-ops, the fold, and a reopen.
#[test]
fn auto_compaction_admits_only_when_over_budget() {
    let (root, _g, _u) = testgen::write_basic("compact_auto_e2e");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let write = |graphs: &Graphs, q: &str| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => {
                execute_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            _ => panic!("expected a node write: {q}"),
        }
    };
    let q = |graphs: &Graphs, query: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let r = Engine::new(&view, &cache)
            .run(&parser::parse(query).unwrap())
            .unwrap();
        r
    };

    // Three flushes, one born indexed node each ⇒ three similarly-sized upper segments.
    for (name, age) in [("Dave", 50), ("Frank", 60), ("Gina", 70)] {
        write(
            &graphs,
            &format!("MERGE (n:Person {{name:'{name}'}}) SET n.age = {age}"),
        );
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .expect("flush is non-empty");
    }
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        3,
        "three upper segments stacked"
    );

    let probe = |graphs: &Graphs| -> Vec<String> {
        let s = |r: QueryResult| format!("{:?}", r.rows[0][0]);
        vec![
            s(q(graphs, "MATCH (n:Person {age:50}) RETURN n.name")),
            s(q(graphs, "MATCH (n:Person {age:60}) RETURN n.name")),
            s(q(graphs, "MATCH (n:Person {age:70}) RETURN n.name")),
            s(q(graphs, "MATCH (n:Person) RETURN count(*)")),
        ]
    };
    let before = probe(&graphs);
    assert_eq!(before[0], "Str(\"Dave\")");
    assert_eq!(before[3], "Int(6)", "3 base Person + 3 born");

    // Within budget (threshold ≥ segment count) and disabled (0) are both no-ops.
    assert_eq!(
        graphs
            .compact_graph_segments_auto("people", &vc, &root, 3)
            .unwrap(),
        None,
        "3 segments, threshold 3 ⇒ within budget"
    );
    assert_eq!(
        graphs
            .compact_graph_segments_auto("people", &vc, &root, 0)
            .unwrap(),
        None,
        "threshold 0 ⇒ admission disabled"
    );
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        3,
        "no-op auto calls left the stack untouched"
    );

    // Over budget (threshold 2 < 3): admit and fold. The three are one tier ⇒ whole run.
    let set_uuid = graphs
        .compact_graph_segments_auto("people", &vc, &root, 2)
        .unwrap()
        .expect("threshold 2 admits a compaction");
    let post = graphs.get("people").unwrap();
    assert_eq!(post.uuid(), set_uuid, "served the compacted set");
    assert_eq!(
        post.stack().segments().len(),
        1,
        "the one-tier run folded into a single segment"
    );
    drop(post);
    assert_eq!(
        graphs.writer("people").unwrap().core_uuid(),
        set_uuid,
        "delta rebound to the compacted set"
    );
    assert_eq!(
        probe(&graphs),
        before,
        "auto-compaction preserves every read"
    );

    // Now within budget again (1 segment) ⇒ auto is a no-op.
    assert_eq!(
        graphs
            .compact_graph_segments_auto("people", &vc, &root, 2)
            .unwrap(),
        None,
        "1 segment, threshold 2 ⇒ nothing left to admit"
    );

    // Reopen: the compacted set reloads and every read survives.
    drop(graphs);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        1,
        "merged segment reloaded"
    );
    assert_eq!(
        probe(&graphs),
        before,
        "reopened auto-compaction preserves every read"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 5 slice 5.2 (merge hardening): a **base-node delete** materialised in the older
/// segment of a run folds correctly through a T3 merge. Bob is a base node, so his
/// tombstone and the `removed` fragments for his two incident base edges are **below-run**
/// — the merge must *carry* them (nothing beneath the run holds Bob), keeping him and his
/// edges gone, while the summed marginals net the delete. A born node in the newer segment
/// tiles above. Every read is identical before and after the compaction and after a reopen.
#[test]
fn compact_folds_a_base_delete_across_the_run() {
    // `write_basic`: Alice/Bob/Carol :Person; KNOWS edges Alice→Bob, Bob→Carol, Alice→Carol.
    let (root, _g, _u) = testgen::write_basic("compact_del_e2e");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let base_uuid = graphs.get("people").unwrap().uuid();

    let write = |graphs: &Graphs, qy: &str| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        match parser::parse_statement(qy).unwrap() {
            parser::ast::Statement::Write(w) => {
                execute_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            _ => panic!("expected a write: {qy}"),
        }
    };
    let q = |graphs: &Graphs, qy: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let snap = graphs
            .writer("people")
            .map(|w| w.delta_snapshot())
            .unwrap_or_else(DeltaSnapshot::empty);
        let view = MergedView::new(gen.as_ref(), snap);
        let r = Engine::new(&view, &cache)
            .run(&parser::parse(qy).unwrap())
            .unwrap();
        r
    };

    // Flush 1 (seg 0): DETACH DELETE Bob — a base-node tombstone + `removed` fragments for
    // his incident KNOWS edges (Alice→Bob on Alice's out side, Bob→Carol on Carol's in side).
    write(&graphs, "MATCH (n:Person {name:'Bob'}) DETACH DELETE n");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("delete flush is non-empty");
    // Flush 2 (seg 1): a born node so the run has two segments to fold.
    write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("born flush is non-empty");

    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        2,
        "two upper segments stacked"
    );
    let old_node_total = graphs
        .get("people")
        .unwrap()
        .stack()
        .extents()
        .nodes
        .total();
    let old_edge_total = graphs
        .get("people")
        .unwrap()
        .stack()
        .extents()
        .edges
        .total();

    let probe = |graphs: &Graphs| -> Vec<String> {
        let s = |v: &Val| format!("{v:?}");
        let scalar = |r: &QueryResult| r.rows.first().map_or("∅".into(), |row| s(&row[0]));
        let fwd = q(
            graphs,
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name",
        );
        vec![
            scalar(&q(graphs, "MATCH (n:Person {name:'Bob'}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:25}) RETURN n.name")),
            s(&q(graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0]),
            s(&q(graphs, "MATCH ()-[:KNOWS]->() RETURN count(*)").rows[0][0]),
            // Bob's delete removed Alice→Bob and Bob→Carol; Alice→Carol survives.
            format!("fwd_rows={}", fwd.rows.len()),
            scalar(&fwd),
            scalar(&q(graphs, "MATCH (n:Person {age:70}) RETURN n.name")),
        ]
    };
    let before = probe(&graphs);
    assert_eq!(before[0], "∅", "Bob gone from the name index");
    assert_eq!(before[1], "∅", "Bob gone from the age index");
    assert_eq!(before[2], "Int(3)", "Alice, Carol, Frank survive");
    assert_eq!(before[3], "Int(1)", "only Alice→Carol KNOWS remains");
    assert_eq!(before[4], "fwd_rows=1", "Alice keeps one KNOWS out-edge");
    assert_eq!(before[5], "Str(\"Carol\")", "…to Carol");
    assert_eq!(before[6], "Str(\"Frank\")", "born Frank by age 70");

    let set_uuid = graphs
        .compact_graph_segments("people", &vc, &root, 0, 2)
        .unwrap();

    let post = graphs.get("people").unwrap();
    assert_eq!(post.uuid(), set_uuid, "served the compacted set");
    assert_eq!(post.base_uuid(), base_uuid, "base preserved");
    assert_eq!(post.stack().segments().len(), 1, "run folded into one");
    assert_eq!(
        post.stack().extents().nodes.total(),
        old_node_total,
        "node id space invariant"
    );
    assert_eq!(
        post.stack().extents().edges.total(),
        old_edge_total,
        "edge id space invariant"
    );
    drop(post);
    assert_eq!(
        graphs.writer("people").unwrap().core_uuid(),
        set_uuid,
        "delta rebound to the compacted set"
    );
    assert_eq!(
        probe(&graphs),
        before,
        "the carried tombstone + edge removals read identically"
    );

    // Reopen: the below-run tombstone and edge removals reload from the merged segment.
    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(
        graphs.get("people").unwrap().uuid(),
        set_uuid,
        "reopen names the compacted set"
    );
    assert_eq!(probe(&graphs), before, "reopen preserves every read");

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 5 slice 5.2 (merge hardening): compacting a **partial run** `[1, 3)` with a
/// segment below it (seg 0) and above it (seg 3) preserves cross-segment precedence. Carol
/// is patched in every segment (10→20→30→40); the merge folds seg 1⊕seg 2 to their own
/// newest (30) yet the whole stack still resolves to seg 3's 40 (above the run wins), and
/// seg 0's below-run value (10) stays superseded — the merged segment's carried index
/// removal keeps suppressing it. Each flush also births a distinct node so the run's bands
/// are non-trivial. Reads are identical before/after the compaction and after a reopen.
#[test]
fn compact_a_partial_run_preserves_precedence() {
    let (root, _g, _u) = testgen::write_basic("compact_partial_e2e");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let base_uuid = graphs.get("people").unwrap().uuid();

    let write = |graphs: &Graphs, qy: &str| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        match parser::parse_statement(qy).unwrap() {
            parser::ast::Statement::Write(w) => {
                execute_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            _ => panic!("expected a write: {qy}"),
        }
    };
    let q = |graphs: &Graphs, qy: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let snap = graphs
            .writer("people")
            .map(|w| w.delta_snapshot())
            .unwrap_or_else(DeltaSnapshot::empty);
        let view = MergedView::new(gen.as_ref(), snap);
        let r = Engine::new(&view, &cache)
            .run(&parser::parse(qy).unwrap())
            .unwrap();
        r
    };

    // Four flushes: each patches base Carol's indexed age and births one distinct node, so
    // seg k carries Carol=(11·(k+1)) and a born node aged 91+k. Carol's ladder avoids the
    // base ages (Alice 30 / Bob 25 / Carol 40) so a seek pins exactly one node.
    for (k, (dave, dage, cage)) in [
        ("D1", 91, 11),
        ("D2", 92, 22),
        ("D3", 93, 33),
        ("D4", 94, 44),
    ]
    .into_iter()
    .enumerate()
    {
        write(
            &graphs,
            &format!("MERGE (n:Person {{name:'{dave}'}}) SET n.age = {dage}"),
        );
        write(
            &graphs,
            &format!("MATCH (n:Person {{name:'Carol'}}) SET n.age = {cage}"),
        );
        graphs
            .flush_graph_to_segment("people", &vc, &root)
            .unwrap()
            .unwrap_or_else(|| panic!("flush {k} is non-empty"));
    }
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        4,
        "four upper segments stacked"
    );
    let old_node_total = graphs
        .get("people")
        .unwrap()
        .stack()
        .extents()
        .nodes
        .total();
    let old_edge_total = graphs
        .get("people")
        .unwrap()
        .stack()
        .extents()
        .edges
        .total();

    let probe = |graphs: &Graphs| -> Vec<String> {
        let s = |v: &Val| format!("{v:?}");
        let scalar = |r: &QueryResult| r.rows.first().map_or("∅".into(), |row| s(&row[0]));
        vec![
            // Carol resolves to seg 3's value (above the run), not the merged run's newest.
            s(&q(graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age").rows[0][0]),
            scalar(&q(graphs, "MATCH (n:Person {age:44}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:33}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:22}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:11}) RETURN n.name")),
            // Every born node — below (D1), within (D2,D3), above (D4) the run — survives.
            scalar(&q(graphs, "MATCH (n:Person {age:91}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:92}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:93}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:94}) RETURN n.name")),
            s(&q(graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0]),
        ]
    };
    let before = probe(&graphs);
    assert_eq!(before[0], "Int(44)", "Carol = seg 3 (above run)");
    assert_eq!(before[1], "Str(\"Carol\")", "seek age 44 → Carol");
    assert_eq!(before[2], "∅", "merged run's internal 33 superseded");
    assert_eq!(before[3], "∅", "run's 22 superseded");
    assert_eq!(before[4], "∅", "seg 0's below-run 11 superseded");
    assert_eq!(before[5], "Str(\"D1\")", "below-run born node");
    assert_eq!(before[6], "Str(\"D2\")", "within-run born node");
    assert_eq!(before[7], "Str(\"D3\")", "within-run born node");
    assert_eq!(before[8], "Str(\"D4\")", "above-run born node");
    assert_eq!(before[9], "Int(7)", "3 base + 4 born Person");

    // Compact only the middle run [1, 3): seg 0 stays below, seg 3 stays above.
    let set_uuid = graphs
        .compact_graph_segments("people", &vc, &root, 1, 3)
        .unwrap();

    let post = graphs.get("people").unwrap();
    assert_eq!(post.uuid(), set_uuid, "served the compacted set");
    assert_eq!(post.base_uuid(), base_uuid, "base preserved");
    assert_eq!(
        post.stack().segments().len(),
        3,
        "4 segments − 2 merged + 1 = 3"
    );
    assert_eq!(
        post.stack().extents().nodes.total(),
        old_node_total,
        "node id space invariant"
    );
    assert_eq!(
        post.stack().extents().edges.total(),
        old_edge_total,
        "edge id space invariant"
    );
    drop(post);
    assert_eq!(
        probe(&graphs),
        before,
        "partial-run compaction preserves precedence"
    );

    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        3,
        "spliced set reloaded"
    );
    assert_eq!(probe(&graphs), before, "reopen preserves every read");

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 5 slice 5.2 (merge hardening): a merge whose run **includes a zero-width band** —
/// seg 0 is a patch-only flush (Carol's age, no born node ⇒ an empty node/edge band) — folds
/// correctly with a births-carrying seg 1. The contiguity check accepts the zero-width tile,
/// the patched base row and its carried index removal survive, and the born node reads back.
#[test]
fn compact_folds_a_zero_width_band() {
    let (root, _g, _u) = testgen::write_basic("compact_zerowidth_e2e");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let write = |graphs: &Graphs, qy: &str| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        match parser::parse_statement(qy).unwrap() {
            parser::ast::Statement::Write(w) => {
                execute_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            _ => panic!("expected a write: {qy}"),
        }
    };
    let q = |graphs: &Graphs, qy: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let snap = graphs
            .writer("people")
            .map(|w| w.delta_snapshot())
            .unwrap_or_else(DeltaSnapshot::empty);
        let view = MergedView::new(gen.as_ref(), snap);
        let r = Engine::new(&view, &cache)
            .run(&parser::parse(qy).unwrap())
            .unwrap();
        r
    };

    // Flush 1 (seg 0): patch-only — a base-node index move, no births ⇒ zero-width bands.
    write(&graphs, "MATCH (n:Person {name:'Carol'}) SET n.age = 99");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("patch-only flush is non-empty");
    let gen0 = graphs.get("people").unwrap();
    let seg0 = &gen0.stack().segments()[0];
    assert_eq!(
        seg0.manifest.node_band.0, seg0.manifest.node_band.1,
        "seg 0 has a zero-width node band (patch-only)"
    );
    drop(gen0);
    // Flush 2 (seg 1): a born node so the run mixes a zero-width and a non-empty band.
    write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("born flush is non-empty");

    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        2,
        "two upper segments"
    );
    let old_node_total = graphs
        .get("people")
        .unwrap()
        .stack()
        .extents()
        .nodes
        .total();

    let probe = |graphs: &Graphs| -> Vec<String> {
        let s = |v: &Val| format!("{v:?}");
        let scalar = |r: &QueryResult| r.rows.first().map_or("∅".into(), |row| s(&row[0]));
        vec![
            s(&q(graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age").rows[0][0]),
            scalar(&q(graphs, "MATCH (n:Person {age:99}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:40}) RETURN n.name")),
            scalar(&q(graphs, "MATCH (n:Person {age:70}) RETURN n.name")),
            s(&q(graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0]),
        ]
    };
    let before = probe(&graphs);
    assert_eq!(before[0], "Int(99)", "Carol's patched age");
    assert_eq!(before[1], "Str(\"Carol\")", "seek age 99 → Carol");
    assert_eq!(
        before[2], "∅",
        "base age 40 superseded via the carried removal"
    );
    assert_eq!(before[3], "Str(\"Frank\")", "born Frank by age 70");
    assert_eq!(before[4], "Int(4)", "Alice, Bob, Carol, Frank");

    let set_uuid = graphs
        .compact_graph_segments("people", &vc, &root, 0, 2)
        .unwrap();
    let post = graphs.get("people").unwrap();
    assert_eq!(post.uuid(), set_uuid, "served the compacted set");
    assert_eq!(post.stack().segments().len(), 1, "run folded into one");
    assert_eq!(
        post.stack().extents().nodes.total(),
        old_node_total,
        "node id space invariant"
    );
    drop(post);
    assert_eq!(probe(&graphs), before, "zero-width fold reads identically");

    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(
        graphs.get("people").unwrap().uuid(),
        set_uuid,
        "reopen names the compacted set"
    );
    assert_eq!(probe(&graphs), before, "reopen preserves every read");

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 5 slice 5.2 (merge hardening): a merge over an **encrypted** stack writes a fresh
/// per-segment cipher + KDF header and seals the manifest MAC — mirroring the flush path —
/// so the merged segment is ciphertext, decrypts on read, and reopens only WITH the key.
#[test]
fn compact_encrypts_the_merged_segment() {
    let key: &[u8] = b"an-at-rest-master-key-32byteslong";
    let (root, _g) = testgen::write_indexed_people_keyed("compact_keyed_e2e", Some(key));
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let mut graphs = Graphs::open_all(&root, Some(key)).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let write = |graphs: &Graphs, qy: &str| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        match parser::parse_statement(qy).unwrap() {
            parser::ast::Statement::Write(w) => {
                execute_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            _ => panic!("expected a write: {qy}"),
        }
    };
    let age_of = |graphs: &Graphs, name: &str| -> Option<i64> {
        let gen = graphs.get("people").unwrap();
        let snap = graphs
            .writer("people")
            .map(|w| w.delta_snapshot())
            .unwrap_or_else(DeltaSnapshot::empty);
        let view = MergedView::new(gen.as_ref(), snap);
        let r = Engine::new(&view, &cache)
            .run(
                &parser::parse(&format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.age"))
                    .unwrap(),
            )
            .unwrap();
        r.rows.first().and_then(|row| match &row[0] {
            Val::Int(n) => Some(*n),
            _ => None,
        })
    };

    // Two flushes stack two encrypted segments (a born node each + a base patch).
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 91");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 92");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();

    assert_eq!(graphs.get("people").unwrap().stack().segments().len(), 2);
    let set_uuid = graphs
        .compact_graph_segments("people", &vc, &root, 0, 2)
        .unwrap();

    // The merged segment carries its own encryption header + sealed MAC — proof it is
    // ciphertext, not plaintext beside the encrypted core.
    let gen1 = graphs.get("people").unwrap();
    assert_eq!(gen1.stack().segments().len(), 1, "run folded into one");
    let seg = &gen1.stack().segments()[0];
    let header = seg
        .manifest
        .encryption
        .as_ref()
        .expect("merged segment manifest carries an encryption header");
    assert_eq!(header.aead, graph_format::crypto::AEAD_NAME);
    assert!(seg.manifest.mac.is_some(), "merged segment is MAC-sealed");
    drop(gen1);

    // Reads decrypt through the merged segment: Dave/Frank born, Alice newest-wins (92).
    assert_eq!(age_of(&graphs, "Dave"), Some(50), "born Dave decrypts");
    assert_eq!(age_of(&graphs, "Frank"), Some(70), "born Frank decrypts");
    assert_eq!(age_of(&graphs, "Alice"), Some(92), "Alice newest-wins (92)");

    // Reopen WITH the key: the merged encrypted segment reloads and serves.
    drop(graphs);
    let graphs = Graphs::open_all(&root, Some(key)).unwrap();
    assert_eq!(
        graphs.get("people").unwrap().uuid(),
        set_uuid,
        "reopen names the compacted set"
    );
    assert_eq!(
        age_of(&graphs, "Dave"),
        Some(50),
        "Dave decrypts after reopen"
    );
    assert_eq!(
        age_of(&graphs, "Alice"),
        Some(92),
        "Alice = 92 after reopen"
    );
    drop(graphs);

    // Reopen WITHOUT the key is refused (the MAC-sealed encrypted set cannot open).
    assert!(
        Graphs::open_all(&root, None).is_err(),
        "an encrypted compacted set refuses to open without the key"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 5 slice 5.2 (merge hardening): a merge against a **non-filesystem** store uploads
/// the merged segment, spliced set manifest and `current` pointer through the `ObjectStore`
/// abstraction (the run's old segments stay in the store for a later GC). A fresh open that
/// reads *only* through the in-memory store serves the folded data store-natively.
#[test]
fn compact_uploads_to_an_object_store() {
    use graph_format::store::mem::MemObjectStore;
    use graph_format::store::ObjectStore as _;

    let (root, _g) = testgen::write_indexed_people("compact_seg_memstore");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mem = Arc::new(MemObjectStore::new());
    load_dir_into_mem(&mem, &root, &root);

    let mut graphs = Graphs::open_all_with_store(
        mem.clone() as Arc<dyn ObjectStore>,
        None,
        true,
        None,
        crate::degree_column::DegreeResidency::Lazy,
        None,
    )
    .unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let base_uuid = graphs.get("people").unwrap().uuid();

    let write = |graphs: &Graphs, qy: &str| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        match parser::parse_statement(qy).unwrap() {
            parser::ast::Statement::Write(w) => {
                execute_write(
                    &writer,
                    gen.as_ref(),
                    &w,
                    &HashMap::new(),
                    TEST_BOLT_VERSION,
                )
                .unwrap();
            }
            _ => panic!("expected a node write: {qy}"),
        }
    };
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    write(&graphs, "MERGE (n:Person {name:'Frank'}) SET n.age = 70");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    assert_eq!(graphs.get("people").unwrap().stack().segments().len(), 2);

    let set_uuid = graphs
        .compact_graph_segments("people", &vc, &root, 0, 2)
        .unwrap();

    // The store now names the compacted set; its manifest references exactly one segment.
    assert_eq!(
        String::from_utf8(mem.read_all("people/current").unwrap())
            .unwrap()
            .trim(),
        set_uuid.0.to_string(),
        "remote current names the compacted set"
    );
    let uploaded_set =
        graph_format::setmanifest::SetManifest::read_via(mem.as_ref(), "people", set_uuid).unwrap();
    assert_eq!(
        uploaded_set.segments.len(),
        1,
        "the uploaded set references the single merged segment"
    );
    // The merged segment's SEGMENT.json is in the store; the two pre-merge dirs also remain
    // (GC is a later phase), so the store holds three segment dirs.
    let seg_uuids = mem.list("people/segments").unwrap();
    assert_eq!(
        seg_uuids.len(),
        3,
        "merged + two pre-merge segment dirs (old ones GC'd later)"
    );
    assert!(
        mem.exists(&format!(
            "people/segments/{}/SEGMENT.json",
            uploaded_set.segments[0].uuid.0
        ))
        .unwrap(),
        "the merged SEGMENT.json was uploaded"
    );

    // Reopen reading ONLY through the mem store: the folded data is served store-natively.
    drop(graphs);
    let graphs = Graphs::open_all_with_store(
        mem.clone() as Arc<dyn ObjectStore>,
        None,
        true,
        None,
        crate::degree_column::DegreeResidency::Lazy,
        None,
    )
    .unwrap();
    let gen = graphs.get("people").unwrap();
    assert_eq!(gen.uuid(), set_uuid, "store reopen names the compacted set");
    assert_eq!(gen.base_uuid(), base_uuid, "base preserved");
    assert_eq!(gen.stack().segments().len(), 1, "merged segment from store");
    let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
    let names: Vec<String> = Engine::new(&view, &cache)
        .run(&parser::parse("MATCH (n:Person) WHERE n.age >= 50 RETURN n.name").unwrap())
        .unwrap()
        .rows
        .iter()
        .map(|r| match &r[0] {
            Val::Str(s) => s.clone(),
            v => panic!("name not str: {v:?}"),
        })
        .collect();
    assert!(
        names.contains(&"Dave".to_string()) && names.contains(&"Frank".to_string()),
        "both born nodes served from the merged store-native segment: {names:?}"
    );

    std::fs::remove_dir_all(&root).ok();
}
