// SPDX-License-Identifier: Apache-2.0
//! `limits_and_estimates` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// The result-pool byte estimate must cover the *allocated* footprint of a
/// result — every `String`'s capacity and every `Vec`'s capacity, including the
/// nested ones — not just a flat per-value constant (HIK-141).
///
/// The bound below is derived from the construction of `r` (we know exactly how
/// many strings of what length, and how many `Vec` slots, were built), **not**
/// by running a second estimator over the same data — asserting `impl A == impl
/// B` would only prove the two agree. It is a floor: the estimate may legally be
/// larger (allocator slack, per-entry bookkeeping), never smaller.
#[test]
fn result_byte_estimate_covers_string_and_container_capacity() {
    use std::mem::size_of;

    const VAL: usize = size_of::<Val>();
    const PAIR: usize = size_of::<(String, Val)>();
    const ROWS: usize = 8;
    const KEY_A: &str = "a_reasonably_long_map_key";
    const KEY_B: &str = "another_map_key_here";
    const COL_A: &str = "a_very_long_column_name_that_is_not_short";
    const COL_B: &str = "another_long_column_name_for_the_second_column";
    const VEC_LEN: usize = 32;

    let s = |n: usize| "x".repeat(n); // `repeat` allocates exactly `n` bytes

    let rows: Vec<Vec<Val>> = (0..ROWS)
        .map(|i| {
            vec![
                Val::Str(s(512 + i)),
                Val::List(vec![
                    Val::Str(s(256)),
                    Val::List(vec![Val::Str(s(128)), Val::Int(i as i64)]),
                    Val::Map(vec![
                        (KEY_A.to_string(), Val::Str(s(64))),
                        (KEY_B.to_string(), Val::Vector(vec![0.5f32; VEC_LEN])),
                    ]),
                ]),
            ]
        })
        .collect();
    let r = QueryResult {
        columns: vec![COL_A.to_string(), COL_B.to_string()],
        rows,
    };

    // Per row, counted off the literal above:
    //   outer row `Vec<Val>`               2 slots
    //   Str(512+i)                         512+i bytes of `String` heap
    //   List of 3                          3 slots
    //     Str(256)                         256
    //     List of 2                        2 slots + 128
    //     Map of 2                         2 `(String, Val)` slots
    //       KEY_A -> Str(64)               KEY_A.len() + 64
    //       KEY_B -> Vector(VEC_LEN)       KEY_B.len() + VEC_LEN * 4
    let per_row_fixed = 2 * VAL
        + 3 * VAL
        + 256
        + 2 * VAL
        + 128
        + 2 * PAIR
        + KEY_A.len()
        + 64
        + KEY_B.len()
        + VEC_LEN * size_of::<f32>();
    let strings: usize = (0..ROWS).map(|i| 512 + i).sum();
    let floor = size_of::<QueryResult>()
        + 2 * size_of::<String>()
        + COL_A.len()
        + COL_B.len()
        + ROWS * size_of::<Vec<Val>>()
        + ROWS * per_row_fixed
        + strings;

    let est = estimate_result_bytes(&r);
    assert!(
        est >= floor,
        "estimate {est} under-counts the result's allocated footprint; it must be \
         at least {floor} bytes (summed String/Vec capacities + owning struct sizes)"
    );

    // The `0` heap arms (Node/Rel/Point/temporals) rest entirely on the enum slot
    // covering their inline payload. `Val::Rel` is the widest of them — four fields,
    // one u64 each less the u32 reltype — so if the slot ever shrinks below what a
    // Rel actually occupies, those arms silently under-charge. Pin the floor here
    // rather than in a comment. See the CONTRACT note on `val_heap_bytes`.
    assert!(
        VAL >= 3 * size_of::<u64>() + size_of::<u32>(),
        "size_of::<Val>() = {VAL} no longer covers Val::Rel's inline payload; the \
         zero-heap arms in val_heap_bytes must be revisited"
    );
    assert_eq!(
        val_bytes(&Val::Node(7)),
        VAL,
        "a scalar variant must charge exactly its inline slot"
    );
}

/// HIK-150: the maintained-degree fast path is given up only while the edge delete is
/// **live**, not permanently. A flush resolves the identity tombstone against the effective
/// adjacency into one `removed` entry per real core edge id, which the segment fold already
/// counts exactly — so after the flush the path arms again and its answer is exact.
///
/// This is what makes "decline" a bounded cost rather than "any graph that has ever deleted
/// an edge loses the fast path forever", and it is the assertion that would catch a future
/// change to the flush's tombstone resolution silently making the segment fold wrong instead.
#[test]
fn an_edge_delete_gives_the_degree_fast_path_back_after_a_flush() {
    let (root, _g) = testgen::write_indexed_people("degree_selfheal");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // Alice(0) -[:KNOWS]-> Bob(1) is the only core edge.
    let deg_now = |graphs: &Graphs| -> Result<u64, anyhow::Error> {
        let g = graphs.get("people").unwrap();
        let snap = graphs
            .writer("people")
            .map(|w| w.delta_snapshot())
            .unwrap_or_else(DeltaSnapshot::empty);
        let view = MergedView::new(g.as_ref(), snap);
        let eng = Engine::new(&view, &cache);
        eng.directed_edge_count(0, true)
    };
    assert_eq!(deg_now(&graphs).unwrap(), 1, "the core edge is counted");

    {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let parser::ast::Statement::WriteEdge(w) = parser::parse_statement(
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
        )
        .unwrap() else {
            panic!("expected an edge write");
        };
        execute_edge_write(
            &writer,
            gen.as_ref(),
            &w,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
        .unwrap();
    }

    // Live: refused, typed — the fold cannot know the tombstone's multiplicity.
    let err = deg_now(&graphs).unwrap_err();
    assert_eq!(
        err.downcast_ref::<crate::exec::DegreeNotExact>(),
        Some(&crate::exec::DegreeNotExact::EdgeTombstone),
        "a live edge delete must refuse the maintained degree, got: {err:#}"
    );

    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("an edge delete flushes a non-empty delta");

    // Flushed: the delta is empty, the segment carries a resolved per-edge-id removal, and
    // the maintained degree is exact again — agreeing with the adjacency overlay.
    let gen = graphs.get("people").unwrap();
    let snap = graphs
        .writer("people")
        .map(|w| w.delta_snapshot())
        .unwrap_or_else(DeltaSnapshot::empty);
    let view = MergedView::new(gen.as_ref(), snap);
    let eng = Engine::new(&view, &cache);
    let overlaid = eng.outgoing_adj(0).unwrap().len() as u64;
    assert_eq!(overlaid, 0, "the edge is gone from the stacked view");
    assert_eq!(
        eng.directed_edge_count(0, true).unwrap(),
        overlaid,
        "after a flush the maintained degree is exact again"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// HIK-151: Stage B is now reachable over a **stacked** set (base + upper segment) as well
/// as a live delta — the other half of the gate it had inherited from Stage 7.
///
/// Stage 7 needs a singleton set because it reads the base range index and histograms
/// directly. Stage B does not: it walks the ordinary segment-aware seams, and
/// `directed_edge_count` adds each segment's fence-gated born−removed fragment to the core
/// degree. This asserts both halves on the same view — the count agrees with the
/// materialising walk, and the final hop really is answered from maintained degrees rather
/// than by walking (so the optimisation is genuinely reachable, not merely correct).
#[test]
fn the_count_walk_is_reachable_and_exact_over_a_stacked_set() {
    let (root, _g) = testgen::write_indexed_people("stageb_stacked");
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

    // A born edge AND a delete of the one core edge, flushed together into an upper
    // segment: the segment therefore carries **both** terms `directed_edge_count` folds —
    // a born fragment and a removed one — which is the composition the old gate excluded
    // outright, since it refused any stacked set.
    write(
        &graphs,
        "MERGE (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'})",
    );
    write(
        &graphs,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
    );
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a born edge and a delete flush a non-empty delta");
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        1,
        "the set must be stacked for this test to mean anything"
    );

    // …and a further born edge live in the delta on top of the segment.
    write(
        &graphs,
        "MERGE (a:Person {name:'Carol'})-[r:KNOWS]->(b:Person {name:'Alice'})",
    );

    let gen = graphs.get("people").unwrap();
    let snap = graphs.writer("people").unwrap().delta_snapshot();
    let view = MergedView::new(gen.as_ref(), snap);
    assert!(!view.core_stack().is_singleton() && !view.delta().is_empty());

    let visits = || crate::exec::ADJ_VISIT_COUNT.with(|c| c.get());
    let reset = || crate::exec::ADJ_VISIT_COUNT.with(|c| c.set(0));

    reset();
    let counted = {
        let ast = parser::parse("MATCH (x)-[]->()-[]->(z) RETURN count(*)").unwrap();
        match Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0] {
            Val::Int(n) => n as usize,
            ref v => panic!("count not int: {v:?}"),
        }
    };
    let count_visits = visits();

    reset();
    let materialised = {
        let ast = parser::parse("MATCH (x)-[]->()-[]->(z) RETURN z").unwrap();
        Engine::new(&view, &cache).run(&ast).unwrap().rows.len()
    };
    let walk_visits = visits();

    assert_eq!(
        counted, materialised,
        "over a stacked set + live delta, the count walk must agree with the materialising walk"
    );
    assert!(counted > 0, "the fixture must produce rows to compare");
    assert!(
        count_visits < walk_visits,
        "the final hop must be answered from maintained degrees over a stacked set too \
         ({count_visits} visits counting vs {walk_visits} materialising)"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// HIK-152: `live_edge_count` must not over-count when a node tombstone arrives **after** a
/// flush has moved that node's edges into a core segment.
///
/// `edges_lost_to_node_tombstones` read the *base* CSR only, so a segment-born edge killed
/// by a later `DELETE n` was added by `stack().edge_count_delta()` and never subtracted —
/// `lost.core` could not see it (not in the base) and `lost.born` could not either (no
/// longer in the delta).
///
/// Both halves are compared on the same view: `count(*)` (the metadata path, Stage E →
/// `live_edge_count`) against the materialising walk's row count. Neither is a hand-computed
/// constant; the claim is that two code paths disagree.
#[test]
fn a_node_delete_after_a_flush_must_not_over_count_edges() {
    let (root, _g) = testgen::write_indexed_people("hik152_after_flush");
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
    // `count(*)` over edges (metadata path) vs the materialising walk, on one view.
    let check = |graphs: &Graphs, what: &str| -> usize {
        let gen = graphs.get("people").unwrap();
        let snap = graphs
            .writer("people")
            .map(|w| w.delta_snapshot())
            .unwrap_or_else(DeltaSnapshot::empty);
        let view = MergedView::new(gen.as_ref(), snap);
        let counted = {
            let ast = parser::parse("MATCH ()-[r]->() RETURN count(*)").unwrap();
            match Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0] {
                Val::Int(n) => n as usize,
                ref v => panic!("count not int: {v:?}"),
            }
        };
        let walked = {
            let ast = parser::parse("MATCH (a)-[r]->(b) RETURN r").unwrap();
            Engine::new(&view, &cache).run(&ast).unwrap().rows.len()
        };
        assert_eq!(
            counted, walked,
            "{what}: the live edge count and the materialising walk must agree"
        );
        // The per-reltype grouping shares `edges_lost_to_node_tombstones`, so it has to be
        // checked too — otherwise the fix is verified on one of its two consumers.
        let grouped: usize = {
            let ast = parser::parse("MATCH ()-[r]->() RETURN type(r), count(*)").unwrap();
            Engine::new(&view, &cache)
                .run(&ast)
                .unwrap()
                .rows
                .iter()
                .map(|r| match r[1] {
                    Val::Int(n) => n as usize,
                    ref v => panic!("group count not int: {v:?}"),
                })
                .sum()
        };
        assert_eq!(
            grouped, walked,
            "{what}: the per-reltype live counts must sum to the materialising walk"
        );
        counted
    };

    // A born edge, flushed into a segment.
    write(
        &graphs,
        "MERGE (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'})",
    );
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a born edge flushes a non-empty delta");
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        1,
        "the fixture must actually have a segment — segmentFlushBytes defaults to 0, so a \
         silently-singleton set is the easy way for this test to pass for the wrong reason"
    );
    let before = check(&graphs, "flushed, no delete");

    // …then delete the endpoint node. The segment-born edge dies with it.
    write(&graphs, "MATCH (n:Person {name:'Carol'}) DETACH DELETE n");
    let after = check(&graphs, "node deleted after the flush");
    assert!(
        after < before,
        "deleting Carol must remove edges ({after} vs {before})"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// HIK-152, second instance — found while fixing the first and **not** in the ticket.
///
/// The same function gated its core-adjacency branch on `Generation::node_count()`, which is
/// the *base* manifest's count. A node born in a segment sits above it, so deleting one
/// skipped the branch entirely: its incident edges are in the segment, not the delta, so
/// neither term saw them.
///
/// The distinguishing fixture is a node that **only exists in a segment** — created and
/// flushed — then detached and deleted.
#[test]
fn deleting_a_segment_born_node_must_not_over_count_edges() {
    let (root, _g) = testgen::write_indexed_people("hik152_segborn_node");
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
    let check = |graphs: &Graphs, what: &str| -> usize {
        let gen = graphs.get("people").unwrap();
        let snap = graphs
            .writer("people")
            .map(|w| w.delta_snapshot())
            .unwrap_or_else(DeltaSnapshot::empty);
        let view = MergedView::new(gen.as_ref(), snap);
        let counted = {
            let ast = parser::parse("MATCH ()-[r]->() RETURN count(*)").unwrap();
            match Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0] {
                Val::Int(n) => n as usize,
                ref v => panic!("count not int: {v:?}"),
            }
        };
        let walked = {
            let ast = parser::parse("MATCH (a)-[r]->(b) RETURN r").unwrap();
            Engine::new(&view, &cache).run(&ast).unwrap().rows.len()
        };
        assert_eq!(counted, walked, "{what}: live count vs materialising walk");
        counted
    };

    // Dave exists nowhere in the base — he is born in the delta with an edge, then flushed,
    // so his dense id sits in the segment's born band, above the base node count.
    write(
        &graphs,
        "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Dave'})",
    );
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a born node + edge flushes a non-empty delta");
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        1,
        "the fixture must actually have a segment"
    );
    let base_nodes = graphs.get("people").unwrap().node_count();
    let stacked_nodes = graphs
        .get("people")
        .unwrap()
        .stack()
        .extents()
        .nodes
        .total();
    assert!(
        stacked_nodes > base_nodes,
        "Dave must be a segment-born id ({stacked_nodes} vs base {base_nodes}) — otherwise \
         this test is not exercising the id-gate instance at all"
    );
    let before = check(&graphs, "segment-born node, alive");

    write(&graphs, "MATCH (n:Person {name:'Dave'}) DETACH DELETE n");
    let after = check(&graphs, "segment-born node deleted");
    assert!(
        after < before,
        "deleting Dave must remove his edge ({after} vs {before})"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// HIK-152 control: the **singleton** case must be unchanged. The fix routes the core side
/// through the flush's effective-adjacency resolver, so it has to still produce exactly the
/// base CSR's answer when there is no segment — otherwise "subtract more" would be a
/// regression everywhere rather than a fix in one configuration.
#[test]
fn a_node_delete_on_a_singleton_core_still_counts_edges_exactly() {
    let (root, _g) = testgen::write_indexed_people("hik152_singleton_control");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    assert!(
        graphs.get("people").unwrap().stack().is_singleton(),
        "this control is only meaningful without a segment"
    );

    {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let parser::ast::Statement::Write(w) =
            parser::parse_statement("MATCH (n:Person {name:'Bob'}) DETACH DELETE n").unwrap()
        else {
            panic!("expected a node write");
        };
        execute_write(
            &writer,
            gen.as_ref(),
            &w,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
        .unwrap();
    }

    let gen = graphs.get("people").unwrap();
    let snap = graphs.writer("people").unwrap().delta_snapshot();
    let view = MergedView::new(gen.as_ref(), snap);
    let counted = {
        let ast = parser::parse("MATCH ()-[r]->() RETURN count(*)").unwrap();
        match Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0] {
            Val::Int(n) => n as usize,
            ref v => panic!("count not int: {v:?}"),
        }
    };
    let walked = {
        let ast = parser::parse("MATCH (a)-[r]->(b) RETURN r").unwrap();
        Engine::new(&view, &cache).run(&ast).unwrap().rows.len()
    };
    assert_eq!(
        counted, walked,
        "a singleton core's live edge count must still match the walk"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// HIK-152, third instance — found in self-review, and the error in the **opposite**
/// direction, which is why it is worth its own test.
///
/// The old code read the base CSR, which still lists an edge that a flush has since
/// *removed*. So a core edge deleted into a segment and then implicated in a node tombstone
/// was subtracted **twice**: once by `stack().edge_count_delta()` when it flushed, and again
/// by `lost.core` when the node died. Under-counting rather than over-counting, from the
/// same single-axis read.
///
/// Routing through `effective_adj` fixes it for free — it honours each segment's `removed`
/// entries — but "for free" is exactly the kind of claim that needs a test rather than an
/// argument.
#[test]
fn a_node_delete_must_not_double_subtract_an_edge_a_flush_already_removed() {
    let (root, _g) = testgen::write_indexed_people("hik152_removed_twice");
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

    // A born edge that survives, flushed…
    write(
        &graphs,
        "MERGE (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'})",
    );
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("born edge flushes");
    // …then delete the *core* edge Alice→Bob and flush that too, so a segment carries a
    // `removed` entry for an edge the base CSR still lists.
    write(
        &graphs,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
    );
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("edge delete flushes");
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        2,
        "the fixture needs a born segment and a removed segment"
    );

    // Now tombstone Alice — the endpoint of the already-removed edge.
    write(&graphs, "MATCH (n:Person {name:'Alice'}) DETACH DELETE n");

    let gen = graphs.get("people").unwrap();
    let snap = graphs.writer("people").unwrap().delta_snapshot();
    let view = MergedView::new(gen.as_ref(), snap);
    let counted = {
        let ast = parser::parse("MATCH ()-[r]->() RETURN count(*)").unwrap();
        match Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0] {
            Val::Int(n) => n as usize,
            ref v => panic!("count not int: {v:?}"),
        }
    };
    let walked = {
        let ast = parser::parse("MATCH (a)-[r]->(b) RETURN r").unwrap();
        Engine::new(&view, &cache).run(&ast).unwrap().rows.len()
    };
    assert_eq!(
        walked, 1,
        "only Bob→Carol should survive — otherwise this fixture is not the one described"
    );
    assert_eq!(
        counted, walked,
        "an edge a flush already removed must not be subtracted again by a node tombstone"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// A large but entirely *legitimate* query, sized so one parse costs measurable wall
/// time — a wide projection, not one of the ticket's pathological `CALL` runs. Those are
/// the wrong instrument now: `MAX_PARSE_CALLS` bounds them to a few milliseconds, which is
/// too tight to assert on without flake, and they would test the work budget (already
/// fixed) rather than where the parse runs. An ordinary big query stalls the reactor for
/// just as long, which is the point.
///
/// A wide `RETURN` rather than a `WITH` chain, because this query has to be accepted by
/// *both* parsers: on a writable graph it goes through `parse_statement`, which currently
/// rejects a read's `WITH` outright (see the merge note on HIK-267). It also has to stay
/// inside `MAX_PARSE_CALLS` — 1,500 items already parse, 5,000 do not — so the item count
/// keeps a wide margin below that ceiling.
fn wide_projection_query(items: usize) -> String {
    let projection: Vec<String> = (0..items).map(|i| format!("n.age + {i} AS c{i}")).collect();
    format!("MATCH (n:Person) RETURN {}", projection.join(", "))
}

/// HIK-267 regression: parsing must not run on the reactor.
///
/// Same instrument as [`writes_do_not_block_the_reactor`] (HIK-87), because it is the
/// same bug class and parsing is the last heavy work left inline in `handle_request`.
/// `#[tokio::test]` is a **current-thread** runtime — the one place a blocked reactor is
/// directly observable — and a single `yield_now()` gives every ready task exactly one
/// poll. If the parses run inline (the bug), that one trip through the scheduler costs
/// FLOOD × one parse, and every other connection multiplexed on that worker waits out
/// all of it. Handed to a blocking thread, each poll parks at the join handle and the
/// reactor comes straight back.
///
/// This drives the **read-only** arm (`parser::parse`, no writable layer) deliberately:
/// `delta.enabled = false` is the default deployment and is hit exactly as hard, so a
/// fix that only covered the write arm would fix nothing for most installs.
///
/// The bound is calibrated against a parse measured on this box and build profile, not a
/// hard-coded millisecond, so it neither flakes on a slow machine nor passes vacuously
/// on a fast one.
#[tokio::test]
async fn parses_do_not_block_the_reactor() {
    const FLOOD: usize = 8;
    let query = wide_projection_query(1_000);

    // Calibrate: what one parse of this query costs. Warm first, so the measurement is
    // not paying for anything the process only initialises once.
    parser::parse(&query).expect("the calibration query must be legitimate Cypher");
    let t0 = Instant::now();
    parser::parse(&query).unwrap();
    let one_parse = t0.elapsed();
    assert!(
        one_parse >= Duration::from_millis(1),
        "the calibration query should cost real parse time; measured {one_parse:?} — is it \
         still big enough to measure against?"
    );

    // Both arms, because they are two call sites and only one of them is on the default
    // deployment path: a writable graph parses through `parse_statement`, a graph with no
    // writer through `parse`. Re-inlining either would be a regression, so neither may be
    // left to the other's coverage.
    for writable in [false, true] {
        let (root, ctx) = build_ctx_limited(
            &format!("server_parse_off_reactor_{writable}"),
            TestLimits {
                writable,
                ..Default::default()
            },
        );
        let permits = ctx.parse_limit.available_permits();

        let flood: Vec<_> = (0..FLOOD)
            .map(|_| {
                let ctx = ctx.clone();
                let query = query.clone();
                tokio::spawn(async move {
                    let mut sess = authenticated_session("reporting");
                    let run = message::Request::Run {
                        query,
                        params: PsValue::Map(vec![]),
                        extra: PsValue::Map(vec![]),
                    };
                    handle_request(&mut sess, &ctx, run).await
                })
            })
            .collect();

        let t0 = Instant::now();
        tokio::task::yield_now().await;
        let reactor_stall = t0.elapsed();
        assert!(
            reactor_stall < one_parse,
            "(writable={writable}) the reactor was held for {reactor_stall:?} while {FLOOD} \
             queries parsed (one parse = {one_parse:?}) — parsing is running on a reactor worker"
        );
        // Timing alone cannot tell "the parse moved to a blocking thread" from "something
        // yielded before the parse", which would leave the bug in place and the assertion
        // above green. The permits pin the mechanism: every flooded query is holding one
        // right now, so the parse it is doing is provably the off-reactor one.
        assert_eq!(
            ctx.parse_limit.available_permits(),
            permits - FLOOD,
            "(writable={writable}) every flooded query should be holding a parse permit \
             while it parses"
        );

        // …and every query still parsed and ran: this is not a fast path that skipped the
        // work, and the permits all came back.
        for t in flood {
            t.await.unwrap().expect("every flooded RUN should succeed");
        }
        assert_eq!(ctx.parse_limit.available_permits(), permits);
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// A ctx with an explicit `maxConcurrentParses` cap — the harness for the parse-gate
/// tests.
fn build_gated_parse_ctx(tag: &str, max_concurrent_parses: usize) -> (PathBuf, Arc<ConnCtx>) {
    build_ctx_limited(
        tag,
        TestLimits {
            max_concurrent_parses,
            ..Default::default()
        },
    )
}

/// The cap is what stops the fix from simply *moving* the denial of service into tokio's
/// 512-thread blocking pool — the pool query execution runs on. An uncapped
/// `spawn_blocking` per RUN would hand that pool an unbounded queue of CPU-bound parses,
/// and reads would starve behind them.
///
/// While a flood is in flight no permit is left; once it drains every permit is back, and
/// every query still parsed.
#[tokio::test]
async fn concurrent_parses_are_capped() {
    const FLOOD: usize = 6;
    const CAP: usize = 2;
    let (root, ctx) = build_gated_parse_ctx("server_parses_capped", CAP);
    assert_eq!(ctx.parse_limit.available_permits(), CAP);
    let query = wide_projection_query(1_000);

    let flood: Vec<_> = (0..FLOOD)
        .map(|_| {
            let ctx = ctx.clone();
            let query = query.clone();
            tokio::spawn(async move { parse_off_reactor(&ctx, query, parser::parse).await })
        })
        .collect();
    tokio::task::yield_now().await;
    assert_eq!(
        ctx.parse_limit.available_permits(),
        0,
        "every parse permit should be in use while a flood is queued"
    );

    for t in flood {
        let (_query, parsed) = t.await.unwrap().expect("the parse task should complete");
        parsed.expect("every capped parse should still succeed");
    }
    // The cap is fully released once the flood drains — the permit lives with the parse,
    // not with the caller — and no query was lost to the gate.
    assert_eq!(ctx.parse_limit.available_permits(), CAP);
    let _ = std::fs::remove_dir_all(&root);
}

/// A `spawn_blocking` task cannot be cancelled: if the client hangs up mid-RUN, the await
/// on the join handle is dropped but the parse runs to completion on its thread anyway.
///
/// So the permit is moved *into* the closure. Held in the async frame instead, it would be
/// released the instant the caller was cancelled while the parse still burned CPU — and a
/// flood of clients that disconnect immediately after RUN could overrun the cap at will,
/// which is exactly the blocking-pool starvation the cap exists to prevent.
#[tokio::test]
async fn an_abandoned_parse_holds_its_permit_until_it_finishes() {
    const CAP: usize = 2;
    let (root, ctx) = build_gated_parse_ctx("server_parse_abandoned", CAP);
    let query = wide_projection_query(1_000);

    let task = {
        let ctx = ctx.clone();
        tokio::spawn(async move { parse_off_reactor(&ctx, query, parser::parse).await })
    };
    // One poll: the permit is taken and the parse is handed to the blocking pool.
    tokio::task::yield_now().await;
    assert_eq!(ctx.parse_limit.available_permits(), CAP - 1);

    // The client hangs up: the caller is cancelled, the parse is not.
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        ctx.parse_limit.available_permits(),
        CAP - 1,
        "an abandoned parse must keep its permit while it is still running — releasing it \
         at cancellation lets a hung-up client overrun the cap"
    );

    // It runs to completion, and only then is the permit released.
    while ctx.parse_limit.available_permits() < CAP {
        tokio::task::yield_now().await;
    }
    let _ = std::fs::remove_dir_all(&root);
}
