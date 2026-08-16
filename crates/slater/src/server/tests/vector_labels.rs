// SPDX-License-Identifier: Apache-2.0
//! `vector_labels` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// A **value** removal is permanent — `REMOVE n.embedding` destroys the value, and no amount
/// of label churn brings it back (there is nothing to bring back). Flush it, then re-enter and
/// leave scope: the node stays out. This is the guard that the kind split did not turn *every*
/// flushed removal into an un-suppressible one — a value removal must ignore the re-label the
/// label removal honours.
#[test]
fn a_value_removal_stays_gone_across_a_flush_and_label_churn() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) =
        testgen::write_vector_docs_keyed("vec_value_removal_permanent", &base, "Keyed");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let ids = |g: &Graphs| -> Vec<u64> {
        vknn(g, &graph, &cache, &VQ, 3)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };
    assert_eq!(ids(&graphs), vec![0, 1, 2]);

    // Destroy the value, then flush it into a segment as a *value* removal.
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Keyed {name:'d00'}) REMOVE n.embedding",
    );
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the value removal flushes");
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "gone once the value is removed + flushed"
    );

    // Churn the label: leave scope, then re-enter. A *label* removal would resurface here; a
    // value removal must not — the value is genuinely gone.
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) SET n:Doc");
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "a flushed value removal stays gone regardless of later label churn"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The removal **kind** must survive a T3 merge and a consolidation, not just a flush. Drive
/// `REMOVE n:Doc` → flush → `SET n:Doc` → flush (two segments) → **merge** → **consolidate**,
/// and assert d00 carries its original vector through every rung. The re-label is flushed into
/// the newer segment, and the older segment's `label_removal` must fold forward *as a label
/// removal* (not a value removal) so the merged segment and the consolidation dump both let
/// the re-labelled node keep its base vector.
#[test]
fn the_re_label_kind_survives_a_merge_and_a_consolidation() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) =
        testgen::write_vector_docs_keyed("vec_relabel_merge_consolidate", &base, "Keyed");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let top =
        |g: &Graphs| -> Option<(u64, f64)> { vknn(g, &graph, &cache, &VQ, 3).first().copied() };

    // Segment 1: the label removal.
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("segment 1 — the label removal");
    // Segment 2: the re-label (a separate segment so a compaction has a run to fold).
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) SET n:Doc");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("segment 2 — the re-label");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 2);
    assert_eq!(
        top(&graphs),
        Some((0, 0.0)),
        "d00 is back at its base vector with the re-label in a newer segment"
    );

    // Merge the run: the older segment's label_removal must carry forward *as a label removal*.
    graphs
        .compact_graph_segments(&graph, &vc, &root, 0, 2)
        .unwrap();
    assert_eq!(
        graphs.get(&graph).unwrap().stack().segments().len(),
        1,
        "the run folded into one segment"
    );
    assert_eq!(
        top(&graphs),
        Some((0, 0.0)),
        "d00 keeps its base vector across the merge (kind preserved)"
    );

    // Consolidate: the dump reads the level fold, so a kind-blind merge would drop d00 here.
    let dumped = dump_vectors(&graphs, &graph, &cache, &root.join("_dump"));
    let dumped_ids: Vec<u64> = dumped.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        dumped_ids,
        vec![0, 1, 2],
        "d00 is in :Doc scope at consolidation, so the rebuild indexes it — got {dumped_ids:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The re-label must restore a vector that lives in an **older segment**, not just one in the
/// base — the case that exercises the per-segment suppression accumulator (`segments_knn`), a
/// site the base-arm fix does not cover. d02 is re-embedded into segment 1 at distance 0.05
/// (its live vector is now in a segment, not the base); segment 2 then drops its `:Doc` label,
/// and the delta re-adds it. A kind-blind accumulator would fold segment 2's removal forward
/// and suppress d02 in segment 1's own scan, dropping it from the results even though it is
/// back in scope. The kind-aware accumulator un-suppresses the re-labelled id, so segment 1
/// still surfaces its 0.05 vector.
#[test]
fn a_re_label_restores_a_vector_held_in_an_older_segment() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) =
        testgen::write_vector_docs_keyed("vec_relabel_older_segment", &base, "Keyed");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let ids = |g: &Graphs| -> Vec<u64> {
        vknn(g, &graph, &cache, &VQ, 3)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };
    assert_eq!(ids(&graphs), vec![0, 1, 2]);

    // Segment 1: re-embed d02 with a *closer* vector (0.05). Its live embedding now lives in a
    // segment, above its stale base 0.4.
    let d02_close = at_distance(0.05);
    let parts: Vec<String> = d02_close.iter().map(|x| format!("{x:?}")).collect();
    vwrite(
        &graphs,
        &graph,
        &format!(
            "MATCH (n:Keyed {{name:'d02'}}) SET n.embedding = vecf32([{}])",
            parts.join(", ")
        ),
    );
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("segment 1 — d02 re-embedded into a segment");
    assert_eq!(
        ids(&graphs),
        vec![0, 2, 1],
        "d02 now leads d01 — its segment vector (0.05) beats d01's base (0.2)"
    );

    // Segment 2: drop d02's :Doc label (a label removal in a newer segment than its vector).
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d02'}) REMOVE n:Doc");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("segment 2 — d02's label removal");
    assert_eq!(
        ids(&graphs),
        vec![0, 1],
        "d02 out of scope while unlabelled"
    );

    // Re-enter scope from the delta. d02's live vector is in segment 1, *older* than segment
    // 2's removal — the per-segment accumulator must un-suppress it so segment 1 surfaces 0.05.
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d02'}) SET n:Doc");
    let restored = vknn(&graphs, &graph, &cache, &VQ, 3);
    let d02 = restored.iter().find(|(id, _)| *id == 2);
    assert!(
        d02.is_some_and(|(_, s)| (s - 0.05).abs() < 1e-3),
        "d02 restored at its segment-1 vector (0.05), got {restored:?}"
    );
    assert_eq!(
        restored.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![0, 2, 1],
        "and it leads d01 again — the older-segment vector, not the base"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// Phase 4 slice 4.1: a births-only delta folds into an upper core segment (the
/// O(delta) T2 flush), the base is preserved, and every born entity reads back from
/// the segment (index seek, count, traversal) with an empty delta — surviving a reopen.
#[test]
fn flush_to_segment_folds_births_into_a_core_segment() {
    let (root, _g) = testgen::write_indexed_people("flush_seg_e2e");
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
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
    write(
        &graphs,
        "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Eve'})",
    );

    // Flush the delta into an upper core segment.
    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes");

    // The served generation is a new set over the *same* base, carrying one segment.
    let gen1 = graphs.get("people").unwrap();
    assert_eq!(gen1.uuid(), set_uuid, "identity is the new set uuid");
    assert_eq!(gen1.base_uuid(), base_uuid, "base preserved by the flush");
    assert_eq!(gen1.stack().segments().len(), 1, "one upper segment");

    // The delta is retired: the active memtable is empty, the writer is re-bound.
    let writer = graphs.writer("people").unwrap();
    assert!(writer.snapshot().is_empty(), "delta retired empty");
    assert_eq!(writer.core_uuid(), set_uuid, "writer re-bound to the set");

    // Read back with an empty delta — every born entity is served from the segment.
    let q = |graphs: &Graphs, q: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let ast = parser::parse(q).unwrap();
        let r = Engine::new(&view, &cache).run(&ast).unwrap();
        r
    };
    // Index seek (name is indexed in the base) finds the flushed born node's props.
    let dave = q(
        &graphs,
        "MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age",
    );
    assert_eq!(dave.rows.len(), 1, "index seek finds Dave in the segment");
    assert!(
        matches!(dave.rows[0][1], Val::Int(50)),
        "Dave age from segment"
    );
    // Count over the merged marginals: 3 base + 2 born.
    let n = q(&graphs, "MATCH (n:Person) RETURN count(*)");
    assert!(
        matches!(n.rows[0][0], Val::Int(5)),
        "3 base + 2 born from the segment: {:?}",
        n.rows[0][0]
    );
    // The born edge traverses from the segment adjacency.
    let knows = q(
        &graphs,
        "MATCH (a:Person {name:'Dave'})-[:KNOWS]->(b) RETURN b.name",
    );
    assert_eq!(knows.rows.len(), 1, "the born KNOWS edge traverses");
    assert!(
        matches!(&knows.rows[0][0], Val::Str(s) if s == "Eve"),
        "KNOWS target from segment: {:?}",
        knows.rows[0][0]
    );

    // Reopen from disk: the set + segment reload, and the data survives.
    drop(writer);
    drop(gen1);
    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    let gen2 = graphs.get("people").unwrap();
    assert_eq!(gen2.uuid(), set_uuid, "reopen names the flushed set");
    assert_eq!(gen2.stack().segments().len(), 1, "segment reloaded");
    let view = MergedView::new(gen2.as_ref(), DeltaSnapshot::empty());
    let ast = parser::parse("MATCH (n:Person {name:'Eve'}) RETURN n.age").unwrap();
    let eve = Engine::new(&view, &cache).run(&ast).unwrap();
    assert!(
        matches!(eve.rows[0][0], Val::Int(60)),
        "Eve reloaded from the segment: {:?}",
        eve.rows[0][0]
    );

    std::fs::remove_dir_all(&root).ok();
}
