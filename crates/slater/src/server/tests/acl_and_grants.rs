// SPDX-License-Identifier: Apache-2.0
//! `acl_and_grants` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// The write grant is **per graph**: holding it on one graph authorises nothing on
/// another, and reads never need it.
#[test]
fn the_write_grant_is_per_graph_and_reads_stay_allowed() {
    let acl = acl_json(serde_json::json!({
        "people": ["read"],
        "scratch": ["read", "write"],
    }));
    let write = parser::parse_statement("MERGE (n:Person {name:'Dave'}) SET n.age = 1").unwrap();
    assert!(authorize_statement(&acl, "u", "scratch", &write).is_ok());
    assert!(
        authorize_statement(&acl, "u", "people", &write).is_err(),
        "a write grant on `scratch` must not leak to `people`"
    );

    let read = parser::parse_statement("MATCH (n:Person) RETURN count(*)").unwrap();
    assert!(!statement_mutates(&read));
    assert!(authorize_statement(&acl, "u", "people", &read).is_ok());
    assert!(authorize_statement(&acl, "u", "scratch", &read).is_ok());
}

/// `count(*)` over a **merged** view must net the delta's born rows in and its
/// suppressed rows out — and must do so without scanning the core (the fast path
/// reads `live_node_count`). Checked against the executor's own materialising scan,
/// which is the definition of what a read sees.
#[tokio::test]
async fn merged_count_star_nets_born_and_suppressed_rows() {
    let (_root, ctx) =
        build_writable_ctx_caps("merged_count", "slater-build", 1 << 20, 0, 0, 0, 0, 8, 0);
    let writer = ctx.graphs.writer("people").unwrap();
    let gen = ctx.graphs.get("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let try_write = |q: &str| {
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write: {q}"),
        };
        execute_write(
            &writer,
            gen.as_ref(),
            &stmt,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
    };
    let write = |q: &str| try_write(q).unwrap();
    let count = |q: &str| -> i64 {
        let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
        let ast = parser::parse(q).unwrap();
        let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
        match rows[0][0] {
            Val::Int(n) => n,
            ref other => panic!("expected an int count, got {other:?}"),
        }
    };
    // The materialising scan — the ground truth the fast path must agree with.
    let scanned = || -> i64 {
        let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
        let ast = parser::parse("MATCH (n) RETURN n.name").unwrap();
        let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
        rows.len() as i64
    };
    let check = |expected: i64| {
        assert_eq!(
            count("MATCH (n) RETURN count(*)"),
            expected,
            "whole-graph count"
        );
        assert_eq!(
            count("MATCH (n:Person) RETURN count(*)"),
            expected,
            "labelled count"
        );
        assert_eq!(scanned(), expected, "the scan agrees with the fast path");
    };

    check(3); // Alice, Bob, Carol.
    write("MERGE (n:Person {name:'Dave'}) SET n.age = 1"); // born
    check(4);
    write("MATCH (n:Person {name:'Alice'}) DETACH DELETE n"); // suppress a core row (Alice has edges)
    check(3);
    write("MATCH (n:Person {name:'Dave'}) DELETE n"); // suppress a born row (no edges)
    check(2);
    // A delete of a key that exists nowhere is refused outright, so it can never
    // enter the delta as an inert tombstone and wrongly decrement the count.
    assert!(try_write("MATCH (n:Person {name:'Ghost'}) DELETE n").is_err());
    check(2);
    write("MERGE (n:Person {name:'Alice'}) SET n.age = 31"); // resurrect the core row
    check(3);
}

/// The whole-graph metadata shapes — `labels(n)[0]`, `type(r)` and the bare edge
/// `count(*)` — must stay metadata reads over a delta and agree with the materialising
/// scan. Deleting a node also kills its incident edges, so the edge count drops by
/// that node's degree. Fixture: 3 `:Person`, one `Alice-[:KNOWS]->Bob`.
#[tokio::test]
async fn merged_metadata_and_edge_counts_track_the_delta() {
    let (_root, ctx) =
        build_writable_ctx_caps("merged_meta", "slater-build", 1 << 20, 0, 0, 0, 0, 8, 0);
    let writer = ctx.graphs.writer("people").unwrap();
    let gen = ctx.graphs.get("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let write = |q: &str| {
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write: {q}"),
        };
        execute_write(
            &writer,
            gen.as_ref(),
            &stmt,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
        .unwrap();
    };
    let rows = |q: &str| -> Vec<Vec<Val>> {
        let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
        let ast = parser::parse(q).unwrap();
        let out = Engine::new(&view, &cache).run(&ast).unwrap().rows;
        out
    };
    let one_int = |q: &str| -> i64 {
        let r = rows(q);
        match r[0][0] {
            Val::Int(n) => n,
            ref other => panic!("expected an int, got {other:?}"),
        }
    };
    // The count column of the first group row (`Val` has no `PartialEq`).
    let group_count = |q: &str| -> i64 {
        let r = rows(q);
        match r[0][1] {
            Val::Int(n) => n,
            ref other => panic!("expected an int count, got {other:?}"),
        }
    };
    // The materialising scan — ground truth for the edge count.
    let scanned_edges = || -> i64 { rows("MATCH ()-[r]->() RETURN r").len() as i64 };

    // Baseline: 3 nodes, 1 edge. The bare edge count used to have no fast path at all.
    assert_eq!(one_int("MATCH ()-[r]->() RETURN count(*)"), 1);
    assert_eq!(scanned_edges(), 1);
    assert_eq!(group_count("MATCH (n) RETURN labels(n)[0], count(*)"), 3);
    assert_eq!(group_count("MATCH ()-[r]->() RETURN type(r), count(*)"), 1);

    // A born node adds a label group but no edges.
    write("MERGE (n:Person {name:'Dave'}) SET n.age = 1");
    assert_eq!(
        group_count("MATCH (n) RETURN labels(n)[0], count(*)"),
        4,
        "born node counted in the label group"
    );
    assert_eq!(
        one_int("MATCH ()-[r]->() RETURN count(*)"),
        1,
        "born node adds no edges"
    );
    assert_eq!(scanned_edges(), 1);

    // DETACH-deleting a core endpoint also removes the edge incident to it (a plain
    // DELETE would be rejected while the edge is still there).
    write("MATCH (n:Person {name:'Bob'}) DETACH DELETE n");
    assert_eq!(
        one_int("MATCH ()-[r]->() RETURN count(*)"),
        0,
        "Alice→Bob dies with its endpoint"
    );
    assert_eq!(scanned_edges(), 0, "the scan agrees");
    assert_eq!(
        group_count("MATCH (n) RETURN labels(n)[0], count(*)"),
        3,
        "label group drops the deleted node"
    );
    assert!(
        rows("MATCH ()-[r]->() RETURN type(r), count(*)").is_empty(),
        "an empty reltype group is not emitted"
    );
}

/// An edge tombstone cannot be netted out of a counter (a deleted **core** edge carries
/// no edge id), so the edge fast paths must **decline** rather than report a wrong
/// number — the matcher then produces the right answer.
#[tokio::test]
async fn edge_tombstone_makes_the_edge_fast_path_decline_not_lie() {
    let (_root, ctx) = build_writable_ctx_caps(
        "merged_edge_tomb",
        "slater-build",
        1 << 20,
        0,
        0,
        0,
        0,
        8,
        0,
    );
    let writer = ctx.graphs.writer("people").unwrap();
    let gen = ctx.graphs.get("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    assert!(
        MergedView::new(gen.as_ref(), writer.delta_snapshot())
            .live_edge_count()
            .unwrap()
            .is_some(),
        "an empty delta is exactly countable"
    );

    let stmt = match parser::parse_statement(
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
    )
    .unwrap()
    {
        parser::ast::Statement::WriteEdge(w) => w,
        other => panic!("expected an edge delete, got {other:?}"),
    };
    execute_edge_write(
        &writer,
        gen.as_ref(),
        &stmt,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();

    let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
    assert!(
        view.live_edge_count().unwrap().is_none(),
        "an edge tombstone makes the counter-derived count inexact ⇒ decline"
    );
    // The query still answers correctly, via full execution.
    let ast = parser::parse("MATCH ()-[r]->() RETURN count(*)").unwrap();
    let counted = match Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0] {
        Val::Int(n) => n,
        ref other => panic!("expected an int, got {other:?}"),
    };
    assert_eq!(counted, 0, "the deleted edge is suppressed by the matcher");
}

/// A delta-born node is a real, readable node, so a plain `MATCH … SET` must be able
/// to update it — both while it is still in the active memtable and after it has been
/// flushed to an L0 segment. (It used to resolve the key against the core only, so
/// updating a node you had just created failed with "use MERGE to create it".)
#[tokio::test]
async fn match_set_updates_a_delta_born_node() {
    let (_root, ctx) =
        build_writable_ctx_caps("set_born", "slater-build", 1 << 20, 0, 0, 0, 0, 8, 0);
    let writer = ctx.graphs.writer("people").unwrap();
    let gen = ctx.graphs.get("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let try_write = |q: &str| {
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write: {q}"),
        };
        execute_write(
            &writer,
            gen.as_ref(),
            &stmt,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
    };
    let age_of = |name: &str| -> Option<i64> {
        let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
        let ast =
            parser::parse(&format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.age")).unwrap();
        let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
        rows.first().map(|r| match r[0] {
            Val::Int(n) => n,
            ref other => panic!("expected an int age, got {other:?}"),
        })
    };

    // Born, still in the active memtable → SET must find it.
    try_write("MERGE (n:Person {name:'Dave'}) SET n.age = 1").unwrap();
    try_write("MATCH (n:Person {name:'Dave'}) SET n.age = 2").unwrap();
    assert_eq!(
        age_of("Dave"),
        Some(2),
        "SET on a born node in the memtable"
    );

    // Flush it to an L0 segment, then SET again → must resolve across the levels.
    assert!(writer.flush_to_l0().unwrap(), "born row flushed to L0");
    try_write("MATCH (n:Person {name:'Dave'}) SET n.age = 3").unwrap();
    assert_eq!(age_of("Dave"), Some(3), "SET on a born node flushed to L0");

    // A key that exists in neither the core nor the delta is still a clear error.
    let e = try_write("MATCH (n:Person {name:'Nobody'}) SET n.age = 1").unwrap_err();
    assert!(e.message.contains("node to update"), "got: {}", e.message);
}

/// The same invariants once the delta is spread across **sealed L0 levels**: a born
/// row, its tombstone, and its resurrection each land in a different level, so the
/// count summary must fold newest-wins across levels rather than sum them.
#[tokio::test]
async fn merged_count_star_folds_across_l0_levels() {
    // memtable_bytes = 1 ⇒ every write flushes; trigger 0 ⇒ no compaction, so the
    // levels stay distinct and the cross-level fold is what is under test.
    let (_root, ctx) =
        build_writable_ctx_caps("merged_count_l0", "slater-build", 1, 0, 0, 0, 0, 8, 0);
    let writer = ctx.graphs.writer("people").unwrap();
    let gen = ctx.graphs.get("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let write = |q: &str| {
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write: {q}"),
        };
        execute_write(
            &writer,
            gen.as_ref(),
            &stmt,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
        .unwrap();
    };
    let count = || -> i64 {
        let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
        let ast = parser::parse("MATCH (n:Person) RETURN count(*)").unwrap();
        let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
        match rows[0][0] {
            Val::Int(n) => n,
            ref other => panic!("expected an int count, got {other:?}"),
        }
    };

    write("MERGE (n:Person {name:'Dave'}) SET n.age = 1");
    maybe_maintain_delta(&ctx, "people", &writer).await;
    assert_eq!(count(), 4, "born in L0");

    write("MATCH (n:Person {name:'Dave'}) DELETE n");
    maybe_maintain_delta(&ctx, "people", &writer).await;
    assert_eq!(
        count(),
        3,
        "tombstoned in a newer level than it was born in"
    );

    write("MERGE (n:Person {name:'Dave'}) SET n.age = 2");
    maybe_maintain_delta(&ctx, "people", &writer).await;
    assert_eq!(
        count(),
        4,
        "a newer MERGE resurrects it: the older tombstone must not still subtract"
    );
    assert!(writer.l0_len() >= 2, "the levels really are distinct");
}

/// Phase 4d-ii-a: the write path auto-maintains the delta. With a 1-byte memtable
/// cap every write flushes to an L0 segment; with a 3-segment compaction trigger the
/// third flush collapses the stack. Drives `execute_write` + `maybe_maintain_delta`
/// exactly as the RUN handler does, and confirms the born rows survive.
#[tokio::test]
async fn write_path_auto_flushes_and_compacts() {
    let (root, ctx) = build_writable_ctx_caps("auto_maint", "slater-build", 1, 3, 0, 0, 0, 8, 0);
    let writer = ctx.graphs.writer("people").unwrap();
    let gen = ctx.graphs.get("people").unwrap();

    let write = |q: &str| {
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a node write: {q}"),
        };
        execute_write(
            &writer,
            gen.as_ref(),
            &stmt,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
        .unwrap();
    };

    write("MERGE (n:Person {name:'Dave'}) SET n.age = 1");
    maybe_maintain_delta(&ctx, "people", &writer).await;
    assert_eq!(writer.l0_len(), 1, "first write flushed");
    assert!(writer.snapshot().is_empty(), "memtable freed by the flush");

    write("MERGE (n:Person {name:'Erin'}) SET n.age = 2");
    maybe_maintain_delta(&ctx, "people", &writer).await;
    assert_eq!(writer.l0_len(), 2, "second write flushed");

    write("MERGE (n:Person {name:'Fay'}) SET n.age = 3");
    maybe_maintain_delta(&ctx, "people", &writer).await;
    assert_eq!(
        writer.l0_len(),
        1,
        "third flush hit the compaction trigger and collapsed the stack"
    );

    // All three born rows still read back through the compacted delta.
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
    let ast = parser::parse("MATCH (n:Person) RETURN n.name").unwrap();
    let names: HashSet<String> = Engine::new(&view, &cache)
        .run(&ast)
        .unwrap()
        .rows
        .iter()
        .filter_map(|r| match &r[0] {
            Val::Str(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    for n in ["Dave", "Erin", "Fay"] {
        assert!(
            names.contains(n),
            "born {n} survives flush+compaction: {names:?}"
        );
    }
    std::fs::remove_dir_all(&root).ok();
}

/// Phase 6 closing slice: the write path auto-fires the two **segment-tier** rungs.
/// With a 1-byte `segmentFlushBytes` every write folds the whole delta into a core
/// segment (T2); with a 2-segment `maxUpperSegments` the third flush tips the stack
/// over budget and the same `maybe_maintain_delta` pass compacts a run (T3). Drives
/// `execute_write` + `maybe_maintain_delta` exactly as the RUN handler does, confirms
/// the stack grows then collapses, and that every born row survives — including a
/// reopen from disk (the segments are durable, the delta empty after each flush).
#[tokio::test]
async fn write_path_auto_flushes_and_compacts_segments() {
    // memtable_bytes 1 (L0 rungs also fire, harmlessly — the whole delta flushes
    // anyway), l0 trigger 0, no consolidation; segment_flush_bytes 1, max_upper 2.
    let (root, ctx) = build_writable_ctx_caps("auto_seg", "slater-build", 1, 0, 0, 0, 1, 2, 0);
    let writer = ctx.graphs.writer("people").unwrap();

    let write = |q: &str| {
        let gen = ctx.graphs.get("people").unwrap();
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a node write: {q}"),
        };
        execute_write(
            &writer,
            gen.as_ref(),
            &stmt,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
        .unwrap();
    };
    let segment_count = || ctx.graphs.get("people").unwrap().stack().segments().len();

    write("MERGE (n:Person {name:'Dave'}) SET n.age = 1");
    maybe_maintain_delta(&ctx, "people", &writer).await;
    assert_eq!(
        segment_count(),
        1,
        "first write flushed the delta into a segment"
    );
    assert_eq!(writer.total_bytes(), 0, "delta retired by the flush");

    write("MERGE (n:Person {name:'Erin'}) SET n.age = 2");
    maybe_maintain_delta(&ctx, "people", &writer).await;
    assert_eq!(segment_count(), 2, "second write appended a second segment");

    write("MERGE (n:Person {name:'Fay'}) SET n.age = 3");
    maybe_maintain_delta(&ctx, "people", &writer).await;
    let after = segment_count();
    assert!(
        after < 3,
        "third flush tipped the stack past maxUpperSegments and T3 folded a run: {after} segments"
    );
    assert!(
        after <= 2,
        "the stack is back within the 2-segment budget after compaction: {after}"
    );

    // Every born row reads back through the compacted segment stack.
    let names_through = |gen: &Generation, w: &Arc<DeltaWriter>| -> HashSet<String> {
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::new(gen, w.delta_snapshot());
        let ast = parser::parse("MATCH (n:Person) RETURN n.name").unwrap();
        let out: HashSet<String> = Engine::new(&view, &cache)
            .run(&ast)
            .unwrap()
            .rows
            .iter()
            .filter_map(|r| match &r[0] {
                Val::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        out
    };
    let served = ctx.graphs.get("people").unwrap();
    let names = names_through(served.as_ref(), &writer);
    for n in ["Dave", "Erin", "Fay"] {
        assert!(
            names.contains(n),
            "born {n} survives the segment fold: {names:?}"
        );
    }

    // Reopen the graph from disk with no writable layer: the born rows live in the
    // durable segments (the delta was empty after the last flush), so a fresh read
    // still serves them.
    let reopened = Graphs::open_all(&root, None).unwrap();
    let cache = BlockCache::new(1 << 20);
    let gen = reopened.get("people").unwrap();
    let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
    let ast = parser::parse("MATCH (n:Person) RETURN n.name").unwrap();
    let reopened_names: HashSet<String> = Engine::new(&view, &cache)
        .run(&ast)
        .unwrap()
        .rows
        .iter()
        .filter_map(|r| match &r[0] {
            Val::Str(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    for n in ["Dave", "Erin", "Fay"] {
        assert!(
            reopened_names.contains(n),
            "born {n} is durable across a reopen: {reopened_names:?}"
        );
    }
    std::fs::remove_dir_all(&root).ok();
}

/// Phase 7 slice 7.3: the write path auto-fires the T4 **GC** sweep after a T3 compaction.
/// With `segmentGcGraceSecs > 0` the sweep that `maybe_maintain_delta` runs after a
/// compaction folds a run *marks* the run's now-orphaned segment dirs (a `.gcmark` per dir)
/// but waits out the grace before deleting — so the marker's presence proves the wiring
/// fired GC without a fold-then-sleep. An explicit immediate sweep then reclaims them.
#[tokio::test]
async fn write_path_auto_gc_marks_orphans_after_compaction() {
    // segment_flush_bytes 1 (flush each write), max_upper 2 (compact when >2), grace 3600
    // (the auto-GC marks the orphans but holds them through the grace).
    let (root, ctx) = build_writable_ctx_caps("auto_gc", "slater-build", 1, 0, 0, 0, 1, 2, 3600);
    let writer = ctx.graphs.writer("people").unwrap();
    let write = |q: &str| {
        let gen = ctx.graphs.get("people").unwrap();
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a node write: {q}"),
        };
        execute_write(
            &writer,
            gen.as_ref(),
            &stmt,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
        .unwrap();
    };
    // Count the GC grace markers the sweep stamps under `<graph>/.gc/` (a `seg-<uuid>` per
    // orphaned segment observed within the grace).
    let gcmark_count = |root: &Path| -> usize {
        std::fs::read_dir(root.join("people").join(".gc"))
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().starts_with("seg-"))
                    .count()
            })
            .unwrap_or(0)
    };

    // Four flushes tip the stack past maxUpperSegments and drive at least one compaction,
    // whose orphaned run dirs the wiring's GC sweep marks.
    for (i, name) in ["Dave", "Erin", "Fay", "Gina"].iter().enumerate() {
        write(&format!(
            "MERGE (n:Person {{name:'{name}'}}) SET n.age = {i}"
        ));
        maybe_maintain_delta(&ctx, "people", &writer).await;
    }
    assert!(
        ctx.graphs.get("people").unwrap().stack().segments().len() <= 2,
        "the stack stayed within the compaction budget"
    );
    let marked = gcmark_count(&root);
    assert!(
        marked >= 1,
        "the auto-GC sweep marked the compacted run's orphaned dirs: {marked}"
    );

    // An immediate explicit sweep reclaims the marked orphans end-to-end.
    let rep = ctx.graphs.gc_orphan_segments("people", &root, 0).unwrap();
    assert!(
        !rep.deleted_segments.is_empty(),
        "the marked orphans are reclaimed: {rep:?}"
    );
    // Only live segments remain, and every born row still reads back.
    let cache = BlockCache::new(1 << 20);
    let served = ctx.graphs.get("people").unwrap();
    assert_eq!(
        seg_dirs(&root).len(),
        served.stack().segments().len(),
        "no orphan dirs linger after the sweep"
    );
    let view = MergedView::new(served.as_ref(), writer.delta_snapshot());
    let names: HashSet<String> = Engine::new(&view, &cache)
        .run(&parser::parse("MATCH (n:Person) RETURN n.name").unwrap())
        .unwrap()
        .rows
        .iter()
        .filter_map(|r| match &r[0] {
            Val::Str(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    for n in ["Dave", "Erin", "Fay", "Gina"] {
        assert!(names.contains(n), "born {n} survives GC: {names:?}");
    }
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn consolidation_due_is_a_fraction_of_core() {
    // Disabled / degenerate cases.
    assert!(!consolidation_due(1_000, 500, 0), "percent 0 disables");
    assert!(!consolidation_due(0, 5, 25), "empty core never fires");
    assert!(
        !consolidation_due(3, 3, 10),
        "core too small for 10% to mean a whole entity (threshold rounds to 0)"
    );
    // 25% of 4 entities = 1: one changed entity fires.
    assert!(consolidation_due(4, 1, 25));
    assert!(!consolidation_due(4, 0, 25), "no delta yet");
    // 10% of 100M entities = 10M: bounded write amplification on a large core.
    assert!(consolidation_due(100_000_000, 10_000_000, 10));
    assert!(!consolidation_due(100_000_000, 9_999_999, 10));
    // No overflow near u64 max.
    assert!(consolidation_due(u64::MAX, u64::MAX / 2, 25));
}

#[test]
fn window_permits_gates_the_fraction_trigger() {
    use crate::cron_window::CronWindow;
    // No window ⇒ a due consolidation is always permitted.
    assert!(window_permits(&None, (3, 15, 6, 3)));
    assert!(window_permits(&None, (12, 15, 6, 3)));

    // A 01:00–05:59 daily window permits inside and defers outside (hour granularity).
    let w = CronWindow::parse("0 1-5 * * *").unwrap();
    assert!(window_permits(&w, (1, 1, 1, 0)), "01:xx is inside");
    assert!(window_permits(&w, (5, 28, 12, 6)), "05:xx is inside");
    assert!(!window_permits(&w, (0, 15, 6, 3)), "00:xx is outside");
    assert!(!window_permits(&w, (12, 15, 6, 3)), "noon is outside");

    // A weekday-only window also gates on the day of week.
    let wd = CronWindow::parse("* 1-5 * * 1-5").unwrap();
    assert!(window_permits(&wd, (2, 10, 6, 3)), "02:xx Wednesday inside");
    assert!(!window_permits(&wd, (2, 10, 6, 0)), "02:xx Sunday deferred");
}

/// Phase 4d-ii-b end-to-end through the write path + real builder: a write that
/// pushes the delta past `deltaCorePercent` of the core auto-fires a background
/// consolidation, which folds the write into a fresh generation and retires the
/// delta — no manual `CALL` needed. Ignored by default (spawns `slater-build`).
#[tokio::test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
async fn write_path_auto_consolidates_at_core_fraction() {
    use std::time::Duration;
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    // The `people` fixture is 3 nodes + 1 edge = 4 entities; 25% = a threshold of 1,
    // so a single write is due. (Flush/compaction left at defaults; hard cap off.)
    let (root, ctx) = build_writable_ctx_caps("auto_consol", &bin, 64 << 20, 4, 25, 0, 0, 8, 0);
    let writer = ctx.graphs.writer("people").unwrap();
    let gen0 = ctx.graphs.get("people").unwrap();

    let stmt =
        match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 99").unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
    execute_write(
        &writer,
        gen0.as_ref(),
        &stmt,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();
    assert!(consolidation_due(4, writer.delta_entity_count() as u64, 25));

    // The write-path hook spawns the background consolidation.
    maybe_maintain_delta(&ctx, "people", &writer).await;

    // Wait for the detached consolidation to publish a fresh generation.
    let mut waited = 0u64;
    while ctx.graphs.get("people").unwrap().uuid() == gen0.uuid() {
        assert!(
            waited < 120_000,
            "auto-consolidation did not complete in time"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        waited += 100;
    }
    let gen1 = ctx.graphs.get("people").unwrap();
    assert_ne!(gen1.uuid(), gen0.uuid(), "a fresh generation was published");

    // Alice's write is now baked into the new core; the delta retired.
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::new(gen1.as_ref(), writer.delta_snapshot());
    let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap();
    let age = Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0].clone();
    assert!(
        matches!(age, Val::Int(99)),
        "folded write served from the new core"
    );
    assert!(
        !writer.is_consolidating(),
        "consolidation released its claim"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A ConnCtx over a writable-layer-enabled `people` graph, with `builder_bin`
/// pointed at the given binary — the harness for the `CALL slater.consolidate()`
/// trigger (`execute_consolidate`).
fn build_writable_ctx(tag: &str, builder_bin: &str) -> (PathBuf, Arc<ConnCtx>) {
    build_writable_ctx_caps(tag, builder_bin, 64 << 20, 4, 0, 0, 0, 8, 0)
}

/// [`build_writable_ctx`] with explicit delta caps, so a test can drive the auto
/// flush/compaction/consolidation thresholds (Phase 4d-ii, Phase 6 segment tiers).
#[allow(clippy::too_many_arguments)]
fn build_writable_ctx_caps(
    tag: &str,
    builder_bin: &str,
    memtable_bytes: usize,
    l0_compaction_trigger: usize,
    delta_core_percent: usize,
    delta_hard_bytes: usize,
    segment_flush_bytes: usize,
    max_upper_segments: usize,
    segment_gc_grace_secs: u64,
) -> (PathBuf, Arc<ConnCtx>) {
    let (root, _graph) = testgen::write_indexed_people(tag);
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let graphs = Arc::new(graphs);
    // A minimal ACL (unused by consolidation, but ConnCtx requires one).
    let acl_path = root.join("acl.json");
    let json = serde_json::json!({
        "users": { "writer": {
            "passwordArgon2id": hash_password("pw").unwrap(),
            "grants": { "people": ["read"] }
        }}
    });
    std::fs::write(&acl_path, json.to_string()).unwrap();
    let acl = Arc::new(AclHandle::load(&acl_path).unwrap());
    let ctx = Arc::new(ConnCtx {
        fulltext_max_hits: crate::config::DEFAULT_FULLTEXT_MAX_HITS,
        acl,
        graphs,
        cache: Arc::new(BlockCache::new(1 << 20)),
        vector_cache: Arc::new(VectorIndexCache::new(1 << 20)),
        rw_indexes: Arc::new(RwIndexCache::new()),
        rw_index_cfg: crate::rwindex::RwIndexConfig::default(),
        result_cache: Arc::new(ResultCache::new(1 << 20)),
        max_rows: 100_000,
        timeout_ms: 0,
        max_intermediate: 1_000_000,
        max_scan: 500_000_000,
        intermediate_budget: Arc::new(GlobalIntermediateBudget::new(8_000_000)),
        max_shortest_path_explore: 0,
        adj_stream_threshold: 8192,
        adj_stream_chunk: 8192,
        fanout_pool: None,
        beam_width: 64,
        temp_beam_width: 128,
        bind_addr: "127.0.0.1:7687".to_string(),
        default_graph: Some("people".to_string()),
        use_selection: RwLock::new(HashMap::new()),
        memgraph_users: RwLock::new(HashSet::new()),
        max_message_bytes: 64 * 1024 * 1024,
        max_pre_auth_bytes: 64 * 1024,
        login_timeout_ms: 0,
        tls_handshake_timeout_ms: 0,
        idle_timeout_ms: 0,
        pre_auth_limit: Arc::new(Semaphore::new(semaphore_permits(4_096))),
        auth_limit: Arc::new(Semaphore::new(semaphore_permits(4))),
        max_auth_failures: 3,
        write_limit: Arc::new(Semaphore::new(semaphore_permits(4))),
        parse_limit: Arc::new(Semaphore::new(semaphore_permits(32))),
        per_ip: Arc::new(Mutex::new(HashMap::new())),
        max_per_ip: 0,
        diag: Arc::new(crate::diag::Diagnostics::new(false)),
        conn_limit: Arc::new(Semaphore::new(semaphore_permits(16_384))),
        max_connections: 16_384,
        max_pre_auth_connections: 4_096,
        data_dir: root.clone(),
        builder_bin: builder_bin.to_string(),
        builder_limits: BuilderLimits::default(),
        builder_key_env: None,
        memtable_bytes,
        l0_compaction_trigger,
        segment_flush_bytes,
        max_upper_segments,
        segment_gc_grace_secs,
        delta_core_percent,
        delta_hard_bytes,
        consolidate_window: None,
    });
    (root, ctx)
}

/// Drive a durable `SET` on Alice through the writable layer — a small helper for
/// the consolidation-trigger tests so there is a live delta to fold.
fn write_alice_age_99(ctx: &Arc<ConnCtx>) {
    let gen0 = ctx.graphs.get("people").unwrap();
    let writer = ctx.graphs.writer("people").unwrap();
    let stmt =
        match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 99").unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
    execute_write(
        &writer,
        gen0.as_ref(),
        &stmt,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();
}

/// The `CALL slater.consolidate()` trigger reaches consolidation and surfaces a
/// builder failure as a query `Failure` (not a panic), non-destructively: a missing
/// builder binary fails the rebuild, the old core keeps serving, and the delta stays
/// live. Proves the RUN-handler → `execute_consolidate` → `consolidate_graph` wiring
/// (data dir, builder bin, caches, `spawn_blocking`, error propagation).
#[tokio::test]
async fn bolt_consolidate_surfaces_a_builder_failure() {
    let (root, ctx) = build_writable_ctx("bolt_consolidate_fail", "/nonexistent/slater-build-xyz");
    write_alice_age_99(&ctx);
    let gen0 = ctx.graphs.get("people").unwrap();

    let err = execute_consolidate(&ctx, "people").await.unwrap_err();
    assert!(
        err.message.contains("consolidation failed"),
        "expected a surfaced builder failure, got: {}",
        err.message
    );
    // Non-destructive: old core still served, the write still overlaid.
    assert_eq!(ctx.graphs.get("people").unwrap().uuid(), gen0.uuid());
    let writer = ctx.graphs.writer("people").unwrap();
    assert!(
        !writer.snapshot().is_empty(),
        "the delta must survive a failed consolidation"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A lost `begin_consolidation` single-flight race surfaces as the typed
/// `CODE_CONSOLIDATION_IN_PROGRESS` code, so `spawn_auto_consolidation` can classify
/// it debug-not-warn by branching on the *type* rather than matching message text (the
/// substring `.contains("already in progress")` it replaced would false-positive on any
/// unrelated error that merely mentioned the phrase).
#[tokio::test]
async fn execute_consolidate_reports_a_lost_race_by_typed_code() {
    let (root, ctx) = build_writable_ctx("bolt_consolidate_race", "/nonexistent/slater-build-xyz");
    write_alice_age_99(&ctx);
    // Hold the exclusive claim first, so the trigger below loses the single-flight race.
    let writer = ctx.graphs.writer("people").unwrap();
    assert!(
        writer.begin_consolidation(),
        "the test must win the claim first"
    );

    let err = execute_consolidate(&ctx, "people").await.unwrap_err();
    assert_eq!(
        err.code, CODE_CONSOLIDATION_IN_PROGRESS,
        "a lost race must classify by typed code, got {}: {}",
        err.code, err.message
    );

    writer.end_consolidation();
    std::fs::remove_dir_all(&root).ok();
}

/// True end-to-end through the Bolt trigger and the real `slater-build` binary:
/// `CALL slater.consolidate()` folds the delta into a fresh generation, returns its
/// id as the `generation` column, and retires the delta. Ignored by default (needs
/// the builder) — run it exactly like `consolidate_via_real_builder`, with
/// `SLATER_BUILD_BIN` pointing at the built binary.
#[tokio::test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
async fn bolt_consolidate_trigger_folds_delta_via_real_builder() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let (root, ctx) = build_writable_ctx("bolt_consolidate_real", &bin);
    write_alice_age_99(&ctx);
    let gen0 = ctx.graphs.get("people").unwrap();

    let (cols, rows) = execute_consolidate(&ctx, "people").await.unwrap();
    assert_eq!(cols, vec!["generation".to_string()]);
    let new_uuid = ctx.graphs.get("people").unwrap().uuid();
    assert_ne!(
        new_uuid,
        gen0.uuid(),
        "consolidation rebuilt a new generation"
    );
    assert!(
        matches!(&rows[0][0], PsValue::String(s) if *s == new_uuid.to_string()),
        "the trigger returns the new generation id"
    );
    let writer = ctx.graphs.writer("people").unwrap();
    assert!(
        writer.snapshot().is_empty(),
        "the delta is retired once folded into the core"
    );
    std::fs::remove_dir_all(&root).ok();
}
