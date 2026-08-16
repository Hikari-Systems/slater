// SPDX-License-Identifier: Apache-2.0
//! `dump_and_gc` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// End-to-end Phase 1d-B: a durable delta is folded into a fresh generation by
/// consolidation. The injected builder inspects the dump (proving the serialiser
/// saw the *merged* state) and independently publishes the known-correct
/// consolidated generation; afterwards the served core carries the write with no
/// delta, the writer is re-bound to the new core, and the consumed WAL segments
/// are gone — leaving only the fresh post-freeze segment.
#[test]
fn consolidate_folds_delta_into_fresh_generation() {
    let (root, _graph) = testgen::write_indexed_people("consolidate_e2e");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let gen0 = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let wal_dir = writer.wal_dir();

    // Overwrite Alice's age via the delta.
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
    assert!(
        !writer.snapshot().is_empty(),
        "delta live before consolidation"
    );

    // Builder stand-in: assert the dump reflects the merged age, then — modelling a
    // client that keeps writing *during* the rebuild (freeze has happened, retire has
    // not) — apply a post-freeze write (Bob's age → 77) before publishing an
    // independently-correct consolidated generation (Alice age 99) at a new uuid. The
    // post-freeze write is deliberately absent from the dump, so it must be carried
    // forward onto the new core by retire (Phase 4a).
    let new_uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0099);
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let writer_mid = writer.clone();
    let gen_mid = gen0.clone();
    let build =
        |dump: &Path, g: &str, dd: &Path, _key: Option<&[u8]>, _acl: Option<&Path>| -> Result<()> {
            assert_eq!(
                dump_age(dump, "Alice"),
                Some(99),
                "dump should carry the merged age"
            );
            assert_ne!(
                dump_age(dump, "Bob"),
                Some(77),
                "the post-freeze write (Bob age 77) must not be in the frozen dump"
            );
            assert_eq!(g, "people");
            let bob = match parser::parse_statement("MATCH (n:Person {name:'Bob'}) SET n.age = 77")
                .unwrap()
            {
                parser::ast::Statement::Write(w) => w,
                _ => unreachable!(),
            };
            execute_write(
                &writer_mid,
                gen_mid.as_ref(),
                &bob,
                &HashMap::new(),
                TEST_BOLT_VERSION,
            )
            .unwrap();
            testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
            Ok(())
        };
    let published = graphs
        .consolidate_graph("people", &cache, &vc, &root, None, build)
        .unwrap();
    assert_eq!(published.0, new_uuid, "swapped to the new generation");

    // The served core is now the new generation with Alice's write baked in; the
    // post-freeze Bob write survived as a delta re-resolved onto the new core.
    let gen1 = graphs.get("people").unwrap();
    assert_eq!(gen1.uuid().0, new_uuid);
    assert!(
        !writer.snapshot().is_empty(),
        "the post-freeze write is carried forward, not dropped"
    );
    let read_age = |name: &str| -> Val {
        let view = MergedView::new(
            gen1.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let ast =
            parser::parse(&format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.age")).unwrap();
        let age = Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0].clone();
        age
    };
    assert!(
        matches!(read_age("Alice"), Val::Int(99)),
        "consolidated age served from the core"
    );
    assert!(
        matches!(read_age("Bob"), Val::Int(77)),
        "post-freeze write served from the carried-forward delta over the new core"
    );

    // The writer is re-bound to the new core; the scratch dump is cleaned up; only
    // the post-freeze segment remains (freeze's fresh segment, now holding Bob).
    assert_eq!(
        writer.core_uuid(),
        gen1.uuid(),
        "writer re-bound to new core"
    );
    assert_no_consolidate_scratch(&root, "people");
    assert_eq!(
        wal_count(&wal_dir),
        1,
        "only the post-freeze segment remains"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 7 slice 7.1: the consolidation dump serialiser folds the **core stack**, so a
/// retarget over a stacked set collapses it to a *correct* singleton. After a flush moves a
/// base-node patch (Alice→99), a base-node delete (Carol), a born node (Dave) and a born
/// edge (Dave→Bob) into one segment, dumping the served stacked generation with an empty
/// delta must reflect the **segment** state — not the stale base bytes the Phase-0.5
/// byte-copy fast path would emit. Concretely: Alice carries the segment's patched age
/// (proving the fast path yields to the decode-through-stack slow path for a
/// segment-overridden base id), Carol is elided and the survivors renumbered gaplessly
/// (proving the segment tombstone joins the combined tombstone set that drives `compact_id`),
/// and Dave + his born edge appear with compacted endpoints.
#[test]
fn consolidation_dump_folds_the_segment_stack() {
    let (root, _g) = testgen::write_indexed_people("retarget_dump_71");
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
    // A base-node patch, a base-node delete, a born node, and a born edge from the born
    // node to a surviving base node — every stack override kind in one flush.
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
    write(&graphs, "MATCH (n:Person {name:'Carol'}) DETACH DELETE n");
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(
        &graphs,
        "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Bob'})",
    );
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes");
    let gen = graphs.get("people").unwrap();
    assert_eq!(gen.stack().segments().len(), 1, "one upper segment");
    assert!(
        graphs.writer("people").unwrap().snapshot().is_empty(),
        "delta retired empty — the dump reads the stack alone"
    );

    // Dump the served *stacked* generation with an empty delta.
    let dir = root.join(".retarget71.dump");
    let _ = std::fs::remove_dir_all(&dir);
    let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
    crate::consolidate::serialise_binary_dump(&Engine::new(&view, &cache), &view, &dir, None)
        .unwrap();

    // Read it back: id → name / age, and the edges as (src-name, dst-name, reltype).
    use graph_format::consolidate_dump::DumpReader;
    let r = DumpReader::open(&dir, None).unwrap();
    let keys = r.meta().property_keys.clone();
    let reltypes = r.meta().reltypes.clone();
    let mut id_name: HashMap<u64, String> = HashMap::new();
    let mut id_age: HashMap<u64, i64> = HashMap::new();
    r.for_each_node(|id, _lb, pb| {
        for (k, v) in graph_format::columns::decode_props(pb).unwrap() {
            match keys[k as usize].as_str() {
                "name" => {
                    if let graph_format::ids::Value::Str(s) = v {
                        id_name.insert(id, s);
                    }
                }
                "age" => {
                    if let graph_format::ids::Value::Int(i) = v {
                        id_age.insert(id, i);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    })
    .unwrap();
    let mut edges: Vec<(String, String, String)> = Vec::new();
    r.for_each_edge(|_id, s, d, t, _pb| {
        edges.push((
            id_name[&s].clone(),
            id_name[&d].clone(),
            reltypes[t as usize].clone(),
        ));
        Ok(())
    })
    .unwrap();

    // Three survivors — Carol is gone, and the dense ids are gapless [0,1,2].
    assert_eq!(id_name.len(), 3, "Carol elided: Alice, Bob, Dave survive");
    let mut ids: Vec<u64> = id_name.keys().copied().collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2], "survivors renumbered gaplessly");
    let name_set: std::collections::HashSet<&str> = id_name.values().map(String::as_str).collect();
    assert!(
        !name_set.contains("Carol"),
        "the segment tombstone reclaimed Carol"
    );
    for expect in ["Alice", "Bob", "Dave"] {
        assert!(name_set.contains(expect), "{expect} present in the dump");
    }
    // The segment patch wins over the stale base bytes — THE fix under test.
    let age_of = |who: &str| -> i64 {
        let id = *id_name.iter().find(|(_, n)| n.as_str() == who).unwrap().0;
        id_age[&id]
    };
    assert_eq!(
        age_of("Alice"),
        99,
        "Alice carries the SEGMENT-patched age, not base 30"
    );
    assert_eq!(
        age_of("Bob"),
        25,
        "untouched base node keeps its byte-copied age"
    );
    assert_eq!(age_of("Dave"), 50, "segment-born node carried");

    // The surviving base edge and the born edge, both with compacted endpoints.
    assert_eq!(
        edges.len(),
        2,
        "Alice→Bob (base) + Dave→Bob (born): {edges:?}"
    );
    assert!(edges.contains(&("Alice".into(), "Bob".into(), "KNOWS".into())));
    assert!(edges.contains(&("Dave".into(), "Bob".into(), "KNOWS".into())));

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::remove_dir_all(&root).ok();
}

/// Phase 7 slice 7.1 (orchestration): `consolidate_graph` over a **stacked** set folds it
/// back to a singleton via the Phase-0 direct dump path — the terminal D50 rung. The
/// injected builder asserts the dump it is handed reflects the folded segment state (proving
/// the retarget reads through the stack, not the stale base), then publishes an
/// independently-correct singleton; afterwards the served core is a singleton (the stack
/// collapsed), the writer is re-bound, and a post-freeze write is carried forward.
#[test]
fn consolidate_over_a_stacked_set_collapses_to_a_singleton() {
    let (root, _g) = testgen::write_indexed_people("retarget_e2e_71");
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
    // Flush a patch + delete + born into a segment, so the core we consolidate is stacked.
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
    write(&graphs, "MATCH (n:Person {name:'Carol'}) DETACH DELETE n");
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes");
    let gen0 = graphs.get("people").unwrap();
    assert_eq!(
        gen0.stack().segments().len(),
        1,
        "core is stacked before the retarget"
    );

    // Builder stand-in: assert the dump carries the folded segment state (Alice patched,
    // Carol gone, Dave born), apply a post-freeze write (Bob→77) modelling a client writing
    // during the rebuild, then publish an independently-correct singleton.
    let new_uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0071);
    let writer = graphs.writer("people").unwrap();
    let writer_mid = writer.clone();
    let gen_mid = gen0.clone();
    let build =
        |dump: &Path, g: &str, dd: &Path, _key: Option<&[u8]>, _acl: Option<&Path>| -> Result<()> {
            let nodes = dump_nodes(dump);
            assert_eq!(
                dump_age(dump, "Alice"),
                Some(99),
                "dump carries the segment patch"
            );
            assert!(
                !nodes.contains_key("Carol"),
                "dump reclaimed the segment tombstone"
            );
            assert_eq!(
                dump_age(dump, "Dave"),
                Some(50),
                "dump carries the segment-born node"
            );
            assert_eq!(g, "people");
            let bob = match parser::parse_statement("MATCH (n:Person {name:'Bob'}) SET n.age = 77")
                .unwrap()
            {
                parser::ast::Statement::Write(w) => w,
                _ => unreachable!(),
            };
            execute_write(
                &writer_mid,
                gen_mid.as_ref(),
                &bob,
                &HashMap::new(),
                TEST_BOLT_VERSION,
            )
            .unwrap();
            testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
            Ok(())
        };
    let published = graphs
        .consolidate_graph("people", &cache, &vc, &root, None, build)
        .unwrap();
    assert_eq!(
        published.0, new_uuid,
        "swapped to the consolidated singleton"
    );

    // The stack collapsed: the served core is now a singleton, the writer re-bound.
    let gen1 = graphs.get("people").unwrap();
    assert_eq!(gen1.uuid().0, new_uuid);
    assert!(
        gen1.stack().is_singleton(),
        "the retarget folded the segment stack into a singleton base"
    );
    assert_eq!(
        writer.core_uuid(),
        gen1.uuid(),
        "writer re-bound to the new core"
    );
    // The post-freeze write survived as a delta re-resolved onto the new core.
    let read_age = |name: &str| -> Val {
        let view = MergedView::new(gen1.as_ref(), writer.delta_snapshot());
        let ast =
            parser::parse(&format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.age")).unwrap();
        let age = Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0].clone();
        age
    };
    assert!(
        matches!(read_age("Bob"), Val::Int(77)),
        "post-freeze write carried forward"
    );
    assert_no_consolidate_scratch(&root, "people");

    std::fs::remove_dir_all(&root).ok();
}

// ── Phase 7 slice 7.2: orphan segment/set GC ─────────────────────────────────

/// The `<uuid>.json` set manifest file names under `<root>/people/sets/`.
fn set_files(root: &Path) -> Vec<String> {
    std::fs::read_dir(root.join("people").join("sets"))
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".json") && !n.starts_with('.'))
                .collect()
        })
        .unwrap_or_default()
}

/// Phase 7 slice 7.2: the GC sweep reclaims the disk the flush and compaction slices
/// intentionally leave behind. Two flushes stack two segments and orphan the first set;
/// GC reclaims the stale set while both (live) segments survive. Compacting the two
/// segments into one then orphans the run's two dirs + the pre-compaction set; GC reclaims
/// exactly those, keeping the merged segment and the current set — and never touching the
/// base generation directory. Reads stay consistent across the whole sweep.
#[test]
fn gc_reclaims_stale_sets_and_compacted_segments() {
    let (root, _g) = testgen::write_indexed_people("gc_reclaim_72");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let base_uuid = graphs.get("people").unwrap().base_uuid();

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
            _ => panic!("expected a write: {q}"),
        }
    };

    // Two flushes → two segments; `current` names set2 (base + seg1 + seg2), set1 is stale.
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    assert_eq!(graphs.get("people").unwrap().stack().segments().len(), 2);
    assert_eq!(set_files(&root).len(), 2, "set1 (stale) + set2 (current)");
    assert_eq!(seg_dirs(&root).len(), 2, "two live segments");

    // Immediate GC reclaims the stale set1.json; both segments are live under set2.
    let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
    assert_eq!(rep.deleted_sets.len(), 1, "the stale set is reclaimed");
    assert!(
        rep.deleted_segments.is_empty(),
        "both segments live under set2"
    );
    assert_eq!(set_files(&root).len(), 1, "only the current set remains");
    assert_eq!(seg_dirs(&root).len(), 2, "segments untouched");

    // Compact the two segments into one → set3 (base + merged); seg1, seg2 and set2 orphan.
    graphs
        .compact_graph_segments("people", &vc, &root, 0, 2)
        .unwrap();
    assert_eq!(graphs.get("people").unwrap().stack().segments().len(), 1);
    assert_eq!(
        seg_dirs(&root).len(),
        3,
        "2 compacted + 1 merged on disk pre-GC"
    );

    let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
    assert_eq!(
        rep.deleted_segments.len(),
        2,
        "the compacted run's dirs reclaimed"
    );
    assert_eq!(
        rep.deleted_sets.len(),
        1,
        "the pre-compaction set reclaimed"
    );
    assert_eq!(seg_dirs(&root).len(), 1, "only the merged segment remains");
    assert_eq!(set_files(&root).len(), 1, "only the current set remains");
    assert!(
        root.join("people").join(base_uuid.0.to_string()).exists(),
        "GC never touches the base generation directory"
    );

    // Reads are consistent after the sweep: 3 base + Dave + Eve.
    let gen = graphs.get("people").unwrap();
    let w = graphs.writer("people").unwrap();
    let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
    let n = Engine::new(&view, &cache)
        .run(&parser::parse("MATCH (n:Person) RETURN count(*)").unwrap())
        .unwrap();
    assert!(matches!(n.rows[0][0], Val::Int(5)), "count intact after GC");

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 7 slice 7.2: an orphan is not deleted until it has been observed unreferenced for
/// the grace period. A stale set is marked (not deleted) by sweeps within the grace, and
/// only an eligible (here: immediate) sweep reclaims it — the reader-safety guarantee.
#[test]
fn gc_respects_the_grace_before_reclaiming() {
    let (root, _g) = testgen::write_indexed_people("gc_grace_72");
    let wal = root.join("_wal");
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
            _ => panic!("expected a write: {q}"),
        }
    };
    // Two flushes orphan set1.
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    assert_eq!(set_files(&root).len(), 2, "set1 stale + set2 current");

    // A large grace: the first sweep only *marks* the stale set — nothing is deleted.
    let rep = graphs.gc_orphan_segments("people", &root, 3600).unwrap();
    assert!(
        rep.deleted_sets.is_empty() && rep.deleted_segments.is_empty(),
        "nothing deleted within the grace"
    );
    assert!(
        rep.marked >= 1,
        "the stale set was marked for a later sweep"
    );
    assert_eq!(
        set_files(&root).len(),
        2,
        "stale set still present within grace"
    );
    // A second sweep, still within the grace, keeps waiting.
    let rep2 = graphs.gc_orphan_segments("people", &root, 3600).unwrap();
    assert!(rep2.deleted_sets.is_empty(), "still waiting out the grace");
    assert_eq!(set_files(&root).len(), 2);
    // Once eligible (immediate), the stale set is reclaimed.
    let rep3 = graphs.gc_orphan_segments("people", &root, 0).unwrap();
    assert_eq!(rep3.deleted_sets.len(), 1, "eligible orphan reclaimed");
    assert_eq!(set_files(&root).len(), 1, "only the current set remains");

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 7 slice 7.2: after a retarget collapses a stacked set to a singleton (slice 7.1),
/// `current` names a bare generation with no set file — so the *whole* prior set and every
/// one of its segments is orphaned. GC reclaims them all, leaving the base generation and
/// the freshly built singleton generation directories intact and the graph readable.
#[test]
fn gc_after_retarget_reclaims_the_prior_set() {
    let (root, _g) = testgen::write_indexed_people("gc_retarget_72");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let base_uuid = graphs.get("people").unwrap().base_uuid();

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
            _ => panic!("expected a write: {q}"),
        }
    };
    // Flush a segment so the core is stacked (set1 over base + seg).
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    assert_eq!(seg_dirs(&root).len(), 1);
    assert_eq!(set_files(&root).len(), 1);

    // Retarget to a singleton via an injected builder that publishes a fresh generation.
    let new_uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0072);
    let build = |_dump: &Path,
                 g: &str,
                 dd: &Path,
                 _key: Option<&[u8]>,
                 _acl: Option<&Path>|
     -> Result<()> {
        assert_eq!(g, "people");
        testgen::write_indexed_people_at(dd, new_uuid, [30, 25, 40]);
        Ok(())
    };
    graphs
        .consolidate_graph("people", &cache, &vc, &root, None, build)
        .unwrap();
    let gen1 = graphs.get("people").unwrap();
    assert!(gen1.stack().is_singleton(), "retarget collapsed the stack");
    assert_eq!(gen1.uuid().0, new_uuid);
    // The prior set + segment linger on disk until GC (the deferred reclamation).
    assert_eq!(seg_dirs(&root).len(), 1, "prior segment lingers pre-GC");
    assert_eq!(set_files(&root).len(), 1, "prior set lingers pre-GC");

    // GC reclaims the whole prior set + its segment (current is a bare singleton gen).
    let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
    assert_eq!(rep.deleted_segments.len(), 1, "prior segment reclaimed");
    assert_eq!(rep.deleted_sets.len(), 1, "prior set reclaimed");
    assert_eq!(seg_dirs(&root).len(), 0);
    assert_eq!(set_files(&root).len(), 0);
    // Both generation directories survive — GC only touches segments/ and sets/.
    assert!(
        root.join("people").join(base_uuid.0.to_string()).exists(),
        "base generation survives"
    );
    assert!(
        root.join("people").join(new_uuid.to_string()).exists(),
        "the retargeted singleton generation survives"
    );

    // The singleton still serves.
    let gen = graphs.get("people").unwrap();
    let w = graphs.writer("people").unwrap();
    let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
    let alice = Engine::new(&view, &cache)
        .run(&parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap())
        .unwrap();
    assert!(
        matches!(alice.rows[0][0], Val::Int(30)),
        "singleton readable after GC"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// The **T3 merge** fold. Two segments, each touching the same vector index: the older
/// re-embeds a node, the newer removes another's embedding. Folding them into one segment
/// must preserve both — and must carry the *removal* forward, since the removed node's
/// vector still sits in the base below the run and would otherwise resurface the moment
/// the segment that suppressed it was merged away.
#[test]
fn a_segment_merge_folds_vector_embeds_and_removals() {
    let (root, graph, _) = testgen::write_basic("vec_merge_t3");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let write = |graphs: &Graphs, q: &str| {
        let gen = graphs.get(&graph).unwrap();
        let writer = graphs.writer(&graph).unwrap();
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
            _ => panic!("expected a write: {q}"),
        }
    };
    let knn = |graphs: &Graphs, q: &str| -> Vec<i64> {
        let gen = graphs.get(&graph).unwrap();
        let snap = DeltaSnapshot::from_memtable(graphs.writer(&graph).unwrap().snapshot());
        let view = MergedView::new(gen.as_ref(), snap);
        let ast = parser::parse(&format!(
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 5, vecf32({q})) \
                 YIELD node, score RETURN id(node) AS id"
        ))
        .unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        res.rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(i) => i,
                ref o => panic!("unexpected KNN row {o:?}"),
            })
            .collect()
    };

    // Segment 0: re-embed Alice (0).
    write(
        &graphs,
        "MATCH (n:Person {name:'Alice'}) SET n.embedding = vecf32([0.0, 0.0, 1.0])",
    );
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("first flush");

    // Segment 1: remove Bob (1)'s embedding.
    write(&graphs, "MATCH (n:Person {name:'Bob'}) REMOVE n.embedding");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("second flush");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 2);

    let before = knn(&graphs, "[0.0, 0.0, 1.0]");
    assert_eq!(before[0], 0, "Alice's re-embed leads");
    assert!(
        !before.contains(&1),
        "Bob's embedding is removed: {before:?}"
    );

    // Fold the two segments into one.
    graphs
        .compact_graph_segments(&graph, &vc, &root, 0, 2)
        .unwrap();
    assert_eq!(
        graphs.get(&graph).unwrap().stack().segments().len(),
        1,
        "the run folded into a single segment"
    );

    let after = knn(&graphs, "[0.0, 0.0, 1.0]");
    assert_eq!(
        after, before,
        "the merged segment must read identically to the run it replaced — Alice's \
             re-embed kept, and Bob's removal still suppressing the base vector below the run"
    );
    assert!(
        !after.contains(&1),
        "the removal must be carried into the merged segment, or Bob's base vector \
             resurfaces the moment the segment that suppressed it is folded away; got {after:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// End-to-end (HIK-113): a flush whose live embedded set crosses the floor seals a
/// per-segment Vamana; the KNN read beam-searches it (recall ≥ 0.9 vs an exact brute force
/// over the live set); a T3 merge rebuilds it and — crucially — **frees the retired
/// segments' pinned PQ** (the pinning trap: `bytes()` must not grow); and a segment whose
/// sealed files are **deleted** falls back to an exact brute force. One heavy test because
/// the seal only fires above the ~2000-vector floor.
#[test]
fn sealed_segment_index_recall_merge_unpin_and_missing_sidecar_fallback() {
    // Deterministic unit vectors (negative components ⇒ must re-embed via a bound param).
    fn xorshift(s: &mut u64) -> u64 {
        *s ^= *s << 13;
        *s ^= *s >> 7;
        *s ^= *s << 17;
        *s
    }
    let dim = 16usize;
    let n = 2_100usize; // just over SEGMENT_INDEX_MIN_VECTORS (2000)
    let mut st = 0x9e37_79b9_7f4a_7c15u64;
    let vecs: Vec<Vec<f32>> = (0..n)
        .map(|_| {
            let v: Vec<f32> = (0..dim)
                .map(|_| (xorshift(&mut st) % 2000) as f32 / 1000.0 - 1.0)
                .collect();
            let nrm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
            v.iter().map(|x| x / nrm).collect()
        })
        .collect();

    let (root, graph) = testgen::write_vector_docs("vec_seg_sealed", &vecs);
    let wal = root.join("_wal");
    let cache = BlockCache::new(4 << 20);
    let vc = VectorIndexCache::new(64 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // Re-embed every doc onto its own vector (delta), then flush — the segment's live
    // embedded set is all `n`, which seals a Vamana. Do it twice ⇒ two sealed segments.
    let reembed_all = |graphs: &Graphs| {
        for (i, v) in vecs.iter().enumerate() {
            embed_param(graphs, &graph, &format!("d{i:02}"), v);
        }
    };
    reembed_all(&graphs);
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("first sealing flush");
    let bytes_1seg = vc.bytes();
    assert!(
        bytes_1seg > 0,
        "the sealed segment's PQ must be pinned after the flush"
    );

    // Recall of the sealed beam vs an exact brute force over the live set — the base index
    // is brute-force (n < ann_threshold) but every id is superseded by the segment, so the
    // answer comes from the sealed segment beam. Assert against independently-derived truth.
    let knn = |q: &[f32], k: usize| -> Vec<u64> {
        let gen = graphs.get(&graph).unwrap();
        let snap = DeltaSnapshot::from_memtable(graphs.writer(&graph).unwrap().snapshot());
        let view = MergedView::new(gen.as_ref(), snap);
        let parts: Vec<String> = q.iter().map(|x| format!("{x:?}")).collect();
        let ast = parser::parse(&format!(
            "CALL db.idx.vector.queryNodes('Doc', 'embedding', {k}, vecf32([{}])) \
                 YIELD node, score RETURN id(node) AS id",
            parts.join(", ")
        ))
        .unwrap();
        let res = Engine::new(&view, &cache)
            .with_vector_cache(&vc, 64)
            .with_temp_beam_width(128)
            .run(&ast)
            .unwrap();
        res.rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(i) => i as u64,
                ref o => panic!("unexpected KNN row {o:?}"),
            })
            .collect()
    };
    let cosine = |a: &[f32], b: &[f32]| -> f32 {
        let (mut d, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
        for (x, y) in a.iter().zip(b) {
            d += *x as f64 * *y as f64;
            na += *x as f64 * *x as f64;
            nb += *y as f64 * *y as f64;
        }
        (1.0 - d / (na.sqrt() * nb.sqrt())) as f32
    };
    let brute = |q: &[f32], k: usize| -> Vec<u64> {
        let mut s: Vec<(f32, u64)> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (cosine(q, v), i as u64))
            .collect();
        s.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        s.into_iter().take(k).map(|(_, i)| i).collect()
    };
    let k = 10;
    // NB: no KNN query yet — a query would build the base brute matrix and page `.vamana`
    // blocks into the pool, and `bytes()` counts those too. The pinned-set leak check below
    // measures `bytes()` at points where only the pinned segment PQ is resident.

    // A second sealing flush ⇒ two segments pinned.
    reembed_all(&graphs);
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("second sealing flush");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 2);
    let bytes_2seg = vc.bytes();
    assert!(
        bytes_2seg > bytes_1seg,
        "two sealed segments pin more than one ({bytes_2seg} vs {bytes_1seg})"
    );

    // Merge the two into one. The retired inputs' pinned PQ must be freed — `bytes()` must
    // NOT grow (mutation-check: drop `unpin_retired_segment_pqs` and `bytes()` would hold
    // all three segments' PQ, i.e. exceed `bytes_2seg`).
    graphs
        .compact_graph_segments(&graph, &vc, &root, 0, 2)
        .unwrap();
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 1);
    let bytes_merged = vc.bytes();
    assert!(
        bytes_merged <= bytes_1seg + (bytes_1seg / 4),
        "after a 2→1 merge the pinned set must be ~one segment ({bytes_merged}), not the \
             two retired segments plus the merged one — the retired PQ leaked (bytes_1seg \
             {bytes_1seg}, bytes_2seg {bytes_2seg})"
    );
    // Recall of the merged sealed beam vs an exact brute force over the live set. The base
    // index is brute-force, but every id is superseded by the segment, so the answer comes
    // from the sealed segment beam. Truth is independently derived (brute here), never a
    // second implementation.
    let mut total = 0.0f64;
    let qn = 10;
    for qi in 0..qn {
        let q = &vecs[(qi * 197) % n];
        let got: std::collections::HashSet<u64> = knn(q, k).into_iter().collect();
        let want = brute(q, k);
        total += want.iter().filter(|id| got.contains(id)).count() as f64 / k as f64;
    }
    let recall = total / qn as f64;
    assert!(
        recall >= 0.9,
        "merged sealed segment beam recall@{k} was {recall:.3} (vs exact brute over the live \
             set)"
    );

    // Delete the merged segment's sealed files: the opener must fall back to `None` ⇒ an
    // exact brute force over the sidecar ids. Reopen the graph so the deletion takes effect.
    let seg_uuid = graphs.get(&graph).unwrap().stack().segments()[0]
        .manifest
        .segment_uuid;
    let seg_dir = root
        .join(&graph)
        .join("segments")
        .join(seg_uuid.0.to_string());
    let mut deleted = 0;
    for e in std::fs::read_dir(&seg_dir).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_str().unwrap().to_string();
        if name.ends_with(".vamana") || name.ends_with(".pq") {
            std::fs::remove_file(&p).unwrap();
            deleted += 1;
        }
    }
    assert!(deleted >= 2, "expected the sealed .vamana + .pq to delete");
    let vc2 = VectorIndexCache::new(64 << 20);
    let mut graphs2 = Graphs::open_all(&root, None).unwrap();
    graphs2
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let knn2 = |q: &[f32], k: usize| -> Vec<u64> {
        let gen = graphs2.get(&graph).unwrap();
        let snap = DeltaSnapshot::from_memtable(graphs2.writer(&graph).unwrap().snapshot());
        let view = MergedView::new(gen.as_ref(), snap);
        let parts: Vec<String> = q.iter().map(|x| format!("{x:?}")).collect();
        let ast = parser::parse(&format!(
            "CALL db.idx.vector.queryNodes('Doc', 'embedding', {k}, vecf32([{}])) \
                 YIELD node, score RETURN id(node) AS id",
            parts.join(", ")
        ))
        .unwrap();
        let res = Engine::new(&view, &cache)
            .with_vector_cache(&vc2, 64)
            .run(&ast)
            .unwrap();
        res.rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(i) => i as u64,
                ref o => panic!("unexpected KNN row {o:?}"),
            })
            .collect()
    };
    // The brute fallback is exact: it must recover the brute-force top-k exactly.
    let got: Vec<u64> = knn2(&vecs[0], k);
    let want = brute(&vecs[0], k);
    assert_eq!(
        got, want,
        "a segment whose sealed files were deleted must fall back to an EXACT brute force"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A written embedding survives a T2 flush, and a **removed** one stays removed.
///
/// The removal is the sharp half, and it needs its own channel on disk. An indexed
/// embedding is routed out of the column store (D12), so a node's property record never
/// held one — which makes a flushed row that lacks an embedding ambiguous: `REMOVE
/// n.embedding` and an unrelated `SET n.age = 99` produce byte-identical rows, and both
/// read back as `Null`. Value absence cannot express a removal. Without the segment's
/// `vec.meta` sidecar (and the delta's `NodeDelta` before a flush), the node's stale base
/// vector goes on scoring forever and `REMOVE n.embedding` silently does nothing to KNN.
#[test]
fn a_flush_carries_a_written_vector_and_a_removed_one_stays_removed() {
    let (root, graph, _) = testgen::write_basic("vec_flush_removal");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let write = |graphs: &Graphs, q: &str| {
        let gen = graphs.get(&graph).unwrap();
        let writer = graphs.writer(&graph).unwrap();
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
            _ => panic!("expected a write: {q}"),
        }
    };
    let knn = |graphs: &Graphs, q: &str| -> Vec<i64> {
        let gen = graphs.get(&graph).unwrap();
        let snap = DeltaSnapshot::from_memtable(graphs.writer(&graph).unwrap().snapshot());
        let view = MergedView::new(gen.as_ref(), snap);
        let ast = parser::parse(&format!(
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 5, vecf32({q})) \
                 YIELD node, score RETURN id(node) AS id"
        ))
        .unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        res.rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(i) => i,
                ref o => panic!("unexpected KNN row {o:?}"),
            })
            .collect()
    };

    // Alice (0) starts in the base index at [0.1, 0.2, 0.3].
    assert_eq!(
        knn(&graphs, "[0.1, 0.2, 0.3]")[0],
        0,
        "Alice leads on her own vector"
    );

    // Re-embed her, then flush the delta into a core segment. The embedding rides the
    // node row into the segment — `Value::Vector` is a first-class wire type — so it is
    // still exactly ranked with the delta now empty.
    write(
        &graphs,
        "MATCH (n:Person {name:'Alice'}) SET n.embedding = vecf32([0.0, 0.0, 1.0])",
    );
    assert_eq!(
        knn(&graphs, "[0.0, 0.0, 1.0]")[0],
        0,
        "visible from the delta"
    );
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes");
    assert_eq!(
        graphs.get(&graph).unwrap().stack().segments().len(),
        1,
        "the write is now in a segment, not the delta"
    );
    assert_eq!(
        knn(&graphs, "[0.0, 0.0, 1.0]")[0],
        0,
        "the vector must survive the flush and still lead"
    );

    // Now remove it. She must leave the index entirely — including for a query aimed at
    // the vector the *base* still holds for her, which is what would resurface.
    write(
        &graphs,
        "MATCH (n:Person {name:'Alice'}) REMOVE n.embedding",
    );
    let after = knn(&graphs, "[0.1, 0.2, 0.3]");
    assert!(
        !after.contains(&0),
        "a removed embedding must take the node out of the index — the stale base vector \
             must not resurface; got {after:?}"
    );
    assert_eq!(
        after.len(),
        2,
        "the other two Person embeddings remain: {after:?}"
    );

    // The removal must also survive its own flush (the sidecar, not the row, carries it).
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the removal flushes");
    let after_flush = knn(&graphs, "[0.1, 0.2, 0.3]");
    assert!(
        !after_flush.contains(&0),
        "the removal must survive being flushed into a segment; got {after_flush:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}
