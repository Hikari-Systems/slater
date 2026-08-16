// SPDX-License-Identifier: Apache-2.0
//! `flush_segment` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// Phase 4 slice 4.4-c: a flush over a **stacked L0** (the active memtable plus ≥2 sealed
/// L0 levels) folds every level newest-wins into ONE segment. A core node patched in all
/// three levels resolves to the newest value; born nodes allocated in different levels tile
/// contiguously above the shared base; a born edge whose endpoints span levels traverses.
/// All read back through an empty delta and survive a reopen.
#[test]
fn flush_to_segment_folds_a_stacked_l0() {
    let (root, _g) = testgen::write_indexed_people("flush_seg_stacked_l0");
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

    // Level L0-oldest: patch a core node only (0 born).
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
    assert!(graphs.writer("people").unwrap().flush_to_l0().unwrap());

    // Level L0-newer: re-patch the same core node (newer wins over 99), born Dave, and a
    // born edge Alice-KNOWS->Dave (a core endpoint + a same-level born endpoint).
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 77");
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(
        &graphs,
        "MERGE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Dave'})",
    );
    assert!(graphs.writer("people").unwrap().flush_to_l0().unwrap());
    assert_eq!(
        graphs.writer("people").unwrap().l0_len(),
        2,
        "two L0 levels"
    );

    // Active memtable (newest): re-patch Alice again (55 wins over 77 and 99), born Eve.
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 55");
    write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
    assert!(
        !graphs.writer("people").unwrap().snapshot().is_empty(),
        "active memtable carries the newest level"
    );

    // Flush: folds [active ⊕ L0-newer ⊕ L0-oldest] into one segment.
    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a stacked delta flushes");

    let gen1 = graphs.get("people").unwrap();
    assert_eq!(gen1.uuid(), set_uuid, "identity is the new set uuid");
    assert_eq!(gen1.base_uuid(), base_uuid, "base preserved by the flush");
    assert_eq!(gen1.stack().segments().len(), 1, "one folded upper segment");
    let writer = graphs.writer("people").unwrap();
    assert!(writer.snapshot().is_empty(), "delta retired empty");
    assert_eq!(writer.l0_len(), 0, "L0 levels consumed by the flush");

    let q = |graphs: &Graphs, q: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let ast = parser::parse(q).unwrap();
        let r = Engine::new(&view, &cache).run(&ast).unwrap();
        r
    };

    // Newest-wins across three levels: Alice's age is 55 (active), not 77 or 99.
    let alice = q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.age");
    assert!(
        matches!(alice.rows[0][0], Val::Int(55)),
        "Alice's newest patch wins across the stack: {:?}",
        alice.rows[0][0]
    );
    // Born nodes from different levels both land (Dave from L0-newer, Eve from active).
    let dave = q(&graphs, "MATCH (n:Person {name:'Dave'}) RETURN n.age");
    assert!(
        matches!(dave.rows[0][0], Val::Int(50)),
        "Dave (born in a sealed L0) is in the segment: {:?}",
        dave.rows[0][0]
    );
    let eve = q(&graphs, "MATCH (n:Person {name:'Eve'}) RETURN n.age");
    assert!(
        matches!(eve.rows[0][0], Val::Int(60)),
        "Eve (born in the active level) is in the segment: {:?}",
        eve.rows[0][0]
    );
    // Count: 3 base + 2 born = 5.
    let n = q(&graphs, "MATCH (n:Person) RETURN count(*)");
    assert!(
        matches!(n.rows[0][0], Val::Int(5)),
        "3 base + 2 born folded: {:?}",
        n.rows[0][0]
    );
    // The born edge (endpoints resolved across levels) traverses.
    let knows = q(
        &graphs,
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name ORDER BY b.name",
    );
    // Alice already KNOWS Bob in the base; the folded born edge adds Dave.
    let targets: Vec<String> = knows
        .rows
        .iter()
        .filter_map(|r| match &r[0] {
            Val::Str(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert!(
        targets.contains(&"Dave".to_string()),
        "the folded born edge Alice->Dave traverses: {targets:?}"
    );

    // Reopen from disk: the folded segment reloads and the merged data survives.
    drop(writer);
    drop(gen1);
    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    let gen2 = graphs.get("people").unwrap();
    assert_eq!(gen2.uuid(), set_uuid, "reopen names the flushed set");
    assert_eq!(gen2.stack().segments().len(), 1, "folded segment reloaded");
    let view = MergedView::new(gen2.as_ref(), DeltaSnapshot::empty());
    let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap();
    let alice2 = Engine::new(&view, &cache).run(&ast).unwrap();
    assert!(
        matches!(alice2.rows[0][0], Val::Int(55)),
        "newest-wins fold survives reopen: {:?}",
        alice2.rows[0][0]
    );

    std::fs::remove_dir_all(&root).ok();
}

/// A flush over an **off-heap** L0 stack (the previously-deferred case). With `offHeapL0`
/// every `flush_to_l0` seals a *block image* rather than a resident memtable, so the T2
/// flush folds it at the `SegmentData` level (`flush_segment_data`) instead of rebuilding a
/// memtable. Exercises every fold kind — a core-node patch re-applied across levels
/// (newest-wins), born nodes from different levels, a born edge, a **core-edge property
/// patch** (the v4 `core_patched_edges` that off-heap now persists), and a core-node delete —
/// all read back through an empty delta and survive a from-disk reopen.
#[test]
fn flush_to_segment_folds_an_off_heap_l0_stack() {
    let (root, _g) = testgen::write_indexed_people("flush_seg_offheap_l0");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    // Off-heap L0 needs a resident block cache to page its sealed levels.
    let wcache = Arc::new(graph_format::blockcache::BlockCache::new(1 << 20));

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg_offheap(&wal), &root, Some(wcache))
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

    // L0-oldest (off-heap): patch a core node, born Dave, a born edge, and a core-edge patch
    // on the base Alice-KNOWS->Bob edge — the endpoints off-heap must now persist (v4).
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(
        &graphs,
        "MERGE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Dave'})",
    );
    write(
        &graphs,
        "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) SET r.since = 2099",
    );
    assert!(graphs.writer("people").unwrap().flush_to_l0().unwrap());

    // Active memtable (newest): re-patch Alice (55 wins over 99), born Eve, delete Carol.
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 55");
    write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
    write(&graphs, "MATCH (n:Person {name:'Carol'}) DETACH DELETE n");
    assert_eq!(
        graphs.writer("people").unwrap().l0_len(),
        1,
        "one off-heap L0 level"
    );

    // The flush folds [active ⊕ off-heap L0] into one segment — no longer a bail.
    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("an off-heap-stacked delta flushes");
    let gen1 = graphs.get("people").unwrap();
    assert_eq!(gen1.base_uuid(), base_uuid, "base preserved");
    assert_eq!(gen1.stack().segments().len(), 1, "one folded upper segment");
    let writer = graphs.writer("people").unwrap();
    assert!(writer.snapshot().is_empty(), "delta retired empty");
    assert_eq!(writer.l0_len(), 0, "the off-heap L0 level was consumed");

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
    let check = |graphs: &Graphs, tag: &str| {
        // Newest-wins core patch (55 over 99).
        assert!(
            matches!(
                q(graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.age").rows[0][0],
                Val::Int(55)
            ),
            "{tag}: Alice's newest patch wins"
        );
        // Born nodes from both levels.
        assert!(
            matches!(
                q(graphs, "MATCH (n:Person {name:'Dave'}) RETURN n.age").rows[0][0],
                Val::Int(50)
            ),
            "{tag}: Dave (off-heap L0 born) present"
        );
        assert!(
            matches!(
                q(graphs, "MATCH (n:Person {name:'Eve'}) RETURN n.age").rows[0][0],
                Val::Int(60)
            ),
            "{tag}: Eve (active born) present"
        );
        // Carol deleted; 3 base − 1 + 2 born = 4.
        assert_eq!(
            q(graphs, "MATCH (n:Person {name:'Carol'}) RETURN n")
                .rows
                .len(),
            0,
            "{tag}: Carol deleted through the off-heap fold"
        );
        assert!(
            matches!(
                q(graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
                Val::Int(4)
            ),
            "{tag}: 3 base − Carol + Dave + Eve = 4"
        );
        // The core-edge patch (endpoints recovered from the persisted v4 field).
        assert!(
                matches!(
                    q(graphs, "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) RETURN r.since").rows[0][0],
                    Val::Int(2099)
                ),
                "{tag}: the off-heap core-edge patch folded into the segment"
            );
        // The born edge traverses.
        let targets: Vec<String> = q(
            graphs,
            "MATCH (:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name",
        )
        .rows
        .iter()
        .filter_map(|r| match &r[0] {
            Val::Str(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
        assert!(
            targets.contains(&"Dave".to_string()),
            "{tag}: born edge Alice->Dave traverses: {targets:?}"
        );
    };
    check(&graphs, "post-flush");

    // Reopen from disk (no writable layer): the folded segment serves everything.
    drop(writer);
    drop(gen1);
    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(
        graphs.get("people").unwrap().uuid(),
        set_uuid,
        "reopen names the flushed set"
    );
    check(&graphs, "post-reopen");

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 4 slice 4.4-d: a flush against a **non-filesystem** store uploads the segment,
/// set manifest and `current` pointer through the `ObjectStore` abstraction (the segment
/// is staged locally, then published to the store). A fresh open that reads *only* through
/// the in-memory store — no local filesystem — serves the flushed born node, proving the
/// upload round-trips store-natively.
#[test]
fn flush_to_segment_uploads_to_an_object_store() {
    use graph_format::store::mem::MemObjectStore;
    use graph_format::store::ObjectStore as _;

    // Build the base generation locally, then seed a mem store from it — the mem store is
    // the served backend; the local dir is only the WAL + segment staging area.
    let (root, _g) = testgen::write_indexed_people("flush_seg_memstore");
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
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");

    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes");

    // The store now holds the set, an updated `current`, and the segment's SEGMENT.json.
    assert_eq!(
        String::from_utf8(mem.read_all("people/current").unwrap())
            .unwrap()
            .trim(),
        set_uuid.0.to_string(),
        "remote current names the flushed set"
    );
    assert!(
        mem.exists(&graph_format::setmanifest::SetManifest::key(
            "people", set_uuid
        ))
        .unwrap(),
        "the set manifest was uploaded"
    );
    let seg_json_keys: Vec<String> = mem
        .list("people/segments")
        .unwrap()
        .iter()
        .map(|u| format!("people/segments/{u}/SEGMENT.json"))
        .collect();
    assert_eq!(seg_json_keys.len(), 1, "one segment dir uploaded");
    assert!(
        mem.exists(&seg_json_keys[0]).unwrap(),
        "SEGMENT.json uploaded to the store"
    );

    // Reopen reading ONLY through the mem store (no local fs): the flushed data is served.
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
    assert_eq!(gen.uuid(), set_uuid, "store reopen names the flushed set");
    assert_eq!(gen.base_uuid(), base_uuid, "base preserved");
    assert_eq!(gen.stack().segments().len(), 1, "segment loaded from store");
    let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
    let ast = parser::parse("MATCH (n:Person {name:'Dave'}) RETURN n.age").unwrap();
    let dave = Engine::new(&view, &cache).run(&ast).unwrap();
    assert!(
        matches!(dave.rows[0][0], Val::Int(50)),
        "born Dave served from the store-native segment: {:?}",
        dave.rows.first()
    );
    let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap();
    let alice = Engine::new(&view, &cache).run(&ast).unwrap();
    assert!(
        matches!(alice.rows[0][0], Val::Int(99)),
        "Alice's flushed patch served from the store: {:?}",
        alice.rows.first()
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 7 slice 7.4: GC reclaims a **remote** store's orphaned objects, not only local
/// staged dirs. Over a `MemObjectStore` (`is_local_fs == false`), a stale set's manifest and
/// a compacted run's segment objects are removed from the store via `ObjectStore::delete`; a
/// store-native reopen then serves only the live merged segment.
#[test]
fn gc_reclaims_orphans_from_an_object_store() {
    use graph_format::store::mem::MemObjectStore;
    use graph_format::store::ObjectStore as _;

    let (root, _g) = testgen::write_indexed_people("gc_memstore_74");
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
    // Number of segment "dirs" and set manifest objects the store currently holds.
    let store_segments =
        |mem: &MemObjectStore| -> usize { mem.list("people/segments").unwrap().len() };
    let store_sets = |mem: &MemObjectStore| -> usize {
        mem.list("people/sets")
            .unwrap()
            .into_iter()
            .filter(|n| n.ends_with(".json"))
            .count()
    };
    let set_key = |u: GenId| graph_format::setmanifest::SetManifest::key("people", u);

    // Two flushes upload two segments; set1 is now stale, set2 current.
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    let set1 = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .unwrap();
    assert_eq!(store_segments(&mem), 2, "two segments uploaded");
    assert_eq!(
        store_sets(&mem),
        2,
        "set1 (stale) + set2 (current) uploaded"
    );
    assert!(mem.exists(&set_key(set1)).unwrap(), "set1 object present");

    // GC reclaims the stale set1 manifest FROM THE STORE (not just a local file).
    let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
    assert_eq!(rep.deleted_sets.len(), 1);
    assert!(
        !mem.exists(&set_key(set1)).unwrap(),
        "the stale set object was deleted from the store"
    );
    assert_eq!(store_sets(&mem), 1, "only the current set object remains");
    assert_eq!(store_segments(&mem), 2, "both segments still live");

    // Compact the two segments into one → the run's two segments orphan in the store.
    graphs
        .compact_graph_segments("people", &vc, &root, 0, 2)
        .unwrap();
    assert_eq!(
        store_segments(&mem),
        3,
        "2 compacted + 1 merged in the store pre-GC"
    );

    let rep = graphs.gc_orphan_segments("people", &root, 0).unwrap();
    assert_eq!(
        rep.deleted_segments.len(),
        2,
        "the run's segment objects reclaimed from the store"
    );
    assert_eq!(
        rep.deleted_sets.len(),
        1,
        "the superseded set object reclaimed"
    );
    assert_eq!(
        store_segments(&mem),
        1,
        "only the merged segment remains in the store"
    );
    assert_eq!(store_sets(&mem), 1);

    // The merged segment's objects are intact — a store-native reopen serves every row.
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
    assert_eq!(
        gen.stack().segments().len(),
        1,
        "merged segment loads from the store"
    );
    let view = MergedView::new(gen.as_ref(), DeltaSnapshot::empty());
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
    for n in ["Alice", "Bob", "Carol", "Dave", "Eve"] {
        assert!(names.contains(n), "{n} served after store GC: {names:?}");
    }

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 4 slice 4.2: a delta of **core-resolved node patches** (a `SET`/`REMOVE` on a
/// node the base already carries) flushes into an upper segment as full replace-rows.
/// Every kind is exercised end-to-end through the query overlay with an empty delta:
/// a moved indexed value (base index entry superseded via the removal sidecar + the new
/// value re-added), a removed indexed value, a fresh non-indexed property (base props
/// preserved in the full row), an added label, and a mixed-in born node — all surviving
/// a reopen.
#[test]
fn flush_to_segment_materialises_core_node_patches() {
    // `write_basic` gives Alice/Bob/Carol :Person (name+age indexed, ages 30/25/40) and
    // Acme/Globex :Company, with both labels defined so a label-add is accepted.
    let (root, _g, _u) = testgen::write_basic("flush_seg_patch_e2e");
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
    // Alice(30) → 99 and gains the pre-existing :Company label; Bob gains a fresh
    // non-indexed city; Carol loses her indexed age; Zoe is a mixed-in birth.
    write(
        &graphs,
        "MATCH (n:Person {name:'Alice'}) SET n.age = 99, n:Company",
    );
    write(
        &graphs,
        "MATCH (n:Person {name:'Bob'}) SET n.city = 'Berlin'",
    );
    write(&graphs, "MATCH (n:Person {name:'Carol'}) REMOVE n.age");
    write(&graphs, "MERGE (n:Person {name:'Zoe'}) SET n.age = 7");

    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes");

    let gen1 = graphs.get("people").unwrap();
    assert_eq!(gen1.stack().segments().len(), 1, "one upper segment");
    assert!(
        graphs.writer("people").unwrap().snapshot().is_empty(),
        "delta retired empty"
    );

    // Query the flushed set with an empty delta — everything is served by the segment.
    let q = |graphs: &Graphs, q: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let r = Engine::new(&view, &cache)
            .run(&parser::parse(q).unwrap())
            .unwrap();
        r
    };
    let names = |r: &QueryResult| -> Vec<String> {
        let mut ns: Vec<String> = r
            .rows
            .iter()
            .map(|row| match &row[0] {
                Val::Str(s) => s.clone(),
                v => panic!("expected a name string, got {v:?}"),
            })
            .collect();
        ns.sort();
        ns
    };

    // Moved indexed value: the old value is gone (removal sidecar suppressed the base
    // hit), the new value finds Alice, an untouched value still finds Bob.
    assert!(
        q(&graphs, "MATCH (n:Person) WHERE n.age = 30 RETURN n.name")
            .rows
            .is_empty(),
        "Alice's old indexed age (30) is superseded"
    );
    assert_eq!(
        names(&q(
            &graphs,
            "MATCH (n:Person) WHERE n.age = 99 RETURN n.name"
        )),
        vec!["Alice"],
        "the moved indexed value finds Alice at 99"
    );
    assert_eq!(
        names(&q(
            &graphs,
            "MATCH (n:Person) WHERE n.age = 25 RETURN n.name"
        )),
        vec!["Bob"],
        "an untouched base index entry still stands"
    );
    // Removed indexed value: Carol's age index entry is gone, and her property reads Null
    // while her preserved base name survives in the full row.
    assert!(
        q(&graphs, "MATCH (n:Person) WHERE n.age = 40 RETURN n.name")
            .rows
            .is_empty(),
        "Carol's removed indexed age is superseded with no replacement"
    );
    let carol = q(&graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age");
    assert!(
        matches!(carol.rows[0][0], Val::Null),
        "Carol's age is removed: {:?}",
        carol.rows[0][0]
    );
    // Fresh non-indexed property with base props preserved.
    let bob = q(
        &graphs,
        "MATCH (n:Person {name:'Bob'}) RETURN n.city, n.age",
    );
    assert!(
        matches!(&bob.rows[0][0], Val::Str(s) if s == "Berlin"),
        "Bob's new city: {:?}",
        bob.rows[0][0]
    );
    assert!(
        matches!(bob.rows[0][1], Val::Int(25)),
        "Bob's base age preserved in the full row: {:?}",
        bob.rows[0][1]
    );
    // Added label surfaces in a label scan (Alice joins the base Companies); she is still
    // a Person too (the base label is preserved in the full row).
    assert_eq!(
        names(&q(&graphs, "MATCH (n:Company) RETURN n.name")),
        vec!["Acme", "Alice", "Globex"],
        "the added :Company label is served by the segment beside the base companies"
    );
    assert_eq!(
        names(&q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.name")),
        vec!["Alice"],
        "Alice keeps her base :Person label"
    );
    assert!(
        matches!(
            q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
            Val::Int(4)
        ),
        "3 base Persons + born Zoe; patches do not change the node count"
    );
    // The mixed-in born node reads back through its index entry.
    assert_eq!(
        names(&q(
            &graphs,
            "MATCH (n:Person) WHERE n.age = 7 RETURN n.name"
        )),
        vec!["Zoe"],
        "the born node is found by its index entry"
    );

    // Reopen from disk: the patch full-rows and removal sidecars reload.
    drop(gen1);
    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    let gen2 = graphs.get("people").unwrap();
    assert_eq!(gen2.uuid(), set_uuid, "reopen names the flushed set");
    let view = MergedView::new(gen2.as_ref(), DeltaSnapshot::empty());
    let alice = Engine::new(&view, &cache)
        .run(&parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap())
        .unwrap();
    assert!(
        matches!(alice.rows[0][0], Val::Int(99)),
        "Alice's patched age reloaded from the segment: {:?}",
        alice.rows[0][0]
    );
    assert!(
        Engine::new(&view, &cache)
            .run(&parser::parse("MATCH (n:Person) WHERE n.age = 30 RETURN n.name").unwrap())
            .unwrap()
            .rows
            .is_empty(),
        "the removal sidecar survives the reopen"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 4 slice 4.2, cross-layer removal obligation: a second flush that re-patches a
/// node already carried by the *first* flush's segment must supersede the value that
/// lives in the **lower segment** (not just the base). The writer reads the base-below
/// row through the stack, so it lists the lower segment's id in its removal sidecar, and
/// the oldest→newest `fold_index_eq` yields newest-wins across two stacked segments.
#[test]
fn flush_to_segment_supersedes_a_lower_segment_value() {
    let (root, _g, _u) = testgen::write_basic("flush_seg_restack_e2e");
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
        let parser::ast::Statement::Write(w) = parser::parse_statement(qy).unwrap() else {
            panic!("expected a write: {qy}");
        };
        execute_write(
            &writer,
            gen.as_ref(),
            &w,
            &HashMap::new(),
            TEST_BOLT_VERSION,
        )
        .unwrap();
    };
    let q = |graphs: &Graphs, qy: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        // A reopened graph (post-drop) has no writable layer; fall back to an empty delta.
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

    // First flush: Alice 30 → 99 lands in segment #1.
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("first flush");
    // Second flush: Alice 99 → 7. The base-below value (99) lives in segment #1.
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 7");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("second flush");

    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        2,
        "two stacked segments"
    );
    // Newest value wins; both older values (the base's 30 and segment #1's 99) are gone.
    assert_eq!(
        q(&graphs, "MATCH (n:Person) WHERE n.age = 7 RETURN n.name")
            .rows
            .len(),
        1,
        "the newest flush's value wins across two segments"
    );
    assert!(
        q(&graphs, "MATCH (n:Person) WHERE n.age = 99 RETURN n.name")
            .rows
            .is_empty(),
        "segment #1's superseded value is dropped by segment #2's removal"
    );
    assert!(
        q(&graphs, "MATCH (n:Person) WHERE n.age = 30 RETURN n.name")
            .rows
            .is_empty(),
        "the original base value stays superseded"
    );
    let alice = q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.age");
    assert!(
        matches!(alice.rows[0][0], Val::Int(7)),
        "Alice's twice-patched age: {:?}",
        alice.rows[0][0]
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 4 slice 4.3: a **node delete** flushes into an upper segment as a full-row
/// tombstone plus incident-edge removal fragments. `DETACH DELETE` of Bob (the target of
/// the base's one Alice-KNOWS->Bob edge) must, once flushed with an empty delta: drop Bob
/// from an index seek and the label count (its base-indexed values superseded via the
/// `removals` sidecar, the node/label marginals netted down), and drop the incident edge
/// from Alice's outgoing traversal and the reltype count (a `removed` adjacency fragment
/// on Alice's surviving side, the edge marginal netted down) — all surviving a reopen.
#[test]
fn flush_to_segment_materialises_a_node_delete() {
    let (root, _g) = testgen::write_indexed_people("flush_seg_del_node_e2e");
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
        // A reopened graph (post-drop) has no writable layer; fall back to an empty delta.
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

    // DETACH DELETE Bob (dst of the Alice-KNOWS->Bob base edge), then flush.
    write(&graphs, "MATCH (n:Person {name:'Bob'}) DETACH DELETE n");
    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a delete flushes a non-empty delta");

    let gen1 = graphs.get("people").unwrap();
    assert_eq!(gen1.base_uuid(), base_uuid, "base preserved by the flush");
    assert_eq!(gen1.stack().segments().len(), 1, "one upper segment");
    assert!(
        graphs.writer("people").unwrap().snapshot().is_empty(),
        "delta retired"
    );

    // Bob is gone from the index seek, the label count, and Alice's traversal — read
    // through the (now empty) delta, so the segment alone must answer.
    assert!(
        q(&graphs, "MATCH (n:Person {name:'Bob'}) RETURN n.name")
            .rows
            .is_empty(),
        "deleted Bob is superseded in the name index"
    );
    assert!(
        matches!(
            q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
            Val::Int(2)
        ),
        "2 survivors (Alice, Carol) after the delete"
    );
    assert!(
        q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"
        )
        .rows
        .is_empty(),
        "the incident edge is removed on Alice's surviving side"
    );
    assert!(
        matches!(
            q(&graphs, "MATCH ()-[:KNOWS]->() RETURN count(*)").rows[0][0],
            Val::Int(0)
        ),
        "the reltype edge count nets the removed edge to zero"
    );
    // Alice and Carol still read normally.
    assert_eq!(
        q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.name")
            .rows
            .len(),
        1,
        "Alice untouched by Bob's delete"
    );

    // Reopen from disk: the tombstone + removals reload and still hide Bob and his edge.
    drop(gen1);
    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(
        graphs.get("people").unwrap().uuid(),
        set_uuid,
        "reopen names the set"
    );
    assert!(
        q(&graphs, "MATCH (n:Person {name:'Bob'}) RETURN n.name")
            .rows
            .is_empty(),
        "Bob stays deleted across a reopen"
    );
    assert!(
        matches!(
            q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
            Val::Int(2)
        ),
        "survivor count stable across a reopen"
    );
    assert!(
        q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"
        )
        .rows
        .is_empty(),
        "the removed edge stays gone across a reopen"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 4 slice 4.3: an explicit **edge delete** (`DELETE r` on a core edge, both
/// endpoints surviving) flushes into an upper segment as a pure adjacency removal on
/// *both* endpoints' sides (no node tombstone, no edge row) with the edge/reltype
/// marginals netted down. The edge stops traversing from either direction while both
/// nodes remain, surviving a reopen.
#[test]
fn flush_to_segment_materialises_an_edge_delete() {
    let (root, _g) = testgen::write_indexed_people("flush_seg_del_edge_e2e");
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
    let q = |graphs: &Graphs, qy: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        // A reopened graph (post-drop) has no writable layer; fall back to an empty delta.
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

    write(
        &graphs,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
    );
    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("an edge delete flushes a non-empty delta");

    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        1,
        "one upper segment"
    );
    // Both nodes remain; only the edge is gone, from both traversal directions.
    assert!(
        matches!(
            q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
            Val::Int(3)
        ),
        "an edge delete leaves every node"
    );
    assert!(
        q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"
        )
        .rows
        .is_empty(),
        "removed on Alice's outgoing side"
    );
    assert!(
        q(
            &graphs,
            "MATCH (a)-[:KNOWS]->(b:Person {name:'Bob'}) RETURN a.name"
        )
        .rows
        .is_empty(),
        "removed on Bob's incoming side"
    );
    assert!(
        matches!(
            q(&graphs, "MATCH ()-[:KNOWS]->() RETURN count(*)").rows[0][0],
            Val::Int(0)
        ),
        "the reltype edge count nets to zero"
    );

    // Reopen: the removal fragments reload and the edge stays gone.
    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(
        graphs.get("people").unwrap().uuid(),
        set_uuid,
        "reopen names the set"
    );
    assert!(
        q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"
        )
        .rows
        .is_empty(),
        "the removed edge stays gone across a reopen"
    );
    assert!(
        matches!(
            q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
            Val::Int(3)
        ),
        "node count stable across a reopen"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// HIK-100 sub-item 6: deleting several core edges off one hub in a single delta must
/// resolve that hub's effective adjacency (base CSR + every lower segment) **once**, not
/// once per deleted edge — the O(D²)→O(D) memoisation. A first flush turns a fan of born
/// edges into a lower segment (making them *core*); a second delta deletes several of them
/// and flushes. The thread-local `EFFECTIVE_ADJ_CALLS` counter (the flush runs inline on
/// this thread) proves the bound, and the survivor set proves the fold is unchanged.
#[test]
fn effective_adj_memoised_per_hub_on_multi_edge_delete() {
    let (root, _g) = testgen::write_indexed_people("flush_seg_hub_effadj");
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

    // Build a hub H0 with a fan of five KNOWS edges to fresh leaves, then flush so the fan
    // lands in a lower core segment.
    write(&graphs, "MERGE (h:Person {name:'H0'}) SET h.age = 40");
    for i in 1..=5 {
        write(
            &graphs,
            &format!("MERGE (l:Person {{name:'L{i}'}}) SET l.age = {i}"),
        );
        write(
            &graphs,
            &format!("MERGE (h:Person {{name:'H0'}})-[:KNOWS]->(l:Person {{name:'L{i}'}})"),
        );
    }
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("the fan flushes into a segment");

    // Delete three of H0's now-core KNOWS edges — three explicit core-edge deletes, all at
    // the same hub source, in one delta.
    for i in 1..=3 {
        write(
            &graphs,
            &format!(
                "MATCH (h:Person {{name:'H0'}})-[r:KNOWS]->(l:Person {{name:'L{i}'}}) DELETE r"
            ),
        );
    }

    // Flush inline on this thread; count effective_adj calls across this flush only.
    crate::flush_segment::EFFECTIVE_ADJ_CALLS.with(|c| c.set(0));
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("the edge deletes flush a non-empty delta");
    let calls = crate::flush_segment::EFFECTIVE_ADJ_CALLS.with(|c| c.get());
    assert_eq!(
        calls, 1,
        "effective_adj resolved once for the hub, not once per deleted edge (got {calls})"
    );

    // The fold is unchanged: exactly the two undeleted edges remain, to L4 and L5.
    let mut names: Vec<String> = q(
        &graphs,
        "MATCH (h:Person {name:'H0'})-[:KNOWS]->(l) RETURN l.name",
    )
    .rows
    .iter()
    .map(|r| match &r[0] {
        Val::Str(s) => s.clone(),
        o => panic!("expected a name, got {o:?}"),
    })
    .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["L4".to_string(), "L5".to_string()],
        "only the three deleted edges are gone"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 4 slice 4.4-a: a **core-edge patch** (`SET r.p = v` on an edge the core already
/// carries) flushes into an upper segment as a full **replace** edge row — the base props
/// overlaid by the patch — that `resolve_edge_row` serves over the base, with no marginal
/// change (topology untouched). The base fixture's one edge `Alice-KNOWS->Bob` carries
/// `since = 2020`; after patching `since → 2099` and adding a fresh `note`, an empty-delta
/// read serves both from the segment, the base `since` is gone, the endpoints/counts are
/// unchanged, and it all survives a reopen.
#[test]
fn flush_to_segment_materialises_a_core_edge_patch() {
    let (root, _g) = testgen::write_indexed_people("flush_seg_patch_edge_e2e");
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
    let q = |graphs: &Graphs, qy: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        // A reopened graph (post-drop) has no writable layer; fall back to an empty delta.
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

    // Base edge Alice-KNOWS->Bob carries since=2020; the existing-edge MERGE resolves it
    // and routes the SET to `patch_core_edge` (in-place patch, no duplicate born edge).
    write(
        &graphs,
        "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) \
             SET r.since = 2099, r.note = 'hi'",
    );
    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("an edge patch flushes a non-empty delta");

    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        1,
        "one upper segment"
    );
    assert!(
        graphs.writer("people").unwrap().snapshot().is_empty(),
        "delta retired empty"
    );

    // The overlaid prop is served from the segment; the fresh prop too; the base value gone.
    let since = q(
        &graphs,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b) RETURN r.since",
    );
    assert!(
        matches!(since.rows[0][0], Val::Int(2099)),
        "patched edge prop served from the segment: {:?}",
        since.rows[0][0]
    );
    let note = q(
        &graphs,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b) RETURN r.note",
    );
    assert!(
        matches!(&note.rows[0][0], Val::Str(s) if s == "hi"),
        "fresh edge prop served from the segment: {:?}",
        note.rows[0][0]
    );
    // Topology + counts unchanged: both endpoints remain, the edge still traverses, and the
    // node/edge marginals are untouched by a patch.
    let bob = q(
        &graphs,
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name",
    );
    assert!(
        matches!(&bob.rows[0][0], Val::Str(s) if s == "Bob"),
        "the patched edge still traverses to Bob: {:?}",
        bob.rows[0][0]
    );
    assert!(
        matches!(
            q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
            Val::Int(3)
        ),
        "an edge patch changes no node count"
    );
    assert!(
        matches!(
            q(&graphs, "MATCH ()-[:KNOWS]->() RETURN count(*)").rows[0][0],
            Val::Int(1)
        ),
        "an edge patch changes no edge count"
    );

    // Reopen from disk: the replace row reloads and still serves the patched value.
    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(
        graphs.get("people").unwrap().uuid(),
        set_uuid,
        "reopen names the set"
    );
    let since = q(
        &graphs,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b) RETURN r.since",
    );
    assert!(
        matches!(since.rows[0][0], Val::Int(2099)),
        "the patched edge prop reloaded from the segment: {:?}",
        since.rows[0][0]
    );

    std::fs::remove_dir_all(&root).ok();
}

/// A **patch-then-delete** of the same core edge in one delta: `SET r.p` then `DELETE r` on
/// the base Alice-KNOWS->Bob edge. The memtable resolves this to a pure adjacency tombstone
/// (dropping the by-id patch index), so the edge is suppressed **on read** (the live-delta
/// bug the flush writer previously refused) and the flush materialises it as an ordinary
/// core-edge delete — the edge is gone, the edge count nets down, and it stays gone across a
/// reopen. The endpoints and node count are untouched.
#[test]
fn flush_to_segment_materialises_a_patch_then_delete_of_a_core_edge() {
    let (root, _g) = testgen::write_indexed_people("flush_seg_patch_del_edge_e2e");
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
    let edge_count = |graphs: &Graphs| -> i64 {
        match q(graphs, "MATCH ()-[:KNOWS]->() RETURN count(*)").rows[0][0] {
            Val::Int(n) => n,
            ref v => panic!("count not an int: {v:?}"),
        }
    };

    // Patch the base edge, then delete it — both in one (pre-flush) delta.
    write(
        &graphs,
        "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) \
             SET r.since = 2099, r.note = 'hi'",
    );
    write(
        &graphs,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
    );

    // The live read overlay already suppresses the edge — the patch does not resurrect it.
    assert_eq!(
        edge_count(&graphs),
        0,
        "patch-then-delete is gone on read (pre-flush)"
    );
    assert_eq!(
        q(
            &graphs,
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"
        )
        .rows
        .len(),
        0,
        "the deleted edge does not traverse pre-flush"
    );

    // Flush: it materialises as a core-edge delete (adjacency removal), not an edge row.
    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("the delete flushes a non-empty delta");
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        1,
        "one upper segment"
    );
    assert!(
        graphs.writer("people").unwrap().snapshot().is_empty(),
        "delta retired empty"
    );

    // Still gone after the flush, with the edge count netted down and the nodes intact.
    assert_eq!(
        edge_count(&graphs),
        0,
        "the edge stays deleted after the flush"
    );
    assert!(
        matches!(
            q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
            Val::Int(3)
        ),
        "the endpoints survive — only the edge was deleted"
    );

    // Reopen from disk: the adjacency removal reloads and the edge is still gone.
    drop(graphs);
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(
        graphs.get("people").unwrap().uuid(),
        set_uuid,
        "reopen names the set"
    );
    assert_eq!(
        edge_count(&graphs),
        0,
        "the delete is durable across a reopen"
    );
    assert!(
        matches!(
            q(&graphs, "MATCH (n:Person) RETURN count(*)").rows[0][0],
            Val::Int(3)
        ),
        "the endpoints reload intact"
    );

    std::fs::remove_dir_all(&root).ok();
}
