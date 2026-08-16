// SPDX-License-Identifier: Apache-2.0
//! `writes` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// A write whose business key matches no existing node (or is not range-indexed)
/// is a clean execution error, and a `RETURN` after `SET` is refused for now.
#[test]
fn write_errors_are_clean() {
    let (root, _g, _) = testgen::write_basic("write_err");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();

    // No such Person: a MATCH … SET on an absent key is an error (MATCH does not
    // create — the message points at MERGE, which does).
    let absent =
        match parser::parse_statement("MATCH (n:Person {name:'Nobody'}) SET n.age = 1").unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
    let e = execute_write(
        &writer,
        gen.as_ref(),
        &absent,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap_err();
    assert!(
        e.message.contains("node to update") && e.message.contains("MERGE"),
        "got: {}",
        e.message
    );

    std::fs::remove_dir_all(&root).ok();
}

/// `RETURN` after a write projects the node the write just touched, reading through the
/// post-commit overlay — so it reports the value it wrote, not the pre-write one.
#[test]
fn a_write_returns_the_node_it_wrote() {
    let (root, _g, _) = testgen::write_basic("write_return");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();

    let write_stmt = |q: &str| match parser::parse_statement(q).unwrap() {
        parser::ast::Statement::Write(w) => w,
        other => panic!("expected a node write, got {other:?}"),
    };

    // A projected property must be the *new* value; `age` was absent before this write.
    let w = write_stmt("MATCH (n:Person {name:'Alice'}) SET n.age = 41 RETURN n.age AS age");
    let (cols, rows) = execute_write(
        &writer,
        gen.as_ref(),
        &w,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();
    assert_eq!(cols, vec!["age".to_string()]);
    assert_eq!(rows, vec![vec![PsValue::Int(41)]]);

    // Graphiti's shape: the uuid of the merged node, aliased.
    let w = write_stmt("MATCH (n:Person {name:'Alice'}) SET n.age = 42 RETURN n.name AS uuid");
    let (cols, rows) = execute_write(
        &writer,
        gen.as_ref(),
        &w,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();
    assert_eq!(cols, vec!["uuid".to_string()]);
    assert_eq!(rows, vec![vec![PsValue::str("Alice")]]);

    // A whole node encodes as a Bolt Node struct carrying the overlaid properties.
    let w = write_stmt("MATCH (n:Person {name:'Alice'}) SET n.age = 43 RETURN n");
    let (_, rows) = execute_write(
        &writer,
        gen.as_ref(),
        &w,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();
    match &rows[0][0] {
        PsValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_NODE);
            let PsValue::Map(props) = &fields[2] else {
                panic!("expected a property map, got {:?}", fields[2])
            };
            assert!(
                props
                    .iter()
                    .any(|(k, v)| k == "age" && *v == PsValue::Int(43)),
                "the projection must read through the write: {props:?}",
            );
        }
        other => panic!("expected a Node struct, got {other:?}"),
    }

    // A MERGE that *creates* has no dense id until the commit allocates one — the
    // projection has to resolve it afterwards, which is the case most likely to regress.
    let w = write_stmt("MERGE (n:Person {name:'Zoe'}) SET n.age = 7 RETURN n.age AS age");
    let (_, rows) = execute_write(
        &writer,
        gen.as_ref(),
        &w,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();
    assert_eq!(rows, vec![vec![PsValue::Int(7)]]);

    // A write with no RETURN still reports nothing.
    let w = write_stmt("MATCH (n:Person {name:'Alice'}) SET n.age = 44");
    let (cols, rows) = execute_write(
        &writer,
        gen.as_ref(),
        &w,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();
    assert!(cols.is_empty() && rows.is_empty());

    std::fs::remove_dir_all(&root).ok();
}

/// A batched write projects **one row per input row, in input order** — the property a
/// bulk loader relies on to line results up against what it sent.
#[test]
fn a_batched_write_returns_one_row_per_input_row() {
    let (root, _g, _) = testgen::write_basic("write_return_batch");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();

    let w = match parser::parse_statement(
        "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age \
         RETURN n.name AS name, n.age AS age",
    )
    .unwrap()
    {
        parser::ast::Statement::Write(w) => w,
        other => panic!("expected a node write, got {other:?}"),
    };
    // A mix of an existing node (Alice) and two the batch creates, so the projection has
    // to resolve both a core dense id and freshly allocated born ids.
    let rows = Val::List(vec![
        Val::Map(vec![
            ("name".into(), Val::Str("Alice".into())),
            ("age".into(), Val::Int(31)),
        ]),
        Val::Map(vec![
            ("name".into(), Val::Str("Yves".into())),
            ("age".into(), Val::Int(32)),
        ]),
        Val::Map(vec![
            ("name".into(), Val::Str("Zoe".into())),
            ("age".into(), Val::Int(33)),
        ]),
    ]);
    let params = HashMap::from([("rows".to_string(), rows)]);
    let (cols, out) = execute_write(&writer, gen.as_ref(), &w, &params, TEST_BOLT_VERSION).unwrap();

    assert_eq!(cols, vec!["name".to_string(), "age".to_string()]);
    assert_eq!(
        out,
        vec![
            vec![PsValue::str("Alice"), PsValue::Int(31)],
            vec![PsValue::str("Yves"), PsValue::Int(32)],
            vec![PsValue::str("Zoe"), PsValue::Int(33)],
        ],
    );

    std::fs::remove_dir_all(&root).ok();
}

/// End-to-end Phase 2b: a business-key `DELETE` tombstones the anchor; a
/// subsequent read no longer binds it (read-your-deletes), a whole-label count
/// drops it (the count fast path falls back to a real scan under a live delta),
/// and the tombstone survives a writer reopen (WAL replay).
#[test]
fn delete_then_read_suppresses_node_and_survives_reopen() {
    let (root, _g, _) = testgen::write_basic("delete_ryow");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    // Helpers reading through the live overlay.
    let alice_rows = |w: &Arc<DeltaWriter>| -> usize {
        let view = MergedView::new(gen.as_ref(), DeltaSnapshot::from_memtable(w.snapshot()));
        let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.name").unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        res.rows.len()
    };
    let person_count = |w: &Arc<DeltaWriter>| -> i64 {
        let view = MergedView::new(gen.as_ref(), DeltaSnapshot::from_memtable(w.snapshot()));
        let ast = parser::parse("MATCH (n:Person) RETURN count(*)").unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        match res.rows[0][0] {
            Val::Int(n) => n,
            ref v => panic!("count not int: {v:?}"),
        }
    };

    // Baseline: Alice present, 3 Person nodes (Alice, Bob, Carol).
    assert_eq!(alice_rows(&writer), 1);
    assert_eq!(person_count(&writer), 3);

    // Delete Alice.
    // DETACH: Alice still has outgoing :KNOWS edges, so a plain DELETE would be
    // rejected (DELETE conformance); DETACH removes the node and detaches its edges.
    let stmt =
        match parser::parse_statement("MATCH (n:Person {name:'Alice'}) DETACH DELETE n").unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
    execute_write(
        &writer,
        gen.as_ref(),
        &stmt,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();

    // Read-your-deletes: the anchor scan no longer yields Alice, the count drops,
    // and her tombstone is stored under dense id 0.
    assert_eq!(alice_rows(&writer), 0, "Alice suppressed after delete");
    assert_eq!(person_count(&writer), 2, "tombstoned node not counted");
    assert!(writer.snapshot().node_patch(0).unwrap().tombstoned);

    // Durability: a fresh writer over the same WAL replays the tombstone.
    drop(writer);
    let reopened = DeltaWriter::open(
        wal.join("people"),
        "people",
        gen.uuid(),
        gen.node_count(),
        gen.edge_count(),
        None,
        |op| resolve_op(&gen, op),
    )
    .unwrap();
    assert!(
        reopened.snapshot().node_patch(0).unwrap().tombstoned,
        "the delete is durable across a reopen (WAL replay)"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// End-to-end Phase 2c: a `MERGE` on an absent business key creates a delta-born
/// node with a synthetic dense id. It reads back through a label scan, grows the
/// whole-label count, and survives a writer reopen (WAL replay re-allocates the
/// same synthetic id). A `MERGE` on an *existing* key patches it in place (no
/// duplicate). NB: addressing a born node by an *indexed* key seek
/// (`MATCH (n:Person {name:'Dave'})`) needs the Phase 2d index overlay — until
/// then a born node is found by a label scan, not a range-index probe.
#[test]
fn merge_creates_delta_born_node_and_survives_reopen() {
    let (root, _g, _) = testgen::write_basic("merge_create");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    // Read all Person (name, age) rows through the live overlay (a label scan, so
    // it enumerates core nodes then delta-born ones).
    let people = |w: &Arc<DeltaWriter>| -> Vec<(String, Option<i64>)> {
        let view = MergedView::new(gen.as_ref(), DeltaSnapshot::from_memtable(w.snapshot()));
        let ast = parser::parse("MATCH (n:Person) RETURN n.name, n.age").unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        res.rows
            .iter()
            .map(|r| {
                let name = match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("name not str: {v:?}"),
                };
                let age = match &r[1] {
                    Val::Int(n) => Some(*n),
                    Val::Null => None,
                    v => panic!("age not int/null: {v:?}"),
                };
                (name, age)
            })
            .collect()
    };

    let base = people(&writer);
    assert!(
        !base.iter().any(|(n, _)| n == "Dave"),
        "Dave absent at start"
    );
    let base_n = base.len();

    // Create Dave via MERGE on an absent business key.
    let stmt =
        match parser::parse_statement("MERGE (n:Person {name:'Dave'}) SET n.age = 50").unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
    assert!(stmt.upsert, "MERGE lowers to an upsert anchor");
    execute_write(
        &writer,
        gen.as_ref(),
        &stmt,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();

    // Read-your-writes: Dave appears in the label scan with both his business-key
    // (name) and his SET property (age), and the count grew by exactly one.
    let after = people(&writer);
    assert_eq!(after.len(), base_n + 1, "count grew by one");
    assert!(
        after.contains(&("Dave".to_string(), Some(50))),
        "born Dave reads back with name+age: {after:?}"
    );

    // MERGE on an existing key patches in place (no second Bob).
    let patch =
        match parser::parse_statement("MERGE (n:Person {name:'Bob'}) SET n.age = 123").unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
    execute_write(
        &writer,
        gen.as_ref(),
        &patch,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();
    let patched = people(&writer);
    assert_eq!(
        patched.len(),
        base_n + 1,
        "MERGE on an existing key does not duplicate"
    );
    assert_eq!(
        patched.iter().filter(|(n, _)| n == "Bob").count(),
        1,
        "exactly one Bob"
    );
    assert!(
        patched.contains(&("Bob".to_string(), Some(123))),
        "Bob patched in place: {patched:?}"
    );

    // Durability: a fresh writer over the same WAL replays create + patch, and the
    // born node keeps its synthetic id (allocation follows replay order).
    drop(writer);
    let reopened = DeltaWriter::open(
        wal.join("people"),
        "people",
        gen.uuid(),
        gen.node_count(),
        gen.edge_count(),
        None,
        |op| resolve_op(&gen, op),
    )
    .unwrap();
    let reopened = Arc::new(reopened);
    let replayed = people(&reopened);
    assert!(
        replayed.contains(&("Dave".to_string(), Some(50))),
        "born Dave is durable across a reopen: {replayed:?}"
    );
    assert!(
        replayed.contains(&("Bob".to_string(), Some(123))),
        "patch is durable across a reopen: {replayed:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// Deferred-from-2c: a `MERGE`-created (delta-born) node can be `DELETE`d by its
/// business key even though it has no core row. The DELETE anchor's core probe
/// returns `Absent`; the write path then resolves the born synthetic id from the
/// delta and tombstones it. The node vanishes from reads and the whole-label count,
/// deleting a genuinely-absent key is a clear error (not a silent no-op), and the
/// delete is durable across a writer reopen (WAL replay).
#[test]
fn delete_removes_a_delta_born_node_by_key() {
    let (root, _g, _) = testgen::write_basic("delete_born");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    // Read the Person names through the full live overlay (label scan enumerating
    // core then delta-born nodes).
    let names = |w: &Arc<DeltaWriter>| -> Vec<String> {
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let ast = parser::parse("MATCH (n:Person) RETURN n.name").unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        res.rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                v => panic!("name not str: {v:?}"),
            })
            .collect()
    };
    let write = |w: &Arc<DeltaWriter>, q: &str| {
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(s) => s,
            _ => panic!("expected a write: {q}"),
        };
        execute_write(w, gen.as_ref(), &stmt, &HashMap::new(), TEST_BOLT_VERSION)
    };

    let base_n = names(&writer).len();
    assert!(
        !names(&writer).contains(&"Dave".to_string()),
        "Dave absent at start"
    );

    // Create Dave (delta-born), then DELETE him by his business key.
    write(&writer, "MERGE (n:Person {name:'Dave'}) SET n.age = 50").unwrap();
    assert!(
        names(&writer).contains(&"Dave".to_string()),
        "born Dave present after create"
    );
    assert_eq!(names(&writer).len(), base_n + 1, "count grew by one");

    write(&writer, "MATCH (n:Person {name:'Dave'}) DELETE n").unwrap();
    let after = names(&writer);
    assert!(
        !after.contains(&"Dave".to_string()),
        "born Dave gone after delete: {after:?}"
    );
    assert_eq!(after.len(), base_n, "count back to the baseline");

    // Deleting a business key absent from both core and delta is a clear error.
    let err = write(&writer, "MATCH (n:Person {name:'Nobody'}) DELETE n").unwrap_err();
    assert!(
        err.message
            .contains("no Person(name = …) node to delete: the business key matches no"),
        "clear no-such-node error: {}",
        err.message
    );

    // Durability: a fresh writer over the same WAL replays create + delete, so Dave
    // stays gone (the DELETE's born synthetic id re-resolves on replay).
    drop(writer);
    let reopened = Arc::new(
        DeltaWriter::open(
            wal.join("people"),
            "people",
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            None,
            |op| resolve_op(&gen, op),
        )
        .unwrap(),
    );
    let replayed = names(&reopened);
    assert!(
        !replayed.contains(&"Dave".to_string()),
        "delete is durable across a reopen: {replayed:?}"
    );
    assert_eq!(replayed.len(), base_n, "count durable across a reopen");
    std::fs::remove_dir_all(&root).ok();
}

/// Write-UNWIND (group-commit surface): `UNWIND $rows AS r MERGE (n:Person {name:
/// r.name}) SET n.age = r.age` creates one node per row under a **single** group
/// commit (one epoch bump), each row's key + SET values evaluated against that row;
/// a batched `MATCH … DELETE` likewise removes them. Durable across a reopen.
#[test]
fn write_unwind_batches_node_writes_under_one_commit() {
    let (root, _g) = testgen::write_indexed_people("unwind_batch");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let names = |w: &Arc<DeltaWriter>| -> Vec<String> {
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let res = Engine::new(&view, &cache)
            .run(&parser::parse("MATCH (n:Person) RETURN n.name").unwrap())
            .unwrap();
        let mut out: Vec<String> = res
            .rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                v => panic!("name not str: {v:?}"),
            })
            .collect();
        out.sort();
        out
    };
    let age = |w: &Arc<DeltaWriter>, nm: &str| -> Vec<i64> {
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let q = format!("MATCH (n:Person {{name:'{nm}'}}) RETURN n.age");
        let res = Engine::new(&view, &cache)
            .run(&parser::parse(&q).unwrap())
            .unwrap();
        res.rows
            .iter()
            .filter_map(|r| match &r[0] {
                Val::Int(n) => Some(*n),
                _ => None,
            })
            .collect()
    };
    let run = |w: &Arc<DeltaWriter>, q: &str, params: &HashMap<String, Val>| {
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(s) => s,
            _ => panic!("expected a write: {q}"),
        };
        execute_write(w, gen.as_ref(), &stmt, params, TEST_BOLT_VERSION).unwrap();
    };

    let base_n = names(&writer).len();
    // A parameter list of row maps — the bulk-import shape.
    let rows = Val::List(vec![
        Val::Map(vec![
            ("name".into(), Val::Str("Xavier".into())),
            ("age".into(), Val::Int(10)),
        ]),
        Val::Map(vec![
            ("name".into(), Val::Str("Yolanda".into())),
            ("age".into(), Val::Int(20)),
        ]),
    ]);
    let mut params = HashMap::new();
    params.insert("rows".to_string(), rows);

    // Batched create: two born nodes, ONE group-committed epoch.
    let e0 = writer.epoch();
    run(
        &writer,
        "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
        &params,
    );
    assert_eq!(
        writer.epoch(),
        e0 + 1,
        "the whole batch is one epoch (group commit)"
    );
    let after = names(&writer);
    assert_eq!(
        after.len(),
        base_n + 2,
        "two born nodes created by the batch"
    );
    assert!(after.contains(&"Xavier".to_string()) && after.contains(&"Yolanda".to_string()));
    assert_eq!(age(&writer, "Xavier"), vec![10], "per-row SET applied");
    assert_eq!(age(&writer, "Yolanda"), vec![20]);

    // Durable across a reopen (WAL replay reconstructs the batch).
    drop(writer);
    let reopened = Arc::new(
        DeltaWriter::open(
            wal.join("people"),
            "people",
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            None,
            |op| resolve_op(&gen, op),
        )
        .unwrap(),
    );
    assert_eq!(age(&reopened, "Xavier"), vec![10], "batched writes durable");

    // Batched DELETE of the two born nodes via UNWIND (one epoch).
    let e1 = reopened.epoch();
    run(
        &reopened,
        "UNWIND $rows AS r MATCH (n:Person {name: r.name}) DELETE n",
        &params,
    );
    assert_eq!(reopened.epoch(), e1 + 1, "the batched delete is one epoch");
    let after_del = names(&reopened);
    assert!(
        !after_del.contains(&"Xavier".to_string()) && !after_del.contains(&"Yolanda".to_string()),
        "batched delete removed both born nodes: {after_del:?}"
    );
    assert_eq!(after_del.len(), base_n, "count back to the baseline");
    std::fs::remove_dir_all(&root).ok();
}

/// W8 — batched **relationship** writes, the edge twin of the test above. Graphiti's bulk
/// edge saves are all `UNWIND $edges AS edge … MERGE (a)-[r:R {uuid: edge.uuid}]->(b)`,
/// and until the edge grammar grew the `UNWIND` prefix the node grammar already had, both
/// `MENTIONS` and `RELATES_TO` were refused — so nothing graphiti extracted persisted.
///
/// Three properties, all of which the single-edge path already had and the batch must not
/// lose: the whole batch is **one** group commit; rows carrying **distinct identity
/// properties produce distinct edges** between the *same* pair rather than collapsing onto
/// one (the parallel-facts property, now under `UNWIND`); and the writes are durable
/// across a reopen. Plus the one thing only the batch has: a `RETURN` projecting one row
/// per input row.
#[test]
fn write_unwind_batches_edge_writes_under_one_commit() {
    let (root, _g) = testgen::write_indexed_people("unwind_edge_batch");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    // Every `fact` on a Bob-KNOWS->Carol edge, sorted — the parallel-edge probe.
    let facts = |w: &Arc<DeltaWriter>| -> Vec<String> {
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let res = Engine::new(&view, &cache)
            .run(
                &parser::parse(
                    "MATCH (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) \
                     RETURN r.fact",
                )
                .unwrap(),
            )
            .unwrap();
        let mut out: Vec<String> = res
            .rows
            .iter()
            .filter_map(|r| match &r[0] {
                Val::Str(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        out.sort();
        out
    };
    let run = |w: &Arc<DeltaWriter>, q: &str, params: &HashMap<String, Val>| {
        let stmt = match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::WriteEdge(s) => s,
            other => panic!("expected an edge write for {q:?}, got {other:?}"),
        };
        execute_edge_write(w, gen.as_ref(), &stmt, params, TEST_BOLT_VERSION).unwrap()
    };

    // Two facts about the *same* pair, distinguished only by `uuid` — exactly the shape
    // graphiti's `RELATES_TO` bulk save emits, and exactly what a store keying edges by
    // endpoint pair alone would fuse into one, losing a fact.
    let row = |uuid: &str, fact: &str| {
        Val::Map(vec![
            ("uuid".into(), Val::Str(uuid.into())),
            ("src".into(), Val::Str("Bob".into())),
            ("dst".into(), Val::Str("Carol".into())),
            ("fact".into(), Val::Str(fact.into())),
        ])
    };
    let params = HashMap::from([(
        "edges".to_string(),
        Val::List(vec![
            row("u1", "Bob taught Carol"),
            row("u2", "Bob hired Carol"),
        ]),
    )]);

    // `SET r = edge` is the whole-row replace graphiti uses, and the RETURN projects the
    // **row** variable, not the relationship — the `RELATES_TO` spelling.
    let e0 = writer.epoch();
    let (cols, out) = run(
        &writer,
        "UNWIND $edges AS edge \
         MATCH (a:Person {name: edge.src}) \
         MATCH (b:Person {name: edge.dst}) \
         MERGE (a)-[r:KNOWS {uuid: edge.uuid}]->(b) \
         SET r = edge \
         WITH r, edge \
         RETURN edge.uuid AS uuid",
        &params,
    );
    assert_eq!(
        writer.epoch(),
        e0 + 1,
        "the whole edge batch is one epoch (group commit)"
    );
    assert_eq!(cols, vec!["uuid".to_string()]);
    assert_eq!(
        out,
        vec![vec![PsValue::str("u1")], vec![PsValue::str("u2")]],
        "one projected row per input row, in input order"
    );
    assert_eq!(
        facts(&writer),
        vec![
            "Bob hired Carol".to_string(),
            "Bob taught Carol".to_string()
        ],
        "two identity-distinct rows are two edges between the same pair, not one"
    );

    // Re-running the identical batch is idempotent by edge identity: each row's `uuid`
    // still names its own edge, so nothing is duplicated.
    run(
        &writer,
        "UNWIND $edges AS edge \
         MATCH (a:Person {name: edge.src}) \
         MATCH (b:Person {name: edge.dst}) \
         MERGE (a)-[r:KNOWS {uuid: edge.uuid}]->(b) \
         SET r = edge \
         WITH r, edge \
         RETURN edge.uuid AS uuid",
        &params,
    );
    assert_eq!(
        facts(&writer).len(),
        2,
        "re-merging the same batch adds no edges"
    );

    // Durable across a reopen (WAL replay reconstructs the batched edge ops). The epoch
    // counts commits in *this* writer's life, so replay restarts it — what must survive is
    // the graph, both parallel edges included.
    drop(writer);
    let reopened = Arc::new(
        DeltaWriter::open(
            wal.join("people"),
            "people",
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            None,
            |op| resolve_op(&gen, op),
        )
        .unwrap(),
    );
    assert_eq!(
        facts(&reopened),
        vec![
            "Bob hired Carol".to_string(),
            "Bob taught Carol".to_string()
        ],
        "both parallel edges survive a WAL replay"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// Phase 2d: a range-index seek overlays the delta — an equality seek finds a
/// delta-born node and drops a tombstoned core node, and a range seek unions the
/// born node into the core hits. The fixture carries a `(Person, name)` index, so
/// `MATCH (n:Person {name: …})` plans a `RangeEq` and `WHERE n.name >= …` a
/// `RangeRange` (see `plan::choose_from_preds`) rather than a label scan — this is
/// the path 2c's label-scan overlay did *not* cover.
#[test]
fn range_index_seek_overlays_born_and_tombstoned() {
    let (root, _g) = testgen::write_indexed_people("range_overlay_2d");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    // Run a query over the live overlay, returning the `name` column as a set.
    let names = |q: &str| -> Vec<String> {
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let ast = parser::parse(q).unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        let mut out: Vec<String> = res
            .rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                v => panic!("name not str: {v:?}"),
            })
            .collect();
        out.sort();
        out
    };

    // Baseline: an equality seek for the not-yet-created Dave finds nothing.
    assert!(
        names("MATCH (n:Person {name:'Dave'}) RETURN n.name").is_empty(),
        "Dave absent before MERGE"
    );

    // Create Dave (a delta-born node) and delete Bob (a core tombstone).
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
    write("MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    // DETACH: Bob has an incident :KNOWS edge, so a plain DELETE would be rejected.
    write("MATCH (n:Person {name:'Bob'}) DETACH DELETE n");

    // RangeEq finds the born node — the headline 2d gap (a label scan already
    // found it in 2c; an *indexed key seek* did not until now).
    assert_eq!(
        names("MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age"),
        vec!["Dave".to_string()],
        "equality seek finds the delta-born node"
    );
    // RangeEq drops the tombstoned core node.
    assert!(
        names("MATCH (n:Person {name:'Bob'}) RETURN n.name").is_empty(),
        "equality seek drops the tombstoned core node"
    );
    // RangeRange (n.name >= 'C') unions the born Dave with core Carol; Alice/Bob
    // are below the bound (and Bob is deleted regardless).
    assert_eq!(
        names("MATCH (n:Person) WHERE n.name >= 'C' RETURN n.name"),
        vec!["Carol".to_string(), "Dave".to_string()],
        "range seek unions the delta-born node into the core hits"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Follow-up from 2d ("moved indexed value"): a *core* node whose property patch
/// changes an INDEXED value is relocated in the range index. `write_indexed_people`
/// carries a (Person, name) RANGE index; patching Alice's `name` to 'Alicia' must
/// move her — an equality seek finds her at the NEW value and misses her at the OLD
/// one, and a range seek relocates her likewise. (The value read back was already
/// correct via the property overlay; this closes the index-*membership* gap.)
/// Durable across a writer reopen.
#[test]
fn moved_indexed_value_relocates_a_patched_core_node() {
    let (root, _g) = testgen::write_indexed_people("moved_index_2d");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let names = |w: &Arc<DeltaWriter>, q: &str| -> Vec<String> {
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let res = Engine::new(&view, &cache)
            .run(&parser::parse(q).unwrap())
            .unwrap();
        let mut out: Vec<String> = res
            .rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                v => panic!("name not str: {v:?}"),
            })
            .collect();
        out.sort();
        out
    };

    // Baseline: Alice at her core value; nothing at 'Alicia'; a `>= 'Alicia'` range
    // excludes her (Alice < Alicia < Bob, Carol).
    assert_eq!(
        names(&writer, "MATCH (n:Person {name:'Alice'}) RETURN n.name"),
        vec!["Alice"]
    );
    assert!(names(&writer, "MATCH (n:Person {name:'Alicia'}) RETURN n.name").is_empty());
    assert_eq!(
        names(
            &writer,
            "MATCH (n:Person) WHERE n.name >= 'Alicia' RETURN n.name"
        ),
        vec!["Bob", "Carol"]
    );

    // Patch the indexed value: Alice → 'Alicia'.
    let stmt =
        match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.name = 'Alicia'")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
    execute_write(
        &writer,
        gen.as_ref(),
        &stmt,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();

    // Equality seek: found at the NEW value (moved in), missed at the OLD one (moved
    // out). The "moved in" is the load-bearing case — the relocated node is absent
    // from the core ISAM at 'Alicia', so without the overlay it is never a candidate.
    assert_eq!(
        names(&writer, "MATCH (n:Person {name:'Alicia'}) RETURN n.name"),
        vec!["Alicia"],
        "equality seek at the new indexed value finds the relocated node"
    );
    assert!(
        names(&writer, "MATCH (n:Person {name:'Alice'}) RETURN n.name").is_empty(),
        "equality seek at the old indexed value no longer finds it"
    );
    // Range seek relocates her into `[>= 'Alicia']`.
    assert_eq!(
        names(
            &writer,
            "MATCH (n:Person) WHERE n.name >= 'Alicia' RETURN n.name"
        ),
        vec!["Alicia", "Bob", "Carol"],
        "range seek unions the relocated core node into the hits"
    );

    // Durable across a reopen (WAL replay re-applies the patch onto the same dense id).
    drop(writer);
    let reopened = Arc::new(
        DeltaWriter::open(
            wal.join("people"),
            "people",
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            None,
            |op| resolve_op(&gen, op),
        )
        .unwrap(),
    );
    assert_eq!(
        names(&reopened, "MATCH (n:Person {name:'Alicia'}) RETURN n.name"),
        vec!["Alicia"],
        "relocation is durable across a reopen"
    );
    assert!(names(&reopened, "MATCH (n:Person {name:'Alice'}) RETURN n.name").is_empty());
    std::fs::remove_dir_all(&root).ok();
}

/// Phase 3b: the traversal read overlay. A `MERGE`-created relationship is
/// walkable (both directions), a deleted core edge no longer traverses, and an
/// edge to a tombstoned node is suppressed (closing the 2b gap). Edges are written
/// directly through the `DeltaWriter` (the write *grammar* is 3c) on the
/// `write_indexed_people` fixture: Alice(0)-[:KNOWS]->Bob(1), plus Carol(2), with a
/// `(Person, name)` index that resolves the anchors.
#[test]
fn edge_overlay_folds_born_and_deleted_edges() {
    let (root, _g) = testgen::write_indexed_people("edge_overlay_3b");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    // Run `q` over the live overlay, returning the single string column.
    let names = |q: &str| -> Vec<String> {
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let ast = parser::parse(q).unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        let mut out: Vec<String> = res
            .rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                v => panic!("expected str, got {v:?}"),
            })
            .collect();
        out.sort();
        out
    };
    let edge = |create: bool, src: u64, dst: u64| {
        let (sname, dname) = (
            ["Alice", "Bob", "Carol"][src as usize],
            ["Alice", "Bob", "Carol"][dst as usize],
        );
        let op = if create {
            WalOp::UpsertEdge {
                src_label: "Person".into(),
                src_key: "name".into(),
                src_value: Value::Str(sname.into()),
                reltype: "KNOWS".into(),
                dst_label: "Person".into(),
                dst_key: "name".into(),
                dst_value: Value::Str(dname.into()),
                edge_key: None,
                replace: false,
                patches: vec![],
            }
        } else {
            WalOp::DeleteEdge {
                src_label: "Person".into(),
                src_key: "name".into(),
                src_value: Value::Str(sname.into()),
                reltype: "KNOWS".into(),
                dst_label: "Person".into(),
                dst_key: "name".into(),
                dst_value: Value::Str(dname.into()),
                edge_key: None,
            }
        };
        writer
            .write(
                op,
                OpResolution::Edge {
                    src: Some(src),
                    dst: Some(dst),
                    edge_id: None,
                },
            )
            .unwrap();
    };

    // Baseline: only the core edge Alice-KNOWS->Bob.
    assert_eq!(
        names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"),
        vec!["Bob".to_string()]
    );
    assert!(names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name").is_empty());

    // Create a born edge Bob-KNOWS->Carol: now traversable outgoing from Bob and
    // incoming to Carol.
    edge(true, 1, 2);
    assert_eq!(
        names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
        vec!["Carol".to_string()],
        "born edge is walkable outgoing"
    );
    assert_eq!(
        names("MATCH (a)-[:KNOWS]->(b:Person {name:'Carol'}) RETURN a.name"),
        vec!["Bob".to_string()],
        "born edge is walkable incoming"
    );

    // Delete the core edge Alice-KNOWS->Bob: it stops traversing (both directions).
    edge(false, 0, 1);
    assert!(
        names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name").is_empty(),
        "deleted core edge no longer walks outgoing"
    );
    assert!(
        names("MATCH (a)-[:KNOWS]->(b:Person {name:'Bob'}) RETURN a.name").is_empty(),
        "deleted core edge no longer walks incoming"
    );
    // The born edge is unaffected by the unrelated delete.
    assert_eq!(
        names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
        vec!["Carol".to_string()]
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 3b (the closed 2b gap): a core edge to a node deleted via the delta is no
/// longer reachable by traversal — the node tombstone suppresses its incident core
/// edges on read.
#[test]
fn edge_overlay_suppresses_edge_to_tombstoned_node() {
    let (root, _g) = testgen::write_indexed_people("edge_overlay_tomb_3b");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let hop = || -> Vec<String> {
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let ast =
            parser::parse("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name").unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        res.rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                v => panic!("expected str, got {v:?}"),
            })
            .collect()
    };

    assert_eq!(hop(), vec!["Bob".to_string()], "core edge reaches Bob");

    // Delete Bob (the edge's destination) through the write path. DETACH because Bob
    // still has the incident :KNOWS edge — a plain DELETE would be rejected.
    let stmt =
        match parser::parse_statement("MATCH (n:Person {name:'Bob'}) DETACH DELETE n").unwrap() {
            parser::ast::Statement::Write(w) => w,
            _ => panic!("expected a write"),
        };
    execute_write(
        &writer,
        gen.as_ref(),
        &stmt,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();

    assert!(
        hop().is_empty(),
        "the core edge to the now-tombstoned Bob is suppressed"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// DELETE conformance (Stage 2): a plain `DELETE` of a node that still has
/// relationships is rejected — in either edge direction — and leaves the node in
/// place; `DETACH DELETE` removes the node and its edges.
#[test]
fn plain_delete_rejects_node_with_relationships_detach_allows() {
    let (root, _g) = testgen::write_indexed_people("delete_conformance_s2");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let run = |q: &str| -> std::result::Result<(), Failure> {
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => execute_write(
                &writer,
                gen.as_ref(),
                &w,
                &HashMap::new(),
                TEST_BOLT_VERSION,
            )
            .map(|_| ()),
            other => panic!("expected a node write for {q:?}, got {other:?}"),
        }
    };
    let present = |name: &str| -> bool {
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let q = format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.name");
        let ast = parser::parse(&q).unwrap();
        let rows = Engine::new(&view, &cache).run(&ast).unwrap().rows.len();
        rows > 0
    };

    // Alice has an outgoing :KNOWS edge to Bob → a plain DELETE is rejected, and
    // Alice is untouched.
    let e = run("MATCH (n:Person {name:'Alice'}) DELETE n").unwrap_err();
    assert!(
        e.message.contains("still has relationships"),
        "got: {}",
        e.message
    );
    assert!(present("Alice"), "the rejected DELETE left Alice in place");

    // Bob has an *incoming* :KNOWS edge from Alice → a plain DELETE is rejected too
    // (the check sees both directions).
    let e = run("MATCH (n:Person {name:'Bob'}) DELETE n").unwrap_err();
    assert!(
        e.message.contains("still has relationships"),
        "got: {}",
        e.message
    );
    assert!(present("Bob"), "the rejected DELETE left Bob in place");

    // DETACH DELETE removes Alice and her edges; a subsequent plain DELETE of Bob
    // now succeeds (his only relationship was the edge from Alice, now gone).
    run("MATCH (n:Person {name:'Alice'}) DETACH DELETE n").unwrap();
    assert!(!present("Alice"), "DETACH DELETE removed Alice");
    run("MATCH (n:Person {name:'Bob'}) DELETE n").unwrap();
    assert!(
        !present("Bob"),
        "Bob had no remaining edges, so plain DELETE worked"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// End-to-end Stage 3: `REMOVE n.p` drops a property, `SET n = {map}` replaces all
/// of them (the anchor business key survives), and touching the anchor key is
/// rejected — all read back through the live overlay.
#[test]
fn remove_and_replace_read_back_through_the_overlay() {
    let (root, _g) = testgen::write_indexed_people("remove_replace_s3");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let run = |q: &str| -> std::result::Result<(), Failure> {
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => execute_write(
                &writer,
                gen.as_ref(),
                &w,
                &HashMap::new(),
                TEST_BOLT_VERSION,
            )
            .map(|_| ()),
            other => panic!("expected a node write for {q:?}, got {other:?}"),
        }
    };
    // A single property, read through the live overlay, rendered to a comparable
    // string (`Val` has no `PartialEq`): `null` / `int:N` / `str:S`.
    let prop = |name: &str, p: &str| -> String {
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let q = format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.{p}");
        let ast = parser::parse(&q).unwrap();
        let mut rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
        match rows.pop().map(|mut r| r.remove(0)).unwrap_or(Val::Null) {
            Val::Null => "null".to_string(),
            Val::Int(n) => format!("int:{n}"),
            Val::Str(s) => format!("str:{s}"),
            other => format!("other:{other:?}"),
        }
    };

    // Seed Alice with a new property, then REMOVE it: the property reads back Null
    // while an untouched core property (age) is unaffected.
    run("MATCH (n:Person {name:'Alice'}) SET n.city = 'NYC'").unwrap();
    assert_eq!(prop("Alice", "city"), "str:NYC");
    run("MATCH (n:Person {name:'Alice'}) REMOVE n.city").unwrap();
    assert_eq!(prop("Alice", "city"), "null", "REMOVE drops the property");
    assert_eq!(
        prop("Alice", "age"),
        "int:30",
        "an untouched core prop stands"
    );

    // Replace-all on Bob: a prior property (city) is wiped, `age` is replaced, and the
    // anchor business key (name) survives even though the map omits it.
    run("MATCH (n:Person {name:'Bob'}) SET n.city = 'LA'").unwrap();
    run("MATCH (n:Person {name:'Bob'}) SET n = {age: 99}").unwrap();
    assert_eq!(prop("Bob", "age"), "int:99", "replace-all set the new age");
    assert_eq!(
        prop("Bob", "city"),
        "null",
        "replace-all wiped the old city"
    );
    assert_eq!(
        prop("Bob", "name"),
        "str:Bob",
        "the anchor business key survives a replace-all"
    );

    // The anchor key cannot be REMOVEd — it is the node's identity.
    let e = run("MATCH (n:Person {name:'Carol'}) REMOVE n.name").unwrap_err();
    assert!(e.message.contains("business-key"), "got: {}", e.message);
    // …but it may be re-set (here via replace-all), which relocates the node in the
    // index — it is then found at its new key value.
    run("MATCH (n:Person {name:'Carol'}) SET n = {name: 'Xavier'}").unwrap();
    assert_eq!(
        prop("Xavier", "name"),
        "str:Xavier",
        "replace-all relocated the node to its new key value"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// End-to-end Stage 4: `SET n += {map}` merges, multiple SET items fold in source
/// order (last-writer-wins), and a replace-all mixed with a following SET
/// group-commits (the post-replace patch lands on top of the replaced base).
#[test]
fn multi_item_and_merge_map_set_fold_in_source_order() {
    let (root, _g) = testgen::write_indexed_people("multi_set_s4");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let run = |q: &str| {
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
            other => panic!("expected a node write for {q:?}, got {other:?}"),
        };
    };
    let prop = |name: &str, p: &str| -> String {
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let q = format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.{p}");
        let ast = parser::parse(&q).unwrap();
        let mut rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
        match rows.pop().map(|mut r| r.remove(0)).unwrap_or(Val::Null) {
            Val::Null => "null".to_string(),
            Val::Int(n) => format!("int:{n}"),
            Val::Str(s) => format!("str:{s}"),
            other => format!("other:{other:?}"),
        }
    };

    // `SET n += {map}` adds every entry.
    run("MATCH (n:Person {name:'Alice'}) SET n += {city: 'NYC', role: 'eng'}");
    assert_eq!(prop("Alice", "city"), "str:NYC");
    assert_eq!(prop("Alice", "role"), "str:eng");

    // Mixed items fold in source order, last-writer-wins across Prop and merge-map.
    run("MATCH (n:Person {name:'Bob'}) SET n.score = 1, n += {score: 2, tier: 'A'}, n.tier = 'B'");
    assert_eq!(
        prop("Bob", "score"),
        "int:2",
        "the later merge-map value wins over the earlier prop"
    );
    assert_eq!(
        prop("Bob", "tier"),
        "str:B",
        "the later prop wins over the merge-map"
    );

    // A replace-all mixed with a following SET group-commits: the replace wipes the
    // earlier property, then the post-replace patch lands on top.
    run("MATCH (n:Person {name:'Carol'}) SET n.old = 'x'");
    run("MATCH (n:Person {name:'Carol'}) SET n = {age: 50}, n.city = 'LA'");
    assert_eq!(prop("Carol", "age"), "int:50", "replace set the new age");
    assert_eq!(
        prop("Carol", "city"),
        "str:LA",
        "the post-replace SET applied on top"
    );
    assert_eq!(
        prop("Carol", "old"),
        "null",
        "the replace wiped the earlier property"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// End-to-end Stage 5: `SET n:Label` / `REMOVE n:Label` change what a node matches and
/// scans as, the label counts stay **exact** under the overlay (no fall-back scan),
/// the first-label grouping re-buckets, and the guards (brand-new label, born identity
/// label) fire.
#[test]
fn label_mutation_matches_scans_counts_and_validates() {
    let (root, _g, _) = testgen::write_basic("label_mut_s5");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let run = |q: &str| -> std::result::Result<(), Failure> {
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => execute_write(
                &writer,
                gen.as_ref(),
                &w,
                &HashMap::new(),
                TEST_BOLT_VERSION,
            )
            .map(|_| ()),
            other => panic!("expected a node write for {q:?}, got {other:?}"),
        }
    };
    let view = || {
        MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        )
    };
    let names = |q: &str| -> Vec<String> {
        let v = view();
        let ast = parser::parse(q).unwrap();
        let mut out: Vec<String> = Engine::new(&v, &cache)
            .run(&ast)
            .unwrap()
            .rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                other => panic!("expected str, got {other:?}"),
            })
            .collect();
        out.sort();
        out
    };
    let count = |q: &str| -> i64 {
        let v = view();
        let ast = parser::parse(q).unwrap();
        let n = match Engine::new(&v, &cache).run(&ast).unwrap().rows[0][0] {
            Val::Int(n) => n,
            ref other => panic!("count not int: {other:?}"),
        };
        n
    };

    let base_person = count("MATCH (n:Person) RETURN count(*)");
    let base_company = count("MATCH (n:Company) RETURN count(*)");

    // SET n:Company on a Person → it now matches and scans as :Company, and the exact
    // label count grows by one; it still matches :Person.
    run("MATCH (n:Person {name:'Alice'}) SET n:Company").unwrap();
    assert!(names("MATCH (n:Company) RETURN n.name").contains(&"Alice".to_string()));
    assert_eq!(
        count("MATCH (n:Company) RETURN count(*)"),
        base_company + 1,
        "exact label count reflects the added label under the overlay"
    );
    assert!(names("MATCH (n:Person) RETURN n.name").contains(&"Alice".to_string()));
    assert_eq!(
        count("MATCH (n:Person) RETURN count(*)"),
        base_person,
        "Person count is unchanged (Alice kept :Person)"
    );

    // REMOVE it → back to the baseline.
    run("MATCH (n:Person {name:'Alice'}) REMOVE n:Company").unwrap();
    assert!(!names("MATCH (n:Company) RETURN n.name").contains(&"Alice".to_string()));
    assert_eq!(count("MATCH (n:Company) RETURN count(*)"), base_company);

    // Removing the identity label of an existing **core** node is allowed; the exact
    // Person count drops, and the node re-buckets to the null first-label group.
    run("MATCH (n:Person {name:'Bob'}) REMOVE n:Person").unwrap();
    assert!(!names("MATCH (n:Person) RETURN n.name").contains(&"Bob".to_string()));
    assert_eq!(
        count("MATCH (n:Person) RETURN count(*)"),
        base_person - 1,
        "exact label count reflects the dropped label"
    );
    // First-label grouping re-buckets Bob from Person to null.
    let group = |first: &str| -> i64 {
        let v = view();
        let q =
            format!("MATCH (n) WITH labels(n)[0] AS l, count(*) AS c WHERE l = '{first}' RETURN c");
        let ast = parser::parse(&q).unwrap();
        let rows = Engine::new(&v, &cache).run(&ast).unwrap().rows;
        match rows.first().map(|r| &r[0]) {
            Some(Val::Int(n)) => *n,
            _ => 0,
        }
    };
    assert_eq!(
        group("Person"),
        base_person - 1,
        "the first-label Person group loses Bob"
    );

    // A brand-new label (absent from the core symbol table) is rejected by name.
    let e = run("MATCH (n:Person {name:'Carol'}) SET n:Ghost").unwrap_err();
    assert!(e.message.contains("not defined"), "got: {}", e.message);

    // A delta-born node's identity label cannot be removed.
    run("MERGE (n:Person {name:'Zoe'}) SET n.age = 1").unwrap();
    let e = run("MATCH (n:Person {name:'Zoe'}) REMOVE n:Person").unwrap_err();
    assert!(e.message.contains("identity label"), "got: {}", e.message);

    std::fs::remove_dir_all(&root).ok();
}

/// End-to-end Stage 7: `CREATE` makes a node from its inline props (business key = the
/// range-indexed one); `MERGE … ON CREATE / ON MATCH SET` fire the right branch by
/// whether the node was created or matched.
#[test]
fn create_and_merge_conditional_sets_end_to_end() {
    let (root, _g) = testgen::write_indexed_people("stage7");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let run = |q: &str| -> std::result::Result<(), Failure> {
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => execute_write(
                &writer,
                gen.as_ref(),
                &w,
                &HashMap::new(),
                TEST_BOLT_VERSION,
            )
            .map(|_| ()),
            parser::ast::Statement::Create(c) => execute_create(
                &writer,
                gen.as_ref(),
                &c,
                &HashMap::new(),
                TEST_BOLT_VERSION,
            )
            .map(|_| ()),
            other => panic!("expected a write/create for {q:?}, got {other:?}"),
        }
    };
    let prop = |name: &str, p: &str| -> String {
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let q = format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.{p}");
        let ast = parser::parse(&q).unwrap();
        let mut rows = Engine::new(&view, &cache).run(&ast).unwrap().rows;
        match rows.pop().map(|mut r| r.remove(0)).unwrap_or(Val::Null) {
            Val::Null => "null".to_string(),
            Val::Int(n) => format!("int:{n}"),
            Val::Str(s) => format!("str:{s}"),
            other => format!("other:{other:?}"),
        }
    };

    // CREATE makes a node with its inline properties (name is the range-indexed key).
    run("CREATE (n:Person {name: 'Zoe', age: 20})").unwrap();
    assert_eq!(
        prop("Zoe", "age"),
        "int:20",
        "CREATE made the node with its props"
    );

    // MERGE on an absent key → ON CREATE fires.
    run("MERGE (n:Person {name: 'Yan'}) ON CREATE SET n.origin = 'created' ON MATCH SET n.origin = 'matched'").unwrap();
    assert_eq!(
        prop("Yan", "origin"),
        "str:created",
        "ON CREATE fired for a new node"
    );

    // MERGE on an existing core key (Alice) → ON MATCH fires.
    run("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.origin = 'created' ON MATCH SET n.origin = 'matched'").unwrap();
    assert_eq!(
        prop("Alice", "origin"),
        "str:matched",
        "ON MATCH fired for an existing node"
    );

    // Re-MERGE Yan → it now matches the delta-born node created above.
    run(
        "MERGE (n:Person {name: 'Yan'}) ON CREATE SET n.origin = 'c2' ON MATCH SET n.origin = 'm2'",
    )
    .unwrap();
    assert_eq!(
        prop("Yan", "origin"),
        "str:m2",
        "the second MERGE matched the born node"
    );

    // CREATE with no range-indexed property among its props is rejected.
    let e = run("CREATE (n:Person {city: 'X'})").unwrap_err();
    assert!(
        e.message.contains("range-indexed business key"),
        "got: {}",
        e.message
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 3c: the relationship write grammar, end to end. `MERGE (a)-[:R]->(b)`
/// creates a walkable edge (idempotent against an existing core edge, and
/// auto-creating an absent endpoint); `MATCH (a)-[r:R]->(b) DELETE r` removes one;
/// an unknown relationship type is rejected.
#[test]
fn edge_write_grammar_end_to_end() {
    let (root, _g) = testgen::write_indexed_people("edge_write_3c");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let run_write = |q: &str| -> std::result::Result<(), Failure> {
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::WriteEdge(w) => execute_edge_write(
                &writer,
                gen.as_ref(),
                &w,
                &HashMap::new(),
                TEST_BOLT_VERSION,
            )
            .map(|_| ()),
            other => panic!("expected an edge write for {q:?}, got {other:?}"),
        }
    };
    let names = |q: &str| -> Vec<String> {
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(writer.snapshot()),
        );
        let ast = parser::parse(q).unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        let mut out: Vec<String> = res
            .rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                v => panic!("expected str, got {v:?}"),
            })
            .collect();
        out.sort();
        out
    };

    // Create Bob-KNOWS->Carol.
    run_write("MERGE (a:Person {name:'Bob'})-[:KNOWS]->(b:Person {name:'Carol'})").unwrap();
    assert_eq!(
        names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
        vec!["Carol".to_string()]
    );

    // Idempotent MERGE of the existing core edge Alice-KNOWS->Bob: no duplicate.
    run_write("MERGE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})").unwrap();
    assert_eq!(
        names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name"),
        vec!["Bob".to_string()],
        "MERGE of an existing core edge does not duplicate it"
    );

    // MERGE with an absent destination auto-creates the born node + edge.
    run_write("MERGE (a:Person {name:'Bob'})-[:KNOWS]->(b:Person {name:'Zoe'})").unwrap();
    assert_eq!(
        names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
        vec!["Carol".to_string(), "Zoe".to_string()],
        "born endpoint Zoe is created and reachable"
    );
    assert!(
        names("MATCH (n:Person) RETURN n.name").contains(&"Zoe".to_string()),
        "born endpoint Zoe is a Person node"
    );

    // Delete the core edge Alice-KNOWS->Bob.
    run_write("MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r")
        .unwrap();
    assert!(
        names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name").is_empty(),
        "the deleted core edge no longer traverses"
    );

    // An unknown relationship type is rejected.
    let err = run_write("MERGE (a:Person {name:'Alice'})-[:NOPE]->(b:Person {name:'Carol'})")
        .unwrap_err();
    assert!(
        err.message.contains("must already exist"),
        "unknown reltype rejected: {}",
        err.message
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 3c durability: a created edge and a deleted core edge survive a WAL
/// reopen — the edge WAL ops replay and re-resolve their endpoints deterministically
/// (born endpoints re-allocate their synthetic ids in replay order).
#[test]
fn edge_writes_survive_a_reopen() {
    let (root, _g) = testgen::write_indexed_people("edge_durable_3c");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    {
        let writer = graphs.writer("people").unwrap();
        // Create Bob-KNOWS->Carol and delete the core Alice-KNOWS->Bob.
        let mk = |q: &str| match parser::parse_statement(q).unwrap() {
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
            _ => panic!("expected an edge write"),
        };
        mk("MERGE (a:Person {name:'Bob'})-[:KNOWS]->(b:Person {name:'Carol'})");
        mk("MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r");
    }

    // Reopen the writer over the same WAL and re-run the reads over the fresh delta.
    let reopened = Arc::new(
        DeltaWriter::open(
            wal.join("people"),
            "people",
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            None,
            |op| resolve_op(&gen, op),
        )
        .unwrap(),
    );
    let names = |q: &str| -> Vec<String> {
        let view = MergedView::new(
            gen.as_ref(),
            DeltaSnapshot::from_memtable(reopened.snapshot()),
        );
        let ast = parser::parse(q).unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        res.rows
            .iter()
            .map(|r| match &r[0] {
                Val::Str(s) => s.clone(),
                v => panic!("expected str, got {v:?}"),
            })
            .collect()
    };
    assert_eq!(
        names("MATCH (a:Person {name:'Bob'})-[:KNOWS]->(b) RETURN b.name"),
        vec!["Carol".to_string()],
        "created edge is durable"
    );
    assert!(
        names("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name").is_empty(),
        "deleted edge stays deleted across a reopen"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// Edge properties (follow-up from 3c): `MERGE (a)-[r:R]->(b) SET r.p = …` gives a
/// delta-born edge properties; a re-`MERGE` patches them in place; they read back via
/// `RETURN r.p`, and survive a reopen. Patching a *core* edge's properties in place is
/// now supported too — a `SET` on an existing core edge updates it, a bare re-`MERGE`
/// stays an idempotent no-op, and the patch replays across a reopen. (`write_indexed_people`
/// carries a core edge Alice-KNOWS->Bob with `since = 2020`.)
#[test]
fn edge_properties_end_to_end() {
    let (root, _g) = testgen::write_indexed_people("edge_props_3");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let cache = BlockCache::new(1 << 20);

    let run_write = |q: &str| -> std::result::Result<(), Failure> {
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::WriteEdge(w) => execute_edge_write(
                &writer,
                gen.as_ref(),
                &w,
                &HashMap::new(),
                TEST_BOLT_VERSION,
            )
            .map(|_| ()),
            other => panic!("expected an edge write for {q:?}, got {other:?}"),
        }
    };
    // Read a single scalar column over the live overlay (Int, or -1 for Null).
    let scalar = |w: &Arc<DeltaWriter>, q: &str| -> Vec<i64> {
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let res = Engine::new(&view, &cache)
            .run(&parser::parse(q).unwrap())
            .unwrap();
        res.rows
            .iter()
            .map(|r| match &r[0] {
                Val::Int(n) => *n,
                Val::Null => -1,
                v => panic!("expected int/null, got {v:?}"),
            })
            .collect()
    };

    // Create a born edge Bob-KNOWS->Carol with a property.
    run_write(
        "MERGE (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) SET r.since = 1999",
    )
    .unwrap();
    assert_eq!(
        scalar(
            &writer,
            "MATCH (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) RETURN r.since"
        ),
        vec![1999],
        "born edge property reads back"
    );

    // Re-MERGE patches the property in place and adds a second one (no duplicate edge).
    run_write(
            "MERGE (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) SET r.since = 2000, r.weight = 5",
        )
        .unwrap();
    assert_eq!(
        scalar(
            &writer,
            "MATCH (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) RETURN r.since"
        ),
        vec![2000],
        "re-MERGE patches the property"
    );
    assert_eq!(
        scalar(
            &writer,
            "MATCH (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) RETURN r.weight"
        ),
        vec![5],
        "a second property is added"
    );

    // Patching a CORE edge's properties in place now updates it (was rejected before).
    assert_eq!(
        scalar(
            &writer,
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN r.since"
        ),
        vec![2020],
        "the core edge's original property reads from the core"
    );
    run_write("MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) SET r.since = 7")
        .unwrap();
    assert_eq!(
        scalar(
            &writer,
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN r.since"
        ),
        vec![7],
        "the core edge's property is patched in place"
    );
    // A bare re-MERGE of that same core edge is still an idempotent no-op — the patch stands.
    run_write("MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'})").unwrap();
    assert_eq!(
        scalar(
            &writer,
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN r.since"
        ),
        vec![7],
        "a bare re-MERGE leaves the core-edge patch intact"
    );

    // Durable across a reopen: the born edge's patched properties AND the core-edge
    // patch replay (the latter re-resolves its core edge id via `resolve_op`).
    drop(writer);
    let reopened = Arc::new(
        DeltaWriter::open(
            wal.join("people"),
            "people",
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            None,
            |op| resolve_op(&gen, op),
        )
        .unwrap(),
    );
    assert_eq!(
        scalar(
            &reopened,
            "MATCH (a:Person {name:'Bob'})-[r:KNOWS]->(b:Person {name:'Carol'}) RETURN r.since"
        ),
        vec![2000],
        "born edge properties are durable across a reopen"
    );
    assert_eq!(
        scalar(
            &reopened,
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN r.since"
        ),
        vec![7],
        "the core-edge patch is durable across a reopen"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The result-cache key includes the delta epoch, so a write invalidates an
/// overlaid result rather than serving it stale.
#[test]
fn result_key_binds_delta_epoch() {
    let g = GenId(uuid::Uuid::from_u128(7));
    let k0 = ResultKey::with_delta_epoch(g, 0, "q");
    let k1 = ResultKey::with_delta_epoch(g, 1, "q");
    assert_ne!(k0, k1, "a bumped epoch keys differently");
    assert_eq!(k0, ResultKey::new(g, "q"), "epoch 0 == the read-only key");
}
