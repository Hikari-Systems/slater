// SPDX-License-Identifier: Apache-2.0
//! `resolve_and_keys` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// Phase 6 slice 6.1: the write path resolves a business key **through the core stack**,
/// closing the 4.1 note (e) gap. After a flush moves born nodes into a segment, a
/// re-`MERGE` of one of those keys must resolve to the *segment* id — patching it in place
/// — rather than allocate a duplicate born node; a `MERGE` of a base key still resolves to
/// the base id; and an edge whose endpoint is a **segment-born** node resolves that
/// endpoint through the fold too. A second flush folds the patches/born edge into a second
/// segment and the counts are still duplicate-free after a reopen.
#[test]
fn resolve_through_the_stack_reuses_a_flushed_key_no_duplicate() {
    let (root, _g) = testgen::write_indexed_people("resolve_stack_e2e");
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
    let q = |graphs: &Graphs, q: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let ast = parser::parse(q).unwrap();
        let r = Engine::new(&view, &cache).run(&ast).unwrap();
        r
    };

    // Flush two born nodes + a born edge into an upper segment.
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
    write(
        &graphs,
        "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Eve'})",
    );
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes");
    assert!(
        graphs.writer("people").unwrap().snapshot().is_empty(),
        "delta retired empty after the flush"
    );

    // Re-MERGE the *segment-born* key Dave: it must resolve to the segment id and patch it,
    // NOT create a second Dave. Without the stack fold, resolve returns Absent → duplicate.
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 99");
    // MERGE a *base* key: resolves to the base id and patches it.
    write(&graphs, "MERGE (n:Person {name:'Alice'}) SET n.age = 31");
    // An edge whose source endpoint is the segment-born Dave resolves that endpoint through
    // the fold (via resolve_endpoint → resolve_business_key), and the base Carol as dst.
    write(
        &graphs,
        "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(c:Person {name:'Carol'})",
    );

    // Exactly one Dave, patched to 99 (the delta patch over the segment row).
    let dave = q(&graphs, "MATCH (n:Person {name:'Dave'}) RETURN n.age");
    assert_eq!(
        dave.rows.len(),
        1,
        "exactly one Dave — no duplicate born node"
    );
    assert!(
        matches!(dave.rows[0][0], Val::Int(99)),
        "Dave patched to 99"
    );
    // Alice patched over the base row; still one Alice.
    let alice = q(&graphs, "MATCH (n:Person {name:'Alice'}) RETURN n.age");
    assert_eq!(alice.rows.len(), 1, "exactly one Alice");
    assert!(
        matches!(alice.rows[0][0], Val::Int(31)),
        "Alice patched to 31"
    );
    // 3 base + 2 born = 5 people, no duplicates introduced by the re-MERGEs.
    let n = q(&graphs, "MATCH (n:Person) RETURN count(*)");
    assert!(
        matches!(n.rows[0][0], Val::Int(5)),
        "5 people: {:?}",
        n.rows[0][0]
    );
    // Dave now KNOWS both Eve (segment edge) and Carol (the new born edge over a folded
    // segment endpoint).
    let mut targets: Vec<String> = q(
        &graphs,
        "MATCH (a:Person {name:'Dave'})-[:KNOWS]->(b) RETURN b.name",
    )
    .rows
    .into_iter()
    .map(|r| match &r[0] {
        Val::Str(s) => s.clone(),
        other => panic!("expected a name: {other:?}"),
    })
    .collect();
    targets.sort();
    assert_eq!(
        targets,
        vec!["Carol".to_string(), "Eve".to_string()],
        "Dave KNOWS Eve + Carol"
    );

    // A second flush folds the patches + the new born edge into a second segment; the id
    // space and counts are unchanged (the re-MERGEs never duplicated).
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("the second delta flushes");
    assert_eq!(
        graphs.get("people").unwrap().stack().segments().len(),
        2,
        "two upper segments after the second flush"
    );
    let n2 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
    assert!(
        matches!(n2.rows[0][0], Val::Int(5)),
        "still 5 after the second flush"
    );
    let dave2 = q(&graphs, "MATCH (n:Person {name:'Dave'}) RETURN n.age");
    assert_eq!(dave2.rows.len(), 1, "still one Dave");
    assert!(
        matches!(dave2.rows[0][0], Val::Int(99)),
        "Dave 99 folded into seg 2"
    );

    // Reopen from disk: the two-segment set reloads and resolution still de-duplicates.
    drop(graphs);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let n3 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
    assert!(
        matches!(n3.rows[0][0], Val::Int(5)),
        "5 after reopen: {:?}",
        n3.rows[0][0]
    );
    // A re-MERGE of Dave after the reopen still resolves through the reloaded stack.
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 77");
    let dave3 = q(&graphs, "MATCH (n:Person {name:'Dave'}) RETURN n.age");
    assert_eq!(
        dave3.rows.len(),
        1,
        "still one Dave after reopen + re-MERGE"
    );
    assert!(
        matches!(dave3.rows[0][0], Val::Int(77)),
        "Dave re-patched to 77 post-reopen"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 6 slice 6.3: the **batched** write path (`execute_write_batch`) resolves the whole
/// batch's business keys through the core stack in one merge-join sweep
/// (`resolve_business_keys_batch`) — byte-identically to the per-row single path, but at one
/// block decompress per touched fragment block instead of per row (the bulk-write ISAM
/// floor, memory `bulk-delete-isam-resolve-floor`). A single `UNWIND … MERGE … SET` batch
/// over a flushed segment must: reuse a *segment-born* key (patch, no duplicate), patch a
/// *base* key, born an *absent* key, and honour a *within-batch duplicate* key (both rows
/// resolve to the same id, group-commit LWW) — leaving the graph duplicate-free.
#[test]
fn batch_resolve_through_the_stack_reuses_flushed_keys_no_duplicate() {
    let (root, _g) = testgen::write_indexed_people("batch_resolve_stack_e2e");
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
            _ => panic!("expected a write: {q}"),
        }
    };
    let batch = |graphs: &Graphs, q: &str, params: &HashMap<String, Val>| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => {
                execute_write(&writer, gen.as_ref(), &w, params, TEST_BOLT_VERSION).unwrap();
            }
            _ => panic!("expected a write: {q}"),
        }
    };
    let ages = |graphs: &Graphs, nm: &str| -> Vec<i64> {
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let qy = format!("MATCH (n:Person {{name:'{nm}'}}) RETURN n.age");
        let res = Engine::new(&view, &cache)
            .run(&parser::parse(&qy).unwrap())
            .unwrap();
        res.rows
            .iter()
            .filter_map(|r| match &r[0] {
                Val::Int(n) => Some(*n),
                _ => None,
            })
            .collect()
    };
    let count = |graphs: &Graphs| -> i64 {
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let res = Engine::new(&view, &cache)
            .run(&parser::parse("MATCH (n:Person) RETURN count(*)").unwrap())
            .unwrap();
        match res.rows[0][0] {
            Val::Int(n) => n,
            ref v => panic!("count not int: {v:?}"),
        }
    };

    // Flush two born nodes into an upper segment (Dave, Eve).
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(&graphs, "MERGE (n:Person {name:'Eve'}) SET n.age = 60");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes");
    assert_eq!(count(&graphs), 5, "3 base + 2 flushed born");

    // One batch: Dave (segment-born → patch), Alice (base → patch), Frank (absent → born),
    // Dave again (within-batch duplicate → same id, group-commit LWW). The merge-join
    // resolve must fold the stack for every distinct key in the sweep.
    let rows = Val::List(vec![
        Val::Map(vec![
            ("name".into(), Val::Str("Dave".into())),
            ("age".into(), Val::Int(99)),
        ]),
        Val::Map(vec![
            ("name".into(), Val::Str("Alice".into())),
            ("age".into(), Val::Int(31)),
        ]),
        Val::Map(vec![
            ("name".into(), Val::Str("Frank".into())),
            ("age".into(), Val::Int(40)),
        ]),
        Val::Map(vec![
            ("name".into(), Val::Str("Dave".into())),
            ("age".into(), Val::Int(88)),
        ]),
    ]);
    let mut params = HashMap::new();
    params.insert("rows".to_string(), rows);
    batch(
        &graphs,
        "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
        &params,
    );

    // Duplicate-free: one Dave (LWW → 88), one Alice (patched → 31), one born Frank (40).
    assert_eq!(ages(&graphs, "Dave"), vec![88], "one Dave, last write wins");
    assert_eq!(ages(&graphs, "Alice"), vec![31], "base Alice patched once");
    assert_eq!(ages(&graphs, "Frank"), vec![40], "absent Frank born once");
    assert_eq!(count(&graphs), 6, "5 + 1 born Frank, no duplicates");

    // Flush + reopen: the batch resolve still de-duplicates against the reloaded 2-seg set.
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("the second delta flushes");
    drop(graphs);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    assert_eq!(count(&graphs), 6, "6 after reopen");

    // A second batch re-touching the now-flushed Dave/Frank keys reuses them (no dup).
    let rows2 = Val::List(vec![
        Val::Map(vec![
            ("name".into(), Val::Str("Dave".into())),
            ("age".into(), Val::Int(77)),
        ]),
        Val::Map(vec![
            ("name".into(), Val::Str("Frank".into())),
            ("age".into(), Val::Int(41)),
        ]),
    ]);
    let mut params2 = HashMap::new();
    params2.insert("rows".to_string(), rows2);
    batch(
        &graphs,
        "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
        &params2,
    );
    assert_eq!(
        ages(&graphs, "Dave"),
        vec![77],
        "Dave re-patched post-reopen"
    );
    assert_eq!(
        ages(&graphs, "Frank"),
        vec![41],
        "Frank re-patched post-reopen"
    );
    assert_eq!(count(&graphs), 6, "still 6 — batch reuse, no duplicate");

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 6 slice 6.1: a base key **deleted into a segment** resolves `Absent` on the write
/// path (its base index entry is superseded by the segment's `removals` sidecar, folded by
/// `CoreStack::fold_index_eq`), so a re-`MERGE` **reborns** it as a fresh born node rather
/// than resurrecting the tombstoned id — and a second re-`MERGE` is idempotent (the born
/// node resolves through the memtable's own identity, not the stack).
#[test]
fn resolve_reborns_a_key_deleted_into_a_segment() {
    let (root, _g) = testgen::write_indexed_people("resolve_rebirth_e2e");
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
    let q = |graphs: &Graphs, q: &str| -> QueryResult {
        let gen = graphs.get("people").unwrap();
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), w.delta_snapshot());
        let ast = parser::parse(q).unwrap();
        let r = Engine::new(&view, &cache).run(&ast).unwrap();
        r
    };

    // Delete a base node with no incident edges (Carol — the only base edge is Alice→Bob),
    // then flush the tombstone into a segment.
    write(&graphs, "MATCH (n:Person {name:'Carol'}) DETACH DELETE n");
    graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("the delete flushes");
    assert!(graphs.writer("people").unwrap().snapshot().is_empty());
    let n0 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
    assert!(
        matches!(n0.rows[0][0], Val::Int(2)),
        "Carol gone: 2 people left"
    );
    let gone = q(&graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age");
    assert_eq!(
        gone.rows.len(),
        0,
        "Carol resolves to nothing after the delete flush"
    );

    // MERGE Carol: resolve returns Absent (the segment removals suppress her base entry),
    // so she is reborn as a fresh born node — count climbs back to 3.
    write(&graphs, "MERGE (n:Person {name:'Carol'}) SET n.age = 41");
    let n1 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
    assert!(
        matches!(n1.rows[0][0], Val::Int(3)),
        "Carol reborn: 3 people"
    );
    let carol = q(&graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age");
    assert_eq!(carol.rows.len(), 1, "exactly one (reborn) Carol");
    assert!(
        matches!(carol.rows[0][0], Val::Int(41)),
        "reborn Carol's age"
    );

    // A second MERGE is idempotent — the born Carol resolves through the memtable, not the
    // stack (which still says Absent), so no fourth node appears.
    write(&graphs, "MERGE (n:Person {name:'Carol'}) SET n.age = 42");
    let n2 = q(&graphs, "MATCH (n:Person) RETURN count(*)");
    assert!(
        matches!(n2.rows[0][0], Val::Int(3)),
        "re-MERGE idempotent: still 3"
    );
    let carol2 = q(&graphs, "MATCH (n:Person {name:'Carol'}) RETURN n.age");
    assert_eq!(carol2.rows.len(), 1, "still one Carol");
    assert!(
        matches!(carol2.rows[0][0], Val::Int(42)),
        "the born Carol re-patched"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 4 slice 4.4-b: **encryption parity**. When the served core is encrypted at rest,
/// a flush must write an encrypted segment — the writer derives a fresh per-segment cipher
/// and KDF header, stamps `manifest.encryption`, and seals the MAC. The segment reopens
/// (MAC-verified, sections decrypted) *with* the key and its born data reads back through
/// an empty delta; reopening the same data directory *without* the key is refused.
#[test]
fn flush_to_segment_encrypts_the_segment_under_a_master_key() {
    let key: &[u8] = b"an-at-rest-master-key-32byteslong";
    let (root, _g) = testgen::write_indexed_people_keyed("flush_seg_keyed_e2e", Some(key));
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let mut graphs = Graphs::open_all(&root, Some(key)).unwrap();
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
    write(
        &graphs,
        "MERGE (a:Person {name:'Dave'})-[:KNOWS]->(b:Person {name:'Alice'})",
    );

    let set_uuid = graphs
        .flush_graph_to_segment("people", &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes");

    // The new segment carries its own encryption header (salt only) — proof the flush
    // wrote ciphertext, not plaintext beside the encrypted core.
    let gen1 = graphs.get("people").unwrap();
    assert_eq!(gen1.base_uuid(), base_uuid, "base preserved by the flush");
    let seg = &gen1.stack().segments()[0];
    let header = seg
        .manifest
        .encryption
        .as_ref()
        .expect("flushed segment manifest carries an encryption header");
    assert_eq!(header.aead, graph_format::crypto::AEAD_NAME);
    assert!(
        seg.manifest.mac.is_some(),
        "flushed segment manifest is MAC-sealed"
    );

    // Read back with an empty delta (still keyed): the born, encrypted node decrypts.
    let dave = {
        let w = graphs.writer("people").unwrap();
        let view = MergedView::new(gen1.as_ref(), w.delta_snapshot());
        let ast = parser::parse("MATCH (n:Person {name:'Dave'}) RETURN n.age").unwrap();
        let r = Engine::new(&view, &cache).run(&ast).unwrap();
        r
    };
    assert!(
        matches!(dave.rows[0][0], Val::Int(50)),
        "Dave decrypts from the keyed segment: {:?}",
        dave.rows[0][0]
    );
    drop(gen1);

    // Reopen the whole data dir WITH the key — set + encrypted segment reload and verify.
    drop(graphs);
    let graphs = Graphs::open_all(&root, Some(key)).unwrap();
    let gen2 = graphs.get("people").unwrap();
    assert_eq!(gen2.uuid(), set_uuid, "reopen names the flushed set");
    let view = MergedView::new(gen2.as_ref(), DeltaSnapshot::empty());
    let ast = parser::parse("MATCH (a:Person {name:'Dave'})-[:KNOWS]->(b) RETURN b.name").unwrap();
    let knows = Engine::new(&view, &cache).run(&ast).unwrap();
    assert!(
        matches!(&knows.rows[0][0], Val::Str(s) if s == "Alice"),
        "the born encrypted edge traverses after reopen: {:?}",
        knows.rows.first()
    );
    drop(gen2);
    drop(graphs);

    // Reopen WITHOUT the key — the encrypted base + segment are refused (no plaintext leak).
    assert!(
        Graphs::open_all(&root, None).is_err(),
        "an encrypted data dir must not open without the key"
    );

    std::fs::remove_dir_all(&root).ok();
}
