// SPDX-License-Identifier: Apache-2.0
//! `consolidation` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// Phase 4c-B end-to-end through the query overlay: a MERGE-born node and a core
/// property patch, once flushed to an L0 level, still read back through the full
/// `MergedView` (label scan **and** index seek), a re-MERGE of the flushed born node
/// reuses its synthetic id (no duplicate), and everything survives a reopen (the L0
/// file reloads, the WAL-tail re-MERGE re-resolves against it).
#[test]
fn flush_to_l0_overlay_reads_and_born_reuse_survive_reopen() {
    let (root, _g) = testgen::write_indexed_people("flush_overlay_e2e");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);

    // A query over the writer's full published delta (active memtable ⊕ L0 levels).
    let names_ages = |graphs: &Graphs, q: &str| -> Vec<(String, Option<i64>)> {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
        let ast = parser::parse(q).unwrap();
        let mut out: Vec<(String, Option<i64>)> = Engine::new(&view, &cache)
            .run(&ast)
            .unwrap()
            .rows
            .iter()
            .map(|r| {
                let name = match &r[0] {
                    Val::Str(s) => s.clone(),
                    v => panic!("name not str: {v:?}"),
                };
                let age = match r.get(1) {
                    Some(Val::Int(n)) => Some(*n),
                    _ => None,
                };
                (name, age)
            })
            .collect();
        out.sort();
        out
    };
    let write = |graphs: &Graphs, q: &str| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
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

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // MERGE-create Dave (born) and patch a core node (Alice.age = 99).
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");

    // Flush the memtable to an L0 level — the active memtable is now empty.
    let writer = graphs.writer("people").unwrap();
    assert!(writer.flush_to_l0().unwrap());
    assert_eq!(writer.l0_len(), 1);
    assert!(
        writer.snapshot().is_empty(),
        "active memtable freed by flush"
    );
    assert_eq!(l0_count(&writer.wal_dir()), 1, "one L0 file on disk");

    // Read back through the L0 level: index seek finds Dave, label scan lists him,
    // Alice's patched age is served.
    assert_eq!(
        names_ages(
            &graphs,
            "MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age"
        ),
        vec![("Dave".to_string(), Some(50))],
        "index seek finds the flushed born node"
    );
    assert_eq!(
        names_ages(
            &graphs,
            "MATCH (n:Person {name:'Alice'}) RETURN n.name, n.age"
        ),
        vec![("Alice".to_string(), Some(99))],
        "the flushed core patch is served"
    );
    let all = names_ages(&graphs, "MATCH (n:Person) RETURN n.name");
    assert!(
        all.iter().any(|(n, _)| n == "Dave"),
        "label scan lists the flushed born node: {all:?}"
    );

    // Re-MERGE the flushed born Dave (post-flush, into the active memtable). It must
    // reuse the L0 synthetic id — no duplicate — and the newer age wins.
    write(&graphs, "MERGE (n:Person {name:'Dave'}) SET n.age = 55");
    assert_eq!(
        writer.delta_snapshot().born_count(),
        1,
        "re-MERGE reuses the flushed born id, no duplicate"
    );
    assert_eq!(
        names_ages(
            &graphs,
            "MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age"
        ),
        vec![("Dave".to_string(), Some(55))],
        "the re-MERGE patch (active memtable) wins over the flushed value"
    );

    // Reopen: the L0 file reloads and the WAL-tail re-MERGE re-resolves against it.
    drop(writer);
    drop(graphs);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    assert_eq!(
        graphs.writer("people").unwrap().l0_len(),
        1,
        "reopen reloads L0"
    );
    assert_eq!(
        graphs
            .writer("people")
            .unwrap()
            .delta_snapshot()
            .born_count(),
        1,
        "reopen does not duplicate the born node"
    );
    assert_eq!(
        names_ages(
            &graphs,
            "MATCH (n:Person {name:'Dave'}) RETURN n.name, n.age"
        ),
        vec![("Dave".to_string(), Some(55))],
        "Dave (age 55) survives the reopen via the L0 file + WAL tail"
    );
    assert_eq!(
        names_ages(
            &graphs,
            "MATCH (n:Person {name:'Alice'}) RETURN n.name, n.age"
        ),
        vec![("Alice".to_string(), Some(99))],
        "Alice's flushed patch survives the reopen"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// Phase 4c-B: consolidation folds a flushed L0 level. A born node lives in an L0
/// segment (not the active memtable); the consolidation dump must still carry it
/// (proving `frozen.l0` reached the merged view), and `retire` deletes the L0 file
/// and clears the level stack.
#[test]
fn consolidation_folds_a_flushed_l0_level() {
    let (root, _graph) = testgen::write_indexed_people("consolidate_l0");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let gen0 = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let wal_dir = writer.wal_dir();

    // MERGE-born Dave + a core patch, then flush both into an L0 level.
    for q in [
        "MERGE (n:Person {name:'Dave'}) SET n.age = 50",
        "MATCH (n:Person {name:'Alice'}) SET n.age = 99",
    ] {
        let stmt = match parser::parse_statement(q).unwrap() {
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
    assert!(writer.flush_to_l0().unwrap());
    assert_eq!(writer.l0_len(), 1);
    assert!(writer.snapshot().is_empty(), "everything flushed to L0");
    assert_eq!(l0_count(&wal_dir), 1);

    // The injected builder proves the dump folded the L0 level (Dave's MERGE + the
    // merged Alice age), then publishes a canned consolidated generation.
    let new_uuid = uuid::Uuid::from_u128(0x4c0b_0000_0000_0000_0000_0000_0000_0001);
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let build =
        |dump: &Path, g: &str, dd: &Path, _key: Option<&[u8]>, _acl: Option<&Path>| -> Result<()> {
            let nodes = dump_nodes(dump);
            assert!(
                nodes.contains_key("Dave"),
                "the flushed born node must be in the dump: {:?}",
                nodes.keys().collect::<Vec<_>>()
            );
            assert_eq!(
                dump_age(dump, "Alice"),
                Some(99),
                "the flushed core patch must be in the dump"
            );
            assert_eq!(g, "people");
            testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
            Ok(())
        };
    let published = graphs
        .consolidate_graph("people", &cache, &vc, &root, None, build)
        .unwrap();
    assert_eq!(published.0, new_uuid);

    // Retire folded + deleted the L0 level: no level stack, no L0 file.
    let writer = graphs.writer("people").unwrap();
    assert_eq!(writer.l0_len(), 0, "L0 stack cleared by retire");
    assert_eq!(l0_count(&wal_dir), 0, "L0 file deleted by retire");
    assert_no_consolidate_scratch(&root, "people");

    std::fs::remove_dir_all(&root).ok();
}

/// A consolidation whose rebuild fails (modelled as the builder erroring before
/// it publishes anything — the crash window between freeze and the `current`
/// swap) is non-destructive: the old core keeps serving, the delta stays live,
/// and the durable write replays on a fresh reopen (the freeze sealed but did not
/// delete its segments).
#[test]
fn failed_consolidation_preserves_the_write_and_old_core() {
    let (root, _graph) = testgen::write_indexed_people("consolidate_crash");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let gen0 = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let wal_dir = writer.wal_dir();
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

    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let build =
        |_d: &Path, _g: &str, _dd: &Path, _k: Option<&[u8]>, _acl: Option<&Path>| -> Result<()> {
            bail!("simulated builder crash")
        };
    let err = graphs
        .consolidate_graph("people", &cache, &vc, &root, None, build)
        .unwrap_err();
    assert!(format!("{err:#}").contains("simulated builder crash"));

    // Old core still served (unchanged uuid); delta still live (age 99 overlaid);
    // the scratch dump is cleaned up.
    let gen_after = graphs.get("people").unwrap();
    assert_eq!(gen_after.uuid(), gen0.uuid(), "old core keeps serving");
    assert!(
        !writer.snapshot().is_empty(),
        "delta not retired on failure"
    );
    assert_eq!(
        writer.snapshot().node_patch(0).unwrap().patches.get("age"),
        Some(&Value::Int(99))
    );
    assert_no_consolidate_scratch(&root, "people");

    // Durability: a fresh writer over the WAL replays the write.
    let reopened = DeltaWriter::open(
        &wal_dir,
        "people",
        gen0.uuid(),
        gen0.node_count(),
        gen0.edge_count(),
        None,
        |op| resolve_op(&gen0, op),
    )
    .unwrap();
    assert_eq!(
        reopened
            .snapshot()
            .node_patch(0)
            .unwrap()
            .patches
            .get("age"),
        Some(&Value::Int(99)),
        "the write survives a failed consolidation + reopen"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// A `people` graph with the writable layer on and Alice's age patched to 99 in the
/// delta — the fixture the two guard-race regressions below both consolidate from.
/// Returns the root, the `Graphs`, the pre-consolidation generation and its writer.
fn consolidation_race_fixture(tag: &str) -> (PathBuf, Graphs, Arc<Generation>, Arc<DeltaWriter>) {
    let (root, _graph) = testgen::write_indexed_people(tag);
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen0 = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
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
    assert!(!writer.snapshot().is_empty(), "the delta carries the write");
    (root, graphs, gen0, writer)
}

/// The background generation guard polls the very same `current` pointer a
/// consolidation publishes, so a poll can land **inside** the publish window and swap
/// the freshly built generation in before the consolidation's own swap reaches it.
///
/// Cleanup ownership must not hinge on who won that race. Only the consolidation can
/// retire the delta it just folded into the new core — the guard does not even know a
/// delta exists — so the consolidation must do its retire whether it performed the
/// swap or merely found it already performed.
///
/// The interleaving is forced deterministically (no threads, no sleeps) through the
/// `build` seam, which `consolidate_graph` invokes at exactly the instant the builder
/// publishes `current`: the injected builder publishes the new generation and then
/// runs *the guard's own swap* (`guard_swap` — the body `guard_sweep` executes), so
/// the served slot already carries the new generation when the consolidation gets
/// there.
///
/// Before the fix the consolidation's swap returned `Ok(None)` here and the op failed
/// with "did not publish a new generation" **despite a successful build** — the delta
/// was never retired and stayed bound to the old core, which wedges every subsequent
/// consolidation forever (`core_uuid() != core.uuid()` ⇒ "the delta is orphaned").
#[test]
fn consolidation_retires_the_delta_when_the_guard_wins_the_swap() {
    let (root, graphs, gen0, writer) = consolidation_race_fixture("consolidate_guard_race");
    let wal_dir = writer.wal_dir();
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let new_uuid = uuid::Uuid::from_u128(0x_8900_0000_0000_0000_0000_0000_0000_0001);
    let build =
        |_d: &Path, _g: &str, dd: &Path, _key: Option<&[u8]>, _acl: Option<&Path>| -> Result<()> {
            testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
            // The guard's poll lands here — after the builder published `current`, before
            // the consolidation swaps the served slot onto it — and wins the swap.
            let swapped = guard_swap(&graphs, "people", &vc).unwrap();
            assert_eq!(
                swapped.map(|g| g.0),
                Some(new_uuid),
                "the guard swapped the consolidation's generation in first"
            );
            Ok(())
        };
    let published = graphs
        .consolidate_graph("people", &cache, &vc, &root, None, build)
        .unwrap();

    // A successful build is reported as one, not as a false failure.
    assert_eq!(published.0, new_uuid, "the built generation is reported");
    assert_eq!(graphs.get("people").unwrap().uuid().0, new_uuid);

    // …and the cleanup the losing swap used to skip actually ran: the writer is
    // re-bound to the new core (not orphaned on the old one), the folded delta is
    // gone, and the consumed WAL segment was dropped (only freeze's fresh, empty
    // segment remains).
    assert_eq!(
        writer.core_uuid().0,
        new_uuid,
        "retire re-bound the writer to the new core — the delta is not orphaned"
    );
    assert_ne!(writer.core_uuid(), gen0.uuid());
    assert!(
        writer.snapshot().is_empty(),
        "retire dropped the folded delta"
    );
    assert_eq!(
        wal_count(&wal_dir),
        1,
        "the consumed WAL segment was retired"
    );
    assert!(!writer.is_consolidating(), "the claim was released");

    // The write is served from the new core, with nothing left overlaying it.
    let gen1 = graphs.get("people").unwrap();
    let view = MergedView::new(gen1.as_ref(), writer.delta_snapshot());
    let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap();
    let age = Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0].clone();
    assert!(
        matches!(age, Val::Int(99)),
        "folded write served from the core"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// The other half of the fix: the guard does not *take* the swap in the first place.
/// A graph with a consolidation/flush/compaction in flight publishes its own
/// `current` and owns the swap that follows, so the guard leaves it alone — under
/// `swap` (it must not steal the swap) and under `exit` (it must not tear the process
/// down over the server's own publish, which is what it did before).
///
/// Same deterministic seam: the real `guard_sweep` runs *inside* the publish window.
#[test]
fn guard_sweep_defers_to_an_in_flight_consolidation() {
    let (root, graphs, gen0, writer) = consolidation_race_fixture("consolidate_guard_defer");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    let new_uuid = uuid::Uuid::from_u128(0x_8900_0000_0000_0000_0000_0000_0000_0002);
    let gen0_for_build = gen0.clone();
    let build =
        |_d: &Path, _g: &str, dd: &Path, _key: Option<&[u8]>, _acl: Option<&Path>| -> Result<()> {
            testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
            // `current` has moved, and the consolidation has not yet swapped. A guard poll
            // landing here must defer on both strategies.
            assert!(matches!(
                guard_sweep(&graphs, &vc, ReloadStrategy::Swap, None),
                SweepAction::Continue
            ));
            assert_eq!(
                graphs.get("people").unwrap().uuid(),
                gen0_for_build.uuid(),
                "the guard left the swap to the in-flight consolidation"
            );
            assert!(
                matches!(
                    guard_sweep(&graphs, &vc, ReloadStrategy::Exit, None),
                    SweepAction::Continue
                ),
                "reloadStrategy=exit must not shut the process down over our own publish"
            );
            Ok(())
        };
    let published = graphs
        .consolidate_graph("people", &cache, &vc, &root, None, build)
        .unwrap();

    // The consolidation performed its own swap and retired the delta.
    assert_eq!(published.0, new_uuid);
    assert_eq!(graphs.get("people").unwrap().uuid().0, new_uuid);
    assert_eq!(
        writer.core_uuid().0,
        new_uuid,
        "writer re-bound to the new core"
    );
    assert!(writer.snapshot().is_empty(), "delta retired");

    // With the claim released, the guard is back on duty for this graph: a *foreign*
    // generation (an external rebuild) is still swapped in as before.
    assert!(!writer.is_consolidating());
    let foreign = publish_copy_as_new_generation(&root, "people", None);
    assert!(matches!(
        guard_sweep(&graphs, &vc, ReloadStrategy::Swap, None),
        SweepAction::Continue
    ));
    assert_eq!(
        graphs.get("people").unwrap().uuid().0,
        foreign,
        "the guard still swaps generations published behind the server's back"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// The same end-to-end consolidation, but with **non-default [`BuilderLimits`]** — the
/// invocation a server running inside a memory- or CPU-capped container actually makes.
///
/// Every other real-builder test passes `BuilderLimits::default()`, i.e. no
/// `--max-memory`, no `--threads`, no timeout. That leaves the flagged invocation
/// completely uncovered, and "two configurations with no test in common" is exactly how
/// HIK-145 and HIK-157 stayed invisible. What this pins down is that the real binary
/// *accepts* what the server sends: `--max-memory` as a **bare byte count** (its
/// `parse_size` treats a suffix-less number as bytes) and `--threads` as an integer. Get
/// either wrong and consolidation fails on every capped deployment while every existing
/// test still passes.
///
/// Ignored by default and run in CI for the same reason as its sibling below.
#[test]
#[ignore = "spawns the real slater-build binary; see the doc comment"]
fn a_production_consolidation_honours_non_default_builder_limits() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let (root, _graph) = testgen::write_indexed_people("consolidate_limits");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen0 = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
    let stmt =
        match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 42").unwrap() {
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

    // A budget at the derived floor, an explicit thread count, and a timeout generous
    // enough that a healthy build never trips it but a wedged one does not hang CI.
    let limits = BuilderLimits {
        max_memory_bytes: MIN_BUILDER_MEMORY,
        threads: 2,
        timeout_secs: 600,
    };
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let new = graphs
        .consolidate_graph("people", &cache, &vc, &root, None, |d, g, dd, key, _acl| {
            run_builder(&bin, d, g, dd, key, limits, None, None)
        })
        .expect("the real builder must accept the flags the server sends");
    assert_ne!(new.0, gen0.uuid().0, "rebuilt a new generation");

    let gen1 = graphs.get("people").unwrap();
    let view = MergedView::new(
        gen1.as_ref(),
        DeltaSnapshot::from_memtable(writer.snapshot()),
    );
    let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age").unwrap();
    let age = Engine::new(&view, &cache).run(&ast).unwrap().rows[0][0].clone();
    assert!(
        matches!(age, Val::Int(42)),
        "the flag-limited build folded the delta into the core, got {age:?}"
    );
    assert_no_consolidate_scratch(&root, "people");
    std::fs::remove_dir_all(&root).ok();
}

/// True end-to-end consolidation through the real `slater-build` binary. Ignored
/// by default — `cargo test -p slater` does not build the builder. Run it with
/// the binary located via `SLATER_BUILD_BIN` (or on `PATH`):
/// ```text
/// cargo build -p slater-build
/// SLATER_BUILD_BIN=$CARGO_TARGET_DIR/debug/slater-build \
///   cargo test -p slater -- --ignored consolidate_via_real_builder
/// ```
#[test]
#[ignore = "spawns the real slater-build binary; see the doc comment"]
fn consolidate_via_real_builder() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let (root, _graph) = testgen::write_indexed_people("consolidate_real");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let gen0 = graphs.get("people").unwrap();
    let writer = graphs.writer("people").unwrap();
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

    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    // A post-freeze write (Bob's age → 77) applied while the real builder runs must
    // be carried forward onto the new core by retire (Phase 4a).
    let writer_mid = writer.clone();
    let gen_mid = gen0.clone();
    let new = graphs
        .consolidate_graph(
            "people",
            &cache,
            &vc,
            &root,
            None,
            |d, g, dd, _key, _acl| {
                let bob =
                    match parser::parse_statement("MATCH (n:Person {name:'Bob'}) SET n.age = 77")
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
                run_builder(&bin, d, g, dd, _key, BuilderLimits::default(), None, None)
            },
        )
        .unwrap();
    assert_ne!(new.0, gen0.uuid().0, "rebuilt a new generation");

    let gen1 = graphs.get("people").unwrap();
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
        "the real builder folded the delta into the core"
    );
    assert!(
        matches!(read_age("Bob"), Val::Int(77)),
        "the post-freeze write survived on the carried-forward delta"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **The vector regression gate.** A consolidation used to destroy the core's own
/// embeddings and vector indexes — silently, exit 0, no warning — and *any* write was
/// enough to get you there: the dump only reads the column store, but an indexed
/// embedding is routed *out* of it (D12), so the dumper never saw one; the dump format
/// had nowhere to put one; and the builder hard-zeroed `vector_stmts` on the dump path.
/// So this fixture's `SET n.age` — which has nothing to do with embeddings — was enough
/// to lose every vector in the graph.
///
/// Asserts the whole round trip: the index declaration survives, and KNN returns the
/// *same* neighbours with the *same* scores across the rebuild.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn consolidate_carries_vector_indexes_and_embeddings() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let (root, graph, _) = testgen::write_basic("consolidate_vectors");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    // Carol's own embedding as the query, so the ranking is unambiguous.
    let knn = |gen: &Generation| -> Vec<(i64, String)> {
        let view = MergedView::read_only(gen);
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.9, 0.8, 0.7])) \
                 YIELD node, score RETURN id(node) AS id, score",
        )
        .unwrap();
        let res = Engine::new(&view, &cache).run(&ast).unwrap();
        res.rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                // Scores are compared as text at full precision: the rebuild legitimately
                // re-encodes the vectors, so pin the contract that is actually promised —
                // same order, same scores to the last digit shown.
                (Val::Int(id), Val::Float(s)) => (*id, format!("{s:.9}")),
                other => panic!("unexpected KNN row {other:?}"),
            })
            .collect()
    };

    let gen0 = graphs.get(&graph).unwrap();
    assert_eq!(
        gen0.manifest().vector_indexes.len(),
        1,
        "fixture must start with a vector index"
    );
    let before = knn(gen0.as_ref());
    assert_eq!(before.len(), 3, "all three Person embeddings are indexed");

    // A write that has nothing whatever to do with the embeddings.
    let writer = graphs.writer(&graph).unwrap();
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

    graphs
        .consolidate_graph(&graph, &cache, &vc, &root, None, |d, g, dd, _key, _acl| {
            run_builder(&bin, d, g, dd, _key, BuilderLimits::default(), None, None)
        })
        .unwrap();

    let gen1 = graphs.get(&graph).unwrap();
    assert_eq!(
        gen1.manifest().vector_indexes.len(),
        1,
        "the vector index must survive consolidation (this is the bug: it used to vanish)"
    );
    assert_eq!(
        knn(gen1.as_ref()),
        before,
        "KNN must return identical neighbours and scores across a consolidation"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The delta arm of the same gate. Now that an embedding can be *written*, the dump
/// must carry the levels above the base too: a node re-embedded since the build has a
/// stale vector in the sealed base index, and reading only the base would rebuild the
/// graph around the old embedding — silently, since the index itself survives and the
/// count is unchanged. The overlay must win.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn consolidate_carries_a_delta_written_vector_over_the_base() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let (root, graph, _) = testgen::write_basic("consolidate_delta_vector");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);

    // Re-embed Alice (0) to a vector far from her original, and query with the *new*
    // one. If the rebuild kept her stale base embedding she will not lead.
    let newvec = [0.0f32, 0.0, 1.0];
    let writer = graphs.writer(&graph).unwrap();
    let gen0 = graphs.get(&graph).unwrap();
    let stmt = match parser::parse_statement(
        "MATCH (n:Person {name:'Alice'}) SET n.embedding = vecf32([0.0, 0.0, 1.0])",
    )
    .unwrap()
    {
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
    drop(gen0);

    graphs
        .consolidate_graph(&graph, &cache, &vc, &root, None, |d, g, dd, _key, _acl| {
            run_builder(&bin, d, g, dd, _key, BuilderLimits::default(), None, None)
        })
        .unwrap();

    let gen1 = graphs.get(&graph).unwrap();
    assert_eq!(
        gen1.manifest().vector_indexes.len(),
        1,
        "the vector index must survive the consolidation"
    );
    let view = MergedView::read_only(gen1.as_ref());
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.0, 0.0, 1.0])) \
             YIELD node, score RETURN id(node) AS id, score",
    )
    .unwrap();
    let res = Engine::new(&view, &cache).run(&ast).unwrap();
    let (id, score) = match (&res.rows[0][0], &res.rows[0][1]) {
        (Val::Int(i), Val::Float(s)) => (*i, *s),
        other => panic!("unexpected KNN row {other:?}"),
    };
    // Alice is now the *exact* match for the query, so she must lead at distance ~0.
    assert_eq!(
        id, 0,
        "the delta-written embedding must have been carried into the rebuild — Alice is \
             the exact match for her own new vector; a stale base vector would not lead"
    );
    assert!(
        score.abs() < 1e-6,
        "the exact match scores ~0 (cosine distance to itself), got {score}"
    );
    // And the new vector is what was stored, not the old one.
    assert_eq!(
        gen1.manifest().vector_indexes[0].dim as usize,
        newvec.len(),
        "dim is unchanged by the re-embed"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// Read `n.embedding` for a `:Doc` fixture node through the *column* path (D12 applies:
/// an in-scope node's embedding reads back `Null`, an out-of-scope node's reads verbatim).
/// `None` is `Null`.
fn vread_embedding(
    graphs: &Graphs,
    graph: &str,
    cache: &BlockCache,
    name: &str,
) -> Option<Vec<f32>> {
    let gen = graphs.get(graph).unwrap();
    let snap = DeltaSnapshot::from_memtable(graphs.writer(graph).unwrap().snapshot());
    let view = MergedView::new(gen.as_ref(), snap);
    let ast = parser::parse(&format!(
        "MATCH (n:Key {{name:'{name}'}}) RETURN n.embedding AS e"
    ))
    .unwrap();
    let res = Engine::new(&view, cache).run(&ast).unwrap();
    assert_eq!(res.rows.len(), 1, "the fixture node must still exist");
    match &res.rows[0][0] {
        Val::Null => None,
        Val::Vector(v) => Some(v.clone()),
        other => panic!("unexpected n.embedding {other:?}"),
    }
}

/// **HIK-122.** A label removal is *conditional* suppression, not a delete: HIK-118 makes
/// the KNN path promise that a later `SET n:Doc` puts the node back in scope and re-scores
/// its vector. A consolidation running while the node is out of scope must keep that
/// promise. It used not to — and only a consolidation could show it, so the loss was
/// timing-dependent.
///
/// The exact review repro: re-embed → `REMOVE n:Doc` → **consolidate** → `SET n:Doc`.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn a_consolidation_while_out_of_scope_keeps_a_relabelled_nodes_embedding() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    // d00 starts far from the query (0.9); the write below moves it to an exact match (0.0),
    // so a stale base vector could never be mistaken for the carried one.
    let base: Vec<Vec<f32>> = [0.9, 0.3, 0.55].iter().map(|d| at_distance(*d)).collect();
    // The business key rides a *second* label, so the node can leave the vector index's
    // scope (`:Doc`) and still be addressable by a write (`:Key`).
    let (root, graph) = testgen::write_vector_docs_keyed("hik122_consolidate", &base, "Key");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let v1 = at_distance(0.0);

    // 1. Re-embed d00, flushed into its own segment (sidecar `ids=[0]`). Anchored on the
    //    business-key label, which is where the `name` range index lives.
    let mut params = HashMap::new();
    params.insert(
        "v".to_string(),
        Val::List(v1.iter().map(|x| Val::Float(*x as f64)).collect()),
    );
    vwrite_params(
        &graphs,
        &graph,
        "MATCH (n:Key {name:'d00'}) SET n.embedding = vecf32($v)",
        &params,
    );
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the re-embed flushes into a segment");

    // 2. Take d00 out of the index's scope, flushed into a second segment (sidecar
    //    `label_removals=[0]`).
    vwrite(&graphs, &graph, "MATCH (n:Key {name:'d00'}) REMOVE n:Doc");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the label removal flushes into a segment");
    assert!(
        !vknn(&graphs, &graph, &cache, &VQ, 3)
            .iter()
            .any(|(id, _)| *id == 0),
        "out of scope: d00 must not be returned by KNN while it lacks :Doc"
    );

    // 3. A background consolidation, run while d00 is out of scope.
    graphs
        .consolidate_graph(&graph, &cache, &vc, &root, None, |d, g, dd, _key, _acl| {
            run_builder(&bin, d, g, dd, _key, BuilderLimits::default(), None, None)
        })
        .unwrap();
    assert_eq!(
        graphs.get(&graph).unwrap().manifest().vector_indexes.len(),
        1,
        "the vector index must survive the consolidation"
    );
    // Out of scope, the embedding is a plain column value and reads back verbatim — this is
    // the canonical out-of-scope representation a fresh build would also produce, and the
    // proof the rebuild did not simply throw the vector away.
    assert_eq!(
        vread_embedding(&graphs, &graph, &cache, "d00"),
        Some(v1.clone()),
        "the consolidation must carry the out-of-scope node's embedding into the new \
             generation, not delete it"
    );

    // 4. Put d00 back in scope. HIK-118's promise: its vector scores again.
    vwrite(&graphs, &graph, "MATCH (n:Key {name:'d00'}) SET n:Doc");

    let got = vknn(&graphs, &graph, &cache, &VQ, 3);
    let d00 = got.iter().find(|(id, _)| *id == 0).unwrap_or_else(|| {
        panic!(
            "HIK-122: `SET n:Doc` must put d00 back in the index with the embedding it had \
                 before the consolidation — the consolidation destroyed it; got {got:?}"
        )
    });
    assert!(
        d00.1.abs() < 1e-5,
        "d00 must score its re-embedded vector (an exact match, ~0), not the base's stale \
             0.9; got {}",
        d00.1
    );
    // And back in scope the column read is suppressed again (D12), so the vector is served
    // by exactly one arm — the index — not two.
    assert_eq!(
        vread_embedding(&graphs, &graph, &cache, "d00"),
        None,
        "back in scope, D12 suppresses the column read: the KNN path serves the embedding"
    );

    // The fold says `Set(v)` for d00 now, from a *column* value no delta patch names. A
    // flush must carry that across: the sidecar is what decides whether the fold's candidate
    // set ever sees the node, so a flush that does not name it would silently undo the
    // re-label — KNN-visible before, gone after, with nothing in between to blame.
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the re-label flushes into a segment");
    let after_flush = vknn(&graphs, &graph, &cache, &VQ, 3);
    assert!(
        after_flush.iter().any(|(id, s)| *id == 0 && s.abs() < 1e-5),
        "a flush must not lose the re-labelled node's embedding — the fold resolved it to \
             the column vector, so the sidecar has to name it too; got {after_flush:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// One write can leave the index's scope **and** delete the embedding —
/// `SET n = {…} REMOVE n:Doc`. The de-labelling says "retain, the value is untouched"; the
/// replace says "the value is gone". The deletion is the stronger fact, and mixing them up
/// is silent in the dangerous direction: the consolidation would rescue the vector the user
/// just threw away back into the column store, where `RETURN n.embedding` hands it out again.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn a_value_removal_that_also_leaves_scope_stays_deleted_across_a_consolidation() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let base: Vec<Vec<f32>> = [0.0, 0.3, 0.55].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("hik122_gone_and_out", &base, "Key");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // Delete d00's embedding *and* take it out of scope, in one delta.
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Key {name:'d00'}) REMOVE n.embedding",
    );
    vwrite(&graphs, &graph, "MATCH (n:Key {name:'d00'}) REMOVE n:Doc");

    graphs
        .consolidate_graph(&graph, &cache, &vc, &root, None, |d, g, dd, _key, _acl| {
            run_builder(&bin, d, g, dd, _key, BuilderLimits::default(), None, None)
        })
        .unwrap();

    assert_eq!(
        vread_embedding(&graphs, &graph, &cache, "d00"),
        None,
        "the embedding was deleted: the consolidation must not resurrect it into the \
             column store just because the node also left the index's scope"
    );
    // And it stays gone once the node is back in scope.
    vwrite(&graphs, &graph, "MATCH (n:Key {name:'d00'}) SET n:Doc");
    let got = vknn(&graphs, &graph, &cache, &VQ, 3);
    assert!(
        !got.iter().any(|(id, _)| *id == 0),
        "d00's embedding was deleted; re-labelling must not bring it back — a deletion is \
             not a scope change; got {got:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The **base-index** arm of HIK-122, and the harder one: nothing re-embeds d00, so its
/// only copy is the one D12 routed *out* of the column store into the sealed base index.
/// `REMOVE n:Doc` takes it out of scope; the fold supersedes its base entry; and the
/// consolidation's property walk cannot rescue it, because the props record never held it.
/// Every copy is then gone — this arm really does destroy the vector, not merely hide it.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn a_consolidation_while_out_of_scope_keeps_a_base_indexed_embedding() {
    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let base: Vec<Vec<f32>> = [0.0, 0.3, 0.55].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("hik122_base_index", &base, "Key");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // d00 leads on the base index and nothing ever re-embeds it.
    assert_eq!(
        vknn(&graphs, &graph, &cache, &VQ, 1)[0].0,
        0,
        "d00 is the exact match on the base index"
    );

    // Out of scope, flushed to a segment (sidecar `label_removals=[0]`).
    vwrite(&graphs, &graph, "MATCH (n:Key {name:'d00'}) REMOVE n:Doc");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the label removal flushes into a segment");

    // A consolidation while out of scope.
    graphs
        .consolidate_graph(&graph, &cache, &vc, &root, None, |d, g, dd, _key, _acl| {
            run_builder(&bin, d, g, dd, _key, BuilderLimits::default(), None, None)
        })
        .unwrap();

    // Back in scope. HIK-118: "a later `SET n:Doc` must be able to un-suppress this id and
    // score its base vector again" — the consolidation must not have made that a lie.
    vwrite(&graphs, &graph, "MATCH (n:Key {name:'d00'}) SET n:Doc");
    let got = vknn(&graphs, &graph, &cache, &VQ, 3);
    let d00 = got.iter().find(|(id, _)| *id == 0).unwrap_or_else(|| {
        panic!(
            "HIK-122: the consolidation destroyed d00's base-index embedding — its only \
                 copy — so `SET n:Doc` can never bring it back; got {got:?}"
        )
    });
    assert!(
        d00.1.abs() < 1e-5,
        "d00 must score its original base vector (an exact match, ~0); got {}",
        d00.1
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A wrong-width embedding must be refused at the write. Both KNN arms hard-error on a
/// dim mismatch, and a bad row would otherwise ride the flush into a segment and the
/// rebuild into the next generation before anyone noticed.
#[test]
fn a_write_rejects_an_embedding_of_the_wrong_dimension() {
    let (root, graph, _) = testgen::write_basic("write_bad_vector_dim");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let writer = graphs.writer(&graph).unwrap();
    let gen = graphs.get(&graph).unwrap();

    // The fixture's index on (:Person {embedding}) is 3-dimensional.
    let stmt = match parser::parse_statement(
        "MATCH (n:Person {name:'Alice'}) SET n.embedding = vecf32([1.0, 2.0])",
    )
    .unwrap()
    {
        parser::ast::Statement::Write(w) => w,
        _ => unreachable!(),
    };
    let e = execute_write(
        &writer,
        gen.as_ref(),
        &stmt,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .expect_err("a 2-dim value on a 3-dim index must be refused");
    let msg = format!("{e:?}");
    assert!(
        msg.contains("3-dimensional") && msg.contains("2 dimensions"),
        "the error should name both widths, got: {msg}"
    );

    // An *unindexed* vector property is unconstrained — the core admits any width.
    let ok = match parser::parse_statement(
        "MATCH (n:Person {name:'Alice'}) SET n.shadow = vecf32([1.0, 2.0])",
    )
    .unwrap()
    {
        parser::ast::Statement::Write(w) => w,
        _ => unreachable!(),
    };
    execute_write(
        &writer,
        gen.as_ref(),
        &ok,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .expect("an unindexed vector property carries no dimension contract");
    std::fs::remove_dir_all(&root).ok();
}

/// HIK-145: the **encrypted** arm of carry-by-reference — the configuration that had no test
/// and in which the carry had therefore never worked.
///
/// Every build mints a fresh per-generation salt, so a `.vamana` hard-linked out of the base
/// generation into the new one is sealed under the *old* generation's key while the new
/// generation's manifest declares the *new* salt. Before the fix this fails the Poly1305 tag
/// on the base's first block inside `streaming_merge` (the build aborts), and — if it got
/// past that — again at serve time. After the fix the carried graph is its own salt-bearing
/// artifact and KNN returns the right neighbour.
///
/// The second half re-opens the whole data directory from cold (test 2 of the ticket): the
/// artifact's salt lives on disk in its own manifest, so a restart re-derives the cipher
/// identically with no in-memory state.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn consolidate_carries_an_encrypted_vamana_index_by_reference() {
    use graph_format::manifest::AnnMode;

    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let work = std::env::temp_dir().join(format!("slater_vamana_enc_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).unwrap();
    let data = work.join("data");
    let wal = work.join("_wal");

    // 32 hex chars = a 16-byte master key; the KDF takes any length.
    let key_hex = "0123456789abcdef0123456789abcdef";
    let key = graph_format::crypto::hex_decode(key_hex).unwrap();

    let (dim, n) = (16usize, 400usize);
    let (script, vectors) = vamana_fixture_script(dim, n);
    let input = work.join("dump.cypher");
    std::fs::write(&input, &script).unwrap();

    let build_args = |cmd: &mut std::process::Command| {
        cmd.args(["--graph", "docs"])
            .args(["--data-dir", data.to_str().unwrap()])
            .args(["--ann-threshold", "50"])
            .args(["--pq-subspaces", "8"])
            .args(["--pq-bits", "8"])
            .arg("--encrypt")
            .args(["--key-env", "SLATER_HIK145_KEY"])
            .env("SLATER_HIK145_KEY", key_hex);
    };

    let mut cmd = std::process::Command::new(&bin);
    cmd.args(["--input", input.to_str().unwrap()])
        .args(["--pk", "__dump_id__"])
        .args(["--cluster", "none"]);
    build_args(&mut cmd);
    assert!(
        cmd.status().expect("spawn slater-build").success(),
        "the encrypted fixture build must succeed"
    );

    let mut graphs = Graphs::open_all(&data, Some(&key)).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &data, None)
        .unwrap();
    let cache = BlockCache::new(1 << 22);
    let vc = VectorIndexCache::new(1 << 22);

    let gen0 = graphs.get("docs").unwrap();
    assert!(
        matches!(
            gen0.manifest().vector_indexes[0].mode,
            AnnMode::Vamana { .. }
        ),
        "the fixture must actually be a Vamana index, else this proves nothing"
    );
    let base_uuid = gen0.base_uuid();
    let base_vamana = data
        .join("docs")
        .join(base_uuid.to_string())
        .join("vector/Doc.embedding.vamana");
    let base_bytes = std::fs::read(&base_vamana).expect("base .vamana must exist");
    drop(gen0);

    graphs
        .consolidate_graph("docs", &cache, &vc, &data, None, |d, g, dd, _key, _acl| {
            // HIK-149: this graph carries a Vamana index, so its dump has a vector-carry
            // sidecar as well as the four base files — the one dump shape the marker test
            // below cannot reach.
            assert_dump_is_sealed(d);
            let mut cmd = std::process::Command::new(&bin);
            cmd.arg("--input")
                .arg(d)
                .args(["--input-format", "slater-dump"]);
            let _ = (g, dd);
            build_args(&mut cmd);
            let st = cmd.status().context("spawn builder")?;
            anyhow::ensure!(st.success(), "encrypted consolidating build failed: {st}");
            Ok(())
        })
        .expect("an encrypted carry-by-reference consolidation must succeed");

    // The carried graph must still be the *same bytes* — the carry exists to avoid rewriting
    // them, and re-sealing under the new generation key would rewrite every one.
    let carried = graphs.get("docs").unwrap();
    assert!(
        matches!(
            carried.manifest().vector_indexes[0].mode,
            AnnMode::Vamana { .. }
        ),
        "a carried Vamana base must stay Vamana, not be rebuilt as brute-force"
    );
    let carried_bytes = carried_vamana_bytes(&data, "docs", carried.as_ref());
    assert_eq!(
        carried_bytes, base_bytes,
        "an encrypted pure-permutation consolidation must carry the .vamana byte-identically"
    );
    drop(carried);

    let probe = |graphs: &Graphs, what: &str| {
        let g = graphs.get("docs").unwrap();
        let view = MergedView::read_only(g.as_ref());
        let body: Vec<String> = vectors[7].iter().map(|x| format!("{x:.6}")).collect();
        let ast = parser::parse(&format!(
            "CALL db.idx.vector.queryNodes('Doc', 'embedding', 1, vecf32([{}])) \
             YIELD node, score RETURN id(node) AS id, score",
            body.join(", ")
        ))
        .unwrap();
        let res = Engine::new(&view, &cache)
            .with_vector_cache(&vc, 96)
            .run(&ast)
            .unwrap_or_else(|e| panic!("{what}: KNN over the carried encrypted index: {e:#}"));
        assert_eq!(
            res.rows.len(),
            1,
            "{what}: the carried index must return a hit"
        );
        assert!(
            matches!(res.rows[0][0], Val::Int(7)),
            "{what}: a node's own embedding must be its own nearest neighbour, got {:?}",
            res.rows[0][0]
        );
        let Val::Float(score) = res.rows[0][1] else {
            panic!("{what}: score should be a float");
        };
        assert!(
            score.abs() < 1e-5,
            "{what}: an exact match must score ~0; got {score}"
        );
    };

    probe(&graphs, "after consolidation");

    // A **second** consolidation, carrying the artifact that the first one produced. This is
    // the artifact→artifact case: the base `.vamana` is no longer a generation file, so its
    // salt and its HIK-140 subkey label come from its own manifest and from nowhere else —
    // and its on-disk name is now `Doc.embedding.vamana` while the label it was sealed under
    // is still `vector/Doc.embedding.vamana`. If the subkey were inferred from the path this
    // is the consolidation that would fail.
    graphs
        .consolidate_graph("docs", &cache, &vc, &data, None, |d, g, dd, _key, _acl| {
            // HIK-149: this graph carries a Vamana index, so its dump has a vector-carry
            // sidecar as well as the four base files — the one dump shape the marker test
            // below cannot reach.
            assert_dump_is_sealed(d);
            let mut cmd = std::process::Command::new(&bin);
            cmd.arg("--input")
                .arg(d)
                .args(["--input-format", "slater-dump"]);
            let _ = (g, dd);
            build_args(&mut cmd);
            let st = cmd.status().context("spawn builder")?;
            anyhow::ensure!(st.success(), "second consolidating build failed: {st}");
            Ok(())
        })
        .expect("carrying an already-carried artifact must succeed");
    assert_eq!(
        carried_vamana_bytes(&data, "docs", graphs.get("docs").unwrap().as_ref()),
        base_bytes,
        "a second carry must still be the original bytes — never re-sealed, never rebuilt"
    );
    probe(&graphs, "after a second consolidation");

    // Test 2: cold restart. Drop every open handle and re-open the data dir from disk — the
    // carried artifact's salt must round-trip through its own manifest.
    drop(graphs);
    let reopened = Graphs::open_all(&data, Some(&key)).unwrap();
    probe(&reopened, "after restart");

    // Retention: the GC sweep must not reclaim an artifact the live set references. The
    // *superseded* artifact (the first consolidation's) is genuinely unreferenced and must
    // go — but it shares an inode with the live one, so the bytes survive regardless.
    let live_artifacts: Vec<GenId> = graph_format::setmanifest::SetManifest::read_via(
        &graph_format::store::fs::FsObjectStore::new(data.clone()),
        "docs",
        reopened.get("docs").unwrap().uuid(),
    )
    .unwrap()
    .vector_artifacts
    .iter()
    .map(|a| a.uuid)
    .collect();
    assert_eq!(
        live_artifacts.len(),
        1,
        "the live set must name exactly the artifact the served generation references"
    );
    // Two consolidations ⇒ two artifact directories, of which exactly one is live. Assert the
    // superseded one is actually reclaimed, or "the live one survived" would be vacuously
    // true of a sweep that does nothing at all.
    let on_disk: Vec<String> = std::fs::read_dir(data.join("docs").join("vecidx"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        on_disk.len(),
        2,
        "two consolidations must have left a live artifact and a superseded one, got {on_disk:?}"
    );
    let rep = reopened.gc_orphan_segments("docs", &data, 0).unwrap();
    assert_eq!(
        rep.deleted_vector_artifacts.len(),
        1,
        "the sweep must reclaim the superseded artifact — a sweep that reclaims nothing \
         cannot prove it spares the live one"
    );
    assert!(
        !rep.deleted_vector_artifacts.contains(&live_artifacts[0]),
        "GC must never reclaim a carried graph the live set references"
    );
    assert!(
        data.join("docs")
            .join("vecidx")
            .join(live_artifacts[0].0.to_string())
            .join("Doc.embedding.vamana")
            .exists(),
        "the live carried graph must still be on disk after a sweep"
    );
    // …and the graph still serves from it.
    drop(reopened);
    let after_gc = Graphs::open_all(&data, Some(&key)).unwrap();
    probe(&after_gc, "after a GC sweep");
    drop(after_gc);
    let reopened = Graphs::open_all(&data, Some(&key)).unwrap();
    drop(reopened);

    // The optimisation must survive being encrypted: the artifact is a hard link to the
    // original inode, not a re-sealed 370 GB copy.
    let vecidx = data.join("docs").join("vecidx");
    let artifact_dir = std::fs::read_dir(&vecidx)
        .expect("an encrypted carry must publish a vecidx/ artifact")
        .map(|e| e.unwrap().path())
        .find(|p| p.join("Doc.embedding.vamana").exists())
        .expect("the artifact must hold the carried graph file");
    assert_eq!(
        same_inode(&artifact_dir.join("Doc.embedding.vamana"), &base_vamana),
        Some(true),
        "an encrypted carry must hard-link the base inode, not re-seal its bytes"
    );

    // ── Test 3 (HIK-144 parity): a MAC-stripped artifact manifest, under a configured key.
    let json = artifact_dir.join("VECIDX.json");
    let sealed_json = std::fs::read(&json).unwrap();
    let mut doc: serde_json::Value = serde_json::from_slice(&sealed_json).unwrap();
    assert!(
        doc.get("mac").is_some_and(|m| !m.is_null()),
        "an encrypted carry must seal its artifact manifest in the first place"
    );
    let real_mac = doc["mac"].clone();
    doc["mac"] = serde_json::Value::Null;
    std::fs::write(&json, serde_json::to_vec_pretty(&doc).unwrap()).unwrap();
    let err = match Graphs::open_all(&data, Some(&key)) {
        Ok(_) => panic!("a MAC-stripped carried artifact must be refused under a key"),
        Err(e) => e,
    };
    assert!(
        err.chain().any(|c| matches!(
            c.downcast_ref::<graph_format::crypto::MacRejected>(),
            Some(graph_format::crypto::MacRejected::Missing { .. })
        )),
        "must be refused by type, not by chance: {err:#}"
    );

    // ── Test 4: an artifact sealed under a *different* master key. Everything else in the
    // image is sealed under the real one, so this isolates the artifact — it must fail
    // closed with a readable refusal, never decrypt to garbage.
    doc["mac"] = real_mac;
    let mut m: graph_format::vecmanifest::VectorIndexManifest =
        serde_json::from_value(doc).unwrap();
    m.seal_mac(b"a completely different operator master key")
        .unwrap();
    std::fs::write(&json, m.to_bytes().unwrap()).unwrap();
    let err = match Graphs::open_all(&data, Some(&key)) {
        Ok(_) => panic!("an artifact sealed under another key must be refused"),
        Err(e) => e,
    };
    assert!(
        err.chain().any(|c| matches!(
            c.downcast_ref::<graph_format::crypto::MacRejected>(),
            Some(graph_format::crypto::MacRejected::Mismatch { .. })
        )),
        "must be a typed MAC mismatch, not a block-decrypt failure or garbage: {err:#}"
    );

    // Restored, the image opens again — proving the two refusals above were caused by the
    // tampering and nothing else.
    std::fs::write(&json, &sealed_json).unwrap();
    assert!(
        Graphs::open_all(&data, Some(&key)).is_ok(),
        "the untampered image must still open"
    );

    std::fs::remove_dir_all(&work).ok();
}

// ── HIK-283: a rebuild must carry the ACL stamp forward ──────────────────────

/// The consolidation rebuild must be told which ACL to stamp against.
///
/// `run_builder` passed the data dir, the graph and the encryption key but not `--acl`,
/// and `slater-build` writes `aclBlake3` only when given it. So the rebuilt manifest was
/// unstamped, `check_manifest_policy` refused it under `requireAclStamp`, and `current`
/// was left naming a generation the server would not open — which under the default
/// `reloadStrategy = exit` means the graph does not come back up.
///
/// Asserted at the seam rather than end to end, so it runs in the normal suite: the
/// end-to-end path needs the real builder binary and is `#[ignore]`d. What is pinned is
/// that the server hands its configured ACL path to whatever performs the rebuild — the
/// exact thing that was missing.
#[test]
fn consolidation_hands_the_configured_acl_to_the_builder() {
    let (root, _graph) = testgen::write_indexed_people("hik283_acl_passthrough");
    let wal = root.join("_wal");
    let acl_path = write_acl(&root);

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    graphs.set_manifest_policy(Some(acl_path.clone()), true);

    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let writer = graphs.writer("people").unwrap();
    let w = match parser::parse_statement("MERGE (n:Person {name:'Ada'}) SET n.age = 41").unwrap() {
        parser::ast::Statement::Write(w) => w,
        _ => unreachable!(),
    };
    execute_write(
        &writer,
        graphs.get("people").unwrap().as_ref(),
        &w,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();

    // Record what the builder was handed, then fail the build — the assertion is about
    // the arguments, and failing keeps the test from needing a real rebuild.
    let seen: std::sync::Mutex<Option<Option<PathBuf>>> = std::sync::Mutex::new(None);
    let build = |_d: &Path, _g: &str, _dd: &Path, _k: Option<&[u8]>, acl: Option<&Path>| {
        *seen.lock().unwrap() = Some(acl.map(|p| p.to_path_buf()));
        anyhow::bail!("stop here: the arguments are what this test is about")
    };
    let _ = graphs.consolidate_graph("people", &cache, &vc, &root, None, build);

    let handed = seen
        .lock()
        .unwrap()
        .clone()
        .expect("the builder was never invoked");
    assert_eq!(
        handed,
        Some(acl_path),
        "the rebuild must be stamped with the ACL the server enforces, or the generation \
         it publishes is refused by the very policy that server is running"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A consolidation must not launder a rejected `acl.json` into a blessed one.
///
/// The `aclBlake3` stamp exists so that an edit to `acl.json` does **not** take effect
/// until a rebuild the operator controls: `reload_checked` refuses a candidate that
/// violates a served generation's stamp, and the last-good ACL keeps serving. That leaves
/// two deliberately different notions of "the ACL" — the in-force one held by `AclHandle`,
/// and whatever bytes are on disk right now.
///
/// Handing `slater-build` the *path* makes the rebuild read the second. It stamps the
/// tampered digest, and the swap-in check then compares that stamp against
/// `live_acl_digest()`, which re-reads the very same tampered file — so the two agree and
/// the generation is accepted. The tampered ACL is now what the served generation is
/// stamped against, so the next reload adopts it. Every step behaves as documented; the
/// composition is the hole.
///
/// The same thing bites without an attacker: an operator editing `acl.json` during a
/// multi-hour consolidation gets a generation stamped against a file the running server
/// never accepted.
#[test]
fn consolidation_refuses_to_stamp_an_acl_the_server_never_accepted() {
    let (root, _graph) = testgen::write_indexed_people("hik_acl_launder");
    let wal = root.join("_wal");
    let acl_path = write_acl(&root);
    let digest_a = graph_format::integrity::hash_file(&acl_path).unwrap();
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!(digest_a));

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    graphs.set_manifest_policy(Some(acl_path.clone()), true);

    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let writer = graphs.writer("people").unwrap();
    let w = match parser::parse_statement("MERGE (n:Person {name:'Ada'}) SET n.age = 41").unwrap() {
        parser::ast::Statement::Write(w) => w,
        _ => unreachable!(),
    };
    execute_write(
        &writer,
        graphs.get("people").unwrap().as_ref(),
        &w,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();

    // The attacker's edit. On disk only — the running server's in-force ACL is still A,
    // which is what `acl_pin` carries.
    let tampered = serde_json::json!({
        "users": { "reporting": { "passwordArgon2id": hash_password("pw").unwrap(),
            "grants": { "people": ["read", "write"], "secret": ["read"] } } }
    });
    std::fs::write(&acl_path, tampered.to_string()).unwrap();
    let digest_b = graph_format::integrity::hash_file(&acl_path).unwrap();
    assert_ne!(digest_a, digest_b, "the tamper must change the digest");

    // A builder that behaves exactly as `slater-build` does: stamp whatever `--acl`
    // names, read at the moment the child runs.
    let new_uuid = uuid::Uuid::from_u128(0x89_0000_0000_0000_0000_0000_0000_0002);
    let build = |_d: &Path, _g: &str, dd: &Path, _k: Option<&[u8]>, acl: Option<&Path>| {
        testgen::write_indexed_people_at(dd, new_uuid, [41, 25, 40]);
        if let Some(p) = acl {
            let stamp = graph_format::integrity::hash_file(p).unwrap();
            patch_manifest(dd, "people", "aclBlake3", serde_json::json!(stamp));
        }
        Ok(())
    };

    let res = graphs.consolidate_graph("people", &cache, &vc, &root, Some(&digest_a), build);

    assert!(
        res.is_err(),
        "a rebuild stamped against an acl.json the server never accepted must be refused; \
         it was accepted, so the tampered ACL is now blessed by the served generation"
    );
    let served = graphs.get("people").unwrap();
    assert_ne!(
        served.manifest().acl_blake3.as_deref(),
        Some(digest_b.as_str()),
        "the served generation must not carry the tampered ACL's stamp"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The window the pre-build check only narrows: `acl.json` changes *while* the rebuild
/// runs, which on a large graph is hours.
///
/// The pre-build hash agrees, so the build starts; the child then reads a file that has
/// since changed and stamps the new digest. Only the check on what was actually published
/// can catch this, which is why both exist — and why this test tampers from inside the
/// build closure rather than before it.
#[test]
fn consolidation_refuses_an_acl_swapped_while_the_rebuild_runs() {
    let (root, _graph) = testgen::write_indexed_people("hik_acl_launder_midbuild");
    let wal = root.join("_wal");
    let acl_path = write_acl(&root);
    let digest_a = graph_format::integrity::hash_file(&acl_path).unwrap();
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!(digest_a));

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    graphs.set_manifest_policy(Some(acl_path.clone()), true);

    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let writer = graphs.writer("people").unwrap();
    let w = match parser::parse_statement("MERGE (n:Person {name:'Ada'}) SET n.age = 41").unwrap() {
        parser::ast::Statement::Write(w) => w,
        _ => unreachable!(),
    };
    execute_write(
        &writer,
        graphs.get("people").unwrap().as_ref(),
        &w,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();

    let new_uuid = uuid::Uuid::from_u128(0x89_0000_0000_0000_0000_0000_0000_0003);
    let acl_for_closure = acl_path.clone();
    let build = |_d: &Path, _g: &str, dd: &Path, _k: Option<&[u8]>, acl: Option<&Path>| {
        // The edit lands after the pre-build check has already passed.
        let tampered = serde_json::json!({
            "users": { "reporting": { "passwordArgon2id": hash_password("pw").unwrap(),
                "grants": { "people": ["read", "write"], "secret": ["read"] } } }
        });
        std::fs::write(&acl_for_closure, tampered.to_string()).unwrap();
        testgen::write_indexed_people_at(dd, new_uuid, [41, 25, 40]);
        if let Some(p) = acl {
            let stamp = graph_format::integrity::hash_file(p).unwrap();
            patch_manifest(dd, "people", "aclBlake3", serde_json::json!(stamp));
        }
        Ok(())
    };

    let err = graphs
        .consolidate_graph("people", &cache, &vc, &root, Some(&digest_a), build)
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("refusing the consolidated generation"),
        "the published generation's stamp must be checked, not just the file before the \
         build; got: {msg}"
    );
    std::fs::remove_dir_all(&root).ok();
}
