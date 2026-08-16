// SPDX-License-Identifier: Apache-2.0
//! `reload_and_integrity` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// Micro-benchmark isolating the write-resolve cost: time **and resident memory** of
/// `resolve_business_key` over the 30%-delete segment (`wikidata_id` in `0..=p30`,
/// ascending), cached vs uncached, against a real large core — no WAL/memtable/flush
/// machinery. Answers "is the ISAM resolve the bulk-delete bottleneck, does the range
/// cache fix it, and what does the batch path cost in RSS?". RSS is sampled per phase
/// (`/proc/self/statm`) plus the process `VmHWM`: the per-row path stays flat, the
/// batch path's working set scales with `SLATER_SMOKE_BENCH_N` (raise it to see it).
/// Gated behind the `perf-mem` build switch (so the bench and its Linux-only `/proc`
/// RSS sampling are excluded from a normal `cargo test`), plus env-gated + `#[ignore]`:
/// `SLATER_SMOKE_DATADIR=<dir> SLATER_SMOKE_GRAPH=<graph> \
///   cargo test -p slater --lib --features perf-mem \
///   bench_resolve_business_key -- --ignored --nocapture`
#[cfg(feature = "perf-mem")]
#[test]
#[ignore = "needs a prebuilt generation; see SLATER_SMOKE_DATADIR"]
fn bench_resolve_business_key_over_the_segment() {
    // Self-skip rather than panic when the fixture is absent, matching the idiom the
    // other environment-sensitive tests use (see `cache::tests`, which returns early
    // when the box has too few cores). A benchmark that needs a prebuilt generation
    // has nothing to do without one — that is not a failure. This also keeps
    // `--features perf-mem -- --ignored` blanket-runnable; it is NOT why the
    // consolidation CI job filters by module (see the note there).
    let Ok(data_dir) = std::env::var("SLATER_SMOKE_DATADIR") else {
        eprintln!("skipping: set SLATER_SMOKE_DATADIR to a slater data directory");
        return;
    };
    let graph = std::env::var("SLATER_SMOKE_GRAPH").unwrap_or_else(|_| "wiki1m".to_string());
    let p30: i64 = std::env::var("SLATER_SMOKE_P30")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(332894);
    // Sample size — a small ascending run reproduces the "re-probe the same block"
    // pattern without a 10-minute loop. Default 5000.
    let n: i64 = std::env::var("SLATER_SMOKE_BENCH_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);
    let store: Arc<dyn ObjectStore> = Arc::new(FsObjectStore::new(&data_dir));

    // Resident set size right now (`/proc/self/statm` field 2 × page, matching
    // slater-build's `diag::rss_bytes`) and the process-wide high-water mark
    // (`VmHWM`). The per-row path holds nothing across iterations; the batch path
    // materialises the whole distinct value set + the merge-join's `Vec<Vec<u64>>`
    // of ids resident, so its working set grows with the batch size — the memory
    // side of the bulk-write floor. Deltas are noisy at small N (glibc/jemalloc
    // retain freed pages); raise `SLATER_SMOKE_BENCH_N` to see the batch cost grow.
    fn rss_now() -> u64 {
        std::fs::read_to_string("/proc/self/statm")
            .ok()
            .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
            .map_or(0, |pages| pages * 4096)
    }
    fn rss_peak() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines().find_map(|l| {
                    l.strip_prefix("VmHWM:")?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
            })
            .map_or(0, |kb| kb * 1024)
    }
    let mib = |b: u64| b as f64 / 1048576.0;

    let run = |label: &str, budget: Option<usize>| {
        // verify_integrity = false: the copy-completeness re-hash of a 1M-node core
        // would dwarf the loop we are timing (and the server pays it once at boot).
        let t_open = std::time::Instant::now();
        let gen = Generation::open_with_store_opts_cached(
            store.as_ref(),
            &graph,
            None,
            false,
            budget,
            crate::degree_column::DegreeResidency::Lazy,
            None,
        )
        .expect("open generation");
        let open_elapsed = t_open.elapsed();
        // Index geometry — few big blocks ⇒ decode-per-probe dominates.
        if let Some(r) = gen.range_index("node_Entity_wikidata_id") {
            println!("  index blocks = {}", r.num_blocks());
        }
        let rss_open = rss_now();
        let lo = p30 - n + 1;
        let t0 = std::time::Instant::now();
        let mut hits = 0u64;
        let mut rss_perrow_peak = rss_open;
        for (i, k) in (lo..=p30).enumerate() {
            if let KeyResolution::Unique(_) =
                resolve_business_key(&gen, "Entity", "wikidata_id", &Value::Int(k))
            {
                hits += 1;
            }
            // Sample sparsely — the per-row path frees each probe's decode buffer,
            // so resident stays flat; this confirms it rather than costing a syscall
            // per key.
            if i % 512 == 0 {
                rss_perrow_peak = rss_perrow_peak.max(rss_now());
            }
        }
        let loop_elapsed = t0.elapsed();
        let rss_after_perrow = rss_now();
        println!(
            "{label}: open {open_elapsed:?}; per-row resolved {n} keys ({hits} hits) in \
                 {loop_elapsed:?} ({:.1} µs/resolve)",
            loop_elapsed.as_micros() as f64 / n as f64
        );
        println!(
            "  mem: rss after open {:.1}MiB → after per-row {:.1}MiB (Δ{:+.1}, loop-peak {:.1}MiB)",
            mib(rss_open),
            mib(rss_after_perrow),
            mib(rss_after_perrow) - mib(rss_open),
            mib(rss_perrow_peak),
        );

        // The batch merge-join resolve (slice 6.3): sweep the same ascending run once
        // instead of one point probe per key. Same verdicts, one decompress per touched
        // block for the whole batch — the bulk-write floor fix.
        let rss_before_batch = rss_now();
        let values: Vec<Value> = (lo..=p30).map(Value::Int).collect();
        let refs: Vec<&Value> = values.iter().collect();
        let t1 = std::time::Instant::now();
        let batch = resolve_business_keys_batch(&gen, "Entity", "wikidata_id", &refs);
        let batch_elapsed = t1.elapsed();
        // Sample while the result (and any allocator pages the merge-join's transient
        // `Vec<Vec<u64>>` grew into) is still resident, before `batch`/`values` drop.
        let rss_after_batch = rss_now();
        let batch_hits = batch
            .iter()
            .filter(|r| matches!(r, KeyResolution::Unique(_)))
            .count();
        assert_eq!(batch_hits as u64, hits, "batch verdicts match per-row");
        println!(
            "{label}: batch-resolved {n} keys ({batch_hits} hits) in {batch_elapsed:?} \
                 ({:.1} µs/resolve, {:.1}× per-row)",
            batch_elapsed.as_micros() as f64 / n as f64,
            loop_elapsed.as_secs_f64() / batch_elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        );
        println!(
                "  mem: rss before batch {:.1}MiB → after batch {:.1}MiB (Δ{:+.1} for {n} keys resident); \
                 process VmHWM {:.1}MiB",
                mib(rss_before_batch),
                mib(rss_after_batch),
                mib(rss_after_batch) - mib(rss_before_batch),
                mib(rss_peak()),
            );
        drop(batch);
        drop(values);
    };

    run("uncached", None);
    run("cached-16MiB", Some(16 * 1024 * 1024));
}

#[test]
fn in_flight_gauge_tracks_without_diagnostics() {
    // The idle gate depends on `queries_in_flight` being maintained even when
    // load-test diagnostics are OFF (the default).
    let d = crate::diag::Diagnostics::new(false);
    assert_eq!(d.in_flight(), 0);
    d.on_query_start();
    d.on_query_start();
    assert_eq!(d.in_flight(), 2);
    d.on_query_ok(1.0);
    assert_eq!(d.in_flight(), 1);
    d.on_query_err(&anyhow::anyhow!("boom"));
    assert_eq!(d.in_flight(), 0);

    // A task-join failure must also decrement with diagnostics OFF, otherwise the
    // gauge (whose increment is unconditional) leaks upward forever.
    d.on_query_start();
    assert_eq!(d.in_flight(), 1);
    d.on_query_task_failed();
    assert_eq!(d.in_flight(), 0);
}

#[test]
fn is_already_in_progress_matches_only_the_typed_cause() {
    let typed = anyhow::Error::new(ConsolidationInProgress {
        op: "consolidation",
        graph: "people".into(),
    });
    assert!(is_already_in_progress(&typed));
    // Display text is preserved (the downstream Failure-message path relies on it).
    assert_eq!(
        typed.to_string(),
        "a consolidation for 'people' is already in progress"
    );
    // A *different* error that merely happens to contain the words must NOT match —
    // the old substring test produced exactly this false positive.
    assert!(!is_already_in_progress(&anyhow::anyhow!(
        "some other job already in progress elsewhere"
    )));
    assert!(!is_already_in_progress(&anyhow::anyhow!(
        "unrelated failure"
    )));
}

#[test]
fn acl_stamp_matches_serves_and_mismatch_refuses() {
    let (root, _g, _) = testgen::write_basic("aclstamp_match");
    let acl_path = write_acl(&root);
    let live = graph_format::integrity::hash_file(&acl_path).unwrap();

    // Stamped with the live digest → serves.
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!(live));
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.set_manifest_policy(Some(acl_path.clone()), false);
    assert!(graphs.verify_manifest_policy().is_ok());

    // Stamped with a stale digest → refuses to serve.
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!("deadbeef"));
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.set_manifest_policy(Some(acl_path), false);
    assert!(graphs.verify_manifest_policy().is_err());
}

#[test]
fn acl_digest_acceptable_matches_served_stamp() {
    let (root, _g, _) = testgen::write_basic("acl_digest_ok");
    let acl_path = write_acl(&root);
    let live = graph_format::integrity::hash_file(&acl_path).unwrap();
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!(live));

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.set_manifest_policy(Some(acl_path), false);

    assert!(
        graphs.acl_digest_acceptable(&live),
        "matching digest accepted"
    );
    assert!(
        !graphs.acl_digest_acceptable("deadbeef"),
        "a digest other than the stamp is refused"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn unstamped_generation_accepts_any_acl_digest() {
    // A legacy/plaintext image with no aclBlake3 stamp imposes no hot-reload
    // constraint, so the ACL keeps hot-reloading as before.
    let (root, _g, _) = testgen::write_basic("acl_digest_unstamped");
    let acl_path = write_acl(&root);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.set_manifest_policy(Some(acl_path), false);
    assert!(graphs.acl_digest_acceptable("anything"));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn hot_reload_refuses_tamper_then_adopts_matching_rebuild() {
    let (root, _g, _) = testgen::write_basic("acl_hotreload_e2e");
    let acl_path = write_acl(&root);
    let live = graph_format::integrity::hash_file(&acl_path).unwrap();
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!(live));

    let acl = AclHandle::load(&acl_path).unwrap();
    assert!(acl.snapshot().can_read("reporting", "people"));
    assert!(!acl.snapshot().can_read("reporting", "secret"));

    // ── Tamper: edit acl.json at runtime to self-grant a new read. The served
    // generation still carries the *old* stamp, so the enforced reload refuses it.
    let tampered = serde_json::json!({
        "users": { "reporting": { "passwordArgon2id": hash_password("pw").unwrap(),
            "grants": { "people": ["read"], "secret": ["read"] } } }
    });
    std::fs::write(&acl_path, tampered.to_string()).unwrap();

    let graphs = {
        let mut g = Graphs::open_all(&root, None).unwrap();
        g.set_manifest_policy(Some(acl_path.clone()), false);
        Arc::new(g)
    };
    let g1 = graphs.clone();
    assert!(!acl.reload_checked(move |d| g1.acl_digest_acceptable(d)));
    assert!(
        !acl.snapshot().can_read("reporting", "secret"),
        "tampered grant must not take effect"
    );

    // ── Legitimate change: a generation rebuilt against the new acl.json carries a
    // matching stamp. Re-open to model the swapped-in generation; the enforced
    // reload now accepts the same file.
    let newdigest = graph_format::integrity::hash_file(&acl_path).unwrap();
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!(newdigest));
    let graphs2 = {
        let mut g = Graphs::open_all(&root, None).unwrap();
        g.set_manifest_policy(Some(acl_path), false);
        Arc::new(g)
    };
    let g2 = graphs2.clone();
    assert!(acl.reload_checked(move |d| g2.acl_digest_acceptable(d)));
    assert!(
        acl.snapshot().can_read("reporting", "secret"),
        "ACL matching the new stamp is adopted"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn unstamped_generation_ignored_unless_required() {
    let (root, _g, _) = testgen::write_basic("aclstamp_absent");
    let acl_path = write_acl(&root);

    // Legacy image with no aclBlake3 serves when not required.
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.set_manifest_policy(Some(acl_path.clone()), false);
    assert!(graphs.verify_manifest_policy().is_ok());

    // requireAclStamp turns the absence into a refusal.
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.set_manifest_policy(Some(acl_path), true);
    assert!(graphs.verify_manifest_policy().is_err());
}

/// Re-seal every generation manifest of `graph` with a MAC under `key` — and publish a
/// sealed singleton set manifest for each — as an encrypted build would. (The fixture
/// data stays plaintext; the MAC path is independent of whether blocks are encrypted.)
/// Both documents are needed: under a key HIK-144 requires the *composition* to be
/// authenticated as well, so a sealed MANIFEST beside an absent or unsealed set is
/// refused.
fn reseal_manifest_with_mac(root: &Path, graph: &str, key: &[u8]) {
    let sets = root.join(graph).join("sets");
    std::fs::create_dir_all(&sets).unwrap();
    for entry in std::fs::read_dir(root.join(graph)).unwrap() {
        let man = entry.unwrap().path().join("MANIFEST.json");
        if man.exists() {
            let mut m: graph_format::manifest::Manifest =
                serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
            m.seal_mac(key).unwrap();
            std::fs::write(&man, m.to_json().unwrap()).unwrap();

            let uuid = m.build_uuid;
            let mut set = graph_format::setmanifest::SetManifest::singleton(uuid, 0);
            set.seal_mac(key).unwrap();
            std::fs::write(
                sets.join(format!("{}.json", uuid.0)),
                set.to_bytes().unwrap(),
            )
            .unwrap();
        }
    }
}

#[test]
fn manifest_mac_catches_tamper_through_open() {
    let (root, _g, _) = testgen::write_basic("mac_e2e");
    let key: &[u8] = b"operator master key";
    reseal_manifest_with_mac(&root, "people", key);

    // Sealed manifest opens cleanly with the key (MAC verifies; data plaintext).
    assert!(Generation::open_with_key(&root, "people", Some(key)).is_ok());

    // Tamper a MAC-covered field the content-hash does NOT cover (nodeCount)
    // without resealing: the MAC check refuses before anything else. A plaintext
    // image (no MAC) would happily serve this forged count.
    patch_manifest(&root, "people", "nodeCount", serde_json::json!(999_999));
    let err = Generation::open_with_key(&root, "people", Some(key))
        .err()
        .expect("tampered manifest must fail to open");
    assert!(
        format!("{err:#}").contains("MAC"),
        "expected a MAC error, got: {err:#}"
    );
}

#[test]
fn keyed_server_refuses_macless_generation_unconditionally() {
    let (root, _g, _) = testgen::write_basic("require_mac");
    let _acl_path = write_acl(&root);
    // The plaintext fixture carries no MAC; a server configured with a master key must
    // refuse it (the MAC-strip downgrade guard). This is deliberately not a policy flag —
    // there is no legitimate keyed-but-unauthenticated deployment, so there is nothing to
    // configure.
    //
    // HIK-144 moved *where* that refusal happens: it is now enforced at open, so the
    // server never even holds an unauthenticated generation, rather than opening it and
    // rejecting it a step later in `verify_manifest_policy`. Refusing earlier is what
    // makes every other opener (`slater query`, consolidation, the benchmarks) inherit
    // the same policy.
    let err = Graphs::open_all(&root, Some(b"master"))
        .err()
        .expect("a keyed server must refuse an unauthenticated generation at open");
    assert!(
        err.chain().any(|e| matches!(
            e.downcast_ref::<graph_format::crypto::MacRejected>(),
            Some(graph_format::crypto::MacRejected::Missing { .. })
        )),
        "must be refused by type: {err:#}"
    );
}

/// End-to-end Phase 1c: a business-key `SET` resolves the anchor to a core
/// dense id, is durably logged + folded into the memtable, and a subsequent
/// read sees the overwrite through the overlay — read-your-writes — with the
/// value surviving a writer reopen (WAL replay).
#[test]
fn write_then_read_your_writes_and_survives_reopen() {
    let (root, _g, _) = testgen::write_basic("ryow");
    let wal = root.join("_wal");
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let gen = graphs.get("people").unwrap();
    let writer = graphs.writer("people").expect("writable layer is on");
    let epoch0 = writer.epoch();

    // Overwrite Alice's age and add a new property.
    let stmt = match parser::parse_statement(
        "MATCH (n:Person {name: 'Alice'}) SET n.age = 99, n.rating = 'AAA'",
    )
    .unwrap()
    {
        parser::ast::Statement::Write(w) => w,
        _ => panic!("expected a write"),
    };
    let out = execute_write(
        &writer,
        gen.as_ref(),
        &stmt,
        &HashMap::new(),
        TEST_BOLT_VERSION,
    )
    .unwrap();
    assert_eq!(
        out,
        (Vec::new(), Vec::new()),
        "a no-RETURN write acks empty"
    );
    assert!(writer.epoch() > epoch0, "the write bumps the delta epoch");

    // The write resolved Alice to dense id 0 and folded the patch.
    let snap = writer.snapshot();
    let d = snap.node_patch(0).expect("resolved by dense id");
    assert_eq!(d.patches.get("age"), Some(&Value::Int(99)));
    drop(snap);

    // Read-your-writes through the merged view.
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::new(
        gen.as_ref(),
        DeltaSnapshot::from_memtable(writer.snapshot()),
    );
    let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age, n.rating").unwrap();
    let res = Engine::new(&view, &cache).run(&ast).unwrap();
    assert_eq!(res.rows.len(), 1);
    assert!(
        matches!(res.rows[0][0], Val::Int(99)),
        "overwritten age read back"
    );
    assert!(
        matches!(&res.rows[0][1], Val::Str(s) if s == "AAA"),
        "new property read back"
    );

    // Durability: a fresh writer over the same WAL replays the committed write.
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
    assert_eq!(
        reopened
            .snapshot()
            .node_patch(0)
            .unwrap()
            .patches
            .get("age"),
        Some(&Value::Int(99)),
        "the write is durable across a reopen (WAL replay)"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A generation's stamp is checked against the ACL **in force**, not against a re-read of
/// `acl.json`.
///
/// Those diverge whenever an edit has been refused: `reload_checked` keeps the last-good
/// ACL serving while the file holds something the server rejected. Verifying a stamp by
/// re-hashing the file therefore asks only "does the file agree with itself", which
/// whoever wrote the file can arrange — the mechanism behind HIK-284.
///
/// Here a generation is stamped against a tampered `acl.json` while the in-force ACL is
/// still the original. Against the file it verifies; against the handle it must not.
#[test]
fn a_stamp_is_verified_against_the_acl_in_force_not_the_file_on_disk() {
    let (root, _g, _) = testgen::write_basic("hik284_in_force");
    let acl_path = write_acl(&root);
    let digest_a = graph_format::integrity::hash_file(&acl_path).unwrap();
    let acl = Arc::new(AclHandle::load(&acl_path).unwrap());
    assert_eq!(acl.digest(), digest_a);

    // The file is tampered; the handle is not reloaded, so the in-force ACL is still A.
    let tampered = serde_json::json!({
        "users": { "reporting": { "passwordArgon2id": hash_password("pw").unwrap(),
            "grants": { "people": ["read", "write"], "secret": ["read"] } } }
    });
    std::fs::write(&acl_path, tampered.to_string()).unwrap();
    let digest_b = graph_format::integrity::hash_file(&acl_path).unwrap();
    assert_ne!(digest_a, digest_b);

    // A generation stamped against the tampered file — what a rebuild reading the path
    // would produce.
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!(digest_b));

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.set_manifest_policy(Some(acl_path.clone()), false);

    // Without the in-force ACL bound, the check hashes the file and the tampered stamp
    // agrees with it — the hole, kept here as the contrast that gives the next line
    // meaning.
    let m = graphs.get("people").unwrap().manifest().clone();
    assert!(
        graphs
            .check_manifest_policy("people", &m, graphs.live_acl_digest().unwrap().as_deref())
            .is_ok(),
        "precondition: against the file, the tampered stamp verifies"
    );

    graphs.set_in_force_acl(acl.clone());
    let err = graphs
        .check_manifest_policy("people", &m, graphs.live_acl_digest().unwrap().as_deref())
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("refusing to serve"),
        "against the ACL in force, a stamp for a rejected acl.json must be refused: {err:#}"
    );
    let _ = std::fs::remove_dir_all(&root);
}
