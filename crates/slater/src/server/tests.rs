// SPDX-License-Identifier: Apache-2.0
//! Unit tests for the parent module (extracted verbatim from the inline
//! `mod tests`; a pure relocation, no test logic changed).

use super::*;
use crate::acl::hash_password;
use crate::testgen;
use tokio::net::TcpStream;

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
    let data_dir = std::env::var("SLATER_SMOKE_DATADIR")
        .expect("set SLATER_SMOKE_DATADIR to a slater data directory");
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

/// Write a temp ACL granting `reporting`/`pw` read on `people`, return its path.
fn write_acl(root: &Path) -> std::path::PathBuf {
    let path = root.join("acl.json");
    let json = serde_json::json!({
        "users": {
            "reporting": {
                "passwordArgon2id": hash_password("pw").unwrap(),
                "grants": { "people": ["read"] }
            }
        }
    });
    std::fs::write(&path, json.to_string()).unwrap();
    path
}

/// Patch one top-level field in every generation manifest of `graph` under
/// `root`. Safe for fields outside the data-file inventory (e.g. `aclBlake3`),
/// which `content_hash` excludes, so `open_all` still validates afterwards.
fn patch_manifest(root: &Path, graph: &str, key: &str, value: serde_json::Value) {
    for entry in std::fs::read_dir(root.join(graph)).unwrap() {
        let man = entry.unwrap().path().join("MANIFEST.json");
        if man.exists() {
            let mut v: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
            v[key] = value.clone();
            std::fs::write(&man, serde_json::to_string_pretty(&v).unwrap()).unwrap();
        }
    }
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

/// A `DeltaConfig` with the writable layer on and a throwaway WAL directory.
fn delta_cfg(wal_dir: &Path) -> DeltaConfig {
    DeltaConfig {
        enabled: true,
        wal_dir: wal_dir.to_string_lossy().into_owned(),
        memtable_bytes: 64 << 20,
        l0_compaction_trigger: 4,
        segment_flush_bytes: 0,
        max_upper_segments: 8,
        delta_core_percent: 0,
        delta_hard_bytes: 0,
        consolidate_window: String::new(),
        builder_bin: "slater-build".to_string(),
        off_heap_l0: false,
        segment_gc_grace_secs: 0,
    }
}

/// [`delta_cfg`] reading sealed L0 levels **off-heap** (a block image paged through the
/// shared cache, not a resident memtable) — the config a T2 flush over off-heap L0 exercises.
fn delta_cfg_offheap(wal_dir: &Path) -> DeltaConfig {
    DeltaConfig {
        off_heap_l0: true,
        ..delta_cfg(wal_dir)
    }
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
    let out = execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
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
    let e = execute_write(&writer, gen.as_ref(), &absent, &HashMap::new()).unwrap_err();
    assert!(
        e.message.contains("node to update") && e.message.contains("MERGE"),
        "got: {}",
        e.message
    );

    // RETURN after SET is not yet supported.
    let with_ret =
        match parser::parse_statement("MATCH (n:Person {name:'Alice'}) SET n.age = 1 RETURN n")
            .unwrap()
        {
            parser::ast::Statement::Write(w) => w,
            _ => unreachable!(),
        };
    let e = execute_write(&writer, gen.as_ref(), &with_ret, &HashMap::new()).unwrap_err();
    assert!(
        e.message.contains("RETURN after a write"),
        "got: {}",
        e.message
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
    execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

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
    execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

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
    execute_write(&writer, gen.as_ref(), &patch, &HashMap::new()).unwrap();
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
        execute_write(w, gen.as_ref(), &stmt, &HashMap::new())
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
        execute_write(w, gen.as_ref(), &stmt, params).unwrap();
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
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
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
    execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

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
    execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

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
            parser::ast::Statement::Write(w) => {
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
            }
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
            parser::ast::Statement::Write(w) => {
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
            }
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
            parser::ast::Statement::Write(w) => {
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
            }
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
            parser::ast::Statement::Write(w) => {
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
            }
            parser::ast::Statement::Create(c) => {
                execute_create(&writer, gen.as_ref(), &c, &HashMap::new()).map(|_| ())
            }
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
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
            }
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
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).map(|_| ())
            }
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

/// Count the `*.wal` segment files under a WAL directory.
fn wal_count(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("wal"))
        .count()
}

/// Read a binary consolidation dump into a `{ node name → node props }` map, for
/// tests that assert the serialiser saw the merged state. Nodes are keyed by their
/// `name` property (the fixtures' business key).
fn dump_nodes(
    dump: &Path,
) -> std::collections::HashMap<String, Vec<(String, graph_format::ids::Value)>> {
    use graph_format::consolidate_dump::DumpReader;
    let r = DumpReader::open(dump).unwrap();
    let keys = r.meta().property_keys.clone();
    let mut out = std::collections::HashMap::new();
    r.for_each_node(|_, _lb, pb| {
        let props: Vec<(String, graph_format::ids::Value)> =
            graph_format::columns::decode_props(pb)
                .unwrap()
                .into_iter()
                .map(|(k, v)| (keys[k as usize].clone(), v))
                .collect();
        if let Some((_, graph_format::ids::Value::Str(name))) =
            props.iter().find(|(k, _)| k == "name")
        {
            out.insert(name.clone(), props);
        }
        Ok(())
    })
    .unwrap();
    out
}

/// The integer `age` of node `name` in a binary dump, if present.
fn dump_age(dump: &Path, name: &str) -> Option<i64> {
    dump_nodes(dump).get(name).and_then(|p| {
        p.iter()
            .find(|(k, _)| k == "age")
            .and_then(|(_, v)| match v {
                graph_format::ids::Value::Int(i) => Some(*i),
                _ => None,
            })
    })
}

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
    execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
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
    let build = |dump: &Path, g: &str, dd: &Path| -> Result<()> {
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
        execute_write(&writer_mid, gen_mid.as_ref(), &bob, &HashMap::new()).unwrap();
        testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
        Ok(())
    };
    let published = graphs
        .consolidate_graph("people", &cache, &vc, &root, build)
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
    assert!(!root.join("people").join(".consolidate.dump").exists());
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            _ => panic!("expected a write: {q}"),
        }
    };
    // A base-node patch, a base-node delete, a born node, and a born edge from the born
    // node to a surviving base node — every stack override kind in one flush.
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
    write(&graphs, "MATCH (n:Person {name:'Carol'}) DELETE n");
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
    crate::consolidate::serialise_binary_dump(&Engine::new(&view, &cache), &view, &dir).unwrap();

    // Read it back: id → name / age, and the edges as (src-name, dst-name, reltype).
    use graph_format::consolidate_dump::DumpReader;
    let r = DumpReader::open(&dir).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            _ => panic!("expected a write: {q}"),
        }
    };
    // Flush a patch + delete + born into a segment, so the core we consolidate is stacked.
    write(&graphs, "MATCH (n:Person {name:'Alice'}) SET n.age = 99");
    write(&graphs, "MATCH (n:Person {name:'Carol'}) DELETE n");
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
    let build = |dump: &Path, g: &str, dd: &Path| -> Result<()> {
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
        execute_write(&writer_mid, gen_mid.as_ref(), &bob, &HashMap::new()).unwrap();
        testgen::write_indexed_people_at(dd, new_uuid, [99, 25, 40]);
        Ok(())
    };
    let published = graphs
        .consolidate_graph("people", &cache, &vc, &root, build)
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
    assert!(!root.join("people").join(".consolidate.dump").exists());

    std::fs::remove_dir_all(&root).ok();
}

// ── Phase 7 slice 7.2: orphan segment/set GC ─────────────────────────────────

/// The segment directory names (uuid dirs, skipping dot-files) under `<root>/people/`.
fn seg_dirs(root: &Path) -> Vec<String> {
    std::fs::read_dir(root.join("people").join("segments"))
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| !n.starts_with('.'))
                .collect()
        })
        .unwrap_or_default()
}

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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
    let build = |_dump: &Path, g: &str, dd: &Path| -> Result<()> {
        assert_eq!(g, "people");
        testgen::write_indexed_people_at(dd, new_uuid, [30, 25, 40]);
        Ok(())
    };
    graphs
        .consolidate_graph("people", &cache, &vc, &root, build)
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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

// ── the write ladder's vector levels (HIK-111) ────────────────────────────────────────

/// The unit query vector every level test scores against.
const VQ: [f32; 2] = [1.0, 0.0];

/// The unit 2-vector at cosine **distance** `d` from [`VQ`]: `cos θ = 1 − d`, so the
/// distance a KNN scan reports for it is `d` itself (to f32 rounding). Lets a fixture and
/// a write be specified directly in the quantity the assertions are about.
fn at_distance(d: f64) -> Vec<f32> {
    let cos = 1.0 - d;
    let sin = (1.0 - cos * cos).max(0.0).sqrt();
    vec![cos as f32, sin as f32]
}

/// `SET n.embedding = vecf32([…])` for a `:Doc` fixture node, as Cypher text.
fn set_embedding(name: &str, v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
    format!(
        "MATCH (n:Doc {{name:'{name}'}}) SET n.embedding = vecf32([{}])",
        parts.join(", ")
    )
}

/// Run a write statement against the graph's delta.
fn vwrite(graphs: &Graphs, graph: &str, q: &str) {
    vwrite_params(graphs, graph, q, &HashMap::new());
}

/// [`vwrite`] with bound parameters. A vector with a negative component has no *literal*
/// spelling the Phase 1c write grammar admits (a unary minus is an expression, not a
/// literal), so a re-embed onto an arbitrary vector has to go through `vecf32($v)`.
fn vwrite_params(graphs: &Graphs, graph: &str, q: &str, params: &HashMap<String, Val>) {
    let gen = graphs.get(graph).unwrap();
    let writer = graphs.writer(graph).unwrap();
    match parser::parse_statement(q).unwrap() {
        parser::ast::Statement::Write(w) => {
            execute_write(&writer, gen.as_ref(), &w, params)
                .unwrap_or_else(|e| panic!("write failed ({q}): {e:?}"));
        }
        _ => panic!("expected a write: {q}"),
    }
}

/// Re-embed a `:Doc` fixture node onto `vector`, through a bound `vecf32($v)`.
fn embed_param(graphs: &Graphs, graph: &str, name: &str, vector: &[f32]) {
    let mut params = HashMap::new();
    params.insert(
        "v".to_string(),
        Val::List(vector.iter().map(|x| Val::Float(*x as f64)).collect()),
    );
    vwrite_params(
        graphs,
        graph,
        &format!("MATCH (n:Doc {{name:'{name}'}}) SET n.embedding = vecf32($v)"),
        &params,
    );
}

/// KNN over the merged view (base + segments + delta), as `(id, score)` in rank order.
fn vknn(graphs: &Graphs, graph: &str, cache: &BlockCache, q: &[f32], k: usize) -> Vec<(u64, f64)> {
    let gen = graphs.get(graph).unwrap();
    let snap = DeltaSnapshot::from_memtable(graphs.writer(graph).unwrap().snapshot());
    let view = MergedView::new(gen.as_ref(), snap);
    let parts: Vec<String> = q.iter().map(|x| format!("{x:?}")).collect();
    let ast = parser::parse(&format!(
        "CALL db.idx.vector.queryNodes('Doc', 'embedding', {k}, vecf32([{}])) \
             YIELD node, score RETURN id(node) AS id, score",
        parts.join(", ")
    ))
    .unwrap();
    let res = Engine::new(&view, cache).run(&ast).unwrap();
    res.rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Val::Int(i), Val::Float(s)) => (*i as u64, *s),
            other => panic!("unexpected KNN row {other:?}"),
        })
        .collect()
}

/// **The hazard.** Node 7 is embedded at three different levels at once: the sealed base
/// holds its original vector, a core **segment** re-embedded it, and the **delta**
/// re-embedded it again. Only the delta's vector is live; the other two are stale copies
/// of the same node id.
///
/// `merge_topk` deliberately does not dedup by node id, so a stale copy that survives its
/// level's scan does not merely misorder the results — it takes one of the `k` slots and
/// **evicts a live candidate**, and the k-th neighbour goes missing. No error, no panic,
/// no log line. The numbers are chosen so that both stale copies are *closer* to the query
/// than the live one (0.0 and 0.1 vs 0.5): a stale entry that is farther away can never win
/// a slot, so it would prove nothing.
///
/// Truth, computed by hand from the effective newest-wins vector set
/// {d00: 0.2, d01: 0.3, d02: 0.55, d07: **0.5**, …}: the top-4 is
/// `[d00 0.2, d01 0.3, d07 0.5, d02 0.55]`.
///
/// Suppress the base with the global set but let the *segment's* copy through (one flat
/// overlay, no per-level suppression) and you get `[d07 0.1, d00 0.2, d01 0.3, d07 0.5]` —
/// node 7 twice, and the live d02 evicted off the end of k.
#[test]
fn knn_suppresses_a_stale_vector_at_every_level_it_lives_at() {
    let base: Vec<Vec<f32>> = [0.2, 0.3, 0.55, 0.9, 0.95, 1.0, 1.05, 0.0]
        .iter()
        .map(|d| at_distance(*d))
        .collect();
    let (root, graph) = testgen::write_vector_docs("vec_levels_hazard", &base);
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // A segment re-embeds node 7 — stale, but *closer* to the query than the truth.
    vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(0.1)));
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the re-embed flushes into a segment");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 1);

    // The delta re-embeds it again. This is the live vector.
    vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(0.5)));

    let got = vknn(&graphs, &graph, &cache, &VQ, 4);
    let want = [(0u64, 0.2f64), (1, 0.3), (7, 0.5), (2, 0.55)];
    assert_eq!(
        got.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        want.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        "the top-4 over the effective (newest-wins) vector set; got {got:?}"
    );
    for ((gid, gs), (wid, ws)) in got.iter().zip(&want) {
        assert_eq!(gid, wid);
        assert!(
            (gs - ws).abs() < 1e-5,
            "node {gid} should score {ws}, got {gs}"
        );
    }
    assert_eq!(
        got.iter().filter(|(id, _)| *id == 7).count(),
        1,
        "node 7 must appear exactly once — it is embedded at three levels and only the \
             delta's vector is live; got {got:?}"
    );
    assert!(
        got.iter().any(|(id, _)| *id == 2),
        "the k-th live neighbour (d02) must still be there — a stale duplicate that reaches \
             the merge evicts it, silently; got {got:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The hazard extended to **three core segment levels** (HIK-113): node 7 is re-embedded in
/// three successive flushes, so three different segments each hold a stale-or-live vector for
/// the same node id. The per-segment fold's `superseded_above` must suppress the two older
/// copies in their own scans (each older segment sees a newer one that touched node 7), so
/// node 7 reaches the merge from exactly the newest segment and only once. If any older
/// level leaks its copy, it takes a `k` slot and the k-th live neighbour (d02) vanishes.
///
/// The stale copies (0.05, 0.1) and the base copy (0.0) are all *closer* to the query than
/// the live one (0.5): a farther stale copy could never win a slot and would prove nothing.
#[test]
fn knn_suppresses_a_stale_vector_across_three_segment_levels() {
    let base: Vec<Vec<f32>> = [0.2, 0.3, 0.55, 0.9, 0.95, 1.0, 1.05, 0.0]
        .iter()
        .map(|d| at_distance(*d))
        .collect();
    let (root, graph) = testgen::write_vector_docs("vec_three_seg_hazard", &base);
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // Three flushes, each re-embedding node 7 to a different vector — three segment levels.
    for d in [0.05, 0.1, 0.5] {
        vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(d)));
        graphs
            .flush_graph_to_segment(&graph, &vc, &root)
            .unwrap()
            .expect("each re-embed flushes into its own segment");
    }
    assert_eq!(
        graphs.get(&graph).unwrap().stack().segments().len(),
        3,
        "three flushes ⇒ three segment levels"
    );

    let got = vknn(&graphs, &graph, &cache, &VQ, 4);
    let want_ids = [0u64, 1, 7, 2];
    assert_eq!(
        got.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        want_ids.to_vec(),
        "top-4 over the newest-wins set {{d00:0.2, d01:0.3, d07:0.5, d02:0.55}}; got {got:?}"
    );
    let seven = got.iter().find(|(id, _)| *id == 7).unwrap();
    assert!(
        (seven.1 - 0.5).abs() < 1e-5,
        "node 7 must score the NEWEST segment's 0.5, not an older segment's 0.05/0.1 nor the \
             base's 0.0; got {}",
        seven.1
    );
    assert_eq!(
        got.iter().filter(|(id, _)| *id == 7).count(),
        1,
        "node 7 is embedded at three segment levels + the base; only the newest may emit it \
             — a duplicate means an older level failed to suppress; got {got:?}"
    );
    assert!(
        got.iter().any(|(id, _)| *id == 2),
        "the k-th live neighbour (d02) must survive — a leaked stale copy would evict it; \
             got {got:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The same three-level stack, seen by the **other** consumer of the fold: the binary
/// consolidation dump. If the dump and the KNN path disagree about which level wins, a
/// vector goes missing on consolidation — and only on consolidation, where nothing is
/// looking. So the dump must carry exactly one vector per node, the newest one, and a
/// removal must stay removed.
#[test]
fn the_consolidation_dump_carries_one_vector_per_node_newest_wins() {
    let base: Vec<Vec<f32>> = [0.2, 0.3, 0.55, 0.9, 0.95, 1.0, 1.05, 0.0]
        .iter()
        .map(|d| at_distance(*d))
        .collect();
    let (root, graph) = testgen::write_vector_docs("vec_levels_dump", &base);
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // Segment: re-embed node 7, and remove node 3's embedding.
    vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(0.1)));
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Doc {name:'d03'}) REMOVE n.embedding",
    );
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the writes flush into a segment");
    // Delta: re-embed node 7 again (superseding the segment's copy), and remove node 4's.
    vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(0.5)));
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Doc {name:'d04'}) REMOVE n.embedding",
    );

    let gen = graphs.get(&graph).unwrap();
    let snap = DeltaSnapshot::from_memtable(graphs.writer(&graph).unwrap().snapshot());
    let view = MergedView::new(gen.as_ref(), snap);
    let dump = root.join("_dump");
    std::fs::create_dir_all(&dump).unwrap();
    crate::consolidate::serialise_binary_dump(&Engine::new(&view, &cache), &view, &dump).unwrap();

    let reader = graph_format::consolidate_dump::DumpReader::open(&dump).unwrap();
    let mut dumped: Vec<(u64, Vec<f32>)> = Vec::new();
    reader
        .for_each_vector(|node_id, _key_id, v| {
            dumped.push((node_id, v.to_vec()));
            Ok(())
        })
        .unwrap();
    dumped.sort_by_key(|(id, _)| *id);

    let ids: Vec<u64> = dumped.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        ids,
        vec![0, 1, 2, 5, 6, 7],
        "one vector per node with a live embedding: 3 and 4 were removed (at different \
             levels), and every other node keeps exactly one — got {ids:?}"
    );
    let seven = &dumped.iter().find(|(id, _)| *id == 7).unwrap().1;
    assert_eq!(
        seven,
        &at_distance(0.5),
        "node 7's dumped vector must be the delta's (the newest level), not the segment's \
             stale copy nor the base's"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A **randomised** cross-level property test: the merged top-k must *equal* an exact
/// brute force over the effective live vector set — not approximate it, not recall it.
///
/// The truth is derived from the write script the test itself issues (base vectors, then
/// each round's re-embeds / removals / node deletes replayed into a plain map), never read
/// off a second implementation of the fold. The base index is `AnnMode::BruteForce`, so
/// both sides are exact and the assertion is equality of ids *and* scores.
#[test]
fn knn_across_levels_equals_a_brute_force_over_the_live_set() {
    // Deterministic PRNG (the fixture path takes no `rand` dependency).
    fn next(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }
    fn unit_vec(state: &mut u64) -> Vec<f32> {
        let mut v: Vec<f32> = (0..3)
            .map(|_| (next(state) % 2000) as f32 / 1000.0 - 1.0)
            .collect();
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if n < 1e-3 {
            return vec![1.0, 0.0, 0.0];
        }
        for x in &mut v {
            *x /= n;
        }
        v
    }

    const N: usize = 12;
    for seed in 0..8u64 {
        let st = &mut (0x9E37_79B9_7F4A_7C15u64 ^ seed.wrapping_mul(0x2545_F491_4F6C_DD1D));
        let base: Vec<Vec<f32>> = (0..N).map(|_| unit_vec(st)).collect();
        let (root, graph) = testgen::write_vector_docs(&format!("vec_levels_prop_{seed}"), &base);
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        // The independently-derived truth: the effective live vector set, replayed from
        // the very statements the test issues.
        let mut live: HashMap<u64, Vec<f32>> = base
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, v)| (i as u64, v))
            .collect();
        let mut deleted: HashSet<u64> = HashSet::new();

        // 0–3 core segments, then a final round that stays in the delta.
        let segments = (next(st) % 4) as usize;
        for round in 0..=segments {
            for id in 0..N as u64 {
                if deleted.contains(&id) || !next(st).is_multiple_of(3) {
                    continue; // ~1 node in 3 is touched per round
                }
                let name = format!("d{id:02}");
                match next(st) % 8 {
                    0 => {
                        vwrite(
                            &graphs,
                            &graph,
                            &format!("MATCH (n:Doc {{name:'{name}'}}) DELETE n"),
                        );
                        deleted.insert(id);
                        live.remove(&id);
                    }
                    1 | 2 => {
                        vwrite(
                            &graphs,
                            &graph,
                            &format!("MATCH (n:Doc {{name:'{name}'}}) REMOVE n.embedding"),
                        );
                        live.remove(&id);
                    }
                    _ => {
                        let v = unit_vec(st);
                        embed_param(&graphs, &graph, &name, &v);
                        live.insert(id, v);
                    }
                }
            }
            // Every round but the last is flushed down into a core segment; the last one
            // stays in the write delta, so the query sees all three tiers at once.
            if round < segments {
                graphs.flush_graph_to_segment(&graph, &vc, &root).unwrap();
            }
        }

        for _ in 0..3 {
            let q = unit_vec(st);
            let k = 1 + (next(st) % 6) as usize;
            // Exact brute force over the live set, in the engine's total order (D26:
            // distance ascending, node id ascending on a tie).
            let mut want: Vec<(u64, f64)> = live
                .iter()
                .map(|(id, v)| (*id, 1.0 - crate::vector::cosine_similarity(&q, v)))
                .collect();
            want.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            want.truncate(k);

            let got = vknn(&graphs, &graph, &cache, &q, k);
            assert_eq!(
                got.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                want.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                "seed {seed}, {segments} segment(s), k={k}: the merged top-k must equal the \
                     brute force over the live set\n  got  {got:?}\n  want {want:?}"
            );
            for ((_, gs), (_, ws)) in got.iter().zip(&want) {
                assert!((gs - ws).abs() < 1e-5, "score {gs} vs {ws}");
            }
        }
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Overwriting an indexed embedding with a **non-vector** value takes the node out of the
/// index. The write path admits it (`validate_vector_dims` only constrains a
/// `Value::Vector`), and the newest level then says this node has no embedding — so it has
/// none. Leaving the level below to go on scoring its stale vector is exactly the silent
/// wrongness a removal exists to prevent, at either level.
#[test]
fn a_non_vector_overwrite_takes_the_node_out_of_the_index() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs("vec_levels_scalar", &base);
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let ids = |g: &Graphs| -> Vec<u64> {
        vknn(g, &graph, &cache, &VQ, 3)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };
    assert_eq!(ids(&graphs), vec![0, 1, 2], "all three start in the index");

    // In the delta: node 0's embedding becomes an integer.
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Doc {name:'d00'}) SET n.embedding = 5",
    );
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "the delta says node 0 has no embedding, so its stale base vector must not score"
    );

    // And through a flush, where the segment's *row* is what says so.
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the overwrite flushes");
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "…and it must still not score once the overwrite lives in a core segment"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The flush must decide vector-index membership by the node's **effective label set** —
/// the same question the read fold asks — not by the label the write anchored on.
///
/// They differ on a multi-label node whose business key lives on a label other than the
/// index's, which is an ordinary shape: key on `(:Keyed {name})`, vector index on
/// `(:Doc {embedding})`. Ask the anchor label and the segment's `vec.meta` sidecar names
/// nobody, so the fold's candidate set never sees the node — and since the sidecar is the
/// *only* channel that can express either fact (D12: the row cannot), two writes are
/// silently undone by a background job:
///
/// * a **re-embed** reverts to the stale base vector at the flush;
/// * a **removal** resurfaces the vector the user deleted.
///
/// Both are invisible until someone queries — no error, no panic, no log line.
#[test]
fn a_flush_keys_vector_membership_on_the_effective_labels_not_the_anchor() {
    // Base: node 0 at 0.9, node 1 at 0.2, node 2 at 0.4. Every node is :Doc:Keyed.
    let base: Vec<Vec<f32>> = [0.9, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("vec_levels_anchor", &base, "Keyed");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let ids = |g: &Graphs| -> Vec<u64> {
        vknn(g, &graph, &cache, &VQ, 3)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };
    assert_eq!(ids(&graphs), vec![1, 2, 0], "base order: 0.2, 0.4, 0.9");

    // Re-embed node 0 at distance 0 — through the *Keyed* anchor, while the index is on Doc.
    let v = at_distance(0.0);
    let parts: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
    vwrite(
        &graphs,
        &graph,
        &format!(
            "MATCH (n:Keyed {{name:'d00'}}) SET n.embedding = vecf32([{}])",
            parts.join(", ")
        ),
    );
    assert_eq!(
        ids(&graphs),
        vec![0, 1, 2],
        "the re-embed leads from the delta"
    );
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the re-embed flushes");
    assert_eq!(
        ids(&graphs),
        vec![0, 1, 2],
        "…and must still lead once flushed — a write silently reverting to the stale base \
             vector at a background flush is the worst kind of wrong"
    );

    // The removal half, through the same anchor.
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Keyed {name:'d01'}) REMOVE n.embedding",
    );
    assert_eq!(ids(&graphs), vec![0, 2], "node 1 leaves the index");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the removal flushes");
    assert_eq!(
        ids(&graphs),
        vec![0, 2],
        "…and must stay gone — the flush must carry the removal, or the deleted embedding \
             resurfaces"
    );
    std::fs::remove_dir_all(&root).ok();
}

// ── HIK-116: a label removal takes a node out of that label's vector index ──────────────

/// Run a read query over the merged view and return its rows.
fn vread(graphs: &Graphs, graph: &str, cache: &BlockCache, q: &str) -> Vec<Vec<Val>> {
    let gen = graphs.get(graph).unwrap();
    let snap = DeltaSnapshot::from_memtable(graphs.writer(graph).unwrap().snapshot());
    let view = MergedView::new(gen.as_ref(), snap);
    let ast = parser::parse(q).unwrap();
    let rows = Engine::new(&view, cache).run(&ast).unwrap().rows;
    rows
}

/// `count(:Doc)` over the merged view.
fn doc_count(graphs: &Graphs, graph: &str, cache: &BlockCache) -> i64 {
    match vread(graphs, graph, cache, "MATCH (n:Doc) RETURN count(n) AS c")[0][0] {
        Val::Int(c) => c,
        ref o => panic!("count(:Doc) is not an int: {o:?}"),
    }
}

/// The dumped `(node_id, vector)` set of the consolidation view — what a rebuild indexes.
fn dump_vectors(
    graphs: &Graphs,
    graph: &str,
    cache: &BlockCache,
    dump: &std::path::Path,
) -> Vec<(u64, Vec<f32>)> {
    let gen = graphs.get(graph).unwrap();
    let snap = DeltaSnapshot::from_memtable(graphs.writer(graph).unwrap().snapshot());
    let view = MergedView::new(gen.as_ref(), snap);
    std::fs::create_dir_all(dump).unwrap();
    crate::consolidate::serialise_binary_dump(&Engine::new(&view, cache), &view, dump).unwrap();
    let reader = graph_format::consolidate_dump::DumpReader::open(dump).unwrap();
    let mut out: Vec<(u64, Vec<f32>)> = Vec::new();
    reader
        .for_each_vector(|node_id, _key_id, v| {
            out.push((node_id, v.to_vec()));
            Ok(())
        })
        .unwrap();
    out.sort_by_key(|(id, _)| *id);
    out
}

/// **The bug (HIK-116).** A vector index is scoped to a `(label, property)` pair. A write
/// that drops the label (`REMOVE n:Doc`) must take the node out of the `(:Doc, embedding)`
/// index — scope-symmetric with `SET n:Doc` admitting it — and keep it out across the whole
/// write ladder: delta → T2 flush → T3 merge → consolidation. It must not delete the
/// embedding *value*.
///
/// The node is `:Doc:Keyed`: the vector index is on `:Doc` but the business key (the write's
/// anchor) is on `:Keyed`, so the write drops the very label the index is scoped to while
/// still addressing the node. An indexed embedding is routed out of the row (D12), so a
/// flushed row that lost the label cannot *say* so — the removal rides an explicit channel
/// at each rung (the delta's `labels_removed`, then the segment sidecar, then the merged
/// sidecar, then the consolidation `superseded` set), exactly as a value removal does (D63).
/// Miss any one rung and the answer depends on *which level the node happens to live on*.
///
/// d00 is the query's exact match (cosine distance 0.0), so a resurfaced base vector does
/// not merely reorder the results — it leads them. The assertion bites at every rung.
#[test]
fn removing_the_index_label_evicts_a_node_across_the_whole_ladder() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4, 0.6]
        .iter()
        .map(|d| at_distance(*d))
        .collect();
    let (root, graph) =
        testgen::write_vector_docs_keyed("vec_label_removal_ladder", &base, "Keyed");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let ids = |g: &Graphs| -> Vec<u64> {
        vknn(g, &graph, &cache, &VQ, 4)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };

    assert_eq!(
        ids(&graphs),
        vec![0, 1, 2, 3],
        "base order: 0.0, 0.2, 0.4, 0.6"
    );
    assert_eq!(doc_count(&graphs, &graph, &cache), 4);

    // Drop the :Doc label from d00 — anchored on :Keyed, the index is on :Doc. The value
    // is untouched.
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");

    // Rung 1 — the delta. count(:Doc) and labels(n) already drop (the symptom); the fix is
    // that KNN drops with them rather than leaving d00 the top hit at its stale base vector.
    assert_eq!(doc_count(&graphs, &graph, &cache), 3, "count(:Doc) dropped");
    assert!(
        !ids(&graphs).contains(&0),
        "d00 left the :Doc index at the delta; got {:?}",
        ids(&graphs)
    );
    assert_eq!(
        ids(&graphs),
        vec![1, 2, 3],
        "the other three :Doc nodes remain"
    );

    // Rung 2 — the T2 flush. The removal must ride the segment sidecar; the row cannot
    // express it (D12). Put a second, unrelated flush after it so a compaction has a run.
    // The business key is on :Keyed, so re-embeds anchor there, not on :Doc.
    let keyed_embed = |name: &str, v: &[f32]| {
        let parts: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
        format!(
            "MATCH (n:Keyed {{name:'{name}'}}) SET n.embedding = vecf32([{}])",
            parts.join(", ")
        )
    };
    vwrite(&graphs, &graph, &keyed_embed("d01", &at_distance(0.15)));
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the label removal + re-embed flush");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 1);
    vwrite(&graphs, &graph, &keyed_embed("d02", &at_distance(0.35)));
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("a second segment to fold");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 2);
    assert!(
        !ids(&graphs).contains(&0),
        "d00 must stay gone once flushed into a segment; got {:?}",
        ids(&graphs)
    );

    // Rung 3 — the T3 merge. Fold the two segments; the below-run removal must be carried
    // into the merged sidecar, or d00's base vector resurfaces the moment the segment that
    // suppressed it is folded away.
    graphs
        .compact_graph_segments(&graph, &vc, &root, 0, 2)
        .unwrap();
    assert_eq!(
        graphs.get(&graph).unwrap().stack().segments().len(),
        1,
        "the run folded into one segment"
    );
    assert!(
        !ids(&graphs).contains(&0),
        "d00 must stay gone across the merge; got {:?}",
        ids(&graphs)
    );

    // Rung 4 — the consolidation dump. This reads the level fold (not the raw sidecar union
    // the KNN read path uses), so a segment-level removal that the fold swallowed would
    // resurface *only here*. A node out of :Doc scope must not be indexed by the rebuild.
    let dumped = dump_vectors(&graphs, &graph, &cache, &root.join("_dump"));
    let dumped_ids: Vec<u64> = dumped.iter().map(|(id, _)| *id).collect();
    assert!(
        !dumped_ids.contains(&0),
        "d00 is out of :Doc scope, so the rebuild must not index it — got {dumped_ids:?}"
    );
    assert_eq!(
        dumped_ids,
        vec![1, 2, 3],
        "every still-:Doc node keeps exactly one vector; got {dumped_ids:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **The un-remove.** `REMOVE n:Doc` then `SET n:Doc` puts the node back in the index at its
/// *original* embedding — the value was never deleted, only the label scope changed. This is
/// how "the value is retained" is observable: a base-indexed embedding reads back `Null`
/// through `RETURN n.embedding` (D12 routes it out of the row), so the vector's survival is
/// shown by re-entering scope and finding the same base vector still there.
#[test]
fn re_adding_the_index_label_restores_the_original_vector() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("vec_label_unremove", &base, "Keyed");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let _ = &wal;

    let top =
        |g: &Graphs| -> Option<(u64, f64)> { vknn(g, &graph, &cache, &VQ, 3).first().copied() };
    assert_eq!(top(&graphs), Some((0, 0.0)), "d00 leads at distance 0.0");

    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");
    assert!(
        !vknn(&graphs, &graph, &cache, &VQ, 3)
            .iter()
            .any(|(id, _)| *id == 0),
        "d00 is out of the index while unlabelled"
    );

    // Put the label back. Nothing re-set the embedding, so the value that comes back is the
    // base one — d00 leads at distance 0.0 again.
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) SET n:Doc");
    let restored = top(&graphs).expect("d00 is back in the index");
    assert_eq!(restored.0, 0, "d00 back in the :Doc index");
    assert!(
        restored.1.abs() < 1e-5,
        "…at its original base vector (distance 0.0), got {}",
        restored.1
    );
    std::fs::remove_dir_all(&root).ok();
}

/// Removing an **unrelated** label must not evict a node from a different label's vector
/// index. The node is `:Doc:Keyed`; dropping `:Keyed` leaves it `:Doc`, so it stays in the
/// `(:Doc, embedding)` index — through the delta *and* through a flush (the flush must not
/// mistake an in-scope node for a removed one).
#[test]
fn removing_an_unrelated_label_keeps_the_node_in_the_index() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("vec_unrelated_label", &base, "Keyed");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let leads = |g: &Graphs| -> bool {
        vknn(g, &graph, &cache, &VQ, 3)
            .first()
            .is_some_and(|(id, s)| *id == 0 && s.abs() < 1e-5)
    };
    assert!(leads(&graphs), "d00 leads at distance 0.0");

    // Drop the non-index label. d00 is still :Doc.
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Keyed {name:'d00'}) REMOVE n:Keyed",
    );
    assert!(
        leads(&graphs),
        "removing :Keyed must not evict d00 from the :Doc index (delta)"
    );
    assert_eq!(
        doc_count(&graphs, &graph, &cache),
        3,
        ":Doc membership is unchanged"
    );

    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the unrelated label drop flushes");
    assert!(
        leads(&graphs),
        "removing :Keyed must not evict d00 from the :Doc index (flushed)"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A node whose embedding is removed by **value** *and* whose index label is removed must be
/// gone (either reason suffices), and must not double-count or resurface across a flush. It
/// is a legal, if odd, combination and the two removal channels must compose cleanly.
#[test]
fn value_removal_and_label_removal_compose() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("vec_value_and_label", &base, "Keyed");
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

    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Keyed {name:'d00'}) REMOVE n.embedding",
    );
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "gone via both channels, exactly once"
    );

    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the combined removal flushes");
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "still gone once flushed — the two removals must not resurface each other"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **The headline (HIK-118).** The un-remove across a **flush**: `REMOVE n:Doc` → flush →
/// `SET n:Doc` restores the node at its *original base vector*. HIK-116 made this work while
/// the removal lived in the delta; once the removal is flushed to a segment sidecar, the flat
/// removal could not tell a scope-removal (should un-suppress on re-label) from a value-removal
/// (permanent), so a re-label silently failed to restore the vector. The sidecar now records
/// the removal **kind**, so a `label_removal` un-suppresses when the node re-enters scope.
///
/// Truth is hand-derived: d00 is the query's exact match (cosine distance 0.0), so its return
/// is unambiguous — a leading hit at distance 0.0 *is* the original base vector (nothing
/// re-set the value; a base-indexed embedding reads `Null` via `RETURN`, so re-entering scope
/// is the only observable channel — D12/D64).
#[test]
fn re_adding_the_index_label_restores_the_vector_across_a_flush() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) =
        testgen::write_vector_docs_keyed("vec_unremove_across_flush", &base, "Keyed");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let top =
        |g: &Graphs| -> Option<(u64, f64)> { vknn(g, &graph, &cache, &VQ, 3).first().copied() };

    assert_eq!(top(&graphs), Some((0, 0.0)), "d00 leads at distance 0.0");

    // Leave scope, then flush so the removal lands in a segment sidecar (the delta is retired).
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the label removal flushes into a segment sidecar");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 1);
    assert!(
        !vknn(&graphs, &graph, &cache, &VQ, 3)
            .iter()
            .any(|(id, _)| *id == 0),
        "d00 is out of the :Doc index while unlabelled, and the removal is now flushed"
    );

    // Re-enter scope. The removal is a *label* removal in the sidecar, so re-adding the label
    // must un-suppress d00 and bring back its original base vector — the whole point of D65.
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) SET n:Doc");
    let restored = top(&graphs).expect("d00 is back in the :Doc index after a flushed un-remove");
    assert_eq!(restored.0, 0, "d00 back in the :Doc index across the flush");
    assert!(
        restored.1.abs() < 1e-5,
        "…at its original base vector (distance 0.0), got {}",
        restored.1
    );
    std::fs::remove_dir_all(&root).ok();
}

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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            _ => panic!("expected a write: {q}"),
        }
    };
    let batch = |graphs: &Graphs, q: &str, params: &HashMap<String, Val>| {
        let gen = graphs.get("people").unwrap();
        let writer = graphs.writer("people").unwrap();
        match parser::parse_statement(q).unwrap() {
            parser::ast::Statement::Write(w) => {
                execute_write(&writer, gen.as_ref(), &w, params).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
    write(&graphs, "MATCH (n:Person {name:'Carol'}) DELETE n");
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
    write(&graphs, "MATCH (n:Person {name:'Carol'}) DELETE n");
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

/// Recursively load every file under `root` into a `MemObjectStore`, keyed by its
/// `/`-joined path relative to `root` — the same keys the store abstraction builds.
fn load_dir_into_mem(store: &graph_format::store::mem::MemObjectStore, root: &Path, dir: &Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            load_dir_into_mem(store, root, &path);
        } else {
            let key = path
                .strip_prefix(root)
                .unwrap()
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            store
                .put(&key, &std::fs::read(&path).unwrap(), None)
                .unwrap();
        }
    }
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
        execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
            }
            parser::ast::Statement::WriteEdge(w) => {
                execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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
                execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
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

/// Number of `*.l0` segment files under `<wal>/<graph>/l0/`.
fn l0_count(wal_dir: &Path) -> usize {
    let l0 = wal_dir.join("l0");
    std::fs::read_dir(&l0)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("l0"))
                .count()
        })
        .unwrap_or(0)
}

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
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
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
        execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
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
    let build = |dump: &Path, g: &str, dd: &Path| -> Result<()> {
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
        .consolidate_graph("people", &cache, &vc, &root, build)
        .unwrap();
    assert_eq!(published.0, new_uuid);

    // Retire folded + deleted the L0 level: no level stack, no L0 file.
    let writer = graphs.writer("people").unwrap();
    assert_eq!(writer.l0_len(), 0, "L0 stack cleared by retire");
    assert_eq!(l0_count(&wal_dir), 0, "L0 file deleted by retire");
    assert!(!root.join("people").join(".consolidate.dump").exists());

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
    execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();

    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let build =
        |_d: &Path, _g: &str, _dd: &Path| -> Result<()> { bail!("simulated builder crash") };
    let err = graphs
        .consolidate_graph("people", &cache, &vc, &root, build)
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
    assert!(!root.join("people").join(".consolidate.dump").exists());

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
    execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
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
    let build = |_d: &Path, _g: &str, dd: &Path| -> Result<()> {
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
        .consolidate_graph("people", &cache, &vc, &root, build)
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
    let build = |_d: &Path, _g: &str, dd: &Path| -> Result<()> {
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
        .consolidate_graph("people", &cache, &vc, &root, build)
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
    execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();

    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    // A post-freeze write (Bob's age → 77) applied while the real builder runs must
    // be carried forward onto the new core by retire (Phase 4a).
    let writer_mid = writer.clone();
    let gen_mid = gen0.clone();
    let new = graphs
        .consolidate_graph("people", &cache, &vc, &root, |d, g, dd| {
            let bob = match parser::parse_statement("MATCH (n:Person {name:'Bob'}) SET n.age = 77")
                .unwrap()
            {
                parser::ast::Statement::Write(w) => w,
                _ => unreachable!(),
            };
            execute_write(&writer_mid, gen_mid.as_ref(), &bob, &HashMap::new()).unwrap();
            run_builder(&bin, d, g, dd)
        })
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
    execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();

    graphs
        .consolidate_graph(&graph, &cache, &vc, &root, |d, g, dd| {
            run_builder(&bin, d, g, dd)
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
    execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
    drop(gen0);

    graphs
        .consolidate_graph(&graph, &cache, &vc, &root, |d, g, dd| {
            run_builder(&bin, d, g, dd)
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
        .consolidate_graph(&graph, &cache, &vc, &root, |d, g, dd| {
            run_builder(&bin, d, g, dd)
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
        .consolidate_graph(&graph, &cache, &vc, &root, |d, g, dd| {
            run_builder(&bin, d, g, dd)
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
        .consolidate_graph(&graph, &cache, &vc, &root, |d, g, dd| {
            run_builder(&bin, d, g, dd)
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
    let e = execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new())
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
    execute_write(&writer, gen.as_ref(), &ok, &HashMap::new())
        .expect("an unindexed vector property carries no dimension contract");
    std::fs::remove_dir_all(&root).ok();
}

/// The **Vamana** arm of the same gate — and, since HIK-117, the server-level proof of
/// **carry-by-reference**. A Vamana index's full vectors live in its `.vamana` blocks; the
/// consolidation no longer streams them back out (the ~370 GB read at scale) and no longer
/// rebuilds the graph from zero. Instead the dump carries a reference to the base
/// `.vamana`/`.pq` plus a `layout → new-id` map, and the builder folds the (here empty) Δ in
/// with `streaming_merge`. With no deletes and no Δ that is the pure-permutation fast path,
/// so the new generation's `.vamana` is **byte-identical** to the base's — the whole thesis
/// of the FreshDiskANN write ladder, asserted end-to-end through the dump and the forked
/// builder, not just the in-crate primitive.
#[test]
#[ignore = "spawns the real slater-build binary; see consolidate_via_real_builder"]
fn consolidate_carries_a_vamana_index_out_of_its_vamana_blocks() {
    use graph_format::manifest::AnnMode;

    let bin = std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let work = std::env::temp_dir().join(format!("slater_vamana_consol_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).unwrap();
    let data = work.join("data");
    let wal = work.join("_wal");

    // 400 × dim-16 cosine vectors: enough to clear an `--ann-threshold 50`, and
    // `pq_subspaces = 8` divides 16, so the index is Vamana-eligible (D29).
    let (dim, n) = (16usize, 400usize);
    let mut seed: u64 = 0xDEAD_BEEF_1234;
    let mut next = || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
    };
    let mut script =
        format!("CALL db.idx.vector.createNodeIndex('Doc', 'embedding', {dim}, 'cosine');\n");
    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(n);
    for i in 0..n {
        let v: Vec<f32> = (0..dim).map(|_| next()).collect();
        let body: Vec<String> = v.iter().map(|x| format!("{x:.6}")).collect();
        script.push_str(&format!(
            "CREATE (:Doc:__DumpVertex__ {{__dump_id__: {i}, embedding: vecf32([{}])}});\n",
            body.join(", ")
        ));
        vectors.push(v);
    }
    let input = work.join("dump.cypher");
    std::fs::write(&input, &script).unwrap();

    let ok = std::process::Command::new(&bin)
        .args(["--input", input.to_str().unwrap()])
        .args(["--graph", "docs"])
        .args(["--data-dir", data.to_str().unwrap()])
        .args(["--pk", "__dump_id__"])
        .args(["--cluster", "none"])
        .args(["--ann-threshold", "50"])
        .args(["--pq-subspaces", "8"])
        .args(["--pq-bits", "8"])
        .status()
        .expect("spawn slater-build")
        .success();
    assert!(ok, "the fixture build must succeed");

    let mut graphs = Graphs::open_all(&data, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &data, None)
        .unwrap();
    let cache = BlockCache::new(1 << 22);
    let vc = VectorIndexCache::new(1 << 22);

    let gen0 = graphs.get("docs").unwrap();
    let desc0 = &gen0.manifest().vector_indexes[0];
    assert!(
        matches!(desc0.mode, AnnMode::Vamana { .. }),
        "the fixture must actually be a Vamana index, else this proves nothing"
    );
    assert_eq!(desc0.count, n as u64);
    // Snapshot the base `.vamana` bytes *before* the consolidation, to prove afterward that
    // carry-by-reference left the graph file untouched (the pure-permutation fast path).
    let base_vamana = data
        .join("docs")
        .join(gen0.base_uuid().to_string())
        .join("vector/Doc.embedding.vamana");
    let base_bytes = std::fs::read(&base_vamana).expect("base .vamana must exist");

    graphs
        .consolidate_graph("docs", &cache, &vc, &data, |d, g, dd| {
            run_builder(&bin, d, g, dd)
        })
        .unwrap();

    let gen1 = graphs.get("docs").unwrap();
    let vidx = &gen1.manifest().vector_indexes;
    assert_eq!(
        vidx.len(),
        1,
        "the vector index must survive consolidation of a Vamana graph"
    );
    assert_eq!(
        vidx[0].count, n as u64,
        "every vector must be carried out of the .vamana blocks — a 0 here is the \
             'read the wrong store' bug, which is silent by construction"
    );
    // Carry-by-reference: the index stays Vamana (not rebuilt as brute-force), and its
    // `.vamana` is byte-identical to the base — the graph was carried, not reconstructed.
    assert!(
        matches!(vidx[0].mode, AnnMode::Vamana { .. }),
        "a carried Vamana base must stay Vamana, not be rebuilt as brute-force"
    );
    let new_vamana = data
        .join("docs")
        .join(gen1.base_uuid().to_string())
        .join("vector/Doc.embedding.vamana");
    assert_ne!(
        new_vamana, base_vamana,
        "the consolidation must publish a new generation"
    );
    assert_eq!(
        std::fs::read(&new_vamana).unwrap(),
        base_bytes,
        "a pure-permutation consolidation must carry the .vamana byte-identically — this is \
             the BLAKE3-unchanged thesis at the server level"
    );

    // The data has to be the real thing, not zeros: query with node 7's own embedding
    // and it must come back first, at distance ~0. (`--cluster none` ⇒ dense id == i.)
    let probe: Vec<String> = vectors[7].iter().map(|x| format!("{x:.6}")).collect();
    let view = MergedView::read_only(gen1.as_ref());
    let ast = parser::parse(&format!(
        "CALL db.idx.vector.queryNodes('Doc', 'embedding', 1, vecf32([{}])) \
             YIELD node, score RETURN id(node) AS id, score",
        probe.join(", ")
    ))
    .unwrap();
    // The carried index is Vamana, so serving KNN needs the vector-index cache the
    // consolidation pinned it into (a brute-force rebuild would not have).
    let res = Engine::new(&view, &cache)
        .with_vector_cache(&vc, 96)
        .run(&ast)
        .unwrap();
    assert_eq!(res.rows.len(), 1, "the carried index must return a hit");
    assert!(
        matches!(res.rows[0][0], Val::Int(7)),
        "a node's own embedding must be its own nearest neighbour, got {:?}",
        res.rows[0][0]
    );
    let Val::Float(score) = res.rows[0][1] else {
        panic!("score should be a float");
    };
    assert!(
        score.abs() < 1e-5,
        "an exact match must score ~0 (cosine is scale-invariant, so the .vamana file's \
             normalised vectors round-trip exactly); got {score}"
    );
    std::fs::remove_dir_all(&work).ok();
}

/// Every operation of the write grammar, so a new one cannot be added without being
/// listed here. Each must parse to a mutating statement.
fn every_write_statement() -> Vec<&'static str> {
    vec![
        // ── node writes (the grammar requires a SET or DELETE after the pattern,
        //    so a bare `MERGE (n:L {k:v})` is not a valid statement) ───────────
        "MERGE (n:Person {name:'Dave'}) SET n.age = 1",
        "MATCH (n:Person {name:'Alice'}) SET n.age = 1",
        "MATCH (n:Person {name:'Alice'}) SET n.age = 1, n.city = 'Oslo'",
        "MATCH (n:Person {name:'Alice'}) DELETE n",
        "MATCH (n:Person {name:'Alice'}) DETACH DELETE n",
        // ── batched (write-`UNWIND`) node writes ─────────────────────────────
        "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
        "UNWIND $rows AS r MATCH (n:Person {name: r.name}) SET n.age = r.age",
        "UNWIND $rows AS r MATCH (n:Person {name: r.name}) DELETE n",
        // ── relationship writes ──────────────────────────────────────────────
        "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'})",
        "MERGE (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) SET r.since = 2020",
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r",
        // ── admin: rewrites the served generation ────────────────────────────
        "CALL slater.consolidate()",
    ]
}

fn acl_json(grants: serde_json::Value) -> Acl {
    let json = serde_json::json!({
        "users": { "u": { "passwordArgon2id": hash_password("pw").unwrap(), "grants": grants } }
    });
    Acl::from_json_str(&json.to_string()).unwrap()
}

/// **A read grant must not authorise any write.** Before the writable layer landed the
/// ACL had only `can_read`, so switching on `delta.enabled` would silently have promoted
/// every reader into a writer. Every operation of the write grammar is checked.
#[test]
fn a_read_only_grant_forbids_every_write_operation() {
    let read_only = acl_json(serde_json::json!({ "people": ["read"] }));
    for q in every_write_statement() {
        let stmt = parser::parse_statement(q).unwrap_or_else(|e| panic!("parse {q}: {e}"));
        assert!(
            statement_mutates(&stmt),
            "{q} must be classified as a mutating statement"
        );
        let err = authorize_statement(&read_only, "u", "people", &stmt).expect_err(q);
        assert_eq!(err.code, CODE_FORBIDDEN, "{q}");
        assert!(err.message.contains("write access"), "{q}: {}", err.message);
    }
}

/// The same statements are authorised once the user also holds `write`.
#[test]
fn a_read_write_grant_authorises_every_write_operation() {
    let rw = acl_json(serde_json::json!({ "people": ["read", "write"] }));
    for q in every_write_statement() {
        let stmt = parser::parse_statement(q).unwrap();
        authorize_statement(&rw, "u", "people", &stmt)
            .unwrap_or_else(|e| panic!("read+write must authorise {q}: {}", e.message));
    }
}

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
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new())
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
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
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
    execute_edge_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();

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
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new())
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
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
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
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
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
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
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
        execute_write(&writer, gen.as_ref(), &stmt, &HashMap::new()).unwrap();
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
    execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
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
        per_ip: Arc::new(Mutex::new(HashMap::new())),
        max_per_ip: 0,
        diag: Arc::new(crate::diag::Diagnostics::new(false)),
        conn_limit: Arc::new(Semaphore::new(semaphore_permits(16_384))),
        max_connections: 16_384,
        max_pre_auth_connections: 4_096,
        data_dir: root.clone(),
        builder_bin: builder_bin.to_string(),
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
    execute_write(&writer, gen0.as_ref(), &stmt, &HashMap::new()).unwrap();
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

/// Stand up a ConnCtx over the shared fixture graph + a temp ACL.
/// Per-connection security limits for the test ConnCtx builders. Defaults are
/// generous/on so existing tests are unaffected; the connection-security tests
/// pass tight values to exercise a specific gate.
#[derive(Clone)]
struct TestLimits {
    max_message_bytes: usize,
    max_pre_auth_bytes: usize,
    login_timeout_ms: u64,
    tls_handshake_timeout_ms: u64,
    idle_timeout_ms: u64,
    max_pre_auth_connections: usize,
    max_per_ip: usize,
    max_concurrent_auth: usize,
    max_auth_failures: usize,
    max_concurrent_writes: usize,
    /// Turn the writable layer on for the fixture graph (WAL under `<root>/_wal`), so
    /// the ctx has a `DeltaWriter` and the RUN write arms are reachable.
    writable: bool,
    load_test_diagnostics: bool,
    /// Replace the single-user fixture ACL with one the test writes itself — for the
    /// multi-user grant checks, where "user B holds no read grant" is the point.
    acl_json: Option<serde_json::Value>,
}

impl Default for TestLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 64 * 1024 * 1024,
            max_pre_auth_bytes: 64 * 1024,
            login_timeout_ms: 0, // off by default so unrelated tests never time out
            tls_handshake_timeout_ms: 0,
            idle_timeout_ms: 0,
            max_pre_auth_connections: 4_096,
            max_per_ip: 0,                // unlimited by default
            max_concurrent_auth: 4,       // as in prod
            max_auth_failures: 3,         // as in prod
            max_concurrent_writes: 4,     // as in prod
            writable: false,              // read-only unless a test asks for writes
            load_test_diagnostics: false, // diagnostics off by default, as in prod
            acl_json: None,               // the single-user fixture ACL
        }
    }
}

fn build_ctx(tag: &str) -> (std::path::PathBuf, Arc<ConnCtx>) {
    build_ctx_limited(tag, TestLimits::default())
}

fn build_ctx_limited(tag: &str, limits: TestLimits) -> (std::path::PathBuf, Arc<ConnCtx>) {
    let (root, _graph, _) = testgen::write_basic(tag);
    let acl_path = match &limits.acl_json {
        Some(json) => {
            let path = root.join("acl.json");
            std::fs::write(&path, json.to_string()).unwrap();
            path
        }
        None => write_acl(&root),
    };
    let acl = Arc::new(AclHandle::load(&acl_path).unwrap());
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    if limits.writable {
        graphs
            .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
            .unwrap();
    }
    let graphs = Arc::new(graphs);
    let cache = Arc::new(BlockCache::new(1 << 20));
    let vector_cache = Arc::new(VectorIndexCache::new(1 << 20));
    for gen in graphs.current_generations() {
        for vi in gen.vamana_indexes() {
            vector_cache.pin(gen.uuid(), vi.ord, vi.pq.clone());
        }
    }
    let result_cache = Arc::new(ResultCache::new(1 << 20));
    let ctx = Arc::new(ConnCtx {
        acl,
        graphs,
        cache,
        vector_cache,
        rw_indexes: Arc::new(RwIndexCache::new()),
        rw_index_cfg: crate::rwindex::RwIndexConfig::default(),
        result_cache,
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
        default_graph: None,
        use_selection: RwLock::new(HashMap::new()),
        memgraph_users: RwLock::new(HashSet::new()),
        max_message_bytes: limits.max_message_bytes,
        max_pre_auth_bytes: limits.max_pre_auth_bytes,
        login_timeout_ms: limits.login_timeout_ms,
        tls_handshake_timeout_ms: limits.tls_handshake_timeout_ms,
        idle_timeout_ms: limits.idle_timeout_ms,
        pre_auth_limit: Arc::new(Semaphore::new(semaphore_permits(
            limits.max_pre_auth_connections,
        ))),
        auth_limit: Arc::new(Semaphore::new(semaphore_permits(
            limits.max_concurrent_auth,
        ))),
        max_auth_failures: limits.max_auth_failures,
        write_limit: Arc::new(Semaphore::new(semaphore_permits(
            limits.max_concurrent_writes,
        ))),
        per_ip: Arc::new(Mutex::new(HashMap::new())),
        max_per_ip: limits.max_per_ip,
        diag: Arc::new(crate::diag::Diagnostics::new(limits.load_test_diagnostics)),
        conn_limit: Arc::new(Semaphore::new(semaphore_permits(16_384))),
        max_connections: 16_384,
        max_pre_auth_connections: limits.max_pre_auth_connections,
        data_dir: root.clone(),
        builder_bin: "slater-build".to_string(),
        memtable_bytes: 64 << 20,
        l0_compaction_trigger: 4,
        segment_flush_bytes: 0,
        max_upper_segments: 8,
        segment_gc_grace_secs: 0,
        delta_core_percent: 0,
        delta_hard_bytes: 0,
        consolidate_window: None,
    });
    (root, ctx)
}

/// Recursively copy a (small fixture) directory tree.
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}

/// A ConnCtx serving two graphs (`people` + a copy `places`), with `reporting`
/// granted read on both — exercises the ambiguous (multi-graph) selection path.
fn build_multi_ctx(tag: &str) -> Arc<ConnCtx> {
    let (root, _graph, _) = testgen::write_basic(tag);
    let places = root.join("places");
    copy_dir(&root.join("people"), &places);
    // The manifest records its own graph name (and open_all rejects a mismatch);
    // the data-file content hash excludes MANIFEST.json, so renaming the copied
    // graph to "places" only requires patching that one field.
    for entry in std::fs::read_dir(&places).unwrap() {
        let gen_dir = entry.unwrap().path();
        let man = gen_dir.join("MANIFEST.json");
        if man.exists() {
            let mut v: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
            v["graph"] = serde_json::json!("places");
            std::fs::write(&man, serde_json::to_string_pretty(&v).unwrap()).unwrap();
        }
    }
    let acl_path = root.join("acl.json");
    let json = serde_json::json!({
        "users": { "reporting": {
            "passwordArgon2id": hash_password("pw").unwrap(),
            "grants": { "people": ["read"], "places": ["read"] }
        }}
    });
    std::fs::write(&acl_path, json.to_string()).unwrap();
    let acl = Arc::new(AclHandle::load(&acl_path).unwrap());
    let graphs = Arc::new(Graphs::open_all(&root, None).unwrap());
    Arc::new(ConnCtx {
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
        // A default is configured but must NOT be silently served for queries.
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
        per_ip: Arc::new(Mutex::new(HashMap::new())),
        max_per_ip: 0,
        diag: Arc::new(crate::diag::Diagnostics::new(false)),
        conn_limit: Arc::new(Semaphore::new(semaphore_permits(16_384))),
        max_connections: 16_384,
        max_pre_auth_connections: 4_096,
        data_dir: root.clone(),
        builder_bin: "slater-build".to_string(),
        memtable_bytes: 64 << 20,
        l0_compaction_trigger: 4,
        segment_flush_bytes: 0,
        max_upper_segments: 8,
        segment_gc_grace_secs: 0,
        delta_core_percent: 0,
        delta_hard_bytes: 0,
        consolidate_window: None,
    })
}

#[test]
fn unknown_db_name_errors_and_lists_the_served_graphs() {
    let (_root, ctx) = build_ctx("select_unknown_db");
    let extra = PsValue::Map(vec![("db".into(), PsValue::str("eu-ai-act"))]);
    let err = ctx.select_graph(&extra, "reporting", None).unwrap_err();
    assert_eq!(err.code, CODE_NOT_FOUND);
    assert!(
        err.message.contains("'eu-ai-act' is not served"),
        "{}",
        err.message
    );
    // The real name is offered so a typo is self-correcting.
    assert!(err.message.contains("people"), "{}", err.message);
}

#[test]
fn ambiguous_session_errors_instead_of_silently_serving_the_default() {
    let ctx = build_multi_ctx("select_ambiguous");
    // No `db` field, and `reporting` can read two graphs: must error, not fall
    // back to `default_graph` ("people").
    let empty = PsValue::Map(vec![]);
    let err = ctx.select_graph(&empty, "reporting", None).unwrap_err();
    assert_eq!(err.code, CODE_NOT_FOUND);
    assert!(err.message.contains("no graph selected"), "{}", err.message);
    assert!(
        err.message.contains("people") && err.message.contains("places"),
        "{}",
        err.message
    );
    // An empty (not just absent) db string is treated the same.
    let blank = PsValue::Map(vec![("db".into(), PsValue::str(""))]);
    assert!(ctx.select_graph(&blank, "reporting", None).is_err());
    // Naming an exact, served graph still works.
    let named = PsValue::Map(vec![("db".into(), PsValue::str("places"))]);
    assert_eq!(
        ctx.select_graph(&named, "reporting", None).ok(),
        Some("places".to_string())
    );
}

#[tokio::test]
async fn begin_validates_the_graph_and_remembers_it_for_the_transaction() {
    let ctx = build_multi_ctx("begin_validate");
    let mut sess = Session {
        user: Some("reporting".into()),
        failed: false,
        pending: None,
        tx_graph: None,
        version: (5, 4),
        auth_failures: 0,
        login_deadline: None,
    };
    // BEGIN naming an unserved graph fails at BEGIN, before any RUN.
    let bad = message::Request::Begin(PsValue::Map(vec![("db".into(), PsValue::str("eu-ai-act"))]));
    let err = handle_request(&mut sess, &ctx, bad).await.unwrap_err();
    assert_eq!(err.code, CODE_NOT_FOUND);
    assert!(sess.tx_graph.is_none());
    // BEGIN with no db does NOT bind the transaction — the graph is deferred to
    // the RUN (clients like Memgraph Lab put `db` on the RUN, not the BEGIN). The
    // BEGIN itself succeeds; an unnamed graph only errors if the RUN omits it too.
    let unbound = message::Request::Begin(PsValue::Map(vec![]));
    assert!(handle_request(&mut sess, &ctx, unbound).await.is_ok());
    assert!(sess.tx_graph.is_none());
    // BEGIN naming a served graph is remembered for the transaction's RUNs.
    let good = message::Request::Begin(PsValue::Map(vec![("db".into(), PsValue::str("places"))]));
    assert!(handle_request(&mut sess, &ctx, good).await.is_ok());
    assert_eq!(sess.tx_graph.as_deref(), Some("places"));
    // COMMIT ends the transaction and clears the held graph.
    assert!(handle_request(&mut sess, &ctx, message::Request::Commit)
        .await
        .is_ok());
    assert!(sess.tx_graph.is_none());
}

#[tokio::test]
async fn warm_cache_pulls_blocks_into_a_cold_cache() {
    let (root, ctx) = build_ctx("warm_cache_warms");
    // A fresh block cache holds nothing until something reads.
    assert_eq!(ctx.cache.bytes(), 0, "cache should start cold");
    warm_cache("MATCH (n:Person) RETURN n.name", &ctx).await;
    assert!(
        ctx.cache.bytes() > 0,
        "warming query should have faulted blocks into the cache"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn warm_cache_is_a_noop_when_unset() {
    let (root, ctx) = build_ctx("warm_cache_noop");
    // Empty and whitespace-only both mean "disabled" — neither touches the cache.
    warm_cache("", &ctx).await;
    warm_cache("   \n  ", &ctx).await;
    assert_eq!(
        ctx.cache.bytes(),
        0,
        "an unset warming query must not read anything"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn warm_cache_survives_a_bad_query() {
    let (root, ctx) = build_ctx("warm_cache_bad");
    // A parse error must not panic or abort — it logs and leaves the cache cold.
    warm_cache("THIS IS NOT CYPHER", &ctx).await;
    assert_eq!(ctx.cache.bytes(), 0, "a bad warming query warms nothing");
    // A syntactically valid query against a label that does not exist executes
    // (and warms whatever it scans) without taking the server down.
    warm_cache("MATCH (n:NoSuchLabel) RETURN n", &ctx).await;
    let _ = std::fs::remove_dir_all(&root);
}

/// Spawn the connection handler over a fresh loopback listener, returning the
/// bound address so a client can connect. Goes through `serve_conn` (plaintext),
/// not `handle_connection` directly, so the tests exercise the same admission path
/// as production: the antechamber permit and the login deadline are taken at accept.
async fn spawn_server(ctx: Arc<ConnCtx>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let _ = serve_conn(sock, None, ctx).await;
            });
        }
    });
    addr
}

/// A minimal async Bolt client for the tests.
struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // Handshake: preamble + offer 5.4 then 4.4.
        let mut hs = Vec::new();
        hs.extend_from_slice(&handshake::PREAMBLE);
        hs.extend_from_slice(&[0, 0, 4, 5]);
        hs.extend_from_slice(&[0, 0, 4, 4]);
        hs.extend_from_slice(&[0, 0, 0, 0]);
        hs.extend_from_slice(&[0, 0, 0, 0]);
        stream.write_all(&hs).await.unwrap();
        let mut reply = [0u8; 4];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [0, 0, 4, 5], "should negotiate Bolt 5.4");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    async fn send(&mut self, msg: PsValue) {
        self.stream
            .write_all(&message::to_wire(&msg))
            .await
            .unwrap();
    }

    /// Read the next response message as a decoded struct `(tag, fields)`.
    async fn recv(&mut self) -> (u8, Vec<PsValue>) {
        loop {
            if let Some((body, consumed)) = chunk::decode_message(&self.buf).unwrap() {
                self.buf.drain(..consumed);
                match crate::bolt::packstream::from_slice(&body).unwrap() {
                    PsValue::Struct { tag, fields } => return (tag, fields),
                    other => panic!("expected a struct, got {other:?}"),
                }
            }
            let mut tmp = [0u8; 4096];
            let n = self.stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "server closed unexpectedly");
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    fn hello() -> PsValue {
        PsValue::Struct {
            tag: message::tag::HELLO,
            fields: vec![PsValue::Map(vec![(
                "user_agent".into(),
                PsValue::str("slater-test/1.0"),
            )])],
        }
    }

    fn logon(user: &str, pw: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::LOGON,
            fields: vec![PsValue::Map(vec![
                ("scheme".into(), PsValue::str("basic")),
                ("principal".into(), PsValue::str(user)),
                ("credentials".into(), PsValue::str(pw)),
            ])],
        }
    }

    /// A 4.4-style HELLO carrying auth inline (no separate LOGON).
    fn hello_with_auth(user: &str, pw: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::HELLO,
            fields: vec![PsValue::Map(vec![
                ("user_agent".into(), PsValue::str("slater-test/1.0")),
                ("scheme".into(), PsValue::str("basic")),
                ("principal".into(), PsValue::str(user)),
                ("credentials".into(), PsValue::str(pw)),
            ])],
        }
    }

    fn run(query: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::RUN,
            fields: vec![
                PsValue::str(query),
                PsValue::Map(vec![]),
                PsValue::Map(vec![("db".into(), PsValue::str("people"))]),
            ],
        }
    }

    fn pull_all() -> PsValue {
        PsValue::Struct {
            tag: message::tag::PULL,
            fields: vec![PsValue::Map(vec![("n".into(), PsValue::Int(-1))])],
        }
    }

    fn discard(n: i64) -> PsValue {
        PsValue::Struct {
            tag: message::tag::DISCARD,
            fields: vec![PsValue::Map(vec![("n".into(), PsValue::Int(n))])],
        }
    }

    fn logoff() -> PsValue {
        PsValue::Struct {
            tag: message::tag::LOGOFF,
            fields: vec![],
        }
    }

    /// A RUN that names no `db` — the shape that resolves through `tx_graph`.
    fn run_no_db(query: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::RUN,
            fields: vec![
                PsValue::str(query),
                PsValue::Map(vec![]),
                PsValue::Map(vec![]),
            ],
        }
    }

    /// A BEGIN naming its target graph, which `Request::Begin` resolves into `tx_graph`.
    fn begin_db(graph: &str) -> PsValue {
        PsValue::Struct {
            tag: message::tag::BEGIN,
            fields: vec![PsValue::Map(vec![("db".into(), PsValue::str(graph))])],
        }
    }

    /// Clears the Bolt FAILED state a failed LOGON leaves behind.
    fn reset() -> PsValue {
        PsValue::Struct {
            tag: message::tag::RESET,
            fields: vec![],
        }
    }
}

#[tokio::test]
async fn full_handshake_logon_run_pull_returns_records() {
    let (root, ctx) = build_ctx("server_e2e");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    c.send(Client::run(
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
    ))
    .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    // SUCCESS {fields: ["name"]}.
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![PsValue::str("name")]))
    );

    c.send(Client::pull_all()).await;
    let mut names = Vec::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            if let PsValue::List(vals) = &fields[0] {
                names.push(vals[0].as_str().unwrap().to_string());
            }
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            assert_eq!(fields[0].get("has_more"), Some(&PsValue::Bool(false)));
            break;
        }
    }
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn discard_honours_its_n_and_leaves_the_rest_pending() {
    let (root, ctx) = build_ctx("server_discard_n");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // Three rows pending.
    c.send(Client::run(
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
    ))
    .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // DISCARD n=2 drops two rows without emitting RECORDs and reports has_more.
    c.send(Client::discard(2)).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(fields[0].get("has_more"), Some(&PsValue::Bool(true)));

    // The remaining row is still there: DISCARD -1 drains it and completes.
    c.send(Client::discard(-1)).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(fields[0].get("has_more"), Some(&PsValue::Bool(false)));

    // Buffer drained: a follow-up PULL now errors (no pending result).
    c.send(Client::pull_all()).await;
    assert_eq!(c.recv().await.0, message::tag::FAILURE);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn show_storage_info_includes_per_pool_cache_metrics() {
    let (root, ctx) = build_ctx("server_storage_info");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // Touch the block cache first so its counters are non-trivial.
    c.send(Client::run("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::pull_all()).await;
    while c.recv().await.0 != message::tag::SUCCESS {}

    c.send(Client::run("SHOW STORAGE INFO")).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![
            PsValue::str("storage info"),
            PsValue::str("value")
        ]))
    );

    c.send(Client::pull_all()).await;
    let mut kv: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            if let PsValue::List(vals) = &fields[0] {
                if let (Some(key), PsValue::Int(v)) = (vals[0].as_str(), &vals[1]) {
                    kv.insert(key.to_string(), *v);
                }
            }
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            break;
        }
    }

    // The manifest stats are still there…
    assert!(kv.contains_key("vertex_count"), "manifest rows must remain");
    // …and every pool now reports its full metric set.
    for pool in ["block", "vector", "result"] {
        for metric in ["bytes", "entries", "hits", "misses", "evictions"] {
            let key = format!("{pool}_cache_{metric}");
            assert!(kv.contains_key(&key), "SHOW STORAGE INFO missing `{key}`");
        }
    }
    // The MATCH above went through the block cache, so it recorded an access.
    assert!(
        kv["block_cache_hits"] + kv["block_cache_misses"] >= 1,
        "block cache should show at least one access after the MATCH"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnostics_disabled_by_default_errors() {
    // With `loadTestDiagnostics` off (the default), the statement must fail
    // rather than leak a surface — and no diagnostics state is maintained.
    let (root, ctx) = build_ctx("server_diag_off");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    c.send(Client::run("CALL slater.diagnostics()")).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::FAILURE, "disabled diagnostics must fail");
    // The message should point the operator at the flag.
    let msg = fields[0]
        .get("message")
        .and_then(PsValue::as_str)
        .unwrap_or_default();
    assert!(
        msg.contains("loadTestDiagnostics"),
        "failure should name the flag, got: {msg}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnostics_enabled_returns_health_metrics() {
    // Stand up a server with diagnostics enabled, drive one query so the
    // query counters are non-trivial, then read the snapshot.
    let (root, ctx) = build_ctx_limited(
        "server_diag_on",
        TestLimits {
            load_test_diagnostics: true,
            ..Default::default()
        },
    );
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // A successful query so `queries_ok_total` and a latency sample are recorded.
    c.send(Client::run("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::pull_all()).await;
    while c.recv().await.0 != message::tag::SUCCESS {}

    c.send(Client::run("CALL slater.diagnostics()")).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(
        tag,
        message::tag::SUCCESS,
        "enabled diagnostics must succeed"
    );
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![
            PsValue::str("metric"),
            PsValue::str("value")
        ]))
    );

    c.send(Client::pull_all()).await;
    let mut metrics: std::collections::HashMap<String, PsValue> = std::collections::HashMap::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            if let PsValue::List(vals) = &fields[0] {
                if let Some(key) = vals[0].as_str() {
                    metrics.insert(key.to_string(), vals[1].clone());
                }
            }
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            break;
        }
    }

    // Headline rows are present: process RSS, the cgroup limit (may be -1 when
    // unconstrained), and the echoed connection cap.
    assert!(
        metrics.contains_key("rss_bytes"),
        "snapshot missing rss_bytes"
    );
    assert!(
        metrics.contains_key("cgroup_mem_limit_bytes"),
        "snapshot missing cgroup_mem_limit_bytes"
    );
    assert_eq!(
        metrics.get("conn_limit"),
        Some(&PsValue::Int(16_384)),
        "echoed connection cap should match the configured maxConnections"
    );
    // The MATCH was counted as a completed query.
    match metrics.get("queries_ok_total") {
        Some(PsValue::Int(n)) => assert!(*n >= 1, "expected >=1 ok query, got {n}"),
        other => panic!("queries_ok_total missing or not an int: {other:?}"),
    }
    // A latency percentile was recorded (>= 0; -1 would mean no samples).
    match metrics.get("latency_p50_ms") {
        Some(PsValue::Float(v)) => assert!(*v >= 0.0, "expected a latency sample, got {v}"),
        other => panic!("latency_p50_ms missing or not a float: {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn parse_use_statement_recognises_the_database_switch_forms() {
    assert_eq!(
        parse_use_statement("USE eu_ai_act").as_deref(),
        Some("eu_ai_act")
    );
    assert_eq!(
        parse_use_statement("use database eu_ai_act;").as_deref(),
        Some("eu_ai_act")
    );
    assert_eq!(
        parse_use_statement("  USE   `eu_ai_act` ").as_deref(),
        Some("eu_ai_act")
    );
    assert_eq!(
        parse_use_statement("USE DATABASE \"eu_ai_act\"").as_deref(),
        Some("eu_ai_act")
    );
    // Not a bare USE / malformed → ignored (falls through to the query path).
    assert_eq!(parse_use_statement("MATCH (n) RETURN n"), None);
    assert_eq!(parse_use_statement("USE"), None);
    assert_eq!(parse_use_statement("USE a b"), None);
    assert_eq!(parse_use_statement("USEFUL eu_ai_act"), None);
}

// ── GQL PR 5 — optional `GQL` / `CYPHER` dialect prefix ───────────────────

#[test]
fn strip_dialect_prefix_removes_the_selector_only() {
    // The keyword (any case), with or without a numeric version token, is dropped.
    assert_eq!(
        strip_dialect_prefix("GQL MATCH (n) RETURN n"),
        "MATCH (n) RETURN n"
    );
    assert_eq!(
        strip_dialect_prefix("cypher MATCH (n) RETURN n"),
        "MATCH (n) RETURN n"
    );
    assert_eq!(
        strip_dialect_prefix("CYPHER 25 MATCH (n) RETURN n"),
        "MATCH (n) RETURN n"
    );
    assert_eq!(
        strip_dialect_prefix("  cypher 5.0\n MATCH (n) RETURN n"),
        "MATCH (n) RETURN n"
    );

    // A bare query is returned untouched, and an identifier merely sharing the
    // prefix (`cypher_score`) is never mistaken for a selector.
    assert_eq!(
        strip_dialect_prefix("MATCH (n) RETURN n"),
        "MATCH (n) RETURN n"
    );
    assert_eq!(
        strip_dialect_prefix("RETURN cypher_score"),
        "RETURN cypher_score"
    );
    // `CYPHER` immediately followed by a query keyword (no version) keeps the
    // keyword — only the selector is consumed.
    assert_eq!(strip_dialect_prefix("GQL RETURN 1"), "RETURN 1");
}

#[test]
fn dialect_prefix_parses_to_the_same_ast_as_the_bare_query() {
    // GQL / CYPHER prefixes are pure dialect selectors: after stripping, the
    // remainder parses to the identical AST as the unprefixed query.
    let bare = parser::parse("MATCH (n) RETURN n").unwrap();
    for q in ["GQL MATCH (n) RETURN n", "CYPHER MATCH (n) RETURN n"] {
        let stripped = strip_dialect_prefix(q);
        assert_eq!(parser::parse(stripped).unwrap(), bare, "for {q:?}");
    }
    // A bare query is byte-for-byte unaffected by the strip.
    assert_eq!(
        strip_dialect_prefix("MATCH (n) RETURN n"),
        "MATCH (n) RETURN n"
    );
}

// ── GQL PR 5 — additive GQLSTATUS metadata ────────────────────────────────

#[test]
fn gqlstatus_completion_distinguishes_empty_from_nonempty() {
    // A non-empty result completes `00000`; an empty one is GQL `02000` (no data).
    let nonempty = gqlstatus_completion(3);
    let status = |pairs: &[(String, PsValue)], k: &str| {
        pairs
            .iter()
            .find(|(kk, _)| kk == k)
            .and_then(|(_, v)| v.as_str().map(str::to_string))
    };
    assert_eq!(status(&nonempty, "gql_status").as_deref(), Some("00000"));
    let empty = gqlstatus_completion(0);
    assert_eq!(status(&empty, "gql_status").as_deref(), Some("02000"));
}

#[test]
fn failure_message_keeps_legacy_keys_and_adds_gqlstatus() {
    // Syntax / access-mode errors map to GQL class 42; everything else to 50000.
    assert_eq!(Failure::new(CODE_SYNTAX, "x".into()).gqlstatus().0, "42000");
    assert_eq!(
        Failure::new(CODE_ACCESS_MODE, "x".into()).gqlstatus().0,
        "42000"
    );
    assert_eq!(
        Failure::new(CODE_EXECUTION, "x".into()).gqlstatus().0,
        "50000"
    );

    // The wire FAILURE keeps `code`/`message` and gains the GQLSTATUS pair.
    let PsValue::Struct { tag, fields } = Failure::new(CODE_SYNTAX, "bad".into()).to_message()
    else {
        panic!("expected a Struct");
    };
    assert_eq!(tag, message::tag::FAILURE);
    let PsValue::Map(m) = &fields[0] else {
        panic!("expected a Map");
    };
    for key in ["code", "message", "gql_status", "status_description"] {
        assert!(
            m.iter().any(|(k, _)| k == key),
            "missing metadata key {key}"
        );
    }
}

#[tokio::test]
async fn begin_without_db_defers_to_the_run_graph() {
    // Memgraph Lab's wire shape: an explicit transaction whose BEGIN names no
    // graph, with `db` riding on the RUN inside it. A multi-graph user must still
    // succeed — the unbound BEGIN defers, and the RUN resolves the graph.
    let ctx = build_multi_ctx("begin_defer_run");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // BEGIN with empty metadata (no `db`).
    c.send(PsValue::Struct {
        tag: message::tag::BEGIN,
        fields: vec![PsValue::Map(vec![])],
    })
    .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // RUN carrying the graph in its `db` field.
    c.send(PsValue::Struct {
        tag: message::tag::RUN,
        fields: vec![
            PsValue::str("MATCH (n:Person) RETURN n.name AS name ORDER BY name"),
            PsValue::Map(vec![]),
            PsValue::Map(vec![("db".into(), PsValue::str("places"))]),
        ],
    })
    .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    c.send(Client::pull_all()).await;
    let mut names = Vec::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            if let PsValue::List(vals) = &fields[0] {
                names.push(vals[0].as_str().unwrap().to_string());
            }
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            break;
        }
    }
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
}

#[tokio::test]
async fn returns_node_and_relationship_structures() {
    let (root, ctx) = build_ctx("server_structs");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    c.send(Client::run(
        "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, r",
    ))
    .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::pull_all()).await;

    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::RECORD);
    let row = match &fields[0] {
        PsValue::List(vals) => vals,
        other => panic!("expected a record list, got {other:?}"),
    };
    // Node a: struct 'N' with [id, labels, props, element_id] (Bolt 5).
    match &row[0] {
        PsValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_NODE);
            assert_eq!(fields.len(), 4);
            assert_eq!(
                fields[1],
                PsValue::List(vec![PsValue::str("Person")]),
                "labels"
            );
            assert_eq!(fields[2].get("name"), Some(&PsValue::str("Alice")));
        }
        other => panic!("expected a Node struct, got {other:?}"),
    }
    // Relationship r: struct 'R' with [id, start, end, type, props, +3 element ids].
    match &row[1] {
        PsValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_RELATIONSHIP);
            assert_eq!(fields.len(), 8);
            assert_eq!(fields[1], PsValue::Int(0), "start node id (Alice)");
            assert_eq!(fields[2], PsValue::Int(1), "end node id (Bob)");
            assert_eq!(fields[3], PsValue::str("KNOWS"), "type");
            assert_eq!(fields[4].get("since"), Some(&PsValue::Int(2020)));
        }
        other => panic!("expected a Relationship struct, got {other:?}"),
    }
    // Drain the trailing SUCCESS.
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn returns_path_structure() {
    let (root, ctx) = build_ctx("server_path");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    c.send(Client::run(
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN p",
    ))
    .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::pull_all()).await;

    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::RECORD);
    let row = match &fields[0] {
        PsValue::List(vals) => vals,
        other => panic!("expected a record list, got {other:?}"),
    };
    // Path p: struct 'P' (0x50) with [nodes, rels, indices].
    let (path_tag, path_fields) = match &row[0] {
        PsValue::Struct { tag, fields } => (*tag, fields),
        other => panic!("expected a Path struct, got {other:?}"),
    };
    assert_eq!(path_tag, TAG_PATH);
    assert_eq!(path_fields.len(), 3);

    // Field 0: the two nodes (Alice at index 0, Bob at index 1).
    let nodes = match &path_fields[0] {
        PsValue::List(ns) => ns,
        other => panic!("expected a node list, got {other:?}"),
    };
    assert_eq!(nodes.len(), 2);
    for (n, name) in nodes.iter().zip(["Alice", "Bob"]) {
        match n {
            PsValue::Struct { tag, fields } => {
                assert_eq!(*tag, TAG_NODE);
                assert_eq!(fields[2].get("name"), Some(&PsValue::str(name)));
            }
            other => panic!("expected a Node struct, got {other:?}"),
        }
    }

    // Field 1: one UnboundRelationship (0x72) — [id, type, props, element_id],
    // no endpoint ids (the node list supplies them).
    let rels = match &path_fields[1] {
        PsValue::List(rs) => rs,
        other => panic!("expected a rel list, got {other:?}"),
    };
    assert_eq!(rels.len(), 1);
    match &rels[0] {
        PsValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_UNBOUND_REL);
            assert_eq!(fields.len(), 4); // Bolt 5: id, type, props, element_id
            assert_eq!(fields[0], PsValue::Int(0), "edge id");
            assert_eq!(fields[1], PsValue::str("KNOWS"), "type");
            assert_eq!(fields[2].get("since"), Some(&PsValue::Int(2020)));
        }
        other => panic!("expected an UnboundRelationship struct, got {other:?}"),
    }

    // Field 2: indices weaving the single forward segment — rel 1 (+, forward)
    // into node index 1 (Bob).
    assert_eq!(
        path_fields[2],
        PsValue::List(vec![PsValue::Int(1), PsValue::Int(1)]),
        "path indices"
    );

    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn returns_point2d_structure() {
    let (root, ctx) = build_ctx("server_point");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    c.send(Client::run(
        "RETURN point({latitude: 32.5, longitude: 34.25}) AS p",
    ))
    .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::pull_all()).await;

    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::RECORD);
    let row = match &fields[0] {
        PsValue::List(vals) => vals,
        other => panic!("expected a record list, got {other:?}"),
    };
    // Point2D struct (0x58): [srid::Int=4326, x::Float=longitude, y::Float=latitude].
    match &row[0] {
        PsValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_POINT2D);
            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0], PsValue::Int(4326), "srid");
            assert_eq!(fields[1], PsValue::Float(34.25), "x = longitude");
            assert_eq!(fields[2], PsValue::Float(32.5), "y = latitude");
        }
        other => panic!("expected a Point2D struct, got {other:?}"),
    }

    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    let _ = std::fs::remove_dir_all(&root);
}

// Bolt v2 temporal structs (Date 0x44, LocalTime 0x74, LocalDateTime 0x64,
// Duration 0x45). FalkorDB never wires temporals over Bolt, so this validates
// the published Neo4j PackStream encoding an official driver would decode.
#[tokio::test]
async fn returns_temporal_structures() {
    let (root, ctx) = build_ctx("server_temporal");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    c.send(Client::run(
        "RETURN date('1970-01-02') AS d, localtime({hour:1, minute:0, second:1}) AS t, \
                    localdatetime('1970-01-01T00:00:05') AS dt, \
                    duration({months:2, days:3, hours:1, minutes:0, seconds:4}) AS u",
    ))
    .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::pull_all()).await;

    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::RECORD);
    let row = match &fields[0] {
        PsValue::List(vals) => vals,
        other => panic!("expected a record list, got {other:?}"),
    };

    // Date 0x44: [days] — 1970-01-02 is 1 day past the epoch.
    match &row[0] {
        PsValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_DATE);
            assert_eq!(fields, &vec![PsValue::Int(1)]);
        }
        other => panic!("expected a Date struct, got {other:?}"),
    }
    // LocalTime 0x74: [nanoOfDay] — 01:00:01 = 3601 s.
    match &row[1] {
        PsValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_LOCAL_TIME);
            assert_eq!(fields, &vec![PsValue::Int(3601 * 1_000_000_000)]);
        }
        other => panic!("expected a LocalTime struct, got {other:?}"),
    }
    // LocalDateTime 0x64: [seconds, nanoseconds] — epoch + 5 s.
    match &row[2] {
        PsValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_LOCAL_DATETIME);
            assert_eq!(fields, &vec![PsValue::Int(5), PsValue::Int(0)]);
        }
        other => panic!("expected a LocalDateTime struct, got {other:?}"),
    }
    // Duration 0x45: [months, days, seconds, nanoseconds] — 2mo 3d 1h4s.
    match &row[3] {
        PsValue::Struct { tag, fields } => {
            assert_eq!(*tag, TAG_DURATION);
            assert_eq!(
                fields,
                &vec![
                    PsValue::Int(2),
                    PsValue::Int(3),
                    PsValue::Int(3604),
                    PsValue::Int(0),
                ]
            );
        }
        other => panic!("expected a Duration struct, got {other:?}"),
    }

    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn hello_embedded_auth_authenticates_the_4_4_fallback() {
    let (root, ctx) = build_ctx("server_hello_auth");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    // 4.4-style: credentials ride in HELLO, no separate LOGON.
    c.send(Client::hello_with_auth("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // The connection is authenticated, so RUN/PULL proceed.
    c.send(Client::run("MATCH (n:Person) RETURN count(*) AS c"))
        .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::pull_all()).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::RECORD);
    assert_eq!(fields[0], PsValue::List(vec![PsValue::Int(3)]));
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn bad_password_fails_and_run_before_logon_fails() {
    let (root, ctx) = build_ctx("server_auth");
    let addr = spawn_server(ctx).await;

    // Wrong password → FAILURE on LOGON.
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "wrong")).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::FAILURE);
    assert_eq!(
        fields[0].get("code").and_then(PsValue::as_str),
        Some(CODE_UNAUTHORIZED)
    );

    // RUN before LOGON → FAILURE (unauthenticated).
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::run("MATCH (n) RETURN n")).await;
    assert_eq!(c.recv().await.0, message::tag::FAILURE);
    let _ = std::fs::remove_dir_all(&root);
}

/// A `LOGON` metadata map, as `authenticate` sees it once the message is decoded.
fn logon_meta(user: &str, pw: &str) -> PsValue {
    PsValue::Map(vec![
        ("scheme".into(), PsValue::str("basic")),
        ("principal".into(), PsValue::str(user)),
        ("credentials".into(), PsValue::str(pw)),
    ])
}

/// A freshly handshaken, unauthenticated session (no login deadline).
fn pre_auth_session() -> Session {
    Session {
        user: None,
        failed: false,
        pending: None,
        tx_graph: None,
        version: (5, 4),
        auth_failures: 0,
        login_deadline: None,
    }
}

// ── HIK-123: session state must not outlive the identity it belongs to ──────────
//
// A Bolt connection can carry more than one principal (LOGOFF→LOGON, or a bare
// re-LOGON). Every one of these drives a *real socket* through the actual message
// loop, because the bug lived in the handlers' bookkeeping, not in a helper.

/// An ACL with a reader on the fixture graph and a second user who holds no grant
/// at all — the "next user on the pooled connection".
fn two_user_acl_json() -> serde_json::Value {
    serde_json::json!({
        "users": {
            // A: may read the fixture graph.
            "reporting": {
                "passwordArgon2id": hash_password("pw").unwrap(),
                "grants": { "people": ["read"] }
            },
            // B: authenticates fine, but is granted nothing anywhere.
            "intruder": {
                "passwordArgon2id": hash_password("pw2").unwrap(),
                "grants": {}
            }
        }
    })
}

fn two_user_ctx(tag: &str) -> (std::path::PathBuf, Arc<ConnCtx>) {
    build_ctx_limited(
        tag,
        TestLimits {
            acl_json: Some(two_user_acl_json()),
            ..Default::default()
        },
    )
}

/// (a) The cross-user read: A's buffered rows must not be drainable by B.
///
/// Before the fix, LOGOFF cleared only `sess.user`, so `sess.pending` still held A's
/// rows and `Request::Pull` handed them to B without ever looking at `sess.user`.
#[tokio::test]
async fn logoff_does_not_leave_the_prior_users_rows_for_the_next_user() {
    let (root, ctx) = two_user_ctx("server_hik123_pending");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    // A authenticates and RUNs, buffering rows it never pulls.
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::run("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // A leaves; B takes the same connection.
    c.send(Client::logoff()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("intruder", "pw2")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // B pulls. Any RECORD here is A's data on B's session.
    c.send(Client::pull_all()).await;
    let (tag, _) = c.recv().await;
    assert_ne!(
        tag,
        message::tag::RECORD,
        "PULL after LOGOFF/LOGON returned the previous user's buffered rows"
    );
    assert_eq!(
        tag,
        message::tag::FAILURE,
        "a PULL with no RUN of its own must fail, not succeed silently"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The same leak reached without a LOGOFF at all — `authenticate` deliberately
/// permits re-LOGON on an authenticated session (token rotation), so the identity
/// can change while `pending` survives. Fixing only the LOGOFF handler leaves this
/// path open; it is why the clear lives in `authenticate` too.
#[tokio::test]
async fn a_bare_relogon_does_not_inherit_the_prior_users_rows() {
    let (root, ctx) = two_user_ctx("server_hik123_relogon");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::run("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // No LOGOFF — B simply LOGONs over A.
    c.send(Client::logon("intruder", "pw2")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    c.send(Client::pull_all()).await;
    let (tag, _) = c.recv().await;
    assert_ne!(
        tag,
        message::tag::RECORD,
        "a re-LOGON inherited the previous user's buffered rows"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// (b) The read-ACL bypass: A's open-transaction graph must not carry B's RUN.
///
/// B holds no read grant on `people`, so the *only* way B's db-less RUN can be
/// served is the `tx_graph` arm short-circuiting `select_graph`/`can_read`. Before
/// the fix it did exactly that and returned A's graph.
#[tokio::test]
async fn logoff_does_not_leave_the_prior_users_transaction_graph_for_the_next_user() {
    let (root, ctx) = two_user_ctx("server_hik123_tx_graph");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    // A opens a transaction naming the graph → sess.tx_graph = Some("people").
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::begin_db("people")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // A leaves mid-transaction; B takes the connection.
    c.send(Client::logoff()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("intruder", "pw2")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // B runs a db-less query. It must be refused on B's own (empty) grants.
    c.send(Client::run_no_db("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(
        tag,
        message::tag::FAILURE,
        "a db-less RUN was served from the prior user's transaction graph"
    );
    assert_eq!(
        fields[0].get("code").and_then(PsValue::as_str),
        Some(CODE_FORBIDDEN),
        "the refusal must be an authorization failure on B's grants"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The `tx_graph` arm re-checks the ACL per RUN, not once at BEGIN — so a grant
/// revoked by an ACL hot-reload stops being served *inside* an open transaction,
/// with no identity change involved. Independent of the LOGOFF clear: this one
/// survives a correct session-state handoff.
#[tokio::test]
async fn a_grant_revoked_mid_transaction_stops_serving_reads() {
    let (root, ctx) = two_user_ctx("server_hik123_revoke");
    let acl_path = root.join("acl.json");
    // Hold the handle so the test can drive the reload itself: `snapshot()` does not
    // poll, and hanging the assertion on mtime-granularity polling would make it flaky
    // (or, worse, pass against a stale ACL for the wrong reason).
    let acl = ctx.acl.clone();
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::begin_db("people")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // In-transaction RUN is served while the grant stands.
    c.send(Client::run_no_db("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::pull_all()).await;
    assert_eq!(c.recv().await.0, message::tag::RECORD);
    while c.recv().await.0 == message::tag::RECORD {}

    // The operator revokes the read grant, and the hot-reload picks it up.
    std::fs::write(
        &acl_path,
        serde_json::json!({
            "users": {
                "reporting": {
                    "passwordArgon2id": hash_password("pw").unwrap(),
                    "grants": {}
                }
            }
        })
        .to_string(),
    )
    .unwrap();
    assert!(acl.reload(), "the revoked ACL must install");
    assert!(
        !acl.snapshot().can_read("reporting", "people"),
        "precondition: the grant is gone from the live ACL"
    );

    // The next RUN in the *same* transaction must not ride the BEGIN-time decision.
    c.send(Client::run_no_db("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(
        tag,
        message::tag::FAILURE,
        "a read was served on a grant revoked mid-transaction"
    );
    assert_eq!(
        fields[0].get("code").and_then(PsValue::as_str),
        Some(CODE_FORBIDDEN)
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-90 regression: argon2id must not run on the reactor.
///
/// `#[tokio::test]` is a **current-thread** runtime — the one place a blocked reactor
/// is directly observable. Spawned tasks only advance when the test yields, and a
/// single `yield_now()` gives every ready task exactly one poll. If the verify runs
/// inline (the bug), that one trip through the scheduler costs `FLOOD × one verify`
/// — the whole server is deaf for that long. With the verify handed to a blocking
/// thread, the poll parks immediately and the reactor comes straight back.
///
/// The bound is calibrated against a *measured* verify on this machine and build
/// profile rather than a hard-coded millisecond count, so it neither flakes on a slow
/// box nor passes vacuously on a fast one.
#[tokio::test]
async fn concurrent_logons_do_not_block_the_reactor() {
    const FLOOD: usize = 8;
    let (root, ctx) = build_ctx("server_auth_off_reactor");

    // Calibrate: what one verify costs. An unknown principal deliberately burns a
    // full dummy hash (anti-enumeration), so this is the flood's per-attempt price.
    // The first unknown-principal verify also *mints* the lazy dummy hash — a second
    // argon2 — so warm it before timing anything.
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());
    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());
    let one_verify = t0.elapsed();
    assert!(
        one_verify >= Duration::from_millis(1),
        "argon2id should cost real time; measured {one_verify:?} — is the ACL path being skipped?"
    );

    let flood: Vec<_> = (0..FLOOD)
        .map(|_| {
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let mut sess = pre_auth_session();
                authenticate(&mut sess, &ctx, &logon_meta("nobody", "wrong")).await
            })
        })
        .collect();

    let t0 = Instant::now();
    tokio::task::yield_now().await;
    let reactor_stall = t0.elapsed();
    assert!(
        reactor_stall < one_verify,
        "the reactor was held for {reactor_stall:?} while {FLOOD} LOGONs verified \
             (one verify = {one_verify:?}) — the hash is running on a reactor worker"
    );

    // …and every attempt still failed: this is not a fast-path that skips the hash.
    for t in flood {
        let err = t.await.unwrap().unwrap_err();
        assert_eq!(err.code, CODE_UNAUTHORIZED);
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// The concurrency cap is what stops the fix from simply *moving* the denial of
/// service into tokio's 512-thread blocking pool (which query execution shares).
///
/// Two things are checked. The direct one: while a flood is in flight, no permit is
/// left, and once it drains every permit is back — the permit lives with the hash, not
/// with the caller, so a client that hangs up mid-`LOGON` cannot leak the cap. The
/// corroborating one: 6 verifies under a cap of 2 take several *waves* of a single
/// verify's wall time, i.e. they did not all run at once. (This box has more than two
/// cores, so uncapped they would not serialise on CPU alone.)
#[tokio::test]
async fn concurrent_verifies_are_capped() {
    const FLOOD: usize = 6;
    const CAP: usize = 2;
    let (root, ctx) = build_ctx_limited(
        "server_auth_capped",
        TestLimits {
            max_concurrent_auth: CAP,
            ..Default::default()
        },
    );
    assert_eq!(ctx.auth_limit.available_permits(), CAP);

    // Warm the lazily-minted dummy hash, then time a single verify (see
    // `concurrent_logons_do_not_block_the_reactor`).
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());
    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());
    let one_verify = t0.elapsed();

    let flood: Vec<_> = (0..FLOOD)
        .map(|_| {
            let ctx = ctx.clone();
            tokio::spawn(async move { verify_off_reactor(&ctx, "nobody", "wrong", None).await })
        })
        .collect();
    tokio::task::yield_now().await;
    assert_eq!(
        ctx.auth_limit.available_permits(),
        0,
        "every verify permit should be in use while a flood is queued"
    );

    let t0 = Instant::now();
    for t in flood {
        assert!(!t.await.unwrap().unwrap());
    }
    let elapsed = t0.elapsed();
    // FLOOD/CAP = 3 waves in principle; assert 2, so per-verify variance (the first
    // hash pays the cold 19 MiB allocation) cannot flake it. Uncapped, all FLOOD would
    // run together and this would land near a single verify.
    assert!(
        elapsed >= one_verify * 2,
        "{FLOOD} verifies under a cap of {CAP} finished in {elapsed:?} (one verify = \
             {one_verify:?}) — they cannot have been serialised into waves"
    );
    // The cap is fully released once the flood drains — the permit lives with the
    // hash, not with the caller.
    assert_eq!(ctx.auth_limit.available_permits(), CAP);
    let _ = std::fs::remove_dir_all(&root);
}

/// The unknown-principal path must keep burning a full argon2id verify against the
/// dummy hash: that equalisation is what stops username enumeration by timing, and
/// moving the hash off the reactor must not have "optimised" it away.
#[tokio::test]
async fn unknown_principal_still_pays_for_a_full_verify() {
    let (root, ctx) = build_ctx("server_auth_timing_equalised");

    // Warm the lazily-built dummy hash so its one-off mint is not counted.
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());

    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "reporting", "wrong", None)
        .await
        .unwrap());
    let known_user = t0.elapsed();

    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "no-such-user", "wrong", None)
        .await
        .unwrap());
    let unknown_user = t0.elapsed();

    // Same work, so the same order of magnitude. A skipped verify would be orders of
    // magnitude faster, which is exactly what the enumeration attack looks for.
    assert!(
        unknown_user * 2 >= known_user,
        "an unknown principal took {unknown_user:?} against {known_user:?} for a known \
             one — the timing equalisation is gone"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The login deadline bounds the *wait* for a verify permit — a queued attempt must
/// not outlive the login window it belongs to. But that window is the **pre-auth** one:
/// it must not be turned around and used to refuse a re-auth (LOGON without LOGOFF —
/// token rotation) on a session that authenticated long ago, whose deadline is in the
/// past by construction. (An expired deadline only bites when the acquire actually has
/// to wait; with a permit free, `timeout_at` takes it and never trips.)
#[tokio::test]
async fn an_expired_login_deadline_bounds_the_pre_auth_wait_only() {
    let (root, ctx) = build_ctx_limited(
        "server_auth_deadline",
        TestLimits {
            max_concurrent_auth: 1,
            ..Default::default()
        },
    );
    let expired = TokioInstant::now() - Duration::from_secs(1);

    // Occupy the single verify permit, so the next attempt must queue.
    let hog = {
        let ctx = ctx.clone();
        tokio::spawn(async move { verify_off_reactor(&ctx, "nobody", "wrong", None).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(ctx.auth_limit.available_permits(), 0);

    // Unauthenticated and past its deadline: refused rather than queued — even with
    // the right password, so an anonymous flood cannot sit in the queue for ever.
    let mut anon = pre_auth_session();
    anon.login_deadline = Some(expired);
    let err = authenticate(&mut anon, &ctx, &logon_meta("reporting", "pw"))
        .await
        .unwrap_err();
    assert_eq!(err.code, CODE_UNAUTHORIZED);
    assert!(
        anon.user.is_none(),
        "a timed-out attempt must not authenticate"
    );

    // Already authenticated: the same expired deadline must NOT refuse a re-auth. It
    // waits for the permit (which the hog is holding) and then verifies.
    let mut live = pre_auth_session();
    live.user = Some("reporting".into());
    live.login_deadline = Some(expired);
    authenticate(&mut live, &ctx, &logon_meta("reporting", "pw"))
        .await
        .unwrap();
    assert_eq!(live.user.as_deref(), Some("reporting"));

    assert!(!hog.await.unwrap().unwrap());
    let _ = std::fs::remove_dir_all(&root);
}

/// A connection gets a small allowance of failed LOGONs and is then hung up on, so a
/// single socket cannot keep queueing verifies for its whole login window.
#[tokio::test]
async fn repeated_bad_logons_close_the_connection() {
    let (root, ctx) = build_ctx_limited(
        "server_auth_attempt_cap",
        TestLimits {
            max_auth_failures: 2,
            ..Default::default()
        },
    );
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // A failed LOGON puts the connection in the Bolt FAILED state, so a stuffer must
    // RESET between guesses — that is the attempt loop the cap has to bound.
    c.send(Client::logon("reporting", "wrong")).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::FAILURE);
    assert_eq!(
        fields[0].get("code").and_then(PsValue::as_str),
        Some(CODE_UNAUTHORIZED)
    );
    c.send(Client::reset()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // Second failure spends the allowance: the FAILURE is still reported…
    c.send(Client::logon("reporting", "wrong")).await;
    assert_eq!(c.recv().await.0, message::tag::FAILURE);

    // …and then the server hangs up — RESET does not launder the attempt count, and no
    // further guess on this socket ever reaches the hash.
    let mut tmp = [0u8; 64];
    let n = c.stream.read(&mut tmp).await.unwrap();
    assert_eq!(
        n, 0,
        "the connection should have been closed after 2 failed LOGONs"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A ctx whose fixture graph has the writable layer on, with an explicit
/// `maxConcurrentWrites` cap — the harness for the write-gate tests.
fn build_gated_write_ctx(tag: &str, max_concurrent_writes: usize) -> (PathBuf, Arc<ConnCtx>) {
    build_ctx_limited(
        tag,
        TestLimits {
            writable: true,
            max_concurrent_writes,
            ..Default::default()
        },
    )
}

/// A batched `UNWIND … MERGE … SET` over `rows` fresh business keys, prefixed by `tag`
/// so concurrent jobs never touch the same key. Batched on purpose: it is a *single*
/// write (one resolve sweep, one group commit, one fsync) that costs enough wall time
/// to be measured against, which is what the reactor-stall calibration needs.
fn batch_write_job(tag: &str, rows: usize) -> (WriteJob, HashMap<String, Val>) {
    let list = Val::List(
        (0..rows)
            .map(|i| {
                Val::Map(vec![
                    ("name".into(), Val::Str(format!("{tag}-{i}"))),
                    ("age".into(), Val::Int(i as i64)),
                ])
            })
            .collect(),
    );
    let params = HashMap::from([("rows".to_string(), list)]);
    let stmt = match parser::parse_statement(
        "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
    )
    .unwrap()
    {
        parser::ast::Statement::Write(w) => w,
        _ => panic!("expected a node write"),
    };
    (WriteJob::Node(Box::new(stmt)), params)
}

/// Live node count over the core ⊕ delta — proof a write actually landed.
fn overlaid_node_count(ctx: &Arc<ConnCtx>, graph: &str) -> i64 {
    let gen = ctx.graphs.get(graph).unwrap();
    let writer = ctx.graphs.writer(graph).unwrap();
    let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
    let res = Engine::new(&view, ctx.cache.as_ref())
        .run(&parser::parse("MATCH (n:Person) RETURN count(*)").unwrap())
        .unwrap();
    match res.rows[0][0] {
        Val::Int(n) => n,
        ref v => panic!("count is not an int: {v:?}"),
    }
}

/// HIK-87 regression: write execution must not run on the reactor.
///
/// `#[tokio::test]` is a **current-thread** runtime — the one place a blocked reactor is
/// directly observable. Spawned tasks only advance when the test yields, and a single
/// `yield_now()` gives every ready task exactly one poll. If the writes run inline (the
/// bug), that one trip through the scheduler costs FLOOD × one write — the whole server
/// is deaf for that long, every other connection on that worker included. With the write
/// handed to a blocking thread, each poll parks immediately and the reactor comes
/// straight back.
///
/// The bound is calibrated against a *measured* write on this box and build profile
/// rather than a hard-coded millisecond, so it neither flakes on a slow machine nor
/// passes vacuously on a fast one.
#[tokio::test]
async fn writes_do_not_block_the_reactor() {
    const FLOOD: usize = 8;
    const ROWS: usize = 500;
    let (root, ctx) = build_gated_write_ctx("server_writes_off_reactor", 4);
    let gen = ctx.graphs.get("people").unwrap();
    let writer = ctx.graphs.writer("people").expect("writable layer is on");

    // Calibrate: what one write of this shape costs. Warm first — the first write mints
    // the WAL segment and faults in the ISAM blocks the resolve sweeps.
    let (job, params) = batch_write_job("warm", ROWS);
    execute_write_off_reactor(&ctx, &writer, &gen, job, params)
        .await
        .unwrap();
    let (job, params) = batch_write_job("calibrate", ROWS);
    let t0 = Instant::now();
    execute_write_off_reactor(&ctx, &writer, &gen, job, params)
        .await
        .unwrap();
    let one_write = t0.elapsed();
    assert!(
        one_write >= Duration::from_millis(1),
        "a {ROWS}-row group commit should cost real time; measured {one_write:?} — is the \
             write actually resolving and fsyncing?"
    );

    // Build the jobs up front: parsing and materialising the rows is caller-side work,
    // and it must not be confused with the execution we are timing.
    let jobs: Vec<_> = (0..FLOOD)
        .map(|i| batch_write_job(&format!("flood-{i}"), ROWS))
        .collect();
    let flood: Vec<_> = jobs
        .into_iter()
        .map(|(job, params)| {
            let ctx = ctx.clone();
            let writer = writer.clone();
            let gen = gen.clone();
            tokio::spawn(async move {
                execute_write_off_reactor(&ctx, &writer, &gen, job, params).await
            })
        })
        .collect();

    let t0 = Instant::now();
    tokio::task::yield_now().await;
    let reactor_stall = t0.elapsed();
    assert!(
        reactor_stall < one_write,
        "the reactor was held for {reactor_stall:?} while {FLOOD} writes executed (one \
             write = {one_write:?}) — write execution is running on a reactor worker"
    );

    // …and every write still committed: this is not a fast path that skipped the work.
    for t in flood {
        t.await.unwrap().unwrap();
    }
    assert_eq!(
        overlaid_node_count(&ctx, "people"),
        3 + ((FLOOD + 2) * ROWS) as i64,
        "3 fixture people + every row of the warm, calibration and flood batches"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The concurrency cap is what stops the fix from simply *moving* the denial of service
/// into tokio's 512-thread blocking pool — the pool query execution runs on. Writes to
/// one graph serialise behind that graph's single `DeltaWriter` lock, so an uncapped
/// `spawn_blocking` would hand the pool an unbounded queue of tasks that immediately
/// park on a mutex, and reads would starve behind them.
///
/// While a flood is in flight no permit is left; once it drains every permit is back —
/// and every write committed, in one graph's serialised order.
#[tokio::test]
async fn concurrent_writes_are_capped() {
    const FLOOD: usize = 6;
    const CAP: usize = 2;
    const ROWS: usize = 500;
    let (root, ctx) = build_gated_write_ctx("server_writes_capped", CAP);
    assert_eq!(ctx.write_limit.available_permits(), CAP);
    let gen = ctx.graphs.get("people").unwrap();
    let writer = ctx.graphs.writer("people").expect("writable layer is on");

    let jobs: Vec<_> = (0..FLOOD)
        .map(|i| batch_write_job(&format!("capped-{i}"), ROWS))
        .collect();
    let flood: Vec<_> = jobs
        .into_iter()
        .map(|(job, params)| {
            let ctx = ctx.clone();
            let writer = writer.clone();
            let gen = gen.clone();
            tokio::spawn(async move {
                execute_write_off_reactor(&ctx, &writer, &gen, job, params).await
            })
        })
        .collect();
    tokio::task::yield_now().await;
    assert_eq!(
        ctx.write_limit.available_permits(),
        0,
        "every write permit should be in use while a flood is queued"
    );

    for t in flood {
        t.await.unwrap().unwrap();
    }
    // The cap is fully released once the flood drains — the permit lives with the write,
    // not with the caller — and no write was lost to the gate.
    assert_eq!(ctx.write_limit.available_permits(), CAP);
    assert_eq!(
        overlaid_node_count(&ctx, "people"),
        3 + (FLOOD * ROWS) as i64,
        "every capped write committed its whole batch"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A `spawn_blocking` task cannot be cancelled: if the client hangs up mid-write, the
/// await on the join handle is dropped but the write runs to completion — as it must,
/// since its WAL append and fsync may already have happened (we never un-commit a
/// durable write; we simply never get to ack it).
///
/// So the permit is moved *into* the closure. Held in the async frame instead, it would
/// be released the instant the caller was cancelled while the write still ran — and a
/// flood of clients that disconnect mid-write could overrun the cap at will, which is
/// exactly the blocking-pool starvation the cap exists to prevent.
#[tokio::test]
async fn an_abandoned_write_holds_its_permit_and_still_commits() {
    const ROWS: usize = 2_000;
    const CAP: usize = 2;
    let (root, ctx) = build_gated_write_ctx("server_write_abandoned", CAP);
    let gen = ctx.graphs.get("people").unwrap();
    let writer = ctx.graphs.writer("people").expect("writable layer is on");

    let (job, params) = batch_write_job("abandoned", ROWS);
    let task = {
        let ctx = ctx.clone();
        let writer = writer.clone();
        let gen = gen.clone();
        tokio::spawn(
            async move { execute_write_off_reactor(&ctx, &writer, &gen, job, params).await },
        )
    };
    // One poll: the permit is taken and the write is handed to the blocking pool.
    tokio::task::yield_now().await;
    assert_eq!(ctx.write_limit.available_permits(), CAP - 1);

    // The client hangs up: the caller is cancelled, the write is not.
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        ctx.write_limit.available_permits(),
        CAP - 1,
        "an abandoned write must keep its permit while it is still running — releasing it \
             at cancellation lets a hung-up client overrun the cap"
    );

    // It runs to completion and its rows are durable, permit released only then.
    while ctx.write_limit.available_permits() < CAP {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        overlaid_node_count(&ctx, "people"),
        3 + ROWS as i64,
        "the abandoned write committed; a durable write is never rolled back because the \
             client stopped listening"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn write_query_is_rejected_read_only() {
    let (root, ctx) = build_ctx("server_readonly");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    c.send(Client::run("CREATE (n:Person {name: 'Mallory'})"))
        .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::FAILURE);
    assert_eq!(
        fields[0].get("code").and_then(PsValue::as_str),
        Some(CODE_ACCESS_MODE)
    );

    // After a FAILURE the connection is FAILED: a further RUN is IGNORED until RESET.
    c.send(Client::run("MATCH (n) RETURN n")).await;
    assert_eq!(c.recv().await.0, message::tag::IGNORED);
    c.send(PsValue::Struct {
        tag: message::tag::RESET,
        fields: vec![],
    })
    .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn vector_knn_query_returns_nodes_and_scores_over_bolt() {
    let (root, ctx) = build_ctx("server_knn");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    // Query equals Alice's embedding → Alice (id 0) is the nearest, score ~0.
    c.send(Client::run(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node) AS id, score",
    ))
    .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![
            PsValue::str("id"),
            PsValue::str("score")
        ]))
    );

    c.send(Client::pull_all()).await;
    let mut ids = Vec::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            if let PsValue::List(vals) = &fields[0] {
                ids.push(vals[0].as_int().unwrap());
                // First hit is the exact match: score ~0.
                if ids.len() == 1 {
                    match &vals[1] {
                        PsValue::Float(f) => assert!(f.abs() < 1e-6, "exact match score ~0"),
                        other => panic!("score should be a float, got {other:?}"),
                    }
                }
            }
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            break;
        }
    }
    assert_eq!(ids, vec![0, 2], "Alice (exact) then Carol");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn meta_stats_procedure_returns_counts_over_bolt() {
    // Phase 11: a metadata CALL flows through the normal RUN/PULL query path
    // (it is NOT a pre-parse interception), so its Map output is PackStream-
    // encoded like any other value.
    let (root, ctx) = build_ctx("server_metastats");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    c.send(Client::run(
        "CALL db.meta.stats() YIELD labels, nodeCount, relCount RETURN labels, nodeCount, relCount",
    ))
    .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![
            PsValue::str("labels"),
            PsValue::str("nodeCount"),
            PsValue::str("relCount"),
        ]))
    );

    c.send(Client::pull_all()).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::RECORD);
    let PsValue::List(vals) = &fields[0] else {
        panic!("expected a record list, got {:?}", fields[0]);
    };
    // labels is a {label: count} map; nodeCount/relCount are the scalar totals.
    assert_eq!(vals[0].get("Person"), Some(&PsValue::Int(3)));
    assert_eq!(vals[0].get("Company"), Some(&PsValue::Int(2)));
    assert_eq!(vals[1].as_int(), Some(5));
    assert_eq!(vals[2].as_int(), Some(5));

    let (tag, _) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn whole_graph_reltype_metadata_over_bolt() {
    // The unanchored introspection queries that broke the incident, answered
    // over the wire from resident metadata. Fixture: KNOWS×3, WORKS_AT×2.
    let (root, ctx) = build_ctx("server_reltype_meta");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    // A1 — DISTINCT type(r): one column `t`, one record per reltype.
    c.send(Client::run("MATCH ()-[r]->() RETURN DISTINCT type(r) AS t"))
        .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![PsValue::str("t")]))
    );
    c.send(Client::pull_all()).await;
    let mut types = Vec::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            let PsValue::List(vals) = &fields[0] else {
                panic!("expected a record list, got {:?}", fields[0]);
            };
            types.push(vals[0].as_str().unwrap().to_string());
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            break;
        }
    }
    types.sort();
    assert_eq!(types, vec!["KNOWS".to_string(), "WORKS_AT".to_string()]);

    // B1 — type(r), count(*): two columns, per-reltype edge counts.
    c.send(Client::run(
        "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c",
    ))
    .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![PsValue::str("t"), PsValue::str("c")]))
    );
    c.send(Client::pull_all()).await;
    let mut counts = std::collections::HashMap::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            let PsValue::List(vals) = &fields[0] else {
                panic!("expected a record list, got {:?}", fields[0]);
            };
            counts.insert(
                vals[0].as_str().unwrap().to_string(),
                vals[1].as_int().unwrap(),
            );
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            break;
        }
    }
    assert_eq!(counts.get("KNOWS"), Some(&3));
    assert_eq!(counts.get("WORKS_AT"), Some(&2));
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn identical_query_is_served_from_the_result_cache() {
    let (root, ctx) = build_ctx("server_resultcache");
    let addr = spawn_server(ctx.clone()).await;

    let drive = move |query: &'static str| async move {
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::logon("reporting", "pw")).await;
        c.recv().await;
        c.send(Client::run(query)).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;
        let mut rows = 0;
        loop {
            let (tag, _) = c.recv().await;
            if tag == message::tag::RECORD {
                rows += 1;
            } else {
                break;
            }
        }
        rows
    };

    let q = "MATCH (n:Person) RETURN n.name AS name ORDER BY name";
    let first = drive(q).await;
    let after_first = ctx.result_cache.metrics();
    assert_eq!(after_first.misses, 1, "first run is a cache miss");
    assert_eq!(ctx.result_cache.len(), 1);

    let second = drive(q).await;
    let after_second = ctx.result_cache.metrics();
    assert_eq!(first, second, "both runs return the same row count");
    assert_eq!(after_second.misses, 1, "second run adds no miss");
    assert!(after_second.hits >= 1, "second run is a cache hit");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn nondeterministic_query_bypasses_the_result_cache() {
    let (root, ctx) = build_ctx("server_resultcache_nd");
    let addr = spawn_server(ctx.clone()).await;

    let drive = move |query: &'static str| async move {
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::logon("reporting", "pw")).await;
        c.recv().await;
        c.send(Client::run(query)).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;
        loop {
            let (tag, _) = c.recv().await;
            if tag != message::tag::RECORD {
                break;
            }
        }
    };

    // A query calling timestamp() is never written to (or read from) the cache.
    let q = "RETURN timestamp() AS t";
    drive(q).await;
    drive(q).await;
    let m = ctx.result_cache.metrics();
    assert_eq!(
        ctx.result_cache.len(),
        0,
        "non-deterministic query is not cached"
    );
    assert_eq!(m.hits, 0, "no cache hit for a non-deterministic query");

    // Sanity: a deterministic query in the same context still caches normally.
    drive("RETURN 1 AS one").await;
    assert_eq!(ctx.result_cache.len(), 1, "deterministic query is cached");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn open_all_discovers_the_fixture_graph() {
    let (root, _graph, _) = testgen::write_basic("server_openall");
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs.names(), vec!["people".to_string()]);
    assert!(graphs.get("people").is_some());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn tls_acceptor_is_none_when_disabled() {
    let cfg = TlsConfig::default();
    assert!(!cfg.enabled());
    assert!(build_tls_acceptor(&cfg).unwrap().is_none());
}

// ── Generation guard (M8) ──────────────────────────────────────────────

/// Recursively copy `src` to `dst` (files + subdirectories).
fn copy_dir_all(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

/// Copy `graph`'s live generation directory to a fresh UUID, optionally
/// truncating `corrupt` (a path relative to the generation dir) in the copy to
/// simulate a half-rsynced generation, then republish `current` to name the new
/// UUID. Returns the new UUID.
///
/// The copy's MANIFEST is restamped with the new `build_uuid`, because a generation is
/// no longer identified by its `current` pointer alone: HIK-144 requires the MANIFEST to
/// agree that it *is* the generation the set names, so that a directory cannot be
/// swapped underneath an authenticated set. A real publisher (the builder) writes that
/// field itself; only this hand-rolled copy has to restamp it.
fn publish_copy_as_new_generation(root: &Path, graph: &str, corrupt: Option<&str>) -> uuid::Uuid {
    let graph_dir = root.join(graph);
    let old = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let new_uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_00ff);
    let src = graph_dir.join(old.trim());
    let dst = graph_dir.join(new_uuid.to_string());
    copy_dir_all(&src, &dst);
    {
        let man = dst.join("MANIFEST.json");
        let mut m: graph_format::manifest::Manifest =
            serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
        m.build_uuid = GenId(new_uuid);
        std::fs::write(&man, m.to_json().unwrap()).unwrap();
    }
    if let Some(rel) = corrupt {
        let victim = dst.join(rel);
        let mut bytes = std::fs::read(&victim).unwrap();
        bytes.truncate(bytes.len().saturating_sub(16));
        std::fs::write(&victim, bytes).unwrap();
    }
    std::fs::write(
        graph_dir.join("current"),
        format!("{}\n", new_uuid.hyphenated()),
    )
    .unwrap();
    new_uuid
}

/// Exactly the swap the generation guard performs on a graph it is allowed to swap:
/// take the graph's swap mutex, then adopt whatever `current` names. `guard_sweep`
/// inlines this (it must hold the same lock across its *decision*, not just the
/// swap), so this is how a test makes the guard's swap happen at a chosen instant —
/// including inside another operation's publish window.
fn guard_swap(graphs: &Graphs, name: &str, vc: &VectorIndexCache) -> Result<Option<GenId>> {
    let _swap = graphs.swap_lock(name)?;
    graphs.swap_locked(name, vc)
}

#[test]
fn swap_refuses_a_truncated_new_generation() {
    let (root, _g, old) = testgen::write_basic("guard_swap_refuse");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // A half-copied (truncated) new generation is published under `current`.
    publish_copy_as_new_generation(&root, "people", Some("node_props.blk"));
    let err = guard_swap(&graphs, "people", &vc).err().unwrap();
    assert!(
        err.chain().any(|e| e.to_string().contains("integrity")),
        "unexpected error: {err:#}"
    );
    // The live generation is untouched — the corrupt copy never took over.
    assert_eq!(graphs.get("people").unwrap().uuid().0, old);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn swap_applies_a_valid_new_generation_while_in_flight_reads_the_old() {
    let (root, _g, old) = testgen::write_basic("guard_swap_apply");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // An in-flight query's snapshot, taken before the swap.
    let in_flight = graphs.get("people").unwrap();

    let new = publish_copy_as_new_generation(&root, "people", None);
    let swapped = guard_swap(&graphs, "people", &vc).unwrap();
    assert_eq!(swapped.map(|g| g.0), Some(new));

    // New queries see the new generation; the in-flight handle still reads old.
    assert_eq!(graphs.get("people").unwrap().uuid().0, new);
    assert_eq!(in_flight.uuid().0, old);

    // A second swap with no further change on disk is a clean no-op.
    assert!(guard_swap(&graphs, "people", &vc).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&root);
}

/// The publish primitive that consolidation, flush and compaction all share. Flush and
/// compaction have no injectable seam inside their publish window (they write `current`
/// themselves), so the property the guard race turns on is asserted here directly: once
/// an op has published a generation, `adopt_published_generation` gives it the same
/// answer — *this is the served generation* — whether the op swapped it in itself or
/// the guard got there first. It never reports "nothing was published" for a generation
/// that was.
#[test]
fn adopt_published_generation_is_idempotent_after_a_racing_swap() {
    let (root, _g, old) = testgen::write_basic("adopt_after_race");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // Nothing published: the answer is the generation already served. This is how a
    // caller detects a builder that exited 0 without publishing anything (it compares
    // against the core it started from) — the check the old `Ok(None)` used to make.
    assert_eq!(
        graphs
            .adopt_published_generation("people", &vc)
            .unwrap()
            .uuid()
            .0,
        old,
        "an unchanged pointer reports the served generation"
    );

    // Now an op publishes, and the guard wins the swap. The op must still get *its own*
    // generation back — the whole point: its post-swap cleanup is keyed off this answer.
    let new = publish_copy_as_new_generation(&root, "people", None);
    assert_eq!(
        guard_swap(&graphs, "people", &vc).unwrap().map(|g| g.0),
        Some(new),
        "the guard swapped first"
    );
    assert_eq!(
        graphs
            .adopt_published_generation("people", &vc)
            .unwrap()
            .uuid()
            .0,
        new,
        "the op adopts the generation it published, whoever performed the swap"
    );
    assert_eq!(graphs.get("people").unwrap().uuid().0, new);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn exit_strategy_guard_sweep_signals_shutdown_on_change() {
    let (root, _g, _) = testgen::write_basic("guard_exit_sweep");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // No change yet → keep serving.
    assert!(matches!(
        guard_sweep(&graphs, &vc, ReloadStrategy::Exit, None),
        SweepAction::Continue
    ));

    // A changed `current` → shutdown signal naming the graph. Exit does not even
    // open the new generation — the orchestrator restart re-opens it cleanly.
    publish_copy_as_new_generation(&root, "people", None);
    match guard_sweep(&graphs, &vc, ReloadStrategy::Exit, None) {
        SweepAction::Shutdown(name) => assert_eq!(name, "people"),
        SweepAction::Continue => panic!("expected a shutdown signal on a changed current"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn swap_strategy_guard_sweep_swaps_in_place() {
    let (root, _g, old) = testgen::write_basic("guard_swap_sweep");
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    let new = publish_copy_as_new_generation(&root, "people", None);
    assert!(matches!(
        guard_sweep(&graphs, &vc, ReloadStrategy::Swap, None),
        SweepAction::Continue
    ));
    assert_ne!(new, old);
    assert_eq!(graphs.get("people").unwrap().uuid().0, new);
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-93: the generation guard must not adopt an `acl.json` that fails the served
/// ACL stamp. The swap's own policy check hashes the live `acl.json` (read #1); the
/// post-swap ACL adopt is a *second* read, and if it re-reads the file unconditionally
/// (the old `reload()`), the bytes it loads need not be the bytes read #1 verified — a
/// check-then-load TOCTOU. The adopt now goes through `reload_checked`, which hashes
/// the exact bytes it loads and installs them only when that digest still matches every
/// served generation's stamp, so the bytes loaded are the bytes checked.
///
/// The race is made deterministic with two graphs and no threads: `people` is stamped
/// against `acl.json` bytes A (it pins the ACL); `docs` is unstamped and is the graph
/// the guard legitimately swaps (its swap policy check passes regardless of the live
/// ACL). Before the sweep, `acl.json` is tampered to bytes B (a `secret` self-grant),
/// standing in for a file that changed after read #1. The guard swaps `docs` and then
/// adopts the ACL: the old unconditional `reload()` adopted B unverified (self-grant
/// live); `reload_checked` re-checks B against `people`'s stamp `digest(A)`, mismatches,
/// and keeps the last-good ACL.
#[test]
fn guard_swap_refuses_a_stamp_violating_acl_after_the_swap() {
    // Two graphs in one root: `people` (from the fixture) and a copy `docs`. The
    // manifest embeds the graph name, so re-stamp the copy's `graph` field to "docs"
    // (a field content_hash does not cover, and the plaintext fixture carries no MAC).
    let (root, _g, _) = testgen::write_basic("guard_acl_toctou");
    copy_dir_all(&root.join("people"), &root.join("docs"));
    patch_manifest(&root, "docs", "graph", serde_json::json!("docs"));

    // acl.json bytes A grant `reporting`/`pw` read on `people`. Stamp only `people`
    // with digest(A); `docs` stays unstamped, so the guard may swap it freely.
    let acl_path = write_acl(&root);
    let digest_a = graph_format::integrity::hash_file(&acl_path).unwrap();
    patch_manifest(&root, "people", "aclBlake3", serde_json::json!(digest_a));

    let acl = AclHandle::load(&acl_path).unwrap();
    assert!(acl.snapshot().can_read("reporting", "people"));
    assert!(!acl.snapshot().can_read("reporting", "secret"));

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.set_manifest_policy(Some(acl_path.clone()), false);
    let vc = VectorIndexCache::new(1 << 20);

    // Publish a fresh generation for the *unstamped* `docs`, so the guard swaps it.
    publish_copy_as_new_generation(&root, "docs", None);

    // Tamper acl.json to bytes B: a `secret` self-grant. digest(B) != digest(A), so B
    // violates `people`'s served stamp (a fresh argon2 salt alone already diverges A).
    let tampered = serde_json::json!({
        "users": { "reporting": { "passwordArgon2id": hash_password("pw").unwrap(),
            "grants": { "people": ["read"], "secret": ["read"] } } }
    });
    std::fs::write(&acl_path, tampered.to_string()).unwrap();

    // Sweep: swaps `docs`, then adopts the ACL through the stamp gate.
    assert!(matches!(
        guard_sweep(&graphs, &vc, ReloadStrategy::Swap, Some(&acl)),
        SweepAction::Continue
    ));
    // `docs` really was swapped, so the adopt path ran.
    assert_ne!(
        graphs.get("docs").unwrap().uuid().0,
        graphs.get("people").unwrap().uuid().0,
        "the guard should have swapped docs to its new generation"
    );

    // The stamp-violating self-grant must NOT have been adopted (pre-fix: it was),
    // and the last-good stamp-matching ACL keeps serving.
    assert!(
        !acl.snapshot().can_read("reporting", "secret"),
        "guard must not adopt an acl.json that violates a served generation's stamp"
    );
    assert_eq!(
        acl.digest(),
        digest_a,
        "the stamp-matching last-good ACL must be kept"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn swap_moves_pinned_pq_from_the_old_generation_to_the_new() {
    let f = testgen::VamanaFixture {
        n: 64,
        dim: 8,
        r: 16,
        alpha: 1.2,
        pq_subspaces: 4,
        pq_bits: 6,
        vector_block_size: 1024,
    };
    let (root, _g, _) = testgen::write_vamana("guard_swap_pq", &f);
    let graphs = Graphs::open_all(&root, None).unwrap();
    let vc = VectorIndexCache::new(1 << 20);

    // Pin the live generation's resident PQ, as `serve` does at startup.
    let old = graphs.get("docs").unwrap();
    for vi in old.vamana_indexes() {
        vc.pin(old.uuid(), vi.ord, vi.pq.clone());
    }
    assert!(vc.resident_pq(old.uuid(), 0).is_some());

    let new = publish_copy_as_new_generation(&root, "docs", None);
    guard_swap(&graphs, "docs", &vc).unwrap();

    // The new generation's PQ is now pinned and the old generation's released —
    // so the pool's resident set tracks the live generation (D32).
    assert!(
        vc.resident_pq(GenId(new), 0).is_some(),
        "new generation PQ should be pinned"
    );
    assert!(
        vc.resident_pq(old.uuid(), 0).is_none(),
        "old generation PQ should be unpinned after swap"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn exit_strategy_guard_task_signals_shutdown_over_oneshot() {
    let (root, _g, _) = testgen::write_basic("guard_exit_task");
    let graphs = Arc::new(Graphs::open_all(&root, None).unwrap());
    let vc = Arc::new(VectorIndexCache::new(1 << 20));
    let (tx, rx) = tokio::sync::oneshot::channel();
    // A tight poll interval so the test does not wait the production default.
    spawn_generation_guard(
        graphs.clone(),
        vc,
        ReloadStrategy::Exit,
        Duration::from_millis(20),
        tx,
        None,
    );

    publish_copy_as_new_generation(&root, "people", None);
    let reason = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("guard should fire within the timeout")
        .expect("the shutdown sender should not be dropped");
    assert_eq!(reason, "people");
    let _ = std::fs::remove_dir_all(&root);
}

// ── Connection-security limits ────────────────────────────────────────────

#[test]
fn semaphore_permits_maps_zero_to_unlimited() {
    assert_eq!(semaphore_permits(0), Semaphore::MAX_PERMITS);
    assert_eq!(semaphore_permits(5), 5);
}

#[test]
fn per_ip_key_keeps_ipv4_and_masks_ipv6_to_64() {
    use std::net::{IpAddr, Ipv4Addr};
    let v4: IpAddr = Ipv4Addr::new(203, 0, 113, 5).into();
    assert_eq!(per_ip_key(v4), v4, "IPv4 keys on the full /32");

    let a: IpAddr = "2001:db8:1:2:3:4:5:6".parse().unwrap();
    let b: IpAddr = "2001:db8:1:2:ffff:ffff:ffff:ffff".parse().unwrap();
    assert_eq!(per_ip_key(a), per_ip_key(b), "same /64 ⇒ same key");
    let c: IpAddr = "2001:db8:1:3::1".parse().unwrap();
    assert_ne!(
        per_ip_key(a),
        per_ip_key(c),
        "different /64 ⇒ different key"
    );
}

#[test]
fn try_acquire_per_ip_caps_and_releases() {
    use std::net::{IpAddr, Ipv4Addr};
    let map: Arc<Mutex<HashMap<IpAddr, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let key: IpAddr = Ipv4Addr::LOCALHOST.into();
    let g1 = try_acquire_per_ip(&map, key, 2).expect("first slot");
    let g2 = try_acquire_per_ip(&map, key, 2).expect("second slot");
    assert!(
        try_acquire_per_ip(&map, key, 2).is_none(),
        "third is over the cap"
    );
    drop(g1);
    let g3 = try_acquire_per_ip(&map, key, 2).expect("a freed slot is reusable");
    drop(g2);
    drop(g3);
    assert!(
        map.lock().unwrap().is_empty(),
        "the map drains to empty once all sources disconnect"
    );
}

#[tokio::test]
async fn framed_enforces_the_body_cap_and_a_larger_cap_admits_the_same_message() {
    use tokio::io::duplex;
    // A single ~1000-byte chunked message (len header + body + 00 00 terminator).
    let body = vec![0xABu8; 1000];
    let mut wire = Vec::new();
    wire.extend_from_slice(&(body.len() as u16).to_be_bytes());
    wire.extend_from_slice(&body);
    wire.extend_from_slice(&[0, 0]);

    // Under a 256-byte cap the framer refuses it before allocating the body.
    let (mut client, server) = duplex(1 << 16);
    client.write_all(&wire).await.unwrap();
    let mut framed = Framed::new(server, 256);
    assert!(
        framed.read_message().await.is_err(),
        "a 1000-byte message must be refused under a 256-byte cap"
    );

    // The identical bytes are accepted once the cap is raised (the post-auth case).
    let (mut client, server) = duplex(1 << 16);
    client.write_all(&wire).await.unwrap();
    let mut framed = Framed::new(server, 4096);
    let got = framed
        .read_message()
        .await
        .unwrap()
        .expect("a full message");
    assert_eq!(got, body);
}

#[tokio::test]
async fn login_deadline_closes_an_idle_unauthenticated_connection() {
    let (_root, ctx) = build_ctx_limited(
        "login_deadline",
        TestLimits {
            login_timeout_ms: 200,
            ..Default::default()
        },
    );
    let addr = spawn_server(ctx).await;
    // Connect but never send the handshake: the server must close us out.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 4];
    match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {} // clean EOF or reset — both mean "closed"
        Ok(Ok(n)) => panic!("server sent {n} bytes to an unauthenticated idle peer"),
        Err(_) => panic!("server did not close the idle pre-auth connection in time"),
    }
}

#[tokio::test]
async fn pre_auth_cap_is_tight_then_relaxes_after_login() {
    let (_root, ctx) = build_ctx_limited(
        "diff_cap",
        TestLimits {
            max_pre_auth_bytes: 512,
            max_message_bytes: 1 << 20,
            ..Default::default()
        },
    );
    let addr = spawn_server(ctx).await;

    // Pre-auth: a HELLO whose user-agent body blows past 512 bytes is refused —
    // the connection closes before the message is decoded.
    {
        let mut c = Client::connect(addr).await;
        let huge = "x".repeat(4000);
        c.send(PsValue::Struct {
            tag: message::tag::HELLO,
            fields: vec![PsValue::Map(vec![(
                "user_agent".into(),
                PsValue::str(&huge),
            )])],
        })
        .await;
        let mut buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(2), c.stream.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => {}
            Ok(Ok(n)) => {
                panic!("server accepted a {n}-byte reply to an oversized pre-auth msg")
            }
            Err(_) => panic!("server did not reject the oversized pre-auth message"),
        }
    }

    // Post-auth: the same connection, once authenticated, accepts a RUN whose
    // parameter map far exceeds the pre-auth cap (proving the ratchet).
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    let pad = "x".repeat(4000); // > 512-byte pre-auth cap, < 1 MiB post-auth cap
    c.send(PsValue::Struct {
        tag: message::tag::RUN,
        fields: vec![
            PsValue::str("RETURN 1 AS one"),
            PsValue::Map(vec![("pad".into(), PsValue::str(&pad))]),
            PsValue::Map(vec![("db".into(), PsValue::str("people"))]),
        ],
    })
    .await;
    assert_eq!(
        c.recv().await.0,
        message::tag::SUCCESS,
        "a large post-auth message must be read, not rejected by the pre-auth cap"
    );
}

#[tokio::test]
async fn pre_auth_budget_rejects_excess_anonymous_connections() {
    let (_root, ctx) = build_ctx_limited(
        "pre_auth_budget",
        TestLimits {
            max_pre_auth_connections: 1,
            ..Default::default()
        },
    );
    let addr = spawn_server(ctx).await;

    // A holds the only antechamber slot (handshake done, not yet authenticated).
    let _a = Client::connect(addr).await;

    // B is accepted at TCP level but the handler rejects it for lack of a slot,
    // so its handshake never completes.
    let mut b = TcpStream::connect(addr).await.unwrap();
    let mut hs = Vec::new();
    hs.extend_from_slice(&handshake::PREAMBLE);
    hs.extend_from_slice(&[0, 0, 4, 5]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    let _ = b.write_all(&hs).await;
    let mut reply = [0u8; 4];
    match tokio::time::timeout(Duration::from_secs(2), b.read_exact(&mut reply)).await {
        Ok(Err(_)) => {} // EOF / reset: rejected as expected
        Ok(Ok(_)) => panic!("second anonymous connection should have been rejected"),
        Err(_) => panic!("server neither served nor rejected the excess anon connection"),
    }
}

#[tokio::test]
async fn global_connection_cap_blocks_until_a_slot_frees() {
    let (_root, ctx) = build_ctx_limited("global_cap", TestLimits::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_limit = Arc::new(Semaphore::new(1)); // exactly one slot
    let (_tx, rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(accept_loop(listener, ctx, None, conn_limit, rx));

    // First client takes the only slot.
    let a = Client::connect(addr).await;
    // Second cannot be serviced while at capacity (the permit is taken before
    // accept, so the server never reads B's handshake).
    assert!(
        tokio::time::timeout(Duration::from_millis(300), Client::connect(addr))
            .await
            .is_err(),
        "a second connection must not be serviced while at capacity"
    );
    // Freeing the first frees the slot.
    drop(a);
    tokio::time::timeout(Duration::from_secs(2), Client::connect(addr))
        .await
        .expect("a slot must free once the first connection closes");
}

/// A throwaway self-signed acceptor, minted in-process — no key material in the repo
/// and nothing to expire. The TLS tests below never validate the chain (the client
/// side is a raw socket), so a bare `localhost` leaf is all the server needs.
fn test_tls_acceptor() -> TlsAcceptor {
    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(issued.key_pair.serialize_der());
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![issued.cert.der().clone()], key.into())
        .unwrap();
    TlsAcceptor::from(Arc::new(config))
}

/// HIK-72. A peer that completes TCP and then never sends a ClientHello must be torn
/// down and must not hold a connection slot while it stalls.
///
/// The regression this pins has two halves, and the test fails on either:
///
/// 1. **Ordering.** The antechamber permit is taken at `accept()`, so a socket still
///    inside the TLS handshake is *counted* against `maxPreAuthConnections`. When the
///    permit was taken behind the handshake (in `handle_connection`), anonymous TLS
///    sockets were uncounted and could occupy the entire global pool — the plaintext
///    path's headroom guarantee simply did not exist on the TLS path.
/// 2. **Liveness.** The handshake is bounded, so the slot comes back. With exactly one
///    global permit, B is served only if A's stalled handshake is actually torn down;
///    before the fix A held it forever and the accept loop stopped draining the queue.
///
/// `loginTimeoutMs` is deliberately **off** here: the handshake bound has to stand on
/// its own, or the guard would evaporate for any operator who widened the login window.
#[tokio::test]
async fn a_stalled_tls_handshake_does_not_hold_a_connection_permit() {
    let (_root, ctx) = build_ctx_limited(
        "tls_slow_loris",
        TestLimits {
            // 1s, sampled at 200ms below: a 5× margin, so a loaded CI box cannot
            // tear A down before the mid-handshake assertion gets to look at it.
            tls_handshake_timeout_ms: 1_000,
            login_timeout_ms: 0, // off on purpose — the handshake bound stands alone
            max_pre_auth_connections: 8,
            ..Default::default()
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_limit = Arc::new(Semaphore::new(1)); // exactly one slot to fight over
    let (_tx, rx) = tokio::sync::oneshot::channel::<String>();
    let gauges = ctx.clone();
    tokio::spawn(accept_loop(
        listener,
        ctx,
        Some(test_tls_acceptor()),
        conn_limit,
        rx,
    ));

    // A: completes the TCP handshake, then says nothing at all. Never a ClientHello.
    let _slow_loris = TcpStream::connect(addr).await.unwrap();

    // Half 1 — while A is stalled *mid-handshake*, it is already accounted for: it
    // holds an antechamber slot. (The global pool is not worth asserting on: the
    // accept loop reserves its next permit *before* parking in `accept()`, so with a
    // pool of one, "none available" is just as true of an idle server.)
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        gauges.pre_auth_limit.available_permits(),
        7,
        "a socket stalled mid-ClientHello must hold a pre-auth slot — the permit is \
             taken at accept, not behind the TLS handshake"
    );

    // Half 2 — B gets served only once A's handshake is torn down and its permits are
    // released. Note connecting proves nothing: the kernel completes the TCP handshake
    // into the listen backlog even while the accept loop is parked on `conn_limit`. So
    // B speaks, and waits to be spoken to — a rustls server that has actually accepted
    // the socket answers a bogus ClientHello with an alert (or closes); a server whose
    // accept loop is starved leaves B's read hanging forever, which is the pre-fix
    // behaviour this test is here to catch.
    let mut b = TcpStream::connect(addr).await.unwrap();
    b.write_all(b"\x16\x03\x01\x00\x05not a real ClientHello")
        .await
        .unwrap();
    let mut buf = [0u8; 8];
    tokio::time::timeout(Duration::from_secs(5), b.read(&mut buf))
        .await
        .expect(
            "the accept loop never came back round: the stalled TLS handshake is still \
                 holding the only connection permit",
        )
        .ok();

    // And A is gone, not merely overtaken: the slot it held comes *back*. It was torn
    // down at the deadline rather than held for as long as the attacker cares to keep
    // the socket open — which is the whole claim. (B is on its way out too, having
    // been sent an alert, so wait for the pool to settle rather than sampling it.)
    let settled = tokio::time::timeout(Duration::from_secs(5), async {
        while gauges.pre_auth_limit.available_permits() < 8 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "the stalled handshake never released its antechamber slot ({} still in use)",
        8 - gauges.pre_auth_limit.available_permits(),
    );
}

/// HIK-103. A peer that completes the pre-auth handshake and then never drains its
/// receive window must not park the server in a pre-auth write while holding an
/// antechamber permit — the login deadline has to bound the *writes* of that window,
/// not only its reads (HIK-72 covered the reads).
///
/// The mock stream hands over a valid ClientHello and then returns `Poll::Pending` on
/// every write: a zero-window client that reads nothing back. Two halves, and the fix
/// is what makes the first pass:
///   * **bounded** — with a login deadline set, `handle_connection` is torn down at the
///     deadline (a [`WriteDeadlineExceeded`]) and the antechamber permit comes back.
///     Before the fix the write ignored the deadline and this hung for the full 5s.
///   * **unbounded** — with no deadline (the pre-fix write behaviour on *every* path),
///     the write parks and the permit stays held, proving the stall and the permit-hold
///     are real and that the deadline is precisely what releases them.
#[tokio::test]
async fn a_stalled_pre_auth_write_is_bounded_by_the_login_deadline() {
    /// Delivers a fixed ClientHello, then reads nothing more and never accepts a write —
    /// a peer with a zero receive window that has stopped draining the socket.
    struct StallWriter {
        hello: Vec<u8>,
        read_off: usize,
    }
    impl AsyncRead for StallWriter {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let remaining = &this.hello[this.read_off..];
            if remaining.is_empty() {
                // Handshake delivered; now silent. (The write blocks first regardless.)
                return std::task::Poll::Pending;
            }
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.read_off += n;
            std::task::Poll::Ready(Ok(()))
        }
    }
    impl AsyncWrite for StallWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Pending // zero receive window: never accepts a byte
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    // A valid 20-byte ClientHello: preamble + four version proposals (5.4 first).
    let client_hello = || {
        let mut h = Vec::new();
        h.extend_from_slice(&handshake::PREAMBLE);
        h.extend_from_slice(&[0, 0, 4, 5]);
        h.extend_from_slice(&[0, 0, 0, 0]);
        h.extend_from_slice(&[0, 0, 0, 0]);
        h.extend_from_slice(&[0, 0, 0, 0]);
        h
    };
    let (_root, ctx) = build_ctx("hik103_stalled_pre_auth_write");

    // Bounded: a login deadline is set, so the stalled reply write is torn down at it
    // and the antechamber permit is released. (`handle_connection` owns the permit, so
    // returning drops it.) Pre-fix, the write ignored the deadline and this hung 5s.
    let sem = Arc::new(Semaphore::new(1));
    let permit = sem.clone().try_acquire_owned().unwrap();
    assert_eq!(sem.available_permits(), 0, "the permit is held on entry");
    let pre_auth = PreAuth {
        permit: Some(permit),
        deadline: Some(TokioInstant::now() + Duration::from_millis(200)),
    };
    let stream = StallWriter {
        hello: client_hello(),
        read_off: 0,
    };
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        handle_connection(stream, ctx.clone(), pre_auth),
    )
    .await
    .expect("the stalled pre-auth write was never torn down at the login deadline");
    let err =
        outcome.expect_err("a stalled pre-auth write must surface an error, not close cleanly");
    assert!(
        err.downcast_ref::<WriteDeadlineExceeded>().is_some(),
        "the teardown must be the write-deadline breach, got: {err:?}"
    );
    assert_eq!(
        sem.available_permits(),
        1,
        "the antechamber permit must come back once the stalled write is torn down"
    );

    // Unbounded (the pre-fix write behaviour): no deadline, so the write parks forever
    // and keeps its permit. Sampled while the task is still alive — proof that the stall
    // and the permit-hold are real, and that the deadline above is what tears them down.
    let sem2 = Arc::new(Semaphore::new(1));
    let permit2 = sem2.clone().try_acquire_owned().unwrap();
    let pre_auth2 = PreAuth {
        permit: Some(permit2),
        deadline: None,
    };
    let stream2 = StallWriter {
        hello: client_hello(),
        read_off: 0,
    };
    let ctx2 = ctx.clone();
    let task = tokio::spawn(async move { handle_connection(stream2, ctx2, pre_auth2).await });
    tokio::time::sleep(Duration::from_millis(300)).await; // clears the handshake read, parks in the write
    assert!(
        !task.is_finished(),
        "with no write deadline the pre-auth write parks — the pre-fix behaviour"
    );
    assert_eq!(
        sem2.available_permits(),
        0,
        "a parked pre-auth write keeps holding its antechamber permit"
    );
    task.abort();
    let _ = std::fs::remove_dir_all(&_root);
}

/// The TLS handshake is bounded by whichever of the two deadlines lands first, and
/// by either one alone when the other is off.
#[tokio::test]
async fn tls_handshake_deadline_is_the_sooner_of_the_two_bounds() {
    let deadline_ms = |login_timeout_ms, tls_handshake_timeout_ms| {
        let (_root, ctx) = build_ctx_limited(
            "tls_deadline",
            TestLimits {
                login_timeout_ms,
                tls_handshake_timeout_ms,
                ..Default::default()
            },
        );
        let pre_auth = PreAuth::admit(&ctx).expect("antechamber is empty");
        let now = TokioInstant::now();
        pre_auth
            .tls_deadline(&ctx)
            .map(|dl| (dl - now).as_millis() as u64)
    };
    // The login window is the whole pre-auth budget, so a handshake bound inside it
    // is what binds; a login window shorter than the handshake bound overrides it.
    assert!(matches!(deadline_ms(10_000, 5_000), Some(ms) if (4_900..=5_000).contains(&ms)));
    assert!(matches!(deadline_ms(1_000, 5_000), Some(ms) if (900..=1_000).contains(&ms)));
    // Either alone still bounds the handshake. The `loginTimeoutMs = 0` row is the
    // one that matters: it is why the handshake gets its own knob at all.
    assert!(matches!(deadline_ms(0, 5_000), Some(ms) if (4_900..=5_000).contains(&ms)));
    assert!(matches!(deadline_ms(10_000, 0), Some(ms) if (9_900..=10_000).contains(&ms)));
    // Both off = unbounded. Documented as "do not".
    assert_eq!(deadline_ms(0, 0), None);
}

#[tokio::test]
async fn per_ip_cap_rejects_excess_from_one_source() {
    let (_root, ctx) = build_ctx_limited(
        "per_ip_cap",
        TestLimits {
            max_per_ip: 1,
            ..Default::default()
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_limit = Arc::new(Semaphore::new(1024)); // generous; isolate the per-IP gate
    let (_tx, rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(accept_loop(listener, ctx, None, conn_limit, rx));

    // First connection from 127.0.0.1 is fine.
    let _a = Client::connect(addr).await;
    // A second from the same source is accepted then dropped by the per-IP cap.
    let mut b = TcpStream::connect(addr).await.unwrap();
    let mut hs = Vec::new();
    hs.extend_from_slice(&handshake::PREAMBLE);
    hs.extend_from_slice(&[0, 0, 4, 5]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    let _ = b.write_all(&hs).await;
    let mut reply = [0u8; 4];
    match tokio::time::timeout(Duration::from_secs(2), b.read_exact(&mut reply)).await {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("second connection from the same source should be rejected"),
        Err(_) => panic!("server neither served nor rejected the per-IP excess"),
    }
}

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
