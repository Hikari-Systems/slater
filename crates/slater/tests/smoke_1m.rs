// SPDX-License-Identifier: Apache-2.0
//! Writable-layer smoke test against a real, large (≈1M-node) served generation.
//!
//! Serves an existing generation (pointed at by env vars) with the writable layer
//! enabled, then drives the deferred-follow-up features over Bolt against real data:
//! delete-a-born-node-by-key, moved-indexed-value relocation, and edge properties.
//! `#[ignore]`: it needs a prebuilt generation on disk, so it is opt-in.
//!
//! ```text
//! SLATER_SMOKE_DATADIR=/home/rickk/perf-gens/wiki1m SLATER_SMOKE_GRAPH=wiki1m \
//!   cargo test -p slater --test smoke_1m -- --ignored --nocapture
//! ```
//! The fixture is Wikidata-shaped: `:Entity` keyed by an indexed `wikidata_id` (plus a
//! `name`), joined by `:LINK` edges.

use slater::bolt::client::BoltClient;
use std::path::Path;
use std::time::Duration;
use tokio::net::TcpListener;

fn write_acl(root: &Path, graph: &str) -> std::path::PathBuf {
    let path = root.join("acl.json");
    let hash = slater::acl::hash_password("pw").unwrap();
    let json = serde_json::json!({
        "users": {
            "smoke": { "passwordArgon2id": hash, "grants": { graph: ["read", "write"] } }
        }
    });
    std::fs::write(&path, json.to_string()).unwrap();
    path
}

/// Run a query and return its string rows (column 0). Panics on a Bolt failure — a
/// happy-path write/read must never error in the smoke run.
fn strs(c: &mut BoltClient, graph: &str, q: &str) -> Vec<String> {
    let (_cols, rows) = c
        .run_pull(q, Some(graph))
        .unwrap_or_else(|e| panic!("query failed: {q}\n  {e}"));
    rows.iter()
        .filter_map(|r| r.first().and_then(|v| v.as_str().map(str::to_string)))
        .collect()
}

/// Run a query and return its integer rows (column 0).
fn ints(c: &mut BoltClient, graph: &str, q: &str) -> Vec<i64> {
    let (_cols, rows) = c
        .run_pull(q, Some(graph))
        .unwrap_or_else(|e| panic!("query failed: {q}\n  {e}"));
    rows.iter()
        .filter_map(|r| r.first().and_then(|v| v.as_int()))
        .collect()
}

fn exec(c: &mut BoltClient, graph: &str, q: &str) {
    c.run_pull(q, Some(graph))
        .unwrap_or_else(|e| panic!("write failed: {q}\n  {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a prebuilt generation; see SLATER_SMOKE_DATADIR"]
async fn writable_layer_smoke_on_a_large_core() {
    let data_dir = std::env::var("SLATER_SMOKE_DATADIR")
        .expect("set SLATER_SMOKE_DATADIR to a slater data directory");
    let graph = std::env::var("SLATER_SMOKE_GRAPH").unwrap_or_else(|_| "wiki1m".to_string());

    let scratch = std::env::temp_dir().join(format!("slater_smoke_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let acl_path = write_acl(&scratch, &graph);
    let wal_dir = scratch.join("wal");

    let cfg = serde_json::json!({
        "server": { "bind": "127.0.0.1", "port": 0 },
        "dataBackend": { "kind": "fs", "fs": { "dir": data_dir } },
        "aclPath": acl_path.to_str().unwrap(),
        "requireAclStamp": false,
        "reloadStrategy": "exit",
        "generationPollMs": 600000,
        "log": { "level": "warn" },
        "delta": { "enabled": true, "walDir": wal_dir.to_str().unwrap() }
    });
    let cfg: slater::config::AppConfig = serde_json::from_value(cfg).expect("build AppConfig");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        if let Err(e) = slater::server::serve_with_listener(cfg, listener).await {
            eprintln!("server ended: {e:#}");
        }
    });
    tokio::time::sleep(Duration::from_millis(800)).await;

    let g = graph.clone();
    let joined = tokio::task::spawn_blocking(move || {
        let mut c = BoltClient::connect("127.0.0.1", port, Duration::from_secs(30))
            .expect("connect");
        c.login("smoke-test/1", "smoke", "pw").expect("login");

        // ── Baseline: an indexed seek on the real core reads back through the overlay.
        let base = strs(&mut c, &g, "MATCH (n:Entity {wikidata_id: 412684}) RETURN n.name");
        println!("baseline 412684.name = {base:?}");
        assert_eq!(base, vec!["maldonite"], "known core entity reads back");

        // ── Item 1: create a delta-born node by an unused key, then DELETE it by key.
        let born_id = 990000000123_i64;
        exec(&mut c, &g, &format!(
            "MERGE (n:Entity {{wikidata_id: {born_id}}}) SET n.name = 'SmokeBornNode'"
        ));
        let after_create = strs(&mut c, &g, &format!(
            "MATCH (n:Entity {{wikidata_id: {born_id}}}) RETURN n.name"
        ));
        assert_eq!(after_create, vec!["SmokeBornNode"], "born node reads back by indexed key");
        exec(&mut c, &g, &format!(
            "MATCH (n:Entity {{wikidata_id: {born_id}}}) DELETE n"
        ));
        let after_delete = strs(&mut c, &g, &format!(
            "MATCH (n:Entity {{wikidata_id: {born_id}}}) RETURN n.name"
        ));
        assert!(after_delete.is_empty(), "born node gone after DELETE by key: {after_delete:?}");
        println!("item 1 (delete born node by key): OK");

        // ── Item 2: move a CORE node's indexed value (patch its wikidata_id) and confirm
        // the range index relocates it: found at the new key, missed at the old.
        let moved_to = 990000000456_i64;
        let name_412685 = strs(&mut c, &g, "MATCH (n:Entity {wikidata_id: 412685}) RETURN n.name");
        assert_eq!(name_412685, vec!["gemtuzumab ozogamicin"], "core entity 412685 present");
        exec(&mut c, &g, &format!(
            "MATCH (n:Entity {{wikidata_id: 412685}}) SET n.wikidata_id = {moved_to}"
        ));
        let at_new = strs(&mut c, &g, &format!(
            "MATCH (n:Entity {{wikidata_id: {moved_to}}}) RETURN n.name"
        ));
        assert_eq!(at_new, vec!["gemtuzumab ozogamicin"], "seek at the NEW indexed value finds it");
        let at_old = strs(&mut c, &g, "MATCH (n:Entity {wikidata_id: 412685}) RETURN n.wikidata_id");
        assert!(at_old.is_empty(), "seek at the OLD indexed value misses it: {at_old:?}");
        // Read back the moved value as the effective wikidata_id.
        let moved_val = ints(&mut c, &g, &format!(
            "MATCH (n:Entity {{wikidata_id: {moved_to}}}) RETURN n.wikidata_id"
        ));
        assert_eq!(moved_val, vec![moved_to], "the moved node's key reads the new value");
        println!("item 2 (moved indexed value): OK");

        // ── Item 3: create a delta-born LINK edge between two core entities with a
        // property, and read the property back through a traversal.
        exec(&mut c, &g, &format!(
            "MERGE (a:Entity {{wikidata_id: 412684}})-[r:LINK]->(b:Entity {{wikidata_id: {moved_to}}}) SET r.weight = 7"
        ));
        let w = ints(&mut c, &g, &format!(
            "MATCH (a:Entity {{wikidata_id: 412684}})-[r:LINK]->(b:Entity {{wikidata_id: {moved_to}}}) RETURN r.weight"
        ));
        assert_eq!(w, vec![7], "born edge property reads back over a real-core traversal");
        // Patch the born edge in place (re-MERGE) and confirm the update.
        exec(&mut c, &g, &format!(
            "MERGE (a:Entity {{wikidata_id: 412684}})-[r:LINK]->(b:Entity {{wikidata_id: {moved_to}}}) SET r.weight = 9"
        ));
        let w2 = ints(&mut c, &g, &format!(
            "MATCH (a:Entity {{wikidata_id: 412684}})-[r:LINK]->(b:Entity {{wikidata_id: {moved_to}}}) RETURN r.weight"
        ));
        assert_eq!(w2, vec![9], "re-MERGE patches the born edge property");
        println!("item 3 (edge properties): OK");

        println!("ALL SMOKE CHECKS PASSED on graph {g}");
    })
    .await;
    joined.expect("smoke client task panicked");
    server.abort();
}

/// Bulk-delete stress: tombstone a ~30% segment of the graph (`:Entity` whose indexed
/// `wikidata_id <= SLATER_SMOKE_P30`, default 332894 for the 1M Wikidata fixture) through
/// the writable layer, then confirm the segment is gone from indexed seeks and the whole
/// count while the rest survives. A small `memtableBytes` keeps the per-write publish
/// cheap and lets the auto flush/compaction bound the L0 fan-out across the ~300K deletes.
///
/// NB — runtime is **fsync-bound**: each delete is its own fsync'd group-commit (batching
/// was deferred in Phase 1c), so throughput is one commit per statement. On a slow-fsync
/// box (WSL2, ~10ms) 300K deletes take ~1h (~12ms each); on NVMe (~0.1ms) it is ~30s. The
/// *reads* afterwards are fast (an indexed range seek and a full-scan count over the 1M
/// core with the tombstone overlay each complete in ~1.5s). Correctness is the point here,
/// not write throughput — run it deliberately, not in a tight loop.
///
/// ```text
/// SLATER_SMOKE_DATADIR=/home/rickk/perf-gens/wiki1m SLATER_SMOKE_GRAPH=wiki1m \
///   cargo test -p slater --test smoke_1m delete_thirty_percent -- --ignored --nocapture
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a prebuilt generation; bulk-deletes ~300K nodes"]
async fn delete_thirty_percent_segment() {
    let data_dir = std::env::var("SLATER_SMOKE_DATADIR")
        .expect("set SLATER_SMOKE_DATADIR to a slater data directory");
    let graph = std::env::var("SLATER_SMOKE_GRAPH").unwrap_or_else(|_| "wiki1m".to_string());
    let p30: i64 = std::env::var("SLATER_SMOKE_P30")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(332894);

    let scratch = std::env::temp_dir().join(format!("slater_smoke_del_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let acl_path = write_acl(&scratch, &graph);
    let wal_dir = scratch.join("wal");

    let cfg = serde_json::json!({
        "server": { "bind": "127.0.0.1", "port": 0 },
        "dataBackend": { "kind": "fs", "fs": { "dir": data_dir } },
        "aclPath": acl_path.to_str().unwrap(),
        "requireAclStamp": false,
        "reloadStrategy": "exit",
        "generationPollMs": 600000,
        "log": { "level": "warn" },
        // Enumerating the 30% segment returns ~300K rows over a range scan — lift the
        // per-query row / intermediate / scan caps well above that.
        "query": { "maxRows": 5000000, "maxIntermediate": 5000000, "maxScan": 5000000 },
        // Small active memtable so the per-write clone-on-publish stays cheap and the
        // auto flush + L0 compaction keep the read fan-out bounded across ~300K deletes.
        "delta": {
            "enabled": true,
            "walDir": wal_dir.to_str().unwrap(),
            "memtableBytes": 1 << 20,
            "l0CompactionTrigger": 4
        }
    });
    let cfg: slater::config::AppConfig = serde_json::from_value(cfg).expect("build AppConfig");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        if let Err(e) = slater::server::serve_with_listener(cfg, listener).await {
            eprintln!("server ended: {e:#}");
        }
    });
    tokio::time::sleep(Duration::from_millis(800)).await;

    let g = graph.clone();
    let joined = tokio::task::spawn_blocking(move || {
        let mut c =
            BoltClient::connect("127.0.0.1", port, Duration::from_secs(60)).expect("connect");
        c.login("smoke-test/1", "smoke", "pw").expect("login");

        // Baseline whole-label count (empty delta ⇒ answered from the manifest).
        let total = ints(&mut c, &g, "MATCH (n:Entity) RETURN count(*)")[0];
        println!("baseline :Entity count = {total}");
        assert!(total > 0);

        // Enumerate the 30% segment by an indexed range seek.
        let t0 = std::time::Instant::now();
        let victims = ints(
            &mut c,
            &g,
            &format!("MATCH (n:Entity) WHERE n.wikidata_id <= {p30} RETURN n.wikidata_id"),
        );
        let seg = victims.len() as i64;
        println!(
            "selected segment: {seg} entities (wikidata_id <= {p30}) = {:.1}% in {:?}",
            100.0 * seg as f64 / total as f64,
            t0.elapsed()
        );
        assert!(seg > 0 && seg < total, "a real, partial segment");
        // A witness entity beyond the segment (maldonite, id 412684 > p30) that must survive.
        assert_eq!(
            strs(
                &mut c,
                &g,
                "MATCH (n:Entity {wikidata_id: 412684}) RETURN n.name"
            ),
            vec!["maldonite"],
            "survivor present pre-delete"
        );

        // Delete every entity in the segment by its business key.
        let t1 = std::time::Instant::now();
        for (i, id) in victims.iter().enumerate() {
            exec(
                &mut c,
                &g,
                &format!("MATCH (n:Entity {{wikidata_id: {id}}}) DELETE n"),
            );
            if (i + 1) % 50_000 == 0 {
                println!("  deleted {}/{seg} ({:?})", i + 1, t1.elapsed());
            }
        }
        println!("deleted {seg} entities in {:?}", t1.elapsed());

        // The segment is gone from an indexed range seek…
        let t2 = std::time::Instant::now();
        let remaining_in_seg = ints(
            &mut c,
            &g,
            &format!("MATCH (n:Entity) WHERE n.wikidata_id <= {p30} RETURN n.wikidata_id"),
        );
        println!(
            "range seek over the deleted segment now returns {} rows ({:?})",
            remaining_in_seg.len(),
            t2.elapsed()
        );
        assert!(
            remaining_in_seg.is_empty(),
            "the whole 30% segment is suppressed on an indexed range seek"
        );

        // …and from the whole-label count (full scan + tombstone suppression).
        let t3 = std::time::Instant::now();
        let after = ints(&mut c, &g, "MATCH (n:Entity) RETURN count(*)")[0];
        println!(
            "post-delete :Entity count = {after} (scan {:?})",
            t3.elapsed()
        );
        assert_eq!(
            after,
            total - seg,
            "count drops by exactly the deleted segment"
        );

        // A specific deleted id is gone; a survivor beyond the segment stays.
        assert!(
            strs(
                &mut c,
                &g,
                "MATCH (n:Entity {wikidata_id: 1}) RETURN n.name"
            )
            .is_empty(),
            "a deleted id no longer resolves"
        );
        assert_eq!(
            strs(
                &mut c,
                &g,
                "MATCH (n:Entity {wikidata_id: 412684}) RETURN n.name"
            ),
            vec!["maldonite"],
            "an entity outside the segment survives"
        );
        println!("30% SEGMENT DELETE TEST PASSED: {seg} of {total} deleted, {after} remain");
    })
    .await;
    joined.expect("delete task panicked");
    server.abort();
}
