// SPDX-License-Identifier: Apache-2.0
//! Scale smoke: drive the full write clause set over Bolt against the **existing 91.6M
//! wikidata generation** as the starting core (read-only on the core; the writable layer
//! writes only to a scratch WAL + in-memory delta). Does NOT consolidate — a 91M rebuild
//! dumps ~180GB, which exceeds a typical box's free disk; the core generation is never
//! modified. Proves business-key resolution through the real ISAM, overlay read-back, and
//! DELETE conformance all hold at 91.6M-node scale. Companion to `smoke_1m`.
//!
//! Run:
//! ```text
//! SLATER_WD91M_DIR=/path/to/91m/data-dir SLATER_WD91M_GRAPH=wd91m_wr \
//!   cargo test -p slater --features testkit --test wd91m_write_smoke -- --ignored --nocapture
//! ```

use std::path::Path;
use std::time::{Duration, Instant};

use slater::bolt::client::BoltClient;
use tokio::net::TcpListener;

fn write_acl(root: &Path, graph: &str) -> std::path::PathBuf {
    let path = root.join("acl.json");
    let hash = slater::acl::hash_password("pw").unwrap();
    let json = serde_json::json!({
        "users": { "writer": { "passwordArgon2id": hash, "grants": { graph: ["read", "write"] } } }
    });
    std::fs::write(&path, json.to_string()).unwrap();
    path
}

fn scalar(c: &mut BoltClient, g: &str, q: &str) -> String {
    let (_c, rows) = c
        .run_pull(q, Some(g))
        .unwrap_or_else(|e| panic!("query failed: {q}\n  {e}"));
    match rows.first().and_then(|r| r.first()) {
        None => "none".to_string(),
        Some(v) if v.as_int().is_some() => format!("int:{}", v.as_int().unwrap()),
        Some(v) if v.as_str().is_some() => format!("str:{}", v.as_str().unwrap()),
        Some(_) => "null".to_string(),
    }
}

fn exec(c: &mut BoltClient, g: &str, q: &str) {
    c.run_pull(q, Some(g))
        .unwrap_or_else(|e| panic!("write failed: {q}\n  {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs the prebuilt 91M generation via SLATER_WD91M_DIR"]
async fn every_write_clause_over_bolt_on_the_91m_core() {
    let data_dir = std::env::var("SLATER_WD91M_DIR").expect("set SLATER_WD91M_DIR");
    let graph = std::env::var("SLATER_WD91M_GRAPH").unwrap_or_else(|_| "wd91m_wr".to_string());

    let scratch = std::env::temp_dir().join(format!("slater_wd91m_{}", std::process::id()));
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
    let t_open = Instant::now();
    let server = tokio::spawn(async move {
        if let Err(e) = slater::server::serve_with_listener(cfg, listener).await {
            eprintln!("server ended: {e:#}");
        }
    });
    // The 91M core cold-opens from disk; give it room before the first query.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let g = graph.clone();
    tokio::task::spawn_blocking(move || {
        let mut c = BoltClient::connect("127.0.0.1", port, Duration::from_secs(120)).expect("connect");
        c.login("wd91m/1", "writer", "pw").expect("login");

        // Warm the core with a whole-graph count (answered from the resident manifest).
        let total = scalar(&mut c, &g, "MATCH (n:Entity) RETURN count(*)");
        println!("[wd91m] Entity count = {total}  (core opened in {:?})", t_open.elapsed());

        // Discover a real *connected* entity (for the DELETE-conformance leg) and a real
        // plain entity (for the ON MATCH leg) from the actual core.
        let connected = {
            let (_c, rows) = c
                .run_pull(
                    "MATCH (a:Entity)-[]->(b:Entity) RETURN a.wikidata_id LIMIT 1",
                    Some(&g),
                )
                .expect("find a connected entity");
            rows.first().and_then(|r| r.first()).and_then(|v| v.as_int()).expect("a connected id")
        };
        println!("[wd91m] connected entity wikidata_id = {connected}");

        // ── SET forms on a delta-born entity (fresh, synthetic key) ────────────────
        let born = 999_000_000_000_001_i64;
        exec(&mut c, &g, &format!("MERGE (n:Entity {{wikidata_id: {born}}}) SET n.name = 'ProbeBorn'"));
        assert_eq!(scalar(&mut c, &g, &q(born, "name")), "str:ProbeBorn");
        exec(&mut c, &g, &format!("MATCH (n:Entity {{wikidata_id: {born}}}) SET n += {{kind: 'test'}}"));
        assert_eq!(scalar(&mut c, &g, &q(born, "kind")), "str:test");
        exec(&mut c, &g, &format!("MATCH (n:Entity {{wikidata_id: {born}}}) SET n = {{name: 'Replaced'}}"));
        assert_eq!(scalar(&mut c, &g, &q(born, "name")), "str:Replaced");
        assert_eq!(scalar(&mut c, &g, &q(born, "kind")), "null", "replace-all wiped kind");
        exec(&mut c, &g, &format!("MATCH (n:Entity {{wikidata_id: {born}}}) SET n.a = 1, n.b = 2"));
        assert_eq!(scalar(&mut c, &g, &q(born, "a")), "int:1");
        assert_eq!(scalar(&mut c, &g, &q(born, "b")), "int:2");
        exec(&mut c, &g, &format!("MATCH (n:Entity {{wikidata_id: {born}}}) REMOVE n.a"));
        assert_eq!(scalar(&mut c, &g, &q(born, "a")), "null", "REMOVE dropped a");

        // ── CREATE + MERGE ON CREATE / ON MATCH ───────────────────────────────────
        let created = 999_000_000_000_002_i64;
        exec(&mut c, &g, &format!("CREATE (n:Entity {{wikidata_id: {created}, name: 'Fresh'}})"));
        assert_eq!(scalar(&mut c, &g, &q(created, "name")), "str:Fresh");
        let yan = 999_000_000_000_003_i64;
        exec(&mut c, &g, &format!("MERGE (n:Entity {{wikidata_id: {yan}}}) ON CREATE SET n.origin='c' ON MATCH SET n.origin='m'"));
        assert_eq!(scalar(&mut c, &g, &q(yan, "origin")), "str:c", "ON CREATE for a new id");
        exec(&mut c, &g, &format!("MERGE (n:Entity {{wikidata_id: {yan}}}) ON CREATE SET n.origin='c' ON MATCH SET n.origin='m'"));
        assert_eq!(scalar(&mut c, &g, &q(yan, "origin")), "str:m", "ON MATCH the second time");

        // A real core entity, patched in place (proves business-key resolution via the
        // real 91M ISAM), read back through the overlay.
        exec(&mut c, &g, &format!("MATCH (n:Entity {{wikidata_id: {connected}}}) SET n.probe_touch = 'yes'"));
        assert_eq!(scalar(&mut c, &g, &q(connected, "probe_touch")), "str:yes", "core entity patched by key");

        // ── DELETE conformance on a real connected core entity ─────────────────────
        assert!(
            c.run_pull(&format!("MATCH (n:Entity {{wikidata_id: {connected}}}) DELETE n"), Some(&g)).is_err(),
            "plain DELETE of a connected core entity must be rejected at 91M scale"
        );
        c.reset().expect("reset after the intentional failure");
        assert_ne!(scalar(&mut c, &g, &q(connected, "wikidata_id")), "none", "rejected DELETE left it in place");
        exec(&mut c, &g, &format!("MATCH (n:Entity {{wikidata_id: {connected}}}) DETACH DELETE n"));
        assert_eq!(scalar(&mut c, &g, &q(connected, "wikidata_id")), "none", "DETACH DELETE removed it");

        // A born entity with no edges → a plain DELETE works.
        exec(&mut c, &g, &format!("MATCH (n:Entity {{wikidata_id: {created}}}) DELETE n"));
        assert_eq!(scalar(&mut c, &g, &q(created, "wikidata_id")), "none", "plain DELETE of an unconnected node");

        println!("[wd91m] ALL WRITE CLAUSES VERIFIED against the 91M core (consolidate skipped — disk)");
    })
    .await
    .expect("client task");

    server.abort();
    let _ = std::fs::remove_dir_all(&scratch);
}

/// `MATCH (n:Entity {wikidata_id: <id>}) RETURN n.<prop>`.
fn q(id: i64, prop: &str) -> String {
    format!("MATCH (n:Entity {{wikidata_id: {id}}}) RETURN n.{prop}")
}
