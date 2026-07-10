// SPDX-License-Identifier: Apache-2.0
//! The full openCypher write clause set, driven over a **live Bolt session** against a
//! real server, then folded through a **real `slater-build` consolidation** and re-read
//! from the rebuilt generation — the exit criterion of the write-clauses plan.
//!
//! Unlike the in-crate unit tests (which call `execute_write` directly), this exercises
//! the whole chain a client sees: Bolt protocol → auth → server dispatch → WAL + memtable
//! → overlay read-back → `CALL slater.consolidate()` (spawning the real builder) → the
//! swapped-in fresh core still serving every write.
//!
//! `#[ignore]`: it spawns the in-process server and the real `slater-build` binary
//! (located via `SLATER_BUILD_BIN`, else `slater-build` on `PATH`). Run:
//! ```text
//! SLATER_BUILD_BIN=$CARGO_TARGET_DIR/debug/slater-build \
//!   cargo test -p slater --test writable_bolt_roundtrip -- --ignored --nocapture
//! ```

use std::path::Path;
use std::time::Duration;

use slater::bolt::client::BoltClient;
use tokio::net::TcpListener;

fn write_acl(root: &Path, graph: &str) -> std::path::PathBuf {
    let path = root.join("acl.json");
    let hash = slater::acl::hash_password("pw").unwrap();
    let json = serde_json::json!({
        "users": {
            "writer": { "passwordArgon2id": hash, "grants": { graph: ["read", "write"] } }
        }
    });
    std::fs::write(&path, json.to_string()).unwrap();
    path
}

/// The value of a single scalar column (row 0, col 0) rendered `null` / `int:N` / `str:S`.
fn scalar(c: &mut BoltClient, graph: &str, q: &str) -> String {
    let (_cols, rows) = c
        .run_pull(q, Some(graph))
        .unwrap_or_else(|e| panic!("query failed: {q}\n  {e}"));
    match rows.first().and_then(|r| r.first()) {
        None => "none".to_string(),
        Some(v) if v.as_int().is_some() => format!("int:{}", v.as_int().unwrap()),
        Some(v) if v.as_str().is_some() => format!("str:{}", v.as_str().unwrap()),
        Some(_) => "null".to_string(),
    }
}

fn exec(c: &mut BoltClient, graph: &str, q: &str) {
    c.run_pull(q, Some(graph))
        .unwrap_or_else(|e| panic!("write failed: {q}\n  {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns the server + real slater-build; set SLATER_BUILD_BIN"]
async fn every_write_clause_over_bolt_survives_consolidation() {
    let root = std::env::temp_dir().join(format!("slater_wbrt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    // Fixture: 3 :Person keyed by `name` (range-indexed) + one :KNOWS edge Alice→Bob.
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0042);
    let graph = slater::testgen::write_indexed_people_at(&root, uuid, [30, 25, 40]);
    assert_eq!(graph, "people");

    let acl_path = write_acl(&root, &graph);
    let wal_dir = root.join("wal");
    let builder_bin =
        std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());

    let cfg = serde_json::json!({
        "server": { "bind": "127.0.0.1", "port": 0 },
        "dataBackend": { "kind": "fs", "fs": { "dir": root.to_str().unwrap() } },
        "aclPath": acl_path.to_str().unwrap(),
        "requireAclStamp": false,
        "reloadStrategy": "exit",
        "generationPollMs": 600000,
        "log": { "level": "warn" },
        "delta": { "enabled": true, "walDir": wal_dir.to_str().unwrap(), "builderBin": builder_bin }
    });
    let cfg: slater::config::AppConfig = serde_json::from_value(cfg).expect("build AppConfig");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        if let Err(e) = slater::server::serve_with_listener(cfg, listener).await {
            eprintln!("server ended: {e:#}");
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    let g = graph.clone();
    tokio::task::spawn_blocking(move || {
        let mut c =
            BoltClient::connect("127.0.0.1", port, Duration::from_secs(30)).expect("connect");
        c.login("writer/1", "writer", "pw").expect("login");

        // ── Baseline: the core reads back through the (empty) overlay.
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Alice'}) RETURN n.age"), "int:30");

        // ── SET forms, over Bolt ──────────────────────────────────────────────────
        // MERGE create + property SET.
        exec(&mut c, &g, "MERGE (n:Person {name:'Dave'}) SET n.age = 50");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Dave'}) RETURN n.age"), "int:50");
        // += merge map.
        exec(&mut c, &g, "MATCH (n:Person {name:'Dave'}) SET n += {city: 'NYC'}");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Dave'}) RETURN n.city"), "str:NYC");
        // = replace-all (city wiped, age replaced).
        exec(&mut c, &g, "MATCH (n:Person {name:'Dave'}) SET n = {age: 51}");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Dave'}) RETURN n.age"), "int:51");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Dave'}) RETURN n.city"), "null");
        // Multi-item SET in one statement.
        exec(&mut c, &g, "MATCH (n:Person {name:'Carol'}) SET n.a = 1, n.b = 2");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Carol'}) RETURN n.a"), "int:1");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Carol'}) RETURN n.b"), "int:2");

        // ── REMOVE a property ─────────────────────────────────────────────────────
        exec(&mut c, &g, "MATCH (n:Person {name:'Bob'}) SET n.tag = 'x'");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Bob'}) RETURN n.tag"), "str:x");
        exec(&mut c, &g, "MATCH (n:Person {name:'Bob'}) REMOVE n.tag");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Bob'}) RETURN n.tag"), "null");

        // ── CREATE + MERGE ON CREATE / ON MATCH ───────────────────────────────────
        exec(&mut c, &g, "CREATE (n:Person {name: 'Zoe', age: 20})");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Zoe'}) RETURN n.age"), "int:20");
        exec(&mut c, &g, "MERGE (n:Person {name:'Yan'}) ON CREATE SET n.origin = 'created' ON MATCH SET n.origin = 'matched'");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Yan'}) RETURN n.origin"), "str:created");
        exec(&mut c, &g, "MERGE (n:Person {name:'Yan'}) ON CREATE SET n.origin = 'created' ON MATCH SET n.origin = 'matched'");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Yan'}) RETURN n.origin"), "str:matched");

        // ── DELETE conformance ────────────────────────────────────────────────────
        // Alice still has her :KNOWS edge → a plain DELETE is rejected.
        assert!(
            c.run_pull("MATCH (n:Person {name:'Alice'}) DELETE n", Some(&g)).is_err(),
            "plain DELETE of a connected node must be rejected over Bolt"
        );
        // A query FAILURE puts the Bolt connection in the FAILED state; RESET to continue.
        c.reset().expect("reset after the intentional failure");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Alice'}) RETURN n.name"), "str:Alice");
        // DETACH DELETE removes Alice and her edge.
        exec(&mut c, &g, "MATCH (n:Person {name:'Alice'}) DETACH DELETE n");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Alice'}) RETURN n.name"), "none");
        // Zoe has no edges → a plain DELETE works.
        exec(&mut c, &g, "MATCH (n:Person {name:'Zoe'}) DELETE n");
        assert_eq!(scalar(&mut c, &g, "MATCH (n:Person {name:'Zoe'}) RETURN n.name"), "none");

        // ── Consolidate: fold the whole delta into a fresh generation via the real
        // builder, then confirm the rebuilt core still serves every write.
        exec(&mut c, &g, "CALL slater.consolidate()");
    })
    .await
    .expect("client task");

    // A fresh connection is guaranteed to see the swapped-in generation.
    let g = graph.clone();
    tokio::task::spawn_blocking(move || {
        let mut c =
            BoltClient::connect("127.0.0.1", port, Duration::from_secs(30)).expect("reconnect");
        c.login("writer/1", "writer", "pw").expect("login");

        // Property writes survived the rebuild.
        assert_eq!(
            scalar(&mut c, &g, "MATCH (n:Person {name:'Dave'}) RETURN n.age"),
            "int:51",
            "replace-all survived consolidation"
        );
        assert_eq!(
            scalar(&mut c, &g, "MATCH (n:Person {name:'Carol'}) RETURN n.a"),
            "int:1",
            "multi-item SET survived"
        );
        assert_eq!(
            scalar(&mut c, &g, "MATCH (n:Person {name:'Carol'}) RETURN n.b"),
            "int:2"
        );
        assert_eq!(
            scalar(&mut c, &g, "MATCH (n:Person {name:'Yan'}) RETURN n.origin"),
            "str:matched",
            "ON MATCH result survived"
        );
        // Deletes survived: Alice (DETACH) and Zoe (plain) are gone.
        assert_eq!(
            scalar(&mut c, &g, "MATCH (n:Person {name:'Alice'}) RETURN n.name"),
            "none",
            "DETACH DELETE survived consolidation"
        );
        assert_eq!(
            scalar(&mut c, &g, "MATCH (n:Person {name:'Zoe'}) RETURN n.name"),
            "none",
            "plain DELETE survived consolidation"
        );
        // Untouched core rows are intact.
        assert_eq!(
            scalar(&mut c, &g, "MATCH (n:Person {name:'Bob'}) RETURN n.age"),
            "int:25"
        );
    })
    .await
    .expect("readback task");

    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}
