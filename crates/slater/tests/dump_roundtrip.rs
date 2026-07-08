// SPDX-License-Identifier: Apache-2.0
//! End-to-end round-trip for the `slater dump` CLI.
//!
//! Serves a known fixture graph over Bolt, runs the **real** `slater dump` binary
//! against it, asserts the emitted business-key `MERGE` dump byte-for-byte, then
//! feeds that dump to the **real** `slater-build` and confirms the rebuild produces
//! a fresh generation — closing the dump → build → serve loop the tool exists for.
//!
//! `#[ignore]`: it spawns the `slater` binary (so it is heavier than a unit test)
//! and the rebuild leg needs `SLATER_BUILD_BIN`. Run:
//! ```text
//! SLATER_BUILD_BIN=$CARGO_TARGET_DIR/debug/slater-build \
//!   cargo test -p slater --features testkit --test dump_roundtrip -- --ignored
//! ```

use std::path::Path;
use std::process::Command;
use tokio::net::TcpListener;

/// The exact dump the fixture must produce: `CREATE INDEX` DDL, then nodes in
/// dense-id order (Alice/Bob/Carol), then the single edge. Matches the emitted
/// dialect of `consolidate::serialise_merge_dump`.
const EXPECTED_DUMP: &str = "\
CREATE INDEX FOR (n:Person) ON (n.name);
MERGE (n:Person {name: 'Alice'}) SET n.age = 30;
MERGE (n:Person {name: 'Bob'}) SET n.age = 25;
MERGE (n:Person {name: 'Carol'}) SET n.age = 40;
MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020;
";

fn write_acl(root: &Path) -> std::path::PathBuf {
    let path = root.join("acl.json");
    let hash = slater::acl::hash_password("pw").unwrap();
    let json = serde_json::json!({
        "users": {
            "reporting": {
                "passwordArgon2id": hash,
                "grants": { "people": ["read"] }
            }
        }
    });
    std::fs::write(&path, json.to_string()).unwrap();
    path
}

fn build_config(root: &Path, acl_path: &Path) -> slater::config::AppConfig {
    // The fixture is built unstamped, so `requireAclStamp` is off (as in the other
    // in-process server tests); the slow poll keeps the guard idle for the test.
    let value = serde_json::json!({
        "server": { "bind": "127.0.0.1", "port": 0 },
        "dataBackend": { "kind": "fs", "fs": { "dir": root.to_str().unwrap() } },
        "aclPath": acl_path.to_str().unwrap(),
        "requireAclStamp": false,
        "reloadStrategy": "exit",
        "generationPollMs": 600000,
        "log": { "level": "warn" }
    });
    serde_json::from_value(value).expect("build AppConfig")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns the slater binary; needs SLATER_BUILD_BIN for the rebuild leg"]
async fn dump_round_trips_through_the_real_builder() {
    let root = std::env::temp_dir().join(format!("slater_dumprt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    // Known fixture: 3 :Person keyed by `name` + one :KNOWS edge, one (Person,name)
    // range index (see `testgen::write_indexed_people`).
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0007);
    let graph = slater::testgen::write_indexed_people_at(&root, uuid, [30, 25, 40]);
    assert_eq!(graph, "people");

    let acl_path = write_acl(&root);
    let cfg = build_config(&root, &acl_path);

    // Bind loopback ourselves so we learn the port, then hand the listener to the
    // production server entry point.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        if let Err(e) = slater::server::serve_with_listener(cfg, listener).await {
            eprintln!("server ended: {e:#}");
        }
    });
    // Let the server open the graph before the client connects.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Run the real `slater dump` over Bolt (blocking client → a blocking task so the
    // async server keeps running on the other worker thread).
    let dump_path = root.join("dump.cypher");
    let slater_bin = env!("CARGO_BIN_EXE_slater").to_string();
    let port = addr.port();
    let dp = dump_path.clone();
    let out = tokio::task::spawn_blocking(move || {
        Command::new(&slater_bin)
            .args([
                "dump",
                "people",
                "-u",
                "reporting",
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "-o",
                dp.to_str().unwrap(),
            ])
            .env("SLATER_DUMP_PASSWORD", "pw")
            .output()
            .expect("run slater dump")
    })
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "slater dump failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let dump = std::fs::read_to_string(&dump_path).unwrap();
    assert_eq!(dump, EXPECTED_DUMP, "dump text mismatch");

    // Rebuild leg: the real `slater-build` must ingest the dump into a fresh
    // generation (proves the dump is round-trippable, not just plausible text).
    if let Ok(builder) = std::env::var("SLATER_BUILD_BIN") {
        let rebuilt = root.join("rebuilt");
        std::fs::create_dir_all(&rebuilt).unwrap();
        let status = Command::new(&builder)
            .args([
                "--input",
                dump_path.to_str().unwrap(),
                "--graph",
                "people",
                "--data-dir",
                rebuilt.to_str().unwrap(),
            ])
            .status()
            .expect("run slater-build");
        assert!(status.success(), "slater-build failed to ingest the dump");
        assert!(
            rebuilt.join("people").join("current").exists(),
            "rebuild produced no `current` generation pointer"
        );
    } else {
        eprintln!("SLATER_BUILD_BIN unset — skipped the rebuild leg (dump text asserted only)");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}
