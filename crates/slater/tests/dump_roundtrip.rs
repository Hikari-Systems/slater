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

/// Run the real `slater-build` over `input`, producing a generation of graph `people`
/// under `data_dir`. Panics if the builder fails.
fn run_builder(builder: &str, input: &Path, data_dir: &Path) {
    std::fs::create_dir_all(data_dir).unwrap();
    let status = Command::new(builder)
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "people",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .status()
        .expect("run slater-build");
    assert!(status.success(), "slater-build failed on {input:?}");
    assert!(
        data_dir.join("people").join("current").exists(),
        "slater-build produced no `current` generation pointer in {data_dir:?}"
    );
}

/// Serve `data_dir` over Bolt, run the real `slater dump` binary against it into
/// `out_path`, and return the dump text. The server is torn down before returning.
async fn serve_and_dump(data_dir: &Path, acl_path: &Path, out_path: &Path) -> String {
    let cfg = build_config(data_dir, acl_path);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        if let Err(e) = slater::server::serve_with_listener(cfg, listener).await {
            eprintln!("server ended: {e:#}");
        }
    });
    // Let the server open the graph before the client connects.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let slater_bin = env!("CARGO_BIN_EXE_slater").to_string();
    let op = out_path.to_path_buf();
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
                op.to_str().unwrap(),
            ])
            .env("SLATER_DUMP_PASSWORD", "pw")
            .output()
            .expect("run slater dump")
    })
    .await
    .unwrap();
    server.abort();
    assert!(
        out.status.success(),
        "slater dump failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    std::fs::read_to_string(out_path).unwrap()
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

        // Serve the freshly-rebuilt generation and dump it again. This drives the *real*
        // builder's forward CSR — where a source's edge ids are dense-contiguous, so the edge
        // id is stored as `edge_id_base` and derived on read as `base + k` — through the serve +
        // traversal path, and reads the edge property `r.since` via that derived id from
        // `edge_props`. A wrong forward id or an `edge_props` misalignment would drop the edge
        // or its property from the re-dump. (Node order can differ from the fixture after
        // clustering, so assert on the business-key edge line, not the whole dump.)
        let cfg2 = build_config(&rebuilt, &acl_path);
        let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap();
        let server2 = tokio::spawn(async move {
            if let Err(e) = slater::server::serve_with_listener(cfg2, listener2).await {
                eprintln!("rebuilt server ended: {e:#}");
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let dump2_path = root.join("dump2.cypher");
        let slater_bin2 = env!("CARGO_BIN_EXE_slater").to_string();
        let port2 = addr2.port();
        let dp2 = dump2_path.clone();
        let out2 = tokio::task::spawn_blocking(move || {
            Command::new(&slater_bin2)
                .args([
                    "dump",
                    "people",
                    "-u",
                    "reporting",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    &port2.to_string(),
                    "-o",
                    dp2.to_str().unwrap(),
                ])
                .env("SLATER_DUMP_PASSWORD", "pw")
                .output()
                .expect("run slater dump on rebuilt")
        })
        .await
        .unwrap();
        assert!(
            out2.status.success(),
            "dump of the rebuilt generation failed: {}",
            String::from_utf8_lossy(&out2.stderr)
        );
        let dump2 = std::fs::read_to_string(&dump2_path).unwrap();
        assert!(
            dump2.contains(
                "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020;"
            ),
            "re-dump of the real-built generation lost the KNOWS edge or its r.since property \
             (forward edge_id_base / edge_props): {dump2}"
        );
        server2.abort();
    } else {
        eprintln!("SLATER_BUILD_BIN unset — skipped the rebuild leg (dump text asserted only)");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&root);
}

/// The seed graph for the multi-label round-trip: Alice is `:Person:Employee`, Bob is
/// plain `:Person`. Both are keyed on `Person.name` — `Employee` is a bare marker label
/// with no index, which is exactly the shape the dump used to drop.
const MULTI_LABEL_SEED: &str = "\
CREATE INDEX FOR (n:Person) ON (n.name);
MERGE (n:Person:Employee {name: 'Alice'}) SET n.age = 30;
MERGE (n:Person {name: 'Bob'}) SET n.age = 25;
MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020;
";

/// Count the node `MERGE` statements in a dump (edge MERGEs bind `a`, not `n`).
fn node_merge_lines(dump: &str) -> Vec<&str> {
    dump.lines()
        .filter(|l| l.starts_with("MERGE (n:"))
        .collect()
}

/// The seed graph for the injection round-trip (HIK-84). Every identifier here is
/// *hostile*: a label carrying a `;`, a reltype carrying a `;` and a comment, and two
/// property keys that — un-quoted — would close the `SET` and splice a whole extra
/// statement into the rebuilt script (`MERGE (m:Owned …)`, `CREATE (:Pwned …)`). The
/// first key is verbatim the payload from the finding. A `bio` value carries a quote,
/// a backslash, a newline, a `;` and a backtick, so the *value* escaping is pinned on
/// the same trip. Written in the quoted dialect, so the real builder ingesting this
/// seed at all is already proof that its grammar accepts every form `dump` now emits.
const HOSTILE_SEED: &str = r#"CREATE INDEX FOR (n:Person) ON (n.name);
MERGE (n:Person:`Odd Label; DROP INDEX` {name: 'Alice'}) SET n.age = 30, n.```x`` = 'v'; MERGE (m:Owned {id:'atk'}) SET m.a` = 1, n.`x``) CREATE (:Pwned {y:1}) //` = 2, n.bio = 'a\'; MERGE (m:Owned {id:\'atk\'}) SET m.b = 1;\nline2 \\ back `tick';
MERGE (n:Person {name: 'Bob'}) SET n.age = 25;
MERGE (a:Person {name: 'Alice'})-[r:`KNOWS OF; //`]->(b:Person {name: 'Bob'}) SET r.since = 2020;
"#;

/// Regression for HIK-84: `dump` interpolated labels, reltypes and property keys into
/// the emitted Cypher **raw**. Property keys are arbitrary strings over Bolt, so a
/// hostile key spliced an independent statement into the script an operator later fed
/// to `slater-build` — a stored, cross-privilege injection into the rebuild.
///
/// This drives the whole loop rather than asserting the emitted text looks quoted:
/// seed through the real `slater-build`, serve, dump with the real `slater dump`,
/// rebuild *that dump*, re-serve, re-dump. The assertions are on the **rebuilt
/// generation**: if any identifier re-parsed as structure, the rebuild would carry an
/// extra `:Owned`/`:Pwned` node, so the node count (and the statement count) is the
/// injection oracle. The fixed-point check then proves the trip is lossless, not just
/// safe — every hostile name and value comes back byte-identical.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns the slater binary and the real builder; needs SLATER_BUILD_BIN"]
async fn hostile_identifiers_cannot_inject_structure_into_the_rebuild() {
    let Ok(builder) = std::env::var("SLATER_BUILD_BIN") else {
        eprintln!("SLATER_BUILD_BIN unset — skipping the injection round-trip");
        return;
    };

    let root = std::env::temp_dir().join(format!("slater_injrt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let acl_path = write_acl(&root);

    // Leg 1: hostile seed → real builder → generation.
    let seed = root.join("seed.cypher");
    std::fs::write(&seed, HOSTILE_SEED).unwrap();
    let built = root.join("built");
    run_builder(&builder, &seed, &built);

    // Leg 2: serve it and dump it. Every hostile name must come back backtick-quoted,
    // with the inner backticks doubled (the escape-the-escape case).
    let dump1 = serve_and_dump(&built, &acl_path, &root.join("dump1.cypher")).await;
    for expected in [
        // Labels: identity first, the hostile one quoted.
        "MERGE (n:Person:`Odd Label; DROP INDEX` {name: 'Alice'})",
        // The finding's payload, inert: one quoted *name*, inner backticks doubled.
        "n.```x`` = 'v'; MERGE (m:Owned {id:'atk'}) SET m.a` = 1",
        // A key that would otherwise close the pattern and create a second node.
        "n.`x``) CREATE (:Pwned {y:1}) //` = 2",
        // Reltype.
        "-[r:`KNOWS OF; //`]->",
        // The value escaping (unchanged, but pinned): quote, backslash, newline, `;`.
        r#"n.bio = 'a\'; MERGE (m:Owned {id:\'atk\'}) SET m.b = 1;\nline2 \\ back `tick'"#,
    ] {
        assert!(
            dump1.contains(expected),
            "dump did not quote/escape as expected — missing `{expected}` in:\n{dump1}"
        );
    }

    // Leg 3: rebuild *the dump* → serve → re-dump, and read the answer out of the
    // rebuilt generation.
    let rebuilt = root.join("rebuilt");
    run_builder(&builder, &root.join("dump1.cypher"), &rebuilt);
    let dump2 = serve_and_dump(&rebuilt, &acl_path, &root.join("dump2.cypher")).await;

    // The injection oracle: nothing executed as structure. Two nodes in, two nodes out
    // — an `:Owned`/`:Pwned` node spliced by a hostile key would be a third node MERGE.
    assert_eq!(
        node_merge_lines(&dump2).len(),
        2,
        "the rebuild grew a node — an identifier re-parsed as structure:\n{dump2}"
    );
    assert!(
        !dump2.contains("MERGE (n:Owned") && !dump2.contains("MERGE (n:Pwned"),
        "the rebuilt generation carries an injected node:\n{dump2}"
    );
    // …and no spliced index/DDL either: 1 index + 2 nodes + 1 edge, exactly.
    assert_eq!(
        dump2.lines().count(),
        4,
        "the rebuild has extra statements:\n{dump2}"
    );

    // Lossless as well as safe: the hostile names and values survive the trip intact
    // (the dump of the rebuild reproduces the dump it was built from, byte for byte).
    assert_eq!(
        dump1, dump2,
        "hostile identifiers/values did not round-trip byte-identically"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// Regression for HIK-70: `dump` used to emit only a node's identity label, so every
/// other label was silently lost on the documented dump → build → serve round-trip.
///
/// This drives the **real** loop rather than just asserting the emitted text: seed a
/// multi-label graph through the real `slater-build`, serve it, dump it with the real
/// `slater dump`, rebuild *that dump* with the real `slater-build`, then serve and
/// re-dump the rebuild. The final dump is read back out of the rebuilt generation's
/// `node_labels`, so it can only carry `:Employee` if the label actually survived the
/// full trip. It also pins the MERGE semantics: the extra label must not fork a second
/// node (the node MERGE count stays at 2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns the slater binary and the real builder; needs SLATER_BUILD_BIN"]
async fn multi_label_nodes_keep_every_label_through_the_real_round_trip() {
    let Ok(builder) = std::env::var("SLATER_BUILD_BIN") else {
        eprintln!("SLATER_BUILD_BIN unset — skipping the multi-label round-trip");
        return;
    };

    let root = std::env::temp_dir().join(format!("slater_mlrt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let acl_path = write_acl(&root);

    // Leg 1: seed → real builder → generation.
    let seed = root.join("seed.cypher");
    std::fs::write(&seed, MULTI_LABEL_SEED).unwrap();
    let built = root.join("built");
    run_builder(&builder, &seed, &built);

    // Leg 2: serve the built generation and dump it.
    let dump1 = serve_and_dump(&built, &acl_path, &root.join("dump1.cypher")).await;
    assert!(
        dump1.contains("MERGE (n:Person:Employee {name: 'Alice'})"),
        "dump dropped Alice's `:Employee` label (identity label only): {dump1}"
    );
    assert!(
        dump1.contains("MERGE (n:Person {name: 'Bob'})"),
        "single-label node did not survive the dump: {dump1}"
    );

    // Leg 3: rebuild *the dump* → serve → re-dump. `:Employee` can only appear here if
    // the builder actually persisted it, so this closes the loop the module promises.
    let rebuilt = root.join("rebuilt");
    run_builder(&builder, &root.join("dump1.cypher"), &rebuilt);
    let dump2 = serve_and_dump(&rebuilt, &acl_path, &root.join("dump2.cypher")).await;
    assert!(
        dump2.contains("MERGE (n:Person:Employee {name: 'Alice'})"),
        "rebuilt generation lost Alice's `:Employee` label: {dump2}"
    );

    // MERGE semantics: the extra label rides the identity merge, it does not create a
    // second Alice. Two nodes in, two nodes out — and the edge still resolves.
    assert_eq!(
        node_merge_lines(&dump2).len(),
        2,
        "the extra label forked a node — expected 2 node MERGEs: {dump2}"
    );
    assert!(
        dump2.contains(
            "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020;"
        ),
        "the edge did not survive the multi-label round-trip: {dump2}"
    );

    // The dump is a fixed point: dumping the rebuild reproduces the dump it was built
    // from (labels included), so the round-trip is stable, not merely lossless once.
    assert_eq!(dump1, dump2, "dump → build → dump is not a fixed point");

    let _ = std::fs::remove_dir_all(&root);
}
