// SPDX-License-Identifier: Apache-2.0
//! `gql_dialect` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

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
