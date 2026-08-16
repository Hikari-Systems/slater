// SPDX-License-Identifier: Apache-2.0
//! `copy_and_ctx` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

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
    let mut sess = authenticated_session("reporting");
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
