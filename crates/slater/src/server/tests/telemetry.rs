// SPDX-License-Identifier: Apache-2.0
//! The spans a request actually produces, read back from an in-memory OpenTelemetry
//! exporter behind the same `tracing-opentelemetry` layer `hs_utils::otel` installs.
//!
//! The subscriber is process-global, not `with_default`: `slater.execute` and
//! `slater.encode` are created on tokio's blocking pool, whose threads never see a
//! thread-local subscriber — a `with_default` harness would report those spans
//! missing, or worse, pass a test that never looked. Being global, it also collects
//! every other test's spans; each test here finds its own by a unique marker in the
//! query text (or a span it opened itself), never by "the latest span".

use std::sync::OnceLock;

use opentelemetry::trace::{SpanId, SpanKind, Status, TraceContextExt as _, TracerProvider as _};
use opentelemetry::Value as OtelValue;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::Layer as _;

use super::*;

struct Harness {
    exporter: InMemorySpanExporter,
    _provider: SdkTracerProvider,
}

fn harness() -> &'static Harness {
    static HARNESS: OnceLock<Harness> = OnceLock::new();
    HARNESS.get_or_init(|| {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("slater-test"))
            // Info, as production exports by default; and so the rest of the suite's
            // `debug!` call sites do not start formatting their arguments.
            .with_filter(tracing_subscriber::filter::LevelFilter::INFO);
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
            .expect("no other test installs a global subscriber");
        // What `hs_utils::otel::init` installs when export is on; without it the
        // `TRACEPARENT` handoff injects nothing.
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        Harness {
            exporter,
            _provider: provider,
        }
    })
}

fn spans() -> Vec<SpanData> {
    harness().exporter.get_finished_spans().unwrap()
}

fn attr<'a>(span: &'a SpanData, key: &str) -> Option<&'a OtelValue> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| &kv.value)
}

fn attr_str(span: &SpanData, key: &str) -> Option<String> {
    attr(span, key).map(|v| v.as_str().into_owned())
}

/// The finished `bolt.run` whose query text carries `marker`.
fn run_span_for(marker: &str) -> SpanData {
    let mut found: Vec<SpanData> = spans()
        .into_iter()
        .filter(|s| s.name == "bolt.run")
        .filter(|s| attr_str(s, "db.query.text").is_some_and(|q| q.contains(marker)))
        .collect();
    assert_eq!(found.len(), 1, "exactly one bolt.run for {marker}");
    found.pop().unwrap()
}

fn children_of(parent: &SpanData) -> Vec<SpanData> {
    spans()
        .into_iter()
        .filter(|s| {
            s.parent_span_id == parent.span_context.span_id()
                && s.span_context.trace_id() == parent.span_context.trace_id()
        })
        .collect()
}

async fn run(ctx: &Arc<ConnCtx>, query: &str, params: PsValue) -> Result<Vec<PsValue>, Failure> {
    let mut sess = authenticated_session("reporting");
    handle_request_traced(
        &mut sess,
        ctx,
        message::Request::Run {
            query: query.to_string(),
            params,
            extra: PsValue::Map(vec![]),
        },
    )
    .await
}

/// One RUN is one trace: a `bolt.run` server-kind root carrying the graph, the
/// operation, the user and the query, with parse, execute and encode beneath it —
/// including the two that run on the blocking pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_is_one_trace_of_parse_execute_and_encode() {
    harness();
    let (_root, ctx) = build_ctx("otel_run_trace");
    run(&ctx, "RETURN 'otel-marker-1' AS m", PsValue::Map(vec![]))
        .await
        .expect("query runs");

    let root = run_span_for("otel-marker-1");
    assert_eq!(root.span_kind, SpanKind::Server);
    assert_eq!(
        root.parent_span_id,
        SpanId::INVALID,
        "a RUN is a root, not a connection's child"
    );
    assert_eq!(attr_str(&root, "db.system.name").as_deref(), Some("slater"));
    assert_eq!(attr_str(&root, "db.namespace").as_deref(), Some("people"));
    assert_eq!(
        attr_str(&root, "db.operation.name").as_deref(),
        Some("read")
    );
    assert_eq!(attr_str(&root, "enduser.id").as_deref(), Some("reporting"));
    assert_eq!(
        attr_str(&root, "network.protocol.version").as_deref(),
        Some("5.4")
    );
    assert_eq!(root.status, Status::Unset);

    let mut names: Vec<String> = children_of(&root)
        .iter()
        .map(|s| s.name.to_string())
        .collect();
    names.sort();
    assert_eq!(names, ["slater.encode", "slater.execute", "slater.parse"]);
    let exec = children_of(&root)
        .into_iter()
        .find(|s| s.name == "slater.execute")
        .unwrap();
    assert_eq!(
        attr(&exec, "db.response.returned_rows"),
        Some(&OtelValue::I64(1))
    );
    assert_eq!(
        attr(&exec, "slater.result_cache.hit"),
        Some(&OtelValue::Bool(false))
    );
}

/// Parameters are data, and never recorded as span attributes; the query text is the
/// one-line, truncated form the log already prints. (A failure message, exported as
/// the status, can quote a value — this test covers a successful run.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parameters_never_reach_the_exporter() {
    harness();
    let (_root, ctx) = build_ctx("otel_params");
    let params = PsValue::Map(vec![("secret".into(), PsValue::str("hunter2-otel"))]);
    run(&ctx, "RETURN $secret AS s, 'otel-marker-2' AS m", params)
        .await
        .expect("query runs");

    let root = run_span_for("otel-marker-2");
    let mut trace: Vec<SpanData> = children_of(&root);
    trace.push(root);
    for span in &trace {
        for kv in &span.attributes {
            assert!(
                !kv.value.as_str().contains("hunter2-otel"),
                "{} carries a parameter value in {}",
                span.name,
                kv.key
            );
        }
    }
}

/// A syntax error is the client's fault: reported as `error.type`, but the span
/// status stays unset, so it does not count as a server error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_syntax_error_is_recorded_but_is_not_a_server_error() {
    harness();
    let (_root, ctx) = build_ctx("otel_syntax");
    let err = run(&ctx, "RETURN 'otel-marker-3' AS", PsValue::Map(vec![]))
        .await
        .expect_err("not a statement");
    assert_eq!(err.code, CODE_SYNTAX);

    let root = run_span_for("otel-marker-3");
    assert_eq!(attr_str(&root, "error.type").as_deref(), Some(CODE_SYNTAX));
    assert_eq!(root.status, Status::Unset);
    let parse = children_of(&root)
        .into_iter()
        .find(|s| s.name == "slater.parse")
        .unwrap();
    assert_eq!(
        attr(&parse, "slater.parse.ok"),
        Some(&OtelValue::Bool(false))
    );
}

/// An execution failure IS a server error, even though its Bolt code is a
/// `Neo.ClientError.*` one — budget trips and timeouts share it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_execution_failure_is_a_server_error() {
    harness();
    // `build_ctx` caps results at 100 000 rows; 200 000 trips it at execution.
    let (_root, ctx) = build_ctx("otel_exec_fail");
    let err = run(
        &ctx,
        "UNWIND range(1, 200000) AS x RETURN x, 'otel-marker-4' AS m",
        PsValue::Map(vec![]),
    )
    .await
    .expect_err("200 000 rows past the 100 000-row cap");
    assert_eq!(err.code, CODE_EXECUTION, "{}", err.message);

    let root = run_span_for("otel-marker-4");
    assert_eq!(
        attr_str(&root, "error.type").as_deref(),
        Some(CODE_EXECUTION)
    );
    assert!(
        matches!(root.status, Status::Error { .. }),
        "execution failures must mark the span ERROR, got {:?}",
        root.status
    );
}

/// The builder child is handed this trace in `TRACEPARENT`, naming the
/// `slater.builder` span as its parent — the link that makes a consolidation one
/// trace across two processes. A stand-in builder prints what it received.
#[cfg(unix)]
#[test]
fn the_builder_child_receives_the_builder_span_as_its_traceparent() {
    use std::os::unix::fs::PermissionsExt as _;
    harness();
    let dir = std::env::temp_dir().join(format!("slater_otel_builder_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let script = dir.join("fake-builder");
    // Fails on purpose: a failed builder's stdout tail is in the error, which is how
    // this test reads what the child saw.
    std::fs::write(
        &script,
        "#!/bin/sh\necho \"TP=${TRACEPARENT:-none}\"\nexit 3\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let outer = tracing::info_span!("otel-test.consolidate");
    let outer_cx = outer.context();
    let err = outer
        .in_scope(|| {
            run_builder(
                script.to_str().unwrap(),
                &dir.join("dump"),
                "g",
                &dir,
                None,
                BuilderLimits::default(),
                None,
                None,
            )
        })
        .expect_err("the stand-in builder exits 3");
    drop(outer);
    let msg = format!("{err:#}");

    let trace_id = outer_cx.span().span_context().trace_id();
    let builder = spans()
        .into_iter()
        .find(|s| s.name == "slater.builder" && s.span_context.trace_id() == trace_id)
        .expect("a slater.builder span in the caller's trace");
    assert_eq!(builder.span_kind, SpanKind::Client);
    assert_eq!(
        builder.parent_span_id,
        outer_cx.span().span_context().span_id()
    );
    assert_eq!(
        attr(&builder, "process.exit.code"),
        Some(&OtelValue::I64(3))
    );
    assert!(matches!(builder.status, Status::Error { .. }));

    let expected = format!("TP=00-{}-{}-01", trace_id, builder.span_context.span_id());
    assert!(
        msg.contains(&expected),
        "child saw a different context: {msg}\nwanted {expected}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
