// SPDX-License-Identifier: Apache-2.0
//! OpenTelemetry for the server: the span names, the attribute helpers, the query
//! metrics, and the `TRACEPARENT` handoff to the `slater-build` child.
//!
//! The exporter itself is `hs_utils::otel` (installed in `main`), and it is inert
//! until `otel.enabled` is set: with it off, every span here is an ordinary
//! `tracing` span, every attribute call is a no-op, and every instrument is bound
//! to the no-op global meter.
//!
//! # Conventions
//!
//! * Span names are dotted (`bolt.run`, `slater.execute`). Boundary spans carry
//!   `otel.kind` as their only `tracing` field.
//! * **Every other attribute goes through [`OpenTelemetrySpanExt::set_attribute`],
//!   never a `tracing` field.** The stdout formatter prints a span's fields as a
//!   prefix on every log line inside it, so a `db.query.text` field would repeat
//!   the query on each of them, and so would a user name or graph name. Attributes
//!   set this way reach the exporter only.
//! * A request is one trace. A Bolt connection is NOT a span: driver pools keep
//!   connections open for hours, which would make one trace of thousands of
//!   requests. `bolt.run` / `bolt.logon` are roots, and carry the peer address.
//! * Status ERROR means the server failed, not that the client asked for something
//!   it could not have — see [`is_client_fault`].
//!
//! # Cardinality
//!
//! Metric attributes are bounded: `db.namespace` is a served graph (fixed at boot),
//! `db.operation.name` is one of [`Op`], `outcome` is one of three values. Unbounded
//! values (query text, user, peer address) are span attributes only.

use std::net::SocketAddr;
use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::trace::Status;
use opentelemetry::{global, KeyValue};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::{
    Failure, CODE_ACCESS_MODE, CODE_CONSOLIDATION_IN_PROGRESS, CODE_FORBIDDEN, CODE_NOT_FOUND,
    CODE_REQUEST, CODE_SYNTAX, CODE_UNAUTHORIZED,
};

/// The W3C trace-context environment carrier (the OpenTelemetry environment-variable
/// propagation convention). The server sets it on the `slater-build` child so the
/// build's spans join the consolidation's trace; `slater-build` reads it at startup.
pub(crate) const TRACEPARENT_ENV: &str = "TRACEPARENT";
pub(crate) const TRACESTATE_ENV: &str = "TRACESTATE";

/// Longest status message exported. Failure text can carry a builder's output tail
/// (two streams × 40 lines) or a parser excerpt; the span needs the gist, not that.
const MAX_STATUS_MESSAGE: usize = 512;

/// What a `bolt.run` did, for `db.operation.name`. A closed set, so it is safe as a
/// metric attribute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Read,
    Write,
    Create,
    WriteEdge,
    Consolidate,
    Use,
    Introspect,
}

impl Op {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Op::Read => "read",
            Op::Write => "write",
            Op::Create => "create",
            Op::WriteEdge => "write_edge",
            Op::Consolidate => "consolidate",
            Op::Use => "use",
            Op::Introspect => "introspect",
        }
    }
}

/// What one `RUN` resolved to, for the request metric. See `Session::run`.
#[derive(Debug, Default)]
pub(crate) struct RunRecord {
    pub(crate) graph: Option<String>,
    pub(crate) op: Option<Op>,
}

/// A failure the client caused and could have avoided: bad credentials, a missing
/// grant, an unknown graph, a syntax error, a write on a read-only connection, a
/// malformed request, or a consolidation already running. These are reported as
/// `error.type` but leave the span status unset, as HTTP semconv does for a 4xx.
///
/// An allowlist, deliberately: `ExecutionFailed` is also a `ClientError` code, but it
/// covers budget trips, timeouts and tasks that died — the failures an operator is
/// watching for. Anything not named here is a server failure.
pub(crate) fn is_client_fault(code: &str) -> bool {
    matches!(
        code,
        CODE_UNAUTHORIZED
            | CODE_FORBIDDEN
            | CODE_NOT_FOUND
            | CODE_SYNTAX
            | CODE_ACCESS_MODE
            | CODE_REQUEST
            | CODE_CONSOLIDATION_IN_PROGRESS
    )
}

/// The root span for one `RUN`. `peer` is the client socket address.
pub(crate) fn run_span(peer: Option<SocketAddr>, version: (u8, u8), user: &str) -> Span {
    let span = tracing::info_span!(parent: None, "bolt.run", otel.kind = "server");
    span.set_attribute("db.system.name", "slater");
    span.set_attribute("network.protocol.name", "bolt");
    span.set_attribute(
        "network.protocol.version",
        format!("{}.{}", version.0, version.1),
    );
    if !user.is_empty() {
        span.set_attribute("enduser.id", user.to_string());
    }
    set_peer(&span, peer);
    span
}

/// The root span for a `HELLO`/`LOGON` that carries credentials. The principal is
/// recorded only once it has authenticated (see [`logon_succeeded`]): a failed
/// attempt's "user name" is attacker-chosen text, and is sometimes a mistyped
/// password.
pub(crate) fn logon_span(peer: Option<SocketAddr>) -> Span {
    let span = tracing::info_span!(parent: None, "bolt.logon", otel.kind = "server");
    span.set_attribute("network.protocol.name", "bolt");
    set_peer(&span, peer);
    span
}

pub(crate) fn logon_succeeded(span: &Span, user: &str) {
    span.set_attribute("enduser.id", user.to_string());
}

fn set_peer(span: &Span, peer: Option<SocketAddr>) {
    if let Some(peer) = peer {
        span.set_attribute("client.address", peer.ip().to_string());
        span.set_attribute("client.port", i64::from(peer.port()));
    }
}

/// The statement a `RUN` carries, in the same one-line, 160-character form the
/// `query executed` log line uses: nothing reaches the exporter that the log does not
/// already show, and a 64 MB statement is not shipped whole. Parameters are never
/// recorded as attributes. (A failure's message, exported as the span status, can
/// still quote a value it failed on, exactly as the `query failed` log line does.)
pub(crate) fn run_query_text(query: &str) {
    Span::current().set_attribute("db.query.text", super::log_query(query));
}

/// The (validated) graph a `RUN` resolved to.
pub(crate) fn run_graph(record: &mut RunRecord, graph: &str) {
    Span::current().set_attribute("db.namespace", graph.to_string());
    record.graph = Some(graph.to_string());
}

/// What a `RUN` turned out to be, once parsed (or recognised as USE / introspection).
pub(crate) fn run_op(record: &mut RunRecord, op: Op) {
    Span::current().set_attribute("db.operation.name", op.as_str());
    record.op = Some(op);
}

/// Record a request's failure on `span`.
pub(crate) fn record_failure(span: &Span, failure: &Failure) {
    span.set_attribute("error.type", failure.code);
    span.set_attribute("db.response.status_code", failure.code);
    if !is_client_fault(failure.code) {
        span.set_status(Status::error(truncate(&failure.message)));
    }
}

/// Record an internal (non-Bolt) failure: a consolidation, a flush, a refused swap.
pub(crate) fn record_error(span: &Span, error: &anyhow::Error) {
    span.set_attribute("error.type", "internal");
    span.set_status(Status::error(truncate(&format!("{error:#}"))));
}

fn truncate(message: &str) -> String {
    match message.char_indices().nth(MAX_STATUS_MESSAGE) {
        Some((cut, _)) => format!("{}…", &message[..cut]),
        None => message.to_string(),
    }
}

/// The `TRACEPARENT` / `TRACESTATE` values that continue the current span's trace,
/// or none when there is nothing to continue (export off, or the span not sampled
/// in). Set on the `slater-build` child's environment.
pub(crate) fn child_trace_env() -> Vec<(&'static str, String)> {
    let mut carrier = std::collections::HashMap::new();
    let cx = Span::current().context();
    global::get_text_map_propagator(|p| p.inject_context(&cx, &mut carrier));
    let mut env = Vec::new();
    if let Some(v) = carrier.remove("traceparent") {
        env.push((TRACEPARENT_ENV, v));
    }
    if let Some(v) = carrier.remove("tracestate").filter(|v| !v.is_empty()) {
        env.push((TRACESTATE_ENV, v));
    }
    env
}

// ── Metrics ─────────────────────────────────────────────────────────────────

/// Duration buckets, in seconds: sub-millisecond point reads through to a query
/// that runs into a 30 s `timeoutMs`. (The OTel default boundaries assume
/// milliseconds and would put every query under 5 s in the first bucket.)
const QUERY_DURATION_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    60.0,
];

/// A consolidation rebuilds a core: seconds on a test graph, an hour or more at
/// Wikidata scale.
const CONSOLIDATION_DURATION_BUCKETS: &[f64] = &[
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0, 7200.0, 14400.0,
];

pub(crate) struct Metrics {
    pub(crate) request_duration: Histogram<f64>,
    pub(crate) block_cache: Counter<u64>,
    pub(crate) result_cache: Counter<u64>,
    pub(crate) consolidation_duration: Histogram<f64>,
}

/// The server's instruments, created on first use.
///
/// Lazily, and never before `main` has installed the exporter: `global::meter`
/// binds to whichever provider is global *when it is called*, so instruments made
/// earlier would stay no-op for the life of the process.
pub(crate) fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let m = global::meter("slater");
        Metrics {
            request_duration: m
                .f64_histogram("slater.request.duration")
                .with_unit("s")
                .with_description("Bolt RUN duration, request to result buffered")
                .with_boundaries(QUERY_DURATION_BUCKETS.to_vec())
                .build(),
            block_cache: m
                .u64_counter("slater.block_cache.accesses")
                .with_unit("{access}")
                .with_description("Block-cache lookups by queries, by result (hit/miss)")
                .build(),
            result_cache: m
                .u64_counter("slater.result_cache.lookups")
                .with_unit("{lookup}")
                .with_description("Result-cache lookups by read queries, by result (hit/miss)")
                .build(),
            consolidation_duration: m
                .f64_histogram("slater.consolidation.duration")
                .with_unit("s")
                .with_description("Consolidation wall time: dump, rebuild, swap and retire")
                .with_boundaries(CONSOLIDATION_DURATION_BUCKETS.to_vec())
                .build(),
        }
    })
}

/// `outcome` for a request metric: `ok`, `client_error` or `error` — the three-way
/// split [`is_client_fault`] draws, so the error rate counts server failures only.
pub(crate) fn outcome(result: Result<(), &Failure>) -> &'static str {
    match result {
        Ok(()) => "ok",
        Err(f) if is_client_fault(f.code) => "client_error",
        Err(_) => "error",
    }
}

pub(crate) fn kv(key: &'static str, value: impl Into<opentelemetry::Value>) -> KeyValue {
    KeyValue::new(key, value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::CODE_EXECUTION;

    #[test]
    fn execution_failures_are_server_faults_despite_their_client_error_code() {
        // Budget trips, timeouts and dead tasks all surface as ExecutionFailed — a
        // `ClientError` code. A prefix rule would have hidden every one of them.
        assert!(CODE_EXECUTION.starts_with("Neo.ClientError."));
        assert!(!is_client_fault(CODE_EXECUTION));
        assert!(is_client_fault(CODE_SYNTAX));
        assert!(is_client_fault(CODE_UNAUTHORIZED));
    }

    #[test]
    fn status_messages_are_bounded_on_a_char_boundary() {
        let long = "é".repeat(MAX_STATUS_MESSAGE + 10);
        let t = truncate(&long);
        assert_eq!(t.chars().count(), MAX_STATUS_MESSAGE + 1); // + the ellipsis
        assert_eq!(truncate("short"), "short");
    }
}
