// SPDX-License-Identifier: Apache-2.0
//! OpenTelemetry for `slater-build`: one trace per build — a `slater_build.run` root
//! and a span per phase (`build.pass1`, `build.resolve`, `build.emit.topology`, …) —
//! plus the process CPU / memory / I/O / pressure metrics `hs_utils::otel` reports.
//!
//! # Configuration
//!
//! The same `otel` block the server reads, from the same places the server reads it:
//! `/app/config.json` (else `./config.json`), the `${CONFIG_PATH:-/sandbox}/config.json`
//! overlay, and `otel__*` environment overrides — but here **every file is optional**
//! (a standalone build has none), and **only the `otel` subtree is read**. The house
//! loader resolves every `[SECRET]:` in the whole document; a build has no business
//! pulling the server's S3 keys into its heap to find out whether to export spans.
//!
//! So a consolidation child — which inherits the server's environment and working
//! directory — exports exactly when the server does, with the same endpoint and key,
//! and a standalone build is switched on with e.g. `otel__enabled=true
//! otel__endpoint=… otel__apiKey=…`.
//!
//! The service name is `<serviceName>-build` (`slater-build` when none is set), so a
//! build's spans and process metrics are not mixed into the server's.
//!
//! # Trace context
//!
//! A W3C `TRACEPARENT` (and `TRACESTATE`) in the environment — the OpenTelemetry
//! environment-variable carrier — makes the build a child of that trace. The server
//! sets it on a consolidation's builder, so a consolidation is one trace from the
//! `CALL slater.consolidate()` (or the write that triggered it) to the last phase.
//!
//! # Not a runtime dependency
//!
//! With `otel.enabled` unset (the default) nothing is exported and no exporter thread
//! starts: [`init`] is then exactly the plain stdout logging the build always had.

use serde_json::Value;

pub const TRACEPARENT_ENV: &str = "TRACEPARENT";
pub const TRACESTATE_ENV: &str = "TRACESTATE";

/// Install logging (and, when configured, the OTLP exporters). `quiet` keeps stdout
/// silent — the `-q` contract — while still exporting, at `otel.level` or `info`.
///
/// Returns the guard to hold for the life of the build (dropping it flushes); the
/// warnings met while reading the config are logged once logging is up.
///
/// Infallible on purpose. An exporter that cannot be built (a malformed endpoint, say)
/// falls back to plain logging with a warning: telemetry must never be the reason a
/// build — or the consolidation waiting on it — fails.
pub fn init(quiet: bool) -> Option<hs_utils::otel::OtelGuard> {
    let (mut cfg, warnings) = load_config();
    cfg.service_name = build_service_name(&cfg.service_name);
    let level = if quiet {
        // stdout off; the export would follow it (an empty `otel.level` means "the
        // log level") and go dark too, so give it the level stdout would have had.
        if cfg.level.trim().is_empty() {
            cfg.level = "info".to_string();
        }
        "off"
    } else {
        "info"
    };
    let guard = match hs_utils::otel::init(level, &cfg) {
        Ok(guard) => Some(guard),
        Err(e) => {
            // `otel::init` fails only while building an exporter, before it installs
            // the subscriber, so plain logging can still go in.
            hs_utils::logging::init(level);
            tracing::warn!("telemetry export disabled: {e:#}");
            None
        }
    };
    for w in warnings {
        tracing::warn!("{w}");
    }
    guard
}

/// `<serviceName>-build`, or `slater-build` when no name is configured. A name that
/// already ends in `-build` (set for the build on purpose) is kept as it is.
pub fn build_service_name(configured: &str) -> String {
    let name = configured.trim();
    if name.is_empty() {
        "slater-build".to_string()
    } else if name.ends_with("-build") {
        name.to_string()
    } else {
        format!("{name}-build")
    }
}

/// Read the `otel` block: base file, overlay, env overrides — every file optional,
/// only the `otel` subtree resolved. A file that exists but cannot be read or parsed
/// is skipped with a warning (returned, to be logged once a subscriber exists): a
/// broken telemetry config must not fail a build, but it must not be silent either.
pub fn load_config() -> (hs_utils::otel::OtelConfig, Vec<String>) {
    let base = if std::path::Path::new("/app/config.json").exists() {
        "/app/config.json".to_string()
    } else {
        "config.json".to_string()
    };
    let sandbox = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "/sandbox".to_string());
    let overlay = format!("{}/config.json", sandbox.trim_end_matches('/'));
    load_config_from(&[base, overlay], std::env::vars())
}

/// The testable core of [`load_config`]: `files` in layering order, `env` as
/// `(name, value)` pairs.
pub fn load_config_from(
    files: &[String],
    env: impl IntoIterator<Item = (String, String)>,
) -> (hs_utils::otel::OtelConfig, Vec<String>) {
    let mut warnings = Vec::new();
    let mut otel = Value::Object(Default::default());
    for path in files {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                warnings.push(format!(
                    "telemetry config: cannot read {path}: {e}; skipped"
                ));
                continue;
            }
        };
        match serde_json::from_str::<Value>(&text) {
            Ok(mut doc) => {
                if let Some(sub) = doc.get_mut("otel").map(Value::take) {
                    hs_utils::config::deep_merge(&mut otel, sub);
                }
            }
            Err(e) => warnings.push(format!(
                "telemetry config: cannot parse {path}: {e}; skipped"
            )),
        }
    }
    // Env overrides are `__`-paths from the document root; apply only the `otel__*`
    // ones, onto the `otel` subtree alone, so no other key is created or resolved.
    let mut root = serde_json::json!({ "otel": otel });
    for (name, value) in env {
        if let Some(rest) = name.strip_prefix("otel__") {
            set_path(
                &mut root["otel"],
                &rest.split("__").collect::<Vec<_>>(),
                value,
            );
        }
    }
    let mut otel = root["otel"].take();
    // `[SECRET]:` indirection (the API key) + the all-strings normalisation the
    // config deserialisers expect — over this subtree only.
    hs_utils::config::prepare_config(&mut otel);
    let cfg = serde_json::from_value(otel).unwrap_or_else(|e| {
        warnings.push(format!(
            "telemetry config: invalid `otel` block ({e}); export disabled"
        ));
        hs_utils::otel::OtelConfig::default()
    });
    (cfg, warnings)
}

fn set_path(node: &mut Value, path: &[&str], value: String) {
    if !node.is_object() {
        *node = Value::Object(Default::default());
    }
    let map = node.as_object_mut().expect("just made an object");
    match path {
        [] => {}
        [leaf] => {
            map.insert((*leaf).to_string(), Value::String(value));
        }
        [head, rest @ ..] => set_path(
            map.entry((*head).to_string()).or_insert(Value::Null),
            rest,
            value,
        ),
    }
}

/// The build's root span, `slater_build.run`, as a child of `parent` when one was
/// handed over (see [`parent_from_env`]).
pub fn run_span(
    graph: &str,
    input_format: &str,
    parent: Option<opentelemetry::Context>,
) -> tracing::Span {
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    let span = tracing::info_span!("slater_build.run");
    if let Some(parent) = parent {
        // Fails only if the span already started, which a fresh span has not.
        let _ = span.set_parent(parent);
    }
    span.set_attribute("db.namespace", graph.to_string());
    span.set_attribute("slater_build.input.format", input_format.to_string());
    span
}

/// The trace context a parent process handed us in `TRACEPARENT` / `TRACESTATE`, if
/// any. Call after [`init`]: it extracts with the global propagator, which
/// `hs_utils::otel::init` sets to W3C trace-context when export is enabled. With
/// export off the global propagator is a no-op and this returns `None` — correctly,
/// since a parent for spans nobody exports changes nothing.
pub fn parent_from_env() -> Option<opentelemetry::Context> {
    parent_from(
        std::env::var(TRACEPARENT_ENV).ok(),
        std::env::var(TRACESTATE_ENV).ok(),
    )
}

pub fn parent_from(
    traceparent: Option<String>,
    tracestate: Option<String>,
) -> Option<opentelemetry::Context> {
    use opentelemetry::trace::TraceContextExt as _;
    let mut carrier = std::collections::HashMap::new();
    carrier.insert("traceparent".to_string(), traceparent?);
    if let Some(ts) = tracestate {
        carrier.insert("tracestate".to_string(), ts);
    }
    let cx = opentelemetry::global::get_text_map_propagator(|p| p.extract(&carrier));
    cx.span().span_context().is_valid().then_some(cx)
}
