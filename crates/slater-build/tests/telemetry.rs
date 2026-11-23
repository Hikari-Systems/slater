// SPDX-License-Identifier: Apache-2.0
//! `slater-build`'s telemetry: where its `otel` block comes from, and the trace it
//! produces — a `slater_build.run` root that adopts a parent's `TRACEPARENT`, with a
//! span per phase beneath it. Spans are read back from an in-memory exporter behind
//! the same `tracing-opentelemetry` layer `hs_utils::otel` installs.

use std::sync::OnceLock;

use opentelemetry::trace::{SpanId, TraceId, TracerProvider as _};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use slater_build::diag::BuildDiag;
use slater_build::telemetry::{build_service_name, load_config_from, parent_from, run_span};
use tracing_subscriber::layer::SubscriberExt as _;

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
        let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("build-test"));
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
            .unwrap();
        // What `hs_utils::otel::init` installs when export is on.
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        Harness {
            exporter,
            _provider: provider,
        }
    })
}

fn spans_in(trace: TraceId) -> Vec<SpanData> {
    harness()
        .exporter
        .get_finished_spans()
        .unwrap()
        .into_iter()
        .filter(|s| s.span_context.trace_id() == trace)
        .collect()
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_build_otel_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const PARENT: &str = "00f067aa0ba902b7";

/// Handed a `TRACEPARENT`, the build is a child of it — not a new root — and each
/// phase is a `build.<phase>` child of the build.
#[test]
fn the_build_joins_the_parents_trace_and_each_phase_is_a_child() {
    harness();
    let parent =
        parent_from(Some(format!("00-{TRACE}-{PARENT}-01")), None).expect("a valid traceparent");
    let diag = BuildDiag::disabled();
    {
        let run = run_span("people", "Merge", Some(parent));
        let _entered = run.enter();
        drop(diag.phase("pass1"));
        drop(diag.phase("emit.topology"));
    }

    let trace = TraceId::from_hex(TRACE).unwrap();
    let spans = spans_in(trace);
    let run = spans
        .iter()
        .find(|s| s.name == "slater_build.run")
        .expect("the build span is in the parent's trace");
    assert_eq!(run.parent_span_id, SpanId::from_hex(PARENT).unwrap());
    let mut phases: Vec<String> = spans
        .iter()
        .filter(|s| s.parent_span_id == run.span_context.span_id())
        .map(|s| s.name.to_string())
        .collect();
    phases.sort();
    assert_eq!(phases, ["build.emit.topology", "build.pass1"]);
}

#[test]
fn a_malformed_or_absent_traceparent_is_no_parent() {
    harness();
    assert!(parent_from(None, None).is_none());
    assert!(parent_from(Some("not-a-traceparent".into()), None).is_none());
    // All-zero ids are invalid by the W3C spec.
    let zeros = format!("00-{}-{}-01", "0".repeat(32), "0".repeat(16));
    assert!(parent_from(Some(zeros), None).is_none());
}

#[test]
fn the_build_reports_under_its_own_service_name() {
    assert_eq!(build_service_name(""), "slater-build");
    assert_eq!(build_service_name("  "), "slater-build");
    assert_eq!(build_service_name("slater"), "slater-build");
    assert_eq!(build_service_name("bae-slater"), "bae-slater-build");
    // Set for the build on purpose: kept, not doubled.
    assert_eq!(build_service_name("slater-build"), "slater-build");
}

fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// No config file at all — the usual standalone build — is not an error: export is
/// simply off.
#[test]
fn no_config_files_means_export_off_without_warnings() {
    let dir = scratch("none");
    let (cfg, warnings) = load_config_from(
        &[
            dir.join("config.json").display().to_string(),
            dir.join("sandbox/config.json").display().to_string(),
        ],
        env(&[]),
    );
    assert!(!cfg.enabled);
    assert!(warnings.is_empty(), "{warnings:?}");
}

/// Base, then overlay, then `otel__*` env — the server's layering — with the API
/// key's `[SECRET]:` indirection resolved. Env names outside `otel__` are ignored.
#[test]
fn the_otel_block_layers_like_the_servers_and_resolves_its_secret() {
    let dir = scratch("layers");
    let key = dir.join("honeycomb-key");
    std::fs::write(&key, "the-key\n").unwrap();
    let base = dir.join("config.json");
    std::fs::write(
        &base,
        serde_json::json!({
            "server": { "port": 7687 },
            "otel": { "endpoint": "http://base:4318", "serviceName": "slater", "sampleRatio": 0.5 }
        })
        .to_string(),
    )
    .unwrap();
    let overlay = dir.join("overlay.json");
    std::fs::write(
        &overlay,
        serde_json::json!({
            "otel": { "endpoint": "http://overlay:4318", "apiKey": format!("[SECRET]:{}", key.display()) }
        })
        .to_string(),
    )
    .unwrap();

    let (cfg, warnings) = load_config_from(
        &[base.display().to_string(), overlay.display().to_string()],
        env(&[
            ("otel__enabled", "true"),
            ("otel__environment", "preview"),
            ("server__port", "1"),
            ("PATH", "/bin"),
        ]),
    );
    assert!(warnings.is_empty(), "{warnings:?}");
    assert!(cfg.enabled, "env override");
    assert_eq!(cfg.endpoint, "http://overlay:4318", "overlay beats base");
    assert_eq!(
        cfg.service_name, "slater",
        "base survives where nothing overrides it"
    );
    assert_eq!(cfg.environment, "preview");
    assert_eq!(cfg.sample_ratio, 0.5);
    assert_eq!(
        cfg.api_key, "the-key",
        "[SECRET]: resolved, trailing newline stripped"
    );
}

/// A broken config file is skipped with a warning — it must not fail a build, and it
/// must not be silent — and the remaining layers still apply.
#[test]
fn an_unparseable_config_file_is_skipped_loudly() {
    let dir = scratch("broken");
    let base = dir.join("config.json");
    std::fs::write(&base, "{ not json").unwrap();
    let (cfg, warnings) = load_config_from(
        &[base.display().to_string()],
        env(&[("otel__enabled", "true"), ("otel__apiKeyRequired", "false")]),
    );
    assert!(cfg.enabled);
    assert!(!cfg.api_key_required);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("cannot parse"), "{warnings:?}");
}
