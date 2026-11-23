// SPDX-License-Identifier: Apache-2.0
//! `slater` — the online, read-only Bolt server (binary entry point).
//!
//! Speaks Bolt over (optionally) TLS, authenticates against a JSON ACL, selects
//! a graph, parses a read-only Cypher subset, and executes against the immutable
//! on-disk format through bounded caches — keeping resident memory flat
//! regardless of graph size.
//!
//! This is a thin wrapper: the engine itself lives in the `slater` library (see
//! `lib.rs`), so integration tests can drive it in-process. Here we run the
//! stdlib-only subcommands, load config + logging, build the `tokio` runtime,
//! and hand off to [`slater::server::serve`].

use anyhow::Context;
use slater::{acl, config, dump, health, help, query, server};
use tracing::info;

// On Linux, jemalloc is the global allocator. Built with the `background_threads`
// feature, its purge threads return freed heap to the OS on jemalloc's decay timer
// without the process needing to make `free()` calls — so a burst of heavy queries
// (wide scans, aggregation tables, expansion frontiers) no longer pins RSS at its
// post-burst high-water once load subsides. This replaces the former idle-gated
// `malloc_trim` FFI, moving the last `unsafe` out of this crate and into the audited
// allocator. Non-Linux targets keep the system allocator unchanged.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> anyhow::Result<()> {
    // `--help`/`help` runs before any config load so it works with no config file
    // present, and lists the subcommands + config knobs the server has no flags for.
    help::help_subcommand();
    // Stdlib-only subcommands run before anything else and without the async
    // runtime, mirroring the house pattern. `hash-password` mints an argon2id
    // hash for the ACL and exits; the health probe speaks Bolt (not HTTP, unlike
    // `hs_utils::healthcheck`), so we use a Bolt handshake probe.
    acl::hash_password_subcommand();
    let default_port = config::load().map(|c| c.server.port).unwrap_or(7687);
    health::check_subcommand(default_port);
    // `diagnostics` opens a Bolt session, runs `CALL slater.diagnostics()`, prints
    // the snapshot as JSON, and exits — an operator/CI convenience over the same
    // statement the load-test coordinator reads.
    health::diagnostics_subcommand(default_port);
    // `dump` connects over Bolt to a running server, authenticates, and exports a
    // graph as business-key `MERGE` Cypher (or `--list`s the caller's graphs). It
    // is a blocking Bolt client, so like the probes above it runs before the async
    // runtime is built; it needs only the default port from config.
    dump::dump_subcommand(default_port);

    let mut cfg = config::load()?;
    if cfg.otel.service_name.trim().is_empty() {
        cfg.otel.service_name = "slater".to_string();
    }

    // `query` runs one Cypher statement in-process against a mounted generation
    // and exits — a scripting/CI convenience. It needs the resolved config (for
    // the storage backend, encryption key, and query budgets), so unlike the
    // stdlib-only subcommands above it runs just after config load, before the
    // async runtime is built. It inits its own logging (or none, under `-q`).
    query::query_subcommand(&cfg);

    // Build the tokio runtime ourselves (the stdlib-only subcommands above must run
    // before any async machinery). Logging is installed from inside it: the OTLP
    // exporter's runtime gauges look for the current runtime when they start.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // Query execution and storage reads run on the blocking pool; raise its size
    // for a remote backend, where each in-flight cold read parks one thread on
    // the network round-trip. 0 keeps the tokio default.
    if cfg.server.max_blocking_threads > 0 {
        builder.max_blocking_threads(cfg.server.max_blocking_threads);
    }
    let runtime = builder.build().context("build tokio runtime")?;

    // stdout logging always, plus OTLP traces + metrics when `otel.enabled` (inert
    // otherwise — this is then exactly `hs_utils::logging::init`). The guard flushes
    // whatever is still queued when it drops, so it lives until `main` returns.
    let mut otel = runtime.block_on(async { hs_utils::otel::init(&cfg.log.level, &cfg.otel) })?;
    info!(
        version = env!("CARGO_PKG_VERSION"),
        bind = %cfg.server.bind,
        port = cfg.server.port,
        data_dir = %cfg.data_dir(),
        tls = cfg.tls.enabled(),
        writable = cfg.delta.enabled,
        "starting slater (Bolt graph engine)"
    );

    // Hand off to the Bolt listener, which opens every graph under `data_dir`, loads
    // the ACL, and serves connections until the process is signalled.
    //
    // A signal normally kills the process outright, which would drop the spans of
    // the last few seconds — typically the ones that explain why it was stopped. So
    // with export on, SIGTERM/SIGINT flush first and then exit as the signal would
    // have (128 + signo). With export off nothing is installed, and a signal does
    // exactly what it always did.
    let flush_on_signal = cfg.otel.enabled;
    let served = runtime.block_on(async move {
        if !flush_on_signal {
            return server::serve(cfg).await.map(|()| None);
        }
        tokio::select! {
            r = server::serve(cfg) => r.map(|()| None),
            code = termination_signal() => Ok(Some(code)),
        }
    });
    // Off the runtime: the exporters' shutdown blocks on their own threads.
    otel.shutdown();
    match served? {
        Some(code) => std::process::exit(code),
        None => Ok(()),
    }
}

/// Wait for SIGTERM or SIGINT; returns the exit code the signal's default action
/// implies (128 + signo). Only awaited when OTLP export is on — see `main`.
#[cfg(unix)]
async fn termination_signal() -> i32 {
    use tokio::signal::unix::{signal, SignalKind};
    let (mut term, mut int) = match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(t), Ok(i)) => (t, i),
        // Cannot listen: never resolve, leaving the default (immediate) action.
        _ => return std::future::pending().await,
    };
    tokio::select! {
        _ = term.recv() => { info!("SIGTERM: flushing telemetry and exiting"); 143 }
        _ = int.recv() => { info!("SIGINT: flushing telemetry and exiting"); 130 }
    }
}

#[cfg(not(unix))]
async fn termination_signal() -> i32 {
    let _ = tokio::signal::ctrl_c().await;
    130
}
