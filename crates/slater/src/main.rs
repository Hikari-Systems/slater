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

    let cfg = config::load()?;

    // `query` runs one Cypher statement in-process against a mounted generation
    // and exits — a scripting/CI convenience. It needs the resolved config (for
    // the storage backend, encryption key, and query budgets), so unlike the
    // stdlib-only subcommands above it runs just after config load, before the
    // async runtime is built. It inits its own logging (or none, under `-q`).
    query::query_subcommand(&cfg);

    hs_utils::logging::init(&cfg.log.level);
    info!(
        version = env!("CARGO_PKG_VERSION"),
        bind = %cfg.server.bind,
        port = cfg.server.port,
        data_dir = %cfg.data_dir(),
        tls = cfg.tls.enabled(),
        "starting slater (read-only Bolt graph engine)"
    );

    // Build the tokio runtime ourselves (the stdlib-only subcommands above must run
    // before any async machinery), then hand off to the Bolt listener, which opens
    // every graph under `data_dir`, loads the ACL, and serves connections until the
    // process is signalled.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // Query execution and storage reads run on the blocking pool; raise its size
    // for a remote backend, where each in-flight cold read parks one thread on
    // the network round-trip. 0 keeps the tokio default.
    if cfg.server.max_blocking_threads > 0 {
        builder.max_blocking_threads(cfg.server.max_blocking_threads);
    }
    let runtime = builder.build().context("build tokio runtime")?;
    runtime.block_on(server::serve(cfg))
}
