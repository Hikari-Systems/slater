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
use slater::{acl, config, health, server};
use tracing::info;

fn main() -> anyhow::Result<()> {
    // Stdlib-only subcommands run before anything else and without the async
    // runtime, mirroring the house pattern. `hash-password` mints an argon2id
    // hash for the ACL and exits; the health probe speaks Bolt (not HTTP, unlike
    // `hs_utils::healthcheck`), so we use a Bolt handshake probe.
    acl::hash_password_subcommand();
    health::check_subcommand(config::load().map(|c| c.server.port).unwrap_or(7687));

    let cfg = config::load()?;
    hs_utils::logging::init(&cfg.log.level);
    info!(
        bind = %cfg.server.bind,
        port = cfg.server.port,
        data_dir = %cfg.data_dir,
        tls = cfg.tls.enabled(),
        "starting slater (read-only Bolt graph engine)"
    );

    // Build the tokio runtime ourselves (the stdlib-only subcommands above must run
    // before any async machinery), then hand off to the Bolt listener, which opens
    // every graph under `data_dir`, loads the ACL, and serves connections until the
    // process is signalled.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    runtime.block_on(server::serve(cfg))
}
