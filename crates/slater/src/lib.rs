// SPDX-License-Identifier: Apache-2.0
//! `slater` — the online, read-only Bolt graph engine, as a library.
//!
//! The modules below make up the server: the Bolt/PackStream wire layer, the
//! ACL, the read-only Cypher parser/planner/executor, the immutable on-disk
//! generation reader, the three bounded cache pools, and the `tokio` listener
//! that ties them together. They are exposed as a library (not just compiled
//! into the binary) so integration tests under `crates/slater/tests/` can drive
//! the real server in-process — notably the bounded-RSS *headline* test, which
//! stands up [`server::serve_with_listener`] against a synthetic above-threshold
//! Vamana generation far larger than the cache budgets and samples
//! `/proc/self/statm` under load (D34).
//!
//! The `slater` binary (`main.rs`) is a thin wrapper that loads config, inits
//! logging, builds the `tokio` runtime, and hands off to [`server::serve`].

pub mod acl;
pub mod algo;
pub mod bolt;
pub mod cache;
pub mod config;
pub mod consolidate;
pub mod cron_window;
pub mod degree_column;
pub mod delta_writer;
pub mod diag;
pub mod dump;
pub mod exec;
pub mod flush_segment;
pub mod generation;
pub mod health;
pub mod help;
pub mod introspect;
pub mod merge_segment;
pub mod parser;
pub mod plan;
pub mod query;
pub mod read_view;
pub mod rwindex;
pub mod segstack;
pub mod server;
pub mod temporal;
pub mod vector;

// Shared in-crate test fixture (built directly with the `graph-format` writers).
// Compiled only for the crate's own unit tests; the `tests/` integration crate
// builds its own (much larger) fixture from the public `graph-format` API.
// Gated `pub` under `testkit` (as well as `test`) so the delta-overlay benchmark
// can build a real generation from the public `graph-format` API without shipping
// the fixture code in a normal `slater` build.
#[cfg(any(test, feature = "testkit"))]
pub mod testgen;

// Phase 8 read-amp harness support (scaled fixture + stacked-set builder + cold-cache
// reader). Gated `pub` under `testkit` like `testgen` so the `segment_read_amp` bench can
// build stacked fixtures without shipping the code in a normal build.
#[cfg(any(test, feature = "testkit"))]
pub mod benchkit;

// Shared Vamana + PQ plumbing for the FreshDiskANN performance suite (HIK-120): the on-disk
// index lifecycle (ANN-space map → build → write → beam-search → consolidate → merge) reused by
// the vector_recall / vector_delete_io / streaming_merge benches. Gated `pub` under `testkit`
// like `testgen` / `benchkit`.
#[cfg(any(test, feature = "testkit"))]
pub mod vecbench;
