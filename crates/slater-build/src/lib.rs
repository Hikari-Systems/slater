// SPDX-License-Identifier: Apache-2.0
//! `slater-build` as a **library** — the offline writer's pipeline, callable rather than
//! only spawnable.
//!
//! # Why this exists
//! The build used to be binary-only (no `lib.rs`, every module private to the binary), so
//! the one way to reach it was `execve`. That forced the server to locate a *second*
//! binary on disk — which it can get wrong (`slater:latest-lite` ships none at all, and
//! `/app` is not on the distroless `PATH`) and which can be a different version than the
//! server that spawned it.
//!
//! Exposing the pipeline lets the server re-exec **itself** as a consolidation worker
//! instead: same code, same version, nothing to find. The worker is still a separate
//! *process* — that part is load-bearing and unchanged. A rebuild peaks at ~5.66 GB RSS on
//! the 91.6M-node core, runs 1.07–2.08× its own `--max-memory` cap, and nothing anywhere
//! caps RSS, so it must not share an address space with a server whose headline guarantee
//! is a few hundred MB of resident memory. What changes here is *which* binary the child
//! is, not whether there is one.
//!
//! # What stays in the binary
//! The clap CLI, the `resolve_*` argument plumbing, and — importantly — the
//! `#[global_allocator]`. A crate may only have one, and `slater` declares its own, so an
//! allocator here would refuse to link the moment the server depends on this crate.

pub mod buckets;
pub mod build_external;
pub mod cluster;
pub mod common;
pub mod compression;
pub mod diag;
pub mod direct_ingest;
pub mod merge_build;
pub mod model;
pub mod overlay;
pub mod parser;
pub mod resolve;
pub mod set_eval;
pub mod shared;

// The surface a caller actually needs to run a build, re-exported so it does not have to
// know which module each piece lives in.
pub use build_external::build_external;
pub use cluster::ClusterMode;
pub use diag::BuildDiag;
pub use shared::{BuildOptions, InputFormat};
