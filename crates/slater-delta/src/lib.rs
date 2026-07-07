// SPDX-License-Identifier: Apache-2.0
//! Slater writable layer — a read-favouring log-structured-merge (LSM) tree
//! layered over the immutable core generation.
//!
//! The immutable `graph-format` generation is the fully-compacted **core** (the
//! bottom level). Writes accumulate in a WAL + in-RAM [`memtable`], spill to
//! immutable L0 delta segments, and a *consolidation rebuild* (major compaction,
//! fan-in two) folds `{core + frozen delta}` into a new core published through the
//! atomic generation swap that already exists in `graph-format`/`slater-build`.
//!
//! This crate is the single owner of the delta byte formats and the read-merge
//! fold *logic*, so the writer (`slater`) and the consolidation reader
//! (`slater-build`) can never drift — the same discipline `graph-format`'s module
//! docs describe for the core format.
//!
//! # Layers, newest-wins on read
//!
//! ```text
//! active memtable  ->  L0(newest..oldest)  ->  core
//! ```
//!
//! First hit wins for existence; property patches fold last-writer-wins; a
//! tombstone in any layer suppresses older layers.
//!
//! # Invariants (see `docs/WRITABLE-PLAN.md`)
//!
//! - Delta records bind to the **business key**, never to a per-generation dense
//!   id (dense ids are unstable across builds; the `cluster` phase permutes them).
//! - The delta layer carries its own explicit byte budgets and never grows
//!   resident memory with core size.
//! - An **empty delta is a zero-cost read fast path** — [`DeltaSnapshot::is_empty`]
//!   lets the reader skip the overlay entirely.
//!
//! British English is used throughout docs and messages.

pub mod identity;
pub mod interner;
pub mod memtable;
pub mod wal;

pub use identity::{EdgeIdentity, NodeIdentity};
pub use memtable::{DeltaEdge, DeltaSnapshot, EdgeDelta, Memtable, NodeDelta, OpResolution};
pub use wal::{replay_dir, replay_segment, Replay, SealedSegment, Seq, WalOp, WalRecord, WalSink};
