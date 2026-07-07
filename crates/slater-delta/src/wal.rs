// SPDX-License-Identifier: Apache-2.0
//! Write-ahead log — the durability floor for the writable layer.
//!
//! A per-graph append-only log of primitive statements (the builder's `model.rs`
//! grammar plus a delete variant), each tagged with a monotonic sequence number.
//! Durability contract (confirmed): **batch-fsync / group commit** — many writes
//! share one `fsync`, and a write's Bolt `SUCCESS` is returned *strictly after*
//! the `fsync` that covers it, so *acknowledged implies durable* and the kill-9
//! window is closed.
//!
//! Responsibilities (built out in Phase 1):
//! - append + batch-fsync;
//! - segment rotation, one WAL segment per memtable generation / L0 flush;
//! - replay-on-startup to reconstruct the active memtable;
//! - truncate a segment once its contents are durable elsewhere (flushed to L0,
//!   or absorbed by a consolidation).
//!
//! The on-disk durability discipline mirrors the builder's atomic publish
//! (`common::write_manifest_and_publish`, DECISIONS D14): temp + fsync + atomic
//! rename, with a `SLATER_*_FAIL_AFTER`-style fault-injection hook exercised by a
//! kill-during-batch replay test.
//!
//! # DESIGN: two durability seams with contradictory contracts
//!
//! The WAL is split across **two seams that must not be folded together** (see
//! `docs/WRITABLE-PLAN.md` §"WAL durability tiers"):
//!
//! - **`WalSink` — the local durability floor.** Ordered, append-structured,
//!   fsync-durable at sub-millisecond latency. **Local disk is the only medium
//!   that honours this contract; it is _not_ parameterised by the storage
//!   backend.** A record never travels through `ObjectStore`. A write is acked to
//!   the Bolt client only after `WalSink::sync` (the group-commit fsync) resolves.
//! - **`ObjectStore` — shipping of sealed segments.** Sealed WAL segments are
//!   shipped as **numbered, immutable, content-addressed** objects (never one
//!   growing object — S3/GCS have no append), with a `wal/HEAD` pointer object
//!   written **last** as the copy-completeness barrier, exactly as
//!   `common::write_manifest_and_publish` writes `current` last. This is the
//!   *only* place the backend contract is involved, and it reuses
//!   `ObjectStore::put` verbatim — no WAL-shaped methods are added to the trait.
//!
//! So `fs`/`s3`/`gcs` governs **only the shipping tier**; the floor is always
//! local. Consequences that Phase 1/4 wire and the ops guide must state:
//! - **Truncation gate:** a local segment is not retired until its object-store
//!   PUT is acked (trivially satisfied on a local-disk-only deployment, which has
//!   no shipping tier — the floor *is* the durable store).
//! - **Freeze forces a flush:** consolidation reads the frozen delta from the
//!   object store, so freeze ships the frozen WAL tail (and any un-shipped L0)
//!   *before* spawning the builder, overriding the periodic writeback timer.
//! - **The writeback interval is one knob with two faces:** object-store RPO *and*
//!   cross-replica read-visibility lag. A process crash loses nothing (local
//!   replay); losing the local volume loses at most one interval of un-shipped
//!   writes — so the writer node needs a durable local volume, not ephemeral
//!   instance storage.
//!
//! Phase 0 is a documented placeholder; no records are written yet.

/// A monotonic per-graph WAL sequence number.
///
/// Assigned in write order; the replay invariant is `replay(WAL) == memtable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Seq(pub u64);

impl Seq {
    /// The next sequence number.
    pub fn next(self) -> Seq {
        Seq(self.0 + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_advances_monotonically() {
        let a = Seq::default();
        let b = a.next();
        assert!(b > a);
        assert_eq!(b, Seq(1));
    }
}
