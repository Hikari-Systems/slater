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
