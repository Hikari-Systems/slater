// SPDX-License-Identifier: Apache-2.0
//! The per-graph single-writer that fronts the writable layer.
//!
//! [`DeltaWriter`] owns a graph's durability floor ([`WalSink`]) and its
//! authoritative in-RAM [`Memtable`], and publishes an immutable read snapshot
//! that queries overlay through [`MergedView`](crate::read_view::MergedView). It is
//! the runtime home of the write flow the plan describes:
//!
//! ```text
//! parse → resolve business key to a core dense id (ISAM) → WAL append+commit
//!       → memtable apply → publish snapshot
//! ```
//!
//! # One writer, many readers
//! Every mutation is serialised behind [`DeltaWriter::inner`] (a `Mutex`), so the
//! memtable has exactly one writer — matching the discipline
//! [`slater_delta::memtable`] is built for. Readers never touch that lock: they
//! take a cheap `Arc<Memtable>` clone from the published `RwLock<Arc<Memtable>>`
//! guard (the same shape the generation guard uses for `Arc<Generation>`), so a
//! query pins a consistent delta for its whole life and the writer can move on.
//!
//! # Durability = the commit barrier
//! [`DeltaWriter::write`] returns only after [`WalSink::commit`] has fsynced the
//! record, so a `Seq` handed back to the caller is durable: the Bolt `SUCCESS` the
//! server sends afterwards therefore implies durability (D44). Phase 1c commits one
//! record per call; group-commit batching (many records, one fsync) is a later
//! throughput optimisation the segment format already supports.
//!
//! # Core-generation binding
//! The resolved dense-id read index ([`Memtable::by_dense`]) is only valid against
//! the *core generation the writes were resolved against* — dense ids are permuted
//! by every rebuild. [`DeltaWriter::core_uuid`] records that generation so the
//! server can fail safe (serve the pure core) rather than overlay a delta onto a
//! generation whose dense ids no longer line up. Re-resolving a live delta across a
//! hot-reload swap is out of scope for Phase 1c — consolidation (Phase 1d) is the
//! sanctioned path that folds the delta into a fresh core.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{Context, Result};
use graph_format::ids::Generation as GenId;
use slater_delta::{replay_dir, Memtable, Seq, WalOp, WalRecord, WalSink};

/// Serialised writer state — reached only under the `Mutex`, never by readers.
struct WriterInner {
    dir: PathBuf,
    /// The segment currently open for appends.
    sink: WalSink,
    /// The authoritative memtable; the published snapshot is a clone of it.
    mem: Memtable,
    /// The last sequence number assigned (0 before the first write).
    seq: Seq,
}

/// The per-graph writable-layer writer. Cheap to clone-share behind an `Arc`.
pub struct DeltaWriter {
    graph: String,
    /// The core generation the delta's dense ids were resolved against.
    core_uuid: GenId,
    /// Single-writer serialisation of every mutation.
    inner: Mutex<WriterInner>,
    /// The published immutable read snapshot (readers clone the `Arc`).
    snapshot: RwLock<Arc<Memtable>>,
    /// Bumps on every published change; folded into the result-cache key so an
    /// overlaid result is invalidated by the next write (see `server::result_key`).
    epoch: AtomicU64,
}

impl DeltaWriter {
    /// Open (or re-open) the writer for `graph`, whose WAL segments live under
    /// `dir`. Replays every committed record into the authoritative memtable —
    /// resolving each business key to its current-core dense id via `resolve`
    /// (`None` for a key absent from the core, i.e. a delta-born node) — then opens
    /// a *fresh* segment after the highest existing one so no committed segment is
    /// ever truncated. `core_uuid` is the generation the dense ids resolve against.
    pub fn open(
        dir: impl AsRef<Path>,
        graph: &str,
        core_uuid: GenId,
        resolve: impl Fn(&WalOp) -> Option<u64>,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let replay = replay_dir(&dir).with_context(|| format!("replay WAL dir {dir:?}"))?;
        let mut mem = Memtable::new();
        for rec in &replay.records {
            mem.apply(&rec.op, resolve(&rec.op));
        }

        let next_segment = next_segment_number(&dir)?;
        let sink = WalSink::create(&dir, next_segment)
            .with_context(|| format!("open WAL segment {next_segment} under {dir:?}"))?;

        let snapshot = Arc::new(mem.clone());
        Ok(Self {
            graph: graph.to_string(),
            core_uuid,
            inner: Mutex::new(WriterInner {
                dir,
                sink,
                mem,
                seq: replay.last_seq,
            }),
            snapshot: RwLock::new(snapshot),
            epoch: AtomicU64::new(1),
        })
    }

    /// The graph this writer serves.
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// The core generation the delta's dense ids were resolved against. The server
    /// overlays this delta only on a generation with this UUID.
    pub fn core_uuid(&self) -> GenId {
        self.core_uuid
    }

    /// A consistent immutable snapshot of the memtable — one `Arc` clone, no writer
    /// contention. A query pins this for its whole life.
    pub fn snapshot(&self) -> Arc<Memtable> {
        self.snapshot.read().expect("delta snapshot lock").clone()
    }

    /// The current delta epoch (monotonic; bumps on every published write).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Durably apply one write: append the record, commit (fsync — the ack
    /// barrier), fold it into the authoritative memtable, and publish the new
    /// snapshot. Returns the durable sequence number. `resolved` is the business
    /// key's current-core dense id (`None` marks a delta-born node, Phase 2).
    pub fn write(&self, op: WalOp, resolved: Option<u64>) -> Result<Seq> {
        let mut inner = self.inner.lock().expect("delta writer lock");
        let seq = inner.seq.next();
        let rec = WalRecord { seq, op };
        inner.sink.append(&rec).context("append WAL record")?;
        inner.sink.commit(seq).context("commit WAL batch")?;
        inner.seq = seq;
        inner.mem.apply(&rec.op, resolved);
        // Publish an immutable clone, then bump the epoch so readers keying on it
        // see the new state (publish-before-bump: an observer that reads the higher
        // epoch is guaranteed to also see the swapped-in snapshot).
        let published = Arc::new(inner.mem.clone());
        *self.snapshot.write().expect("delta snapshot lock") = published;
        self.epoch.fetch_add(1, Ordering::AcqRel);
        Ok(seq)
    }

    /// Number of distinct node identities currently carrying a delta (diagnostics).
    pub fn node_delta_count(&self) -> usize {
        self.snapshot().node_delta_count()
    }

    /// Approximate resident memtable size in bytes (diagnostics / budget checks).
    pub fn bytes(&self) -> usize {
        self.inner.lock().expect("delta writer lock").mem.bytes()
    }

    /// The directory holding this graph's WAL segments.
    pub fn wal_dir(&self) -> PathBuf {
        self.inner.lock().expect("delta writer lock").dir.clone()
    }
}

/// The next unused segment number under `dir`: one past the highest `NNNN.wal`, or
/// 0 if the directory has none. Opening a fresh number guarantees an existing
/// committed segment is never truncated by [`WalSink::create`].
fn next_segment_number(dir: &Path) -> Result<u64> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e).with_context(|| format!("list WAL dir {dir:?}")),
    };
    let mut max: Option<u64> = None;
    for entry in rd {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wal") {
            continue;
        }
        if let Some(n) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
        {
            max = Some(max.map_or(n, |m| m.max(n)));
        }
    }
    Ok(match max {
        Some(m) => m + 1,
        None => 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::ids::Value;

    fn upsert(label: &str, key: &str, value: Value, patches: &[(&str, Value)]) -> WalOp {
        WalOp::UpsertNode {
            label: label.into(),
            key: key.into(),
            value,
            patches: patches
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    /// A resolver that maps the fixture's tickers to fixed dense ids.
    fn resolve_ticker(op: &WalOp) -> Option<u64> {
        let WalOp::UpsertNode { value, .. } = op;
        match value {
            Value::Str(s) if s == "A" => Some(10),
            Value::Str(s) if s == "B" => Some(20),
            _ => None,
        }
    }

    fn tmp(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("slater_dw_{tag}_{}", std::process::id()))
    }

    #[test]
    fn write_publishes_snapshot_and_bumps_epoch() {
        let dir = tmp("publish");
        let _ = std::fs::remove_dir_all(&dir);
        let w = DeltaWriter::open(&dir, "g", GenId(uuid::Uuid::nil()), resolve_ticker).unwrap();
        assert_eq!(w.snapshot().node_delta_count(), 0);
        let e0 = w.epoch();

        w.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(10))],
            ),
            Some(10),
        )
        .unwrap();

        let snap = w.snapshot();
        let d = snap.node_patch(10).expect("resolved by dense id");
        assert_eq!(d.patches.get("price"), Some(&Value::Int(10)));
        assert!(w.epoch() > e0, "epoch bumps on write");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reopen_replays_committed_writes() {
        let dir = tmp("reopen");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let w = DeltaWriter::open(&dir, "g", GenId(uuid::Uuid::nil()), resolve_ticker).unwrap();
            w.write(
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("A".into()),
                    &[("price", Value::Int(11))],
                ),
                Some(10),
            )
            .unwrap();
            w.write(
                upsert(
                    "Company",
                    "ticker",
                    Value::Str("B".into()),
                    &[("price", Value::Int(22))],
                ),
                Some(20),
            )
            .unwrap();
        }
        // A fresh writer over the same dir must rebuild the same memtable from the WAL.
        let w2 = DeltaWriter::open(&dir, "g", GenId(uuid::Uuid::nil()), resolve_ticker).unwrap();
        let snap = w2.snapshot();
        assert_eq!(snap.node_delta_count(), 2);
        assert_eq!(
            snap.node_patch(10).unwrap().patches.get("price"),
            Some(&Value::Int(11))
        );
        assert_eq!(
            snap.node_patch(20).unwrap().patches.get("price"),
            Some(&Value::Int(22))
        );
        // The reopened writer appends to a fresh segment, leaving the sealed one intact.
        w2.write(
            upsert(
                "Company",
                "ticker",
                Value::Str("A".into()),
                &[("price", Value::Int(99))],
            ),
            Some(10),
        )
        .unwrap();
        let w3 = DeltaWriter::open(&dir, "g", GenId(uuid::Uuid::nil()), resolve_ticker).unwrap();
        assert_eq!(
            w3.snapshot().node_patch(10).unwrap().patches.get("price"),
            Some(&Value::Int(99)),
            "last-writer-wins survives across segments"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn next_segment_number_advances_past_existing() {
        let dir = tmp("segno");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(next_segment_number(&dir).unwrap(), 0); // missing dir
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("0000000000.wal"), b"x").unwrap();
        std::fs::write(dir.join("0000000003.wal"), b"x").unwrap();
        std::fs::write(dir.join("notes.txt"), b"x").unwrap();
        assert_eq!(next_segment_number(&dir).unwrap(), 4);
        std::fs::remove_dir_all(&dir).ok();
    }
}
