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
//! # Phase 1a — the local `WalSink` floor
//!
//! Segment file layout (`<dir>/<segment>.wal`):
//! ```text
//! MAGIC(8 "SLWAL001") ‖ frame*                 (frames in append order)
//! frame  = len:u32(LE) ‖ crc32c:u32(LE) ‖ payload[len]   (crc over payload)
//! payload= kind:u8 ‖ ( RECORD: encoded WalRecord | COMMIT: committed_seq:uvarint )
//! ```
//! A batch appends one or more `RECORD` frames then a single `COMMIT` frame and
//! fsyncs — that fsync is the durability point and the Bolt ack barrier. Replay
//! keeps records **only up to the last complete `COMMIT`**, so a torn or
//! un-fsynced tail (a `crc` mismatch, a short read, or records past the last
//! marker) is discarded — exactly "the writes whose batch fsync completed, and no
//! more".

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use graph_format::ids::Value;
use graph_format::wire::{read_uvarint, read_value, write_uvarint, write_value};

/// Segment magic — a quick "is this a Slater WAL segment" sniff.
const WAL_MAGIC: &[u8; 8] = b"SLWAL001";

const KIND_RECORD: u8 = 1;
const KIND_COMMIT: u8 = 2;

const OP_UPSERT_NODE: u8 = 1;
const OP_DELETE_NODE: u8 = 2;
const OP_UPSERT_EDGE: u8 = 3;
const OP_DELETE_EDGE: u8 = 4;
const OP_REMOVE_NODE_PROPS: u8 = 5;
const OP_REPLACE_NODE: u8 = 6;
const OP_SET_NODE_LABELS: u8 = 7;

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

/// A node business key `(label, key-property, value)` — the identity the writer
/// resolves to a current-core dense id.
pub type NodeKey<'a> = (&'a str, &'a str, &'a Value);

/// A primitive write operation. A node op names its target by a business key
/// `(label, key, value)`; an edge op names both endpoints by their business keys
/// plus the relationship type (Phase 3).
///
/// Symbol *names* are carried inline (not delta-local ids) so replay re-interns to
/// identical ids deterministically and the segment is self-describing.
#[derive(Debug, Clone, PartialEq)]
pub enum WalOp {
    /// Overwrite (last-writer-wins) properties on the node identified by
    /// `(label, key, value)`. In Phase 2 the same op also creates a delta-born node.
    UpsertNode {
        label: String,
        key: String,
        value: Value,
        /// `(property-name, value)` patches, applied in order (later wins).
        patches: Vec<(String, Value)>,
    },
    /// Tombstone the node identified by `(label, key, value)`: the core row is
    /// suppressed on read and dropped at consolidation (Phase 2). Last-writer-wins
    /// with [`WalOp::UpsertNode`] — a later upsert on the same key resurrects it.
    DeleteNode {
        label: String,
        key: String,
        value: Value,
    },
    /// Drop properties from the node identified by `(label, key, value)` (`REMOVE n.p`):
    /// each named property is folded out of the core row on read and dropped at
    /// consolidation. Last-writer-wins with [`WalOp::UpsertNode`] on the same key.
    RemoveNodeProps {
        label: String,
        key: String,
        value: Value,
        /// The property names to remove, in source order.
        props: Vec<String>,
    },
    /// Replace *all* properties of the node identified by `(label, key, value)`
    /// (`SET n = {map}`): the core properties are ignored on read and the node carries
    /// only `patches` afterwards (the anchor business key is re-seeded from identity).
    ReplaceNode {
        label: String,
        key: String,
        value: Value,
        patches: Vec<(String, Value)>,
    },
    /// Add and/or drop labels on the node identified by `(label, key, value)`
    /// (`SET n:Label` / `REMOVE n:Label`). The labels are unioned with (or folded out
    /// of) the node's core/identity labels on read. Last-writer-wins per label name.
    SetNodeLabels {
        label: String,
        key: String,
        value: Value,
        added: Vec<String>,
        removed: Vec<String>,
    },
    /// Create (or, once edge properties land, patch) the relationship identified by
    /// `(src business key) -[reltype]-> (dst business key)` (Phase 3). A `MERGE`
    /// create; idempotent by edge identity. `patches` is reserved for edge-property
    /// overlays (empty for now).
    UpsertEdge {
        src_label: String,
        src_key: String,
        src_value: Value,
        reltype: String,
        dst_label: String,
        dst_key: String,
        dst_value: Value,
        patches: Vec<(String, Value)>,
    },
    /// Tombstone the relationship `(src) -[reltype]-> (dst)`: the edge is suppressed
    /// on traversal and dropped at consolidation (Phase 3). Last-writer-wins with
    /// [`WalOp::UpsertEdge`].
    DeleteEdge {
        src_label: String,
        src_key: String,
        src_value: Value,
        reltype: String,
        dst_label: String,
        dst_key: String,
        dst_value: Value,
    },
}

impl WalOp {
    /// The single node business key a *node* op targets — `None` for an edge op.
    pub fn node_key(&self) -> Option<NodeKey<'_>> {
        match self {
            WalOp::UpsertNode {
                label, key, value, ..
            }
            | WalOp::DeleteNode { label, key, value }
            | WalOp::RemoveNodeProps {
                label, key, value, ..
            }
            | WalOp::ReplaceNode {
                label, key, value, ..
            }
            | WalOp::SetNodeLabels {
                label, key, value, ..
            } => Some((label, key, value)),
            WalOp::UpsertEdge { .. } | WalOp::DeleteEdge { .. } => None,
        }
    }

    /// The `(src key, reltype, dst key)` an *edge* op targets — `None` for a node op.
    pub fn edge_keys(&self) -> Option<(NodeKey<'_>, &str, NodeKey<'_>)> {
        match self {
            WalOp::UpsertEdge {
                src_label,
                src_key,
                src_value,
                reltype,
                dst_label,
                dst_key,
                dst_value,
                ..
            }
            | WalOp::DeleteEdge {
                src_label,
                src_key,
                src_value,
                reltype,
                dst_label,
                dst_key,
                dst_value,
            } => Some((
                (src_label, src_key, src_value),
                reltype,
                (dst_label, dst_key, dst_value),
            )),
            WalOp::UpsertNode { .. }
            | WalOp::DeleteNode { .. }
            | WalOp::RemoveNodeProps { .. }
            | WalOp::ReplaceNode { .. }
            | WalOp::SetNodeLabels { .. } => None,
        }
    }
}

/// One durable WAL entry: a sequence number and the operation it records.
#[derive(Debug, Clone, PartialEq)]
pub struct WalRecord {
    pub seq: Seq,
    pub op: WalOp,
}

impl WalRecord {
    fn encode_payload(&self, buf: &mut Vec<u8>) {
        buf.push(KIND_RECORD);
        write_uvarint(buf, self.seq.0);
        match &self.op {
            WalOp::UpsertNode {
                label,
                key,
                value,
                patches,
            } => {
                buf.push(OP_UPSERT_NODE);
                write_str(buf, label);
                write_str(buf, key);
                write_value(buf, value);
                write_uvarint(buf, patches.len() as u64);
                for (prop, val) in patches {
                    write_str(buf, prop);
                    write_value(buf, val);
                }
            }
            WalOp::DeleteNode { label, key, value } => {
                buf.push(OP_DELETE_NODE);
                write_str(buf, label);
                write_str(buf, key);
                write_value(buf, value);
            }
            WalOp::RemoveNodeProps {
                label,
                key,
                value,
                props,
            } => {
                buf.push(OP_REMOVE_NODE_PROPS);
                write_str(buf, label);
                write_str(buf, key);
                write_value(buf, value);
                write_uvarint(buf, props.len() as u64);
                for p in props {
                    write_str(buf, p);
                }
            }
            WalOp::ReplaceNode {
                label,
                key,
                value,
                patches,
            } => {
                buf.push(OP_REPLACE_NODE);
                write_str(buf, label);
                write_str(buf, key);
                write_value(buf, value);
                write_uvarint(buf, patches.len() as u64);
                for (prop, val) in patches {
                    write_str(buf, prop);
                    write_value(buf, val);
                }
            }
            WalOp::SetNodeLabels {
                label,
                key,
                value,
                added,
                removed,
            } => {
                buf.push(OP_SET_NODE_LABELS);
                write_str(buf, label);
                write_str(buf, key);
                write_value(buf, value);
                write_uvarint(buf, added.len() as u64);
                for l in added {
                    write_str(buf, l);
                }
                write_uvarint(buf, removed.len() as u64);
                for l in removed {
                    write_str(buf, l);
                }
            }
            WalOp::UpsertEdge {
                src_label,
                src_key,
                src_value,
                reltype,
                dst_label,
                dst_key,
                dst_value,
                patches,
            } => {
                buf.push(OP_UPSERT_EDGE);
                write_str(buf, src_label);
                write_str(buf, src_key);
                write_value(buf, src_value);
                write_str(buf, reltype);
                write_str(buf, dst_label);
                write_str(buf, dst_key);
                write_value(buf, dst_value);
                write_uvarint(buf, patches.len() as u64);
                for (prop, val) in patches {
                    write_str(buf, prop);
                    write_value(buf, val);
                }
            }
            WalOp::DeleteEdge {
                src_label,
                src_key,
                src_value,
                reltype,
                dst_label,
                dst_key,
                dst_value,
            } => {
                buf.push(OP_DELETE_EDGE);
                write_str(buf, src_label);
                write_str(buf, src_key);
                write_value(buf, src_value);
                write_str(buf, reltype);
                write_str(buf, dst_label);
                write_str(buf, dst_key);
                write_value(buf, dst_value);
            }
        }
    }

    /// Decode a `RECORD` payload (the leading `KIND_RECORD` byte already consumed).
    fn decode_record_body(r: &mut &[u8]) -> Result<WalRecord> {
        let seq = Seq(read_uvarint(r)?);
        let op_tag = read_u8(r)?;
        match op_tag {
            OP_UPSERT_NODE => {
                let label = read_str(r)?;
                let key = read_str(r)?;
                let value = read_value(r)?;
                let n = read_uvarint(r)? as usize;
                let mut patches = Vec::with_capacity(n.min(r.len()));
                for _ in 0..n {
                    let prop = read_str(r)?;
                    let val = read_value(r)?;
                    patches.push((prop, val));
                }
                Ok(WalRecord {
                    seq,
                    op: WalOp::UpsertNode {
                        label,
                        key,
                        value,
                        patches,
                    },
                })
            }
            OP_DELETE_NODE => {
                let label = read_str(r)?;
                let key = read_str(r)?;
                let value = read_value(r)?;
                Ok(WalRecord {
                    seq,
                    op: WalOp::DeleteNode { label, key, value },
                })
            }
            OP_REMOVE_NODE_PROPS => {
                let label = read_str(r)?;
                let key = read_str(r)?;
                let value = read_value(r)?;
                let n = read_uvarint(r)? as usize;
                let mut props = Vec::with_capacity(n.min(r.len()));
                for _ in 0..n {
                    props.push(read_str(r)?);
                }
                Ok(WalRecord {
                    seq,
                    op: WalOp::RemoveNodeProps {
                        label,
                        key,
                        value,
                        props,
                    },
                })
            }
            OP_REPLACE_NODE => {
                let label = read_str(r)?;
                let key = read_str(r)?;
                let value = read_value(r)?;
                let n = read_uvarint(r)? as usize;
                let mut patches = Vec::with_capacity(n.min(r.len()));
                for _ in 0..n {
                    let prop = read_str(r)?;
                    let val = read_value(r)?;
                    patches.push((prop, val));
                }
                Ok(WalRecord {
                    seq,
                    op: WalOp::ReplaceNode {
                        label,
                        key,
                        value,
                        patches,
                    },
                })
            }
            OP_SET_NODE_LABELS => {
                let label = read_str(r)?;
                let key = read_str(r)?;
                let value = read_value(r)?;
                let na = read_uvarint(r)? as usize;
                let mut added = Vec::with_capacity(na.min(r.len()));
                for _ in 0..na {
                    added.push(read_str(r)?);
                }
                let nr = read_uvarint(r)? as usize;
                let mut removed = Vec::with_capacity(nr.min(r.len()));
                for _ in 0..nr {
                    removed.push(read_str(r)?);
                }
                Ok(WalRecord {
                    seq,
                    op: WalOp::SetNodeLabels {
                        label,
                        key,
                        value,
                        added,
                        removed,
                    },
                })
            }
            OP_UPSERT_EDGE => {
                let src_label = read_str(r)?;
                let src_key = read_str(r)?;
                let src_value = read_value(r)?;
                let reltype = read_str(r)?;
                let dst_label = read_str(r)?;
                let dst_key = read_str(r)?;
                let dst_value = read_value(r)?;
                let n = read_uvarint(r)? as usize;
                let mut patches = Vec::with_capacity(n.min(r.len()));
                for _ in 0..n {
                    let prop = read_str(r)?;
                    let val = read_value(r)?;
                    patches.push((prop, val));
                }
                Ok(WalRecord {
                    seq,
                    op: WalOp::UpsertEdge {
                        src_label,
                        src_key,
                        src_value,
                        reltype,
                        dst_label,
                        dst_key,
                        dst_value,
                        patches,
                    },
                })
            }
            OP_DELETE_EDGE => {
                let src_label = read_str(r)?;
                let src_key = read_str(r)?;
                let src_value = read_value(r)?;
                let reltype = read_str(r)?;
                let dst_label = read_str(r)?;
                let dst_key = read_str(r)?;
                let dst_value = read_value(r)?;
                Ok(WalRecord {
                    seq,
                    op: WalOp::DeleteEdge {
                        src_label,
                        src_key,
                        src_value,
                        reltype,
                        dst_label,
                        dst_key,
                        dst_value,
                    },
                })
            }
            other => bail!("unknown WAL op tag {other}"),
        }
    }
}

/// The local durability floor: an append-structured, fsync-durable log segment on
/// local disk. **Not parameterised by the storage backend** (see D44) — shipping
/// sealed segments to an object store is a separate seam.
pub struct WalSink {
    segment: u64,
    dir: PathBuf,
    path: PathBuf,
    file: BufWriter<File>,
    /// Logical length of the segment as of the last **completed** `commit` — the
    /// offset a failed/aborted batch is rolled back to (HIK-105). Frames appended past
    /// this offset carry no commit marker yet; if their batch never reaches a commit
    /// they must be truncated away, or the *next* batch's commit marker would
    /// retro-commit them on replay (replay's committed prefix is "every frame up to
    /// the last commit marker").
    committed_len: u64,
    /// Logical length of everything written into the segment so far, committed or not.
    /// Tracked in-process (not `fstat`-ed) so the group-commit ack path stays
    /// syscall-light; `commit`/`truncate_to` keep it in step with the file.
    cur_len: u64,
}

impl WalSink {
    /// Open segment `segment` under `dir`, creating `dir` if needed and writing the
    /// magic header. The magic becomes durable with the first `commit`; the segment's
    /// *directory entry* is made durable here, before any writer can ack against it.
    pub fn create(dir: impl AsRef<Path>, segment: u64) -> Result<Self> {
        let dir = dir.as_ref();
        let dir_existed = dir.is_dir();
        fs::create_dir_all(dir).with_context(|| format!("create WAL dir {dir:?}"))?;
        let path = segment_path(dir, segment);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("create WAL segment {path:?}"))?;
        let mut file = BufWriter::new(file);
        file.write_all(WAL_MAGIC)?;
        // Push the magic to the file immediately (no fsync — it carries no committed
        // data). A freshly opened segment can otherwise sit 0 bytes on disk until its
        // first commit, and a concurrent/subsequent `replay_dir` would choke on the
        // missing magic. (`replay_bytes` also tolerates a 0-byte segment for the
        // power-loss-before-flush case.)
        file.flush().context("flush WAL magic on create")?;
        // Make the *directory entry* durable before the sink is handed to a writer.
        // `commit`'s fdatasync persists the segment's data and size but says nothing
        // about the name that finds it: without this, a power loss after an acked
        // commit can leave the file unreachable and the acked write is lost. One fsync
        // per segment — off the per-commit group-commit path.
        fsync_dir(dir)?;
        // If the WAL dir itself is new, its own entry in the parent is equally
        // volatile: fsyncing a directory does not persist the link that names it. On a
        // graph's first-ever write that would take the whole dir — segment and all —
        // down with it. Only on the create-the-dir path; segment rotation skips it.
        if !dir_existed {
            if let Some(parent) = dir.parent().filter(|p| p.is_dir()) {
                fsync_dir(parent)?;
            }
        }
        Ok(Self {
            segment,
            dir: dir.to_path_buf(),
            path,
            file,
            // The magic is flushed above; it is the first thing every batch is
            // rolled back *to* (a rollback never eats the header).
            committed_len: WAL_MAGIC.len() as u64,
            cur_len: WAL_MAGIC.len() as u64,
        })
    }

    /// This segment's number.
    pub fn segment(&self) -> u64 {
        self.segment
    }

    /// This segment's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Buffer one record frame (not yet durable — durability comes from `commit`).
    ///
    /// On any write error the partially-written batch is rolled back to the last
    /// committed offset (HIK-105) before the error is returned, so a failed append
    /// never leaves orphan frames for a later commit to retro-commit. Prefer
    /// [`WalSink::append_batch`] for the whole-batch path: it additionally covers a
    /// mid-batch **panic** via a scope guard, which append+commit as separate calls
    /// cannot.
    pub fn append(&mut self, rec: &WalRecord) -> Result<()> {
        if let Err(e) = self.write_record_frame(rec) {
            self.truncate_to(self.committed_len)
                .context("roll back WAL batch after a failed append")?;
            return Err(e);
        }
        Ok(())
    }

    /// Close the current batch: write the commit marker, flush, and fsync. Returns
    /// only once the batch is durable — the Bolt ack barrier. `committed_seq` is the
    /// highest sequence number the batch makes durable.
    ///
    /// A failure while writing/flushing the commit marker rolls the batch back to the
    /// last committed offset, so a torn commit frame cannot linger for the next
    /// commit to absorb.
    pub fn commit(&mut self, committed_seq: Seq) -> Result<()> {
        if let Err(e) = self.write_commit_frame(committed_seq) {
            self.truncate_to(self.committed_len)
                .context("roll back WAL batch after a failed commit")?;
            return Err(e);
        }
        Ok(())
    }

    /// Append every record in `recs` then commit them under one fsync, **atomically**:
    /// if any frame fails to write, the commit fsync fails, *or the thread unwinds
    /// mid-batch*, the segment is truncated back to its pre-batch offset so a partial
    /// batch leaves no frames behind. This is the whole-batch equivalent of
    /// append×N + commit and is what [`crate`]'s writer uses on the ack path.
    ///
    /// Atomicity on both the `Err` and the unwind paths comes from a stack-local RAII
    /// [`Rollback`] guard (not a `Drop` on `WalSink` — the sink outlives a mid-batch
    /// panic inside the long-lived writer, so its own `Drop` would never run). The
    /// guard mirrors HIK-95's "prepare then install (moves only)" shape: all fallible
    /// work runs armed, and success is a single disarming move that cannot unwind.
    pub fn append_batch(&mut self, recs: &[WalRecord], committed_seq: Seq) -> Result<()> {
        // Self-heal: if a *previous* aborted batch left an orphan tail whose rollback
        // truncate itself failed (a rare double I/O fault), re-establish the committed
        // floor before appending, so this batch's commit can never retro-commit it.
        // Normally a no-op (`cur_len == committed_len` after every clean commit).
        if self.cur_len != self.committed_len {
            self.truncate_to(self.committed_len)
                .context("re-establish committed WAL floor before batch")?;
        }
        let mut batch = Rollback::arm(self);
        for rec in recs {
            batch.sink.write_record_frame(rec)?;
        }
        batch.sink.write_commit_frame(committed_seq)?;
        batch.disarm();
        Ok(())
    }

    /// Encode + write one record frame into the buffer, advancing `cur_len`. Raw: no
    /// rollback of its own (the caller owns batch atomicity).
    fn write_record_frame(&mut self, rec: &WalRecord) -> Result<()> {
        #[cfg(test)]
        maybe_inject_write_fault()?;
        let mut payload = Vec::new();
        rec.encode_payload(&mut payload);
        write_frame(&mut self.file, &payload)?;
        self.cur_len += frame_len(payload.len());
        Ok(())
    }

    /// Write the commit marker, flush and fsync — the durability point. Raw: no
    /// rollback of its own. On success `committed_len` advances to the new tip.
    fn write_commit_frame(&mut self, committed_seq: Seq) -> Result<()> {
        let mut payload = Vec::with_capacity(4);
        payload.push(KIND_COMMIT);
        write_uvarint(&mut payload, committed_seq.0);
        write_frame(&mut self.file, &payload)?;
        self.cur_len += frame_len(payload.len());
        self.file.flush().context("flush WAL buffer")?;
        #[cfg(test)]
        maybe_inject_commit_fsync_fault()?;
        self.file
            .get_ref()
            .sync_data()
            .context("fsync WAL segment")?;
        // The fsync landed: everything up to here is now durable and becomes the
        // rollback floor for any later batch.
        self.committed_len = self.cur_len;
        Ok(())
    }

    /// Rewind the segment to `off` (the last committed offset), discarding any
    /// uncommitted frames a failed/aborted batch left behind. Durable: the shrink is
    /// `sync_data`-ed so replay after a crash-during-rollback cannot see the dropped
    /// tail either. Truncation shrinks the *existing* file, so it changes no directory
    /// entry and needs no dir fsync — it composes with HIK-71's create/seal dir-fsyncs
    /// without adding one to this path.
    fn truncate_to(&mut self, off: u64) -> Result<()> {
        // Flush first so the BufWriter holds no bytes it would later replay past the
        // truncation point; then shrink and re-seat the OS file position at `off`.
        self.file
            .flush()
            .context("flush WAL buffer before rollback truncate")?;
        // Seam (test-only) for the rollback repair failing on a dying disk. It fires
        // *before* `set_len` deliberately: the modelled fault is "the truncate never
        // became durable", and in-process there is no way to make a page-cache-visible
        // `set_len` un-happen. A real fault at the `sync_data` below leaves the shrink
        // in the page cache but not on the platter, so the crash that follows restores
        // the pre-truncate bytes — which is exactly the state failing here produces,
        // and the only state replay-after-crash can observe.
        #[cfg(test)]
        maybe_inject_truncate_fsync_fault()?;
        let mut file = self.file.get_ref();
        file.set_len(off)
            .context("truncate WAL segment on rollback")?;
        file.seek(SeekFrom::Start(off))
            .context("seek WAL segment after rollback truncate")?;
        file.sync_data()
            .context("fsync WAL segment after rollback truncate")?;
        self.cur_len = off;
        Ok(())
    }

    /// Flush + fsync and hand back the sealed (immutable) segment descriptor. The
    /// caller opens a fresh segment for subsequent writes. Unlike `commit` this syncs
    /// the file's full metadata and re-fsyncs the parent directory, so the sealed
    /// segment the shipping tier picks up is durable name and all.
    pub fn seal(mut self) -> Result<SealedSegment> {
        self.file.flush().context("flush WAL buffer on seal")?;
        self.file
            .get_ref()
            .sync_all()
            .context("fsync WAL segment on seal")?;
        fsync_dir(&self.dir)?;
        Ok(SealedSegment {
            segment: self.segment,
            path: self.path,
        })
    }
}

/// Open `dir` and fsync it, making the directory entries created under it durable.
///
/// A failure here is a **hard error**: this is the WAL, and its callers ack writes to
/// the client on the strength of it. It is deliberately not the best-effort
/// `let _ = d.sync_all()` used on the L0 publish path.
fn fsync_dir(dir: &Path) -> Result<()> {
    let d = File::open(dir).with_context(|| format!("open WAL dir for fsync {dir:?}"))?;
    d.sync_all()
        .with_context(|| format!("fsync WAL dir {dir:?}"))?;
    #[cfg(test)]
    DIR_FSYNCS.with(|n| n.set(n.get() + 1));
    Ok(())
}

#[cfg(test)]
thread_local! {
    /// Test seam: counts [`fsync_dir`] calls on the current thread, so a test can pin
    /// *that the directory is synced* on create/seal (a real power-loss test is out of
    /// reach in-process). Thread-local, so parallel tests can't perturb each other.
    static DIR_FSYNCS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };

    /// Test seam (HIK-105): when `Some(n)`, the *n*-th following record-frame write on
    /// this thread returns an injected `io::Error` (`Some(0)` = the very next write),
    /// then resets to `None`. Lets a test force a batch to fail **part-way through** so
    /// the rollback is exercised on real prior frames. Thread-local for test isolation.
    static FAIL_WRITE_AFTER: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };

    /// As `FAIL_WRITE_AFTER`, but `panic!`s instead of returning `Err` — exercises the
    /// rollback guard on the **unwind** path.
    static PANIC_WRITE_AFTER: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };

    /// Test seam (HIK-130): when `Some(n)`, the *n*-th following **commit-path fsync**
    /// on this thread returns an injected `io::Error`, then resets to `None`. The
    /// record-frame seam above cannot reach this arm — it fires only inside
    /// `write_record_frame` — which is why the marker-ordering hazard went untested.
    static FAIL_COMMIT_FSYNC_AFTER: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };

    /// Test seam (HIK-130): as above, but for [`WalSink::truncate_to`]'s own fsync —
    /// the rollback repair. Arming both drives the compound fault (a dying disk failing
    /// the commit fsync *and* the rollback that tries to undo it).
    static FAIL_TRUNCATE_FSYNC_AFTER: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Trip an armed fault counter (test-only): `Some(0)` fires now and disarms; `Some(n)`
/// counts down; `None` never fires.
#[cfg(test)]
fn trip_fault(cell: &'static std::thread::LocalKey<std::cell::Cell<Option<u64>>>) -> bool {
    cell.with(|c| match c.get() {
        Some(0) => {
            c.set(None);
            true
        }
        Some(n) => {
            c.set(Some(n - 1));
            false
        }
        None => false,
    })
}

/// Fire any armed record-frame-write fault seam (test-only). Kept out of
/// `write_record_frame`'s body so the production build has no seam at all.
#[cfg(test)]
fn maybe_inject_write_fault() -> Result<()> {
    if trip_fault(&FAIL_WRITE_AFTER) {
        return Err(anyhow::Error::new(std::io::Error::other(
            "injected WAL record-frame write fault",
        )));
    }
    if trip_fault(&PANIC_WRITE_AFTER) {
        panic!("injected WAL record-frame write panic");
    }
    Ok(())
}

/// Fire any armed commit-fsync fault seam (test-only). Called where the *real*
/// `sync_data` would be, so an injected failure leaves exactly the on-disk state a
/// failed fsync leaves: whatever the preceding flush already handed to the OS.
#[cfg(test)]
fn maybe_inject_commit_fsync_fault() -> Result<()> {
    if trip_fault(&FAIL_COMMIT_FSYNC_AFTER) {
        return Err(anyhow::Error::new(std::io::Error::other(
            "injected WAL commit fsync fault",
        )));
    }
    Ok(())
}

/// Fire any armed rollback-truncate-fsync fault seam (test-only).
#[cfg(test)]
fn maybe_inject_truncate_fsync_fault() -> Result<()> {
    if trip_fault(&FAIL_TRUNCATE_FSYNC_AFTER) {
        return Err(anyhow::Error::new(std::io::Error::other(
            "injected WAL rollback truncate fsync fault",
        )));
    }
    Ok(())
}

/// An immutable, sealed WAL segment — the unit the object-store shipping tier
/// ships (D44). Phase 1a produces it; shipping lands with the backend wiring.
#[derive(Debug, Clone)]
pub struct SealedSegment {
    pub segment: u64,
    pub path: PathBuf,
}

/// The outcome of replaying a segment (or a directory of segments): the durably
/// committed records in order, and the highest committed sequence number.
#[derive(Debug, Default)]
pub struct Replay {
    pub records: Vec<WalRecord>,
    pub last_seq: Seq,
}

/// `<dir>/<segment>.wal`.
pub fn segment_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("{segment:010}.wal"))
}

/// Replay a single segment file, returning only records up to the last complete
/// commit marker (a torn/un-fsynced tail is dropped).
pub fn replay_segment(path: &Path) -> Result<Replay> {
    let bytes = fs::read(path).with_context(|| format!("read WAL segment {path:?}"))?;
    let mut out = Replay::default();
    replay_bytes(&bytes, &mut out).with_context(|| format!("replay WAL segment {path:?}"))?;
    Ok(out)
}

/// Fuzz/test entry point: replay a single in-memory segment image, exercising the
/// frame framing (`len ‖ crc ‖ payload`) and the `decode_record_body` byte decoder
/// without touching the filesystem. Held to the same never-panic /
/// no-giant-pre-allocation contract as the core-segment and wire decoders (see the
/// `wal_replay` fuzz target). Prefer [`replay_segment`]/[`replay_dir`] for real use.
#[doc(hidden)]
pub fn replay_bytes_for_fuzz(bytes: &[u8]) -> Result<Replay> {
    let mut out = Replay::default();
    replay_bytes(bytes, &mut out)?;
    Ok(out)
}

/// Replay every `*.wal` segment under `dir` in ascending segment order, folding
/// their committed records into one ordered stream. Missing dir ⇒ empty replay.
pub fn replay_dir(dir: &Path) -> Result<Replay> {
    let mut segments: Vec<(u64, PathBuf)> = Vec::new();
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Replay::default()),
        Err(e) => return Err(e).with_context(|| format!("list WAL dir {dir:?}")),
    };
    for entry in rd {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wal") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(n) = stem.parse::<u64>() else { continue };
        segments.push((n, path));
    }
    segments.sort_by_key(|(n, _)| *n);

    let mut out = Replay::default();
    for (_, path) in segments {
        let bytes = fs::read(&path).with_context(|| format!("read WAL segment {path:?}"))?;
        replay_bytes(&bytes, &mut out).with_context(|| format!("replay WAL segment {path:?}"))?;
    }
    Ok(out)
}

/// Parse `bytes` (one segment image), appending committed records to `out` and
/// advancing `out.last_seq`. Stops cleanly at the first torn frame.
fn replay_bytes(bytes: &[u8], out: &mut Replay) -> Result<()> {
    // A 0-byte segment is a freshly created one whose magic never reached disk (a
    // crash/power-loss between `create` and its flush). It holds no committed record,
    // so it replays to nothing rather than wedging the whole directory.
    if bytes.is_empty() {
        return Ok(());
    }
    if bytes.len() < WAL_MAGIC.len() || &bytes[..WAL_MAGIC.len()] != WAL_MAGIC {
        bail!("bad or missing WAL magic");
    }
    let mut pos = WAL_MAGIC.len();
    let mut pending: Vec<WalRecord> = Vec::new();
    while pos < bytes.len() {
        // Frame header: len:u32 ‖ crc:u32. A short read here is the crash tail.
        if pos + 8 > bytes.len() {
            break;
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
        let body_start = pos + 8;
        let body_end = match body_start.checked_add(len) {
            Some(end) if end <= bytes.len() => end,
            _ => break, // truncated payload — torn tail
        };
        let payload = &bytes[body_start..body_end];
        if crc32c::crc32c(payload) != crc {
            break; // torn / corrupt frame — stop, keep what committed before it
        }
        pos = body_end;

        let mut r = payload;
        let kind = read_u8(&mut r)?;
        match kind {
            KIND_RECORD => pending.push(WalRecord::decode_record_body(&mut r)?),
            KIND_COMMIT => {
                let committed_seq = Seq(read_uvarint(&mut r)?);
                out.records.append(&mut pending); // drains pending
                if committed_seq > out.last_seq {
                    out.last_seq = committed_seq;
                }
            }
            other => bail!("unknown WAL frame kind {other}"),
        }
    }
    // Any records past the last commit marker are an un-fsynced tail — dropped.
    Ok(())
}

fn write_frame(w: &mut impl Write, payload: &[u8]) -> Result<()> {
    let crc = crc32c::crc32c(payload);
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(&crc.to_le_bytes())?;
    w.write_all(payload)?;
    Ok(())
}

/// On-disk size of a frame carrying a `payload_len`-byte payload: `len:u32 ‖ crc:u32
/// ‖ payload`. Keeps [`WalSink::cur_len`] in step with [`write_frame`] without an
/// `fstat` on the ack path.
fn frame_len(payload_len: usize) -> u64 {
    8 + payload_len as u64
}

/// RAII rollback guard for [`WalSink::append_batch`] (HIK-105). While armed, dropping
/// it truncates the sink back to the offset captured at batch start — covering **both**
/// the `Err` path (a `?` early-return unwinds through this `Drop`) and a mid-batch
/// **panic**, with no `catch_unwind`. It deliberately lives on the *stack* of the
/// batch call, not as a `Drop` on `WalSink`: the sink is owned by the long-lived
/// writer and survives a panic (HIK-95), so its own `Drop` would never run for the
/// unwind we must cover. `disarm` cancels the rollback once the commit fsync lands.
struct Rollback<'a> {
    sink: &'a mut WalSink,
    /// Offset to rewind to on drop; `None` once the batch has durably committed.
    rollback_to: Option<u64>,
}

impl<'a> Rollback<'a> {
    fn arm(sink: &'a mut WalSink) -> Self {
        let rollback_to = Some(sink.committed_len);
        Rollback { sink, rollback_to }
    }

    /// The batch committed durably — cancel the rollback (the disarming "install" move,
    /// which cannot unwind).
    fn disarm(&mut self) {
        self.rollback_to = None;
    }
}

impl Drop for Rollback<'_> {
    fn drop(&mut self) {
        if let Some(off) = self.rollback_to {
            // Err or unwind before disarm: rewind so the partial batch leaves nothing a
            // later commit could retro-commit. The rollback is best-effort here because
            // `Drop` cannot propagate — but the orphan frames carry *no* commit marker,
            // so replay drops them even if this truncate fails, and the next
            // `append_batch` re-establishes the floor before it writes. So a failed
            // rollback degrades to "harmless orphan tail", never to a resurrected write.
            let _ = self.sink.truncate_to(off);
        }
    }
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    write_uvarint(buf, s.len() as u64);
    buf.extend_from_slice(s.as_bytes());
}

fn read_str(r: &mut &[u8]) -> Result<String> {
    let len = read_uvarint(r)? as usize;
    if r.len() < len {
        bail!("WAL string truncated");
    }
    let (s, rest) = r.split_at(len);
    *r = rest;
    String::from_utf8(s.to_vec()).context("WAL string not utf-8")
}

fn read_u8(r: &mut &[u8]) -> Result<u8> {
    let Some((&b, rest)) = r.split_first() else {
        bail!("WAL byte truncated");
    };
    *r = rest;
    Ok(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upsert(
        seq: u64,
        label: &str,
        key: &str,
        value: Value,
        patches: &[(&str, Value)],
    ) -> WalRecord {
        WalRecord {
            seq: Seq(seq),
            op: WalOp::UpsertNode {
                label: label.into(),
                key: key.into(),
                value,
                patches: patches
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            },
        }
    }

    #[test]
    fn delete_op_round_trips_through_a_segment() {
        let dir = std::env::temp_dir().join(format!("slater_wal_del_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let up = upsert(1, "Company", "ticker", Value::Str("A".into()), &[]);
        let del = WalRecord {
            seq: Seq(2),
            op: WalOp::DeleteNode {
                label: "Company".into(),
                key: "ticker".into(),
                value: Value::Str("A".into()),
            },
        };
        {
            let mut sink = WalSink::create(&dir, 0).unwrap();
            sink.append(&up).unwrap();
            sink.append(&del).unwrap();
            sink.commit(Seq(2)).unwrap();
        }
        let replay = replay_dir(&dir).unwrap();
        assert_eq!(replay.records, vec![up, del.clone()]);
        // The decoded op exposes its node business key; an edge op would be `None`.
        assert_eq!(
            del.op.node_key(),
            Some(("Company", "ticker", &Value::Str("A".into())))
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn remove_and_replace_node_ops_round_trip_through_a_segment() {
        let dir = std::env::temp_dir().join(format!("slater_wal_remrep_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let remove = WalRecord {
            seq: Seq(1),
            op: WalOp::RemoveNodeProps {
                label: "Company".into(),
                key: "ticker".into(),
                value: Value::Str("A".into()),
                props: vec!["price".into(), "sector".into()],
            },
        };
        let replace = WalRecord {
            seq: Seq(2),
            op: WalOp::ReplaceNode {
                label: "Company".into(),
                key: "ticker".into(),
                value: Value::Str("A".into()),
                patches: vec![("name".into(), Value::Str("Acme".into()))],
            },
        };
        {
            let mut sink = WalSink::create(&dir, 0).unwrap();
            sink.append(&remove).unwrap();
            sink.append(&replace).unwrap();
            sink.commit(Seq(2)).unwrap();
        }
        let replay = replay_dir(&dir).unwrap();
        assert_eq!(replay.records, vec![remove.clone(), replace.clone()]);
        // Both are node ops — they expose the node business key, not edge keys.
        assert_eq!(
            remove.op.node_key(),
            Some(("Company", "ticker", &Value::Str("A".into())))
        );
        assert!(replace.op.edge_keys().is_none());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn edge_ops_round_trip_through_a_segment() {
        let dir = std::env::temp_dir().join(format!("slater_wal_edge_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let create = WalRecord {
            seq: Seq(1),
            op: WalOp::UpsertEdge {
                src_label: "Company".into(),
                src_key: "ticker".into(),
                src_value: Value::Str("A".into()),
                reltype: "OWNS".into(),
                dst_label: "Drug".into(),
                dst_key: "id".into(),
                dst_value: Value::Int(7),
                patches: vec![("since".into(), Value::Int(2020))],
            },
        };
        let delete = WalRecord {
            seq: Seq(2),
            op: WalOp::DeleteEdge {
                src_label: "Company".into(),
                src_key: "ticker".into(),
                src_value: Value::Str("A".into()),
                reltype: "OWNS".into(),
                dst_label: "Drug".into(),
                dst_key: "id".into(),
                dst_value: Value::Int(7),
            },
        };
        {
            let mut sink = WalSink::create(&dir, 0).unwrap();
            sink.append(&create).unwrap();
            sink.append(&delete).unwrap();
            sink.commit(Seq(2)).unwrap();
        }
        let replay = replay_dir(&dir).unwrap();
        assert_eq!(replay.records, vec![create.clone(), delete.clone()]);
        // An edge op exposes its endpoint keys, not a single node key.
        assert!(create.op.node_key().is_none());
        let (src, rel, dst) = create.op.edge_keys().expect("edge op");
        assert_eq!(src, ("Company", "ticker", &Value::Str("A".into())));
        assert_eq!(rel, "OWNS");
        assert_eq!(dst, ("Drug", "id", &Value::Int(7)));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn seq_advances_monotonically() {
        let a = Seq::default();
        let b = a.next();
        assert!(b > a);
        assert_eq!(b, Seq(1));
    }

    #[test]
    fn append_commit_replay_round_trips() {
        let dir = std::env::temp_dir().join(format!("slater_wal_rt_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let r0 = upsert(
            1,
            "Company",
            "ticker",
            Value::Str("A".into()),
            &[("price", Value::Int(10))],
        );
        let r1 = upsert(
            2,
            "Company",
            "ticker",
            Value::Str("B".into()),
            &[("price", Value::Float(2.5))],
        );
        {
            let mut sink = WalSink::create(&dir, 0).unwrap();
            sink.append(&r0).unwrap();
            sink.append(&r1).unwrap();
            sink.commit(Seq(2)).unwrap();
        }
        let replay = replay_dir(&dir).unwrap();
        assert_eq!(replay.records, vec![r0, r1]);
        assert_eq!(replay.last_seq, Seq(2));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn uncommitted_tail_is_dropped() {
        // A batch that was appended but never committed (crash before fsync) must
        // not appear on replay.
        let dir = std::env::temp_dir().join(format!("slater_wal_tail_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let committed = upsert(1, "L", "k", Value::Int(1), &[]);
        let lost = upsert(2, "L", "k", Value::Int(2), &[]);
        {
            let mut sink = WalSink::create(&dir, 0).unwrap();
            sink.append(&committed).unwrap();
            sink.commit(Seq(1)).unwrap();
            sink.append(&lost).unwrap(); // no commit → not durable
                                         // drop without commit: BufWriter flushes bytes on drop, but no marker.
        }
        let replay = replay_dir(&dir).unwrap();
        assert_eq!(replay.records, vec![committed]);
        assert_eq!(replay.last_seq, Seq(1));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn torn_frame_truncation_is_ignored() {
        // Simulate a crash mid-write: truncate the file partway through the second
        // batch's bytes. Replay must return exactly the first, committed batch.
        let dir = std::env::temp_dir().join(format!("slater_wal_torn_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let r0 = upsert(1, "L", "k", Value::Int(1), &[]);
        let path;
        let good_len;
        {
            let mut sink = WalSink::create(&dir, 0).unwrap();
            sink.append(&r0).unwrap();
            sink.commit(Seq(1)).unwrap();
            good_len = fs::metadata(sink.path()).unwrap().len();
            path = sink.path().to_path_buf();
            // Append a second batch and commit so there are extra bytes on disk.
            sink.append(&upsert(2, "L", "k", Value::Int(2), &[]))
                .unwrap();
            sink.commit(Seq(2)).unwrap();
        }
        // Truncate to just past the first commit but inside the second frame.
        let full = fs::read(&path).unwrap();
        let truncated = &full[..(good_len as usize + 5).min(full.len())];
        fs::write(&path, truncated).unwrap();

        let replay = replay_segment(&path).unwrap();
        assert_eq!(replay.records, vec![r0]);
        assert_eq!(replay.last_seq, Seq(1));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn multi_segment_replay_is_ordered() {
        let dir = std::env::temp_dir().join(format!("slater_wal_multi_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let a = upsert(1, "L", "k", Value::Int(1), &[]);
        let b = upsert(2, "L", "k", Value::Int(2), &[]);
        {
            let mut s0 = WalSink::create(&dir, 0).unwrap();
            s0.append(&a).unwrap();
            s0.commit(Seq(1)).unwrap();
            s0.seal().unwrap();
            let mut s1 = WalSink::create(&dir, 1).unwrap();
            s1.append(&b).unwrap();
            s1.commit(Seq(2)).unwrap();
            s1.seal().unwrap();
        }
        let replay = replay_dir(&dir).unwrap();
        assert_eq!(replay.records, vec![a, b]);
        assert_eq!(replay.last_seq, Seq(2));
        fs::remove_dir_all(&dir).ok();
    }

    /// Regression (HIK-71): `create` and `seal` must fsync the segment's *parent
    /// directory*, not just the segment file. Without it a commit can be acked while
    /// the directory entry naming the segment is still only in page cache, and a power
    /// loss loses an acknowledged write.
    ///
    /// A real power-loss test is out of reach in-process, so this pins the observable
    /// behaviour — the dir fsync is performed on the create and seal paths — via the
    /// thread-local `DIR_FSYNCS` counter. It does not (and cannot) prove the kernel /
    /// filesystem / drive honoured the fsync.
    #[test]
    fn create_and_seal_fsync_the_parent_dir() {
        let dir = std::env::temp_dir().join(format!("slater_wal_dirsync_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let before = DIR_FSYNCS.with(|n| n.get());
        let mut sink = WalSink::create(&dir, 0).unwrap();
        let after_create = DIR_FSYNCS.with(|n| n.get());
        assert_eq!(
            after_create,
            before + 2,
            "creating the first segment must fsync the WAL dir (the segment's entry) \
             and its parent (the freshly created dir's own entry)"
        );

        // Group commit stays on fdatasync alone — the dir entry is already durable, so
        // no further dir fsync belongs on the per-commit ack path.
        sink.append(&upsert(1, "L", "k", Value::Int(1), &[]))
            .unwrap();
        sink.commit(Seq(1)).unwrap();
        assert_eq!(DIR_FSYNCS.with(|n| n.get()), after_create);

        sink.seal().unwrap();
        let after_seal = DIR_FSYNCS.with(|n| n.get());
        assert_eq!(after_seal, after_create + 1, "seal must fsync the WAL dir");

        // Rotation into an existing dir: the new segment's entry still needs the dir
        // fsync, but the parent does not — the dir itself is already durable.
        let next = WalSink::create(&dir, 1).unwrap();
        assert_eq!(DIR_FSYNCS.with(|n| n.get()), after_seal + 1);
        next.seal().unwrap();

        fs::remove_dir_all(&dir).ok();
    }

    /// A failed directory fsync must surface as an error, never be swallowed: the
    /// callers ack client writes on the strength of it.
    #[test]
    fn fsync_dir_propagates_failure() {
        let missing =
            std::env::temp_dir().join(format!("slater_wal_absent_{}", std::process::id()));
        let _ = fs::remove_dir_all(&missing);
        let err = fsync_dir(&missing).expect_err("fsync of a non-existent dir must fail");
        let io = err
            .downcast_ref::<std::io::Error>()
            .expect("the io::Error must be preserved in the chain");
        assert_eq!(io.kind(), std::io::ErrorKind::NotFound);
    }

    /// Regression (HIK-105), `Err` path: a batch that fails **part-way through** must
    /// leave no frames behind — not even the frames it wrote *before* the failing one.
    /// A following *successful* batch (whose commit marker would otherwise retro-commit
    /// the orphans) must replay to exactly the two good writes. Pre-fix (guard's
    /// truncate neutered) the orphan `seq 2` frame comes back as committed.
    #[test]
    fn failed_mid_batch_append_leaves_no_frames_err_path() {
        let dir = std::env::temp_dir().join(format!("slater_wal_rollerr_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let good0 = upsert(1, "L", "k", Value::Int(1), &[]);
        // A three-record batch that will fail on its *second* record frame.
        let doomed = vec![
            upsert(2, "L", "k", Value::Int(2), &[]),
            upsert(3, "L", "k", Value::Int(3), &[]),
            upsert(4, "L", "k", Value::Int(4), &[]),
        ];
        let survivor = upsert(5, "L", "k", Value::Int(5), &[]);
        {
            let mut sink = WalSink::create(&dir, 0).unwrap();
            // A clean committed batch first, so the rollback floor is past the magic.
            sink.append_batch(std::slice::from_ref(&good0), Seq(1))
                .unwrap();

            // Fail the doomed batch on its 2nd record write (one frame already buffered).
            FAIL_WRITE_AFTER.with(|c| c.set(Some(1)));
            let err = sink
                .append_batch(&doomed, Seq(4))
                .expect_err("the injected fault must fail the batch mid-way");
            // Branch on the error *type*, never its text (coding standard).
            assert!(
                err.downcast_ref::<std::io::Error>().is_some(),
                "the injected io::Error must be preserved in the chain: {err:#}"
            );
            FAIL_WRITE_AFTER.with(|c| assert_eq!(c.get(), None, "seam must be consumed"));

            // A *successful* batch to the same segment. Its commit marker is exactly
            // what would retro-commit the doomed batch's orphan frame(s) pre-fix.
            sink.append_batch(std::slice::from_ref(&survivor), Seq(5))
                .unwrap();
        }

        let replay = replay_dir(&dir).unwrap();
        // The failed batch is ABSENT: only the two acknowledged writes survive.
        assert_eq!(replay.records, vec![good0, survivor]);
        assert_eq!(replay.last_seq, Seq(5));
        fs::remove_dir_all(&dir).ok();
    }

    /// Regression (HIK-105), **unwind** path: a *panic* mid-batch must roll the segment
    /// back via the scope guard's `Drop` (no `catch_unwind` in the writer). The sink
    /// survives the panic — as it does inside the long-lived writer — and a following
    /// successful batch must not resurrect the orphan frames.
    #[test]
    fn failed_mid_batch_append_leaves_no_frames_unwind_path() {
        let dir = std::env::temp_dir().join(format!("slater_wal_rollpanic_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let good0 = upsert(1, "L", "k", Value::Int(1), &[]);
        let doomed = vec![
            upsert(2, "L", "k", Value::Int(2), &[]),
            upsert(3, "L", "k", Value::Int(3), &[]),
            upsert(4, "L", "k", Value::Int(4), &[]),
        ];
        let survivor = upsert(5, "L", "k", Value::Int(5), &[]);

        let mut sink = WalSink::create(&dir, 0).unwrap();
        sink.append_batch(std::slice::from_ref(&good0), Seq(1))
            .unwrap();

        // Panic on the doomed batch's 2nd record write. Drive it through `append_batch`
        // and catch the unwind *here* so the test can inspect the after-state; the
        // rollback runs during the unwind, inside the guard's `Drop`.
        PANIC_WRITE_AFTER.with(|c| c.set(Some(1)));
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = sink.append_batch(&doomed, Seq(4));
        }));
        assert!(
            res.is_err(),
            "the injected panic must unwind out of append_batch"
        );
        PANIC_WRITE_AFTER.with(|c| assert_eq!(c.get(), None, "seam must be consumed"));

        // Sink survived the panic; a following good batch must not retro-commit orphans.
        sink.append_batch(std::slice::from_ref(&survivor), Seq(5))
            .unwrap();
        drop(sink);

        let replay = replay_dir(&dir).unwrap();
        assert_eq!(replay.records, vec![good0, survivor]);
        assert_eq!(replay.last_seq, Seq(5));
        fs::remove_dir_all(&dir).ok();
    }

    /// Regression (HIK-130), **commit-fsync** path: the batch is never acked (the commit
    /// fsync fails), its rollback repair also fails (same dying disk), and the process
    /// dies before any later `append_batch` could self-heal. Replay must **not** commit
    /// it — an unacked batch coming back is precisely the failure a WAL exists to
    /// prevent.
    ///
    /// This is the arm the HIK-105 seam could not reach: `maybe_inject_write_fault` fires
    /// only inside `write_record_frame`, so both rollback tests above exercise the arm
    /// that was already correct. Pre-fix (marker flushed to the OS *before* the fsync it
    /// gates on) this test fails with the doomed batch's three records resurrected.
    ///
    /// The three legs are all load-bearing — see `compound_fault_legs_are_each_load_bearing`,
    /// which pins that removing any one of them lets the existing defences win.
    #[test]
    fn unacked_batch_is_not_retro_committed_when_commit_fsync_and_rollback_both_fail() {
        let dir =
            std::env::temp_dir().join(format!("slater_wal_retrocommit_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let good0 = upsert(1, "L", "k", Value::Int(1), &[]);
        let doomed = vec![
            upsert(2, "L", "k", Value::Int(2), &[]),
            upsert(3, "L", "k", Value::Int(3), &[]),
            upsert(4, "L", "k", Value::Int(4), &[]),
        ];
        {
            let mut sink = WalSink::create(&dir, 0).unwrap();
            // A clean committed batch first, so the rollback floor is past the magic and
            // the retro-commit has something to be distinguished from.
            sink.append_batch(std::slice::from_ref(&good0), Seq(1))
                .unwrap();

            // Leg 1: the commit fsync fails — the client never gets an ack.
            FAIL_COMMIT_FSYNC_AFTER.with(|c| c.set(Some(0)));
            // Leg 2: the rollback guard's own repair fails too (correlated, not p² — one
            // dying disk fails both).
            FAIL_TRUNCATE_FSYNC_AFTER.with(|c| c.set(Some(0)));

            let err = sink
                .append_batch(&doomed, Seq(4))
                .expect_err("the injected commit fsync fault must fail the batch");
            // Branch on the error *type*, never its text (coding standard).
            assert!(
                err.downcast_ref::<std::io::Error>().is_some(),
                "the injected io::Error must be preserved in the chain: {err:#}"
            );
            FAIL_COMMIT_FSYNC_AFTER
                .with(|c| assert_eq!(c.get(), None, "commit seam must be consumed"));
            FAIL_TRUNCATE_FSYNC_AFTER
                .with(|c| assert_eq!(c.get(), None, "truncate seam must be consumed"));

            // Leg 3: the process dies here. No further `append_batch`, so the self-heal at
            // the top of `append_batch` never runs — dropping the sink is the crash.
        }

        let replay = replay_dir(&dir).unwrap();
        assert_eq!(
            replay.records,
            vec![good0],
            "an unacked batch must never be replayed as committed"
        );
        assert_eq!(
            replay.last_seq,
            Seq(1),
            "last_seq must not advance to an unacked batch's seq"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_dir_replays_empty() {
        let dir = std::env::temp_dir().join(format!("slater_wal_empty_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let replay = replay_dir(&dir).unwrap();
        assert!(replay.records.is_empty());
        assert_eq!(replay.last_seq, Seq(0));
    }

    #[test]
    fn freshly_created_and_zero_byte_segments_replay_empty() {
        let dir = std::env::temp_dir().join(format!("slater_wal_fresh_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        // `create` flushes the magic, so a fresh never-committed segment already has
        // its 8-byte header on disk and replays to nothing (no committed records).
        let sink = WalSink::create(&dir, 0).unwrap();
        assert_eq!(
            fs::metadata(sink.path()).unwrap().len(),
            WAL_MAGIC.len() as u64
        );
        // A 0-byte segment (a crash/power-loss between create and its flush) is
        // tolerated: it holds no committed record, so it must not wedge the dir.
        fs::write(segment_path(&dir, 1), b"").unwrap();
        let replay = replay_dir(&dir).unwrap();
        assert!(replay.records.is_empty());
        assert_eq!(replay.last_seq, Seq(0));
        fs::remove_dir_all(&dir).ok();
    }
}
