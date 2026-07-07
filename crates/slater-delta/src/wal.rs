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
use std::io::{BufWriter, Write};
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
            | WalOp::DeleteNode { label, key, value } => Some((label, key, value)),
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
            WalOp::UpsertNode { .. } | WalOp::DeleteNode { .. } => None,
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
                let mut patches = Vec::with_capacity(n);
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
            OP_UPSERT_EDGE => {
                let src_label = read_str(r)?;
                let src_key = read_str(r)?;
                let src_value = read_value(r)?;
                let reltype = read_str(r)?;
                let dst_label = read_str(r)?;
                let dst_key = read_str(r)?;
                let dst_value = read_value(r)?;
                let n = read_uvarint(r)? as usize;
                let mut patches = Vec::with_capacity(n);
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
    path: PathBuf,
    file: BufWriter<File>,
    /// Highest seq appended but not yet returned by a completed `commit`.
    highest_appended: Option<Seq>,
}

impl WalSink {
    /// Open segment `segment` under `dir`, creating `dir` if needed and writing the
    /// magic header. The magic becomes durable with the first `commit`.
    pub fn create(dir: impl AsRef<Path>, segment: u64) -> Result<Self> {
        let dir = dir.as_ref();
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
        Ok(Self {
            segment,
            path,
            file,
            highest_appended: None,
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
    pub fn append(&mut self, rec: &WalRecord) -> Result<()> {
        let mut payload = Vec::new();
        rec.encode_payload(&mut payload);
        write_frame(&mut self.file, &payload)?;
        self.highest_appended = Some(rec.seq);
        Ok(())
    }

    /// Close the current batch: write the commit marker, flush, and fsync. Returns
    /// only once the batch is durable — the Bolt ack barrier. `committed_seq` is the
    /// highest sequence number the batch makes durable.
    pub fn commit(&mut self, committed_seq: Seq) -> Result<()> {
        let mut payload = Vec::with_capacity(4);
        payload.push(KIND_COMMIT);
        write_uvarint(&mut payload, committed_seq.0);
        write_frame(&mut self.file, &payload)?;
        self.file.flush().context("flush WAL buffer")?;
        self.file
            .get_ref()
            .sync_data()
            .context("fsync WAL segment")?;
        Ok(())
    }

    /// Flush + fsync and hand back the sealed (immutable) segment descriptor. The
    /// caller opens a fresh segment for subsequent writes.
    pub fn seal(mut self) -> Result<SealedSegment> {
        self.file.flush().context("flush WAL buffer on seal")?;
        self.file
            .get_ref()
            .sync_data()
            .context("fsync WAL segment on seal")?;
        Ok(SealedSegment {
            segment: self.segment,
            path: self.path,
        })
    }
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
