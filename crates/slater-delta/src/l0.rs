// SPDX-License-Identifier: Apache-2.0
//! Immutable **L0 delta segment** — a frozen memtable spilled to disk (Phase 4b).
//!
//! When the active memtable reaches its byte budget it is *flushed* to an L0
//! segment: an immutable, content-checked file holding the whole folded delta
//! ([`Memtable::serialise`]). The active memtable then resets empty (rebased so its
//! synthetic ids start past the flushed level), and the L0 segment joins the read
//! stack as a level between the memtable and the core:
//!
//! ```text
//! active memtable  ->  L0(newest..oldest)  ->  core
//! ```
//!
//! A read overlays the levels newest-first (Phase 4c wires this into
//! [`DeltaSnapshot`](crate::memtable::DeltaSnapshot)); consolidation folds them all
//! into a fresh core and the segments are then retired. Because an L0 segment lives
//! only between a flush and the next consolidation there is **no** back-compatibility
//! obligation — the body format ([`Memtable::serialise`]) may change freely, and a
//! version or checksum mismatch is a hard error on open.
//!
//! # On-disk layout
//! ```text
//! MAGIC(8 "SLL0SEG1") ‖ crc32c:u32(LE) ‖ stored      (crc over `stored`)
//! stored = body                                       (plaintext)
//!        | nonce(24) ‖ ciphertext(body + 16)          (sealed, MAGIC "SLL0SEGE")
//! body   = Memtable::serialise()
//! ```
//! A segment is one immutable blob written once, so on an encrypted deployment (HIK-146)
//! it seals whole, under a subkey bound to its own file name — a segment renamed into
//! another slot fails to open. Sealed and plaintext segments carry different magics and
//! the key policy is symmetric: either mismatch is refused (see [`crate::seal`]).
//! The file is written temp-then-`rename` so a reader never observes a torn image;
//! the crc catches media corruption. Reads load the whole body resident (the delta
//! is byte-budgeted, so this never grows with core size — an off-heap `pread`
//! variant is a later RSS refinement, not a correctness concern).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::memtable::Memtable;
use crate::seal::{bind, frame_blob, l0_name, unframe_blob, DeltaCipher};

/// Magic prefix identifying an L0 segment file.
const L0_MAGIC: &[u8; 8] = b"SLL0SEG1";

/// Magic prefix identifying a **sealed** L0 segment file (HIK-146).
const L0_MAGIC_SEALED: &[u8; 8] = b"SLL0SEGE";

/// How a refusal names an L0 segment to an operator.
const L0_SUBJECT: &str = "L0 delta segment";

/// An opened, immutable L0 delta segment: the reloaded [`Memtable`] it holds answers
/// the full [`DeltaSnapshot`](crate::memtable::DeltaSnapshot) read surface, so a read
/// stack folds one uniformly over each level (Phase 4c).
#[derive(Debug, Clone)]
pub struct L0Segment {
    path: PathBuf,
    mem: Arc<Memtable>,
}

impl L0Segment {
    /// Write `mem` to `path` as an immutable, content-checked L0 segment. The image is
    /// staged in a sibling `.tmp` file, fsynced, then atomically `rename`d into place,
    /// so a concurrent or later [`Self::open`] never sees a partial file.
    pub fn write(
        mem: &Memtable,
        path: impl AsRef<Path>,
        cipher: Option<&DeltaCipher>,
    ) -> Result<()> {
        let path = path.as_ref();
        let body = mem.serialise();
        // Sealed under the segment's **final** name, not the staging `.tmp` one: the tmp
        // file is renamed into place, and the reader derives its subkey from what it opens.
        let cipher = bind(cipher, &l0_name(path, None)?);
        let framed = frame_blob(L0_MAGIC, L0_MAGIC_SEALED, cipher.as_deref(), &body)?;

        let tmp = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)
                .with_context(|| format!("create L0 temp file {tmp:?}"))?;
            f.write_all(&framed)
                .with_context(|| format!("write L0 segment {tmp:?}"))?;
            f.sync_all()
                .with_context(|| format!("fsync L0 segment {tmp:?}"))?;
        }
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename L0 segment into place {path:?}"))?;
        // Fsync the directory so the rename is durable.
        if let Some(dir) = path.parent() {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }

    /// Open an L0 segment, verifying its magic against the configured key, then its
    /// checksum, then (sealed) its AEAD tag, and reloading the folded [`Memtable`]. A
    /// truncated, mis-magicked, corrupted, unsealed-under-a-key or wrong-key file is a
    /// hard error.
    pub fn open(path: impl AsRef<Path>, cipher: Option<&DeltaCipher>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let bytes = std::fs::read(&path).with_context(|| format!("read L0 segment {path:?}"))?;
        let cipher = bind(cipher, &l0_name(&path, None)?);
        let body = unframe_blob(
            L0_MAGIC,
            L0_MAGIC_SEALED,
            cipher.as_deref(),
            &bytes,
            L0_SUBJECT,
        )
        .with_context(|| format!("open L0 segment {path:?}"))?;
        let mem =
            Memtable::deserialise(&body).with_context(|| format!("decode L0 segment {path:?}"))?;
        Ok(Self {
            path,
            mem: Arc::new(mem),
        })
    }

    /// The reloaded, immutable memtable this segment overlays.
    pub fn memtable(&self) -> &Arc<Memtable> {
        &self.mem
    }

    /// The file this segment was loaded from (retired at consolidation).
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalOp;
    use crate::OpResolution;
    use graph_format::ids::Value;

    fn tmp(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("slater_l0_{tag}_{}", std::process::id()))
    }

    /// Build a memtable exercising every stored shape: a core-node patch, a born node,
    /// a tombstoned core node, a born edge (core→born endpoint), and a core-edge
    /// tombstone.
    fn populate() -> Memtable {
        let mut m = Memtable::with_bases(100, 10);
        // Core-node property patch (dense 5).
        m.apply(
            &WalOp::UpsertNode {
                label: "Person".into(),
                key: "name".into(),
                value: Value::Str("Alice".into()),
                patches: [("age".to_string(), Value::Int(30))].into_iter().collect(),
            },
            OpResolution::Node(Some(5)),
        );
        // Born node (absent from core → synthetic 100).
        m.apply(
            &WalOp::UpsertNode {
                label: "Person".into(),
                key: "name".into(),
                value: Value::Str("Zoe".into()),
                patches: [("age".to_string(), Value::Int(9))].into_iter().collect(),
            },
            OpResolution::Node(None),
        );
        // Tombstone a core node (dense 7).
        m.apply(
            &WalOp::DeleteNode {
                label: "Person".into(),
                key: "name".into(),
                value: Value::Str("Bob".into()),
            },
            OpResolution::Node(Some(7)),
        );
        // Born edge: core node 5 → born node (synthetic).
        m.apply(
            &WalOp::UpsertEdge {
                src_label: "Person".into(),
                src_key: "name".into(),
                src_value: Value::Str("Alice".into()),
                reltype: "KNOWS".into(),
                dst_label: "Person".into(),
                dst_key: "name".into(),
                dst_value: Value::Str("Zoe".into()),
                patches: Default::default(),
            },
            OpResolution::Edge {
                src: Some(5),
                dst: None,
                edge_id: None,
            },
        );
        // Tombstone a core edge (both endpoints core).
        m.apply(
            &WalOp::DeleteEdge {
                src_label: "Person".into(),
                src_key: "name".into(),
                src_value: Value::Str("Alice".into()),
                reltype: "KNOWS".into(),
                dst_label: "Person".into(),
                dst_key: "name".into(),
                dst_value: Value::Str("Carol".into()),
            },
            OpResolution::Edge {
                src: Some(5),
                dst: Some(8),
                edge_id: None,
            },
        );
        m
    }

    /// Every observable read matches between the original and a serialise→deserialise
    /// round-trip.
    fn assert_reads_match(a: &Memtable, b: &Memtable) {
        assert_eq!(a.node_delta_count(), b.node_delta_count());
        assert_eq!(a.synthetic_base(), b.synthetic_base());
        assert_eq!(a.edge_synthetic_base(), b.edge_synthetic_base());
        assert_eq!(a.born_count(), b.born_count());
        assert_eq!(a.born_edge_count(), b.born_edge_count());
        assert_eq!(a.bytes(), b.bytes());
        // Node patches over the full dense range touched.
        for id in 0..a.synthetic_base() + a.born_count() {
            assert_eq!(a.node_patch(id), b.node_patch(id), "node_patch({id})");
        }
        // Born label scan + edges from the born endpoints and core node 5.
        assert_eq!(
            a.born_ids_with_label("Person"),
            b.born_ids_with_label("Person")
        );
        for id in [5u64, 7, 8, 100] {
            assert_eq!(a.out_edges(id), b.out_edges(id), "out_edges({id})");
            assert_eq!(a.in_edges(id), b.in_edges(id), "in_edges({id})");
        }
        // Identity recovery for a born node.
        assert_eq!(a.node_identity_by_dense(100), b.node_identity_by_dense(100));
    }

    #[test]
    fn serialise_deserialise_round_trips_every_read() {
        let m = populate();
        let bytes = m.serialise();
        let back = Memtable::deserialise(&bytes).unwrap();
        assert_reads_match(&m, &back);
        // Deterministic: equal memtables serialise to identical bytes.
        assert_eq!(bytes, populate().serialise());
    }

    #[test]
    fn empty_memtable_round_trips() {
        let m = Memtable::with_bases(42, 3);
        let back = Memtable::deserialise(&m.serialise()).unwrap();
        assert!(back.is_empty());
        assert_eq!(back.synthetic_base(), 42);
        assert_eq!(back.edge_synthetic_base(), 3);
    }

    #[test]
    fn segment_write_open_round_trips() {
        let dir = tmp("seg");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("000001.l0");
        let m = populate();
        L0Segment::write(&m, &path, None).unwrap();
        let seg = L0Segment::open(&path, None).unwrap();
        assert_reads_match(&m, seg.memtable());
        assert_eq!(seg.path(), path);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupted_segment_is_rejected() {
        let dir = tmp("corrupt");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("000001.l0");
        L0Segment::write(&populate(), &path, None).unwrap();
        // Flip a byte in the body (past magic + crc).
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        assert!(
            L0Segment::open(&path, None).is_err(),
            "checksum catches corruption"
        );

        // Bad magic is rejected too.
        std::fs::write(&path, b"XXXXXXXX____body").unwrap();
        assert!(
            L0Segment::open(&path, None).is_err(),
            "magic mismatch rejected"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
