// SPDX-License-Identifier: Apache-2.0
//! Per-node label store (`node_labels.blk`).
//!
//! `columns` holds only a node's *properties*, not which labels it carries, so
//! the forward "node → its labels" mapping needs its own store. One record per
//! node, addressed by dense node id (the blockfile's global record index).
//!
//! Two on-disk record encodings, chosen at build time by the **label alphabet size**:
//!
//! * **bitmask** (alphabet ≤ 64): a fixed `u64` little-endian mask per node, bit `id`
//!   set iff the node carries label `id`. Stored in a **Raw** block container. `labels(n)`
//!   is one load + set-bit enumeration and `n:Label` is `mask >> id & 1` — no zstd
//!   decompress on the innermost label predicate, and 8 bytes/node flat.
//! * **varint** (alphabet > 64): the original `uvarint(count) ‖ count × uvarint(label_id)`
//!   in a zstd container, for graphs whose alphabet does not fit a `u64` mask.
//!
//! The reader auto-detects which from its block container codec (Raw ⇒ bitmask,
//! Zstd ⇒ varint), so no separate flag is stored. A node with no surviving labels
//! (e.g. one that only carried the dropped `__DumpVertex__` marker) gets a zero mask
//! / empty record so the id alignment with `node_props.blk` is preserved.
//!
// DESIGN: this is the *forward* map (node → labels), which answers `labels(n)`
// and label predicates `n:Label` during a scan with one block read. The *inverted*
// postings (label → nodes), used to seed a selective label scan, are a separate
// `labels.post` file built in a later milestone; the forward store is what the
// builder can produce directly and what every per-node read needs.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};

use crate::blockfile::{BlockCodec, BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::wire::{read_uvarint, write_uvarint};

/// Largest label alphabet that fits the `u64` bitmask encoding. At or below this the store
/// uses fixed 8-byte masks in a Raw container; above it, delta-free varint lists in zstd.
pub const BITMASK_MAX_LABELS: usize = 64;

/// Whether a build with `label_alphabet` distinct labels uses the bitmask encoding.
pub fn use_bitmask(label_alphabet: usize) -> bool {
    label_alphabet <= BITMASK_MAX_LABELS
}

/// Encode a node's labels as a `u64` bitmask (little-endian). Every id must be `< 64`
/// (guaranteed when the alphabet is ≤ 64, i.e. the caller chose bitmask mode).
pub fn encode_labels_bitmask(label_ids: &[u32]) -> [u8; 8] {
    let mut mask = 0u64;
    for &l in label_ids {
        debug_assert!(l < 64, "bitmask label id {l} >= 64");
        mask |= 1u64 << (l & 63);
    }
    mask.to_le_bytes()
}

/// Decode a bitmask record (`u64` LE) into ascending label ids.
fn decode_labels_bitmask(rec: &[u8]) -> Result<Vec<u32>> {
    if rec.len() != 8 {
        bail!("bitmask label record is {} bytes, expected 8", rec.len());
    }
    let mut mask = u64::from_le_bytes(rec.try_into().unwrap());
    let mut out = Vec::with_capacity(mask.count_ones() as usize);
    while mask != 0 {
        out.push(mask.trailing_zeros());
        mask &= mask - 1;
    }
    Ok(out)
}

/// Encode one node's label record (`uvarint(count) ‖ count × uvarint(label_id)`)
/// to bytes. The single source of the record layout — both [`NodeLabelsWriter`]
/// and the external builder (which pre-encodes labels in pass 1 and byte-copies
/// the record into the store at emit) go through this so the two can never drift.
pub fn encode_labels_record(label_ids: &[u32]) -> Vec<u8> {
    let mut rec = Vec::new();
    encode_labels_record_into(&mut rec, label_ids);
    rec
}

/// [`encode_labels_record`] appending into a caller-owned buffer — see
/// [`encode_props_record_into`](crate::columns::encode_props_record_into).
pub fn encode_labels_record_into(rec: &mut Vec<u8>, label_ids: &[u32]) {
    write_uvarint(rec, label_ids.len() as u64);
    for l in label_ids {
        write_uvarint(rec, *l as u64);
    }
}

/// Writer for `node_labels.blk`. Append nodes strictly in dense-id order.
pub struct NodeLabelsWriter {
    inner: BlockFileWriter,
    next: u64,
    /// `true` ⇒ each record is a `u64` bitmask in a Raw container; `false` ⇒ varint in zstd.
    bitmask: bool,
}

impl NodeLabelsWriter {
    /// Create the store in **varint** mode (zstd container) — the alphabet-agnostic default
    /// used by tests and fixtures. Production builds pick the mode by alphabet via
    /// [`NodeLabelsWriter::create_for_alphabet`].
    pub fn create(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
    ) -> Result<Self> {
        Self::create_with_cipher(path, target_block_bytes, zstd_level, None)
    }

    /// Create the store in varint mode, optionally AEAD-encrypted (`cipher = None` ⇒ plaintext).
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        Ok(Self {
            inner: BlockFileWriter::create_with_cipher(
                path,
                target_block_bytes,
                zstd_level,
                cipher,
            )?,
            next: 0,
            bitmask: false,
        })
    }

    /// Create the store choosing the encoding by `label_alphabet`: a `u64`-bitmask Raw container
    /// when the alphabet fits ([`use_bitmask`]), else the varint zstd container. This is the
    /// production constructor; the reader auto-detects the mode from the container codec.
    pub fn create_for_alphabet(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
        label_alphabet: usize,
    ) -> Result<Self> {
        if !use_bitmask(label_alphabet) {
            return Self::create_with_cipher(path, target_block_bytes, zstd_level, cipher);
        }
        Ok(Self {
            // Raw container: a bitmask record is already the queryable form; a zstd pass would be
            // a ~1.0× tax paid on every fault. `zstd_level` is ignored under Raw.
            inner: BlockFileWriter::create_with_codec(
                path,
                target_block_bytes,
                BlockCodec::Raw,
                zstd_level,
                cipher,
            )?,
            next: 0,
            bitmask: true,
        })
    }

    /// Append one node's label-id list; returns its dense node id. Encodes in the writer's mode.
    pub fn append(&mut self, label_ids: &[u32]) -> Result<u64> {
        if self.bitmask {
            let m = encode_labels_bitmask(label_ids);
            self.append_raw(&m)
        } else {
            self.append_raw(&encode_labels_record(label_ids))
        }
    }

    /// Append a node from a pass-1-encoded **varint** label blob, re-encoding to the writer's
    /// mode: a byte-copy in varint mode, a decode-then-mask in bitmask mode. The external
    /// builder's emit path uses this so pass 1 can stay alphabet-agnostic.
    pub fn append_blob(&mut self, varint_blob: &[u8]) -> Result<u64> {
        if self.bitmask {
            let ids = decode_labels_varint(varint_blob)?;
            let m = encode_labels_bitmask(&ids);
            self.append_raw(&m)
        } else {
            self.append_raw(varint_blob)
        }
    }

    /// Append a record already in the writer's on-disk encoding, byte-for-byte, returning its
    /// dense node id.
    pub fn append_raw(&mut self, rec: &[u8]) -> Result<u64> {
        self.inner.append_record(rec)?;
        let id = self.next;
        self.next += 1;
        Ok(id)
    }

    pub fn len(&self) -> u64 {
        self.next
    }

    pub fn is_empty(&self) -> bool {
        self.next == 0
    }

    pub fn finish(self) -> Result<u64> {
        self.inner.finish()
    }
}

/// Reader over `node_labels.blk`. The record encoding (bitmask vs varint) is auto-detected
/// from the block container codec at open; [`NodeLabelsReader::bitmask`] exposes it so a
/// caller decoding a cached block passes the right mode to [`decode_labels`].
pub struct NodeLabelsReader {
    inner: BlockFileReader,
    bitmask: bool,
}

impl NodeLabelsReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cipher(path, None)
    }

    /// Open the store, supplying the per-generation cipher for an encrypted file.
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let src = Arc::new(crate::store::fs::FileObject::open(path)?);
        Self::open_src(src, cipher)
    }

    /// Open from any positional-read source (local file or remote object).
    pub fn open_src(
        src: Arc<dyn crate::store::RandomReadAt>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let inner = BlockFileReader::open_src(src, cipher)?;
        let bitmask = inner.codec() == BlockCodec::Raw;
        Ok(Self { inner, bitmask })
    }

    pub fn len(&self) -> u64 {
        self.inner.total_records()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether records are `u64` bitmasks (`true`) or varint lists (`false`).
    pub fn bitmask(&self) -> bool {
        self.bitmask
    }

    /// The label-id list for a node.
    pub fn labels(&self, node_id: u64) -> Result<Vec<u32>> {
        let rec = self.inner.read_record_global(node_id)?;
        decode_labels(&rec, self.bitmask)
    }

    /// The underlying block file, so a caller holding a block cache can read this
    /// store's records through it and decode them with [`decode_labels`] (passing
    /// [`NodeLabelsReader::bitmask`]).
    pub fn inner(&self) -> &BlockFileReader {
        &self.inner
    }
}

/// Decode a node's varint label record (`uvarint(count) ‖ count × uvarint(label_id)`).
fn decode_labels_varint(rec: &[u8]) -> Result<Vec<u32>> {
    let mut r = rec;
    let count = read_uvarint(&mut r)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(read_uvarint(&mut r)? as u32);
    }
    Ok(out)
}

/// Decode a node's label record in the given mode (`bitmask` = the reader's
/// [`NodeLabelsReader::bitmask`]). Public so a cached-block reader can decode a record it holds.
pub fn decode_labels(rec: &[u8], bitmask: bool) -> Result<Vec<u32>> {
    if bitmask {
        decode_labels_bitmask(rec)
    } else {
        decode_labels_varint(rec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_nl_{}_{}", std::process::id(), name))
    }

    #[test]
    fn node_labels_roundtrip_varint() {
        let path = tmp("roundtrip_varint");
        // Large alphabet ⇒ varint mode (zstd container).
        let mut w = NodeLabelsWriter::create(&path, 1024, 3).unwrap();
        let nodes: Vec<Vec<u32>> = vec![vec![0, 1], vec![], vec![2], vec![0, 2, 3]];
        for (i, ls) in nodes.iter().enumerate() {
            assert_eq!(w.append(ls).unwrap(), i as u64);
        }
        w.finish().unwrap();

        let r = NodeLabelsReader::open(&path).unwrap();
        assert!(!r.bitmask(), "zstd container ⇒ varint mode");
        assert_eq!(r.len(), nodes.len() as u64);
        for (i, ls) in nodes.iter().enumerate() {
            assert_eq!(&r.labels(i as u64).unwrap(), ls);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn node_labels_roundtrip_bitmask() {
        let path = tmp("roundtrip_bitmask");
        // Small alphabet ⇒ bitmask mode (Raw container). Includes id 63, the top bit.
        let mut w = NodeLabelsWriter::create_for_alphabet(&path, 1024, 3, None, 64).unwrap();
        let nodes: Vec<Vec<u32>> = vec![vec![0, 1], vec![], vec![2], vec![0, 2, 3], vec![63]];
        for (i, ls) in nodes.iter().enumerate() {
            assert_eq!(w.append(ls).unwrap(), i as u64);
        }
        w.finish().unwrap();

        let r = NodeLabelsReader::open(&path).unwrap();
        assert!(r.bitmask(), "Raw container ⇒ bitmask mode");
        assert_eq!(r.len(), nodes.len() as u64);
        for (i, ls) in nodes.iter().enumerate() {
            assert_eq!(&r.labels(i as u64).unwrap(), ls, "node {i}");
        }
        // append_blob from a pass-1 varint blob lands the same mask.
        let path2 = tmp("roundtrip_blob");
        let mut w2 = NodeLabelsWriter::create_for_alphabet(&path2, 1024, 3, None, 10).unwrap();
        for ls in &nodes {
            if ls.iter().all(|&l| l < 10) {
                w2.append_blob(&encode_labels_record(ls)).unwrap();
            }
        }
        w2.finish().unwrap();
        let r2 = NodeLabelsReader::open(&path2).unwrap();
        assert!(r2.bitmask());
        let mut j = 0;
        for ls in &nodes {
            if ls.iter().all(|&l| l < 10) {
                assert_eq!(&r2.labels(j).unwrap(), ls);
                j += 1;
            }
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&path2);
    }
}
