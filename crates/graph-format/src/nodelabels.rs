// SPDX-License-Identifier: Apache-2.0
//! Per-node label store (`node_labels.blk`).
//!
//! `columns` holds only a node's *properties*, not which labels it carries, so
//! the forward "node → its labels" mapping needs its own store. One record per
//! node, addressed by dense node id (the blockfile's global record index):
//! `uvarint(count) ‖ count × uvarint(label_id)`, where each `label_id` indexes the
//! MANIFEST `labels` symbol table. A node with no surviving labels (e.g. one that
//! only ever carried the dropped `__DumpVertex__` marker) gets an empty record so
//! the id alignment with `node_props.blk` is preserved.
//!
// DESIGN: this is the *forward* map (node → labels), which answers `labels(n)`
// and label predicates `n:Label` during a scan with one block read. The *inverted*
// postings (label → nodes), used to seed a selective label scan, are a separate
// `labels.post` file built in a later milestone; the forward store is what the
// builder can produce directly and what every per-node read needs.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::wire::{read_uvarint, write_uvarint};

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
}

impl NodeLabelsWriter {
    pub fn create(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
    ) -> Result<Self> {
        Self::create_with_cipher(path, target_block_bytes, zstd_level, None)
    }

    /// Create the store, optionally AEAD-encrypted (`cipher = None` ⇒ plaintext).
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
        })
    }

    /// Append one node's label-id list; returns its dense node id.
    pub fn append(&mut self, label_ids: &[u32]) -> Result<u64> {
        self.append_raw(&encode_labels_record(label_ids))
    }

    /// Append a record already encoded by [`encode_labels_record`], byte-for-byte,
    /// returning its dense node id. The external builder's emit path uses this to
    /// copy a pass-1-encoded label record straight into the store with no re-encode.
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

/// Reader over `node_labels.blk`.
pub struct NodeLabelsReader {
    inner: BlockFileReader,
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
        Ok(Self {
            inner: BlockFileReader::open_src(src, cipher)?,
        })
    }

    pub fn len(&self) -> u64 {
        self.inner.total_records()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The label-id list for a node.
    pub fn labels(&self, node_id: u64) -> Result<Vec<u32>> {
        let rec = self.inner.read_record_global(node_id)?;
        decode_labels(&rec)
    }

    /// The underlying block file, so a caller holding a block cache can read this
    /// store's records through it and decode them with [`decode_labels`].
    pub fn inner(&self) -> &BlockFileReader {
        &self.inner
    }
}

/// Decode a node's label record (`uvarint(count) ‖ count × uvarint(label_id)`).
/// Public so a cached-block reader can decode a record it already holds.
pub fn decode_labels(rec: &[u8]) -> Result<Vec<u32>> {
    let mut r = rec;
    let count = read_uvarint(&mut r)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(read_uvarint(&mut r)? as u32);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_nl_{}_{}", std::process::id(), name))
    }

    #[test]
    fn node_labels_roundtrip() {
        let path = tmp("roundtrip");
        let mut w = NodeLabelsWriter::create(&path, 1024, 3).unwrap();
        let nodes: Vec<Vec<u32>> = vec![
            vec![0, 1], // multi-label
            vec![],     // a node whose only label was the dropped marker
            vec![2],    // single label
            vec![0, 2, 3],
        ];
        for (i, ls) in nodes.iter().enumerate() {
            assert_eq!(w.append(ls).unwrap(), i as u64);
        }
        w.finish().unwrap();

        let r = NodeLabelsReader::open(&path).unwrap();
        assert_eq!(r.len(), nodes.len() as u64);
        for (i, ls) in nodes.iter().enumerate() {
            assert_eq!(&r.labels(i as u64).unwrap(), ls);
        }
        let _ = std::fs::remove_file(&path);
    }
}
