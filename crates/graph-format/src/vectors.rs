// SPDX-License-Identifier: Apache-2.0
//! Full-precision vector store (`vectors.f32.blk`).
//!
//! Holds the dense `f32` vectors for every vector index, **grouped by index**:
//! the builder writes one index's vectors contiguously, records the group's
//! starting record in the MANIFEST ([`crate::manifest::VectorIndexDesc::first_record`]),
//! then writes the next index's group. A brute-force cosine scan therefore reads a
//! single contiguous record range `[first_record, first_record + count)` and never
//! has to dispatch on which index a record belongs to.
//!
//! Each record is `uvarint(node_id) ‖ uvarint(dim) ‖ dim × f32(LE)`. The node id
//! is stored alongside the vector so the reader can map a KNN hit straight back to
//! a dense node id; `dim` is stored per record so the store is self-describing
//! (and a corrupt/mismatched dimension is caught at read time, not trusted from
//! the MANIFEST).
//!
// DESIGN: `vecf32` property values are routed *out* of the column store and into
// this file (D4 keeps `Value::Vector` a first-class type for exactly this). A
// vector is therefore not materialised by a `RETURN n.embedding` column read; the
// reader fetches it here when a query asks for it or the KNN path needs it.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::wire::{capacity_hint, checked_span, read_uvarint, write_uvarint};

/// One stored vector and the dense node it belongs to.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorEntry {
    pub node_id: u64,
    pub vector: Vec<f32>,
}

/// Writer for `vectors.f32.blk`. Append vectors in index-group order; the global
/// record index of the first vector in a group is the group's `first_record`.
pub struct VectorStoreWriter {
    inner: BlockFileWriter,
    next: u64,
}

impl VectorStoreWriter {
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

    /// Append one vector for `node_id`; returns its global record index.
    pub fn append(&mut self, node_id: u64, vector: &[f32]) -> Result<u64> {
        let mut rec = Vec::with_capacity(10 + vector.len() * 4);
        write_uvarint(&mut rec, node_id);
        write_uvarint(&mut rec, vector.len() as u64);
        for x in vector {
            rec.write_f32::<LittleEndian>(*x)?;
        }
        self.inner.append_record(&rec)?;
        let id = self.next;
        self.next += 1;
        Ok(id)
    }

    /// Number of vectors appended so far (= the next group's `first_record`).
    pub fn len(&self) -> u64 {
        self.next
    }

    pub fn is_empty(&self) -> bool {
        self.next == 0
    }

    /// Flush, returning the block count.
    pub fn finish(self) -> Result<u64> {
        self.inner.finish()
    }
}

/// Reader over `vectors.f32.blk`.
pub struct VectorStoreReader {
    inner: BlockFileReader,
}

impl VectorStoreReader {
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

    /// Total number of stored vectors across all index groups.
    pub fn len(&self) -> u64 {
        self.inner.total_records()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Fetch the vector at a global record index.
    pub fn get(&self, global: u64) -> Result<VectorEntry> {
        let rec = self.inner.read_record_global(global)?;
        decode(&rec)
    }

    /// Fetch a whole index group `[first_record, first_record + count)`.
    pub fn group(&self, first_record: u64, count: u64) -> Result<Vec<VectorEntry>> {
        // `count` comes from an index-group descriptor read off disk, and no buffer bounds it
        // here — reserve a bounded prefix and grow as the records are actually read. Each
        // iteration's `get` errors past the end of the file (`wire::capacity_hint`).
        let mut out = Vec::with_capacity(capacity_hint(count as usize));
        for g in first_record..first_record + count {
            out.push(self.get(g)?);
        }
        Ok(out)
    }

    /// The underlying block file, so a caller holding a block cache can read this
    /// store's records through it (`BlockCache::record`) and decode them with
    /// [`decode_vector`]. This is the path the brute-force KNN scan takes — it
    /// reads an index group through the block LRU rather than uncached `pread`s.
    pub fn inner(&self) -> &BlockFileReader {
        &self.inner
    }
}

/// Decode a vector record (`uvarint(node_id) ‖ uvarint(dim) ‖ dim × f32(LE)`) into
/// a [`VectorEntry`]. Public so a cached-block reader can decode a record sliced
/// out of a block it already holds decompressed.
pub fn decode_vector(rec: &[u8]) -> Result<VectorEntry> {
    decode(rec)
}

fn decode(rec: &[u8]) -> Result<VectorEntry> {
    let mut r = rec;
    let node_id = read_uvarint(&mut r)?;
    let dim = read_uvarint(&mut r)?;
    // `dim` is an untrusted uvarint, so `dim * 4` is a *product of on-disk data*: computed in
    // `usize` it wraps, and a forged `dim` chosen to wrap to exactly `r.len()` passes the
    // check below and reaches `with_capacity(dim)` at its full width. Multiply checked.
    let span = checked_span("vector record", dim, 4)?;
    if r.len() != span {
        bail!(
            "vector record length mismatch (dim {dim}, {} bytes left)",
            r.len()
        );
    }
    let dim = dim as usize;
    let mut vector = Vec::with_capacity(dim);
    for _ in 0..dim {
        vector.push(r.read_f32::<LittleEndian>()?);
    }
    Ok(VectorEntry { node_id, vector })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_vec_{}_{}", std::process::id(), name))
    }

    #[test]
    fn vector_store_groups_roundtrip() {
        let path = tmp("groups");
        // Two index groups in one file: 1024-dim and a small 3-dim, to prove the
        // store is self-describing about dimensionality.
        let mut w = VectorStoreWriter::create(&path, 4096, 3).unwrap();

        let group_a: Vec<(u64, Vec<f32>)> = (0..50u64)
            .map(|i| {
                (
                    i,
                    (0..1024).map(|d| (i as f32) * 0.001 + d as f32).collect(),
                )
            })
            .collect();
        let a_first = w.len();
        for (nid, v) in &group_a {
            w.append(*nid, v).unwrap();
        }

        let group_b: Vec<(u64, Vec<f32>)> =
            vec![(100, vec![0.1, -0.2, 0.3]), (101, vec![1.0, 2.0, 3.0])];
        let b_first = w.len();
        for (nid, v) in &group_b {
            w.append(*nid, v).unwrap();
        }
        w.finish().unwrap();

        let r = VectorStoreReader::open(&path).unwrap();
        assert_eq!(r.len(), (group_a.len() + group_b.len()) as u64);

        let got_a = r.group(a_first, group_a.len() as u64).unwrap();
        for (entry, (nid, v)) in got_a.iter().zip(&group_a) {
            assert_eq!(entry.node_id, *nid);
            assert_eq!(&entry.vector, v);
        }
        let got_b = r.group(b_first, group_b.len() as u64).unwrap();
        for (entry, (nid, v)) in got_b.iter().zip(&group_b) {
            assert_eq!(entry.node_id, *nid);
            assert_eq!(&entry.vector, v);
        }
        let _ = std::fs::remove_file(&path);
    }
}
