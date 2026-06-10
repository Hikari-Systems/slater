//! Property store for nodes and edges.
//!
//! One record per entity, addressed by its dense id (the blockfile's global
//! record index). A record is the entity's property map:
//! `uvarint(count) ‖ count × ( uvarint(key_id) ‖ value )`, where `key_id` indexes
//! the property-key symbol table in the MANIFEST and `value` is the inline
//! [`crate::wire`] encoding.
//!
// DESIGN: stored row-per-entity rather than strictly column-per-property. The
// dominant read is "materialise a matched entity's properties for a RETURN map
// projection", which this serves with a single block read. Per-property column
// scans (rare, only for un-indexed aggregations) fall back to reading entity
// records; the indexes (`isam`) cover the selective cases.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::blockfile::{BlockFileReader, BlockFileWriter, RecordLoc};
use crate::crypto::BlockCipher;
use crate::ids::Value;
use crate::wire::{read_uvarint, read_value, write_uvarint, write_value};

/// Writer for a property `.blk` file. Append entities strictly in dense-id order
/// (0, 1, 2, …); the append position becomes the entity id.
pub struct PropsWriter {
    inner: BlockFileWriter,
    next_id: u64,
}

impl PropsWriter {
    pub fn create(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
    ) -> Result<Self> {
        Self::create_with_cipher(path, target_block_bytes, zstd_level, None)
    }

    /// Create a property store, optionally AEAD-encrypted (`cipher = None` ⇒
    /// plaintext, identical to [`PropsWriter::create`]).
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
            next_id: 0,
        })
    }

    /// Append one entity's property map. Keys are property-key symbol ids.
    /// Returns the entity's dense id.
    pub fn append(&mut self, props: &[(u32, Value)]) -> Result<u64> {
        let mut rec = Vec::new();
        write_uvarint(&mut rec, props.len() as u64);
        for (key_id, value) in props {
            write_uvarint(&mut rec, *key_id as u64);
            write_value(&mut rec, value);
        }
        self.inner.append_record(&rec)?;
        let id = self.next_id;
        self.next_id += 1;
        Ok(id)
    }

    /// Number of entities appended so far.
    pub fn len(&self) -> u64 {
        self.next_id
    }

    pub fn is_empty(&self) -> bool {
        self.next_id == 0
    }

    /// Flush, returning the block count.
    pub fn finish(self) -> Result<u64> {
        self.inner.finish()
    }
}

/// Reader for a property `.blk` file.
pub struct PropsReader {
    inner: BlockFileReader,
}

impl PropsReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cipher(path, None)
    }

    /// Open a property store, supplying the per-generation cipher for an
    /// encrypted file (`cipher = None` ⇒ plaintext).
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        Ok(Self {
            inner: BlockFileReader::open_with_cipher(path, cipher)?,
        })
    }

    /// Number of entities in the store.
    pub fn len(&self) -> u64 {
        self.inner.total_records()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Decode and return an entity's property map as `(key_id, Value)` pairs.
    pub fn props(&self, entity_id: u64) -> Result<Vec<(u32, Value)>> {
        let rec = self.inner.read_record_global(entity_id)?;
        decode_props(&rec)
    }

    /// Decode an entity's props from an already-fetched record (used when the
    /// caller holds a cached block and a [`RecordLoc`]).
    pub fn props_at(&self, loc: RecordLoc) -> Result<Vec<(u32, Value)>> {
        let rec = self.inner.read_record(loc)?;
        decode_props(&rec)
    }

    /// The underlying block file, so a caller holding a block cache can read this
    /// store's records through it (`BlockCache::record`) and decode them with
    /// [`decode_props`].
    pub fn inner(&self) -> &BlockFileReader {
        &self.inner
    }
}

/// Decode a property record (`uvarint(count) ‖ count × (uvarint(key_id) ‖ value)`)
/// into `(key_id, Value)` pairs. Public so a cached-block reader can decode a
/// record sliced out of a block it already holds decompressed.
pub fn decode_props(rec: &[u8]) -> Result<Vec<(u32, Value)>> {
    let mut r = rec;
    let count = read_uvarint(&mut r)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let key_id = read_uvarint(&mut r)? as u32;
        let value = read_value(&mut r)?;
        out.push((key_id, value));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_col_{}_{}", std::process::id(), name))
    }

    #[test]
    fn props_roundtrip_per_entity() {
        let path = tmp("props");
        let mut w = PropsWriter::create(&path, 2048, 3).unwrap();

        // Key symbol table (conceptually in the manifest): 0=name 1=confidence
        // 2=sources 3=embedding.
        let entities: [Vec<(u32, Value)>; 3] = [
            vec![
                (0u32, Value::Str("Camelid".into())),
                (1, Value::Int(1)),
                (2, Value::List(vec![Value::Str("Fowler-2010".into())])),
            ],
            vec![], // a node with no properties
            vec![
                (0, Value::Str("Alpaca".into())),
                (3, Value::Vector(vec![0.1, -0.2, 0.3])),
            ],
        ];
        for (i, e) in entities.iter().enumerate() {
            assert_eq!(w.append(e).unwrap(), i as u64);
        }
        w.finish().unwrap();

        let r = PropsReader::open(&path).unwrap();
        assert_eq!(r.len(), 3);
        for (i, e) in entities.iter().enumerate() {
            assert_eq!(&r.props(i as u64).unwrap(), e);
        }
        let _ = std::fs::remove_file(&path);
    }
}
