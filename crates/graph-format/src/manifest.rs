//! The generation MANIFEST — the inventory and self-description of an image.
//!
//! Written last by the builder (after every data file is fsynced and hashed) and
//! validated first by the reader. Serialised as `MANIFEST.json`.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::ids::Generation;

/// Which entity a range index or vector index attaches to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Node,
    Edge,
}

/// How a vector index is built and therefore which read path the server takes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AnnMode {
    /// Full-precision vectors only; the reader does brute-force cosine.
    BruteForce,
    /// Disk-native single-layer Vamana graph + product-quantised codes.
    Vamana {
        /// Bounded out-degree used during construction.
        r: u32,
        /// Long-edge pruning factor.
        alpha: f32,
        /// Entry medoid (dense node id within the index's vector set).
        medoid: u64,
        /// PQ subspace count.
        pq_subspaces: u32,
        /// Bits per PQ subspace code.
        pq_bits: u32,
    },
}

/// Distance metric for a vector index. Cosine is what the estate uses today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    Cosine,
    Dot,
    L2,
}

/// Descriptor for one declared range index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeIndexDesc {
    /// Index file stem under `range/` (`<name>.isam`).
    pub name: String,
    pub entity: EntityKind,
    /// Node label or relationship type the index ranges over.
    pub label_or_type: String,
    pub property: String,
}

/// Descriptor for one declared vector index over a `(label, property)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexDesc {
    pub label: String,
    pub property: String,
    pub dim: u32,
    pub metric: Metric,
    pub count: u64,
    /// Index of this index's first vector record in `vectors.f32.blk`. Its
    /// vectors occupy the contiguous global range `[firstRecord, firstRecord +
    /// count)` — the builder groups vectors by `(label, property)`, so a
    /// brute-force scan reads exactly one group with no per-record dispatch.
    #[serde(default)]
    pub first_record: u64,
    pub mode: AnnMode,
}

/// At-rest encryption header — KDF parameters and salt only. The key itself is
/// supplied at runtime and is NEVER written into the data directory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionHeader {
    /// AEAD identifier, e.g. `xchacha20poly1305`.
    pub aead: String,
    /// KDF identifier, e.g. `blake3-derive-key`.
    pub kdf: String,
    /// KDF salt (hex). Per generation.
    pub salt_hex: String,
}

/// One file in the generation inventory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub name: String,
    pub bytes: u64,
    pub blake3: String,
}

/// The full generation manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    /// `"SLATER01"` — quick sniff before trusting the JSON.
    pub magic: String,
    pub format_version: u32,
    pub build_uuid: Generation,
    pub graph: String,
    /// Unix seconds at build time (supplied by the builder).
    pub created_unix: i64,
    /// BLAKE3 content hash over the data-file inventory (excludes MANIFEST itself).
    pub content_hash: String,
    /// Per-file target block size in bytes (file name → bytes).
    pub block_sizes: BTreeMap<String, u32>,
    pub codec: String,
    pub zstd_level: i32,
    #[serde(default)]
    pub encryption: Option<EncryptionHeader>,
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub reltypes: Vec<String>,
    /// Property-key symbol table. A `(key_id, value)` pair in `node_props.blk` /
    /// `edge_props.blk` carries `key_id = index into this vector`. Bounded and
    /// small, so it lives resident in the MANIFEST rather than a dictionary file.
    #[serde(default)]
    pub property_keys: Vec<String>,
    #[serde(default)]
    pub range_indexes: Vec<RangeIndexDesc>,
    #[serde(default)]
    pub vector_indexes: Vec<VectorIndexDesc>,
    /// Inventory of data files (everything except `MANIFEST.json`).
    pub files: Vec<FileEntry>,
}

impl Manifest {
    /// Recompute the content hash over `files` and compare to `content_hash`.
    /// Returns `Ok(())` only if they match.
    pub fn verify_content_hash(&self) -> Result<()> {
        let inv: Vec<(String, String)> = self
            .files
            .iter()
            .map(|f| (f.name.clone(), f.blake3.clone()))
            .collect();
        let computed = crate::integrity::content_hash(&inv);
        if computed != self.content_hash {
            anyhow::bail!(
                "manifest content hash mismatch (declared {}, recomputed {})",
                self.content_hash,
                computed
            );
        }
        Ok(())
    }

    /// Serialise to pretty JSON.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).context("serialise manifest")
    }

    /// Write `MANIFEST.json` into `dir`.
    pub fn write_to_dir(&self, dir: impl AsRef<Path>) -> Result<()> {
        let path = dir.as_ref().join("MANIFEST.json");
        std::fs::write(&path, self.to_json()?)
            .with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }

    /// Read and parse `MANIFEST.json` from `dir`.
    pub fn read_from_dir(dir: impl AsRef<Path>) -> Result<Self> {
        let path = dir.as_ref().join("MANIFEST.json");
        let text =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let m: Manifest =
            serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        Ok(m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        let files = vec![FileEntry {
            name: "node_props.blk".into(),
            bytes: 123,
            blake3: "deadbeef".into(),
        }];
        let content_hash = crate::integrity::content_hash(
            &files
                .iter()
                .map(|f| (f.name.clone(), f.blake3.clone()))
                .collect::<Vec<_>>(),
        );
        Manifest {
            magic: "SLATER01".into(),
            format_version: crate::FORMAT_VERSION,
            build_uuid: Generation(uuid::Uuid::nil()),
            graph: "eu_ai_act".into(),
            created_unix: 1_700_000_000,
            content_hash,
            block_sizes: BTreeMap::from([("node_props.blk".to_string(), 262_144)]),
            codec: "zstd".into(),
            zstd_level: 3,
            encryption: None,
            node_count: 1,
            edge_count: 0,
            labels: vec!["Concept".into()],
            reltypes: vec![],
            property_keys: vec!["name".into(), "embedding".into()],
            range_indexes: vec![],
            vector_indexes: vec![VectorIndexDesc {
                label: "Chunk".into(),
                property: "embedding".into(),
                dim: 1024,
                metric: Metric::Cosine,
                count: 1,
                first_record: 0,
                mode: AnnMode::BruteForce,
            }],
            files,
        }
    }

    #[test]
    fn manifest_json_roundtrips() {
        let m = sample();
        let json = m.to_json().unwrap();
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn content_hash_verifies_and_detects_tamper() {
        let mut m = sample();
        m.verify_content_hash().unwrap();
        // Tamper with a file hash without updating content_hash.
        m.files[0].blake3 = "cafebabe".into();
        assert!(m.verify_content_hash().is_err());
    }

    #[test]
    fn write_then_read_dir() {
        let dir = std::env::temp_dir().join(format!("slater_man_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let m = sample();
        m.write_to_dir(&dir).unwrap();
        let back = Manifest::read_from_dir(&dir).unwrap();
        assert_eq!(m, back);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
