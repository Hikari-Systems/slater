// SPDX-License-Identifier: Apache-2.0
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

/// Descriptor for one stored per-(label, property) value→count histogram. Aligned
/// by position with the records in `prop_hist.blk`. Present ⇒ the generation can
/// answer a whole-label group-by / `count(DISTINCT)` on this `(label, property)`
/// from O(distinct) instead of an O(index) `distinct_key_counts` walk. Absent (the
/// index is over an edge, or its distinct count exceeded `--histogram-max-distinct`)
/// ⇒ the query path falls back to the walk: slower, never incorrect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PropertyHistogramDesc {
    /// Range-index file stem this histogram derives from (= [`RangeIndexDesc::name`]).
    pub index_name: String,
    pub label: String,
    pub property: String,
    /// Number of distinct non-null values (= record's pair count = ISAM key count).
    pub distinct_count: u64,
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
    /// Per-reltype distinct **source** node counts (index = reltype id, aligned
    /// with `reltypes`). Non-empty ⇒ the generation carries `reltype_src.post`;
    /// the planner uses these to cost a relationship-type scan against a label
    /// scan, and the posting ids drive an outgoing typed first hop. Empty ⇒ no
    /// posting (the rel-type scan is simply unavailable, never incorrect).
    #[serde(default)]
    pub reltype_source_counts: Vec<u64>,
    /// Per-reltype distinct **target** node counts (index = reltype id). The
    /// `reltype_tgt.post` analog of [`Self::reltype_source_counts`], for incoming
    /// (and, unioned with sources, undirected) typed first hops.
    #[serde(default)]
    pub reltype_target_counts: Vec<u64>,
    /// Per-(label, property) value→count histograms carried in `prop_hist.blk`,
    /// one descriptor per stored histogram, aligned by position with the file's
    /// records. Non-empty ⇒ the grouped-index fast path reads these instead of
    /// walking the ISAM. Empty ⇒ no histograms (every group-by/count(DISTINCT)
    /// falls back to `distinct_key_counts`). See [`PropertyHistogramDesc`].
    #[serde(default)]
    pub property_histograms: Vec<PropertyHistogramDesc>,
    /// BLAKE3 digest (hex) of the live `acl.json` this generation was built
    /// against (`slater-build --acl`). `None` ⇒ not stamped (older images, or the
    /// flag was not given). When present, the server re-hashes the configured live
    /// `acl.json` at open time and refuses to serve this graph if it differs.
    #[serde(default)]
    pub acl_blake3: Option<String>,
    /// Keyed-BLAKE3 MAC (hex) over the canonicalised manifest, under a subkey
    /// derived from the at-rest master key. `None` ⇒ plaintext image (no master
    /// key, no MAC). Authenticates every other field — including `content_hash`,
    /// the file inventory, the encryption header, and `acl_blake3`.
    #[serde(default)]
    pub mac: Option<String>,
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

    /// The canonical byte string the MAC is computed over: this manifest with
    /// `mac` cleared, serialised compactly. Deterministic — serde fixes struct
    /// field order, `block_sizes` is a `BTreeMap`, and every other collection is
    /// an order-stable `Vec` written in a fixed order by the builder. Clearing
    /// `mac` is what lets the same bytes be reproduced at verify time.
    fn mac_message(&self) -> Result<Vec<u8>> {
        let mut canon = self.clone();
        canon.mac = None;
        serde_json::to_vec(&canon).context("serialise manifest for MAC")
    }

    /// Compute the keyed-BLAKE3 MAC under the master-key-derived subkey and store
    /// it in `mac`. Call this **last** at build time — after every other field
    /// (including `acl_blake3`) is final and immediately before `write_to_dir`.
    pub fn seal_mac(&mut self, master_key: &[u8]) -> Result<()> {
        let key = crate::crypto::derive_manifest_mac_key(master_key);
        let mac = crate::crypto::manifest_mac(&key, &self.mac_message()?);
        self.mac = Some(mac);
        Ok(())
    }

    /// Recompute the MAC and compare it to the stored `mac`. `Ok(())` only on a
    /// match. Errors if `mac` is absent — callers gate on presence first and only
    /// call this when a MAC is expected.
    pub fn verify_mac(&self, master_key: &[u8]) -> Result<()> {
        let stored = self
            .mac
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("manifest carries no MAC but one was required"))?;
        let key = crate::crypto::derive_manifest_mac_key(master_key);
        let computed = crate::crypto::manifest_mac(&key, &self.mac_message()?);
        if computed != stored {
            anyhow::bail!("manifest MAC mismatch — refusing to serve a tampered manifest");
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
            reltype_source_counts: vec![],
            reltype_target_counts: vec![],
            property_histograms: vec![],
            acl_blake3: None,
            mac: None,
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
    fn mac_seal_and_verify_roundtrips() {
        let key = b"operator master key";
        let mut m = sample();
        assert!(m.mac.is_none());
        m.seal_mac(key).unwrap();
        assert!(m.mac.is_some());
        m.verify_mac(key).unwrap();
    }

    #[test]
    fn mac_detects_tamper_in_every_authenticated_field() {
        let key = b"operator master key";
        let base = {
            let mut m = sample();
            m.seal_mac(key).unwrap();
            m
        };

        // Each closure mutates one authenticated field; the stored MAC must then
        // fail to verify. This proves the MAC blanket-covers the manifest.
        let check = |what: &str, tamper: &dyn Fn(&mut Manifest)| {
            let mut m = base.clone();
            tamper(&mut m);
            assert!(
                m.verify_mac(key).is_err(),
                "tampering with {what} must break the MAC"
            );
        };
        check("content_hash", &|m| m.content_hash = "00".into());
        check("file hash", &|m| m.files[0].blake3 = "cafebabe".into());
        check("graph name", &|m| m.graph = "other".into());
        check("acl_blake3", &|m| m.acl_blake3 = Some("deadbeef".into()));
        check("encryption header", &|m| {
            m.encryption = Some(EncryptionHeader {
                aead: "x".into(),
                kdf: "y".into(),
                salt_hex: "00".into(),
            })
        });
    }

    #[test]
    fn mac_rejects_wrong_key() {
        let mut m = sample();
        m.seal_mac(b"key A").unwrap();
        assert!(m.verify_mac(b"key B").is_err());
    }

    #[test]
    fn mac_message_excludes_the_mac_field() {
        // The canonical message must be identical whether or not `mac` is set,
        // otherwise the MAC could never verify against itself.
        let mut a = sample();
        let with_none = a.mac_message().unwrap();
        a.mac = Some("whatever".into());
        let with_some = a.mac_message().unwrap();
        assert_eq!(with_none, with_some);
    }

    #[test]
    fn verify_mac_errors_when_absent() {
        let m = sample(); // mac: None
        assert!(m.verify_mac(b"key").is_err());
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
