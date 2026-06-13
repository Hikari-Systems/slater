// SPDX-License-Identifier: Apache-2.0
//! Build scaffolding shared by the in-memory [`crate::build`] and the external
//! [`crate::build_external`] paths: cipher derivation, the file inventory +
//! MANIFEST, and the atomic publish. Both paths produce the identical generation
//! format — only how they get the records into the stores differs — so this is the
//! single owner of "seal it and swap `current` into place".

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use graph_format::crypto::{self, BlockCipher};
use graph_format::ids::Generation;
use graph_format::integrity::{content_hash, hash_file};
use graph_format::manifest::{EncryptionHeader, Manifest, RangeIndexDesc, VectorIndexDesc};

/// Derive the per-generation block cipher and the MANIFEST encryption header (which
/// records the KDF salt, never the key) when encryption is requested.
pub fn derive_cipher(
    encryption_key: &Option<Vec<u8>>,
) -> (Option<Arc<BlockCipher>>, Option<EncryptionHeader>) {
    match encryption_key {
        Some(key) => {
            let salt = crypto::random_salt();
            let header = EncryptionHeader {
                aead: crypto::AEAD_NAME.to_string(),
                kdf: crypto::KDF_NAME.to_string(),
                salt_hex: crypto::hex_encode(&salt),
            };
            (
                Some(Arc::new(BlockCipher::from_master(key, &salt))),
                Some(header),
            )
        }
        None => (None, None),
    }
}

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// fsync a directory so a rename/creation within it is durable (matters on
/// remote/network filesystems such as NFS).
pub fn fsync_dir(dir: &Path) -> Result<()> {
    let f = fs::File::open(dir).with_context(|| format!("open dir {}", dir.display()))?;
    f.sync_all()
        .with_context(|| format!("fsync dir {}", dir.display()))?;
    Ok(())
}

/// What a successful build produced.
pub struct BuildOutcome {
    pub generation: Generation,
    pub content_hash: String,
    pub dir: PathBuf,
    pub node_count: u64,
    pub edge_count: u64,
}

/// Everything needed to inventory, seal and publish a staged generation.
pub struct PublishInputs<'a> {
    pub tmp_dir: &'a Path,
    pub graph_dir: &'a Path,
    pub final_dir: &'a Path,
    pub generation: Generation,
    pub graph: &'a str,
    pub zstd_level: i32,
    pub block_sizes: BTreeMap<String, u32>,
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub reltypes: Vec<String>,
    pub property_keys: Vec<String>,
    pub range_indexes: Vec<RangeIndexDesc>,
    pub vector_indexes: Vec<VectorIndexDesc>,
    pub encryption_header: Option<EncryptionHeader>,
    pub encryption_key: &'a Option<Vec<u8>>,
    pub acl_blake3: Option<String>,
    /// Extra inventory files beyond the fixed stores (the Vamana/PQ vector files).
    pub extra_files: Vec<String>,
}

/// Inventory every file (size + BLAKE3), assemble + (optionally) MAC-seal the
/// MANIFEST, then atomically publish: fsync the temp dir, rename it into place, and
/// swap the `current` pointer. Returns the build outcome.
pub fn write_manifest_and_publish(inp: PublishInputs) -> Result<BuildOutcome> {
    // ---- inventory + content hash ----
    let mut file_names: Vec<String> = vec![
        "node_props.blk".into(),
        "node_labels.blk".into(),
        "edge_props.blk".into(),
        "topology.csr.blk".into(),
        "vectors.f32.blk".into(),
    ];
    for ri in &inp.range_indexes {
        file_names.push(format!("range/{}.isam", ri.name));
    }
    file_names.extend(inp.extra_files.iter().cloned());
    file_names.sort();

    let mut files = Vec::new();
    for name in &file_names {
        let path = inp.tmp_dir.join(name);
        let bytes = fs::metadata(&path)
            .with_context(|| format!("stat {}", path.display()))?
            .len();
        let blake3 = hash_file(&path)?;
        files.push(graph_format::manifest::FileEntry {
            name: name.clone(),
            bytes,
            blake3,
        });
    }
    let inventory: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = content_hash(&inventory);

    let mut manifest = Manifest {
        magic: String::from_utf8_lossy(graph_format::MAGIC).to_string(),
        format_version: graph_format::FORMAT_VERSION,
        build_uuid: inp.generation,
        graph: inp.graph.to_string(),
        created_unix: now_unix(),
        content_hash: content_hash.clone(),
        block_sizes: inp.block_sizes,
        codec: "zstd".into(),
        zstd_level: inp.zstd_level,
        encryption: inp.encryption_header,
        node_count: inp.node_count,
        edge_count: inp.edge_count,
        labels: inp.labels,
        reltypes: inp.reltypes,
        property_keys: inp.property_keys,
        range_indexes: inp.range_indexes,
        vector_indexes: inp.vector_indexes,
        acl_blake3: inp.acl_blake3,
        mac: None,
        files,
    };
    if let Some(key) = inp.encryption_key {
        manifest.seal_mac(key)?;
    }
    manifest.write_to_dir(inp.tmp_dir)?;

    // ---- atomic publish ----
    fsync_dir(inp.tmp_dir)?;
    if inp.final_dir.exists() {
        bail!(
            "generation directory already exists: {}",
            inp.final_dir.display()
        );
    }
    fs::rename(inp.tmp_dir, inp.final_dir)
        .with_context(|| format!("publish {}", inp.final_dir.display()))?;
    fsync_dir(inp.graph_dir)?;

    let current = inp.graph_dir.join("current");
    let current_tmp = inp.graph_dir.join(".current.tmp");
    fs::write(&current_tmp, format!("{}\n", inp.generation.0))
        .with_context(|| format!("write {}", current_tmp.display()))?;
    fs::rename(&current_tmp, &current).with_context(|| format!("swap {}", current.display()))?;
    fsync_dir(inp.graph_dir)?;

    Ok(BuildOutcome {
        generation: inp.generation,
        content_hash,
        dir: inp.final_dir.to_path_buf(),
        node_count: inp.node_count,
        edge_count: inp.edge_count,
    })
}
