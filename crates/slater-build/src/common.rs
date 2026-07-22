// SPDX-License-Identifier: Apache-2.0
//! Build scaffolding for the [`crate::build_external`] path: cipher derivation,
//! the file inventory + MANIFEST, and the atomic publish. This is the single owner
//! of "seal it and swap `current` into place".

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use rayon::prelude::*;

use graph_format::crypto::{self, file_cipher, BlockCipher};
use graph_format::histogram::{
    derive_histogram_from_isam, encode_histogram, write_property_histograms,
};
use graph_format::ids::Generation;
use graph_format::integrity::content_hash;
use graph_format::manifest::{
    EncryptionHeader, EntityKind, HubDegreeDesc, Manifest, PropertyHistogramDesc, RangeIndexDesc,
    VectorIndexDesc,
};
use graph_format::store::{join_key, ObjectStore};

/// Derive the per-generation block cipher and the MANIFEST encryption header (which
/// records the KDF salt, never the key) when encryption is requested.
pub fn derive_cipher(
    encryption_key: &Option<zeroize::Zeroizing<Vec<u8>>>,
) -> (Option<Arc<BlockCipher>>, Option<EncryptionHeader>) {
    match encryption_key {
        Some(key) => {
            let salt = crypto::random_salt();
            let header = EncryptionHeader {
                aead: crypto::AEAD_NAME.to_string(),
                kdf: crypto::KDF_NAME.to_string(),
                salt_hex: crypto::hex_encode(&salt),
                aad_scheme: crypto::AAD_SCHEME.to_string(),
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
    /// Name of the backend-aware profile that produced `zstd_level` (`"local"` /
    /// `"remote"` / `"max"` / `"manual"`); recorded in the manifest for inspection.
    pub compression_profile: String,
    pub block_sizes: BTreeMap<String, u32>,
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub reltypes: Vec<String>,
    pub property_keys: Vec<String>,
    pub range_indexes: Vec<RangeIndexDesc>,
    pub vector_indexes: Vec<VectorIndexDesc>,
    /// Per-reltype distinct source/target node counts for the endpoint postings
    /// (`reltype_src.post` / `reltype_tgt.post`), index = reltype id.
    pub reltype_source_counts: Vec<u64>,
    pub reltype_target_counts: Vec<u64>,
    /// Whole-graph metadata summaries tallied during emit (see the matching
    /// [`Manifest`] fields). All index-aligned with `reltypes` / `labels`; the cube
    /// marginals are sparse `(key…, count)` tuples sorted by key.
    pub reltype_edge_counts: Vec<u64>,
    pub reltype_self_loop_counts: Vec<u64>,
    pub label_node_counts: Vec<u64>,
    pub first_label_counts: Vec<u64>,
    pub src_label_reltype_counts: Vec<(u32, u32, u64)>,
    pub reltype_tgt_label_counts: Vec<(u32, u32, u64)>,
    pub schema_triple_counts: Vec<(u32, u32, u32, u64)>,
    /// Per-(label, property) value→count histogram descriptors (`prop_hist.blk`),
    /// aligned by position with that file's records. Produced by
    /// [`build_property_histograms`].
    pub property_histograms: Vec<PropertyHistogramDesc>,
    /// Hub-degree sidecar descriptor (`hub_degrees.blk`). `Some` for every build that
    /// wrote the sidecar (always, for the external builder); the file is in the fixed
    /// inventory regardless.
    pub hub_degrees: Option<HubDegreeDesc>,
    pub encryption_header: Option<EncryptionHeader>,
    pub encryption_key: &'a Option<zeroize::Zeroizing<Vec<u8>>>,
    pub acl_blake3: Option<String>,
    /// Extra inventory files beyond the fixed stores (the Vamana/PQ vector files).
    pub extra_files: Vec<String>,
    /// When set, also publish the finished generation to this object store after
    /// the local atomic publish (upload every file, then write the remote
    /// `current` pointer last). `None` ⇒ filesystem-only publish.
    pub store: Option<Arc<dyn ObjectStore>>,
    /// Compute the per-file SHA-256 and CRC32C object checksums even when this build
    /// does not itself publish to an object store — for a generation that will be
    /// copied to S3/GCS by some other means and must keep its content-grade
    /// integrity check there. See [`write_manifest_and_publish`].
    pub force_object_checksums: bool,
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
        "reltype_src.post".into(),
        "reltype_tgt.post".into(),
        "prop_hist.blk".into(),
        "hub_degrees.blk".into(),
        "node_degrees.blk".into(),
    ];
    for ri in &inp.range_indexes {
        file_names.push(format!("range/{}.isam", ri.name));
    }
    file_names.extend(inp.extra_files.iter().cloned());
    file_names.sort();

    // SHA-256 (S3) and CRC32C (GCS) exist so a generation *served from an object
    // store* can be integrity-checked against the store's server-computed object
    // checksum from a metadata request, with no body read. A filesystem-only
    // generation is verified by re-hashing its bytes with BLAKE3, so it needs
    // neither — and SHA-256 is the slowest of the three by a wide margin (no tree
    // structure, so it cannot be parallelised within a file). Compute them only
    // when this build publishes to a store, or when the caller says the generation
    // is bound for one (D56). The content hash is unaffected: it folds only the
    // inventory's `(name, blake3)` pairs, and MANIFEST.json is not in the inventory.
    let object_checksums = inp.store.is_some() || inp.force_object_checksums;

    // Each file is an independent read+hash. BLAKE3 already fans out *within* a file
    // via `update_rayon` — which matters because `topology.csr.blk` alone is ~71% of
    // the bytes — and this fans out across the rest.
    let mut files: Vec<graph_format::manifest::FileEntry> = file_names
        .par_iter()
        .map(|name| -> Result<graph_format::manifest::FileEntry> {
            let path = inp.tmp_dir.join(name);
            let bytes = fs::metadata(&path)
                .with_context(|| format!("stat {}", path.display()))?
                .len();
            let (blake3, sha256, crc32c) =
                graph_format::integrity::hash_file_checksums(&path, object_checksums)?;
            Ok(graph_format::manifest::FileEntry {
                name: name.clone(),
                bytes,
                blake3,
                sha256,
                crc32c,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // `par_iter` preserves order, but the inventory's order is load-bearing (it is
    // folded into the content hash), so pin it to the sorted names rather than to a
    // property of the iterator.
    files.sort_by(|a, b| a.name.cmp(&b.name));
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
        compression_profile: inp.compression_profile,
        encryption: inp.encryption_header,
        node_count: inp.node_count,
        edge_count: inp.edge_count,
        labels: inp.labels,
        reltypes: inp.reltypes,
        property_keys: inp.property_keys,
        range_indexes: inp.range_indexes,
        vector_indexes: inp.vector_indexes,
        reltype_source_counts: inp.reltype_source_counts,
        reltype_target_counts: inp.reltype_target_counts,
        reltype_edge_counts: inp.reltype_edge_counts,
        reltype_self_loop_counts: inp.reltype_self_loop_counts,
        label_node_counts: inp.label_node_counts,
        first_label_counts: inp.first_label_counts,
        src_label_reltype_counts: inp.src_label_reltype_counts,
        reltype_tgt_label_counts: inp.reltype_tgt_label_counts,
        schema_triple_counts: inp.schema_triple_counts,
        property_histograms: inp.property_histograms,
        hub_degrees: inp.hub_degrees,
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

    // Publish the singleton set manifest (`sets/<uuid>.json`) *before* the `current`
    // pointer, so `current` only ever names a set whose manifest is fully written.
    // Set uuid == generation uuid in a singleton, so `current` keeps naming the
    // generation directory (nothing that reads `current` changes).
    let set = graph_format::setmanifest::SetManifest::singleton(inp.generation, now_unix());
    let sets_dir = inp.graph_dir.join("sets");
    fs::create_dir_all(&sets_dir).with_context(|| format!("create {}", sets_dir.display()))?;
    let set_path = sets_dir.join(format!("{}.json", inp.generation.0));
    let set_tmp = sets_dir.join(format!(".{}.json.tmp", inp.generation.0));
    fs::write(&set_tmp, set.to_bytes()?).with_context(|| format!("write {}", set_tmp.display()))?;
    fs::rename(&set_tmp, &set_path).with_context(|| format!("publish {}", set_path.display()))?;
    fsync_dir(&sets_dir)?;
    fsync_dir(inp.graph_dir)?;

    let current = inp.graph_dir.join("current");
    let current_tmp = inp.graph_dir.join(".current.tmp");
    fs::write(&current_tmp, format!("{}\n", inp.generation.0))
        .with_context(|| format!("write {}", current_tmp.display()))?;
    fs::rename(&current_tmp, &current).with_context(|| format!("swap {}", current.display()))?;
    fsync_dir(inp.graph_dir)?;

    // ---- optional remote publish ----
    // The local generation dir is now the validated staging area. Upload every
    // file to the object store, then write the remote `current` pointer last so a
    // reader never sees a pointer to a partially-uploaded generation (the same
    // copy-completeness barrier the local rename-current-last provides).
    if let Some(store) = &inp.store {
        upload_generation(
            store.as_ref(),
            inp.graph,
            inp.generation,
            inp.final_dir,
            &manifest.files,
        )
        .with_context(|| format!("upload generation {} to object store", inp.generation.0))?;
    }

    Ok(BuildOutcome {
        generation: inp.generation,
        content_hash,
        dir: inp.final_dir.to_path_buf(),
        node_count: inp.node_count,
        edge_count: inp.edge_count,
    })
}

/// Upload a finished, locally-published generation to an object store: every
/// data file (with its SHA-256 so S3 validates and stores the object checksum),
/// then the MANIFEST, then the `current` pointer **last** (the publish barrier —
/// `current` only ever names a fully-uploaded generation).
fn upload_generation(
    store: &dyn ObjectStore,
    graph: &str,
    generation: Generation,
    dir: &Path,
    files: &[graph_format::manifest::FileEntry],
) -> Result<()> {
    let base = join_key(graph, &generation.0.to_string());
    for fe in files {
        let bytes =
            fs::read(dir.join(&fe.name)).with_context(|| format!("read {} for upload", fe.name))?;
        store
            .put(&join_key(&base, &fe.name), &bytes, fe.sha256.as_deref())
            .with_context(|| format!("upload {}", fe.name))?;
    }
    // The MANIFEST and current pointer carry no inventory checksum (the MANIFEST
    // is authenticated by its own MAC; current is a tiny pointer).
    let manifest = fs::read(dir.join("MANIFEST.json")).context("read MANIFEST.json for upload")?;
    store
        .put(&join_key(&base, "MANIFEST.json"), &manifest, None)
        .context("upload MANIFEST.json")?;
    // The singleton set manifest, then the `current` pointer last — the same
    // publish barrier as the local path (`current` only names a fully-uploaded set).
    let set = graph_format::setmanifest::SetManifest::singleton(generation, now_unix());
    store
        .put(
            &graph_format::setmanifest::SetManifest::key(graph, generation),
            &set.to_bytes()?,
            None,
        )
        .context("upload set manifest")?;
    store
        .put(
            &join_key(graph, "current"),
            format!("{}\n", generation.0).as_bytes(),
            None,
        )
        .context("write remote current pointer")?;
    Ok(())
}

/// Derive per-(label, property) value→count histograms from the already-written
/// **node** range-index ISAMs under `tmp_dir/range/`, write them into
/// `tmp_dir/prop_hist.blk`, and return the aligned descriptors for the MANIFEST.
///
/// Shared by both build paths and called *after* the range indexes are written, so
/// each histogram run-length-counts the finished ISAM via the same
/// `distinct_key_counts` the query path uses — the two builders are therefore
/// guaranteed to agree. A node index whose distinct count exceeds `max_distinct`
/// (or any index, when `max_distinct == 0`) is skipped and logged; edge indexes
/// never get a histogram. `prop_hist.blk` is always written (empty if nothing
/// qualifies) so the inventory and content hash stay stable.
pub fn build_property_histograms(
    tmp_dir: &Path,
    range_indexes: &[RangeIndexDesc],
    block_size: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
    max_distinct: u64,
) -> Result<Vec<PropertyHistogramDesc>> {
    let mut descs = Vec::new();
    let mut records = Vec::new();
    for ri in range_indexes {
        if ri.entity != EntityKind::Node {
            continue;
        }
        let rel = format!("range/{}.isam", ri.name);
        let isam_path = tmp_dir.join(&rel);
        match derive_histogram_from_isam(&isam_path, file_cipher(&cipher, &rel), max_distinct)? {
            Some(pairs) => {
                descs.push(PropertyHistogramDesc {
                    index_name: ri.name.clone(),
                    label: ri.label_or_type.clone(),
                    property: ri.property.clone(),
                    distinct_count: pairs.len() as u64,
                });
                records.push(encode_histogram(&pairs));
            }
            None => {
                tracing::info!(
                    "histogram skipped for ({}, {}): distinct values exceed \
                     --histogram-max-distinct {} (group-by/count(DISTINCT) will scan the index)",
                    ri.label_or_type,
                    ri.property,
                    max_distinct
                );
            }
        }
    }
    write_property_histograms(
        tmp_dir.join("prop_hist.blk"),
        &records,
        block_size,
        zstd_level,
        file_cipher(&cipher, "prop_hist.blk"),
    )?;
    Ok(descs)
}
