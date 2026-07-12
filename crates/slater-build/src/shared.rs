// SPDX-License-Identifier: Apache-2.0
//! Build primitives shared across the build front-end: the [`BuildOptions`]
//! config, the first-seen [`Interner`], and vector-index construction (the
//! brute-force `vectors.f32.blk` store plus the disk-native Vamana/PQ path).
//!
//! Kept separate from the build orchestration in [`crate::build_external`] and
//! from the publish/manifest scaffolding in [`crate::common`], so the config and
//! the vector-store writers have one unambiguous home.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use graph_format::crypto::BlockCipher;
use graph_format::manifest::{AnnMode, Metric, VectorIndexDesc};
use graph_format::pq::{train_codebooks, PqParams, PqWriter};
use graph_format::vamana::{bfs_order, build_vamana, VamanaWriter};
use graph_format::vectors::VectorStoreWriter;

use crate::model::VectorIndexStmt;

/// Tunables for one build (all have sensible defaults in the CLI).
/// Format of the build `--input`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum InputFormat {
    /// A primitive-Cypher creation script (the default; parsed in pass 1).
    Cypher,
    /// A binary consolidation dump directory ([`graph_format::consolidate_dump`]),
    /// ingested directly (parse / dedup / resolve skipped) — see
    /// [`crate::direct_ingest`].
    SlaterDump,
}

pub struct BuildOptions {
    /// Format of the input (`--input-format`). `SlaterDump` selects the direct
    /// binary-dump ingest path; `Cypher` (default) parses the input script.
    pub input_format: InputFormat,
    /// Identity model of the input dump.
    ///
    /// `None` (default) ⇒ **merge** style: nodes/edges are business-key `MERGE`
    /// statements, the per-pattern business key is the node identity, and edges resolve
    /// endpoints by it (see [`crate::merge_build`]); dumps must be self-contained.
    ///
    /// `Some(field)` ⇒ **single-global-key** ("dump_id style") import: `field` is the
    /// unique node identity across the whole dump (label-agnostic, integer-valued),
    /// edges reference endpoints by it, and `field` is stored as a queryable property.
    /// `Some("__dump_id__")` ingests legacy FalkorDB `GRAPH.DUMP` files.
    pub pk: Option<String>,
    /// Target block size for prop/label/topology files, bytes.
    pub block_size: usize,
    /// Target block size for **range (ISAM) index** leaf blocks, bytes. Deliberately
    /// smaller than `block_size`: a range index is probed by *point* lookups (business-key
    /// write resolve, indexed equality/range seeks), and a lookup decodes a whole leaf, so
    /// a large block makes every probe decode tens of thousands of entries. Smaller leaves
    /// keep a point lookup cheap even on a cache miss, at a modest cost to compression and
    /// range-scan seek count. See D53.
    pub range_block_size: usize,
    /// Target block size for the vector store, bytes.
    pub vector_block_size: usize,
    /// Effective zstd level applied to all published `.blk`/index files. Resolved
    /// from [`Self::compression_profile`] (and any explicit `--zstd-level`) at the
    /// CLI before the build starts; everything downstream reads this scalar.
    pub zstd_level: i32,
    /// Name of the backend-aware profile that produced [`Self::zstd_level`]
    /// (`"local"` / `"remote"` / `"max"` / `"manual"`). Recorded in the manifest.
    pub compression_profile: String,
    /// Cap on a per-(label, property) histogram's distinct-key count: a node range
    /// index with more distinct values than this is not given a `prop_hist.blk`
    /// histogram (it would be as large as the index for no benefit). `0` disables
    /// histograms entirely. See [`graph_format::histogram`].
    pub histogram_max_distinct: u64,
    /// Degree at/above which a node is recorded in the `hub_degrees.blk` sidecar (its
    /// out or in direction). Must be `<=` any query-side stream threshold so the sidecar
    /// holds an exact degree for every node a query might stream. See
    /// [`graph_format::hubdegree`].
    pub hub_degree_floor: u32,
    /// Degree-column `zstd-dense` selection penalty: zstd wins a chunk only when its size is
    /// `<= degree_zstd_margin ×` the best decompress-free candidate. Low (< 1) = latency-biased
    /// (prefer decompress-free EF, for fs/NVMe); `>= 1` = size/wire-biased (let zstd win, for
    /// object stores). Resolved at the CLI from the compression profile / `--degree-zstd-margin`.
    /// See [`graph_format::degree_ef::DegreeCodecOpts`].
    pub degree_zstd_margin: f64,
    /// Optional `VectorIndexSpec[]` JSON sidecar (label/property/dim/metric).
    pub vector_index_json: Option<PathBuf>,
    /// At-rest encryption master key (raw bytes). `None` ⇒ plaintext image, the
    /// default, so M2–M5 fixtures and the golden test keep working unchanged.
    pub encryption_key: Option<Vec<u8>>,
    /// BLAKE3 digest (hex) of the live `acl.json` to stamp into the manifest
    /// (`--acl`). `None` ⇒ no ACL stamp.
    pub acl_blake3: Option<String>,
    /// Vector indexes with at least this many vectors are built Vamana/PQ; below
    /// it they stay brute-force full-precision (M5 path).
    pub ann_threshold: u64,
    /// Vamana out-degree bound `R`.
    pub vamana_r: u32,
    /// Vamana robust-prune long-edge factor `alpha`.
    pub vamana_alpha: f32,
    /// PQ subspace count `m`.
    pub pq_subspaces: u32,
    /// PQ bits per subspace.
    pub pq_bits: u32,

    // ---- external (bounded-memory) build knobs ----
    /// Working-memory budget (bytes) for spill/sort/cluster in the external path.
    pub max_memory_bytes: usize,
    /// Scratch directory for spill files; `None` ⇒ a `.scratch-<gen>` under the
    /// graph dir. Removed on a successful build unless `keep_temp`.
    pub temp_dir: Option<PathBuf>,
    /// Node-id reordering for on-disk locality (external path only).
    pub cluster: crate::cluster::ClusterMode,
    /// LDG refinement passes (external path only).
    pub cluster_passes: u32,
    /// Keep scratch (buckets/spill) after a successful build, for debugging.
    pub keep_temp: bool,
    /// Resume an interrupted external build from its surviving scratch, instead of
    /// starting fresh.
    pub resume: bool,
    /// Worker-thread cap for the parallel build stages (pass 1, resolve, external
    /// sort spill pool, and the global rayon pool). Defaults at the CLI to
    /// `max(online_cores - 2, 1)`.
    pub threads: usize,
    /// When set, also publish the finished generation to this object store (e.g.
    /// S3) after the local atomic publish: the local build dir is the staging
    /// area, every file is uploaded, then the remote `current` pointer is written
    /// last. `None` ⇒ filesystem-only publish (the default).
    pub publish_store: Option<Arc<dyn graph_format::store::ObjectStore>>,
    /// Compute the per-file SHA-256 / CRC32C object checksums even for a
    /// filesystem-only build. Implied when `publish_store` is set. Set this when the
    /// generation will be copied to S3/GCS by other means and must keep its
    /// content-grade integrity check there rather than falling back to a size check.
    pub object_checksums: bool,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            pk: None,
            input_format: InputFormat::Cypher,
            block_size: 256 * 1024,
            range_block_size: 16 * 1024,
            vector_block_size: 256 * 1024,
            zstd_level: 3,
            compression_profile: "manual".into(),
            histogram_max_distinct: graph_format::histogram::DEFAULT_HISTOGRAM_MAX_DISTINCT,
            hub_degree_floor: graph_format::hubdegree::DEFAULT_HUB_DEGREE_FLOOR,
            degree_zstd_margin: graph_format::degree_ef::DEFAULT_ZSTD_SELECT_MARGIN,
            vector_index_json: None,
            encryption_key: None,
            acl_blake3: None,
            ann_threshold: 50_000,
            vamana_r: 32,
            vamana_alpha: 1.2,
            pq_subspaces: 16,
            pq_bits: 8,
            max_memory_bytes: 4 * 1024 * 1024 * 1024,
            temp_dir: None,
            cluster: crate::cluster::ClusterMode::Ldg,
            cluster_passes: 3,
            keep_temp: false,
            resume: false,
            publish_store: None,
            object_checksums: false,
            threads: std::thread::available_parallelism()
                .map(|n| n.get().saturating_sub(2).max(1))
                .unwrap_or(1),
        }
    }
}

/// First-seen interner: name → dense id, preserving insertion order.
#[derive(Default)]
pub(crate) struct Interner {
    map: HashMap<String, u32>,
    names: Vec<String>,
}

impl Interner {
    pub(crate) fn intern(&mut self, name: &str) -> u32 {
        if let Some(id) = self.map.get(name) {
            return *id;
        }
        let id = self.names.len() as u32;
        self.map.insert(name.to_string(), id);
        self.names.push(name.to_string());
        id
    }

    pub(crate) fn get(&self, name: &str) -> Option<u32> {
        self.map.get(name).copied()
    }

    pub(crate) fn into_names(self) -> Vec<String> {
        self.names
    }

    /// Borrow the name table (e.g. to checkpoint it without consuming the interner).
    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// Reconstruct an interner from its persisted name table (for resume).
    pub(crate) fn from_names(names: Vec<String>) -> Self {
        let map = names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as u32))
            .collect();
        Self { map, names }
    }
}

/// Lloyd iterations used when training PQ codebooks. Fixed (not a CLI knob) — the
/// build is offline, and more iterations only sharpen the codebook a little.
const PQ_ITERS: usize = 25;

/// An index's gathered vectors, awaiting a brute-force-vs-Vamana routing decision.
pub(crate) struct PendingIndex {
    pub(crate) label: String,
    pub(crate) property: String,
    pub(crate) dim: u32,
    pub(crate) metric: Metric,
    pub(crate) entries: Vec<(u64, Vec<f32>)>,
}

/// Route each gathered [`PendingIndex`] by cardinality and write the vector store
/// (`vectors.f32.blk`) plus any Vamana/PQ files. Both build paths gather the
/// `(node_id, vector)` sets differently but emit the index identically. Returns the
/// manifest descriptors and the extra (Vamana/PQ) inventory file names.
pub(crate) fn write_vector_indexes(
    tmp_dir: &Path,
    pending: &[PendingIndex],
    opts: &BuildOptions,
    cipher: Option<Arc<BlockCipher>>,
    block_sizes: &mut BTreeMap<String, u32>,
) -> Result<(Vec<VectorIndexDesc>, Vec<String>)> {
    fs::create_dir_all(tmp_dir.join("vector"))
        .with_context(|| format!("create {}", tmp_dir.join("vector").display()))?;
    let mut vector_indexes: Vec<VectorIndexDesc> = Vec::new();
    // Extra inventory files produced by the Vamana path (rel paths under tmp_dir).
    let mut vector_files: Vec<String> = Vec::new();
    let mut w = VectorStoreWriter::create_with_cipher(
        tmp_dir.join("vectors.f32.blk"),
        opts.vector_block_size,
        opts.zstd_level,
        cipher.clone(),
    )?;
    for pi in pending {
        let count = pi.entries.len() as u64;
        if count >= opts.ann_threshold && vamana_eligible(pi, opts) {
            // Disk-native Vamana/PQ path.
            let (desc, files) = build_vamana_index(tmp_dir, pi, opts, cipher.clone())?;
            for (name, block) in &files {
                vector_files.push(name.clone());
                block_sizes.insert(name.clone(), *block);
            }
            vector_indexes.push(desc);
        } else {
            // Brute-force: append the group to vectors.f32.blk (M5 path).
            let first_record = w.len();
            for (nid, xs) in &pi.entries {
                w.append(*nid, xs)?;
            }
            vector_indexes.push(VectorIndexDesc {
                label: pi.label.clone(),
                property: pi.property.clone(),
                dim: pi.dim,
                metric: pi.metric,
                count,
                first_record,
                mode: AnnMode::BruteForce,
            });
        }
    }
    w.finish()?;
    block_sizes.insert("vectors.f32.blk".into(), opts.vector_block_size as u32);
    Ok((vector_indexes, vector_files))
}

/// Whether an above-threshold index can use the Vamana/PQ path. v1 supports the
/// **cosine** metric only (the build normalises to unit vectors and navigates by
/// squared-L2 — D29), and PQ needs `pq_subspaces` to divide the dimension. Anything
/// else falls back to brute force, with a note on stderr.
fn vamana_eligible(pi: &PendingIndex, opts: &BuildOptions) -> bool {
    if pi.metric != Metric::Cosine {
        eprintln!(
            "note: vector index {}.{} is above the ANN threshold but its metric is not cosine; \
             building brute-force (Vamana v1 is cosine-only)",
            pi.label, pi.property
        );
        return false;
    }
    if pi.dim % opts.pq_subspaces != 0 {
        eprintln!(
            "note: vector index {}.{} dim {} is not divisible by --pq-subspaces {}; \
             building brute-force",
            pi.label, pi.property, pi.dim, opts.pq_subspaces
        );
        return false;
    }
    true
}

/// L2-normalise a vector to unit length (cosine path — D29). A zero vector is left
/// as-is (its cosine to anything is defined as 0 elsewhere).
fn normalise(v: &[f32]) -> Vec<f32> {
    let norm: f64 = v
        .iter()
        .map(|&x| (x as f64) * (x as f64))
        .sum::<f64>()
        .sqrt();
    if norm == 0.0 {
        return v.to_vec();
    }
    v.iter().map(|&x| (x as f64 / norm) as f32).collect()
}

/// Build the Vamana graph + PQ codes for one above-threshold index and write its
/// `vector/<l>.<p>.vamana` + `.pq` files. Returns the MANIFEST descriptor and the
/// `(rel_path, block_size)` of each file written, for the inventory.
///
/// The Vamana nodes and the PQ codes are written in the **same BFS-from-medoid
/// layout order**, so a vamana index `i` and PQ code record `i` refer to the same
/// vector; adjacency is rewritten to that permuted index space and the medoid is
/// recorded as its post-permutation index.
fn build_vamana_index(
    tmp_dir: &Path,
    pi: &PendingIndex,
    opts: &BuildOptions,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<(VectorIndexDesc, Vec<(String, u32)>)> {
    // Normalise once; both the graph build and PQ training work in this space.
    let normed: Vec<Vec<f32>> = pi.entries.iter().map(|(_, v)| normalise(v)).collect();

    let graph = build_vamana(&normed, opts.vamana_r as usize, opts.vamana_alpha)
        .with_context(|| format!("build Vamana graph for {}.{}", pi.label, pi.property))?;
    let order = bfs_order(&graph);
    // old (build) index → new (storage/layout) index.
    let mut new_of = vec![0u32; order.len()];
    for (new_idx, &old) in order.iter().enumerate() {
        new_of[old as usize] = new_idx as u32;
    }
    let medoid_new = new_of[graph.medoid as usize];

    // `.vamana`: full vectors + block-relative adjacency, in layout order.
    let vam_rel = format!("vector/{}.{}.vamana", pi.label, pi.property);
    let mut vw = VamanaWriter::create_with_cipher(
        tmp_dir.join(&vam_rel),
        opts.vector_block_size,
        opts.zstd_level,
        cipher.clone(),
    )?;
    for &old in &order {
        let nbrs: Vec<u32> = graph.adjacency[old as usize]
            .iter()
            .map(|&j| new_of[j as usize])
            .collect();
        vw.append(pi.entries[old as usize].0, &normed[old as usize], &nbrs)?;
    }
    vw.finish()?;

    // `.pq`: trained codebooks + per-vector codes, same layout order.
    let params = PqParams::new(pi.dim, opts.pq_subspaces, opts.pq_bits)?;
    let codebook = train_codebooks(&normed, params, PQ_ITERS)
        .with_context(|| format!("train PQ codebooks for {}.{}", pi.label, pi.property))?;
    let pq_rel = format!("vector/{}.{}.pq", pi.label, pi.property);
    let mut pw = PqWriter::create_with_cipher(
        tmp_dir.join(&pq_rel),
        &codebook,
        opts.block_size,
        opts.zstd_level,
        cipher,
    )?;
    for &old in &order {
        let codes = codebook.encode(&normed[old as usize])?;
        pw.append_codes(pi.entries[old as usize].0, &codes)?;
    }
    pw.finish()?;

    let desc = VectorIndexDesc {
        label: pi.label.clone(),
        property: pi.property.clone(),
        dim: pi.dim,
        metric: pi.metric,
        count: pi.entries.len() as u64,
        // first_record is a vectors.f32.blk offset; the Vamana arm never reads that
        // store, so it is irrelevant here (D31).
        first_record: 0,
        mode: AnnMode::Vamana {
            r: opts.vamana_r,
            alpha: opts.vamana_alpha,
            medoid: medoid_new as u64,
            pq_subspaces: opts.pq_subspaces,
            pq_bits: opts.pq_bits,
        },
    };
    Ok((
        desc,
        vec![
            (vam_rel, opts.vector_block_size as u32),
            (pq_rel, opts.block_size as u32),
        ],
    ))
}

pub(crate) fn parse_metric(s: &str) -> Result<Metric> {
    match s.to_ascii_lowercase().as_str() {
        "cosine" => Ok(Metric::Cosine),
        "euclidean" | "l2" => Ok(Metric::L2),
        "ip" | "dot" | "dotproduct" | "inner_product" => Ok(Metric::Dot),
        other => bail!("unknown vector metric '{other}'"),
    }
}

/// `VectorIndexSpec` sidecar entry — the JSON shape of `--vector-index-json`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct VectorIndexSpec {
    label: String,
    property: String,
    dim: u32,
    #[serde(default = "default_metric")]
    metric: String,
}

fn default_metric() -> String {
    "cosine".to_string()
}

pub(crate) fn load_vector_sidecar(path: &Path) -> Result<Vec<VectorIndexStmt>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read vector-index sidecar {}", path.display()))?;
    let specs: Vec<VectorIndexSpec> = serde_json::from_str(&text)
        .with_context(|| format!("parse vector-index sidecar {}", path.display()))?;
    Ok(specs
        .into_iter()
        .map(|s| VectorIndexStmt {
            label: s.label,
            property: s.property,
            dim: s.dim,
            metric: s.metric,
        })
        .collect())
}
