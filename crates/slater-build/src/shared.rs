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
use graph_format::extsort::{ExtSorter, SortRecord};
use graph_format::manifest::{AnnMode, AnnNav, Metric, VectorIndexDesc};
use graph_format::pq::{ann_point, ann_pq_params, l2_norm, train_codebooks, PqParams, PqWriter};
use graph_format::vamana::{bfs_order, build_vamana, build_vamana_ip, VamanaWriter};
use graph_format::vamana_merge::{compose_final_ids, streaming_merge, MergeInputs, MergeParams};
use graph_format::vectors::VectorStoreWriter;
use graph_format::wire::{read_uvarint, write_uvarint};

use crate::cluster::Permutation;
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
    pub encryption_key: Option<zeroize::Zeroizing<Vec<u8>>>,
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

/// A declared vector index, awaiting a brute-force-vs-Vamana routing decision.
///
/// The gathered `(node_id, vector)` entries are **not** held here — they are spilled
/// to a per-index [`ExtSorter<PendingVector>`] under a [`MemoryBudget`] reservation
/// (like every other build sink) and streamed into the writer. Holding them resident
/// in a `Vec` here accumulated `nodes × dim × 4` bytes off-budget, OOMing large
/// `--pk` builds regardless of `--max-memory`. Only the routing metadata and the
/// pushed `count` (needed for the `count >= ann_threshold` decision before the sorter
/// is drained) live on the struct.
pub(crate) struct PendingIndex {
    pub(crate) label: String,
    pub(crate) property: String,
    pub(crate) dim: u32,
    pub(crate) metric: Metric,
    /// Number of `(node_id, vector)` entries pushed into this index's sorter.
    pub(crate) count: u64,
    /// Present iff this index is **carried by reference** (HIK-117): the sorter then holds
    /// only the Δ (the `streaming_merge` inserts), never the base — the base graph is folded
    /// in from the referenced files rather than rebuilt.
    pub(crate) carry: Option<crate::model::VectorCarry>,
}

/// One gathered vector, spilled to and merged back from the per-index sorter.
///
/// Keyed by a per-index monotonic `seq` assigned in push (scan) order, so
/// [`ExtSorter::sorted`] restores the **exact insertion order** — byte-identical to
/// the previous fully-resident `Vec`, for both the identity and permutation build
/// paths (where entries are in scan order, not `node_id` order). `id`/`vec` are the
/// payload. The record is self-delimiting: `seq` and `id` are uvarints and the vector
/// is the remaining bytes as little-endian `f32`s, so no length prefix is needed.
pub(crate) struct PendingVector {
    pub(crate) seq: u64,
    pub(crate) id: u64,
    pub(crate) vec: Vec<f32>,
}

impl SortRecord for PendingVector {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.seq);
        write_uvarint(buf, self.id);
        for x in &self.vec {
            buf.extend_from_slice(&x.to_le_bytes());
        }
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let seq = read_uvarint(r)?;
        let id = read_uvarint(r)?;
        if !r.len().is_multiple_of(4) {
            bail!(
                "PendingVector record has a truncated f32 tail ({} bytes)",
                r.len()
            );
        }
        let mut vec = Vec::with_capacity(r.len() / 4);
        for chunk in r.chunks_exact(4) {
            vec.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        *r = &r[r.len()..];
        Ok(PendingVector { seq, id, vec })
    }
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        // `seq` is unique within an index, so this is a total order (one sorter per
        // index — seqs never collide across the records a sorter holds).
        self.seq.cmp(&other.seq)
    }
    fn size_hint(&self) -> usize {
        // 2 uvarints (≤10B each) + the f32 payload — the run-file cost.
        20 + self.vec.len() * 4
    }
    fn resident_hint(&self) -> usize {
        // The struct plus the heap the `Vec<f32>` owns: this is what the budget must
        // see so the sorter spills before the vector set exceeds its reservation.
        std::mem::size_of::<Self>() + self.vec.len() * 4
    }
}

/// Route each declared [`PendingIndex`] by cardinality and write the vector store
/// (`vectors.f32.blk`) plus any Vamana/PQ files. Both build paths gather the
/// `(node_id, vector)` sets differently but emit the index identically. Returns the
/// manifest descriptors and the extra (Vamana/PQ) inventory file names.
///
/// `sorters[i]` holds index `pending[i]`'s gathered entries, spilled under a budget
/// reservation and merged back in scan (`seq`) order — the same order the entries
/// were pushed, so the emitted store is byte-identical to appending them from a
/// resident `Vec`. The brute-force arm streams straight from the merge into the
/// writer (peak resident bounded by the reservation, not the set size); the Vamana
/// arm has to collect the group resident because its v1 graph build is in-memory.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_vector_indexes(
    tmp_dir: &Path,
    pending: &[PendingIndex],
    sorters: Vec<ExtSorter<PendingVector>>,
    opts: &BuildOptions,
    cipher: Option<Arc<BlockCipher>>,
    block_sizes: &mut BTreeMap<String, u32>,
    perm: &Permutation,
    data_dir: &Path,
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
    for (pi, sorter) in pending.iter().zip(sorters) {
        // ── Carried arm (HIK-117), checked FIRST — before the cardinality routing. A carried
        // index's sorter holds only the Δ (the base is folded in by reference), so `pi.count`
        // is the *insert* count and could be tiny or zero; routing on it would wrongly demote a
        // carried graph to brute-force over a handful of vectors.
        if let Some(carry) = &pi.carry {
            let (desc, files) = carry_vamana_index(
                tmp_dir,
                pi,
                carry,
                sorter,
                opts,
                cipher.clone(),
                perm,
                data_dir,
            )?;
            for (name, block) in &files {
                vector_files.push(name.clone());
                block_sizes.insert(name.clone(), *block);
            }
            vector_indexes.push(desc);
            continue;
        }
        let count = pi.count;
        if count >= opts.ann_threshold && vamana_eligible(pi, opts) {
            // Disk-native Vamana/PQ path. The in-memory graph build needs the whole
            // group resident, so materialise it here (in scan order) — but only for
            // this one index, and only at write time, not accumulated across the scan.
            let entries: Vec<(u64, Vec<f32>)> = sorter
                .sorted()?
                .map(|r| r.map(|e| (e.id, e.vec)))
                .collect::<Result<_>>()?;
            let (desc, files) = build_vamana_index(tmp_dir, pi, &entries, opts, cipher.clone())?;
            for (name, block) in &files {
                vector_files.push(name.clone());
                block_sizes.insert(name.clone(), *block);
            }
            vector_indexes.push(desc);
        } else {
            // Brute-force: stream the group straight into vectors.f32.blk (M5 path).
            let first_record = w.len();
            for entry in sorter.sorted()? {
                let entry = entry?;
                w.append(entry.id, &entry.vec)?;
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

/// Whether an above-threshold index can use the Vamana/PQ path.
///
/// All three metrics are supported (v8): each is mapped into a space where squared-L2 —
/// the only thing Vamana's robust prune is sound over — ranks the way the metric does. See
/// `graph_format::pq::ann_point`. The one remaining gate is PQ's: `pq_subspaces` must
/// divide the dimension. (It is the same gate for dot, whose ANN space is `dim + dim/m`
/// over `m + 1` subspaces and so divides exactly when `dim` divides by `m`.) Anything else
/// falls back to brute force, with a note on stderr.
fn vamana_eligible(pi: &PendingIndex, opts: &BuildOptions) -> bool {
    if !pi.dim.is_multiple_of(opts.pq_subspaces) {
        eprintln!(
            "note: vector index {}.{} dim {} is not divisible by --pq-subspaces {}; \
             building brute-force",
            pi.label, pi.property, pi.dim, opts.pq_subspaces
        );
        return false;
    }
    true
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
    entries: &[(u64, Vec<f32>)],
    opts: &BuildOptions,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<(VectorIndexDesc, Vec<(String, u32)>)> {
    // The ANN space: the transform under which squared-L2 — the only thing robust prune is
    // sound over — ranks the way this index's metric does. Both the graph build and PQ
    // training work in it, and the query path maps the query into the *same* space through
    // `ann_query`. One definition, in `graph_format::pq` (the D29 invariant, generalised to
    // all three metrics) — see its DESIGN note for why two arms must not disagree here.
    //
    // The **stored** vectors stay raw (below): the space is a navigation device, and the
    // exact re-rank scores the raw vector with the true metric.
    // HIK-137: Dot is built **IP-native** (raw inner product, no norm augmentation) — the graph via
    // `build_vamana_ip` and the codebook trained on the raw vectors over plain `PqParams`. Cosine
    // and L2 are untouched: they keep the augmented/`ann_point` L2-reduced ANN space. The branch is
    // on the metric alone; everything below reads `is_ip`/`nav`.
    let is_ip = pi.metric == Metric::Dot;
    let nav = if is_ip {
        AnnNav::InnerProduct
    } else {
        AnnNav::Augmented
    };
    let params = if is_ip {
        // No augmentation subspace: the codebook is dim-`dim`/`subspaces`-wide, trained on the raw
        // vectors, and `AdcTable::new_ip` reads the raw query against it.
        PqParams::new(pi.dim, opts.pq_subspaces, opts.pq_bits)?
    } else {
        ann_pq_params(pi.metric, pi.dim, opts.pq_subspaces, opts.pq_bits)?
    };
    let space_dim = params.dim as usize;
    // M = max‖x‖ over the indexed set — the dot/MIPS augmentation constant. Computed over
    // *every* entry, which is what makes `M² − ‖x‖² ≥ 0` hold for all of them. For the IP-native
    // Dot path it is **inert** (nothing augments), but still recorded in the manifest (§9 #3) and
    // still screened for f32 overflow below, which is a whole-manifest concern for every metric.
    let max_norm = entries
        .iter()
        .map(|(_, v)| l2_norm(v))
        .fold(0.0f64, f64::max);
    // M is recorded in the MANIFEST as an f32, and a vector of large-but-legal f32
    // components (1024 dimensions of 3e38) has a norm that overflows f32 to `+inf`. That
    // is not merely a dot-index problem: `serde_json` serialises a non-finite float as
    // `null`, so the manifest of *any* metric would fail to read back — the build would
    // "succeed" and publish a generation the server cannot open. Refuse it here, where the
    // message can say why.
    if (max_norm as f32).is_infinite() {
        bail!(
            "vector index {}.{} has a maximum vector norm ({max_norm:e}) that overflows f32; \
             its magnitudes are too large to index",
            pi.label,
            pi.property
        );
    }
    // The navigation-space points the graph is built over and the codebook is trained on. IP-native
    // Dot uses the **raw** vectors (no augmentation); cosine/L2 use the `ann_point` L2-reduced map.
    let points: Vec<Vec<f32>> = if is_ip {
        entries.iter().map(|(_, v)| v.clone()).collect()
    } else {
        entries
            .iter()
            .map(|(_, v)| ann_point(pi.metric, v, max_norm, space_dim))
            .collect::<Result<_>>()?
    };

    let graph = if is_ip {
        build_vamana_ip(&points, opts.vamana_r as usize)
    } else {
        build_vamana(&points, opts.vamana_r as usize, opts.vamana_alpha)
    }
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
        // The **raw** vector, not the ANN-space point: magnitudes must survive a rebuild
        // (a consolidation reads its vectors back out of here), and the exact re-rank
        // scores the raw vector under the true metric. The record carries no node id —
        // the `.pq` written below is the single layout→id map.
        vw.append(&entries[old as usize].1, &nbrs)?;
    }
    vw.finish()?;

    // `.pq`: trained codebooks + per-vector codes, same layout order — in the ANN space,
    // which is what the resident beam navigates by.
    let codebook = train_codebooks(&points, params, PQ_ITERS)
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
        let codes = codebook.encode(&points[old as usize])?;
        pw.append_codes(entries[old as usize].0, &codes)?;
    }
    pw.finish()?;

    let desc = VectorIndexDesc {
        label: pi.label.clone(),
        property: pi.property.clone(),
        dim: pi.dim,
        metric: pi.metric,
        count: entries.len() as u64,
        // first_record is a vectors.f32.blk offset; the Vamana arm never reads that
        // store, so it is irrelevant here (D31).
        first_record: 0,
        mode: AnnMode::Vamana {
            r: opts.vamana_r,
            alpha: opts.vamana_alpha,
            medoid: medoid_new as u64,
            pq_subspaces: opts.pq_subspaces,
            pq_bits: opts.pq_bits,
            // A freshly built index has no holes: the builder only ever sees live vectors.
            live_count: entries.len() as u64,
            max_norm: max_norm as f32,
            nav,
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

/// Carry a Vamana index through a consolidation **by reference** (HIK-117): fold this
/// build's Δ into the referenced base with `streaming_merge`, writing a fresh
/// `vector/<l>.<p>.vamana` + `.pq` pair — instead of the `O(N·R·L)` from-scratch
/// [`build_vamana_index`].
///
/// # The one composition that must be exactly right
/// The `.vamana` graph is carried, only the `.pq` id column is rewritten, so a wrong
/// `layout → new-id` map makes every KNN point at the wrong node with a plausible score and
/// **no error**. The map is `layout_to_dump_id ∘ perm.final_of` — the dump's compacted
/// dump-ids composed with this build's provisional→final permutation ([`compose_final_ids`],
/// `HOLE` carried through). `perm.final_of` is **old → new** (`cluster.rs::Permutation::Table`
/// holds `final_of_prov[prov] = final`); under `ClusterMode::None` it is the identity, so the
/// map is the dump-id column unchanged. The Δ inserts arrive from the sorter already keyed by
/// **final** id (the emit gather applied `perm.final_of` when it routed them), so they sit in
/// the same id space as `base_final_ids` with no further composition.
///
/// When the Δ is empty and there is no new delete, `streaming_merge` hard-links the base
/// `.vamana` byte-identically (`stats.vamana_carried`) — the whole point of the slice.
#[allow(clippy::too_many_arguments)]
fn carry_vamana_index(
    tmp_dir: &Path,
    pi: &PendingIndex,
    carry: &crate::model::VectorCarry,
    sorter: ExtSorter<PendingVector>,
    opts: &BuildOptions,
    cipher: Option<Arc<BlockCipher>>,
    perm: &Permutation,
    data_dir: &Path,
) -> Result<(VectorIndexDesc, Vec<(String, u32)>)> {
    // The `layout → dump-id` table, composed through this build's permutation into final ids.
    let layout_to_dump_id = read_carry_map(&carry.carry_map_path, carry.base_records)
        .with_context(|| {
            format!(
                "read carry map for vector index {}.{}",
                pi.label, pi.property
            )
        })?;
    let base_final_ids = compose_final_ids(&layout_to_dump_id, |id| perm.final_of(id));

    // The Δ inserts: the sorter holds *only* the delta for a carried index, already in scan
    // order and keyed by final id.
    let inserts: Vec<(u64, Vec<f32>)> = sorter
        .sorted()?
        .map(|r| r.map(|e| (e.id, e.vec)))
        .collect::<Result<_>>()?;

    let base_vamana = data_dir.join(&carry.base_vamana);
    let base_pq = data_dir.join(&carry.base_pq);
    let vam_rel = format!("vector/{}.{}.vamana", pi.label, pi.property);
    let pq_rel = format!("vector/{}.{}.pq", pi.label, pi.property);
    let vam_out = tmp_dir.join(&vam_rel);
    let pq_out = tmp_dir.join(&pq_rel);

    let params = MergeParams {
        medoid: carry.medoid as graph_format::vamana::VamanaIndex,
        r: carry.r as usize,
        alpha: carry.alpha,
        // Search-list width for an insert's greedy search — the build default (wider than R),
        // mirroring the base's build-time L; the base's exact L is not per-index state.
        l_build: ((carry.r as usize) * 2).max(64),
        metric: pi.metric,
        max_norm: carry.max_norm as f64,
        // HIK-137: carry the base's navigation discriminator so an IP-native base is folded by the
        // IP insert-weave + IP delete re-prune (and its Δ encoded raw), never spliced with augmented
        // edges. An augmented base stays augmented.
        nav: carry.nav,
        // The `.vamana` uses `vector_block_size`, the `.pq` uses `block_size` — matching
        // `build_vamana_index` so a carried index is byte-shaped like a freshly built one.
        vamana_block_bytes: opts.vector_block_size,
        pq_block_bytes: opts.block_size,
        zstd_level: opts.zstd_level,
        cipher,
    };
    let stats = streaming_merge(
        &base_vamana,
        &base_pq,
        &MergeInputs {
            base_final_ids: &base_final_ids,
            inserts: &inserts,
        },
        &params,
        &vam_out,
        &pq_out,
    )
    .with_context(|| {
        format!(
            "carry vector index {}.{} through consolidation ({} inserts, base {})",
            pi.label,
            pi.property,
            inserts.len(),
            base_vamana.display()
        )
    })?;

    let desc = VectorIndexDesc {
        label: pi.label.clone(),
        property: pi.property.clone(),
        dim: pi.dim,
        metric: pi.metric,
        // `count` is the record count (holes included) that bounds a layout ordinal; the
        // carried graph keeps every ordinal (holes, not compaction).
        count: stats.out_records,
        first_record: 0,
        mode: AnnMode::Vamana {
            r: carry.r,
            alpha: carry.alpha,
            // The medoid ordinal is carried: the layout is preserved (holes not compaction,
            // inserts appended past the base), so the base entry point stays valid.
            medoid: carry.medoid,
            pq_subspaces: carry.pq_subspaces,
            pq_bits: carry.pq_bits,
            live_count: stats.live,
            max_norm: carry.max_norm,
            // HIK-137 phase 3: the carried base's navigation discriminator is preserved through the
            // consolidation. An IP-native base folds a Δ with the IP insert-weave + IP delete re-prune
            // (`streaming_merge`/`consolidate_deletes` above, gated on this `nav`) and re-emits
            // `nav: inner_product`, so a Dot base stays IP-native across every consolidation.
            nav: carry.nav,
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

/// Read a carried index's raw-`u64`-LE `layout → dump-id` sidecar (`HOLE` for a dead ordinal).
/// `expected` is the base record count; a mismatch is a corrupt dump and is refused, since the
/// table indexes the base `.vamana` by position.
fn read_carry_map(path: &Path, expected: u64) -> Result<Vec<u64>> {
    let bytes =
        fs::read(path).with_context(|| format!("read carry map sidecar {}", path.display()))?;
    if bytes.len() % 8 != 0 {
        bail!(
            "carry map {} has {} bytes, not a whole number of u64s",
            path.display(),
            bytes.len()
        );
    }
    let n = (bytes.len() / 8) as u64;
    if n != expected {
        bail!(
            "carry map {} holds {n} ids but the dump declares {expected} base records — they \
             index the base .vamana by position",
            path.display()
        );
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect())
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
            carry: None,
        })
        .collect())
}

#[cfg(test)]
mod vector_gather_tests {
    //! Regression tests for HIK-96: the vector gather is spilled to a budgeted
    //! `ExtSorter` instead of accumulated fully resident in a `Vec`. They pin the two
    //! properties the fix must hold: (1) peak resident vector state is bounded by the
    //! reservation — the sorter spills to disk rather than holding the whole set — and
    //! (2) the emitted `vectors.f32.blk` is byte-identical regardless of how much the
    //! sorter spilled, i.e. scan order is preserved.

    use super::*;
    use graph_format::extsort::ExtSorter;
    use graph_format::membudget::MemoryBudget;
    use graph_format::vectors::VectorStoreReader;

    /// A deterministic dim-`dim` vector for node `i` (distinct across `i`).
    fn vec_for(i: u64, dim: u32) -> Vec<f32> {
        (0..dim).map(|d| (i as f32) * 0.5 + d as f32).collect()
    }

    fn scratch(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("slater_hik96_{}_{}", std::process::id(), name))
    }

    fn count_run_files(dir: &Path) -> usize {
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().map(|x| x == "run").unwrap_or(false))
                    .count()
            })
            .unwrap_or(0)
    }

    /// The sorter spills its runs to disk under a tiny reservation — it does **not**
    /// hold all N vectors resident — and the merge yields them back in exact scan
    /// (`seq`) order. This is the bound the old resident `Vec` violated.
    #[test]
    fn vector_sorter_spills_and_preserves_scan_order() {
        let dir = scratch("spill");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let n = 4000u64;
        let dim = 16u32;
        // Reserve only ~4 KiB — far below n × (≈40 + dim×4) bytes — so a resident Vec
        // could never fit and the sorter is forced to spill. `new_inline` spills
        // synchronously on this thread, so the run files are on disk deterministically
        // by the time we look (no pool-worker race).
        let budget = MemoryBudget::new(1 << 20);
        let res = budget.reserve_now("test", 4096, 1).unwrap();
        let mut sorter = ExtSorter::<PendingVector>::new_inline(&dir, res, 1).unwrap();
        for i in 0..n {
            sorter
                .push(PendingVector {
                    seq: i,
                    id: 10_000 + i,
                    vec: vec_for(i, dim),
                })
                .unwrap();
        }
        // Bound proof: the vector set was spilled, not held resident.
        assert!(
            count_run_files(&dir) > 1,
            "expected the sorter to spill multiple run files under a tiny reservation, \
             found {}",
            count_run_files(&dir)
        );

        // Order proof: merged output is exactly the insertion order.
        let mut seen = 0u64;
        for (expected, rec) in sorter.sorted().unwrap().enumerate() {
            let rec = rec.unwrap();
            assert_eq!(rec.seq, expected as u64);
            assert_eq!(rec.id, 10_000 + expected as u64);
            assert_eq!(rec.vec, vec_for(expected as u64, dim));
            seen += 1;
        }
        assert_eq!(seen, n, "every pushed vector must survive the round-trip");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Build one brute-force index's `vectors.f32.blk` from a per-index sorter with a
    /// given reservation, returning (raw store bytes, read-back entries).
    fn build_store(dir: &Path, entries: &[(u64, Vec<f32>)], dim: u32, res_bytes: usize) -> Vec<u8> {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let budget = MemoryBudget::new(res_bytes.max(1) * 4);
        let res = budget.reserve_now("test", res_bytes, 1).unwrap();
        let mut sorter = ExtSorter::<PendingVector>::new(dir, res, 3).unwrap();
        for (seq, (id, v)) in entries.iter().enumerate() {
            sorter
                .push(PendingVector {
                    seq: seq as u64,
                    id: *id,
                    vec: v.clone(),
                })
                .unwrap();
        }
        let pending = vec![PendingIndex {
            label: "Doc".into(),
            property: "emb".into(),
            dim,
            metric: Metric::Cosine,
            count: entries.len() as u64,
            carry: None,
        }];
        let opts = BuildOptions::default(); // ann_threshold 50_000 ⇒ brute-force arm
        let mut block_sizes = BTreeMap::new();
        write_vector_indexes(
            dir,
            &pending,
            vec![sorter],
            &opts,
            None,
            &mut block_sizes,
            &Permutation::Identity,
            dir,
        )
        .unwrap();
        std::fs::read(dir.join("vectors.f32.blk")).unwrap()
    }

    /// The emitted store is byte-identical whether the sorter ran as a single resident
    /// run or spilled into many — scan order is preserved end to end — and every
    /// `(id, vec)` is present and correct on read-back.
    #[test]
    fn vector_store_is_byte_identical_regardless_of_spill() {
        let dim = 8u32;
        let n = 1500u64;
        let entries: Vec<(u64, Vec<f32>)> =
            (0..n).map(|i| (7_000 + i * 3, vec_for(i, dim))).collect();

        // Huge reservation ⇒ one resident run (the pre-fix all-resident shape).
        let big_dir = scratch("byte_big");
        let bytes_big = build_store(&big_dir, &entries, dim, 64 << 20);
        // Tiny reservation ⇒ many spilled runs (the fix's bounded path).
        let small_dir = scratch("byte_small");
        let bytes_small = build_store(&small_dir, &entries, dim, 4096);

        assert_eq!(
            bytes_big, bytes_small,
            "vectors.f32.blk must be byte-identical regardless of spill (scan order preserved)"
        );

        // Correctness: every (id, vec) survives, in order.
        let reader = VectorStoreReader::open(small_dir.join("vectors.f32.blk")).unwrap();
        assert_eq!(reader.len(), n);
        for (i, (id, v)) in entries.iter().enumerate() {
            let got = reader.get(i as u64).unwrap();
            assert_eq!(got.node_id, *id);
            assert_eq!(&got.vector, v);
        }
        let _ = std::fs::remove_dir_all(&big_dir);
        let _ = std::fs::remove_dir_all(&small_dir);
    }
}

/// HIK-117: the **carried arm** of `write_vector_indexes`, at the real call site. These are the
/// server/dump/builder-level lift of HIK-115's in-crate proof: a consolidation carries the
/// `.vamana` by reference and rewrites only the `.pq` id column, so a wrong
/// `layout_to_dump_id ∘ perm.final_of` composition makes every KNN point at the wrong node with
/// a plausible score and no error. The tests attack that composition directly, with a
/// **non-monotone** permutation, and pin the BLAKE3-carried fast path.
#[cfg(test)]
mod carry_tests {
    use super::*;
    use graph_format::consolidate_dump::DumpWriter;
    use graph_format::membudget::MemoryBudget;
    use graph_format::pq::{ann_query, normalise, AdcTable, PqReader, HOLE};
    use graph_format::vamana::{beam_search, BeamParams, VamanaReader};
    use std::path::PathBuf;

    /// A tiny deterministic LCG (graph-format's own `Lcg` is `pub(crate)`), so the fixtures
    /// are reproducible without a dependency.
    struct Rng(u64);
    impl Rng {
        fn next_f64(&mut self) -> f64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("slater_carry_{}_{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn unit_vectors(dim: usize, n: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = Rng(seed);
        (0..n)
            .map(|_| {
                let v: Vec<f32> = (0..dim).map(|_| (rng.next_f64() as f32) - 0.5).collect();
                normalise(&v)
            })
            .collect()
    }

    /// Carry-by-reference is proven by **byte-identity** of the `.vamana` (a hard-link, or a
    /// byte copy on cross-device) — strictly at least as strong as the BLAKE3 the ticket names.
    fn same_bytes(a: &Path, b: &Path) -> bool {
        std::fs::read(a).unwrap() == std::fs::read(b).unwrap()
    }

    fn cosine(q: &[f32], v: &[f32]) -> f64 {
        let (mut dot, mut nq, mut nv) = (0.0f64, 0.0f64, 0.0f64);
        for (x, y) in q.iter().zip(v) {
            dot += *x as f64 * *y as f64;
            nq += *x as f64 * *x as f64;
            nv += *y as f64 * *y as f64;
        }
        if nq == 0.0 || nv == 0.0 {
            1.0
        } else {
            1.0 - dot / (nq.sqrt() * nv.sqrt())
        }
    }

    /// KNN over an on-disk `.vamana`+`.pq`, mirroring `exec::vamana_knn`.
    fn knn(vamana: &Path, pq: &Path, query: &[f32], medoid: u32, k: usize) -> Vec<(u64, f32)> {
        let reader = VamanaReader::open_with_cipher(vamana, None).unwrap();
        let resident = PqReader::open_with_cipher(pq, None)
            .unwrap()
            .load_resident()
            .unwrap();
        let n = resident.len();
        let space_dim = resident.codebook.params.dim as usize;
        let qn = ann_query(Metric::Cosine, query, space_dim).unwrap();
        let adc = AdcTable::new(&resident.codebook, &qn).unwrap();
        beam_search(
            BeamParams {
                medoid,
                beam_width: 96,
                k,
                num_nodes: n,
            },
            |i| adc.estimate(resident.codes_of(i as usize)),
            |i| {
                let node = reader.node(i)?;
                Ok((node.vector, node.neighbours))
            },
            |v| cosine(query, v) as f32,
            |i| {
                let id = resident.node_ids[i as usize];
                Ok(if id == HOLE { None } else { Some(id) })
            },
        )
        .unwrap()
        .iter()
        .map(|h| (h.node_id, h.exact))
        .collect()
    }

    fn brute(live: &[(u64, Vec<f32>)], query: &[f32], k: usize) -> Vec<u64> {
        let mut s: Vec<(f64, u64)> = live.iter().map(|(id, v)| (cosine(query, v), *id)).collect();
        s.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        s.into_iter().take(k).map(|(_, id)| id).collect()
    }

    /// Options with an ANN threshold low enough to build a Vamana index over the fixture.
    fn opts() -> BuildOptions {
        BuildOptions {
            ann_threshold: 10,
            vamana_r: 16,
            vamana_alpha: 1.2,
            pq_subspaces: 4,
            pq_bits: 8,
            ..BuildOptions::default()
        }
    }

    /// Build a base `.vamana`/`.pq` for `(label, property)` under
    /// `data_dir/graph/base_uuid/vector/`, exactly as a fresh build would, returning
    /// `(desc, base_dir_rel, base_pq_node_ids_in_layout_order)`.
    fn build_base(
        data_dir: &Path,
        entries: &[(u64, Vec<f32>)],
        dim: u32,
    ) -> (VectorIndexDesc, String, Vec<u64>) {
        let base_rel = "g/base";
        let base_dir = data_dir.join(base_rel);
        std::fs::create_dir_all(base_dir.join("vector")).unwrap();
        let pi = PendingIndex {
            label: "Doc".into(),
            property: "emb".into(),
            dim,
            metric: Metric::Cosine,
            count: entries.len() as u64,
            carry: None,
        };
        let (desc, _files) = build_vamana_index(&base_dir, &pi, entries, &opts(), None).unwrap();
        let pq = PqReader::open_with_cipher(base_dir.join("vector/Doc.emb.pq"), None)
            .unwrap()
            .load_resident()
            .unwrap();
        (desc, base_rel.to_string(), pq.node_ids.clone())
    }

    fn carry_for(
        data_dir: &Path,
        dump_dir: &Path,
        base_rel: &str,
        desc: &VectorIndexDesc,
        layout_to_dump_id: &[u64],
    ) -> crate::model::VectorCarry {
        let (r, alpha, medoid, max_norm, pq_subspaces, pq_bits, nav) = match desc.mode {
            AnnMode::Vamana {
                r,
                alpha,
                medoid,
                max_norm,
                pq_subspaces,
                pq_bits,
                nav,
                ..
            } => (r, alpha, medoid, max_norm, pq_subspaces, pq_bits, nav),
            _ => unreachable!("fixture builds Vamana"),
        };
        let _ = data_dir;
        let mut dw = DumpWriter::create(dump_dir).unwrap();
        let map_file = dw.write_vector_carry("Doc.emb", layout_to_dump_id).unwrap();
        crate::model::VectorCarry {
            base_vamana: format!("{base_rel}/vector/Doc.emb.vamana"),
            base_pq: format!("{base_rel}/vector/Doc.emb.pq"),
            carry_map_path: dump_dir.join(&map_file),
            base_records: layout_to_dump_id.len() as u64,
            r,
            alpha,
            medoid,
            max_norm,
            pq_subspaces,
            pq_bits,
            nav,
        }
    }

    /// Run the carried arm through the real `write_vector_indexes`, returning the emitted
    /// out-dir and the desc.
    fn run_carry(
        data_dir: &Path,
        out_dir: &Path,
        carry: crate::model::VectorCarry,
        delta: &[(u64, Vec<f32>)],
        perm: &Permutation,
    ) -> VectorIndexDesc {
        std::fs::create_dir_all(out_dir.join("vector")).unwrap();
        let budget = MemoryBudget::new(1 << 20);
        let mut sorter = ExtSorter::<PendingVector>::new(
            out_dir,
            budget.reserve_now("t", 1 << 16, 1).unwrap(),
            3,
        )
        .unwrap();
        for (seq, (id, v)) in delta.iter().enumerate() {
            sorter
                .push(PendingVector {
                    seq: seq as u64,
                    id: *id,
                    vec: v.clone(),
                })
                .unwrap();
        }
        let pending = vec![PendingIndex {
            label: "Doc".into(),
            property: "emb".into(),
            dim: carry_dim(&carry, delta),
            metric: Metric::Cosine,
            count: delta.len() as u64,
            carry: Some(carry),
        }];
        let mut block_sizes = BTreeMap::new();
        let (descs, _files) = write_vector_indexes(
            out_dir,
            &pending,
            vec![sorter],
            &opts(),
            None,
            &mut block_sizes,
            perm,
            data_dir,
        )
        .unwrap();
        descs.into_iter().next().unwrap()
    }

    fn carry_dim(_c: &crate::model::VectorCarry, delta: &[(u64, Vec<f32>)]) -> u32 {
        delta.first().map(|(_, v)| v.len() as u32).unwrap_or(24)
    }

    /// **The killer, at the builder arm.** A pure-permutation carry: the `.vamana` must be
    /// BLAKE3-identical to the base (fast path), the `.pq` id column must be exactly
    /// `layout_to_dump_id ∘ perm.final_of` with a **non-monotone** perm, and KNN must return
    /// the same nodes by their *permuted* ids with identical scores.
    #[test]
    fn pure_permutation_carries_vamana_and_composes_ids() {
        let data_dir = scratch("pure");
        let dim = 24usize;
        let vectors = unit_vectors(dim, 120, 0x0117_0000_0000_0001);
        // dump ids 0..N (the builder's provisional id space), deliberately not the layout order.
        let entries: Vec<(u64, Vec<f32>)> = vectors
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, v)| (i as u64, v))
            .collect();
        let (desc, base_rel, layout_ids) = build_base(&data_dir, &entries, dim as u32);
        let base_vamana = data_dir.join(&base_rel).join("vector/Doc.emb.vamana");
        let base_pq = data_dir.join(&base_rel).join("vector/Doc.emb.pq");
        let medoid = match desc.mode {
            AnnMode::Vamana { medoid, .. } => medoid as u32,
            _ => unreachable!(),
        };

        // Query near entry 3; record base KNN.
        let query = {
            let mut q = vectors[3].clone();
            q[0] += 0.05;
            normalise(&q)
        };
        let before = knn(&base_vamana, &base_pq, &query, medoid, 10);
        assert_eq!(before.len(), 10);

        // A NON-MONOTONE perm over the dump-id space (a reversal is non-monotone), so a
        // backwards or applied-twice composition cannot pass.
        let n = entries.len() as u64;
        let table: Vec<u32> = (0..n).map(|i| (n - 1 - i) as u32).collect();
        let perm = Permutation::Table(table.clone());
        let layout_to_dump_id = layout_ids.clone(); // no deletes: every ordinal is live

        let dump_dir = data_dir.join("dump");
        std::fs::create_dir_all(&dump_dir).unwrap();
        let carry = carry_for(&data_dir, &dump_dir, &base_rel, &desc, &layout_to_dump_id);
        let out_dir = data_dir.join("out");
        let out_desc = run_carry(&data_dir, &out_dir, carry, &[], &perm);

        let out_vamana = out_dir.join("vector/Doc.emb.vamana");
        let out_pq_path = out_dir.join("vector/Doc.emb.pq");
        assert!(
            same_bytes(&out_vamana, &base_vamana),
            "a pure-permutation carry must leave the .vamana byte-identical (the whole thesis)"
        );
        assert_eq!(out_desc.count, n, "record count preserved");

        // The composition, asserted directly on the emitted id column.
        let out_pq = PqReader::open_with_cipher(&out_pq_path, None)
            .unwrap()
            .load_resident()
            .unwrap();
        let expected: Vec<u64> = layout_to_dump_id
            .iter()
            .map(|&d| perm.final_of(d))
            .collect();
        assert_eq!(
            out_pq.node_ids, expected,
            "the .pq id column is the composition"
        );

        // HIK-137: the carry re-emits the base's navigation discriminator rather than hardcoding
        // one — a cosine base stays `Augmented` (this fixture), and an IP base would stay
        // `InnerProduct`. A carry that reverted the discriminator would mis-navigate the survivor.
        let (in_nav, out_nav) = match (&desc.mode, &out_desc.mode) {
            (AnnMode::Vamana { nav: a, .. }, AnnMode::Vamana { nav: b, .. }) => (*a, *b),
            _ => unreachable!("both are Vamana"),
        };
        assert_eq!(out_nav, in_nav, "the carry preserves the nav discriminator");

        // KNN returns the same nodes under the permuted ids, same scores.
        let after = knn(&out_vamana, &out_pq_path, &query, medoid, 10);
        let want: Vec<(u64, f32)> = before
            .iter()
            .map(|(id, s)| (perm.final_of(*id), *s))
            .collect();
        assert_eq!(after, want);
        assert_ne!(after[0].0, before[0].0, "ids genuinely moved");
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// The `ClusterMode::None` identity path (`final_id == dump_id`): still carries by
    /// reference, and the id column is the dump-id column unchanged.
    #[test]
    fn identity_perm_carries_and_leaves_ids_unchanged() {
        let data_dir = scratch("identity");
        let dim = 24usize;
        let vectors = unit_vectors(dim, 80, 0x0117_0000_0000_0002);
        let entries: Vec<(u64, Vec<f32>)> = vectors
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, v)| (i as u64, v))
            .collect();
        let (desc, base_rel, layout_ids) = build_base(&data_dir, &entries, dim as u32);
        let base_vamana = data_dir.join(&base_rel).join("vector/Doc.emb.vamana");

        let dump_dir = data_dir.join("dump");
        std::fs::create_dir_all(&dump_dir).unwrap();
        let carry = carry_for(&data_dir, &dump_dir, &base_rel, &desc, &layout_ids);
        let out_dir = data_dir.join("out");
        run_carry(&data_dir, &out_dir, carry, &[], &Permutation::Identity);

        assert!(
            same_bytes(&out_dir.join("vector/Doc.emb.vamana"), &base_vamana),
            "identity carry is byte-identical too"
        );
        let out_pq = PqReader::open_with_cipher(out_dir.join("vector/Doc.emb.pq"), None)
            .unwrap()
            .load_resident()
            .unwrap();
        assert_eq!(
            out_pq.node_ids, layout_ids,
            "identity perm leaves the dump-id column unchanged"
        );
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Delete + Δ re-embed through the carried arm: the fast path must **not** fire, KNN over
    /// the live set (independently-derived truth) must be correct under the permuted ids, and a
    /// deleted node must never come back — even queried by its own vector.
    #[test]
    fn delete_and_delta_carry_is_correct_and_not_fast_path() {
        let data_dir = scratch("delta");
        let dim = 32usize;
        let all = unit_vectors(dim, 220, 0x0117_0000_0000_0003);
        let base_n = 180usize;
        let base_vecs = &all[..base_n];
        let delta_vecs = &all[base_n..];
        let entries: Vec<(u64, Vec<f32>)> = base_vecs
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, v)| (i as u64, v))
            .collect();
        let (desc, base_rel, layout_ids) = build_base(&data_dir, &entries, dim as u32);
        let base_vamana = data_dir.join(&base_rel).join("vector/Doc.emb.vamana");
        let medoid = match desc.mode {
            AnnMode::Vamana { medoid, .. } => medoid as u32,
            _ => unreachable!(),
        };

        // Delete 20 base dump-ids; a reversal perm over the base-id space for the survivors.
        let n = base_n as u64;
        let table: Vec<u32> = (0..n).map(|i| (n - 1 - i) as u32).collect();
        let perm = Permutation::Table(table);
        let mut rng = Rng(0xdead_0000_0000_0001);
        let mut dead = std::collections::HashSet::new();
        while dead.len() < 20 {
            dead.insert((rng.next_f64() * n as f64) as u64 % n);
        }
        // layout_to_dump_id: HOLE for a deleted dump-id, else the dump-id (compact identity here).
        let layout_to_dump_id: Vec<u64> = layout_ids
            .iter()
            .map(|&d| if dead.contains(&d) { HOLE } else { d })
            .collect();

        // Δ: 40 fresh vectors with ids past the base, keyed by their *final* id (perm applied,
        // as the emit gather would): the merge takes them as inserts in this space.
        let delta: Vec<(u64, Vec<f32>)> = delta_vecs
            .iter()
            .enumerate()
            .map(|(k, v)| (10_000 + k as u64, v.clone()))
            .collect();

        let dump_dir = data_dir.join("dump");
        std::fs::create_dir_all(&dump_dir).unwrap();
        let carry = carry_for(&data_dir, &dump_dir, &base_rel, &desc, &layout_to_dump_id);
        let out_dir = data_dir.join("out");
        let out_desc = run_carry(&data_dir, &out_dir, carry, &delta, &perm);
        let out_vamana = out_dir.join("vector/Doc.emb.vamana");
        let out_pq = out_dir.join("vector/Doc.emb.pq");

        assert!(
            !same_bytes(&out_vamana, &base_vamana),
            "a delete+Δ merge must rewrite the .vamana, not carry it"
        );
        assert_eq!(
            out_desc.live_count(),
            (base_n - dead.len() + delta.len()) as u64,
            "live_count = surviving base + Δ"
        );

        // The live set (surviving base under the perm + Δ), the independently-derived truth.
        let mut live: Vec<(u64, Vec<f32>)> = Vec::new();
        for (i, &d) in layout_ids.iter().enumerate() {
            if !dead.contains(&d) {
                // recover the raw base vector for dump id d
                live.push((perm.final_of(d), base_vecs[d as usize].clone()));
            }
            let _ = i;
        }
        for (id, v) in &delta {
            live.push((*id, v.clone()));
        }

        let k = 10;
        let mut total = 0.0f64;
        for q in 0..20 {
            let mut query = live[(q * 7) % live.len()].1.clone();
            query[0] += 0.02;
            let query = normalise(&query);
            let got: std::collections::HashSet<u64> = knn(&out_vamana, &out_pq, &query, medoid, k)
                .into_iter()
                .map(|(id, _)| id)
                .collect();
            let truth = brute(&live, &query, k);
            total += truth.iter().filter(|id| got.contains(id)).count() as f64 / k as f64;
        }
        let recall = total / 20.0;
        assert!(
            recall >= 0.8,
            "recall@{k} over the live set was {recall:.3}, want ≥ 0.8"
        );

        // A deleted node never comes back — query by its own vector. KNN returns *final* ids, so
        // the deleted node is its permuted id `final_of(d)` (checking the raw dump-id `d` would
        // be a false positive: a surviving node legitimately holds *some other* node's dump-id as
        // its final id under a non-identity perm).
        for &d in dead.iter().take(5) {
            let hits = knn(&out_vamana, &out_pq, &base_vecs[d as usize], medoid, 10);
            let permuted = perm.final_of(d);
            assert!(
                !hits.iter().any(|(id, _)| *id == permuted),
                "deleted node (dump-id {d}, final {permuted}) was returned"
            );
        }
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
