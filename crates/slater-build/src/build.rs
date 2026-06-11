// SPDX-License-Identifier: Apache-2.0
//! The two-pass offline build: a stream of parsed statements in, an immutable,
//! generation-numbered on-disk image out.
//!
//! Pass 1 ingests every node (assigning dense `NodeId`s and recording the
//! transient `__dump_id__ → NodeId` map); pass 2 ingests relationships against
//! that map (assigning dense `EdgeId`s and building forward+reverse CSR). Symbol
//! tables (labels, relationship types, property keys) are interned in first-seen
//! order. `vecf32` values are routed to the vector store when a matching vector
//! index is declared, and otherwise kept inline as a column value; all other
//! scalars/arrays go to the column store. Range indexes become ISAM files. The
//! whole image is written to a temp directory, fsynced, hashed, sealed with a
//! MANIFEST, then atomically renamed into place and the `current` pointer swapped.
//!
// DESIGN: the build runs offline (CI / an admin box), never in the serving hot
// path, so it freely holds the whole parsed graph in memory (HashMaps, Vecs) —
// the streaming reader only avoids slurping the raw *text*, which can be huge.

use std::collections::HashMap;
use std::fs;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use graph_format::columns::PropsWriter;
use graph_format::crypto::{self, BlockCipher};
use graph_format::ids::Value;
use graph_format::ids::{EdgeId, Generation, NodeId};
use graph_format::integrity::{content_hash, hash_file};
use graph_format::isam::write_isam_with_cipher;
use graph_format::manifest::{
    AnnMode, EncryptionHeader, EntityKind, Manifest, Metric, RangeIndexDesc, VectorIndexDesc,
};
use graph_format::nodelabels::NodeLabelsWriter;
use graph_format::pq::{train_codebooks, PqParams, PqWriter};
use graph_format::topology::{write_csr_with_cipher, Edge};
use graph_format::vamana::{bfs_order, build_vamana, VamanaWriter};
use graph_format::vectors::VectorStoreWriter;

use crate::model::{Entity, Statement, VectorIndexStmt};
use crate::parser::{parse_statement, StatementReader};

/// Tunables for one build (all have sensible defaults in the CLI).
pub struct BuildOptions {
    /// Target block size for prop/label/topology/range files, bytes.
    pub block_size: usize,
    /// Target block size for the vector store, bytes.
    pub vector_block_size: usize,
    pub zstd_level: i32,
    /// Optional `VectorIndexSpec[]` JSON sidecar (label/property/dim/metric).
    pub vector_index_json: Option<PathBuf>,
    /// At-rest encryption master key (raw bytes). `None` ⇒ plaintext image, the
    /// default, so M2–M5 fixtures and the golden test keep working unchanged.
    pub encryption_key: Option<Vec<u8>>,
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
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            block_size: 256 * 1024,
            vector_block_size: 256 * 1024,
            zstd_level: 3,
            vector_index_json: None,
            encryption_key: None,
            ann_threshold: 50_000,
            vamana_r: 32,
            vamana_alpha: 1.2,
            pq_subspaces: 16,
            pq_bits: 8,
        }
    }
}

/// What a successful build produced.
pub struct BuildOutcome {
    pub generation: Generation,
    pub content_hash: String,
    pub dir: PathBuf,
    pub node_count: u64,
    pub edge_count: u64,
}

const DUMP_VERTEX: &str = "__DumpVertex__";
const DUMP_ID: &str = "__dump_id__";

/// First-seen interner: name → dense id, preserving insertion order.
#[derive(Default)]
struct Interner {
    map: HashMap<String, u32>,
    names: Vec<String>,
}

impl Interner {
    fn intern(&mut self, name: &str) -> u32 {
        if let Some(id) = self.map.get(name) {
            return *id;
        }
        let id = self.names.len() as u32;
        self.map.insert(name.to_string(), id);
        self.names.push(name.to_string());
        id
    }

    fn get(&self, name: &str) -> Option<u32> {
        self.map.get(name).copied()
    }

    fn into_names(self) -> Vec<String> {
        self.names
    }
}

/// Lloyd iterations used when training PQ codebooks. Fixed (not a CLI knob) — the
/// build is offline, and more iterations only sharpen the codebook a little.
const PQ_ITERS: usize = 25;

/// An index's gathered vectors, awaiting a brute-force-vs-Vamana routing decision.
struct PendingIndex {
    label: String,
    property: String,
    dim: u32,
    metric: Metric,
    entries: Vec<(u64, Vec<f32>)>,
}

/// A fully-resolved node held in memory during the build.
struct NodeRec {
    label_ids: Vec<u32>,
    /// Scalar/array/inline-vector properties to write to the column store.
    scalar_props: Vec<(u32, Value)>,
    /// `(property key, vector)` routed to the vector store (one per matched index).
    vector_props: Vec<(String, Vec<f32>)>,
}

/// Parse a whole dump script and build a generation. Returns the outcome.
pub fn build(
    input: impl BufRead,
    graph: &str,
    data_dir: &Path,
    opts: &BuildOptions,
) -> Result<BuildOutcome> {
    // ---- bucket statements -------------------------------------------------
    let mut node_stmts = Vec::new();
    let mut edge_stmts = Vec::new();
    let mut range_stmts = Vec::new();
    let mut vector_stmts: Vec<VectorIndexStmt> = Vec::new();

    let mut reader = StatementReader::new(input);
    while let Some(raw) = reader.next_statement()? {
        match parse_statement(&raw)
            .with_context(|| format!("in statement: {}", truncate(&raw, 120)))?
        {
            Statement::Node(n) => node_stmts.push(n),
            Statement::Edge(e) => edge_stmts.push(e),
            Statement::RangeIndex(r) => {
                // Drop the dump marker index — it ranges over the transient
                // __DumpVertex__/__dump_id__ that we never persist.
                if r.label_or_type != DUMP_VERTEX && r.property != DUMP_ID {
                    range_stmts.push(r);
                }
            }
            Statement::VectorIndex(v) => vector_stmts.push(v),
            Statement::Ignored => {}
        }
    }

    // Merge any sidecar vector-index specs.
    if let Some(path) = &opts.vector_index_json {
        vector_stmts.extend(load_vector_sidecar(path)?);
    }
    // Dedup vector indexes by (label, property); first declaration wins.
    let mut seen = std::collections::HashSet::new();
    vector_stmts.retain(|v| seen.insert((v.label.clone(), v.property.clone())));

    // ---- symbol tables -----------------------------------------------------
    let mut labels = Interner::default();
    let mut reltypes = Interner::default();
    let mut keys = Interner::default();

    // Set of (label, property) that have a declared vector index, for routing.
    let vec_index_set: std::collections::HashSet<(String, String)> = vector_stmts
        .iter()
        .map(|v| (v.label.clone(), v.property.clone()))
        .collect();

    // ---- pass 1: nodes -----------------------------------------------------
    let mut nodes: Vec<NodeRec> = Vec::with_capacity(node_stmts.len());
    let mut dump_to_node: HashMap<i64, NodeId> = HashMap::new();

    for stmt in node_stmts {
        let node_id = NodeId(nodes.len() as u64);

        let mut label_names: Vec<String> = Vec::new();
        for l in &stmt.labels {
            if l != DUMP_VERTEX {
                label_names.push(l.clone());
            }
        }
        let label_ids: Vec<u32> = label_names.iter().map(|l| labels.intern(l)).collect();

        let mut scalar_props = Vec::new();
        let mut vector_props = Vec::new();
        for (k, v) in stmt.props {
            if k == DUMP_ID {
                if let Value::Int(id) = v {
                    if dump_to_node.insert(id, node_id).is_some() {
                        bail!("duplicate __dump_id__ {id}");
                    }
                } else {
                    bail!("__dump_id__ must be an integer");
                }
                continue;
            }
            match v {
                Value::Vector(xs)
                    if label_names
                        .iter()
                        .any(|l| vec_index_set.contains(&(l.clone(), k.clone()))) =>
                {
                    // Routed to the vector store (a declared index covers it).
                    vector_props.push((k, xs));
                }
                other => {
                    // Everything else (incl. unindexed vectors) stays inline.
                    let kid = keys.intern(&k);
                    scalar_props.push((kid, other));
                }
            }
        }
        nodes.push(NodeRec {
            label_ids,
            scalar_props,
            vector_props,
        });
    }

    // ---- pass 2: edges -----------------------------------------------------
    let mut edges: Vec<Edge> = Vec::with_capacity(edge_stmts.len());
    let mut edge_props: Vec<Vec<(u32, Value)>> = Vec::with_capacity(edge_stmts.len());

    for stmt in edge_stmts {
        let src = *dump_to_node.get(&stmt.src_dump_id).with_context(|| {
            format!(
                "edge references unknown source __dump_id__ {}",
                stmt.src_dump_id
            )
        })?;
        let dst = *dump_to_node.get(&stmt.dst_dump_id).with_context(|| {
            format!(
                "edge references unknown target __dump_id__ {}",
                stmt.dst_dump_id
            )
        })?;
        let edge_id = EdgeId(edges.len() as u64);
        let reltype = reltypes.intern(&stmt.reltype);
        edges.push(Edge {
            src,
            dst,
            reltype,
            edge: edge_id,
        });
        let props: Vec<(u32, Value)> = stmt
            .props
            .into_iter()
            .map(|(k, v)| (keys.intern(&k), v))
            .collect();
        edge_props.push(props);
    }

    let node_count = nodes.len() as u64;
    let edge_count = edges.len() as u64;

    // ---- stage the generation directory ------------------------------------
    let graph_dir = data_dir.join(graph);
    fs::create_dir_all(&graph_dir).with_context(|| format!("create {}", graph_dir.display()))?;
    let generation = Generation(uuid::Uuid::new_v4());
    let tmp_dir = graph_dir.join(format!(".tmp-{}", generation.0));
    let final_dir = graph_dir.join(generation.0.to_string());
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir).ok();
    }
    fs::create_dir_all(tmp_dir.join("range"))
        .with_context(|| format!("create {}", tmp_dir.display()))?;

    // Derive the per-generation block cipher (and the MANIFEST header that records
    // the KDF salt — never the key) when encryption is requested. Every block
    // writer below threads this cipher; `None` writes the plaintext format.
    let (cipher, encryption_header): (Option<Arc<BlockCipher>>, Option<EncryptionHeader>) =
        match &opts.encryption_key {
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
        };

    let mut block_sizes: std::collections::BTreeMap<String, u32> = Default::default();

    // node_props.blk
    {
        let mut w = PropsWriter::create_with_cipher(
            tmp_dir.join("node_props.blk"),
            opts.block_size,
            opts.zstd_level,
            cipher.clone(),
        )?;
        for n in &nodes {
            w.append(&n.scalar_props)?;
        }
        w.finish()?;
        block_sizes.insert("node_props.blk".into(), opts.block_size as u32);
    }

    // node_labels.blk
    {
        let mut w = NodeLabelsWriter::create_with_cipher(
            tmp_dir.join("node_labels.blk"),
            opts.block_size,
            opts.zstd_level,
            cipher.clone(),
        )?;
        for n in &nodes {
            w.append(&n.label_ids)?;
        }
        w.finish()?;
        block_sizes.insert("node_labels.blk".into(), opts.block_size as u32);
    }

    // edge_props.blk
    {
        let mut w = PropsWriter::create_with_cipher(
            tmp_dir.join("edge_props.blk"),
            opts.block_size,
            opts.zstd_level,
            cipher.clone(),
        )?;
        for p in &edge_props {
            w.append(p)?;
        }
        w.finish()?;
        block_sizes.insert("edge_props.blk".into(), opts.block_size as u32);
    }

    // topology.csr.blk
    write_csr_with_cipher(
        tmp_dir.join("topology.csr.blk"),
        node_count,
        &edges,
        opts.block_size,
        opts.zstd_level,
        cipher.clone(),
    )?;
    block_sizes.insert("topology.csr.blk".into(), opts.block_size as u32);

    // ---- vector indexes ----------------------------------------------------
    // First gather each index's `(node_id, vector)` set, then route each by
    // cardinality: below `--ann-threshold` it is full-precision in
    // `vectors.f32.blk` (the M5 brute-force path); at/above it, a disk-native
    // Vamana graph + PQ codes in `vector/<l>.<p>.{vamana,pq}` (M7). Every node's
    // `vecf32` for an indexed `(label, property)` was already routed out of the
    // column store (D12), so this is the full label-filtered candidate set.
    let mut pending: Vec<PendingIndex> = Vec::new();
    for vi in &vector_stmts {
        let metric = parse_metric(&vi.metric)?;
        let label_id = labels.get(&vi.label);
        let mut entries: Vec<(u64, Vec<f32>)> = Vec::new();
        if let Some(lid) = label_id {
            for (nid, n) in nodes.iter().enumerate() {
                if !n.label_ids.contains(&lid) {
                    continue;
                }
                if let Some((_, xs)) = n.vector_props.iter().find(|(k, _)| *k == vi.property) {
                    if xs.len() as u32 != vi.dim {
                        bail!(
                            "vector index {}.{} declared dim {} but node {nid} has {}",
                            vi.label,
                            vi.property,
                            vi.dim,
                            xs.len()
                        );
                    }
                    entries.push((nid as u64, xs.clone()));
                }
            }
        }
        pending.push(PendingIndex {
            label: vi.label.clone(),
            property: vi.property.clone(),
            dim: vi.dim,
            metric,
            entries,
        });
    }

    fs::create_dir_all(tmp_dir.join("vector"))
        .with_context(|| format!("create {}", tmp_dir.join("vector").display()))?;

    let mut vector_indexes: Vec<VectorIndexDesc> = Vec::new();
    // Extra inventory files produced by the Vamana path (rel paths under tmp_dir).
    let mut vector_files: Vec<String> = Vec::new();
    {
        let mut w = VectorStoreWriter::create_with_cipher(
            tmp_dir.join("vectors.f32.blk"),
            opts.vector_block_size,
            opts.zstd_level,
            cipher.clone(),
        )?;
        for pi in &pending {
            let count = pi.entries.len() as u64;
            if count >= opts.ann_threshold && vamana_eligible(pi, opts) {
                // Disk-native Vamana/PQ path.
                let (desc, files) = build_vamana_index(&tmp_dir, pi, opts, cipher.clone())?;
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
    }

    // range/<name>.isam
    let mut range_indexes: Vec<RangeIndexDesc> = Vec::new();
    for ri in &range_stmts {
        let (entity, name) = match ri.entity {
            Entity::Node => (
                EntityKind::Node,
                format!("node_{}_{}", ri.label_or_type, ri.property),
            ),
            Entity::Edge => (
                EntityKind::Edge,
                format!("edge_{}_{}", ri.label_or_type, ri.property),
            ),
        };
        let key_id = keys.get(&ri.property);
        let entries = match (ri.entity, key_id) {
            (_, None) => Vec::new(), // property never appeared as a column value
            (Entity::Node, Some(kid)) => {
                let lid = labels.get(&ri.label_or_type);
                let mut v = Vec::new();
                if let Some(lid) = lid {
                    for (nid, n) in nodes.iter().enumerate() {
                        if !n.label_ids.contains(&lid) {
                            continue;
                        }
                        if let Some((_, val)) = n.scalar_props.iter().find(|(k, _)| *k == kid) {
                            v.push((val.clone(), nid as u64));
                        }
                    }
                }
                v
            }
            (Entity::Edge, Some(kid)) => {
                let rid = reltypes.get(&ri.label_or_type);
                let mut v = Vec::new();
                if let Some(rid) = rid {
                    for (eid, (edge, props)) in edges.iter().zip(&edge_props).enumerate() {
                        if edge.reltype != rid {
                            continue;
                        }
                        if let Some((_, val)) = props.iter().find(|(k, _)| *k == kid) {
                            v.push((val.clone(), eid as u64));
                        }
                    }
                }
                v
            }
        };
        let rel_path = format!("range/{name}.isam");
        write_isam_with_cipher(
            tmp_dir.join(&rel_path),
            entries,
            opts.block_size,
            opts.zstd_level,
            cipher.clone(),
        )?;
        block_sizes.insert(rel_path.clone(), opts.block_size as u32);
        range_indexes.push(RangeIndexDesc {
            name,
            entity,
            label_or_type: ri.label_or_type.clone(),
            property: ri.property.clone(),
        });
    }

    // ---- inventory + content hash ------------------------------------------
    let mut file_names: Vec<String> = vec![
        "node_props.blk".into(),
        "node_labels.blk".into(),
        "edge_props.blk".into(),
        "topology.csr.blk".into(),
        "vectors.f32.blk".into(),
    ];
    for ri in &range_indexes {
        file_names.push(format!("range/{}.isam", ri.name));
    }
    file_names.extend(vector_files);
    file_names.sort();

    let mut files = Vec::new();
    for name in &file_names {
        let path = tmp_dir.join(name);
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

    let manifest = Manifest {
        magic: String::from_utf8_lossy(graph_format::MAGIC).to_string(),
        format_version: graph_format::FORMAT_VERSION,
        build_uuid: generation,
        graph: graph.to_string(),
        created_unix: now_unix(),
        content_hash: content_hash.clone(),
        block_sizes,
        codec: "zstd".into(),
        zstd_level: opts.zstd_level,
        encryption: encryption_header,
        node_count,
        edge_count,
        labels: labels.into_names(),
        reltypes: reltypes.into_names(),
        property_keys: keys.into_names(),
        range_indexes,
        vector_indexes,
        files,
    };
    manifest.write_to_dir(&tmp_dir)?;

    // ---- atomic publish ----------------------------------------------------
    fsync_dir(&tmp_dir)?;
    if final_dir.exists() {
        // A UUID collision is astronomically unlikely; refuse rather than clobber.
        bail!(
            "generation directory already exists: {}",
            final_dir.display()
        );
    }
    fs::rename(&tmp_dir, &final_dir).with_context(|| format!("publish {}", final_dir.display()))?;
    fsync_dir(&graph_dir)?;

    // Swap the `current` pointer atomically (write temp, rename over).
    let current = graph_dir.join("current");
    let current_tmp = graph_dir.join(".current.tmp");
    fs::write(&current_tmp, format!("{}\n", generation.0))
        .with_context(|| format!("write {}", current_tmp.display()))?;
    fs::rename(&current_tmp, &current).with_context(|| format!("swap {}", current.display()))?;
    fsync_dir(&graph_dir)?;

    Ok(BuildOutcome {
        generation,
        content_hash,
        dir: final_dir,
        node_count,
        edge_count,
    })
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

fn parse_metric(s: &str) -> Result<Metric> {
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

fn load_vector_sidecar(path: &Path) -> Result<Vec<VectorIndexStmt>> {
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

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// fsync a directory so a rename/creation within it is durable (matters on
/// remote/network filesystems such as NFS).
fn fsync_dir(dir: &Path) -> Result<()> {
    let f = fs::File::open(dir).with_context(|| format!("open dir {}", dir.display()))?;
    f.sync_all()
        .with_context(|| format!("fsync dir {}", dir.display()))?;
    Ok(())
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let head: String = s.chars().take(n).collect();
        format!("{head}…")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::manifest::Manifest;
    use graph_format::pq::{AdcTable, PqReader};
    use graph_format::vamana::{beam_search, VamanaReader};

    /// A deterministic LCG so the synthetic dump is reproducible without a `rand`
    /// dependency (mirrors graph-format's training RNG).
    struct Lcg(u64);
    impl Lcg {
        fn next_f64(&mut self) -> f64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    fn unit(v: &[f32]) -> Vec<f32> {
        let n: f64 = v
            .iter()
            .map(|&x| (x as f64) * (x as f64))
            .sum::<f64>()
            .sqrt();
        v.iter().map(|&x| (x as f64 / n) as f32).collect()
    }

    fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
        let mut dot = 0.0f64;
        let (mut na, mut nb) = (0.0f64, 0.0f64);
        for (x, y) in a.iter().zip(b) {
            dot += *x as f64 * *y as f64;
            na += *x as f64 * *x as f64;
            nb += *y as f64 * *y as f64;
        }
        (1.0 - dot / (na.sqrt() * nb.sqrt())) as f32
    }

    /// Build a dump script of `n` nodes each carrying a `dim`-dim `vecf32`
    /// embedding, plus a cosine vector index over `(:Doc, embedding)`. Returns the
    /// script and the raw (un-normalised) vectors for ground-truth checks.
    fn synthetic_dump(n: usize, dim: usize) -> (String, Vec<Vec<f32>>) {
        let mut rng = Lcg(0xDEAD_BEEF_1234);
        let mut script = String::new();
        script.push_str("CALL db.idx.vector.createNodeIndex('Doc', 'embedding', ");
        script.push_str(&format!("{dim}, 'cosine');\n"));
        let mut vectors = Vec::with_capacity(n);
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|_| (rng.next_f64() as f32) - 0.5).collect();
            let body: Vec<String> = v.iter().map(|x| format!("{x:.6}")).collect();
            script.push_str(&format!(
                "CREATE (:Doc:__DumpVertex__ {{__dump_id__: {i}, embedding: vecf32([{}])}});\n",
                body.join(", ")
            ));
            vectors.push(v);
        }
        (script, vectors)
    }

    #[test]
    fn above_threshold_builds_vamana_and_pq_files_with_acceptable_recall() {
        let dim = 16;
        let n = 400;
        let (script, vectors) = synthetic_dump(n, dim);
        let data_dir =
            std::env::temp_dir().join(format!("slater_build_vam_{}", std::process::id()));
        let _ = fs::remove_dir_all(&data_dir);

        let opts = BuildOptions {
            // A low threshold forces the Vamana path on this small synthetic set.
            ann_threshold: 50,
            vamana_r: 24,
            vamana_alpha: 1.2,
            pq_subspaces: 8,
            pq_bits: 8,
            ..Default::default()
        };
        let outcome = build(script.as_bytes(), "docs", &data_dir, &opts).unwrap();

        // The descriptor records Vamana mode with the build parameters.
        let manifest = Manifest::read_from_dir(&outcome.dir).unwrap();
        assert_eq!(manifest.vector_indexes.len(), 1);
        let desc = &manifest.vector_indexes[0];
        assert_eq!(desc.count, n as u64);
        let (medoid, pqm) = match desc.mode {
            AnnMode::Vamana {
                r,
                medoid,
                pq_subspaces,
                ..
            } => {
                assert_eq!(r, 24);
                (medoid, pq_subspaces)
            }
            AnnMode::BruteForce => panic!("expected Vamana mode above the threshold"),
        };
        assert_eq!(pqm, 8);

        // The two ANN files were written and are in the manifest inventory.
        let vam_path = outcome.dir.join("vector/Doc.embedding.vamana");
        let pq_path = outcome.dir.join("vector/Doc.embedding.pq");
        assert!(vam_path.exists() && pq_path.exists());
        assert!(manifest
            .files
            .iter()
            .any(|f| f.name == "vector/Doc.embedding.vamana"));
        assert!(manifest
            .files
            .iter()
            .any(|f| f.name == "vector/Doc.embedding.pq"));

        // Read the ANN files back and run the same beam search the server will,
        // checking recall@k against brute-force ground truth.
        let vam = VamanaReader::open_with_cipher(&vam_path, None).unwrap();
        let pq = PqReader::open_with_cipher(&pq_path, None).unwrap();
        let resident = pq.load_resident().unwrap();
        assert_eq!(vam.len(), n as u64);
        assert_eq!(resident.len(), n);

        let k = 10;
        let queries = 15;
        let mut recall_sum = 0.0f64;
        for q in 0..queries {
            let query = unit(&vectors[(q * 23) % n]);
            let adc = AdcTable::new(&resident.codebook, &query).unwrap();
            let hits = beam_search(
                medoid as u32,
                64,
                k,
                n,
                |i| adc.estimate(resident.codes_of(i as usize)),
                |i| {
                    let node = vam.node(i).unwrap();
                    Ok((node.vector, node.neighbours))
                },
                |v| cosine_distance(&query, v),
            )
            .unwrap();
            // Map hits back to dense node ids and compare with brute force over the
            // original (raw) vectors.
            let got: std::collections::HashSet<u64> = hits
                .iter()
                .map(|h| vam.node(h.index).unwrap().node_id)
                .collect();
            let mut truth: Vec<(f32, u64)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| (cosine_distance(&query, &unit(v)), i as u64))
                .collect();
            truth.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            let found = truth
                .iter()
                .take(k)
                .filter(|(_, id)| got.contains(id))
                .count();
            recall_sum += found as f64 / k as f64;
        }
        let recall = recall_sum / queries as f64;
        assert!(recall >= 0.8, "build→read recall@{k} was {recall:.3}");

        let _ = fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn below_threshold_stays_brute_force() {
        let dim = 8;
        let n = 30;
        let (script, _) = synthetic_dump(n, dim);
        let data_dir = std::env::temp_dir().join(format!("slater_build_bf_{}", std::process::id()));
        let _ = fs::remove_dir_all(&data_dir);

        // Default threshold (50k) ⇒ this 30-vector index stays brute-force.
        let outcome = build(
            script.as_bytes(),
            "docs",
            &data_dir,
            &BuildOptions::default(),
        )
        .unwrap();
        let manifest = Manifest::read_from_dir(&outcome.dir).unwrap();
        assert!(matches!(
            manifest.vector_indexes[0].mode,
            AnnMode::BruteForce
        ));
        // No ANN files written for a brute-force index.
        assert!(!outcome.dir.join("vector/Doc.embedding.vamana").exists());
        let _ = fs::remove_dir_all(&data_dir);
    }
}
