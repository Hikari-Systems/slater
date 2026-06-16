// SPDX-License-Identifier: Apache-2.0
//! Opening and validating an immutable graph generation.
//!
//! A generation is a content-hashed, append-only directory written by
//! `slater-build` (see `graph_format::manifest` and `docs/DECISIONS.md` D14).
//! This module is the reader's entry point: it resolves the `current` pointer,
//! parses the MANIFEST, **re-hashes every inventory file against the manifest and
//! refuses to serve on any mismatch** (the copy-completeness guard for a
//! publish that landed half a generation, e.g. an in-progress rsync onto remote
//! storage), opens every reader, and builds the
//! inverted label/relationship-type postings the executor needs for selective
//! scans (D11 — `slater-build` only emits the *forward* per-node label store).
//
// Many accessors below are consumed only from later M4 sub-steps (cache, parser,
// executor). Allow dead_code for now so the build stays warning-clean; the allow
// is removed once those callers land.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use graph_format::blockfile::BlockFileReader;
use graph_format::columns::PropsReader;
use graph_format::crypto::{self, BlockCipher};
use graph_format::ids::Generation as GenId;
use graph_format::integrity::hash_file;
use graph_format::isam::IsamReader;
use graph_format::manifest::AnnMode;
use graph_format::manifest::Manifest;
use graph_format::nodelabels::NodeLabelsReader;
use graph_format::postings::decode_endpoint_posting;
use graph_format::pq::{PqReader, ResidentPq};
use graph_format::topology::TopologyReader;
use graph_format::vamana::VamanaReader;
use graph_format::vectors::VectorStoreReader;
use graph_format::{FORMAT_VERSION, MAGIC};
use tracing::info;

/// An opened, validated graph generation. Immutable for its lifetime — a new
/// generation is a *new* `Generation` value, never an in-place mutation, so the
/// caches can key on the generation UUID and orphan stale entries on swap.
/// Which endpoint of a typed relationship a rel-type scan drives from:
/// `Source` for an outgoing first hop, `Target` for incoming, `Either` (the
/// union) for undirected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelEndpointSide {
    Source,
    Target,
    Either,
}

pub struct Generation {
    graph: String,
    uuid: GenId,
    dir: PathBuf,
    manifest: Manifest,

    node_props: PropsReader,
    node_labels: NodeLabelsReader,
    edge_props: PropsReader,
    topology: TopologyReader,
    /// Per-reltype endpoint postings (`reltype_src.post` / `reltype_tgt.post`),
    /// record index = reltype id. `None` ⇒ the generation predates the postings;
    /// the planner then simply never offers a relationship-type scan. Each holds
    /// only its sparse directory at open — records are read lazily per query.
    reltype_src_post: Option<BlockFileReader>,
    reltype_tgt_post: Option<BlockFileReader>,
    vectors: VectorStoreReader,
    /// Range (ISAM) indexes keyed by their MANIFEST `name` (= file stem under `range/`).
    range_indexes: HashMap<String, IsamReader>,
    /// Disk-native Vamana/PQ indexes (above the ANN threshold), keyed by
    /// `(label, property)`. Each holds its block reader, its position in
    /// `manifest.vector_indexes` (the cache ordinal), and its resident PQ codes.
    vamana_indexes: HashMap<(String, String), VamanaIndex>,

    /// Symbol-table name → id lookups (the inverse of the MANIFEST `Vec<String>`s).
    label_ids: HashMap<String, u32>,
    reltype_ids: HashMap<String, u32>,
    property_key_ids: HashMap<String, u32>,

    /// Per-label node counts, computed at open by a single scan (D11). We keep
    /// only the *counts* resident — not the full id postings — so the open-time
    /// footprint stays O(#labels) rather than O(#nodes). Label *scans* re-derive
    /// their ids on demand via [`Generation::collect_nodes_with_label`].
    label_counts: HashMap<u32, u64>,
    /// Per-relationship-type edge counts (same bounded-memory rationale). No caller
    /// ever enumerates the edge ids of a type, so only the counts are retained.
    reltype_counts: HashMap<u32, u64>,
}

/// One opened Vamana/PQ index. The medoid + R/alpha/PQ params live in the MANIFEST
/// descriptor (`manifest.vector_indexes[ord].mode`); here we hold the on-disk block
/// reader and the resident PQ codes (loaded once at open — the navigation set).
pub struct VamanaIndex {
    /// Position in `manifest.vector_indexes` — the vector-index cache ordinal.
    pub ord: u32,
    pub reader: VamanaReader,
    pub pq: Arc<ResidentPq>,
}

impl Generation {
    /// Resolve `<data_dir>/<graph>/current`, open that generation, validate it,
    /// and build its in-memory indexes. Fails fast (and the caller should exit
    /// non-zero) on a missing pointer, an unknown format version, or an integrity
    /// mismatch — the latter being a generation half-copied onto the data dir
    /// (which may be remote/network storage).
    ///
    /// Opens a plaintext generation; an encrypted one is refused (no key). Use
    /// [`Generation::open_with_key`] to supply the at-rest master key.
    pub fn open(data_dir: impl AsRef<Path>, graph: &str) -> Result<Self> {
        Self::open_with_key(data_dir, graph, None)
    }

    /// As [`Generation::open`], but supplying the at-rest master key (raw bytes)
    /// used to derive this generation's block cipher. The key is required iff the
    /// MANIFEST carries an `encryption` header; an encrypted generation opened
    /// without a key is refused with a clear error (never garbage).
    pub fn open_with_key(
        data_dir: impl AsRef<Path>,
        graph: &str,
        master_key: Option<&[u8]>,
    ) -> Result<Self> {
        let graph_dir = data_dir.as_ref().join(graph);
        let uuid = GenId(read_current(&graph_dir)?);
        let dir = graph_dir.join(uuid.to_string());

        let manifest = Manifest::read_from_dir(&dir)
            .with_context(|| format!("read MANIFEST for generation {} of graph {graph}", uuid))?;

        // Sniff the magic and format version before trusting anything else.
        if manifest.magic.as_bytes() != MAGIC {
            bail!(
                "generation {} of graph {graph} has unexpected magic {:?}",
                uuid,
                manifest.magic
            );
        }
        if manifest.format_version != FORMAT_VERSION {
            bail!(
                "generation {} of graph {graph} is format version {} but this build understands {FORMAT_VERSION}",
                uuid,
                manifest.format_version
            );
        }
        if manifest.graph != graph {
            bail!(
                "generation {} claims graph {:?} but lives under {:?}",
                uuid,
                manifest.graph,
                graph
            );
        }

        // Manifest authentication: when a master key is configured and the
        // manifest carries a MAC, verify it before trusting any other field. This
        // authenticates content_hash, the file inventory, the encryption header,
        // and the ACL stamp — so an attacker without the key cannot forge a
        // manifest that opens. A plaintext image carries no MAC and is guarded
        // only by the copy-completeness hash below (see THREAT_MODEL.md). The
        // "require a MAC when absent" downgrade policy lives in the server, which
        // holds the config flags.
        if let Some(key) = master_key {
            if manifest.mac.is_some() {
                manifest.verify_mac(key).with_context(|| {
                    format!("verify MANIFEST MAC for generation {uuid} of graph {graph}")
                })?;
            }
        }

        // Copy-completeness guard: re-hash every inventory file from disk and
        // refuse on the first mismatch, then confirm the manifest's own content
        // hash is self-consistent with that inventory.
        verify_against_disk(&dir, &manifest)?;

        // Derive the per-generation block cipher from the runtime master key and
        // the MANIFEST salt. The key is required iff the generation is encrypted;
        // a plaintext generation ignores any key and opens as before.
        let cipher = derive_cipher(&manifest, master_key, graph, &uuid)?;

        // Open every reader. Each only reads its footer/sparse index at open
        // (block bytes stay lazy via pread — D16), so this is cheap and keeps
        // resident memory to the directories alone. Each reader is handed the
        // cipher so a cache-miss block read decrypts before decompressing (D28).
        let node_props = PropsReader::open_with_cipher(dir.join("node_props.blk"), cipher.clone())?;
        let node_labels =
            NodeLabelsReader::open_with_cipher(dir.join("node_labels.blk"), cipher.clone())?;
        let edge_props = PropsReader::open_with_cipher(dir.join("edge_props.blk"), cipher.clone())?;
        let topology =
            TopologyReader::open_with_cipher(dir.join("topology.csr.blk"), cipher.clone())?;
        // Endpoint postings (format v2+). Gate on file existence so a hand-built
        // fixture without them still opens; the format-version check already
        // fences real generations.
        let open_post = |name: &str| -> Result<Option<BlockFileReader>> {
            let path = dir.join(name);
            if path.exists() {
                Ok(Some(BlockFileReader::open_with_cipher(
                    path,
                    cipher.clone(),
                )?))
            } else {
                Ok(None)
            }
        };
        let reltype_src_post = open_post("reltype_src.post")?;
        let reltype_tgt_post = open_post("reltype_tgt.post")?;
        let vectors =
            VectorStoreReader::open_with_cipher(dir.join("vectors.f32.blk"), cipher.clone())?;

        let mut range_indexes = HashMap::new();
        for ri in &manifest.range_indexes {
            let path = dir.join("range").join(format!("{}.isam", ri.name));
            let reader = IsamReader::open_with_cipher(&path, cipher.clone())
                .with_context(|| format!("open range index {}", path.display()))?;
            range_indexes.insert(ri.name.clone(), reader);
        }

        // Open the disk-native Vamana/PQ indexes (above the ANN threshold). Each
        // reads only its block-file footer + PQ codebook header at open; the
        // resident PQ codes are loaded once here (the navigation set the beam search
        // holds resident — never a full in-memory graph). Below-threshold indexes
        // stay brute-force over `vectors.f32.blk` and open nothing extra.
        let mut vamana_indexes = HashMap::new();
        for (ord, vi) in manifest.vector_indexes.iter().enumerate() {
            if !matches!(vi.mode, AnnMode::Vamana { .. }) {
                continue;
            }
            let stem = format!("vector/{}.{}", vi.label, vi.property);
            let reader =
                VamanaReader::open_with_cipher(dir.join(format!("{stem}.vamana")), cipher.clone())
                    .with_context(|| format!("open Vamana store {stem}.vamana"))?;
            let pq = PqReader::open_with_cipher(dir.join(format!("{stem}.pq")), cipher.clone())
                .with_context(|| format!("open PQ store {stem}.pq"))?;
            let resident = Arc::new(
                pq.load_resident()
                    .with_context(|| format!("load resident PQ codes for {stem}.pq"))?,
            );
            vamana_indexes.insert(
                (vi.label.clone(), vi.property.clone()),
                VamanaIndex {
                    ord: ord as u32,
                    reader,
                    pq: resident,
                },
            );
        }

        let label_ids = invert_symbols(&manifest.labels);
        let reltype_ids = invert_symbols(&manifest.reltypes);
        let property_key_ids = invert_symbols(&manifest.property_keys);

        let label_counts = build_label_counts(&node_labels)?;
        let reltype_counts = build_reltype_counts(&topology)?;

        info!(
            graph,
            generation = %uuid,
            nodes = manifest.node_count,
            edges = manifest.edge_count,
            labels = manifest.labels.len(),
            reltypes = manifest.reltypes.len(),
            range_indexes = manifest.range_indexes.len(),
            vector_indexes = manifest.vector_indexes.len(),
            "opened generation"
        );

        Ok(Self {
            graph: graph.to_string(),
            uuid,
            dir,
            manifest,
            node_props,
            node_labels,
            edge_props,
            topology,
            reltype_src_post,
            reltype_tgt_post,
            vectors,
            range_indexes,
            vamana_indexes,
            label_ids,
            reltype_ids,
            property_key_ids,
            label_counts,
            reltype_counts,
        })
    }

    /// Read just the `current` pointer's generation UUID without opening (or
    /// validating) the generation. The generation guard calls this every poll to
    /// detect a published swap cheaply — the data dir may be remote/network
    /// storage (e.g. NFS), so we poll
    /// this small text file rather than watch it for events (D14/D16).
    pub fn current_uuid(data_dir: impl AsRef<Path>, graph: &str) -> Result<uuid::Uuid> {
        read_current(&data_dir.as_ref().join(graph))
    }

    // ── Identity / metadata ────────────────────────────────────────────────

    pub fn graph(&self) -> &str {
        &self.graph
    }
    pub fn uuid(&self) -> GenId {
        self.uuid
    }
    pub fn dir(&self) -> &Path {
        &self.dir
    }
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub fn node_count(&self) -> u64 {
        self.manifest.node_count
    }
    pub fn edge_count(&self) -> u64 {
        self.manifest.edge_count
    }

    // ── Symbol-table lookups ───────────────────────────────────────────────

    pub fn label_id(&self, name: &str) -> Option<u32> {
        self.label_ids.get(name).copied()
    }
    pub fn reltype_id(&self, name: &str) -> Option<u32> {
        self.reltype_ids.get(name).copied()
    }
    pub fn property_key_id(&self, name: &str) -> Option<u32> {
        self.property_key_ids.get(name).copied()
    }
    pub fn label_name(&self, id: u32) -> Option<&str> {
        self.manifest.labels.get(id as usize).map(String::as_str)
    }
    pub fn reltype_name(&self, id: u32) -> Option<&str> {
        self.manifest.reltypes.get(id as usize).map(String::as_str)
    }
    pub fn property_key_name(&self, id: u32) -> Option<&str> {
        self.manifest
            .property_keys
            .get(id as usize)
            .map(String::as_str)
    }

    // ── Readers ─────────────────────────────────────────────────────────────

    pub fn node_props(&self) -> &PropsReader {
        &self.node_props
    }
    pub fn node_labels(&self) -> &NodeLabelsReader {
        &self.node_labels
    }
    pub fn edge_props(&self) -> &PropsReader {
        &self.edge_props
    }
    pub fn topology(&self) -> &TopologyReader {
        &self.topology
    }
    pub fn vectors(&self) -> &VectorStoreReader {
        &self.vectors
    }
    pub fn range_index(&self, name: &str) -> Option<&IsamReader> {
        self.range_indexes.get(name)
    }

    /// The opened Vamana/PQ index over `(label, property)`, if one exists (i.e. the
    /// index was built above the ANN threshold).
    pub fn vamana_index(&self, label: &str, property: &str) -> Option<&VamanaIndex> {
        self.vamana_indexes
            .get(&(label.to_string(), property.to_string()))
    }

    /// Every opened Vamana/PQ index — used to pin resident PQ codes into the
    /// vector-index cache pool at server startup.
    pub fn vamana_indexes(&self) -> impl Iterator<Item = &VamanaIndex> {
        self.vamana_indexes.values()
    }

    // ── Inverted counts + on-demand label scan ─────────────────────────────

    /// Number of nodes carrying `label_id` (precomputed at open; O(1)).
    pub fn label_node_count(&self, label_id: u32) -> u64 {
        self.label_counts.get(&label_id).copied().unwrap_or(0)
    }

    /// Number of edges of relationship type `reltype_id` (precomputed at open; O(1)).
    pub fn reltype_edge_count(&self, reltype_id: u32) -> u64 {
        self.reltype_counts.get(&reltype_id).copied().unwrap_or(0)
    }

    /// True when this generation carries the per-reltype endpoint postings (format
    /// v2+), so a relationship-type scan can drive a typed first hop.
    pub fn has_reltype_postings(&self) -> bool {
        self.reltype_src_post.is_some() && self.reltype_tgt_post.is_some()
    }

    /// Distinct **source** node count for `reltype_id` — nodes with an outgoing
    /// edge of that type (O(1), from the manifest). 0 if absent/unknown.
    pub fn reltype_source_count(&self, reltype_id: u32) -> u64 {
        self.manifest
            .reltype_source_counts
            .get(reltype_id as usize)
            .copied()
            .unwrap_or(0)
    }

    /// Distinct **target** node count for `reltype_id` — nodes with an incoming
    /// edge of that type (O(1), from the manifest). 0 if absent/unknown.
    pub fn reltype_target_count(&self, reltype_id: u32) -> u64 {
        self.manifest
            .reltype_target_counts
            .get(reltype_id as usize)
            .copied()
            .unwrap_or(0)
    }

    /// Ascending distinct node ids that carry an edge of any reltype in
    /// `reltype_ids` on the requested `side` (union over the types; for
    /// [`RelEndpointSide::Either`], also the union of source and target). One
    /// record read per (reltype, side); the single-reltype single-side case
    /// returns the decoded posting directly. Errors if the postings are absent —
    /// callers must gate on [`Self::has_reltype_postings`].
    pub fn collect_endpoint_nodes_for_reltypes(
        &self,
        reltype_ids: &[u32],
        side: RelEndpointSide,
    ) -> Result<Vec<u64>> {
        let (Some(src), Some(tgt)) = (&self.reltype_src_post, &self.reltype_tgt_post) else {
            bail!("generation has no reltype endpoint postings");
        };
        let read = |reader: &BlockFileReader, t: u32| -> Result<Vec<u64>> {
            if (t as u64) < reader.total_records() {
                decode_endpoint_posting(&reader.read_record_global(t as u64)?)
            } else {
                Ok(Vec::new())
            }
        };
        // Single reltype on a single side: the posting is already ascending+distinct.
        if reltype_ids.len() == 1 {
            match side {
                RelEndpointSide::Source => return read(src, reltype_ids[0]),
                RelEndpointSide::Target => return read(tgt, reltype_ids[0]),
                RelEndpointSide::Either => {}
            }
        }
        let mut set = std::collections::BTreeSet::new();
        for &t in reltype_ids {
            if matches!(side, RelEndpointSide::Source | RelEndpointSide::Either) {
                set.extend(read(src, t)?);
            }
            if matches!(side, RelEndpointSide::Target | RelEndpointSide::Either) {
                set.extend(read(tgt, t)?);
            }
        }
        Ok(set.into_iter().collect())
    }

    /// Dense node ids carrying `label_id`, ascending — re-derived on demand by a
    /// single pass over the node-label column. We deliberately do **not** keep a
    /// resident id posting (it would be O(#nodes) per label); the open-time
    /// footprint stays bounded and label scans pay the scan only when they run.
    pub fn collect_nodes_with_label(&self, label_id: u32) -> Result<Vec<u64>> {
        let mut ids = Vec::new();
        self.node_labels.inner().for_each_record(|node_id, rec| {
            if graph_format::nodelabels::decode_labels(rec)?.contains(&label_id) {
                ids.push(node_id);
            }
            Ok(())
        })?;
        Ok(ids)
    }
}

/// Read and parse `<graph_dir>/current` into a generation UUID.
fn read_current(graph_dir: &Path) -> Result<uuid::Uuid> {
    let path = graph_dir.join("current");
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("read current pointer {}", path.display()))?;
    let trimmed = text.trim();
    uuid::Uuid::parse_str(trimmed)
        .with_context(|| format!("parse generation uuid {trimmed:?} from {}", path.display()))
}

/// Derive this generation's block cipher from the runtime master key and the
/// MANIFEST encryption header. Returns `None` for a plaintext generation; a clear
/// error (not a panic) when an encrypted generation is opened without a key, or
/// when the header names an AEAD/KDF this build does not implement.
fn derive_cipher(
    manifest: &Manifest,
    master_key: Option<&[u8]>,
    graph: &str,
    uuid: &GenId,
) -> Result<Option<Arc<BlockCipher>>> {
    let Some(header) = &manifest.encryption else {
        return Ok(None);
    };
    if header.aead != crypto::AEAD_NAME {
        bail!(
            "generation {uuid} of graph {graph} uses AEAD {:?}, which this build does not implement",
            header.aead
        );
    }
    if header.kdf != crypto::KDF_NAME {
        bail!(
            "generation {uuid} of graph {graph} uses KDF {:?}, which this build does not implement",
            header.kdf
        );
    }
    let key = master_key.ok_or_else(|| {
        anyhow::anyhow!(
            "generation {uuid} of graph {graph} is encrypted at rest but no key was supplied \
             (set config.encryption.keyEnv or keyFile)"
        )
    })?;
    let salt = crypto::hex_decode(&header.salt_hex)
        .with_context(|| format!("decode encryption salt for generation {uuid}"))?;
    Ok(Some(Arc::new(BlockCipher::from_master(key, &salt))))
}

/// Re-hash every file in the manifest inventory from disk and compare to the
/// declared per-file hash, then confirm the overall content hash is consistent.
fn verify_against_disk(dir: &Path, manifest: &Manifest) -> Result<()> {
    for fe in &manifest.files {
        let path = dir.join(&fe.name);
        let computed = hash_file(&path)
            .with_context(|| format!("re-hash generation file {}", path.display()))?;
        if computed != fe.blake3 {
            bail!(
                "generation file {} failed its integrity check \
                 (manifest {}, on-disk {}) — refusing to serve an incomplete copy",
                fe.name,
                fe.blake3,
                computed
            );
        }
    }
    // Every file matched its declared hash; the manifest's own content_hash must
    // therefore equal the hash over the (name, hash) inventory.
    manifest
        .verify_content_hash()
        .context("manifest content hash is inconsistent with its file inventory")?;
    Ok(())
}

/// Build `name → id` from a MANIFEST symbol-table vector (id = index).
fn invert_symbols(symbols: &[String]) -> HashMap<String, u32> {
    symbols
        .iter()
        .enumerate()
        .map(|(i, s)| (s.clone(), i as u32))
        .collect()
}

/// Build the inverted label postings (`label_id → ascending node ids`) by a
/// single forward pass over the per-node label store.
///
/// Scans block-by-block (each block decompressed once) rather than per-node:
/// `read_record_global` re-decompresses a node's whole block on every call, so a
/// per-node loop does O(records-per-block) redundant zstd work per block — which
/// dominates open time on a large store (e.g. a 340k-node graph). Node ids arrive
/// ascending, so the postings stay sorted without an extra pass.
fn build_label_counts(node_labels: &NodeLabelsReader) -> Result<HashMap<u32, u64>> {
    let mut counts: HashMap<u32, u64> = HashMap::new();
    node_labels.inner().for_each_record(|_node_id, rec| {
        for label_id in graph_format::nodelabels::decode_labels(rec)? {
            *counts.entry(label_id).or_default() += 1;
        }
        Ok(())
    })?;
    Ok(counts)
}

/// Count edges per relationship type from the forward CSR. Each edge appears
/// exactly once in the outgoing adjacency, so a single pass over the outgoing
/// records covers every edge once. We keep only the counts — never the edge-id
/// lists — because no query path enumerates the edges of a type; the lists would
/// be O(#edges) resident (≈6 GB on full Wikidata) for no benefit.
///
/// The CSR block file stores outgoing records (global ids `0..node_count`)
/// followed by incoming records (`node_count..2*node_count`); we scan it
/// block-by-block (decompressing each block once) and skip the incoming half so
/// each edge is counted exactly once.
fn build_reltype_counts(topology: &TopologyReader) -> Result<HashMap<u32, u64>> {
    let mut counts: HashMap<u32, u64> = HashMap::new();
    let node_count = topology.node_count();
    topology.inner().for_each_record(|global, rec| {
        if global >= node_count {
            return Ok(()); // incoming half — already counted via the outgoing record
        }
        for adj in graph_format::topology::decode_adj(rec)? {
            *counts.entry(adj.reltype).or_default() += 1;
        }
        Ok(())
    })?;
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::columns::PropsWriter;
    use graph_format::crypto::{self, BlockCipher};
    use graph_format::ids::{EdgeId, NodeId, Value};
    use graph_format::isam::write_isam_with_cipher;
    use graph_format::manifest::{
        AnnMode, EncryptionHeader, EntityKind, FileEntry, Metric, RangeIndexDesc, VectorIndexDesc,
    };
    use graph_format::nodelabels::NodeLabelsWriter;
    use graph_format::topology::{write_csr_with_cipher, Edge};
    use graph_format::vectors::VectorStoreWriter;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    const BLOCK: usize = 4096;
    const LEVEL: i32 = 3;

    /// Write a small, representative generation directly with the graph-format
    /// writers (no dependency on the `slater-build` binary), publish a `current`
    /// pointer, and return `(data_dir, graph, uuid)`.
    ///
    /// Shape: labels Person(0)/Company(1); reltypes KNOWS(0)/WORKS_AT(1);
    /// property keys name(0)/age(1)/embedding(2). Nodes: 0 Alice:Person{name,age},
    /// 1 Bob:Person{name}, 2 Acme:Company{name}. Edges: 0 (0)-[:KNOWS]->(1),
    /// 1 (0)-[:WORKS_AT]->(2). One vector index on (Person, embedding) holding
    /// node 0's embedding; one range index on (Person, name).
    fn write_fixture(tag: &str) -> (PathBuf, String, uuid::Uuid) {
        write_fixture_keyed(tag, None)
    }

    /// As [`write_fixture`], but optionally AEAD-encrypting every data file under a
    /// per-generation cipher derived from `master_key` and recording the salt in
    /// the MANIFEST `encryption` header. `None` writes the plaintext fixture.
    fn write_fixture_keyed(tag: &str, master_key: Option<&[u8]>) -> (PathBuf, String, uuid::Uuid) {
        let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0001);
        let graph = "people".to_string();
        // Each test gets its own root (tests run in parallel and tear their dirs
        // down), so the generation UUID can be the same fixed value throughout.
        let root = std::env::temp_dir().join(format!("slater_gen_{}_{tag}", std::process::id()));
        let dir = root.join(&graph).join(uuid.to_string());
        std::fs::create_dir_all(dir.join("range")).unwrap();

        // Derive the block cipher + MANIFEST header when a key is supplied.
        let (cipher, encryption): (Option<Arc<BlockCipher>>, Option<EncryptionHeader>) =
            match master_key {
                Some(key) => {
                    let salt = [0x42u8; crypto::SALT_LEN];
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

        // node_props.blk — embedding is routed to the vector store (D12), so it
        // is absent from node 0's property map.
        let mut np = PropsWriter::create_with_cipher(
            dir.join("node_props.blk"),
            BLOCK,
            LEVEL,
            cipher.clone(),
        )
        .unwrap();
        np.append(&[(0, Value::Str("Alice".into())), (1, Value::Int(30))])
            .unwrap();
        np.append(&[(0, Value::Str("Bob".into()))]).unwrap();
        np.append(&[(0, Value::Str("Acme".into()))]).unwrap();
        np.finish().unwrap();

        // node_labels.blk
        let mut nl = NodeLabelsWriter::create_with_cipher(
            dir.join("node_labels.blk"),
            BLOCK,
            LEVEL,
            cipher.clone(),
        )
        .unwrap();
        nl.append(&[0]).unwrap(); // Person
        nl.append(&[0]).unwrap(); // Person
        nl.append(&[1]).unwrap(); // Company
        nl.finish().unwrap();

        // edge_props.blk — KNOWS edge has a property, WORKS_AT is bare.
        let mut ep = PropsWriter::create_with_cipher(
            dir.join("edge_props.blk"),
            BLOCK,
            LEVEL,
            cipher.clone(),
        )
        .unwrap();
        ep.append(&[(1, Value::Int(2020))]).unwrap(); // since: 2020
        ep.append(&[]).unwrap();
        ep.finish().unwrap();

        // topology.csr.blk
        let edges = vec![
            Edge {
                src: NodeId(0),
                dst: NodeId(1),
                reltype: 0,
                edge: EdgeId(0),
            },
            Edge {
                src: NodeId(0),
                dst: NodeId(2),
                reltype: 1,
                edge: EdgeId(1),
            },
        ];
        write_csr_with_cipher(
            dir.join("topology.csr.blk"),
            3,
            &edges,
            BLOCK,
            LEVEL,
            cipher.clone(),
        )
        .unwrap();

        // reltype_src.post / reltype_tgt.post — KNOWS(0): src{0} tgt{1};
        // WORKS_AT(1): src{0} tgt{2}.
        let (reltype_source_counts, reltype_target_counts) =
            graph_format::postings::write_reltype_endpoint_postings(
                dir.join("reltype_src.post"),
                dir.join("reltype_tgt.post"),
                2,
                &edges,
                BLOCK,
                LEVEL,
                cipher.clone(),
            )
            .unwrap();

        // vectors.f32.blk — one vector for node 0 under the (Person, embedding) index.
        let mut vw = VectorStoreWriter::create_with_cipher(
            dir.join("vectors.f32.blk"),
            BLOCK,
            LEVEL,
            cipher.clone(),
        )
        .unwrap();
        vw.append(0, &[0.1, 0.2, 0.3]).unwrap();
        vw.finish().unwrap();

        // range/node_Person_name.isam
        let range_name = "node_Person_name".to_string();
        write_isam_with_cipher(
            dir.join("range").join(format!("{range_name}.isam")),
            vec![
                (Value::Str("Alice".into()), 0),
                (Value::Str("Bob".into()), 1),
            ],
            BLOCK,
            LEVEL,
            cipher.clone(),
        )
        .unwrap();

        // Hash the inventory and assemble the manifest.
        let mut block_sizes = BTreeMap::new();
        let mut files = Vec::new();
        let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
            let path = dir.join(name);
            let bytes = std::fs::metadata(&path).unwrap().len();
            files.push(FileEntry {
                name: name.to_string(),
                bytes,
                blake3: hash_file(&path).unwrap(),
            });
            bs.insert(name.to_string(), BLOCK as u32);
        };
        for name in [
            "node_props.blk",
            "node_labels.blk",
            "edge_props.blk",
            "topology.csr.blk",
            "reltype_src.post",
            "reltype_tgt.post",
            "vectors.f32.blk",
            "range/node_Person_name.isam",
        ] {
            add(name, &mut files, &mut block_sizes);
        }
        files.sort_by(|a, b| a.name.cmp(&b.name));
        let inv: Vec<(String, String)> = files
            .iter()
            .map(|f| (f.name.clone(), f.blake3.clone()))
            .collect();
        let content_hash = graph_format::integrity::content_hash(&inv);

        let manifest = Manifest {
            magic: "SLATER01".into(),
            format_version: FORMAT_VERSION,
            build_uuid: GenId(uuid),
            graph: graph.clone(),
            created_unix: 1_700_000_000,
            content_hash,
            block_sizes,
            codec: "zstd".into(),
            zstd_level: LEVEL,
            encryption,
            node_count: 3,
            edge_count: 2,
            labels: vec!["Person".into(), "Company".into()],
            reltypes: vec!["KNOWS".into(), "WORKS_AT".into()],
            property_keys: vec!["name".into(), "age".into(), "embedding".into()],
            range_indexes: vec![RangeIndexDesc {
                name: range_name,
                entity: EntityKind::Node,
                label_or_type: "Person".into(),
                property: "name".into(),
            }],
            vector_indexes: vec![VectorIndexDesc {
                label: "Person".into(),
                property: "embedding".into(),
                dim: 3,
                metric: Metric::Cosine,
                count: 1,
                first_record: 0,
                mode: AnnMode::BruteForce,
            }],
            reltype_source_counts,
            reltype_target_counts,
            acl_blake3: None,
            mac: None,
            files,
        };
        manifest.write_to_dir(&dir).unwrap();

        // Publish the current pointer.
        std::fs::write(
            root.join(&graph).join("current"),
            format!("{}\n", uuid.hyphenated()),
        )
        .unwrap();

        (root, graph, uuid)
    }

    #[test]
    fn open_validates_and_exposes_readers() {
        let (root, graph, uuid) = write_fixture("open_validates_and_exposes_readers");
        let gen = Generation::open(&root, &graph).unwrap();

        assert_eq!(gen.uuid(), GenId(uuid));
        assert_eq!(gen.graph(), "people");
        assert_eq!(gen.node_count(), 3);
        assert_eq!(gen.edge_count(), 2);

        // Properties materialise per entity.
        let alice = gen.node_props().props(0).unwrap();
        assert!(alice.contains(&(0, Value::Str("Alice".into()))));
        assert!(alice.contains(&(1, Value::Int(30))));

        // Forward labels.
        assert_eq!(gen.node_labels().labels(2).unwrap(), vec![1]);

        // Topology: node 0 has two outgoing edges, of distinct types.
        let out = gen.topology().outgoing(NodeId(0)).unwrap();
        assert_eq!(out.len(), 2);

        // Vector store group for the single index.
        let g = gen.vectors().group(0, 1).unwrap();
        assert_eq!(g[0].node_id, 0);
        assert_eq!(g[0].vector, vec![0.1, 0.2, 0.3]);

        // Range index lookup.
        let hits = gen
            .range_index("node_Person_name")
            .unwrap()
            .lookup_eq(&Value::Str("Bob".into()))
            .unwrap();
        assert_eq!(hits, vec![1]);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn reltype_endpoint_postings_resolve() {
        let (root, graph, _) = write_fixture("reltype_endpoint_postings_resolve");
        let gen = Generation::open(&root, &graph).unwrap();
        assert!(gen.has_reltype_postings());
        // KNOWS(0): src {0}, tgt {1}; WORKS_AT(1): src {0}, tgt {2}.
        assert_eq!(gen.reltype_source_count(0), 1);
        assert_eq!(gen.reltype_target_count(0), 1);
        assert_eq!(
            gen.collect_endpoint_nodes_for_reltypes(&[0], RelEndpointSide::Source)
                .unwrap(),
            vec![0]
        );
        assert_eq!(
            gen.collect_endpoint_nodes_for_reltypes(&[0], RelEndpointSide::Target)
                .unwrap(),
            vec![1]
        );
        // Either over both types: sources {0} ∪ targets {1,2} = {0,1,2}.
        assert_eq!(
            gen.collect_endpoint_nodes_for_reltypes(&[0, 1], RelEndpointSide::Either)
                .unwrap(),
            vec![0, 1, 2]
        );
        // Union of sources across both types is just {0}.
        assert_eq!(
            gen.collect_endpoint_nodes_for_reltypes(&[0, 1], RelEndpointSide::Source)
                .unwrap(),
            vec![0]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn symbol_tables_invert() {
        let (root, graph, _) = write_fixture("symbol_tables_invert");
        let gen = Generation::open(&root, &graph).unwrap();
        assert_eq!(gen.label_id("Person"), Some(0));
        assert_eq!(gen.label_id("Company"), Some(1));
        assert_eq!(gen.label_id("Nope"), None);
        assert_eq!(gen.reltype_id("WORKS_AT"), Some(1));
        assert_eq!(gen.property_key_id("embedding"), Some(2));
        assert_eq!(gen.label_name(0), Some("Person"));
        assert_eq!(gen.reltype_name(0), Some("KNOWS"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn label_counts_and_on_demand_scan() {
        let (root, graph, _) = write_fixture("label_counts_and_on_demand_scan");
        let gen = Generation::open(&root, &graph).unwrap();

        let person = gen.label_id("Person").unwrap();
        let company = gen.label_id("Company").unwrap();
        // Counts are resident; id lists are re-derived on demand (no resident posting).
        assert_eq!(gen.label_node_count(person), 2);
        assert_eq!(gen.label_node_count(company), 1);
        assert_eq!(gen.collect_nodes_with_label(person).unwrap(), &[0, 1]);
        assert_eq!(gen.collect_nodes_with_label(company).unwrap(), &[2]);

        let knows = gen.reltype_id("KNOWS").unwrap();
        let works_at = gen.reltype_id("WORKS_AT").unwrap();
        assert_eq!(gen.reltype_edge_count(knows), 1);
        assert_eq!(gen.reltype_edge_count(works_at), 1);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn rejects_content_hash_mismatch() {
        let (root, graph, uuid) = write_fixture("rejects_content_hash_mismatch");
        // Corrupt a data file *after* the manifest was written — exactly the
        // half-copied-generation failure mode.
        let victim = root
            .join(&graph)
            .join(uuid.to_string())
            .join("node_props.blk");
        let mut bytes = std::fs::read(&victim).unwrap();
        bytes.push(0xFF);
        std::fs::write(&victim, bytes).unwrap();

        let err = Generation::open(&root, &graph).err().unwrap();
        assert!(
            err.to_string().contains("integrity check")
                || err.chain().any(|e| e.to_string().contains("integrity")),
            "unexpected error: {err:#}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn rejects_unknown_format_version() {
        let (root, graph, uuid) = write_fixture("rejects_unknown_format_version");
        // Bump the format version in the manifest without the reader understanding it.
        let dir = root.join(&graph).join(uuid.to_string());
        let mut manifest = Manifest::read_from_dir(&dir).unwrap();
        manifest.format_version = FORMAT_VERSION + 1;
        // Re-publish; content hash is unaffected (it covers data files, not the
        // manifest header), so this isolates the version check.
        manifest.write_to_dir(&dir).unwrap();

        let err = Generation::open(&root, &graph).err().unwrap();
        assert!(
            err.to_string().contains("format version"),
            "unexpected error: {err:#}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn rejects_missing_current() {
        let root = std::env::temp_dir().join(format!("slater_gen_missing_{}", std::process::id()));
        std::fs::create_dir_all(root.join("ghost")).unwrap();
        assert!(Generation::open(&root, "ghost").is_err());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn encrypted_generation_opens_with_the_right_key() {
        let key = b"at-rest-master-key";
        let (root, graph, uuid) = write_fixture_keyed("enc_open", Some(key));

        let gen = Generation::open_with_key(&root, &graph, Some(key)).unwrap();
        assert_eq!(gen.uuid(), GenId(uuid));
        assert_eq!(gen.node_count(), 3);

        // Every store decrypts transparently: props, labels, topology, vectors,
        // and the range index all read back exactly as the plaintext fixture.
        let alice = gen.node_props().props(0).unwrap();
        assert!(alice.contains(&(0, Value::Str("Alice".into()))));
        assert!(alice.contains(&(1, Value::Int(30))));
        assert_eq!(gen.node_labels().labels(2).unwrap(), vec![1]);
        assert_eq!(gen.topology().outgoing(NodeId(0)).unwrap().len(), 2);
        let g = gen.vectors().group(0, 1).unwrap();
        assert_eq!(g[0].vector, vec![0.1, 0.2, 0.3]);
        let hits = gen
            .range_index("node_Person_name")
            .unwrap()
            .lookup_eq(&Value::Str("Bob".into()))
            .unwrap();
        assert_eq!(hits, vec![1]);

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn encrypted_generation_refuses_absent_and_wrong_key() {
        let key = b"at-rest-master-key";
        let (root, graph, _) = write_fixture_keyed("enc_refuse", Some(key));

        // Absent key: a clear error naming the generation, not a panic.
        let err = Generation::open(&root, &graph).err().unwrap();
        assert!(
            err.to_string().contains("encrypted at rest")
                || err.chain().any(|e| e.to_string().contains("encrypted")),
            "unexpected error: {err:#}"
        );

        // Wrong key: refused while opening a store (the AEAD tag fails). The
        // sealed ISAM top-level / a block read surfaces a clean error.
        let err = Generation::open_with_key(&root, &graph, Some(b"wrong-key"))
            .err()
            .unwrap();
        assert!(
            err.chain().any(|e| e.to_string().contains("wrong key")),
            "unexpected error: {err:#}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn plaintext_generation_opens_even_with_a_key_configured() {
        // Encryption is optional: a plaintext generation must keep opening, with
        // or without a runtime key present (so M2–M5 fixtures keep working).
        let (root, graph, _) = write_fixture("plain_with_key");
        assert!(Generation::open_with_key(&root, &graph, Some(b"ignored")).is_ok());
        assert!(Generation::open(&root, &graph).is_ok());
        let _ = std::fs::remove_dir_all(&root);
    }
}
