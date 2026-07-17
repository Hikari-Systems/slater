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
use graph_format::histogram::decode_histogram;
use graph_format::ids::Generation as GenId;
use graph_format::ids::Value;
use graph_format::isam::IsamReader;
use graph_format::manifest::AnnMode;
use graph_format::manifest::Manifest;
use graph_format::manifest::VectorIndexDesc;
use graph_format::nodelabels::NodeLabelsReader;
use graph_format::postings::{
    decode_endpoint_posting, endpoint_posting_cursor, EndpointPostingIter,
};
use graph_format::pq::{PqReader, ResidentPq};
use graph_format::store::fs::FsObjectStore;
use graph_format::store::{join_key, FileIntegrity, ObjectStore};
use graph_format::topology::TopologyReader;
use graph_format::vamana::VamanaReader;
use graph_format::vectors::VectorStoreReader;
use graph_format::{FORMAT_VERSION, MAGIC};
use rayon::prelude::*;
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
    /// The **set** uuid (== `<graph>/current`) — this served image's identity, used
    /// as the block-cache / result-cache scope so a swap orphans stale entries. In a
    /// Phase-1 singleton it equals `base_uuid`; a later flush gives a set a fresh uuid
    /// over the same base.
    uuid: GenId,
    /// The **base generation** uuid — the directory `<graph>/<base_uuid>/` whose
    /// `.blk` files this image reads. Distinct from `uuid` once a set stacks segments
    /// over a shared base.
    base_uuid: GenId,
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
    /// Per-(label, property) value→count histograms (`prop_hist.blk`), keyed by the
    /// range-index `name` they derive from. Tiny and few, so decoded resident at
    /// open; the grouped-index fast path reads them in place of an ISAM walk. Empty
    /// ⇒ no precompute (the fast path falls back to `distinct_key_counts`).
    prop_histograms: HashMap<String, Vec<(Value, u64)>>,
    /// Hub-degree sidecar (`hub_degrees.blk`): ascending-by-id `(node_id, degree)` for
    /// every core node whose out/in degree is at or above [`Self::hub_degree_floor`], so
    /// a traversal can decide a node is a hub with an O(log n) binary search and no
    /// adjacency read. Empty + `hub_degree_floor == None` ⇒ no sidecar (older generation);
    /// the caller then falls back to the record's leading edge count.
    hub_out_degrees: Vec<(u64, u32)>,
    hub_in_degrees: Vec<(u64, u32)>,
    /// Degree floor the sidecar was built with (`Some` ⇔ the sidecar is present). A node
    /// absent from a list therefore has degree `< floor` in that direction.
    hub_degree_floor: Option<u32>,
    /// Dense per-node out/in degree column (`node_degrees.blk`) — every node's exact
    /// degree, for an O(1) lookup with no adjacency read. `None` ⇒ no column (older
    /// generation); the caller falls back to the record's leading count. Present ⇒ residency
    /// is **chunk-lazy** (a chunk faults on first touch, cold chunks free on the idle sweep)
    /// unless configured `pinned`. See [`crate::generation::Generation::node_out_degree`].
    degree_column: Option<crate::degree_column::DegreeColumn>,
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

    /// The immutable **core stack** over this base: the set's upper core segments and the
    /// id→member routing table (see [`crate::segstack`]). A singleton set (every graph
    /// until the Phase 4 flush) carries an empty stack, and every read short-circuits to
    /// the base readers above — behaviourally identical to a bare generation.
    stack: crate::segstack::CoreStack,
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

/// Refuse a Vamana index whose structure would make every search on it quietly wrong.
///
/// Each of these is a **silent** failure — a wrong or empty answer with no error anywhere —
/// which is why they are checked once at open, where the cost is one extra `pread`, rather
/// than left to surface as "recall got worse".
///
/// 1. **The `.pq` is the layout→id map.** Since v8 it is the *only* one: the `.vamana`
///    record is pure geometry. If the two files disagree on record count, every layout
///    ordinal past the shorter of them maps to the wrong node — or panics the emit closure.
/// 2. **The medoid must exist.** It is the entry point of every beam search, and the search
///    indexes the resident codes with it directly; an out-of-range medoid is an immediate
///    panic on an ordinary query.
/// 3. **The medoid must not be orphaned.** A *deleted* medoid is fine — a hole is still a
///    waypoint. A medoid with **no out-edges** is not: every search enters there, expands
///    it, finds nothing to push onto the beam, and terminates having seen exactly one node.
///    Recall for the entire index goes to zero, with no error and no panic. This is the
///    invariant a delete-splice must preserve (see `AnnMode::Vamana::medoid`); here is
///    where a violation is caught.
#[allow(clippy::too_many_arguments)]
fn validate_vamana_index(
    stem: &str,
    reader: &VamanaReader,
    pq: &ResidentPq,
    desc: &VectorIndexDesc,
    medoid: u64,
    nav: graph_format::manifest::AnnNav,
    pq_subspaces: u32,
    pq_bits: u32,
) -> Result<()> {
    let records = reader.len();
    if pq.len() as u64 != records {
        bail!(
            "vector index {stem} is inconsistent: the .vamana holds {records} records but its \
             .pq layout→id map holds {} — they are written in lockstep and index each other by \
             position",
            pq.len()
        );
    }
    if desc.count != records {
        bail!(
            "vector index {stem} is inconsistent: the MANIFEST declares {} records but the \
             .vamana holds {records} (`count` is the record count, holes included)",
            desc.count
        );
    }
    // 4a. The (metric, nav) pair must be self-consistent BEFORE the codebook-space check, because
    // that check cannot catch a forged `nav: inner_product` on a cosine/L2 index: an `InnerProduct`
    // codebook is `PqParams::new(dim, …)`, and cosine/L2 augmented codebooks have the *identical*
    // width (only Dot augments), so the space check below passes and the beam would then navigate a
    // cosine/L2 graph by IP-ADC — silently mis-navigating. `nav == InnerProduct` is only ever
    // produced for Dot, so refuse the mismatch here rather than serve it (HIK-137 phase 4).
    nav.check_metric(desc.metric, "vector index")
        .with_context(|| format!("vector index {stem}"))?;
    // 4b. The codebook must be in the space the MANIFEST's (metric, nav) pair implies.
    //
    // The read path derives the query transform from the descriptor and the *codebook's*
    // dimension. Those are the only inputs, and if they disagree the search still runs with
    // wrong neighbours, plausible scores, and no error anywhere. Tie the file back to the
    // descriptor so the space cannot drift — the invariant `graph_format::pq`'s DESIGN note
    // states, enforced. HIK-137: an `InnerProduct` (IP-native) index is trained on the RAW
    // vectors over plain `PqParams` (dim, not the dot augmentation's dim + dsub); an `Augmented`
    // index is in the metric's L2-reduced ANN space (`ann_pq_params`). Checking against the wrong
    // one would reject a valid IP index — or, worse, accept an augmented codebook under an
    // `InnerProduct` label (the beam would then navigate by IP-ADC over an augmented codebook).
    let expected = match nav {
        graph_format::manifest::AnnNav::InnerProduct => {
            graph_format::pq::PqParams::new(desc.dim, pq_subspaces, pq_bits)
        }
        graph_format::manifest::AnnNav::Augmented => {
            graph_format::pq::ann_pq_params(desc.metric, desc.dim, pq_subspaces, pq_bits)
        }
    }
    .with_context(|| format!("vector index {stem} has invalid PQ parameters"))?;
    if pq.codebook.params != expected {
        bail!(
            "vector index {stem} declares metric {:?} / nav {nav:?} over dim {} with \
             {pq_subspaces}×{pq_bits}-bit PQ, which is the space {expected:?} — but its .pq codebook \
             is {:?}. The build and the read path would navigate in different spaces.",
            desc.metric,
            desc.dim,
            pq.codebook.params
        );
    }
    if records == 0 {
        return Ok(());
    }
    if medoid >= records {
        bail!(
            "vector index {stem} names medoid layout ordinal {medoid}, but the .vamana holds \
             only {records} records"
        );
    }
    if records > 1 {
        let entry = reader
            .node(medoid as u32)
            .with_context(|| format!("read the medoid record of vector index {stem}"))?;
        if entry.neighbours.is_empty() {
            bail!(
                "vector index {stem} has an orphaned medoid (layout ordinal {medoid} has no \
                 out-edges): it is the fixed entry point of every beam search, so every query \
                 would expand one node and return nothing — recall would be zero, silently"
            );
        }
    }
    Ok(())
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
    ///
    /// Opens from the local filesystem rooted at `data_dir`; the serve path uses
    /// [`Generation::open_with_store`] to open from any configured backend.
    pub fn open_with_key(
        data_dir: impl AsRef<Path>,
        graph: &str,
        master_key: Option<&[u8]>,
    ) -> Result<Self> {
        let store = FsObjectStore::new(data_dir.as_ref());
        Self::open_with_store(&store, graph, master_key)
    }

    /// Resolve `<graph>/current` in `store`, open that generation, validate it,
    /// and build its in-memory indexes — the backend-agnostic core. The local
    /// filesystem is one backend ([`open_with_key`](Generation::open_with_key));
    /// an object store (S3) is another. Every file is read positionally through
    /// the store so the on-disk format, the readers, and validation are identical
    /// across backends.
    pub fn open_with_store(
        store: &dyn ObjectStore,
        graph: &str,
        master_key: Option<&[u8]>,
    ) -> Result<Self> {
        Self::open_with_store_opts(store, graph, master_key, true)
    }

    /// As [`open_with_store`](Generation::open_with_store), but `verify_integrity`
    /// controls the copy-completeness re-hash at open. The filesystem backend
    /// keeps it on; a remote backend may disable it (re-hashing every object over
    /// the network at open is expensive) and rely on the manifest MAC + per-block
    /// AEAD instead — see `THREAT_MODEL.md`.
    pub fn open_with_store_opts(
        store: &dyn ObjectStore,
        graph: &str,
        master_key: Option<&[u8]>,
        verify_integrity: bool,
    ) -> Result<Self> {
        Self::open_with_store_opts_cached(
            store,
            graph,
            master_key,
            verify_integrity,
            None,
            crate::degree_column::DegreeResidency::Lazy,
            None,
        )
    }

    /// As [`open_with_store_opts`](Generation::open_with_store_opts), but with an
    /// optional per-generation **range-index block-cache budget**. When `Some(n)` with
    /// `n > 0`, one decompressed-leaf-block cache of `n` bytes is built and shared across
    /// every range (ISAM) reader in this generation, so a repeated equality/range probe
    /// (a write resolve or an indexed seek over a contiguous key run) decompresses each
    /// leaf once rather than once per probe. `None`/`0` opens the readers uncached (the
    /// behaviour every other open path keeps). The cache is owned by the generation, so
    /// dropping it on swap frees the budget.
    ///
    /// `degree_residency` selects the dense degree column's residency
    /// ([`DegreeResidency`](crate::degree_column::DegreeResidency)): `Lazy` faults chunks on
    /// demand and frees cold ones on the idle sweep and on byte-budget pressure; `Pinned`
    /// prefaults the whole column here. `degree_column_bytes` is that lazy byte budget (`None`
    /// ⇒ [`DEFAULT_BUDGET_BYTES`](crate::degree_column::DEFAULT_BUDGET_BYTES)); it is ignored
    /// under `Pinned`.
    pub fn open_with_store_opts_cached(
        store: &dyn ObjectStore,
        graph: &str,
        master_key: Option<&[u8]>,
        verify_integrity: bool,
        range_index_cache_bytes: Option<usize>,
        degree_residency: crate::degree_column::DegreeResidency,
        degree_column_bytes: Option<usize>,
    ) -> Result<Self> {
        // `current` names a *set* uuid. Resolve it to the base generation: read the
        // set manifest if one exists, else treat the uuid as an implicit singleton
        // (base = the uuid itself) — the fallback for fixtures and pre-set images.
        let uuid = GenId(Self::current_uuid_in(store, graph)?);
        let set = if graph_format::setmanifest::SetManifest::exists_via(store, graph, uuid) {
            graph_format::setmanifest::SetManifest::read_via(store, graph, uuid)
                .with_context(|| format!("read set manifest for {uuid} of graph {graph}"))?
        } else {
            // Implicit singleton: `current` names a bare generation, so the uuid *is* the
            // base and there are no segments (fixtures and pre-set images).
            graph_format::setmanifest::SetManifest::singleton(uuid, 0)
        };
        let base_uuid = set.base;
        // Backend-relative key prefix for the base generation's files.
        let base = join_key(graph, &base_uuid.to_string());
        let dir = PathBuf::from(&base);

        let manifest = Manifest::read_via(store, &base)
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

        // Copy-completeness guard: re-hash every inventory file through the store
        // and refuse on the first mismatch, then confirm the manifest's own
        // content hash is self-consistent with that inventory. Skipped when the
        // backend opts out (see `verify_integrity`); the keyed-MANIFEST MAC and
        // per-block AEAD still authenticate an encrypted generation regardless.
        if verify_integrity {
            verify_against_store(store, &base, &manifest)?;
        }

        // Derive the per-generation block cipher from the runtime master key and
        // the MANIFEST salt. The key is required iff the generation is encrypted;
        // a plaintext generation ignores any key and opens as before.
        let cipher = derive_cipher(&manifest, master_key, graph, &uuid)?;

        // Open every reader. Each only reads its footer/sparse index at open
        // (block bytes stay lazy via positional reads — D16), so this is cheap and
        // keeps resident memory to the directories alone. Each reader is handed the
        // cipher so a cache-miss block read decrypts before decompressing (D28).
        let open_blk = |name: &str| -> Result<Arc<dyn graph_format::store::RandomReadAt>> {
            store.open(&join_key(&base, name))
        };
        let node_props = PropsReader::open_src(open_blk("node_props.blk")?, cipher.clone())?;
        let node_labels = NodeLabelsReader::open_src(open_blk("node_labels.blk")?, cipher.clone())?;
        let edge_props = PropsReader::open_src(open_blk("edge_props.blk")?, cipher.clone())?;
        let topology = TopologyReader::open_src(open_blk("topology.csr.blk")?, cipher.clone())?;
        // Endpoint postings (format v2+). Gate on existence so a hand-built
        // fixture without them still opens; the format-version check already
        // fences real generations.
        let open_post = |name: &str| -> Result<Option<BlockFileReader>> {
            let key = join_key(&base, name);
            if store.exists(&key)? {
                Ok(Some(BlockFileReader::open_src(
                    store.open(&key)?,
                    cipher.clone(),
                )?))
            } else {
                Ok(None)
            }
        };
        let reltype_src_post = open_post("reltype_src.post")?;
        let reltype_tgt_post = open_post("reltype_tgt.post")?;
        let vectors = VectorStoreReader::open_src(open_blk("vectors.f32.blk")?, cipher.clone())?;

        // One decoded-leaf-block cache shared across this generation's range readers
        // (built only when a positive budget is supplied — the server path; every other
        // opener leaves the readers uncached). Keyed per reader by the index's ordinal,
        // so `(ordinal, block)` is unique within the cache.
        let range_cache = range_index_cache_bytes
            .filter(|&n| n > 0)
            .map(|n| Arc::new(graph_format::isam::DecodedBlockCache::new(n)));

        // Open every range index concurrently — each is an independent S3 footer
        // read, and large graphs carry 100+ of them, so a serial loop here is the
        // bulk of a cold start. rayon bounds the fan-out to the core count; the
        // store and cipher are `Send + Sync`. First error wins.
        let range_indexes = manifest
            .range_indexes
            .par_iter()
            .enumerate()
            .map(|(ordinal, ri)| -> Result<(String, IsamReader)> {
                let key = join_key(&base, &format!("range/{}.isam", ri.name));
                let mut reader = IsamReader::open_src(store.open(&key)?, cipher.clone())
                    .with_context(|| format!("open range index {key}"))?;
                if let Some(cache) = &range_cache {
                    reader = reader.with_block_cache(cache.clone(), ordinal as u32);
                }
                Ok((ri.name.clone(), reader))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        // Value→count histograms (format v3+). Gate on existence (a hand-built
        // fixture may omit it). Records align by position with `property_histograms`;
        // decode them resident, keyed by index name.
        let mut prop_histograms = HashMap::new();
        let hist_key = join_key(&base, "prop_hist.blk");
        if store.exists(&hist_key)? && !manifest.property_histograms.is_empty() {
            let reader = BlockFileReader::open_src(store.open(&hist_key)?, cipher.clone())
                .with_context(|| format!("open histogram store {hist_key}"))?;
            for (i, d) in manifest.property_histograms.iter().enumerate() {
                let rec = reader.read_record_global(i as u64).with_context(|| {
                    format!("read histogram record {i} for index {}", d.index_name)
                })?;
                prop_histograms.insert(d.index_name.clone(), decode_histogram(&rec)?);
            }
        }

        // Hub-degree sidecar (`hub_degrees.blk`): decode both lists resident (a few MB
        // even on 91.6M nodes — hubs are rare). Gate on the manifest desc *and* file
        // existence, so an older generation (no desc) or a hand-built fixture declines
        // cleanly and the caller falls back to the record's leading edge count. Record 0
        // is the out-hub list, record 1 the in-hub list.
        let mut hub_out_degrees = Vec::new();
        let mut hub_in_degrees = Vec::new();
        let mut hub_degree_floor = None;
        let hub_key = join_key(&base, "hub_degrees.blk");
        if let Some(desc) = &manifest.hub_degrees {
            if store.exists(&hub_key)? {
                let reader = BlockFileReader::open_src(store.open(&hub_key)?, cipher.clone())
                    .with_context(|| format!("open hub-degree sidecar {hub_key}"))?;
                hub_out_degrees = graph_format::hubdegree::decode_hub_list(
                    &reader.read_record_global(0).context("read hub out-list")?,
                )?;
                hub_in_degrees = graph_format::hubdegree::decode_hub_list(
                    &reader.read_record_global(1).context("read hub in-list")?,
                )?;
                hub_degree_floor = Some(desc.floor);
            }
        }

        // Dense per-node degree column (`node_degrees.blk`): every node's exact out/in
        // degree, resident. Gated purely on file existence (not a manifest field) so a
        // generation *retrofitted* with the column — or one built with it — loads it, and
        // one without falls back to the record's leading count. Present ⇒ the degree-sum
        // count fast path answers a penultimate-frontier lookup in O(1) with no I/O.
        // Residency policy for the dense degree column (`cache.degreeColumn`): chunk-lazy by
        // default, or `pinned` (eager prefault, never evicted) for latency-critical/object-store
        // deployments — threaded from config through the server's open/swap paths.
        let mut degree_column = None;
        let nd_key = join_key(&base, "node_degrees.blk");
        if store.exists(&nd_key)? {
            let reader = BlockFileReader::open_src(store.open(&nd_key)?, cipher.clone())
                .with_context(|| format!("open node-degree column {nd_key}"))?;
            degree_column = Some(
                crate::degree_column::DegreeColumn::open(
                    reader,
                    manifest.node_count as usize,
                    degree_residency,
                    degree_column_bytes.unwrap_or(crate::degree_column::DEFAULT_BUDGET_BYTES),
                )
                .with_context(|| format!("open node-degree column {nd_key}"))?,
            );
        }

        // Open the disk-native Vamana/PQ indexes (above the ANN threshold). Each
        // reads only its block-file footer + PQ codebook header at open; the
        // resident PQ codes are loaded once here (the navigation set the beam search
        // holds resident — never a full in-memory graph). Below-threshold indexes
        // stay brute-force over `vectors.f32.blk` and open nothing extra.
        let mut vamana_indexes = HashMap::new();
        for (ord, vi) in manifest.vector_indexes.iter().enumerate() {
            let AnnMode::Vamana {
                medoid,
                pq_subspaces,
                pq_bits,
                nav,
                ..
            } = vi.mode
            else {
                continue;
            };
            let stem = format!("vector/{}.{}", vi.label, vi.property);
            let reader = VamanaReader::open_src(
                store.open(&join_key(&base, &format!("{stem}.vamana")))?,
                cipher.clone(),
            )
            .with_context(|| format!("open Vamana store {stem}.vamana"))?;
            let pq = PqReader::open_src(
                store.open(&join_key(&base, &format!("{stem}.pq")))?,
                cipher.clone(),
            )
            .with_context(|| format!("open PQ store {stem}.pq"))?;
            let resident = Arc::new(
                pq.load_resident()
                    .with_context(|| format!("load resident PQ codes for {stem}.pq"))?,
            );
            validate_vamana_index(
                &stem,
                &reader,
                &resident,
                vi,
                medoid,
                nav,
                pq_subspaces,
                pq_bits,
            )?;
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

        // Prefer the per-label / per-reltype counts persisted in the manifest
        // (tallied once at build), falling back to an open-time scan only for
        // generations built before those fields existed (empty ⇒ scan, never wrong).
        let label_counts = if manifest.label_node_counts.is_empty() {
            build_label_counts(&node_labels)?
        } else {
            counts_from_vec(&manifest.label_node_counts)
        };
        let reltype_counts = if manifest.reltype_edge_counts.is_empty() {
            build_reltype_counts(&topology)?
        } else {
            counts_from_vec(&manifest.reltype_edge_counts)
        };

        // Load the set's upper core segments (the LSM stack over this base). A singleton set
        // has none, so this is a zero-cost single-band routing table; a set with segments
        // opens each segment's readers through the same store, sharing one block cache.
        let stack = if set.segments.is_empty() {
            crate::segstack::CoreStack::singleton(manifest.node_count, manifest.edge_count)
        } else {
            crate::segstack::CoreStack::load(
                store,
                graph,
                &set,
                manifest.node_count,
                manifest.edge_count,
                master_key,
                verify_integrity,
                range_index_cache_bytes,
            )
            .with_context(|| format!("load core segments for set {uuid} of graph {graph}"))?
        };

        info!(
            graph,
            generation = %uuid,
            nodes = manifest.node_count,
            edges = manifest.edge_count,
            labels = manifest.labels.len(),
            reltypes = manifest.reltypes.len(),
            range_indexes = manifest.range_indexes.len(),
            vector_indexes = manifest.vector_indexes.len(),
            segments = stack.segments().len(),
            "opened generation"
        );

        Ok(Self {
            graph: graph.to_string(),
            uuid,
            base_uuid,
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
            prop_histograms,
            hub_out_degrees,
            hub_in_degrees,
            hub_degree_floor,
            degree_column,
            vamana_indexes,
            label_ids,
            reltype_ids,
            property_key_ids,
            label_counts,
            reltype_counts,
            stack,
        })
    }

    /// The immutable core stack over this base (the set's upper segments + id routing).
    /// A singleton set carries an empty stack — see [`crate::segstack::CoreStack`].
    pub fn stack(&self) -> &crate::segstack::CoreStack {
        &self.stack
    }

    /// Read just the `current` pointer's generation UUID without opening (or
    /// validating) the generation. The generation guard calls this every poll to
    /// detect a published swap cheaply — the data dir may be remote/network
    /// storage (e.g. NFS), so we poll
    /// this small text file rather than watch it for events (D14/D16).
    pub fn current_uuid(data_dir: impl AsRef<Path>, graph: &str) -> Result<uuid::Uuid> {
        let store = FsObjectStore::new(data_dir.as_ref());
        Self::current_uuid_in(&store, graph)
    }

    /// As [`current_uuid`](Generation::current_uuid) but against any backend —
    /// the generation guard uses this to detect a published swap over the
    /// configured store.
    pub fn current_uuid_in(store: &dyn ObjectStore, graph: &str) -> Result<uuid::Uuid> {
        read_current_via(store, graph)
    }

    // ── Identity / metadata ────────────────────────────────────────────────

    pub fn graph(&self) -> &str {
        &self.graph
    }
    pub fn uuid(&self) -> GenId {
        self.uuid
    }
    /// The base generation uuid (the directory this image reads). Equal to
    /// [`uuid`](Self::uuid) for a singleton set; distinct once segments stack over a
    /// shared base.
    pub fn base_uuid(&self) -> GenId {
        self.base_uuid
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

    /// The precomputed value→count histogram for the range index `name`, if the
    /// generation carries one (it does not when the index is over an edge or its
    /// distinct count exceeded the build's `--histogram-max-distinct`). The pairs
    /// are ascending by key and identical to `range_index(name).distinct_key_counts()`
    /// — the grouped-index fast path uses them to skip the O(index) walk.
    pub fn property_histogram(&self, name: &str) -> Option<&[(Value, u64)]> {
        self.prop_histograms.get(name).map(Vec::as_slice)
    }

    /// The hub-degree sidecar's floor if this generation carries one (`Some` ⇔
    /// `hub_degrees.blk` was loaded). A node absent from a hub list has degree `< floor`
    /// in that direction; a caller uses that to bound a non-listed node's degree instead
    /// of assuming zero. `None` ⇒ no sidecar (older generation) ⇒ fall back to reading
    /// the record's leading edge count.
    pub fn hub_degree_floor(&self) -> Option<u32> {
        self.hub_degree_floor
    }

    /// Exact **out**-degree of core node `node` if it is a listed hub (out-degree `>=`
    /// the sidecar floor), else `None` (its out-degree is `< floor`, or there is no
    /// sidecar). O(log n) binary search over the resident list, no adjacency read.
    pub fn core_out_degree_if_hub(&self, node: u64) -> Option<u64> {
        self.hub_out_degrees
            .binary_search_by_key(&node, |&(id, _)| id)
            .ok()
            .map(|i| self.hub_out_degrees[i].1 as u64)
    }

    /// Exact **in**-degree of core node `node` if it is a listed hub, else `None`. The
    /// reverse-direction counterpart of [`Self::core_out_degree_if_hub`].
    pub fn core_in_degree_if_hub(&self, node: u64) -> Option<u64> {
        self.hub_in_degrees
            .binary_search_by_key(&node, |&(id, _)| id)
            .ok()
            .map(|i| self.hub_in_degrees[i].1 as u64)
    }

    /// Exact **out**-degree of core node `node` from the dense degree column
    /// (`node_degrees.blk`) — O(1), no I/O — or `None` if this generation carries no
    /// column (fall back to the record's leading count).
    pub fn node_out_degree(&self, node: u64) -> Option<u32> {
        self.degree_column.as_ref().and_then(|c| c.out_degree(node))
    }

    /// Exact **in**-degree from the dense column, or `None` when absent. Counterpart of
    /// [`Self::node_out_degree`].
    pub fn node_in_degree(&self, node: u64) -> Option<u32> {
        self.degree_column.as_ref().and_then(|c| c.in_degree(node))
    }

    /// Drop dense-degree chunks untouched for at least `ttl` (chunk-lazy residency). No-op when
    /// the generation carries no column or it is pinned. Returns the number of chunks freed —
    /// the idle-TTL sweep calls this so cold degree chunks free like block-cache entries.
    pub fn evict_cold_degree_chunks(
        &self,
        now: std::time::Instant,
        ttl: std::time::Duration,
    ) -> usize {
        self.degree_column
            .as_ref()
            .map_or(0, |c| c.evict_expired(now, ttl))
    }

    /// Dense-degree chunks currently resident (both halves), or `None` when the generation
    /// carries no column. Diagnostic / test hook — lets a caller assert the chunk-lazy column
    /// only materialised what it touched (e.g. that a hub answered from the sidecar faulted no
    /// dense chunk).
    pub fn degree_column_resident_chunks(&self) -> Option<usize> {
        self.degree_column.as_ref().map(|c| c.resident_chunks())
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

    /// Number of self-loop edges of relationship type `reltype_id` (edges whose
    /// source and target are the same node). Reads the manifest directly; 0 when the
    /// generation predates the field.
    ///
    /// FOLLOW-UP: currently UNUSED by the query engine — the undirected count is
    /// `2×edge` (the matcher counts a self-loop twice, so no subtraction). Kept as
    /// genuine schema metadata (and the natural input to a future labelled-undirected
    /// or `db.schema` feature); drop it if that never lands.
    pub fn reltype_self_loop_count(&self, reltype_id: u32) -> u64 {
        self.manifest
            .reltype_self_loop_counts
            .get(reltype_id as usize)
            .copied()
            .unwrap_or(0)
    }

    /// True when this generation carries per-reltype self-loop counts.
    pub fn has_reltype_self_loop_counts(&self) -> bool {
        !self.manifest.reltype_self_loop_counts.is_empty()
    }

    /// Number of nodes whose **first** label (`labels(n)[0]`) is `label_id` (O(1),
    /// from the manifest). 0 when absent/unknown — callers detect that via
    /// [`Self::has_first_label_counts`].
    pub fn first_label_count(&self, label_id: u32) -> u64 {
        self.manifest
            .first_label_counts
            .get(label_id as usize)
            .copied()
            .unwrap_or(0)
    }

    /// True when this generation carries first-label counts (⇒ the `labels(n)[0]`
    /// metadata fast path can reproduce first-label semantics exactly).
    pub fn has_first_label_counts(&self) -> bool {
        !self.manifest.first_label_counts.is_empty()
    }

    /// Sum of [`Self::first_label_count`] over all labels — the number of nodes with
    /// at least one label. The whole-graph `labels(n)[0]` null bucket (zero-label
    /// nodes) is `node_count − first_labelled_node_count`.
    pub fn first_labelled_node_count(&self) -> u64 {
        self.manifest.first_label_counts.iter().sum()
    }

    /// Edge-schema **source** marginal: edges of type `reltype_id` whose source
    /// carries `src_label_id`. `None` when the generation lacks the marginal (⇒ the
    /// source-labelled rel fast path declines); `Some(0)` for an absent key.
    pub fn src_label_reltype_count(&self, src_label_id: u32, reltype_id: u32) -> Option<u64> {
        let v = &self.manifest.src_label_reltype_counts;
        if v.is_empty() {
            return None;
        }
        Some(
            match v.binary_search_by(|e| (e.0, e.1).cmp(&(src_label_id, reltype_id))) {
                Ok(i) => v[i].2,
                Err(_) => 0,
            },
        )
    }

    /// Edge-schema **target** marginal: edges of type `reltype_id` whose target
    /// carries `tgt_label_id`. `None` when the generation lacks the marginal.
    pub fn reltype_tgt_label_count(&self, reltype_id: u32, tgt_label_id: u32) -> Option<u64> {
        let v = &self.manifest.reltype_tgt_label_counts;
        if v.is_empty() {
            return None;
        }
        Some(
            match v.binary_search_by(|e| (e.0, e.1).cmp(&(reltype_id, tgt_label_id))) {
                Ok(i) => v[i].2,
                Err(_) => 0,
            },
        )
    }

    /// Full edge-schema cube cell: edges of type `reltype_id` whose source carries
    /// `src_label_id` and target carries `tgt_label_id`. `None` when the generation
    /// lacks the cube (⇒ the both-endpoints-labelled fast path declines).
    pub fn schema_triple_count(
        &self,
        src_label_id: u32,
        reltype_id: u32,
        tgt_label_id: u32,
    ) -> Option<u64> {
        let v = &self.manifest.schema_triple_counts;
        if v.is_empty() {
            return None;
        }
        let key = (src_label_id, reltype_id, tgt_label_id);
        Some(match v.binary_search_by(|e| (e.0, e.1, e.2).cmp(&key)) {
            Ok(i) => v[i].3,
            Err(_) => 0,
        })
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

    /// A **lazy** ascending cursor per base endpoint posting selected by
    /// (`reltype_ids`, `side`) — the streaming counterpart of
    /// [`collect_endpoint_nodes_for_reltypes`](Self::collect_endpoint_nodes_for_reltypes),
    /// which expands every posting into one `Vec<u64>` (733 MB for a dense reltype) before
    /// the first id. Each cursor owns only the compressed Elias–Fano posting and yields ids
    /// one window at a time, so the anchor merge stays bounded and a pushed `LIMIT` stops the
    /// walk. Each cursor is individually ascending+distinct; different (reltype, side) cursors
    /// may overlap, so the caller's k-way merge dedups. An empty/absent posting contributes no
    /// cursor.
    pub fn endpoint_posting_cursors(
        &self,
        reltype_ids: &[u32],
        side: RelEndpointSide,
    ) -> Result<Vec<EndpointPostingIter>> {
        let (Some(src), Some(tgt)) = (&self.reltype_src_post, &self.reltype_tgt_post) else {
            bail!("generation has no reltype endpoint postings");
        };
        let mut cursors = Vec::new();
        let mut push = |reader: &BlockFileReader, t: u32| -> Result<()> {
            if (t as u64) < reader.total_records() {
                cursors.push(endpoint_posting_cursor(
                    &reader.read_record_global(t as u64)?,
                )?);
            }
            Ok(())
        };
        for &t in reltype_ids {
            if matches!(side, RelEndpointSide::Source | RelEndpointSide::Either) {
                push(src, t)?;
            }
            if matches!(side, RelEndpointSide::Target | RelEndpointSide::Either) {
                push(tgt, t)?;
            }
        }
        Ok(cursors)
    }

    /// Number of records in the base node-label column — the dense id range a base label scan
    /// sweeps (`0..len`). Ids at or above this are not in the base column (they are born in a
    /// segment or the delta) and are supplied by the overlay sources of the anchor merge.
    pub fn node_label_column_len(&self) -> u64 {
        self.node_labels.inner().total_records()
    }

    /// Dense node ids carrying `label_id`, ascending — re-derived on demand by a
    /// single pass over the node-label column. We deliberately do **not** keep a
    /// resident id posting (it would be O(#nodes) per label); the open-time
    /// footprint stays bounded and label scans pay the scan only when they run.
    pub fn collect_nodes_with_label(&self, label_id: u32) -> Result<Vec<u64>> {
        let mut ids = Vec::new();
        let bitmask = self.node_labels.bitmask();
        self.node_labels.inner().for_each_record(|node_id, rec| {
            if graph_format::nodelabels::decode_labels(rec, bitmask)?.contains(&label_id) {
                ids.push(node_id);
            }
            Ok(())
        })?;
        Ok(ids)
    }
}

/// Read and parse `<graph>/current` into a generation UUID via the store.
fn read_current_via(store: &dyn ObjectStore, graph: &str) -> Result<uuid::Uuid> {
    let key = join_key(graph, "current");
    let bytes = store
        .read_all(&key)
        .with_context(|| format!("read current pointer {key}"))?;
    let text =
        String::from_utf8(bytes).with_context(|| format!("current pointer {key} is not UTF-8"))?;
    let trimmed = text.trim();
    uuid::Uuid::parse_str(trimmed)
        .with_context(|| format!("parse generation uuid {trimmed:?} from {key}"))
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

/// Verify every file in the manifest inventory through the store, then confirm
/// the overall content hash is self-consistent. Each file's check is delegated
/// to [`ObjectStore::verify_file`], so the backend picks the cheapest sound
/// method — a local file re-hashes its bytes (BLAKE3); S3 compares the object's
/// size from a metadata `HEAD` with no body read.
fn verify_against_store(store: &dyn ObjectStore, base: &str, manifest: &Manifest) -> Result<()> {
    // Each file's check is one independent store round-trip (a metadata HEAD on
    // S3), so verify them concurrently and surface the first failure. rayon bounds
    // the fan-out to the core count; `ObjectStore` is `Send + Sync`.
    manifest.files.par_iter().try_for_each(|fe| -> Result<()> {
        let key = join_key(base, &fe.name);
        store
            .verify_file(
                &key,
                &FileIntegrity {
                    size: fe.bytes,
                    blake3: &fe.blake3,
                    sha256: fe.sha256.as_deref(),
                    crc32c: fe.crc32c.as_deref(),
                },
            )
            .with_context(|| format!("verify generation file {}", fe.name))
    })?;
    // Every file matched what the manifest asserts; the manifest's own
    // content_hash must therefore equal the hash over the (name, hash) inventory.
    manifest
        .verify_content_hash()
        .context("manifest content hash is inconsistent with its file inventory")?;
    Ok(())
}

/// Turn a persisted per-id count vector (index = id) into the resident `id → count`
/// map, keeping only non-zero entries so the footprint stays O(#present symbols).
/// Used to skip the open-time label/reltype scans when the manifest carries the
/// counts (built once at build time).
fn counts_from_vec(v: &[u64]) -> HashMap<u32, u64> {
    v.iter()
        .enumerate()
        .filter(|(_, &c)| c > 0)
        .map(|(i, &c)| (i as u32, c))
        .collect()
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
    let bitmask = node_labels.bitmask();
    node_labels.inner().for_each_record(|_node_id, rec| {
        for label_id in graph_format::nodelabels::decode_labels(rec, bitmask)? {
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
        for adj in graph_format::topology::decode_adj(rec, true)? {
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
                blake3: graph_format::integrity::hash_file(&path).unwrap(),
                sha256: None,
                crc32c: None,
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
            compression_profile: String::new(),
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
            // Left empty so this fixture exercises the open-time scan fallback for
            // the whole-graph metadata counts; the persisted path is covered by the
            // slater-build goldens and the exec metadata fast-path tests.
            reltype_edge_counts: vec![],
            reltype_self_loop_counts: vec![],
            label_node_counts: vec![],
            first_label_counts: vec![],
            src_label_reltype_counts: vec![],
            reltype_tgt_label_counts: vec![],
            schema_triple_counts: vec![],
            property_histograms: vec![],
            hub_degrees: None,
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

    /// Recursively load every file under `root` into a `MemObjectStore`, keyed by
    /// its `/`-joined path relative to `root` — i.e. the same keys the store
    /// abstraction builds (`<graph>/current`, `<graph>/<uuid>/<file>`, …).
    fn load_dir_into_mem(
        store: &graph_format::store::mem::MemObjectStore,
        root: &Path,
        dir: &Path,
    ) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                load_dir_into_mem(store, root, &path);
            } else {
                let rel = path.strip_prefix(root).unwrap();
                let key = rel
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                store
                    .put(&key, &std::fs::read(&path).unwrap(), None)
                    .unwrap();
            }
        }
    }

    /// The same generation opens identically through a non-filesystem backend:
    /// every reader, the integrity re-hash, and the `current` pointer resolve
    /// through the `ObjectStore` abstraction with no filesystem access.
    #[test]
    fn opens_through_in_memory_backend() {
        let (root, graph, uuid) = write_fixture("opens_through_in_memory_backend");
        let mem = graph_format::store::mem::MemObjectStore::new();
        load_dir_into_mem(&mem, &root, &root);

        let gen = Generation::open_with_store(&mem, &graph, None).unwrap();
        assert_eq!(gen.uuid(), GenId(uuid));
        assert_eq!(gen.node_count(), 3);
        // A property read, a label read, a topology read, a vector read, and a
        // range-index lookup all flow through the in-memory store.
        let alice = gen.node_props().props(0).unwrap();
        assert!(alice.contains(&(0, Value::Str("Alice".into()))));
        assert_eq!(gen.node_labels().labels(2).unwrap(), vec![1]);
        assert_eq!(gen.topology().outgoing(NodeId(0)).unwrap().len(), 2);
        assert_eq!(
            gen.vectors().group(0, 1).unwrap()[0].vector,
            vec![0.1, 0.2, 0.3]
        );
        let hits = gen
            .range_index("node_Person_name")
            .unwrap()
            .lookup_eq(&Value::Str("Bob".into()))
            .unwrap();
        assert_eq!(hits, vec![1]);

        // Discovery and the swap-detection pointer read also work via the store.
        assert_eq!(Generation::current_uuid_in(&mem, &graph).unwrap(), uuid);
        let _ = std::fs::remove_dir_all(&root);
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
    fn opens_through_an_explicit_set_manifest() {
        use graph_format::setmanifest::SetManifest;
        let (root, graph, uuid) = write_fixture("opens_through_a_set_manifest");
        // Write a singleton set manifest alongside the generation (set == base == gen).
        let sets = root.join(&graph).join("sets");
        std::fs::create_dir_all(&sets).unwrap();
        let set = SetManifest::singleton(GenId(uuid), 0);
        std::fs::write(sets.join(format!("{uuid}.json")), set.to_bytes().unwrap()).unwrap();

        // The reader resolves `current` → set manifest → base, and serves identically.
        let gen = Generation::open(&root, &graph).unwrap();
        assert_eq!(gen.uuid(), GenId(uuid), "identity is the set uuid");
        assert_eq!(gen.base_uuid(), GenId(uuid), "base == set in a singleton");
        assert_eq!(gen.node_count(), 3);
        assert_eq!(
            gen.node_props().props(0).unwrap(),
            vec![(0, Value::Str("Alice".into())), (1, Value::Int(30))]
        );

        // A set manifest with an unknown magic must fail cleanly (not open garbage).
        let mut bad: SetManifest =
            serde_json::from_slice(&std::fs::read(sets.join(format!("{uuid}.json"))).unwrap())
                .unwrap();
        bad.magic = "NOTASET".into();
        std::fs::write(sets.join(format!("{uuid}.json")), bad.to_bytes().unwrap()).unwrap();
        let err = match Generation::open(&root, &graph) {
            Ok(_) => panic!("expected a clean failure on a bad set manifest"),
            Err(e) => e,
        };
        assert!(format!("{err:#}").contains("not a set manifest"));

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn opens_a_set_with_an_upper_segment() {
        use graph_format::manifest::FileEntry;
        use graph_format::segmanifest::{SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION};
        use graph_format::segment::{NodeRow, SegmentWriter};
        use graph_format::setmanifest::{SegmentRef, SetManifest};

        let (root, graph, base_uuid) = write_fixture("opens_a_set_with_an_upper_segment");
        let seg_uuid = uuid::Uuid::from_u128(0x5_e600_0000_0000_0000_0000_0000_0002);
        let set_uuid = uuid::Uuid::from_u128(0x5_e700_0000_0000_0000_0000_0000_0003);

        // Write a segment carrying one born node at dense id 3 (the base has 3 nodes / 2 edges).
        let seg_dir = root
            .join(&graph)
            .join("segments")
            .join(seg_uuid.to_string());
        std::fs::create_dir_all(seg_dir.parent().unwrap()).unwrap();
        let mut w = SegmentWriter::create(&seg_dir, 0x11, 4096, 3).unwrap();
        w.push_node(
            3,
            &NodeRow {
                labels: vec!["Person".into()],
                props: vec![("name".into(), Value::Str("Zed".into()))],
                tombstoned: false,
            },
        )
        .unwrap();
        w.finish().unwrap();
        let mut m = SegmentManifest {
            magic: SEGMENT_MAGIC.into(),
            version: SEGMENT_MANIFEST_VERSION,
            segment_uuid: GenId(seg_uuid),
            base: GenId(base_uuid),
            created_unix: 0,
            node_band: (3, 4),
            edge_band: (2, 2), // no edges in this segment
            content_hash: String::new(),
            encryption: None,
            node_count_delta: 1,
            edge_count_delta: 0,
            reltype_edge_deltas: vec![],
            label_node_deltas: vec![("Person".into(), 1)],
            hub_degree_out_deltas: vec![],
            hub_degree_in_deltas: vec![],
            marginals_exact: true,
            dirty_vectors: vec![],
            dirty_indexes: vec![],
            label_membership_touch: None,
            mac: None,
            files: vec![FileEntry {
                name: "node.blk".into(),
                bytes: 0,
                blake3: "aa".into(),
                sha256: None,
                crc32c: None,
            }],
        };
        m.set_content_hash();
        m.write_to_dir(&seg_dir).unwrap();

        // Publish a set over the same base and repoint `current` at it.
        let sets = root.join(&graph).join("sets");
        std::fs::create_dir_all(&sets).unwrap();
        let mut set = SetManifest::singleton(GenId(base_uuid), 0);
        set.set_uuid = GenId(set_uuid);
        set.segments = vec![SegmentRef::from_manifest(&m)];
        std::fs::write(
            sets.join(format!("{set_uuid}.json")),
            set.to_bytes().unwrap(),
        )
        .unwrap();
        std::fs::write(root.join(&graph).join("current"), set_uuid.to_string()).unwrap();

        let gen = Generation::open(&root, &graph).unwrap();
        assert_eq!(gen.uuid(), GenId(set_uuid), "identity is the set uuid");
        assert_eq!(
            gen.base_uuid(),
            GenId(base_uuid),
            "base is the shared generation"
        );
        // The stack loaded the segment and routes the appended band to it…
        assert_eq!(gen.stack().segments().len(), 1);
        assert_eq!(
            gen.stack().extents().nodes.route(3),
            Some(graph_format::extents::SegmentOrd::Upper(0))
        );
        assert_eq!(
            gen.stack().segments()[0]
                .reader
                .node_row(3)
                .unwrap()
                .unwrap()
                .labels,
            vec!["Person".to_string()]
        );
        // …but the base read surface is untouched — the stack is inert until Phase 3 wires it.
        assert_eq!(
            gen.node_count(),
            3,
            "node_count is still the base count in slice 3.1"
        );
        assert_eq!(
            gen.node_props().props(0).unwrap(),
            vec![(0, Value::Str("Alice".into())), (1, Value::Int(30))]
        );
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
        // Asserted over the whole chain (`{:#}`), not just the outermost layer: the version
        // is now checked inside `Manifest::read_from_dir`, *before* the strict parse, so the
        // refusal is nested under the "read MANIFEST for generation …" context. It has to be
        // there — the `Manifest` struct is schema-locked to the current version, so a
        // manifest one bump behind fails inside serde on a field name and the check down
        // here would never run at all (see `manifest::parse_manifest`).
        assert!(
            format!("{err:#}").contains("format version"),
            "unexpected error: {err:#}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    // ── The Vamana open-time guards ──────────────────────────────────────────────
    //
    // Each of these is a *silent* failure if it is not caught: the index still opens, every
    // query still returns rows, and the rows are wrong (or empty). They are checked at open
    // because that is where one `pread` buys certainty.

    /// Write a `.vamana` + `.pq` pair and hand back what `validate_vamana_index` takes.
    /// `orphan_medoid` writes record 0 with no out-edges; `pq_records` short-writes the
    /// `.pq` so the two files can be made to disagree.
    #[allow(clippy::type_complexity)]
    fn vamana_pair(
        tag: &str,
        metric: graph_format::manifest::Metric,
        subspaces: u32,
        orphan_medoid: bool,
        pq_records: usize,
    ) -> (
        PathBuf,
        graph_format::vamana::VamanaReader,
        ResidentPq,
        VectorIndexDesc,
    ) {
        use graph_format::pq::{ann_point, ann_pq_params, l2_norm, train_codebooks, PqWriter};
        use graph_format::vamana::{VamanaReader, VamanaWriter};

        let dim = 4usize;
        let n = 6usize;
        let dir = std::env::temp_dir().join(format!("slater_vamval_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let raw: Vec<Vec<f32>> = (0..n)
            .map(|i| (0..dim).map(|d| (i + d) as f32 + 1.0).collect())
            .collect();

        let mut vw = VamanaWriter::create_with_cipher(dir.join("x.vamana"), 4096, 3, None).unwrap();
        for (i, v) in raw.iter().enumerate() {
            let nbrs: Vec<u32> = if i == 0 && orphan_medoid {
                vec![]
            } else {
                (0..n as u32).filter(|&j| j != i as u32).collect()
            };
            vw.append(v, &nbrs).unwrap();
        }
        vw.finish().unwrap();

        let params = ann_pq_params(metric, dim as u32, subspaces, 4).unwrap();
        let max_norm = raw.iter().map(|v| l2_norm(v)).fold(0.0f64, f64::max);
        let points: Vec<Vec<f32>> = raw
            .iter()
            .map(|v| ann_point(metric, v, max_norm, params.dim as usize).unwrap())
            .collect();
        let cb = train_codebooks(&points, params, 5).unwrap();
        let mut pw = PqWriter::create_with_cipher(dir.join("x.pq"), &cb, 4096, 3, None).unwrap();
        for p in points.iter().take(pq_records) {
            pw.append_codes(0, &cb.encode(p).unwrap()).unwrap();
        }
        pw.finish().unwrap();

        let reader = VamanaReader::open_with_cipher(dir.join("x.vamana"), None).unwrap();
        let resident = PqReader::open_with_cipher(dir.join("x.pq"), None)
            .unwrap()
            .load_resident()
            .unwrap();
        let desc = VectorIndexDesc {
            label: "Doc".into(),
            property: "embedding".into(),
            dim: dim as u32,
            metric,
            count: n as u64,
            first_record: 0,
            mode: AnnMode::Vamana {
                r: 8,
                alpha: 1.2,
                medoid: 0,
                pq_subspaces: subspaces,
                pq_bits: 4,
                live_count: n as u64,
                max_norm: max_norm as f32,
                nav: graph_format::manifest::AnnNav::Augmented,
            },
        };
        (dir, reader, resident, desc)
    }

    /// **The medoid trap.** A *deleted* medoid is fine — a hole is still a waypoint. A
    /// medoid with no *out-edges* is not: every beam search enters there, expands it, finds
    /// nothing to push onto the beam, and terminates having seen exactly one node. Recall
    /// for the entire index goes to zero, with no error and no panic. S5's delete-splice is
    /// what could do this; refusing it here is what stops it being served.
    #[test]
    fn an_orphaned_medoid_is_refused_rather_than_served() {
        use graph_format::manifest::Metric;
        let (dir, reader, pq, desc) = vamana_pair("orphan", Metric::Cosine, 2, true, 6);
        let err = validate_vamana_index(
            "x",
            &reader,
            &pq,
            &desc,
            0,
            graph_format::manifest::AnnNav::Augmented,
            2,
            4,
        )
        .expect_err("an index whose entry point has no out-edges must not open");
        assert!(
            err.to_string().contains("orphaned medoid"),
            "unexpected: {err:#}"
        );

        // The same index with the medoid's edges intact opens fine — the guard is not
        // simply rejecting everything.
        let (dir2, reader2, pq2, desc2) = vamana_pair("ok", Metric::Cosine, 2, false, 6);
        validate_vamana_index(
            "x",
            &reader2,
            &pq2,
            &desc2,
            0,
            graph_format::manifest::AnnNav::Augmented,
            2,
            4,
        )
        .unwrap();
        // An out-of-range medoid is refused too: the beam search indexes the resident codes
        // with it directly, so it is a panic on an ordinary query.
        let err = validate_vamana_index(
            "x",
            &reader2,
            &pq2,
            &desc2,
            99,
            graph_format::manifest::AnnNav::Augmented,
            2,
            4,
        )
        .unwrap_err();
        assert!(err.to_string().contains("medoid"), "unexpected: {err:#}");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// The `.pq` is the *only* layout→id map since v8. If it and the `.vamana` disagree on
    /// record count, every ordinal past the shorter of them maps to the wrong node — or
    /// panics the emit closure.
    #[test]
    fn a_pq_and_vamana_record_count_disagreement_is_refused() {
        use graph_format::manifest::Metric;
        let (dir, reader, pq, desc) = vamana_pair("short", Metric::Cosine, 2, false, 4);
        let err = validate_vamana_index(
            "x",
            &reader,
            &pq,
            &desc,
            0,
            graph_format::manifest::AnnNav::Augmented,
            2,
            4,
        )
        .unwrap_err();
        assert!(err.to_string().contains("lockstep"), "unexpected: {err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The read path derives its query transform from `desc.metric` and the codebook's
    /// dimension, and from nothing else. A `dot` descriptor over a codebook trained in
    /// cosine space makes `ann_query` a no-op: the beam then navigates by plain squared-L2
    /// while the re-rank scores by inner product. Wrong neighbours, plausible scores, no
    /// error. Tie the codebook back to the descriptor so the build and the read path cannot
    /// end up in different spaces.
    #[test]
    fn a_codebook_in_the_wrong_ann_space_for_the_metric_is_refused() {
        use graph_format::manifest::Metric;
        // Files built honestly for cosine (codebook dim = 4)...
        let (dir, reader, pq, mut desc) = vamana_pair("space", Metric::Cosine, 2, false, 6);
        assert_eq!(pq.codebook.params.dim, 4);
        // ...but the MANIFEST claims dot, whose ANN space is dim + dsub = 6 over 3 subspaces.
        desc.metric = Metric::Dot;
        let err = validate_vamana_index(
            "x",
            &reader,
            &pq,
            &desc,
            0,
            graph_format::manifest::AnnNav::Augmented,
            2,
            4,
        )
        .expect_err("a codebook in the wrong space for the declared metric must not open");
        assert!(
            err.to_string().contains("different spaces"),
            "unexpected: {err:#}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// HIK-137: the reader must **dispatch on `nav`, or refuse legibly**. A genuine IP-native index
    /// (raw codebook, plain `PqParams`) validates as `InnerProduct`; the *same* raw codebook
    /// validated as `Augmented`, and an augmented Dot codebook validated as `InnerProduct`, are both
    /// refused for the space mismatch — so a Dot index built the old (augmented) way can never be
    /// mis-navigated by the IP-ADC read path, and vice versa.
    #[test]
    fn ip_native_index_validates_and_a_space_nav_mismatch_is_refused() {
        use graph_format::manifest::{AnnMode, AnnNav, Metric, VectorIndexDesc};
        use graph_format::pq::{
            ann_point, ann_pq_params, l2_norm, train_codebooks, PqParams, PqWriter,
        };
        use graph_format::vamana::{VamanaReader, VamanaWriter};

        let (dim, n, subspaces, bits) = (4usize, 6usize, 2u32, 4u32);
        let dir = std::env::temp_dir().join(format!("slater_ipval_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let raw: Vec<Vec<f32>> = (0..n)
            .map(|i| (0..dim).map(|d| (i + d) as f32 + 1.0).collect())
            .collect();

        // A `.vamana` over the raw vectors, complete adjacency (no orphan medoid).
        let mut vw = VamanaWriter::create_with_cipher(dir.join("x.vamana"), 4096, 3, None).unwrap();
        for (i, v) in raw.iter().enumerate() {
            let nbrs: Vec<u32> = (0..n as u32).filter(|&j| j != i as u32).collect();
            vw.append(v, &nbrs).unwrap();
        }
        vw.finish().unwrap();
        let reader = VamanaReader::open_with_cipher(dir.join("x.vamana"), None).unwrap();

        let mk_desc = |nav| VectorIndexDesc {
            label: "Doc".into(),
            property: "embedding".into(),
            dim: dim as u32,
            metric: Metric::Dot,
            count: n as u64,
            first_record: 0,
            mode: AnnMode::Vamana {
                r: 8,
                alpha: 1.2,
                medoid: 0,
                pq_subspaces: subspaces,
                pq_bits: bits,
                live_count: n as u64,
                max_norm: 1.0,
                nav,
            },
        };
        let write_pq = |name: &str, cb: &graph_format::pq::Codebook, encoded: &[Vec<f32>]| {
            let mut pw = PqWriter::create_with_cipher(dir.join(name), cb, 4096, 3, None).unwrap();
            for p in encoded {
                pw.append_codes(0, &cb.encode(p).unwrap()).unwrap();
            }
            pw.finish().unwrap();
            PqReader::open_with_cipher(dir.join(name), None)
                .unwrap()
                .load_resident()
                .unwrap()
        };

        // (1) A genuine IP-native codebook: plain PqParams over the RAW vectors.
        let ip_cb =
            train_codebooks(&raw, PqParams::new(dim as u32, subspaces, bits).unwrap(), 5).unwrap();
        let ip_pq = write_pq("ip.pq", &ip_cb, &raw);
        validate_vamana_index(
            "x",
            &reader,
            &ip_pq,
            &mk_desc(AnnNav::InnerProduct),
            0,
            AnnNav::InnerProduct,
            subspaces,
            bits,
        )
        .expect("a raw codebook under InnerProduct must validate");
        // The SAME raw codebook validated as Augmented (Dot) expects the augmented space
        // (dim + dsub), so it is refused — no silent mis-navigation.
        let err = validate_vamana_index(
            "x",
            &reader,
            &ip_pq,
            &mk_desc(AnnNav::Augmented),
            0,
            AnnNav::Augmented,
            subspaces,
            bits,
        )
        .expect_err("a raw codebook under Augmented must be refused");
        assert!(
            err.to_string().contains("different spaces"),
            "unexpected: {err:#}"
        );

        // (2) An augmented Dot codebook validated as InnerProduct must ALSO be refused: the IP-ADC
        // read path must never navigate an augmented codebook.
        let aug_params = ann_pq_params(Metric::Dot, dim as u32, subspaces, bits).unwrap();
        let max_norm = raw.iter().map(|v| l2_norm(v)).fold(0.0f64, f64::max);
        let aug_points: Vec<Vec<f32>> = raw
            .iter()
            .map(|v| ann_point(Metric::Dot, v, max_norm, aug_params.dim as usize).unwrap())
            .collect();
        let aug_cb = train_codebooks(&aug_points, aug_params, 5).unwrap();
        let aug_pq = write_pq("aug.pq", &aug_cb, &aug_points);
        let err = validate_vamana_index(
            "x",
            &reader,
            &aug_pq,
            &mk_desc(AnnNav::InnerProduct),
            0,
            AnnNav::InnerProduct,
            subspaces,
            bits,
        )
        .expect_err("an augmented codebook under InnerProduct must be refused");
        assert!(
            err.to_string().contains("different spaces"),
            "unexpected: {err:#}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// HIK-137 phase 4: the hole the space check (above) **cannot** close. For cosine and L2 the
    /// ANN codebook is `PqParams::new(dim, …)` — the *same width* as a raw IP codebook (only Dot
    /// augments) — so a forged/bit-rotted `nav: inner_product` on a cosine/L2 index passes the
    /// space check and would then be navigated by `AdcTable::new_ip`, silently mis-navigating a
    /// cosine/L2 graph by inner product. `validate_vamana_index` must refuse the `(metric, nav)`
    /// mismatch outright, with the typed `NavMetricMismatch` a caller can branch on.
    #[test]
    fn a_forged_inner_product_nav_on_a_cosine_or_l2_index_is_refused() {
        use graph_format::manifest::{AnnNav, Metric, NavMetricMismatch};
        for metric in [Metric::Cosine, Metric::L2] {
            let tag = format!("navmix_{metric:?}");
            // Built honestly for cosine/L2 — the codebook width equals a raw IP codebook's.
            let (dir, reader, pq, desc) = vamana_pair(&tag, metric, 2, false, 6);
            // Sanity: pre-guard, the codebook-space check alone would pass — a cosine/L2 codebook
            // *is* PqParams::new(dim, …), which is exactly what InnerProduct expects. So without the
            // (metric, nav) guard this whole call returns Ok — the mis-navigation hole.
            assert_eq!(
                pq.codebook.params,
                graph_format::pq::PqParams::new(desc.dim, 2, 4).unwrap(),
                "a {metric:?} codebook shares the raw IP codebook width — the space check is blind here"
            );
            let err = validate_vamana_index(
                "x",
                &reader,
                &pq,
                &desc,
                0,
                AnnNav::InnerProduct, // forged: this index is cosine/L2, not Dot
                2,
                4,
            )
            .expect_err(
                "nav=inner_product on a cosine/L2 index must be refused, not mis-navigated",
            );
            // Typed, not message-matched (house rule): a caller branches on the kind of rejection.
            let typed = err
                .downcast_ref::<NavMetricMismatch>()
                .unwrap_or_else(|| panic!("expected a typed NavMetricMismatch, got: {err:#}"));
            assert_eq!(typed.metric, metric);
            let _ = std::fs::remove_dir_all(&dir);
        }

        // The guard rejects only the *forged* pairing — a genuine cosine index with no nav
        // (Augmented) still opens, so this is not blanket rejection.
        let (dir, reader, pq, desc) = vamana_pair("navmix_ok", Metric::Cosine, 2, false, 6);
        validate_vamana_index("x", &reader, &pq, &desc, 0, AnnNav::Augmented, 2, 4)
            .expect("a cosine index navigated Augmented must still open");
        let _ = std::fs::remove_dir_all(&dir);
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
