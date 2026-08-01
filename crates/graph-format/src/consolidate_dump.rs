// SPDX-License-Identifier: Apache-2.0
//! The binary consolidation dump — the intermediate `slater` hands `slater-build`
//! for a *direct* consolidation rebuild (BUILD-PERF Phase 0 / the segmented-core
//! plan's quick win).
//!
//! Consolidation is dump-and-rebuild: the server serialises the merged
//! `core ⊕ delta` view, then `slater-build` rebuilds a fresh generation from it.
//! Historically that intermediate was business-key `MERGE` **Cypher text**
//! ([`crate::…`] — actually `slater/src/consolidate.rs`): ~150 s to write and ~58 s
//! to re-parse at 10 M nodes, because the builder must re-parse every statement and
//! re-resolve every business key it just took apart. This module is the binary
//! replacement: the dump already carries dense ids and global symbol ids, so the
//! builder skips parse, node dedup, and endpoint resolution entirely, entering its
//! pipeline at the post-resolve `EdgeRec`/`NodeRec` shape.
//!
//! # Layout — a directory of four files
//! - `meta.json` — [`DumpMeta`]: magic/version, node & edge counts, the global
//!   `labels` / `reltypes` / `property_keys` symbol tables (so record ids are
//!   self-describing), and the range- and vector-index declarations to recreate.
//! - `nodes.blk` — a [`BlockFileWriter`] container, one record per surviving node in
//!   ascending **compacted** dense-id order (record index *is* the new node id).
//!   A record is `blob(labels_record) ‖ blob(props_record)` where the two inner
//!   records use the canonical [`encode_labels_record`] / [`encode_props_record`]
//!   layouts with **global** symbol ids — so the builder byte-copies each blob
//!   straight into its node store with no re-encode.
//! - `edges.blk` — a [`BlockFileWriter`] container, one record per edge in emit
//!   order: `uvarint(src) ‖ uvarint(dst) ‖ uvarint(reltype) ‖ blob(props_record)`,
//!   endpoints already resolved to compacted node ids.
//! - `vectors.blk` — one record per embedding, ascending by `(node_id, key_id)`:
//!   `uvarint(node_id) ‖ uvarint(key_id) ‖ uvarint(dim) ‖ dim × f32(LE)`. Embeddings
//!   need their own stream because an *indexed* one is routed **out** of the column
//!   store (D12) and so is simply absent from a node's props record — before this
//!   file existed, a consolidation silently rebuilt the graph with no vectors and no
//!   vector indexes at all.
//!
//! # Why binary, not the text dialect
//! The text dump reads *both endpoints' node records per edge* to recover their
//! business keys; the binary dump carries the endpoint ids directly, so the whole
//! dump side is O(nodes + edges) block copies with no per-edge lookups. Parallel
//! edges are preserved verbatim (the text `MERGE` dialect silently collapsed
//! `(a)-[:T]->(b)` duplicates; the binary path does not).
//!
//! # Determinism
//! Nodes are emitted in ascending compacted-id order; edges in adjacency-walk order
//! over compacted sources; symbol tables in manifest order followed by first-seen
//! delta additions. A fixed `(core, delta)` therefore dumps byte-identically — the
//! property the consolidation golden gate rests on.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::columns::encode_props_record_into;
use crate::crypto::{open_blob, seal_blob, AeadRejected, BlockCipher, FileCipher};
use crate::ids::{Generation, Value};
use crate::manifest::{AnnNav, EntityKind, Metric};
use crate::nodelabels::encode_labels_record_into;
use crate::wire::{read_uvarint, write_uvarint};

/// Magic at the head of `meta.json`, distinguishing a consolidation dump from a
/// generation image (`SLATER01`).
pub const DUMP_MAGIC: &str = "SLDUMP01";

/// Dump-format version. Bumped on any incompatible change to the record or meta
/// layout; the builder refuses a dump whose version it does not understand.
///
/// v3 (HIK-117): a Vamana vector index can be **carried by reference** — its base
/// `.vamana`/`.pq` are referenced (not re-dumped) and only a `layout → new-id` map plus
/// the Δ vectors travel, so the builder folds writes in with `streaming_merge` instead
/// of rebuilding the graph from zero. See [`DumpVectorCarry`].
pub const DUMP_VERSION: u32 = 3;

/// zstd level for the transient dump files. Low: the dump is written once and read
/// once, so build/parse CPU dominates any saved bytes.
pub const DUMP_ZSTD: i32 = 3;

/// Target block size (bytes) for the dump's `.blk` files, matching the builder's
/// transient-bucket block size (`build_external::BUCKET_BLOCK`).
pub const DUMP_BLOCK: usize = 1 << 20;

const META_FILE: &str = "meta.json";
const NODES_FILE: &str = "nodes.blk";
const EDGES_FILE: &str = "edges.blk";
const VECTORS_FILE: &str = "vectors.blk";

/// Magic on a **sealed** `meta.json` / vector-carry sidecar (HIK-149).
///
/// # Why only the sealed arm carries a magic
///
/// The plaintext arm of both files is byte-for-byte what an unkeyed deployment wrote
/// before this existed — raw JSON, raw `u64`s — so an unkeyed consolidation pays nothing
/// and its dump stays byte-identical. That is possible because the two arms are trivially
/// distinguishable without a plaintext header: `serde_json::to_vec_pretty` always begins
/// `{`, and a carry sidecar's first `u64` is a node id or [`crate::pq::HOLE`], neither of
/// which can spell this magic.
///
/// (The `.blk` files need none of this — [`BlockFileWriter`] already writes a different
/// magic for a sealed file and [`BlockFileReader::open_src`] already refuses both
/// mismatches, HIK-146.)
const SEALED_BLOB_MAGIC: &[u8; 8] = b"SLDSEAL1";

/// The per-file subkey name a dump artifact is sealed under, in the delta key's namespace.
///
/// The `dump/` prefix is not decoration. The dump shares one key with the WAL and the L0
/// segments (all three are manifest-less artifacts of the same graph, so all three derive
/// from [`crate::crypto::derive_delta_key`]), and `slater-delta` seals its own files under
/// `wal/…` and `l0/…`. Without a prefix here, a dump `nodes.blk` and an off-heap L0
/// segment's `node.blk` could land on the same subkey, and a block of one would open as a
/// block of the other.
fn subkey_name(file: &str) -> String {
    format!("dump/{file}")
}

/// Bind an optional dump cipher to one file of the dump.
fn bind(cipher: Option<&Arc<BlockCipher>>, file: &str) -> Option<Arc<FileCipher>> {
    cipher.map(|c| Arc::new(c.for_file(&subkey_name(file))))
}

/// Seal one whole dump artifact written in a single shot (`meta.json`, a carry sidecar):
/// `MAGIC(8) ‖ nonce(24) ‖ ciphertext`, or `body` verbatim with no key.
///
/// Returns a [`Cow`] so the unkeyed arm **borrows**. That is not tidiness: a carry sidecar
/// is 8 bytes per base `.vamana` record — ~733 MB at 91.6 M nodes — and copying it here
/// would hold two of them live at once, on the plaintext deployments that gain nothing from
/// this function at all.
fn seal_whole<'a>(cipher: Option<&Arc<FileCipher>>, body: &'a [u8]) -> Result<Cow<'a, [u8]>> {
    let Some(c) = cipher else {
        return Ok(Cow::Borrowed(body));
    };
    let sealed = seal_blob(c, 0, body)?;
    let mut out = Vec::with_capacity(SEALED_BLOB_MAGIC.len() + sealed.len());
    out.extend_from_slice(SEALED_BLOB_MAGIC);
    out.extend_from_slice(&sealed);
    Ok(Cow::Owned(out))
}

/// The inverse of [`seal_whole`], with the **symmetric** key policy: a sealed artifact with
/// no key is refused, and — the direction that matters — a *plaintext* artifact under a
/// configured key is refused too.
///
/// The second half is the substitution HIK-146 found one level down in `BlockFileReader`.
/// A dump has no manifest enumerating its files, so nothing else would notice a sealed
/// `meta.json` swapped for a plaintext one naming a graph of the attacker's choosing.
/// Both refusals are typed ([`AeadRejected`]), never message text.
///
/// [`Cow`] for the same reason as [`seal_whole`]: the unkeyed arm must not copy a
/// several-hundred-megabyte sidecar just to hand it back.
fn open_whole<'a>(
    cipher: Option<&Arc<FileCipher>>,
    bytes: &'a [u8],
    subject: &'static str,
) -> Result<Cow<'a, [u8]>> {
    let sealed = bytes.starts_with(SEALED_BLOB_MAGIC);
    match (sealed, cipher) {
        (true, None) => Err(AeadRejected::KeyRequired { subject }.into()),
        (false, Some(_)) => Err(AeadRejected::Unsealed { subject }.into()),
        (false, None) => Ok(Cow::Borrowed(bytes)),
        (true, Some(c)) => open_blob(c, 0, &bytes[SEALED_BLOB_MAGIC.len()..]).map(Cow::Owned),
    }
}

/// A range index to recreate on the rebuilt generation. Mirrors the text dump's
/// `CREATE INDEX FOR (n:Label) ON (n.prop)` DDL — only the entity, label/type and
/// property matter; the builder assigns its own on-disk index stem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DumpRangeIndex {
    pub entity: EntityKind,
    pub label_or_type: String,
    pub property: String,
}

/// A vector index to recreate on the rebuilt generation. The ANN parameters (`R`,
/// `alpha`, PQ subspaces/bits, the ANN threshold) are *build* options, not per-index
/// state, so a *fresh* rebuild re-derives them exactly as a fresh build would — only the
/// declaration travels.
///
/// A **carried** Vamana index ([`carry`](Self::carry) is `Some`) is different: its graph
/// is not re-dumped and not rebuilt. The base's build params travel in the [`DumpVectorCarry`]
/// (they are per-index state that a fresh build would re-derive *differently* — see
/// [`AnnMode::Vamana::max_norm`](crate::manifest::AnnMode)), and the builder runs
/// `streaming_merge` over the referenced base rather than `build_vamana_index`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DumpVectorIndex {
    pub label: String,
    pub property: String,
    pub dim: u32,
    pub metric: Metric,
    /// Present iff this index is carried by reference (a Vamana base folded via
    /// `streaming_merge`); `None` for a brute-force index or a from-scratch rebuild.
    #[serde(default)]
    pub carry: Option<DumpVectorCarry>,
}

/// Everything the builder needs to carry a Vamana index by reference instead of
/// rebuilding it — the header of a `streaming_merge` (HIK-115/S6). Small scalars only;
/// the `layout → dump-id` table is a **binary sidecar** ([`carry_map_file`](Self::carry_map_file)),
/// never inlined into `meta.json` (at 91.6M nodes it is ~733 MB of `u64`s).
///
/// # The paths are references, on purpose
/// [`base_vamana`](Self::base_vamana) / [`base_pq`](Self::base_pq) are **data-dir-relative**
/// (`<graph>/<base_uuid>/vector/<l>.<p>.{vamana,pq}`); the builder joins them with its own
/// `--data-dir`. The `.vamana` holds the full raw vectors (the ~370 GB the carry exists to
/// avoid re-reading), so `streaming_merge` **hard-links** it — the base and the new
/// generation live under the same `data_dir`, and the old generation keeps serving until
/// the swap, so the referenced files are alive for the whole build.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DumpVectorCarry {
    /// Data-dir-relative path to the base `.vamana` — either inside the base generation's
    /// directory, or inside a `vecidx/<uuid>/` artifact when an earlier consolidation
    /// already carried it (see [`Self::base_vamana_artifact`]).
    pub base_vamana: String,
    /// Data-dir-relative path to the base `.pq`. Always inside the base **generation**:
    /// every merge rewrites the id column, so the `.pq` is never carried.
    pub base_pq: String,
    /// The base generation's uuid. The builder reads and authenticates that generation's
    /// `MANIFEST.json` to learn the salt its `.pq` (and, absent
    /// [`Self::base_vamana_artifact`], its `.vamana`) was sealed under — the salt is never
    /// copied into the dump (HIK-145). `None` on a pre-HIK-145 dump, which can only be
    /// carried when the image is plaintext.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_gen: Option<Generation>,
    /// Set when the base `.vamana` is already a carried artifact: its own
    /// `vecidx/<uuid>/VECIDX.json` records the salt **and** the HIK-140 subkey label the
    /// file was sealed under, neither of which is derivable from where the file now sits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_vamana_artifact: Option<Generation>,
    /// Dump-relative filename of the raw-`u64`-LE `layout → dump-id` sidecar (`HOLE` for a
    /// tombstoned/superseded/deleted ordinal), one entry per base `.vamana` record.
    pub carry_map_file: String,
    /// Base record count = the sidecar's entry count; the builder validates the two agree.
    pub base_records: u64,
    /// Carried base build params (`AnnMode::Vamana`) — re-deriving them from the survivors
    /// would silently move the ANN space (see the `max_norm` note on `AnnMode::Vamana`).
    pub r: u32,
    pub alpha: f32,
    pub medoid: u64,
    pub max_norm: f32,
    pub pq_subspaces: u32,
    pub pq_bits: u32,
    /// How the carried base graph is navigated (HIK-137). **Additive-optional**: absent on a
    /// pre-HIK-137 dump ⇒ [`AnnNav::Augmented`]. Threaded so a merged/consolidated IP-native base
    /// re-emits a manifest that still carries `nav: inner_product` — a Dot base must not silently
    /// become augmented across a consolidation.
    #[serde(default, skip_serializing_if = "AnnNav::is_augmented")]
    pub nav: AnnNav,
}

/// The dump's `meta.json`: everything the builder needs that is not in the
/// `.blk` streams. Symbol tables make the record ids self-describing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DumpMeta {
    pub magic: String,
    pub version: u32,
    pub node_count: u64,
    pub edge_count: u64,
    /// Global label symbol table: a node record's label ids index this.
    pub labels: Vec<String>,
    /// Global relationship-type symbol table: an edge record's `reltype` indexes this.
    pub reltypes: Vec<String>,
    /// Global property-key symbol table: a props record's `key_id`s index this.
    pub property_keys: Vec<String>,
    /// Range indexes to recreate on the rebuilt generation.
    pub range_indexes: Vec<DumpRangeIndex>,
    /// Vector indexes to recreate. Empty for a graph that carries none.
    #[serde(default)]
    pub vector_indexes: Vec<DumpVectorIndex>,
}

/// Append `blob(rec) = uvarint(len) ‖ rec`, so a reader can split concatenated
/// self-delimiting records without a separator.
fn write_blob(buf: &mut Vec<u8>, rec: &[u8]) {
    write_uvarint(buf, rec.len() as u64);
    buf.extend_from_slice(rec);
}

/// Split one `blob` off the front of `r`, returning its bytes and advancing `r`.
fn read_blob<'a>(r: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = read_uvarint(r)? as usize;
    if r.len() < len {
        bail!("dump blob truncated: want {len}, have {}", r.len());
    }
    let (blob, rest) = r.split_at(len);
    *r = rest;
    Ok(blob)
}

/// Read a carried Vamana index's `layout → dump-id` sidecar back as a `Vec<u64>` (`HOLE`
/// for a tombstoned/superseded/deleted ordinal), decrypting it under `cipher` if the dump
/// was sealed. `expected` is the base record count from [`DumpVectorCarry::base_records`]; a
/// length mismatch is a corrupt dump and is refused rather than silently truncated — the
/// table indexes the base `.vamana` by position, so a wrong length would misalign every id.
///
/// # Why this is a free function
///
/// `slater-build` resolves the sidecar to an absolute path early (`VectorCarry::carry_map_path`)
/// and reads it far from any [`DumpReader`], so it had grown **its own copy** of this decode
/// in `shared.rs`. That copy is what HIK-149 first broke: sealing the sidecar made the
/// `DumpReader` method decrypt and left the production reader measuring ciphertext, which
/// surfaced as "holds 406 ids but the dump declares 400 base records". One implementation,
/// reachable from a path alone, so the sealed and unsealed readers cannot disagree again.
pub fn read_vector_carry_at(
    path: &Path,
    expected: u64,
    cipher: Option<&Arc<BlockCipher>>,
) -> Result<Vec<u64>> {
    let file = path
        .file_name()
        .and_then(|n| n.to_str())
        .with_context(|| format!("vector carry sidecar {} has no file name", path.display()))?;
    let stored = std::fs::read(path)
        .with_context(|| format!("read vector carry sidecar {}", path.display()))?;
    let bytes = open_whole(
        bind(cipher, file).as_ref(),
        &stored,
        "consolidation dump vector-carry sidecar",
    )
    .with_context(|| format!("open vector carry sidecar {}", path.display()))?;
    if bytes.len() % 8 != 0 {
        bail!(
            "vector carry sidecar {} has {} bytes, not a whole number of u64s",
            path.display(),
            bytes.len()
        );
    }
    let n = bytes.len() / 8;
    if n as u64 != expected {
        bail!(
            "vector carry sidecar {} holds {n} ids but the carry declares {expected} base \
             records — they index the base .vamana by position",
            path.display()
        );
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect())
}

/// Streaming writer for a consolidation dump directory. Append nodes in ascending
/// compacted-id order, then edges in emit order, then [`finish`](DumpWriter::finish)
/// with the symbol tables and index DDL.
pub struct DumpWriter {
    dir: PathBuf,
    /// The dump's cipher, or `None` for an unkeyed deployment. Held (rather than only the
    /// per-file ciphers derived up front) because the vector-carry sidecars are named after
    /// index stems that are not known until they are written.
    cipher: Option<Arc<BlockCipher>>,
    nodes: BlockFileWriter,
    edges: BlockFileWriter,
    vectors: BlockFileWriter,
    node_count: u64,
    edge_count: u64,
    vector_count: u64,
    last_vector_key: Option<(u64, u32)>,
    node_scratch: Vec<u8>,
    edge_scratch: Vec<u8>,
    vector_scratch: Vec<u8>,
    props_scratch: Vec<u8>,
}

impl DumpWriter {
    /// Create the dump directory and its three block files. `dir` is created (and its
    /// parents) if absent; existing `nodes.blk`/`edges.blk` are overwritten.
    ///
    /// `cipher` is the dump's at-rest key ([`crate::crypto::delta_cipher`] over the graph
    /// name), or `None` for an unkeyed deployment — and it is a **required argument, not a
    /// defaulted one** (HIK-149). There is deliberately no plaintext-by-default
    /// constructor beside this: the whole finding was that writing the entire merged graph
    /// in the clear was what happened when nobody at the call site mentioned encryption, and
    /// a `create(dir)` overload would restore exactly that.
    pub fn create(dir: impl AsRef<Path>, cipher: Option<Arc<BlockCipher>>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create dump dir {}", dir.display()))?;
        let mk = |file: &str| {
            BlockFileWriter::create_with_cipher(
                dir.join(file),
                DUMP_BLOCK,
                DUMP_ZSTD,
                bind(cipher.as_ref(), file),
            )
        };
        let nodes = mk(NODES_FILE)?;
        let edges = mk(EDGES_FILE)?;
        let vectors = mk(VECTORS_FILE)?;
        Ok(Self {
            dir,
            cipher,
            nodes,
            edges,
            vectors,
            node_count: 0,
            edge_count: 0,
            vector_count: 0,
            last_vector_key: None,
            node_scratch: Vec::new(),
            edge_scratch: Vec::new(),
            vector_scratch: Vec::new(),
            props_scratch: Vec::new(),
        })
    }

    /// Append one node's `(labels, props)` in **global** symbol ids. The append
    /// position is the node's compacted dense id, so nodes must be appended in
    /// ascending id order with no gaps (tombstoned ids elided, ids renumbered).
    pub fn append_node(&mut self, labels: &[u32], props: &[(u32, Value)]) -> Result<()> {
        self.node_scratch.clear();
        self.props_scratch.clear();
        encode_labels_record_into(&mut self.props_scratch, labels);
        write_blob(&mut self.node_scratch, &self.props_scratch);
        self.props_scratch.clear();
        encode_props_record_into(&mut self.props_scratch, props);
        write_blob(&mut self.node_scratch, &self.props_scratch);
        self.nodes.append_record(&self.node_scratch)?;
        self.node_count += 1;
        Ok(())
    }

    /// Append one edge with endpoints already resolved to compacted node ids and a
    /// **global** reltype id. `props` are in global key ids.
    pub fn append_edge(
        &mut self,
        src: u64,
        dst: u64,
        reltype: u32,
        props: &[(u32, Value)],
    ) -> Result<()> {
        self.edge_scratch.clear();
        write_uvarint(&mut self.edge_scratch, src);
        write_uvarint(&mut self.edge_scratch, dst);
        write_uvarint(&mut self.edge_scratch, reltype as u64);
        self.props_scratch.clear();
        encode_props_record_into(&mut self.props_scratch, props);
        write_blob(&mut self.edge_scratch, &self.props_scratch);
        self.edges.append_record(&self.edge_scratch)?;
        self.edge_count += 1;
        Ok(())
    }

    /// Append a node whose label and property records are **already encoded** in the
    /// canonical [`encode_labels_record`](crate::nodelabels::encode_labels_record) /
    /// [`encode_props_record`](crate::columns::encode_props_record) layouts with global
    /// symbol ids — a byte-copy from a base generation's stores, skipping the
    /// decode + re-encode [`append_node`](Self::append_node) does. The caller must
    /// have seeded the dump's symbol tables from the base manifest so the record's
    /// ids stay valid. Same append-order contract as [`append_node`](Self::append_node).
    pub fn append_node_raw(&mut self, labels_rec: &[u8], props_rec: &[u8]) -> Result<()> {
        self.node_scratch.clear();
        write_blob(&mut self.node_scratch, labels_rec);
        write_blob(&mut self.node_scratch, props_rec);
        self.nodes.append_record(&self.node_scratch)?;
        self.node_count += 1;
        Ok(())
    }

    /// Append an edge whose property record is **already encoded** (see
    /// [`append_node_raw`](Self::append_node_raw)); `reltype` is a base/global reltype id.
    pub fn append_edge_raw(
        &mut self,
        src: u64,
        dst: u64,
        reltype: u32,
        props_rec: &[u8],
    ) -> Result<()> {
        self.edge_scratch.clear();
        write_uvarint(&mut self.edge_scratch, src);
        write_uvarint(&mut self.edge_scratch, dst);
        write_uvarint(&mut self.edge_scratch, reltype as u64);
        write_blob(&mut self.edge_scratch, props_rec);
        self.edges.append_record(&self.edge_scratch)?;
        self.edge_count += 1;
        Ok(())
    }

    /// Append one node's embedding: `uvarint(node_id) ‖ uvarint(key_id) ‖ uvarint(dim)
    /// ‖ dim × f32(LE)`, where `node_id` is the **compacted** dense id and `key_id`
    /// indexes the dump's `property_keys`.
    ///
    /// Must be called in ascending `(node_id, key_id)` order — the builder merge-joins
    /// this stream against the ascending node stream in one pass, so an out-of-order
    /// append would silently attach an embedding to the wrong node (or drop it). The
    /// order is asserted rather than trusted: a violation is a bug, never data loss.
    pub fn append_vector(&mut self, node_id: u64, key_id: u32, vector: &[f32]) -> Result<()> {
        if let Some(last) = self.last_vector_key {
            if (node_id, key_id) <= last {
                bail!(
                    "dump vectors must be appended in ascending (node_id, key_id) order: \
                     ({node_id}, {key_id}) follows ({}, {})",
                    last.0,
                    last.1
                );
            }
        }
        self.last_vector_key = Some((node_id, key_id));
        self.vector_scratch.clear();
        write_uvarint(&mut self.vector_scratch, node_id);
        write_uvarint(&mut self.vector_scratch, key_id as u64);
        write_uvarint(&mut self.vector_scratch, vector.len() as u64);
        for x in vector {
            self.vector_scratch.extend_from_slice(&x.to_le_bytes());
        }
        self.vectors.append_record(&self.vector_scratch)?;
        self.vector_count += 1;
        Ok(())
    }

    /// Write a carried Vamana index's `layout → dump-id` table to a binary sidecar in the
    /// dump directory and return its dump-relative filename (for [`DumpVectorCarry::carry_map_file`]).
    ///
    /// Raw little-endian `u64`, one per base `.vamana` record, `HOLE` ([`crate::pq::HOLE`])
    /// for a tombstoned/superseded/deleted ordinal. Not put in `meta.json`: at scale this is
    /// hundreds of MB, which JSON would both bloat and read back slowly.
    ///
    /// `ord` is the index's position in the dump's `vector_indexes`, and it is what makes
    /// the filename **injective** (HIK-158). `stem` (`label.property`) is decoration, kept
    /// only so a human reading the scratch directory can tell the sidecars apart — it is
    /// still sanitised, because that is what stops an odd label escaping the directory, but
    /// sanitising is *not* injective and must never be the only thing separating two files:
    ///
    /// * `(:Doc {` b.a `})` → stem `Doc.b.a` → `carry_Doc_b_a`
    /// * ``(:`Doc.b` {a})`` → stem `Doc.b.a` → `carry_Doc_b_a`
    ///
    /// Labels and property keys are arbitrary strings over Bolt, so that collision is
    /// reachable rather than theoretical. `std::fs::write` truncates, so before this the
    /// second index silently overwrote the first's table — and nothing downstream caught it:
    /// a shared name means a shared per-file subkey, so the AEAD passes, and
    /// `read_vector_carry_at`'s length check only fires when the two indexes happen to have
    /// *different* base record counts. With equal counts the first index would carry the
    /// second's `layout → dump-id` table, mapping every base ordinal to the wrong node and
    /// returning wrong neighbours with no error and exit 0.
    pub fn write_vector_carry(
        &mut self,
        ord: usize,
        stem: &str,
        layout_to_dump_id: &[u64],
    ) -> Result<String> {
        let safe: String = stem
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let file = format!("carry.{ord}.{safe}.ids");
        let mut buf: Vec<u8> = Vec::with_capacity(layout_to_dump_id.len() * 8);
        for &id in layout_to_dump_id {
            buf.extend_from_slice(&id.to_le_bytes());
        }
        let path = self.dir.join(&file);
        // Sealed like `meta.json` (HIK-149). It is only an id table, but it states which
        // base records died and how the survivors renumber — the shape of the deletions
        // since the last build. Nothing else in the dump is readable on a keyed
        // deployment, and this must not be the exception that is.
        let framed = seal_whole(bind(self.cipher.as_ref(), &file).as_ref(), &buf)?;
        std::fs::write(&path, &framed)
            .with_context(|| format!("write vector carry sidecar {}", path.display()))?;
        Ok(file)
    }

    /// Number of nodes / edges / vectors appended so far.
    pub fn node_count(&self) -> u64 {
        self.node_count
    }
    pub fn edge_count(&self) -> u64 {
        self.edge_count
    }
    pub fn vector_count(&self) -> u64 {
        self.vector_count
    }

    /// Flush the block files and write `meta.json`. Consumes the writer.
    pub fn finish(
        self,
        labels: Vec<String>,
        reltypes: Vec<String>,
        property_keys: Vec<String>,
        range_indexes: Vec<DumpRangeIndex>,
        vector_indexes: Vec<DumpVectorIndex>,
    ) -> Result<()> {
        let node_count = self.node_count;
        let edge_count = self.edge_count;
        self.nodes.finish().context("finish dump nodes.blk")?;
        self.edges.finish().context("finish dump edges.blk")?;
        self.vectors.finish().context("finish dump vectors.blk")?;
        let meta = DumpMeta {
            magic: DUMP_MAGIC.to_string(),
            version: DUMP_VERSION,
            node_count,
            edge_count,
            labels,
            reltypes,
            property_keys,
            range_indexes,
            vector_indexes,
        };
        let json = serde_json::to_vec_pretty(&meta).context("serialise dump meta")?;
        // Sealed too, not merely authenticated (HIK-149). `meta.json` carries the graph's
        // entire symbol space — every label, every relationship type, every property key.
        // That is graph content, so leaving it legible would leave the finding half open.
        let framed = seal_whole(bind(self.cipher.as_ref(), META_FILE).as_ref(), &json)?;
        let meta_path = self.dir.join(META_FILE);
        std::fs::write(&meta_path, &framed)
            .with_context(|| format!("write dump meta {}", meta_path.display()))?;
        Ok(())
    }
}

/// Reader over a consolidation dump directory. Opens the metadata eagerly (small)
/// and streams the node / edge block files on demand.
pub struct DumpReader {
    dir: PathBuf,
    /// The dump's cipher, or `None` for an unkeyed deployment. Kept for the vector-carry
    /// sidecars, which are opened lazily by name.
    cipher: Option<Arc<BlockCipher>>,
    meta: DumpMeta,
    nodes: BlockFileReader,
    edges: BlockFileReader,
    vectors: BlockFileReader,
}

impl DumpReader {
    /// Open a dump directory: parse and validate `meta.json`, open the three block files.
    /// Errors if the magic/version is not understood or a file is missing.
    ///
    /// `cipher` must be the same key the writer used ([`DumpWriter::create`]) — for a
    /// consolidation, [`crate::crypto::delta_cipher`] over the graph name, which is why the
    /// server hands the builder the master key and the builder is told the graph by
    /// `--graph`. Every mismatch fails closed: a sealed dump with no key, a plaintext dump
    /// under a key, and a sealed dump under the wrong key are all refused, the first two
    /// with a typed [`AeadRejected`]. `meta.json` is read first, so on a whole-dump
    /// mismatch that is where the verdict comes from.
    pub fn open(dir: impl AsRef<Path>, cipher: Option<Arc<BlockCipher>>) -> Result<Self> {
        let dir = dir.as_ref();
        let meta_path = dir.join(META_FILE);
        let stored = std::fs::read(&meta_path)
            .with_context(|| format!("read dump meta {}", meta_path.display()))?;
        let json = open_whole(
            bind(cipher.as_ref(), META_FILE).as_ref(),
            &stored,
            "consolidation dump meta.json",
        )
        .with_context(|| format!("open dump meta {}", meta_path.display()))?;
        let meta: DumpMeta = serde_json::from_slice(&json)
            .with_context(|| format!("parse dump meta {}", meta_path.display()))?;
        if meta.magic != DUMP_MAGIC {
            bail!(
                "not a consolidation dump: magic {:?} != {DUMP_MAGIC:?}",
                meta.magic
            );
        }
        if meta.version != DUMP_VERSION {
            bail!(
                "unsupported consolidation dump version {} (this build understands {DUMP_VERSION})",
                meta.version
            );
        }
        let nodes = BlockFileReader::open_with_cipher(
            dir.join(NODES_FILE),
            bind(cipher.as_ref(), NODES_FILE),
        )?;
        let edges = BlockFileReader::open_with_cipher(
            dir.join(EDGES_FILE),
            bind(cipher.as_ref(), EDGES_FILE),
        )?;
        if nodes.total_records() != meta.node_count {
            bail!(
                "dump nodes.blk has {} records but meta declares {}",
                nodes.total_records(),
                meta.node_count
            );
        }
        if edges.total_records() != meta.edge_count {
            bail!(
                "dump edges.blk has {} records but meta declares {}",
                edges.total_records(),
                meta.edge_count
            );
        }
        let vectors = BlockFileReader::open_with_cipher(
            dir.join(VECTORS_FILE),
            bind(cipher.as_ref(), VECTORS_FILE),
        )?;
        Ok(Self {
            dir: dir.to_path_buf(),
            cipher,
            meta,
            nodes,
            edges,
            vectors,
        })
    }

    pub fn meta(&self) -> &DumpMeta {
        &self.meta
    }

    /// Read a carried Vamana index's `layout → dump-id` sidecar back as a `Vec<u64>`.
    /// Thin wrapper over [`read_vector_carry_at`], which is what `slater-build` calls.
    pub fn read_vector_carry(&self, file: &str, expected: u64) -> Result<Vec<u64>> {
        read_vector_carry_at(&self.dir.join(file), expected, self.cipher.as_ref())
    }

    /// Stream every node record in id order, handing the callback the raw
    /// (labels_record, props_record) blob slices — pre-encoded in the canonical
    /// [`encode_labels_record`](crate::nodelabels::encode_labels_record) /
    /// [`encode_props_record`](crate::columns::encode_props_record) layouts, so the
    /// builder byte-copies each straight into its stores.
    pub fn for_each_node(&self, mut f: impl FnMut(u64, &[u8], &[u8]) -> Result<()>) -> Result<()> {
        self.nodes.for_each_record(|id, rec| {
            let mut r = rec;
            let labels_blob = read_blob(&mut r)?;
            let props_blob = read_blob(&mut r)?;
            f(id, labels_blob, props_blob)
        })
    }

    /// Stream every edge record in emit order: `(edge_id, src, dst, reltype,
    /// props_record_blob)`, endpoints already compacted node ids.
    pub fn for_each_edge(
        &self,
        mut f: impl FnMut(u64, u64, u64, u32, &[u8]) -> Result<()>,
    ) -> Result<()> {
        self.edges.for_each_record(|id, rec| {
            let mut r = rec;
            let src = read_uvarint(&mut r)?;
            let dst = read_uvarint(&mut r)?;
            let reltype = read_uvarint(&mut r)? as u32;
            let props_blob = read_blob(&mut r)?;
            f(id, src, dst, reltype, props_blob)
        })
    }

    /// Stream every embedding in ascending `(node_id, key_id)` order: `(node_id,
    /// key_id, vector)`. `node_id` is a compacted dense id and `key_id` indexes
    /// [`DumpMeta::property_keys`], so the builder can merge-join this against the
    /// ascending node stream in a single pass.
    pub fn for_each_vector(
        &self,
        mut f: impl FnMut(u64, u32, Vec<f32>) -> Result<()>,
    ) -> Result<()> {
        self.vectors.for_each_record(|_, rec| {
            let mut r = rec;
            let node_id = read_uvarint(&mut r)?;
            let key_id = read_uvarint(&mut r)? as u32;
            let dim = read_uvarint(&mut r)? as usize;
            if r.len() < dim * 4 {
                bail!(
                    "dump vector for node {node_id} truncated: want {} bytes, have {}",
                    dim * 4,
                    r.len()
                );
            }
            let vector: Vec<f32> = r[..dim * 4]
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            f(node_id, key_id, vector)
        })
    }

    /// Total embeddings in the dump.
    pub fn vector_count(&self) -> u64 {
        self.vectors.total_records()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columns::decode_props;
    use crate::crypto::delta_cipher;
    use crate::nodelabels::decode_labels;

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("slater_dump_{}_{}", std::process::id(), name))
    }

    /// HIK-149 fixtures: the dump's cipher for one graph, and a marker that is graph
    /// content in every file a dump has — a label and a property key land in `meta.json`,
    /// a property value in `nodes.blk`.
    const MASTER: &[u8] = b"a runtime master key";
    const MARKER: &str = "HIK149-DUMP-MARKER-4d2f";

    fn dump_cipher(graph: &str) -> Arc<BlockCipher> {
        Arc::new(delta_cipher(MASTER, graph))
    }

    /// `DumpReader::open`'s error, or a panic. (`DumpReader` is not `Debug`, so
    /// `unwrap_err` is unavailable — the same shape `rejects_foreign_magic` uses.)
    fn open_err(dir: &Path, cipher: Option<Arc<BlockCipher>>) -> anyhow::Error {
        match DumpReader::open(dir, cipher) {
            Ok(_) => panic!("expected the dump open to be refused"),
            Err(e) => e,
        }
    }

    /// Write a small dump carrying `MARKER` as a label, a property key and a property
    /// value, plus a vector-carry sidecar. Returns the sidecar's filename.
    fn write_marked_dump(dir: &Path, cipher: Option<Arc<BlockCipher>>) -> String {
        let _ = std::fs::remove_dir_all(dir);
        let mut w = DumpWriter::create(dir, cipher).unwrap();
        w.append_node(&[0], &[(0, Value::Str(MARKER.into()))])
            .unwrap();
        w.append_edge(0, 0, 0, &[]).unwrap();
        w.append_vector(0, 0, &[1.0, 2.0]).unwrap();
        let carry = w
            .write_vector_carry(0, "Doc.embedding", &[7, HOLE_ID, 3])
            .unwrap();
        w.finish(
            vec![MARKER.into()],
            vec!["KNOWS".into()],
            vec![MARKER.into()],
            vec![],
            vec![],
        )
        .unwrap();
        carry
    }

    const HOLE_ID: u64 = crate::pq::HOLE;

    /// Every regular file in the dump directory that contains `MARKER` verbatim.
    fn files_leaking_marker(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if !p.is_file() {
                continue;
            }
            let b = std::fs::read(&p).unwrap();
            if b.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()) {
                out.push(p.file_name().unwrap().to_string_lossy().to_string());
            }
        }
        out.sort();
        out
    }

    /// HIK-149 — the finding itself. A dump written with a key must put **nothing** in the
    /// clear, and must read back identically under that key.
    ///
    /// The unkeyed arm is asserted in the same test rather than a separate one, because the
    /// point is the contrast: the same call with `None` leaks the marker from both the
    /// symbol table (`meta.json`) and the node record (`nodes.blk`). That is what
    /// consolidation of an encrypted graph did on every run.
    #[test]
    fn a_sealed_dump_puts_nothing_in_the_clear_and_round_trips() {
        let plain_dir = tmp("seal_plain");
        write_marked_dump(&plain_dir, None);
        assert_eq!(
            files_leaking_marker(&plain_dir),
            vec!["meta.json".to_string(), "nodes.blk".to_string()],
            "the unkeyed dump is the pre-HIK-149 shape: graph content in the clear"
        );

        let dir = tmp("seal_sealed");
        let carry = write_marked_dump(&dir, Some(dump_cipher("docs")));
        assert!(
            files_leaking_marker(&dir).is_empty(),
            "a sealed dump leaked: {:?}",
            files_leaking_marker(&dir)
        );

        // …and it is still a dump: same key, same contents.
        let r = DumpReader::open(&dir, Some(dump_cipher("docs"))).unwrap();
        assert_eq!(r.meta().labels, vec![MARKER]);
        assert_eq!(r.meta().property_keys, vec![MARKER]);
        let mut props = Vec::new();
        r.for_each_node(|_, _, pb| {
            props.extend(decode_props(pb).unwrap());
            Ok(())
        })
        .unwrap();
        assert_eq!(props, vec![(0, Value::Str(MARKER.into()))]);
        let mut vecs = Vec::new();
        r.for_each_vector(|id, k, v| {
            vecs.push((id, k, v));
            Ok(())
        })
        .unwrap();
        assert_eq!(vecs, vec![(0, 0, vec![1.0, 2.0])]);
        assert_eq!(r.read_vector_carry(&carry, 3).unwrap(), vec![7, HOLE_ID, 3]);

        let _ = std::fs::remove_dir_all(&plain_dir);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The key policy is **symmetric** and typed, in all three directions.
    ///
    /// The middle one is the substitution HIK-146 found a level down and this ticket's
    /// description warned would be here too: a dump has no manifest enumerating its files,
    /// so without this a plaintext `meta.json` dropped in place of the sealed one would be
    /// read straight into a rebuild of an encrypted graph.
    #[test]
    fn the_dump_key_policy_is_symmetric_and_typed() {
        let sealed = tmp("policy_sealed");
        write_marked_dump(&sealed, Some(dump_cipher("docs")));
        let plain = tmp("policy_plain");
        write_marked_dump(&plain, None);

        // Sealed, no key.
        let e = open_err(&sealed, None);
        assert_eq!(
            e.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::KeyRequired {
                subject: "consolidation dump meta.json"
            }),
            "got: {e:#}"
        );
        // Plaintext, under a key — the downgrade.
        let e = open_err(&plain, Some(dump_cipher("docs")));
        assert_eq!(
            e.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::Unsealed {
                subject: "consolidation dump meta.json"
            }),
            "got: {e:#}"
        );
        // Sealed, wrong key.
        let e = open_err(
            &sealed,
            Some(Arc::new(delta_cipher(b"another key", "docs"))),
        );
        assert_eq!(
            e.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch),
            "got: {e:#}"
        );

        let _ = std::fs::remove_dir_all(&sealed);
        let _ = std::fs::remove_dir_all(&plain);
    }

    /// A dump is bound to the **graph** it was dumped from, not merely to the master key.
    ///
    /// Same reasoning as the WAL's (HIK-146): one master key covers every graph on the
    /// deployment, so without the graph in the KDF preimage, graph `a`'s dump dropped into
    /// graph `b`'s consolidation would decrypt and rebuild `b` from `a`'s data. The server
    /// derives from the graph it dumped and `slater-build` from its `--graph`, so a
    /// mismatch can only be a substitution.
    #[test]
    fn a_sealed_dump_is_bound_to_its_graph() {
        let dir = tmp("seal_graphbound");
        write_marked_dump(&dir, Some(dump_cipher("docs")));
        let e = open_err(&dir, Some(dump_cipher("other")));
        assert_eq!(
            e.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::TagMismatch),
            "got: {e:#}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The per-file half of the same substitution: `meta.json` stays sealed, but one
    /// `.blk` is swapped for a plaintext block file of the attacker's choosing.
    ///
    /// `meta.json` is what delivers the verdict for a whole-dump mismatch, so a test that
    /// only replaced everything would never reach the block files. This one gets past the
    /// meta check and has to be refused by `BlockFileReader` itself.
    #[test]
    fn a_plaintext_blk_substituted_into_a_sealed_dump_is_refused() {
        let dir = tmp("seal_substitute");
        write_marked_dump(&dir, Some(dump_cipher("docs")));
        // A plaintext nodes.blk with one forged record, in place of the sealed one.
        let mut w = BlockFileWriter::create(dir.join(NODES_FILE), DUMP_BLOCK, DUMP_ZSTD).unwrap();
        w.append_record(b"forged").unwrap();
        w.finish().unwrap();

        let e = open_err(&dir, Some(dump_cipher("docs")));
        assert_eq!(
            e.downcast_ref::<AeadRejected>(),
            Some(&AeadRejected::Unsealed {
                subject: "block file"
            }),
            "a plaintext block file inside a sealed dump must be refused, got: {e:#}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An unkeyed deployment pays **nothing**: no magic, no nonce, no tag. `meta.json` is
    /// the same raw JSON it always was and the `.blk` files are the same plaintext
    /// containers, so a plaintext consolidation is byte-for-byte what it was before
    /// HIK-149 — which is why the sealed arm alone carries a magic.
    #[test]
    fn an_unkeyed_dump_is_unframed_plaintext() {
        let dir = tmp("seal_unkeyed");
        let carry = write_marked_dump(&dir, None);
        let meta = std::fs::read(dir.join(META_FILE)).unwrap();
        assert_eq!(meta[0], b'{', "meta.json must still be raw JSON");
        assert!(serde_json::from_slice::<DumpMeta>(&meta).is_ok());
        let ids = std::fs::read(dir.join(&carry)).unwrap();
        assert_eq!(ids.len(), 3 * 8, "the carry sidecar must still be raw u64s");
        for f in [NODES_FILE, EDGES_FILE, VECTORS_FILE] {
            assert!(
                !BlockFileReader::open(dir.join(f)).unwrap().is_encrypted(),
                "{f} must be a plaintext container"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn write_read_roundtrip() {
        let dir = tmp("roundtrip");
        let _ = std::fs::remove_dir_all(&dir);

        let nodes: Vec<(Vec<u32>, Vec<(u32, Value)>)> = vec![
            (
                vec![0, 2],
                vec![(0, Value::Str("Alice".into())), (1, Value::Int(30))],
            ),
            (vec![], vec![]), // a node with no labels or props
            (
                vec![1],
                vec![(2, Value::List(vec![Value::Int(1), Value::Str("x".into())]))],
            ),
        ];
        let edges: Vec<(u64, u64, u32, Vec<(u32, Value)>)> = vec![
            (0, 2, 0, vec![(3, Value::Int(2020))]),
            (2, 0, 1, vec![]),
            (0, 2, 0, vec![]), // a parallel edge — must survive verbatim
        ];

        let mut w = DumpWriter::create(&dir, None).unwrap();
        for (ls, ps) in &nodes {
            w.append_node(ls, ps).unwrap();
        }
        for (s, d, t, ps) in &edges {
            w.append_edge(*s, *d, *t, ps).unwrap();
        }
        assert_eq!(w.node_count(), 3);
        assert_eq!(w.edge_count(), 3);
        w.finish(
            vec!["Person".into(), "VIP".into(), "Company".into()],
            vec!["KNOWS".into(), "OWNS".into()],
            vec!["name".into(), "age".into(), "tags".into(), "since".into()],
            vec![DumpRangeIndex {
                entity: EntityKind::Node,
                label_or_type: "Person".into(),
                property: "name".into(),
            }],
            vec![],
        )
        .unwrap();

        let r = DumpReader::open(&dir, None).unwrap();
        assert_eq!(r.meta().node_count, 3);
        assert_eq!(r.meta().edge_count, 3);
        assert_eq!(r.meta().labels, vec!["Person", "VIP", "Company"]);
        assert_eq!(r.meta().range_indexes.len(), 1);

        let mut got_nodes = Vec::new();
        r.for_each_node(|id, lb, pb| {
            got_nodes.push((
                id,
                decode_labels(lb, false).unwrap(),
                decode_props(pb).unwrap(),
            ));
            Ok(())
        })
        .unwrap();
        assert_eq!(got_nodes.len(), 3);
        for (i, (ls, ps)) in nodes.iter().enumerate() {
            assert_eq!(got_nodes[i].0, i as u64);
            assert_eq!(&got_nodes[i].1, ls);
            assert_eq!(&got_nodes[i].2, ps);
        }

        let mut got_edges = Vec::new();
        r.for_each_edge(|id, s, d, t, pb| {
            got_edges.push((id, s, d, t, decode_props(pb).unwrap()));
            Ok(())
        })
        .unwrap();
        assert_eq!(got_edges.len(), 3);
        for (i, (s, d, t, ps)) in edges.iter().enumerate() {
            assert_eq!(got_edges[i].0, i as u64);
            assert_eq!(got_edges[i].1, *s);
            assert_eq!(got_edges[i].2, *d);
            assert_eq!(got_edges[i].3, *t);
            assert_eq!(&got_edges[i].4, ps);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_foreign_magic() {
        let dir = tmp("foreign");
        let _ = std::fs::remove_dir_all(&dir);
        let w = DumpWriter::create(&dir, None).unwrap();
        w.finish(vec![], vec![], vec![], vec![], vec![]).unwrap();
        // Corrupt the magic.
        let meta_path = dir.join(META_FILE);
        let mut meta: DumpMeta =
            serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
        meta.magic = "NOTADUMP".into();
        std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
        let err = match DumpReader::open(&dir, None) {
            Ok(_) => panic!("expected a foreign-magic refusal"),
            Err(e) => e,
        };
        assert!(format!("{err:#}").contains("not a consolidation dump"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **HIK-158.** Two vector indexes whose `label.property` stems sanitise to the same
    /// string must not collide on one sidecar filename.
    ///
    /// The stems here are the ticket's own example, and they are reachable: labels and
    /// property keys are arbitrary strings over Bolt.
    ///
    /// * `(:Doc {` b.a `})` → stem `Doc.b.a`
    /// * ``(:`Doc.b` {a})`` → stem `Doc.b.a`
    ///
    /// The tables are given **equal lengths on purpose**. `read_vector_carry_at` checks the
    /// declared `base_records` against the file, so with unequal lengths that check masks
    /// the collision and a naive test passes against the unfixed code. Equal lengths remove
    /// the mask and leave only the filename standing between the two indexes — which is the
    /// bug: `std::fs::write` truncates, so the second write replaced the first's table, and
    /// the first index would then read the second's `layout → dump-id` mapping. A shared
    /// name also means a shared per-file subkey, so the AEAD passes too.
    #[test]
    fn two_indexes_whose_stems_sanitise_alike_get_distinct_carry_sidecars() {
        let dir = tmp("carry_collide");
        let _ = std::fs::remove_dir_all(&dir);
        let mut w = DumpWriter::create(&dir, None).unwrap();

        // Same sanitised stem, different indexes, same table length.
        let a = vec![10u64, 11, 12, 13];
        let b = vec![20u64, 21, 22, 23];
        let file_a = w.write_vector_carry(0, "Doc.b.a", &a).unwrap();
        let file_b = w.write_vector_carry(1, "Doc.b.a", &b).unwrap();

        assert_ne!(
            file_a, file_b,
            "distinct indexes must not share a sidecar filename"
        );
        // Read back through the same free function production uses, so this cannot pass on
        // a reader that disagrees with the writer.
        let read = |f: &str, n: usize| read_vector_carry_at(&dir.join(f), n as u64, None).unwrap();
        assert_eq!(
            read(&file_a, a.len()),
            a,
            "index 0 must read back its OWN layout table, not index 1's"
        );
        assert_eq!(read(&file_b, b.len()), b);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The same collision, end to end through a **sealed** dump: a shared filename would
    /// also mean a shared per-file subkey, so the AEAD cannot be what catches this.
    #[test]
    fn colliding_stems_stay_distinct_when_the_dump_is_sealed() {
        let cipher = Arc::new(BlockCipher::from_master(
            b"master-key-bytes",
            &[3u8; crate::crypto::SALT_LEN],
        ));
        let dir = tmp("carry_collide_sealed");
        let _ = std::fs::remove_dir_all(&dir);
        let mut w = DumpWriter::create(&dir, Some(cipher.clone())).unwrap();
        let a = vec![1u64, 2, 3, 4];
        let b = vec![5u64, 6, 7, 8];
        let file_a = w.write_vector_carry(0, "Doc.b.a", &a).unwrap();
        let file_b = w.write_vector_carry(1, "Doc.b.a", &b).unwrap();
        assert_ne!(file_a, file_b);
        let read = |f: &str| read_vector_carry_at(&dir.join(f), 4, Some(&cipher)).unwrap();
        assert_eq!(read(&file_a), a);
        assert_eq!(read(&file_b), b);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A carried Vamana index round-trips through the dump: the `DumpVectorCarry` scalars
    /// survive `meta.json`, and the `layout → dump-id` sidecar reads back exactly — holes
    /// included, and only for the length the carry declares.
    #[test]
    fn vector_carry_roundtrips() {
        use crate::pq::HOLE;
        let dir = tmp("carry");
        let _ = std::fs::remove_dir_all(&dir);
        let mut w = DumpWriter::create(&dir, None).unwrap();
        w.append_node(&[0], &[(0, Value::Str("x".into()))]).unwrap();
        let layout = vec![7u64, HOLE, 3, 0, HOLE, 5];
        let map_file = w.write_vector_carry(0, "Doc.embedding", &layout).unwrap();
        let carry = DumpVectorCarry {
            base_gen: Some(Generation(uuid::Uuid::from_u128(9))),
            base_vamana_artifact: None,
            base_vamana: "g/base/vector/Doc.embedding.vamana".into(),
            base_pq: "g/base/vector/Doc.embedding.pq".into(),
            carry_map_file: map_file,
            base_records: layout.len() as u64,
            r: 32,
            alpha: 1.2,
            medoid: 0,
            max_norm: 2.5,
            pq_subspaces: 8,
            pq_bits: 8,
            nav: AnnNav::InnerProduct,
        };
        w.finish(
            vec!["Doc".into()],
            vec![],
            vec!["embedding".into()],
            vec![],
            vec![DumpVectorIndex {
                label: "Doc".into(),
                property: "embedding".into(),
                dim: 16,
                metric: Metric::Cosine,
                carry: Some(carry.clone()),
            }],
        )
        .unwrap();

        let r = DumpReader::open(&dir, None).unwrap();
        let got = &r.meta().vector_indexes[0];
        assert_eq!(got.carry.as_ref(), Some(&carry));
        let back = r
            .read_vector_carry(&carry.carry_map_file, carry.base_records)
            .unwrap();
        assert_eq!(back, layout);
        // A wrong declared length is refused, not silently truncated.
        let err = r
            .read_vector_carry(&carry.carry_map_file, carry.base_records + 1)
            .unwrap_err();
        assert!(format!("{err:#}").contains("index the base .vamana by position"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fixed (node, edge) input dumps byte-identically across runs — the golden
    /// property the consolidation gate rests on. Compares the raw `.blk` bytes.
    #[test]
    fn dump_is_byte_deterministic() {
        let mk = |dir: &Path| {
            let mut w = DumpWriter::create(dir, None).unwrap();
            w.append_node(&[0], &[(0, Value::Str("Alice".into()))])
                .unwrap();
            w.append_node(&[0], &[(0, Value::Str("Bob".into()))])
                .unwrap();
            w.append_edge(0, 1, 0, &[(1, Value::Int(2020))]).unwrap();
            w.finish(
                vec!["Person".into()],
                vec!["KNOWS".into()],
                vec!["name".into(), "since".into()],
                vec![],
                vec![],
            )
            .unwrap();
        };
        let a = tmp("det_a");
        let b = tmp("det_b");
        let _ = std::fs::remove_dir_all(&a);
        let _ = std::fs::remove_dir_all(&b);
        mk(&a);
        mk(&b);
        for f in [NODES_FILE, EDGES_FILE, META_FILE] {
            assert_eq!(
                std::fs::read(a.join(f)).unwrap(),
                std::fs::read(b.join(f)).unwrap(),
                "dump file {f} not byte-identical across runs"
            );
        }
        let _ = std::fs::remove_dir_all(&a);
        let _ = std::fs::remove_dir_all(&b);
    }
}
