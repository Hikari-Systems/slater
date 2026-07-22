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

/// How a Vamana graph was constructed and is navigated — the HIK-137 MIPS discriminator.
///
/// **Additive-optional** (`#[serde(default)]`, like [`VectorIndexDesc::first_record`]): a manifest
/// written before this field existed has no `nav` key and parses to [`AnnNav::Augmented`], so the
/// entire live cosine/L2 estate keeps working and is **not** force-rebuilt. The field is serialised
/// only when it is *not* `Augmented` (see [`AnnMode::Vamana::nav`]), so every existing cosine/L2
/// (and pre-HIK-137 augmented-Dot) manifest is byte-identical to before.
///
/// A reader dispatches on this: an `Augmented` index is navigated through the L2-reduced ANN space
/// ([`crate::pq::ann_point`]/[`crate::pq::ann_query`]/[`crate::pq::AdcTable::new`]); an
/// `InnerProduct` index is navigated natively over raw inner product
/// ([`crate::vamana::build_vamana_ip`]/[`crate::pq::AdcTable::new_ip`]). Mistaking one for the other
/// silently mis-navigates, which is exactly what this field exists to prevent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AnnNav {
    /// The graph was built and the codebook trained in the metric's **L2-reduced ANN space**
    /// (`ann_point`): unit vectors for cosine, raw for L2, norm-augmented for Dot/MIPS. The default
    /// for every manifest that predates HIK-137. Navigated by [`crate::pq::AdcTable::new`].
    #[default]
    Augmented,
    /// **IP-native (MIPS)** — the graph was built over raw inner product
    /// ([`crate::vamana::build_vamana_ip`]) and the codebook trained on the **raw** vectors, with
    /// no norm augmentation. Navigated by [`crate::pq::AdcTable::new_ip`]. Only ever valid for
    /// [`Metric::Dot`]. Introduced by HIK-137 phase 2.
    InnerProduct,
}

/// The `(metric, nav)` pair is inconsistent: an `InnerProduct` (IP-native) discriminator was
/// found on a non-`Dot` index. Typed so a reader can branch on the *kind* of rejection rather than
/// its message text (house rule; the [`crate::pq::NonFiniteEmbedding`]/[`crate::wire::DecodeRejected`]
/// family).
///
/// This is the on-disk field the codebook-space check **cannot** catch: an `InnerProduct` codebook is
/// trained on the raw vectors over `PqParams::new(dim, …)`, and a **cosine or L2** codebook has the
/// *identical* width (`ann_pq_params` reduces to `PqParams::new(dim, …)` for both — only `Dot`
/// augments), so a forged/bit-rotted `nav: inner_product` on a cosine/L2 index passes the width check
/// and would then be navigated by `AdcTable::new_ip` — silently mis-navigating. `nav == InnerProduct`
/// is only ever *produced* for [`Metric::Dot`] (`build_vamana_ip`/segment seal both gate on it), so
/// this refuses forged state and never a legitimate index.
#[derive(Debug, thiserror::Error)]
#[error(
    "{what}: nav=inner_product (IP-native MIPS navigation) is only valid for a Dot index, but the \
     declared metric is {metric:?} — refusing rather than mis-navigate a {metric:?} graph by inner \
     product"
)]
pub struct NavMetricMismatch {
    pub what: &'static str,
    pub metric: Metric,
}

impl AnnNav {
    /// Whether this is the (default) augmented navigation — used by `skip_serializing_if` so a
    /// cosine/L2/augmented-Dot manifest omits the `nav` key entirely and stays byte-identical.
    pub fn is_augmented(&self) -> bool {
        matches!(self, AnnNav::Augmented)
    }

    /// Enforce the `(metric, nav)` invariant: [`AnnNav::InnerProduct`] is only valid for
    /// [`Metric::Dot`]. Every reader that is about to dispatch on `nav` (the base-index open
    /// validator and the shared beam navigator, which also covers sealed segments) calls this first,
    /// so a forged/corrupted `nav: inner_product` on a cosine/L2 index is **refused**, never walked
    /// by the IP navigator. `Augmented` always passes (cosine/L2/legacy-Dot); a legitimate IP index
    /// is `Dot` + `InnerProduct` and passes. See [`NavMetricMismatch`].
    pub fn check_metric(self, metric: Metric, what: &'static str) -> Result<(), NavMetricMismatch> {
        if matches!(self, AnnNav::InnerProduct) && metric != Metric::Dot {
            return Err(NavMetricMismatch { what, metric });
        }
        Ok(())
    }
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
        /// Entry point: the medoid's **vamana layout index** (its record position in
        /// the `.vamana` file), *not* a dense node id — the builder records it after
        /// the BFS permutation (D30). Since the BFS is seeded from the medoid it is
        /// always `0` in practice; it is kept explicit so a future layout that does
        /// not start at the medoid stays readable.
        ///
        /// # The invariant: **never orphan the medoid**
        ///
        /// Every beam search enters here and nowhere else. A *deleted* medoid is fine —
        /// its `.pq` id becomes [`crate::pq::HOLE`], it is never emitted, and it stays a
        /// navigational waypoint like any other hole. What must **never** happen is a
        /// delete-splice removing the medoid's *out-edges*: that isolates the entry point,
        /// every search then expands exactly one node and returns nothing useful, and
        /// recall for the whole index silently goes to zero — no error, no panic. The
        /// generation open path refuses an index whose medoid has no out-edges (with more
        /// than one record) rather than serve it; a splice must skip the medoid, or
        /// re-point `medoid` first.
        medoid: u64,
        /// PQ subspace count **as configured** (`--pq-subspaces`). The codebook's own
        /// `subspaces` is this `+ 1` for a dot index, which carries an extra subspace for
        /// the MIPS norm augmentation — see [`crate::pq::ann_pq_params`]. Read the
        /// codebook, not this field, when you need the code width.
        pq_subspaces: u32,
        /// Bits per PQ subspace code.
        pq_bits: u32,
        /// Records in the `.vamana`/`.pq` that are **not** holes — the emitted-eligible
        /// count. `VectorIndexDesc::count` is the *record* count (holes included), which is
        /// what bounds a layout ordinal; this is what a user-visible "how many vectors are
        /// in this index" answer wants. Equal to `count` on a freshly built index.
        live_count: u64,
        /// The largest L2 norm over the indexed vectors — the `M` of the dot/MIPS norm
        /// augmentation (`x' = [x, √(M² − ‖x‖²)]`, see [`crate::pq::ann_point`]).
        ///
        /// Recorded for **every** metric, but only read for [`Metric::Dot`]. It is here
        /// because it is not recoverable later: a graph carried through a consolidation by
        /// reference must augment any *newly inserted* point with the same `M` its existing
        /// points were augmented with, and re-deriving `M` from the survivors would give a
        /// different (smaller) constant and silently place the new point in a different
        /// space from the rest of the graph.
        max_norm: f32,
        /// How this graph is navigated — the HIK-137 MIPS discriminator (see [`AnnNav`]).
        /// **Additive-optional**: absent in every pre-HIK-137 manifest ⇒ [`AnnNav::Augmented`], so
        /// the live estate is not force-rebuilt; serialised only when [`AnnNav::InnerProduct`], so
        /// existing cosine/L2/augmented-Dot manifests are byte-identical. For an `InnerProduct`
        /// index, `max_norm` above is an **inert recorded field** (no augmentation uses it).
        #[serde(default, skip_serializing_if = "AnnNav::is_augmented")]
        nav: AnnNav,
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

/// Descriptor for the hub-degree sidecar (`hub_degrees.blk`). Records the degree
/// `floor` at/above which a node was listed (so the reader knows an absent node has
/// degree `< floor`) and the two list lengths (informational). Its presence gates the
/// reader's zero-I/O hub probe; absence ⇒ fall back to the record's leading edge count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HubDegreeDesc {
    /// Degree at/above which a node is listed in `hub_degrees.blk` (its out or in
    /// direction). A node absent from a list therefore has `< floor` in that direction.
    pub floor: u32,
    /// Number of nodes in the out-hub list (record 0).
    pub out_hubs: u64,
    /// Number of nodes in the in-hub list (record 1).
    pub in_hubs: u64,
}

/// Descriptor for one declared vector index over a `(label, property)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorIndexDesc {
    pub label: String,
    pub property: String,
    pub dim: u32,
    pub metric: Metric,
    /// **Records** in the index — for a Vamana index that means holes included, because a
    /// layout ordinal is a record position and the adjacency is expressed in ordinals. It
    /// is therefore the bound a neighbour ordinal is checked against; using the live count
    /// there would reject perfectly valid neighbours (a hole is a legal, navigable
    /// neighbour) and quietly cut recall. See [`AnnMode::Vamana::live_count`] for the count
    /// that *is* the number of live vectors.
    pub count: u64,
    /// Index of this index's first vector record in `vectors.f32.blk`. Its
    /// vectors occupy the contiguous global range `[firstRecord, firstRecord +
    /// count)` — the builder groups vectors by `(label, property)`, so a
    /// brute-force scan reads exactly one group with no per-record dispatch.
    #[serde(default)]
    pub first_record: u64,
    pub mode: AnnMode,
}

impl VectorIndexDesc {
    /// Vectors a query can actually be returned: [`Self::count`] minus the holes. A
    /// brute-force index has no holes, so it is just the count.
    pub fn live_count(&self) -> u64 {
        match self.mode {
            AnnMode::BruteForce => self.count,
            // Clamped: a manifest is an untrusted on-disk document, and a forged
            // `liveCount > count` must not underflow the ratio below.
            AnnMode::Vamana { live_count, .. } => live_count.min(self.count),
        }
    }

    /// The fraction of the index's records that are tombstoned holes, in `[0, 1]`.
    ///
    /// Holes are navigated but never emitted, so they cost IO and beam width without
    /// returning anything — which is what makes this the rebuild trigger (and a `/health`
    /// field): past some ratio the index should be rebuilt rather than accumulate. An empty
    /// index is `0.0`, not `NaN`.
    pub fn dead_ratio(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        (self.count - self.live_count()) as f64 / self.count as f64
    }
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
    /// How each block's AEAD associated data is derived — `file-block-v1` binds the
    /// block to its file (a per-file subkey) and to its ordinal within that file
    /// (HIK-140).
    ///
    /// Deliberately **not** `#[serde(default)]`: an encrypted image written before the
    /// binding existed must fail to parse with a readable error rather than open with
    /// its blocks unbound and relocatable. Enforced again at cipher-derivation time by
    /// [`crypto::check_aad_scheme`](crate::crypto::check_aad_scheme).
    pub aad_scheme: String,
}

/// One file in the generation inventory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub name: String,
    pub bytes: u64,
    pub blake3: String,
    /// Base64 SHA-256 of the file content (the `x-amz-checksum-sha256` form).
    /// Optional and additive: absent on generations built before it existed, and
    /// omitted from the JSON when `None` so those manifests are byte-unchanged.
    /// The S3 backend compares it to S3's server-computed object checksum to
    /// verify integrity from object metadata, without reading the body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// Base64 of the file content's CRC32C as a big-endian `u32` — the form GCS
    /// stores and returns as the object's `crc32c` checksum. Optional and additive
    /// exactly like [`Self::sha256`] (omitted from JSON when `None`, so older
    /// manifests stay byte-unchanged). The GCS backend compares it to GCS's
    /// server-computed object checksum to verify integrity from object metadata,
    /// without reading the body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<String>,
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
    /// Which backend-aware compression profile produced `zstd_level` — `"local"`,
    /// `"remote"`, `"max"`, or `"manual"` (an explicit `--zstd-level`). Purely
    /// informational (like [`Self::block_sizes`]): the reader needs nothing here
    /// because zstd streams are self-describing. Empty on images built before the
    /// profile existed. See `slater-build --compression-profile`.
    #[serde(default)]
    pub compression_profile: String,
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
    /// Per-reltype **edge** counts (index = reltype id, aligned with `reltypes`) —
    /// the number of relationships of each type. Distinct from
    /// [`Self::reltype_source_counts`] (those are distinct *node* counts). Non-empty
    /// ⇒ the whole-graph `type(r), count(*)` fast path and open-time `reltype_counts`
    /// read this directly; empty ⇒ recomputed at open by a CSR scan (older images),
    /// never incorrect. `sum(reltype_edge_counts) == edge_count`.
    #[serde(default)]
    pub reltype_edge_counts: Vec<u64>,
    /// Per-reltype **self-loop** edge counts (index = reltype id) — edges whose
    /// source and target are the same node. Lets the undirected `()-[r]-()` count
    /// fast path compute `2·edge − self_loop` exactly. Empty ⇒ unknown (the
    /// undirected fast path declines and scans).
    #[serde(default)]
    pub reltype_self_loop_counts: Vec<u64>,
    /// Per-label node **occurrence** counts (index = label id, aligned with
    /// `labels`) — the number of nodes carrying each label (a multi-label node is
    /// counted under every one of its labels). Persists what `build_label_counts`
    /// recomputes at open; non-empty ⇒ open skips that scan. `label_node_count`
    /// reads this. Empty ⇒ recomputed at open (older images), never incorrect.
    #[serde(default)]
    pub label_node_counts: Vec<u64>,
    /// Per-label **first-label** counts (index = label id) — the number of nodes
    /// whose `labels(n)[0]` (first stored label) is this label. Answers
    /// `labels(n)[0], count(*)` / `DISTINCT labels(n)[0]` with exact first-label
    /// semantics even when multi-label nodes exist. The null bucket (zero-label
    /// nodes) is `node_count − sum(first_label_counts)`. Empty ⇒ the label-metadata
    /// fast path declines (cannot reproduce first-label semantics from occurrences).
    #[serde(default)]
    pub first_label_counts: Vec<u64>,
    /// Sparse `(src_label_id, reltype_id) → edge count` marginal of the edge schema
    /// cube — edges whose source **carries** `src_label`, by reltype (a multi-label
    /// source contributes to each of its labels). Answers `(:A)-[r]->() RETURN
    /// type(r), count(*)`. Sorted by key for deterministic emit. Empty ⇒ the labeled
    /// rel fast path declines for source-labelled patterns.
    #[serde(default)]
    pub src_label_reltype_counts: Vec<(u32, u32, u64)>,
    /// Sparse `(reltype_id, tgt_label_id) → edge count` marginal — edges whose
    /// target **carries** `tgt_label`, by reltype. Answers `()-[r]->(:B) RETURN
    /// type(r), count(*)`. Sorted by key. Empty ⇒ decline for target-labelled patterns.
    #[serde(default)]
    pub reltype_tgt_label_counts: Vec<(u32, u32, u64)>,
    /// Sparse `(src_label_id, reltype_id, tgt_label_id) → edge count` — the full
    /// edge schema cube (source carries `src_label` **and** target carries
    /// `tgt_label`). Answers `(:A)-[r]->(:B)` / `(:A)-[:R]->(:B) RETURN count(*)`.
    /// Sorted by key. Empty ⇒ decline for both-endpoints-labelled patterns. Read a
    /// single cell only — never sum across a label axis (multi-label double-counts).
    ///
    /// FOLLOW-UP: this cube (with the marginals above) is enough to back a
    /// `db.schema`-style procedure that returns the labelled `(:A)-[:R]->(:B)`
    /// metagraph with counts, parallel to `db.meta.stats()` — not yet exposed.
    #[serde(default)]
    pub schema_triple_counts: Vec<(u32, u32, u32, u64)>,
    /// Per-(label, property) value→count histograms carried in `prop_hist.blk`,
    /// one descriptor per stored histogram, aligned by position with the file's
    /// records. Non-empty ⇒ the grouped-index fast path reads these instead of
    /// walking the ISAM. Empty ⇒ no histograms (every group-by/count(DISTINCT)
    /// falls back to `distinct_key_counts`). See [`PropertyHistogramDesc`].
    #[serde(default)]
    pub property_histograms: Vec<PropertyHistogramDesc>,
    /// Hub-degree sidecar descriptor (`hub_degrees.blk`). `Some` ⇒ the generation
    /// carries a per-node out/in degree list for nodes at/above
    /// [`HubDegreeDesc::floor`], so a traversal can decide a node is a hub with O(1)
    /// memory and no adjacency read. `None` ⇒ older generation without the sidecar;
    /// the reader falls back to the record's leading edge count. See [`HubDegreeDesc`].
    #[serde(default)]
    pub hub_degrees: Option<HubDegreeDesc>,
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
        parse_manifest(&text).with_context(|| format!("parse {}", path.display()))
    }

    /// Read and parse `MANIFEST.json` for the generation rooted at `base_key`
    /// in an [`ObjectStore`](crate::store::ObjectStore) (any backend).
    pub fn read_via(store: &dyn crate::store::ObjectStore, base_key: &str) -> Result<Self> {
        let key = crate::store::join_key(base_key, "MANIFEST.json");
        let bytes = store
            .read_all(&key)
            .with_context(|| format!("read {key}"))?;
        let text = std::str::from_utf8(&bytes).with_context(|| format!("parse {key}"))?;
        parse_manifest(text).with_context(|| format!("parse {key}"))
    }
}

/// Parse a MANIFEST document, **checking its format version first**.
///
/// The `Manifest` struct is schema-locked to the *current* `FORMAT_VERSION`: every field
/// added by a version bump is a required field. So a manifest from an older version does
/// not fail with "wrong version" — it fails inside serde, on whichever field happens to be
/// new, with a message naming a Rust struct field (`missing field 'live_count'`). The
/// reader's actual version gate then never runs, because the parse died before it.
///
/// That is precisely backwards at the one moment it matters: a format bump is exactly when
/// every existing generation on disk must be refused, and refused *legibly* — "rebuild
/// required", not a serde field name. So read the version out first. It is the one field
/// whose meaning is stable across versions, which is the entire point of having it.
fn parse_manifest(text: &str) -> Result<Manifest> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct VersionProbe {
        format_version: u32,
    }
    // A document too broken to yield even a version falls through to the full parse, whose
    // error is the more useful one there.
    if let Ok(p) = serde_json::from_str::<VersionProbe>(text) {
        if p.format_version != crate::FORMAT_VERSION {
            anyhow::bail!(
                "MANIFEST is on-disk format version {}, but this build understands version {}. \
                 Slater has no backwards compatibility: the generation must be rebuilt.",
                p.format_version,
                crate::FORMAT_VERSION
            );
        }
    }
    Ok(serde_json::from_str(text)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A format bump makes every *new* `AnnMode::Vamana` field a required field, so an
    /// older manifest carrying a Vamana index does not fail with "wrong version" — it
    /// fails inside serde on a field name (`missing field 'live_count'`), and the reader's
    /// version gate never runs, because the parse died before reaching it. That is exactly
    /// backwards at the one moment it matters: a bump is *when* every generation on disk
    /// must be refused, and refused legibly.
    ///
    /// The input here is a real one: it is what a v7 generation with an ANN index looks
    /// like on disk right now.
    #[test]
    fn an_older_manifest_is_refused_on_its_version_not_on_a_serde_field() {
        let v7 = r#"{
            "magic":"SLATER01","formatVersion":7,
            "buildUuid":"00000000-0000-0000-0000-000000000001","graph":"docs",
            "createdUnix":1700000000,"contentHash":"abc","blockSizes":{},
            "codec":"zstd","zstdLevel":3,"compressionProfile":"",
            "nodeCount":10,"edgeCount":0,
            "labels":["Doc"],"reltypes":[],"propertyKeys":["embedding"],
            "rangeIndexes":[],
            "vectorIndexes":[{"label":"Doc","property":"embedding","dim":8,
              "metric":"cosine","count":10,"firstRecord":0,
              "mode":{"kind":"vamana","r":24,"alpha":1.2,"medoid":0,
                      "pq_subspaces":8,"pq_bits":8}}],
            "reltypeSourceCounts":[],"reltypeTargetCounts":[],"reltypeEdgeCounts":[],
            "reltypeSelfLoopCounts":[],"labelNodeCounts":[],"firstLabelCounts":[],
            "srcLabelReltypeCounts":[],"reltypeTgtLabelCounts":[],
            "schemaTripleCounts":[],"propertyHistograms":[],"files":[]
        }"#;
        let err = parse_manifest(v7).unwrap_err().to_string();
        assert!(
            err.contains("format version 7") && err.contains("rebuilt"),
            "an old manifest must be refused on its version, with a rebuild instruction. \
             Got: {err}"
        );
        assert!(
            !err.contains("missing field"),
            "the operator must not be shown a serde field name. Got: {err}"
        );
    }

    /// HIK-137 additive-optional discriminator: a **current-version** manifest whose Vamana mode
    /// has **no `nav` key** — i.e. every generation built before HIK-137, including the live
    /// cosine/L2 estate and any pre-HIK-137 Dot index — must still parse, with `nav` defaulting to
    /// `Augmented`. This is the "estate is NOT force-rebuilt" guarantee, tested explicitly.
    #[test]
    fn a_pre_hik137_manifest_without_nav_parses_as_augmented() {
        let json = format!(
            r#"{{
            "magic":"SLATER01","formatVersion":{ver},
            "buildUuid":"00000000-0000-0000-0000-000000000001","graph":"docs",
            "createdUnix":1700000000,"contentHash":"abc","blockSizes":{{}},
            "codec":"zstd","zstdLevel":3,"compressionProfile":"",
            "nodeCount":10,"edgeCount":0,
            "labels":["Doc"],"reltypes":[],"propertyKeys":["embedding"],
            "rangeIndexes":[],
            "vectorIndexes":[{{"label":"Doc","property":"embedding","dim":8,
              "metric":"dot","count":10,"firstRecord":0,
              "mode":{{"kind":"vamana","r":24,"alpha":1.2,"medoid":0,
                      "pq_subspaces":8,"pq_bits":8,"live_count":10,"max_norm":3.5}}}}],
            "reltypeSourceCounts":[],"reltypeTargetCounts":[],"reltypeEdgeCounts":[],
            "reltypeSelfLoopCounts":[],"labelNodeCounts":[],"firstLabelCounts":[],
            "srcLabelReltypeCounts":[],"reltypeTgtLabelCounts":[],
            "schemaTripleCounts":[],"propertyHistograms":[],"files":[]
        }}"#,
            ver = crate::FORMAT_VERSION
        );
        let m =
            parse_manifest(&json).expect("a pre-HIK-137 manifest (no nav key) must still parse");
        match m.vector_indexes[0].mode {
            AnnMode::Vamana { nav, max_norm, .. } => {
                assert_eq!(
                    nav,
                    AnnNav::Augmented,
                    "an absent nav key must default to Augmented"
                );
                assert_eq!(max_norm, 3.5, "the recorded max_norm must survive");
            }
            _ => panic!("expected a Vamana mode"),
        }
    }

    /// The discriminator round-trips, and — critically — an `Augmented` index **omits** the `nav`
    /// key on serialize (so existing cosine/L2/augmented-Dot manifests are byte-identical to
    /// before), while an `InnerProduct` index emits `"nav":"inner_product"`.
    #[test]
    fn nav_discriminator_roundtrips_and_augmented_omits_the_key() {
        let augmented = AnnMode::Vamana {
            r: 32,
            alpha: 1.2,
            medoid: 0,
            pq_subspaces: 16,
            pq_bits: 8,
            live_count: 10,
            max_norm: 2.0,
            nav: AnnNav::Augmented,
        };
        let js = serde_json::to_string(&augmented).unwrap();
        assert!(
            !js.contains("nav"),
            "an Augmented index must omit the nav key so old manifests stay byte-identical: {js}"
        );
        assert_eq!(augmented, serde_json::from_str(&js).unwrap());

        let ip = AnnMode::Vamana {
            r: 32,
            alpha: 1.2,
            medoid: 0,
            pq_subspaces: 16,
            pq_bits: 8,
            live_count: 10,
            max_norm: 2.0,
            nav: AnnNav::InnerProduct,
        };
        let js = serde_json::to_string(&ip).unwrap();
        assert!(
            js.contains(r#""nav":"inner_product""#),
            "an InnerProduct index must record nav so the reader dispatches to the IP navigator: {js}"
        );
        assert_eq!(ip, serde_json::from_str(&js).unwrap());
    }

    /// The `(metric, nav)` invariant (HIK-137 phase 4). `InnerProduct` is only valid for `Dot`; a
    /// forged/corrupted `nav: inner_product` on a cosine/L2 index must be refused, because the
    /// codebook-width check cannot catch it (cosine/L2 augmented codebooks share the raw IP width).
    #[test]
    fn inner_product_nav_is_only_valid_for_a_dot_index() {
        // The legitimate IP index: Dot + InnerProduct passes.
        AnnNav::InnerProduct
            .check_metric(Metric::Dot, "x")
            .expect("Dot + InnerProduct is the legitimate IP index");
        // Augmented always passes for every metric — cosine/L2/legacy-Dot are untouched.
        for m in [Metric::Cosine, Metric::L2, Metric::Dot] {
            AnnNav::Augmented
                .check_metric(m, "x")
                .expect("Augmented navigation is valid for every metric");
        }
        // The forged pairs the codebook-width check cannot see: refuse, with the metric on the
        // typed error so a caller can branch on the *kind* of rejection, not the message text.
        for m in [Metric::Cosine, Metric::L2] {
            let err = AnnNav::InnerProduct
                .check_metric(m, "vec.Doc.embedding")
                .expect_err("nav=inner_product on a non-Dot index must be refused");
            assert_eq!(err.metric, m);
            assert_eq!(err.what, "vec.Doc.embedding");
        }
    }

    /// An unknown/garbage `nav` *value* (not merely an absent key) is rejected by serde — the enum
    /// has no `#[serde(other)]` catch-all, so a corrupted discriminator can never fall through to a
    /// default that mis-navigates. (`#[serde(default)]` only supplies a value when the key is
    /// **absent**; a present-but-invalid value still fails deserialization.)
    #[test]
    fn an_unknown_nav_value_is_refused_not_defaulted() {
        // Serialize a legitimate IP index, then corrupt *only* the nav string, so every other field
        // stays correctly named and the parse can fail for exactly one reason: the bad discriminator.
        let ip = AnnMode::Vamana {
            r: 32,
            alpha: 1.2,
            medoid: 0,
            pq_subspaces: 16,
            pq_bits: 8,
            live_count: 10,
            max_norm: 2.0,
            nav: AnnNav::InnerProduct,
        };
        let js = serde_json::to_string(&ip).unwrap();
        assert!(js.contains(r#""nav":"inner_product""#));
        let corrupted = js.replace(r#""nav":"inner_product""#, r#""nav":"quaternion""#);
        let parsed: Result<AnnMode, _> = serde_json::from_str(&corrupted);
        assert!(
            parsed.is_err(),
            "a nav value outside the known variants must be refused, not defaulted to Augmented: {corrupted}"
        );
    }

    fn sample() -> Manifest {
        let files = vec![FileEntry {
            name: "node_props.blk".into(),
            bytes: 123,
            blake3: "deadbeef".into(),
            sha256: None,
            crc32c: None,
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
            compression_profile: String::new(),
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
            reltype_edge_counts: vec![],
            reltype_self_loop_counts: vec![],
            label_node_counts: vec![1],
            first_label_counts: vec![1],
            src_label_reltype_counts: vec![],
            reltype_tgt_label_counts: vec![],
            schema_triple_counts: vec![],
            property_histograms: vec![],
            hub_degrees: None,
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
    fn manifest_without_summary_fields_defaults_to_empty() {
        // A generation built before the whole-graph metadata summaries existed has a
        // MANIFEST.json lacking those keys. They must deserialize to empty vectors
        // (⇒ the query paths fall back to an open-time scan), never error.
        let mut v = serde_json::to_value(sample()).unwrap();
        let obj = v.as_object_mut().unwrap();
        for k in [
            "reltypeEdgeCounts",
            "reltypeSelfLoopCounts",
            "labelNodeCounts",
            "firstLabelCounts",
            "srcLabelReltypeCounts",
            "reltypeTgtLabelCounts",
            "schemaTripleCounts",
        ] {
            assert!(obj.remove(k).is_some(), "sample manifest should carry {k}");
        }
        let back: Manifest = serde_json::from_value(v).unwrap();
        assert!(back.reltype_edge_counts.is_empty());
        assert!(back.reltype_self_loop_counts.is_empty());
        assert!(back.label_node_counts.is_empty());
        assert!(back.first_label_counts.is_empty());
        assert!(back.src_label_reltype_counts.is_empty());
        assert!(back.reltype_tgt_label_counts.is_empty());
        assert!(back.schema_triple_counts.is_empty());
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
                aad_scheme: crate::crypto::AAD_SCHEME.to_string(),
                aead: "x".into(),
                kdf: "y".into(),
                salt_hex: "00".into(),
            })
        });
    }

    /// HIK-140: `aadScheme` is required on an encrypted manifest. An image written
    /// before per-file/per-ordinal binding existed has no such key, and must fail to
    /// **parse** rather than open with its blocks unbound and relocatable.
    #[test]
    fn an_encrypted_manifest_without_an_aad_scheme_does_not_parse() {
        let mut m = sample();
        m.encryption = Some(EncryptionHeader {
            aead: crate::crypto::AEAD_NAME.into(),
            kdf: crate::crypto::KDF_NAME.into(),
            salt_hex: "00".repeat(32),
            aad_scheme: crate::crypto::AAD_SCHEME.into(),
        });
        let json = m.to_json().unwrap();
        assert!(parse_manifest(&json).is_ok(), "the sealed form parses");

        // Drop the key, exactly as a pre-HIK-140 image would have it.
        let mut v: serde_json::Value = serde_json::from_str(&json).unwrap();
        v["encryption"]
            .as_object_mut()
            .unwrap()
            .remove("aadScheme")
            .unwrap();
        let legacy = serde_json::to_string(&v).unwrap();
        let err = parse_manifest(&legacy).unwrap_err();
        assert!(
            err.to_string().contains("aadScheme"),
            "the error must name the missing field: {err:#}"
        );
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
