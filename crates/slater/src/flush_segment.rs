// SPDX-License-Identifier: Apache-2.0
//! The **T2 flush** writer: materialise the write-delta into an immutable upper core
//! segment (`docs/SEGMENTED-CORE-PLAN.md`, Phase 4).
//!
//! A flush is the O(delta) alternative to consolidation: instead of reading the whole core
//! back out and rebuilding a fresh generation, it writes the delta's touched entities as a
//! single small **core segment** that stacks *over* the unchanged base (the base is
//! preserved — no re-resolution, no id renumbering). The read path already merges such a
//! segment (Phase 3); this module is its first writer.
//!
//! # Scope (slice 4.1 — births-only)
//! This slice materialises a delta consisting solely of **born** nodes/edges (a `MERGE` of
//! entities absent from the core), with their adjacency, index and posting fragments and
//! *exact* marginals. A core-resolved patch or tombstone needs the base row to fill a full
//! replace-row, and folding a stacked L0 level needs a cross-level walk; both are deferred
//! to later slices, so this writer **refuses** (`bail!`) a delta carrying either — the
//! orchestration never fires it in that shape yet (the auto-trigger is unwired).
//!
//! # Full rows, replace semantics
//! Segments hold *full* rows, not patches: the newest segment carrying an id wins in a
//! single read. For a born node the effective row is `{business key} ∪ patches` (a patch
//! wins over the key) plus its `{identity label} ∪ labels_added ∖ labels_removed` — matching
//! [`Memtable::to_segment_data`]'s `born_index` derivation so the segment's node row and its
//! index fragment can never disagree.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use graph_format::crypto::BlockCipher;
use graph_format::ids::{Generation as GenId, Value};
use graph_format::manifest::FileEntry;
use graph_format::segindex::{write_index_fragments, IndexSpec};
use graph_format::segmanifest::{
    DirtyIndex, SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION,
};
use graph_format::segment::{AdjEdge, EdgeRow, NodeRow, SegmentWriter};
use graph_format::segpostings::{write_posting_fragments, PostingSpec};
use slater_delta::Memtable;

/// Block target size + zstd level for a core segment's payload sections. A flush is small
/// relative to the core; small blocks keep a cold point read's one-time decode cheap.
const SEG_BLOCK_BYTES: usize = 16 * 1024;
const SEG_ZSTD_LEVEL: i32 = 3;

/// Open-time context for materialising a flush segment.
pub struct FlushInputs<'a> {
    /// The segment's final directory: `<data_dir>/<graph>/segments/<seg_uuid>`.
    pub seg_dir: &'a Path,
    /// This segment's fresh uuid (its directory name and manifest id).
    pub seg_uuid: GenId,
    /// The base generation this segment deltas over (unchanged by the flush).
    pub base_uuid: GenId,
    /// The stack top *before* this flush: base node/edge count + every existing segment
    /// band. This segment's appended band starts here. For a first flush over a singleton
    /// set this equals the base count and the memtable's synthetic base.
    pub prior_node_total: u64,
    pub prior_edge_total: u64,
    /// The at-rest block cipher for the segment's sections (`None` = plaintext), and the
    /// runtime master key used to seal the manifest MAC (`None` = no MAC).
    pub cipher: Option<Arc<BlockCipher>>,
    pub master_key: Option<&'a [u8]>,
    /// Wall-clock stamp for the manifest (the caller supplies it — the workflow/runtime is
    /// clock-free by construction).
    pub created_unix: i64,
}

/// Materialise `mem` — a **births-only** frozen memtable — into a core segment at
/// `inp.seg_dir`, writing every section (`node/adj_out/adj_in/edge.blk`), the index and
/// posting fragments, and a sealed `SEGMENT.json`. Returns the sealed manifest, from which
/// the caller derives a [`SegmentRef`](graph_format::setmanifest::SegmentRef) for the new
/// set. Refuses a delta carrying any core-resolved patch/tombstone (id below the synthetic
/// base) — that is a later slice.
pub fn write_births_segment(mem: &Memtable, inp: &FlushInputs) -> Result<SegmentManifest> {
    let data = mem.to_segment_data();
    let synthetic_base = data.synthetic_base;
    let edge_synthetic_base = data.edge_synthetic_base;

    // The memtable's synthetic base must be the stack top the caller computed, else a born
    // id would not land in the appended band (the Phase 3.2 obligation).
    if synthetic_base != inp.prior_node_total {
        bail!(
            "flush node synthetic base {synthetic_base} != stack-top node total {}",
            inp.prior_node_total
        );
    }
    if edge_synthetic_base != inp.prior_edge_total {
        bail!(
            "flush edge synthetic base {edge_synthetic_base} != stack-top edge total {}",
            inp.prior_edge_total
        );
    }

    std::fs::create_dir_all(inp.seg_dir)
        .with_context(|| format!("create segment dir {}", inp.seg_dir.display()))?;

    // ── nodes: full born rows, sorted by dense id (data.nodes is already sorted) ─────────
    // Effective props/labels per born node, computed once and shared with the index below
    // so the node row and its index fragment cannot diverge.
    struct BornNode {
        id: u64,
        label: String,
        props: Vec<(String, Value)>,
        labels: Vec<String>,
        tombstoned: bool,
    }
    let mut born_nodes: Vec<BornNode> = Vec::with_capacity(data.nodes.len());
    for (id, label, key, keyval, delta) in &data.nodes {
        if *id < synthetic_base {
            bail!(
                "flush_to_segment: node {id} is a core-resolved patch/tombstone \
                 (below synthetic base {synthetic_base}) — not supported in this slice"
            );
        }
        let props = born_props(key, keyval, delta);
        let labels = effective_labels(label, delta);
        born_nodes.push(BornNode {
            id: *id,
            label: label.clone(),
            props,
            labels,
            tombstoned: delta.tombstoned,
        });
    }

    // ── edges: reconstruct full born rows from adjacency (endpoints/reltype) + edge delta
    // (props). A born edge appears once in adj_out (at its src) carrying dst/reltype/id. ──
    let mut edge_meta: BTreeMap<u64, (u64, u64, String)> = BTreeMap::new(); // edge_id → (src,dst,reltype)
    for (src, edges) in &data.adj_out {
        for e in edges {
            let Some(eid) = e.edge_id else {
                bail!(
                    "flush_to_segment: an out-adjacency entry at node {src} has no edge id \
                     (a core-edge tombstone) — not supported in this slice"
                );
            };
            edge_meta.insert(eid, (*src, e.other, e.reltype.clone()));
        }
    }

    // ── open the section writer and stream the four sorted sections ──────────────────────
    let mut w = SegmentWriter::create_with_cipher(
        inp.seg_dir,
        inp.seg_uuid.0.as_u128(),
        SEG_BLOCK_BYTES,
        SEG_ZSTD_LEVEL,
        inp.cipher.clone(),
    )
    .with_context(|| format!("create segment writer at {}", inp.seg_dir.display()))?;

    for n in &born_nodes {
        w.push_node(
            n.id,
            &NodeRow {
                labels: n.labels.clone(),
                props: n.props.clone(),
                tombstoned: n.tombstoned,
            },
        )
        .with_context(|| format!("push node {}", n.id))?;
    }

    // Adjacency fragments (data.adj_out/adj_in are sorted by endpoint; push in that order).
    for (src, edges) in &data.adj_out {
        let frag = born_adj_fragment(edges, *src, /*is_out=*/ true)?;
        w.push_adj_out(*src, &frag)
            .with_context(|| format!("push out-adjacency for {src}"))?;
    }
    for (dst, edges) in &data.adj_in {
        let frag = born_adj_fragment(edges, *dst, /*is_out=*/ false)?;
        w.push_adj_in(*dst, &frag)
            .with_context(|| format!("push in-adjacency for {dst}"))?;
    }

    // Edge rows, ascending edge id (data.edges is sorted).
    for (eid, delta) in &data.edges {
        if *eid < edge_synthetic_base {
            bail!(
                "flush_to_segment: edge {eid} is a core-edge patch/tombstone \
                 (below edge synthetic base {edge_synthetic_base}) — not supported in this slice"
            );
        }
        let (src, dst, reltype) = edge_meta.get(eid).cloned().ok_or_else(|| {
            anyhow::anyhow!("flush_to_segment: born edge {eid} has no adjacency entry")
        })?;
        w.push_edge(
            *eid,
            &EdgeRow {
                src,
                dst,
                reltype,
                props: delta
                    .patches
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                tombstoned: delta.tombstoned,
            },
        )
        .with_context(|| format!("push edge {eid}"))?;
    }

    w.finish()
        .with_context(|| format!("finish segment sections at {}", inp.seg_dir.display()))?;

    // ── index fragments: one ISAM per (label, prop) over born (value, id) pairs ───────────
    // Group the born nodes' effective props by (label, prop). No removals (births-only).
    let mut spec_index: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut specs: Vec<IndexSpec> = Vec::new();
    for n in &born_nodes {
        if n.tombstoned {
            continue; // a born-then-deleted node indexes nothing
        }
        for (prop, val) in &n.props {
            let k = (n.label.clone(), prop.clone());
            let idx = *spec_index.entry(k.clone()).or_insert_with(|| {
                specs.push(IndexSpec {
                    label: k.0.clone(),
                    prop: k.1.clone(),
                    entries: Vec::new(),
                    removals: Vec::new(),
                });
                specs.len() - 1
            });
            specs[idx].entries.push((val.clone(), n.id));
        }
    }
    if !specs.is_empty() {
        write_index_fragments(
            inp.seg_dir,
            &specs,
            SEG_BLOCK_BYTES,
            SEG_ZSTD_LEVEL,
            inp.cipher.clone(),
        )
        .with_context(|| format!("write index fragments at {}", inp.seg_dir.display()))?;
    }

    // ── posting fragments: per reltype, ascending-distinct born src/tgt endpoint ids ──────
    let mut post: BTreeMap<String, (Vec<u64>, Vec<u64>)> = BTreeMap::new();
    for (eid, (src, dst, reltype)) in &edge_meta {
        // A born edge that is also tombstoned (born-then-deleted) drives nothing.
        if data
            .edges
            .binary_search_by_key(eid, |(id, _)| *id)
            .ok()
            .map(|i| data.edges[i].1.tombstoned)
            .unwrap_or(false)
        {
            continue;
        }
        let e = post.entry(reltype.clone()).or_default();
        e.0.push(*src);
        e.1.push(*dst);
    }
    let posting_specs: Vec<PostingSpec> = post
        .into_iter()
        .map(|(reltype, (mut src_ids, mut tgt_ids))| {
            src_ids.sort_unstable();
            src_ids.dedup();
            tgt_ids.sort_unstable();
            tgt_ids.dedup();
            PostingSpec {
                reltype,
                src_ids,
                tgt_ids,
            }
        })
        .collect();
    if !posting_specs.is_empty() {
        write_posting_fragments(inp.seg_dir, &posting_specs)
            .with_context(|| format!("write posting fragments at {}", inp.seg_dir.display()))?;
    }

    // ── marginals (births-only ⇒ exact) ─────────────────────────────────────────────────
    let live_nodes: Vec<&BornNode> = born_nodes.iter().filter(|n| !n.tombstoned).collect();
    let node_count_delta = live_nodes.len() as i64;
    let mut label_node_deltas: BTreeMap<String, i64> = BTreeMap::new();
    for n in &live_nodes {
        for l in &n.labels {
            *label_node_deltas.entry(l.clone()).or_insert(0) += 1;
        }
    }
    let mut reltype_edge_deltas: BTreeMap<String, i64> = BTreeMap::new();
    let mut edge_count_delta: i64 = 0;
    for (eid, delta) in &data.edges {
        if delta.tombstoned {
            continue;
        }
        let (_, _, reltype) = edge_meta
            .get(eid)
            .ok_or_else(|| anyhow::anyhow!("born edge {eid} missing adjacency"))?;
        *reltype_edge_deltas.entry(reltype.clone()).or_insert(0) += 1;
        edge_count_delta += 1;
    }

    let node_band = (synthetic_base, synthetic_base + data.born_count);
    let edge_band = (
        edge_synthetic_base,
        edge_synthetic_base + data.born_edge_count,
    );

    // ── inventory + manifest ─────────────────────────────────────────────────────────────
    let files = inventory(inp.seg_dir)?;
    let dirty_indexes: Vec<DirtyIndex> = specs
        .iter()
        .enumerate()
        .map(|(k, s)| DirtyIndex {
            label: s.label.clone(),
            property: s.prop.clone(),
            fragment: format!("idx_{k}.isam"),
        })
        .collect();

    let mut manifest = SegmentManifest {
        magic: SEGMENT_MAGIC.into(),
        version: SEGMENT_MANIFEST_VERSION,
        segment_uuid: inp.seg_uuid,
        base: inp.base_uuid,
        created_unix: inp.created_unix,
        node_band,
        edge_band,
        content_hash: String::new(),
        encryption: None,
        node_count_delta,
        edge_count_delta,
        reltype_edge_deltas: reltype_edge_deltas.into_iter().collect(),
        label_node_deltas: label_node_deltas.into_iter().collect(),
        marginals_exact: true,
        dirty_indexes,
        mac: None,
        files,
    };
    manifest.set_content_hash();
    if let Some(key) = inp.master_key {
        manifest
            .seal_mac(key)
            .context("seal segment manifest MAC")?;
    }
    manifest
        .verify_marginals()
        .context("self-check flush segment marginals")?;
    manifest
        .write_to_dir(inp.seg_dir)
        .with_context(|| format!("write SEGMENT.json at {}", inp.seg_dir.display()))?;

    Ok(manifest)
}

/// The effective property map of a born node: its business key overlaid by patches (a patch
/// wins over the key), dropping any `removed` name — mirroring
/// [`Memtable::to_segment_data`]'s `born_index` so the node row and its index agree.
fn born_props(key: &str, keyval: &Value, delta: &slater_delta::NodeDelta) -> Vec<(String, Value)> {
    let mut props: BTreeMap<String, Value> = BTreeMap::new();
    if !key.is_empty() {
        props.insert(key.to_string(), keyval.clone());
    }
    if !delta.replaced {
        for r in &delta.removed {
            props.remove(r);
        }
    } else {
        // `SET n = {map}` — the key anchor still names the node; patches replace the rest.
        props.retain(|k, _| k == key);
    }
    for (p, v) in &delta.patches {
        props.insert(p.clone(), v.clone());
    }
    props.into_iter().collect()
}

/// The effective label set of a born node: its identity label ∪ `labels_added` ∖
/// `labels_removed`, de-duplicated and ordered.
fn effective_labels(label: &str, delta: &slater_delta::NodeDelta) -> Vec<String> {
    let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    if !label.is_empty() {
        set.insert(label.to_string());
    }
    for l in &delta.labels_added {
        set.insert(l.clone());
    }
    for l in &delta.labels_removed {
        set.remove(l);
    }
    set.into_iter().collect()
}

/// Map a node's born delta edges into segment adjacency fragment entries. `is_out` selects
/// the label direction for error messages only (the `other` endpoint is already the correct
/// neighbour for each side). Refuses a core-edge tombstone (no edge id) — a later slice.
fn born_adj_fragment(
    edges: &[slater_delta::DeltaEdge],
    node: u64,
    is_out: bool,
) -> Result<Vec<AdjEdge>> {
    let mut frag = Vec::with_capacity(edges.len());
    for e in edges {
        let Some(edge_id) = e.edge_id else {
            let side = if is_out { "out" } else { "in" };
            bail!(
                "flush_to_segment: an {side}-adjacency entry at node {node} has no edge id \
                 (a core-edge tombstone) — not supported in this slice"
            );
        };
        frag.push(AdjEdge {
            other: e.other,
            reltype: e.reltype.clone(),
            edge_id,
            removed: e.tombstoned,
        });
    }
    Ok(frag)
}

/// Build the sealed manifest's file inventory: every file in the segment dir (all sections
/// and fragments; `SEGMENT.json` is written *after* and is never in its own inventory),
/// each with its BLAKE3 hash, name-sorted so the content hash is deterministic.
fn inventory(seg_dir: &Path) -> Result<Vec<FileEntry>> {
    let mut files: Vec<FileEntry> = Vec::new();
    for entry in std::fs::read_dir(seg_dir)
        .with_context(|| format!("list segment dir {}", seg_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "SEGMENT.json" {
            continue;
        }
        let path = entry.path();
        let bytes = entry.metadata()?.len();
        let (blake3, sha256, crc32c) =
            graph_format::integrity::hash_file_checksums(&path, /*object_checksums=*/ false)?;
        files.push(FileEntry {
            name,
            bytes,
            blake3,
            sha256,
            crc32c,
        });
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(files)
}
