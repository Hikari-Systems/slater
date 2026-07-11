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
//! # Scope (slices 4.1 births-only + 4.2 node core-patches)
//! 4.1 materialised a delta of solely **born** nodes/edges (a `MERGE` of entities absent
//! from the core), with their adjacency, index and posting fragments and *exact* marginals.
//! 4.2 adds **core-resolved node patches** (a `SET`/`REMOVE` on a node the core already
//! carries, id below the delta's synthetic base): the writer reads the node's *base row*
//! (the core stack's effective row below the delta — a lower segment's full row, else the
//! base generation record), overlays the delta into a **full replace-row**, and records the
//! index **removal sidecars** that supersede the base's now-stale indexed values.
//!
//! Still deferred (each `bail!`ed, and the auto-trigger stays unwired so the orchestration
//! never fires them): a **core-edge patch** (the base exposes no by-id endpoint reader to
//! fill a full edge row — later slice), a **tombstone/delete** of a core node or edge (4.3,
//! which also writes the incident-edge removal fragments), and a **stacked L0 level** fold
//! (needs a cross-level walk — 4.4).
//!
//! # Full rows, replace semantics
//! Segments hold *full* rows, not patches: the newest segment carrying an id wins in a
//! single read (no cross-segment fold). For a **born** node the effective row is
//! `{business key} ∪ patches` (a patch wins over the key) plus its
//! `{identity label} ∪ labels_added ∖ labels_removed` — matching
//! [`Memtable::to_segment_data`]'s `born_index` derivation. For a **core-patched** node the
//! effective row is its base-below-delta row overlaid by the delta exactly as the read path
//! folds it ([`Executor::overlay_node_props`](crate::exec) / `node_label_ids_par`): a
//! replace-all clears the base props (re-seeding the anchor business key), `removed` names
//! drop, `patches` overwrite, and labels are `base ∖ labels_removed ∪ labels_added`. Because
//! the node row *replaces* the base row wholesale, every base index entry the effective row
//! no longer matches is listed in the segment's `removals` sidecar (Phase-3 obligation), so
//! the oldest→newest `fold_index_*` retain yields newest-wins.

use std::collections::{BTreeMap, BTreeSet};
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
use slater_delta::{Memtable, NodeDelta};

use crate::generation::Generation;

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
    /// The currently-served core (the base generation + any existing upper segments) the
    /// delta was resolved against. A core-resolved node patch reads its base-below-delta row
    /// from here — the winning lower segment's full row ([`CoreStack::resolve_node_row`]),
    /// else the base generation's node record — before overlaying the delta.
    pub core: &'a Generation,
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

/// Materialise `mem` — a frozen memtable of **born** nodes/edges and/or **core-resolved
/// node patches** — into a core segment at `inp.seg_dir`, writing every section
/// (`node/adj_out/adj_in/edge.blk`), the index and posting fragments, and a sealed
/// `SEGMENT.json`. Returns the sealed manifest, from which the caller derives a
/// [`SegmentRef`](graph_format::setmanifest::SegmentRef) for the new set. Refuses a delta
/// carrying a core node/edge tombstone, a core-edge patch, or a stacked L0 level — later
/// slices (see the module scope note).
pub fn write_flush_segment(mem: &Memtable, inp: &FlushInputs) -> Result<SegmentManifest> {
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

    // ── nodes: full rows, sorted by dense id (data.nodes is already sorted) ───────────────
    // Every touched node — born (id ≥ synthetic base) or core-patched (id below it) —
    // becomes a full replace-row. Its effective props/labels are computed once and shared
    // with the index fragments below so a node row and its index entry cannot diverge. For a
    // core patch `base_props` holds the base-below-delta props keyed by name, so the index
    // step can suppress exactly the base entries the effective row supersedes.
    struct SegNode {
        id: u64,
        /// Identity label — the key the base secondary index is grouped under.
        label: String,
        props: Vec<(String, Value)>,
        labels: Vec<String>,
        tombstoned: bool,
        /// `None` for a born node; `Some((base props, base labels))` for a core patch — the
        /// node's effective row *below* this delta (a lower segment's full row, else the base
        /// generation record), the input to the index removal + label-marginal diff.
        base: Option<(BTreeMap<String, Value>, Vec<String>)>,
    }
    let mut seg_nodes: Vec<SegNode> = Vec::with_capacity(data.nodes.len());
    for (id, label, key, keyval, delta) in &data.nodes {
        if *id >= synthetic_base {
            // Born node: effective row is the business key overlaid by patches.
            seg_nodes.push(SegNode {
                id: *id,
                label: label.clone(),
                props: born_props(key, keyval, delta),
                labels: effective_labels(label, delta),
                tombstoned: delta.tombstoned,
                base: None,
            });
            continue;
        }
        // Core-resolved node: a tombstone (delete) is slice 4.3 — refuse it here so the
        // full-row/removal machinery below only ever sees a live patch.
        if delta.tombstoned {
            bail!(
                "flush_to_segment: node {id} is a core-resolved tombstone (delete) — deferred \
                 to slice 4.3"
            );
        }
        let (base_props, base_labels) = read_base_node_row(inp.core, *id)?;
        let props = core_patch_props(&base_props, key, keyval, delta);
        let labels = core_patch_labels(&base_labels, delta);
        seg_nodes.push(SegNode {
            id: *id,
            label: label.clone(),
            props,
            labels,
            tombstoned: false,
            base: Some((base_props, base_labels)),
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

    for n in &seg_nodes {
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

    // ── index fragments: one ISAM per (label, prop) over (value, id) pairs, plus the
    // removal sidecar. A born node contributes an entry per effective prop (no removals). A
    // core-patched node's full row *replaces* its base row, so for every base-indexed prop
    // whose value the effective row changed or dropped it lists a `removal` (superseding the
    // stale base entry), and for every prop the effective row changed or added it lists a
    // fresh entry — the minimal diff that yields newest-wins under the oldest→newest fold.
    // Grouping is under the identity label, matching the base secondary index and the
    // memtable's `born_index` / `core_patched` derivations. ────────────────────────────────
    let mut spec_index: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut specs: Vec<IndexSpec> = Vec::new();
    let mut spec_slot = |specs: &mut Vec<IndexSpec>, label: &str, prop: &str| -> usize {
        *spec_index
            .entry((label.to_string(), prop.to_string()))
            .or_insert_with(|| {
                specs.push(IndexSpec {
                    label: label.to_string(),
                    prop: prop.to_string(),
                    entries: Vec::new(),
                    removals: Vec::new(),
                });
                specs.len() - 1
            })
    };
    for n in &seg_nodes {
        match &n.base {
            None => {
                if n.tombstoned {
                    continue; // a born-then-deleted node indexes nothing
                }
                for (prop, val) in &n.props {
                    let slot = spec_slot(&mut specs, &n.label, prop);
                    specs[slot].entries.push((val.clone(), n.id));
                }
            }
            Some((base_props, _)) => {
                let eff: BTreeMap<&str, &Value> =
                    n.props.iter().map(|(p, v)| (p.as_str(), v)).collect();
                // Suppress every base entry the effective row no longer matches (changed or
                // removed value).
                for (prop, bval) in base_props {
                    if eff.get(prop.as_str()) != Some(&bval) {
                        let slot = spec_slot(&mut specs, &n.label, prop);
                        specs[slot].removals.push(n.id);
                    }
                }
                // Re-add every entry the effective row changed or introduced.
                for (prop, val) in &n.props {
                    if base_props.get(prop) != Some(val) {
                        let slot = spec_slot(&mut specs, &n.label, prop);
                        specs[slot].entries.push((val.clone(), n.id));
                    }
                }
            }
        }
    }
    // A single node id can be superseded once per (label, prop); the fold + the writer both
    // require ascending, de-duplicated removals.
    for s in &mut specs {
        s.removals.sort_unstable();
        s.removals.dedup();
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

    // ── marginals (exact — every contribution is provable) ───────────────────────────────
    // A born (live) node adds one to the node count and to each of its labels. A core patch
    // leaves the node count unchanged (the base already counts it) and moves a label count
    // only where the effective row's label set differs from its base-below-delta set.
    let mut node_count_delta: i64 = 0;
    let mut label_node_deltas: BTreeMap<String, i64> = BTreeMap::new();
    for n in &seg_nodes {
        match &n.base {
            None => {
                if n.tombstoned {
                    continue; // born-then-deleted: nets to nothing
                }
                node_count_delta += 1;
                for l in &n.labels {
                    *label_node_deltas.entry(l.clone()).or_insert(0) += 1;
                }
            }
            Some((_, base_labels)) => {
                let before: BTreeSet<&str> = base_labels.iter().map(String::as_str).collect();
                let after: BTreeSet<&str> = n.labels.iter().map(String::as_str).collect();
                for l in after.difference(&before) {
                    *label_node_deltas.entry((*l).to_string()).or_insert(0) += 1;
                }
                for l in before.difference(&after) {
                    *label_node_deltas.entry((*l).to_string()).or_insert(0) -= 1;
                }
            }
        }
    }
    // Drop labels whose net change cancels to zero so the sparse manifest stays minimal.
    label_node_deltas.retain(|_, d| *d != 0);
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

/// Read node `id`'s **base-below-delta** row (props keyed by name, plus labels) as the core
/// stack sees it under this flush's delta: a lower segment's winning full row, else the base
/// generation's record. Mirrors [`Executor::core_named_props`](crate::exec) +
/// `node_label_ids_par`, so the overlaid full row equals what a pre-flush query returned. A
/// base row that is already a tombstone can only arise from a delete the write path never
/// produces for a `SET`/`REMOVE`, so it is refused rather than silently resurrected.
fn read_base_node_row(
    core: &Generation,
    id: u64,
) -> Result<(BTreeMap<String, Value>, Vec<String>)> {
    if let Some(row) = core.stack().resolve_node_row(id)? {
        if row.tombstoned {
            bail!(
                "flush_to_segment: node {id} patches a base-tombstoned node — unexpected (a \
                 delete is slice 4.3)"
            );
        }
        return Ok((row.props.into_iter().collect(), row.labels));
    }
    let props = core
        .node_props()
        .props(id)
        .with_context(|| format!("read base props for core-patched node {id}"))?
        .into_iter()
        .map(|(kid, v)| (core.property_key_name(kid).unwrap_or("?").to_string(), v))
        .collect();
    let labels = core
        .node_labels()
        .labels(id)
        .with_context(|| format!("read base labels for core-patched node {id}"))?
        .into_iter()
        .filter_map(|lid| core.label_name(lid).map(str::to_string))
        .collect();
    Ok((props, labels))
}

/// Overlay a core node's delta onto its `base` props into the effective full-row property
/// list, mirroring [`Executor::overlay_node_props`](crate::exec) for a non-born node: a
/// replace-all clears the base props and re-seeds the anchor business key, `removed` names
/// drop, then `patches` overwrite (last-writer-wins, and a patch on the anchor key wins).
fn core_patch_props(
    base: &BTreeMap<String, Value>,
    key: &str,
    keyval: &Value,
    delta: &NodeDelta,
) -> Vec<(String, Value)> {
    let mut props = base.clone();
    if delta.replaced {
        props.clear();
        if !key.is_empty() {
            props.insert(key.to_string(), keyval.clone());
        }
    }
    for r in &delta.removed {
        props.remove(r);
    }
    for (p, v) in &delta.patches {
        props.insert(p.clone(), v.clone());
    }
    props.into_iter().collect()
}

/// The effective label set of a core-patched node: its base labels with `labels_removed`
/// folded out then `labels_added` unioned in — the same order `node_label_ids_par` applies
/// (the [`NodeDelta`] invariant keeps a name out of both sets, so the order only documents
/// the mirror).
fn core_patch_labels(base_labels: &[String], delta: &NodeDelta) -> Vec<String> {
    let mut set: BTreeSet<String> = base_labels.iter().cloned().collect();
    for l in &delta.labels_removed {
        set.remove(l);
    }
    for l in &delta.labels_added {
        set.insert(l.clone());
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
