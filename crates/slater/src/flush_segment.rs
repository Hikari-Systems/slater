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
//! # Scope (slices 4.1 births-only + 4.2 node core-patches + 4.3 deletes + 4.4-a edge patches)
//! 4.1 materialised a delta of solely **born** nodes/edges (a `MERGE` of entities absent
//! from the core), with their adjacency, index and posting fragments and *exact* marginals.
//! 4.2 adds **core-resolved node patches** (a `SET`/`REMOVE` on a node the core already
//! carries, id below the delta's synthetic base): the writer reads the node's *base row*
//! (the core stack's effective row below the delta — a lower segment's full row, else the
//! base generation record), overlays the delta into a **full replace-row**, and records the
//! index **removal sidecars** that supersede the base's now-stale indexed values.
//! 4.3 adds **deletes**. A core-node delete is materialised as a full-row **tombstone**
//! (the effective-row-empty case of a core patch: every base-indexed value moves to the
//! `removals` sidecar, the node count and its labels net down by one) *and* the removal of
//! its incident edges: the writer reads the deleted node's **effective adjacency** (base
//! folded with every lower segment, mirroring [`for_each_adj_overlaid`](crate::exec)) and
//! writes a `removed` adjacency fragment for each incident edge on the *surviving
//! neighbour's* side (the read path drops a dead edge by that fragment's `edge_id`, never by
//! a per-neighbour segment-tombstone check), netting each out of the edge/reltype marginals.
//! An explicit **edge delete** (`DELETE r` on a core edge — carried in the delta as an
//! adjacency tombstone with no edge id, matched by identity) is resolved to its core edge
//! id(s) the same way and removed on *both* live endpoints' sides. A born edge incident to a
//! node deleted in the same delta is dropped wholesale (it never reaches a lower layer).
//! 4.4-a adds **core-edge patches** (a `SET r.p = v` on an edge the core already carries):
//! the writer reads the edge's *base props* (a lower segment's winning full row via
//! [`CoreStack::resolve_edge_row`], else the base generation's edge props), overlays the
//! patch into a **full replace-row** the segment serves over the base, and changes no
//! marginal (topology is untouched). The endpoints + reltype a patch omits from adjacency are
//! surfaced by [`Memtable::to_segment_data`] in `core_patched_edges`. slater carries no
//! relationship range index consulted at query time, so — unlike a node patch — an edge patch
//! needs no index removal sidecar.
//!
//! A patch-**then-delete** of the same core edge in one delta is handled: the memtable's
//! `delete_edge` resolves it into a pure adjacency tombstone (dropping the by-id patch index),
//! so it flows through `suppressed` as an ordinary core-edge delete — the writer needs no
//! special case (the edge-row loop's tombstoned-core-edge branch is now an invariant guard).
//!
//! A **stacked L0** flush folds every level newest-wins into one segment: a **resident** stack
//! folds in RAM (`Memtable::merge_levels`), an **off-heap** stack (a block image, not a
//! memtable) folds at the `SegmentData` level (`slater_delta::flush_segment_data`), so the flush
//! writer consumes a `SegmentData` and no longer cares which. Every write op now flushes,
//! resident or off-heap — no per-op `bail!` remains.
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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use graph_format::crypto::BlockCipher;
use graph_format::ids::{Generation as GenId, NodeId, Value};
use graph_format::manifest::{AnnMode, EncryptionHeader, FileEntry};
use graph_format::segindex::{write_index_fragments, IndexSpec};
use graph_format::segmanifest::{
    DirtyIndex, DirtyVector, SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION,
};
use graph_format::segment::{AdjEdge, EdgeRow, NodeRow, SegmentWriter};
use graph_format::segpostings::{write_posting_fragments, PostingSpec};
use graph_format::segvamana::{seal_segment_index, SealedVamanaMeta};
use graph_format::segvectors::{write_vector_fragments, VectorSpec};
use slater_delta::l0_offheap::SegmentData;
use slater_delta::NodeDelta;

use crate::generation::Generation;

/// Block target size + zstd level for a core segment's payload sections. A flush is small
/// relative to the core; small blocks keep a cold point read's one-time decode cheap.
pub(crate) const SEG_BLOCK_BYTES: usize = 16 * 1024;
pub(crate) const SEG_ZSTD_LEVEL: i32 = 3;

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
    /// The manifest encryption header (AEAD/KDF names + this segment's fresh KDF salt, never
    /// the key) recorded so [`crate::segstack`] can re-derive the same cipher on reopen.
    /// `Some` iff `cipher` is — the read side keys off it.
    pub encryption_header: Option<EncryptionHeader>,
    /// Wall-clock stamp for the manifest (the caller supplies it — the workflow/runtime is
    /// clock-free by construction).
    pub created_unix: i64,
}

/// Materialise `mem` — a frozen memtable of **born** nodes/edges and/or **core-resolved
/// node patches** — into a core segment at `inp.seg_dir`, writing every section
/// (`node/adj_out/adj_in/edge.blk`), the index and posting fragments, and a sealed
/// `SEGMENT.json`. Returns the sealed manifest, from which the caller derives a
/// [`SegmentRef`](graph_format::setmanifest::SegmentRef) for the new set. `data` is the folded
/// delta (a single memtable's `to_segment_data`, a resident-L0 `merge_levels` fold, or an
/// off-heap-L0 `flush_segment_data` fold — the caller picks), so every write op flushes,
/// stacked or not, resident or off-heap.
pub fn write_flush_segment(data: &SegmentData, inp: &FlushInputs) -> Result<SegmentManifest> {
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
        let (base_props, base_labels) = read_base_node_row(inp.core, *id)?;
        // Core-resolved node delete: a full-row tombstone. Its effective row is empty, so
        // this is the degenerate case of a core patch — every base-indexed value moves to
        // the `removals` sidecar (the index step below reads `base` for exactly that), the
        // node count and each base label net down by one (the marginal step), and the node
        // section carries a tombstone row that `resolve_node_row` returns to supersede the
        // live base/lower-segment row. The incident-edge removal fragments are written from
        // the deleted node's *effective adjacency* further below.
        if delta.tombstoned {
            seg_nodes.push(SegNode {
                id: *id,
                label: label.clone(),
                props: Vec::new(),
                labels: Vec::new(),
                tombstoned: true,
                base: Some((base_props, base_labels)),
            });
            continue;
        }
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

    // ── edges + deletes: assemble the segment's edge topology from the delta's born edges
    // and, for deletes, from the deleted nodes' effective adjacency below this flush. ───────
    //
    // `tombstoned_all` is every node this delta drops — a core delete or a born-then-deleted
    // node. An edge incident to such a node does not survive, so a born edge touching one is
    // discarded and a suppressed edge's removal fragment is written only on the *other*,
    // surviving side (a dropped node cannot bind as an anchor, so its own adjacency is never
    // read).
    let tombstoned_all: BTreeSet<u64> = data
        .nodes
        .iter()
        .filter(|(_, _, _, _, d)| d.tombstoned)
        .map(|(id, _, _, _, _)| *id)
        .collect();

    // Live born edges (`edge_id → (src, dst, reltype)`): a delta-born relationship whose
    // endpoints both survive. A born-then-deleted edge (its adjacency entry tombstoned) or
    // one incident to a dropped node never reaches a lower layer, so it is materialised
    // nowhere — no row, no adjacency, no posting, no marginal.
    let mut live_born_edges: BTreeMap<u64, (u64, u64, String)> = BTreeMap::new();
    for (src, edges) in &data.adj_out {
        for e in edges {
            let Some(eid) = e.edge_id else {
                continue; // a core-edge delete (no id) — resolved into `suppressed` below
            };
            if e.tombstoned || tombstoned_all.contains(src) || tombstoned_all.contains(&e.other) {
                continue;
            }
            live_born_edges.insert(eid, (*src, e.other, e.reltype.clone()));
        }
    }

    // Suppressed core edges (`edge_id → (src, dst, reltype)`): every base/lower-segment edge
    // this delta removes — implicitly as an incident edge of a deleted node, or explicitly as
    // a `DELETE r` on a core edge (carried as an adjacency tombstone with no edge id, matched
    // by identity). Both are resolved to concrete core edge ids against the deleted / aliased
    // node's *effective* adjacency (base folded with every lower segment).
    let mut suppressed: BTreeMap<u64, (u64, u64, String)> = BTreeMap::new();
    // (a) every incident edge of a deleted core node, both directions (deduped by edge id so
    //     a self-loop, seen on both sides, is counted once).
    for (id, _label, _key, _keyval, delta) in &data.nodes {
        if *id >= synthetic_base || !delta.tombstoned {
            continue; // a born-then-deleted node has no core adjacency; a live patch none.
        }
        for (eid, other, reltype) in effective_adj(inp.core, *id, /*outgoing=*/ true)? {
            suppressed.entry(eid).or_insert((*id, other, reltype));
        }
        for (eid, other, reltype) in effective_adj(inp.core, *id, /*outgoing=*/ false)? {
            suppressed.entry(eid).or_insert((other, *id, reltype));
        }
    }
    // (b) explicit core-edge deletes. Each is carried once in `adj_out` at its source with no
    //     edge id; identity semantics remove *every* parallel edge of that reltype to the
    //     neighbour, mirroring the delta's `(reltype, neighbour)` suppression in `for_each_adj_overlaid`.
    for (src, edges) in &data.adj_out {
        // Only sources carrying an explicit core-edge delete (a no-id adjacency entry) need the
        // base/lower-segment adjacency resolved. Compute `effective_adj` — which reads the base
        // CSR *and every lower segment* — **once per hub** and index it by `(neighbour, reltype)`,
        // so `D` deletes on a `D`-degree hub cost O(D) rather than re-reading that adjacency per
        // deleted edge (the O(D²) blow-up on a hub-heavy delete).
        if !edges.iter().any(|e| e.edge_id.is_none()) {
            continue;
        }
        let eff = effective_adj(inp.core, *src, /*outgoing=*/ true)?;
        let mut by_key: HashMap<(u64, &str), Vec<u64>> = HashMap::new();
        for (eid, other, reltype) in &eff {
            by_key
                .entry((*other, reltype.as_str()))
                .or_default()
                .push(*eid);
        }
        for e in edges {
            if e.edge_id.is_some() {
                continue;
            }
            if let Some(eids) = by_key.get(&(e.other, e.reltype.as_str())) {
                for &eid in eids {
                    suppressed.insert(eid, (*src, e.other, e.reltype.clone()));
                }
            }
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

    // Adjacency fragments: each touched node's list of born edges (`removed = false`, both
    // endpoints) plus a removal entry for every suppressed edge on each *surviving* endpoint's
    // side. Built into per-node maps (a node can both gain a born edge and lose a core one, as
    // node 0 does when its neighbour is deleted) and pushed in ascending id order.
    let mut out_frags: BTreeMap<u64, Vec<AdjEdge>> = BTreeMap::new();
    let mut in_frags: BTreeMap<u64, Vec<AdjEdge>> = BTreeMap::new();
    for (eid, (src, dst, reltype)) in &live_born_edges {
        out_frags.entry(*src).or_default().push(AdjEdge {
            other: *dst,
            reltype: reltype.clone(),
            edge_id: *eid,
            removed: false,
        });
        in_frags.entry(*dst).or_default().push(AdjEdge {
            other: *src,
            reltype: reltype.clone(),
            edge_id: *eid,
            removed: false,
        });
    }
    for (eid, (src, dst, reltype)) in &suppressed {
        if !tombstoned_all.contains(src) {
            out_frags.entry(*src).or_default().push(AdjEdge {
                other: *dst,
                reltype: reltype.clone(),
                edge_id: *eid,
                removed: true,
            });
        }
        if !tombstoned_all.contains(dst) {
            in_frags.entry(*dst).or_default().push(AdjEdge {
                other: *src,
                reltype: reltype.clone(),
                edge_id: *eid,
                removed: true,
            });
        }
    }
    for (src, frag) in &out_frags {
        w.push_adj_out(*src, frag)
            .with_context(|| format!("push out-adjacency for {src}"))?;
    }
    for (dst, frag) in &in_frags {
        w.push_adj_in(*dst, frag)
            .with_context(|| format!("push in-adjacency for {dst}"))?;
    }

    // Core-edge patch endpoints (`edge_id → (src, dst, reltype)`): a `SET r.p` on an existing
    // core edge below the synthetic base. A patch leaves topology alone, so the endpoints are
    // absent from `data.adj_out`; the memtable surfaces them here so the row can carry them.
    let core_patched_edges: BTreeMap<u64, (u64, u64, String)> = data
        .core_patched_edges
        .iter()
        .map(|(eid, src, dst, rt)| (*eid, (*src, *dst, rt.clone())))
        .collect();

    // Edge rows, ascending edge id (data.edges is sorted, core ids before born ids). A live
    // born edge carries a full row; a core-edge patch below the synthetic base is a full
    // *replace* row — the base edge props (a lower segment's winning row, else the base
    // generation's) overlaid by the patch, `resolve_edge_row` serving it over the base with
    // no marginal change (topology is untouched). A core-edge delete carries no row (a pure
    // adjacency removal), and a born edge that did not survive is materialised nowhere. Pushed
    // ascending, so core-patch ids (below the band) precede born ids and the id fence widens
    // to include them.
    for (eid, edelta) in &data.edges {
        if *eid < edge_synthetic_base {
            // A core-edge patch. A patch-**then-delete** of the same core edge in one delta is
            // resolved by the memtable into a pure adjacency tombstone (`delete_edge` drops the
            // by-id patch index and registers the removal in the adjacency overlay), so it flows
            // through `suppressed` as an ordinary core-edge delete and never reaches here as a
            // tombstoned edge row. This guards that invariant.
            if edelta.tombstoned {
                bail!(
                    "flush_to_segment: core edge {eid} appears as a tombstoned edge row — a \
                     patch-then-delete should have been resolved to an adjacency removal by the \
                     memtable (invariant violation)"
                );
            }
            let Some((src, dst, reltype)) = core_patched_edges.get(eid).cloned() else {
                bail!("flush_to_segment: core-patched edge {eid} has no recorded endpoints");
            };
            let mut props = read_base_edge_row(inp.core, *eid)?;
            for (k, v) in &edelta.patches {
                props.insert(k.clone(), v.clone()); // a patch wins over the base value
            }
            w.push_edge(
                *eid,
                &EdgeRow {
                    src,
                    dst,
                    reltype,
                    props: props.into_iter().collect(),
                    tombstoned: false,
                },
            )
            .with_context(|| format!("push core-patched edge {eid}"))?;
            continue;
        }
        let Some((src, dst, reltype)) = live_born_edges.get(eid).cloned() else {
            continue;
        };
        w.push_edge(
            *eid,
            &EdgeRow {
                src,
                dst,
                reltype,
                props: edelta
                    .patches
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                tombstoned: false,
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

    // ── vector sidecar: which nodes this segment embeds, and which it un-embeds. ──────────
    //
    // Read from the *deltas*, not the rows, and that is the whole point. An indexed
    // embedding is routed out of the column store (D12), so a base node's row never held
    // one — which makes a flushed row that lacks an embedding ambiguous: `REMOVE
    // n.embedding` and an unrelated `SET n.age = 99` produce byte-identical rows. Only the
    // `NodeDelta` distinguishes them, and only here, while we still have it. Without this
    // the removed node's stale base vector keeps scoring forever.
    //
    // The embeddings themselves need no fragment — `Value::Vector` is a first-class wire
    // type, so a written vector is already in the node's row (see `segvectors`).
    //
    // Membership is decided by the node's **effective label set**, which is the same question
    // the read fold asks (`exec::vector_levels`: "the index is scoped to a label, and a write
    // can add one"). Asking the *identity* label instead — the label the write happened to
    // anchor on — is a different question, and the two differ on a multi-label node whose
    // business key lives on a label other than the index's: `MATCH (n:Keyed {name:'x'}) SET
    // n.embedding = …` where the index is on `(:Doc {embedding})`. The embedding is then live
    // in the delta and *silently reverts to the stale base vector at the flush* (the sidecar
    // names nobody, so the fold's candidate set never sees it) — and a `REMOVE n.embedding`
    // through the same anchor resurfaces the vector the user deleted. The row carries the
    // vector either way; only the sidecar decides whether anyone looks.
    let vector_indexes = &inp.core.manifest().vector_indexes;
    let effective_labels: BTreeMap<u64, &[String]> = seg_nodes
        .iter()
        .map(|n| (n.id, n.labels.as_slice()))
        .collect();
    // The rows this segment is about to write. Needed because an embedding can reach a segment
    // *without* a delta patch naming it: a node that was out of the index's scope carried its
    // embedding as an ordinary column value, and `read_base_node_row` propagates it into the
    // flushed row. See the `labels_added` arm below.
    let effective_rows: BTreeMap<u64, &[(String, Value)]> = seg_nodes
        .iter()
        .map(|n| (n.id, n.props.as_slice()))
        .collect();
    let mut vec_specs: Vec<VectorSpec> = Vec::new();
    // The sealed read-only Vamana index per `(label, property)`, aligned with `vec_specs`
    // (HIK-113). `None` ⇒ the segment's live embedded set stayed below the floor (or the base's
    // norms overflow), so this level is brute-forced from the sidecar ids.
    let mut vec_metas: Vec<Option<SealedVamanaMeta>> = Vec::new();
    for vi in vector_indexes {
        let mut spec = VectorSpec {
            label: vi.label.clone(),
            prop: vi.property.clone(),
            ids: Vec::new(),
            label_removals: Vec::new(),
            value_removals: Vec::new(),
        };
        // The segment's **own** `(id, vector)` for every id it embeds — gathered from the same
        // `Value::Vector` patch that puts the id in `spec.ids`, so the sealed index and the
        // sidecar name exactly the same set. De-duped by id (last patch wins) via the map.
        let mut entries: BTreeMap<u64, Vec<f32>> = BTreeMap::new();
        for (dense, _label, _key, _key_value, nd) in &data.nodes {
            if nd.tombstoned {
                // A tombstoned node is suppressed by its tombstone, not by a vector removal.
                continue;
            }
            let labelled = effective_labels
                .get(dense)
                .is_some_and(|ls| ls.iter().any(|l| l == &vi.label));
            if !labelled {
                // The node is not (effectively) in this index's scope. If this delta is what
                // took it out — `REMOVE n:Doc` dropped the index's label — the node *left* a
                // scope it was in, so the base/lower vector it still carries must be superseded:
                // record a **label** removal. The row cannot express this (D12 routed the embedding
                // out), and without it the stale base vector resurfaces the moment the delta is
                // retired at the flush — the write silently undone by a background job. This is
                // the remove-direction twin of keying membership on the effective label set for
                // the *add* direction (HIK-111 Finding 1). A node that simply never carried the
                // label contributes nothing.
                //
                // It is a **label** removal, not a value one, and the distinction is load-bearing
                // (HIK-118): the embedding value is untouched (D64), so a later `SET n:Doc` must be
                // able to un-suppress this id and score its base vector again. `value_removals`
                // (below) would suppress it permanently.
                if nd.labels_removed.contains(&vi.label) {
                    spec.label_removals.push(*dense);
                }
                continue;
            }
            if let Some(Value::Vector(v)) = nd.patches.get(&vi.property) {
                spec.ids.push(*dense);
                entries.insert(*dense, v.clone());
            } else if nd.patches.contains_key(&vi.property)
                || nd.replaced
                || nd.removed.contains(&vi.property)
            {
                // The node's embedding is gone. Three ways to lose one, and all three have to
                // land here or the level below goes on scoring a vector a newer level already
                // took away: `REMOVE n.embedding`; a `SET n = {…}` replace that did not re-set
                // it (one that did took the branch above); and an overwrite with a value that
                // is **not** a vector (`SET n.embedding = 5` — the write path admits it, since
                // `validate_vector_dims` only constrains a `Value::Vector`). The read fold
                // takes the same position on all three (`exec::delta_says`), so the flush must
                // not lose one of them: the flushed row would say `embedding = 5` while nothing
                // suppressed the base, and the stale base vector would resurface *at the
                // flush* — the write silently undone by a background job. This is a **value**
                // removal (the value is gone): it stays suppressed regardless of any later label
                // churn (HIK-118), unlike the `label_removals` above.
                spec.value_removals.push(*dense);
            } else if nd.labels_added.contains(&vi.label) {
                // `SET n:Doc` brought the node **into** the index's scope — the mirror of the
                // `label_removals` arm above, and it has to be recorded for the same reason
                // (HIK-122).
                //
                // While the node was out of scope its embedding was an ordinary *column* value:
                // D12 only routes an embedding out of the row for a node that carries an index's
                // label, so an out-of-scope node's vector lives in the props record, and
                // `read_base_node_row` has already propagated it into the row this flush writes.
                // The row therefore *has* the vector — but the sidecar is what decides whether
                // anyone looks (the fold's candidate set is the sidecar ids ∪ removals), and no
                // level below holds an index entry for it either, because the base index only
                // ever indexes nodes that were in scope at build time.
                //
                // Leave it unnamed and the vector is reachable by **no query at all**: KNN never
                // sees it, and `suppress_indexed_vector` starts answering `Null` for the column
                // read the moment the label lands. `exec::delta_vector_for` resolves this same
                // node to `Set(v)` before the flush, so failing to name it here would also mean
                // the flush *changes the answer* — KNN-visible before, gone after.
                if let Some(Value::Vector(v)) = effective_rows
                    .get(dense)
                    .and_then(|p| p.iter().find(|(k, _)| k == &vi.property))
                    .map(|(_, v)| v)
                {
                    spec.ids.push(*dense);
                    entries.insert(*dense, v.clone());
                }
            }
            // Otherwise the delta says nothing about this node's embedding, and the level
            // below it (base or older segment) keeps whatever it had.
        }
        spec.ids.sort_unstable();
        spec.ids.dedup();
        spec.label_removals.sort_unstable();
        spec.label_removals.dedup();
        spec.value_removals.sort_unstable();
        spec.value_removals.dedup();

        // Seal a read-only Vamana over this segment's own embeddings when the live set crosses
        // the floor. Reuse the base's codebook (and its `max_norm`) when the base has a Vamana
        // index for this `(label, property)`; a brute-force base has none, so the sealer trains
        // one. Below the floor the sealer writes nothing and returns `None` (brute-force
        // fallback). Read from the deltas here — this is the one place the flush still has them.
        let entries: Vec<(u64, Vec<f32>)> = entries.into_iter().collect();
        let base = inp
            .core
            .vamana_index(&vi.label, &vi.property)
            .and_then(|bi| match &vi.mode {
                AnnMode::Vamana { max_norm, .. } => Some((&bi.pq.codebook, *max_norm as f64)),
                _ => None,
            });
        let meta = seal_segment_index(
            inp.seg_dir,
            &vi.label,
            &vi.property,
            &entries,
            vi.metric,
            vi.dim,
            base,
            inp.cipher.clone(),
            SEG_BLOCK_BYTES,
            SEG_ZSTD_LEVEL,
        )
        .with_context(|| format!("seal segment vector index {}.{}", vi.label, vi.property))?;

        vec_specs.push(spec);
        vec_metas.push(meta);
    }
    write_vector_fragments(inp.seg_dir, &vec_specs)
        .with_context(|| format!("write vector sidecar at {}", inp.seg_dir.display()))?;
    let dirty_vectors: Vec<DirtyVector> = vec_specs
        .iter()
        .zip(&vec_metas)
        .filter(|(s, meta)| {
            !s.ids.is_empty()
                || !s.label_removals.is_empty()
                || !s.value_removals.is_empty()
                || meta.is_some()
        })
        .map(|(s, meta)| DirtyVector {
            label: s.label.clone(),
            property: s.prop.clone(),
            graph: *meta,
        })
        .collect();

    // ── posting fragments: per reltype, ascending-distinct born src/tgt endpoint ids. Only a
    // live born edge drives a scan; a delete removes nothing from the (additive) base postings
    // — a stale driving hit is filtered by the adjacency removal above at read time. ─────────
    let mut post: BTreeMap<String, (Vec<u64>, Vec<u64>)> = BTreeMap::new();
    for (src, dst, reltype) in live_born_edges.values() {
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
    // only where the effective row's label set differs from its base-below-delta set. A core
    // delete is the degenerate patch to the empty row: the node count nets down by one and
    // every base label with it (the empty `after` differs from every `before` label).
    let mut node_count_delta: i64 = 0;
    let mut label_node_deltas: BTreeMap<String, i64> = BTreeMap::new();
    // Every label whose *membership* any node row changes vs the base — the per-node symmetric
    // difference, unioned WITHOUT cancellation (unlike `label_node_deltas`, a drop-from-A +
    // add-to-B that nets zero still changes the id set, so both labels stay touched). Lets a
    // label scan skip a segment that touches none of the scanned label's members.
    let mut label_membership_touch: BTreeSet<String> = BTreeSet::new();
    for n in &seg_nodes {
        match &n.base {
            None => {
                if n.tombstoned {
                    continue; // born-then-deleted: nets to nothing
                }
                node_count_delta += 1;
                for l in &n.labels {
                    *label_node_deltas.entry(l.clone()).or_insert(0) += 1;
                    // A born node introduces a new id under each of its labels.
                    label_membership_touch.insert(l.clone());
                }
            }
            Some((_, base_labels)) => {
                if n.tombstoned {
                    node_count_delta -= 1; // a core delete drops the node the base still counts
                }
                let before: BTreeSet<&str> = base_labels.iter().map(String::as_str).collect();
                let after: BTreeSet<&str> = n.labels.iter().map(String::as_str).collect();
                for l in after.difference(&before) {
                    *label_node_deltas.entry((*l).to_string()).or_insert(0) += 1;
                    label_membership_touch.insert((*l).to_string());
                }
                for l in before.difference(&after) {
                    *label_node_deltas.entry((*l).to_string()).or_insert(0) -= 1;
                    label_membership_touch.insert((*l).to_string());
                }
            }
        }
    }
    // Drop labels whose net change cancels to zero so the sparse manifest stays minimal.
    label_node_deltas.retain(|_, d| *d != 0);
    // Edge marginals: a live born edge adds one to its reltype; a suppressed core edge (a
    // deleted node's incident edge, or an explicit `DELETE r`) subtracts one. The two sets are
    // disjoint by construction (a born edge is not in the base to be suppressed).
    let mut reltype_edge_deltas: BTreeMap<String, i64> = BTreeMap::new();
    let mut edge_count_delta: i64 = 0;
    for (_src, _dst, reltype) in live_born_edges.values() {
        *reltype_edge_deltas.entry(reltype.clone()).or_insert(0) += 1;
        edge_count_delta += 1;
    }
    for (_src, _dst, reltype) in suppressed.values() {
        *reltype_edge_deltas.entry(reltype.clone()).or_insert(0) -= 1;
        edge_count_delta -= 1;
    }
    reltype_edge_deltas.retain(|_, d| *d != 0);

    // Per-node degree deltas for the hub sidecar (Component 2): a born edge adds one to its
    // src's out-degree and its dst's in-degree; a suppressed edge subtracts one from each.
    // Derived from the same disjoint born/suppressed sets as the edge marginals. Only nodes
    // whose `|Δ| >=` the hub floor are recorded (sparse), ascending by id.
    let mut out_deg_delta: HashMap<u64, i64> = HashMap::new();
    let mut in_deg_delta: HashMap<u64, i64> = HashMap::new();
    for (src, dst, _reltype) in live_born_edges.values() {
        *out_deg_delta.entry(*src).or_insert(0) += 1;
        *in_deg_delta.entry(*dst).or_insert(0) += 1;
    }
    for (src, dst, _reltype) in suppressed.values() {
        *out_deg_delta.entry(*src).or_insert(0) -= 1;
        *in_deg_delta.entry(*dst).or_insert(0) -= 1;
    }
    let hub_floor = graph_format::hubdegree::DEFAULT_HUB_DEGREE_FLOOR as i64;
    let to_hub_deltas = |m: HashMap<u64, i64>| -> Vec<(u64, i64)> {
        let mut v: Vec<(u64, i64)> = m
            .into_iter()
            .filter(|(_, d)| d.abs() >= hub_floor)
            .collect();
        v.sort_unstable();
        v
    };
    let hub_degree_out_deltas = to_hub_deltas(out_deg_delta);
    let hub_degree_in_deltas = to_hub_deltas(in_deg_delta);

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
        encryption: inp.encryption_header.clone(),
        node_count_delta,
        edge_count_delta,
        reltype_edge_deltas: reltype_edge_deltas.into_iter().collect(),
        label_node_deltas: label_node_deltas.into_iter().collect(),
        hub_degree_out_deltas,
        hub_degree_in_deltas,
        marginals_exact: true,
        dirty_indexes,
        dirty_vectors,
        // Authoritative and exact: the flush materialises every node row, so it knows precisely
        // which labels changed membership (empty ⇒ a pure property/edge patch the label scan
        // can skip).
        label_membership_touch: Some(label_membership_touch.into_iter().collect()),
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

/// Read edge `id`'s **base-below-delta** property map (keyed by name) as the core stack sees
/// it under this flush's delta: a lower segment's winning full row, else the base
/// generation's edge props. The edge mirror of [`read_base_node_row`], matching
/// [`Executor::core_named_edge_props`](crate::exec) so the overlaid full row equals what a
/// pre-flush `RETURN r.p` returned. A base row that is already a tombstone would mean a patch
/// on a deleted edge — the write path never produces that, so it is refused.
fn read_base_edge_row(core: &Generation, id: u64) -> Result<BTreeMap<String, Value>> {
    if let Some(row) = core.stack().resolve_edge_row(id)? {
        if row.tombstoned {
            bail!("flush_to_segment: edge {id} patches a base-tombstoned edge — unexpected");
        }
        return Ok(row.props.into_iter().collect());
    }
    let props = core
        .edge_props()
        .props(id)
        .with_context(|| format!("read base props for core-patched edge {id}"))?
        .into_iter()
        .map(|(kid, v)| (core.property_key_name(kid).unwrap_or("?").to_string(), v))
        .collect();
    Ok(props)
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

#[cfg(test)]
thread_local! {
    // Test-only per-thread call counter for `effective_adj`, proving the O(D²)→O(D)
    // memoisation (the flush runs inline on the caller's thread, so this is not polluted by
    // concurrently-running tests). See `effective_adj_memoised_per_hub_on_multi_edge_delete`.
    pub(crate) static EFFECTIVE_ADJ_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Read node `node`'s **effective adjacency** below this flush in direction `outgoing`: the
/// base CSR folded with every lower core segment's fragment, mirroring
/// [`for_each_adj_overlaid`](crate::exec) (oldest→newest — a `removed` fragment suppresses by
/// edge id, a born fragment appends). Returns `(edge_id, neighbour, reltype-name)` for each
/// surviving incident edge — the input to a delete's removal fragments and netted marginals.
/// A born endpoint id (≥ the base node count) has no base CSR record; its edges come wholly
/// from the segment fragments.
fn effective_adj(core: &Generation, node: u64, outgoing: bool) -> Result<Vec<(u64, u64, String)>> {
    #[cfg(test)]
    EFFECTIVE_ADJ_CALLS.with(|c| c.set(c.get() + 1));
    let base_nodes = core.topology().node_count();
    let mut list: Vec<(u64, u64, String)> = if node < base_nodes {
        let adjs = if outgoing {
            core.topology().outgoing(NodeId(node))
        } else {
            core.topology().incoming(NodeId(node))
        }
        .with_context(|| format!("read base adjacency for node {node}"))?;
        adjs.into_iter()
            .map(|a| {
                let rt = core.reltype_name(a.reltype).unwrap_or("").to_string();
                (a.edge.0, a.neighbour.0, rt)
            })
            .collect()
    } else {
        Vec::new()
    };
    for seg in core.stack().segments() {
        let r = &seg.reader;
        let frag = if outgoing {
            if !r.may_hold_out_adj(node) {
                continue;
            }
            r.out_adj(node)?
        } else {
            if !r.may_hold_in_adj(node) {
                continue;
            }
            r.in_adj(node)?
        };
        if frag.is_empty() {
            continue;
        }
        let mut removed: BTreeSet<u64> = BTreeSet::new();
        let mut born: Vec<(u64, u64, String)> = Vec::new();
        for e in frag {
            if e.removed {
                removed.insert(e.edge_id);
            } else {
                born.push((e.edge_id, e.other, e.reltype));
            }
        }
        if !removed.is_empty() {
            list.retain(|(eid, _, _)| !removed.contains(eid));
        }
        list.extend(born);
    }
    Ok(list)
}

/// Build the sealed manifest's file inventory: every file in the segment dir (all sections
/// and fragments; `SEGMENT.json` is written *after* and is never in its own inventory),
/// each with its BLAKE3 hash, name-sorted so the content hash is deterministic. Shared with
/// the T3 merge writer ([`crate::merge_segment`]) so the two produce byte-consistent
/// manifests.
pub(crate) fn inventory(seg_dir: &Path) -> Result<Vec<FileEntry>> {
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
