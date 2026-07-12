// SPDX-License-Identifier: Apache-2.0
//! The **T3 segment↔segment merge** writer: fold a contiguous run of immutable upper core
//! segments into one, O(inputs) (`docs/SEGMENTED-CORE-PLAN.md`, Phase 5).
//!
//! A T2 flush ([`crate::flush_segment`]) appends one small segment per fold, so the stack
//! grows with write traffic; unbounded, a point read would fan out across every segment. T3
//! compaction bounds that fan-out: it merges a run of adjacent segments into a single
//! segment that is their **newest-wins fold**, reads identically to the run it replaces, and
//! is O(inputs) (it reads only the merged segments, never the base). The merged segment
//! takes the run's ordinal position in the set, so precedence is preserved: everything below
//! the run stays below the merged segment, everything above stays above.
//!
//! # Why summed marginals are exact
//! The merged segment must contribute the same signed count deltas as the run it replaces
//! (its read semantics are identical), and the run's contribution is the sum of its members'
//! deltas. So the merged manifest's marginals are simply the **sum** of the inputs'
//! (`marginals_exact` = AND). A born-then-deleted id nets to zero across the run — its
//! `+1` (birth) and `-1` (delete) cancel in the sum — which is exactly what dropping the
//! reclaimed row leaves.
//!
//! # The fold, newest-wins with reclamation
//! Segments hold **full rows** (replace semantics), so each dimension folds independently:
//! - **Node / edge rows** — the newest input carrying an id wins in one read. A tombstone
//!   for an id **born within the run** (in the run's own band) is **reclaimed** (dropped
//!   entirely — no layer below the run holds it); a tombstone for a **below-run** id (base or
//!   a segment beneath the run) is kept, so it keeps superseding that lower row.
//! - **Adjacency fragments** — per node, fold the inputs' `out_adj`/`in_adj` fragments
//!   oldest→newest (a `removed` entry suppresses a prior born append by edge id, a born entry
//!   appends), mirroring [`for_each_adj_overlaid`](crate::exec). A born-then-removed edge born
//!   within the run cancels (reclaimed); a `removed` of a below-run edge is carried so it
//!   keeps suppressing the base/lower fragment.
//! - **Index fragments** — per `(label, prop)`, fold the inputs' entry id-sets + removal
//!   sidecars oldest→newest (newest-wins). The winning entries' **values** are read from the
//!   merged (full-row) node — segments carry no `(value, id)` iterator, but every live index
//!   id has a node row here (index entries derive from node props), so the row is the
//!   authoritative value. A removal of a **below-run** id is carried (to keep suppressing the
//!   base entry); a removal of a within-run born id is reclaimed with its entry.
//! - **Postings** — the per-reltype driving sets union (a superset is always correct; a
//!   stale hit is filtered by the folded adjacency at read time).
//!
//! # Encryption
//! Like a flush, a merge over an encrypted stack writes a **fresh per-segment** cipher +
//! KDF header derived from the runtime master key; the read side re-derives it on reopen
//! ([`crate::segstack::derive_segment_cipher`]).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use graph_format::crypto::BlockCipher;
use graph_format::ids::{Generation as GenId, Value};
use graph_format::manifest::EncryptionHeader;
use graph_format::segindex::{write_index_fragments, IndexSpec};
use graph_format::segmanifest::{
    DirtyIndex, SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION,
};
use graph_format::segment::{AdjEdge, EdgeRow, NodeRow, SegmentWriter};
use graph_format::segpostings::{write_posting_fragments, PostingSpec};

use crate::flush_segment::{inventory, SEG_BLOCK_BYTES, SEG_ZSTD_LEVEL};
use crate::segstack::LoadedSegment;

/// Open-time context for materialising a merged segment. The `inputs` — the contiguous run
/// of segments to fold, **oldest → newest** — are passed to [`write_merge_segment`]
/// alongside this, mirroring [`crate::flush_segment::write_flush_segment`]'s `(mem, inp)`.
pub struct MergeInputs<'a> {
    /// The merged segment's final directory: `<data_dir>/<graph>/segments/<seg_uuid>`.
    pub seg_dir: &'a Path,
    /// The merged segment's fresh uuid (its directory name and manifest id).
    pub seg_uuid: GenId,
    /// The base generation the run (and so the merged segment) deltas over — unchanged.
    pub base_uuid: GenId,
    /// The at-rest block cipher for the merged segment's sections (`None` = plaintext), and
    /// the runtime master key used to seal the manifest MAC (`None` = no MAC).
    pub cipher: Option<Arc<BlockCipher>>,
    pub master_key: Option<&'a [u8]>,
    /// The manifest encryption header (AEAD/KDF names + this segment's fresh KDF salt) so
    /// [`crate::segstack`] re-derives the same cipher on reopen. `Some` iff `cipher` is.
    pub encryption_header: Option<EncryptionHeader>,
    /// Wall-clock stamp for the manifest (the caller supplies it — the runtime is clock-free).
    pub created_unix: i64,
}

/// Fold `inputs` — a contiguous run of upper core segments, **oldest → newest** — into one
/// merged segment at `inp.seg_dir`, writing every section, index/posting fragment, and a
/// sealed `SEGMENT.json` whose marginals are the sum of the inputs'. Returns the sealed
/// manifest, from which the caller derives a [`SegmentRef`](graph_format::setmanifest::SegmentRef)
/// to splice into the new set in place of the run. Requires at least two inputs whose bands
/// tile contiguously (the caller passes a real ordinal run).
pub fn write_merge_segment(
    inputs: &[&LoadedSegment],
    inp: &MergeInputs,
) -> Result<SegmentManifest> {
    if inputs.len() < 2 {
        bail!(
            "merge needs at least two segments to fold, got {}",
            inputs.len()
        );
    }

    // ── run bands: the merged segment owns the union of the inputs' bands. The inputs are a
    // contiguous ordinal run, so their bands tile (each starts where the previous ended,
    // possibly zero-width); [min start, max end) covers exactly the run's born ids with no
    // foreign id and no gap. Verify the tiling the caller promised. ────────────────────────
    for w in inputs.windows(2) {
        let (prev, cur) = (&w[0].manifest, &w[1].manifest);
        if cur.node_band.0 != prev.node_band.1 {
            bail!(
                "merge run is not node-contiguous: band {:?} does not follow {:?}",
                cur.node_band,
                prev.node_band
            );
        }
        if cur.edge_band.0 != prev.edge_band.1 {
            bail!(
                "merge run is not edge-contiguous: band {:?} does not follow {:?}",
                cur.edge_band,
                prev.edge_band
            );
        }
    }
    let node_band = (
        inputs.first().unwrap().manifest.node_band.0,
        inputs.last().unwrap().manifest.node_band.1,
    );
    let edge_band = (
        inputs.first().unwrap().manifest.edge_band.0,
        inputs.last().unwrap().manifest.edge_band.1,
    );
    // An id born *within the run* (in its own band) has no layer below the run to supersede,
    // so its tombstone/removal is reclaimable; a below-run id's is carried.
    let within_run_node = |id: u64| node_band.0 <= id && id < node_band.1;
    let within_run_edge = |id: u64| edge_band.0 <= id && id < edge_band.1;

    // ── node rows: newest input carrying an id wins (full row). Reclaim a tombstone for a
    // within-run born id (nothing below holds it); keep a below-run tombstone. ──────────────
    let mut node_rows: BTreeMap<u64, NodeRow> = BTreeMap::new();
    for seg in inputs {
        for &id in seg.reader.node_ids() {
            if let Some(row) = seg.reader.node_row(id)? {
                node_rows.insert(id, row); // oldest→newest: a later input overwrites
            }
        }
    }
    node_rows.retain(|&id, row| !(row.tombstoned && within_run_node(id)));

    // ── edge rows: newest input wins; reclaim a within-run born tombstone. (The flush writer
    // deletes via adjacency `removed` fragments, not edge-row tombstones, so a tombstoned
    // edge row is defensive; a live born-then-deleted edge row is left in place — its
    // adjacency is suppressed by the fold below, matching pre-merge read semantics.) ────────
    let mut edge_rows: BTreeMap<u64, EdgeRow> = BTreeMap::new();
    for seg in inputs {
        for &id in seg.reader.edge_ids() {
            if let Some(row) = seg.reader.edge_row(id)? {
                edge_rows.insert(id, row);
            }
        }
    }
    edge_rows.retain(|&id, row| !(row.tombstoned && within_run_edge(id)));

    // ── adjacency: per node, fold the run's fragments oldest→newest. A born entry appends; a
    // `removed` entry cancels a within-run born append (reclaimed) or, for a below-run edge,
    // is carried so it keeps suppressing the base/lower fragment. ────────────────────────────
    let out_frags = fold_adjacency(inputs, /*outgoing=*/ true, &within_run_edge)?;
    let in_frags = fold_adjacency(inputs, /*outgoing=*/ false, &within_run_edge)?;

    // ── open the section writer and stream the four sorted sections (BTreeMap ⇒ ascending) ──
    std::fs::create_dir_all(inp.seg_dir)
        .with_context(|| format!("create segment dir {}", inp.seg_dir.display()))?;
    let mut w = SegmentWriter::create_with_cipher(
        inp.seg_dir,
        inp.seg_uuid.0.as_u128(),
        SEG_BLOCK_BYTES,
        SEG_ZSTD_LEVEL,
        inp.cipher.clone(),
    )
    .with_context(|| format!("create merge segment writer at {}", inp.seg_dir.display()))?;

    for (id, row) in &node_rows {
        w.push_node(*id, row)
            .with_context(|| format!("push merged node {id}"))?;
    }
    for (src, frag) in &out_frags {
        w.push_adj_out(*src, frag)
            .with_context(|| format!("push merged out-adjacency for {src}"))?;
    }
    for (dst, frag) in &in_frags {
        w.push_adj_in(*dst, frag)
            .with_context(|| format!("push merged in-adjacency for {dst}"))?;
    }
    for (id, row) in &edge_rows {
        w.push_edge(*id, row)
            .with_context(|| format!("push merged edge {id}"))?;
    }
    w.finish()
        .with_context(|| format!("finish merged sections at {}", inp.seg_dir.display()))?;

    // ── index fragments: per (label, prop), fold entry id-sets + removals oldest→newest, then
    // read each live id's value from its merged (full) node row. ─────────────────────────────
    let specs = fold_index(inputs, &node_rows, &within_run_node)?;
    if !specs.is_empty() {
        write_index_fragments(
            inp.seg_dir,
            &specs,
            SEG_BLOCK_BYTES,
            SEG_ZSTD_LEVEL,
            inp.cipher.clone(),
        )
        .with_context(|| format!("write merged index fragments at {}", inp.seg_dir.display()))?;
    }

    // ── posting fragments: union the per-reltype driving sets (a superset stays correct) ────
    let posting_specs = fold_postings(inputs);
    if !posting_specs.is_empty() {
        write_posting_fragments(inp.seg_dir, &posting_specs).with_context(|| {
            format!(
                "write merged posting fragments at {}",
                inp.seg_dir.display()
            )
        })?;
    }

    // ── marginals: sum the inputs' signed deltas (exact iff every input is) ──────────────────
    let mut node_count_delta: i64 = 0;
    let mut edge_count_delta: i64 = 0;
    let mut label_node_deltas: BTreeMap<String, i64> = BTreeMap::new();
    let mut reltype_edge_deltas: BTreeMap<String, i64> = BTreeMap::new();
    let mut marginals_exact = true;
    // Union the run's membership-touch sets: a label the merged segment changes vs the
    // segment below the run must have been changed by some input (changes compose), so the
    // union is a correct (conservative) touch set. One `None` (unknown) input poisons the whole
    // union to `None` — we cannot assert completeness the reader would trust to skip.
    let mut label_membership_touch: Option<BTreeSet<String>> = Some(BTreeSet::new());
    // Per-node hub-degree deltas compose across the run the same way — sum each input's
    // signed contribution, then re-threshold (a node whose net drops below the floor is
    // dropped; net zero cancels). Summing already-thresholded inputs can miss a node whose
    // per-input deltas were each sub-floor but sum over the floor — harmless: such a node
    // has bounded (~floor × run) degree and materialises cheaply; only million-edge hubs,
    // which any single input records, must be caught.
    let mut hub_out: BTreeMap<u64, i64> = BTreeMap::new();
    let mut hub_in: BTreeMap<u64, i64> = BTreeMap::new();
    for seg in inputs {
        let m = &seg.manifest;
        node_count_delta += m.node_count_delta;
        edge_count_delta += m.edge_count_delta;
        for (l, d) in &m.label_node_deltas {
            *label_node_deltas.entry(l.clone()).or_insert(0) += *d;
        }
        for (t, d) in &m.reltype_edge_deltas {
            *reltype_edge_deltas.entry(t.clone()).or_insert(0) += *d;
        }
        for (id, d) in &m.hub_degree_out_deltas {
            *hub_out.entry(*id).or_insert(0) += *d;
        }
        for (id, d) in &m.hub_degree_in_deltas {
            *hub_in.entry(*id).or_insert(0) += *d;
        }
        marginals_exact &= m.marginals_exact;
        label_membership_touch = match (label_membership_touch.take(), &m.label_membership_touch) {
            (Some(mut acc), Some(set)) => {
                acc.extend(set.iter().cloned());
                Some(acc)
            }
            _ => None,
        };
    }
    label_node_deltas.retain(|_, d| *d != 0);
    reltype_edge_deltas.retain(|_, d| *d != 0);
    let hub_floor = graph_format::hubdegree::DEFAULT_HUB_DEGREE_FLOOR as i64;
    let hub_degree_out_deltas: Vec<(u64, i64)> = hub_out
        .into_iter()
        .filter(|(_, d)| d.abs() >= hub_floor)
        .collect();
    let hub_degree_in_deltas: Vec<(u64, i64)> = hub_in
        .into_iter()
        .filter(|(_, d)| d.abs() >= hub_floor)
        .collect();

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
        marginals_exact,
        dirty_indexes,
        label_membership_touch: label_membership_touch.map(|s| s.into_iter().collect()),
        mac: None,
        files,
    };
    manifest.set_content_hash();
    if let Some(key) = inp.master_key {
        manifest.seal_mac(key).context("seal merge segment MAC")?;
    }
    manifest
        .verify_marginals()
        .context("self-check merge segment marginals")?;
    manifest
        .write_to_dir(inp.seg_dir)
        .with_context(|| format!("write SEGMENT.json at {}", inp.seg_dir.display()))?;

    Ok(manifest)
}

/// Fold the run's adjacency fragments in one direction into a per-node merged fragment map
/// (ascending node id). For each node with a fragment in any input, replay the inputs
/// oldest→newest: a born entry appends (newest wins by edge id), a `removed` entry cancels a
/// within-run born append (reclaimed) else — for a below-run edge — is carried as a removal.
fn fold_adjacency(
    inputs: &[&LoadedSegment],
    outgoing: bool,
    within_run_edge: &impl Fn(u64) -> bool,
) -> Result<BTreeMap<u64, Vec<AdjEdge>>> {
    let mut nodes: BTreeSet<u64> = BTreeSet::new();
    for seg in inputs {
        let keys = if outgoing {
            seg.reader.adj_out_ids()
        } else {
            seg.reader.adj_in_ids()
        };
        nodes.extend(keys.iter().copied());
    }

    let mut frags: BTreeMap<u64, Vec<AdjEdge>> = BTreeMap::new();
    for node in nodes {
        let mut born: BTreeMap<u64, AdjEdge> = BTreeMap::new();
        let mut removed: BTreeMap<u64, AdjEdge> = BTreeMap::new();
        for seg in inputs {
            let frag = if outgoing {
                if !seg.reader.may_hold_out_adj(node) {
                    continue;
                }
                seg.reader.out_adj(node)?
            } else {
                if !seg.reader.may_hold_in_adj(node) {
                    continue;
                }
                seg.reader.in_adj(node)?
            };
            for e in frag {
                if e.removed {
                    // Cancel a within-run born append; else — a below-run edge — carry the
                    // removal so the merged fragment keeps suppressing the base/lower entry.
                    // A within-run edge always has its born append seen first (oldest→newest,
                    // born precedes removal), so it never leaks into `removed`.
                    if born.remove(&e.edge_id).is_none() && !within_run_edge(e.edge_id) {
                        removed.insert(e.edge_id, e);
                    }
                } else {
                    born.insert(e.edge_id, e); // newest born wins (idempotent by id)
                }
            }
        }
        if born.is_empty() && removed.is_empty() {
            continue; // fully reclaimed — no fragment
        }
        let frag: Vec<AdjEdge> = born.into_values().chain(removed.into_values()).collect();
        frags.insert(node, frag);
    }
    Ok(frags)
}

/// Fold the run's index fragments into merged [`IndexSpec`]s. Per `(label, prop)`, replay the
/// inputs oldest→newest to decide the live entry id-set (a removal drops an older within-run
/// entry; a below-run removal is carried) then read each live id's value from its merged node
/// row — the authoritative full-row value (segments carry no `(value, id)` iterator).
fn fold_index(
    inputs: &[&LoadedSegment],
    node_rows: &BTreeMap<u64, NodeRow>,
    within_run_node: &impl Fn(u64) -> bool,
) -> Result<Vec<IndexSpec>> {
    let mut pairs: BTreeSet<(String, String)> = BTreeSet::new();
    for seg in inputs {
        if let Some(idx) = &seg.index {
            for (l, p) in idx.indexed() {
                pairs.insert((l.to_string(), p.to_string()));
            }
        }
    }

    let mut specs: Vec<IndexSpec> = Vec::new();
    for (label, prop) in &pairs {
        let mut live: BTreeSet<u64> = BTreeSet::new();
        let mut removals: BTreeSet<u64> = BTreeSet::new();
        for seg in inputs {
            let Some(idx) = &seg.index else { continue };
            for &id in idx.removals(label, prop) {
                live.remove(&id);
                if !within_run_node(id) {
                    removals.insert(id); // keep suppressing the base/lower entry
                }
            }
            // The full-sweep range probe (both bounds open) yields every id this fragment
            // holds an entry for; the value is taken from the merged node row below.
            for id in idx.lookup_range(label, prop, None, true, None, true)? {
                live.insert(id);
            }
        }

        let mut entries: Vec<(Value, u64)> = Vec::new();
        for &id in &live {
            let row = node_rows.get(&id).ok_or_else(|| {
                anyhow::anyhow!("merge index ({label},{prop}): live id {id} has no merged node row")
            })?;
            if row.tombstoned {
                continue; // a tombstoned row indexes nothing (its removal is already carried)
            }
            let (_, v) = row.props.iter().find(|(k, _)| k == prop).ok_or_else(|| {
                anyhow::anyhow!(
                    "merge index ({label},{prop}): live id {id}'s row lacks the indexed prop"
                )
            })?;
            entries.push((v.clone(), id));
        }
        let removals: Vec<u64> = removals.into_iter().collect(); // BTreeSet ⇒ ascending, distinct
        if !entries.is_empty() || !removals.is_empty() {
            specs.push(IndexSpec {
                label: label.clone(),
                prop: prop.clone(),
                entries,
                removals,
            });
        }
    }
    Ok(specs)
}

/// Union the run's posting driving sets per reltype (ascending, distinct). A superset is
/// always correct: a stale driving hit for a removed edge is filtered by the folded
/// adjacency at read time, so postings need no removal tracking.
/// The size ratio within which two segments count as the **same tier** for size-tiered run
/// selection: a run is same-tier when its largest member is no more than [`SIZE_TIER_RATIO`]×
/// its smallest. Folding within a tier bounds write amplification — each byte is rewritten at
/// most once per tier climbed, never repeatedly against a much larger segment (the
/// size-tiered-compaction invariant, adapted to a stacked core's contiguity constraint: only
/// *adjacent* segments may fold, because their id bands must stay contiguous).
pub const SIZE_TIER_RATIO: u64 = 4;

/// The minimum run length a compaction folds: two segments (folding one is a no-op).
pub const MIN_COMPACTION_RUN: usize = 2;

/// Size-tiered run selection (Phase 5 slice 5.3, the fourth rung of the D50 ladder).
///
/// Given the upper segments' on-disk `sizes` (oldest → newest, one entry per segment) and the
/// admission threshold `max_upper_segments`, choose the contiguous run `[start, end)` to fold,
/// or `None` when no compaction is admissible.
///
/// **Admission** is by segment *count* — the read-fan-out cost, since a point read may consult
/// every segment: the stack is compacted only once it exceeds `max_upper_segments`
/// (`0` disables admission entirely; the explicit [`Graphs::compact_graph_segments`](crate::server::Graphs::compact_graph_segments)
/// path is unaffected). **Selection** is *size-tiered*: among the contiguous runs of ≥
/// [`MIN_COMPACTION_RUN`] same-tier segments (largest ≤ [`SIZE_TIER_RATIO`]× smallest), pick the
/// **longest** (it reduces fan-out most while rewriting each byte exactly once), tie-breaking by
/// the **smallest** total bytes (prefer the cheaper, smaller tier). If no two adjacent segments
/// share a tier (sizes escalate by more than the ratio at every step), fall back to the adjacent
/// **pair** with the smallest combined bytes — the cheapest merge that still reduces the count,
/// so the policy always makes progress while over budget.
///
/// The returned run is always a valid input to `write_merge_segment` — contiguous and of length
/// ≥ [`MIN_COMPACTION_RUN`].
pub fn select_compaction_run(sizes: &[u64], max_upper_segments: usize) -> Option<(usize, usize)> {
    // Admission: disabled, or the stack is within its fan-out budget (and so short it can't fold).
    if max_upper_segments == 0
        || sizes.len() <= max_upper_segments
        || sizes.len() < MIN_COMPACTION_RUN
    {
        return None;
    }

    // A zero-size run (only a defensive case — a segment always carries at least a meta file)
    // is same-tier with anything; otherwise the largest must be within the ratio of the smallest.
    let same_tier = |lo: u64, hi: u64| lo == 0 || hi <= lo.saturating_mul(SIZE_TIER_RATIO);

    // Longest contiguous same-tier run, tie-broken by smallest total bytes. Runs from each start
    // are considered independently (O(n²) over the segment count, which is tens at most) because
    // dropping a run's smallest member can *raise* the floor and admit a longer run to its right.
    let mut best: Option<(usize, usize, u64)> = None; // (start, end, total_bytes)
    for i in 0..sizes.len() {
        let (mut lo, mut hi, mut total) = (sizes[i], sizes[i], sizes[i]);
        let mut j = i + 1;
        while j < sizes.len() {
            let (nlo, nhi) = (lo.min(sizes[j]), hi.max(sizes[j]));
            if !same_tier(nlo, nhi) {
                break;
            }
            lo = nlo;
            hi = nhi;
            total = total.saturating_add(sizes[j]);
            j += 1;
        }
        let len = j - i;
        if len < MIN_COMPACTION_RUN {
            continue;
        }
        let better = match best {
            None => true,
            Some((bs, be, bt)) => len > be - bs || (len == be - bs && total < bt),
        };
        if better {
            best = Some((i, j, total));
        }
    }
    if let Some((start, end, _)) = best {
        return Some((start, end));
    }

    // No two adjacent segments share a tier: fall back to the cheapest adjacent pair so the
    // count still drops. `sizes.len() >= MIN_COMPACTION_RUN` here, so the loop is non-empty.
    let (mut best_start, mut best_bytes) = (0usize, u64::MAX);
    for k in 0..sizes.len() - 1 {
        let combined = sizes[k].saturating_add(sizes[k + 1]);
        if combined < best_bytes {
            best_bytes = combined;
            best_start = k;
        }
    }
    Some((best_start, best_start + 2))
}

fn fold_postings(inputs: &[&LoadedSegment]) -> Vec<PostingSpec> {
    let mut post: BTreeMap<String, (BTreeSet<u64>, BTreeSet<u64>)> = BTreeMap::new();
    for seg in inputs {
        let Some(p) = &seg.postings else { continue };
        for rt in p.reltypes() {
            let e = post.entry(rt.to_string()).or_default();
            e.0.extend(p.src_ids(rt).iter().copied());
            e.1.extend(p.tgt_ids(rt).iter().copied());
        }
    }
    post.into_iter()
        .map(|(reltype, (src, tgt))| PostingSpec {
            reltype,
            src_ids: src.into_iter().collect(),
            tgt_ids: tgt.into_iter().collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::select_compaction_run;

    #[test]
    fn admission_is_disabled_when_threshold_is_zero() {
        // `0` never admits, regardless of how tall the stack is.
        assert_eq!(select_compaction_run(&[10, 10, 10, 10], 0), None);
    }

    #[test]
    fn a_stack_within_budget_is_not_compacted() {
        // len <= max_upper_segments ⇒ within the fan-out budget ⇒ no-op.
        assert_eq!(select_compaction_run(&[10, 10, 10], 3), None);
        assert_eq!(select_compaction_run(&[10, 10, 10], 4), None);
        // Too short to fold at all.
        assert_eq!(select_compaction_run(&[10], 1), None);
        assert_eq!(select_compaction_run(&[], 1), None);
    }

    #[test]
    fn uniform_over_budget_folds_the_whole_stack() {
        // All same-tier ⇒ the longest run is the entire stack.
        assert_eq!(select_compaction_run(&[10, 10, 10, 10], 2), Some((0, 4)));
    }

    #[test]
    fn longest_same_tier_run_wins_over_a_shorter_one() {
        // [0,3) are within a 4× band (10..=30); seg 3 (1000) is its own tier. The length-3 run
        // beats any length-2, and the lone big segment is left above.
        assert_eq!(select_compaction_run(&[10, 20, 30, 1000], 2), Some((0, 3)));
    }

    #[test]
    fn ties_break_toward_the_smaller_tier() {
        // Two disjoint same-tier runs of equal length: the cheaper (smaller-byte) one is chosen.
        // [10,20] (tier ~10, total 30) vs [400,500] (tier ~400, total 900) ⇒ the small tier.
        assert_eq!(select_compaction_run(&[10, 20, 400, 500], 2), Some((0, 2)));
    }

    #[test]
    fn a_dropped_floor_can_admit_a_longer_run_to_the_right() {
        // From seg 0 the run stops at seg 2 (10*4 = 40 < 100). But starting at seg 1 raises the
        // floor to 40, and 100,120 <= 40*4 = 160 ⇒ [1,4) is a length-3 run the greedy-from-0 scan
        // would miss. The independent per-start scan finds it.
        assert_eq!(select_compaction_run(&[10, 40, 100, 120], 2), Some((1, 4)));
    }

    #[test]
    fn strictly_escalating_sizes_fall_back_to_the_cheapest_pair() {
        // Every adjacent pair is 100× apart — well beyond SIZE_TIER_RATIO — so no same-tier run
        // exists; the policy still reduces the count by folding the two cheapest adjacent segments.
        let sizes = [1u64, 100, 10_000, 1_000_000];
        assert_eq!(select_compaction_run(&sizes, 2), Some((0, 2)));
    }

    #[test]
    fn a_zero_width_segment_joins_its_neighbours() {
        // A defensive case: a 0-byte segment is same-tier with anything, so it folds in.
        assert_eq!(select_compaction_run(&[0, 10, 20], 2), Some((0, 3)));
    }
}
