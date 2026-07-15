// SPDX-License-Identifier: Apache-2.0
//! FreshDiskANN's `StreamingMerge` — folding a batch of vector writes into a base Vamana
//! **without rebuilding the graph** (FreshDiskANN S6).
//!
//! # Why this exists
//!
//! Folding vector writes into the base used to reconstruct the whole Vamana graph from zero —
//! `O(N·R·L)` distance computations, hours at scale. Two earlier slices make that
//! unnecessary:
//!
//! * S1 ([`crate::vamana`]) made the `.vamana` record **id-free pure geometry** — the `.pq`
//!   id column is the single `layout → node_id` map. A consolidation permutes every dense id,
//!   but a file that mentions no dense id is *unaffected by that permutation*.
//! * S5 ([`crate::vamana_delete`]) established **holes, not compaction** — a deleted record
//!   keeps its layout ordinal; only its `.pq` id becomes [`HOLE`].
//!
//! So a consolidation's entire obligation to the vector index collapses to three operations,
//! in increasing cost:
//!
//! 1. **Pure permutation** (no deletes, no new vectors) — rewrite the small `.pq` id column
//!    (`new_id[layout] = perm.final_of(old_id[layout])`) and **carry the `.vamana` by
//!    reference, byte-identically**. Its BLAKE3 is unchanged; the graph is not touched at all.
//!    This is [`streaming_merge`]'s fast path and the whole thesis of the slice.
//! 2. **Deletes** — S5's splice + robust-prune pass, reused verbatim
//!    ([`crate::vamana_delete::consolidate_deletes`]).
//! 3. **Inserts** — greedy-search + robust-prune + back-link each new point into the *carried*
//!    graph, encoding with the **existing** codebook (no PQ retrain).
//!
//! # Cost, honestly
//!
//! `O(Δ·R·L)` compute + `O(N)` **sequential** IO. FreshDiskANN's StreamingMerge is `O(Δ)`
//! *compute*, but it **rewrites the index file** — it is **not** `O(Δ)` IO, and this does not
//! pretend otherwise. In-place patching is impossible: the `.vamana` is a zstd blockfile with
//! a footer directory of byte offsets, so re-encoding one record shifts every later block's
//! offset. The win is skipping the `O(N·R·L)` graph construction and the resident
//! materialisation a from-scratch build needs. The `O(Δ)`-IO patch layer (an LSM over the
//! vector index) is a deliberate follow-on, not built here.
//!
//! # The silent-failure surface
//!
//! The graph is carried by reference and only the id column is rewritten, so a subtly wrong
//! `layout → new_id` map produces KNN results that point at the **wrong nodes with
//! plausible-looking scores, no error, exit 0**. This module takes the composed id column as
//! an explicit input ([`MergeInputs::base_final_ids`], `HOLE` for a tombstoned ordinal) — the
//! composition `layout_to_dump_id ∘ perm.final_of` is the caller's, and is unit-tested where
//! it lives. The other trap is an **insert linking a live node at a hole**: greedy search
//! returns dead records as navigational waypoints in its visited pool, so [`merge_insert`]
//! **filters holes out of the candidate pool before pruning** — the defence of S5's
//! `no_live_node_references_a_hole` invariant.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};

use crate::crypto::BlockCipher;
use crate::manifest::Metric;
use crate::pq::{ann_point, sq_l2, PqReader, PqWriter, ResidentPq, HOLE};
use crate::vamana::{
    decode_node, greedy_search_over, robust_prune_over, AdjRead, AdjWrite, Expanded, PointSet,
    VamanaIndex, VamanaReader, VamanaWriter,
};
use crate::vamana_delete::{
    consolidate_deletes, recommended_cache_records, ConsolidateOpts, RECOMMENDED_CACHE_BLOCKS,
};

/// The scalar shape of one StreamingMerge — the graph's build parameters, carried forward
/// from the base's [`crate::manifest::AnnMode::Vamana`] so the merge measures distance in the
/// **same ANN space** the base was built in. Nothing here is re-derived from the new data;
/// re-deriving `max_norm` or the medoid from the merged set would silently change the space
/// the carried adjacency lives in.
#[derive(Clone)]
pub struct MergeParams {
    /// The base's fixed entry point (its layout ordinal). Carried unchanged — inserts append
    /// past the base, so the medoid ordinal stays valid.
    pub medoid: VamanaIndex,
    /// Out-degree bound `R`, as the base was built with.
    pub r: usize,
    /// Robust-prune long-edge factor `alpha`, as the base was built with.
    pub alpha: f32,
    /// Search-list width during an insert's greedy search (wider than `R` for candidates).
    pub l_build: usize,
    /// The index metric. With `max_norm` it defines the ANN space ([`ann_point`]).
    pub metric: Metric,
    /// `M = max‖x‖` over the base indexed set (the dot/MIPS augmentation constant), from the
    /// base MANIFEST. Read only for [`Metric::Dot`].
    pub max_norm: f64,
    /// Target block size for the output `.vamana`.
    pub vamana_block_bytes: usize,
    /// Target block size for the output `.pq`.
    pub pq_block_bytes: usize,
    pub zstd_level: i32,
    pub cipher: Option<Arc<BlockCipher>>,
}

/// The two data inputs to a merge: the rewritten id column and the new vectors.
pub struct MergeInputs<'a> {
    /// One entry per **base** layout ordinal: the new dense node id for that ordinal, or
    /// [`HOLE`] if the ordinal is tombstoned (deleted, or re-embedded and superseded by an
    /// entry in `inserts`). Length must equal the base record count.
    ///
    /// This is `layout_to_dump_id ∘ perm.final_of` already composed — the single riskiest
    /// input, held explicit so its construction is testable in isolation from the graph work.
    pub base_final_ids: &'a [u64],
    /// The Δ set: `(new_dense_id, raw_vector)` for every node embedded or re-embedded since
    /// the base. Inserted in this order (determinism), encoded with the base codebook.
    pub inserts: &'a [(u64, Vec<f32>)],
}

/// What one merge did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MergeStats {
    /// Base records (holes included).
    pub base_records: u64,
    /// Base ordinals that are holes after the merge.
    pub dead: u64,
    /// Δ records appended.
    pub inserted: u64,
    /// Records in the output `.vamana` (`base_records + inserted`).
    pub out_records: u64,
    /// Non-hole records in the output — the new `live_count`.
    pub live: u64,
    /// True when the `.vamana` was carried by reference (byte-identical) rather than rewritten
    /// — i.e. the pure-permutation fast path fired. This being true is the proof that
    /// carry-by-reference happened.
    pub vamana_carried: bool,
}

/// Run a StreamingMerge over one index's `.vamana` + `.pq`, writing a fresh, consistent pair
/// to `vamana_out` / `pq_out`.
///
/// The base graph is **carried, not rebuilt**: deletes patch it in place (S5), inserts extend
/// it (S6), and a merge with neither carries the `.vamana` byte-for-byte. The codebook is
/// carried untouched — **no PQ retrain**; use [`DriftMeter`] to decide when a full rebuild
/// (which retrains) is due.
///
/// Writes to fresh paths; the caller publishes by rename. Never patches in place.
pub fn streaming_merge(
    vamana_in: &Path,
    pq_in: &Path,
    inputs: &MergeInputs,
    params: &MergeParams,
    vamana_out: &Path,
    pq_out: &Path,
) -> Result<MergeStats> {
    let reader = VamanaReader::open_with_cipher(vamana_in, params.cipher.clone())
        .with_context(|| format!("open {}", vamana_in.display()))?;
    let base_pq = PqReader::open_with_cipher(pq_in, params.cipher.clone())
        .with_context(|| format!("open {}", pq_in.display()))?
        .load_resident()
        .with_context(|| format!("load {}", pq_in.display()))?;
    let base_count = reader.len() as usize;
    ensure!(
        base_pq.len() == base_count,
        "vector index is inconsistent: {} holds {base_count} records but {} holds {} — they \
         index each other by position",
        vamana_in.display(),
        pq_in.display(),
        base_pq.len()
    );
    ensure!(
        inputs.base_final_ids.len() == base_count,
        "base_final_ids has {} entries but the base .vamana holds {base_count} records — they \
         index each other by position",
        inputs.base_final_ids.len()
    );
    ensure!(params.r >= 1, "the out-degree bound R must be at least 1");
    ensure!(
        base_count <= u32::MAX as usize,
        "a .vamana with {base_count} records exceeds the u32 layout-ordinal space"
    );

    // A pre-existing hole must stay a hole. The caller composes `HOLE` through for it
    // (`compose_final_ids`), and handing a dead record a *live* id would resurrect whatever
    // stale geometry the hole still carries — with a plausible score and no error. Reject that
    // composition bug here, at the boundary, rather than emit a silently-wrong index.
    for i in 0..base_count {
        if base_pq.is_hole(i) {
            ensure!(
                inputs.base_final_ids[i] == HOLE,
                "base layout ordinal {i} is a pre-existing hole, but base_final_ids assigns it a \
                 live id ({}) — a hole cannot be relabelled live; its geometry is stale",
                inputs.base_final_ids[i]
            );
        }
    }

    let dead: Vec<bool> = inputs.base_final_ids.iter().map(|&id| id == HOLE).collect();
    // A *new* delete is a base_final_id that went `HOLE` on a record the base still held live.
    // The base's *pre-existing* holes already have cleaned adjacency (S5 is their only producer,
    // and it — like this pass — leaves no reachable node naming a hole), so they are not graph
    // work: only new deletes are.
    let any_new_dead = (0..base_count).any(|i| dead[i] && !base_pq.is_hole(i));
    let base_live = dead.iter().filter(|&&d| !d).count() as u64;
    let mut stats = MergeStats {
        base_records: base_count as u64,
        dead: (base_count as u64) - base_live,
        inserted: inputs.inserts.len() as u64,
        out_records: (base_count + inputs.inserts.len()) as u64,
        live: base_live + inputs.inserts.len() as u64,
        vamana_carried: false,
    };

    // ── Fast path: a pure permutation. No *new* hole to splice out, no vector to weave in, so
    // the graph geometry is byte-identical — carry the `.vamana` by reference and only relabel
    // the `.pq` (pre-existing holes stay `HOLE`, with their codes carried). This is where the
    // BLAKE3-unchanged thesis is delivered, and it must keep firing after the first
    // consolidation — a base accumulates holes, and every later pure-permute consolidation must
    // still carry, or the payoff evaporates.
    if !any_new_dead && inputs.inserts.is_empty() {
        carry_vamana_file(vamana_in, vamana_out)?;
        write_pq(&base_pq, inputs.base_final_ids, &[], &[], params, pq_out)?;
        stats.vamana_carried = true;
        return Ok(stats);
    }

    // Past the fast path there is real graph work, which needs a real entry point. An empty
    // base is not a *carry* case — it should be built fresh — and the insert greedy search
    // enters at `medoid`, so an out-of-range one would read a record that does not exist.
    ensure!(
        base_count > 0,
        "streaming merge needs a non-empty carried base (got 0 records) once there are deletes \
         or inserts; an empty index must be built fresh, not carried"
    );
    ensure!(
        (params.medoid as usize) < base_count,
        "medoid layout ordinal {} is out of range — the base holds {base_count} records",
        params.medoid
    );

    let space_dim = base_pq.codebook.params.dim as usize;

    // ── Stage 1: deletes. Reuse S5 verbatim, into a scratch `.vamana` beside the output. The
    // pass preserves every ordinal (holes, not compaction), so `dead`, `base_final_ids` and
    // `base_pq` stay in lockstep with the patched file. Skipped when there is no *new* delete —
    // pre-existing holes already have cleaned adjacency, so re-running the pass over them is a
    // no-op that would only cost the O(N) rewrite it exists to avoid.
    let scratch = vamana_out.with_extension("streaming-merge-scratch");
    let _ = fs::remove_file(&scratch);
    let deleted_reader;
    let cur_reader: &VamanaReader = if any_new_dead {
        let opts = ConsolidateOpts {
            medoid: params.medoid,
            r: params.r,
            alpha: params.alpha,
            metric: params.metric,
            max_norm: params.max_norm,
            space_dim,
            cache_records: recommended_cache_records(params.r),
            cache_blocks: RECOMMENDED_CACHE_BLOCKS,
        };
        let mut vw = VamanaWriter::create_with_cipher(
            &scratch,
            params.vamana_block_bytes,
            params.zstd_level,
            params.cipher.clone(),
        )
        .with_context(|| format!("create scratch {}", scratch.display()))?;
        consolidate_deletes(&reader, &dead, &opts, &mut vw)
            .context("delete half of the streaming merge")?;
        vw.finish()?;
        deleted_reader = VamanaReader::open_with_cipher(&scratch, params.cipher.clone())
            .with_context(|| format!("re-open scratch {}", scratch.display()))?;
        &deleted_reader
    } else {
        &reader
    };
    debug_assert_eq!(cur_reader.len() as usize, base_count);

    // ── Stage 2: inserts. Weave each Δ point into the carried graph.
    let delta_ann: Vec<Vec<f32>> = inputs
        .inserts
        .iter()
        .map(|(_, v)| ann_point(params.metric, v, params.max_norm, space_dim))
        .collect::<Result<_>>()?;
    let points = MergePoints {
        reader: cur_reader,
        base_count,
        metric: params.metric,
        max_norm: params.max_norm,
        space_dim,
        delta_ann: &delta_ann,
        cache: RefCell::new(HashMap::new()),
    };
    let mut adj = MergeAdj {
        reader: cur_reader,
        base_count,
        dirty: HashMap::new(),
        delta_adj: vec![Vec::new(); inputs.inserts.len()],
        base_cache: RefCell::new(HashMap::new()),
    };
    let mut expanded = Expanded::Set(Default::default());
    for k in 0..inputs.inserts.len() {
        let p = (base_count + k) as VamanaIndex;
        merge_insert(p, &mut adj, &points, &dead, params, &mut expanded)
            .with_context(|| format!("insert Δ vector {k} (dense id {})", inputs.inserts[k].0))?;
    }

    // ── Emit: one sequential pass. Base records keep their raw vector and take their patched
    // adjacency (or the original, unchanged); Δ records follow. Holes are emitted like any
    // other base record — they stay navigational waypoints with their (now hole-free-toward)
    // adjacency, and their `.pq` id is HOLE, so they are never returned.
    let mut vw = VamanaWriter::create_with_cipher(
        vamana_out,
        params.vamana_block_bytes,
        params.zstd_level,
        params.cipher.clone(),
    )
    .with_context(|| format!("create {}", vamana_out.display()))?;
    emit_merged(cur_reader, base_count, inputs.inserts, &adj, &mut vw)?;
    vw.finish()?;

    let delta_ids: Vec<u64> = inputs.inserts.iter().map(|(id, _)| *id).collect();
    write_pq(
        &base_pq,
        inputs.base_final_ids,
        &delta_ids,
        &delta_ann,
        params,
        pq_out,
    )?;

    let _ = fs::remove_file(&scratch);
    Ok(stats)
}

/// Emit the merged `.vamana`: one sequential pass over the carried base, then the Δ records.
///
/// The base sweep decompresses each block **once** via [`BlockFileReader::for_each_record`]
/// (with bounded read-ahead) and takes BOTH the raw vector and the base-adjacency fallback from
/// that single [`decode_node`]. This is HIK-119's fix: the old loop read each base record with
/// `cur_reader.node(i)` — a full-block zstd inflate per record — *and* `neighbours_into` re-read
/// the same record's block a second time, so scanning `0..N` re-inflated each block
/// `O(records/block)` times (measured ~20–24× too slow at the builder's real params).
///
/// The output is content-identical to that old per-record emit: `for_each_record` visits records
/// `0..base_count` in ascending order (so `global` is the layout ordinal), the dirty overlay wins
/// for a back-linked / delete-spliced base node exactly as `neighbours_into` chose it, an
/// untouched node falls back to its own on-disk neighbours, and the forged-neighbour-ordinal
/// bounds check `base_neighbours` performed is preserved. Holes are emitted like any other base
/// record; their `.pq` id (written separately by [`write_pq`]) is `HOLE`.
fn emit_merged(
    cur_reader: &VamanaReader,
    base_count: usize,
    inserts: &[(u64, Vec<f32>)],
    adj: &MergeAdj,
    vw: &mut VamanaWriter,
) -> Result<()> {
    let total = base_count + inserts.len();
    let mut nbrs_buf: Vec<VamanaIndex> = Vec::new();
    cur_reader.inner().for_each_record(|global, rec| {
        let node = decode_node(rec)?;
        nbrs_buf.clear();
        if let Some(v) = adj.dirty_neighbours(global as u32) {
            nbrs_buf.extend_from_slice(v);
        } else {
            // A neighbour ordinal is untrusted on-disk data — keep the bounds check
            // `base_neighbours` did (a forged one must not index out of bounds downstream).
            for &nb in &node.neighbours {
                ensure!(
                    (nb as usize) < total,
                    ".vamana record {global} names neighbour ordinal {nb}, but the merged graph \
                     holds only {total} records"
                );
            }
            nbrs_buf.extend_from_slice(&node.neighbours);
        }
        vw.append(&node.vector, &nbrs_buf)?;
        Ok(())
    })?;
    // The Δ-insert emit is O(Δ): each reads its own edges from the in-memory `delta_adj` slab
    // (`neighbours_into` for an index ≥ base_count touches no disk), so it has no amplification.
    for (k, (_, raw)) in inserts.iter().enumerate() {
        adj.neighbours_into((base_count + k) as VamanaIndex, &mut nbrs_buf)?;
        vw.append(raw, &nbrs_buf)?;
    }
    Ok(())
}

/// Carry a `.vamana` by reference: hard-link the base file to `out`, byte-identically. Falls
/// back to a byte copy if the link fails (e.g. a cross-device output dir) — the result is the
/// same bytes and the same BLAKE3 either way; the link merely avoids the copy.
fn carry_vamana_file(vamana_in: &Path, vamana_out: &Path) -> Result<()> {
    let _ = fs::remove_file(vamana_out);
    match fs::hard_link(vamana_in, vamana_out) {
        Ok(()) => Ok(()),
        Err(_) => fs::copy(vamana_in, vamana_out)
            .with_context(|| {
                format!(
                    "carry {} → {} (hard-link failed, copy fallback)",
                    vamana_in.display(),
                    vamana_out.display()
                )
            })
            .map(|_| ()),
    }
}

/// Write the output `.pq`: the base id column relabelled to `base_final_ids` (HOLE for a
/// tombstoned ordinal, codes carried untouched), then one record per Δ vector — its new id and
/// codes freshly encoded with the **base codebook** over the ANN-space point (`delta_ann[k]`).
///
/// `delta_ids[k]` and `delta_ann[k]` are the same Δ vector's id and ANN point; they index each
/// other by position, so the loop reads both from the same `k` — misaligning them is not
/// expressible.
fn write_pq(
    base_pq: &ResidentPq,
    base_final_ids: &[u64],
    delta_ids: &[u64],
    delta_ann: &[Vec<f32>],
    params: &MergeParams,
    pq_out: &Path,
) -> Result<()> {
    debug_assert_eq!(delta_ids.len(), delta_ann.len());
    let mut pw = PqWriter::create_with_cipher(
        pq_out,
        &base_pq.codebook,
        params.pq_block_bytes,
        params.zstd_level,
        params.cipher.clone(),
    )
    .with_context(|| format!("create {}", pq_out.display()))?;
    for (i, &id) in base_final_ids.iter().enumerate() {
        pw.append_codes(id, base_pq.codes_of(i))?;
    }
    for (id, ann) in delta_ids.iter().zip(delta_ann) {
        let codes = base_pq.codebook.encode(ann)?;
        pw.append_codes(*id, &codes)?;
    }
    pw.finish()?;
    Ok(())
}

/// Compose a carried index's `layout_ordinal → new_dense_id` column ([`MergeInputs::base_final_ids`])
/// from the base's `layout_to_dump_id` map and this build's id `remap` (`perm.final_of`).
///
/// This is the **single riskiest composition in the slice**. The `.vamana` graph is carried by
/// reference and only this id column is rewritten, so a wrong direction, a double application,
/// or a tombstone not carried through resurfaces as KNN pointing at the wrong node with a
/// plausible score and **no error**. It is a free function, taking `remap` as a closure so it
/// carries no dependency on the builder's `Permutation`, and is unit-tested exhaustively.
///
/// A [`HOLE`] entry — a tombstoned base ordinal, or one re-embedded and superseded by a Δ
/// insert — is **carried through untouched**: it is the sentinel, not a dense id, and must not
/// be fed to `remap` (`u64::MAX` is not a valid provisional id). Everything else is the base's
/// dump id pushed through `remap`, which the caller supplies as `perm.final_of` — old → new.
pub fn compose_final_ids(layout_to_dump_id: &[u64], remap: impl Fn(u64) -> u64) -> Vec<u64> {
    layout_to_dump_id
        .iter()
        .map(|&dump_id| {
            if dump_id == HOLE {
                HOLE
            } else {
                remap(dump_id)
            }
        })
        .collect()
}

// ── The disk-backed graph the inserts mutate ────────────────────────────────────

/// The vectors, as a [`PointSet`] over the carried base plus the Δ slab. Every distance is
/// measured in the ANN space, exactly as the base graph was built. Base ANN vectors are cached
/// as they are touched — the working set of a batch of inserts is `O(Δ·L)`, not `O(N)`, so
/// this stays bounded by Δ.
struct MergePoints<'a> {
    reader: &'a VamanaReader,
    base_count: usize,
    metric: Metric,
    max_norm: f64,
    space_dim: usize,
    delta_ann: &'a [Vec<f32>],
    cache: RefCell<HashMap<VamanaIndex, Vec<f32>>>,
}

impl MergePoints<'_> {
    /// The ANN-space vector for index `i` (a base ordinal or a Δ index `≥ base_count`).
    fn ann(&self, i: VamanaIndex) -> Result<Vec<f32>> {
        let iu = i as usize;
        if iu >= self.base_count {
            return Ok(self.delta_ann[iu - self.base_count].clone());
        }
        if let Some(v) = self.cache.borrow().get(&i) {
            return Ok(v.clone());
        }
        let raw = self.reader.node(i)?.vector;
        let ann = ann_point(self.metric, &raw, self.max_norm, self.space_dim)?;
        self.cache.borrow_mut().insert(i, ann.clone());
        Ok(ann)
    }
}

impl PointSet for MergePoints<'_> {
    fn len(&self) -> usize {
        self.base_count + self.delta_ann.len()
    }
    fn dist(&self, a: VamanaIndex, b: VamanaIndex) -> Result<f64> {
        Ok(sq_l2(&self.ann(a)?, &self.ann(b)?))
    }
}

/// The adjacency the inserts mutate: base out-edges read through the carried reader, with a
/// **dirty overlay** for the `O(Δ·R)` base records the back-links touch and a slab for the Δ
/// records' own edges. Nothing base-wide is materialised.
struct MergeAdj<'a> {
    reader: &'a VamanaReader,
    base_count: usize,
    dirty: HashMap<VamanaIndex, Vec<VamanaIndex>>,
    delta_adj: Vec<Vec<VamanaIndex>>,
    base_cache: RefCell<HashMap<VamanaIndex, Vec<VamanaIndex>>>,
}

impl MergeAdj<'_> {
    /// The dirty-overlay adjacency for base ordinal `i` — the back-linked / delete-spliced
    /// edges an insert pass patched in — or `None` if the record is untouched. Lets the emit
    /// consult the overlay without going through the disk-reading `neighbours_into`, so the emit
    /// can pair it with a record it already decoded once in a block sweep. Only meaningful for a
    /// base ordinal (`i < base_count`); a Δ index's edges live in `delta_adj`, not here.
    fn dirty_neighbours(&self, i: VamanaIndex) -> Option<&[VamanaIndex]> {
        self.dirty.get(&i).map(|v| v.as_slice())
    }

    fn base_neighbours(&self, i: VamanaIndex) -> Result<Vec<VamanaIndex>> {
        if let Some(v) = self.base_cache.borrow().get(&i) {
            return Ok(v.clone());
        }
        let nbrs = self.reader.node(i)?.neighbours;
        // A neighbour ordinal is untrusted on-disk data. Reject a forged one here rather than
        // let it index out of bounds deep inside a prune.
        let total = self.base_count + self.delta_adj.len();
        for &nb in &nbrs {
            ensure!(
                (nb as usize) < total,
                ".vamana record {i} names neighbour ordinal {nb}, but the merged graph holds \
                 only {total} records"
            );
        }
        self.base_cache.borrow_mut().insert(i, nbrs.clone());
        Ok(nbrs)
    }
}

impl AdjRead for MergeAdj<'_> {
    fn neighbours_into(&self, i: VamanaIndex, out: &mut Vec<VamanaIndex>) -> Result<()> {
        out.clear();
        let iu = i as usize;
        if iu >= self.base_count {
            out.extend_from_slice(&self.delta_adj[iu - self.base_count]);
        } else if let Some(v) = self.dirty.get(&i) {
            out.extend_from_slice(v);
        } else {
            out.extend_from_slice(&self.base_neighbours(i)?);
        }
        Ok(())
    }
}

impl AdjWrite for MergeAdj<'_> {
    fn set_neighbours(&mut self, i: VamanaIndex, nbrs: Vec<VamanaIndex>) -> Result<()> {
        let iu = i as usize;
        if iu >= self.base_count {
            self.delta_adj[iu - self.base_count] = nbrs;
        } else {
            self.dirty.insert(i, nbrs);
        }
        Ok(())
    }
}

/// One insert into the carried graph: greedy-search from the medoid, **drop every hole from
/// the candidate pool**, robust-prune the rest into `p`'s out-edges, then back-link — re-pruning
/// any neighbour that overflows `R`.
///
/// The hole-filter is not an optimisation. Greedy search returns dead records as navigational
/// waypoints in its visited set, and robust-pruning an unfiltered pool would let a live Δ node
/// choose a hole as an out-neighbour — resurrecting exactly the `no_live_node_references_a_hole`
/// violation S5 exists to prevent, with no error and a wasted block read on every query that
/// reaches `p`. The back-link re-prune is safe without a second filter: `p` is live and every
/// neighbour it back-links into is a live node whose adjacency is already hole-free.
fn merge_insert(
    p: VamanaIndex,
    adj: &mut MergeAdj,
    points: &MergePoints,
    dead: &[bool],
    params: &MergeParams,
    expanded: &mut Expanded,
) -> Result<()> {
    let base_count = adj.base_count;
    let is_dead = |c: VamanaIndex| (c as usize) < base_count && dead[c as usize];

    let visited = greedy_search_over(params.medoid, p, adj, points, params.l_build, expanded)?;
    let mut cands: Vec<VamanaIndex> = visited;
    cands.retain(|&c| c != p && !is_dead(c));
    cands.sort_unstable();
    cands.dedup();

    let pruned = robust_prune_over(p, &cands, params.alpha, params.r, points)?;
    adj.set_neighbours(p, pruned.clone())?;

    let mut nbrs_j: Vec<VamanaIndex> = Vec::new();
    for &j in &pruned {
        adj.neighbours_into(j, &mut nbrs_j)?;
        let mut changed = false;
        if !nbrs_j.contains(&p) {
            nbrs_j.push(p);
            changed = true;
        }
        if nbrs_j.len() > params.r {
            nbrs_j = robust_prune_over(j, &nbrs_j, params.alpha, params.r, points)?;
            changed = true;
        }
        if changed {
            adj.set_neighbours(j, std::mem::take(&mut nbrs_j))?;
        }
    }
    Ok(())
}

// ── Drift metric ─────────────────────────────────────────────────────────────────

/// An online accumulator of PQ **estimate drift** — the mean `|estimate − exact|` over the beam
/// searches a query path runs.
///
/// The merge does **not** retrain PQ (the codebook is carried), so as the vector distribution
/// drifts away from what the codebook was trained on, the PQ estimate the beam navigates by
/// grows less faithful and recall decays — silently. This turns that decay into an observable:
/// a query path records each expanded node's estimate against its exact re-rank, and when the
/// running mean crosses a configured threshold the index is marked due for a full rebuild
/// (which retrains). Cheap — one abs and two adds per expansion — and honest.
#[derive(Debug, Default, Clone, Copy)]
pub struct DriftMeter {
    sum_abs: f64,
    n: u64,
}

impl DriftMeter {
    /// Fold one `(estimate, exact)` observation from a beam expansion into the running mean.
    #[inline]
    pub fn record(&mut self, estimate: f32, exact: f32) {
        self.sum_abs += (estimate as f64 - exact as f64).abs();
        self.n += 1;
    }

    /// The mean absolute drift so far (`0.0` before any observation).
    pub fn mean(&self) -> f64 {
        if self.n == 0 {
            0.0
        } else {
            self.sum_abs / self.n as f64
        }
    }

    /// How many observations have been folded in.
    pub fn count(&self) -> u64 {
        self.n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pq::{ann_pq_params, ann_query, l2_norm, normalise, train_codebooks, AdcTable, Lcg};
    use crate::vamana::{beam_search, bfs_order, build_vamana, BeamParams};
    use std::path::PathBuf;

    const BLOCK: usize = 4096;
    const LEVEL: i32 = 3;

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("slater_vammerge_{}_{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// `n` random unit vectors — cosine fixtures. Deterministic.
    fn unit_vectors(dim: usize, n: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = Lcg(seed);
        (0..n)
            .map(|_| {
                let v: Vec<f32> = (0..dim).map(|_| (rng.next_f64() as f32) - 0.5).collect();
                normalise(&v)
            })
            .collect()
    }

    /// The exact re-rank distance — a self-contained copy of `slater`'s `vector::distance`, so
    /// the beam's exact closure and the brute-force truth score with the *same* function
    /// (independently-derived truth; no dependency on the `slater` crate).
    fn exact_dist(metric: Metric, q: &[f32], v: &[f32]) -> f64 {
        match metric {
            Metric::Cosine => {
                let mut dot = 0.0f64;
                let (mut nq, mut nv) = (0.0f64, 0.0f64);
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
            Metric::L2 => q
                .iter()
                .zip(v)
                .map(|(x, y)| {
                    let d = *x as f64 - *y as f64;
                    d * d
                })
                .sum(),
            Metric::Dot => -q
                .iter()
                .zip(v)
                .map(|(x, y)| *x as f64 * *y as f64)
                .sum::<f64>(),
        }
    }

    struct Base {
        vpath: PathBuf,
        ppath: PathBuf,
        medoid: VamanaIndex,
        r: usize,
        metric: Metric,
        max_norm: f64,
        /// Layout-order raw vectors and dense node ids (ids are deliberately NOT the layout
        /// ordinal — a test that confused the two would fail).
        layout_raw: Vec<Vec<f32>>,
        layout_ids: Vec<u64>,
    }

    /// Build and lay out a real Vamana base exactly as `slater-build` does: graph over the
    /// ANN-space points, BFS-from-medoid layout, raw vectors in the `.vamana`, ids + codes in
    /// the `.pq`.
    fn build_base(
        dir: &Path,
        tag: &str,
        vectors: &[Vec<f32>],
        node_ids: &[u64],
        r: usize,
        metric: Metric,
    ) -> Base {
        let dim = vectors[0].len();
        let params = ann_pq_params(metric, dim as u32, 4, 8).unwrap();
        let space_dim = params.dim as usize;
        let max_norm = vectors.iter().map(|v| l2_norm(v)).fold(0.0f64, f64::max);
        let points: Vec<Vec<f32>> = vectors
            .iter()
            .map(|v| ann_point(metric, v, max_norm, space_dim).unwrap())
            .collect();
        let g = build_vamana(&points, r, 1.2).unwrap();
        let order = bfs_order(&g);
        let mut new_of = vec![0u32; order.len()];
        for (ni, &old) in order.iter().enumerate() {
            new_of[old as usize] = ni as u32;
        }
        let medoid = new_of[g.medoid as usize];

        let vpath = dir.join(format!("{tag}.vamana"));
        let ppath = dir.join(format!("{tag}.pq"));
        let mut vw = VamanaWriter::create_with_cipher(&vpath, BLOCK, LEVEL, None).unwrap();
        let mut layout_raw = Vec::with_capacity(order.len());
        let mut layout_ids = Vec::with_capacity(order.len());
        for &old in &order {
            let nbrs: Vec<u32> = g.adjacency[old as usize]
                .iter()
                .map(|&j| new_of[j as usize])
                .collect();
            vw.append(&vectors[old as usize], &nbrs).unwrap();
            layout_raw.push(vectors[old as usize].clone());
            layout_ids.push(node_ids[old as usize]);
        }
        vw.finish().unwrap();

        let cb = train_codebooks(&points, params, 15).unwrap();
        let mut pw = PqWriter::create_with_cipher(&ppath, &cb, BLOCK, LEVEL, None).unwrap();
        for &old in &order {
            pw.append_codes(
                node_ids[old as usize],
                &cb.encode(&points[old as usize]).unwrap(),
            )
            .unwrap();
        }
        pw.finish().unwrap();

        Base {
            vpath,
            ppath,
            medoid,
            r,
            metric,
            max_norm,
            layout_raw,
            layout_ids,
        }
    }

    fn params_for(base: &Base) -> MergeParams {
        MergeParams {
            medoid: base.medoid,
            r: base.r,
            alpha: 1.2,
            l_build: (base.r * 2).max(64),
            metric: base.metric,
            max_norm: base.max_norm,
            vamana_block_bytes: BLOCK,
            pq_block_bytes: BLOCK,
            zstd_level: LEVEL,
            cipher: None,
        }
    }

    /// KNN over an on-disk `.vamana` + `.pq`, mirroring `exec::vamana_knn`: PQ estimate for
    /// navigation, the raw metric for the exact re-rank, a `HOLE` id suppressed from the
    /// results. Returns `(node_id, exact_score)` in the beam's order.
    fn knn(
        vamana: &Path,
        pq: &Path,
        query: &[f32],
        metric: Metric,
        medoid: VamanaIndex,
        k: usize,
        beam: usize,
    ) -> Vec<(u64, f32)> {
        let reader = VamanaReader::open_with_cipher(vamana, None).unwrap();
        let resident = PqReader::open_with_cipher(pq, None)
            .unwrap()
            .load_resident()
            .unwrap();
        let n = resident.len();
        let space_dim = resident.codebook.params.dim as usize;
        let qn = ann_query(metric, query, space_dim).unwrap();
        let adc = AdcTable::new(&resident.codebook, &qn).unwrap();
        let hits = beam_search(
            BeamParams {
                medoid,
                beam_width: beam,
                k,
                num_nodes: n,
            },
            |i| adc.estimate(resident.codes_of(i as usize)),
            |i| {
                let node = reader.node(i)?;
                Ok((node.vector, node.neighbours))
            },
            |v| exact_dist(metric, query, v) as f32,
            |i| {
                let id = resident.node_ids[i as usize];
                Ok(if id == HOLE { None } else { Some(id) })
            },
        )
        .unwrap();
        hits.iter().map(|h| (h.node_id, h.exact)).collect()
    }

    /// Brute-force top-`k` node ids over an explicit live set — the independently-derived truth
    /// for a recall check. Scored with the same `exact_dist`, tie-broken on node id (D26).
    fn brute_force(live: &[(u64, Vec<f32>)], query: &[f32], metric: Metric, k: usize) -> Vec<u64> {
        let mut scored: Vec<(f64, u64)> = live
            .iter()
            .map(|(id, v)| (exact_dist(metric, query, v), *id))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(_, id)| id).collect()
    }

    fn blake3_of(path: &Path) -> String {
        blake3::hash(&std::fs::read(path).unwrap())
            .to_hex()
            .to_string()
    }

    /// **The killer test.** A consolidation permutes every dense id but must not touch the
    /// graph: after a pure-permutation merge the KNN must return the *same nodes* (by their new
    /// business ids) with the *same scores*, and the `.vamana` file must be **BLAKE3-unchanged**
    /// — the proof the id-free format actually bought carry-by-reference rather than a rebuild.
    ///
    /// The relabel is a **monotone** shift (`+SHIFT`): a real LDG permutation is not monotone,
    /// but monotonicity is what keeps the D26 node-id tie-break in the same order so the test's
    /// *ordered* equality is exact rather than only set-equal at the tie boundary. (Carry-by-
    /// reference is orthogonal to the permutation's shape; a non-monotone perm reorders ties in
    /// the *answer*, which is the query contract's business, not this slice's.)
    #[test]
    fn consolidation_carrying_the_vamana_preserves_node_identity() {
        let dir = scratch("carry_identity");
        let vectors = unit_vectors(24, 200, 0x51a7_0000_0000_0001);
        let ids: Vec<u64> = (0..vectors.len() as u64).map(|i| 1000 + i * 7).collect();
        let base = build_base(&dir, "base", &vectors, &ids, 16, Metric::Cosine);

        // Record the top-10 (by node id) on the base.
        let query = {
            let mut q = vectors[3].clone();
            q[0] += 0.05;
            normalise(&q)
        };
        let before = knn(
            &base.vpath,
            &base.ppath,
            &query,
            base.metric,
            base.medoid,
            10,
            64,
        );
        assert_eq!(before.len(), 10);

        const SHIFT: u64 = 500_000;
        let base_final_ids: Vec<u64> = base.layout_ids.iter().map(|&id| id + SHIFT).collect();

        let vout = dir.join("out.vamana");
        let pout = dir.join("out.pq");
        let stats = streaming_merge(
            &base.vpath,
            &base.ppath,
            &MergeInputs {
                base_final_ids: &base_final_ids,
                inserts: &[],
            },
            &params_for(&base),
            &vout,
            &pout,
        )
        .unwrap();

        assert!(
            stats.vamana_carried,
            "a pure permutation must carry the .vamana by reference, not rebuild it"
        );
        assert_eq!(
            blake3_of(&vout),
            blake3_of(&base.vpath),
            "the carried .vamana must be BLAKE3-identical to the base — this hash is the whole \
             thesis of the slice"
        );

        let after = knn(&vout, &pout, &query, base.metric, base.medoid, 10, 64);
        let expected: Vec<(u64, f32)> = before.iter().map(|(id, s)| (id + SHIFT, *s)).collect();
        assert_eq!(
            after, expected,
            "the same nodes (by their permuted ids) must come back with identical scores"
        );
        // And the relabel really did change the ids (the test would be vacuous otherwise).
        assert!(after[0].0 != before[0].0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A node deleted pre-consolidation stays a hole through the merge: its ordinal survives
    /// (holes, not compaction), its `.pq` id is `HOLE`, it is never emitted, and no reachable
    /// node names it.
    #[test]
    fn carried_vamana_holes_survive_a_consolidation() {
        let dir = scratch("holes");
        let vectors = unit_vectors(16, 120, 0x51a7_0000_0000_0002);
        let ids: Vec<u64> = (0..vectors.len() as u64).map(|i| 1000 + i).collect();
        let base = build_base(&dir, "base", &vectors, &ids, 12, Metric::Cosine);

        // Delete three ordinals (one of them adjacent to the medoid, to force a splice), relabel
        // the rest.
        let victims = [5u32, 40, 90];
        let victim_ids: Vec<u64> = victims
            .iter()
            .map(|&o| base.layout_ids[o as usize])
            .collect();
        let base_final_ids: Vec<u64> = base
            .layout_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                if victims.contains(&(i as u32)) {
                    HOLE
                } else {
                    id + 500_000
                }
            })
            .collect();

        let vout = dir.join("out.vamana");
        let pout = dir.join("out.pq");
        let stats = streaming_merge(
            &base.vpath,
            &base.ppath,
            &MergeInputs {
                base_final_ids: &base_final_ids,
                inserts: &[],
            },
            &params_for(&base),
            &vout,
            &pout,
        )
        .unwrap();
        assert!(!stats.vamana_carried, "a delete merge rewrites the .vamana");
        assert_eq!(
            stats.out_records,
            base.layout_raw.len() as u64,
            "holes, not compaction: the record count is preserved"
        );
        assert_eq!(stats.dead, 3);

        let out_pq = PqReader::open_with_cipher(&pout, None)
            .unwrap()
            .load_resident()
            .unwrap();
        let out_v = VamanaReader::open_with_cipher(&vout, None).unwrap();
        for &o in &victims {
            assert!(out_pq.is_hole(o as usize), "ordinal {o} must be a hole");
        }
        // No reachable node (live, or the medoid) names a hole after the merge — S5's invariant.
        for i in 0..out_pq.len() {
            let reachable = !out_pq.is_hole(i) || i as u32 == base.medoid;
            if !reachable {
                continue;
            }
            for &nb in &out_v.node(i as u32).unwrap().neighbours {
                assert!(
                    !out_pq.is_hole(nb as usize),
                    "reachable ordinal {i} still names hole {nb}"
                );
            }
        }
        // A deleted node's id never comes back — query with its own vector.
        for (&o, &vid) in victims.iter().zip(&victim_ids) {
            let q = &base.layout_raw[o as usize];
            let hits = knn(&vout, &pout, q, base.metric, base.medoid, 10, 64);
            assert!(
                !hits
                    .iter()
                    .any(|(id, _)| *id == vid + 500_000 || *id == vid),
                "deleted node {vid} was returned"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The payoff must survive **repeated** consolidation. A base accumulates holes, so if the
    /// fast path only fired on a hole-free base, only the *first* consolidation of a fresh index
    /// would carry by reference. Here: merge #1 deletes (rewrites), then merge #2 is a pure
    /// permutation over the *holed* output — it must still carry the `.vamana` byte-identically.
    #[test]
    fn a_pure_permutation_over_a_holed_base_still_carries_by_reference() {
        let dir = scratch("holed_carry");
        let vectors = unit_vectors(16, 150, 0x51a7_0000_0000_0006);
        let ids: Vec<u64> = (0..vectors.len() as u64).map(|i| 1000 + i).collect();
        let base = build_base(&dir, "base", &vectors, &ids, 12, Metric::Cosine);
        let params = params_for(&base);

        // Merge #1: delete a handful (slow path → holed output).
        let victims = [7u32, 33, 88, 120];
        let ids1: Vec<u64> = base
            .layout_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                if victims.contains(&(i as u32)) {
                    HOLE
                } else {
                    id
                }
            })
            .collect();
        let v1 = dir.join("g1.vamana");
        let p1 = dir.join("g1.pq");
        let s1 = streaming_merge(
            &base.vpath,
            &base.ppath,
            &MergeInputs {
                base_final_ids: &ids1,
                inserts: &[],
            },
            &params,
            &v1,
            &p1,
        )
        .unwrap();
        assert!(!s1.vamana_carried, "merge #1 has new deletes → rewrites");

        // Record top-10 on the holed generation.
        let query = {
            let mut q = base.layout_raw[2].clone();
            q[0] += 0.05;
            normalise(&q)
        };
        let before = knn(&v1, &p1, &query, base.metric, base.medoid, 10, 64);

        // Merge #2: a pure permutation over the holed base — holes stay HOLE, everything else
        // relabels. No new delete, no insert.
        let pq1 = PqReader::open_with_cipher(&p1, None)
            .unwrap()
            .load_resident()
            .unwrap();
        let ids2: Vec<u64> = (0..pq1.len())
            .map(|i| {
                if pq1.is_hole(i) {
                    HOLE
                } else {
                    pq1.node_ids[i] + 500_000
                }
            })
            .collect();
        let v2 = dir.join("g2.vamana");
        let p2 = dir.join("g2.pq");
        let s2 = streaming_merge(
            &v1,
            &p1,
            &MergeInputs {
                base_final_ids: &ids2,
                inserts: &[],
            },
            &params,
            &v2,
            &p2,
        )
        .unwrap();
        assert!(
            s2.vamana_carried,
            "a pure permutation over a holed base must STILL carry by reference — the payoff \
             cannot be one-shot"
        );
        assert_eq!(
            blake3_of(&v2),
            blake3_of(&v1),
            "the carried .vamana over a holed base must be BLAKE3-identical"
        );
        let after = knn(&v2, &p2, &query, base.metric, base.medoid, 10, 64);
        let expected: Vec<(u64, f32)> = before.iter().map(|(id, s)| (id + 500_000, *s)).collect();
        assert_eq!(
            after, expected,
            "node identity preserved through the second consolidation"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A composition bug that hands a pre-existing hole a *live* id would resurrect stale
    /// geometry with no error. It must be refused at the boundary.
    #[test]
    fn assigning_a_live_id_to_a_pre_existing_hole_is_refused() {
        let dir = scratch("hole_resurrect");
        let vectors = unit_vectors(16, 80, 0x51a7_0000_0000_0007);
        let ids: Vec<u64> = (0..vectors.len() as u64).map(|i| 1000 + i).collect();
        let base = build_base(&dir, "base", &vectors, &ids, 12, Metric::Cosine);
        let params = params_for(&base);

        // Make a holed generation.
        let ids1: Vec<u64> = base
            .layout_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| if i == 10 { HOLE } else { id })
            .collect();
        let v1 = dir.join("g1.vamana");
        let p1 = dir.join("g1.pq");
        streaming_merge(
            &base.vpath,
            &base.ppath,
            &MergeInputs {
                base_final_ids: &ids1,
                inserts: &[],
            },
            &params,
            &v1,
            &p1,
        )
        .unwrap();

        // Now try to relabel that hole (ordinal 10) live — must be refused.
        let pq1 = PqReader::open_with_cipher(&p1, None)
            .unwrap()
            .load_resident()
            .unwrap();
        let hole_ord = (0..pq1.len()).find(|&i| pq1.is_hole(i)).unwrap();
        let mut bad: Vec<u64> = (0..pq1.len())
            .map(|i| {
                if pq1.is_hole(i) {
                    HOLE
                } else {
                    pq1.node_ids[i]
                }
            })
            .collect();
        bad[hole_ord] = 777_777; // resurrect the hole — the bug
        let err = streaming_merge(
            &v1,
            &p1,
            &MergeInputs {
                base_final_ids: &bad,
                inserts: &[],
            },
            &params,
            &dir.join("g2.vamana"),
            &dir.join("g2.pq"),
        )
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("pre-existing hole"),
            "expected a hole-resurrection refusal, got: {err:#}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Recall over the **live set** after a batch of inserts + deletes, and the Δ nearest-to-self
    /// property. Truth is brute force over the live set (independently derived), not another
    /// index. The nearest-to-self assertion is what catches a broken back-link: a Δ node that no
    /// live node points at is unreachable and would not come back even for its own vector.
    #[test]
    fn streaming_merge_recall_over_the_live_set() {
        let dir = scratch("recall");
        let base_n = 300usize;
        let dim = 32usize;
        let all = unit_vectors(dim, base_n + 40, 0x51a7_0000_0000_0003);
        let base_vecs = &all[..base_n];
        let delta_vecs = &all[base_n..]; // 40 fresh vectors
        let ids: Vec<u64> = (0..base_n as u64).map(|i| 1000 + i).collect();
        let base = build_base(&dir, "base", base_vecs, &ids, 24, Metric::Cosine);

        // Delete 30 base ordinals; relabel the survivors (identity relabel here for simplicity).
        let mut rng = Lcg(0xd00d_0000_0000_0001);
        let mut dead_ord = std::collections::HashSet::new();
        while dead_ord.len() < 30 {
            dead_ord.insert((rng.next_f64() * base_n as f64) as u32 % base_n as u32);
        }
        let base_final_ids: Vec<u64> = base
            .layout_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                if dead_ord.contains(&(i as u32)) {
                    HOLE
                } else {
                    id
                }
            })
            .collect();

        // Δ: 40 fresh vectors with brand-new ids well above the base range.
        let inserts: Vec<(u64, Vec<f32>)> = delta_vecs
            .iter()
            .enumerate()
            .map(|(k, v)| (900_000 + k as u64, v.clone()))
            .collect();

        // The live set = surviving base + Δ.
        let mut live: Vec<(u64, Vec<f32>)> = Vec::new();
        for (i, &id) in base.layout_ids.iter().enumerate() {
            if !dead_ord.contains(&(i as u32)) {
                live.push((id, base.layout_raw[i].clone()));
            }
        }
        for (id, v) in &inserts {
            live.push((*id, v.clone()));
        }

        let vout = dir.join("out.vamana");
        let pout = dir.join("out.pq");
        streaming_merge(
            &base.vpath,
            &base.ppath,
            &MergeInputs {
                base_final_ids: &base_final_ids,
                inserts: &inserts,
            },
            &params_for(&base),
            &vout,
            &pout,
        )
        .unwrap();

        // Recall@10 over the live set.
        let k = 10;
        let queries = 20;
        let mut total = 0.0f64;
        for q in 0..queries {
            let mut query = live[(q * 13) % live.len()].1.clone();
            query[0] += 0.03;
            let query = normalise(&query);
            let got: std::collections::HashSet<u64> =
                knn(&vout, &pout, &query, base.metric, base.medoid, k, 96)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect();
            let truth = brute_force(&live, &query, base.metric, k);
            let found = truth.iter().filter(|id| got.contains(id)).count();
            total += found as f64 / k as f64;
        }
        let recall = total / queries as f64;
        assert!(
            recall >= 0.8,
            "recall@{k} over the live set was {recall:.3}, want ≥ 0.8"
        );

        // S5's invariant must survive the *insert* path too: after a delete+insert merge, no
        // reachable node — live base, medoid, or a Δ node — may name a hole. This is the
        // assertion that catches a missing hole-filter in `merge_insert` (greedy search returns
        // dead waypoints, and an unfiltered prune would link a live Δ node straight at a hole).
        {
            let out_pq = PqReader::open_with_cipher(&pout, None)
                .unwrap()
                .load_resident()
                .unwrap();
            let out_v = VamanaReader::open_with_cipher(&vout, None).unwrap();
            for i in 0..out_pq.len() {
                let reachable = !out_pq.is_hole(i) || i as u32 == base.medoid;
                if !reachable {
                    continue;
                }
                for &nb in &out_v.node(i as u32).unwrap().neighbours {
                    assert!(
                        !out_pq.is_hole(nb as usize),
                        "reachable ordinal {i} names hole {nb} after a delete+insert merge — the \
                         insert hole-filter regressed S5's no_live_node_references_a_hole"
                    );
                }
            }
        }

        // Δ nearest-to-self: each inserted node is top-1 for its own vector (catches a broken
        // back-link — an unreachable Δ node would not come back at all).
        for (id, v) in &inserts {
            let hits = knn(&vout, &pout, v, base.metric, base.medoid, 1, 96);
            assert_eq!(
                hits.first().map(|(i, _)| *i),
                Some(*id),
                "inserted node {id} is not its own nearest neighbour — its back-links are broken"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Byte-determinism: the same inputs produce the same output files (the generation content
    /// hash is computed over them).
    #[test]
    fn streaming_merge_is_byte_deterministic() {
        let dir = scratch("determinism");
        let vectors = unit_vectors(24, 200, 0x51a7_0000_0000_0004);
        let ids: Vec<u64> = (0..vectors.len() as u64).map(|i| 1000 + i).collect();
        let inserts: Vec<(u64, Vec<f32>)> = unit_vectors(24, 12, 0x51a7_0000_0000_0044)
            .into_iter()
            .enumerate()
            .map(|(k, v)| (900_000 + k as u64, v))
            .collect();

        let run = |tag: &str| -> (String, String) {
            let base = build_base(&dir, tag, &vectors, &ids, 16, Metric::Cosine);
            let base_final_ids: Vec<u64> = base
                .layout_ids
                .iter()
                .enumerate()
                .map(|(i, &id)| if i % 17 == 0 { HOLE } else { id + 100 })
                .collect();
            let vout = dir.join(format!("{tag}.out.vamana"));
            let pout = dir.join(format!("{tag}.out.pq"));
            streaming_merge(
                &base.vpath,
                &base.ppath,
                &MergeInputs {
                    base_final_ids: &base_final_ids,
                    inserts: &inserts,
                },
                &params_for(&base),
                &vout,
                &pout,
            )
            .unwrap();
            (blake3_of(&vout), blake3_of(&pout))
        };
        let a = run("a");
        let b = run("b");
        assert_eq!(a.0, b.0, "the merged .vamana must be byte-deterministic");
        assert_eq!(a.1, b.1, "the merged .pq must be byte-deterministic");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The insert stage's working set is bounded by **Δ**, not by the base size: weaving a
    /// handful of vectors into a large base must touch only `O(Δ·L·R)` base records, not `O(N)`.
    /// (The emit pass is a separate `O(N)` *sequential* stream, not a resident cost.) Drives the
    /// insert primitives directly so the touched-set caches can be measured.
    #[test]
    fn streaming_merge_insert_working_set_is_delta_bounded() {
        let dir = scratch("bounded");
        let n = 8000usize;
        let vectors = unit_vectors(16, n, 0x51a7_0000_0000_0005);
        let ids: Vec<u64> = (0..n as u64).map(|i| 1000 + i).collect();
        let base = build_base(&dir, "base", &vectors, &ids, 16, Metric::Cosine);

        let reader = VamanaReader::open_with_cipher(&base.vpath, None).unwrap();
        let params = params_for(&base);
        let dead = vec![false; n];
        let delta = unit_vectors(16, 3, 0x51a7_0000_0000_0055);
        let delta_ann: Vec<Vec<f32>> = delta
            .iter()
            .map(|v| ann_point(base.metric, v, base.max_norm, 16).unwrap())
            .collect();
        let points = MergePoints {
            reader: &reader,
            base_count: n,
            metric: base.metric,
            max_norm: base.max_norm,
            space_dim: 16,
            delta_ann: &delta_ann,
            cache: RefCell::new(HashMap::new()),
        };
        let mut adj = MergeAdj {
            reader: &reader,
            base_count: n,
            dirty: HashMap::new(),
            delta_adj: vec![Vec::new(); delta.len()],
            base_cache: RefCell::new(HashMap::new()),
        };
        let mut expanded = Expanded::Set(Default::default());
        for k in 0..delta.len() {
            merge_insert(
                (n + k) as VamanaIndex,
                &mut adj,
                &points,
                &dead,
                &params,
                &mut expanded,
            )
            .unwrap();
        }
        let touched = points
            .cache
            .borrow()
            .len()
            .max(adj.base_cache.borrow().len());
        assert!(
            touched < n / 3,
            "3 inserts touched {touched} of {n} base records — the insert working set is not \
             Δ-bounded"
        );
        // The dirty overlay (back-linked base records) is O(Δ·R), nowhere near N.
        assert!(adj.dirty.len() <= 3 * (base.r + 4));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Reproduce `streaming_merge`'s delete stage independently: run [`consolidate_deletes`]
    /// with the *same* opts into our own scratch. For a delete-only merge there is no insert
    /// back-linking, so the emit's dirty overlay is empty and it must reproduce each post-delete
    /// record verbatim — vector and neighbours, holes included. This is the independently-derived
    /// truth for HIK-119's decompress-once emit (no cross-impl "old == new"): the carried records
    /// are produced by the shared delete primitive, and the emit's contract is to copy them
    /// through faithfully at the right ordinal.
    fn post_delete_reader(dir: &Path, base: &Base, dead: &[bool]) -> VamanaReader {
        let base_pq = PqReader::open_with_cipher(&base.ppath, None)
            .unwrap()
            .load_resident()
            .unwrap();
        let space_dim = base_pq.codebook.params.dim as usize;
        let params = params_for(base);
        let opts = ConsolidateOpts {
            medoid: params.medoid,
            r: params.r,
            alpha: params.alpha,
            metric: params.metric,
            max_norm: params.max_norm,
            space_dim,
            cache_records: recommended_cache_records(params.r),
            cache_blocks: RECOMMENDED_CACHE_BLOCKS,
        };
        let truth = dir.join("truth_cur.vamana");
        let reader = VamanaReader::open_with_cipher(&base.vpath, None).unwrap();
        let mut vw = VamanaWriter::create_with_cipher(&truth, BLOCK, LEVEL, None).unwrap();
        consolidate_deletes(&reader, dead, &opts, &mut vw).unwrap();
        vw.finish().unwrap();
        VamanaReader::open_with_cipher(&truth, None).unwrap()
    }

    /// **HIK-119 output-identity, delete-only.** A base spanning several blocks, a scatter of
    /// deletes (some adjacent, to force splices). The decompress-once emit must produce a
    /// `.vamana` whose every record — vector AND adjacency, holes included — is identical to the
    /// carried post-delete records. Exercises the base-fallback branch, the per-ordinal vector
    /// pairing (an off-by-one between `global` and its record would surface here), and holes.
    #[test]
    fn emit_delete_only_is_content_identical_to_carried_records() {
        let dir = scratch("emit_delete_only");
        let vectors = unit_vectors(24, 200, 0x51a7_0000_0000_0008);
        let ids: Vec<u64> = (0..vectors.len() as u64).map(|i| 1000 + i).collect();
        let base = build_base(&dir, "base", &vectors, &ids, 16, Metric::Cosine);
        let params = params_for(&base);

        let victims = [3u32, 4, 5, 50, 51, 120, 199];
        let base_final_ids: Vec<u64> = base
            .layout_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                if victims.contains(&(i as u32)) {
                    HOLE
                } else {
                    id + 7
                }
            })
            .collect();
        let dead: Vec<bool> = base_final_ids.iter().map(|&id| id == HOLE).collect();

        let truth = post_delete_reader(&dir, &base, &dead);

        let vout = dir.join("out.vamana");
        let pout = dir.join("out.pq");
        let stats = streaming_merge(
            &base.vpath,
            &base.ppath,
            &MergeInputs {
                base_final_ids: &base_final_ids,
                inserts: &[],
            },
            &params,
            &vout,
            &pout,
        )
        .unwrap();
        assert!(!stats.vamana_carried, "a delete merge rewrites the .vamana");

        let got = VamanaReader::open_with_cipher(&vout, None).unwrap();
        assert_eq!(got.len(), truth.len(), "record count must be preserved");
        assert!(
            got.len() >= 100,
            "fixture must span several blocks to be meaningful"
        );
        for i in 0..truth.len() as u32 {
            let e = truth.node(i).unwrap();
            let g = got.node(i).unwrap();
            assert_eq!(
                g.vector, e.vector,
                "record {i} vector diverged from the carried base"
            );
            assert_eq!(
                g.neighbours, e.neighbours,
                "record {i} adjacency diverged from the carried base"
            );
        }
        // The writer is deterministic and the record byte-stream + block params are identical, so
        // the emitted file is byte-identical to the carried post-delete records.
        assert_eq!(
            blake3_of(&vout),
            blake3_of(&dir.join("truth_cur.vamana")),
            "delete-only emit must be BLAKE3-identical to the carried post-delete records"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **HIK-119 output-identity, delete + insert (the real slow path).** Reconstruct the carried
    /// post-delete reader, then drive the *same* insert sequence (`merge_insert` in order over a
    /// fresh `MergeAdj`, mirroring `streaming_merge`) to obtain the known dirty overlay and Δ
    /// adjacency. Build the expected `(vector, neighbours)` list directly from that overlay + the
    /// carried reader + Δ, and assert the merge's output matches it record-for-record. This is the
    /// branch the fix most endangers: a dirty (back-linked) base node must take the overlay, an
    /// untouched one its own on-disk neighbours.
    #[test]
    fn emit_delete_and_insert_is_content_identical_to_overlay_truth() {
        let dir = scratch("emit_del_ins");
        let base_n = 220usize;
        let dim = 24usize;
        let all = unit_vectors(dim, base_n + 15, 0x51a7_0000_0000_0009);
        let base_vecs = &all[..base_n];
        let delta_vecs = &all[base_n..];
        let ids: Vec<u64> = (0..base_n as u64).map(|i| 1000 + i).collect();
        let base = build_base(&dir, "base", base_vecs, &ids, 16, Metric::Cosine);
        let params = params_for(&base);

        let victims = [10u32, 11, 60, 130, 219];
        let base_final_ids: Vec<u64> = base
            .layout_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                if victims.contains(&(i as u32)) {
                    HOLE
                } else {
                    id + 3
                }
            })
            .collect();
        let dead: Vec<bool> = base_final_ids.iter().map(|&id| id == HOLE).collect();
        let inserts: Vec<(u64, Vec<f32>)> = delta_vecs
            .iter()
            .enumerate()
            .map(|(k, v)| (900_000 + k as u64, v.clone()))
            .collect();

        // Independently reconstruct the emit's inputs: the carried post-delete reader, then the
        // insert overlay produced by the same primitive in the same order.
        let cur = post_delete_reader(&dir, &base, &dead);
        let base_pq = PqReader::open_with_cipher(&base.ppath, None)
            .unwrap()
            .load_resident()
            .unwrap();
        let space_dim = base_pq.codebook.params.dim as usize;
        let delta_ann: Vec<Vec<f32>> = inserts
            .iter()
            .map(|(_, v)| ann_point(params.metric, v, params.max_norm, space_dim).unwrap())
            .collect();
        let points = MergePoints {
            reader: &cur,
            base_count: base_n,
            metric: params.metric,
            max_norm: params.max_norm,
            space_dim,
            delta_ann: &delta_ann,
            cache: RefCell::new(HashMap::new()),
        };
        let mut adj = MergeAdj {
            reader: &cur,
            base_count: base_n,
            dirty: HashMap::new(),
            delta_adj: vec![Vec::new(); inserts.len()],
            base_cache: RefCell::new(HashMap::new()),
        };
        let mut expanded = Expanded::Set(Default::default());
        for k in 0..inserts.len() {
            merge_insert(
                (base_n + k) as VamanaIndex,
                &mut adj,
                &points,
                &dead,
                &params,
                &mut expanded,
            )
            .unwrap();
        }
        // Expected list: base records take the overlay if dirty, else their carried neighbours;
        // Δ records take their own slab adjacency.
        let mut expected: Vec<(Vec<f32>, Vec<VamanaIndex>)> = Vec::new();
        for i in 0..base_n as u32 {
            let node = cur.node(i).unwrap();
            let nbrs = match adj.dirty.get(&i) {
                Some(v) => v.clone(),
                None => node.neighbours.clone(),
            };
            expected.push((node.vector, nbrs));
        }
        for (k, (_, raw)) in inserts.iter().enumerate() {
            expected.push((raw.clone(), adj.delta_adj[k].clone()));
        }
        // Prove the overlay is non-trivial, or the dirty branch would go untested.
        assert!(
            !adj.dirty.is_empty(),
            "the insert back-links must dirty some base records"
        );

        // Run the merge under test and compare.
        let vout = dir.join("out.vamana");
        let pout = dir.join("out.pq");
        streaming_merge(
            &base.vpath,
            &base.ppath,
            &MergeInputs {
                base_final_ids: &base_final_ids,
                inserts: &inserts,
            },
            &params,
            &vout,
            &pout,
        )
        .unwrap();
        let got = VamanaReader::open_with_cipher(&vout, None).unwrap();
        assert_eq!(got.len() as usize, expected.len(), "output record count");
        for (i, (vec, nbrs)) in expected.iter().enumerate() {
            let g = got.node(i as u32).unwrap();
            assert_eq!(
                &g.vector, vec,
                "record {i} vector diverged from overlay truth"
            );
            assert_eq!(
                &g.neighbours, nbrs,
                "record {i} adjacency diverged from overlay truth"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **HIK-119 anti-amplification guard.** The emit must decode each block *once*, not
    /// re-inflate a whole block per record. Drives `emit_merged` directly over a `VamanaReader`
    /// backed by a read-counting source, and asserts the emit issues ~one block read per block —
    /// far fewer than the per-record `node(i)` path, which reads one block per record. A
    /// regression to per-record reads would make the two counts equal and trip this.
    #[test]
    fn emit_reads_each_block_once_not_once_per_record() {
        use crate::store::fs::FileObject;
        use crate::store::RandomReadAt;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct CountingSource {
            inner: FileObject,
            reads: AtomicU64,
        }
        impl RandomReadAt for CountingSource {
            fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
                self.reads.fetch_add(1, Ordering::Relaxed);
                self.inner.read_exact_at(buf, offset)
            }
            fn len(&self) -> u64 {
                self.inner.len()
            }
        }

        let dir = scratch("emit_guard");
        let vectors = unit_vectors(24, 240, 0x51a7_0000_0000_000a);
        let ids: Vec<u64> = (0..vectors.len() as u64).map(|i| 1000 + i).collect();
        let base = build_base(&dir, "base", &vectors, &ids, 16, Metric::Cosine);

        let src = Arc::new(CountingSource {
            inner: FileObject::open(&base.vpath).unwrap(),
            reads: AtomicU64::new(0),
        });
        let reader = VamanaReader::open_src(src.clone(), None).unwrap();
        let n = reader.len() as usize;
        assert!(n >= 200, "fixture must span several blocks");

        // The emit path: untouched base records (empty dirty overlay) — the amplification-prone
        // fallback branch — swept via `for_each_record`.
        let adj = MergeAdj {
            reader: &reader,
            base_count: n,
            dirty: HashMap::new(),
            delta_adj: Vec::new(),
            base_cache: RefCell::new(HashMap::new()),
        };
        let vout = dir.join("guard.out.vamana");
        let mut vw = VamanaWriter::create_with_cipher(&vout, BLOCK, LEVEL, None).unwrap();
        let before = src.reads.load(Ordering::Relaxed);
        emit_merged(&reader, n, &[], &adj, &mut vw).unwrap();
        vw.finish().unwrap();
        let emit_reads = src.reads.load(Ordering::Relaxed) - before;

        // The old per-record pattern, over the same source, for a mutation-check baseline: one
        // block read per record.
        let before_pr = src.reads.load(Ordering::Relaxed);
        for i in 0..n as u32 {
            let _ = reader.node(i).unwrap();
        }
        let perrec_reads = src.reads.load(Ordering::Relaxed) - before_pr;

        assert_eq!(
            perrec_reads, n as u64,
            "the per-record node() path reads exactly one block per record ({n})"
        );
        assert!(
            emit_reads * 4 <= n as u64,
            "emit read {emit_reads} blocks for {n} records — with ~27 records/block it should be \
             ~9; a value near {n} means the per-record amplification regressed"
        );
        assert!(
            emit_reads * 8 < perrec_reads,
            "emit ({emit_reads} reads) must decode each block once, far below the per-record path \
             ({perrec_reads})"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The id remap composition — the slice's single riskiest line, tested exhaustively against
    /// hand-derived truth. The `remap` is deliberately non-monotone and non-involution
    /// (`remap(remap(x)) != x`), so a "wrong direction" or "applied twice" bug cannot slip
    /// through, and the `dump_id`s are not their own indices, so a pass-through bug is visible.
    #[test]
    fn compose_final_ids_maps_through_remap_and_carries_holes() {
        // A concrete perm table `t` with `t[old] = new`: 0→3, 1→0, 2→4, 3→1, 4→2.
        let t = [3u64, 0, 4, 1, 2];
        let remap = |old: u64| t[old as usize];

        // layout ordinal → base dump id (old dense id); ordinal 2 is a tombstone.
        let layout_to_dump_id = vec![4u64, 1, HOLE, 0, 3];
        let got = compose_final_ids(&layout_to_dump_id, remap);

        // remap(4)=2, remap(1)=0, HOLE→HOLE, remap(0)=3, remap(3)=1.
        assert_eq!(got, vec![2, 0, HOLE, 3, 1]);
        // The sentinel is preserved exactly (not fed to remap as if it were u64::MAX), and the
        // ids genuinely moved (not an accidental identity).
        assert_eq!(got[2], HOLE);
        assert_ne!(got, layout_to_dump_id);
        // Identity remap passes everything through untouched, holes included.
        assert_eq!(
            compose_final_ids(&layout_to_dump_id, |x| x),
            layout_to_dump_id
        );
    }

    #[test]
    fn drift_meter_tracks_mean_absolute_error() {
        let mut m = DriftMeter::default();
        assert_eq!(m.mean(), 0.0);
        assert_eq!(m.count(), 0);
        m.record(1.0, 1.5); // 0.5
        m.record(2.0, 1.0); // 1.0
        assert_eq!(m.count(), 2);
        assert!((m.mean() - 0.75).abs() < 1e-9);
    }
}
