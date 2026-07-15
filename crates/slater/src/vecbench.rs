// SPDX-License-Identifier: Apache-2.0
//! Shared vector-benchmark plumbing for the FreshDiskANN performance suite (HIK-120).
//!
//! Three of the five benches ([`vector_recall`](../../benches/vector_recall.rs),
//! [`vector_delete_io`](../../benches/vector_delete_io.rs),
//! [`streaming_merge`](../../benches/streaming_merge.rs)) all need the same on-disk
//! Vamana + PQ lifecycle: map raw vectors into ANN space (`pq::ann_point`), train a 16×8
//! codebook, build the proximity graph (`vamana::build_vamana`), write it out in BFS-from-medoid
//! layout, and query it back with `vamana::beam_search`. Getting the ANN-space mapping wrong
//! compiles but silently builds a wrong-space graph (the D29 invariant), so it is centralised
//! here **once** rather than re-derived in three bench files.
//!
//! Ground truth for recall is [`crate::vector::distance`] — the exact metric distance over the
//! **live set**, recomputed independently, never "impl A agrees with impl B" (the house rule).
//!
//! Gated `pub` under `testkit` like [`crate::testgen`] / [`crate::benchkit`].

#![cfg(any(test, feature = "testkit"))]

use std::cell::Cell;
use std::path::{Path, PathBuf};

use anyhow::Result;

use graph_format::manifest::Metric;
use graph_format::pq::{
    ann_point, ann_pq_params, ann_query, l2_norm, train_codebooks, AdcTable, Codebook, PqParams,
    PqReader, PqWriter, ResidentPq, HOLE,
};
use graph_format::vamana::{
    beam_search, bfs_order, build_vamana, BeamParams, VamanaGraph, VamanaIndex, VamanaReader,
    VamanaWriter,
};
use graph_format::vamana_delete::{
    consolidate_deletes, recommended_cache_records, ConsolidateOpts, RECOMMENDED_CACHE_BLOCKS,
};
use graph_format::vamana_merge::{streaming_merge, MergeInputs, MergeParams, MergeStats};

use crate::vector::distance;

/// The builder shape slater-build ships (`slater-build/src/shared.rs`): R=32, α=1.2, PQ 16×8,
/// 25 Lloyd iterations, 256 KiB blocks, zstd 3.
pub const VAMANA_R: usize = 32;
pub const VAMANA_ALPHA: f32 = 1.2;
pub const PQ_SUBSPACES: u32 = 16;
pub const PQ_BITS: u32 = 8;
pub const PQ_ITERS: usize = 25;
pub const BLOCK: usize = 256 * 1024;
pub const ZSTD: i32 = 3;

/// splitmix64 — the deterministic stream the whole suite shares (mirrors `vector_knn.rs`), so
/// fixtures are stable run-to-run without an `rand` dependency.
pub struct SplitMix64(pub u64);
impl SplitMix64 {
    pub fn next_f32(&mut self) -> f32 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let unit = (z >> 40) as f32 / (1u32 << 24) as f32;
        unit * 2.0 - 1.0
    }
}

/// `n` random dim-`d` vectors with **deliberately unequal norms** — each vector is scaled by a
/// random factor in `[0.25, 4.0)`. On unit vectors cosine, L2 and dot coincide and a per-metric
/// recall comparison proves nothing; unequal norms pull the three metrics genuinely apart.
pub fn random_vectors_unequal_norms(n: usize, d: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = SplitMix64(seed);
    (0..n)
        .map(|_| {
            let scale = 0.25 + (rng.next_f32() * 0.5 + 0.5) * 3.75; // ~[0.25, 4.0)
            (0..d).map(|_| rng.next_f32() * scale).collect()
        })
        .collect()
}

/// The ANN-space bundle for one metric over a raw vector set: the mapped points, the trained
/// codebook, the per-point codes, and the in-memory proximity graph — everything a recall or IO
/// measurement needs, all in raw/dense input order.
pub struct VecFixture {
    pub metric: Metric,
    pub dim: usize,
    /// Raw vectors, dense input order (index `i` ⇒ dump id `i`).
    pub raw: Vec<Vec<f32>>,
    pub space_dim: usize,
    pub max_norm: f64,
    pub params: PqParams,
    pub codebook: Codebook,
    /// `codes[i]` = `codebook.encode(ann_point(raw[i]))`, input order.
    pub codes: Vec<Vec<u8>>,
    /// Proximity graph over the ANN points, adjacency indexed in input order.
    pub graph: VamanaGraph,
}

impl VecFixture {
    /// Build the whole ANN lifecycle for `metric` over `raw`, exactly as the builder does
    /// (`ann_pq_params` → `ann_point` → `train_codebooks` → `build_vamana`).
    pub fn build(metric: Metric, raw: Vec<Vec<f32>>) -> Result<Self> {
        assert!(!raw.is_empty());
        let dim = raw[0].len();
        let params = ann_pq_params(metric, dim as u32, PQ_SUBSPACES, PQ_BITS)?;
        let space_dim = params.dim as usize;
        let max_norm = raw.iter().map(|v| l2_norm(v)).fold(0.0_f64, f64::max);
        let points: Vec<Vec<f32>> = raw
            .iter()
            .map(|v| ann_point(metric, v, max_norm, space_dim))
            .collect::<Result<_>>()?;
        let codebook = train_codebooks(&points, params, PQ_ITERS)?;
        let codes: Vec<Vec<u8>> = points
            .iter()
            .map(|p| codebook.encode(p))
            .collect::<Result<_>>()?;
        let graph = build_vamana(&points, VAMANA_R, VAMANA_ALPHA)?;
        Ok(Self {
            metric,
            dim,
            raw,
            space_dim,
            max_norm,
            params,
            codebook,
            codes,
            graph,
        })
    }

    /// Beam-search the **in-memory** graph (the base-index recall path): navigate by the PQ
    /// estimate, re-rank by the exact metric distance. Returns the emitted dump ids (input
    /// indices), best-first.
    pub fn beam_topk_inmem(&self, q_raw: &[f32], k: usize, beam: usize) -> Result<Vec<u64>> {
        let qa = ann_query(self.metric, q_raw, self.space_dim)?;
        let adc = AdcTable::new(&self.codebook, &qa)?;
        let hits = beam_search(
            BeamParams {
                medoid: self.graph.medoid,
                beam_width: beam,
                k,
                num_nodes: self.raw.len(),
            },
            |i| adc.estimate(&self.codes[i as usize]),
            |i| {
                Ok((
                    self.raw[i as usize].clone(),
                    self.graph.adjacency[i as usize].clone(),
                ))
            },
            |v| distance(self.metric, q_raw, v) as f32,
            |i| Ok(Some(i as u64)),
        )?;
        Ok(hits.into_iter().map(|h| h.node_id).collect())
    }
}

/// Exact top-`k` over a **live** subset of a raw vector set — independently-derived truth for
/// recall. `live` are the dump ids (input indices) still in the index; ties break by ascending
/// id, matching the D26 total order.
pub fn exact_topk(
    metric: Metric,
    raw: &[Vec<f32>],
    live: &[u64],
    q_raw: &[f32],
    k: usize,
) -> Vec<u64> {
    let mut scored: Vec<(f64, u64)> = live
        .iter()
        .map(|&id| (distance(metric, q_raw, &raw[id as usize]), id))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

/// recall@k = |approx ∩ exact| / |exact| (guards an empty exact set as 1.0).
pub fn recall_at_k(approx: &[u64], exact: &[u64]) -> f64 {
    if exact.is_empty() {
        return 1.0;
    }
    let hit = approx.iter().filter(|id| exact.contains(id)).count();
    hit as f64 / exact.len() as f64
}

/// An on-disk Vamana + PQ index, written in BFS-from-medoid layout (the builder's layout, so
/// block locality matches production). Layout ordinal `i` carries dump id `layout_dump_ids[i]`.
pub struct DiskIndex {
    pub vamana: PathBuf,
    pub pq: PathBuf,
    /// The medoid's **layout** ordinal (the fixed beam-search entry point).
    pub medoid: VamanaIndex,
    /// `layout_dump_ids[i]` = the dump id (input index) of the record at layout ordinal `i`.
    pub layout_dump_ids: Vec<u64>,
}

/// Write `fx` to `dir/{tag}.vamana` + `dir/{tag}.pq` in BFS layout. `dead` (optional, indexed by
/// **input** index) marks holes: a hole keeps its record + codes + adjacency (a navigational
/// waypoint) but its `.pq` node-id column is [`HOLE`], so beam search never emits it. `None` ⇒
/// all live.
pub fn write_disk_index(
    dir: &Path,
    tag: &str,
    fx: &VecFixture,
    dead: Option<&[bool]>,
) -> Result<DiskIndex> {
    std::fs::create_dir_all(dir).ok();
    let order = bfs_order(&fx.graph); // layout order (Vec<VamanaIndex>)
    let mut new_of = vec![0u32; order.len()];
    for (newi, &old) in order.iter().enumerate() {
        new_of[old as usize] = newi as u32;
    }

    let vpath = dir.join(format!("{tag}.vamana"));
    let mut vw = VamanaWriter::create_with_cipher(&vpath, BLOCK, ZSTD, None)?;
    for &old in &order {
        let nbrs: Vec<VamanaIndex> = fx.graph.adjacency[old as usize]
            .iter()
            .map(|&n| new_of[n as usize])
            .collect();
        vw.append(&fx.raw[old as usize], &nbrs)?;
    }
    vw.finish()?;

    let ppath = dir.join(format!("{tag}.pq"));
    write_pq(&ppath, fx, &order, dead)?;

    Ok(DiskIndex {
        vamana: vpath,
        pq: ppath,
        medoid: new_of[fx.graph.medoid as usize],
        layout_dump_ids: order.iter().map(|&o| o as u64).collect(),
    })
}

/// Write just the `.pq` column for `fx` in `order`, optionally with holes. Separated so the
/// delete-IO bench can pair one base `.vamana` (full adjacency = *lazy*) with a holes `.pq`, and
/// a consolidated `.vamana` with the same holes `.pq`.
pub fn write_pq(
    path: &Path,
    fx: &VecFixture,
    order: &[VamanaIndex],
    dead: Option<&[bool]>,
) -> Result<()> {
    let mut pw = PqWriter::create_with_cipher(path, &fx.codebook, BLOCK, ZSTD, None)?;
    for &old in order {
        let node_id = match dead {
            Some(d) if d[old as usize] => HOLE,
            _ => old as u64,
        };
        pw.append_codes(node_id, &fx.codes[old as usize])?;
    }
    pw.finish()?;
    Ok(())
}

/// The BFS layout of `fx.graph` and the medoid's layout ordinal — for callers that write the
/// `.vamana` once and then several `.pq` variants over the same order.
pub fn layout(fx: &VecFixture) -> (Vec<VamanaIndex>, VamanaIndex) {
    let order = bfs_order(&fx.graph);
    let mut new_of = vec![0u32; order.len()];
    for (newi, &old) in order.iter().enumerate() {
        new_of[old as usize] = newi as u32;
    }
    (order, new_of[fx.graph.medoid as usize])
}

/// Beam-search an **on-disk** index. Navigates by the resident PQ estimate, re-ranks by exact
/// distance over the raw vector fetched from the `.vamana`, and skips holes. If `fetches` is
/// given, it is incremented once per node the beam expands — the DiskANN IO unit (one node =
/// one random read). Returns the emitted dump ids best-first.
#[allow(clippy::too_many_arguments)]
pub fn beam_topk_disk(
    vamana: &Path,
    pq: &Path,
    medoid: VamanaIndex,
    metric: Metric,
    space_dim: usize,
    q_raw: &[f32],
    k: usize,
    beam: usize,
    fetches: Option<&Cell<u64>>,
) -> Result<Vec<u64>> {
    let reader = VamanaReader::open_with_cipher(vamana, None)?;
    let resident: ResidentPq = PqReader::open_with_cipher(pq, None)?.load_resident()?;
    let qa = ann_query(metric, q_raw, space_dim)?;
    let adc = AdcTable::new(&resident.codebook, &qa)?;
    let n = reader.len() as usize;
    let hits = beam_search(
        BeamParams {
            medoid,
            beam_width: beam,
            k,
            num_nodes: n,
        },
        |i| adc.estimate(resident.codes_of(i as usize)),
        |i| {
            if let Some(c) = fetches {
                c.set(c.get() + 1);
            }
            let node = reader.node(i)?;
            Ok((node.vector, node.neighbours))
        },
        |v| distance(metric, q_raw, v) as f32,
        |i| {
            Ok(if resident.is_hole(i as usize) {
                None
            } else {
                Some(resident.node_ids[i as usize])
            })
        },
    )?;
    Ok(hits.into_iter().map(|h| h.node_id).collect())
}

/// The `ConsolidateOpts` a delete-consolidation runs with, at the builder's R/α.
pub fn consolidate_opts(fx: &VecFixture, medoid: VamanaIndex) -> ConsolidateOpts {
    ConsolidateOpts {
        medoid,
        r: VAMANA_R,
        alpha: VAMANA_ALPHA,
        metric: fx.metric,
        max_norm: fx.max_norm,
        space_dim: fx.space_dim,
        cache_records: recommended_cache_records(VAMANA_R),
        cache_blocks: RECOMMENDED_CACHE_BLOCKS,
    }
}

/// Consolidate `base_vamana` (the lazy, full-adjacency graph) against `dead` (indexed by
/// **layout** ordinal), writing the spliced graph to `dir/{tag}.vamana`. No live node's
/// adjacency names a hole afterwards, so beam search never fetches a dead record. Returns the
/// output path (layout ordinals + medoid are preserved).
pub fn consolidate_to(
    dir: &Path,
    tag: &str,
    base_vamana: &Path,
    dead_layout: &[bool],
    opts: &ConsolidateOpts,
) -> Result<PathBuf> {
    let reader = VamanaReader::open_with_cipher(base_vamana, None)?;
    let out = dir.join(format!("{tag}.vamana"));
    let mut vw = VamanaWriter::create_with_cipher(&out, BLOCK, ZSTD, None)?;
    consolidate_deletes(&reader, dead_layout, opts, &mut vw)?;
    vw.finish()?;
    Ok(out)
}

/// The `MergeParams` for a streaming merge at the builder's shape.
pub fn merge_params(fx: &VecFixture, medoid: VamanaIndex) -> MergeParams {
    MergeParams {
        medoid,
        r: VAMANA_R,
        alpha: VAMANA_ALPHA,
        l_build: (VAMANA_R * 2).max(64),
        metric: fx.metric,
        max_norm: fx.max_norm,
        vamana_block_bytes: BLOCK,
        pq_block_bytes: BLOCK,
        zstd_level: ZSTD,
        cipher: None,
    }
}

/// Run a streaming merge of `base` (its `.vamana`/`.pq`) with `inserts` (new raw vectors keyed by
/// dump id) and `base_final_ids` (layout ordinal ⇒ surviving dump id, [`HOLE`] to tombstone),
/// writing `dir/{tag}.vamana` + `.pq`. Returns the stats (`vamana_carried` = fast-path fired).
pub fn merge_to(
    dir: &Path,
    tag: &str,
    base: &DiskIndex,
    base_final_ids: &[u64],
    inserts: &[(u64, Vec<f32>)],
    params: &MergeParams,
) -> Result<(PathBuf, PathBuf, MergeStats)> {
    let vout = dir.join(format!("{tag}.vamana"));
    let pout = dir.join(format!("{tag}.pq"));
    let inputs = MergeInputs {
        base_final_ids,
        inserts,
    };
    let stats = streaming_merge(&base.vamana, &base.pq, &inputs, params, &vout, &pout)?;
    Ok((vout, pout, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The plumbing is self-consistent: an on-disk index recalls the same as the in-memory one
    /// (both navigate the same graph + PQ), and exact top-1 of the query itself is the query's
    /// own id. This guards the ANN-space mapping and the BFS-layout neighbour remap — a wrong
    /// mapping compiles but tanks recall.
    #[test]
    fn disk_and_inmem_agree_and_recall_is_high() {
        let raw = random_vectors_unequal_norms(400, 64, 0xF00D);
        let fx = VecFixture::build(Metric::Cosine, raw).unwrap();
        let dir = std::env::temp_dir().join(format!("slater_vecbench_{}", std::process::id()));
        let disk = write_disk_index(&dir, "t", &fx, None).unwrap();

        let live: Vec<u64> = (0..fx.raw.len() as u64).collect();
        let mut inmem_sum = 0.0;
        let mut disk_sum = 0.0;
        let reps = 20;
        for s in 0..reps {
            let q = &fx.raw[(s * 7) % fx.raw.len()];
            let exact = exact_topk(Metric::Cosine, &fx.raw, &live, q, 10);
            inmem_sum += recall_at_k(&fx.beam_topk_inmem(q, 10, 64).unwrap(), &exact);
            disk_sum += recall_at_k(
                &beam_topk_disk(
                    &disk.vamana,
                    &disk.pq,
                    disk.medoid,
                    Metric::Cosine,
                    fx.space_dim,
                    q,
                    10,
                    64,
                    None,
                )
                .unwrap(),
                &exact,
            );
        }
        assert!(
            inmem_sum / reps as f64 > 0.8,
            "in-mem recall too low: {}",
            inmem_sum / reps as f64
        );
        assert!(
            disk_sum / reps as f64 > 0.8,
            "disk recall too low: {}",
            disk_sum / reps as f64
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
