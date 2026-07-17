// SPDX-License-Identifier: Apache-2.0
//! A core segment's **sealed, read-only Vamana index** (FreshDiskANN's read-only temp
//! index, T2/T3; see `docs/SEGMENTED-CORE-PLAN.md` and HIK-113).
//!
//! # Why a per-segment index at all
//! [`crate::segvectors`] already tells a KNN read *which* nodes a segment embeds and which it
//! un-embeds (the `vec.meta` sidecar). That is enough to brute-force the segment level — scan
//! every embedded vector on every query — and for a small segment it is the right thing. But a
//! merged segment (T3) is the *larger* half of the overlay, and brute-forcing it on every query
//! is exactly the cost the write-ladder exists to remove. So a segment whose live embedded set
//! crosses [`SEGMENT_INDEX_MIN_VECTORS`] seals its own Vamana graph beside the sidecar: a
//! `vec.<label>.<property>.vamana` (pure geometry, id-free — v8) and a `vec.<label>.<property>.pq`
//! (the layout→id map + codes the beam navigates by), byte-for-byte the same file shapes the
//! base generation uses ([`crate::vamana`], [`crate::pq`]).
//!
//! # Absence is meaningful
//! A segment below the floor — or one that predates this feature — writes **no** `.vamana`/`.pq`,
//! and [`SegmentVamanaSet::open_if_present_via`] then yields no index for that `(label, property)`.
//! The read side falls back to the exact brute force over the sidecar's ids. That is the whole
//! compatibility story: every pre-existing segment keeps working with zero migration, and a
//! corrupt/half-written pair is likewise treated as "no index" (brute force) rather than an error.
//!
//! # Cross-level comparability is *not* required
//! Each level (base, each segment, the delta) runs its **own** beam in its **own** PQ space, and
//! only the **exact** re-rank — the raw vector under the true metric — crosses levels, via
//! `slater`'s `merge_topk`. So reusing the base's codebook (when the base has one) is a *cost*
//! decision, not a correctness one: it keeps a k-means run off the flush's critical path. A
//! segment whose base is brute-force (no codebook to borrow) trains its own; the two never have
//! to agree.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::crypto::BlockCipher;
use crate::manifest::{AnnNav, Metric};
use crate::pq::{
    ann_point, ann_pq_params, l2_norm, train_codebooks, Codebook, PqParams, PqReader, PqWriter,
    ResidentPq,
};
use crate::store::{join_key, ObjectStore};
use crate::vamana::{bfs_order, build_vamana, build_vamana_ip, VamanaReader, VamanaWriter};

/// Below this many **live** embedded vectors a segment seals no Vamana graph — a proximity
/// graph over a handful of points has genuinely worse recall than a linear scan, and building
/// one is pointless. Below the floor ⇒ no `.vamana`/`.pq` ⇒ the read side brute-forces the
/// segment's sidecar ids exactly.
pub const SEGMENT_INDEX_MIN_VECTORS: usize = 2_000;

/// k-means iterations when a segment must train its own codebook (base is brute-force). The
/// set is bounded by construction (the segment is size-tiered), so this is bounded work.
const SEG_PQ_ITERS: usize = 25;

/// Out-degree bound and long-edge factor for a sealed segment graph — the offline builder's
/// defaults; a segment is small, so these are ample.
const SEG_VAMANA_R: usize = 32;
const SEG_VAMANA_ALPHA: f32 = 1.2;

/// Metadata a segment records for one sealed `(label, property)` Vamana index — the fields a
/// query needs that the `.vamana`/`.pq` files themselves do not carry. Rides
/// [`crate::segmanifest::DirtyVector::graph`], so it is MAC-covered: a forged `medoid` (or a
/// forged `Some`/`None`) would silently corrupt or hide a segment's search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SealedVamanaMeta {
    /// The beam's entry point: the medoid's **layout** ordinal in the `.vamana`/`.pq` (D30).
    pub medoid: u64,
    /// Record count (holes included) — the `.vamana`/`.pq` are written in lockstep, so this
    /// cross-checks both at open. A freshly sealed segment has no holes, so it also equals the
    /// live count.
    pub count: u64,
    /// How this segment's graph is navigated — the HIK-137 MIPS discriminator. **Additive-optional**:
    /// absent on every pre-HIK-137 segment manifest ⇒ [`AnnNav::Augmented`], and omitted from the
    /// serialised form when `Augmented` so existing segments stay byte-identical. A Dot segment seals
    /// IP-native (`InnerProduct`); the read path dispatches on it exactly as the base does.
    #[serde(default, skip_serializing_if = "AnnNav::is_augmented")]
    pub nav: AnnNav,
}

/// Pick the largest PQ subspace count in `{16,8,4,2,1}` that divides `dim`, so a fresh-trained
/// codebook always has valid parameters (`1` divides everything).
fn pick_subspaces(dim: u32) -> u32 {
    for s in [16u32, 8, 4, 2, 1] {
        if dim.is_multiple_of(s) {
            return s;
        }
    }
    1
}

/// Seal a segment's read-only Vamana index for one `(label, property)`.
///
/// * `entries` — the segment's **own** `(dense node id, raw vector)` for every id it embeds and
///   that is still live in the merged/flushed segment. Below [`SEGMENT_INDEX_MIN_VECTORS`] this
///   returns `Ok(None)` and writes nothing (brute-force fallback).
/// * `base` — the base's `(codebook, max_norm)` to reuse when the base has a Vamana index for
///   this `(label, property)`. `None` ⇒ train a fresh codebook over `entries` (the base is
///   brute-force). `max_norm` is only load-bearing for [`Metric::Dot`] (the MIPS augmentation).
///
/// Writes `vec.<label>.<property>.vamana` + `.pq` into `dir` in the **same BFS-from-medoid
/// layout order**, exactly as `slater-build`'s offline builder does, and returns the
/// [`SealedVamanaMeta`] to stamp on the segment manifest's `DirtyVector`. `Ok(None)` when it
/// declined to seal (below the floor, or a norm that overflows f32).
#[allow(clippy::too_many_arguments)]
pub fn seal_segment_index(
    dir: impl AsRef<Path>,
    label: &str,
    property: &str,
    entries: &[(u64, Vec<f32>)],
    metric: Metric,
    dim: u32,
    base: Option<(&Codebook, f64)>,
    cipher: Option<Arc<BlockCipher>>,
    block_bytes: usize,
    zstd_level: i32,
) -> Result<Option<SealedVamanaMeta>> {
    if entries.len() < SEGMENT_INDEX_MIN_VECTORS {
        return Ok(None);
    }

    // HIK-137: a **Dot** segment seals IP-native — raw inner-product closeness (`build_vamana_ip`),
    // a codebook trained on the **raw** vectors (plain `PqParams`, no augmentation subspace), and the
    // highest-norm entry — identical to the offline base build. Cosine/L2 keep the L2-reduced ANN
    // space (`ann_point`). `is_ip` gates every fork below and rides `nav` onto the segment manifest so
    // the read path dispatches to `AdcTable::new_ip` or refuses, never mis-navigates.
    let is_ip = metric == Metric::Dot;
    let nav = if is_ip {
        AnnNav::InnerProduct
    } else {
        AnnNav::Augmented
    };

    // Resolve the codebook + PQ params + the max-norm the (augmented) space is built against. Reuse
    // the base's when we can (keeps k-means off this path) — but only when its width matches this
    // segment's space (an IP segment must not encode raw points against an augmented base codebook,
    // nor vice versa); otherwise train one over the segment's own vectors.
    let base_reusable = base.filter(|(cb, _)| (cb.params.dim == dim) == is_ip);
    let (codebook, max_norm) = match base_reusable {
        Some((cb, mn)) => (cb.clone(), mn),
        None => {
            let subspaces = pick_subspaces(dim);
            let params = if is_ip {
                // No augmentation subspace: the codebook is dim-wide, trained on the raw vectors.
                PqParams::new(dim, subspaces, 8)?
            } else {
                ann_pq_params(metric, dim, subspaces, 8)?
            };
            let mn = entries
                .iter()
                .map(|(_, v)| l2_norm(v))
                .fold(0.0f64, f64::max);
            // A norm that overflows f32 makes the dot augmentation NaN (see `ann_point`) and
            // would poison a `serde_json` manifest for *any* metric. Decline rather than seal a
            // graph the reader cannot open — the brute-force fallback still answers exactly.
            // (Inert for the IP-native path — nothing augments — but a cheap, honest screen.)
            if !is_ip && (mn as f32).is_infinite() {
                return Ok(None);
            }
            let train_points: Vec<Vec<f32>> = if is_ip {
                entries.iter().map(|(_, v)| v.clone()).collect()
            } else {
                entries
                    .iter()
                    .map(|(_, v)| ann_point(metric, v, mn, params.dim as usize))
                    .collect::<Result<_>>()?
            };
            let cb = train_codebooks(&train_points, params, SEG_PQ_ITERS).with_context(|| {
                format!("train PQ codebooks for segment index {label}.{property}")
            })?;
            (cb, mn)
        }
    };
    let space_dim = codebook.params.dim as usize;

    // The build/encode points: raw for IP-native Dot, the L2-reduced ANN map for cosine/L2. The
    // graph build and the PQ codes both work over this same set.
    let points: Vec<Vec<f32>> = if is_ip {
        entries.iter().map(|(_, v)| v.clone()).collect()
    } else {
        entries
            .iter()
            .map(|(_, v)| ann_point(metric, v, max_norm, space_dim))
            .collect::<Result<_>>()
            .with_context(|| format!("map segment index {label}.{property} into ANN space"))?
    };

    let graph = if is_ip {
        build_vamana_ip(&points, SEG_VAMANA_R)
    } else {
        build_vamana(&points, SEG_VAMANA_R, SEG_VAMANA_ALPHA)
    }
    .with_context(|| format!("build Vamana graph for segment index {label}.{property}"))?;
    let order = bfs_order(&graph);
    // old (build) index → new (storage/layout) index.
    let mut new_of = vec![0u32; order.len()];
    for (new_idx, &old) in order.iter().enumerate() {
        new_of[old as usize] = new_idx as u32;
    }
    let medoid_new = new_of[graph.medoid as usize];

    // `.vamana`: **raw** vectors + remapped adjacency, in layout order, id-free (v8).
    let vam_path = dir.as_ref().join(format!("vec.{label}.{property}.vamana"));
    let mut vw =
        VamanaWriter::create_with_cipher(&vam_path, block_bytes, zstd_level, cipher.clone())?;
    for &old in &order {
        let nbrs: Vec<u32> = graph.adjacency[old as usize]
            .iter()
            .map(|&j| new_of[j as usize])
            .collect();
        vw.append(&entries[old as usize].1, &nbrs)?;
    }
    vw.finish()?;

    // `.pq`: the codebook + per-vector codes (ANN space) + the layout→id map (dense node id),
    // in the same layout order. Freshly sealed ⇒ no holes.
    let pq_path = dir.as_ref().join(format!("vec.{label}.{property}.pq"));
    let mut pw =
        PqWriter::create_with_cipher(&pq_path, &codebook, block_bytes, zstd_level, cipher)?;
    for &old in &order {
        let codes = codebook.encode(&points[old as usize])?;
        pw.append_codes(entries[old as usize].0, &codes)?;
    }
    pw.finish()?;

    Ok(Some(SealedVamanaMeta {
        medoid: medoid_new as u64,
        count: entries.len() as u64,
        nav,
    }))
}

/// One opened sealed segment index: the on-disk geometry reader + the resident PQ (codes +
/// layout→id map) + the entry medoid. `ord` is a **segment-local** ordinal (0-based over the
/// segment's sealed indexes) used, together with the segment uuid, to key the vector-index
/// cache — segment uuids are globally unique, so `(seg_uuid, ord)` never collides with a base
/// generation's `(gen_uuid, ord)`.
pub struct SegmentVamanaIndex {
    pub ord: u32,
    pub medoid: u64,
    /// How this segment's graph is navigated (HIK-137). Carried from [`SealedVamanaMeta::nav`] so
    /// the read path dispatches to the IP navigator for a Dot segment, or the augmented one, never
    /// mis-navigating one as the other.
    pub nav: AnnNav,
    pub reader: VamanaReader,
    pub pq: Arc<ResidentPq>,
}

/// A segment's opened sealed Vamana indexes, one per `(label, property)` that crossed the floor
/// at flush/merge. Absent entries ⇒ the read side brute-forces that `(label, property)`.
pub struct SegmentVamanaSet {
    indexes: Vec<((String, String), SegmentVamanaIndex)>,
}

impl SegmentVamanaSet {
    /// Open every sealed index a segment declares (`DirtyVector::graph = Some`), via `store`
    /// under `prefix` (the segment directory key). A declared-but-missing or unreadable pair is
    /// **skipped** — treated as "no sealed index", so the read side brute-forces it exactly
    /// rather than failing the whole segment open. Returns `None` when the segment sealed
    /// nothing.
    pub fn open_if_present_via(
        store: &dyn ObjectStore,
        prefix: &str,
        dirty_vectors: &[crate::segmanifest::DirtyVector],
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Option<Self>> {
        let mut indexes = Vec::new();
        let mut ord = 0u32;
        for dv in dirty_vectors {
            let Some(meta) = dv.graph else { continue };
            let stem = format!("vec.{}.{}", dv.label, dv.property);
            let vam_key = join_key(prefix, &format!("{stem}.vamana"));
            let pq_key = join_key(prefix, &format!("{stem}.pq"));
            // Declared, but the files are gone/half-written: fall back to brute force rather
            // than error. That is what makes the sidecar's absence meaningful either way.
            if !store.exists(&vam_key)? || !store.exists(&pq_key)? {
                continue;
            }
            let reader = match VamanaReader::open_src(store.open(&vam_key)?, cipher.clone()) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let pq = match PqReader::open_src(store.open(&pq_key)?, cipher.clone())
                .and_then(|r| r.load_resident())
            {
                Ok(p) => p,
                Err(_) => continue,
            };
            // Lockstep + medoid sanity: a mismatch means the pair is not the graph the manifest
            // describes. Treat it as "no index" (brute force), never a wrong search.
            if pq.len() as u64 != reader.len()
                || reader.len() != meta.count
                || (!reader.is_empty() && meta.medoid >= reader.len())
            {
                continue;
            }
            indexes.push((
                (dv.label.clone(), dv.property.clone()),
                SegmentVamanaIndex {
                    ord,
                    medoid: meta.medoid,
                    nav: meta.nav,
                    reader,
                    pq: Arc::new(pq),
                },
            ));
            ord += 1;
        }
        Ok((!indexes.is_empty()).then_some(Self { indexes }))
    }

    /// The sealed index for `(label, property)`, if the segment sealed one.
    pub fn get(&self, label: &str, property: &str) -> Option<&SegmentVamanaIndex> {
        self.indexes
            .iter()
            .find(|((l, p), _)| l == label && p == property)
            .map(|(_, ix)| ix)
    }

    /// Every sealed index — for pinning the resident PQ set at generation open / retirement.
    pub fn iter(&self) -> impl Iterator<Item = &SegmentVamanaIndex> {
        self.indexes.iter().map(|(_, ix)| ix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pq::{ann_query, AdcTable, Lcg};
    use crate::segmanifest::DirtyVector;
    use crate::store::fs::FsObjectStore;
    use crate::vamana::{beam_search, decode_node, BeamParams};

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("slater_segvam_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn rand_vectors(dim: usize, n: usize, seed: u64) -> Vec<(u64, Vec<f32>)> {
        let mut rng = Lcg(seed);
        (0..n)
            .map(|i| {
                (
                    i as u64 + 1000,
                    (0..dim).map(|_| (rng.next_f64() as f32) - 0.5).collect(),
                )
            })
            .collect()
    }

    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        let mut dot = 0.0f64;
        let (mut na, mut nb) = (0.0f64, 0.0f64);
        for (x, y) in a.iter().zip(b) {
            dot += *x as f64 * *y as f64;
            na += *x as f64 * *x as f64;
            nb += *y as f64 * *y as f64;
        }
        if na == 0.0 || nb == 0.0 {
            return 1.0;
        }
        (1.0 - dot / (na.sqrt() * nb.sqrt())) as f32
    }

    fn brute(entries: &[(u64, Vec<f32>)], query: &[f32], k: usize) -> Vec<u64> {
        let mut scored: Vec<(f32, u64)> = entries
            .iter()
            .map(|(id, v)| (cosine(query, v), *id))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(_, id)| id).collect()
    }

    /// A sealed segment index must recover the brute-force top-k over its own vectors — the DoD
    /// recall bar. Truth is a brute force written here, not a second implementation.
    #[test]
    fn sealed_segment_recall_matches_brute_force() {
        let dir = tmp("recall");
        let dim = 32;
        let entries = rand_vectors(dim, 3_000, 0x51a7_e100_0001);
        let meta = seal_segment_index(
            &dir,
            "Doc",
            "embedding",
            &entries,
            Metric::Cosine,
            dim as u32,
            None, // train fresh
            None,
            4096,
            3,
        )
        .unwrap()
        .expect("above the floor ⇒ sealed");
        assert_eq!(meta.count, 3_000);

        let dv = vec![DirtyVector {
            label: "Doc".into(),
            property: "embedding".into(),
            graph: Some(meta),
        }];
        let store = FsObjectStore::new(dir.parent().unwrap());
        let prefix = dir.file_name().unwrap().to_str().unwrap();
        let set = SegmentVamanaSet::open_if_present_via(&store, prefix, &dv, None)
            .unwrap()
            .expect("a sealed index opens");
        let ix = set.get("Doc", "embedding").unwrap();

        let queries = rand_vectors(dim, 20, 0xfeed_0001);
        let k = 10;
        let mut total = 0.0f64;
        for (_, q) in &queries {
            let qn = ann_query(Metric::Cosine, q, ix.pq.codebook.params.dim as usize).unwrap();
            let adc = AdcTable::new(&ix.pq.codebook, &qn).unwrap();
            let hits = beam_search(
                BeamParams {
                    medoid: ix.medoid as u32,
                    beam_width: 64,
                    k,
                    num_nodes: ix.pq.len(),
                },
                |i| adc.estimate(ix.pq.codes_of(i as usize)),
                |i| {
                    let node = decode_node(&ix.reader.node(i).map(|n| encode_back(&n)).unwrap())?;
                    Ok((node.vector, node.neighbours))
                },
                |v| cosine(q, v),
                |i| Ok(Some(ix.pq.node_ids[i as usize])),
            )
            .unwrap();
            let got: std::collections::HashSet<u64> = hits.iter().map(|h| h.node_id).collect();
            let want = brute(&entries, q, k);
            total += want.iter().filter(|id| got.contains(id)).count() as f64 / k as f64;
        }
        let recall = total / queries.len() as f64;
        assert!(recall >= 0.9, "sealed segment recall@{k} was {recall:.3}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A **Dot** segment seals IP-native (HIK-137): the meta carries `nav: InnerProduct`, the graph
    /// is built over raw inner product, and it is navigated by `AdcTable::new_ip`. Recall is measured
    /// on a MIPS-hard fixture (a heavy-norm outlier every 97th vector) against an independent
    /// brute-force IP truth — the shape that craters an augmented segment.
    #[test]
    fn dot_segment_seals_ip_native_and_recovers_the_ip_topk() {
        use crate::pq::AdcTable;
        let dir = tmp("ip_recall");
        let dim = 16;
        // MIPS-hard: mostly unit-norm, every 97th a ~30× outlier (near every query under IP).
        let raw = rand_vectors(dim, 3_000, 0x15ee_d001);
        let entries: Vec<(u64, Vec<f32>)> = raw
            .into_iter()
            .enumerate()
            .map(|(i, (id, v))| {
                let scale = if i.is_multiple_of(97) { 30.0 } else { 1.0 };
                let nrm = (v.iter().map(|x| x * x).sum::<f32>()).sqrt().max(1e-6);
                (id, v.iter().map(|x| x * scale / nrm).collect())
            })
            .collect();
        let meta = seal_segment_index(
            &dir,
            "Doc",
            "emb",
            &entries,
            Metric::Dot,
            dim as u32,
            None,
            None,
            4096,
            3,
        )
        .unwrap()
        .expect("above the floor ⇒ sealed");
        assert_eq!(
            meta.nav,
            AnnNav::InnerProduct,
            "a Dot segment must seal IP-native"
        );

        let dv = vec![DirtyVector {
            label: "Doc".into(),
            property: "emb".into(),
            graph: Some(meta),
        }];
        let store = FsObjectStore::new(dir.parent().unwrap());
        let prefix = dir.file_name().unwrap().to_str().unwrap();
        let set = SegmentVamanaSet::open_if_present_via(&store, prefix, &dv, None)
            .unwrap()
            .expect("a sealed index opens");
        let ix = set.get("Doc", "emb").unwrap();
        assert_eq!(
            ix.nav,
            AnnNav::InnerProduct,
            "the opened index must carry the IP nav so the reader dispatches"
        );

        let neg_dot = |a: &[f32], b: &[f32]| -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        let brute_ip = |q: &[f32], k: usize| {
            let mut s: Vec<(f32, u64)> =
                entries.iter().map(|(id, v)| (neg_dot(q, v), *id)).collect();
            s.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            s.into_iter().take(k).map(|(_, id)| id).collect::<Vec<_>>()
        };

        let queries = rand_vectors(dim, 30, 0xfeed_0002);
        let k = 10;
        let mut total = 0.0f64;
        for (_, q) in &queries {
            // IP-native serve: raw query into `AdcTable::new_ip` (no `ann_query`), exact-IP re-rank.
            let adc = AdcTable::new_ip(&ix.pq.codebook, q).unwrap();
            let hits = beam_search(
                BeamParams {
                    medoid: ix.medoid as u32,
                    beam_width: 64,
                    k,
                    num_nodes: ix.pq.len(),
                },
                |i| adc.estimate(ix.pq.codes_of(i as usize)),
                |i| {
                    let node = decode_node(&ix.reader.node(i).map(|n| encode_back(&n)).unwrap())?;
                    Ok((node.vector, node.neighbours))
                },
                |v| neg_dot(q, v),
                |i| Ok(Some(ix.pq.node_ids[i as usize])),
            )
            .unwrap();
            let got: std::collections::HashSet<u64> = hits.iter().map(|h| h.node_id).collect();
            let want = brute_ip(q, k);
            total += want.iter().filter(|id| got.contains(id)).count() as f64 / k as f64;
        }
        let recall = total / queries.len() as f64;
        assert!(
            recall >= 0.9,
            "IP-native segment recall@{k} was {recall:.3}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // The beam's `fetch` in this test reads through the uncached `VamanaReader::node`; re-encode
    // the decoded node so the closure's `decode_node` has bytes to chew (the production path
    // reads raw block bytes through the cache). Keeps the test honest about the on-disk format.
    fn encode_back(n: &crate::vamana::VamanaNode) -> Vec<u8> {
        use crate::wire::write_uvarint;
        use byteorder::{LittleEndian, WriteBytesExt};
        let mut rec = Vec::new();
        write_uvarint(&mut rec, n.vector.len() as u64);
        for x in &n.vector {
            rec.write_f32::<LittleEndian>(*x).unwrap();
        }
        write_uvarint(&mut rec, n.neighbours.len() as u64);
        for nb in &n.neighbours {
            write_uvarint(&mut rec, *nb as u64);
        }
        rec
    }

    /// Below the floor seals nothing at all — the read side then brute-forces the sidecar ids.
    #[test]
    fn below_the_floor_seals_nothing() {
        let dir = tmp("floor");
        let entries = rand_vectors(8, 100, 0x1234);
        let out = seal_segment_index(
            &dir,
            "Doc",
            "emb",
            &entries,
            Metric::Cosine,
            8,
            None,
            None,
            4096,
            3,
        )
        .unwrap();
        assert!(out.is_none(), "100 < floor ⇒ no sealed index");
        assert!(!dir.join("vec.Doc.emb.vamana").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A declared index whose files were deleted opens as `None` (brute-force fallback), not an
    /// error — the compatibility discipline.
    #[test]
    fn a_declared_but_missing_index_opens_as_none() {
        let dir = tmp("missing");
        // Nothing on disk, but the manifest claims a sealed index.
        let dv = vec![DirtyVector {
            label: "Doc".into(),
            property: "emb".into(),
            graph: Some(SealedVamanaMeta {
                medoid: 0,
                count: 3_000,
                nav: AnnNav::Augmented,
            }),
        }];
        let store = FsObjectStore::new(dir.parent().unwrap());
        let prefix = dir.file_name().unwrap().to_str().unwrap();
        let set = SegmentVamanaSet::open_if_present_via(&store, prefix, &dv, None).unwrap();
        assert!(set.is_none(), "missing files ⇒ no index ⇒ brute force");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
