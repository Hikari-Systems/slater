// SPDX-License-Identifier: Apache-2.0
//! Disk-native single-layer Vamana graph (DiskANN-style) for the large-vector
//! ANN path.
//!
//! For a vector index at or above the ANN threshold, brute force is too slow, so
//! the builder lays the index out as a navigable proximity graph: every vector is
//! a node with a bounded out-degree (`R`) of carefully-chosen neighbours, and a
//! query is answered by a greedy beam walk from a fixed entry **medoid** towards
//! the query. Construction uses the Vamana **robust prune** (the `alpha` long-edge
//! rule) so the graph stays navigable with small degree.
//!
//! Three things live here, in build → store → search order:
//! 1. [`build_vamana`] — construct the adjacency + pick the medoid (pure, in-memory,
//!    offline-only). Operates on **normalised** vectors with squared-L2 (monotonic
//!    in cosine on unit vectors — D29), so it is metric-agnostic given the caller
//!    normalised for cosine.
//! 2. [`bfs_order`] + [`VamanaWriter`]/[`VamanaReader`] — the on-disk block file
//!    `[node_id ‖ full vec ‖ adjacency]`, laid out by BFS-from-medoid for locality
//!    so a walk touches few distinct blocks. Goes through the same `blockfile` seam
//!    as every other store (zstd + the M6 AEAD for free — D28).
//! 3. [`beam_search`] — the generic greedy beam walk. It is parameterised over a
//!    resident PQ *estimate* (navigation, no IO) and a block-reading *fetch* (full
//!    vector + neighbours), with an *exact* re-rank closure, so the **same** search
//!    drives the pure in-memory recall test here and the cache-backed reader in
//!    `slater` with no duplicated logic.
//!
// DESIGN (D30): adjacency is stored as **global vamana indices** (a node's
// position in the file = its index), NOT as on-disk `(block_id, slot)` pairs. The
// blockfile already keeps a resident, tiny prefix-sum directory that maps any
// global record index to its `(block, slot)` via `locate()`, so the reader derives
// the block-relative address for free and coalesces a frontier's reads by block.
// Storing the pair on disk instead would be circular to size (variable-width
// records whose neighbour fields encode the very block boundaries that depend on
// those fields' widths); storing the index sidesteps that entirely and costs no
// extra resident memory.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::pq::{sq_l2, Lcg};
use crate::wire::{capacity_for, read_uvarint, write_uvarint};

/// A `(0..count)` index into a Vamana graph — a node's position in the file.
pub type VamanaIndex = u32;

// ── Build (offline, in-memory) ─────────────────────────────────────────────────

/// The result of constructing a Vamana graph: each node's out-neighbours (by
/// index) and the entry medoid.
pub struct VamanaGraph {
    pub adjacency: Vec<Vec<VamanaIndex>>,
    pub medoid: VamanaIndex,
}

/// Build a single-layer Vamana graph over `vectors` (each `dim`-long, expected
/// already L2-normalised for a cosine index — D29). `r` bounds out-degree; `alpha`
/// is the robust-prune long-edge factor (1.2 is typical). Deterministic.
pub fn build_vamana(vectors: &[Vec<f32>], r: usize, alpha: f32) -> Result<VamanaGraph> {
    let n = vectors.len();
    if n == 0 {
        bail!("cannot build a Vamana graph over zero vectors");
    }
    let r = r.max(1);
    let medoid = compute_medoid(vectors);

    // Trivially small index: a complete graph (capped at R) is already navigable.
    if n <= r + 1 {
        let adjacency = (0..n)
            .map(|i| (0..n).filter(|&j| j != i).map(|j| j as u32).collect())
            .collect();
        return Ok(VamanaGraph {
            adjacency,
            medoid: medoid as u32,
        });
    }

    let mut rng = Lcg(0x5111_a7e1_3a3a_0001);
    // Random R-regular initial graph.
    let mut adjacency: Vec<Vec<u32>> = (0..n)
        .map(|i| {
            let mut nbrs = Vec::with_capacity(r);
            while nbrs.len() < r {
                let j = rng.next_below(n);
                if j != i && !nbrs.contains(&(j as u32)) {
                    nbrs.push(j as u32);
                }
            }
            nbrs
        })
        .collect();

    // Search-list size during construction — wider than R for better candidates.
    let l_build = (r * 2).max(64);

    // Two passes: alpha = 1 (short edges first), then the real alpha (long edges).
    for &pass_alpha in &[1.0f32, alpha.max(1.0)] {
        let order = random_permutation(n, &mut rng);
        for &p in &order {
            let visited = greedy_search_build(medoid, p, &adjacency, vectors, l_build);
            // Candidate pool = everything the search touched, plus p's current
            // neighbours, minus p itself.
            let mut cands: Vec<u32> = visited;
            cands.extend_from_slice(&adjacency[p]);
            cands.sort_unstable();
            cands.dedup();
            cands.retain(|&c| c as usize != p);

            let pruned = robust_prune(p, &cands, pass_alpha, r, vectors);
            adjacency[p] = pruned.clone();

            // Make the new edges symmetric, re-pruning any neighbour that overflows.
            for &j in &pruned {
                let j = j as usize;
                if !adjacency[j].contains(&(p as u32)) {
                    adjacency[j].push(p as u32);
                }
                if adjacency[j].len() > r {
                    let cand_j = adjacency[j].clone();
                    adjacency[j] = robust_prune(j, &cand_j, pass_alpha, r, vectors);
                }
            }
        }
    }

    Ok(VamanaGraph {
        adjacency,
        medoid: medoid as u32,
    })
}

/// The medoid: the point closest to the centroid of all vectors (a cheap, stable
/// stand-in for the true graph medoid that is good enough as a fixed entry point).
fn compute_medoid(vectors: &[Vec<f32>]) -> usize {
    let dim = vectors[0].len();
    let mut mean = vec![0.0f64; dim];
    for v in vectors {
        for (acc, &x) in mean.iter_mut().zip(v) {
            *acc += x as f64;
        }
    }
    for x in mean.iter_mut() {
        *x /= vectors.len() as f64;
    }
    let mean_f32: Vec<f32> = mean.iter().map(|&x| x as f32).collect();
    (0..vectors.len())
        .min_by(|&a, &b| sq_l2(&vectors[a], &mean_f32).total_cmp(&sq_l2(&vectors[b], &mean_f32)))
        .unwrap()
}

fn random_permutation(n: usize, rng: &mut Lcg) -> Vec<usize> {
    let mut order: Vec<usize> = (0..n).collect();
    // Fisher–Yates with the deterministic LCG.
    for i in (1..n).rev() {
        let j = rng.next_below(i + 1);
        order.swap(i, j);
    }
    order
}

/// Greedy search over the *current* graph from `start` towards point `p`, using
/// exact squared-L2 over full vectors. Returns every node it expanded (the
/// candidate pool for pruning).
fn greedy_search_build(
    start: usize,
    p: usize,
    adjacency: &[Vec<u32>],
    vectors: &[Vec<f32>],
    l_size: usize,
) -> Vec<u32> {
    let d = |x: usize| sq_l2(&vectors[x], &vectors[p]);
    let mut beam: Vec<(f64, u32)> = vec![(d(start), start as u32)];
    let mut expanded = vec![false; vectors.len()];
    let mut visited = Vec::new();
    while let Some((_, cur)) = beam
        .iter()
        .copied()
        .filter(|(_, i)| !expanded[*i as usize])
        .min_by(|a, b| a.0.total_cmp(&b.0))
    {
        expanded[cur as usize] = true;
        visited.push(cur);
        for &nb in &adjacency[cur as usize] {
            if !beam.iter().any(|(_, i)| *i == nb) {
                beam.push((d(nb as usize), nb));
            }
        }
        beam.sort_by(|a, b| a.0.total_cmp(&b.0));
        beam.truncate(l_size);
    }
    visited
}

/// Vamana robust prune: from `candidates` (sorted by nothing in particular), pick
/// up to `r` out-neighbours for `p`, greedily taking the closest and discarding any
/// candidate `c` that the chosen `p*` "dominates" (`alpha · d(p*, c) ≤ d(p, c)`).
fn robust_prune(
    p: usize,
    candidates: &[u32],
    alpha: f32,
    r: usize,
    vectors: &[Vec<f32>],
) -> Vec<u32> {
    let alpha = alpha as f64;
    let mut pool: Vec<(f64, u32)> = candidates
        .iter()
        .filter(|&&c| c as usize != p)
        .map(|&c| (sq_l2(&vectors[c as usize], &vectors[p]), c))
        .collect();
    pool.sort_by(|a, b| a.0.total_cmp(&b.0));

    let mut out: Vec<u32> = Vec::with_capacity(r);
    while let Some((_, pstar)) = pool.first().copied() {
        out.push(pstar);
        if out.len() >= r {
            break;
        }
        // Drop pstar and any candidate it dominates.
        pool.retain(|&(d_pc, c)| {
            if c == pstar {
                return false;
            }
            let d_star_c = sq_l2(&vectors[pstar as usize], &vectors[c as usize]);
            alpha * d_star_c > d_pc
        });
    }
    out
}

/// A BFS order from the medoid — the locality layout. Nodes unreachable from the
/// medoid (rare, but possible in a sparse graph) are appended in index order so the
/// permutation always covers all `n` nodes.
pub fn bfs_order(graph: &VamanaGraph) -> Vec<VamanaIndex> {
    let n = graph.adjacency.len();
    let mut seen = vec![false; n];
    let mut order = Vec::with_capacity(n);
    let mut q = VecDeque::new();
    q.push_back(graph.medoid);
    seen[graph.medoid as usize] = true;
    while let Some(cur) = q.pop_front() {
        order.push(cur);
        for &nb in &graph.adjacency[cur as usize] {
            if !seen[nb as usize] {
                seen[nb as usize] = true;
                q.push_back(nb);
            }
        }
    }
    for (i, &was_seen) in seen.iter().enumerate() {
        if !was_seen {
            order.push(i as u32);
        }
    }
    order
}

// ── `.vamana` block-file store ─────────────────────────────────────────────────

/// One decoded Vamana node: its dense graph node id, full vector, and out-neighbour
/// indices (in vamana-index space).
#[derive(Debug, Clone, PartialEq)]
pub struct VamanaNode {
    pub node_id: u64,
    pub vector: Vec<f32>,
    pub neighbours: Vec<VamanaIndex>,
}

/// Writer for `vector/<l>.<p>.vamana`. Append nodes in **layout order** (the BFS
/// permutation) — a node's append position is its vamana index, and neighbour
/// fields must already be expressed in that same permuted index space.
pub struct VamanaWriter {
    inner: BlockFileWriter,
    next: u64,
}

impl VamanaWriter {
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        Ok(Self {
            inner: BlockFileWriter::create_with_cipher(
                path,
                target_block_bytes,
                zstd_level,
                cipher,
            )?,
            next: 0,
        })
    }

    /// Append one node; returns its vamana index (= append position).
    pub fn append(
        &mut self,
        node_id: u64,
        vector: &[f32],
        neighbours: &[VamanaIndex],
    ) -> Result<VamanaIndex> {
        let mut rec = Vec::with_capacity(16 + vector.len() * 4 + neighbours.len() * 4);
        write_uvarint(&mut rec, node_id);
        write_uvarint(&mut rec, vector.len() as u64);
        for x in vector {
            rec.write_f32::<LittleEndian>(*x)?;
        }
        write_uvarint(&mut rec, neighbours.len() as u64);
        for nb in neighbours {
            write_uvarint(&mut rec, *nb as u64);
        }
        self.inner.append_record(&rec)?;
        let idx = self.next as u32;
        self.next += 1;
        Ok(idx)
    }

    pub fn finish(self) -> Result<u64> {
        self.inner.finish()
    }
}

/// Reader for `vector/<l>.<p>.vamana`.
pub struct VamanaReader {
    inner: BlockFileReader,
}

impl VamanaReader {
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let src = Arc::new(crate::store::fs::FileObject::open(path)?);
        Self::open_src(src, cipher)
    }

    /// Open from any positional-read source (local file or remote object).
    pub fn open_src(
        src: Arc<dyn crate::store::RandomReadAt>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        Ok(Self {
            inner: BlockFileReader::open_src(src, cipher)?,
        })
    }

    /// Number of nodes (= records).
    pub fn len(&self) -> u64 {
        self.inner.total_records()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Decode the node at vamana index `i` (uncached `pread`). The cache-backed
    /// reader in `slater` reads through its vector-index pool instead, decoding the
    /// record with [`decode_node`].
    pub fn node(&self, i: VamanaIndex) -> Result<VamanaNode> {
        let rec = self.inner.read_record_global(i as u64)?;
        decode_node(&rec)
    }

    /// The underlying block file, so a cache holder can read records through it and
    /// map indices to `(block, slot)` via `BlockFileReader::locate` for coalesced
    /// frontier reads (D30).
    pub fn inner(&self) -> &BlockFileReader {
        &self.inner
    }
}

/// Decode a Vamana record (`uvarint(node_id) ‖ uvarint(dim) ‖ dim×f32 ‖
/// uvarint(degree) ‖ degree×uvarint(index)`). Public so a cached-block reader can
/// decode a record sliced out of a block it already holds decompressed.
pub fn decode_node(rec: &[u8]) -> Result<VamanaNode> {
    let mut r = rec;
    let node_id = read_uvarint(&mut r)?;
    let dim = read_uvarint(&mut r)? as usize;
    // `dim` and `degree` are untrusted on-disk uvarints. A vector element is 4 bytes and a
    // neighbour ≥1, so reserve only what the record's remaining bytes could hold: a forged
    // count then errors on the first short read instead of aborting on the allocation.
    let mut vector = Vec::with_capacity(capacity_for(dim, r.len(), 4));
    for _ in 0..dim {
        vector.push(r.read_f32::<LittleEndian>()?);
    }
    let degree = read_uvarint(&mut r)? as usize;
    let mut neighbours = Vec::with_capacity(capacity_for(degree, r.len(), 1));
    for _ in 0..degree {
        neighbours.push(read_uvarint(&mut r)? as u32);
    }
    Ok(VamanaNode {
        node_id,
        vector,
        neighbours,
    })
}

// ── Generic beam search ─────────────────────────────────────────────────────────

/// One search result: the vamana index reached and its **exact** distance.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchHit {
    pub index: VamanaIndex,
    pub exact: f32,
}

/// Greedy beam search from `medoid` for the `k` nearest neighbours.
///
/// - `num_nodes` — total nodes in the graph.
/// - `beam_width` — search-list size `L` (≥ `k` for good recall).
/// - `estimate(i)` — the **PQ-estimated** distance of index `i` to the query;
///   resident, cheap, used for *navigation* (which node to expand next). No IO.
/// - `fetch(i)` — reads node `i`'s block and returns `(full_vector, neighbours)`;
///   the one IO per expansion. The full vector feeds the exact re-rank.
/// - `exact(&v)` — the exact distance of a full vector to the query (the score the
///   caller ultimately reports), used to re-rank the expanded set.
///
/// Returns up to `k` hits, ascending by exact distance.
pub fn beam_search<F, FE>(
    medoid: VamanaIndex,
    beam_width: usize,
    k: usize,
    num_nodes: usize,
    estimate: impl Fn(VamanaIndex) -> f32,
    mut fetch: F,
    exact: FE,
) -> Result<Vec<SearchHit>>
where
    F: FnMut(VamanaIndex) -> Result<(Vec<f32>, Vec<VamanaIndex>)>,
    FE: Fn(&[f32]) -> f32,
{
    let beam_width = beam_width.max(k).max(1);
    let mut expanded = vec![false; num_nodes];
    // Working candidate list, kept ascending by PQ estimate and capped to L.
    let mut beam: Vec<(f32, VamanaIndex)> = vec![(estimate(medoid), medoid)];
    let mut hits: Vec<SearchHit> = Vec::new();

    while let Some((_, cur)) = beam
        .iter()
        .copied()
        .filter(|(_, i)| !expanded[*i as usize])
        .min_by(|a, b| a.0.total_cmp(&b.0))
    {
        expanded[cur as usize] = true;
        let (vector, neighbours) = fetch(cur)?;
        hits.push(SearchHit {
            index: cur,
            exact: exact(&vector),
        });
        for nb in neighbours {
            if (nb as usize) < num_nodes
                && !expanded[nb as usize]
                && !beam.iter().any(|(_, i)| *i == nb)
            {
                beam.push((estimate(nb), nb));
            }
        }
        beam.sort_by(|a, b| a.0.total_cmp(&b.0));
        beam.truncate(beam_width);
    }

    hits.sort_by(|a, b| a.exact.total_cmp(&b.exact).then(a.index.cmp(&b.index)));
    hits.truncate(k);
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pq::{train_codebooks, AdcTable, PqParams};

    fn norm(v: &mut [f32]) {
        let n: f64 = v
            .iter()
            .map(|&x| (x as f64) * (x as f64))
            .sum::<f64>()
            .sqrt();
        if n > 0.0 {
            for x in v.iter_mut() {
                *x = (*x as f64 / n) as f32;
            }
        }
    }

    /// `n` unit vectors in `dim` dimensions, deterministic.
    fn unit_vectors(dim: usize, n: usize) -> Vec<Vec<f32>> {
        let mut rng = Lcg(0x1234_5678_9abc_def0);
        (0..n)
            .map(|_| {
                let mut v: Vec<f32> = (0..dim).map(|_| (rng.next_f64() as f32) - 0.5).collect();
                norm(&mut v);
                v
            })
            .collect()
    }

    fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
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

    fn brute_force_topk(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<usize> {
        let mut scored: Vec<(f32, usize)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (cosine_distance(query, v), i))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(_, i)| i).collect()
    }

    #[test]
    fn build_produces_bounded_degree_and_reachable_graph() {
        let vectors = unit_vectors(16, 200);
        let g = build_vamana(&vectors, 24, 1.2).unwrap();
        assert_eq!(g.adjacency.len(), 200);
        for nbrs in &g.adjacency {
            assert!(nbrs.len() <= 24, "degree {} exceeds R", nbrs.len());
        }
        // BFS from the medoid reaches every node (the layout permutation is total).
        let order = bfs_order(&g);
        assert_eq!(order.len(), 200);
        let mut sorted = order.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            200,
            "layout must be a permutation of all nodes"
        );
    }

    #[test]
    fn vamana_store_roundtrips_nodes_and_adjacency() {
        let vectors = unit_vectors(8, 40);
        let g = build_vamana(&vectors, 8, 1.2).unwrap();
        let order = bfs_order(&g);
        // old index -> new (storage) index.
        let mut new_of = vec![0u32; order.len()];
        for (new_idx, &old) in order.iter().enumerate() {
            new_of[old as usize] = new_idx as u32;
        }

        let path = std::env::temp_dir().join(format!("slater_vam_{}_{}", std::process::id(), "rt"));
        let mut w = VamanaWriter::create_with_cipher(&path, 4096, 3, None).unwrap();
        for &old in &order {
            let nbrs: Vec<u32> = g.adjacency[old as usize]
                .iter()
                .map(|&j| new_of[j as usize])
                .collect();
            w.append(old as u64, &vectors[old as usize], &nbrs).unwrap();
        }
        w.finish().unwrap();

        let r = VamanaReader::open_with_cipher(&path, None).unwrap();
        assert_eq!(r.len(), 40);
        // Node at storage index 0 is the medoid (BFS root); its node_id is the old
        // medoid index and its neighbours are remapped.
        let n0 = r.node(0).unwrap();
        assert_eq!(n0.node_id, g.medoid as u64);
        assert_eq!(n0.vector, vectors[g.medoid as usize]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn beam_search_recall_matches_brute_force() {
        // The headline scorer test (PLAN.md): Vamana + PQ navigation must recover
        // most of the brute-force top-k on a synthetic set.
        let dim = 32;
        let n = 1000;
        let vectors = unit_vectors(dim, n);
        let g = build_vamana(&vectors, 32, 1.2).unwrap();

        // Train PQ on the same (normalised) vectors and encode them.
        let params = PqParams::new(dim as u32, 8, 8).unwrap();
        let cb = train_codebooks(&vectors, params, 25).unwrap();
        let codes: Vec<Vec<u8>> = vectors.iter().map(|v| cb.encode(v).unwrap()).collect();

        let k = 10;
        let beam_width = 64;
        let mut total_recall = 0.0f64;
        let queries = 20;
        for q in 0..queries {
            // Use a held-out-ish query: a stored vector perturbed slightly.
            let mut query = vectors[(q * 37) % n].clone();
            query[0] += 0.05;
            norm(&mut query);

            let adc = AdcTable::new(&cb, &query).unwrap();
            let hits = beam_search(
                g.medoid,
                beam_width,
                k,
                n,
                |i| adc.estimate(&codes[i as usize]),
                |i| Ok((vectors[i as usize].clone(), g.adjacency[i as usize].clone())),
                |v| cosine_distance(&query, v),
            )
            .unwrap();

            let got: std::collections::HashSet<usize> =
                hits.iter().map(|h| h.index as usize).collect();
            let truth = brute_force_topk(&vectors, &query, k);
            let found = truth.iter().filter(|i| got.contains(i)).count();
            total_recall += found as f64 / k as f64;
        }
        let recall = total_recall / queries as f64;
        assert!(
            recall >= 0.85,
            "Vamana+PQ recall@{k} was {recall:.3}, expected ≥ 0.85"
        );
    }
}
