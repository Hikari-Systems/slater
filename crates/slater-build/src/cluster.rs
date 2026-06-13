// SPDX-License-Identifier: Apache-2.0
//! Bounded-memory node clustering → a locality-aware node-id permutation.
//!
//! In this format a node *is* its record position, so packing graph-proximate
//! nodes into the same on-disk block means assigning them adjacent final node ids
//! that fall in the same block. This module computes that permutation under a
//! memory cap.
//!
//! Modes:
//! - [`ClusterMode::None`]   — identity (final id = provisional id); zero extra state.
//! - [`ClusterMode::Ldg`]    — streaming Linear-Deterministic-Greedy partitioning.
//!   Holds one `O(N)` `Vec<u32>` (node→partition, ~366 MB at 91.6M nodes —
//!   independent of edge count) plus a per-node sparse tally (`O(degree)`); edges
//!   are streamed from disk, never resident. Partitions are sized to ≈ one block's
//!   node capacity, so each block ends up ≈ one cluster.
//! - [`ClusterMode::Bfs`]    — reserved (per-bucket BFS); not yet implemented.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};

use graph_format::blockfile::{BlockFileReader, BlockFileWriter};
use graph_format::extsort::{ExtSorter, SortRecord};
use graph_format::wire::{read_uvarint, write_uvarint};

const UNASSIGNED: u32 = u32::MAX;
const ADJ_BLOCK_BYTES: usize = 256 * 1024;

/// How node ids are reordered for on-disk locality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ClusterMode {
    /// Identity order — final id = provisional id, no reorder.
    None,
    /// Streaming Linear-Deterministic-Greedy graph partitioning (default).
    Ldg,
    /// Per-bucket BFS ordering (reserved, not yet implemented).
    Bfs,
}

/// Tunables for one clustering run.
pub struct ClusterParams {
    pub mode: ClusterMode,
    pub passes: u32,
    /// Target node records per partition (≈ one output block's worth).
    pub block_capacity: u32,
    /// Memory budget for the adjacency external sort.
    pub mem_budget: usize,
    pub temp_dir: PathBuf,
    pub zstd_level: i32,
}

/// A `prov_node_id → final_node_id` bijection on `0..node_count`.
pub enum Permutation {
    /// Identity — final id equals provisional id (no reorder).
    Identity,
    /// Explicit table; `final_of_prov[prov] = final`.
    Table(Vec<u32>),
}

impl Permutation {
    #[inline]
    pub fn final_of(&self, prov: u64) -> u64 {
        match self {
            Permutation::Identity => prov,
            Permutation::Table(v) => v[prov as usize] as u64,
        }
    }

    /// True when no reorder happens — lets emit take the zero-sort streaming path.
    pub fn is_identity(&self) -> bool {
        matches!(self, Permutation::Identity)
    }

    /// Borrow the explicit table, if any (for persisting on resume).
    pub fn table(&self) -> Option<&[u32]> {
        match self {
            Permutation::Table(v) => Some(v),
            Permutation::Identity => None,
        }
    }
}

/// One half-edge in the undirected clustering adjacency, sorted by `node`.
struct AdjPair {
    node: u64,
    nbr: u64,
}

impl SortRecord for AdjPair {
    fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.node);
        write_uvarint(buf, self.nbr);
    }
    fn decode(r: &mut &[u8]) -> Result<Self> {
        let node = read_uvarint(r)?;
        let nbr = read_uvarint(r)?;
        Ok(AdjPair { node, nbr })
    }
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.node.cmp(&other.node).then(self.nbr.cmp(&other.nbr))
    }
    fn size_hint(&self) -> usize {
        16
    }
}

/// Compute the node-id permutation. `scan_edges` streams every directed edge as a
/// `(src_prov, dst_prov)` pair; it is invoked once (to build the undirected
/// adjacency). The graph itself is never held resident.
pub fn build_permutation<E>(
    node_count: u64,
    params: &ClusterParams,
    scan_edges: E,
) -> Result<Permutation>
where
    E: Fn(&mut dyn FnMut(u64, u64) -> Result<()>) -> Result<()>,
{
    match params.mode {
        ClusterMode::None => return Ok(Permutation::Identity),
        ClusterMode::Bfs => bail!("--cluster=bfs is not yet implemented; use 'ldg' or 'none'"),
        ClusterMode::Ldg => {}
    }
    if node_count == 0 {
        return Ok(Permutation::Table(Vec::new()));
    }
    if node_count > u32::MAX as u64 {
        bail!(
            "ldg clustering addresses at most {} nodes (got {node_count}); use --cluster=none",
            u32::MAX
        );
    }
    let n = node_count as usize;
    // Budget guard: the node→partition map is the one large resident.
    let part_bytes = (n as u128) * 4;
    if part_bytes > params.mem_budget as u128 {
        bail!(
            "ldg node→partition map needs {} MiB which exceeds the build memory budget; \
             use --cluster=none or raise --max-memory",
            part_bytes / (1024 * 1024)
        );
    }

    let cap = params.block_capacity.max(1) as u64;
    let p = node_count.div_ceil(cap).max(1) as usize;

    // 1) Build the sorted undirected adjacency on disk (each edge → both directions,
    //    self-loops dropped — they carry no proximity signal).
    let adj_path = params
        .temp_dir
        .join(format!("slater_cluster_adj_{}.blk", std::process::id()));
    {
        let mut sorter =
            ExtSorter::<AdjPair>::new(&params.temp_dir, params.mem_budget, params.zstd_level)?;
        scan_edges(&mut |s, d| {
            if s != d {
                sorter.push(AdjPair { node: s, nbr: d })?;
                sorter.push(AdjPair { node: d, nbr: s })?;
            }
            Ok(())
        })?;
        let mut w = BlockFileWriter::create(&adj_path, ADJ_BLOCK_BYTES, params.zstd_level)?;
        let mut buf = Vec::new();
        for rec in sorter.sorted()? {
            let a = rec?;
            buf.clear();
            a.encode(&mut buf);
            w.append_record(&buf)?;
        }
        w.finish()?;
    }

    // 2) LDG refinement passes over the (reusable) sorted adjacency.
    let mut part_of = vec![UNASSIGNED; n];
    let mut load = vec![0u32; p];
    let cap_f = (node_count as f64 / p as f64).max(1.0);
    let result = (|| -> Result<()> {
        for _ in 0..params.passes.max(1) {
            ldg_pass(&adj_path, &mut part_of, &mut load, cap_f)?;
        }
        Ok(())
    })();
    let _ = std::fs::remove_file(&adj_path);
    result?;

    // 3) Place any node with no edges (never seen in the adjacency) for balance.
    let mut rr = 0usize;
    for v in 0..n {
        if part_of[v] == UNASSIGNED {
            let pp = rr;
            rr = (rr + 1) % p;
            part_of[v] = pp as u32;
            load[pp] += 1;
        }
    }

    Ok(to_permutation(part_of, &load))
}

/// One LDG sweep over the sorted adjacency: process nodes in id order, placing each
/// into the partition holding most of its already-placed neighbours (penalised by
/// partition load for balance). Re-runs re-place every node against the latest state.
fn ldg_pass(adj_path: &Path, part_of: &mut [u32], load: &mut [u32], cap_f: f64) -> Result<()> {
    let r = BlockFileReader::open(adj_path)?;
    let p_count = load.len();
    let mut cur: Option<u64> = None;
    let mut tally: HashMap<u32, u32> = HashMap::new();
    let mut rr = 0usize;
    r.for_each_record(|_, rec| {
        let mut s = rec;
        let a = AdjPair::decode(&mut s)?;
        match cur {
            Some(v) if v == a.node => {}
            Some(v) => {
                place_node(v, &tally, part_of, load, cap_f, &mut rr, p_count);
                tally.clear();
            }
            None => {}
        }
        cur = Some(a.node);
        let np = part_of[a.nbr as usize];
        if np != UNASSIGNED {
            *tally.entry(np).or_insert(0) += 1;
        }
        Ok(())
    })?;
    if let Some(v) = cur {
        place_node(v, &tally, part_of, load, cap_f, &mut rr, p_count);
    }
    Ok(())
}

/// Assign node `v` to its best partition, updating `load`. `rr` round-robins the
/// no-overlap fallback so balance is kept without an `O(P)` least-loaded scan.
fn place_node(
    v: u64,
    tally: &HashMap<u32, u32>,
    part_of: &mut [u32],
    load: &mut [u32],
    cap_f: f64,
    rr: &mut usize,
    p_count: usize,
) {
    let old = part_of[v as usize];
    if old != UNASSIGNED {
        load[old as usize] -= 1;
    }
    // Baseline: a rotating partition (overlap 0 ⇒ score 0).
    let mut best_p = *rr;
    *rr = (*rr + 1) % p_count;
    let mut best_score = match tally.get(&(best_p as u32)) {
        Some(&c) => (c as f64) * (1.0 - load[best_p] as f64 / cap_f),
        None => 0.0,
    };
    for (&pp, &cnt) in tally.iter() {
        let score = (cnt as f64) * (1.0 - load[pp as usize] as f64 / cap_f);
        if score > best_score || (score == best_score && (pp as usize) < best_p) {
            best_score = score;
            best_p = pp as usize;
        }
    }
    part_of[v as usize] = best_p as u32;
    load[best_p] += 1;
}

/// Lay partitions out consecutively (ascending partition id; ascending prov id
/// within each) and rewrite `part_of` in place into the final-id table.
fn to_permutation(mut part_of: Vec<u32>, load: &[u32]) -> Permutation {
    let mut offset = vec![0u64; load.len()];
    let mut acc = 0u64;
    for (i, &l) in load.iter().enumerate() {
        offset[i] = acc;
        acc += l as u64;
    }
    for slot in part_of.iter_mut() {
        let pp = *slot as usize;
        *slot = offset[pp] as u32;
        offset[pp] += 1;
    }
    Permutation::Table(part_of)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(mode: ClusterMode, cap: u32, dir: &Path) -> ClusterParams {
        ClusterParams {
            mode,
            passes: 4,
            block_capacity: cap,
            mem_budget: 1 << 28,
            temp_dir: dir.to_path_buf(),
            zstd_level: 1,
        }
    }

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("slater_cluster_{}_{}", std::process::id(), name))
    }

    /// Cross-block cut: number of edges whose endpoints land in different blocks
    /// (block = final_id / block_capacity).
    fn cut(edges: &[(u64, u64)], perm: &Permutation, cap: u64) -> usize {
        edges
            .iter()
            .filter(|(a, b)| perm.final_of(*a) / cap != perm.final_of(*b) / cap)
            .count()
    }

    /// 4 disjoint cliques of 50, with provisional ids interleaved across cliques so
    /// identity order scatters each clique across every block.
    fn community_graph() -> (u64, Vec<(u64, u64)>) {
        let k = 4u64;
        let per = 50u64;
        let n = k * per;
        // node id = community * per + member, but assign ids interleaved:
        // prov id i belongs to community i % k.
        let community_of = |i: u64| i % k;
        let mut members: Vec<Vec<u64>> = vec![Vec::new(); k as usize];
        for i in 0..n {
            members[community_of(i) as usize].push(i);
        }
        let mut edges = Vec::new();
        for m in &members {
            for a in 0..m.len() {
                for b in (a + 1)..m.len() {
                    edges.push((m[a], m[b]));
                }
            }
        }
        (n, edges)
    }

    #[test]
    fn ldg_permutation_is_a_bijection_and_deterministic() {
        let dir = tmp("bij");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (n, edges) = community_graph();
        let scan = |visit: &mut dyn FnMut(u64, u64) -> Result<()>| {
            for &(a, b) in &edges {
                visit(a, b)?;
            }
            Ok(())
        };
        let p1 = build_permutation(n, &params(ClusterMode::Ldg, 50, &dir), scan).unwrap();
        let p2 = build_permutation(n, &params(ClusterMode::Ldg, 50, &dir), scan).unwrap();

        // Bijection on 0..n.
        let mut seen = vec![false; n as usize];
        for prov in 0..n {
            let f = p1.final_of(prov);
            assert!(f < n, "final id {f} out of range");
            assert!(!seen[f as usize], "final id {f} assigned twice");
            seen[f as usize] = true;
        }
        // Deterministic.
        for prov in 0..n {
            assert_eq!(p1.final_of(prov), p2.final_of(prov));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ldg_reduces_cross_block_cut_versus_identity() {
        let dir = tmp("cut");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (n, edges) = community_graph();
        let scan = |visit: &mut dyn FnMut(u64, u64) -> Result<()>| {
            for &(a, b) in &edges {
                visit(a, b)?;
            }
            Ok(())
        };
        let cap = 50u64;
        let none =
            build_permutation(n, &params(ClusterMode::None, cap as u32, &dir), scan).unwrap();
        let ldg = build_permutation(n, &params(ClusterMode::Ldg, cap as u32, &dir), scan).unwrap();

        let none_cut = cut(&edges, &none, cap);
        let ldg_cut = cut(&edges, &ldg, cap);
        // Identity order scatters the interleaved cliques → many cross-block edges.
        assert!(none_cut > 0, "expected the interleaved layout to cut edges");
        // LDG groups each clique → strictly fewer (here, near-zero) cross-block edges.
        assert!(
            ldg_cut < none_cut,
            "ldg cut {ldg_cut} not better than identity cut {none_cut}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn none_mode_is_identity() {
        let dir = tmp("none");
        let scan = |_: &mut dyn FnMut(u64, u64) -> Result<()>| Ok(());
        let p = build_permutation(10, &params(ClusterMode::None, 4, &dir), scan).unwrap();
        assert!(p.is_identity());
        for i in 0..10 {
            assert_eq!(p.final_of(i), i);
        }
    }
}
