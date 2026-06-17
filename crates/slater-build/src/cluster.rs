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
/// Nodes per LDG **stripe** — the parallelism + restreaming unit. A fixed constant
/// (not derived from `--threads`) so the resulting permutation is identical
/// regardless of the worker count; `--threads` only sets how many stripes run at
/// once. Larger ⇒ more within-stripe live-greedy quality, fewer parallel units.
const STRIPE_NODES: u64 = 1 << 16;

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
    /// Worker cap for the parallel restreaming passes (does not affect the result).
    pub threads: usize,
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
    // Budget guard: the two node→partition maps (double-buffered for the parallel
    // restreaming passes) are the large residents.
    let part_bytes = (n as u128) * 8;
    if part_bytes > params.mem_budget as u128 {
        bail!(
            "ldg node→partition maps need {} MiB which exceeds the build memory budget; \
             use --cluster=none or raise --max-memory",
            part_bytes / (1024 * 1024)
        );
    }

    let cap = params.block_capacity.max(1) as u64;
    let p = node_count.div_ceil(cap).max(1) as usize;
    let nstripes = node_count.div_ceil(STRIPE_NODES).max(1) as usize;

    // 1) Build the sorted undirected adjacency, routed into per-stripe files (by node
    //    id). Each edge → both directions; self-loops carry no proximity signal.
    let stripe_adj = |s: usize| -> PathBuf {
        params.temp_dir.join(format!(
            "slater_cluster_adj_{}_{}.blk",
            std::process::id(),
            s
        ))
    };
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
        let mut writers: Vec<BlockFileWriter> = (0..nstripes)
            .map(|s| BlockFileWriter::create(stripe_adj(s), ADJ_BLOCK_BYTES, params.zstd_level))
            .collect::<Result<_>>()?;
        let mut buf = Vec::new();
        for rec in sorter.sorted()? {
            let a = rec?;
            let s = (a.node / STRIPE_NODES) as usize;
            buf.clear();
            a.encode(&mut buf);
            writers[s].append_record(&buf)?;
        }
        for w in writers {
            w.finish()?;
        }
    }

    // 2) Block-parallel restreaming LDG. Double-buffer the partition map: each pass
    //    reads the frozen previous assignment (so stripes are independent) and writes
    //    the next. Within a stripe, nodes are placed serially in id order using live
    //    in-stripe reads (full greedy locality) and the frozen previous assignment
    //    for cross-stripe neighbours. Deterministic regardless of worker count.
    let cap_f = (node_count as f64 / p as f64).max(1.0);
    // Seed empty (every node unassigned) so the greedy has room to pack clusters;
    // pass 0 places against partial state, later passes refine against the frozen
    // prior assignment.
    let mut part_prev: Vec<u32> = vec![UNASSIGNED; n];
    let mut part_next: Vec<u32> = vec![UNASSIGNED; n];

    let run = (|| -> Result<()> {
        for _ in 0..params.passes.max(1) {
            let mut load_prev = vec![0u32; p];
            for &pp in &part_prev {
                if pp != UNASSIGNED {
                    load_prev[pp as usize] += 1;
                }
            }
            let next_stripe = std::sync::atomic::AtomicU64::new(0);
            let err: std::sync::Mutex<Option<anyhow::Error>> = std::sync::Mutex::new(None);
            let part_next_ptr = SyncMutPtr(part_next.as_mut_ptr());
            let part_prev_r = &part_prev;
            let load_prev_r = &load_prev;
            let next_r = &next_stripe;
            let err_r = &err;
            std::thread::scope(|scope| {
                for _ in 0..params.threads.max(1) {
                    scope.spawn(|| {
                        let pp = &part_next_ptr;
                        loop {
                            if err_r.lock().unwrap().is_some() {
                                break;
                            }
                            let s = next_r.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if s as usize >= nstripes {
                                break;
                            }
                            let lo = s * STRIPE_NODES;
                            let hi = ((s + 1) * STRIPE_NODES).min(node_count);
                            if let Err(e) = ldg_stripe_pass(
                                &stripe_adj(s as usize),
                                lo,
                                hi,
                                part_prev_r,
                                load_prev_r,
                                cap_f,
                                p,
                                pp,
                            ) {
                                let mut g = err_r.lock().unwrap();
                                if g.is_none() {
                                    *g = Some(e);
                                }
                                break;
                            }
                        }
                    });
                }
            });
            if let Some(e) = err.into_inner().unwrap() {
                return Err(e);
            }
            std::mem::swap(&mut part_prev, &mut part_next);
        }
        Ok(())
    })();
    for s in 0..nstripes {
        let _ = std::fs::remove_file(stripe_adj(s));
    }
    run?;

    // Final assignment is in `part_prev` (post-swap). Place any still-unassigned
    // (edge-less) node round-robin for balance, then lay partitions out consecutively.
    let mut load = vec![0u32; p];
    let mut rr = 0usize;
    for slot in part_prev.iter_mut() {
        if *slot == UNASSIGNED {
            *slot = rr as u32;
            rr = (rr + 1) % p;
        }
        load[*slot as usize] += 1;
    }
    Ok(to_permutation(part_prev, &load))
}

/// `*mut u32` wrapper letting stripe workers write **disjoint** ranges of the shared
/// `part_next` in parallel (each stripe owns `[lo, hi)`; no two threads touch the
/// same slot, so the aliasing is sound).
struct SyncMutPtr(*mut u32);
unsafe impl Send for SyncMutPtr {}
unsafe impl Sync for SyncMutPtr {}

/// One restreaming-LDG sweep over a single stripe `[lo, hi)`. Reads neighbour
/// partitions from the live in-stripe chunk (for already-placed, lower-id, in-stripe
/// neighbours) or the frozen `part_prev` (everything else); writes the stripe's slots
/// of `part_next` via `ptr`. `local_load` starts from the frozen global histogram and
/// tracks this stripe's own moves so within-stripe balance stays live.
#[allow(clippy::too_many_arguments)]
fn ldg_stripe_pass(
    adj_path: &Path,
    lo: u64,
    hi: u64,
    part_prev: &[u32],
    load_prev: &[u32],
    cap_f: f64,
    p: usize,
    ptr: &SyncMutPtr,
) -> Result<()> {
    let len = (hi - lo) as usize;
    // SAFETY: stripes are disjoint and each is processed by exactly one worker, so
    // this `&mut` slice never overlaps another's.
    let chunk: &mut [u32] = unsafe { std::slice::from_raw_parts_mut(ptr.0.add(lo as usize), len) };
    // Unseen (edge-less) nodes keep their previous assignment (balanced seed).
    for (i, slot) in chunk.iter_mut().enumerate() {
        *slot = part_prev[lo as usize + i];
    }
    let mut local_load: Vec<u32> = load_prev.to_vec();

    let place = |v: u64, tally: &HashMap<u32, u32>, local_load: &mut [u32], chunk: &mut [u32]| {
        let prev = part_prev[v as usize];
        if prev != UNASSIGNED {
            local_load[prev as usize] -= 1;
        }
        // Deterministic no-overlap baseline (independent of processing order).
        let mut best_p = (v as usize) % p;
        let mut best_score = tally
            .get(&(best_p as u32))
            .map(|&c| c as f64 * (1.0 - local_load[best_p] as f64 / cap_f))
            .unwrap_or(0.0);
        for (&pp, &cnt) in tally.iter() {
            let score = cnt as f64 * (1.0 - local_load[pp as usize] as f64 / cap_f);
            if score > best_score || (score == best_score && (pp as usize) < best_p) {
                best_score = score;
                best_p = pp as usize;
            }
        }
        chunk[(v - lo) as usize] = best_p as u32;
        local_load[best_p] += 1;
    };

    let r = BlockFileReader::open(adj_path)?;
    let mut cur: Option<u64> = None;
    let mut tally: HashMap<u32, u32> = HashMap::new();
    r.for_each_record(|_, rec| {
        let mut s = rec;
        let a = AdjPair::decode(&mut s)?;
        match cur {
            Some(v) if v == a.node => {}
            Some(v) => {
                place(v, &tally, &mut local_load, chunk);
                tally.clear();
            }
            None => {}
        }
        cur = Some(a.node);
        // Live in-stripe read for already-placed lower-id neighbours; frozen otherwise.
        let np = if a.nbr >= lo && a.nbr < a.node {
            chunk[(a.nbr - lo) as usize]
        } else {
            part_prev[a.nbr as usize]
        };
        if np != UNASSIGNED {
            *tally.entry(np).or_insert(0) += 1;
        }
        Ok(())
    })?;
    if let Some(v) = cur {
        place(v, &tally, &mut local_load, chunk);
    }
    Ok(())
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
            threads: 4,
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
