//! Graph-algorithm procedures (`algo.*`, Phase 13).
//!
//! Pure functions over compact adjacency (0-based node indices). The executor
//! (`Engine::build_view`) materialises a filtered subgraph — selecting nodes by
//! label and edges by relationship type — into these index-based adjacency lists,
//! runs the algorithm here, then maps the indexed results back to `Val::Node`s.
//! Semantics are ported from FalkorDB's `proc_*.c` (the *observable* behaviour, not
//! the GraphBLAS/LAGraph implementation): `proc_wcc.c`, `proc_pagerank.c`,
//! `proc_harmonic_centrality.c`, `proc_betweenness.c`, `proc_cdlp.c`. All routines
//! are deterministic (fixed iteration caps + min-id / smallest-label tie-breaks) so
//! results are stable across runs.

use std::collections::{HashMap, VecDeque};

/// Union-find root of `x` with path halving.
fn uf_find(parent: &mut [usize], mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]];
        x = parent[x];
    }
    x
}

/// Weakly-connected components via union-find over the **undirected** view of the
/// directed `edges`. Returns, for each node index, a representative index shared by
/// exactly the nodes in its component (the value itself is arbitrary but stable —
/// the caller maps each group to a canonical component id). Ported from
/// `proc_wcc.c` (LAGraph `ConnectedComponents`).
pub fn wcc(n: usize, edges: &[(usize, usize)]) -> Vec<usize> {
    let mut parent: Vec<usize> = (0..n).collect();
    for &(a, b) in edges {
        let ra = uf_find(&mut parent, a);
        let rb = uf_find(&mut parent, b);
        if ra != rb {
            // union toward the lower index for a stable representative
            if ra < rb {
                parent[rb] = ra;
            } else {
                parent[ra] = rb;
            }
        }
    }
    (0..n).map(|i| uf_find(&mut parent, i)).collect()
}

/// PageRank over the directed graph `out` (out-adjacency per node). Damping
/// `d = 0.85`, uniform teleport, dangling nodes (no out-edges) redistribute their
/// rank uniformly so the scores always sum to 1.0. Iterates to an L1 delta below
/// `1e-9` or a pinned cap of 100 rounds. Ported from `proc_pagerank.c`.
pub fn pagerank(n: usize, out: &[Vec<usize>]) -> Vec<f64> {
    if n == 0 {
        return Vec::new();
    }
    let d = 0.85;
    let nf = n as f64;
    let teleport = (1.0 - d) / nf;
    let outdeg: Vec<usize> = out.iter().map(|o| o.len()).collect();
    let mut rank = vec![1.0 / nf; n];
    for _ in 0..100 {
        let dangling: f64 = (0..n).filter(|&i| outdeg[i] == 0).map(|i| rank[i]).sum();
        let mut next = vec![teleport + d * dangling / nf; n];
        for i in 0..n {
            if outdeg[i] == 0 {
                continue;
            }
            let share = d * rank[i] / outdeg[i] as f64;
            for &j in &out[i] {
                next[j] += share;
            }
        }
        let delta: f64 = (0..n).map(|i| (next[i] - rank[i]).abs()).sum();
        rank = next;
        if delta < 1e-9 {
            break;
        }
    }
    rank
}

/// Harmonic closeness centrality over the directed graph `out`: for each node, the
/// sum of `1/distance` over every other node reachable by directed BFS. Returns
/// `(score, reachable_count)` per node; a sink (no reachable nodes) scores exactly
/// `0.0`. Ported from `proc_harmonic_centrality.c` (computed exactly rather than via
/// HyperLogLog estimation — observationally identical on the relative orderings the
/// FalkorDB tests assert).
pub fn harmonic(n: usize, out: &[Vec<usize>]) -> Vec<(f64, usize)> {
    let mut res = Vec::with_capacity(n);
    let mut dist = vec![usize::MAX; n];
    for s in 0..n {
        dist[s] = 0;
        let mut q = VecDeque::new();
        q.push_back(s);
        let mut touched = vec![s];
        let mut score = 0.0;
        let mut reachable = 0usize;
        while let Some(u) = q.pop_front() {
            let du = dist[u];
            for &v in &out[u] {
                if dist[v] == usize::MAX {
                    dist[v] = du + 1;
                    touched.push(v);
                    score += 1.0 / (du + 1) as f64;
                    reachable += 1;
                    q.push_back(v);
                }
            }
        }
        res.push((score, reachable));
        for t in touched {
            dist[t] = usize::MAX;
        }
    }
    res
}

/// Betweenness centrality via Brandes' algorithm (directed, unweighted, exact — all
/// nodes used as sources). FalkorDB's `samplingSize`/`samplingSeed` only approximate
/// the score on large graphs; here we always compute the full betweenness (a
/// superset that matches FalkorDB's exact relative orderings). Returns the raw
/// dependency sum per node; a node on no shortest path scores exactly `0.0`. Ported
/// from `proc_betweenness.c`.
pub fn betweenness(n: usize, out: &[Vec<usize>]) -> Vec<f64> {
    let mut cb = vec![0.0; n];
    for s in 0..n {
        let mut stack: Vec<usize> = Vec::new();
        let mut preds: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut sigma = vec![0.0f64; n];
        let mut dist = vec![-1i64; n];
        sigma[s] = 1.0;
        dist[s] = 0;
        let mut q = VecDeque::new();
        q.push_back(s);
        while let Some(v) = q.pop_front() {
            stack.push(v);
            for &w in &out[v] {
                if dist[w] < 0 {
                    dist[w] = dist[v] + 1;
                    q.push_back(w);
                }
                if dist[w] == dist[v] + 1 {
                    sigma[w] += sigma[v];
                    preds[w].push(v);
                }
            }
        }
        let mut delta = vec![0.0f64; n];
        while let Some(w) = stack.pop() {
            for &v in &preds[w] {
                delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
            }
            if w != s {
                cb[w] += delta[w];
            }
        }
    }
    cb
}

/// CDLP — community detection by **synchronous** label propagation over the
/// undirected adjacency `undir`. Each round, every node adopts the most frequent
/// label among its neighbours, ties broken by the smallest label (deterministic).
/// Stops on convergence or after `max_iter` rounds. Returns the final label index
/// per node (the caller maps each label group to a canonical community id). Ported
/// from `proc_cdlp.c`.
pub fn cdlp(n: usize, undir: &[Vec<usize>], max_iter: usize) -> Vec<usize> {
    let mut label: Vec<usize> = (0..n).collect();
    for _ in 0..max_iter {
        let mut next = label.clone();
        let mut changed = false;
        for u in 0..n {
            if undir[u].is_empty() {
                continue;
            }
            let mut counts: HashMap<usize, usize> = HashMap::new();
            for &v in &undir[u] {
                *counts.entry(label[v]).or_insert(0) += 1;
            }
            // argmax by count, then smallest label — independent of map iteration order
            let mut best_label = label[u];
            let mut best_count = 0usize;
            for (&lbl, &c) in &counts {
                if c > best_count || (c == best_count && lbl < best_label) {
                    best_count = c;
                    best_label = lbl;
                }
            }
            if best_label != label[u] {
                changed = true;
            }
            next[u] = best_label;
        }
        label = next;
        if !changed {
            break;
        }
    }
    label
}
