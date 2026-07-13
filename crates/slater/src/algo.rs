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
//!
//! Every kernel is `O(V·E)` in the worst case (harmonic and betweenness run one BFS
//! per source), so each takes an [`Interrupt`] hook and calls it as it works: the
//! executor passes its deadline check, making a long-running `algo.*` call abortable
//! at `timeoutMs` instead of wedging the connection until it completes. The kernels
//! allocate only `O(V+E)` scratch over the *already-charged* view the executor built,
//! so the memory bound lives in [`Engine::build_view`](crate::exec) and the time
//! bound lives here.

use anyhow::Result;
use std::collections::{HashMap, VecDeque};

/// Cancellation hook called by every kernel as it works (see [`Ticker`]): the
/// executor passes its per-query deadline check, so an `Err` aborts the algorithm
/// mid-flight. `Fn`, not `FnMut`, so the caller can hold `&self`.
pub type Interrupt<'a> = &'a (dyn Fn() -> Result<()> + 'a);

/// Units of algorithm work (edges relaxed, nodes settled, ranks updated) between two
/// [`Interrupt`] calls. Small enough that a wedged kernel aborts promptly, large
/// enough that the clock read is amortised to nothing.
const INTERRUPT_STRIDE: u64 = 4096;

/// Amortises the [`Interrupt`] over units of work: fires on the very first `tick`
/// (so an already-elapsed deadline aborts even a tiny graph) and then once every
/// [`INTERRUPT_STRIDE`] units.
struct Ticker<'a> {
    interrupt: Interrupt<'a>,
    since: u64,
}

impl<'a> Ticker<'a> {
    fn new(interrupt: Interrupt<'a>) -> Self {
        Self {
            interrupt,
            since: 0,
        }
    }

    /// Account `work` units and run the interrupt when the stride is due.
    fn tick(&mut self, work: u64) -> Result<()> {
        if self.since == 0 || self.since >= INTERRUPT_STRIDE {
            (self.interrupt)()?;
            self.since = 0;
        }
        self.since = self.since.saturating_add(work.max(1));
        Ok(())
    }
}

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
/// `proc_wcc.c` (LAGraph `ConnectedComponents`). `interrupt` is called every
/// [`INTERRUPT_STRIDE`] edges.
pub fn wcc(n: usize, edges: &[(usize, usize)], interrupt: Interrupt<'_>) -> Result<Vec<usize>> {
    let mut tick = Ticker::new(interrupt);
    let mut parent: Vec<usize> = (0..n).collect();
    for &(a, b) in edges {
        tick.tick(1)?;
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
    Ok((0..n).map(|i| uf_find(&mut parent, i)).collect())
}

/// PageRank over the directed graph `out` (out-adjacency per node). Damping
/// `d = 0.85`, uniform teleport, dangling nodes (no out-edges) redistribute their
/// rank uniformly so the scores always sum to 1.0. Iterates to an L1 delta below
/// `1e-9` or a pinned cap of 100 rounds. Ported from `proc_pagerank.c`. `interrupt`
/// is called every [`INTERRUPT_STRIDE`] rank updates.
pub fn pagerank(n: usize, out: &[Vec<usize>], interrupt: Interrupt<'_>) -> Result<Vec<f64>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let mut tick = Ticker::new(interrupt);
    let d = 0.85;
    let nf = n as f64;
    let teleport = (1.0 - d) / nf;
    let outdeg: Vec<usize> = out.iter().map(|o| o.len()).collect();
    let mut rank = vec![1.0 / nf; n];
    for _ in 0..100 {
        let dangling: f64 = (0..n).filter(|&i| outdeg[i] == 0).map(|i| rank[i]).sum();
        let mut next = vec![teleport + d * dangling / nf; n];
        for i in 0..n {
            tick.tick(1 + outdeg[i] as u64)?;
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
    Ok(rank)
}

/// Harmonic closeness centrality over the directed graph `out`: for each node, the
/// sum of `1/distance` over every other node reachable by directed BFS. Returns
/// `(score, reachable_count)` per node; a sink (no reachable nodes) scores exactly
/// `0.0`. Ported from `proc_harmonic_centrality.c` (computed exactly rather than via
/// HyperLogLog estimation — observationally identical on the relative orderings the
/// FalkorDB tests assert). `O(V·E)`: `interrupt` is called every
/// [`INTERRUPT_STRIDE`] settled nodes, so the whole sweep stays abortable.
pub fn harmonic(
    n: usize,
    out: &[Vec<usize>],
    interrupt: Interrupt<'_>,
) -> Result<Vec<(f64, usize)>> {
    let mut tick = Ticker::new(interrupt);
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
            tick.tick(1 + out[u].len() as u64)?;
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
    Ok(res)
}

/// Betweenness centrality via Brandes' algorithm (directed, unweighted, exact — all
/// nodes used as sources). FalkorDB's `samplingSize`/`samplingSeed` only approximate
/// the score on large graphs; here we always compute the full betweenness (a
/// superset that matches FalkorDB's exact relative orderings). Returns the raw
/// dependency sum per node; a node on no shortest path scores exactly `0.0`. Ported
/// from `proc_betweenness.c`. `O(V·E)`: `interrupt` is called every
/// [`INTERRUPT_STRIDE`] settled nodes, so the whole sweep stays abortable.
pub fn betweenness(n: usize, out: &[Vec<usize>], interrupt: Interrupt<'_>) -> Result<Vec<f64>> {
    let mut tick = Ticker::new(interrupt);
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
            tick.tick(1 + out[v].len() as u64)?;
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
    Ok(cb)
}

/// CDLP — community detection by **synchronous** label propagation over the
/// undirected adjacency `undir`. Each round, every node adopts the most frequent
/// label among its neighbours, ties broken by the smallest label (deterministic).
/// Stops on convergence or after `max_iter` rounds. Returns the final label index
/// per node (the caller maps each label group to a canonical community id). Ported
/// from `proc_cdlp.c`. `interrupt` is called every [`INTERRUPT_STRIDE`] node
/// updates.
pub fn cdlp(
    n: usize,
    undir: &[Vec<usize>],
    max_iter: usize,
    interrupt: Interrupt<'_>,
) -> Result<Vec<usize>> {
    let mut tick = Ticker::new(interrupt);
    let mut label: Vec<usize> = (0..n).collect();
    for _ in 0..max_iter {
        let mut next = label.clone();
        let mut changed = false;
        for u in 0..n {
            tick.tick(1 + undir[u].len() as u64)?;
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
    Ok(label)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Directed line 0→1→2→3 (out-adjacency).
    fn line_out() -> Vec<Vec<usize>> {
        vec![vec![1], vec![2], vec![3], vec![]]
    }

    /// The same line as symmetric adjacency and as directed edge pairs.
    fn line_undir() -> Vec<Vec<usize>> {
        vec![vec![1], vec![0, 2], vec![1, 3], vec![2]]
    }
    fn line_edges() -> Vec<(usize, usize)> {
        vec![(0, 1), (1, 2), (2, 3)]
    }

    fn ok() -> Result<()> {
        Ok(())
    }
    fn boom() -> Result<()> {
        anyhow::bail!("interrupted")
    }

    // HIK-88: every kernel must poll the interrupt as it works, so an aborting hook
    // (the executor's elapsed-deadline check) stops the O(V·E) sweep mid-flight.
    // Pre-fix the kernels took no interrupt and ran to completion uninterruptibly.
    #[test]
    fn kernels_abort_when_the_interrupt_errors() {
        assert!(wcc(4, &line_edges(), &boom).is_err(), "wcc");
        assert!(pagerank(4, &line_out(), &boom).is_err(), "pagerank");
        assert!(harmonic(4, &line_out(), &boom).is_err(), "harmonic");
        assert!(betweenness(4, &line_out(), &boom).is_err(), "betweenness");
        assert!(cdlp(4, &line_undir(), 10, &boom).is_err(), "cdlp");
    }

    // A no-op interrupt must not perturb the result: the kernels compute exactly what
    // the pre-interrupt versions did.
    #[test]
    fn kernels_are_unchanged_under_a_noop_interrupt() {
        // WCC: the connected line is a single component.
        let roots = wcc(4, &line_edges(), &ok).unwrap();
        assert!(roots.iter().all(|&r| r == roots[0]));

        // PageRank: a proper distribution summing to 1.
        let ranks = pagerank(4, &line_out(), &ok).unwrap();
        assert!((ranks.iter().sum::<f64>() - 1.0).abs() < 1e-9);

        // Harmonic: node 0 reaches 3 nodes (1/1 + 1/2 + 1/3); the sink reaches none.
        let hc = harmonic(4, &line_out(), &ok).unwrap();
        assert_eq!(hc[3], (0.0, 0));
        assert_eq!(hc[0].1, 3);
        assert!((hc[0].0 - (1.0 + 0.5 + 1.0 / 3.0)).abs() < 1e-9);

        // Betweenness: on a line the interior nodes carry the through-traffic; the
        // endpoints lie on no interior shortest path.
        let cb = betweenness(4, &line_out(), &ok).unwrap();
        assert_eq!(cb[0], 0.0);
        assert!(cb[1] > 0.0 && cb[2] > 0.0);

        // CDLP is deterministic (fixed tie-breaks): the same labels on every run,
        // one per node.
        let comm = cdlp(4, &line_undir(), 10, &ok).unwrap();
        assert_eq!(comm.len(), 4);
        assert_eq!(comm, cdlp(4, &line_undir(), 10, &ok).unwrap());
    }
}
