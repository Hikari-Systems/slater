// SPDX-License-Identifier: Apache-2.0
//! `vector_delete` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Delete consolidation (FreshDiskANN S5) ────────────────────────────────
//
// A hole is navigable but never emitted, so every search that *expands* one pays a
// block read for a record that can never be returned — forever, and more of them as the
// dead fraction grows. `graph_format::vamana_delete` patches the holes out of the
// adjacency; afterwards no reachable node names one and the dead records cost **zero**
// query IO. These two tests are the ones that decide whether that is true.

/// The shape of one delete-consolidation probe: `n`, dim and the block size are held
/// fixed across every fixture so the IO numbers are comparable.
fn s5_fixture() -> testgen::VamanaFixture {
    testgen::VamanaFixture {
        n: 2000,
        dim: 32,
        r: 24,
        alpha: 1.2,
        pq_subspaces: 8,
        pq_bits: 8,
        // A handful of records per block. The block reads a query pays then track the
        // records it *expands*, which is the quantity a hole inflates. Fat blocks would
        // make the measurement vacuous: with the whole store in a few blocks, every walk
        // faults all of them whether it expands the holes or not.
        vector_block_size: 512,
    }
}

/// Run `queries` KNN queries against a Vamana generation and return `(block reads **per
/// query**, mean recall@k over the live set)`.
///
/// The vector cache is **fresh for each query** and its budget is far larger than the
/// store, so no block is ever evicted and none is inherited from the previous query:
/// `metrics().misses` is then exactly the number of distinct blocks *that one query*
/// faulted — the IO it pays. That is the quantity the DoD bounds, and a cache shared
/// across the run would not measure it (the union of blocks touched by 20 queries
/// saturates at "the whole store" long before the dead fraction can move it).
///
/// Ground truth is an exact brute force over the raw vectors of the nodes that are *not*
/// holed. (Independently derived; nothing here compares one implementation to another.)
fn s5_probe(
    root: &std::path::Path,
    graph: &str,
    raw: &[Vec<f32>],
    holed: &HashSet<u64>,
    queries: usize,
    k: usize,
) -> (f64, f64) {
    s5_probe_at(root, graph, raw, holed, queries, k, 96)
}

#[allow(clippy::too_many_arguments)]
fn s5_probe_at(
    root: &std::path::Path,
    graph: &str,
    raw: &[Vec<f32>],
    holed: &HashSet<u64>,
    queries: usize,
    k: usize,
    beam_width: usize,
) -> (f64, f64) {
    let gen = Generation::open(root, graph).unwrap();
    let block_cache = BlockCache::new(1 << 20);
    let (ord, pq_bytes) = {
        let vi = gen.vamana_index("Doc", "embedding").unwrap();
        (vi.ord, vi.pq.resident_bytes())
    };

    let mut recall_sum = 0.0f64;
    let mut misses = 0u64;
    for qi in 0..queries {
        let vec_cache = VectorIndexCache::new(pq_bytes + (64 << 20));
        vec_cache.pin(
            gen.uuid(),
            ord,
            gen.vamana_index("Doc", "embedding").unwrap().pq.clone(),
        );

        let mut q = raw[(qi * 97) % raw.len()].clone();
        q[0] += 0.05;

        let mut ranked: Vec<(f64, u64)> = raw
            .iter()
            .enumerate()
            .map(|(i, v)| (1.0 - vector::cosine_similarity(&q, v), i as u64))
            .collect();
        ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let truth_k: HashSet<u64> = ranked
            .iter()
            .filter(|(_, id)| !holed.contains(id))
            .take(k)
            .map(|(_, id)| *id)
            .collect();

        let mut params = HashMap::new();
        params.insert(
            "q".to_string(),
            Val::List(q.iter().map(|x| Val::Float(*x as f64)).collect()),
        );
        let engine = Engine::new(&gen, &block_cache)
            .with_vector_cache(&vec_cache, beam_width)
            .with_params(params);
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Doc', 'embedding', 10, $q) \
                 YIELD node, score RETURN id(node) AS id, score",
        )
        .unwrap();
        let res = engine.run(&ast).unwrap();
        let got: HashSet<u64> = res
            .rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(n) => n as u64,
                _ => panic!("id(node) should be an integer"),
            })
            .collect();
        for id in &got {
            assert!(!holed.contains(id), "a hole must never be emitted");
        }
        recall_sum += truth_k.iter().filter(|id| got.contains(id)).count() as f64 / k as f64;

        let m = vec_cache.metrics();
        assert_eq!(
            m.evictions, 0,
            "the budget must be big enough that a miss means a first touch, not a re-fault"
        );
        misses += m.misses;
    }
    (misses as f64 / queries as f64, recall_sum / queries as f64)
}

/// Hole every `step`-th node id.
fn s5_holed_set(n: usize, step: usize) -> HashSet<u64> {
    (0..n as u64).filter(|id| id % step as u64 == 0).collect()
}

/// The beam widths a query is tried at, ascending. `s5_io_at_recall` returns the IO at
/// the first one that clears the recall bar.
const S5_BEAMS: [usize; 6] = [16, 24, 32, 48, 64, 96];

/// The block reads a query must pay on this index to reach `target` recall@k **over the
/// live set** — i.e. the IO cost of *actually answering the query*, not of some fixed
/// beam width. Returns `(beam_width, block reads per query, recall)`.
fn s5_io_at_recall(
    root: &std::path::Path,
    graph: &str,
    raw: &[Vec<f32>],
    holed: &HashSet<u64>,
    queries: usize,
    k: usize,
    target: f64,
) -> (usize, f64, f64) {
    for beam in S5_BEAMS {
        let (io, recall) = s5_probe_at(root, graph, raw, holed, queries, k, beam);
        if recall >= target {
            return (beam, io, recall);
        }
    }
    panic!(
        "this index cannot reach recall {target} at any beam width up to {} — that is not \
             an IO regression, it is a broken graph",
        S5_BEAMS[S5_BEAMS.len() - 1]
    );
}

/// **The test that proves the slice works.** Query IO must not grow with the deleted
/// fraction.
///
/// # What "IO per query" has to mean here, and why the obvious measurement is vacuous
///
/// Measure `misses` at a **fixed beam width** and a lazily-deleted index costs *exactly*
/// the same IO as a healthy one — not approximately, exactly. It has to: tombstoning a
/// record rewrites the `.pq` id column and **nothing else**, so the `.vamana` is byte
/// identical, the PQ estimates are identical, and `beam_search` therefore walks the
/// identical nodes in the identical order. `emit` returns `None` for the holes and that
/// is the *whole* difference. (Measured: 18.6 / 33.0 / 62.1 / 89.4 block reads at beams
/// 16 / 32 / 64 / 96 — the same three significant figures at 0%, 50%, 67% and 80% dead.)
///
/// So a fixed-beam miss-count assertion would pass with **no consolidation whatsoever**.
/// It cannot fail, and a test that cannot fail is worse than none.
///
/// What a hole actually costs is a **beam slot**: it is expanded, it occupies one of the
/// `L` slots, and it returns nothing. Recall over the live set falls, and the only way to
/// get it back is to **widen the beam** — which is where the IO finally lands. So the
/// honest measure is **IO at iso-recall**: the block reads a query needs to reach the
/// same recall@10 over the live set. Measured that way the cost is exactly what the slice
/// claims, and it grows with the dead fraction:
///
/// ```text
///  dead    IO to reach recall@10 ≥ 0.8 over the live set
///          lazily deleted        delete-consolidated
///    0%       18.6                   18.6
///   50%       33.0                   17.8
///   67%       62.1                   17.6      ← 3.5× the IO, for the same answer
///   80%       62.1                   17.3
/// ```
#[test]
fn delete_consolidation_does_not_grow_query_io_with_the_dead_fraction() {
    let fix = s5_fixture();
    let (k, queries, target) = (10, 20, 0.8);

    // The IO baseline: a healthy index with nothing deleted.
    let (root0, graph0, raw0) = testgen::write_vamana("s5_io_clean", &fix);
    let (beam_clean, io_clean, _) =
        s5_io_at_recall(&root0, &graph0, &raw0, &HashSet::new(), queries, k, target);
    let _ = std::fs::remove_dir_all(&root0);

    // Two thirds deleted — well past the 20% consolidation trigger. First lazily (the
    // holes left in the adjacency, today's behaviour), then delete-consolidated. Same
    // vectors, same graph, same queries, same holes: the *only* difference is the pass.
    let holed: HashSet<u64> = (0..fix.n as u64)
        .filter(|id| !id.is_multiple_of(3))
        .collect();
    let h = holed.clone();
    let (root1, graph1, raw1, _) =
        testgen::write_vamana_holed("s5_io_lazy", &fix, move |id, _| h.contains(&id));
    let (beam_lazy, io_lazy, _) =
        s5_io_at_recall(&root1, &graph1, &raw1, &holed, queries, k, target);
    let _ = std::fs::remove_dir_all(&root1);

    let h = holed.clone();
    let (root2, graph2, raw2, _) =
        testgen::write_vamana_holed_consolidated("s5_io_done", &fix, move |id, _| h.contains(&id));
    assert_eq!(raw2, raw1, "the fixtures must differ only by the pass");
    let (beam_done, io_done, recall_done) =
        s5_io_at_recall(&root2, &graph2, &raw2, &holed, queries, k, target);
    let _ = std::fs::remove_dir_all(&root2);

    assert!(
        io_done <= io_clean * 1.1,
        "query IO GREW with the dead fraction: {io_done:.1} block reads per query (beam \
             {beam_done}) at 67% dead vs {io_clean:.1} (beam {beam_clean}) with nothing \
             deleted, for the same recall. The holes are still costing beam slots — the \
             consolidation did not patch them out of the adjacency."
    );
    assert!(
        io_done * 1.5 <= io_lazy,
        "the consolidation removed no IO: {io_done:.1} block reads per query (beam \
             {beam_done}) vs {io_lazy:.1} (beam {beam_lazy}) for the same index, the same \
             deletes and the same recall, with the holes left in the adjacency. A pass that \
             is functionally correct but does not reduce block reads has FAILED — that is the \
             entire point of the slice."
    );
    assert!(recall_done >= target);
}

/// Recall@10 over the **live** set, against an exact brute force over the live set, must
/// clear the 0.8 bar — and must not be materially below what the *same* live set gets
/// from an index with no deletes in it at all. Patching the dead nodes out of the
/// adjacency re-points their in-edges at what lay behind them (the splice); if that
/// splice were wrong, the graph would be quietly less navigable and recall would sag
/// even though every structural invariant still held.
#[test]
fn delete_consolidation_keeps_recall_over_the_live_set() {
    let fix = s5_fixture();
    let k = 10;
    let queries = 20;
    // A third of the index deleted, INCLUDING the medoid — the entry point of every
    // search. If the pass ever splices the medoid's own out-edges away, recall here is
    // not "a bit low", it is zero.
    let holed_ids = s5_holed_set(fix.n, 3);

    // The "before": the same index, the same graph, the same queries — with nothing
    // deleted, scored against the exact top-k of everything it holds. That is this
    // Vamana's intrinsic recall, and it is the bar the deleted-and-consolidated index
    // must still clear over *its* live set. (Scoring the undeleted index against the
    // live-set truth would be nonsense: it would be marked down for correctly returning
    // nodes that have not been deleted in it.)
    let (root0, graph0, raw0) = testgen::write_vamana("s5_recall_before", &fix);
    let (_, recall_before) = s5_probe(&root0, &graph0, &raw0, &HashSet::new(), queries, k);
    let _ = std::fs::remove_dir_all(&root0);

    let h = holed_ids.clone();
    let (root1, graph1, raw1, medoid_id) =
        testgen::write_vamana_holed_consolidated("s5_recall_after", &fix, move |id, is_medoid| {
            is_medoid || h.contains(&id)
        });
    assert_eq!(
        raw1, raw0,
        "the fixture must be deterministic across builds"
    );
    let mut holed = holed_ids.clone();
    holed.insert(medoid_id);
    let (_, recall_after) = s5_probe(&root1, &graph1, &raw1, &holed, queries, k);

    // The generation still opens — which is itself an assertion: `validate_vamana_index`
    // refuses an index whose medoid has no out-edges, so a pass that orphaned the entry
    // point could not have got this far. And the medoid really was holed.
    let gen = Generation::open(&root1, &graph1).unwrap();
    let vi = gen.vamana_index("Doc", "embedding").unwrap();
    assert!(vi.pq.is_hole(match gen.manifest().vector_indexes[0].mode {
        AnnMode::Vamana { medoid, .. } => medoid as usize,
        _ => panic!("expected a Vamana index"),
    }));
    drop(gen);
    let _ = std::fs::remove_dir_all(&root1);

    assert!(
        recall_after >= 0.8,
        "recall@{k} over the live set after the delete consolidation was \
             {recall_after:.3}, expected ≥ 0.8"
    );
    assert!(
        recall_after >= recall_before - 0.05,
        "the consolidation cost recall on the LIVE set: {recall_after:.3} after vs \
             {recall_before:.3} before the deletes. The splice is meant to preserve \
             navigability through the region the dead nodes bridged."
    );
}
