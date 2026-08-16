// SPDX-License-Identifier: Apache-2.0
//! `vector_ladder` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── the write ladder's vector levels (HIK-111) ────────────────────────────────────────

/// `SET n.embedding = vecf32([…])` for a `:Doc` fixture node, as Cypher text.
fn set_embedding(name: &str, v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
    format!(
        "MATCH (n:Doc {{name:'{name}'}}) SET n.embedding = vecf32([{}])",
        parts.join(", ")
    )
}

/// **The hazard.** Node 7 is embedded at three different levels at once: the sealed base
/// holds its original vector, a core **segment** re-embedded it, and the **delta**
/// re-embedded it again. Only the delta's vector is live; the other two are stale copies
/// of the same node id.
///
/// `merge_topk` deliberately does not dedup by node id, so a stale copy that survives its
/// level's scan does not merely misorder the results — it takes one of the `k` slots and
/// **evicts a live candidate**, and the k-th neighbour goes missing. No error, no panic,
/// no log line. The numbers are chosen so that both stale copies are *closer* to the query
/// than the live one (0.0 and 0.1 vs 0.5): a stale entry that is farther away can never win
/// a slot, so it would prove nothing.
///
/// Truth, computed by hand from the effective newest-wins vector set
/// {d00: 0.2, d01: 0.3, d02: 0.55, d07: **0.5**, …}: the top-4 is
/// `[d00 0.2, d01 0.3, d07 0.5, d02 0.55]`.
///
/// Suppress the base with the global set but let the *segment's* copy through (one flat
/// overlay, no per-level suppression) and you get `[d07 0.1, d00 0.2, d01 0.3, d07 0.5]` —
/// node 7 twice, and the live d02 evicted off the end of k.
#[test]
fn knn_suppresses_a_stale_vector_at_every_level_it_lives_at() {
    let base: Vec<Vec<f32>> = [0.2, 0.3, 0.55, 0.9, 0.95, 1.0, 1.05, 0.0]
        .iter()
        .map(|d| at_distance(*d))
        .collect();
    let (root, graph) = testgen::write_vector_docs("vec_levels_hazard", &base);
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // A segment re-embeds node 7 — stale, but *closer* to the query than the truth.
    vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(0.1)));
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the re-embed flushes into a segment");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 1);

    // The delta re-embeds it again. This is the live vector.
    vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(0.5)));

    let got = vknn(&graphs, &graph, &cache, &VQ, 4);
    let want = [(0u64, 0.2f64), (1, 0.3), (7, 0.5), (2, 0.55)];
    assert_eq!(
        got.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        want.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        "the top-4 over the effective (newest-wins) vector set; got {got:?}"
    );
    for ((gid, gs), (wid, ws)) in got.iter().zip(&want) {
        assert_eq!(gid, wid);
        assert!(
            (gs - ws).abs() < 1e-5,
            "node {gid} should score {ws}, got {gs}"
        );
    }
    assert_eq!(
        got.iter().filter(|(id, _)| *id == 7).count(),
        1,
        "node 7 must appear exactly once — it is embedded at three levels and only the \
             delta's vector is live; got {got:?}"
    );
    assert!(
        got.iter().any(|(id, _)| *id == 2),
        "the k-th live neighbour (d02) must still be there — a stale duplicate that reaches \
             the merge evicts it, silently; got {got:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The hazard extended to **three core segment levels** (HIK-113): node 7 is re-embedded in
/// three successive flushes, so three different segments each hold a stale-or-live vector for
/// the same node id. The per-segment fold's `superseded_above` must suppress the two older
/// copies in their own scans (each older segment sees a newer one that touched node 7), so
/// node 7 reaches the merge from exactly the newest segment and only once. If any older
/// level leaks its copy, it takes a `k` slot and the k-th live neighbour (d02) vanishes.
///
/// The stale copies (0.05, 0.1) and the base copy (0.0) are all *closer* to the query than
/// the live one (0.5): a farther stale copy could never win a slot and would prove nothing.
#[test]
fn knn_suppresses_a_stale_vector_across_three_segment_levels() {
    let base: Vec<Vec<f32>> = [0.2, 0.3, 0.55, 0.9, 0.95, 1.0, 1.05, 0.0]
        .iter()
        .map(|d| at_distance(*d))
        .collect();
    let (root, graph) = testgen::write_vector_docs("vec_three_seg_hazard", &base);
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // Three flushes, each re-embedding node 7 to a different vector — three segment levels.
    for d in [0.05, 0.1, 0.5] {
        vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(d)));
        graphs
            .flush_graph_to_segment(&graph, &vc, &root)
            .unwrap()
            .expect("each re-embed flushes into its own segment");
    }
    assert_eq!(
        graphs.get(&graph).unwrap().stack().segments().len(),
        3,
        "three flushes ⇒ three segment levels"
    );

    let got = vknn(&graphs, &graph, &cache, &VQ, 4);
    let want_ids = [0u64, 1, 7, 2];
    assert_eq!(
        got.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        want_ids.to_vec(),
        "top-4 over the newest-wins set {{d00:0.2, d01:0.3, d07:0.5, d02:0.55}}; got {got:?}"
    );
    let seven = got.iter().find(|(id, _)| *id == 7).unwrap();
    assert!(
        (seven.1 - 0.5).abs() < 1e-5,
        "node 7 must score the NEWEST segment's 0.5, not an older segment's 0.05/0.1 nor the \
             base's 0.0; got {}",
        seven.1
    );
    assert_eq!(
        got.iter().filter(|(id, _)| *id == 7).count(),
        1,
        "node 7 is embedded at three segment levels + the base; only the newest may emit it \
             — a duplicate means an older level failed to suppress; got {got:?}"
    );
    assert!(
        got.iter().any(|(id, _)| *id == 2),
        "the k-th live neighbour (d02) must survive — a leaked stale copy would evict it; \
             got {got:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The same three-level stack, seen by the **other** consumer of the fold: the binary
/// consolidation dump. If the dump and the KNN path disagree about which level wins, a
/// vector goes missing on consolidation — and only on consolidation, where nothing is
/// looking. So the dump must carry exactly one vector per node, the newest one, and a
/// removal must stay removed.
#[test]
fn the_consolidation_dump_carries_one_vector_per_node_newest_wins() {
    let base: Vec<Vec<f32>> = [0.2, 0.3, 0.55, 0.9, 0.95, 1.0, 1.05, 0.0]
        .iter()
        .map(|d| at_distance(*d))
        .collect();
    let (root, graph) = testgen::write_vector_docs("vec_levels_dump", &base);
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    // Segment: re-embed node 7, and remove node 3's embedding.
    vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(0.1)));
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Doc {name:'d03'}) REMOVE n.embedding",
    );
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the writes flush into a segment");
    // Delta: re-embed node 7 again (superseding the segment's copy), and remove node 4's.
    vwrite(&graphs, &graph, &set_embedding("d07", &at_distance(0.5)));
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Doc {name:'d04'}) REMOVE n.embedding",
    );

    let gen = graphs.get(&graph).unwrap();
    let snap = DeltaSnapshot::from_memtable(graphs.writer(&graph).unwrap().snapshot());
    let view = MergedView::new(gen.as_ref(), snap);
    let dump = root.join("_dump");
    std::fs::create_dir_all(&dump).unwrap();
    crate::consolidate::serialise_binary_dump(&Engine::new(&view, &cache), &view, &dump, None)
        .unwrap();

    let reader = graph_format::consolidate_dump::DumpReader::open(&dump, None).unwrap();
    let mut dumped: Vec<(u64, Vec<f32>)> = Vec::new();
    reader
        .for_each_vector(|node_id, _key_id, v| {
            dumped.push((node_id, v.to_vec()));
            Ok(())
        })
        .unwrap();
    dumped.sort_by_key(|(id, _)| *id);

    let ids: Vec<u64> = dumped.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        ids,
        vec![0, 1, 2, 5, 6, 7],
        "one vector per node with a live embedding: 3 and 4 were removed (at different \
             levels), and every other node keeps exactly one — got {ids:?}"
    );
    let seven = &dumped.iter().find(|(id, _)| *id == 7).unwrap().1;
    assert_eq!(
        seven,
        &at_distance(0.5),
        "node 7's dumped vector must be the delta's (the newest level), not the segment's \
             stale copy nor the base's"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A **randomised** cross-level property test: the merged top-k must *equal* an exact
/// brute force over the effective live vector set — not approximate it, not recall it.
///
/// The truth is derived from the write script the test itself issues (base vectors, then
/// each round's re-embeds / removals / node deletes replayed into a plain map), never read
/// off a second implementation of the fold. The base index is `AnnMode::BruteForce`, so
/// both sides are exact and the assertion is equality of ids *and* scores.
#[test]
fn knn_across_levels_equals_a_brute_force_over_the_live_set() {
    // Deterministic PRNG (the fixture path takes no `rand` dependency).
    fn next(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }
    fn unit_vec(state: &mut u64) -> Vec<f32> {
        let mut v: Vec<f32> = (0..3)
            .map(|_| (next(state) % 2000) as f32 / 1000.0 - 1.0)
            .collect();
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if n < 1e-3 {
            return vec![1.0, 0.0, 0.0];
        }
        for x in &mut v {
            *x /= n;
        }
        v
    }

    const N: usize = 12;
    for seed in 0..8u64 {
        let st = &mut (0x9E37_79B9_7F4A_7C15u64 ^ seed.wrapping_mul(0x2545_F491_4F6C_DD1D));
        let base: Vec<Vec<f32>> = (0..N).map(|_| unit_vec(st)).collect();
        let (root, graph) = testgen::write_vector_docs(&format!("vec_levels_prop_{seed}"), &base);
        let wal = root.join("_wal");
        let cache = BlockCache::new(1 << 20);
        let vc = VectorIndexCache::new(1 << 20);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&delta_cfg(&wal), &root, None)
            .unwrap();

        // The independently-derived truth: the effective live vector set, replayed from
        // the very statements the test issues.
        let mut live: HashMap<u64, Vec<f32>> = base
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, v)| (i as u64, v))
            .collect();
        let mut deleted: HashSet<u64> = HashSet::new();

        // 0–3 core segments, then a final round that stays in the delta.
        let segments = (next(st) % 4) as usize;
        for round in 0..=segments {
            for id in 0..N as u64 {
                if deleted.contains(&id) || !next(st).is_multiple_of(3) {
                    continue; // ~1 node in 3 is touched per round
                }
                let name = format!("d{id:02}");
                match next(st) % 8 {
                    0 => {
                        vwrite(
                            &graphs,
                            &graph,
                            &format!("MATCH (n:Doc {{name:'{name}'}}) DELETE n"),
                        );
                        deleted.insert(id);
                        live.remove(&id);
                    }
                    1 | 2 => {
                        vwrite(
                            &graphs,
                            &graph,
                            &format!("MATCH (n:Doc {{name:'{name}'}}) REMOVE n.embedding"),
                        );
                        live.remove(&id);
                    }
                    _ => {
                        let v = unit_vec(st);
                        embed_param(&graphs, &graph, &name, &v);
                        live.insert(id, v);
                    }
                }
            }
            // Every round but the last is flushed down into a core segment; the last one
            // stays in the write delta, so the query sees all three tiers at once.
            if round < segments {
                graphs.flush_graph_to_segment(&graph, &vc, &root).unwrap();
            }
        }

        for _ in 0..3 {
            let q = unit_vec(st);
            let k = 1 + (next(st) % 6) as usize;
            // Exact brute force over the live set, in the engine's total order (D26:
            // distance ascending, node id ascending on a tie).
            let mut want: Vec<(u64, f64)> = live
                .iter()
                .map(|(id, v)| (*id, 1.0 - crate::vector::cosine_similarity(&q, v)))
                .collect();
            want.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            want.truncate(k);

            let got = vknn(&graphs, &graph, &cache, &q, k);
            assert_eq!(
                got.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                want.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                "seed {seed}, {segments} segment(s), k={k}: the merged top-k must equal the \
                     brute force over the live set\n  got  {got:?}\n  want {want:?}"
            );
            for ((_, gs), (_, ws)) in got.iter().zip(&want) {
                assert!((gs - ws).abs() < 1e-5, "score {gs} vs {ws}");
            }
        }
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Overwriting an indexed embedding with a **non-vector** value takes the node out of the
/// index. The write path admits it (`validate_vector_dims` only constrains a
/// `Value::Vector`), and the newest level then says this node has no embedding — so it has
/// none. Leaving the level below to go on scoring its stale vector is exactly the silent
/// wrongness a removal exists to prevent, at either level.
#[test]
fn a_non_vector_overwrite_takes_the_node_out_of_the_index() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs("vec_levels_scalar", &base);
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();

    let ids = |g: &Graphs| -> Vec<u64> {
        vknn(g, &graph, &cache, &VQ, 3)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };
    assert_eq!(ids(&graphs), vec![0, 1, 2], "all three start in the index");

    // In the delta: node 0's embedding becomes an integer.
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Doc {name:'d00'}) SET n.embedding = 5",
    );
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "the delta says node 0 has no embedding, so its stale base vector must not score"
    );

    // And through a flush, where the segment's *row* is what says so.
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the overwrite flushes");
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "…and it must still not score once the overwrite lives in a core segment"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// The flush must decide vector-index membership by the node's **effective label set** —
/// the same question the read fold asks — not by the label the write anchored on.
///
/// They differ on a multi-label node whose business key lives on a label other than the
/// index's, which is an ordinary shape: key on `(:Keyed {name})`, vector index on
/// `(:Doc {embedding})`. Ask the anchor label and the segment's `vec.meta` sidecar names
/// nobody, so the fold's candidate set never sees the node — and since the sidecar is the
/// *only* channel that can express either fact (D12: the row cannot), two writes are
/// silently undone by a background job:
///
/// * a **re-embed** reverts to the stale base vector at the flush;
/// * a **removal** resurfaces the vector the user deleted.
///
/// Both are invisible until someone queries — no error, no panic, no log line.
#[test]
fn a_flush_keys_vector_membership_on_the_effective_labels_not_the_anchor() {
    // Base: node 0 at 0.9, node 1 at 0.2, node 2 at 0.4. Every node is :Doc:Keyed.
    let base: Vec<Vec<f32>> = [0.9, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("vec_levels_anchor", &base, "Keyed");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let ids = |g: &Graphs| -> Vec<u64> {
        vknn(g, &graph, &cache, &VQ, 3)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };
    assert_eq!(ids(&graphs), vec![1, 2, 0], "base order: 0.2, 0.4, 0.9");

    // Re-embed node 0 at distance 0 — through the *Keyed* anchor, while the index is on Doc.
    let v = at_distance(0.0);
    let parts: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
    vwrite(
        &graphs,
        &graph,
        &format!(
            "MATCH (n:Keyed {{name:'d00'}}) SET n.embedding = vecf32([{}])",
            parts.join(", ")
        ),
    );
    assert_eq!(
        ids(&graphs),
        vec![0, 1, 2],
        "the re-embed leads from the delta"
    );
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the re-embed flushes");
    assert_eq!(
        ids(&graphs),
        vec![0, 1, 2],
        "…and must still lead once flushed — a write silently reverting to the stale base \
             vector at a background flush is the worst kind of wrong"
    );

    // The removal half, through the same anchor.
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Keyed {name:'d01'}) REMOVE n.embedding",
    );
    assert_eq!(ids(&graphs), vec![0, 2], "node 1 leaves the index");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the removal flushes");
    assert_eq!(
        ids(&graphs),
        vec![0, 2],
        "…and must stay gone — the flush must carry the removal, or the deleted embedding \
             resurfaces"
    );
    std::fs::remove_dir_all(&root).ok();
}

// ── HIK-116: a label removal takes a node out of that label's vector index ──────────────

/// Run a read query over the merged view and return its rows.
fn vread(graphs: &Graphs, graph: &str, cache: &BlockCache, q: &str) -> Vec<Vec<Val>> {
    let gen = graphs.get(graph).unwrap();
    let snap = DeltaSnapshot::from_memtable(graphs.writer(graph).unwrap().snapshot());
    let view = MergedView::new(gen.as_ref(), snap);
    let ast = parser::parse(q).unwrap();
    let rows = Engine::new(&view, cache).run(&ast).unwrap().rows;
    rows
}

/// `count(:Doc)` over the merged view.
fn doc_count(graphs: &Graphs, graph: &str, cache: &BlockCache) -> i64 {
    match vread(graphs, graph, cache, "MATCH (n:Doc) RETURN count(n) AS c")[0][0] {
        Val::Int(c) => c,
        ref o => panic!("count(:Doc) is not an int: {o:?}"),
    }
}

/// **The bug (HIK-116).** A vector index is scoped to a `(label, property)` pair. A write
/// that drops the label (`REMOVE n:Doc`) must take the node out of the `(:Doc, embedding)`
/// index — scope-symmetric with `SET n:Doc` admitting it — and keep it out across the whole
/// write ladder: delta → T2 flush → T3 merge → consolidation. It must not delete the
/// embedding *value*.
///
/// The node is `:Doc:Keyed`: the vector index is on `:Doc` but the business key (the write's
/// anchor) is on `:Keyed`, so the write drops the very label the index is scoped to while
/// still addressing the node. An indexed embedding is routed out of the row (D12), so a
/// flushed row that lost the label cannot *say* so — the removal rides an explicit channel
/// at each rung (the delta's `labels_removed`, then the segment sidecar, then the merged
/// sidecar, then the consolidation `superseded` set), exactly as a value removal does (D63).
/// Miss any one rung and the answer depends on *which level the node happens to live on*.
///
/// d00 is the query's exact match (cosine distance 0.0), so a resurfaced base vector does
/// not merely reorder the results — it leads them. The assertion bites at every rung.
#[test]
fn removing_the_index_label_evicts_a_node_across_the_whole_ladder() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4, 0.6]
        .iter()
        .map(|d| at_distance(*d))
        .collect();
    let (root, graph) =
        testgen::write_vector_docs_keyed("vec_label_removal_ladder", &base, "Keyed");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    let ids = |g: &Graphs| -> Vec<u64> {
        vknn(g, &graph, &cache, &VQ, 4)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };

    assert_eq!(
        ids(&graphs),
        vec![0, 1, 2, 3],
        "base order: 0.0, 0.2, 0.4, 0.6"
    );
    assert_eq!(doc_count(&graphs, &graph, &cache), 4);

    // Drop the :Doc label from d00 — anchored on :Keyed, the index is on :Doc. The value
    // is untouched.
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");

    // Rung 1 — the delta. count(:Doc) and labels(n) already drop (the symptom); the fix is
    // that KNN drops with them rather than leaving d00 the top hit at its stale base vector.
    assert_eq!(doc_count(&graphs, &graph, &cache), 3, "count(:Doc) dropped");
    assert!(
        !ids(&graphs).contains(&0),
        "d00 left the :Doc index at the delta; got {:?}",
        ids(&graphs)
    );
    assert_eq!(
        ids(&graphs),
        vec![1, 2, 3],
        "the other three :Doc nodes remain"
    );

    // Rung 2 — the T2 flush. The removal must ride the segment sidecar; the row cannot
    // express it (D12). Put a second, unrelated flush after it so a compaction has a run.
    // The business key is on :Keyed, so re-embeds anchor there, not on :Doc.
    let keyed_embed = |name: &str, v: &[f32]| {
        let parts: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
        format!(
            "MATCH (n:Keyed {{name:'{name}'}}) SET n.embedding = vecf32([{}])",
            parts.join(", ")
        )
    };
    vwrite(&graphs, &graph, &keyed_embed("d01", &at_distance(0.15)));
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the label removal + re-embed flush");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 1);
    vwrite(&graphs, &graph, &keyed_embed("d02", &at_distance(0.35)));
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("a second segment to fold");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 2);
    assert!(
        !ids(&graphs).contains(&0),
        "d00 must stay gone once flushed into a segment; got {:?}",
        ids(&graphs)
    );

    // Rung 3 — the T3 merge. Fold the two segments; the below-run removal must be carried
    // into the merged sidecar, or d00's base vector resurfaces the moment the segment that
    // suppressed it is folded away.
    graphs
        .compact_graph_segments(&graph, &vc, &root, 0, 2)
        .unwrap();
    assert_eq!(
        graphs.get(&graph).unwrap().stack().segments().len(),
        1,
        "the run folded into one segment"
    );
    assert!(
        !ids(&graphs).contains(&0),
        "d00 must stay gone across the merge; got {:?}",
        ids(&graphs)
    );

    // Rung 4 — the consolidation dump. This reads the level fold (not the raw sidecar union
    // the KNN read path uses), so a segment-level removal that the fold swallowed would
    // resurface *only here*. A node out of :Doc scope must not be indexed by the rebuild.
    let dumped = dump_vectors(&graphs, &graph, &cache, &root.join("_dump"));
    let dumped_ids: Vec<u64> = dumped.iter().map(|(id, _)| *id).collect();
    assert!(
        !dumped_ids.contains(&0),
        "d00 is out of :Doc scope, so the rebuild must not index it — got {dumped_ids:?}"
    );
    assert_eq!(
        dumped_ids,
        vec![1, 2, 3],
        "every still-:Doc node keeps exactly one vector; got {dumped_ids:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **The un-remove.** `REMOVE n:Doc` then `SET n:Doc` puts the node back in the index at its
/// *original* embedding — the value was never deleted, only the label scope changed. This is
/// how "the value is retained" is observable: a base-indexed embedding reads back `Null`
/// through `RETURN n.embedding` (D12 routes it out of the row), so the vector's survival is
/// shown by re-entering scope and finding the same base vector still there.
#[test]
fn re_adding_the_index_label_restores_the_original_vector() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("vec_label_unremove", &base, "Keyed");
    let wal = root.join("_wal");
    let cache = BlockCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let _ = &wal;

    let top =
        |g: &Graphs| -> Option<(u64, f64)> { vknn(g, &graph, &cache, &VQ, 3).first().copied() };
    assert_eq!(top(&graphs), Some((0, 0.0)), "d00 leads at distance 0.0");

    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");
    assert!(
        !vknn(&graphs, &graph, &cache, &VQ, 3)
            .iter()
            .any(|(id, _)| *id == 0),
        "d00 is out of the index while unlabelled"
    );

    // Put the label back. Nothing re-set the embedding, so the value that comes back is the
    // base one — d00 leads at distance 0.0 again.
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) SET n:Doc");
    let restored = top(&graphs).expect("d00 is back in the index");
    assert_eq!(restored.0, 0, "d00 back in the :Doc index");
    assert!(
        restored.1.abs() < 1e-5,
        "…at its original base vector (distance 0.0), got {}",
        restored.1
    );
    std::fs::remove_dir_all(&root).ok();
}

/// Removing an **unrelated** label must not evict a node from a different label's vector
/// index. The node is `:Doc:Keyed`; dropping `:Keyed` leaves it `:Doc`, so it stays in the
/// `(:Doc, embedding)` index — through the delta *and* through a flush (the flush must not
/// mistake an in-scope node for a removed one).
#[test]
fn removing_an_unrelated_label_keeps_the_node_in_the_index() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("vec_unrelated_label", &base, "Keyed");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let leads = |g: &Graphs| -> bool {
        vknn(g, &graph, &cache, &VQ, 3)
            .first()
            .is_some_and(|(id, s)| *id == 0 && s.abs() < 1e-5)
    };
    assert!(leads(&graphs), "d00 leads at distance 0.0");

    // Drop the non-index label. d00 is still :Doc.
    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Keyed {name:'d00'}) REMOVE n:Keyed",
    );
    assert!(
        leads(&graphs),
        "removing :Keyed must not evict d00 from the :Doc index (delta)"
    );
    assert_eq!(
        doc_count(&graphs, &graph, &cache),
        3,
        ":Doc membership is unchanged"
    );

    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the unrelated label drop flushes");
    assert!(
        leads(&graphs),
        "removing :Keyed must not evict d00 from the :Doc index (flushed)"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A node whose embedding is removed by **value** *and* whose index label is removed must be
/// gone (either reason suffices), and must not double-count or resurface across a flush. It
/// is a legal, if odd, combination and the two removal channels must compose cleanly.
#[test]
fn value_removal_and_label_removal_compose() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) = testgen::write_vector_docs_keyed("vec_value_and_label", &base, "Keyed");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let ids = |g: &Graphs| -> Vec<u64> {
        vknn(g, &graph, &cache, &VQ, 3)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    };
    assert_eq!(ids(&graphs), vec![0, 1, 2]);

    vwrite(
        &graphs,
        &graph,
        "MATCH (n:Keyed {name:'d00'}) REMOVE n.embedding",
    );
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "gone via both channels, exactly once"
    );

    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the combined removal flushes");
    assert_eq!(
        ids(&graphs),
        vec![1, 2],
        "still gone once flushed — the two removals must not resurface each other"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **The headline (HIK-118).** The un-remove across a **flush**: `REMOVE n:Doc` → flush →
/// `SET n:Doc` restores the node at its *original base vector*. HIK-116 made this work while
/// the removal lived in the delta; once the removal is flushed to a segment sidecar, the flat
/// removal could not tell a scope-removal (should un-suppress on re-label) from a value-removal
/// (permanent), so a re-label silently failed to restore the vector. The sidecar now records
/// the removal **kind**, so a `label_removal` un-suppresses when the node re-enters scope.
///
/// Truth is hand-derived: d00 is the query's exact match (cosine distance 0.0), so its return
/// is unambiguous — a leading hit at distance 0.0 *is* the original base vector (nothing
/// re-set the value; a base-indexed embedding reads `Null` via `RETURN`, so re-entering scope
/// is the only observable channel — D12/D64).
#[test]
fn re_adding_the_index_label_restores_the_vector_across_a_flush() {
    let base: Vec<Vec<f32>> = [0.0, 0.2, 0.4].iter().map(|d| at_distance(*d)).collect();
    let (root, graph) =
        testgen::write_vector_docs_keyed("vec_unremove_across_flush", &base, "Keyed");
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&root.join("_wal")), &root, None)
        .unwrap();
    let top =
        |g: &Graphs| -> Option<(u64, f64)> { vknn(g, &graph, &cache, &VQ, 3).first().copied() };

    assert_eq!(top(&graphs), Some((0, 0.0)), "d00 leads at distance 0.0");

    // Leave scope, then flush so the removal lands in a segment sidecar (the delta is retired).
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) REMOVE n:Doc");
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("the label removal flushes into a segment sidecar");
    assert_eq!(graphs.get(&graph).unwrap().stack().segments().len(), 1);
    assert!(
        !vknn(&graphs, &graph, &cache, &VQ, 3)
            .iter()
            .any(|(id, _)| *id == 0),
        "d00 is out of the :Doc index while unlabelled, and the removal is now flushed"
    );

    // Re-enter scope. The removal is a *label* removal in the sidecar, so re-adding the label
    // must un-suppress d00 and bring back its original base vector — the whole point of D65.
    vwrite(&graphs, &graph, "MATCH (n:Keyed {name:'d00'}) SET n:Doc");
    let restored = top(&graphs).expect("d00 is back in the :Doc index after a flushed un-remove");
    assert_eq!(restored.0, 0, "d00 back in the :Doc index across the flush");
    assert!(
        restored.1.abs() < 1e-5,
        "…at its original base vector (distance 0.0), got {}",
        restored.1
    );
    std::fs::remove_dir_all(&root).ok();
}
