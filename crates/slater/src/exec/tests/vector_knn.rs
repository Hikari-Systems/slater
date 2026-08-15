// SPDX-License-Identifier: Apache-2.0
//! `vector_knn` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Vector KNN (M5) ──────────────────────────────────────────────────────

/// The three Person embeddings in the fixture (see `testgen`), by node id.
const FIXTURE_VECS: [(u64, [f32; 3]); 3] = [
    (0, [0.1, 0.2, 0.3]), // Alice
    (1, [0.2, 0.1, 0.0]), // Bob
    (2, [0.9, 0.8, 0.7]), // Carol
];

/// Brute-force reference: cosine-distance to `query`, ascending, tie-break id.
fn reference_knn(query: &[f32], k: usize) -> Vec<(u64, f64)> {
    let mut r: Vec<(u64, f64)> = FIXTURE_VECS
        .iter()
        .map(|(id, v)| (*id, 1.0 - vector::cosine_similarity(query, v)))
        .collect();
    r.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    r.truncate(k);
    r
}

#[test]
fn vector_knn_returns_k_nearest_ordered_with_reference_scores() {
    // Query equals Alice's vector, so Alice (distance 0) is first, then Carol,
    // then Bob — exactly the brute-force reference order and scores.
    let (root, res) = run(
        "exec_knn_ref",
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node) AS id, score",
    );
    assert_eq!(res.columns, vec!["id", "score"]);
    let want = reference_knn(&[0.1, 0.2, 0.3], 2);
    assert_eq!(res.rows.len(), want.len());
    for (got, (wid, wscore)) in res.rows.iter().zip(&want) {
        let Val::Int(id) = got[0] else {
            panic!("id should be an integer, got {:?}", got[0]);
        };
        let Val::Float(score) = got[1] else {
            panic!("score should be a float, got {:?}", got[1]);
        };
        assert_eq!(id as u64, *wid);
        assert!(
            (score - wscore).abs() < 1e-6,
            "score {score} vs reference {wscore}"
        );
    }
    // First hit is the exact match: distance ~0.
    assert!(matches!(res.rows[0][0], Val::Int(0)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_with_pool_is_correct() {
    // A pool-configured engine returns the identical (id, score) kNN rows as the
    // sequential engine. The fixture group is tiny (below KNN_PAR_MIN), so this
    // pins the pool wiring + sequential-fallback path end to end; the `vector`
    // unit test exercises the rayon chunked read/score branch directly.
    let (root, graph, _) = testgen::write_basic("exec_knn_pool");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap(),
    );
    let q = "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.1, 0.2, 0.3])) \
                 YIELD node, score RETURN id(node) AS id, score";
    let res = Engine::new(&gen, &cache)
        .with_fanout_pool(Some(pool))
        .run(&parser::parse(q).unwrap())
        .expect("pool-configured kNN runs");
    let want = reference_knn(&[0.1, 0.2, 0.3], 3);
    assert_eq!(res.rows.len(), want.len());
    for (got, (wid, wscore)) in res.rows.iter().zip(&want) {
        let Val::Int(id) = got[0] else {
            panic!("id should be an integer, got {:?}", got[0]);
        };
        let Val::Float(score) = got[1] else {
            panic!("score should be a float, got {:?}", got[1]);
        };
        assert_eq!(id as u64, *wid);
        assert!(
            (score - wscore).abs() < 1e-6,
            "score {score} vs reference {wscore}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_yield_alias_and_node_projection() {
    // Carol's own vector → Carol is the single nearest neighbour; the yielded
    // node is a real Node we can project a property off.
    let (root, res) = run(
        "exec_knn_alias",
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 1, vecf32([0.9, 0.8, 0.7])) \
             YIELD node AS n, score AS s RETURN n.name AS name, s",
    );
    assert_eq!(res.columns, vec!["name", "s"]);
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Carol");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_yield_where_filters_rows() {
    // Ask for all three but keep only the (near-)exact match via YIELD ... WHERE.
    let (root, res) = run(
        "exec_knn_where",
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score WHERE score < 0.0001 RETURN id(node) AS id",
    );
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(0)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_unknown_index_is_an_error() {
    let (root, graph, _) = testgen::write_basic("exec_knn_noindex");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Company', 'embedding', 1, vecf32([0.1, 0.2, 0.3])) \
             YIELD node RETURN node",
    )
    .unwrap();
    let err = engine.run(&ast).err().unwrap();
    assert!(err.to_string().contains("no vector index"), "got: {err}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_dimension_mismatch_is_an_error() {
    let (root, graph, _) = testgen::write_basic("exec_knn_dim");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    // A 2-dim query against the 3-dim index.
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 1, vecf32([0.1, 0.2])) \
             YIELD node RETURN node",
    )
    .unwrap();
    let err = engine.run(&ast).err().unwrap();
    assert!(err.to_string().contains("dimension"), "got: {err}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_query_vector_from_parameter() {
    let (root, graph, _) = testgen::write_basic("exec_knn_param");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let mut params = HashMap::new();
    // A $param query vector arrives as a list of numbers.
    params.insert(
        "q".to_string(),
        Val::List(vec![Val::Float(0.9), Val::Float(0.8), Val::Float(0.7)]),
    );
    params.insert("k".to_string(), Val::Int(1));
    let engine = Engine::new(&gen, &cache).with_params(params);
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', $k, $q) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    let res = engine.run(&ast).unwrap();
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(2)), "Carol is nearest");
    let _ = std::fs::remove_dir_all(&root);
}

/// Did this run fail with the typed [`graph_format::pq::NonFiniteEmbedding`]?
/// Classified by *type*, never by message text (house rule) — a NaN that merely
/// trips some unrelated arity/type check would not satisfy this.
fn rejected_nonfinite(r: Result<QueryResult>) -> bool {
    r.err().is_some_and(|e| {
        e.downcast_ref::<graph_format::pq::NonFiniteEmbedding>()
            .is_some()
    })
}

#[test]
fn vecf32_rejects_a_nonfinite_component_at_write_ingest() {
    // The organic HIK-134 reproduction: log(-1.0) → NaN by slater's FalkorDB IEEE
    // semantics. vecf32 must reject it with a TYPED finiteness error *before* it becomes
    // a Vector that `SET n.embedding = …` would write into the index. Pre-fix this
    // returned Ok(a NaN-bearing Vector); a NaN slipping an arity check would not match
    // the typed error asserted here.
    let (root, graph, _) = testgen::write_basic("exec_vecf32_write_nan");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let ast = parser::parse("RETURN vecf32([log(-1.0), 0.2, 0.3]) AS v").unwrap();
    assert!(
        rejected_nonfinite(Engine::new(&gen, &cache).run(&ast)),
        "a NaN vecf32 component must be a typed finiteness error"
    );
    // Index uncorrupted: a subsequent clean KNN over the same fixture still returns the
    // reference nearest neighbour (the rejected write never touched the index).
    let ok = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    let res = Engine::new(&gen, &cache).run(&ok).unwrap();
    assert!(
        matches!(res.rows[0][0], Val::Int(0)),
        "nearest is Alice (exact match) — index uncorrupted"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vecf32_rejects_an_infinite_literal_via_the_parse_fold() {
    // vecf32([1e400, …]) — the f64 literal is finite but `as f32` saturates to +inf. The
    // parse-time constant fold must NOT bake it into a Vector literal (which would skip
    // the runtime gate); the runtime vecf32 gate then rejects it. Covers `±inf` *and* the
    // fold-bypass entry point in one shot.
    let (root, graph, _) = testgen::write_basic("exec_vecf32_inf_literal");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let ast = parser::parse("RETURN vecf32([1e400, 0.2, 0.3]) AS v").unwrap();
    assert!(
        rejected_nonfinite(Engine::new(&gen, &cache).run(&ast)),
        "a +inf vecf32 component must be a typed finiteness error"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn query_vector_nonfinite_is_rejected_against_a_clean_index() {
    // The load-bearing case (HIK-134): a NaN QUERY needs no write at all. Against the
    // clean fixture index, both the inline vecf32() form and a `$param` numeric-list form
    // (the distinct `eval_query_vector` gate) must be rejected with the typed finiteness
    // error — NOT answered with a `total_cmp`-ordered garbage result set.
    let (root, graph, _) = testgen::write_basic("exec_query_vec_nan");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // Form A: inline vecf32([log(-1.0), …]) → the vecf32 ingest gate.
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([log(-1.0), 0.2, 0.3])) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    assert!(
        rejected_nonfinite(Engine::new(&gen, &cache).run(&ast)),
        "a clean index + vecf32(NaN) query must be a typed error, not a garbage result"
    );

    // Form B: a $param list carrying a NaN → the eval_query_vector List arm.
    let mut params = HashMap::new();
    params.insert(
        "q".to_string(),
        Val::List(vec![Val::Float(f64::NAN), Val::Float(0.2), Val::Float(0.3)]),
    );
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, $q) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    assert!(
        rejected_nonfinite(Engine::new(&gen, &cache).with_params(params).run(&ast)),
        "a clean index + $param NaN query must be a typed error, not a garbage result"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_reads_route_through_the_block_cache() {
    let (root, graph, _) = testgen::write_basic("exec_knn_cache");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node)",
    )
    .unwrap();
    engine.run(&ast).unwrap();
    let after_first = cache.metrics();
    assert!(after_first.misses > 0, "first run populates the cache");
    engine.run(&ast).unwrap();
    let after_second = cache.metrics();
    assert_eq!(
        after_second.misses, after_first.misses,
        "the vector group should be served from resident blocks on the second run"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A vector index is built over the base generation and is immutable, so a node
/// deleted afterwards is still *in* it. Every other read path suppresses a
/// tombstoned node; before this fix the KNN path did not, and handed the deleted
/// node back as a live `Val::Node` — the vector arm was the one hole.
#[test]
fn vector_knn_suppresses_delta_deleted_nodes() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph, _) = testgen::write_basic("exec_knn_delete");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    // Carol's own embedding as the query, so she is the exact match (distance 0)
    // and must come back first — this is what makes her absence below meaningful.
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.9, 0.8, 0.7])) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    let ids = |res: &QueryResult| -> Vec<i64> {
        res.rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(i) => i,
                ref other => panic!("id should be an integer, got {other:?}"),
            })
            .collect()
    };

    // Baseline: with no delta, Carol (2) is the nearest hit.
    let before = Engine::new(&MergedView::read_only(&gen), &cache)
        .run(&ast)
        .unwrap();
    assert_eq!(
        ids(&before),
        vec![2, 0, 1],
        "the exact match must lead on a pure-core read"
    );

    // Delete Carol. Her vector is still in the sealed index, so only the delta
    // tombstone can keep her out of the results.
    let mut mem = Memtable::new();
    mem.delete_node("Person", "name", Value::Str("Carol".into()), Some(2));
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    assert!(
        view.delta().is_tombstoned(2),
        "Carol tombstoned in the delta"
    );

    let after = Engine::new(&view, &cache).run(&ast).unwrap();
    let got = ids(&after);
    assert!(
        !got.contains(&2),
        "a deleted node must not be returned by KNN, got {got:?}"
    );
    assert_eq!(
        got,
        vec![0, 1],
        "the two live Person embeddings remain, still exact-ranked"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A vector written to the delta is immediately KNN-visible, with **exact** rank (the
/// overlay is brute-forced, not approximated).
///
/// The sharp part is the re-embed. A node whose vector a newer level supersedes still
/// sits in the sealed base index with its *stale* vector, and `TopK` does not dedup by
/// node id — so a merge that did not suppress the base entry would return that node
/// **twice**, at two different scores, and the stale copy could take one of the `k`
/// slots and evict a live candidate. Both the "appears once" and the "old vector no
/// longer matches" assertions below fail if the suppression is dropped.
#[test]
fn a_delta_written_vector_is_knn_visible_and_supersedes_the_base() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph, _) = testgen::write_basic("exec_knn_delta_write");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let knn = |view: &MergedView, q: &str| -> Vec<(i64, f64)> {
        let ast = parser::parse(&format!(
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 5, vecf32({q})) \
                 YIELD node, score RETURN id(node) AS id, score"
        ))
        .unwrap();
        Engine::new(view, &cache)
            .run(&ast)
            .unwrap()
            .rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (Val::Int(i), Val::Float(s)) => (*i, *s),
                other => panic!("unexpected KNN row {other:?}"),
            })
            .collect()
    };

    // Alice (0)'s original embedding, from the fixture.
    let base = MergedView::read_only(&gen);
    let old = knn(&base, "[0.1, 0.2, 0.3]");
    assert_eq!(old[0].0, 0, "Alice is the exact match for her own vector");
    assert!(old[0].1.abs() < 1e-6, "…at distance ~0");

    // Re-embed Alice onto a vector orthogonal to her old one, and add a brand-new
    // node carrying an embedding of its own.
    // Seeded with the core's counts, so a born node's synthetic id cannot collide
    // with a core dense id (`Memtable::new()` bases both at 0 — fine for a
    // patch-only delta, wrong the moment a node is born).
    let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
    mem.upsert_node(
        "Person",
        "name",
        Value::Str("Alice".into()),
        Some(0),
        [("embedding".to_string(), Value::Vector(vec![0.0, 0.0, 1.0]))],
    );
    mem.upsert_node(
        "Person",
        "name",
        Value::Str("Zoe".into()),
        None, // delta-born: no core row at all
        [("embedding".to_string(), Value::Vector(vec![1.0, 0.0, 0.0]))],
    );
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));

    // Alice's NEW vector: she is now the exact match, and appears exactly once.
    let fresh = knn(&view, "[0.0, 0.0, 1.0]");
    assert_eq!(
        fresh[0].0, 0,
        "the delta's vector must win over the base's stale one"
    );
    assert!(
        fresh[0].1.abs() < 1e-6,
        "…at distance ~0, got {}",
        fresh[0].1
    );
    assert_eq!(
        fresh.iter().filter(|(id, _)| *id == 0).count(),
        1,
        "Alice must appear exactly once — the base's stale entry has to be suppressed \
             in the scan, not merged away afterwards; got {fresh:?}"
    );

    // Her OLD vector must no longer be an exact match for her — proof the stale base
    // entry is genuinely gone from the candidate set rather than merely outranked.
    let stale = knn(&view, "[0.1, 0.2, 0.3]");
    let alice = stale
        .iter()
        .find(|(id, _)| *id == 0)
        .expect("Alice is still live, just re-embedded");
    assert!(
        alice.1 > 1e-3,
        "Alice's stale base vector must not still be scoring ~0 against her old query; \
             got {alice:?}"
    );

    // The delta-born node is visible, exactly ranked, on its own vector. Its synthetic
    // id is the first past the core's dense range.
    let zoe = gen.node_count() as i64;
    let born = knn(&view, "[1.0, 0.0, 0.0]");
    assert_eq!(
        born[0].0, zoe,
        "a node born in the delta with an embedding must be KNN-visible; got {born:?}"
    );
    assert!(born[0].1.abs() < 1e-6, "…at distance ~0, got {}", born[0].1);
    let _ = std::fs::remove_dir_all(&root);
}

// ── The RW-index over the delta (HIK-112) ────────────────────────────────────────────
//
// These drive a **real** `DeltaWriter` (real WAL, real seal), because the two properties
// that matter are lifecycle properties: an index rebuilt from a replayed delta must answer
// what the delta says, and no vector may go missing across a seal. A hand-built `Memtable`
// cannot express either.

/// A `RwIndexConfig` with the floors removed, so the tiny fixtures below actually take the
/// index path instead of silently falling back to the brute force (which would make every
/// assertion here vacuous — the fallback is the *old* code, and it passes by construction).
#[cfg(test)]
fn rw_cfg_no_floor() -> crate::rwindex::RwIndexConfig {
    crate::rwindex::RwIndexConfig {
        enabled: true,
        min_vectors: 0,
        max_vectors: 1 << 20,
    }
}

/// The fixture's business-key resolver: Alice/Bob/Carol are core dense ids 0/1/2, and any
/// other name is a delta-born node.
#[cfg(test)]
fn basic_resolve(op: &slater_delta::WalOp) -> slater_delta::OpResolution {
    use slater_delta::{OpResolution, WalOp};
    let value = match op {
        WalOp::UpsertNode { value, .. }
        | WalOp::DeleteNode { value, .. }
        | WalOp::RemoveNodeProps { value, .. }
        | WalOp::ReplaceNode { value, .. }
        | WalOp::SetNodeLabels { value, .. } => value,
        _ => return OpResolution::Node(None),
    };
    OpResolution::Node(match value {
        Value::Str(s) if s == "Alice" => Some(0),
        Value::Str(s) if s == "Bob" => Some(1),
        Value::Str(s) if s == "Carol" => Some(2),
        _ => None,
    })
}

#[cfg(test)]
fn upsert_vec(name: &str, v: Vec<f32>) -> slater_delta::WalOp {
    slater_delta::WalOp::UpsertNode {
        label: "Person".into(),
        key: "name".into(),
        value: Value::Str(name.into()),
        patches: vec![("embedding".into(), Value::Vector(v))],
    }
}

/// The KNN top-`k` a query must produce, derived **independently** of the index: a plain
/// scan of the effective (id, vector) set with `vector::distance`, in the D26 total order.
/// This is the ground truth every RW-index test below is measured against — never "the
/// index agrees with the brute-force walk", which is the parity test that would pass even
/// if both shared a misunderstanding.
#[cfg(test)]
fn expected_topk(live: &[(u64, Vec<f32>)], q: &[f32], k: usize) -> Vec<(i64, f64)> {
    let mut scored: Vec<(f64, u64)> = live
        .iter()
        .map(|(id, v)| {
            (
                vector::distance(graph_format::manifest::Metric::Cosine, q, v),
                *id,
            )
        })
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    scored
        .into_iter()
        .take(k)
        .map(|(s, id)| (id as i64, s))
        .collect()
}

/// Run the KNN with the RW-index arm wired, and report whether the index actually served
/// the delta (rather than falling back), so a test cannot pass vacuously.
#[cfg(test)]
#[allow(clippy::type_complexity)]
fn knn_with_rw(
    gen: &Generation,
    cache: &BlockCache,
    writer: &crate::delta_writer::DeltaWriter,
    rw: &crate::rwindex::RwIndexCache,
    q: &str,
    k: usize,
) -> (Vec<(i64, f64)>, bool) {
    use crate::read_view::MergedView;
    // The delta and its epoch, in ONE atomic read — the pair the index is cut at.
    let published = writer.delta_snapshot_at();
    let epoch = published.epoch;
    let view = MergedView::new(gen, published.delta);
    let ast = parser::parse(&format!(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', {k}, vecf32({q})) \
             YIELD node, score RETURN id(node) AS id, score"
    ))
    .unwrap();
    let rows = Engine::new(&view, cache)
        .with_rw_index(rw, writer.touched_journal(), epoch, rw_cfg_no_floor())
        .run(&ast)
        .unwrap()
        .rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Val::Int(i), Val::Float(s)) => (*i, *s),
            other => panic!("unexpected KNN row {other:?}"),
        })
        .collect();
    // Did the index serve it? It serves iff it stands at exactly the query's epoch.
    let served = rw.index_epoch(gen.uuid(), "Person", "embedding") == Some(epoch);
    (rows, served)
}

/// **The RW-index is a cache of the delta; the delta is the durable thing.** Nothing is
/// persisted, so the recovery story is: replay the WAL, rebuild the index from the replayed
/// delta.
///
/// Drives the *real* WAL: write embeddings (a re-embed of a core node, two born nodes, and
/// a `REMOVE n.embedding` that un-embeds one), drop the writer without any clean shutdown,
/// reopen (which replays), and assert the KNN answers what a brute force over the
/// **replayed** delta says — not what the pre-crash query happened to return. Truth is the
/// state on disk, not the state in the dead process's memory.
#[test]
fn rw_index_rebuilds_from_wal_replay() {
    use crate::delta_writer::DeltaWriter;
    use crate::rwindex::RwIndexCache;
    use slater_delta::WalOp;

    let (root, graph, _) = testgen::write_basic("exec_rw_replay");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let wal = root.join("wal_rw_replay");
    let _ = std::fs::remove_dir_all(&wal);
    let core_n = gen.node_count();

    let open = || {
        DeltaWriter::open(
            &wal,
            &graph,
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            None,
            basic_resolve,
        )
        .unwrap()
    };

    {
        let w = open();
        // Alice (core id 0) is re-embedded onto the +z axis.
        w.write(
            upsert_vec("Alice", vec![0.0, 0.0, 1.0]),
            basic_resolve(&upsert_vec("Alice", vec![])),
        )
        .unwrap();
        // Two delta-born Persons (synthetic ids core_n, core_n + 1).
        w.write(
            upsert_vec("Zoe", vec![1.0, 0.0, 0.0]),
            slater_delta::OpResolution::Node(None),
        )
        .unwrap();
        w.write(
            upsert_vec("Yan", vec![0.0, 1.0, 0.0]),
            slater_delta::OpResolution::Node(None),
        )
        .unwrap();
        // Bob (core id 1) is UN-embedded. Absence cannot express this (D12 keeps an indexed
        // embedding out of the props record), so it rides its own channel — and if the
        // rebuilt index loses it, Bob's *stale base vector* silently starts scoring again.
        w.write(
            WalOp::RemoveNodeProps {
                label: "Person".into(),
                key: "name".into(),
                value: Value::Str("Bob".into()),
                props: vec!["embedding".into()],
            },
            slater_delta::OpResolution::Node(Some(1)),
        )
        .unwrap();
        // No flush, no clean close: just drop it. The WAL is fsynced per commit.
    }

    // Reopen — `DeltaWriter::open` replays the WAL dir into a fresh memtable — and rebuild
    // the index from the replayed delta. A brand-new `RwIndexCache`: nothing survived.
    let w = open();
    let rw = RwIndexCache::new();

    // The effective live set after the replay, derived by hand from the writes above:
    //   0 Alice → the delta's new vector      (the base's [0.1,0.2,0.3] is superseded)
    //   1 Bob   → GONE                        (un-embedded; the base's [0.2,0.1,0.0] must
    //                                          NOT come back)
    //   2 Carol → the base's [0.9,0.8,0.7]    (the delta says nothing about her)
    //   3 Zoe, 4 Yan → born, from the delta
    let live: Vec<(u64, Vec<f32>)> = vec![
        (0, vec![0.0, 0.0, 1.0]),
        (2, vec![0.9, 0.8, 0.7]),
        (core_n, vec![1.0, 0.0, 0.0]),
        (core_n + 1, vec![0.0, 1.0, 0.0]),
    ];

    for q in [
        (vec![0.0f32, 0.0, 1.0], "[0.0, 0.0, 1.0]"),
        (vec![1.0, 0.0, 0.0], "[1.0, 0.0, 0.0]"),
        // Bob's OLD (base) vector. He is un-embedded, so he must not come back AT ALL —
        // let alone lead, which is what a lost removal would do.
        (vec![0.2, 0.1, 0.0], "[0.2, 0.1, 0.0]"),
    ] {
        let (got, served) = knn_with_rw(&gen, &cache, &w, &rw, q.1, 5);
        assert!(
            served,
            "the RW-index must have served the query, not fallen back"
        );
        let want = expected_topk(&live, &q.0, 5);
        assert_eq!(
            got.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            want.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            "query {} — the index rebuilt from the replayed WAL must answer what the \
                 delta on disk says; got {got:?}, want {want:?}",
            q.1
        );
        for (g, e) in got.iter().zip(&want) {
            assert!((g.1 - e.1).abs() < 1e-5, "score {g:?} vs {e:?}");
        }
        assert!(
            !got.iter().any(|(id, _)| *id == 1),
            "Bob was un-embedded; his stale BASE vector must not score. Got {got:?}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// **No vector goes missing across a seal.**
///
/// `flush_to_l0` moves the whole active memtable into a sealed L0 level and resets the
/// memtable. HIK-112 warns that an index tied to the *active memtable* alone would be
/// cleared here, while the L0's core segment is not yet published — and the vectors in it
/// would vanish from KNN with nothing to say so.
///
/// This index is over `mem ⊕ L0`, which a seal does not change, so the seal journals an
/// empty touched set and the index is not touched at all. The test proves the *observable*:
/// the same ids, at the same scores, before and after. The mutation that matters is a
/// clear-on-seal — the **born** nodes below have no base entry at all, so if the delta arm
/// lost them they would disappear outright rather than merely re-rank.
#[test]
fn rw_index_ladder_survives_a_seal() {
    use crate::delta_writer::DeltaWriter;
    use crate::rwindex::RwIndexCache;

    let (root, graph, _) = testgen::write_basic("exec_rw_seal");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let wal = root.join("wal_rw_seal");
    let _ = std::fs::remove_dir_all(&wal);
    let core_n = gen.node_count();

    let w = DeltaWriter::open(
        &wal,
        &graph,
        gen.uuid(),
        gen.node_count(),
        gen.edge_count(),
        None,
        basic_resolve,
    )
    .unwrap();
    let rw = RwIndexCache::new();

    // Eight born Persons on a fan of directions in the x–y plane, plus a re-embed of Alice.
    let born: Vec<(u64, Vec<f32>)> = (0..8u64)
        .map(|i| {
            let a = i as f32 * 0.19;
            (core_n + i, vec![a.cos(), a.sin(), 0.0])
        })
        .collect();
    for (i, (_, v)) in born.iter().enumerate() {
        w.write(
            upsert_vec(&format!("N{i}"), v.clone()),
            slater_delta::OpResolution::Node(None),
        )
        .unwrap();
    }
    w.write(
        upsert_vec("Alice", vec![0.0, 0.0, 1.0]),
        slater_delta::OpResolution::Node(Some(0)),
    )
    .unwrap();

    let mut live: Vec<(u64, Vec<f32>)> = born.clone();
    live.push((0, vec![0.0, 0.0, 1.0])); // Alice, re-embedded
    live.push((1, vec![0.2, 0.1, 0.0])); // Bob, from the base (the delta never touches him)
    live.push((2, vec![0.9, 0.8, 0.7])); // Carol, from the base

    let q = "[1.0, 0.15, 0.0]";
    let qv = vec![1.0f32, 0.15, 0.0];
    let want = expected_topk(&live, &qv, 6);

    let (before, served) = knn_with_rw(&gen, &cache, &w, &rw, q, 6);
    assert!(served, "the RW-index must serve the pre-seal query");
    assert_eq!(
        before.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        want.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        "pre-seal: got {before:?}, want {want:?}"
    );

    // ── SEAL ──────────────────────────────────────────────────────────────────────────
    assert!(w.flush_to_l0().unwrap(), "the memtable had writes to seal");
    assert_eq!(w.l0_len(), 1, "the writes are in a sealed L0 level now");
    assert!(
        w.snapshot().is_empty(),
        "…and the ACTIVE memtable really is empty — without this the test would not \
             actually cross the seal, and would pass whatever the index did"
    );
    // The seal published a new epoch, so the index has to advance across it.
    assert!(
        w.delta_snapshot_at().epoch > 1,
        "the seal must have bumped the epoch"
    );

    let (after, served) = knn_with_rw(&gen, &cache, &w, &rw, q, 6);
    assert!(
        served,
        "the RW-index must still serve after the seal — a journal gap here would silently \
             force a rebuild, which is correct but hides the bug this test exists for"
    );
    assert_eq!(
        after.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        want.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        "A VECTOR WENT MISSING ACROSS THE SEAL. Before {before:?}, after {after:?}, \
             truth {want:?}"
    );
    for (a, b) in after.iter().zip(&before) {
        assert!(
            (a.1 - b.1).abs() < 1e-9,
            "score moved across the seal: {a:?} vs {b:?}"
        );
    }

    // And the born nodes — the ones with no base entry, which a cleared index would lose
    // outright — are still there.
    let born_ids: Vec<i64> = after
        .iter()
        .map(|(i, _)| *i)
        .filter(|i| *i >= core_n as i64)
        .collect();
    assert!(
        born_ids.len() >= 4,
        "the delta-born embeddings must survive the seal; got {after:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// D12 read parity. An *indexed* embedding is routed out of the column store at build
/// time, so a core node's `n.embedding` reads as `Null`. A delta-written embedding is
/// deliberately left in the node's property map (that map carries it to the flush and
/// the rebuild), so without an explicit suppression the same query would answer `Null`
/// for a core-resident node and a vector for a freshly-written one.
///
/// An *unindexed* vector property is not covered by D12 and must still read back — it
/// is an ordinary inline value, exactly as it is in the core.
#[test]
fn a_delta_written_indexed_vector_reads_as_null_like_the_core() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph, _) = testgen::write_basic("exec_d12_delta_vector");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // Alice (0) is core-resident and Person.embedding is vector-indexed, so her
    // embedding already reads as Null — the behaviour the delta must match.
    let base = Engine::new(&MergedView::read_only(&gen), &cache)
        .run(&parser::parse("MATCH (n:Person {name: 'Alice'}) RETURN n.embedding").unwrap())
        .unwrap();
    assert!(
        matches!(base.rows[0][0], Val::Null),
        "a core node's indexed embedding reads as Null (D12), got {:?}",
        base.rows[0][0]
    );

    // Re-embed Alice, and give her an unindexed vector property alongside.
    let mut mem = Memtable::new();
    mem.upsert_node(
        "Person",
        "name",
        Value::Str("Alice".into()),
        Some(0),
        [
            ("embedding".to_string(), Value::Vector(vec![0.1, 0.2, 0.3])),
            ("shadow".to_string(), Value::Vector(vec![0.4, 0.5])),
        ],
    );
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let eng = Engine::new(&view, &cache);

    let got = eng
        .run(
            &parser::parse(
                "MATCH (n:Person {name: 'Alice'}) RETURN n.embedding AS e, n.shadow AS s",
            )
            .unwrap(),
        )
        .unwrap();
    assert!(
        matches!(got.rows[0][0], Val::Null),
        "a delta-written *indexed* embedding must read as Null too, or the same graph \
             answers two ways depending on which level the node lives in; got {:?}",
        got.rows[0][0]
    );
    assert!(
        matches!(&got.rows[0][1], Val::Vector(v) if v == &[0.4, 0.5]),
        "an unindexed vector property is not routed out, so it must read back verbatim; \
             got {:?}",
        got.rows[0][1]
    );

    // The whole-map read (`RETURN n` / properties(n)) must agree with the column read.
    let all = eng
        .run(&parser::parse("MATCH (n:Person {name: 'Alice'}) RETURN properties(n) AS p").unwrap())
        .unwrap();
    let Val::Map(props) = &all.rows[0][0] else {
        panic!("properties() should yield a map");
    };
    assert!(
        !props.iter().any(|(k, _)| k == "embedding"),
        "the indexed embedding must be absent from properties(n), got {props:?}"
    );
    assert!(
        props.iter().any(|(k, _)| k == "shadow"),
        "the unindexed vector must survive properties(n), got {props:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn similarity_and_vecf32_scalar_functions() {
    let (root, res) = run(
        "exec_similarity",
        "RETURN similarity(vecf32([1.0, 0.0]), vecf32([1.0, 0.0])) AS same, \
             similarity(vecf32([1.0, 0.0]), vecf32([0.0, 1.0])) AS orth",
    );
    let Val::Float(same) = res.rows[0][0] else {
        panic!("expected float");
    };
    let Val::Float(orth) = res.rows[0][1] else {
        panic!("expected float");
    };
    assert!((same - 1.0).abs() < 1e-9);
    assert!(orth.abs() < 1e-9);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase8_vector_distance_functions() {
    // Vectors ported from FalkorDB tests/flow/test_vecsim.py::test01_vector_distance.
    // euclidean([1,2],[2,3]) = sqrt(2); cosine = 1 - 8/sqrt(65).
    let (root, res) = run(
        "exec_p8_dist",
        "RETURN vec.euclideanDistance(vecf32([1.0, 2.0]), vecf32([2.0, 3.0])) AS e, \
             vec.cosineDistance(vecf32([1.0, 2.0]), vecf32([2.0, 3.0])) AS c, \
             vec.euclideanDistance(vecf32([1.0, 1.0]), vecf32([1.0, 1.0])) AS esame, \
             vec.cosineDistance(vecf32([1.0, 1.0]), vecf32([1.0, 1.0])) AS csame",
    );
    assert_float(&res.rows[0][0], 2.0_f64.sqrt());
    assert_float(&res.rows[0][1], 1.0 - 8.0 / 65.0_f64.sqrt());
    assert_float(&res.rows[0][2], 0.0);
    assert_float(&res.rows[0][3], 0.0);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase8_vector_distance_null_propagates() {
    // A NULL operand → NULL (either side), for both functions.
    let (root, res) = run(
        "exec_p8_null",
        "RETURN vec.euclideanDistance(null, vecf32([1.0, 1.0])) AS a, \
             vec.euclideanDistance(vecf32([1.0, 1.0]), null) AS b, \
             vec.cosineDistance(null, null) AS c",
    );
    assert!(matches!(res.rows[0][0], Val::Null));
    assert!(matches!(res.rows[0][1], Val::Null));
    assert!(matches!(res.rows[0][2], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase8_vector_distance_errors() {
    // Dimension mismatch is an error (FalkorDB: "Vector dimension mismatch").
    let e = run_err(
        "exec_p8_dim",
        "RETURN vec.euclideanDistance(vecf32([1.0, 1.0]), vecf32([2.0, 2.0, 3.0])) AS d",
    );
    assert!(e.contains("dimension mismatch"), "got: {e}");
    // A non-vector operand is an error (FalkorDB: "Type mismatch"). Pass a
    // string directly (vecf32() would reject it first; the distance arm coerces
    // via as_vector and rejects a non-numeric scalar).
    let e = run_err(
        "exec_p8_type",
        "RETURN vec.cosineDistance([1.0, 1.0], 'foo') AS d",
    );
    assert!(e.contains("vectors"), "got: {e}");
}
