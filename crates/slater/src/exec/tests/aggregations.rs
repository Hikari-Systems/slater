// SPDX-License-Identifier: Apache-2.0
//! `aggregations` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Phase 3: statistical aggregations ────────────────────────────────────

#[test]
fn phase3_stdev_sample_and_population() {
    // Vectors ported from FalkorDB tests/flow/test_aggregation.py::test06_StDev.
    // Edge case: a single value has zero sample deviation.
    let (root, res) = run("exec_p3_stdev1", "RETURN stDev(5.1) AS s");
    assert_float(&res.rows[0][0], 0.0);
    let _ = std::fs::remove_dir_all(&root);

    // 1..10: sample variance = 82.5/9, population variance = 82.5/10.
    let (root, res) = run(
        "exec_p3_stdev2",
        "UNWIND [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] AS x \
             RETURN stDev(x) AS s, stDevP(x) AS sp",
    );
    assert_float(&res.rows[0][0], (82.5_f64 / 9.0).sqrt());
    assert_float(&res.rows[0][1], (82.5_f64 / 10.0).sqrt());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase3_percentile_cont() {
    // FalkorDB test04_percentileCont: linear interpolation over [2,4,6,8,10].
    let cases = [
        (0.0, 2.0),
        (0.1, 2.8),
        (0.33, 4.64),
        (0.5, 6.0),
        (1.0, 10.0),
    ];
    for (i, (p, want)) in cases.iter().enumerate() {
        let (root, res) = run(
            &format!("exec_p3_pcont_{i}"),
            &format!("UNWIND [2, 4, 6, 8, 10] AS x RETURN percentileCont(x, {p}) AS r"),
        );
        assert_float(&res.rows[0][0], *want);
        let _ = std::fs::remove_dir_all(&root);
    }
}

#[test]
fn phase3_percentile_disc() {
    // FalkorDB test05_percentileDisc: nearest-rank over [2,4,6,8,10].
    let cases = [(0.0, 2.0), (0.1, 2.0), (0.33, 4.0), (0.5, 6.0), (1.0, 10.0)];
    for (i, (p, want)) in cases.iter().enumerate() {
        let (root, res) = run(
            &format!("exec_p3_pdisc_{i}"),
            &format!("UNWIND [2, 4, 6, 8, 10] AS x RETURN percentileDisc(x, {p}) AS r"),
        );
        assert_float(&res.rows[0][0], *want);
        let _ = std::fs::remove_dir_all(&root);
    }
    // p == 0 takes index 0 of the sorted values, regardless of input order.
    let (root, res) = run(
        "exec_p3_pdisc_zero",
        "UNWIND [0.5, 0, 1] AS x RETURN percentileDisc(x, 0) AS r",
    );
    assert_float(&res.rows[0][0], 0.0);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase3_empty_aggregation_defaults() {
    // FalkorDB test01_empty_aggregation: with no rows and no grouping key, the
    // statistical aggregates still emit one row — stDev/stDevP→0, percentiles→null.
    let (root, res) = run(
        "exec_p3_empty",
        "MATCH (n) WHERE n.name = 'noneExisting' \
             RETURN stDev(n.v) AS a, stDevP(n.v) AS b, \
                    percentileDisc(n.v, 0.5) AS c, percentileCont(n.v, 0.5) AS d",
    );
    assert_eq!(res.rows.len(), 1);
    let r = &res.rows[0];
    assert_float(&r[0], 0.0);
    assert_float(&r[1], 0.0);
    assert!(matches!(r[2], Val::Null));
    assert!(matches!(r[3], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

// log/log10/exp/e/pi/pow — the camelid §1 gap (TF-IDF scoring needs `log`).
#[test]
fn numeric_log_family_functions() {
    let (root, res) = run(
        "exec_logfns",
        "RETURN log(2.718281828459045) AS ln, log10(1000.0) AS l10, \
             exp(0.0) AS ex, e() AS e, pi() AS pi, pow(2.0, 10.0) AS p",
    );
    let f = |v: &Val| match v {
        Val::Float(x) => *x,
        other => panic!("expected float, got {other:?}"),
    };
    let r = &res.rows[0];
    assert!((f(&r[0]) - 1.0).abs() < 1e-12);
    assert!((f(&r[1]) - 3.0).abs() < 1e-12);
    assert!((f(&r[2]) - 1.0).abs() < 1e-12);
    assert!((f(&r[3]) - std::f64::consts::E).abs() < 1e-12);
    assert!((f(&r[4]) - std::f64::consts::PI).abs() < 1e-12);
    assert!((f(&r[5]) - 1024.0).abs() < 1e-9);
    let _ = std::fs::remove_dir_all(&root);
}

// FalkorDB parity: a non-positive argument to log yields the IEEE result
// (-inf / NaN), not an error; NULL propagates as NULL.
#[test]
fn log_domain_and_null_match_falkordb() {
    let (root, res) = run(
        "exec_log_domain",
        "RETURN log(0.0) AS zero, log(-1.0) AS neg, log(null) AS nul",
    );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Float(x) if x == f64::NEG_INFINITY));
    assert!(matches!(r[1], Val::Float(x) if x.is_nan()));
    assert!(matches!(r[2], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

// eu-ai-act §P1: a relationship whose target node is already bound from a prior
// MATCH must lead with that bound node (reverse adjacency), not full-scan the
// start label once per bound row. We assert correctness here; the reroot in
// `maybe_reroot` removes the O(|start-label|)-per-row blow-up.
#[test]
fn reverse_traversal_to_bound_node() {
    // Bob is reached by Alice and Carol via KNOWS. Bind Bob first, then match
    // the incoming KNOWS with the *source* unbound — the planner should reroot
    // to lead with Bob and walk reverse adjacency.
    let (root, res) = run(
        "exec_bound_end_reroot",
        "MATCH (b:Person {name:'Bob'}) \
             MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS nm ORDER BY nm",
    );
    let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
    assert_eq!(names, vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-147 execution guard: a **parameterised** id lookup must do seek-sized work,
/// not scan-sized work. The plan-level assertions live in `plan.rs`; this one runs
/// the whole engine and measures the intermediate charge, so a future regression in
/// either the id-seek walker or the re-root fails the suite instead of merely making
/// production slow.
///
/// The star fixture makes the two plans differ by construction: leading with `m`
/// scans every node and expands all `2n` LINK edges, whereas seeking the id-anchored
/// `n` and walking one reverse edge touches exactly one. We assert both the peak
/// intermediate charge (bounded by a constant, not `n`) and that a budget far below
/// `n` still completes — the un-rerooted plan blows it, which is precisely the
/// `query.maxIntermediate` failure reported on the 10M sample.
#[test]
fn parameterised_id_lookup_does_seek_sized_work_not_scan_sized() {
    const N: u64 = 2_000;
    let (root, graph) = testgen::write_hub("exec_param_id_seek", N);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let params: HashMap<String, Val> = [("x".to_string(), Val::Int(7))].into_iter().collect();
    let run_with = |budget: u64| {
        let global = GlobalIntermediateBudget::new(0);
        let engine = Engine::new(&gen, &cache)
            .with_max_intermediate(budget)
            .with_global_budget(&global)
            .with_params(params.clone());
        let res = engine.run(
            &parser::parse("MATCH (m)-[:LINK]->(n) WHERE id(n) = $x RETURN m.name AS nm").unwrap(),
        );
        (res, global.peak())
    };

    // A budget two orders of magnitude below the star's edge count. The seek plan
    // needs a handful of elements; the scan plan needs ~2n.
    let (res, peak) = run_with(N / 100);
    let res = res.expect("a parameterised id lookup must not scan the whole star");
    let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
    assert_eq!(names, vec!["hub"], "only the hub links to leaf 7");
    assert!(
        peak < N / 100,
        "seek-sized work expected, peak charge was {peak} against {N} leaves"
    );

    // Same query, unbounded — the answer must not depend on the budget, and the
    // literal spelling must agree with the parameterised one.
    let (unbounded, _) = run_with(0);
    assert_eq!(unbounded.unwrap().rows.len(), 1);
    let engine = Engine::new(&gen, &cache);
    let literal = engine
        .run(&parser::parse("MATCH (m)-[:LINK]->(n) WHERE id(n) = 7 RETURN m.name AS nm").unwrap())
        .unwrap();
    assert_eq!(literal.rows.len(), 1);
    assert_eq!(literal.rows[0][0].to_display(), "hub");
    let _ = std::fs::remove_dir_all(&root);
}

/// The same guarantee for a **row-bound** id — `UNWIND … AS x MATCH (n) WHERE id(n) = x`
/// — measured through the executor, not the planner.
///
/// This has to run on the streamed-MATCH path or it proves nothing. `choose_node_scan`
/// receives the row's scalars only when the anchor is classified *correlated*
/// (`matchclause`'s hoist decision); an uncorrelated anchor is planned once, outside the
/// row loop, with an **empty** bound map. So threading `bound` into the id walkers is
/// inert on its own — the walkers are handed nothing to find — while plan-level tests
/// that call `plan_for_bound` with a hand-built map go green regardless. That is exactly
/// the shape this test exists to refuse.
///
/// Also pins the correctness half, which matters more than the speed half: with the
/// anchor correlated, a plan built from row 1 must never be replayed for row 2. Two rows
/// naming different leaves must return their own leaf, not the first row's twice.
#[test]
fn a_row_bound_id_lookup_does_seek_sized_work_and_stays_per_row() {
    const N: u64 = 2_000;
    let (root, graph) = testgen::write_hub("exec_bound_id_seek", N);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // One row: seek-sized work, not scan-sized. The un-seeked plan scans every node and
    // expands all 2n LINK edges, so a budget two orders of magnitude below that trips.
    let global = GlobalIntermediateBudget::new(0);
    let engine = Engine::new(&gen, &cache)
        .with_max_intermediate(N / 100)
        .with_global_budget(&global);
    let res = engine
        .run(
            &parser::parse(
                "UNWIND [7] AS x MATCH (m)-[:LINK]->(n) WHERE id(n) = x RETURN m.name AS nm",
            )
            .unwrap(),
        )
        .expect("a row-bound id lookup must not scan the whole star");
    let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
    assert_eq!(names, vec!["hub"], "only the hub links to leaf 7");
    assert!(
        global.peak() < N / 100,
        "seek-sized work expected, peak charge was {} against {N} leaves",
        global.peak()
    );

    // Multi-row: the regression guard for the hoist invariant. Each row must be answered
    // from its own binding.
    let engine = Engine::new(&gen, &cache);
    let multi = engine
        .run(
            &parser::parse("UNWIND [7, 11, 3] AS x MATCH (n) WHERE id(n) = x RETURN id(n) AS got")
                .unwrap(),
        )
        .expect("multi-row bound id lookup");
    let got: Vec<String> = multi.rows.iter().map(|r| r[0].to_display()).collect();
    assert_eq!(
        got,
        vec!["7", "11", "3"],
        "each row must seek its own id — a hoisted plan replays row 1's for every row"
    );

    // ── The plain anchor: no traversal, so no re-root to rescue it ──────────────
    //
    // The intermediate budget does not instrument this shape (measured: peak 0 whether
    // it seeks or scans), so assert on `anchor_ids_scanned()` — the ids the anchor scan
    // actually walked. An `IdSeek` walks a handful; an `AllNodes` scan walks the id
    // space. This is the assertion that stays red under half A.1 alone.
    let scanned = |q: &str| -> (Vec<String>, u64) {
        let eng = Engine::new(&gen, &cache);
        let r = eng.run(&parser::parse(q).unwrap()).expect("query");
        (
            r.rows.iter().map(|row| row[0].to_display()).collect(),
            eng.anchor_ids_scanned(),
        )
    };

    let (got, walked) = scanned("WITH 5 AS x MATCH (n) WHERE id(n) = x RETURN id(n) AS got");
    assert_eq!(got, vec!["5"]);
    assert!(
        walked <= 8,
        "a WITH-bound id anchor must seek, not scan — walked {walked} ids of {N}"
    );

    // `id(n) IN <bound list>`, mirroring the `$ids` arm.
    let (got, walked) =
        scanned("WITH [4, 9] AS xs MATCH (n) WHERE id(n) IN xs RETURN id(n) AS got ORDER BY got");
    assert_eq!(got, vec!["4", "9"]);
    assert!(
        walked <= 8,
        "a bound-list id membership must seek — walked {walked} ids of {N}"
    );

    // Reversed operands.
    let (got, walked) = scanned("WITH 6 AS x MATCH (n) WHERE x = id(n) RETURN id(n) AS got");
    assert_eq!(got, vec!["6"]);
    assert!(
        walked <= 8,
        "reversed operands must seek too — walked {walked}"
    );

    // ── The fallbacks must stay fallbacks ───────────────────────────────────────
    //
    // Classifying an anchor *correlated* costs the shared-candidate replay for
    // multi-row inputs. That is the right trade for an O(1) seek and the wrong one for
    // a per-row full scan, so an id expression that cannot resolve must not be treated
    // as seekable — it must scan, and it must answer correctly.
    let (got, _) = scanned("WITH 'five' AS x MATCH (n) WHERE id(n) = x RETURN id(n) AS got");
    assert!(
        got.is_empty(),
        "a non-integer binding is not a node id: no rows, no coercion, no panic"
    );
    // Out of range and negative are provably empty — an empty seek, never an error.
    for q in [
        "WITH 999999 AS x MATCH (n) WHERE id(n) = x RETURN id(n) AS got",
        "WITH -1 AS x MATCH (n) WHERE id(n) = x RETURN id(n) AS got",
    ] {
        let (got, walked) = scanned(q);
        assert!(got.is_empty(), "{q} must return nothing");
        assert!(walked <= 8, "{q} is provably empty — it must not scan");
    }

    // An inline list mixing a bound column with a literal. `const_int` resolves each
    // element, so this seeks — which means the correlation check has to recognise it
    // too, or it is planned with an empty `bound` and silently scans.
    let (got, walked) =
        scanned("WITH 2 AS a MATCH (n) WHERE id(n) IN [a, 3] RETURN id(n) AS got ORDER BY got");
    assert_eq!(got, vec!["2", "3"]);
    assert!(
        walked <= 8,
        "a list mixing a bound column and a literal must seek — walked {walked}"
    );

    // `id(n) < x` is NOT seekable — `collect_id_eq` handles equality only. It must
    // answer correctly by scanning; the point is that it must not be *misclassified*
    // as a per-row plan, which would forfeit the shared-candidate replay for nothing.
    let (got, _) =
        scanned("WITH 3 AS x MATCH (n) WHERE id(n) < x RETURN id(n) AS got ORDER BY got");
    assert_eq!(
        got,
        vec!["0", "1", "2"],
        "an unseekable id comparison still answers"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// The **Vamana arm** of the HIK-122 rescue read. The consolidation tests exercise this
/// through the real builder, but a fixture small enough to run there is always below
/// `ann_threshold` and so always brute-force — this is the only place the other arm is
/// reached, and it has to return the *raw* embedding the user wrote, not the ANN-space
/// point the graph navigates on (`build_vamana_index` transforms for search and stores raw;
/// a rescue that returned the transformed point would write a silently wrong vector into
/// the rebuilt column store).
#[test]
fn base_index_vectors_reads_raw_embeddings_from_a_vamana_index() {
    let fix = testgen::VamanaFixture {
        n: 200,
        dim: 16,
        r: 12,
        alpha: 1.2,
        pq_subspaces: 4,
        pq_bits: 8,
        vector_block_size: 4096,
    };
    let (root, graph, raw) = testgen::write_vamana("exec_rescue_vamana", &fix);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let desc = gen.manifest().vector_indexes[0].clone();
    assert!(
        matches!(desc.mode, AnnMode::Vamana { .. }),
        "the fixture must be above the ANN threshold, or this test proves nothing"
    );

    // A scattered handful, plus an id the index does not hold.
    let wanted: HashSet<u64> = [3u64, 7, 101, 199, 5_000].into_iter().collect();
    let got = engine.base_index_vectors(&desc, &wanted).unwrap();

    assert_eq!(
        got.len(),
        4,
        "every wanted id the base indexes must come back, and only those: id 5000 is not \
             in the index"
    );
    for id in [3u64, 7, 101, 199] {
        assert_eq!(
            got.get(&id),
            Some(&raw[id as usize]),
            "node {id}'s rescued vector must be the raw embedding, byte-for-byte"
        );
    }
    // An empty request must not read the index at all — the common case is no candidates.
    assert!(engine
        .base_index_vectors(&desc, &HashSet::new())
        .unwrap()
        .is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

/// The headline M7 test: a synthetic index far above the ANN threshold **and**
/// far larger than the vector cache budget. The Vamana/PQ arm recovers most of
/// the brute-force top-k while the vector-index pool stays bounded (resident PQ
/// codes + only a handful of paged-in Vamana blocks — never the whole store).
#[test]
fn vamana_knn_matches_brute_force_with_bounded_vector_cache() {
    let fix = testgen::VamanaFixture {
        n: 2000,
        dim: 32,
        r: 24,
        alpha: 1.2,
        pq_subspaces: 8,
        pq_bits: 8,
        vector_block_size: 8192,
    };
    let (root, graph, raw) = testgen::write_vamana("exec_vamana_recall", &fix);
    let gen = Generation::open(&root, &graph).unwrap();
    let block_cache = BlockCache::new(1 << 20);

    // Budget = resident PQ codes + room for only ~8 of the 8 KiB Vamana blocks,
    // far below the full store, so the pool must page during the walk.
    let (ord, pq_bytes, blocks_total) = {
        let vi = gen.vamana_index("Doc", "embedding").unwrap();
        (
            vi.ord,
            vi.pq.resident_bytes(),
            vi.reader.inner().num_blocks(),
        )
    };
    let budget = pq_bytes + 64 * 1024;
    let vec_cache = VectorIndexCache::new(budget);
    vec_cache.pin(
        gen.uuid(),
        ord,
        gen.vamana_index("Doc", "embedding").unwrap().pq.clone(),
    );

    let k = 10;
    let queries = 20;
    let mut recall_sum = 0.0f64;
    for qi in 0..queries {
        // A query near a stored vector, lightly perturbed.
        let mut q = raw[(qi * 97) % fix.n].clone();
        q[0] += 0.05;

        // Brute-force ground truth (cosine over the raw vectors).
        let mut truth: Vec<(f64, u64)> = raw
            .iter()
            .enumerate()
            .map(|(i, v)| (1.0 - vector::cosine_similarity(&q, v), i as u64))
            .collect();
        truth.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let truth_k: std::collections::HashSet<u64> =
            truth.iter().take(k).map(|(_, id)| *id).collect();

        let mut params = HashMap::new();
        params.insert(
            "q".to_string(),
            Val::List(q.iter().map(|x| Val::Float(*x as f64)).collect()),
        );
        let engine = Engine::new(&gen, &block_cache)
            .with_vector_cache(&vec_cache, 96)
            .with_params(params);
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Doc', 'embedding', 10, $q) \
                 YIELD node, score RETURN id(node) AS id, score",
        )
        .unwrap();
        let res = engine.run(&ast).unwrap();
        assert!(res.rows.len() <= k);
        // Scores are ascending cosine distances (the brute-force contract).
        let mut prev = f64::NEG_INFINITY;
        let got: std::collections::HashSet<u64> = res
            .rows
            .iter()
            .map(|r| {
                if let Val::Float(s) = r[1] {
                    assert!(s + 1e-6 >= prev, "scores must be ascending");
                    prev = s;
                }
                match r[0] {
                    Val::Int(n) => n as u64,
                    _ => panic!("id(node) should be an integer"),
                }
            })
            .collect();
        let found = truth_k.iter().filter(|id| got.contains(id)).count();
        recall_sum += found as f64 / k as f64;
    }
    let recall = recall_sum / queries as f64;
    assert!(
        recall >= 0.8,
        "Vamana recall@{k} was {recall:.3}, expected ≥ 0.8"
    );

    // Bounded memory: the pool never grew past its budget (+ at most one
    // oversized block), and held only a fraction of the store's blocks.
    assert!(
        vec_cache.bytes() <= budget + fix.vector_block_size,
        "vector pool {} exceeded budget {}",
        vec_cache.bytes(),
        budget
    );
    assert!(
        vec_cache.block_count() < blocks_total,
        "paged in {} of {} blocks — the whole store should never be resident",
        vec_cache.block_count(),
        blocks_total
    );
    assert!(
        blocks_total > 16,
        "test needs the store to span many blocks"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// **The hole contract (v8).** `.pq` `node_ids[i] == HOLE` ⇒ layout ordinal `i` is a
/// tombstoned record: **never emitted, still navigated through**.
///
/// The two halves fail in opposite directions, and both are silent:
///  * emit a hole ⇒ a deleted vector comes back as a live node;
///  * *prune* a hole from the walk instead of just from the results ⇒ whatever lies
///    behind it becomes unreachable and recall on the **live** nodes quietly drops.
///
/// So this holes the **medoid** — the fixed entry point of every beam search — along
/// with the query's own nearest neighbours. If a hole were dropped from navigation, a
/// holed medoid would isolate the entry point and recall for the whole index would go
/// to **zero**, which is precisely the failure mode `AnnMode::Vamana::medoid` warns
/// about. Passing at ≥ 0.9 recall *over the live set* is therefore a direct assertion
/// that a hole is still a waypoint.
#[test]
fn vamana_hole_is_a_waypoint_but_never_emitted() {
    let fix = testgen::VamanaFixture {
        n: 2000,
        dim: 32,
        r: 24,
        alpha: 1.2,
        pq_subspaces: 8,
        pq_bits: 8,
        vector_block_size: 8192,
    };
    let k = 10;
    let queries = 20;
    // The queries this test will actually issue.
    let query_of = |qi: usize, raw: &[Vec<f32>]| -> Vec<f32> {
        let mut q = raw[(qi * 97) % fix.n].clone();
        q[0] += 0.05;
        q
    };
    let rank_by_distance = |q: &[f32], raw: &[Vec<f32>]| -> Vec<(f64, u64)> {
        let mut v: Vec<(f64, u64)> = raw
            .iter()
            .enumerate()
            .map(|(i, x)| (1.0 - vector::cosine_similarity(q, x), i as u64))
            .collect();
        v.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        v
    };

    // Pick the victims: the **true top-2 of every query the loop below issues**. That
    // choice is load-bearing. Holing nodes that no query would have returned anyway
    // makes the suppression assertion vacuous — it passes whether or not the sentinel
    // is honoured, because the hole was never a top-k candidate in the first place.
    // These are the two nodes each query *must* have surfaced, so a hole that leaks
    // has nowhere to hide. (Deriving them needs the vectors, and the fixture only
    // yields them once written — so build once, choose, rebuild with them holed.)
    let (probe_root, _probe_graph, raw) = testgen::write_vamana("exec_vamana_hole_probe", &fix);
    let victims: HashSet<u64> = (0..queries)
        .flat_map(|qi| {
            rank_by_distance(&query_of(qi, &raw), &raw)
                .into_iter()
                .take(2)
                .map(|(_, id)| id)
                .collect::<Vec<_>>()
        })
        .collect();
    let _ = std::fs::remove_dir_all(&probe_root);

    // Rebuild with those, **and the medoid**, holed.
    let holed_extra = victims.clone();
    let (root, graph, raw2, medoid_node_id) =
        testgen::write_vamana_holed("exec_vamana_hole", &fix, move |id, is_medoid| {
            is_medoid || holed_extra.contains(&id)
        });
    assert_eq!(raw2, raw, "the fixture must be deterministic across builds");
    let mut holed: HashSet<u64> = victims.clone();
    holed.insert(medoid_node_id);

    let gen = Generation::open(&root, &graph).unwrap();
    let vi_desc = &gen.manifest().vector_indexes[0];
    // `count` is the RECORD count — holes included. It is what bounds a neighbour
    // ordinal, so it must not shrink when records are tombstoned.
    assert_eq!(vi_desc.count, fix.n as u64);
    assert_eq!(vi_desc.live_count(), (fix.n - holed.len()) as u64);
    assert!((vi_desc.dead_ratio() - holed.len() as f64 / fix.n as f64).abs() < 1e-12);

    let block_cache = BlockCache::new(1 << 20);
    let (ord, pq_bytes) = {
        let vi = gen.vamana_index("Doc", "embedding").unwrap();
        (vi.ord, vi.pq.resident_bytes())
    };
    assert_eq!(
        gen.vamana_index("Doc", "embedding")
            .unwrap()
            .pq
            .live_count(),
        fix.n - holed.len()
    );
    let vec_cache = VectorIndexCache::new(pq_bytes + 64 * 1024);
    vec_cache.pin(
        gen.uuid(),
        ord,
        gen.vamana_index("Doc", "embedding").unwrap().pq.clone(),
    );

    // Ground truth: brute force over the **live** set only. A hole is deleted, so the
    // truth a correct index must reproduce is the truth without it.
    let mut recall_sum = 0.0f64;
    for qi in 0..queries {
        let q = query_of(qi, &raw);
        let ranked = rank_by_distance(&q, &raw);
        // Premise check: this query really is dominated by holed nodes — its true
        // nearest neighbour is one. Without this the emit assertion below proves
        // nothing.
        assert!(holed.contains(&ranked[0].1));
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
            .with_vector_cache(&vec_cache, 96)
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
        // (a) A hole is never emitted — not the medoid, not the query's own nearest.
        for id in &holed {
            assert!(
                !got.contains(id),
                "holed node {id} was emitted (medoid = {medoid_node_id})"
            );
        }
        // And the sentinel itself must never leak out as a node id.
        assert!(!got.contains(&graph_format::pq::HOLE));

        let found = truth_k.iter().filter(|id| got.contains(id)).count();
        recall_sum += found as f64 / k as f64;
    }
    // (b) A hole is still a waypoint. The entry point of every search is holed; if
    // holes were pruned from the walk rather than only from the results, this would be
    // 0.0, not ≥ 0.9.
    let recall = recall_sum / queries as f64;
    assert!(
        recall >= 0.9,
        "recall@{k} over the live set was {recall:.3} with the medoid holed — a hole \
             must stay navigable, not just unemitted"
    );

    let _ = std::fs::remove_dir_all(&root);
}
