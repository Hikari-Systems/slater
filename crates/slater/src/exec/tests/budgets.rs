// SPDX-License-Identifier: Apache-2.0
//! `budgets` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Regex limits + per-query intermediate budget (Tier-2 hardening) ──────

/// Run `q` with the per-query budget OFF and a server-wide budget set. Asserts
/// the universal invariant — every query refunds its whole global charge, so
/// the live counter returns to zero — and returns `(result, peak_charge)`.
///
/// The `.with_max_intermediate(0)` is what makes the "per-query budget OFF" in that
/// sentence true, and it is not optional: `Engine::new` defaults to
/// [`DEFAULT_MAX_INTERMEDIATE`](crate::exec::DEFAULT_MAX_INTERMEDIATE), so without it
/// every caller here silently runs under a live 1M per-query ceiling. Each of these
/// tests names a *global*-budget behaviour, so a per-query trip would fail them with the
/// wrong error — `is_global_budget_err` would return false and the failure would read as
/// a server-wide-budget regression that does not exist.
fn run_global(root_tag: &str, global: u64, q: &str) -> (Result<QueryResult>, u64) {
    let (root, graph, _) = testgen::write_basic(root_tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(global);
    let engine = Engine::new(&gen, &cache)
        .with_max_intermediate(0)
        .with_global_budget(&budget);
    let ast = parser::parse(q).unwrap();
    let res = engine.run(&ast);
    let peak = budget.peak();
    assert_eq!(
        budget.in_use(),
        0,
        "every query must refund its whole global charge"
    );
    let _ = std::fs::remove_dir_all(&root);
    (res, peak)
}

/// Run `q` with BOTH the per-query and the server-wide budget set, so a test
/// can assert which guard trips first. Also asserts the global refund invariant.
fn run_both(root_tag: &str, per_query: u64, global: u64, q: &str) -> Result<QueryResult> {
    let (root, graph, _) = testgen::write_basic(root_tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(global);
    let engine = Engine::new(&gen, &cache)
        .with_max_intermediate(per_query)
        .with_global_budget(&budget);
    let ast = parser::parse(q).unwrap();
    let res = engine.run(&ast);
    assert_eq!(
        budget.in_use(),
        0,
        "query must refund its whole global charge"
    );
    let _ = std::fs::remove_dir_all(&root);
    res
}

/// True if `res` is the per-query budget error.
fn is_per_query_budget_err(res: &Result<QueryResult>) -> bool {
    res.as_ref().err().is_some_and(|e| {
        format!("{e:#}").contains("intermediate result budget")
            && !format!("{e:#}").contains("server-wide")
    })
}

/// True if `res` is the server-wide budget error.
fn is_global_budget_err(res: &Result<QueryResult>) -> bool {
    res.as_ref()
        .err()
        .is_some_and(|e| format!("{e:#}").contains("server-wide intermediate budget"))
}

/// True if `res` is the transient walk-work (`query.maxScan`) error — the budget a
/// count-pushdown traversal charges instead of the retained `maxIntermediate`.
fn is_scan_budget_err(res: &Result<QueryResult>) -> bool {
    res.as_ref()
        .err()
        .is_some_and(|e| format!("{e:#}").contains("scan budget"))
}

#[test]
fn regex_pattern_length_is_capped() {
    // A pattern past MAX_REGEX_PATTERN_BYTES is refused before compilation.
    let long = "a".repeat(2 * MAX_REGEX_PATTERN_BYTES);
    let err = run_err("exec_regex_len", &format!("RETURN 'a' =~ '{long}'"));
    assert!(
        err.contains("regex pattern is"),
        "expected the pattern-length error, got: {err}"
    );
}

#[test]
fn regex_size_limit_is_enforced() {
    // Well under the length cap in source bytes, but the compiled automaton
    // (a^100M via nested bounded repetition) blows the NFA size limit.
    let err = run_err(
        "exec_regex_size",
        "RETURN 'a' =~ '((((a{100}){100}){100}){100})'",
    );
    assert!(
        err.contains("Invalid regex"),
        "expected a size-limit compile error, got: {err}"
    );
}

#[test]
fn regex_cache_compiles_once_per_query() {
    let (root, graph, _) = testgen::write_basic("exec_regex_cache");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    // `=~` evaluates once per Person row; the pattern must compile once.
    let ast = parser::parse("MATCH (n:Person) WHERE n.name =~ 'A.*' RETURN n.name").unwrap();
    let res = engine.run(&ast).unwrap();
    assert_eq!(col0(&res), vec!["Alice"]);
    assert_eq!(
        engine.regex_cache.borrow().len(),
        1,
        "one constant pattern should occupy exactly one cache slot"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn intermediate_budget_caps_comprehension() {
    // range(0, 100000) charges ~100k; the comprehension's output charges
    // another ~100k, so a 150k budget trips inside the comprehension itself.
    let err = run_budgeted(
        "exec_budget_comp",
        150_000,
        "RETURN [x IN range(0, 100000) | x]",
    )
    .expect_err("the comprehension must exceed the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
}

#[test]
fn intermediate_budget_caps_concat_doubling() {
    // acc + acc doubles per iteration; charging every temp trips the budget
    // after ~12 iterations instead of allocating 2^30 elements.
    let err = run_budgeted(
        "exec_budget_concat",
        10_000,
        "RETURN size(reduce(acc = [0], x IN range(1, 30) | acc + acc))",
    )
    .expect_err("geometric list growth must exceed the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
}

#[test]
fn intermediate_budget_caps_unwind() {
    // range(0, 1000) charges ~1k and fits; the UNWIND'd rows charge ~1k more
    // and trip a 1.5k budget inside apply_unwind.
    let err = run_budgeted(
        "exec_budget_unwind",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN count(x)",
    )
    .expect_err("the unwound rows must exceed the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
}

#[test]
fn global_budget_bounds_concurrent_aggregate() {
    // The mechanism the per-query cap cannot provide: two "in-flight" queries
    // charging against one shared budget. Each is individually fine, but their
    // sum trips the ceiling — and the charge is held until each query refunds.
    let b = GlobalIntermediateBudget::new(1_000);
    assert!(b.try_charge(600), "query A within the ceiling");
    assert!(!b.try_charge(600), "query A+B exceed the ceiling");
    assert_eq!(b.in_use(), 1_200, "both charges live until refunded");
    b.release(600);
    assert_eq!(b.in_use(), 600);
    b.release(600);
    assert_eq!(b.in_use(), 0, "all refunded");
    assert_eq!(b.peak(), 1_200, "peak records the high-water");
}

#[test]
fn global_budget_zero_disables() {
    let b = GlobalIntermediateBudget::new(0);
    assert!(b.try_charge(10_000_000), "a 0 limit never rejects");
    assert_eq!(b.in_use(), 0, "a disabled guard never accumulates");
}

#[test]
fn global_budget_trips_with_per_query_off() {
    // Per-query budget disabled (0), but the server-wide guard still bounds the
    // query — and the distinct error names the global knob.
    let (root, graph, _) = testgen::write_basic("exec_global_solo");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(1_500);
    let engine = Engine::new(&gen, &cache)
        .with_max_intermediate(0)
        .with_global_budget(&budget);
    let ast = parser::parse("UNWIND range(0, 1000) AS x RETURN count(x)").unwrap();
    let err = engine
        .run(&ast)
        .expect_err("the global budget must trip with the per-query budget off");
    assert!(
        format!("{err:#}").contains("server-wide intermediate budget"),
        "expected the global-budget error, got: {err:#}"
    );
    assert_eq!(
        budget.in_use(),
        0,
        "a failed query refunds its whole charge"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_default_engine_is_budgeted_not_unlimited() {
    // `Engine::new` used to leave `max_intermediate` at 0 — and 0 means *unlimited*, not
    // "no budget". So every caller that forgot `.with_max_intermediate()` got an unbounded
    // query, and the failure mode was the machine's OOM killer rather than a `FAILURE` on
    // the wire. The serving paths all set it; the footgun was aimed at everything else
    // (tools, benches, and any new call site), and it went off at least once.
    //
    // The default is now `DEFAULT_MAX_INTERMEDIATE`, so an unconfigured engine trips on a
    // query that materialises more than that.
    //
    // The vehicle has to be a *nested* UNWIND, not one big `range()`: `range()` carries its
    // own hardcoded `MAX_RANGE_LEN` of 1M (eval.rs) and refuses to build a longer list at
    // all, so a single `range(0, 1500000)` fails inside the function and never reaches the
    // budget — it proves nothing about this fix. 1101 × 1101 = 1_212_201 charged elements
    // clears the 1M default while each individual `range()` stays legal. Verified red
    // before the fix: the unbudgeted engine returned `count(*) = 1212201` instead of failing.
    let (root, graph, _) = testgen::write_basic("exec_default_budget");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let ast =
        parser::parse("UNWIND range(0, 1100) AS a UNWIND range(0, 1100) AS b RETURN count(*)")
            .unwrap();

    let err = Engine::new(&gen, &cache)
        .run(&ast)
        .expect_err("an engine with no explicit budget must still be bounded");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the per-query budget error, got: {err:#}"
    );

    // The escape hatch is still there, but it now has to be asked for by name. Prove it
    // on a *small* query rather than by running the 1.2M-row one unbudgeted: 10_201
    // elements is far above a budget of 10 and far below anything worth allocating, so
    // the pair below isolates the sentinel without this test — which runs in the default
    // `cargo test` suite, in parallel, on the mandatory pre-tag job — ever holding ~150 MB
    // of `Val`s with its guard deliberately switched off. Running an engine unbudgeted is
    // exactly what this commit exists to prevent; the test for it should not be the one
    // place that does it at scale.
    let small =
        parser::parse("UNWIND range(0, 100) AS a UNWIND range(0, 100) AS b RETURN count(*)")
            .unwrap();
    Engine::new(&gen, &cache)
        .with_max_intermediate(10)
        .run(&small)
        .expect_err("a budget of 10 must reject 10_201 elements");
    Engine::new(&gen, &cache)
        .with_max_intermediate(0)
        .run(&small)
        .expect("an explicit 0 still disables the budget");

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn global_budget_refunds_after_successful_run() {
    let (root, graph, _) = testgen::write_basic("exec_global_refund");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(10_000);
    let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
    let ast = parser::parse("UNWIND range(0, 100) AS x RETURN count(x)").unwrap();
    engine.run(&ast).expect("well within the budget");
    assert_eq!(budget.in_use(), 0, "a finished query holds no charge");
    assert!(budget.peak() > 0, "it did draw on the budget mid-run");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn global_budget_rises_during_run_and_falls_after() {
    // Observe the live gauge from a second thread *while* a query executes: the
    // global charge must climb above zero during the run and return to zero
    // when it completes (the shared in-flight accounting, end to end).
    use std::sync::atomic::{AtomicBool, Ordering};
    let (root, graph, _) = testgen::write_basic("exec_global_inflight");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    // Generous ceiling so the query never trips the guard; it still charges
    // ~900k elements and holds them for the whole run, so the reader can see it
    // climb. (range() itself caps at 1M elements, so stay under that here.)
    //
    // The per-query budget is disabled explicitly below. This test is about the
    // *global* gauge, and the total charge (the range list, the UNWIND expansion and
    // the aggregate each draw) lands just over `DEFAULT_MAX_INTERMEDIATE` — so once
    // `Engine::new` stopped defaulting to unlimited, the per-query guard tripped first
    // and the run never reached the thing being measured.
    let budget = GlobalIntermediateBudget::new(100_000_000);
    let done = AtomicBool::new(false);
    let mut max_live = 0u64;
    std::thread::scope(|s| {
        s.spawn(|| {
            let engine = Engine::new(&gen, &cache)
                .with_max_intermediate(0)
                .with_global_budget(&budget);
            let ast = parser::parse("UNWIND range(0, 900000) AS x RETURN count(x)").unwrap();
            engine.run(&ast).expect("within the budget");
            done.store(true, Ordering::Release);
        });
        // Sample the live gauge until the query thread signals completion,
        // yielding each iteration so the worker is not starved (the sampler
        // must not monopolise a constrained scheduler). The deadline is a
        // safety net so a stuck query fails the test rather than hanging it.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !done.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
            max_live = max_live.max(budget.in_use());
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
    });
    assert!(
        max_live > 0,
        "the global charge must be observable above zero while the query runs"
    );
    assert_eq!(
        budget.in_use(),
        0,
        "the charge must fall back to zero once the query completes"
    );
    assert!(budget.peak() >= max_live, "peak tracks the live high-water");
    let _ = std::fs::remove_dir_all(&root);
}

// ── Per-query budget across every materialising operation ────────────────

#[test]
fn intermediate_budget_caps_collect() {
    // collect() buffers all inputs; charging the buffer trips a tight budget.
    let err = run_budgeted(
        "exec_budget_collect",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    )
    .expect_err("the collect buffer must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_caps_count_distinct() {
    // count(DISTINCT x) holds a `seen` set; charging it trips the budget.
    let err = run_budgeted(
        "exec_budget_distinct",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN count(DISTINCT x)",
    )
    .expect_err("the DISTINCT seen-set must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_caps_order_by() {
    // ORDER BY clones every row plus its sort key into a buffer (charged).
    let err = run_budgeted(
        "exec_budget_order",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x ORDER BY x",
    )
    .expect_err("the ORDER BY buffer must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_caps_group_by() {
    // A distinct grouping key per row creates ~N groups; charging each group
    // (plus the unwound rows) trips the budget.
    let err = run_budgeted(
        "exec_budget_group",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x AS g, count(*) AS n",
    )
    .expect_err("the group table must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_caps_union() {
    // A UNION accumulates both branches (and a DISTINCT seen-set); a tight
    // budget trips while building it.
    let err = run_budgeted(
        "exec_budget_union",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x \
             UNION UNWIND range(0, 1000) AS y RETURN y",
    )
    .expect_err("the UNION buildup must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_zero_disables_the_cap() {
    // A 0 budget means unlimited: a large materialisation completes.
    let res = run_budgeted(
        "exec_budget_zero",
        0,
        "UNWIND range(0, 200000) AS x RETURN count(x)",
    )
    .expect("a 0 budget must not cap anything");
    assert_eq!(res.rows.len(), 1);
}

#[test]
fn intermediate_budget_allows_within_limit() {
    // Comfortably under the cap → the query succeeds.
    let res = run_budgeted(
        "exec_budget_within",
        100_000,
        "UNWIND range(0, 1000) AS x RETURN count(x)",
    )
    .expect("a query within the budget must succeed");
    assert_eq!(res.rows.len(), 1);
}

#[test]
fn intermediate_budget_threshold_passes_then_trips() {
    // The same materialisation passes under a generous cap and trips under a
    // tight one — the budget actually gates on the charged element count.
    run_budgeted(
        "exec_budget_thresh_ok",
        50_000,
        "RETURN [x IN range(0, 1000) | x]",
    )
    .expect("generous budget passes");
    let err = run_budgeted(
        "exec_budget_thresh_no",
        1_500,
        "RETURN [x IN range(0, 1000) | x]",
    )
    .expect_err("tight budget trips");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

// ── Server-wide budget across the same operations ────────────────────────

#[test]
fn global_budget_trips_on_comprehension() {
    let (res, _) = run_global("exec_g_comp", 1_500, "RETURN [x IN range(0, 1000) | x]");
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_on_collect() {
    let (res, _) = run_global(
        "exec_g_collect",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_on_count_distinct() {
    let (res, _) = run_global(
        "exec_g_distinct",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN count(DISTINCT x)",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_on_order_by() {
    let (res, _) = run_global(
        "exec_g_order",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x ORDER BY x",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_on_union() {
    let (res, _) = run_global(
        "exec_g_union",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x UNION UNWIND range(0, 1000) AS y RETURN y",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_allows_small_query() {
    let (res, peak) = run_global("exec_g_small", 100_000, "RETURN [x IN range(0, 50) | x]");
    assert!(res.is_ok(), "a small query must not trip: {res:?}");
    assert!(peak > 0, "it still drew on the budget");
}

#[test]
fn global_budget_zero_completes_large() {
    // Per-query off and global 0 → no cap; a large materialisation completes
    // and the (disabled) counter never accumulates.
    //
    // "Per-query off" is `run_global`'s doing, and it is load-bearing: with that
    // `.with_max_intermediate(0)` removed, a shape above `DEFAULT_MAX_INTERMEDIATE`
    // fails here with `query.maxIntermediate` instead — the wrong budget entirely.
    // Verified by mutation rather than assumed.
    let (res, peak) = run_global(
        "exec_g_zero",
        0,
        "UNWIND range(0, 200000) AS x RETURN count(x)",
    );
    assert!(res.is_ok(), "0 disables the guard: {res:?}");
    assert_eq!(peak, 0, "a disabled guard never accumulates");
}

#[test]
fn global_budget_refunds_after_a_trip() {
    // run_global already asserts in_use == 0; make the failure path explicit.
    let (res, _) = run_global(
        "exec_g_refund_fail",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    );
    assert!(is_global_budget_err(&res), "expected a trip: {res:?}");
}

// ── Interaction of the two budgets ───────────────────────────────────────

#[test]
fn per_query_budget_trips_first_when_tighter() {
    // Tighter per-query cap (1500) beneath a roomy global (10M) → the per-query
    // guard fires, named by its own error.
    let res = run_both(
        "exec_both_pq",
        1_500,
        10_000_000,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    );
    assert!(is_per_query_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_first_when_tighter() {
    // Tighter global (1500) beneath a roomy per-query cap (10M) → the
    // server-wide guard fires, named by its own error.
    let res = run_both(
        "exec_both_g",
        10_000_000,
        1_500,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn both_budgets_off_completes_large() {
    let res = run_both(
        "exec_both_off",
        0,
        0,
        "UNWIND range(0, 200000) AS x RETURN count(x)",
    );
    assert!(res.is_ok(), "both budgets off → no cap: {res:?}");
}

// ── Expansion charge: a hub read must trip the budget (root cause 2b) ─────

/// Few-thousand-edge hub; comfortably clears `EXPAND_PAR_MIN` (64) so the pooled
/// reader fans out, and small enough to build in well under a millisecond.
const HUB_N: u64 = 3_000;
/// Far below `HUB_N`, so a single hub expansion (which charges ~`HUB_N`) trips it.
const HUB_TIGHT: u64 = 100;
/// Far above the whole star's cumulative charge (~a few × `HUB_N`), so a full
/// expansion completes — the guard must bound hubs without over-charging.
const HUB_GENEROUS: u64 = 10_000_000;

/// Run `q` against an `n`-leaf hub fixture (see [`testgen::write_hub`]) with the
/// given per-query and server-wide budgets (0 disables either), optionally behind
/// a fanout pool so the parallel `expand_chain_par` path is exercised. Asserts the
/// universal refund invariant and returns `(result, global_peak)`.
fn run_hub(
    tag: &str,
    n: u64,
    per_query: u64,
    scan: u64,
    global: u64,
    with_pool: bool,
    q: &str,
) -> (Result<QueryResult>, u64) {
    let (root, graph) = testgen::write_hub(tag, n);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(global);
    let mut engine = Engine::new(&gen, &cache)
        .with_max_intermediate(per_query)
        .with_max_scan(scan)
        .with_global_budget(&budget);
    if with_pool {
        engine = engine.with_fanout_pool(Some(std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap(),
        )));
    }
    let res = engine.run(&parser::parse(q).unwrap());
    let peak = budget.peak();
    assert_eq!(
        budget.in_use(),
        0,
        "every query must refund its whole global charge"
    );
    let _ = std::fs::remove_dir_all(&root);
    (res, peak)
}

// ── Per-query-type budget routing (the retention split) ───────────────────
// The same hub adjacency read is charged against a *different* budget depending on
// what the query does with the rows. `RETURN count(*)` is count-pushdown — it
// retains nothing, so its reads charge the transient `maxScan` budget and never the
// retained `maxIntermediate` nor the server-wide aggregate. A row-returning or
// var-length traversal materialises, so the same reads charge `maxIntermediate`
// (and the global budget). run_hub args: (tag, n, maxIntermediate, maxScan, global).

#[test]
fn hub_count_one_hop_answered_by_degree_terminal() {
    // The degree-sum terminal answers a 1-hop `count(neighbour)` from the hub's stored
    // out-degree in O(1) — it never walks the `HUB_N`-edge adjacency, so the tight scan
    // cap the old row-by-row walk tripped is no longer even approached. (The 2-hop
    // variant below still trips: building its penultimate frontier reads the hub.)
    let (res, _) = run_hub(
        "exec_hub_1hop_degterm",
        HUB_N,
        0,
        HUB_TIGHT,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN count(x)",
    );
    let r = res.expect("degree terminal answers a 1-hop hub count without tripping maxScan");
    assert!(
        matches!(r.rows[0][0], Val::Int(n) if n == HUB_N as i64),
        "1-hop count == hub out-degree: {:?}",
        r.rows[0][0]
    );
}

#[test]
fn hub_count_two_hop_trips_scan_budget() {
    let (res, _) = run_hub(
        "exec_hub_2hop_scan",
        HUB_N,
        0,
        HUB_TIGHT,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN count(y)",
    );
    assert!(is_scan_budget_err(&res), "got: {res:?}");
}

#[test]
fn hub_count_filtered_trips_scan_with_zero_rows() {
    // 2b for counts: `:Hub` matches only the centre, so every neighbour is rejected
    // and ZERO rows complete — yet the adjacency read still charges scan and trips.
    let (res, _) = run_hub(
        "exec_hub_filt_scan",
        HUB_N,
        0,
        HUB_TIGHT,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x:Hub) RETURN count(x)",
    );
    assert!(
        is_scan_budget_err(&res),
        "a filtered count read (no rows complete) must still trip maxScan: {res:?}"
    );
}

#[test]
fn hub_count_ignores_retained_and_global_budgets() {
    // The crux of the split: with the retained *and* global budgets tight (well
    // below `HUB_N`) but scan generous, the count still completes with the right
    // answer — it draws neither — and never charges the server-wide aggregate.
    let (res, peak) = run_hub(
        "exec_hub_count_iso",
        HUB_N,
        HUB_TIGHT,
        HUB_GENEROUS,
        HUB_TIGHT,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN count(x) AS n",
    );
    let res = res.expect("a count must not draw the retained/global budgets");
    assert_eq!(col0(&res), vec![HUB_N.to_string()]);
    assert!(
        peak < HUB_N,
        "count-pushdown must not charge the per-edge reads to the server-wide \
             aggregate: peak={peak}"
    );
}

#[test]
fn hub_materialize_one_hop_trips_per_query_budget() {
    // Row-returning: the same read materialises, so it charges the retained budget.
    let (res, _) = run_hub(
        "exec_hub_1hop_pq",
        HUB_N,
        HUB_TIGHT,
        0,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN x",
    );
    assert!(
        is_per_query_budget_err(&res),
        "a materialising hub read must trip maxIntermediate: {res:?}"
    );
}

#[test]
fn hub_materialize_one_hop_trips_global_budget() {
    let (res, _) = run_hub(
        "exec_hub_1hop_g",
        HUB_N,
        0,
        0,
        HUB_TIGHT,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN x",
    );
    assert!(
        is_global_budget_err(&res),
        "a materialising hub read must trip the server-wide budget: {res:?}"
    );
}

#[test]
fn hub_materialize_two_hop_trips_per_query_budget() {
    let (res, _) = run_hub(
        "exec_hub_2hop_pq",
        HUB_N,
        HUB_TIGHT,
        0,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN y",
    );
    assert!(is_per_query_budget_err(&res), "got: {res:?}");
}

#[test]
fn hub_varlen_count_charges_retained_not_scan() {
    // The two-regime nuance the sweep found: a *var-length* `count(*)` still
    // materialises its per-node path set, so even under count-pushdown it charges
    // the retained budget (and trips it) — unlike a fixed-hop count, which is pure
    // scan. With scan disabled, the trip can only be the retained path materialise.
    let (res, _) = run_hub(
        "exec_hub_varlen_count",
        HUB_N,
        HUB_TIGHT,
        0,
        0,
        false,
        "MATCH (c:Hub)-[:LINK*1..2]->(x) RETURN count(*)",
    );
    assert!(
        is_per_query_budget_err(&res),
        "a var-length count materialises paths and must trip maxIntermediate: {res:?}"
    );
}

#[test]
fn frame_get_flatten_shadowing() {
    // Pins the shadowing convention that makes the parallel walk match the
    // sequential LIFO oracle: a child frame shadows its parent, the last write in
    // a layer wins, and `flatten` (root-first) reproduces both.
    use std::sync::Arc;
    let mut base = HashMap::new();
    base.insert("a".to_string(), Val::Int(1));
    base.insert("b".to_string(), Val::Int(2));
    let root = Frame::root(&base);
    let child = Arc::new(Frame {
        parent: Some(root),
        delta: vec![("b".into(), Val::Int(20))],
    });
    let grand = Arc::new(Frame {
        parent: Some(child),
        delta: vec![("a".into(), Val::Int(100)), ("a".into(), Val::Int(101))],
    });
    assert!(
        matches!(grand.get("b"), Some(Val::Int(20))),
        "child shadows parent"
    );
    assert!(
        matches!(grand.get("a"), Some(Val::Int(101))),
        "last delta wins"
    );
    assert!(grand.get("c").is_none());
    let flat = grand.flatten();
    assert_eq!(flat.len(), 2);
    assert!(matches!(flat.get("a"), Some(Val::Int(101))));
    assert!(matches!(flat.get("b"), Some(Val::Int(20))));
}

#[test]
fn count_pushdown_matches_materialized() {
    // The pushed-down `count(*)`/`count(v)` must equal the row count the
    // materialising path produces — across 1/2/3-hop, a constant co-item, and an
    // empty match. (write_basic: KNOWS Alice->Bob, Bob->Carol.)
    let count_of = |tag: &str, q: &str| -> i64 {
        match &run_budgeted(tag, 1_000_000, q).unwrap().rows[0][0] {
            Val::Int(n) => *n,
            o => panic!("count is not an Int: {o:?}"),
        }
    };
    let rows_of =
        |tag: &str, q: &str| -> usize { run_budgeted(tag, 1_000_000, q).unwrap().rows.len() };
    let cases = [
        (
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN count(*) AS c",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name AS b",
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(c) AS c",
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name AS x",
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN count(*) AS c, 7 AS k",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name AS b",
        ),
        (
            // empty: 3-hop KNOWS dead-ends at Carol.
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN count(*) AS c",
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN d.name AS d",
        ),
    ];
    for (cq, rq) in cases {
        assert_eq!(
            count_of("cpd_eq", cq) as usize,
            rows_of("cpd_eq", rq),
            "`{cq}`"
        );
    }
}

#[test]
fn count_pushdown_falls_back_but_correct() {
    // Shapes that must NOT push down still return the correct count via the
    // materialising path.
    let count_of = |q: &str| -> i64 {
        match &run_budgeted("cpd_fb", 1_000_000, q).unwrap().rows[0][0] {
            Val::Int(n) => *n,
            o => panic!("count is not an Int: {o:?}"),
        }
    };
    // count(DISTINCT) — KNOWS targets {Bob, Carol} = 2 distinct (not pushed: needs
    // the value set), vs 3 total KNOWS edges (Alice->Bob, Bob->Carol, Alice->Carol).
    assert_eq!(
        count_of("MATCH (a:Person)-[:KNOWS]->(b) RETURN count(DISTINCT b) AS c"),
        2
    );
    // WHERE survivor filter — only Alice->Bob of the 3 KNOWS edges (falls back to
    // the materialising path, which applies WHERE).
    assert_eq!(
        count_of("MATCH (a:Person)-[:KNOWS]->(b) WHERE b.name = 'Bob' RETURN count(*) AS c"),
        1
    );
}

#[test]
fn hub_small_expansion_succeeds_under_a_generous_budget() {
    // The guard must bound hubs without over-charging: a generous scan budget lets
    // the whole star expand and return the right count. A materialising run of the
    // same shape really draws the server-wide aggregate (≥ one charge per edge read).
    let (res, _) = run_hub(
        "exec_hub_small_ok",
        HUB_N,
        HUB_GENEROUS,
        HUB_GENEROUS,
        HUB_GENEROUS,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN count(x) AS n",
    );
    let res = res.expect("a generous budget must let the hub expand");
    assert_eq!(col0(&res), vec![HUB_N.to_string()]);
    let (mat, peak) = run_hub(
        "exec_hub_small_mat",
        HUB_N,
        HUB_GENEROUS,
        0,
        HUB_GENEROUS,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN x",
    );
    mat.expect("materialise under a generous budget");
    assert!(
        peak >= HUB_N,
        "a materialising expansion must charge the aggregate ≥ once per edge read: peak={peak}"
    );
}

#[test]
fn hub_expansion_charge_on_parallel_path() {
    // The fanout pool routes a fixed multi-hop chain through `expand_chain_par`,
    // whose adjacency reads gather on rayon — where the per-query `Cell` charge
    // state cannot be touched. The charge is applied on the calling thread once the
    // buffer lands, so the pooled walk routes to the SAME budget as the sequential
    // one: a count trips `maxScan`, a materialising walk trips `maxIntermediate` /
    // the global budget, and under generous budgets both return the sequential
    // result. The hop-1 frontier (`HUB_N` leaves) clears `EXPAND_PAR_MIN`, so the
    // pooled reader truly fans out rather than degrading to a sequential read.
    let cq = "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN count(y) AS n";
    let mq = "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN y";
    // count-pushdown on the pooled path → scan budget.
    let (scan, _) = run_hub("exec_hub_par_scan", HUB_N, 0, HUB_TIGHT, 0, true, cq);
    assert!(
        is_scan_budget_err(&scan),
        "the pooled count must trip maxScan: {scan:?}"
    );
    // materialising on the pooled path → retained + global budgets.
    let (pq, _) = run_hub("exec_hub_par_pq", HUB_N, HUB_TIGHT, 0, 0, true, mq);
    assert!(
        is_per_query_budget_err(&pq),
        "the pooled materialising walk must trip maxIntermediate: {pq:?}"
    );
    let (g, _) = run_hub("exec_hub_par_g", HUB_N, 0, 0, HUB_TIGHT, true, mq);
    assert!(
        is_global_budget_err(&g),
        "the pooled materialising walk must trip the server-wide budget: {g:?}"
    );
    // Generous budgets: pooled and sequential counts agree exactly.
    let (par, _) = run_hub(
        "exec_hub_par_ok",
        HUB_N,
        HUB_GENEROUS,
        HUB_GENEROUS,
        HUB_GENEROUS,
        true,
        cq,
    );
    let (seq, _) = run_hub(
        "exec_hub_seq_ok",
        HUB_N,
        HUB_GENEROUS,
        HUB_GENEROUS,
        HUB_GENEROUS,
        false,
        cq,
    );
    let par = par.expect("pooled generous run");
    let seq = seq.expect("sequential generous run");
    assert_eq!(
        col0(&par),
        col0(&seq),
        "pooled and sequential expansions must agree"
    );
    assert_eq!(col0(&par), vec![HUB_N.to_string()]);
}

#[test]
fn engine_is_not_sync_rayon_invariant() {
    // Compile-time guard-rail for the rayon-safety invariant. The entire argument
    // that `par_gather`/`par_walk` are race-free rests on `&Engine` never crossing a
    // thread boundary: the `Sync + Send` bound on `par_gather`'s closure can only
    // reject a closure that captures `&self` *because* `Engine` is `!Sync` (its
    // per-query `Cell`/`RefCell` charge state — `budget_used`, `scan_used`,
    // `count_acc`, `global_charged`, `regex_cache`). If a future change makes that
    // state `Sync` (e.g. swapping a `Cell` for an `Atomic` to "charge in parallel"),
    // the `AmbiguousIfSync` resolution below becomes ambiguous and this stops
    // compiling — forcing a deliberate re-read of `charge_walk` and the `par_gather`
    // contract before the invariant is weakened.
    trait AmbiguousIfSync<A> {
        fn _f() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}
    // Resolves to the blanket `()` impl unambiguously iff `Engine` is NOT `Sync`.
    let _ = <Engine<'static, Generation> as AmbiguousIfSync<_>>::_f;
}

// ── Engine reuse: the charge resets and refunds per run ───────────────────

#[test]
fn global_charge_resets_between_runs_on_a_reused_engine() {
    let (root, graph, _) = testgen::write_basic("exec_g_reuse");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(100_000);
    let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
    let ast = parser::parse("UNWIND range(0, 500) AS x RETURN count(x)").unwrap();
    for _ in 0..5 {
        engine.run(&ast).expect("within the budget");
        assert_eq!(budget.in_use(), 0, "each run fully refunds before the next");
    }
    // A reused engine that has succeeded many times still trips correctly when a
    // single run exceeds the budget (no stale carry-over inflating the charge).
    let big = parser::parse("UNWIND range(0, 200000) AS x RETURN collect(x)").unwrap();
    assert!(
        engine.run(&big).is_err(),
        "the oversized run must still trip"
    );
    assert_eq!(budget.in_use(), 0, "the tripped run also refunds");
    let _ = std::fs::remove_dir_all(&root);
}

// ── GlobalIntermediateBudget mechanics ───────────────────────────────────

#[test]
fn global_budget_starts_at_zero() {
    let b = GlobalIntermediateBudget::new(1_000);
    assert_eq!(b.in_use(), 0);
    assert_eq!(b.peak(), 0);
    assert_eq!(b.limit(), 1_000);
}

#[test]
fn global_budget_charge_to_exact_limit_then_trips() {
    let b = GlobalIntermediateBudget::new(1_000);
    assert!(
        b.try_charge(1_000),
        "charging exactly to the limit is allowed"
    );
    assert_eq!(b.in_use(), 1_000);
    assert!(!b.try_charge(1), "one element past the limit trips");
    b.release(1_001);
    assert_eq!(b.in_use(), 0);
}

#[test]
fn global_budget_release_cycles_return_to_zero() {
    let b = GlobalIntermediateBudget::new(10_000);
    for _ in 0..1_000 {
        assert!(b.try_charge(7));
        b.release(7);
    }
    assert_eq!(b.in_use(), 0, "balanced charge/release nets to zero");
    assert!(b.peak() >= 7, "peak captured the per-cycle high-water");
}

#[test]
fn varlen_bounds_inverted_range_stays_empty() {
    // `*5..3`: an explicit max below min is an empty range. It must NOT be clamped
    // to `5..5` (which would wrongly match exactly-length-5 walks). Every consumer
    // treats `max < min` as "no path", so the raw inverted bounds are correct.
    let vl = VarLength {
        min: Some(5),
        max: Some(3),
    };
    let (min, max) = varlen_bounds(&vl);
    assert_eq!((min, max), (5, 3));
    assert!(
        max < min,
        "inverted range must stay inverted (empty), not clamp"
    );

    // A normal range is unaffected.
    assert_eq!(
        varlen_bounds(&VarLength {
            min: Some(2),
            max: Some(4)
        }),
        (2, 4)
    );
    // An open `*` still spans 1..=MAX_VARLEN_HOPS.
    assert_eq!(
        varlen_bounds(&VarLength {
            min: None,
            max: None
        }),
        (1, MAX_VARLEN_HOPS)
    );
}

#[test]
fn varlen_charges_intermediate_budget() {
    // A tiny budget trips while materialising variable-length paths…
    let err = run_budgeted(
        "exec_budget_varlen_tiny",
        2,
        "MATCH (a)-[*1..3]->(b) RETURN count(*)",
    )
    .expect_err("varlen paths must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    // …and a generous budget leaves the same query untouched (no over-charge).
    let res = run_budgeted(
        "exec_budget_varlen_ok",
        1_000_000,
        "MATCH (a)-[*1..3]->(b) RETURN count(*)",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 1);
}

#[test]
fn correlated_unwind_seek_returns_right_rows() {
    // `UNWIND … AS w MATCH (n:Person {name:w})` keys the anchor off the per-row
    // scalar `w`. The planner now resolves it to a `node_Person_name` index seek
    // (see plan.rs `bound_scalar_*` tests); this proves the seek path is sound
    // end-to-end — the right rows, no more, no fewer.
    let (root, res) = run(
        "exec_correlated_unwind",
        "UNWIND ['Alice', 'Bob', 'Nobody'] AS w \
             MATCH (n:Person {name: w}) RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn correlated_where_seek_returns_right_rows() {
    // The `WHERE n.name = w` spelling resolves to the same per-row seek.
    let (root, res) = run(
        "exec_correlated_where",
        "UNWIND ['Carol', 'Bob'] AS w \
             MATCH (n:Person) WHERE n.name = w RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn distinct_charges_intermediate_budget() {
    // The `seen` set behind `RETURN DISTINCT` is charged: a budget that admits
    // the 3-row match (3) but not the DISTINCT pass (+3) trips; 1M is untouched.
    let err = run_budgeted(
        "exec_budget_distinct_tiny",
        5,
        "MATCH (n:Person) RETURN DISTINCT n.city",
    )
    .expect_err("DISTINCT must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted(
        "exec_budget_distinct_ok",
        1_000_000,
        "MATCH (n:Person) RETURN DISTINCT n.city",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 2); // London, Paris
}

#[test]
fn order_by_charges_intermediate_budget() {
    // The `keyed` sort buffer clones every row; charged before it is built.
    let err = run_budgeted(
        "exec_budget_order_tiny",
        5,
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
    )
    .expect_err("ORDER BY must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted(
        "exec_budget_order_ok",
        1_000_000,
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 3);
}

#[test]
fn group_by_charges_intermediate_budget() {
    // Each distinct group costs one element; a budget that admits the match (3)
    // and the first group but not the second (Paris) trips.
    let err = run_budgeted(
        "exec_budget_group_tiny",
        4,
        "MATCH (n:Person) RETURN n.city, count(*)",
    )
    .expect_err("GROUP BY must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted(
        "exec_budget_group_ok",
        1_000_000,
        "MATCH (n:Person) RETURN n.city, count(*)",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 2); // {London: 2}, {Paris: 1}
}

#[test]
fn all_shortest_frontier_charges_intermediate_budget() {
    // `ALL SHORTEST`/`SHORTEST k` keep the cloned-per-branch simple-path search
    // (the number of shortest paths can be exponential), whose BFS frontier is
    // charged per expansion layer so a hub-dense graph trips the budget mid-search
    // instead of ballooning RSS. The destination (a Company) is unreachable over
    // `:KNOWS`, so no *result* is ever charged — only the frontier — yet a tiny
    // budget still trips.
    let q = "MATCH (a:Person {name:'Alice'}), (z:Company {name:'Acme'}) \
                 MATCH ALL SHORTEST (a)-[:KNOWS*]->(z) RETURN count(*) AS c";
    let err = run_budgeted("exec_budget_allsp_tiny", 3, q)
        .expect_err("the ALL SHORTEST frontier must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted("exec_budget_allsp_ok", 1_000_000, q)
        .expect("a generous budget must not affect the query");
    assert_eq!(col0(&res), vec!["0"]); // no KNOWS path Person→Company
}

#[test]
fn all_shortest_charges_frontier_branches_proportional_to_depth() {
    // Each live shortest-path branch clones a `Vec<Hop>` + `HashSet` whose size grows with
    // its depth, but the frontier charge used a fixed `charge(1)`, under-counting a deep
    // branch by a factor of its depth. On a length-12 chain the total charge is now
    // ≈ L(L+1)/2 = 78 (proportional), where the old fixed accounting was ≈ 2L-1 = 23. A
    // budget of 40 sits between them: it admitted the old accounting but must trip the new.
    let (root, graph) = testgen::write_chain("exec_chain_depth_charge", 12);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let q = "MATCH ALL SHORTEST (a {name:'n0'})-[r:R*]->(b {name:'n12'}) RETURN size(r) AS l";
    let ast = parser::parse(q).unwrap();

    // A generous budget completes: the single length-12 path.
    let ok = Engine::new(&gen, &cache)
        .with_max_intermediate(10_000)
        .run(&ast)
        .expect("a generous budget completes the chain search");
    assert!(matches!(ok.rows[0][0], Val::Int(12)), "{:?}", ok.rows[0][0]);

    // The depth-proportional charge trips at 40; the old fixed `charge(1)` would not.
    let err = Engine::new(&gen, &cache)
        .with_max_intermediate(40)
        .run(&ast)
        .expect_err("the depth-proportional frontier charge must trip at 40");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn two_free_endpoint_selector_charges_the_search_product() {
    // A selector with two free endpoints scans all candidates on each side and launches a
    // shortest-path search for every (src, dst) pair — quadratic in the id space. On the
    // isolated fixture (8 nodes, no edges) each search does ~0 frontier work, so the old
    // code sailed under a small budget however many pairs it ran; the fix charges the 8×8
    // product up front, tripping before the fan-out runs.
    let (root, graph) = testgen::write_isolated("exec_2free_product", 8);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let two_free = "MATCH ALL SHORTEST (a)-[r:R*]->(b) RETURN count(*) AS c";
    let ast = parser::parse(two_free).unwrap();
    let err = Engine::new(&gen, &cache)
        .with_max_intermediate(20)
        .run(&ast)
        .expect_err("two free endpoints must charge |srcs|×|dsts|");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    // A generous budget still completes (no edges ⇒ no path ⇒ count 0).
    let ok = Engine::new(&gen, &cache)
        .with_max_intermediate(10_000)
        .run(&ast)
        .expect("a generous budget completes");
    assert_eq!(col0(&ok), vec!["0"]);

    // Constraining one endpoint to a single candidate drops the product to 1×8 = 8, which
    // the same budget admits — the charge bites only the pathological two-free case.
    let one_bound = "MATCH ALL SHORTEST (a {name:'n0'})-[r:R*]->(b) RETURN count(*) AS c";
    let ast2 = parser::parse(one_bound).unwrap();
    let ok2 = Engine::new(&gen, &cache)
        .with_max_intermediate(20)
        .run(&ast2)
        .expect("one constrained endpoint stays under the budget");
    assert_eq!(col0(&ok2), vec!["0"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn shortest_path_any_succeeds_under_tiny_budget() {
    // `shortestPath()`/`ANY SHORTEST` now runs a single global-`visited` BFS that
    // enqueues each node at most once and charges no frontier, so it succeeds in
    // `O(V+E)` under a budget the old cloned-per-branch search would trip on (3 is
    // below where the frontier charge fired for `all_shortest_frontier_*`). The
    // unreachable-Company probe returns NULL cheaply; a reachable pair returns its
    // length.
    let unreachable = "MATCH (a:Person {name:'Alice'}), (z:Company {name:'Acme'}) \
                           RETURN shortestPath((a)-[:KNOWS*]->(z)) IS NULL AS np";
    let res = run_budgeted("exec_budget_anysp_unreach", 3, unreachable)
        .expect("the global-visited BFS must not charge the frontier");
    assert_eq!(col0(&res), vec!["true"]); // no KNOWS path Person→Company

    let reachable = "MATCH (a:Person {name:'Alice'}), (z:Person {name:'Carol'}) \
                         RETURN length(shortestPath((a)-[:KNOWS*]->(z))) AS l";
    let res = run_budgeted("exec_budget_anysp_reach", 3, reachable)
        .expect("the global-visited BFS must not charge the frontier");
    assert_eq!(col0(&res), vec!["1"]); // Alice-[:KNOWS]->Carol directly (e4)
}

#[test]
fn shortest_path_explore_cap_bounds_the_bfs() {
    // The dedicated `maxShortestPathExplore` cap bounds the global-visited BFS
    // independently of `maxIntermediate`: the reachable pair the unlimited BFS
    // finds above fails *cleanly* (no panic, no OOM) once the discovery count
    // exceeds the cap, while the default (0 = unlimited) still succeeds and the
    // re-derived path keeps its correct length.
    let q = "MATCH (a:Person {name:'Alice'}), (z:Person {name:'Carol'}) \
                 RETURN length(shortestPath((a)-[:KNOWS*]->(z))) AS l";
    let (root, gen, cache, _) = budgeted_engine("exec_sp_explore_cap", 1_000_000);
    let err = Engine::new(&gen, &cache)
        .with_max_shortest_path_explore(1)
        .run(&parser::parse(q).unwrap())
        .expect_err("the explore cap must bound the BFS");
    assert!(
        format!("{err:#}").contains("maxShortestPathExplore"),
        "expected the explore-cap error, got: {err:#}"
    );
    let res = Engine::new(&gen, &cache)
        .with_max_shortest_path_explore(0)
        .run(&parser::parse(q).unwrap())
        .expect("the default unlimited cap must succeed");
    assert_eq!(col0(&res), vec!["1"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn shortest_path_meets_in_the_middle() {
    // A length-2 shortest path exercises the bidirectional search's meet-in-middle
    // and reconstruction *across* the meeting node — the endpoints share no direct
    // edge; the path is Acme -WORKS_AT- Alice -KNOWS- Bob (undirected, mixed type).
    let q = "MATCH (a:Company {name:'Acme'}), (b:Person {name:'Bob'}) \
                 RETURN length(shortestPath((a)-[*..6]-(b))) AS l";
    let res = run_budgeted("exec_sp_midmeet", 1_000_000, q).expect("a length-2 path exists");
    assert_eq!(col0(&res), vec!["2"]);
}

#[test]
fn shortest_path_with_pool_is_correct() {
    // A pool-configured engine must return identical results to the sequential one
    // (the parallel frontier gather shares the same neighbour logic; the full-graph
    // benchmark exercises the large-frontier rayon branch).
    let (root, gen, cache, _) = budgeted_engine("exec_sp_pool", 1_000_000);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap(),
    );
    let q = "MATCH (a:Company {name:'Acme'}), (b:Person {name:'Bob'}) \
                 RETURN length(shortestPath((a)-[*..6]-(b))) AS l";
    let res = Engine::new(&gen, &cache)
        .with_fanout_pool(Some(pool))
        .run(&parser::parse(q).unwrap())
        .expect("pool-configured shortestPath runs");
    assert_eq!(col0(&res), vec!["2"]);
    let _ = std::fs::remove_dir_all(&root);
}

/// Slice 2 integration: routing a hub through the streaming reader must return
/// **identical** results to materialising it — for both the sequential (`expand_chain`)
/// and pooled (`par_walk`) engines, over count / ordered-rows / undirected / path-var /
/// relationship-property (`rel_ok`) shapes. Driven by a low `adj_stream_threshold` (2)
/// so `write_basic`'s degree-3 anchor (Alice) streams while its degree-1 neighbours
/// materialise — a genuine hub/normal mix in one frontier. Each query is run four ways
/// (seq/pool × stream/materialise); all four must agree byte-for-byte.
#[test]
fn hub_streaming_matches_materialise() {
    let (root, gen, cache, _) = budgeted_engine("exec_hub_stream", 1_000_000);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let disp = |r: &QueryResult| -> Vec<Vec<String>> {
        r.rows
            .iter()
            .map(|row| row.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    let queries = [
        // 2-hop count — the count-pushdown terminal over a streamed hub frontier.
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(*)",
        // 2-hop ordered rows with rel + node vars bound (Alice is the streamed hub).
        "MATCH (a:Person)-[r1:KNOWS]->(b)-[r2:KNOWS]->(c) \
             RETURN a.name AS a, b.name AS b, c.name AS c ORDER BY a, b, c",
        // Type-alternation one-hop from the hub anchor (mixes KNOWS + WORKS_AT out-edges).
        "MATCH (a:Person {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b ORDER BY b",
        // Same, UNORDERED — locks row-order preservation (streamed hop order must equal
        // the materialised `hops_par` order, not merely the same set).
        "MATCH (a:Person {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b",
        // Undirected one-hop from the hub anchor (outgoing-then-incoming stream order).
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]-(x) RETURN x.name AS x ORDER BY x",
        // Path variable: the reconstructed path must match streamed vs materialised.
        "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN length(p) AS len, nodes(p) AS ns",
        // Relationship-property predicate: gated OUT of the parallel path (has props),
        // so this exercises `expand_chain`'s hub arm applying `rel_ok` per streamed hop.
        "MATCH (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b) RETURN b.name AS b ORDER BY b",
    ];
    for q in queries {
        let ast = parser::parse(q).unwrap();
        let run = |pool: Option<std::sync::Arc<rayon::ThreadPool>>, threshold: u64| {
            let mut e = Engine::new(&gen, &cache).with_adj_stream_threshold(threshold);
            if let Some(p) = pool {
                e = e.with_fanout_pool(Some(p));
            }
            e.run(&ast)
                .unwrap_or_else(|err| panic!("`{q}` (threshold {threshold}) failed: {err:#}"))
        };
        // Materialise baseline (threshold beyond any degree) on the sequential engine.
        let base = run(None, u64::MAX);
        let variants = [
            ("seq+stream", run(None, 2)),
            ("pool+materialise", run(Some(pool.clone()), u64::MAX)),
            ("pool+stream", run(Some(pool.clone()), 2)),
        ];
        for (tag, v) in &variants {
            assert_eq!(base.columns, v.columns, "columns differ ({tag}) for `{q}`");
            assert_eq!(disp(&base), disp(v), "rows differ ({tag}) for `{q}`");
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn multi_hop_with_pool_matches_sequential() {
    // The parallel breadth-first chain expansion (`expand_chain_par`) must return
    // exactly the rows — and in the same order — as the sequential depth-first
    // walk, across fixed multi-hop chains, a path variable, a pushed LIMIT, and a
    // tight intermediate budget. The fixture frontier is below `EXPAND_PAR_MIN`, so
    // `par_gather` reads sequentially here; this pins `expand_chain_par`'s merge
    // (node_ok / next-var / charge / cap / path binding) against the DFS path,
    // while the full-Wikidata benchmark exercises the wide-frontier rayon branch.
    let (root, gen, cache, _) = budgeted_engine("exec_multihop_pool", 1_000_000);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    // Var-length is gated OUT of the parallel path, so a `*1..2` query is the
    // sequential walk under both engines — still asserted identical to lock the gate.
    let queries = [
        // 2-hop, ordered, with both rel and node vars bound.
        "MATCH (a:Person)-[r1:KNOWS]->(b)-[r2:KNOWS]->(c) \
             RETURN a.name AS a, b.name AS b, c.name AS c ORDER BY a, b, c",
        // 3-hop mixed types ending in WORKS_AT.
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:WORKS_AT]->(d) \
             RETURN a.name AS a, d.name AS d ORDER BY a, d",
        // Undirected one-hop from a pinned anchor (outgoing-then-incoming order).
        "MATCH (a:Person {name:'Bob'})-[:KNOWS]-(x) RETURN x.name AS x ORDER BY x",
        // Path variable: the bound path must reconstruct identically.
        "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN length(p) AS len, nodes(p) AS ns",
        // Type alternation + an anchor with no LIMIT/ORDER (pushed-cap off).
        "MATCH (a:Person {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b ORDER BY b",
        // Inline property on a non-anchor node — exercises `node_ok` reading the
        // shared `Scope::Frame` on the parallel walk (Bob KNOWS Carol).
        "MATCH (a:Person)-[:KNOWS]->(b {name:'Carol'}) RETURN a.name AS a ORDER BY a",
        // Pushed LIMIT on a 2-hop — gated to the sequential early-exit path under
        // both engines (a capped chain must not breadth-first over-read).
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name AS c LIMIT 1",
        // Variable-length — gated to the sequential path under both engines.
        "MATCH (a:Person {name:'Alice'})-[:KNOWS*1..2]->(b) RETURN b.name AS b ORDER BY b",
    ];
    for q in queries {
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(&gen, &cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
        let par = Engine::new(&gen, &cache)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
        // Whole-result equality preserving row order — the parallel walk must be
        // byte-for-byte identical, not merely the same set.
        let disp = |r: &QueryResult| -> Vec<Vec<String>> {
            r.rows
                .iter()
                .map(|row| row.iter().map(|c| c.to_display()).collect())
                .collect()
        };
        assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
        assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
    }
    // A tight intermediate budget must trip at the same point under both engines:
    // the 2-hop chain emits 1 row (Alice→Bob→Carol), so a budget of 1 fits and 0
    // (with the count terminal) is irrelevant — use a chain that overflows a small
    // budget identically. Alice→Bob→Carol is the lone 2-hop KNOWS path; a budget
    // that the cross-pattern terminal also charges trips both engines alike.
    let q = "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a.name, c.name";
    let ast = parser::parse(q).unwrap();
    let seq = Engine::new(&gen, &cache).with_max_intermediate(1).run(&ast);
    let par = Engine::new(&gen, &cache)
        .with_max_intermediate(1)
        .with_fanout_pool(Some(pool.clone()))
        .run(&ast);
    match (&seq, &par) {
        (Ok(s), Ok(p)) => assert_eq!(s.rows.len(), p.rows.len(), "budget row count differs"),
        (Err(_), Err(_)) => {} // both trip the budget — consistent
        _ => panic!("budget behaviour differs: seq={seq:?}, par={par:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn fixed_chain_enforces_relationship_uniqueness() {
    // HIK-81: a fixed-length chain must not traverse the same relationship twice
    // within one MATCH (openCypher relationship-isomorphism / relationship-uniqueness).
    // `write_cycle` is a directed triangle a→b→c→a with an extra chord c→b, i.e. edges
    //   e0: a→b   e1: b→c   e2: c→a   e3: c→b
    // so b and c are joined by TWO distinct undirected edges (e1, e3) — a genuine
    // 2-cycle — while every node also offers a same-edge bounce-back. Every expected
    // row below is derived by hand from that edge list, not from another slater path.
    let (root, graph) = testgen::write_cycle("exec_reluniq");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let disp = |r: &QueryResult| -> Vec<Vec<String>> {
        r.rows
            .iter()
            .map(|row| row.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    // Sequential walk (`expand_chain`).
    let run_seq = |q: &str| {
        let ast = parser::parse(q).unwrap();
        disp(
            &Engine::new(&gen, &cache)
                .run(&ast)
                .unwrap_or_else(|e| panic!("seq `{q}` failed: {e:#}")),
        )
    };
    // Parallel breadth-first walk (`expand_chain_par` / `walk_merge_hop`).
    let run_par = |q: &str| {
        let ast = parser::parse(q).unwrap();
        disp(
            &Engine::new(&gen, &cache)
                .with_fanout_pool(Some(pool.clone()))
                .run(&ast)
                .unwrap_or_else(|e| panic!("par `{q}` failed: {e:#}")),
        )
    };
    // Hub-streaming arm of the sequential walk (threshold 1 makes every anchor stream).
    let run_hub = |q: &str| {
        let ast = parser::parse(q).unwrap();
        disp(
            &Engine::new(&gen, &cache)
                .with_adj_stream_threshold(1)
                .run(&ast)
                .unwrap_or_else(|e| panic!("hub `{q}` failed: {e:#}")),
        )
    };

    // (1) Undirected closed 2-walk anchored at b, binding both edges. Undirected
    //     neighbours of b are {a via e0, c via e1, c via e3}. Returning to b:
    //       via a: only e0 connects a–b ⇒ r2 == r1 ⇒ rejected (same edge).
    //       via c: e1 and e3 both connect b–c ⇒ the two DISTINCT-edge pairings survive.
    //     Expected (ORDER BY e1, e2): [c,1,3] and [c,3,1]. The three degenerate
    //     bounce-backs (e0/e0, e1/e1, e3/e3) must NOT appear — pre-fix they did, so
    //     this assertion fails without the fix (5 rows, incl. r1 == r2).
    let q = "MATCH (x {name:'b'})-[r1]-(y)-[r2]-(x) \
                 RETURN y.name AS y, id(r1) AS e1, id(r2) AS e2 ORDER BY e1, e2";
    let expected = vec![
        vec!["c".to_string(), "1".to_string(), "3".to_string()],
        vec!["c".to_string(), "3".to_string(), "1".to_string()],
    ];
    assert_eq!(
        run_seq(q),
        expected,
        "sequential must drop bounce-backs, keep the true 2-cycle"
    );
    assert_eq!(
        run_par(q),
        expected,
        "parallel (expand_chain_par) must match"
    );
    assert_eq!(
        run_hub(q),
        expected,
        "hub-streaming arm must enforce uniqueness too"
    );
    for row in run_seq(q) {
        assert_ne!(
            row[1], row[2],
            "a surviving row must bind two distinct edges"
        );
    }

    // (2) count(*) of the same closed walk = 2 (the two genuine 2-cycles), NOT 5
    //     (the pre-fix bounce-back inflation). `degree_terminal_dir` declines here
    //     because the closing node reuses the start variable, so the count flows
    //     through the per-hop merge that now enforces uniqueness.
    let cq = "MATCH (x {name:'b'})-[r1]-(y)-[r2]-(x) RETURN count(*)";
    assert_eq!(
        run_seq(cq),
        vec![vec!["2".to_string()]],
        "count(*) must exclude bounce-backs"
    );
    assert_eq!(run_par(cq), vec![vec!["2".to_string()]]);

    // (3) The rule binds ANONYMOUS relationship elements too, not only named vars:
    //     the same closed walk with no rel variables still yields exactly 2.
    let aq = "MATCH (x {name:'b'})-[]-(y)-[]-(x) RETURN count(*)";
    assert_eq!(
        run_seq(aq),
        vec![vec!["2".to_string()]],
        "anonymous rels are unique too"
    );
    assert_eq!(run_par(aq), vec![vec!["2".to_string()]]);

    // (4) Positive control — nodes may repeat, only relationships are unique, and a
    //     legitimately distinct-edge directed 3-hop chain from a is unaffected.
    //     Directed edges out of a: a-e0->b-e1->c-{e2->a, e3->b}. Both length-3 paths
    //     use three distinct edges, so both survive (one revisits node a).
    //       a→b→c→a  (e0,e1,e2)  and  a→b→c→b  (e0,e1,e3)
    let q3 = "MATCH (a {name:'a'})-[r1]->(m)-[r2]->(n)-[r3]->(z) \
                  RETURN z.name AS z, id(r1) AS e1, id(r2) AS e2, id(r3) AS e3 ORDER BY z";
    let expected3 = vec![
        vec![
            "a".to_string(),
            "0".to_string(),
            "1".to_string(),
            "2".to_string(),
        ],
        vec![
            "b".to_string(),
            "0".to_string(),
            "1".to_string(),
            "3".to_string(),
        ],
    ];
    assert_eq!(
        run_seq(q3),
        expected3,
        "distinct-edge 3-hop chains must still be returned"
    );
    assert_eq!(run_par(q3), expected3);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn aggregation_with_pool_matches_sequential() {
    // The parallel group-by / count(DISTINCT) precompute (Task 12) must produce the
    // same grouped output — same row order, same values — as the sequential per-row
    // eval. The wide fixture has 200 nodes (≥ AGG_PAR_MIN) with `team` ∈ {Red, Blue,
    // null} and unique `name`, so the pooled engine truly fans the property reads out
    // while the grouping/reduction stays single-threaded.
    let (root, graph) = testgen::write_wide("exec_aggregation", 200);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let disp = |r: &QueryResult| -> Vec<Vec<String>> {
        r.rows
            .iter()
            .map(|row| row.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    let queries = [
        // Group-by a property + count(*) — the canonical shape.
        "MATCH (n) RETURN n.team AS t, count(*) AS c ORDER BY t",
        // count(DISTINCT n.p) — single row, no grouping item; nulls excluded.
        "MATCH (n) RETURN count(DISTINCT n.team) AS c",
        // Multiple aggregates over a group, incl. order-sensitive collect().
        "MATCH (n) RETURN n.team AS t, count(*) AS c, collect(n.name) AS names ORDER BY t",
        // min/max over a group (uses the cmp_total reduce path).
        "MATCH (n) RETURN n.team AS t, min(n.name) AS lo, max(n.name) AS hi ORDER BY t",
        // No grouping item, single-arg aggregate over the whole table.
        "MATCH (n) RETURN count(n.team) AS c",
        // A constant grouping item alongside the aggregate.
        "MATCH (n) RETURN n.team AS t, count(*) AS c, 1 AS one ORDER BY t",
    ];
    for q in queries {
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(&gen, &cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
        let par = Engine::new(&gen, &cache)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
        assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
        assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
    }

    // A `$param` grouping key exercises the Param arm of `eval_simple`.
    {
        let q = "MATCH (n) RETURN n.team AS t, count(*) AS c, $k AS k ORDER BY t";
        let ast = parser::parse(q).unwrap();
        let params = HashMap::from([("k".to_string(), Val::Int(7))]);
        let seq = Engine::new(&gen, &cache)
            .with_params(params.clone())
            .run(&ast)
            .unwrap();
        let par = Engine::new(&gen, &cache)
            .with_params(params)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap();
        assert_eq!(disp(&seq), disp(&par), "param rows differ for `{q}`");
    }

    // A tight intermediate budget must trip (or fit) at the same point under both
    // engines — the parallel path charges each new group and each aggregated value
    // in the same order as the sequential merge.
    let q = "MATCH (n) RETURN n.team AS t, count(*) AS c";
    let ast = parser::parse(q).unwrap();
    for budget in [1u64, 2, 3] {
        let seq = Engine::new(&gen, &cache)
            .with_max_intermediate(budget)
            .run(&ast);
        let par = Engine::new(&gen, &cache)
            .with_max_intermediate(budget)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast);
        match (&seq, &par) {
            (Ok(s), Ok(p)) => assert_eq!(disp(s), disp(p), "budget={budget} rows differ"),
            (Err(_), Err(_)) => {}
            _ => panic!("budget={budget} behaviour differs: seq={seq:?}, par={par:?}"),
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-78: an anchor scan must be a **stream**, not one `Vec<u64>` over the whole id
/// space built before the first row is produced. The observable is
/// `anchor_ids_scanned()` — the ids the scan actually walked, counted at the one place
/// they are produced.
///
/// * The **control**: a full-width scan that matches nothing must still walk the whole
///   id space, on both the `AllNodes` and the `LabelScan` sweep. Without this, "the
///   capped scan walked few ids" would be vacuous (a counter that always reads 0 would
///   satisfy it).
/// * The **claim**: the *same* scans under `LIMIT 1` must walk no more than the first
///   window — a few hundredths of the id space — not all of it. An eager scan fails
///   this even if it counts honestly, because a pushed `LIMIT` could only truncate the
///   row loop *after* every id had already been produced and held. (Verified: with the
///   sweeps reverted to `(0..node_count).collect()` / `collect_nodes_with_label`, this
///   assertion reports 20 000 walked ids and fails.)
#[test]
fn anchor_scan_streams_and_limit_short_circuits() {
    const N: u64 = 20_000;
    let (root, graph) = testgen::write_wide("exec_scan_stream", N);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let run = |q: &str| -> (usize, u64) {
        let ast = parser::parse(q).unwrap();
        let eng = Engine::new(&gen, &cache);
        let out = eng
            .run(&ast)
            .unwrap_or_else(|e| panic!("`{q}` failed: {e:#}"));
        (out.rows.len(), eng.anchor_ids_scanned())
    };

    // Control: an uncapped scan walks the whole id space (and the fixture has no index,
    // so `WHERE n.name = …` really is a full sweep, not a seek).
    for q in [
        "MATCH (n) WHERE n.name = 'nobody' RETURN n", // AllNodes
        "MATCH (n:Person {team:'Green'}) RETURN n",   // LabelScan
    ] {
        let (rows, walked) = run(q);
        assert_eq!(rows, 0, "`{q}` matches nothing");
        assert_eq!(walked, N, "`{q}` must walk the whole id space");
    }

    // The claim: `LIMIT 1` stops the scan inside its first window instead of producing
    // 20 000 ids up front. That window is also the scan's entire resident footprint.
    let ceiling = CAND_WINDOW_MIN;
    for q in [
        "MATCH (n) RETURN n LIMIT 1",        // AllNodes
        "MATCH (n:Person) RETURN n LIMIT 1", // LabelScan
    ] {
        let (rows, walked) = run(q);
        assert_eq!(rows, 1, "`{q}` returns one row");
        assert!(
            walked <= ceiling,
            "`{q}` walked {walked} ids (> {ceiling}) of a {N}-id space — LIMIT did not \
                 short-circuit the scan"
        );
    }

    // The stream still yields exactly the ids the eager sweep did.
    let (rows, walked) = run("MATCH (n:Person) RETURN n");
    assert_eq!(rows, N as usize / 2, "every :Person still matches");
    assert_eq!(walked, N, "…and the label sweep still walks the id space");
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-104: the *two arms HIK-78 left eager* — a `LabelScan` under a write delta and every
/// `RelTypeScan` — must also stream, via the order-preserving k-way merge. Same observable
/// as HIK-78 (`anchor_ids_scanned()` = the id space the scan actually walked); the extra
/// obligation is that the merge reproduce the eager `sort`+`dedup` **union across sources**
/// (base ∪ delta/segment overlay) exactly — order, dedup and tombstone suppression.
///
/// A rows-only test would pass without the fix (the eager path returned the same rows); the
/// claim is proven only against the *walked* count, under a delta and for a reltype scan.
#[test]
fn merged_and_reltype_scans_stream_and_limit_short_circuits() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    // ── LabelScan under a write delta (the case this ticket exists for) ──────────────
    const N: u64 = 20_000;
    let (root, graph) = testgen::write_wide("hik104_labelscan_delta", N);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // A live delta: two born :Person nodes + one deleted base :Person (node 2). This forces
    // the *merge* arm — a plain `LabelScan` is lazy only over a pure core with an empty
    // delta — and gives the merge a non-empty overlay plus a tombstone to suppress.
    let mut mem = Memtable::new();
    mem.upsert_node("Person", "name", Value::Str("newp0".into()), None, []);
    mem.upsert_node("Person", "name", Value::Str("newp1".into()), None, []);
    mem.delete_node("Person", "name", Value::Str("node0002".into()), Some(2));
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    assert!(
        view.delta().is_tombstoned(2),
        "node 2 tombstoned in the delta"
    );
    let nc = view.node_count();
    assert!(nc > N, "delta-born ids extend the scan bound");

    let person = view.label_id("Person").unwrap();
    let scan = NodeScan::LabelScan { label_id: person };

    // Correctness vs independently-derived truth: the merged candidate set is exactly the
    // base :Person ids (evens, from the fixture invariant) minus the tombstone, unioned
    // with the delta's born :Person ids — ascending and deduped, one window at a time.
    let born: Vec<u64> = view.delta().born_ids_with_label("Person");
    assert_eq!(born.len(), 2, "two born :Person nodes in the delta");
    let mut expected: Vec<u64> = (0..N).step_by(2).filter(|&i| i != 2).collect();
    expected.extend(&born);
    expected.sort_unstable();
    expected.dedup();

    let eng = Engine::new(&view, &cache);
    let got = eng.scan_candidates(&scan).unwrap();
    assert!(
        got.windows(2).all(|w| w[0] < w[1]),
        "merged label scan must be strictly ascending + deduped, got {got:?}"
    );
    assert_eq!(
        got, expected,
        "merged label scan = base ∪ overlay, minus tombstones"
    );
    // The uncapped drain walked the whole id space (control: the claim below is non-vacuous).
    assert_eq!(
        eng.anchor_ids_scanned(),
        nc,
        "uncapped merge walks the whole id space"
    );

    // The claim: under `LIMIT 1` the merge stops inside its first window (≤ `CAND_WINDOW_MIN`
    // of a 20 000-id space) instead of materialising the union up front.
    let eng2 = Engine::new(&view, &cache);
    let out = eng2
        .run(&parser::parse("MATCH (n:Person) RETURN n LIMIT 1").unwrap())
        .unwrap();
    assert_eq!(out.rows.len(), 1, "LIMIT 1 returns one row");
    assert!(
        eng2.anchor_ids_scanned() <= CAND_WINDOW_MIN,
        "LIMIT 1 walked {} ids (> {CAND_WINDOW_MIN}) under a delta — merge did not \
             short-circuit",
        eng2.anchor_ids_scanned()
    );
    let _ = std::fs::remove_dir_all(&root);

    // ── RelTypeScan over a dense endpoint posting (the 733 MB-class base) ────────────
    const M: u64 = 5_000;
    let (root, graph) = testgen::write_rel_chain("hik104_reltype", M);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // A delta that deletes one endpoint (node 100) — proves the merge's per-window
    // suppression runs for a reltype scan under a write delta too.
    let mut mem = Memtable::new();
    mem.delete_node("N", "name", Value::Str("node0100".into()), Some(100));
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    assert!(
        view.delta().is_tombstoned(100),
        "node 100 tombstoned in the delta"
    );
    let mnc = view.node_count();

    let t = view.reltype_id("T").unwrap();
    let scan = NodeScan::RelTypeScan {
        reltype_ids: vec![t],
        side: RelEndpointSide::Source,
        guaranteed_label: None,
    };
    // Every node but the last is a T source; the tombstone drops node 100.
    let expected: Vec<u64> = (0..M - 1).filter(|&i| i != 100).collect();

    let eng = Engine::new(&view, &cache);
    let got = eng.scan_candidates(&scan).unwrap();
    assert!(
        got.windows(2).all(|w| w[0] < w[1]),
        "merged reltype scan must be strictly ascending + deduped"
    );
    assert_eq!(
        got, expected,
        "reltype scan = dense posting minus the tombstone"
    );
    assert_eq!(
        eng.anchor_ids_scanned(),
        mnc,
        "uncapped reltype merge walks the whole id space"
    );

    // Short-circuit: pulling a single window walks no more than the first window, even
    // though the base posting covers ~all M nodes.
    let eng2 = Engine::new(&view, &cache);
    let mut s = eng2.candidate_stream(&scan).unwrap();
    let first = eng2.next_candidates(&mut s).unwrap();
    assert!(first.is_some(), "the first window yields candidates");
    assert!(
        eng2.anchor_ids_scanned() <= CAND_WINDOW_MIN,
        "one window walked {} ids (> {CAND_WINDOW_MIN}) of a {M}-id space — reltype merge \
             did not short-circuit",
        eng2.anchor_ids_scanned()
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn anchor_filter_with_pool_matches_sequential() {
    // The parallel anchor `node_ok` prefilter (Task 10) must keep exactly the
    // candidates — in the same order — that the sequential inline filter keeps,
    // across the shapes that make `node_ok` actually read a record: a label scan
    // with an inline property, a boolean label expression (full scan), an inline
    // property bound from a parameter, and a tight intermediate budget. The wide
    // fixture has 200 nodes (100 :Person / 100 :Company) so the candidate set
    // clears `SCAN_PAR_MIN` and the pooled engine truly fans the filter out.
    let (root, graph) = testgen::write_wide("exec_anchor_filter", 200);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let queries = [
        // Label scan (Person guaranteed) + inline prop → node_ok reads `team`.
        "MATCH (n:Person {team:'Red'}) RETURN n.name AS name ORDER BY name",
        // Boolean label expr → full scan + per-candidate label decode.
        "MATCH (n:Person|Company) RETURN n.name AS name ORDER BY name",
        // Negated label → full scan, keeps only the :Company half.
        "MATCH (n:!Person) RETURN n.name AS name ORDER BY name",
        // Inline prop with no matching value → every candidate rejected.
        "MATCH (n:Person {team:'Green'}) RETURN n.name AS name ORDER BY name",
        // Aggregate over the filtered set (uncapped, the prefilter's home turf).
        "MATCH (n:Person {team:'Blue'}) RETURN count(*) AS c",
    ];
    for q in queries {
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(&gen, &cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
        let par = Engine::new(&gen, &cache)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
        let disp = |r: &QueryResult| -> Vec<Vec<String>> {
            r.rows
                .iter()
                .map(|row| row.iter().map(|c| c.to_display()).collect())
                .collect()
        };
        assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
        assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
    }
    // A tight intermediate budget must trip (or fit) at the same point under both
    // engines — the prefilter doesn't charge, so the single-threaded merge/terminal
    // still governs the budget identically.
    let q = "MATCH (n:Person|Company) RETURN n.name";
    let ast = parser::parse(q).unwrap();
    let seq = Engine::new(&gen, &cache)
        .with_max_intermediate(10)
        .run(&ast);
    let par = Engine::new(&gen, &cache)
        .with_max_intermediate(10)
        .with_fanout_pool(Some(pool.clone()))
        .run(&ast);
    match (&seq, &par) {
        (Ok(s), Ok(p)) => assert_eq!(s.rows.len(), p.rows.len(), "budget row count differs"),
        (Err(_), Err(_)) => {}
        _ => panic!("budget behaviour differs: seq={seq:?}, par={par:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn build_view_with_pool_matches_sequential() {
    // The parallel `algo.*` subgraph build (`build_view`, Task 11) must produce the
    // same view — hence identical algorithm output — as the sequential build. The
    // per-node adjacency reads gather on the pool while the pos-mapping/select merge
    // stays single-threaded, so node list + 0-based `out` are byte-for-byte identical.
    // Two fixtures: the small edge-bearing `write_basic` graph pins the merge with
    // real adjacency (below `BUILD_VIEW_PAR_MIN`, so `par_gather` reads sequentially),
    // and the 200-node `write_wide` graph clears the threshold so the pooled engine
    // truly fans the reads out (no edges → exercises the parallel read + empty merge).
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let disp = |r: &QueryResult| -> Vec<Vec<String>> {
        r.rows
            .iter()
            .map(|row| row.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    let assert_par_eq = |gen: &Generation, cache: &BlockCache, q: &str| {
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(gen, cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
        let par = Engine::new(gen, cache)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
        assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
        assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
    };

    // Edge-bearing fixture: every algo proc shape, incl. rel-type and label filters.
    let (root, graph, _) = testgen::write_basic("exec_build_view_pool");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let queries = [
        "CALL algo.WCC() YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
        "CALL algo.WCC({relationshipTypes: ['KNOWS']}) YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
        "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
        "CALL algo.pageRank(NULL, NULL) YIELD node, score \
             RETURN node.name AS name, score ORDER BY name",
        "CALL algo.pageRank('Person', 'KNOWS') YIELD node, score \
             RETURN node.name AS name, score ORDER BY name",
        "CALL algo.betweenness() YIELD node, score RETURN node.name AS name, score ORDER BY name",
        "CALL algo.HarmonicCentrality({nodeLabels: ['Person'], relationshipTypes: ['KNOWS']}) \
             YIELD node, score, reachable RETURN node.name AS name, score, reachable ORDER BY name",
        "CALL algo.labelPropagation({relationshipTypes: ['KNOWS']}) YIELD node, communityId \
             RETURN node.name AS name, communityId ORDER BY name",
    ];
    for q in queries {
        assert_par_eq(&gen, &cache, q);
    }
    let _ = std::fs::remove_dir_all(&root);

    // Wide fixture (200 nodes ≥ BUILD_VIEW_PAR_MIN): the pooled build fans the
    // adjacency reads across rayon; pool and sequential must still match exactly.
    let (wroot, wgraph) = testgen::write_wide("exec_build_view_pool_wide", 200);
    let wgen = Generation::open(&wroot, &wgraph).unwrap();
    let wcache = BlockCache::new(1 << 20);
    assert_par_eq(
        &wgen,
        &wcache,
        "CALL algo.pageRank(NULL, NULL) YIELD node, score \
             RETURN node.name AS name, score ORDER BY name",
    );
    assert_par_eq(
        &wgen,
        &wcache,
        "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
    );
    let _ = std::fs::remove_dir_all(&wroot);
}

#[test]
fn rel_match_buffer_charges_intermediate_budget() {
    // `match_single_pattern` buffers a *materialising* relationship pattern's whole
    // result set before the cross-pattern terminal charges it; without charging the
    // buffer a dense expansion (every `:LINK` edge over a 1M-node graph) OOMs the
    // process. A row-returning query (not count-pushdown — that retains nothing and
    // is bounded by `maxScan`) exercises this retained buffer: the fixture's 3 KNOWS
    // edges trip a retained budget of 2 and pass at 1M.
    let err = run_budgeted(
        "exec_budget_relmatch_tiny",
        2,
        "MATCH (a)-[:KNOWS]->(b) RETURN b.name AS b",
    )
    .expect_err("the relationship-match buffer must charge the retained budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted(
        "exec_budget_relmatch_ok",
        1_000_000,
        "MATCH (a)-[:KNOWS]->(b) RETURN b.name AS b",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 3, "3 KNOWS edges materialise 3 rows");
}

#[test]
fn budget_resets_between_runs() {
    let (root, gen, cache, _) = budgeted_engine("exec_budget_reset", 0);
    let engine = Engine::new(&gen, &cache).with_max_intermediate(1_500);
    // Each run charges ~1k; without the per-run reset the second would trip.
    let ast = parser::parse("RETURN size(range(0, 1000))").unwrap();
    engine.run(&ast).expect("first run fits the budget");
    engine
        .run(&ast)
        .expect("the budget must reset between runs");
    let _ = std::fs::remove_dir_all(&root);
}
