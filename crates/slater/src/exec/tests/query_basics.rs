// SPDX-License-Identifier: Apache-2.0
//! `query_basics` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

/// Did this evaluation fail with a typed [`ArithmeticOverflow`]?
///
/// Classified by *type*, never by message text (house rule).
fn overflowed(r: Result<Val>) -> bool {
    r.err()
        .is_some_and(|e| e.downcast_ref::<ArithmeticOverflow>().is_some())
}

/// Integer arithmetic that leaves `i64` **errors**; it never wraps, and never
/// panics.
///
/// Regression for HIK-73. `[profile.release]` sets no `overflow-checks`, so
/// before the fix `+`/`-`/`*` wrapped silently in production while panicking
/// under `cargo test` (a debug build) — the same query quietly lying in prod and
/// killing the process under test. These assertions pin the **release**
/// behaviour, not the debug panic: they demand an `Err`, so the pre-fix code
/// fails them in a release build by returning `Ok(<wrapped>)`, not merely by
/// failing to panic. Run under both profiles, they prove the two now agree.
#[test]
fn arith_int_overflow_is_a_typed_error_not_a_wrap() {
    assert!(overflowed(arith(
        BinOp::Add,
        Val::Int(i64::MAX),
        Val::Int(1)
    )));
    assert!(overflowed(arith(
        BinOp::Sub,
        Val::Int(i64::MIN),
        Val::Int(1)
    )));
    assert!(overflowed(arith(
        BinOp::Mul,
        Val::Int(i64::MAX),
        Val::Int(2)
    )));
    assert!(overflowed(arith(
        BinOp::Mul,
        Val::Int(i64::MIN),
        Val::Int(-1)
    )));
    // `i64::MIN / -1` and `i64::MIN % -1` are a harder bug than the wrap: Rust
    // checks division overflow in *every* profile, so these panicked in release
    // too — `RETURN -9223372036854775808 / -1` was a remote process kill, not a
    // wrong answer. Now a clean error.
    assert!(overflowed(arith(
        BinOp::Div,
        Val::Int(i64::MIN),
        Val::Int(-1)
    )));
    assert!(overflowed(arith(
        BinOp::Mod,
        Val::Int(i64::MIN),
        Val::Int(-1)
    )));

    // Representable arithmetic is untouched, including at the boundary.
    assert!(matches!(
        arith(BinOp::Add, Val::Int(2), Val::Int(3)).unwrap(),
        Val::Int(5)
    ));
    assert!(matches!(
        arith(BinOp::Sub, Val::Int(i64::MAX), Val::Int(1)).unwrap(),
        Val::Int(x) if x == i64::MAX - 1
    ));
    assert!(matches!(
        arith(BinOp::Div, Val::Int(i64::MIN), Val::Int(1)).unwrap(),
        Val::Int(i64::MIN)
    ));
    assert!(matches!(
        arith(BinOp::Mod, Val::Int(i64::MIN), Val::Int(2)).unwrap(),
        Val::Int(0)
    ));
    // Division / modulo by zero keep their own distinct errors — not overflows.
    assert!(!overflowed(arith(BinOp::Div, Val::Int(1), Val::Int(0))));
    assert!(arith(BinOp::Div, Val::Int(1), Val::Int(0)).is_err());
    assert!(!overflowed(arith(BinOp::Mod, Val::Int(1), Val::Int(0))));
    assert!(arith(BinOp::Mod, Val::Int(1), Val::Int(0)).is_err());
    // `^` yields a Float even for integer operands, so it cannot overflow i64.
    assert!(matches!(
        arith(BinOp::Pow, Val::Int(2), Val::Int(3)).unwrap(),
        Val::Float(f) if f == 8.0
    ));
    // A float operand still promotes and saturates to inf, as before.
    assert!(matches!(
        arith(BinOp::Mul, Val::Float(f64::MAX), Val::Float(2.0)).unwrap(),
        Val::Float(f) if f.is_infinite()
    ));
}

/// `sum()` over integers errors past `i64` rather than wrapping — and rather
/// than promoting to `f64` (FalkorDB promotes; Neo4j errors; we error).
///
/// Regression for HIK-73: `RETURN sum(n.big)` past `i64::MAX` returned a
/// *negative* total in release.
#[test]
fn sum_of_ints_past_i64_errors_rather_than_wrapping() {
    assert!(matches!(
        sum(&[Val::Int(1), Val::Int(2)]).unwrap(),
        Val::Int(3)
    ));
    assert!(matches!(
        sum(&[Val::Int(i64::MAX), Val::Int(0)]).unwrap(),
        Val::Int(x) if x == i64::MAX
    ));
    assert!(overflowed(sum(&[Val::Int(i64::MAX), Val::Int(1)])));
    assert!(overflowed(sum(&[Val::Int(i64::MAX), Val::Int(i64::MAX)])));
    assert!(overflowed(sum(&[Val::Int(i64::MIN), Val::Int(-1)])));
    // The overflow is detected mid-fold, not just on the final pair.
    assert!(overflowed(sum(&[
        Val::Int(i64::MAX),
        Val::Int(1),
        Val::Int(-1)
    ])));
    // A float in the column still sums as f64 (unchanged).
    assert!(matches!(
        sum(&[Val::Int(1), Val::Float(0.5)]).unwrap(),
        Val::Float(f) if f == 1.5
    ));
}

/// Unary `-` on `i64::MIN` errors instead of wrapping back to `i64::MIN`
/// (`-x == x`, a silent absurdity) — end-to-end through a real query, so the
/// `Expr::Neg` eval arm and the Bolt-visible failure are both covered.
#[test]
fn negating_i64_min_errors_rather_than_wrapping_to_itself() {
    let (root, graph, _) = testgen::write_basic("exec_neg_overflow");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let run = |q: &str| engine.run(&parser::parse(q).unwrap());

    // `i64::MIN`, spelled without an out-of-range literal: -(i64::MAX) - 1.
    let err = run("RETURN -(-9223372036854775807 - 1) AS v")
        .expect_err("negating i64::MIN must fail, not wrap");
    assert!(
        err.downcast_ref::<ArithmeticOverflow>().is_some(),
        "expected a typed ArithmeticOverflow, got: {err}"
    );
    // Same query shape one step inside the boundary still evaluates.
    let res = run("RETURN -(-9223372036854775807) AS v").unwrap();
    assert!(matches!(res.rows[0][0], Val::Int(x) if x == i64::MAX));

    // `i64::MIN / -1` — this one *panicked*, in release as well as debug, so
    // before the fix this query killed the server process. Now it is a clean,
    // per-query failure and the engine survives it.
    let err = run("RETURN (-9223372036854775807 - 1) / -1 AS v")
        .expect_err("i64::MIN / -1 must fail, not panic");
    assert!(
        err.downcast_ref::<ArithmeticOverflow>().is_some(),
        "expected a typed ArithmeticOverflow, got: {err}"
    );
    // The engine is still usable after the failed query.
    let res = run("RETURN 1 + 1 AS v").unwrap();
    assert!(matches!(res.rows[0][0], Val::Int(2)));

    let _ = std::fs::remove_dir_all(&root);
}

/// A `duration(…)` the engine cannot represent is a clean, typed query error
/// — end to end, through both spellings any authenticated client can send.
///
/// `duration_to_timet` did `years.trunc() as i64`, and Rust's float→int `as`
/// cast **saturates**: `1e19` became `i64::MAX` rather than erroring, and the
/// `years_int * 12` on the next line then overflowed. Debug (overflow-checks
/// on by default) panicked inside query execution, with no `catch_unwind` on
/// the query path. Release (overflow-checks off by default — the profile that
/// *ships*) wrapped and answered silently.
///
/// That asymmetry is why these assertions have to hold under `--release`
/// too: in debug the pre-fix code fails loudly for the wrong reason, so a
/// debug-only test would look like it was doing its job while the silent
/// wrong answer shipped. Every case is asserted on the *answer* — any `Ok` is
/// a failure, whatever it contains — and never on "not the known-wrong
/// value", which is a trap here: the silent release answers vary by input
/// (see `temporal::tests::ten_quintillion_years_is_rejected_not_silently_wrapped`),
/// so a test pinned to one of them passes against the unfixed code.
#[test]
fn absurd_duration_components_are_a_typed_error_not_a_silent_wrap() {
    let (root, graph, _) = testgen::write_basic("exec_duration_overflow");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let run = |q: &str| engine.run(&parser::parse(q).unwrap());

    // Classified by error *type*, never by message text (house rule).
    let bad = |q: &str| match run(q) {
        Ok(res) => panic!(
            "`{q}` must be a query error, but answered {}",
            res.rows[0][0].to_display()
        ),
        Err(e) => assert!(
            e.downcast_ref::<temporal::DurationOutOfRange>().is_some(),
            "expected a typed DurationOutOfRange from `{q}`, got: {e}"
        ),
    };

    // The reported reproductions — the string form (the `duration()` Str
    // arm) and the map form (`build_duration`). Pre-fix in release both
    // answered `P106751991166935DT15H30M7S` (measured, not the `P-1Y` the
    // report predicted — the `extra_days` residue dominates the wrapped
    // `-12` months).
    bad("RETURN duration('P9999999999999999999Y')");
    bad("RETURN duration({years: 1e19})");
    bad("RETURN toString(duration({years: 1e19}))");
    // `1e400` parses to f64 INFINITY (`parse::<f64>` never errors on
    // overflow), and `INFINITY as i64` saturates to `i64::MAX` identically.
    bad("RETURN duration({years: 1e400})");
    // Negative extremes: `-1e19` saturated to `i64::MIN`, which wraps too.
    bad("RETURN duration({years: -1e19})");
    bad("RETURN duration({years: -1e400})");
    bad("RETURN duration('P-9999999999999999999Y')");
    // `i64::MAX` as an integer literal: `as f64` rounds it *up* to 2^63,
    // which has no i64 counterpart at all. This is the **actual** minus-one-
    // year input — it is the only spelling that both saturates the cast and
    // leaves a zero fractional residue, so pre-fix in release it really did
    // answer `P-1Y` (verified: secs = -31_536_000 against v0.23.1).
    bad("RETURN duration({years: 9223372036854775807})");
    // Not just `years` — every component is user-supplied.
    bad("RETURN duration({months: 1e19})");
    bad("RETURN duration({days: 1e400})");
    bad("RETURN duration({seconds: 1e19})");
    // The seconds fold one line down, where representable components make an
    // unrepresentable `time_t` — and `base_time` is non-zero here, which is
    // what overflowed the add.
    bad("RETURN duration({years: 1, days: 1e18})");
    // A duration whose `time_t` leaves chrono's calendar: it decoded back as
    // ~1e14 *days*, so `localdatetime(…) + it` overflowed the `* 86_400`.
    // Refused at construction now, which is the only gate `Val::Duration`
    // has.
    bad("RETURN duration({days: 100000000000000})");
    bad("RETURN localdatetime({year:2000}) + duration({days: 100000000000000})");
    // `duration ± duration` re-encodes through the same fold.
    bad("RETURN duration({years: 1e19}) + duration({years: 1})");
    bad("RETURN duration({years: 1}) - duration({years: 1e19})");

    // Only the unrepresentable is refused: ordinary durations, a value just
    // inside the boundary, and a malformed string (→ NULL, FalkorDB parity)
    // are all unchanged.
    let res = run("RETURN toString(duration('P1Y2M3DT4H5M6S')) AS d").unwrap();
    assert_eq!(render(&res.rows[0][0]), "'P1Y2M3DT4H5M6S'");
    let res = run("RETURN toString(duration({years: 100000})) AS d").unwrap();
    assert_eq!(render(&res.rows[0][0]), "'P100000Y'");
    let res = run("RETURN duration('not a duration') AS d").unwrap();
    assert!(matches!(res.rows[0][0], Val::Null));
    let res = run("RETURN duration(null) AS d").unwrap();
    assert!(matches!(res.rows[0][0], Val::Null));

    // The engine is still usable after the failed queries.
    let res = run("RETURN 1 + 1 AS v").unwrap();
    assert!(matches!(res.rows[0][0], Val::Int(2)));

    let _ = std::fs::remove_dir_all(&root);
}

/// An extreme negative list index / slice bound is out of range, not a crash.
///
/// Same bug class as the rest of HIK-73, found by sweeping the file for
/// unchecked `i64`: `list_index` computed `len as i64 + i` and `slice_range`
/// took `start.abs()`, and `|i64::MIN|` is not an `i64`. Both **panicked in a
/// debug build** (`attempt to negate with overflow`) and wrapped in a release
/// one — so `RETURN [1,2,3][-9223372036854775808]` crashed any dev/test build.
#[test]
fn extreme_negative_list_bounds_are_out_of_range_not_an_overflow() {
    let xs = [1, 2, 3];
    // Slicing: an unreachably-negative start clamps to the whole list, exactly
    // as a merely-large negative one does.
    assert_eq!(slice_range(&xs, i64::MIN, 3), &xs[..]);
    assert_eq!(slice_range(&xs, -100, 3), &xs[..]);
    assert_eq!(slice_range(&xs, 0, i64::MIN), &[] as &[i32]);
    assert_eq!(slice_range(&xs, i64::MIN, i64::MAX), &xs[..]);
    // Ordinary slices are unaffected.
    assert_eq!(slice_range(&xs, 1, 3), &xs[1..]);
    assert_eq!(slice_range(&xs, -2, 3), &xs[1..]);

    // Indexing: out of range → None, not a wrapped (in-range!) index.
    assert_eq!(list_index(3, i64::MIN), None);
    assert_eq!(list_index(3, -4), None);
    assert_eq!(list_index(3, i64::MAX), None);
    assert_eq!(list_index(3, -1), Some(2));
    assert_eq!(list_index(3, 0), Some(0));
}

/// All rows as display strings, sorted, for order-free whole-result equality.
fn rows_disp(res: &QueryResult) -> Vec<Vec<String>> {
    let mut v: Vec<Vec<String>> = res
        .rows
        .iter()
        .map(|r| r.iter().map(|c| c.to_display()).collect())
        .collect();
    v.sort();
    v
}

#[test]
fn power_operator_and_float_literals_eval() {
    // `^` always yields a Float, even for integer operands (Neo4j semantics),
    // and the new float lexis (`1e3`, `.5`) evaluates to the right numbers.
    let (root, res) = run(
        "exec_pow",
        "RETURN 2 ^ 3 AS a, 2 ^ 10 AS b, -2 ^ 2 AS c, 2 ^ 3 ^ 2 AS d, \
             1e3 AS e, .5 AS f, 4 ^ 0.5 AS g",
    );
    let r = &res.rows[0];
    let f = |v: &Val| match v {
        Val::Float(x) => *x,
        other => panic!("expected Float, got {other:?}"),
    };
    assert_eq!(f(&r[0]), 8.0);
    assert_eq!(f(&r[1]), 1024.0);
    assert_eq!(f(&r[2]), 4.0); // (-2) ^ 2
    assert_eq!(f(&r[3]), 64.0); // (2 ^ 3) ^ 2, left-assoc
    assert_eq!(f(&r[4]), 1000.0);
    assert_eq!(f(&r[5]), 0.5);
    assert_eq!(f(&r[6]), 2.0); // 4 ^ 0.5 == sqrt(4)
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn trailing_semicolon_is_accepted() {
    let (root, res) = run("exec_semi", "MATCH (n) RETURN count(*) AS c;");
    assert!(matches!(res.rows[0][0], Val::Int(5)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn all_nodes_scan_counts() {
    let (root, res) = run("exec_count_all", "MATCH (n) RETURN count(*) AS c");
    assert_eq!(res.columns, vec!["c"]);
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(5)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn label_scan_with_projection() {
    let (root, res) = run("exec_label", "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(res.columns, vec!["name"]);
    assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn label_count_uses_fast_path() {
    // Stage 3: `MATCH (n:Person) RETURN count(*)` reads the label posting length
    // (3 Person nodes in the fixture) without materialising rows.
    let (root, res) = run("exec_count_label", "MATCH (n:Person) RETURN count(*) AS c");
    assert_eq!(res.columns, vec!["c"]);
    assert!(
        matches!(res.rows[0][0], Val::Int(3)),
        "{:?}",
        res.rows[0][0]
    );
    let _ = std::fs::remove_dir_all(&root);

    // count(n) over the same pattern is identical.
    let (root, res) = run(
        "exec_count_label_n",
        "MATCH (n:Person) RETURN count(n) AS c",
    );
    assert!(matches!(res.rows[0][0], Val::Int(3)));
    let _ = std::fs::remove_dir_all(&root);

    // An unknown label counts zero (not an error, not a full scan).
    let (root, res) = run("exec_count_unknown", "MATCH (n:Nope) RETURN count(*) AS c");
    assert!(matches!(res.rows[0][0], Val::Int(0)));
    let _ = std::fs::remove_dir_all(&root);
}

// ---- whole-graph label/reltype metadata fast paths (Stage M) ----

/// Open the richer metadata fixture (multi-label node, no-label node, self-loop).
fn meta_gen(tag: &str) -> (std::path::PathBuf, Generation) {
    let (root, graph, _) = testgen::write_meta(tag);
    let gen = Generation::open(&root, &graph).unwrap();
    (root, gen)
}

#[test]
fn meta_reltype_enumeration_and_grouped_counts() {
    let (root, gen) = meta_gen("meta_reltype");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let run = |q: &str| eng.run(&parser::parse(q).unwrap()).unwrap();

    // A1 — DISTINCT type(r): the reltype list.
    let a1 = run("MATCH ()-[r]->() RETURN DISTINCT type(r) AS t");
    assert_eq!(a1.columns, vec!["t"]);
    assert_eq!(col0(&a1), vec!["KNOWS", "OWNS", "WORKS_AT"]);

    // B1 — type(r), count(*): edges per reltype (KNOWS 2, WORKS_AT 2, OWNS 1).
    let b1 = run("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c");
    assert_eq!(
        rows_disp(&b1),
        vec![
            vec!["KNOWS".to_string(), "2".to_string()],
            vec!["OWNS".to_string(), "1".to_string()],
            vec!["WORKS_AT".to_string(), "2".to_string()],
        ]
    );

    // Reverse arrow gives the same totals; count(r) == count(*).
    let b1r = run("MATCH ()<-[r]-() RETURN type(r) AS t, count(r) AS c");
    assert_eq!(rows_disp(&b1r), rows_disp(&b1));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_first_label_enumeration_and_counts() {
    let (root, gen) = meta_gen("meta_label");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let run = |q: &str| eng.run(&parser::parse(q).unwrap()).unwrap();

    // A2 — DISTINCT labels(n)[0]: includes the null bucket (the label-less node).
    let a2 = run("MATCH (n) RETURN DISTINCT labels(n)[0] AS l");
    assert_eq!(col0(&a2), vec!["Admin", "Company", "Person", "null"]);

    // B2 — labels(n)[0], count(*): Person 2 (Alice+Bob first-label), Admin 1
    // (Carol), Company 1 (Acme), null 1 (Ghost).
    let b2 = run("MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c");
    assert_eq!(
        rows_disp(&b2),
        vec![
            vec!["Admin".to_string(), "1".to_string()],
            vec!["Company".to_string(), "1".to_string()],
            vec!["Person".to_string(), "2".to_string()],
            vec!["null".to_string(), "1".to_string()],
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_fast_paths_match_the_scan() {
    // Every fast-pathed form must equal the general matcher on the same query;
    // appending an always-true WHERE forces the matcher (its independent truth).
    let (root, gen) = meta_gen("meta_parity");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let parity = |fast: &str, slow: &str| {
        let f = eng.run(&parser::parse(fast).unwrap()).unwrap();
        let s = eng.run(&parser::parse(slow).unwrap()).unwrap();
        assert_eq!(f.columns, s.columns, "columns: {fast}");
        assert_eq!(rows_disp(&f), rows_disp(&s), "rows: {fast} vs {slow}");
    };
    // bare enumerations + counts, both arrow directions + undirected
    parity(
        "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t",
        "MATCH ()-[r]->() WHERE 1 = 1 RETURN DISTINCT type(r) AS t",
    );
    parity(
        "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c",
        "MATCH ()-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    // undirected: each edge matches in both orientations (self-loops counted
    // twice), so the fast path returns 2× the directed count — verified equal to
    // the matcher.
    parity(
        "MATCH ()-[r]-() RETURN type(r) AS t, count(*) AS c",
        "MATCH ()-[r]-() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH ()-[r]-() RETURN DISTINCT type(r) AS t",
        "MATCH ()-[r]-() WHERE 1 = 1 RETURN DISTINCT type(r) AS t",
    );
    parity(
        "MATCH (n) RETURN DISTINCT labels(n)[0] AS l",
        "MATCH (n) WHERE 1 = 1 RETURN DISTINCT labels(n)[0] AS l",
    );
    parity(
        "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c",
        "MATCH (n) WHERE 1 = 1 RETURN labels(n)[0] AS l, count(*) AS c",
    );
    // labelled schema marginals: source-, target-, reverse-arrow-, multi-label.
    parity(
        "MATCH (:Person)-[r]->() RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Person)-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH ()-[r]->(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH ()-[r]->(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH ()<-[r]-(:Person) RETURN type(r) AS t, count(*) AS c",
        "MATCH ()<-[r]-(:Person) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH (:Admin)-[r]->() RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Admin)-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    // both-endpoints-labelled (the full schema-triple cube), grouped + fully
    // specified, including a multi-label endpoint.
    parity(
        "MATCH (:Person)-[r]->(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Person)-[r]->(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH (:Company)-[r]->(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Company)-[r]->(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH (:Admin)-[r]->(:Company) RETURN DISTINCT type(r) AS t",
        "MATCH (:Admin)-[r]->(:Company) WHERE 1 = 1 RETURN DISTINCT type(r) AS t",
    );
    // undirected with a labelled endpoint — src+tgt marginal (one end) and
    // triple+mirror (both ends), verified equal to the matcher.
    parity(
        "MATCH (:Person)-[r]-() RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Person)-[r]-() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH ()-[r]-(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH ()-[r]-(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH (:Person)-[r]-(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Person)-[r]-(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-77. A `DELETE` of a business key that exists nowhere is a no-op, and must
/// leave the **delta empty** — because an empty delta is what gates the metadata
/// fast paths. This asserts the fast path is *still taken* (the gated recogniser
/// returns `Some`, i.e. the answer comes from resident metadata with no block reads),
/// not merely that the count happens to be numerically right — the matcher would
/// return the same numbers while scanning the graph, which at 91.6M nodes is the
/// known OOM shape.
#[test]
fn noop_node_delete_keeps_the_metadata_fast_path_engaged() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, gen) = meta_gen("meta_noop_delete");
    let cache = BlockCache::new(1 << 20);
    // A labelled-endpoint schema-cube shape: `try_reltype_meta_fast_path` answers it
    // from the resident marginals over a pure core, and **declines** (⇒ full matcher)
    // the moment the delta is non-empty. So "did it return `Some`?" is exactly "was
    // the fast path taken?".
    let ast =
        parser::parse("MATCH (:Person)-[r]->() RETURN type(r) AS t, count(*) AS c").expect("parse");

    // Truth: the same query over the pure core, fast-pathed.
    let core_view = MergedView::read_only(&gen);
    let want = Engine::new(&core_view, &cache)
        .try_reltype_meta_fast_path(&ast.head)
        .unwrap()
        .expect("the fast path answers this shape over a pure core");

    // A delta holding *only* a delete of a key that exists nowhere.
    let mut mem = Memtable::new();
    mem.delete_node("Person", "name", Value::Str("Nobody".into()), None);
    let (mem_empty, mem_deltas) = (mem.is_empty(), mem.node_delta_count());
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let eng = Engine::new(&view, &cache);

    // The load-bearing assertion: the recogniser still answers, so the query is still
    // served from resident metadata rather than falling through to the matcher.
    let got = eng
        .try_reltype_meta_fast_path(&ast.head)
        .unwrap()
        .expect("the metadata fast path must still be engaged after a no-op DELETE");
    assert_eq!(rows_disp(&got), rows_disp(&want), "same resident answer");
    // Why it stays engaged: the no-op tombstone stored nothing, so the delta — the
    // reader's fast-path predicate — is still empty.
    assert!(mem_empty, "a no-op tombstone leaves the memtable empty");
    assert_eq!(mem_deltas, 0, "…and stores no phantom node entry");
    assert!(
        view.delta().is_empty(),
        "the reader's fast-path predicate still holds after a no-op DELETE"
    );
    // …and the query as a whole still answers correctly.
    assert_eq!(rows_disp(&eng.run(&ast).unwrap()), rows_disp(&want));

    // Control: a delete that *does* resolve populates the delta, and the same
    // recogniser then declines — so the assertion above is genuinely sensitive to
    // delta emptiness (it fails on the pre-fix `delete_node`, which stored a phantom
    // entry for `Nobody`). The real delete also still tombstones its node, so the
    // topology overlay keeps suppressing its incident edges.
    let mut mem = Memtable::new();
    mem.delete_node("Admin", "name", Value::Str("Carol".into()), Some(2));
    let live = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    assert!(
        !live.delta().is_empty(),
        "a real delete populates the delta"
    );
    assert!(live.delta().is_tombstoned(2), "and suppresses node 2");
    assert!(
        Engine::new(&live, &cache)
            .try_reltype_meta_fast_path(&ast.head)
            .unwrap()
            .is_none(),
        "over a live delta the labelled-endpoint cube declines — the gate this test guards"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_order_by_skip_limit() {
    // A trailing ORDER BY / SKIP / LIMIT is applied to the finished metadata rows,
    // order-identically to the matcher (compared without re-sorting).
    let (root, gen) = meta_gen("meta_order");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let run = |q: &str| eng.run(&parser::parse(q).unwrap()).unwrap();
    let disp = |res: &QueryResult| -> Vec<Vec<String>> {
        res.rows
            .iter()
            .map(|r| r.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    // Total order (c desc, then key) so ties are deterministic across paths.
    let ordered_parity = |fast: &str, slow: &str| {
        let f = run(fast);
        let s = run(slow);
        assert_eq!(f.columns, s.columns, "cols: {fast}");
        assert_eq!(disp(&f), disp(&s), "ordered rows: {fast} vs {slow}");
    };
    ordered_parity(
        "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY c DESC, t",
        "MATCH ()-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c ORDER BY c DESC, t",
    );
    // LIMIT truncates after ordering: the single largest group.
    assert_eq!(
        disp(&run(
            "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY c DESC, t LIMIT 1"
        )),
        vec![vec!["KNOWS".to_string(), "2".to_string()]],
    );
    // SKIP + LIMIT on the label side.
    ordered_parity(
            "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c ORDER BY c DESC, l SKIP 1 LIMIT 2",
            "MATCH (n) WHERE 1 = 1 RETURN labels(n)[0] AS l, count(*) AS c ORDER BY c DESC, l SKIP 1 LIMIT 2",
        );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_fast_path_reads_no_blocks_under_tiny_budget() {
    // The regression guard: with `maxIntermediate` far below the edge count the
    // metadata queries still SUCCEED (no materialisation), read zero blocks, and
    // charge no budget — while the scanning form of the same question trips.
    let (root, gen) = meta_gen("meta_perf");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache).with_max_intermediate(1);
    for q in [
        "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t",
        "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c",
        "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c",
    ] {
        let before = cache.metrics().misses;
        let res = eng.run(&parser::parse(q).unwrap()).unwrap();
        assert!(!res.rows.is_empty(), "empty result for {q}");
        assert_eq!(cache.metrics().misses, before, "fast path read blocks: {q}");
        assert_eq!(eng.cost(), 0, "fast path charged budget: {q}");
    }
    // The materialising form of the same question DOES trip the tiny budget —
    // exactly the failure the fast path removes.
    let scan = eng.run(
        &parser::parse("MATCH ()-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c").unwrap(),
    );
    assert!(scan.is_err(), "scan should trip maxIntermediate=1");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_declines_still_correct() {
    // Each "do NOT fast-path" shape falls back to the matcher and stays correct.
    let (root, gen) = meta_gen("meta_decline");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let rows = |q: &str| rows_disp(&eng.run(&parser::parse(q).unwrap()).unwrap());

    // rel-type filter.
    assert_eq!(
        rows("MATCH ()-[r:KNOWS]->() RETURN type(r) AS t, count(*) AS c"),
        vec![vec!["KNOWS".to_string(), "2".to_string()]],
    );
    // WHERE predicate.
    assert_eq!(
        rows("MATCH ()-[r]->() WHERE type(r) = 'KNOWS' RETURN type(r) AS t, count(*) AS c"),
        vec![vec!["KNOWS".to_string(), "2".to_string()]],
    );
    // count(DISTINCT …) — declines; here it equals count(*) (all edges distinct).
    assert_eq!(
        rows("MATCH ()-[r]->() RETURN type(r) AS t, count(DISTINCT r) AS c"),
        rows("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c"),
    );
    // a node variable reused on both endpoints `(a)-[r]->(a)` constrains a
    // self-loop, so the whole-graph counts must NOT be used — it declines and the
    // matcher returns only the self-loop (OWNS: Acme→Acme).
    assert_eq!(
        rows("MATCH (a)-[r]->(a) RETURN type(r) AS t, count(*) AS c"),
        vec![vec!["OWNS".to_string(), "1".to_string()]],
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_where_clause_is_not_ignored() {
    // A WHERE narrows the match, so the whole-graph metadata counts would be
    // WRONG — the fast path must decline and the matcher return the *filtered*
    // answer. Each case is chosen so the correct answer DIFFERS from the
    // metadata count, proving the resident counts are not reused.
    let (root, gen) = meta_gen("meta_where");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let rows = |q: &str| rows_disp(&eng.run(&parser::parse(q).unwrap()).unwrap());

    // Whole-graph baseline (fast path): KNOWS 2, WORKS_AT 2, OWNS 1.
    let base = rows("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c");
    assert_eq!(
        base,
        vec![
            vec!["KNOWS".to_string(), "2".to_string()],
            vec!["OWNS".to_string(), "1".to_string()],
            vec!["WORKS_AT".to_string(), "2".to_string()],
        ]
    );

    // WHERE on a source property → only Alice's out-edges (KNOWS 1, WORKS_AT 1).
    let by_src =
        rows("MATCH (a)-[r]->() WHERE a.name = 'Alice' RETURN type(r) AS t, count(*) AS c");
    assert_eq!(
        by_src,
        vec![
            vec!["KNOWS".to_string(), "1".to_string()],
            vec!["WORKS_AT".to_string(), "1".to_string()],
        ]
    );
    assert_ne!(
        by_src, base,
        "WHERE on source property must change the counts"
    );

    // WHERE that prunes an entire reltype group — OWNS must disappear, not be
    // reported with its metadata count of 1.
    let pruned =
        rows("MATCH ()-[r]->() WHERE type(r) <> 'OWNS' RETURN type(r) AS t, count(*) AS c");
    assert_eq!(
        pruned,
        vec![
            vec!["KNOWS".to_string(), "2".to_string()],
            vec!["WORKS_AT".to_string(), "2".to_string()],
        ]
    );
    assert!(
        !pruned.iter().any(|r| r[0] == "OWNS"),
        "WHERE must prune the OWNS group entirely"
    );

    // WHERE that matches nothing → zero rows, NOT the metadata counts.
    let none =
        rows("MATCH ()-[r]->() WHERE r.no_such_prop = 99 RETURN type(r) AS t, count(*) AS c");
    assert!(
        none.is_empty(),
        "a WHERE matching no edges must yield no rows"
    );

    // Label side: a WHERE on a node property → only the matching node's first
    // label (Bob → Person 1), not the whole-graph Person count of 2.
    let base_l = rows("MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c");
    let one = rows("MATCH (n) WHERE n.name = 'Bob' RETURN labels(n)[0] AS l, count(*) AS c");
    assert_eq!(one, vec![vec!["Person".to_string(), "1".to_string()]]);
    assert_ne!(
        one, base_l,
        "WHERE on a node property must change the counts"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn count_with_constant_extra_projection_fast_path() {
    // The benchmark appends `… , $k AS k` (a constant grouping key) to bust the
    // result cache. That is still a single group, so the fast path fires and the
    // extra column is carried through in order.
    let (root, res) = run(
        "exec_count_tag",
        "MATCH (n:Person) RETURN count(*) AS c, 7 AS k",
    );
    assert_eq!(res.columns, vec!["c", "k"]);
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(3)));
    assert!(matches!(res.rows[0][1], Val::Int(7)));
    let _ = std::fs::remove_dir_all(&root);

    // Order preserved when the tag precedes the count.
    let (root, res) = run("exec_count_tag2", "MATCH (n) RETURN 9 AS k, count(n) AS c");
    assert_eq!(res.columns, vec!["k", "c"]);
    assert!(matches!(res.rows[0][0], Val::Int(9)));
    assert!(matches!(res.rows[0][1], Val::Int(5)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn count_with_non_constant_extra_projection_falls_back() {
    // A second item that reads node data is a real grouping key — must NOT take
    // the fast path; group-by-city over the 3 Person nodes yields 2 rows.
    let (root, res) = run(
        "exec_count_group",
        "MATCH (n:Person) RETURN n.city AS city, count(*) AS c",
    );
    assert_eq!(res.rows.len(), 2, "{:?}", res.rows);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn count_with_where_still_correct() {
    // A residual WHERE disables the fast path; the answer must still be right
    // (2 of the 3 Person nodes have age >= 30 in the fixture: Alice 30, Carol 40;
    // Bob is 25).
    let (root, res) = run(
        "exec_count_where",
        "MATCH (n:Person) WHERE n.age >= 30 RETURN count(*) AS c",
    );
    assert!(
        matches!(res.rows[0][0], Val::Int(2)),
        "{:?}",
        res.rows[0][0]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn streaming_scan_where_and_property_projection() {
    // Stage 5: a single node-only MATCH streams without per-row HashMaps. A
    // WHERE filter that reads a property (city = 'London') keeps Alice + Bob,
    // and the projected property comes back correctly.
    let (root, res) = run(
        "exec_stream_where",
        "MATCH (n:Person) WHERE n.city = 'London' RETURN n.name AS name",
    );
    assert_eq!(res.columns, vec!["name"]);
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn streaming_scan_group_by_property_aggregation() {
    // Aggregation over the streamed rows: group the 3 Person nodes by city
    // (London → 2, Paris → 1). Exercises the streaming match feeding
    // project_aggregated with a per-row property read.
    let (root, res) = run(
        "exec_stream_agg",
        "MATCH (n:Person) RETURN n.city AS city, count(*) AS c ORDER BY c DESC",
    );
    assert_eq!(res.columns, vec!["city", "c"]);
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0].to_display(), "London");
    assert!(matches!(res.rows[0][1], Val::Int(2)));
    assert_eq!(res.rows[1][0].to_display(), "Paris");
    assert!(matches!(res.rows[1][1], Val::Int(1)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn streaming_scan_inline_prop_filter() {
    // An inline property on the anchor (handled by node_ok in the streaming
    // path, not a residual WHERE) selects the single matching node.
    let (root, res) = run(
        "exec_stream_inline",
        "MATCH (n:Person {city: 'Paris'}) RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_distinct_count_fast_path() {
    // Stage 7: `count(DISTINCT n.p)` over an indexed property is the number of
    // distinct index keys. age has 3 distinct values; team has one ('Red'),
    // and the index omits Carol (no team) — DISTINCT also excludes null.
    let (root, res) = run(
        "exec_g_distinct_age",
        "MATCH (n:Person) RETURN count(DISTINCT n.age) AS c",
    );
    assert_eq!(res.columns, vec!["c"]);
    assert!(
        matches!(res.rows[0][0], Val::Int(3)),
        "{:?}",
        res.rows[0][0]
    );
    let _ = std::fs::remove_dir_all(&root);

    // With the cache-busting constant tail, and a single distinct value.
    let (root, res) = run(
        "exec_g_distinct_team",
        "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c, 7 AS k",
    );
    assert_eq!(res.columns, vec!["c", "k"]);
    assert!(
        matches!(res.rows[0][0], Val::Int(1)),
        "{:?}",
        res.rows[0][0]
    );
    assert!(matches!(res.rows[0][1], Val::Int(7)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_group_by_fast_path() {
    // Stage 7: group-by an indexed property reads (key, count) from the index.
    // team: Alice/Bob 'Red' (2) and Carol's missing team becomes a null group
    // (1). ORDER BY c DESC puts the larger group first.
    let (root, res) = run(
        "exec_g_groupby_team",
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c ORDER BY c DESC",
    );
    assert_eq!(res.columns, vec!["t", "c"]);
    assert_eq!(res.rows.len(), 2, "{:?}", res.rows);
    assert_eq!(res.rows[0][0].to_display(), "Red");
    assert!(matches!(res.rows[0][1], Val::Int(2)));
    assert!(matches!(res.rows[1][0], Val::Null), "{:?}", res.rows[1][0]);
    assert!(matches!(res.rows[1][1], Val::Int(1)));
    let _ = std::fs::remove_dir_all(&root);

    // All-distinct indexed property: one group of 1 per value (no null group,
    // every Person has an age). `count(n)` behaves like `count(*)` here.
    let (root, res) = run(
        "exec_g_groupby_age",
        "MATCH (n:Person) RETURN n.age AS a, count(n) AS c",
    );
    assert_eq!(
        rows_disp(&res),
        vec![
            vec!["25".to_string(), "1".to_string()],
            vec!["30".to_string(), "1".to_string()],
            vec!["40".to_string(), "1".to_string()],
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_matches_general_path() {
    // The fast path must return exactly what the general (materialise + group)
    // path does. A residual WHERE that keeps every row forces the general path;
    // both group-by team (incl. the null group) and distinct-count must agree.
    let (root, fast) = run(
        "exec_g_cmp_fast",
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c",
    );
    let _ = std::fs::remove_dir_all(&root);
    let (root, general) = run(
        "exec_g_cmp_gen",
        "MATCH (n:Person) WHERE n.age >= 0 RETURN n.team AS t, count(*) AS c",
    );
    assert_eq!(rows_disp(&fast), rows_disp(&general));
    let _ = std::fs::remove_dir_all(&root);

    let (root, fast) = run(
        "exec_g_cmp_fast_d",
        "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c",
    );
    let _ = std::fs::remove_dir_all(&root);
    let (root, general) = run(
        "exec_g_cmp_gen_d",
        "MATCH (n:Person) WHERE n.age >= 0 RETURN count(DISTINCT n.team) AS c",
    );
    assert_eq!(rows_disp(&fast), rows_disp(&general));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_fast_path_guards() {
    // Shapes the fast path must decline, each still answered correctly by the
    // general path.

    // (a) Residual WHERE: age >= 30 keeps Alice (Red) and Carol (null).
    let (root, res) = run(
        "exec_g_guard_where",
        "MATCH (n:Person) WHERE n.age >= 30 RETURN n.team AS t, count(*) AS c",
    );
    assert_eq!(
        rows_disp(&res),
        vec![
            vec!["Red".to_string(), "1".to_string()],
            vec!["null".to_string(), "1".to_string()],
        ]
    );
    let _ = std::fs::remove_dir_all(&root);

    // (b) A non-count aggregate (sum) over the grouping property.
    let (root, res) = run(
        "exec_g_guard_sum",
        "MATCH (n:Person) RETURN n.team AS t, sum(n.age) AS s",
    );
    // Red = Alice 30 + Bob 25 = 55; null group = Carol 40.
    assert_eq!(
        rows_disp(&res),
        vec![
            vec!["Red".to_string(), "55".to_string()],
            vec!["null".to_string(), "40".to_string()],
        ]
    );
    let _ = std::fs::remove_dir_all(&root);

    // (c) Two grouping keys (the second `node.prop` trips the >1-key guard).
    let (root, res) = run(
        "exec_g_guard_twokeys",
        "MATCH (n:Person) RETURN n.team AS t, n.city AS city, count(*) AS c",
    );
    // (Red, London) Alice+Bob = 2; (null, Paris) Carol = 1.
    assert_eq!(res.rows.len(), 2, "{:?}", res.rows);
    let _ = std::fs::remove_dir_all(&root);

    // (d) A non-indexed grouping property (city) — must fall back, still right.
    let (root, res) = run(
        "exec_g_guard_noindex",
        "MATCH (n:Person) RETURN n.city AS city, count(*) AS c ORDER BY c DESC",
    );
    assert_eq!(res.rows[0][0].to_display(), "London");
    assert!(matches!(res.rows[0][1], Val::Int(2)));
    assert_eq!(res.rows[1][0].to_display(), "Paris");
    assert!(matches!(res.rows[1][1], Val::Int(1)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_fast_path_fires_without_scanning() {
    // Proof the fast path actually *fires* (rather than just agreeing with the
    // general path): the index walk charges nothing to the intermediate budget,
    // so a budget far too small for a per-row scan still succeeds. The control —
    // the same query forced onto the general path by a residual WHERE — exhausts
    // that budget scanning the 3 Person rows.
    //
    // The `count(DISTINCT n.p)` shape also exercises the parser quirk where the
    // inner DISTINCT sets `ret.distinct`; the fast path must not be fooled into
    // declining.
    let res = run_budgeted(
        "exec_g_fire_distinct",
        2,
        "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c, 7 AS k",
    )
    .expect("distinct-count fast path must not scan");
    assert!(
        matches!(res.rows[0][0], Val::Int(1)),
        "{:?}",
        res.rows[0][0]
    );

    let res = run_budgeted(
        "exec_g_fire_group",
        2,
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c",
    )
    .expect("group-by fast path must not scan");
    assert_eq!(res.rows.len(), 2);

    // Control: forced onto the general (scanning) path, the same budget trips.
    let err = run_budgeted(
        "exec_g_fire_control",
        2,
        "MATCH (n:Person) WHERE n.age >= 0 RETURN count(DISTINCT n.team) AS c",
    );
    assert!(
        err.is_err(),
        "the general path must exhaust the tiny budget (proving the fast path \
             above genuinely avoided the scan)"
    );
}

#[test]
fn grouped_index_histogram_matches_scan() {
    // Level-1 precompute correctness: a histogram-ON generation answers
    // group-by / count(DISTINCT) from `prop_hist.blk`; an otherwise-identical
    // histogram-OFF generation answers them by walking the ISAM. Every query
    // must return identical rows AND identical row order.
    let ordered = |res: &QueryResult| -> Vec<Vec<String>> {
        res.rows
            .iter()
            .map(|r| r.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    let exec = |root: &std::path::Path, graph: &str, q: &str| -> QueryResult {
        let gen = Generation::open(root, graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let out = Engine::new(&gen, &cache)
            .run(&parser::parse(q).unwrap())
            .unwrap();
        out
    };

    let queries = [
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c ORDER BY c DESC",
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c",
        "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c",
        "MATCH (n:Person) RETURN n.age AS a, count(*) AS c ORDER BY a ASC",
        "MATCH (n:Person) RETURN count(DISTINCT n.age) AS c, 7 AS k",
    ];
    for (i, q) in queries.iter().enumerate() {
        let (root_off, g_off, _) = testgen::write_basic(&format!("exec_hist_off_{i}"));
        // The OFF generation carries no histogram → fallback (index walk).
        let gen_off = Generation::open(&root_off, &g_off).unwrap();
        assert!(gen_off.property_histogram("node_Person_team").is_none());
        drop(gen_off);
        let off = exec(&root_off, &g_off, q);
        let _ = std::fs::remove_dir_all(&root_off);

        let (root_on, g_on, _) = testgen::write_basic_with_histograms(&format!("exec_hist_on_{i}"));
        // The ON generation's histogram is byte-identical to the walk it replaces.
        let gen_on = Generation::open(&root_on, &g_on).unwrap();
        let hist = gen_on
            .property_histogram("node_Person_team")
            .expect("histogram present in the ON generation");
        let walk = gen_on
            .range_index("node_Person_team")
            .unwrap()
            .distinct_key_counts()
            .unwrap();
        assert_eq!(hist, walk.as_slice(), "histogram must equal the index walk");
        drop(gen_on);
        let on = exec(&root_on, &g_on, q);
        let _ = std::fs::remove_dir_all(&root_on);

        assert_eq!(on.columns, off.columns, "columns differ for `{q}`");
        assert_eq!(ordered(&on), ordered(&off), "rows/order differ for `{q}`");
    }
}

#[test]
fn param_indexed_equality_count_fast_path() {
    // Stage 1 + 3: `{name: $n}` selects the name index and the count comes from
    // its `lookup_eq` length, not a label scan + materialise.
    let (root, graph, _) = testgen::write_basic("exec_count_param_idx");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let mut params = HashMap::new();
    params.insert("n".to_string(), Val::Str("Carol".into()));
    let engine = Engine::new(&gen, &cache).with_params(params);
    let ast = parser::parse("MATCH (n:Person {name: $n}) RETURN count(*) AS c").unwrap();
    let res = engine.run(&ast).unwrap();
    assert!(
        matches!(res.rows[0][0], Val::Int(1)),
        "{:?}",
        res.rows[0][0]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn range_index_equality_lookup() {
    let (root, res) = run(
        "exec_rangeeq",
        "MATCH (n:Person {name: 'Bob'}) RETURN n.age AS age",
    );
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(25)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn where_range_filter_and_order() {
    let (root, res) = run(
        "exec_range",
        "MATCH (n:Person) WHERE n.age >= 30 RETURN n.name AS name ORDER BY n.age DESC",
    );
    // Carol (40) then Alice (30).
    let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
    assert_eq!(names, vec!["Carol", "Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn relationship_pattern_traversal() {
    let (root, res) = run(
        "exec_rel",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b",
    );
    let mut pairs: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Alice".into(), "Bob".into()),
            ("Alice".into(), "Carol".into()),
            ("Bob".into(), "Carol".into()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn relationship_value_carries_type_and_stored_endpoints() {
    // Outgoing walk: r is the stored Alice(0)-[:KNOWS]->Bob(1) edge.
    let (root, res) = run(
            "exec_reltype",
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN type(r) AS t, r AS rel",
        );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "KNOWS");
    match res.rows[0][1] {
        Val::Rel {
            start,
            end,
            reltype,
            ..
        } => {
            assert_eq!((start, end, reltype), (0, 1, 0));
        }
        ref other => panic!("expected a relationship, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);

    // Walking the SAME edge incoming must report the same stored direction
    // (start→end is src→dst, not the traversal direction).
    let (root, res) = run(
        "exec_reltype_in",
        "MATCH (b:Person {name: 'Bob'})<-[r:KNOWS]-(a) RETURN r AS rel",
    );
    assert_eq!(res.rows.len(), 1);
    match res.rows[0][0] {
        Val::Rel { start, end, .. } => assert_eq!((start, end), (0, 1)),
        ref other => panic!("expected a relationship, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn incoming_direction_traversal() {
    let (root, res) = run(
        "exec_incoming",
        "MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a.name AS a, b.name AS b",
    );
    // Reverse of the KNOWS edges: Bob<-Alice, Carol<-Bob, Carol<-Alice.
    let mut pairs: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Bob".into(), "Alice".into()),
            ("Carol".into(), "Alice".into()),
            ("Carol".into(), "Bob".into()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn relationship_property_predicate() {
    let (root, res) = run(
        "exec_relprop",
        "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN a.name AS a, b.name AS b",
    );
    // Only the Alice-[:KNOWS {since:2020}]->Bob edge carries the property.
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Alice");
    assert_eq!(res.rows[0][1].to_display(), "Bob");
    let _ = std::fs::remove_dir_all(&root);
}

// Inline property maps whose value is bound earlier (by a `WITH` or an earlier
// node/rel) must resolve against the current scope — `(b {id: x})` behaves like
// `(b) WHERE b.id = x`. This was the last eu-ai-act-data-service parity gap.

#[test]
fn inline_node_prop_resolves_variable_from_with() {
    // The exact reported gap: a WITH-bound value feeding a later inline map.
    let (root, res) = run(
        "exec_inline_with",
        "MATCH (n:Person {name:'Bob'}) WITH n.name AS who \
             MATCH (m:Person {name: who}) RETURN m.age AS age",
    );
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(25)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn inline_node_prop_joins_across_matches() {
    // baseId-style join: carry one node's property into another node's inline map.
    let (root, res) = run(
        "exec_inline_join",
        "MATCH (a:Person {name:'Alice'}) WITH a.city AS c \
             MATCH (p:Person {city: c}) RETURN p.name AS n",
    );
    // Alice and Bob are both in London.
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn inline_rel_prop_resolves_variable() {
    // Variable value on a relationship inline map.
    let (root, res) = run(
        "exec_inline_rel",
        "WITH 2020 AS yr MATCH (a)-[r:KNOWS {since: yr}]->(b) \
             RETURN a.name AS a, b.name AS b",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Alice");
    assert_eq!(res.rows[0][1].to_display(), "Bob");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn inline_node_prop_resolves_property_access() {
    // The value is a property access (`a.name`), not just a bare variable.
    let (root, res) = run(
        "exec_inline_propaccess",
        "MATCH (a:Person {name:'Bob'}) \
             MATCH (m:Person {name: a.name}) RETURN m.name AS n",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Bob");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn inline_node_prop_literal_still_works() {
    // Regression guard: literal inline maps must keep matching after the change.
    let (root, res) = run(
        "exec_inline_literal",
        "MATCH (n:Person {name:'Bob'}) RETURN n.age AS age",
    );
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(25)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn variable_length_expansion() {
    let (root, res) = run(
        "exec_varlen",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b) RETURN b.name AS name",
    );
    // 1 hop: Bob, Carol. 2 hops: Alice→Bob→Carol = Carol again.
    assert_eq!(col0(&res), vec!["Bob", "Carol", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn type_alternation() {
    let (root, res) = run(
        "exec_altern",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS name",
    );
    // Alice KNOWS Bob, KNOWS Carol, WORKS_AT Acme.
    assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn with_aggregation_group_and_having() {
    let (root, res) = run(
        "exec_with",
        "MATCH (n:Person) WITH n.city AS city, count(*) AS c WHERE c > 1 RETURN city, c",
    );
    // London has 2 (Alice, Bob); Paris has 1 (filtered out).
    assert_eq!(res.columns, vec!["city", "c"]);
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "London");
    assert!(matches!(res.rows[0][1], Val::Int(2)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn distinct_and_aggregate_functions() {
    let (root, res) = run(
            "exec_aggs",
            "MATCH (n:Person) RETURN count(n) AS c, sum(n.age) AS total, avg(n.age) AS mean, min(n.age) AS lo, max(n.age) AS hi, collect(DISTINCT n.city) AS cities",
        );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Int(3)));
    assert!(matches!(r[1], Val::Int(95))); // 30+25+40
    assert!(matches!(r[2], Val::Float(f) if (f - 95.0 / 3.0).abs() < 1e-9));
    assert!(matches!(r[3], Val::Int(25)));
    assert!(matches!(r[4], Val::Int(40)));
    match &r[5] {
        Val::List(xs) => {
            let mut cities: Vec<String> = xs.iter().map(|v| v.to_display()).collect();
            cities.sort();
            assert_eq!(cities, vec!["London", "Paris"]);
        }
        other => panic!("expected a list, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn distinct_projection() {
    let (root, res) = run(
        "exec_distinct",
        "MATCH (n:Person) RETURN DISTINCT n.city AS city",
    );
    assert_eq!(col0(&res), vec!["London", "Paris"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn skip_and_limit() {
    let (root, res) = run(
        "exec_skiplimit",
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name SKIP 1 LIMIT 1",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Bob");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn map_projection() {
    let (root, res) = run(
        "exec_mapproj",
        "MATCH (n:Person {name: 'Alice'}) RETURN n {.name, .age} AS m",
    );
    match &res.rows[0][0] {
        Val::Map(m) => {
            assert_eq!(m[0].0, "name");
            assert_eq!(m[0].1.to_display(), "Alice");
            assert_eq!(m[1].0, "age");
            assert!(matches!(m[1].1, Val::Int(30)));
        }
        other => panic!("expected a map, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn case_and_list_predicate_and_in() {
    let (root, res) = run(
            "exec_case",
            "MATCH (n:Person) RETURN n.name AS name, CASE WHEN n.age >= 30 THEN 'senior' ELSE 'junior' END AS band ORDER BY n.name",
        );
    let bands: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(
        bands,
        vec![
            ("Alice".into(), "senior".into()),
            ("Bob".into(), "junior".into()),
            ("Carol".into(), "senior".into()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn where_in_and_string_ops() {
    let (root, res) = run(
        "exec_in",
        "MATCH (n:Person) WHERE n.age IN [25, 40] AND n.name STARTS WITH 'C' RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn union_distinct_and_all() {
    let (root, res) = run(
        "exec_union",
        "MATCH (n:Person) RETURN n.name AS x UNION MATCH (c:Company) RETURN c.name AS x",
    );
    assert_eq!(res.columns, vec!["x"]);
    assert_eq!(col0(&res), vec!["Acme", "Alice", "Bob", "Carol", "Globex"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn optional_match_yields_nulls() {
    // Companies have no outgoing KNOWS, so the optional rel is null.
    let (root, res) = run(
            "exec_optional",
            "MATCH (n:Company) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n.name AS name, m AS friend ORDER BY n.name",
        );
    assert_eq!(res.rows.len(), 2);
    for r in &res.rows {
        assert!(matches!(r[1], Val::Null));
    }
    assert_eq!(res.rows[0][0].to_display(), "Acme");
    let _ = std::fs::remove_dir_all(&root);
}
