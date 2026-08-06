// SPDX-License-Identifier: Apache-2.0
//! Query-runtime benchmark — the measurement gate for the executor work (HIK-216).
//!
//! Every other bench in this directory measures the *storage* side (vectors, segment
//! read-amp, merges). Nothing measured the executor, so every latency claim about it —
//! including an external review's claim that it is the engine's weak link — rested on
//! reading the code. This bench exists so the two open executor issues are ranked by a
//! number rather than by an argument:
//!
//! * **HIK-217** — `expand_chain`'s leaf clones the whole binding, *including the seed
//!   columns*, which `apply_match` then rebuilds positionally from the input row and
//!   never reads out of the clone. Dead work, proportional to seed width.
//! * **HIK-218** — `varlen` materialises every path into one `Vec` before DISTINCT.
//!
//! The decisive shape is the [seed-width pair](#seed-width): identical traversal,
//! identical output cardinality, differing only in how many columns are carried into the
//! matcher. It varies *nothing but* the quantity HIK-217 removes.
//!
//! Run:
//! ```text
//! cargo bench -p slater --features testkit --bench query_runtime
//! ```
//!
//! One-shot mode for a profiler (no separate bin target; `unsafe_code = "forbid"` rules
//! out an in-tree counting allocator, but callgrind needs none):
//! ```text
//! cargo bench -p slater --features testkit --bench query_runtime --no-run
//! SLATER_QR_ONESHOT=seed_width_6 valgrind --tool=callgrind --cache-sim=no <bench-bin>
//! callgrind_annotate --inclusive=yes callgrind.out.*
//! ```

use std::sync::Arc;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};

use slater::benchkit::{self, QueryBudget, Reader};
use slater::testgen;

/// Big enough that per-row cost dominates fixture open, small enough to build in seconds.
const N: u64 = 200_000;

/// Cache budget past the working set, so a warm run never evicts and a miss delta of ~0
/// means block reads are genuinely excluded from the timing (checked below).
const CACHE: usize = 512 << 20;

/// The real fixture needs far more: a 2-hop expansion from 100 scattered anchors
/// touches thousands of topology blocks across an 81.5M-edge CSR, which does not fit
/// in `CACHE`. Undersizing it does not merely slow the bench — `measure` asserts a
/// warm re-run faults zero blocks, so it fails loudly rather than reporting a number
/// that is really a study of the block cache.
const CACHE_REAL: usize = 2048 << 20;

/// A named query and the fixture it runs against.
struct Shape {
    id: &'static str,
    query: &'static str,
}

/// Shapes over the `scale` fixture: `:Person {name, age}`, `KNOWS`, an out-degree-1 ring,
/// range index on `(Person, name)`.
///
/// Every row-producing shape is **unanchored**. The ring gives each node out-degree 1, so
/// an *anchored* chain yields exactly one row and measures nothing; unanchored, each emits
/// ~`N` rows and per-row cost is what the number reflects.
///
/// The terminal label is load-bearing on the count shapes. Without it `degree_terminal_dir`
/// arms and the count short-circuits its last hop into a maintained-degree sum — walking a
/// *different graph* than the row-emitting sibling it is supposed to be the control for.
/// `count_degree_terminal` keeps that fast path visible on purpose.
const SCALE_SHAPES: &[Shape] = &[
    Shape {
        id: "expand_1hop",
        query: "MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN m",
    },
    Shape {
        id: "chain_3hop",
        query: "MATCH (n:Person)-[:KNOWS]->(a)-[:KNOWS]->(b)-[:KNOWS]->(c:Person) RETURN c",
    },
    // Same walk as chain_3hop, no binding materialised: the emit-cost control.
    Shape {
        id: "count_control",
        query: "MATCH (n:Person)-[:KNOWS]->(a)-[:KNOWS]->(b)-[:KNOWS]->(c:Person) RETURN count(*)",
    },
    // Unlabelled terminal: the degree-sum fast path *does* arm. Kept so a regression in it
    // is visible rather than silently folded into the control above.
    Shape {
        id: "count_degree_terminal",
        query: "MATCH (n:Person)-[:KNOWS]->(a)-[:KNOWS]->(b) RETURN count(*)",
    },
    // Output width, holding the walk fixed.
    Shape {
        id: "project_1col",
        query: "MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN m",
    },
    Shape {
        id: "project_6col",
        query: "MATCH (n:Person)-[:KNOWS]->(m:Person) \
                RETURN m, n.name, n.age, m.name, m.age, id(m)",
    },
    // ── Seed width ──────────────────────────────────────────────────────────────
    // The pair that decides HIK-217. Identical traversal, identical row count; the
    // second carries five extra columns into `apply_match`'s seed map, so the leaf
    // clones five extra (String, Val) pairs per row that `apply_match` then rebuilds
    // positionally from `row` and never reads out of the clone.
    Shape {
        id: "seed_width_1",
        query: "MATCH (n:Person) WITH n \
                MATCH (n)-[:KNOWS]->(m:Person) RETURN m",
    },
    Shape {
        id: "seed_width_6",
        query: "MATCH (n:Person) \
                WITH n, n.name AS c1, n.age AS c2, n.name AS c3, n.age AS c4, n.age AS c5 \
                MATCH (n)-[:KNOWS]->(m:Person) RETURN m",
    },
    // `seed_width_6` confounds two costs: evaluating five extra expressions in the WITH,
    // and *carrying* five extra columns through the matcher. This isolates the carry —
    // the five values are integer literals, so evaluation is free and every millisecond
    // above `seed_width_1` is the width of the binding alone.
    Shape {
        id: "seed_width_6_const",
        query: "MATCH (n:Person) WITH n, 1 AS c1, 2 AS c2, 3 AS c3, 4 AS c4, 5 AS c5 \
                MATCH (n)-[:KNOWS]->(m:Person) RETURN m",
    },
    // The evaluation-only control: the same five expressions, projected at RETURN where
    // they are never carried into a matcher. Difference against `seed_width_6` is the
    // carry cost, cross-checking the constant-column measurement above by a second route.
    Shape {
        id: "project_5_extra",
        query: "MATCH (n:Person)-[:KNOWS]->(m:Person) \
                RETURN m, n.name, n.age, n.name, n.age, n.age",
    },
    // ── Variable length ─────────────────────────────────────────────────────────
    Shape {
        id: "varlen_distinct",
        query: "MATCH (n:Person)-[:KNOWS*1..2]->(m:Person) RETURN DISTINCT m",
    },
    Shape {
        id: "varlen_count",
        query: "MATCH (n:Person)-[:KNOWS*1..2]->(m:Person) RETURN count(*)",
    },
];

/// Shapes over the `hub` fixture: one `:Hub` with `N` `LINK` edges to `:Leaf`. Pure
/// fan-out from a single anchor — the emit-heavy extreme.
const HUB_SHAPES: &[Shape] = &[Shape {
    id: "hub_fanout",
    query: "MATCH (h:Hub)-[:LINK]->(l:Leaf) RETURN l",
}];

/// Hard ceiling on retained intermediate elements, and on transient walk elements.
///
/// **Not optional.** `0` means *unlimited* for both; `Engine::new` no longer defaults to
/// that sentinel (it starts from `DEFAULT_MAX_INTERMEDIATE` / `DEFAULT_MAX_SCAN`), but a
/// bench must still state its own ceiling: these shapes are deliberately larger than the
/// server default, so inheriting it would measure the budget error instead of the query.
/// On a graph with hubs an unbudgeted expansion will materialise until the box dies rather
/// than returning a budget error. Sized here at ~10x the largest legitimate shape
/// (200k rows), so every intended measurement fits and a runaway one fails fast.
const MAX_INTERMEDIATE: u64 = 8_000_000;
const MAX_SCAN: u64 = 32_000_000;

fn budget() -> QueryBudget {
    QueryBudget::new(MAX_INTERMEDIATE, MAX_SCAN)
}

/// Execute `query` and return the row count, with `fanout` as the per-query pool.
///
/// A budget error is a *bench* error, not a silent zero: it means the shape outgrew its
/// ceiling and its timing would be meaningless.
fn run_with(reader: &Reader, query: &str, fanout: Option<Arc<rayon::ThreadPool>>) -> usize {
    reader.with_engine(budget(), fanout, |engine| {
        let ast = slater::parser::parse(query).expect("parse");
        engine
            .run(&ast)
            .unwrap_or_else(|e| panic!("exec (budget {MAX_INTERMEDIATE}/{MAX_SCAN}): {e}"))
            .rows
            .len()
    })
}

/// A pool of two workers — enough for `chain_parallelizable` to arm without making the
/// measurement a study of this box's core count.
fn pool() -> Arc<rayon::ThreadPool> {
    Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .expect("build fanout pool"),
    )
}

/// Time one shape on both arms, assert it is measuring what it claims, and print a row.
///
/// The block-miss assertion is the load-bearing one: if a warm re-run still faults blocks
/// in, the timing is a study of the block cache and *no other number in this bench may be
/// read*. Panicking is deliberate — a bench that quietly measures the wrong thing is worse
/// than no bench.
fn measure(reader: &Reader, shape: &Shape, group: &str) -> (f64, f64) {
    let seq = |r: &Reader| run_with(r, shape.query, None);

    // Warm the caches, and capture the row count as an assertion that the shape is not
    // silently empty (a fixture/schema drift would otherwise read as "very fast").
    let rows = seq(reader);
    assert!(
        rows > 0 || shape.query.contains("count("),
        "{}/{}: produced no rows — the fixture or the query has drifted",
        group,
        shape.id
    );

    let before = reader.base_misses();
    let t0 = Instant::now();
    let iters = 3;
    for _ in 0..iters {
        std::hint::black_box(seq(reader));
    }
    let seq_ms = t0.elapsed().as_secs_f64() * 1000.0 / f64::from(iters);
    let warm_misses = reader.base_misses() - before;
    assert_eq!(
        warm_misses, 0,
        "{}/{}: a warm re-run faulted {warm_misses} blocks — the cache is too small and \
         this bench is measuring block reads, not the runtime",
        group, shape.id
    );

    let p = pool();
    let t0 = Instant::now();
    for _ in 0..iters {
        std::hint::black_box(run_with(reader, shape.query, Some(p.clone())));
    }
    let par_ms = t0.elapsed().as_secs_f64() * 1000.0 / f64::from(iters);

    println!(
        "  {:<22} seq {seq_ms:>9.2}ms   pool {par_ms:>9.2}ms   rows {rows}",
        shape.id
    );
    (seq_ms, par_ms)
}

/// The ratios the ticket's ranking rule is written against, computed here rather than by
/// hand so the decision cannot drift from the numbers.
fn report_ranking(t: &std::collections::HashMap<&'static str, f64>) {
    let get = |k: &str| t.get(k).copied().unwrap_or(f64::NAN);
    let (chain, control) = (get("chain_3hop"), get("count_control"));
    let (vd, vc) = (get("varlen_distinct"), get("varlen_count"));
    let (s1, s6) = (get("seed_width_1"), get("seed_width_6"));

    println!("\n── ranking inputs (sequential arm — the default configuration) ──");
    println!(
        "  delta_emit   = (chain_3hop - count_control) / chain_3hop = {:.3}",
        (chain - control) / chain
    );
    println!(
        "  r_varlen     = varlen_count / varlen_distinct            = {:.3}",
        vc / vd
    );
    println!(
        "  varlen/chain = varlen_distinct / chain_3hop              = {:.3}",
        vd / chain
    );
    println!(
        "  seed_width_6 / seed_width_1                              = {:.3}",
        s6 / s1
    );

    // The clean number: constant-valued seed columns cost nothing to evaluate, so
    // everything above `seed_width_1` is the binding carry — which is exactly what
    // HIK-217 removes.
    let s6c = get("seed_width_6_const");
    let p5 = get("project_5_extra");
    let p1 = get("expand_1hop");
    println!("\n── isolating the carry from the evaluation ──");
    println!(
        "  seed_width_6_const / seed_width_1 = {:.3}   ({:.2} ms per carried column, \
         evaluation-free)",
        s6c / s1,
        (s6c - s1) / 5.0
    );
    println!(
        "  project_5_extra   / expand_1hop   = {:.3}   ({:.2} ms per *evaluated* column, \
         never carried)",
        p5 / p1,
        (p5 - p1) / 5.0
    );
    println!(
        "  seed_width_6 - project_5_extra    = {:.2} ms  (same five expressions; the \
         difference is carrying them)",
        s6 - p5
    );
}

fn bench_query_runtime(c: &mut Criterion) {
    // Criterion is used for its harness/CLI only; the numbers this bench exists to
    // produce are the wall-clock matrix below, which needs both arms and the miss
    // assertion. Registering one trivial criterion benchmark keeps `cargo bench`'s
    // reporting happy without duplicating every shape.
    let (scale_root, scale_graph) = benchkit::write_scale("qr", N);
    let scale = Reader::open(&scale_root, &scale_graph, CACHE);

    let mut timings: std::collections::HashMap<&'static str, f64> = Default::default();

    println!("\n══ query_runtime — scale fixture (n={N}, out-degree 1, :Person/KNOWS) ══");
    for s in SCALE_SHAPES {
        let (seq, _par) = measure(&scale, s, "scale");
        timings.insert(s.id, seq);
    }

    let (hub_root, hub_graph) = testgen::write_hub("qr", N);
    let hub = Reader::open(&hub_root, &hub_graph, CACHE);
    println!("\n══ query_runtime — hub fixture (n={N}, single anchor, :Hub/LINK) ══");
    for s in HUB_SHAPES {
        let (seq, _par) = measure(&hub, s, "hub");
        timings.insert(s.id, seq);
    }

    // ── Real fixture: degree-capped wd10m ───────────────────────────────────────
    // Only runs against a *capped* generation. The uncapped 10M subgraph peaks at
    // degree 2,409,783, so its worst-case 2-hop frontier is ~5.8e12 rows — running
    // that from here is what crashed the dev box. `--degree-cap 1024` bounds the
    // frontier at cap^2 ~= 1.05M, which fits inside a real budget.
    //
    // This is the only fixture that can rank HIK-218: the synthetic ring is
    // out-degree 1, so its `*1..2` yields two paths per anchor and cannot reproduce
    // a fan-out blow-up at all.
    //
    // Capped numbers UNDERSTATE production cost (the hubs are what make var-length
    // expensive). Valid for A/B regression work; never a competitive figure.
    if let Ok(dir) = std::env::var("SLATER_WD10M_CAP_DIR") {
        println!("\n══ query_runtime — wd10m degree-capped @1024 (10M nodes / 81.5M edges) ══");
        let r = Reader::open(std::path::Path::new(&dir), "wd10m", CACHE_REAL);
        // Anchors are chosen by an indexed `wikidata_id` range, NOT by scan order.
        // `MATCH (n:Entity) ... LIMIT k` takes the first k nodes by dense id, and dense
        // ids here follow CSR order, which follows prominence — so a bare LIMIT hands
        // back exactly the capped hubs and 100 anchors x cap^2 blows an 8M budget.
        // wikidata_id tracks prominence too, so a high-id range selects ordinary
        // low-degree entities. This is the README's own degree-bounded-anchor
        // methodology; the point is the RATIO between the two shapes, not throughput.
        for s in &[
            Shape {
                id: "wd_varlen_distinct",
                query: "MATCH (n:Entity) WHERE n.wikidata_id > 13000000 WITH n LIMIT 100 \
                        MATCH (n)-[:LINK*1..2]->(m:Entity) RETURN DISTINCT m",
            },
            Shape {
                id: "wd_varlen_count",
                query: "MATCH (n:Entity) WHERE n.wikidata_id > 13000000 WITH n LIMIT 100 \
                        MATCH (n)-[:LINK*1..2]->(m:Entity) RETURN count(*)",
            },
            Shape {
                id: "wd_1hop",
                query: "MATCH (n:Entity) WHERE n.wikidata_id > 13000000 WITH n LIMIT 100 \
                        MATCH (n)-[:LINK]->(m:Entity) RETURN m",
            },
        ] {
            let (seq, _) = measure(&r, s, "wd10m-cap");
            timings.insert(s.id, seq);
        }
        let g = |k: &str| timings.get(k).copied().unwrap_or(f64::NAN);
        println!(
            "\n  r_varlen(wd10m-cap) = wd_varlen_count / wd_varlen_distinct = {:.3}",
            g("wd_varlen_count") / g("wd_varlen_distinct")
        );
    } else {
        println!("\n(skipping wd10m: set SLATER_WD10M_CAP_DIR to a DEGREE-CAPPED generation)");
    }

    report_ranking(&timings);

    c.bench_function("query_runtime/seed_width_6", |b| {
        b.iter(|| std::hint::black_box(run_with(&scale, SCALE_SHAPES[8].query, None)))
    });

    let _ = std::fs::remove_dir_all(&scale_root);
    let _ = std::fs::remove_dir_all(&hub_root);
}

criterion_group!(benches, bench_query_runtime);
criterion_main!(benches);
