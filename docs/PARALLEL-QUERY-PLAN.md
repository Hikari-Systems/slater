# Per-query parallelism — implementation plan (decompose → capped fanout → merge)

Each task below is **self-contained for a fresh `/clear`ed session**. Do them in order
(Task 7 is the foundation; 8–12 depend on it and are independent of each other).

## What already shipped (on `main`)

- `900b650` — count-only postings + compact shortestPath BFS (bounded resident memory).
- `db24696` — **bidirectional** shortestPath BFS (minutes → ~316 ms at full scale).
- `7430c7b` — **optional within-level parallelism for shortestPath** (`query.maxShortestPathFanout`,
  default 1). Introduced the building blocks this plan generalizes:
  - `build_shortest_path_pool()` in `server.rs` (shared, process-global rayon pool).
  - `Engine.sp_pool: Option<Arc<rayon::ThreadPool>>` + `with_shortest_path_pool()`.
  - `neighbours_par(gen, cache, node, dir, type_ids)` free fn in `exec.rs` (Sync-only read).
  - The gather/merge split inside `any_shortest_path` (gather adjacency in the pool, mutate
    `visited`/`parent`/meeting single-threaded).
- Rollback tag: **`pre-parallel-shortestpath`** (= `db24696`) if parallelism must be reverted.
- **`7430c7b` KEEP decision (made with Task 7):** full-Wikidata (91.6M/766M) `shortestPath<=6`
  retest on one build, default-off vs fanout=8 — **176.5 ms → 67.9 ms median (2.6× faster)**,
  anon RSS 991 → 1068 MiB (+8%, all thread stacks + per-worker adjacency buffers; not from a
  changed working set). Clear win at modest, bounded memory cost; defaults off. **Kept** — the
  rollback tag stays available but unused.
- `ec24b1b` (Task 7) — **`par_gather` + `maxShortestPathFanout` → `maxFanout`**: one shared
  fanout-capped pool + one order-preserving helper, reused by all per-query operators (the
  shortestPath fast-path gather now calls it). `build_shortest_path_pool` → `build_fanout_pool`
  (thread name `slater-q-{i}`), `ConnCtx.fanout_pool`, `Engine.fanout_pool`/`with_fanout_pool`.
- (Task 8) — **parallel brute-force kNN**. `read_vector(gen, cache, global)` Sync reader +
  `KNN_PAR_MIN = 256`; `vector_group` now `par_gather`s the candidate reads. `vector::brute_force_knn_par`
  scores `par_chunks` (one per worker) → per-chunk top-k → single-threaded merge with the same
  `(score asc, node id asc)` comparator, so the result is identical to sequential element-for-element.
  Wired into `apply_vector_call`'s `AnnMode::BruteForce` arm. Tests: `vector::knn_par_matches_sequential`
  (rayon branch, all metrics, k incl. > group), `knn_par_falls_back_below_threshold_and_without_pool`,
  `knn_par_propagates_dimension_mismatch`, `exec::vector_knn_with_pool_is_correct` (pool wiring).

- (Task 9) — **parallel multi-hop / fixed-length chain expansion**. `expand_chain_par` →
  `par_walk` (gated by `chain_parallelizable`: pool present + non-quantified chain of ≥1
  fixed-length, property-free rels — **and** `cap.is_none()`) walks the chain from each anchor in
  **bounded breadth batches**: each batch's adjacency reads `par_gather` via the new Sync reader
  `hops_par` (mirrors `expand_with_dir` minus `rel_ok`); the merge (`node_ok`/next-var
  guard/`charge()`/path binding) is single-threaded in input order. Batches expand depth-first on
  in-order prefixes, so rows, order and the charge sequence are byte-for-byte identical to the
  sequential `expand_chain`. **Two memory bounds** (the budget only charges at completion, so an
  uncharged frontier must be bounded structurally): `EXPAND_BATCH=512` flushes the next-hop
  frontier depth-first (bounds live *branch* count); `EXPAND_READ_CHUNK=64` reads adjacency in
  node-chunks freed per chunk (bounds the live *read* buffer — a 512-node frontier of hubs
  otherwise buffers ~14M edges in one `neigh`, where the sequential walk holds only one node's →
  was a 14 GiB OOM before this). A dense chain now fails cleanly at `maxIntermediate` like
  sequential. **`cap.is_none()` gate**: a pushed `LIMIT` routes to the sequential early-exit DFS
  (breadth would over-read a high-degree frontier). Var-length / property-bearing rels stay
  sequential too. `resolve_type_filter` factored out of `expand_with_dir`. Wired into
  `match_single_pattern`'s anchor loop. **Perf** (full Wikidata 91.6M/766M, hub pool, maxFanout
  1→8): 2-hop count 127.9→60.3 ms (**2.1×**), 3-hop count 923→503 ms (**1.8×**), counts
  byte-identical, 3-hop trips the 1M budget cleanly under both (no OOM); anon RSS 1.6→5.2 GiB
  (bounded; opt-in). Harness `perf/cross-engine-hs/bench_multihop.py` (uncapped count-based
  multi-hop over high-degree hubs — the existing `LIMIT 100` 2-hop/3-hop suite stays sequential
  by the `cap` gate). Test: `exec::multi_hop_with_pool_matches_sequential`.

- (Task 10) — **parallel anchor scan + `node_ok` filter**. New Sync free fns
  `node_label_ids_par` / `node_prop_par` (the `&self` bodies, now delegating to them) +
  `node_ok_par`, the thread-safe twin of `node_ok`: it checks an anchor's `label_expr`
  and inline props over `&Generation`/`&BlockCache` only. The inline-prop **values**
  (`wants`) don't depend on the candidate, so `match_single_pattern` evaluates them once
  single-threaded (they may touch the `!Sync` evaluator) and the workers do only Sync
  label/column reads + `loose_eq` — accept/reject is byte-for-byte identical, including
  the `guaranteed`-label skip. The candidate loop `par_gather`s the filter when
  `cap.is_none()` (a pushed `LIMIT` would over-read the whole candidate set before the
  cap stops the scan — the early-exit rule, mirrors Task 9), a fanout pool is configured,
  `candidates.len() >= SCAN_PAR_MIN` (64), and `anchor_filter_reads` confirms `node_ok`
  actually reads a record (skips trivial guaranteed-single-atom / unknown-label / no-prop
  anchors). Survivors expand in input order; capped/small/bound-anchor scans keep the
  inline per-candidate filter with its early break. New wide fixture
  `testgen::write_wide(tag, n)` (n nodes, half `:Person` / half `:Company`, `name` + a
  `:Person`-only `team`, no edges/indexes). Test:
  `exec::anchor_filter_with_pool_matches_sequential` (label-scan+inline-prop, boolean/
  negated label exprs, empty result, aggregate, tight budget — pool vs sequential).

- (Task 11) — **parallel `algo.*` subgraph build (`build_view`)**. Each selected node's
  out-adjacency read is independent and Sync, so `build_view` now `par_gather`s the reads
  over the shared fanout pool (`BUILD_VIEW_PAR_MIN = 64`) via the existing `neighbours_par`
  (`Direction::Outgoing`, rel-type filter = the view's `rels`). `neighbours_par` preserves
  the stored edge order and applies the same rel filter, so mapping each neighbour through
  the `pos` index single-threaded (cheap; `pos` is shared read-only) yields the node list +
  0-based `out` byte-for-byte identical to the sequential build — and thus identical WCC /
  pageRank / betweenness / harmonic / CDLP output. No budget on this path (the algos run
  single-threaded on the built view). The `labels = Some` node-set build stays sequential
  (label-posting collection, a different shape; the adjacency reads are the dominant cost
  and the clear win). Test: `exec::build_view_with_pool_matches_sequential` (every algo
  proc shape + rel/label filters on the edge-bearing `write_basic` fixture pinning the
  merge; `write_wide(200)` clearing the threshold to exercise the rayon read branch).

- (Task 12) — **parallel group-by / `count(DISTINCT)` reduction**. The aggregation
  projection (`project_aggregated`) now `par_gather`s the per-row group-key and
  aggregate-argument reads over the shared fanout pool (`AGG_PAR_MIN = 64`) when the
  shape is **`simple_readable`** — every grouping item and aggregate argument is a
  bound var, literal, param, or one-level `var.key` property read (the Sync-evaluable
  subset). The new Sync free fn `eval_simple(gen, cache, params, cols, row, expr)`
  evaluates those over `&Generation`/`&BlockCache`/`&params` only; it shares the
  property-access body with `Engine::eval` via the extracted free fns `property_val`
  (the `Engine::property` body) and `edge_prop_par` (the `Engine::edge_prop` body),
  so a precomputed cell is **byte-for-byte identical** to the sequential per-row
  `eval`. `plan_par_aggregation` classifies each output item into an `AggItem`
  (`Group{slot}` / `CountStar` / `Agg{name,distinct,slot}`); only **bare** aggregate
  calls (not aggregates nested in an expression) over simple args qualify, and
  two-arg `percentile*` bails to the sequential path. `project_aggregated_par` then
  builds the `BTreeMap<GroupKey, Vec<usize>>` and reduces **single-threaded in input
  order** — charging each new group and each non-null aggregated value (and the
  DISTINCT dedup set) in the *same order* as the sequential body, so the
  `maxIntermediate` budget trips/fits identically. The final value→result reduction
  is the shared free fn `reduce_agg` (factored out of `compute_aggregate`,
  non-percentile arm). Anything else (regex, aggregate-in-expression, nested
  property, no pool, table < `AGG_PAR_MIN`, or no per-row read slots) falls back to
  the unchanged sequential `project_aggregated`. Test:
  `exec::aggregation_with_pool_matches_sequential` (group-by + count(\*),
  count(DISTINCT), multi-aggregate incl. order-sensitive `collect`, min/max,
  no-group single-arg aggregate, constant + `$param` grouping items, tight budgets —
  pool vs sequential, on `write_wide(200)`).

## The reusable pattern

> **gather** a set of independent sub-operations — each doing only `&Generation` + `&BlockCache`
> reads — across a **shared, fanout-capped pool**; **merge** single-threaded.

### Two hard invariants (apply to every task)

1. **Block cache releases its mutex across I/O.** `cache.rs::BlockCache::get_or_try_insert`
   runs `load()` (disk read + zstd decompress) *without* the lock. This is why parallel
   readers' misses overlap. Don't add a parallel reader that holds a lock across I/O.
2. **`Engine` is `!Sync`** (`regex_cache: RefCell`, `budget_used: Cell`). Worker closures
   must **not** capture `&self` or call `&self` methods that touch interior-mutable state
   (regex eval, `charge()`). They call free fns over `&Generation`/`&BlockCache` (both
   `Send + Sync`, shared via `Arc` across connections). Anything needing regex/budget runs
   in the single-threaded merge.

### Cross-cutting rules

- **Budget (`maxIntermediate`)**: charge in the single-threaded merge. (Do **not** make it
  atomic unless a task explicitly needs workers to charge — `regex_cache` keeps `Engine`
  `!Sync` anyway, so workers can't take `&self`.)
- **Determinism**: collect per-item results then merge **in input order**, so results and the
  result-cache key stay reproducible.
- **`LIMIT` / early-exit**: don't eagerly fan out work past a satisfiable `LIMIT`; cap the
  parallel batch or only parallelize when no early-exit applies.
- **Default off**: every operator uses the shared `maxFanout` (Task 7); default 1 keeps the
  server throughput-first.

### Verification (every task)

- `docker run --rm -v "$PWD":/app -w /app rust:1-bookworm bash -lc 'export PATH=/usr/local/cargo/bin:$PATH; cargo test -p slater --lib'`
  (host has no Rust toolchain; build in this container). All tests must pass.
- Add a **correctness test**: pool-configured engine returns identical results to sequential
  (mirror `shortest_path_with_pool_is_correct` in `exec.rs`).
- `cargo fmt -p slater` in the container before committing (pre-commit hook can't run rustfmt
  on the host).
- Commit with a `perf(query): …` message ending `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## Task 7 — Foundation: `par_gather` + generalize `maxShortestPathFanout` → `maxFanout`

**Goal:** one shared pool + one helper, reused by all sites. **Do this first.**

1. **Config rename** (`crates/slater/src/config.rs`):
   - `QueryConfig.max_shortest_path_fanout` → `max_fanout`; serde `maxFanout`;
     `default_max_shortest_path_fanout` → `default_max_fanout` (still returns 1); update the
     `Default for QueryConfig` impl.
   - `config.json`: `"maxShortestPathFanout": 1` → `"maxFanout": 1`.
2. **Server** (`crates/slater/src/server.rs`):
   - `build_shortest_path_pool` → `build_fanout_pool` (logic unchanged; thread name `slater-q-{i}`).
   - `ConnCtx.shortest_path_pool` → `fanout_pool`; build from `cfg.query.max_fanout`.
   - Engine build site (~`with_shortest_path_pool`): rename to `with_fanout_pool`.
   - Both test `ConnCtx` literals: `shortest_path_pool: None` → `fanout_pool: None`.
3. **Engine** (`crates/slater/src/exec.rs`):
   - `Engine.sp_pool` → `fanout_pool`; `with_shortest_path_pool` → `with_fanout_pool`.
   - Add the reusable helper (free fn, near `neighbours_par`):
     ```rust
     /// Map `f` over `items` on the shared fanout pool (or sequentially when the pool is
     /// absent or `items` is smaller than `min_batch`), preserving input order. `f` must
     /// read only Sync state (&Generation/&BlockCache) — never the !Sync Engine.
     fn par_gather<I: Sync, T: Send>(
         pool: Option<&rayon::ThreadPool>,
         items: &[I],
         min_batch: usize,
         f: impl Fn(&I) -> Result<T> + Sync + Send,
     ) -> Result<Vec<T>> {
         match pool {
             Some(p) if items.len() >= min_batch => p.install(|| items.par_iter().map(&f).collect()),
             _ => items.iter().map(|x| f(x)).collect(),
         }
     }
     ```
   - Refactor `any_shortest_path`'s fast-path gather to call
     `par_gather(self.fanout_pool.as_deref(), front, SP_PAR_MIN_FRONTIER, |&node| neighbours_par(gen, cache, node, dir, tids).map(|n| (node, n)))`.
4. Keep `SP_PAR_MIN_FRONTIER = 64`. Tests + fmt + commit (`perf(query): generalize per-query
   fanout into a shared pool + par_gather`).

**Note:** also run the **parallel shortestPath perf retest** that was deferred (see "Benchmark"
section) and make the keep/rollback call for `7430c7b` while you have the pool wired.

---

## Task 8 — Parallel brute-force kNN  *(cleanest; no budget/order/LIMIT entanglement)*

**Where:** `apply_vector_call` (`exec.rs`, search `CALL db.idx.vector.queryNodes`) +
`vector_group` (`exec.rs`, `fn vector_group`). `AnnMode::BruteForce` only.

**Change:**
- Make a Sync reader `read_vector(gen, cache, record_idx) -> Result<VectorEntry>` (mirror the
  body of `vector_group`'s loop; uses `gen.vectors().inner()` + `cache.record(..., FileKind::Vectors, ..)`).
- Replace the sequential read of the index group with
  `par_gather(pool, &(first_record..first_record+count).collect::<Vec<_>>(), KNN_PAR_MIN, |&r| read_vector(gen, cache, r))`
  — or chunk the range and parallel-map chunks to avoid the id Vec.
- Distance compute + top-k: parallel **map + reduction** — each worker keeps a bounded
  min/max heap of size `k`; merge the per-worker heaps. `vector::brute_force_knn` may already
  factor the scoring; keep `score` = distance ascending (D26) and **stable tie-break by node id**
  so results stay deterministic.

**Caveats:** pure compute over independent reads; no budget. Keep the result order stable.
**Test:** kNN with pool vs without returns identical (node, score) rows on the EU-AI-Act-style
fixture (or a small vector fixture). **Payoff:** the README's exact-kNN gap (~16 ms vs ~1 ms HNSW).

---

## Task 9 — Parallel multi-hop / var-length expansion  *(highest traffic)*

**Where:** `expand_chain` (`exec.rs:~3196`) driven by `match_single_pattern` (`exec.rs:~3070`).

**Change:** parallelize each hop level's **adjacency reads** (per frontier node) with
`par_gather` + a `neighbours_par`-style reader (reuse `neighbours_par`; for var-length the rel
type filter is the same shape). Then, **single-threaded**, in input order: dedup/visited,
**`charge()` the budget**, bind hop vars, recurse/emit, and **respect `LIMIT` pushdown**
(`cap`) — stop issuing parallel batches once `out.len() >= cap`.

**Caveats:** this path charges `maxIntermediate` and supports early-exit — both must stay in
the merge. Gate parallelism on property-free / regex-free rel patterns; fall back to the
current `expand_one_hop` path otherwise. Var-length recursion: parallelize per-level frontier,
not the recursion itself.

**Test:** 2-hop / 3-hop / `*1..2` with pool vs without return identical rows (and identical
under a tight `maxIntermediate` and a `LIMIT`). **Payoff:** bench 2-hop/3-hop/var-length.

---

## Task 10 — Parallel anchor scan + `node_ok` filter

**Where:** `scan_candidates` (`exec.rs:~3536`) → `node_ok` (`exec.rs:~3591`) inside
`match_single_pattern`'s candidate loop.

**Change:** after `scan_candidates` yields ids, evaluate the residual predicate per candidate
in parallel via `par_gather` calling a Sync predicate reader (reads `node_props`/labels through
the cache). Merge: keep the candidates that pass, in order, applying `LIMIT`.

**Caveats:** **gate OFF** when the residual uses `=~` (regex) or anything routing through
`regex_cache`/`eval` that touches `!Sync` state — those stay sequential. Preserve order + LIMIT.

**Test:** label-scan + `WHERE` filter (e.g. MeSH `CONTAINS`/`type=`) with pool vs without
identical. **Payoff:** MeSH unanchored scans + wide scans.

---

## Task 11 — Parallel `algo.*` subgraph build (`build_view`)

**Where:** `build_view` (`exec.rs:~2597`).

**Change:** the per-node adjacency reads that populate the view are independent. `par_gather`
the selected nodes → each yields its out-adjacency (filtered to selected nodes / rel types);
merge into the `GraphView` (node list + 0-based `out` index). The node-set build (when
`labels = Some`) can reuse Task 10's parallel label collection.

**Caveats:** the 0-based index mapping must be built from the final node list (merge step).
**Test:** an `algo.*` proc (e.g. pageRank/betweenness) over a fixture, pool vs without, identical.

---

## Task 12 — Parallel group-by / `count(DISTINCT)` reduction

**Where:** the aggregation paths (search `fn …group…` / `DISTINCT` in `exec.rs`; see the
`grouped_index_*` tests for the shapes).

**Change:** parallelize the scan+read phase; aggregate as a **reduction** — each worker builds
a partial `HashMap<groupkey, acc>` (or partial DISTINCT set), merged single-threaded.

**Caveats:** charges `maxIntermediate` — charge during the merge (partial sizes + final).
Deterministic group order on output. **Test:** group-by + `count(DISTINCT)` pool vs without
identical (and under a tight budget). **Payoff:** bench group-by (~19 ms).

---

## (Lower priority, optional)

- **UNION branches / independent MATCH patterns** (`run_single`): run whole sub-queries
  concurrently — coarse fan-out when finer-grained parallelism is saturated.
- **AllShortest / ShortestK** cloned-visited search: per-branch parallel, but budget-charged
  and order-sensitive (`ShortestK` wants the first k) — highest care, lowest priority.

---

## Parked: the full-Wikidata benchmark (separate track)

State when this plan was written:
- slater (`slater-hs`) serves the full ACL-stamped v0.5.0 generation
  (`/tmp/wdbuild/data/wikidata`, graph `wikidata`, 91.6M/766M). Fast-query sweep done
  (`/tmp/bench-hs/results-wikidata/slater.run*.json`); idle anon 71 MiB, fast-query anon peak
  ~1.44 GiB; shortestPath sequential ~316 ms median / ~0.36 GiB.
- Neo4j (`neo4j-hs`, volume `neo4j_wikifull`, 91.6M/766M, range idx online) fast-query sweep
  ran (`results-wikidata/neo4j.run*.json`); count ~4.7 s (disk-bound scan).
- **Pending:** Neo4j shortestPath measurement; write the "Full Wikidata" section into
  `perf/cross-engine-hs/README.md`; push. **DONE: slater parallel-shortestPath perf retest** —
  rebuilt the image (`DOCKER_BUILDKIT=0`; added a `.dockerignore` so the legacy builder stops
  choking on root-owned `target/` files), served `/tmp/wdbuild/data` with
  `/tmp/bench-hs/config.fanout{1,8}.json`, sampled anon via `sample_anon.sh`:
  `shortestPath<=6` **176.5 ms (fanout=1) → 67.9 ms (fanout=8)**, anon 991 → 1068 MiB. (The
  176 ms baseline beats the earlier 316 ms note — warmer page cache / different sampled pairs;
  the apples-to-apples same-build fanout=1↔8 comparison is the basis for the KEEP call above.)
  Retest harness: `/tmp/bench-hs/sp_retest.sh`; results in
  `/tmp/bench-hs/results-wikidata/slater.sp.fanout{1,8}.json`. **LadybugDB is on hold** (edge COPY needs a big
  buffer pool, run alone — `LADYBUG_LOAD_BUFFER_POOL`). FalkorDB/Memgraph = cannot-load.
- Harness honours `ENGINES`, `WIKI_GRAPH`, `BENCH_SKIP`; `sample_anon.sh` captures anon
  high-water (cgroup `memory.peak` is dominated by reclaimable page cache at this scale —
  report **anon**).
- **One big container at a time** (host is 15 GiB).
