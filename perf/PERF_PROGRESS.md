# Slater cold-execution performance — staged fix tracker

> **READ THIS FIRST (resuming in a fresh/clear context).**
> This file is the single source of truth for this work. To continue:
> 1. Read the **Status** table → find the first stage not `DONE`.
> 2. Read that stage's section under **Stages** (problem, change, files, validate, acceptance).
> 3. Make the change on a branch `perf/stage-N-...`, validate with the harness below.
> 4. Update the stage's **Status** + **Result** here, commit this file with the code, open a PR.
> Do **not** trust ambient memory — everything needed is in this file. Keep the **Baseline**
> table frozen; only append "after" numbers per stage.

## Status

| # | Stage | Risk | Status | Result (uncached median) |
|---|-------|------|--------|--------------------------|
| 0 | Config bool deser (server boots from current source) | low | **DONE** | server boots from current source ✓ |
| 1 | Parameters → index selection | low | **DONE** | idx-eq **0.8 ms** (from 901 ms); 1-hop ~30–100 ms (from 952 ms) |
| 2 | Skip redundant per-node label re-check | low | **DONE** | Crime-label-count **0.64 ms** (from 841 ms); also fixed CONTAINS/agg/DISTINCT |
| 3 | Count fast-paths (metadata / index cardinality) | low | **DONE** | counts **0.6–0.8 ms** (from 14–901 ms) |
| 4 | Block-cache record read (no per-record copy / less locking) | high | **DONE** | broad floor drop: 2-hop **48→1.95 ms** (Neo4j parity), agg/CONTAINS/DISTINCT ~3×, 3-hop ~49 ms |
| 5 | Streaming aggregation + projection pushdown + prop memo | med | **DONE** | agg **19.5→10.3 ms**, DISTINCT **20.5→11.6 ms**, CONTAINS **16→9.0 ms** (~1.8× each; now 1.3–2× Neo4j) |
| 6 | Traversal frame **+ LIMIT pushdown** (the real lever) | med | **DONE** | 3-hop **45→1.6 ms** (now *beats* Neo4j), 1-hop **4.8→2.6 ms**, 2-hop 2.0→1.6 ms; no row regressions |

Recommended order: **0 → 1 → 2 → 3** (local, high payoff, low risk) — **DONE 2026-06-12**, see
"Validation results (Stages 0–3)" below. **Stage 4 DONE 2026-06-12** — see "Validation results
(Stage 4)". **Stage 5 DONE 2026-06-12** — see "Validation results (Stage 5)". **Stage 6 DONE
2026-06-12** — see "Validation results (Stage 6)". All six stages are now complete; cold
execution matches or beats Neo4j on every benchmark row except the three Crime-anchored full
scans (agg/CONTAINS/DISTINCT, 1.3–2.3× off), which stay compute-bound (no further fast path
applies without changing the answer).
**Stage 0 is a hard prerequisite** for any server-based validation of Stages 1–6 (see below).

### Bonus (this round): result-cache disable switch
`config.cache.resultCacheBytes` ≤ 0 now **disables** the result LRU entirely (`get` always
misses, `insert` is a no-op) — see `cache.rs` `ResultCache::{enabled,get,insert}` and
`config.rs` `de::usize_floor0`. Added so cold-execution can be benchmarked honestly without
restarting the server, and for deployments that want no result reuse. The old `.max(1)` clamp
that the Baseline note complained about is now bypassed when the budget is 0.

## Background

A faithful export of the "pole" Manchester crime graph (61,521 nodes / 105,840 rels) was loaded
into slater and benchmarked against Neo4j over Bolt. slater wins **memory** (129 MB vs 539 MB at
matched 64 MB cache) and wins **repeated/cached** queries via its result cache (0.4–1.4 ms), but
its **cold/uncached execution is 100–600× slower** than Neo4j on label scans, parameterized
lookups, aggregations and traversals. The causes are local decisions in the planner / executor /
block cache, all fixable without changing slater's architecture or memory story. Each stage below
targets one cause and is independently landable and measurable.

### Root causes (file:line → measured symptom)

1. **Params invisible to index selection.** `crates/slater/src/plan.rs:333` `literal()` matches
   only `Expr::Literal`; `inline_preds` (`plan.rs:203`) and `compare_operands` (`plan.rs:289`)
   drop predicates whose value is a `$param`, so `choose_from_preds` falls to `LabelScan`
   (`plan.rs:246`). → param `{type:$t}` = **901 ms** vs literal `{type:'…'}` = **46 ms**.
2. **Redundant label re-validation.** Candidates already come from the resident label posting
   (`exec.rs:2028` `nodes_with_label`), yet `match_single_pattern` re-checks each via
   `node_ok`→`node_label_ids` (`exec.rs:1748`→`591`), decoding a label record per node.
   `count_all` skips this because its pattern has no label (`exec.rs:2038` guard). →
   `MATCH (c:Crime) RETURN count(c)` = **841 ms** vs `MATCH (n) RETURN count(n)` = **14 ms**.
3. **Per-record global lock + heap copy.** `crates/slater/src/cache.rs:241` `record()` takes a
   single shared `Mutex` (`cache.rs:225`) and returns `rec.to_vec()` (`cache.rs:253`) on every
   node/edge/label/property access. ~30 µs/record dominates every scan and traversal.
4. **Eager full-table materialization; no count fast-path.** `match_single_pattern` builds a
   `Vec<HashMap>`, one clone per row (`exec.rs:1751`); `count` is `indices.len()` over it
   (`exec.rs:2223`). Resident metadata exists (`exec.rs:1332` uses `nodes_with_label().len()`;
   `label_postings` resident at `generation.rs:66`) but the query path ignores it.
5. **Property reads decode the whole map per access, unmemoized.** `node_prop` (`exec.rs:639`)
   decodes the full record and linear-scans for one key; k returned props = k full decodes.
6. **Traversals clone the binding map per hop and buffer all paths** (`exec.rs:1855`, `1892`).
   → 3-hop = **2242 ms** vs Neo4j 3.6 ms.

### Stage 0 prerequisite (why the server won't boot from current source)

`crates/slater/src/config.rs:9-13` documents that `hs_utils::config::load_layered_value()`
stringifies every scalar config leaf. Numeric fields use `deser_*_or_str` helpers, but
`require_acl_stamp: bool` (`config.rs:97`) uses a plain `bool` deserializer, so the stringified
`"true"`/`"false"` fails: `Error: deserialise Slater config … invalid type: string "true",
expected a boolean`. **A current-source build of `slater` (server) therefore cannot start** — the
prebuilt binary used for the baseline predates this. Stage 0 fixes it; until then, server-based
validation of Stages 1–6 is blocked (the baseline harness used the old prebuilt binary).

## Baseline (FROZEN — measured 2026-06-12, matched 64 MB page/block cache)

slater uncached (real execution) vs Neo4j; `perf/bench.py` reproduces these.

| query | slater uncached | slater cached | Neo4j | n4j/slater |
|-------|----------------:|--------------:|------:|-----------:|
| count all nodes | 14 ms | 0.44 ms | 4–9 ms | 1/3.4 |
| Crime label count | 841 ms | 0.44 ms | 2–6 ms | 1/286 |
| point lookup (idx nhs_no) | 13 ms | 0.42 ms | 1.3–5 ms | 1/6 |
| idx-eq count (Crime.type, param) | 901 ms | 0.46 ms | 1.3–5 ms | 1/465 |
| 1-hop Crime→Location (param) | 952–998 ms | 1.34 ms | 2.6–8 ms | 1/245 |
| 2-hop Person→Loc→Area | 45 ms | 1.30 ms | 2.2–8 ms | 1/15 |
| agg crimes by type | 885 ms | 0.53 ms | 9–11 ms | 1/94 |
| 3-hop Officer/Crime/Loc | 2242 ms | 1.33 ms | 3.6–5.5 ms | 1/627 |
| full-scan CONTAINS | 879 ms | 0.44 ms | 5.6–8 ms | 1/158 |
| count DISTINCT type | 890 ms | 0.44 ms | 7–11 ms | 1/120 |

**Memory:** slater **129 MB** RSS vs Neo4j **539 MB** at 64 MB page cache (866 MB at 512 MB).
**Cached note:** slater's result cache *could not* be disabled — `cache.rs:422` clamped the
budget to `.max(1)`, so `resultCacheBytes:0` still cached. **This round added a real disable
switch** (`resultCacheBytes` ≤ 0 → pool off); the validation run below uses it, which is why its
"cached" column ≈ "uncached" (no result reuse). The original "uncached" column used varying
parameters to force misses.

## Validation results (Stages 0–3, measured 2026-06-12, cache DISABLED)

Same machine/data as the Baseline, served by a **freshly built image of current source**, with
`resultCacheBytes: 0` so *every* query truly executes (the "cached" column ≈ "uncached" confirms
the disable). Compare against the frozen Baseline above.

| query | baseline uncached | **now (cache off)** | Neo4j | vs Neo4j | stage |
|-------|------------------:|--------------------:|------:|---------:|-------|
| count all nodes | 14 ms | **0.63 ms** | 3.3 ms | **5.3× faster** | 3 |
| Crime label count | 841 ms | **0.61 ms** | 1.7 ms | **2.8× faster** | 2+3 |
| point lookup (idx nhs_no) | 13 ms | **0.69 ms** | 1.2 ms | **1.8× faster** | 2 |
| idx-eq count (Crime.type) | 901 ms | **1.58 ms** | 1.3 ms | ~parity | 1+3 |
| 1-hop Crime→Location | 952 ms | **~99 ms** (min 19) | 2.0 ms | 1/49× | 1 (anchor); traversal=6 |
| 2-hop Person→Loc→Area | 45 ms | 48 ms | 2.0 ms | 1/24× | 6 (unchanged) |
| agg crimes by type | 885 ms | **61 ms** | 7.6 ms | 1/8× | 2 done; 5 left |
| 3-hop Officer/Crime/Loc | 2242 ms | ~3155 ms | 2.2 ms | 1/1420× | 6 (unchanged*) |
| full-scan CONTAINS | 879 ms | **58 ms** | 4.5 ms | 1/13× | 2 done; 5 left |
| count DISTINCT type | 890 ms | **61 ms** | 6.5 ms | 1/9× | 2 done; 5 left |

**Headline:** the four pure count/index rows go from 13–901 ms to **sub-2 ms and now beat or match
Neo4j**. The label-skip (Stage 2) also cut every Crime-anchored full scan ~15× (CONTAINS, agg,
DISTINCT 879–890 ms → ~60 ms). Traversal-bound rows (1/2/3-hop) and large aggregations are
**Stages 5–6** and are deferred to the next round. Memory unchanged/better: slater **~79 MB** RSS
vs Neo4j **~655 MB**.

\* **3-hop "regression" (2242 → 3155 ms) is not from Stages 0–3.** The traversal hot loop
(`expand_chain`/`expand_one_hop`/`outgoing`/`incoming`) is byte-for-byte unchanged this round, and
`node_ok`'s cost was restored to the pre-Stage-2 level (the guaranteed-empty fast path skips the
symbol-table lookup). The gap is a measurement-context artifact — the frozen Baseline was taken
from a *different prebuilt binary* (an older `target/release/slater`). Stage 6 rewrites this path
and will re-baseline it.

> ⚠️ **Benchmarking gotchas discovered this round (read before re-measuring):**
> 1. **Stale host binary shadowing the port.** A leftover host process
>    (`target/release/slater`) was bound to `127.0.0.1:7687`; `bench.py` connected to *it* instead
>    of the Docker container, so the new binary's wins were invisible. Before benching:
>    `pgrep -af "target/release/slater"` and kill any host listener, or bind the container to a
>    different host port. Confirm with `ss -ltnp | grep 7687`.
> 2. **Result-cache warmth across runs.** With the cache *on*, `$k`-tagged queries cache the same
>    0..24 key set every run, so a *second* run reads as all-hits (~0.4 ms) and looks impossibly
>    fast. Use the new `resultCacheBytes: 0` for honest cold numbers, or restart the container
>    between runs.
> 3. **Small param pools < `--meas`.** CONTAINS (14 terms) and idx-eq/1-hop (13 crime types)
>    repeat within a single run, so even one run partially cache-hits unless the cache is disabled.

## Validation results (Stage 4 — block-cache borrowed record, measured 2026-06-12, cache DISABLED)

Freshly built current-source image (`resultCacheBytes: 0`, so "cached" ≈ "uncached" confirms no
reuse), same machine/data, Neo4j on :7688 as the parity reference. **No `!rows` mismatch on any
row** → correctness preserved. Two back-to-back runs agreed within noise; the medians below are
representative.

| query | Stages 0–3 (cache off) | **Stage 4 (cache off)** | Neo4j | vs Neo4j |
|-------|----------------------:|------------------------:|------:|---------:|
| count all nodes | 0.63 ms | **0.56 ms** | 3.3 ms | 5.7× faster |
| Crime label count | 0.61 ms | **0.58 ms** | 2.2 ms | 3.7× faster |
| point lookup (idx nhs_no) | 0.69 ms | **0.64 ms** | 1.1 ms | 1.7× faster |
| idx-eq count (Crime.type) | 1.58 ms | **1.45 ms** | 1.3 ms | ~parity |
| 1-hop Crime→Location | ~99 ms | **~5 ms** (min 2.7) | 2.2 ms | 1/2.2× |
| 2-hop Person→Loc→Area | 48 ms | **1.95 ms** | 2.1 ms | **~parity** |
| agg crimes by type | 61 ms | **19.5 ms** | 7.8 ms | 1/2.5× |
| 3-hop Officer/Crime/Loc | ~3155 ms* | **~49 ms** | 2.1 ms | 1/24× |
| full-scan CONTAINS | 58 ms | **16 ms** | 4.5 ms | 1/3.6× |
| count DISTINCT type | 61 ms | **20.5 ms** | 6.6 ms | 1/3.1× |

**Headline:** removing the per-record `to_vec()` copy **and** the per-record offset-table
allocation from the cached-record read gave the predicted broad floor drop on every scan/traversal
row. **2-hop reached Neo4j parity** (48 → 1.95 ms); aggregations and full scans fell ~3× (now
2.5–3.7× off Neo4j — these are Stage 5's target); 1-/3-hop traversals fell ~20× and are now the
remaining gap for Stage 6. The pure count/index rows (Stages 1–3) are unchanged, as expected.
Memory unchanged/better: slater **65 MiB** RSS vs Neo4j **655 MiB**.

\* The 3-hop "before" (3155 ms) was the cross-binary artifact flagged under Stages 0–3; Stage 4 now
gives a clean current-source 3-hop of ~49 ms, re-baselining it for Stage 6.

## Validation results (Stage 5 — streaming scan + single-key prop decode, measured 2026-06-12, cache DISABLED)

Freshly built current-source image (`resultCacheBytes: 0`), same machine/data, Neo4j on :7688.
**No `!rows` mismatch on any row** → correctness preserved. Two back-to-back runs agreed within
noise; medians below are representative (the two runs are shown where they differ).

| query | Stage 4 (cache off) | **Stage 5 (cache off)** | Neo4j | vs Neo4j |
|-------|-------------------:|------------------------:|------:|---------:|
| count all nodes | 0.56 ms | 0.61–0.63 ms | 3.0–3.4 ms | ~5× faster |
| Crime label count | 0.58 ms | 0.60–0.63 ms | 1.7–1.8 ms | ~2.8× faster |
| point lookup (idx nhs_no) | 0.64 ms | 0.69 ms | 1.1 ms | 1.6× faster |
| idx-eq count (Crime.type) | 1.45 ms | 1.58 ms | 1.1 ms | 1/1.4× |
| 1-hop Crime→Location | ~5 ms | **4.8 ms** | 2.0–2.4 ms | 1/2.0–2.4× |
| 2-hop Person→Loc→Area | 1.95 ms | 2.04–2.08 ms | 2.1–2.7 ms | **~parity** |
| agg crimes by type | 19.5 ms | **10.3 ms** | 7.7–7.9 ms | 1/1.3× |
| 3-hop Officer/Crime/Loc | ~49 ms | 45 ms | 2.1 ms | 1/21× |
| full-scan CONTAINS | 16 ms | **9.0 ms** | 4.3–4.5 ms | 1/2.0× |
| count DISTINCT type | 20.5 ms | **11.6 ms** | 6.6–6.8 ms | 1/1.7–1.8× |

**Headline:** the three Stage-5 target rows each fell ~**1.8×** (agg 19.5→10.3, DISTINCT
20.5→11.6, CONTAINS 16→9.0 ms) and are now **1.3–2.0× off Neo4j** (agg nearly at parity). Two
changes, both on the Crime-anchored full-scan path:
1. **Streaming single-pattern scan** (`exec.rs` `try_stream_match`): a node-only `MATCH` with no
   relationships streams candidates straight into output rows, dropping the per-row
   `HashMap<String, Val>` binding the general matcher allocates+clones (root cause 4). The anchor
   scan is chosen once, `node_ok` enforces labels/inline props, and the clause `WHERE` is
   re-evaluated per emitted row — identical semantics (row order, intermediate-budget charge).
2. **Single-key property decode** (`columns::decode_one` + `wire::skip_value`): `node_prop`/
   `edge_prop` now decode only the requested key from the cached record, *skipping* the other
   values (stepping over strings/lists/vectors without allocating) instead of decoding the whole
   map into a `Vec<(u32, Value)>` and linear-scanning (root cause 5). Each target reads exactly
   one property per row, so this removes k−1 value allocations per node.

The count/index rows (Stages 1–3) are unchanged within noise. 1-/3-hop traversals are essentially
unchanged (they don't take the no-rel streaming path) and remain Stage 6's target. Memory
unchanged/better: slater **62 MiB** RSS vs Neo4j **758 MiB**. Tests:
`graph-format` `columns::tests::decode_one_matches_full_decode_and_skips`; `slater`
`exec::tests::streaming_scan_{where_and_property_projection,group_by_property_aggregation,inline_prop_filter}`
(327 slater + 50 graph-format lib tests green).

## Validation results (Stage 6 — traversal frame + LIMIT pushdown, measured 2026-06-12, cache DISABLED)

Freshly built current-source image (`resultCacheBytes: 0`), same machine/data, Neo4j on :7688.
**No `!rows` mismatch on any row** → correctness preserved. Two back-to-back runs agreed within
noise; medians below are representative.

| query | Stage 5 (cache off) | **Stage 6 (cache off)** | Neo4j | vs Neo4j |
|-------|-------------------:|------------------------:|------:|---------:|
| count all nodes | 0.61–0.64 ms | 0.59–0.64 ms | 2.8–3.2 ms | ~5× faster |
| Crime label count | 0.59–0.61 ms | 0.59–0.61 ms | 1.6 ms | ~2.7× faster |
| point lookup (idx nhs_no) | 0.66 ms | 0.67–0.69 ms | 1.0 ms | ~1.5× faster |
| idx-eq count (Crime.type) | 1.52–1.56 ms | 1.55–1.57 ms | 1.0–1.1 ms | 1/1.5× |
| 1-hop Crime→Location | 4.7–5.4 ms | **2.6 ms** (min 2.3) | 1.9–2.1 ms | 1/1.3–1.4× |
| 2-hop Person→Loc→Area | 2.0–2.1 ms | **1.6 ms** | 1.9–2.3 ms | **1.1–1.4× faster** |
| agg crimes by type | 10.0–10.3 ms | 10.4 ms | 8.1–8.2 ms | 1/1.3× |
| 3-hop Officer/Crime/Loc | 44–45 ms | **1.6 ms** (min 1.5) | 2.0–2.2 ms | **1.2–1.3× faster** |
| full-scan CONTAINS | 9.0–9.2 ms | 9.1–9.2 ms | 3.9–4.1 ms | 1/2.2–2.3× |
| count DISTINCT type | 11.2–11.5 ms | 11.3–11.5 ms | 6.4–6.5 ms | 1/1.8× |

**Headline: the 3-hop fell 45 → 1.6 ms (~28×) and now *beats* Neo4j; the 1-hop fell 4.8 → 2.6 ms;
the 2-hop also improved to 1.6 ms (now faster than Neo4j).** Every other row is unchanged within
noise — no regression on the sub-2 ms count/index rows or the Stage-5 agg/CONTAINS/DISTINCT rows.
Memory unchanged (~62 MiB RSS vs Neo4j ~750 MiB).

### What actually moved the needle (the doc's hypothesis was wrong)

Root cause 6 named **two** things — "per-hop `binding.clone()`" and "buffer all paths". Measurement
proved it was the **second**, via `LIMIT` interaction, not the first:

- **The binding-clone frame was a no-op on the benchmark.** I first did exactly what the plan said:
  replaced the per-hop `HashMap<String, Val>` clone in `expand_chain`/`match_single_pattern` with a
  mutate-in-place frame (`restore_binding`, push/pop backtracking; `walk` and the var-length `used`
  set already worked this way). It is correct and removes real allocations, but the bench was
  **flat** (3-hop 44→46 ms, 1-hop unchanged). The per-branch clone was never the dominant cost.
- **The real cost was materializing every path before a non-pushed `LIMIT`.** The 3-hop query
  `MATCH (o:Officer)<-[:INVESTIGATED_BY]-(c:Crime)-[:OCCURRED_AT]->(l:Location) … LIMIT 100`
  produces **28,762** paths (one per crime); the executor buffered all of them into a
  `Vec<HashMap>` and only then truncated to 100 in `project`. The 2-hop looked "at parity" only
  because its pattern yields just **368** paths; the 1-hop (one crime type) yields **2,807**. Neo4j
  is fast here because it pulls lazily and stops at 100.
- **Fix — LIMIT pushdown / early-stop.** `projection_row_cap` computes a row cap of `SKIP + LIMIT`
  when the final projection is a plain 1:1 map (no aggregation, no `DISTINCT`, no `ORDER BY`, has a
  `LIMIT`). `run_single_seeded` applies it to the **last** reading clause only (earlier clauses may
  filter/expand downstream). The cap threads through `apply_match` → `match_patterns` →
  `match_single_pattern` → `expand_chain` (and the no-rel `try_stream_match`), which stop emitting
  once `out` reaches the cap. It is exact: early-stop returns the **same prefix** in match-emit
  order that buffer-then-truncate did — proven by `limit_pushdown_*` tests comparing limited vs full
  results. The per-pattern walk is capped only when it is the last pattern **and** there is no
  residual `WHERE` (otherwise the terminal `WHERE`/downstream patterns can drop rows, so a tight cap
  could under-produce — guarded by the `sp_cap = … && where_.is_none()` condition).

The binding frame is kept (it's a correct, allocation-reducing cleanup and the characterization
tests lock its semantics), but the measurable Stage-6 win is entirely the LIMIT pushdown.

### Tests (341 slater + 50 graph-format lib tests green)

- **Frame characterization** (lock the multi-hop result set byte-for-byte across the rewrite):
  `exec::tests::frame_{two_hop_chain_exact_rows,three_hop_chain_exact_rows,
  sibling_branch_binding_isolation,same_end_node_via_two_paths,undirected_traversal,
  where_references_mid_pattern_var,multipattern_comma_join_shared_var,varlen_zero_length_includes_self,
  varlen_relationship_uniqueness,path_var_walk_order}`. `sibling_branch_binding_isolation` is the
  specific frame risk — a node with two out-edges where a broken restore would leak a sibling's mid
  binding.
- **LIMIT pushdown** (early-stop = order-preserving prefix; aggregation/ORDER BY not capped):
  `exec::tests::limit_pushdown_{traversal_returns_order_preserving_prefix,with_skip,
  streaming_scan_prefix}`, `exec::tests::limit_does_not_break_aggregation_or_order`.

## Validation harness

### Test data locations (on this machine)

- **slater dump, ready to ingest:** `/home/rickk/git/personal/pole/data/pole-50.slater.cypher`
  (23 MB, Primitive-Cypher, 61,521 nodes / 105,840 rels). Feeds straight into `slater-build`.
- **Source Neo4j dumps:** `/home/rickk/git/personal/pole/data/pole-50.dump` (`-35/-40/-43` too).
- **Exporter (regenerate the slater dump):** `/home/rickk/git/personal/pole/code/neo4j_to_slater.py`.
- **Benchmark:** `perf/bench.py` (this repo).
- **Python driver:** `python3 -m venv /tmp/pole_venv && /tmp/pole_venv/bin/pip install neo4j`
  (host Python is PEP-668 externally-managed; use a venv).

### Build (Docker only — no host Rust toolchain)

```bash
cd /home/rickk/git/hs/slater
docker build -t slater:local .          # compiles current source into the image
# Optional unit tests via a throwaway builder (slow; installs build deps):
docker run --rm -v "$PWD":/src -w /src rust:1-bookworm bash -lc \
  'apt-get update && apt-get install -y cmake clang libclang-dev >/dev/null && cargo test'
```

### Ingest the pole image

```bash
docker run --rm -v slater_pole:/data \
  -v /home/rickk/git/personal/pole/data:/dumps:ro \
  --entrypoint /app/slater-build slater:local \
  --input /dumps/pole-50.slater.cypher --graph pole --data-dir /data
# Acceptance: stdout reports "61521 nodes, 105840 edges".
```

### Serve (requires Stage 0 so the server boots)

Mint a password hash and ACL, then run the image:

```bash
docker run --rm --entrypoint /app/slater slater:local hash-password polereader   # -> $argon2id$...
mkdir -p /tmp/slater-acl
cat > /tmp/slater-acl/acl.json <<'JSON'
{ "users": { "reporting": {
    "passwordArgon2id": "<paste hash>",
    "grants": { "pole": ["read"] } } } }
JSON
docker run -d --name slater-pole -p 7687:7687 \
  -v slater_pole:/data:ro \
  -v /tmp/slater-acl/acl.json:/config/acl.json:ro \
  slater:local
docker exec slater-pole /app/slater healthcheck localhost 7687   # exit 0 = up
```

Known-good hash for password `polereader` (argon2id, standard format):
`$argon2id$v=19$m=19456,t=2,p=1$R3VyS8OJIiG2Q7ihG1WlJQ$WAnkFldoPaMdxe1lAxUt/qio1Ny/jTV5aeo3p2h7ZuU`

> Pre-Stage-0 alternative: the prebuilt `target/release/slater` (old) boots with a
> `config.json` containing `"requireAclStamp": false` and serves the same image. Useful only to
> re-measure the baseline; Stages 1–6 must be validated against a freshly built server.

### Run the benchmark

```bash
/tmp/pole_venv/bin/python perf/bench.py --slater-pass polereader \
  --neo4j-uri bolt://localhost:7688 --neo4j-pass polepole12 \
  --slater-pid "$(docker inspect -f '{{.State.Pid}}' slater-pole)"
```

Compare the `slater uncached` medians against the Baseline table; the `neo4j` column gives parity
(and flags any row-count mismatch → correctness regression). For an idle Neo4j reference:
`docker run -d --name pole-neo4j -p 7688:7687 -e NEO4J_AUTH=neo4j/polepole12
-e NEO4J_server_memory_pagecache_size=64M -v pole_neo4j_data:/data neo4j:5`
(after loading `pole-50.dump`; see Regenerate appendix).

### Per-stage acceptance (apply to every stage)

1. **Correctness:** `slater-build` still reports 61,521 / 105,840; `bench.py` parity column shows
   equal row counts to Neo4j; (optional) `cargo test` green.
2. **Performance:** the stage's target query hits its goal (Status table), no >10% regression on
   other rows.
3. **Bookkeeping:** update this file's Status + Result, keep the Baseline frozen, commit doc+code.

## Stages (detail)

### Stage 0 — Config bool deserialization (server-boot prerequisite)  **[DONE 2026-06-12]**
- **Result:** added `de::bool` (visitor accepting `true`/`false` and `"true"`/`"false"`) and a
  `de::usize_floor0` helper; applied `de::bool` to `require_acl_stamp`. A current-source image
  now boots and serves the pole graph (healthcheck exits 0). Unit test
  `config::tests::require_acl_stamp_parses_bool_and_string`.
- **Problem:** `config.rs:97` `require_acl_stamp: bool` can't accept the stringified value the
  layered loader produces → server won't boot (see Stage 0 prerequisite above).
- **Change:** add a `deser_bool_or_str` (mirror `de::u64`/`deser_u16_or_str` visitor pattern,
  accepting `true`/`false` and `"true"`/`"false"`), apply via `#[serde(deserialize_with=…)]` to
  `require_acl_stamp` and audit other bare `bool` config leaves.
- **Files:** `crates/slater/src/config.rs` (helper likely there; if `hs-utils` owns the `de::*`
  family, add it alongside and re-export).
- **Validate:** `docker build` then run the server (Serve steps) → `healthcheck` exits 0; add a
  unit test parsing `{"requireAclStamp":"false"}` and `{"requireAclStamp":false}`.

### Stage 1 — Parameters → index selection  **[DONE 2026-06-12]**
- **Result:** `choose_node_scan` now takes a `&HashMap<String, Value>` of the query's params;
  `resolve()` (replacing `literal()`) resolves `Expr::Param` against it, so `{type:$t}` /
  `WHERE n.x=$v` pick `RangeEq`/`RangeRange`. The executor projects its `Val` params to planner
  `Value`s once (`val_to_value` → `Engine.plan_params` in `with_params`). idx-eq count dropped
  **901 ms → 0.8 ms**; 1-hop **952 ms → ~30–100 ms** (the index now anchors the scan; the
  remaining cost is the eager traversal, Stage 6). Planner tests
  `param_equality_on_indexed_property_picks_range_eq`, `unbound_param_falls_back_to_label_scan`.
- **Problem:** params bypass index selection (root cause 1) → param equality/range degrades to a
  label scan (901 ms).
- **Change:** make the planner parameter-aware. The RUN message carries params before planning;
  thread a `&Params` (or pre-substituted AST) into `choose_node_scan` → `inline_preds` /
  `collect_where_preds` / `compare_operands`, and have `literal()` resolve `Expr::Param(name)`
  against it to a constant `Value` so `RangeEq`/`RangeRange` is chosen.
- **Files:** `crates/slater/src/plan.rs`; caller `exec.rs:1743` `match_single_pattern`
  (`choose_node_scan(self.gen, start, where_)` needs the param map in scope).
- **Validate:** bench `idx-eq count (Crime.type)` and `1-hop` drop from ~900 ms to tens of ms;
  results unchanged. Add a planner unit test: param equality on an indexed prop picks `RangeEq`.

### Stage 2 — Skip redundant per-node label re-check  **[DONE 2026-06-12]**
- **Result:** `match_single_pattern` computes `scan_guaranteed_labels(scan)` (a `LabelScan`
  guarantees its label; a `RangeEq`/`RangeRange` guarantees the index's node label) and passes
  it to `node_ok`, which skips the label-record decode for guaranteed labels. The hot traversal
  path (nothing guaranteed) short-circuits before touching the label symbol table, so it stays
  at the pre-Stage-2 cost. Crime-label-count **841 ms → 0.64 ms**; the label-skip also helped
  every Crime-anchored scan — CONTAINS **879 ms → ~60 ms**, agg **885 ms → ~62 ms**, count
  DISTINCT **890 ms → ~61 ms**. Downstream-traversal `node_ok` calls pass `&[]` (no guarantee).
- **Problem:** root cause 2 — `node_ok` re-decodes labels for candidates that already came from
  the label posting (841 ms vs 14 ms).
- **Change:** when the anchor scan guarantees the label(s) (a `LabelScan{L}`, or a `RangeEq`/
  `RangeRange` on an index defined for L), skip the label portion of `node_ok` for those labels
  (still check any *additional* labels / inline props). Alternative: test membership against the
  resident `nodes_with_label` posting instead of decoding a record.
- **Files:** `crates/slater/src/exec.rs` (`match_single_pattern` ~1747, `node_ok` ~2037).
- **Validate:** `Crime label count` ≈ `count all nodes` (~14 ms); results unchanged.

### Stage 3 — Count fast-paths (metadata / index cardinality)  **[DONE 2026-06-12]**
- **Result:** `try_count_fast_path` (called at the top of `run_single`, only on the singleton
  seed — `CALL{}` subqueries bypass it) answers a single-node `count(*)`/`count(n)` from resident
  metadata: 0 labels → `node_count()`; 1 label → `nodes_with_label(L).len()`; a single
  indexed-equality inline prop covering exactly the pattern's label+prop → `lookup_eq().len()`.
  Extra **constant** RETURN items (the benchmark's `, $k AS k`) are allowed (one group). Any
  WHERE / extra pattern / non-constant projection / multi-label / `DISTINCT` falls back. count-all
  **14 ms → 0.64 ms**, Crime-label-count **0.64 ms**, idx-eq count **0.8 ms** — all now *beat*
  Neo4j. Tests: `label_count_uses_fast_path`, `count_with_constant_extra_projection_fast_path`,
  `count_with_non_constant_extra_projection_falls_back`, `count_with_where_still_correct`,
  `param_indexed_equality_count_fast_path`.
- **Problem:** root cause 4 — counts materialize all rows; resident metadata ignored.
- **Change:** recognize an aggregate-only `RETURN count(*)`/`count(n)` with no residual WHERE and
  a single-node pattern: unfiltered → `gen.node_count()`; one label → `nodes_with_label(L).len()`;
  indexed equality (post Stage 1) → `range_index(idx).lookup_eq(v).len()`. Plumb as a planner
  recognition + a short-circuit in `project_aggregated`/`compute_aggregate`.
- **Files:** `crates/slater/src/plan.rs`, `crates/slater/src/exec.rs` (`project_aggregated`
  ~2167, `compute_aggregate` ~2223).
- **Validate:** count queries < 1 ms; results unchanged. Guard: any WHERE/extra pattern disables
  the fast path (add tests for `count(*)` with and without a filter).

### Stage 4 — Block-cache record read (broad constant factor)  **[DONE 2026-06-12]**
- **Result:** `BlockCache::record` / `VectorIndexCache::record` now return a new **`BlockRecord`**
  (`cache.rs`) — an `Arc`-clone of the cached decompressed block plus the record's `start..end`
  byte range — instead of `rec.to_vec()`. `BlockRecord` derefs to `&[u8]`, so every executor call
  site (`node_props`/`edge_props`/`node_label_ids`/`outgoing`/`incoming`/`vector_group` and the
  Vamana beam-search reader) is unchanged via deref coercion — no lifetime churn into the executor.
  A new allocation-free `blockfile::record_range_in_block(raw, slot)` computes the range by reading
  only `count` + the two bracketing slot offsets, so the hot path also drops the per-access
  `parse_block` offset-table `Vec<u32>` allocation. Net: **two heap allocs (copy + offsets) removed
  per record access.** The lock path is unchanged (load already runs outside the lock; sharding/
  `RwLock` deferred — not needed to hit the targets). Outstanding `BlockRecord`s stay valid after
  their block is evicted (the `Arc` keeps it alive) — covered by a test. All 324 slater + 49
  graph-format lib tests green. Tests: `cache::tests::block_record_outlives_eviction_of_its_block`,
  `blockfile::tests::record_range_in_block_matches_record_from_block` (+ updated record tests).
  Bench: broad floor drop, 2-hop → Neo4j parity, no row-count regression — see "Validation results
  (Stage 4)" above.
- **Problem:** root cause 3 — per-record `Mutex` + `to_vec()` copy (and, found in passing, a
  per-record `parse_block` offset-table allocation).
- **Change:** return a shared/borrowed record (`BlockRecord` holding an `Arc<Vec<u8>>` + range)
  instead of copying. (Sharded/`RwLock` cache and a contiguous-range batch API were considered and
  left for later — the borrowed-record + offset-alloc removal already met the floor-drop target.)
- **Files:** `crates/slater/src/cache.rs` (`BlockRecord`, `record` ~283/~330); call sites in
  `exec.rs` unchanged; `crates/graph-format/src/blockfile.rs` (`record_range_in_block`).
- **Risk:** lifetime/aliasing churn across the executor — land behind full `cargo test` + bench.
  *Realised low:* deref coercion kept every call site byte-for-byte; no executor signature changed.
- **Validate:** uniform drop across all scan/traversal rows; no correctness change. ✓

### Stage 5 — Streaming aggregation + projection pushdown + prop memo  **[DONE 2026-06-12]**
- **Result:** two changes, both validated against the harness (see "Validation results (Stage 5)"):
  1. **`try_stream_match`** (`exec.rs`, called at the top of `apply_match`): a single
     non-OPTIONAL node-only `MATCH` (one pattern, no relationships, no path var, fresh-scan anchor)
     streams scan candidates straight into output rows — appending `Val::Node(id)` to a clone of
     the input row — instead of building the general matcher's `Vec<HashMap<String, Val>>` (one
     cloned binding map per row, root cause 4). The anchor scan is chosen once (parameter/`WHERE`-
     aware), `node_ok` enforces the pattern's labels + inline props (with the anchor's own var
     intentionally absent, as in the general path), and the clause `WHERE` is re-evaluated per
     emitted row against the full row scope — identical semantics (row order, the per-row
     `charge(1)` intermediate-budget tick). Any rel / path var / already-bound anchor / OPTIONAL /
     multi-pattern falls back to the general path.
  2. **`columns::decode_one` + `wire::skip_value`** (graph-format): `node_prop`/`edge_prop` decode
     only the requested key from the cached record and *skip* the other values (step over a
     string/list/vector without allocating) rather than decoding the whole map into a
     `Vec<(u32, Value)>` and linear-scanning (root cause 5). One matching value decode + k−1 cheap
     skips, no per-value heap alloc for the unwanted keys.
  Net on the three target rows: agg **19.5→10.3 ms**, count DISTINCT **20.5→11.6 ms**, CONTAINS
  **16→9.0 ms** (~1.8× each; agg nearly at Neo4j parity). No row-count regression; count/index and
  traversal rows unchanged within noise. The doc's "prop memo" idea (decode a node's whole record
  once per row, serve all reads from it) was **not** needed: every target reads exactly one
  property per row, so the skip-based single-key decode strictly beats a memoised full decode
  (partial < full) and adds no per-row scratch map. A future multi-property RETURN over one node
  is the only case a memo would help; revisit if such a row shows up.
- **Problem:** root causes 4 & 5 — `Vec<HashMap>` per row; per-access full prop decode.
- **Change:** stream candidates without building a HashMap per row; decode only the property keys a
  read needs.
- **Files:** `crates/slater/src/exec.rs` (`apply_match`/`try_stream_match`, `node_prop`/`edge_prop`
  ~675); `crates/graph-format/src/columns.rs` (`decode_one`), `crates/graph-format/src/wire.rs`
  (`skip_value`).
- **Validate:** `agg crimes by type`, `count DISTINCT type`, full-scan CONTAINS improve ~1.8×; rows
  unchanged. ✓

### Stage 6 — Traversal frame + LIMIT pushdown  **[DONE 2026-06-12]**
- **Result:** 3-hop **45→1.6 ms** (now beats Neo4j), 1-hop **4.8→2.6 ms**, 2-hop 2.0→1.6 ms; all
  other rows unchanged within noise, no `!rows` mismatch. See "Validation results (Stage 6)" for the
  full table and the analysis of why the doc's binding-clone hypothesis was wrong.
- **Problem:** root cause 6 named per-hop `binding.clone()` **and** full path buffering. Only the
  second mattered, and only via `LIMIT`: a non-pushed `LIMIT` made the executor materialize every
  path (3-hop = 28,762 paths for `LIMIT 100`) into a `Vec<HashMap>` before truncating in `project`.
- **Change (two parts):**
  1. **Binding frame** (the planned change; correct but measurement-neutral): `expand_chain` and
     `match_single_pattern` now mutate one `HashMap` in place with push/pop restore
     (`restore_binding`) instead of cloning the binding per neighbour per hop. One clone per
     *completed* row remains (pushed into `out`). The `walk` path scratch and var-length `used` set
     already used this discipline.
  2. **LIMIT pushdown** (the actual lever): `projection_row_cap` derives a `SKIP + LIMIT` row cap
     when the final projection is a 1:1 map (no aggregation/`DISTINCT`/`ORDER BY`); `run_single_seeded`
     applies it to the last reading clause; the cap threads through `apply_match`/`match_patterns`/
     `match_single_pattern`/`expand_chain`/`try_stream_match`, which stop emitting once `out` is full.
     Exact (same prefix in emit order); the per-pattern walk is capped only on the last pattern with
     no residual `WHERE`.
- **Files:** `crates/slater/src/exec.rs` — `projection_row_cap`, `run_single_seeded`, `apply_match`,
  `try_stream_match`, `match_patterns`, `match_single_pattern`, `expand_chain`, `restore_binding`.
- **Validate:** 3-/1-/2-hop drop to low ms; results unchanged (frame characterization +
  `limit_pushdown_*` tests; 341 slater + 50 graph-format lib tests green). ✓

## Appendix — regenerate test data if lost

```bash
# 1. Load the Neo4j dump into a volume, then start Neo4j 5.
cp /home/rickk/git/personal/pole/data/pole-50.dump /tmp/pole_dump/neo4j.dump
docker run --rm -v pole_neo4j_data:/data -v /tmp/pole_dump:/dumps:ro neo4j:5 \
  neo4j-admin database load neo4j --from-path=/dumps --overwrite-destination=true
docker run -d --name pole-neo4j -p 7688:7687 -v pole_neo4j_data:/data \
  -e NEO4J_AUTH=neo4j/polepole12 -e NEO4J_server_memory_pagecache_size=64M neo4j:5
# 2. Re-export to slater Primitive-Cypher.
/tmp/pole_venv/bin/python /home/rickk/git/personal/pole/code/neo4j_to_slater.py \
  --uri bolt://localhost:7688 --user neo4j --password polepole12 \
  --output /home/rickk/git/personal/pole/data/pole-50.slater.cypher
```
