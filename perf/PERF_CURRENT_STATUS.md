# Performance — current status (slater-only re-measurement)

Fresh slater-only pass on **current `main`** (commit with the multi-hop expansion work:
`45d7584` Arc-frame bindings + `3a1a5bb` count-pushdown), **both `maxFanout=1` and `8`**,
each dataset in an **isolated** container (spun up, benched, torn down — one at a time),
`--memory=12g` cap, `requireAclStamp=false` on the static read-only generations.

- Small graphs (pole/MeSH/EU-AI-Act): default budgets, `maxIntermediate=1M`.
- Wikidata (1M & 91.6M): `maxIntermediate=20M`, `maxIntermediateGlobal=100M` (to let the
  heavy uncapped shapes run; see **Budget trips** below).

RSS is the container cgroup: **anon** = heap/working set (the honest engine footprint);
**total** includes reclaimable OS page cache of the paged store.

> **Whole-graph metadata fast paths (2026-07):** `bench.py` now carries four canonical
> introspection shapes — `MATCH ()-[r]->() RETURN DISTINCT type(r)` / `… type(r),
> count(*)` and `MATCH (n) RETURN DISTINCT labels(n)[0]` / `… labels(n)[0], count(*)`.
> These are answered from resident manifest counts (`reltype_edge_counts` /
> `first_label_counts` / schema marginals) with **zero block reads** and cost
> ~O(reltypes|labels), so they must stay flat as the graph grows regardless of edge
> count — the regression guard for the unanchored-scan incident. Requires a generation
> built with the metadata summaries; older generations answer `type(r)` via the
> open-time scan fallback and decline the labelled / `labels(n)[0]` variants.

## Latency (median ms) + peak RSS (MiB), fanout=1 / fanout=8

| dataset | shape | fan=1 | fan=8 |
|---|---|--:|--:|
| **pole** 62k/106k | count / label / point / idx-eq | 0.56/0.51 · 0.56/0.52 · 0.64/0.61 · 0.57/0.52 | |
| | 1-/2-/3-hop · agg · DISTINCT · scan | 1.29/1.26 · 2.78/2.95 · 3.11/2.68 · 3.12/3.21 · 3.15/2.94 · 0.63/0.66 | |
| | **peak anon / total** | 11 / 22 | 18 / 29 |
| **MeSH** 341k/469k | count / 2-hop / group-by / DISTINCT / 3-hop | 0.55 · 34.1 · 23.0 · 22.5 · 9.7 | 0.54 · 34.4 · 23.5 · 23.3 · 9.8 |
| | **peak anon / total** | 63 / 69 | 63 / 94 |
| **EU-AI-Act** 21k/45k+vec | kNN-10/50/chunk · kNN+1hop · point | 3.13 · 3.35 · 2.20 · 2.71 · 1.02 | ≈fan=1 † |
| | **peak RSS (VmHWM)** | ~156 (incl. resident matrix) | |

> **v0.9.x vector update (2026-06-16):** the EU-AI-Act kNN row reflects the SIMD distance
> kernel + resident pre-normalised matrix (`vectorCacheBytes` raised 32→64 MiB so the
> 54.8 MiB estate fits). kNN-10 Concept 21.3 → **3.13 ms** (~7×), chunk 10.3 → **2.20**,
> kNN+1hop 25.6 → **2.71**. Measured slater-only, 5 restart cycles, mean of medians, on
> the same box. Peak process RSS rose 99 → **~156 MiB** (the resident matrix is held for
> the generation's life; still the smallest of the cross-engine field). † The matrix scan
> is ~fanout-insensitive at this group size (the per-query gather that fanout used to hide
> is gone), so fanout=8 ≈ fanout=1 here; the precise anon/total cgroup split wants a full
> `run_bench_hs.sh` re-run.
| **Wikidata-1M** | count/point/degree/1h/2h/3h · varlen · sp≤6 | 0.54·1.73·1.75·2.09·2.49·2.61 · 2.08 · 2.04 | 0.57·1.89·2.04·1.92·2.83·2.71 · 1.96 · 2.05 |
| | **peak anon / total** | 335 / 568 | 462 / 633 |
| **Wikidata-91.6M** | count/point/degree/1h | 0.56·1.53·1.75·3.43 | 0.56·1.60·1.93·3.99 |
| | 2-hop / 3-hop / varlen / **sp≤6** | 29.1 · 27.7 · 7.6 · **307** | 29.4 · 23.3 · 5.5 · **76.5** |
| | **peak anon / total** | 2,353 / 5,242 | 2,463 / 4,536 |
| **Wikidata-91.6M multi-hop count** | **2-hop count / 3-hop count** | **31.2 / 546.8** | **21.6 / 298.3** |
| | **peak anon / total** | **661 / 5,293** | **1,912 / 5,218** |

## What changed (vs v0.8.0)

- **Multi-hop `RETURN count(*)` is the headline.** Same shapes, isolated A/B vs the v0.8.0
  image (maxIntermediate=20M) earlier this pass: 91.6M **3-hop count** peak anon went from
  **7.7 GB (fan=1) / 9.5 GB (fan=8) → 0.66 GB / 1.9 GB** (≈**24× / 5×**), and latency from
  955/617 ms → 554/298 ms. The count-pushdown no longer materialises the result rows; the
  fanout=8 residual (~1.9 GB) is the **parallel adjacency-read buffers**, not the count.
- **Everything else is neutral** (as designed — the change only touches uncapped multi-hop
  expansion). pole/MeSH/EU/wiki traversal latencies match the published tables within run
  variance.

## fanout=1 vs fanout=8

Helps the **cold, disk-bound, large-working-set** shapes; flat on small/warm ones:
- 91.6M **shortestPath ≤6**: 307 → **76 ms** (4×). **3-hop count**: 547 → **298 ms** (1.8×).
  **var-length**: 7.6 → 5.5 ms. (kNN-10 was 21.3 → 17.5 ms when fanout parallelised the
  per-query vector *gather*; the v0.9.x resident matrix removes that gather, so kNN is now
  ~3 ms and fanout-insensitive at this scale.)
- Costs more anon (parallel worker buffers): 91.6M 3-hop count anon 661 → 1,912 MiB.
- pole/MeSH/EU/wiki-1M: within noise (working set already small/cached).

`maxFanout=1` remains the throughput-default; `8` is the latency dial for big cold traversals.

## Methodology note — MeSH RSS

Isolated single-client MeSH peak is **69 MiB total / 63 anon**, *not* the ~197 the
cross-engine table reports. The 197 is a **cumulative/concurrent** high-water across the
full restart-cycle harness, not an isolated single-run footprint (consistent with the
separate idle-`malloc_trim` investigation: idle MeSH ~16 MiB, single-client steady ~65 MiB,
the high-water only appears under concurrency). Treat 69 MiB as the isolated figure.

## Budget trips (`maxIntermediate=20M`, Wikidata-91.6M)

Only the genuinely **unbounded-fanout** shapes trip — the budget doing its job:

| shape | trip rate (of 20 hub anchors) | both fanouts |
|---|--:|---|
| **3-hop count** (`bench_multihop`) | 10/20 | yes |
| **var-length `*1..2` distinct** (`bench_wiki`) | 6/20 | yes |

Everything else (count/point/degree/1-/2-/3-hop traversal, 2-hop count, shortestPath)
**completes** at 20M.

Key nuance for tuning: with count-pushdown a tripping **count** is now bounded by
*adjacency reads* (compute), **not** row materialisation — so raising the budget for
count-shaped queries is **memory-safe** (RSS stays flat). The row-materialising shapes
(`var-length … distinct`) are the ones whose RSS still scales with the budget.

## maxIntermediate knee sweep (Wikidata-91.6M, fanout=8, 12 GB cap, isolated per budget)

Swept `maxIntermediate` ∈ {1M, 5M, 20M, 50M, 100M, 200M}, one container per budget,
`maxIntermediateGlobal=1B` (so the per-query cap is the only gate). 20 hub anchors per shape.

**Row-materialising — `var-length *1..2 distinct`** (RSS-bound):

| budget | peak anon | trips /20 |
|--:|--:|--:|
| 1M | 584 MiB | 8 |
| 5M | 741 MiB | 6 |
| 20M | 2,385 MiB | 6 |
| 50M | 6,289 MiB | **0** |
| 100M | 6,158 MiB | 0 |
| 200M | 6,303 MiB | 0 |

**Count — `3-hop count(m)`** (compute-bound, count-pushdown):

| budget | peak anon | trips /20 | median ms |
|--:|--:|--:|--:|
| 1M | 2,445 MiB | 15 | 306 |
| 5M | 1,918 MiB | 13 | 288 |
| 20M | 2,307 MiB | 10 | 329 |
| 50M | 2,138 MiB | 8 | 593 |
| 100M | 2,437 MiB | 3 | 1,229 |
| 200M | 2,621 MiB | 2 | 983 |

No OOM at any budget (var-length's real result set for this hub pool caps ~6.3 GB and
plateaus at ≥50M; budget above that is unused headroom).

### The knee: no single good scalar — the two regimes want opposite settings

- **Counts are memory-flat** (~2–2.6 GB **regardless of budget**); their governor is the
  30 s *timeout*, not memory. Raising the budget is pure upside but they still need ~100M+
  to mostly complete and the cost shows up as latency (306 → 1,229 ms), not RSS.
- **Materialisers scale RSS with the budget** until the true result is exhausted (~6.3 GB
  here); their governor *is* memory.

A scalar `maxIntermediate` is forced to compromise: 1M (the 48 MB default sized for the
100–200 MB deployment target) trips counts 15/20; clearing counts means ~100M, which lets a
materialiser balloon to ~6 GB. **Recommendation:** keep the **1M default** (correctly sized
for the 100–200 MB target — the sweep confirms it bounds the materialisers); document that on
a large box you raise it, and that counts are memory-safe to raise. The deeper fix is to
**split the budget by retention semantics** — a tight *retained* high-water (the real RSS/OOM
guard, and what the global aggregate should track) plus a generous *transient/scan* ceiling
(or fold it into the timeout). Count-pushdown retains ~0, so a retained-only cap lets counts
run to the timeout while still bounding materialiser RSS. Keep the cumulative transient charge
too — it trips geometric growth (`reduce(acc+acc)`) early, which a peak gauge would miss.

Raw results: `/tmp/bench-camp/knee/knee-results.txt` (runner `run-knee.sh`).

### Implemented: the retention split (`maxScan`)

The split is shipped (branch `perf/retention-split-budget`): `query.maxScan` (default **500M**)
bounds the *transient* count-pushdown walk work, while `query.maxIntermediate` (default 1M)
keeps bounding *retained* materialisation. Count-pushdown charges route to `maxScan` and do
not draw the server-wide aggregate; var-length and row-building shapes stay on the retained
budget. End-to-end re-run on the 91.6M graph, fanout=8, **stock split defaults** (no overrides;
the validation ran at `maxScan=200M`, which upper-bounds the 500M trip rate since the value is
decoupled from RSS):

| 3-hop count default | trips /20 | peak anon | trip budget |
|---|--:|--:|---|
| old single scalar `maxIntermediate=1M` | 15/20 | ~2.4 GB | maxIntermediate |
| **new split (`1M` retained / `≥200M` scan)** | **≤2/20** | **2.15 GB** | **maxScan** |

13→18 of 20 heavy hub counts now complete, RSS unchanged (~2.1 GB) — the "counts are
memory-safe to raise" thesis as the default; the scan value is decoupled from RSS (flat
~2–2.6 GB across the whole 1M→200M sweep), so 500M costs no memory and only lets a couple more
mega-hubs through. A tight `maxScan=20000` re-trips them (19/20), confirming the budget still
governs count work; the error reads `… scan budget of N elements (query.maxScan)`.

Sweep `maxIntermediate` on the 91.6M graph (e.g. 1M/5M/20M/50M/100M/200M) for the heavy
shapes; record per-budget completion rate + peak anon. Goal: a default that lets typical
91.6M queries complete while still bounding unbounded growth. Expect two regimes — count
shapes (flat RSS, knee is compute/time-bound, can sit high) vs row-materialising shapes
(RSS scales with budget, knee set by acceptable RSS).

---

## Build diagnostics — full 91.6M wikidata on the `writeable` branch (2026-07-09)

`slater-build --diagnostics --diagnostics-interval-ms 250` over the 133 GB business-key MERGE
dump (`wikidata-full-merge.cypher`), 16 cores / `--threads 14`, `--max-memory 4 GiB`.
**53m 46s wall**, 756% average CPU, 8.47 GB peak RSS, 91,600,504 nodes / 1,489,725,024 edges.

**Determinism check.** 8 of the 9 emitted files are byte-identical to the v0.21.0 core
(`c97cdb75…`); only `range/node_Entity_wikidata_id.isam` differs (659 MB → 632 MB), which is
exactly the intended effect of **D53** (smaller leaf blocks for range ISAMs). Hence the new
content-hash `5e8e7307…`. Nothing else drifted.

| phase | wall | % | CPU-s | cpu/wall | read | write | peak RSS |
|---|--:|--:|--:|--:|--:|--:|--:|
| pass1 (parse + metadata) | 11.1m | 21% | 9461 | **14.2×** | 133 G | 12 G | 1.95 G |
| dedup keys | 1.9m | 4% | 183 | 1.6× | 3 G | 7 G | 0.62 G |
| resolve edge endpoints | 11.3m | 21% | 7002 | **10.3×** | 95 G | 158 G | 2.42 G |
| cluster (locality reorder) | 8.7m | 16% | 1534 | 2.9× | 51 G | 36 G | 1.60 G |
| emit node stores | 2.8m | 5% | 232 | 1.4× | 3 G | 8 G | 1.27 G |
| emit topology (CSR + edges) | 12.2m | 23% | 5585 | 7.7× | 75 G | 110 G | **8.06 G** |
| emit.graph_summaries | 4.3m | 8% | 328 | 1.3× | 23 G | 6 G | 3.83 G |
| emit range indexes | 0.3m | 1% | 18 | 1.0× | 1 G | 1 G | 2.50 G |
| publish (hash + manifest) | 1.0m | 2% | 59 | 1.0× | 23 G | 0 G | 2.50 G |

### Findings

1. **Peak RSS is 2× the `--max-memory` budget.** 20.1% of all samples exceed 4 GiB, and every
   one of them is in `emit.topology` (`emit forward CSR + edge_props per band`), peaking at
   **8.06 GB** at t=41 min. `--max-memory` evidently bounds the external-sort budget, not the
   phase's working set. On a memory-capped box this is the phase that OOMs.

2. **~35% of wall clock (18.9 min) runs effectively single-threaded on 16 cores.** Median CPU
   and the fraction of time below 1.5 cores:

   | phase | median CPU | time under 1.5 cores |
   |---|--:|--:|
   | `cluster` | 100% | 67% |
   | `emit.graph_summaries` | 100% | 75% |
   | `emit.node_stores` | 100% | 69% |
   | `dedup` | 103% | 60% |
   | `publish` | 99% | **100%** |

   `cluster` reports 14 active workers and bursts to 1336% at p90, but spends two-thirds of its
   8.7 min on one core — the block-parallel LDG pass has a long serial tail. `publish` is a
   fully serial BLAKE3 over the 23 GB image (411 MB/s). `emit.graph_summaries` is a serial
   91.6M-node tally — the same marginals the query-side metadata fast paths read.

3. **`emit.topology` is the only phase under real pressure** — PSI cpu 8.8 / io 11.5 (every
   other phase is ≲4 io, ~0 cpu) at 917% CPU. It is simultaneously the wall-clock leader (23%),
   the RSS peak, and the IO peak (110 GB written).

4. **`pass1` and `resolve` scale well** (14.2× and 10.3× of wall in CPU-seconds), so the
   parallel-extsort / parallel-pipeline work is doing its job; the remaining headroom is
   entirely in the five near-serial phases above.

5. **IO amplification: 3.1× read / 2.5× write** against the 133 GB input (406 GB read, 337 GB
   written) — the cost of the spill-based external sort. `resolve` alone writes 158 GB.

**Highest-leverage next steps**, in order: (a) bound `emit.topology`'s working set to
`--max-memory` (it is the OOM surface and the RSS peak); (b) parallelise `emit.graph_summaries`
(4.3 min, embarrassingly parallel tally); (c) parallelise the `publish` hash (1.0 min, one core
over 23 GB); (d) chase `cluster`'s serial tail (8.7 min at 2.9×).

## Build diagnostics — full 91.6M wikidata after B1–B4 (2026-07-10)

Same box, same command as the 2026-07-09 run above (16 cores, `--threads 14`, `--max-memory 4 GiB`
default, `--diagnostics-interval-ms 250`), on the `writeable` branch with **B1** (memory accountant,
D58), **B2** (parallel `emit.graph_summaries`), **B3** (publish hashing, D56) and **B4** (per-sub-step
instrumentation + parallel block sealing, D57) landed.

**Content hash `5e8e7307…` — unchanged.** Every item is byte-preserving at full scale, including B3
part 1, which `docs/BUILD-PERF-PLAN.md` had wrongly predicted would force a re-baseline.

Also with **D59**: `slater-build` on jemalloc, and the property-histogram scan bounded.

**47.0 min wall** (was 53.8 min, **−12.5%**), 984% average CPU, **5.66 GB peak RSS** (was 8.47 GB, **−33%**).

| phase | wall | cpu/wall | peak RSS | vs 2026-07-09 |
|---|--:|--:|--:|---|
| pass1 (parse + metadata) | 11.11m | 14.0× | 2.15 G | unchanged |
| dedup keys | **1.13m** | 2.0× | 1.01 G | **−41% wall** |
| resolve edge endpoints | 11.31m | **12.2×** | 4.31 G | level, 10.3→12.2× |
| cluster (locality reorder) | 8.52m | 3.0× | 2.08 G | −2% wall, RSS 1.60→2.08 G |
| emit node stores | **0.98m** | **5.4×** | 1.24 G | **−65% wall**, 1.4→5.4× |
| emit topology (CSR + edges) | 12.03m | **8.6×** | 4.60 G | level, RSS **8.06→4.60 G** |
| emit.graph_summaries | **1.34m** | **9.6×** | 3.71 G | **−69% wall**, 1.3→9.6× |
| emit range indexes | 0.33m | 0.9× | 0.49 G | unchanged |
| emit property histograms | 0.09m | 1.0× | 5.78 G → *see below* | new peak, then fixed |
| publish (hash + manifest) | **0.21m** | 2.2× | 0.44 G | **−79% wall**, 1.0→2.2× |

### Memory: the accountant holds, and now the allocator returns

| metric | 2026-07-09 | glibc (B1–B4) | **jemalloc (D59)** |
|---|--:|--:|--:|
| peak **reserved** | *(not tracked)* | 4.29 G = 1.00× cap | **4.29 G = 1.00× cap** |
| peak RSS | 8.47 G = 2.08× cap | 8.29 G = 1.93× cap | **5.66 G = 1.34× cap †** |
| `emit.topology` peak RSS | 8.06 G | 8.29 G | **4.60 G = 1.07× cap** |
| `stitch` RSS vs reserved | — | 6.25 G vs 0.81 G | **2.53 G vs 0.81 G** |
| samples above 1.25× cap | 20.1% (above 1.0×) | 13.3% | **0.01% (1 of 11,059)** |
| samples above 2× cap | (peak 2.08×) | 0.0% | **0.0%** |

† The single 5.78 GB sample is `emit.prop_hist`, a five-second phase that reserves nothing:
`derive_histogram_from_isam` materialised **every** distinct `(Value, count)` pair before checking the
`--histogram-max-distinct` cap and discarding them, and `node_Entity_wikidata_id` is near-unique over
91.6M nodes. Fixed in D59 (`distinct_key_counts_bounded` abandons mid-scan). **Excluding it, the build's
peak is 4.60 GB = 1.07× the cap.**

`MemoryBudget` (D58) provably never overcommits — peak reserved is exactly the cap. What it could not see
was glibc arena retention: `stitch` held 6.25 GB resident against 0.81 GB reserved while doing nothing but
a verbatim block-concat of finished files. jemalloc's `background_threads` purge threads return that heap
(D59), and also service the ~1.5B small `props_blob` allocations faster — which is why `emit.node_stores`
went 2.8m@1.4× (2026-07-09) → 1.68m@3.4× (glibc) → **0.98m@5.4×** (jemalloc).

### Where the remaining serial time is (per-sub-step, from B4 stage 1; glibc run)

| phase | sub-op | wall | cpu%avg |
|---|---|--:|--:|
| cluster | build undirected adjacency (external sort) | 115.5s | 281% |
| cluster | **route adjacency into stripes** | **272.8s (54%)** | **120%** |
| cluster | ldg pass 0 / 1 / 2 | 27.5 / 40.5 / 43.8s | 812 / 743 / 714% |
| emit.topology | emit forward CSR + edge_props per band | 282.2s | 1436% |
| emit.topology | emit reverse CSR per band | 123.8s | 1453% |
| emit.topology | **stitch CSR + edge_props + postings** | **247.8s (35%)** | **85%** |

Two named, still-serial steps remain, and D57's parallel sealing did not touch either — both are bounded
by *reads*, not by compression:

1. **`cluster` / route adjacency into stripes** — one thread drains the adjacency sorter's k-way merge
   (decompressing run blocks) and scatters into 1,398 stripe files. The fix is to invert the phase:
   route unsorted `(node, nbr)` pairs into per-stripe files in parallel, then sort each stripe in
   parallel — `ldg_stripe_pass` only ever needs *its own* stripe ordered by node, so the stripe files
   come out byte-identical and the permutation cannot move.
2. **`emit.topology` / stitch** — a verbatim block-concat of 176 band files into `topology.csr.blk` +
   `edge_props.blk` (20.4 GB), at 85% CPU and IO-bound. Now 35% of the phase, because the band passes
   themselves got 1.6× faster.

`emit.graph_summaries` and `publish`, the two phases the 2026-07-09 findings called out as serial, are
now 9.6× and 2.2× and together cost 1.55 min (was 5.3 min).

## Build diagnostics — full 91.6M wikidata, final (2026-07-10, `wd91m-g-diag.jsonl`)

Same box and command as above. Adds **D60** (`cluster` sorts each stripe rather than the whole
adjacency), the per-edge blob inlining, and inline sealing for pool-owned writers — on top of
B1–B4 (D56–D58) and jemalloc (D59).

**Content hash `5e8e7307…` — unchanged across all four verification rebuilds.**

**43.48 min wall** (was 53.8, **−19%**), 1082% average CPU, **4.95 GB peak RSS = 1.15× the 4 GiB
cap** (was 8.47 GB = 2.08×). B1's memory acceptance — peak ≤ ~1.25× cap, zero samples above 2× —
**passes**.

| phase | 2026-07-09 | jemalloc (D59) | final | cpu/wall | peak RSS |
|---|--:|--:|--:|--:|--:|
| pass1 | 11.1m | 11.11m | 11.17m | 13.7× | 2.11 G |
| dedup keys | 1.9m | 1.13m | **1.12m** | 2.0× | 1.20 G |
| resolve edge endpoints | 11.3m | 11.31m | **11.20m** | 12.0× | 4.23 G |
| **cluster** | **8.7m** | 8.52m | **4.55m** | **8.3×** | 2.98 G |
| emit node stores | 2.8m | 0.98m | **0.97m** | 5.5× | 1.10 G |
| emit topology | 12.2m | 12.03m | 12.51m | 8.0× | 5.04 G |
| emit.graph_summaries | 4.3m | 1.34m | **1.44m** | 9.3× | 3.85 G |
| emit range indexes | 0.3m | 0.33m | 0.33m | 0.9× | 0.52 G |
| publish | 1.0m | 0.21m | **0.20m** | 2.2× | 0.52 G |
| **total** | **53.8m** | 47.0m | **43.5m** | | **4.95 G** |

### What is serial now, and why

| phase | sub-op | wall | cpu%avg |
|---|---|--:|--:|
| cluster | route adjacency into stripes | 51.2s | 796% |
| cluster | sort adjacency stripes | 100.5s | 989% |
| cluster | ldg pass 0 / 1 / 2 | 26.5 / 42.8 / 41.8s | ~740% |
| emit.topology | partition edges by src band | 49.2s | 1125% |
| emit.topology | emit forward CSR + edge_props per band | 271.8s | 1250% |
| emit.topology | emit reverse CSR per band | 135.2s | 1278% |
| emit.topology | **stitch CSR + edge_props + postings** | **272.5s** | **79%** |

Only one near-serial step is left, and it is **not CPU-starved — it is write-bandwidth-bound**.
`stitch` concatenates 176 band files into 20.4 GB. Three implementations measured within ±10% of
each other: serial `BufWriter` 247.8s @85%, parallel `pwrite` 255.8s @110%, sequential
`std::io::copy` (→ `copy_file_range(2)`; the bytes never enter user space) 272.5s @79%. The last
ships because it costs the least CPU, not because it is faster. **Beating it means writing fewer
bytes, not writing them differently.** See D60.

### The two measurements that overturned an intuition

1. **A *more accurate* memory estimate made the build worse.** With blobs inlined,
   `SortRecord::resident_hint` can report `resolve`'s records exactly. Doing so let each buffer pack
   ~1.7× more records — and `Vec` grows by doubling, so the `realloc` crossing the spill threshold
   holds both arrays. `resolve` 4.31 → **6.04 GB** against a 4 GiB cap; `emit.topology`'s reverse
   band 123.8 → 243.8s; no wall-clock gain. The default's double-count is a load-bearing margin.
2. **85% CPU on `stitch` did not mean it needed threads.** See the table above.
