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

## Latency (median ms) + peak RSS (MiB), fanout=1 / fanout=8

| dataset | shape | fan=1 | fan=8 |
|---|---|--:|--:|
| **pole** 62k/106k | count / label / point / idx-eq | 0.56/0.51 · 0.56/0.52 · 0.64/0.61 · 0.57/0.52 | |
| | 1-/2-/3-hop · agg · DISTINCT · scan | 1.29/1.26 · 2.78/2.95 · 3.11/2.68 · 3.12/3.21 · 3.15/2.94 · 0.63/0.66 | |
| | **peak anon / total** | 11 / 22 | 18 / 29 |
| **MeSH** 341k/469k | count / 2-hop / group-by / DISTINCT / 3-hop | 0.55 · 34.1 · 23.0 · 22.5 · 9.7 | 0.54 · 34.4 · 23.5 · 23.3 · 9.8 |
| | **peak anon / total** | 63 / 69 | 63 / 94 |
| **EU-AI-Act** 21k/45k+vec | kNN-10/50/chunk · kNN+1hop · point | 21.3 · 23.4 · 10.3 · 25.6 · 1.02 | 17.5 · 18.4 · 10.1 · 19.6 · 1.20 |
| | **peak anon / total** | 99 / 121 | 115 / 160 |
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
  **kNN-10**: 21.3 → 17.5 ms. **var-length**: 7.6 → 5.5 ms.
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
