# Load testing — concurrency, brown-out, and memory under load

The benchmarks in `perf/cross-engine-hs/` measure **single-client** latency and
footprint (the headline "bounded RSS at comparable speed" claim). This document
covers the complementary axis: **what happens under many concurrent clients** —
where the service brown-outs, which resource is the binding limiter, and how
resident memory behaves under sustained load.

The harness is [`perf/loadtest/`](../perf/loadtest/) — a [Locust](https://locust.io)
driver over the official neo4j Bolt driver, a named-scenario registry (one per
fragile area), and an automated **coordinator** that ramps load, snapshots
`CALL slater.diagnostics()` under load at each step, detects the capacity knee, and
attributes the limiter. See that README for how to run it; this document records a
representative run and what it found.

## Diagnostics

Load tests correlate client-side latency/throughput with **server-reported health**
read live over Bolt:

```cypher
CALL slater.diagnostics()
```

It returns a `metric`/`value` table — process RSS and CPU, the cgroup memory & CPU
limits, connection-cap headroom and rejections, per-reason query-failure tallies,
and latency percentiles. It is **off by default** (`loadTestDiagnostics: true` to
enable) and inert on the hot path when off. Never enable it on a production replica.

## Test environment (representative run)

- **Image:** `hikarisystems/slater:v0.7.0`, built locally.
- **Graph:** Wikidata **1M nodes / 13.8M edges** (`Entity`/`LINK`, range index on
  `wikidata_id`), ~206 MB on disk (zstd).
- **Config:** `blockCacheBytes` = **256 MiB**, `vectorCacheBytes` = 0,
  `resultCacheBytes` = 16 MiB, `loadTestDiagnostics: true`.
- **Container:** `--memory=4g` cgroup cap (so RSS/limit are reportable).
- **Host:** 16 cores, 15 GiB RAM, WSL2, cgroup v2 (shared with other containers).
- **Shapes:** range-index point lookups, anchored 1-hop expansions, index range
  scans, and 2-hop expansions over a store-spread `wikidata_id` pool (the stock
  pole-schema shapes auto-skip on this graph; wikidata-native shapes were added to
  `queries.py`/`scenarios.py`).

## Results

### `wiki_cache_churn` — point lookups + 1-hop + range scans (256 MiB cache)

| users | rps  | p50   | p99    | fail% | RSS     | status  |
|------:|-----:|------:|-------:|------:|--------:|:--------|
|    50 | 2989 |  9 ms |  40 ms |  0%   | 2059 MB | OK      |
|   100 | 2869 | 10 ms |  48 ms |  0%   | 2315 MB | OK      |
|   250 | 2665 | 10 ms |  63 ms |  0%   | 2467 MB | OK      |
|   500 | 2317 | 11 ms | 150 ms |  0%   | 2590 MB | OK      |
|  1000 | 1837 | 14 ms | 520 ms |  0%   | 2744 MB | ⚠ knee  |

> The RSS column above is the **pre-allocator-fix** build (finding 1). With
> `MALLOC_ARENA_MAX=2` now baked into the runtime image, the same churn mix holds RSS
> flat at **~0.5 GB** across 100→1000 users (0 failures, 0 global-budget rejects —
> the light read mix charges ~0, so the budget guard is inert here). Latency and the
> zero-failure / concurrency story are unchanged.

- **Zero failures**; the service held to 1000 concurrent clients on one box.
- Throughput peaks ~3k rps and **falls** as concurrency rises — core contention,
  not a hard cap. The p99 knee is at ~1000 users (queueing, p99 40 → 520 ms).
- **Block cache: 100% hit rate, 0 evictions, 50 MB resident.** The 206 MB store's
  working set fits comfortably in the 256 MiB cache, so this run is a
  **throughput/concurrency** test, not a cache-eviction test. (A true eviction
  test needs a store ≫ cache — see "The 91.6M graph" below for why that regime is
  hard to drive on a memory-constrained host.)

### `wiki_budget` — 2-hop expansions vs the intermediate guards

2-hop `count(DISTINCT)` fans out on higher-degree nodes, so the budget guard fires
(client-side `Statement.ExecutionFailed`, counted as "failures" by Locust — this is
the guard *working*, not a crash). This shape originally **OOM'd the 4 GB container at
1000 concurrent** even with the server-wide guard, because of two compounding defects
(findings 1 + 2b below). With both fixed — the runtime image's `MALLOC_ARENA_MAX=2`
and adjacency-charging graph expansion — the same suite now holds:

| `wiki_budget` (guard build, `maxIntermediateGlobal` = 1,000,000) | 100 | 500 | 1000 |
|---|--:|--:|--:|
| RSS (live, `docker stats`) | 353 MB | 571 MB | 555 MB |
| fail% (budget guard shedding hub queries) | 60% | 59% | 59% |
| outcome | OK | OK | **no OOM** |

Diagnostics at the end of the ramp: `fail_global_budget_total` ≈ 31k (the guard
rejecting expansion-heavy queries cleanly), `intermediate_global_peak` ≈ 5.1M (graph
expansion is now *visible* to the budget — before finding 2b it pinned at ~1.03M
counting only emitted rows), `fail_budget_total` ≈ 5, idle `rss_bytes` ≈ 524 MB. The
guard sheds ~60% of the deliberately-abusive hub 2-hops as retryable errors while the
container stays well under its 4 GB cap.

For contrast, the pre-fix build: tightening the *per-query* cap 10× (to 100,000)
slashed steady-state RSS to 33 MB but **still OOM'd at 1000 concurrent**, because the
transient peak (not steady state) was retained by the allocator and the expansion
working set was never charged — the two findings below.

## Findings

### 1. Process RSS under concurrency is a retained high-water — not a leak, but ~10× the cache budget

Under load, slater's process RSS (`/proc/self/statm`, confirmed by `docker stats`
and `smaps_rollup`: all **anonymous private-dirty**) rose to **~2.7 GB** while the
logical block cache stayed at **50 MB**. It is **not a leak**: across a fixed
50-user burst, RSS went 2717 → 2496 MB and **plateaued** while 90k *more* queries
ran — it tracks the **peak-concurrency heap high-water**, not cumulative query
count, and partially releases when concurrency drops.

The cause is allocator retention: many small, short-lived per-request allocations
(result encoding, expansion scratch) under the default glibc allocator, which keeps
freed memory in its per-CPU arenas rather than returning it to the OS. The
"resident ≈ cache budgets + small overhead" guarantee holds for the **cache**; under
high concurrency the resident set was dominated by this allocator high-water, ~10× the
256 MiB budget.

**Fixed — `MALLOC_ARENA_MAX=2` (+ `MALLOC_TRIM_THRESHOLD_=131072`) baked into the
runtime image.** Capping the arena count and trimming freed chunks back to the OS holds
RSS to **~0.5–0.6 GB** under the same `wiki_budget` ramp that previously climbed past
3.8 GB and OOM'd — a measured A/B on the guard build: identical run, RSS 555 MB and no
OOM with the env set vs. exit-137 without it. This is the single highest-leverage knob
for resident memory under concurrency; a jemalloc/mimalloc global allocator and/or a
heavy-query concurrency cap remain open options if a workload needs more headroom.
Override the env at run time if a workload genuinely benefits from more arenas.

### 2. The per-query budget does not bound *aggregate* concurrent memory

`query.maxIntermediate` caps **one** query's intermediate to N elements. The server
is exposed to **`N_concurrent × maxIntermediate`**: ~1000 queries each momentarily
building up to a 100k-element `count(DISTINCT)` set ≈ multiple GB instantaneously,
which OOM'd the 4 GB container even after the per-query cap was tightened 10×
(the per-step snapshot misses the transient peak, which is why steady RSS read 33 MB
while the process still died).

**Partial fix — a server-wide budget guard (implemented).** There is now a single
global ceiling, **`query.maxIntermediateGlobal`** (default 8,000,000 elements
≈ 384 MB at ~48 B/element; 0 disables), that all in-flight queries charge against in
addition to the per-query cap. Each `Engine` charges its intermediate elements
against a shared `GlobalIntermediateBudget`; a charge that would cross the ceiling
fails *that* query with a clean, retryable error (`fail_global_budget_total` in
diagnostics, plus the `intermediate_global_in_use` / `intermediate_global_peak`
gauges) instead of growing the heap. A point lookup charges ~0, so light load is
unaffected — re-running `wiki_cache_churn` on the guard build is byte-for-byte the
same (0 rejects, global peak ~211k ≪ the 8M ceiling).

Initially this guard alone did *not* close the `wiki_budget` OOM, which exposed a
deeper bug (finding 2b): on the first guard build `wiki_budget` still OOM'd even with
the ceiling tightened to 1M. The diagnostics were the tell — at the moment of death
the global charge sat right at its limit (`intermediate_global_peak` ≈ 1,026,943) while
**RSS had spiked to 3.8 GB**, so ~1M *charged* elements coincided with ~3.8 GB of real
memory (~3.8 KB per "element"). The guard's accounting was correct (the unit tests pin
the counter at the limit and verify the refund), but the **charge model under-counted
graph-expansion working memory** (finding 2b), and the allocator retained the transient
peak (finding 1). With both of those fixed, the guard now does bound the aggregate:
`intermediate_global_peak` rises to ~5.1M (expansion is finally visible — the few-MB
overshoot above the 1M ceiling is concurrent in-flight charges racing the reject) and
`wiki_budget` holds at 1000 concurrent without OOM (see the results table above).

### 2b. Graph expansion under-charged its working set — *fixed*

`expand_with_dir` reads a node's **entire adjacency list** (`outgoing()`/`incoming()`
→ `Vec<Adj>`) and builds a `Vec<Hop>` from it, and `expand_chain` only `charge(1)`'d
per *completed* path. So expanding a hub node allocated a large adjacency vector (and,
for `count(DISTINCT)`, a `HashMap`-per-row result far heavier than a 48 B `Val`)
**before any charge fired** — the element budget counted emitted rows, not the bytes
the expansion materialised. A node whose neighbours were all filtered out (a `node_ok`
mismatch) completed zero rows and so charged **nothing**, even after reading a million
edges. A flood of concurrent hub 2-hops therefore OOM'd despite both budgets.

**Fix (shipped):** the budgeted traversal wrapper `expand_one_hop` now charges the
produced hop count for every expansion, so reading a hub's adjacency trips the budget
*immediately*, before the `Vec<Hop>` and the downstream rows accumulate. The parallel
path (`expand_chain_par` → `par_walk`) charges the gathered neighbour buffer on the
calling thread once it lands — the rayon workers read adjacency but never touch the
non-`Sync` per-query `Cell`. The charge is deliberately left off `shortestPath()`
reconstruction (which shares `expand_with_dir` but is bounded by the dedicated
`maxShortestPathExplore` cap, not `maxIntermediate`). Unit tests over a hub fixture
(`testgen::write_hub`) prove a 1-hop and a 2-hop trip both the per-query and the
server-wide budget — including the filtered case where **zero rows complete** — on both
the sequential and the pooled paths, while a generous budget still expands the whole
star. In the load test, `wiki_budget`'s `intermediate_global_peak` goes from ~1.03M
(emitted rows only) to ~5.1M (adjacency visible) and the OOM is gone.

Two shapes were considered for the aggregate guard; option 1 is what shipped:

1. **Global intermediate accounting** (shipped) — a process-wide atomic of live
   intermediate elements that each query charges/refunds against alongside its
   per-query cap; a charge over the ceiling fails the query cleanly. With finding 2b's
   adjacency charging it now bounds expansion as well as proportionally-charged growth.
2. **Heavy-query admission control** (future) — a global semaphore limiting how many
   budget-charging (expand/aggregate) queries run concurrently, capping
   `N_concurrent` regardless of per-query size. Independent of the charge model; a
   further lever if a workload needs hard concurrency bounds beyond the budget.

### 3. Engine / planner observations surfaced by the disk-bound shapes

- **`count(n)` is O(1)** — answered from store metadata, not a scan. (Good, but it
  means the stock `count_all`-based scenarios don't exercise the store; the
  wikidata shapes were added for that reason.)
- **Full-scan aggregations trip the budget at the node count.** `max(n.prop)` and
  `... ORDER BY n.prop LIMIT 1` over a 1M-node label scan into a 1M
  `maxIntermediate` budget fail — the planner scans + materialises rather than using
  the range index to satisfy an ordered `LIMIT`. A range-index `min/max` /
  ordered-`LIMIT` fast path would help.
- **Unanchored variable-length ignores the deadline.** `MATCH (n)-[*1..4]-(m)` with
  no anchor on the 91.6M graph ran **far past `timeoutMs` (30 s)** and survived
  client disconnect — the deadline isn't checked inside the var-length inner loop.
- **`WHERE id(n) = $x` is a full node scan** (~10 s on 91.6M), not a seek. The
  **range-index point lookup is the fast path** (`{wikidata_id: $w}` ≈ 1.5 ms).

### 4. `pread` + cgroup page cache: the 91.6M graph on a small host

slater reads the store via `pread` + decompress (no mmap, D16), so the OS page cache
of the store files is charged to the container's cgroup. On the 91.6M / 766M graph
(14 GB on disk), the **open-time integrity scan** (`verify_against_disk` re-hashes
every block) pulls ~12 GB into page cache before serving a single query. On a 15 GiB
host shared with other containers:

- **Uncapped**, that 12 GB baseline leaves no headroom — load OOMs almost immediately.
- A **tight cap** (e.g. 4 GB) keeps the cgroup's page cache bounded *and* reclaims it
  (after open, container memory drops back to ~72 MB), but under a high-rps random
  read workload the page-cache **churn outpaces cgroup reclaim** and OOMs — the cap
  must exceed the shape's *distinct working set* (point-lookup ≈ the 2.7 GB
  `node_props`; 1-hop ≈ the 8.8 GB topology).

Net: a page-cache-inclusive cgroup hard cap is a blunt instrument for a `pread`-based
engine. slater's own heap is bounded (process RSS ~80 MB idle); the cgroup figure is
dominated by reclaimable page cache. For load testing on this host the **1M graph**
gives clean, repeatable numbers; the 91.6M graph needs either a much larger host or a
generous cap sized above the working set.

### 5. Block-cache sizing and the `MALLOC_ARENA_MAX` trade-off

Two follow-up runs probed (a) whether a smaller block cache forces eviction and (b)
what the `MALLOC_ARENA_MAX=2` allocator cap costs.

**Cache eviction is shape-dependent, not just cache-size-dependent.** The range index
is ~5.4 MB on disk → **~29 MB decompressed** (zstd ratio ~5.4×); the cache holds
decompressed blocks. At a **64 MiB** block cache:

- `wiki_cache_churn` (point lookups / 1-hop / range scans) never evicted — resident
  plateaued at **~39 MB, 0 evictions, ~100% hit**. Its working set is bounded by the
  narrow seed pool, not the cache, so it fits regardless. RSS held ~0.28 GB, 0 failures.
- `wiki_budget` (2-hop) **did** churn: resident pinned at the 64 MiB budget with
  **~14 k evictions** (~97.6% hit), because the neighbour-of-neighbour fan-out spans far
  more of the (~790 MB decompressed) topology than the 1-hop neighbourhoods. Still no
  OOM, RSS ~0.38 GB, the budget guard shedding ~60% as clean errors.

To actually evict the **index** you need a cache below ~half its 29 MB footprint
(≈12 MiB); at 64 MiB the index fits and only the 2-hop topology working set spills.

**`MALLOC_ARENA_MAX=2` has no usable throughput cost — it only bounds RSS.** A/B over
the `wiki_budget` ramp (10 GB cap so the default-arena arm doesn't OOM), throughput and
RSS at 1000 concurrent users (throughput means over 3 reps for 2/4):

| `MALLOC_ARENA_MAX` | throughput @1000u | RSS @1000u |
|---|--:|--:|
| **2** (shipped) | ~780 rps | **0.4–0.5 GB** |
| 4 | ~750 rps | 0.6–0.7 GB |
| 8 | ~670 rps | 1.3 GB |
| 128 (glibc default-ish) | ~940 rps | **3.9 GB** |

Among RSS-bounded settings (2/4/8) throughput is statistically flat — raising the arena
cap buys **no** speed, only resident memory, because the knee here is CPU-bound
closed-loop queueing (high cache-hit rate), not allocator-lock contention. The only
config that gained throughput (~+20%) was *unconstrained* arenas, which drove RSS to
3.9 GB and re-introduced the 4 GB-cap OOM — not a usable operating point. So `2` is the
sweet spot and is what the runtime image ships. (p99 at 1000 users is 5–10 s and highly
variable across all arms — closed-loop tail, not an allocator signal.)

## Reproducing

See [`perf/loadtest/README.md`](../perf/loadtest/README.md). In short, with a graph
loaded and the server started with `loadTestDiagnostics: true`:

```bash
python3 -m venv perf/loadtest/.venv
perf/loadtest/.venv/bin/pip install -r perf/loadtest/requirements.txt

# Automated ramp + brown-out knee + limiter attribution:
perf/loadtest/.venv/bin/python perf/loadtest/coordinator.py \
  --scenario wiki_cache_churn --db wikidata1m --password <pw> \
  --users 50,100,250,500,1000 --step 25
```

The wikidata-native shapes (`wiki_point`, `wiki_1hop`, `wiki_range`, `wiki_2hop`) and
scenarios (`wiki_cache_churn`, `wiki_point`, `wiki_budget`) build their value pool
from the graph's range index, so they run on any `Entity`/`wikidata_id` graph and
auto-skip elsewhere.
