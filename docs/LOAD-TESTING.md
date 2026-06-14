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

- **Zero failures**; the service held to 1000 concurrent clients on one box.
- Throughput peaks ~3k rps and **falls** as concurrency rises — core contention,
  not a hard cap. The p99 knee is at ~1000 users (queueing, p99 40 → 520 ms).
- **Block cache: 100% hit rate, 0 evictions, 50 MB resident.** The 206 MB store's
  working set fits comfortably in the 256 MiB cache, so this run is a
  **throughput/concurrency** test, not a cache-eviction test. (A true eviction
  test needs a store ≫ cache — see "The 91.6M graph" below for why that regime is
  hard to drive on a memory-constrained host.)

### `wiki_budget` — 2-hop expansions vs the per-query intermediate guard

2-hop `count(DISTINCT)` fans out past `query.maxIntermediate`, so the guard fires
(client-side `Statement.ExecutionFailed`, counted as "failures" by Locust — this is
the guard *working*, not a crash). Comparing two values of the per-query cap at the
1000-user step:

| `wiki_budget` @ 1000 users | `maxIntermediate` = 1,000,000 | `maxIntermediate` = 100,000 |
|---|--:|--:|
| steady-state RSS (snapshot) | 2496 MB | **33 MB** |
| outcome | **OOM (exit 137)** | **still OOM (exit 137)** |

Tightening the per-query cap 10× slashed *steady-state* RSS but **still OOM'd at
1000 concurrent** — see the aggregate-budget finding below.

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
freed memory in its arenas rather than returning it to the OS (no `MALLOC_ARENA_MAX`,
no `malloc_trim`, no jemalloc). The "resident ≈ cache budgets + small overhead"
guarantee holds for the **cache**; under high concurrency the resident set is
dominated by this allocator high-water, ~10× the 256 MiB budget.

**Mitigations to evaluate:** `MALLOC_ARENA_MAX=2` (or a jemalloc/mimalloc global
allocator), a periodic `malloc_trim`, and/or a global concurrency cap on executing
queries. Worth a dedicated A/B with the harness.

### 2. The per-query budget does not bound *aggregate* concurrent memory

`query.maxIntermediate` caps **one** query's intermediate to N elements. The server
is exposed to **`N_concurrent × maxIntermediate`**: ~1000 queries each momentarily
building up to a 100k-element `count(DISTINCT)` set ≈ multiple GB instantaneously,
which OOM'd the 4 GB container even after the per-query cap was tightened 10×
(the per-step snapshot misses the transient peak, which is why steady RSS read 33 MB
while the process still died).

**Recommended fix — a server-wide budget guard.** A single global ceiling that all
in-flight queries charge against, in addition to the per-query cap. Two shapes:

1. **Global intermediate accounting** (the real bound) — a process-wide atomic of
   live intermediate elements/bytes; each query charges/refunds against both its
   per-query cap *and* a global ceiling (configurable, in the spirit of the cache
   budgets). A query that would push the global total over the ceiling blocks
   (back-pressure) or fails with a clean "server busy" instead of OOMing. The
   per-query in-flight count already exists — this lifts it into a shared atomic
   with a wait/reject path and a `diagnostics` counter.
2. **Heavy-query admission control** (simpler, coarser) — a global semaphore
   limiting how many budget-charging (expand/aggregate) queries run concurrently,
   capping `N_concurrent` regardless of per-query size.

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
