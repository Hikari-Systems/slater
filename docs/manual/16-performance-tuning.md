# 16 · Performance tuning

Slater is built to serve graphs larger than RAM with bounded memory. Most of that
comes for free, but a handful of knobs and query shapes make a large difference.
This page is the tuning playbook: the fast paths to stay on, the memory guards to
size, and the parallelism and cache knobs to turn up.

## Count and metadata fast paths

Certain read shapes are answered from resident summaries and indexes **without
walking the graph**, turning what would be a full scan into an O(1)-ish lookup.
They engage automatically — the point of knowing them is to write queries that
*qualify*.

| Fast path | Example | Disqualified by |
|---|---|---|
| Single-node count | `MATCH (n:Person) RETURN count(*)` | (delta-aware; always available for bare `count(*)`) |
| Label + property count | `MATCH (n:Person {active:true}) RETURN count(*)` | falls back if the property is not range-indexed |
| Whole-graph edge count | `MATCH ()-[r]->() RETURN count(*)` | a constrained endpoint, or the same variable on both ends (self-loop) |
| Relationship-type enumeration | `MATCH ()-[r]->() RETURN type(r), count(*)` | a `WHERE`, a rel-type filter, a rel property, self-loops, or **pending deletes** |
| First-label enumeration | `MATCH (n) RETURN labels(n)[0], count(*)` | any label or property filter |
| Grouped index count | `MATCH (n:Person) RETURN n.age, count(*)` | property not range-indexed |
| Multi-hop count-pushdown | `MATCH (a)-[:KNOWS*2]->(b) RETURN count(*)` | `DISTINCT`, `ORDER BY`, `SKIP`/`LIMIT`, or non-constant extra return items |

Deletes are the subtle one: once the writable layer holds deletes, the maintained
marginals no longer describe the live graph, so the metadata fast paths step aside
and the query runs normally.

## Degree-column residency

Node degrees back the multi-hop count fast path. They live in a dense on-disk
column whose residency you choose:

- `cache.degreeColumn = lazy` (default) — fault ~1 MiB chunks on touch and evict
  cold ones; bounded by `cache.degreeColumnBytes` (default 256 MiB). Elastic
  memory, ideal for a small envelope.
- `cache.degreeColumn = pinned` — prefault the whole column and never evict.
  Lowest latency for degree-heavy workloads, at a fixed memory cost.

A small **hub-degree sidecar** is always resident and gives O(1) exact degree for
the highest-degree nodes, so hubs are routed before their adjacency is
materialised. See [12 Storage](12-storage.md) for the on-disk encoding.

## Hub adjacency streaming

For high-fan multi-hop traversals, materialising a mega-hub's entire adjacency can
blow up memory. When a node's degree is at or above `query.adjStreamThreshold`
(default 8192), its neighbours are **streamed** in chunks of `query.adjStreamChunk`
(default 8192) instead, bounding the live neighbour buffer. Raise the threshold if
you have memory to spare and want fewer, larger reads; lower it to cap peak memory
on skewed graphs.

## Per-query parallelism

`query.maxFanout` (default **1**, i.e. sequential) caps the worker threads a
single query may use for parallelisable work — shortest-path BFS, multi-hop
expansion, brute-force kNN, and anchor scans. Effective parallelism is
`min(maxFanout, cores)`. Raise it (e.g. to the core count) to speed up heavy
analytical queries; keep it at 1 when you serve many small queries concurrently
and would rather not have one query grab every core.

## Memory bounding

These guards keep a single query — or the server as a whole — from exhausting
memory. A breach fails **that query** with a clean, retryable error; it never
takes the server down.

| Knob | Default | Bounds |
|---|---|---|
| `query.maxIntermediate` | 1,000,000 | Retained intermediate elements in one query (comprehensions, `UNWIND`, list concat, aggregate/`DISTINCT` buffers, var-length paths). ~48 B each ≈ 48 MB. |
| `query.maxIntermediateGlobal` | 8,000,000 | The **sum** of retained intermediates across all in-flight queries. |
| `query.maxScan` | 500,000,000 | Transient walk work (adjacency reads/tallies); bounds work, not RSS. |
| `query.maxShortestPathExplore` | 0 (unlimited) | Nodes one `shortestPath()` BFS may discover. |
| `query.maxRows` / `query.timeoutMs` | 100000 / 30000 | Result-size and wall-clock ceilings. |

Under authenticated multi-tenant load, size `maxIntermediate` together with
`server.maxConnections` so the worst-case concurrent memory stays within your
envelope.

## Vector / ANN tuning

Recall-vs-latency for vector search is set at two points:

- **Query time** — `vectorQuery.beamWidth` (default 64) is the beam-search list
  size; larger means higher recall and higher latency.
- **Build time** — `--ann-threshold` (default 50000) decides brute-force vs
  Vamana+PQ; `--vamana-r`, `--vamana-alpha`, `--pq-subspaces`, `--pq-bits` shape
  the graph and quantiser. See [10 Vector search](10-vector-search.md) and
  [06 Build CLI reference](06-build-cli-reference.md).

## Cache sizing

Four resident pools dominate RSS; size them for your target envelope:

| Pool | Knob | Default |
|---|---|---|
| Block cache | `cache.blockCacheBytes` | 64 MiB |
| Vector cache (Vamana + PQ) | `cache.vectorCacheBytes` | 64 MiB |
| Result cache | `cache.resultCacheBytes` | 16 MiB |
| Range-index cache | `cache.rangeIndexCacheBytes` | 16 MiB |

Idle entries are swept on the `cache.cacheTtlMs` interval (default 30 min).
Pinning vector indexes (`vectorIndexPins`) or the degree column trades memory for
latency predictability.

## Next

- The storage-level encoding choices behind these knobs: [12 Storage](12-storage.md).
- Every knob's exact form and default: [14 Configuration reference](14-configuration-reference.md).
