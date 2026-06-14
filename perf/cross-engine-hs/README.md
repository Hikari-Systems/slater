# cross-engine-hs — larger-graph benchmark (MeSH + EU-AI-Act + Wikidata 1M & 91.6M)

The repo's main benchmark runs on the **pole** crime graph: ~62k nodes / ~106k
edges, ~23 MiB, *fully resident in RAM for every engine, no vectors*. On a graph
that small the headline claim — **slater's resident memory is bounded and does not
grow with the graph; the other engines hold the whole graph resident** — can't
actually be exercised: the in-memory engines are only ~1.4–1.7× slater's RSS.

This harness re-runs the **same metrics** against two much larger reference graphs
that ship in the `hs-backend-spot` snapshot, chosen to stress the two axes the toy
graph can't:

| graph | nodes | edges | range idx | vector idx | on-disk (zstd) | vectors (fp32) |
|---|---:|---:|---:|---:|---:|---:|
| **MeSH** | 340,839 | 469,438 | 5 (+1) | 0 | 24.5 MiB | 0 |
| **EU-AI-Act** | 20,766 | 44,790 | 167 | 2 | 58.2 MiB | 54.8 MiB |
| **Wikidata-1M** | 1,000,000 | 13,826,894 | 1 | 0 | 1.66 GiB (raw Cypher) | 0 |
| **Wikidata (full)** | 91,600,404 | 766,504,024 | 1 | 0 | 14 GB (slater gen) | 0 |

- **MeSH** — the large *pure-graph* case (5× the pole edge count): counts,
  point lookups, 1–3 hop traversals, group-by, `DISTINCT`, substring scan.
- **Wikidata-1M** — the *traversal-at-scale* case: a single-label `:Entity` graph
  (1M nodes / 13.8M `:LINK` edges, ~30× MeSH's edges, range index on `wikidata_id`).
  Query suite: count, indexed point lookup, degree, 1–3 hop expansions, bounded
  variable-length, and a bounded shortestPath (≤6) between random entities — the
  shape that exercises slater's `ANY SHORTEST` global-visited BFS.
- **EU-AI-Act** — the *vector* case: 15,238 1024-dim embeddings (54.8 MiB; a
  Concept group of 41 MiB + a Chunk group of 18 MiB). Adds a kNN query suite.
  Note (see the cache sweep below): slater serves kNN today via an **exact
  brute-force** scan that reads full-precision vectors **through the block cache**
  (`blockCacheBytes`), *not* the vector cache (`vectorCacheBytes`, which backs the
  not-yet-active Vamana/PQ path). At the default 64 MiB block budget the 54.8 MiB
  of vectors fit and stay resident; the interesting behaviour is what happens when
  you size that budget *below* a vector group.

Engines are the four from pole — **slater / Neo4j 5 / Memgraph / FalkorDB** — plus
two more: **ArcadeDB** (multi-model JVM server, openCypher over its Neo4j-Bolt
plugin) and **LadybugDB** (a Kùzu-derived *embedded* graph DB, the `real_ladybug`
wheel; no server/port — it runs in-process inside a thin `slater-ladybug` image
against a `.lbug` data volume). Because Neo4j and Memgraph community editions serve
one database at a time, the two graphs are **two separate runs**, each a single
graph across all engines. ArcadeDB and LadybugDB have no openCypher kNN procedure,
so on the EU-AI-Act run they execute only the **non-vector** subset of the suite.

> Thanks to **Arun Sharma ([adsharma](https://github.com/adsharma))** for guidance
> on adding **LadybugDB** to this benchmark.

## Method

Identical to the pole harness (`perf/cross-engine/`): per query, 15 warm-ups then
25 measured calls with a **fresh parameter every call** (so the result cache always
misses — real execution cost), median of the 25; each engine is **restarted before
every run** and re-warmed, and the reported figure is the **mean of 5 such runs**.
Peak/steady RSS is read from the container cgroup after the final run. Row counts
are cross-checked across engines (slater matches the Neo4j/Memgraph reference on
every query). slater is served on its **default cache budgets — block 64 + vector
32 + result 16 MiB** — the config under which "bounded memory" is the whole point.

**ArcadeDB** is driven through the same `neo4j` Python driver over its Bolt plugin
(`encrypted=False`, db `bench`). Its data model is composite-type-per-label-set and
it retypes records when labels change, which breaks the shared `__DumpVertex__`
join-and-strip loader (stale index RIDs); so it has a dedicated `load_arcadedb.py`
that is schema-first and inheritance-based — the label common to every node becomes
a super-type, the others `EXTEND` it, and relationships join on an indexed
`__dump_id__` with no label strip. **LadybugDB** is embedded: each bench run is a
fresh `docker run` of the `slater-ladybug` image (a clean process — no restart
needed), and because it has no server cgroup its RSS is the bench process'
high-water `ru_maxrss`. Its buffer pool is capped (`buffer_pool_size`, default
grabs ~80% of RAM) to 512 MiB so the figure is comparable to the others' cache
budgets. LadybugDB is single-label-per-node, so `load_ladybug.py` maps every node
to one primary table, stores all labels in a `__labels` string, and `engines.py`
rewrites secondary-label patterns to the primary table + a `__labels CONTAINS`
filter; it also has no secondary range indexes (only the primary key), so its
property lookups are scans.

The vector indexes use each engine's native procedure — slater/FalkorDB
`db.idx.vector.queryNodes`, Neo4j `db.index.vector.queryNodes`, Memgraph
`vector_search.search` — and a shared pool of real embeddings as query vectors
(slater keeps embeddings in its vector store, not as a readable property, so the
pool is sampled once from Neo4j and reused identically across engines). For MeSH a
single `:MeshTerm(type)` range index is added uniformly on every engine so `type`
(Drug/Organism/Disease/PharmacologicalAction) is the low-cardinality indexed analog
of pole's `Crime.type`.

```bash
python3 -m venv /tmp/pole_venv && /tmp/pole_venv/bin/pip install neo4j falkordb falkordb-bulk-loader
SNAP=../../../hs-backend-spot/slater-snapshot/cypher
# MeSH
./setup_hs.sh   mesh      $SNAP/bioalphaengine-companies.cypher MeshTerm:type
./run_bench_hs.sh mesh    bench_mesh.py 340839
./aggregate_hs.py /tmp/bench-hs/results-mesh
# EU-AI-Act
./setup_hs.sh   eu_ai_act $SNAP/eu_ai_act.cypher
./run_bench_hs.sh eu_ai_act bench_vec.py 20766
./aggregate_hs.py /tmp/bench-hs/results-eu_ai_act
# Wikidata-1M — slater-build + LadybugDB COPY, the four service engines via native
# bulk importers (see "Loading at scale"); then the standard restart-sweep:
./run_bench_hs.sh wikidata1m bench_wiki.py 1000000
./aggregate_hs.py /tmp/bench-hs/results-wikidata1m
```

`setup_hs.sh` builds the slater generation (`slater-build`, stamping the ACL) and
the `slater-ladybug` image, then loads Neo4j/Memgraph/FalkorDB from the same
primitive-Cypher dump via `load_cypher.py` (a quote-aware parser → UNWIND batches;
FalkorDB needs `vecf32()` on load and `efRuntime:256` on the vector index for full
top-k recall), ArcadeDB via `load_arcadedb.py`, and LadybugDB via a containerized
`load_ladybug.py`.

### Loading at scale (Wikidata-1M)

The uniform `UNWIND…MATCH` loaders are fine at MeSH scale (~60 s/engine) but at 13.8M
edges they cost ~20 min/engine (ArcadeDB ~90 min) — the per-batch Bolt round-trips
and per-row index MATCH dominate. So Wikidata-1M is loaded via each engine's **native
bulk path** from a shared CSV pair (`bulk_export.py` → `nodes.csv` + `edges.csv`,
RFC4180-quoted so names with commas/quotes/`\u…` round-trip). LadybugDB already uses
Kùzu `COPY`. Load times for 1M nodes / 13.8M edges:

| engine | bulk path | load time | vs UNWIND |
|---|---|--:|--:|
| Neo4j | `neo4j-admin database import` (offline, into a volume) | **10 s** | ~100× |
| FalkorDB | `falkordb-bulk-insert` (CSV → binary protocol) | **25 s** | ~50× |
| Memgraph | `LOAD CSV` (in `IN_MEMORY_ANALYTICAL` mode) | **54 s** | ~22× |
| LadybugDB | Kùzu `COPY FROM` | ~2 min | — |
| ArcadeDB | `IMPORT DATABASE` (native importer) | 16.5 min | ~5× |

ArcadeDB is the outlier: even its native importer is slow at edge ingestion. Each
loader builds the engine's range index on `wikidata_id` and persists (Neo4j volume,
FalkorDB `SAVE`, Memgraph snapshot) so the data survives the bench's restart cycle.

## Results

### MeSH — 340,839 nodes / 469,438 edges (pure graph)

Latency, mean of 5 runs. Mark on slater: 🟢 sole fastest, ⚪ ties (within 25%).

| query | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| count all nodes | **0.55 ms 🟢** | 13.92 | 22.69 | 16.20 | 77.84 | 1.45 |
| Disease label count | **0.55 ms 🟢** | 4.28 | 19.86 | 1.04 | 4.28 | 3.87 |
| point lookup (idx meshUi) | 1.97 | 4.18 | 0.48 | 0.49 | 0.73 | 8.40 |
| idx-eq count (MeshTerm.type) | **0.58 ms 🟢** | 5.28 | 4.58 | 2.03 | 370.18 | 2.36 |
| 1-hop type→BROADER_THAN | **1.37 ms ⚪** | 6.33 | 1.29 | 3.21 | 123.77 | 4.77 |
| 2-hop BROADER_THAN chain | 25.07 | 5.81 | 8.42 | 16.28 | 4.71 | 6.20 |
| group-by type | 19.14 | 51.30 | 61.65 | 29.53 | 399.57 | 5.24 |
| 3-hop Drug/action/Drug | 1.54 | 3.69 | 6.26 | 1.02 | 9.73 | 10.82 |
| full-scan CONTAINS | **0.56 ms 🟢** | 5.19 | 23.14 | 1.63 | 15.65 | 4.30 |
| count DISTINCT type | 19.09 | 46.18 | 60.62 | 37.34 | 390.35 | 4.92 |

(ms; mark is slater vs the field. LadybugDB now wins group-by/DISTINCT outright, so
slater no longer earns the 🟢 there.)

| resident memory | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| **peak RSS** | 262 MiB | 1117 MiB | 355 MiB | 454 MiB | 1631 MiB | **125 MiB 🟢** |
| steady RSS | 219 MiB | 1083 MiB | 326 MiB | 453 MiB | 1612 MiB | **125 MiB 🟢** |

slater's footprint is **not** resident graph: idle RSS is ~16 MiB, individual queries
peak at 36–75 MiB; the 262 MiB **peak is transient per-query working memory** across
the suite (wide scans filling the 64 MiB block cache + traversal intermediates) that
the glibc allocator holds as a high-water mark. slater is **fastest on the count /
idx-eq / scan shapes** (its metadata + index fast paths — 10–40× the service engines
on the big graph), ties on the indexed 1-hop, and **trails on the unanchored 2-hop**
(it label-scans all 340k MeshTerm anchors). slater's `query.maxIntermediate` budget
(1M elements) rejects the unanchored all-relationship count used to warm caches — a
deliberate bounded-memory guard — so that priming step is best-effort and skipped.

The two new engines behave very differently:
- **LadybugDB** (embedded, Kùzu-derived, columnar) is the **smallest RSS by far —
  125 MiB** — and its columnar aggregation makes group-by and `count(DISTINCT)` the
  fastest of any engine (~5 ms). But it builds **no secondary range index** (only the
  primary key), so its property point-lookup is a scan (8.4 ms). Its single-label
  model means every node lives in one `MeshTerm` table with the sub-label kept in a
  `__labels` string and rewritten at query time.
- **ArcadeDB** (JVM multi-model server) carries the **heaviest footprint (1631 MiB)**.
  Its sub-type index serves the `:Drug{meshUi}` point lookup fast (0.73 ms), but a
  **polymorphic** `:MeshTerm{type}` filter (or group-by/DISTINCT over the super-type)
  cannot use the per-sub-type indexes and **scans** — 120–400 ms.

### Wikidata-1M — 1,000,000 nodes / 13,826,894 edges (traversal at scale)

Latency, mean of 5 runs. Mark on slater: 🟢 sole fastest, ⚪ ties (within 25%).

| query | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| count all nodes | **0.55 ms 🟢** | 30.70 | 43.76 | 44.08 | 201.28 | 2.02 |
| point lookup (idx wikidata_id) | 1.34 | 4.24 | 0.47 | 0.47 | 1.09 | 7.54 |
| degree (1-hop count) | 1.33 | 6.89 | 0.79 | 0.49 | 1.09 | 3.25 |
| 1-hop neighbours | 1.50 | 10.60 | 1.58 | 0.48 | 0.97 | 3.25 |
| 2-hop | 1.70 | 9.97 | 2.03 | 0.52 | 2.71 | 7.63 |
| 3-hop | 1.65 | 6.29 | 2.10 | 0.68 | 4.05 | 68.25 |
| var-length *1..2 distinct | 1.63 | 167.59 | 547.65 | 0.60 | 1.17 | 20.56 |
| shortestPath ≤6 | 3.14 | 4.23 | 2464.96 | 20.98 | 1.29 | 18.54 |

| resident memory | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| **peak RSS** | 645 MiB ⚪ | 2012 MiB | 2716 MiB | 1506 MiB | 2247 MiB | **604 MiB** |
| steady RSS | 583 MiB ⚪ | 1988 MiB | 1918 MiB | 1218 MiB | 2222 MiB | 604 MiB |

This is where the bounded-memory thesis lands hardest: at 1M nodes / 13.8M edges,
**slater (645 MiB) and embedded LadybugDB (604 MiB) are the only sub-GiB engines**,
while the resident in-memory servers hold **1.5–2.7 GiB** (Memgraph 2716, ArcadeDB
2247, Neo4j 2012, FalkorDB 1506). slater is **sole-fastest on `count`** (its metadata
fast path — 56–365× the service engines) and competitive on the hop traversals
(1.3–1.7 ms). Standouts per engine:

- **shortestPath ≤6** between random (mostly disconnected) entities: slater's
  `ANY SHORTEST` global-visited BFS runs it in **3.1 ms** and ArcadeDB in 1.3 ms,
  while **Memgraph's `*BFS` expansion takes 2465 ms** (it explores to the depth bound)
  and FalkorDB/LadybugDB are ~20 ms. (Per-engine syntax — see `bench_wiki.shortest_path`.)
- **var-length `*1..2` distinct**: FalkorDB 0.6 ms / ArcadeDB 1.2 ms / slater 1.6 ms,
  vs Neo4j 168 ms and Memgraph 548 ms.
- **FalkorDB** is the traversal-latency champion (sub-millisecond 1–3 hop) at a 1.5 GiB
  resident cost; **slater** is within ~3× on those hops at <½ the RAM.
- **LadybugDB** keeps the smallest RSS but has no secondary range index, so its point
  lookup scans (7.5 ms) and its 3-hop (68 ms) feels the capped 512 MiB buffer pool.
- slater's `query.maxIntermediate` (1M) occasionally rejects a var-length expansion
  off a high-degree hub; `bench_wiki` records the median of the calls that succeed.

### Full Wikidata — 91,600,404 nodes / 766,504,024 edges (disk-bound, working set ≫ RAM)

The scale-up of the Wikidata-1M graph to the **complete** generation: 91.6M `:Entity`
nodes / 766M `:LINK` edges (~55× the 1M edge count), range index on `wikidata_id`,
**14 GB** on disk. This is the regime the whole bounded-memory thesis was built for —
the working set is far larger than the **15 GiB** host RAM, so traversals are genuinely
**disk-bound**, and **only the disk-backed engines can hold the graph at all**:

- **slater** and **Neo4j 5** load it (both page the store from disk).
- **FalkorDB** and **Memgraph** keep the whole graph resident in RAM (≈64–128 GiB at
  this scale) → **cannot-load** on a 15 GiB box.
- **ArcadeDB**'s importer runs for ~days at 766M edges → **skipped**.
- **LadybugDB** load is on hold (its edge `COPY` needs a large dedicated buffer pool,
  run in isolation) — to be added.

Because the on-disk store dwarfs RAM, the cgroup `memory.peak` is dominated by
reclaimable OS **page cache** and badly misrepresents an engine's own footprint. So the
resident figure here is the **anonymous** high-water (`memory.stat` `anon` — heap +
caches + query working memory), sampled live during each sweep. Each engine is benched
**in isolation** (one big container at a time on the 15 GiB box).

Latency, ms. slater = mean of 5 restart-cycle runs (very stable); Neo4j = its one clean
complete pass (see the instability note). Mark is **vs the field that can load** (slater
vs Neo4j): 🟢 sole-fastest, ⚪ tie (within 25%).

| query | slater | Neo4j 5 | FalkorDB | Memgraph |
|---|--:|--:|:--:|:--:|
| count all nodes | **0.58 🟢** | ~4000 | cannot-load | cannot-load |
| point lookup (idx wikidata_id) | **1.30 🟢** | 9.72 | cannot-load | cannot-load |
| degree (1-hop count) | **1.33 🟢** | 14.4 | cannot-load | cannot-load |
| 1-hop neighbours | **4.25 🟢** | 12.3 | cannot-load | cannot-load |
| 2-hop | 18.16 ⚪ | 17.4 | cannot-load | cannot-load |
| 3-hop | **26.66 🟢** | 74.9 | cannot-load | cannot-load |
| var-length *1..2 distinct | **9.09 🟢** | 116 † | cannot-load | cannot-load |
| shortestPath ≤6 | **52.6 🟢** (fanout 8) · 82.6 (fanout 1) | 131.9 | cannot-load | cannot-load |

† Neo4j's `var-length *1..2 distinct` completes at 116 ms only on the first warm pass;
on every repeat it **OOMs its 2 GiB heap** (`MemoryPoolOutOfMemoryError` after ~46 s),
and the restart-cycle eventually crashed the container outright. slater answers the same
query in 9 ms within its 1M-element `maxIntermediate` budget — bounded by design.

| anon RSS | slater | Neo4j 5 |
|---|--:|--:|
| idle | 71 MiB | — |
| shortestPath sweep | 700 MiB (fanout 1) · 925 MiB (fanout 8) | **2911 MiB** |

This is the thesis at full strength: serving a **766M-edge** graph, slater's resident
footprint stays **sub-GiB** (idle 71 MiB) and tracks the *query working set*, not the
graph — while Neo4j sits at a committed **~2.9 GiB** (its 2 GiB heap + off-heap buffers,
the same ~2.87 GiB on the fast-query sweep) regardless of query, and the two in-memory
engines can't load the graph at all. slater is **sole-fastest on count / point / degree
/ 1-hop / 3-hop / var-length** — it answers `count` from generation metadata (0.58 ms vs
Neo4j's ~4 s disk scan, ≈7000×) and the point/degree/hop shapes at 3–11× Neo4j — ties on
the cold 2-hop, and is faster on shortestPath. Notes:

- **2-hop / 3-hop**: the sampled anchors are low-`Q` (high-degree hub) entities, so these
  explore large cold-block neighbourhoods — slater 18 / 27 ms, Neo4j 17 / 75 ms. (At 1M
  scale both were ~2 ms; the jump is the disk-bound working set, not the algorithm.)
- **shortestPath ≤6** between random (mostly disconnected) entities forces a full bounded
  BFS to prove no path exists: slater's `ANY SHORTEST` global-visited BFS runs it in
  **82.6 ms sequentially → 52.6 ms** with per-query parallelism (`maxFanout=8`); Neo4j's
  bidirectional BFS medians **131.9 ms** (with a 4 s tail on one pathological pair).
  shortestPath latency is cache-sensitive (slater ranged 53–177 ms across cache states);
  the same-build fanout 1→8 speedup is the stable signal.

**Per-query parallelism on the full graph** (the `maxFanout` track) — same build,
sequential vs 8-way:

| query (slater) | fanout=1 | fanout=8 | speedup |
|---|--:|--:|--:|
| shortestPath ≤6 | 82.6 ms | 52.6 ms | 1.6× |
| 2-hop count(*) | 127.9 ms | 60.3 ms | 2.1× |
| 3-hop count(*) | 923.3 ms | 503.3 ms | 1.8× |

The `count(*)`-over-expansion rows are the unbounded 2-/3-hop reductions (no `LIMIT`),
exercising the parallel anchor-scan + expand + count path. Parallelism trades memory for
latency — the fanout=8 3-hop count's anon working set rises to ~5.2 GiB (8 worker
frontiers) — but stays bounded and well under the in-memory engines' resident graph.

**Bulk build / load** — the irony this whole effort resolved: slater can now *build* a
graph larger than RAM (the in-memory builder was OOM-killed on this one).

| engine | on-disk | builder / loader | build cost |
|---|--:|---|---|
| slater | **14 GB** | `slater-build --external on` (spill-to-disk) | ~29 min, peak **3.5 GiB** RSS under a 4 GiB cap |
| Neo4j 5 | 41.8 GB | `neo4j-admin database import` (offline → volume) | not re-timed (focus is RAM) |

slater's external builder spills to disk and stays under a 4 GiB `--max-memory` cap to
build the 91.6M/766M generation in ~29 min, and the resulting generation is **3× smaller
on disk than Neo4j's store** (14 vs 41.8 GB).

### EU-AI-Act — 20,766 nodes / 44,790 edges, 54.8 MiB of vectors

| query | slater | Neo4j 5 | Memgraph | FalkorDB |
|---|--:|--:|--:|--:|
| kNN top-10 Concept | 16.25 ms | 8.01 ms | 1.88 ms | 1.15 ms |
| kNN top-50 Concept | 16.86 ms | 8.67 ms | 2.01 ms | 1.25 ms |
| kNN top-10 Chunk | 8.26 ms | 5.54 ms | 1.84 ms | 1.34 ms |
| kNN-10 + 1-hop expand | 15.97 ms | 5.29 ms | 1.66 ms | 1.16 ms |
| count all nodes | **0.51 ms 🟢** | 3.45 ms | 1.22 ms | 1.31 ms |
| Concept label count | **0.51 ms 🟢** | 2.82 ms | 1.47 ms | 0.91 ms |
| point lookup (idx id) | 0.97 ms | 3.86 ms | 0.43 ms | 0.43 ms |

| resident memory | slater | Neo4j 5 | Memgraph | FalkorDB |
|---|--:|--:|--:|--:|
| **peak RSS** | **144 MiB 🟢** | 694 MiB | 219 MiB | 317 MiB |
| steady RSS | 139 MiB 🟢 | 687 MiB | 195 MiB | 289 MiB |

(slater sole-smallest here — the next engine, Memgraph at 219 MiB, is >25% larger.)

slater serves the whole graph at **144 MiB — the smallest of the four, 4.8× below
Neo4j.** At the default 64 MiB block budget its 54.8 MiB of vectors *are* resident
(they fit), so the kNN gap is **not** paging — it is purely **algorithmic**: slater
does an exact brute-force O(N) scan (these indexes are below its 50k-vector ANN
threshold) where the others answer from a resident HNSW, so slater is 16 ms where
FalkorDB is 1 ms. The bounded-memory dial shows up only when you size the budget
below the vector set — see the sweep next.

### Cache-budget sweep (slater only) — and the right knob

Brute-force kNN reads full-precision vectors **through the block cache**
(`exec.rs` `vector_group`; the test `vector_knn_reads_route_through_the_block_cache`
asserts it). So the lever for vector residency is **`blockCacheBytes`**, *not*
`vectorCacheBytes` (the latter backs the not-yet-active Vamana/PQ pool — sweeping
it leaves brute-force kNN untouched, a trap worth flagging). Sweeping the **block**
budget below the vector-group sizes (Concept 41 MiB, Chunk 18 MiB):

| block cache | Concept kNN-10 | Chunk kNN-10 | note |
|---:|---:|---:|---|
| 64 MiB (default) | 16.9 ms | 8.6 ms | both groups resident |
| 48 MiB | 16.4 ms | 8.2 ms | Concept (41 MiB) still fits |
| 40 MiB | 43.6 ms | 8.3 ms | **Concept evicts → re-read/decompress each scan** |
| 24 MiB | 43.0 ms | 8.2 ms | Concept thrashes, Chunk (18 MiB) still fits |
| 16 MiB | 42.8 ms | 22.3 ms | **Chunk now evicts too** |

The cliff lands **exactly at each group's working-set size** (Concept ~46 MiB incl.
graph blocks, Chunk ~22 MiB) — a clean, reproducible signal, not a flat line. This
**is** the bounded-memory dial: shrink the budget below a vector group and slater
keeps serving, paying ~2.7× kNN latency (16→44 ms) to re-fetch+re-decompress the
group per query, while RSS falls. The in-memory engines have no equivalent — they
hold the whole graph + HNSW resident or they don't run. (Caveat: the 54.8 MiB file
stays in the host OS page cache, so the degradation here is re-decompression cost,
not physical-disk I/O; a true disk-bound measurement needs a graph larger than RAM.)

Because brute-force vectors share the block LRU with graph reads, a vector-heavy
deployment should size `blockCacheBytes` ≥ its largest vector group. The dedicated
`VectorIndexCache` was built to isolate the large-vector path from graph blocks —
that isolation activates once the Vamana/PQ (M7) path ships and PQ codes (~32×
smaller than full f32) carry the search.

MeSH (no vectors), block cache 64 → 256 → 512 MiB: latencies and RSS **flat** — its
hot blocks already fit in the default 64 MiB, so a bigger budget changes nothing.
(MeSH's peak RSS is **not** block-cache-resident data — idle is ~16 MiB; the ~260
MiB peak is transient per-query working memory the allocator retains, per the MeSH
section above — which is exactly why the block budget doesn't move it.)

## Files

- `load_cypher.py` — primitive-Cypher → Neo4j/Memgraph/FalkorDB (quote-aware parser,
  UNWIND batches, per-engine vector DDL).
- `load_arcadedb.py` — schema-first inheritance loader for ArcadeDB (super-type +
  `EXTENDS` sub-types, `__dump_id__` join; DDL over HTTP/SQL, data over Bolt).
- `load_ladybug.py` — loader for embedded LadybugDB (single primary table per node,
  `__labels` string, type inference + coercion, Kùzu `COPY` for rels); runs inside
  `Dockerfile.ladybug`.
- `bulk_export.py` — emit `nodes.csv` + `edges.csv` (RFC4180-quoted) from a
  single-label/single-rel-type dump, for the native bulk importers (Wikidata-1M).
- `bench_mesh.py` / `bench_vec.py` / `bench_wiki.py` — the three query suites
  (`engines.py` = shared connection layer; dedicated `-hs` ports 7700/7701/7702/6401,
  ArcadeDB 7703; LadybugDB is embedded, no port).
- `bench_with_rss.py` — runs a bench in-process and records peak RSS (LadybugDB).
- `setup_hs.sh` / `run_bench_hs.sh` / `count_hs.py` / `aggregate_hs.py` — orchestration.
