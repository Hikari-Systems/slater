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
  Note (see the cache sweep below): slater serves kNN via an **exact brute-force**
  scan, but with a SIMD distance kernel over a **resident, pre-normalised vector
  matrix** held in the vector cache (`vectorCacheBytes`, default raised 32→64 MiB so
  the 54.8 MiB estate fits) — decoded once, scanned with no per-query gather. If a
  group does not fit the budget, kNN falls back to reading full-precision vectors
  **through the block cache** (`blockCacheBytes`); the sweep below shows that fallback
  regime and what happens when you size a budget *below* a vector group.

Engines are the four from pole — **slater / Neo4j 5 / Memgraph / FalkorDB** — plus
two more: **ArcadeDB** (multi-model JVM server, openCypher over its Neo4j-Bolt
plugin) and **LadybugDB** (a Kùzu-derived *embedded* graph DB, the `real_ladybug`
wheel; no server/port — it runs in-process inside a thin `slater-ladybug` image
against a `.lbug` data volume). Because Neo4j and Memgraph community editions serve
one database at a time, the two graphs are **two separate runs**, each a single
graph across all engines. **ArcadeDB** has no kNN procedure, so on the EU-AI-Act run it
executes only the **non-vector** subset. **LadybugDB does** serve kNN — its Kùzu base
ships a native disk-based HNSW index (`CALL CREATE_VECTOR_INDEX` / `QUERY_VECTOR_INDEX`),
which `bench_vec.py` drives per-engine exactly as it does the other natives; it is no
openCypher `db.idx.vector.queryNodes`, but it is a real ANN index and is benched in full.

> Thanks to **Arun Sharma ([adsharma](https://github.com/adsharma))** for guidance
> on adding **LadybugDB** to this benchmark.

## Method

Identical to the pole harness (`perf/cross-engine/`): per query, 15 warm-ups then
25 measured calls with a **fresh parameter every call** (so the result cache always
misses — real execution cost), median of the 25; each engine is **restarted before
every run** and re-warmed, and the reported figure is the **mean of 5 such runs**.
Peak/steady RSS is read from the container cgroup after the final run.

The figures below are **slater v0.8.0** (the published `hikarisystems/slater:v0.8.0`
image), each engine **measured in isolation** — every other engine container stopped,
started one at a time — so a row is that engine's own footprint with no cross-engine
memory/cache pressure. (`setup_hs.sh` takes a `SLATER_IMG` env override to benchmark a
specific release.) Row counts
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
property lookups are scans. Its embedding-bearing node tables load via Kùzu `COPY`
(per-row `UNWIND…CREATE` of a `FLOAT[1024]` column is ~9 ms/row — minutes for 15k
vectors; `COPY` is ~65× faster), and its native HNSW vector index is built at load
(`CALL CREATE_VECTOR_INDEX`), so it serves kNN from a resident ANN index.

**Cross-engine consistency.** All engines run identical query *text* under one method
(15 warm-ups → 25 measured, fresh parameter each call, median of 25, mean of 5
restart-cycle runs, row counts cross-checked vs Neo4j). Two deliberate asymmetries to
keep in mind when reading the tables: (1) **per-engine syntax** where a shape has no
portable form — `shortestPath` (`bench_wiki.shortest_path`) and kNN (`bench_vec.knn`:
slater/FalkorDB `db.idx.vector.queryNodes`, Neo4j `db.index.vector.queryNodes`, Memgraph
`vector_search.search`, LadybugDB `QUERY_VECTOR_INDEX`); (2) **RSS source** — server
engines report the container cgroup, embedded LadybugDB reports the bench process'
`ru_maxrss` high-water (includes the Python driver + the query-vector pool), so its
memory figures are an upper bound, not strictly comparable to the server cgroups.

The vector indexes use each engine's native procedure (slater/FalkorDB
`db.idx.vector.queryNodes`, Neo4j `db.index.vector.queryNodes`, Memgraph
`vector_search.search`, LadybugDB/Kùzu `QUERY_VECTOR_INDEX`) over a shared pool of real
embeddings as query vectors (slater keeps embeddings in its vector store, not as a
readable property, so the pool is sampled once from Neo4j and reused identically across
engines — the embedded LadybugDB container mounts the same pool file). For MeSH a
single `:MeshTerm(type)` range index is added uniformly on every engine so `type`
(Drug/Organism/Disease/PharmacologicalAction) is the low-cardinality indexed analog
of pole's `Crime.type`.

```bash
python3 -m venv /tmp/pole_venv && /tmp/pole_venv/bin/pip install neo4j falkordb falkordb-bulk-loader
SNAP=../../../hs-backend-spot/slater-snapshot/cypher
# MeSH
./setup_hs.sh   mesh      $SNAP/mesh.cypher MeshTerm:type
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

### Pole — 61,521 nodes / 105,840 edges (the baseline graph, all six engines)

The original four-engine pole benchmark (`perf/cross-engine/`) covers slater / Neo4j /
Memgraph / FalkorDB. Running the same 10 pole shapes through this six-engine harness
(`bench_pole.py`, `setup_hs.sh pole … Crime:type`) adds **ArcadeDB** and **LadybugDB**.
Two loader fixes were needed for pole's heterogeneous schema (no single label spans
every node): `load_arcadedb.py` falls back to a synthetic `__DumpNode__` super-type when
no business label is common (the same reason ArcadeDB is absent from the EU-AI-Act
table), and `load_ladybug.py`'s per-row rel `CAST` now tolerates prop-less rel types.

Latency, mean of 5 runs. Mark on slater: 🟢 sole fastest, ⚪ ties (within 25%).

| query | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| count all nodes | **0.56 ms 🟢** | 6.38 | 3.39 | 3.44 | 15.39 | 2.53 |
| Crime label count | **0.58 ms 🟢** | 5.09 | 4.01 | 1.96 | 7.73 | 1.37 |
| point lookup (idx nhs_no) | 0.63 | 4.49 | 0.47 | 0.49 | 0.75 | 0.50 |
| idx-eq count (Crime.type) | **0.59 ms ⚪** | 3.33 | 0.89 | 0.59 | 3.37 | 1.54 |
| 1-hop Crime→Location | 1.35 | 6.62 | 1.26 | 0.76 | 4.11 | 3.00 |
| 2-hop Person→Loc→Area | 2.44 | 5.52 | 1.21 | 0.99 | 16.31 | 4.09 |
| agg crimes by type | 2.67 ⚪ | 9.10 | 7.14 | 3.68 | 39.33 | 3.11 |
| 3-hop Officer/Crime/Loc | 2.55 | 3.79 | 2.01 | 0.98 | 2.83 | 6.04 |
| full-scan CONTAINS | **0.61 ms 🟢** | 5.70 | 7.22 | 3.23 | 31.84 | 1.90 |
| count DISTINCT type | 2.54 ⚪ | 7.74 | 6.71 | 4.36 | 38.32 | 2.95 |

| resident memory | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| **peak RSS** | **50 MiB 🟢** | 746 | 114 | 140 | 1556 | 198 |
| steady RSS | **39 MiB 🟢** | 743 | 113 | 138 | 1499 | 198 |

On this toy graph the memory thesis can't bite (the whole graph is ~23 MiB, resident for
everyone) — slater is smallest at 50 MiB but only ~2.5–4× below Memgraph/FalkorDB, the
point the larger graphs below make. The added engines behave as elsewhere: **LadybugDB**
is competitive (1–6 ms) at 198 MiB, and its missing secondary index doesn't bite at this
scale (the `nhs_no` lookup scans just 369 Person nodes → 0.50 ms); **ArcadeDB** carries
the heaviest footprint (1556 MiB) and its polymorphic super-type scan makes the
aggregations slow (group-by / `count(DISTINCT)` ~40 ms), consistent with its MeSH profile.
(LadybugDB/ArcadeDB RSS caveats per the *Method* consistency note: ArcadeDB is a cgroup
figure, LadybugDB an embedded `ru_maxrss`.)

### MeSH — 340,839 nodes / 469,438 edges (pure graph)

Latency, mean of 5 runs. Mark on slater: 🟢 sole fastest, ⚪ ties (within 25%).

| query | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| count all nodes | **0.57 ms 🟢** | 14.96 | 23.77 | 16.39 | 81.98 | 2.16 |
| Disease label count | **0.58 ms 🟢** | 4.20 | 20.72 | 1.06 | 4.42 | 4.29 |
| point lookup (idx meshUi) | 1.95 | 3.85 | 0.48 | 0.48 | 0.65 | 8.80 |
| idx-eq count (MeshTerm.type) | **0.60 ms 🟢** | 4.87 | 5.03 | 2.02 | 381.34 | 2.54 |
| 1-hop type→BROADER_THAN | **1.32 ms ⚪** | 5.82 | 1.21 | 4.10 | 389.57 | 4.91 |
| 2-hop BROADER_THAN chain | 33.11 | 5.63 | 8.49 | 16.67 | 443.57 | 6.44 |
| group-by type | 20.09 | 51.45 | 64.15 | 30.56 | 410.95 | 5.49 |
| 3-hop Drug/action/Drug | 7.86 | 3.37 | 5.47 | 1.01 | 9.70 | 11.23 |
| full-scan CONTAINS | **0.59 ms 🟢** | 5.39 | 24.09 | 1.69 | 16.27 | 4.11 |
| count DISTINCT type | 20.17 | 47.78 | 62.64 | 39.31 | 411.07 | 5.28 |

(ms; mark is slater vs the field. LadybugDB now wins group-by/DISTINCT outright, so
slater no longer earns the 🟢 there.)

| resident memory | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| **peak RSS** | 197 MiB | 1083 MiB | 358 MiB | 455 MiB | 1631 MiB | **121 MiB 🟢** |
| steady RSS | 191 MiB | 1082 MiB | 356 MiB | 453 MiB | 1622 MiB | **121 MiB 🟢** |

slater's footprint is **not** resident graph: idle RSS is ~16 MiB, individual queries
peak at 36–75 MiB; the 197 MiB **peak is transient per-query working memory** across
the suite (wide scans filling the 64 MiB block cache + traversal intermediates) that
the glibc allocator holds as a high-water mark. slater is **fastest on the count /
idx-eq / scan shapes** (its metadata + index fast paths — 10–40× the service engines
on the big graph), ties on the indexed 1-hop, and **trails on the unanchored 2-hop**
(it label-scans all 340k MeshTerm anchors). slater's `query.maxIntermediate` budget
(1M elements) rejects the unanchored all-relationship count used to warm caches — a
deliberate bounded-memory guard — so that priming step is best-effort and skipped.

The two new engines behave very differently:
- **LadybugDB** (embedded, Kùzu-derived, columnar) is the **smallest RSS by far —
  121 MiB** — and its columnar aggregation makes group-by and `count(DISTINCT)` the
  fastest of any engine (~5 ms). But it builds **no secondary range index** (only the
  primary key), so its property point-lookup is a scan (8.8 ms). Its single-label
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
| count all nodes | **0.56 ms 🟢** | 35.65 | 43.76 † | 44.08 † | 201.28 † | 2.34 |
| point lookup (idx wikidata_id) | 1.88 | 5.77 | 0.47 † | 0.47 † | 1.09 † | 7.59 |
| degree (1-hop count) | 1.95 | 11.16 | 0.79 † | 0.49 † | 1.09 † | 3.09 |
| 1-hop neighbours | 2.06 | 16.45 | 1.58 † | 0.48 † | 0.97 † | 3.26 |
| 2-hop | 2.17 | 27.44 | 2.03 † | 0.52 † | 2.71 † | 12.24 |
| 3-hop | 2.41 | 14.29 | 2.10 † | 0.68 † | 4.05 † | 131.38 |
| var-length *1..2 distinct | 2.22 | 191.15 | 547.65 † | 0.60 † | 1.17 † | 56.82 |
| shortestPath ≤6 | 2.21 | 6.00 | 2464.96 † | 20.98 † | 1.29 † | 79.32 |

(† Memgraph / FalkorDB / ArcadeDB are the **established** cross-engine figures — at this
scale reloading 13.8M edges into each via the native bulk path is the expensive step, so
they were not re-run in this isolated v0.8.0 pass. slater / Neo4j / LadybugDB are fresh.)

| resident memory | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| **peak RSS** | **~150 MiB 🟢** | ~2330 MiB | 2716 MiB † | 1506 MiB † | 2247 MiB † | ~774 MiB |
| steady RSS | **~145 MiB 🟢** | ~2300 MiB | 1918 MiB † | 1218 MiB † | 2222 MiB † | ~774 MiB |

This is where the bounded-memory thesis lands hardest: at 1M nodes / 13.8M edges,
**slater (~150 MiB) is the smallest of every engine — below even embedded LadybugDB
(~774 MiB)**, while the resident in-memory servers hold **1.5–2.7 GiB** (Neo4j ~2330; the established in-mem trio Memgraph 2716, ArcadeDB 2247, FalkorDB 1506). slater's footprint tracks the *query working
set*, not the graph — its `query.maxIntermediate` budget bounds the heaviest expansions,
so the peak stays ~150 MiB even on the var-length / shortestPath shapes. slater is
**sole-fastest on `count`** (its metadata fast path — 56–365× the service engines) and
competitive on the hop traversals (~2 ms). Standouts per engine:

- **shortestPath ≤6** between random (mostly disconnected) entities: slater's
  `ANY SHORTEST` global-visited BFS runs it in **2.2 ms** and ArcadeDB in 1.3 ms,
  while **Memgraph's `*BFS` expansion takes 2465 ms** (it explores to the depth bound),
  FalkorDB is ~21 ms and LadybugDB ~79 ms. (Per-engine syntax — see `bench_wiki.shortest_path`.)
- **var-length `*1..2` distinct**: FalkorDB 0.6 ms / ArcadeDB 1.2 ms / slater 2.2 ms,
  vs Neo4j 191 ms and Memgraph 548 ms.
- **FalkorDB** is the traversal-latency champion (sub-millisecond 1–3 hop) at a 1.5 GiB
  resident cost; **slater** is within ~3× on those hops at <½ the RAM.
- **LadybugDB** (~774 MiB) has no secondary range index, so its point lookup scans (7.6 ms)
  and its 3-hop (131 ms) feels the capped 512 MiB buffer pool.
- slater's `query.maxIntermediate` (1M) occasionally rejects a var-length expansion
  off a high-degree hub; `bench_wiki` records the median of the calls that succeed.

### Full Wikidata — 91,600,404 nodes / 766,504,024 edges (disk-bound, working set ≫ RAM)

The scale-up of the Wikidata-1M graph to the **complete** generation: 91.6M `:Entity`
nodes / 766M `:LINK` edges (~55× the 1M edge count), range index on `wikidata_id`,
**14 GB** on disk. (Source: the `wikidata_csr.duckdb` CSR dataset —
[download](https://huggingface.co/datasets/ladybugdb/wikidata-20260401/tree/main); the
DuckDB→Cypher-dump conversion scripts live under `perf/datasets/` in a checkout.) This is
the regime the whole bounded-memory thesis was built for —
the working set is far larger than the **15 GiB** host RAM, so traversals are genuinely
**disk-bound**, and **only the disk-backed engines can hold the graph at all**:

- **slater**, **Neo4j 5**, and **LadybugDB** (embedded, Kùzu-derived) load it — all page
  the store from disk.
- **FalkorDB** and **Memgraph** keep the whole graph resident in RAM (≈64–128 GiB at
  this scale) → **cannot-load** on a 15 GiB box.
- **ArcadeDB**'s importer runs for ~days at 766M edges → **skipped**.

LadybugDB *loads*, but only barely: its Kùzu `COPY` of the 766M edges is **not**
bounded-memory — a 4 GiB buffer pool fails (`buffer pool is full`), a 10 GiB pool is
**OOM-killed**, and only a tuned **~8 GiB** pool completes (peak **13 GiB** RSS, ~4.2 min),
right at the edge of the 15 GiB host. (slater's external builder builds the same graph
under a **4 GiB** cap — see *Bulk build / load*.)

> **Note (load cap):** we first capped LadybugDB's load at the **same 4 GiB** slater
> builds under, but its `COPY` was **OOM-killed ~38 s in** (barely past the node load),
> so the cap was **raised to 8 GiB** for it to complete. The load figures below are at
> that raised 8 GiB cap.

Because the on-disk store dwarfs RAM, the cgroup `memory.peak` is dominated by
reclaimable OS **page cache** and badly misrepresents an engine's own footprint. So the
resident figure here is the **anonymous** high-water (`memory.stat` `anon` — heap +
caches + query working memory), sampled live during each sweep. Each engine is benched
**in isolation** (one big container at a time on the 15 GiB box).

Latency, ms. slater = mean of 5 restart-cycle runs (very stable); Neo4j = its one clean
complete pass (see the instability note); LadybugDB = embedded, 512 MiB read pool. Mark
is **vs the field that loads it**: 🟢 sole-fastest, ⚪ tie (within 25%).

| query | slater | Neo4j 5 | LadybugDB | FalkorDB | Memgraph |
|---|--:|--:|--:|:--:|:--:|
| count all nodes | **0.58 🟢** | ~4000 | 34.2 | cannot-load | cannot-load |
| point lookup (idx wikidata_id) | **1.30 🟢** | 9.72 | ~2337 ‡ | cannot-load | cannot-load |
| degree (1-hop count) | **1.33 🟢** | 14.4 | 21.4 | cannot-load | cannot-load |
| 1-hop neighbours | **4.25 🟢** | 12.3 | 22.9 | cannot-load | cannot-load |
| 2-hop | 18.16 ⚪ | 17.4 | over-budget § | cannot-load | cannot-load |
| 3-hop | **26.66 🟢** | 74.9 | over-budget § | cannot-load | cannot-load |
| var-length *1..2 distinct | **9.09 🟢** | 116 † | n.m. § | cannot-load | cannot-load |
| shortestPath ≤6 | **52.6 🟢** (fanout 8) · 82.6 (fanout 1) | 131.9 | n.m. § | cannot-load | cannot-load |

† Neo4j's `var-length *1..2 distinct` completes at 116 ms only on the first warm pass;
on every repeat it **OOMs its 2 GiB heap** (`MemoryPoolOutOfMemoryError` after ~46 s),
and the restart-cycle eventually crashed the container outright. slater answers the same
query in 9 ms within its 1M-element `maxIntermediate` budget — bounded by design.
‡ LadybugDB builds **no secondary index** on `wikidata_id`, so the point lookup is a full
columnar scan of 91.6M nodes (~2.3 s); the cheaper degree/1-hop shapes reuse the
now-cached `wikidata_id` column. § At the **512 MiB read pool** the multi-hop /
var-length / shortestPath shapes exhaust the buffer pool on higher-degree anchors —
Kùzu's buffer manager rejects the allocation (`buffer pool is full`) rather than ballooning
unboundedly. **They are pool-bound, not impossible: raise the read pool and they complete**
— see the larger-pool table below. The point lookup is the exception (structural — a
secondary-index-less scan that a bigger pool does *not* fix).

| anon RSS | slater | Neo4j 5 |
|---|--:|--:|
| idle | **71 MiB** | 1344 MiB |
| shortestPath sweep | **700 MiB (fanout 1) · 925 MiB (fanout 8)** | 2911 MiB |

(Embedded **LadybugDB** has no server cgroup; its `ru_maxrss` on the count/point/degree/
1-hop shapes is **652 MiB** with the 512 MiB read pool. The heavier shapes need a larger
read pool, below.)

**LadybugDB at a larger read pool** (separate experiment — NOT comparable to the 512 MiB
column above; the graph was rebuilt from `perf/datasets/wikidata_csr.duckdb` and each
shape run in a `--memory=13g` container with a per-call timeout). This isolates whether
the `§` shapes are a fundamental limit or just the deliberately-small default pool:

| shape | 512 MiB pool | ≥2 GiB read pool |
|---|---|--:|
| 2-hop | over-budget § | **33 ms** (5/5 calls) |
| 3-hop | over-budget § | **508 ms** (5/5) |
| var-length *1..2 distinct | `buffer pool full` 3/5 § | **220–250 ms** (5/5 at 2/4/8 GiB) |
| shortestPath ≤6 | n.m. § | **~2.0 s** (4/4 @ 4 GiB) |
| point lookup (idx wikidata_id) | ~2.3 s ‡ | ~2.1 s (unchanged) |

So the `§` shapes are **pool-bound, not unbounded** — at the default 512 MiB read pool
LadybugDB's buffer manager rejects the allocation, but a **≥2 GiB** pool clears it and they
complete (var-length goes 3/5 → 5/5). The point lookup stays ~2 s regardless — that one is
structural (no secondary index on `wikidata_id`, so a full columnar scan), and more pool
doesn't fix it. (Caveat: these anchors are sampled from one `wikidata_id` region, so they
skew lower-degree/closer than a worst-case hub pair — the *direction* is robust, the exact
ms are typical-anchor, not adversarial. Contrast slater, whose `query.maxIntermediate`
bounds the same shapes within a fixed budget at **~9–53 ms** without a pool to size.)

This is the thesis at full strength: serving a **766M-edge** graph, slater's resident
footprint stays **sub-GiB** (idle 71 MiB) and tracks the *query working set*, not the
graph — while Neo4j sits at a committed **~2.9 GiB** (its 2 GiB heap + off-heap buffers,
the same ~2.87 GiB on the fast-query sweep) regardless of query, and even **idle** holds
**1.3 GiB** of committed JVM heap (~19× slater's 71 MiB). The two in-memory engines can't
load the graph at all. slater is **sole-fastest on count / point / degree
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

**Per-query parallelism + count-pushdown on the full graph** (the `maxFanout` track).
The uncapped multi-hop `RETURN count(*)` shapes now **count during expansion instead of
materialising the result rows** (the count-pushdown executor change), so their memory is
decoupled from result size. Isolated A/B on the 91.6M graph, **same hub anchors**,
`maxIntermediate=20M`, v0.8.0 vs current `main`:

| 3-hop count(*) | fanout=1 | fanout=8 |
|---|--:|--:|
| v0.8.0 — latency / peak anon | 955 ms / **7.7 GiB** | 617 ms / **9.5 GiB** |
| current — latency / peak anon | **554 ms / 0.66 GiB** | **298 ms / 1.9 GiB** |

The old ~5.2 GiB "worker-frontier" high-water is **gone**: the count holds O(1) rows, so the
fanout=8 residual (~1.9 GiB) is the **parallel adjacency-read buffers**, not the row set —
and latency drops too (no per-row alloc/flatten). Charging is unchanged, so a count over a
mega-hub still trips `maxIntermediate` on *adjacency reads* (compute), bounded as before.

Per-query parallelism still helps the cold disk-bound shapes — shortestPath ≤6 (307→76 ms in
this fresh run), 3-hop count (547→298 ms) — at the cost of more transient worker memory;
`maxFanout=1` is the throughput default. Full fresh slater-only numbers (both fanouts, every
dataset) are in [`perf/PERF_CURRENT_STATUS.md`](../PERF_CURRENT_STATUS.md).

**Bulk build / load** — the irony this whole effort resolved: slater can now *build* a
graph larger than RAM (the in-memory builder was OOM-killed on this one).

| engine | on-disk | builder / loader | build cost |
|---|--:|---|---|
| slater | **14 GB** | `slater-build` (spill-to-disk, `--max-memory 4g`) | ~25 min serial · **~18 min** with parallel pass-1, peak **3.6 GiB** RSS under a **4 GiB** cap |
| Neo4j 5 | 41.8 GB | `neo4j-admin database import` (offline → volume) | not re-timed (focus is RAM) |
| LadybugDB | 21 GB | Kùzu `COPY FROM` CSV (embedded) | ~4.2 min, peak **13 GiB** RSS (needs a tuned ~8 GiB pool) |

slater's external builder spills to disk and stays under a 4 GiB `--max-memory` cap to
build the 91.6M/766M generation in ~25 min — or **~17.5 min** with the v0.5.2 parallel
pass-1 (`SLATER_PARALLEL_PASS1=1`, which fans the dump-ingestion phase across all cores,
~15× on this box, while peak RSS stays under the same 4 GiB cap at 3.6 GiB). The
resulting generation is the **smallest on disk** (14 GB vs LadybugDB 21 GB, Neo4j
41.8 GB). The contrast is the whole point: slater **builds** the larger-than-RAM graph
within a fixed 4 GiB budget, whereas LadybugDB's bulk `COPY` only fits in a narrow ~8 GiB
window and peaks at 13 GiB — bounded-memory by construction vs. by luck.

(Both build figures exclude the CSV→Cypher transcode, which is benchmark setup, not part
of the build; they are `slater-build` reading a pre-materialised dump. Parallel pass-1 was
verified to produce the identical generation — 91,600,404 nodes / 766,504,024 edges.)

### EU-AI-Act — 20,766 nodes / 44,790 edges, 54.8 MiB of vectors

Six engines, each measured in isolation. **ArcadeDB has no kNN procedure**, so it runs
only the non-vector subset (kNN rows = `—`). LadybugDB serves kNN from its native Kùzu
HNSW index.

| query | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| kNN top-10 Concept | 3.13 ms | 8.55 ms | 1.85 ms | 1.23 ms | — | 2.80 ms |
| kNN top-50 Concept | 3.35 ms | 9.47 ms | 2.11 ms | 1.39 ms | — | 2.81 ms |
| kNN top-10 Chunk | 2.20 ms | 5.71 ms | 1.93 ms | 1.48 ms | — | 3.18 ms |
| kNN-10 + 1-hop expand | 2.71 ms | 6.29 ms | 1.82 ms | 1.27 ms | — | 9.23 ms |
| count all nodes | **0.57 ms 🟢** | 2.85 ms | 1.32 ms | 1.42 ms | 10.15 ms | 1.76 ms |
| Concept label count | **0.57 ms 🟢** | 2.93 ms | 1.55 ms | 1.01 ms | 0.83 ms | 1.28 ms |
| point lookup (idx id) | 1.07 ms | 4.16 ms | 0.46 ms | 0.48 ms | 0.81 ms | 1.04 ms |

| resident memory | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| **peak RSS** | **156 MiB 🟢** | 729 MiB | 229 MiB | 312 MiB | 1948 MiB | 286 MiB |
| steady RSS | ~156 MiB 🟢 | 725 MiB | 226 MiB | 283 MiB | 1944 MiB | 286 MiB |

(slater still sole-smallest — the next engine, Memgraph at 229 MiB, is ~1.5× larger.
LadybugDB's 286 MiB is its embedded process `ru_maxrss`, not a server cgroup — see the
consistency note in *Method*; it includes the Python driver + query-vector pool, so it
isn't strictly comparable to the server cgroup figures.)

slater serves the whole graph at **156 MiB — still the smallest of the six.** It answers
kNN with an **exact brute-force scan** (these indexes are below its 50k-vector ANN
threshold), where the others use an approximate resident HNSW — so slater's results are
exact (recall 1.0) where theirs are approximate. Two optimisations (v0.9.x) closed most of
the old gap to the HNSW field:

- a **SIMD distance kernel** (`wide::f32x8` dot, f32 accumulation, hoisted query norm,
  bounded top-k heap), and
- a **resident, pre-normalised vector matrix** — each index group is decoded + unit-
  normalised **once** into a contiguous buffer in the vector-index pool, so a query scans
  resident memory with no per-query gather/allocation (cosine collapses to a single dot).

Together these took Concept top-10 from **~23 ms → ~3.1 ms (~7×)** and Chunk from
**~10 ms → ~2.2 ms**, so slater now **beats Neo4j and LadybugDB on every kNN shape** and is
**within ~1.4× of Memgraph**, trailing only FalkorDB — while staying exact. The residual
gap to FalkorDB/Memgraph is the floor of an exact, memory-bandwidth-bound scan; closing it
further needs quantisation (the M7 Vamana/PQ path), which trades exactness.

The resident matrix is the **speed-for-memory dial**. It lives in `vectorCacheBytes`
(default raised 32 → 64 MiB so the 54.8 MiB estate fits), which lifts peak RSS from the
pre-optimisation **119 MiB → 156 MiB** — still the smallest of the six. It is strictly
budget-gated: if a group does not fit, kNN transparently falls back to the gather path
(no bound is ever exceeded). Keeping `vectorCacheBytes` at the old 32 MiB reverts to the
119 MiB footprint at the SIMD-kernel-only speed (Concept ~10 ms). The opposite dial —
sizing the budget *below* the vector set to force paging — is shown in the sweep next.

### Cache-budget sweep (slater only) — and the right knob

The residency lever is now **`vectorCacheBytes`**: when a cosine group fits, it is held
as the resident, pre-normalised matrix in the `VectorIndexCache` and kNN scans it directly
(the figures in the table above). Sizing `vectorCacheBytes` below a group disables that
path for it, and kNN falls back to reading full-precision vectors **through the block
cache** per query (`exec.rs` `vector_group`; the test
`vector_knn_reads_route_through_the_block_cache` asserts the fallback path). In that
fallback regime `blockCacheBytes` becomes the secondary dial, and sweeping it below the
group sizes (Concept 41 MiB, Chunk 18 MiB) shows the bounded-memory cliff:

| block cache (matrix off) | Concept kNN-10 | Chunk kNN-10 | note |
|---:|---:|---:|---|
| 64 MiB | 16.9 ms | 8.6 ms | both groups resident in the block LRU |
| 48 MiB | 16.4 ms | 8.2 ms | Concept (41 MiB) still fits |
| 40 MiB | 43.6 ms | 8.3 ms | **Concept evicts → re-read/decompress each scan** |
| 24 MiB | 43.0 ms | 8.2 ms | Concept thrashes, Chunk (18 MiB) still fits |
| 16 MiB | 42.8 ms | 22.3 ms | **Chunk now evicts too** |

(These absolute numbers predate the SIMD kernel — they characterise the *shape* of the
fallback path, not current latency; with the SIMD kernel the gather path is ~2.3× faster,
e.g. Concept ~10 ms at the 64 MiB block budget. The cliff still lands **exactly at each
group's working-set size**.) This **is** the bounded-memory dial: shrink the budget below a
vector group and slater keeps serving, paying re-fetch+re-decompress cost per query while
RSS falls — the in-memory engines have no equivalent. (Caveat: the 54.8 MiB file stays in
the host OS page cache, so the degradation is re-decompression cost, not physical-disk I/O;
a true disk-bound measurement needs a graph larger than RAM.)

So: a vector deployment wanting the fast path sizes `vectorCacheBytes` ≥ its largest cosine
group (the resident matrix, isolated from graph blocks in the `VectorIndexCache`); one
trading latency for a smaller footprint shrinks `vectorCacheBytes` below the group (gather
fallback) and, if it wants to shrink further, `blockCacheBytes` below the group too. The
M7 Vamana/PQ path will add a third regime: PQ codes (~32× smaller than full f32) resident
in the same pool, navigated by an ANN search instead of an exact scan.

MeSH (no vectors), block cache 64 → 256 → 512 MiB: latencies and RSS **flat** — its
hot blocks already fit in the default 64 MiB, so a bigger budget changes nothing.
(MeSH's peak RSS is **not** block-cache-resident data — idle is ~16 MiB; the ~260
MiB peak is transient per-query working memory the allocator retains, per the MeSH
section above — which is exactly why the block budget doesn't move it.)

## Files

- `load_cypher.py` — primitive-Cypher → Neo4j/Memgraph/FalkorDB (quote-aware parser,
  UNWIND batches, per-engine vector DDL).
- `load_arcadedb.py` — schema-first inheritance loader for ArcadeDB (super-type +
  `EXTENDS` sub-types, `__dump_id__` join; DDL over HTTP/SQL, data over Bolt). Falls back
  to a synthetic `__DumpNode__` super-type when no single label spans every node (pole).
- `load_ladybug.py` — loader for embedded LadybugDB (single primary table per node,
  `__labels` string, type inference + coercion, Kùzu `COPY` for rels and for
  embedding-bearing node tables, type-aware `CAST` on per-row rel props, and a native
  HNSW `CREATE_VECTOR_INDEX` per declared vector index); runs inside `Dockerfile.ladybug`
  (which bakes in the Kùzu `vector` extension so the query side only needs `LOAD`).
- `load_ladybug_csv.py` — header-less-CSV `COPY` loader for LadybugDB at full-Wikidata
  scale (too large to parse the Cypher dump in memory; `LADYBUG_LOAD_BUFFER_POOL` sizes
  the bulk pool).
- `bulk_export.py` — emit `nodes.csv` + `edges.csv` (RFC4180-quoted) from a
  single-label/single-rel-type dump, for the native bulk importers (Wikidata-1M).
- `bench_mesh.py` / `bench_vec.py` / `bench_wiki.py` / `bench_pole.py` — the query suites
  (`engines.py` = shared connection layer; dedicated `-hs` ports 7700/7701/7702/6401,
  ArcadeDB 7703; LadybugDB is embedded, no port). `bench_pole.py` runs the 10 pole shapes
  across all six engines (the original `perf/cross-engine/` run is servers-only).
- `bench_with_rss.py` — runs a bench in-process and records peak RSS (LadybugDB).
- `setup_hs.sh` / `run_bench_hs.sh` / `count_hs.py` / `aggregate_hs.py` — orchestration.
