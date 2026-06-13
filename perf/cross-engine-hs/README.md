# cross-engine-hs — larger-graph benchmark (MeSH + EU-AI-Act)

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

- **MeSH** — the large *pure-graph* case (5× the pole edge count): counts,
  point lookups, 1–3 hop traversals, group-by, `DISTINCT`, substring scan.
- **EU-AI-Act** — the *vector* case: 15,238 1024-dim embeddings (54.8 MiB; a
  Concept group of 41 MiB + a Chunk group of 18 MiB). Adds a kNN query suite.
  Note (see the cache sweep below): slater serves kNN today via an **exact
  brute-force** scan that reads full-precision vectors **through the block cache**
  (`blockCacheBytes`), *not* the vector cache (`vectorCacheBytes`, which backs the
  not-yet-active Vamana/PQ path). At the default 64 MiB block budget the 54.8 MiB
  of vectors fit and stay resident; the interesting behaviour is what happens when
  you size that budget *below* a vector group.

Engines are the same four as pole plus **LadybugDB**:
**slater / Neo4j 5 / Memgraph / FalkorDB / LadybugDB**.
Because Neo4j and Memgraph community editions serve one database at a time, the
two graphs are **two separate runs**, each a single graph across all five engines.

## Method

Identical to the pole harness (`perf/cross-engine/`): per query, 15 warm-ups then
25 measured calls with a **fresh parameter every call** (so the result cache always
misses — real execution cost), median of the 25; each engine is **restarted before
every run** and re-warmed, and the reported figure is the **mean of 5 such runs**.
Peak/steady RSS is read from the container cgroup after the final run for service
engines. LadybugDB is embedded in the benchmark Python process, so the run-5
wrapper records that process' high-water/current RSS. Row counts are cross-checked
across engines (slater matches the Neo4j/Memgraph reference on every query). slater
is served on its **default cache budgets — block 64 + vector 32 + result 16 MiB** —
the config under which "bounded memory" is the whole point.

The vector indexes use each engine's native procedure — slater/FalkorDB
`db.idx.vector.queryNodes`, Neo4j `db.index.vector.queryNodes`, Memgraph
`vector_search.search` — and a shared pool of real embeddings as query vectors
(slater keeps embeddings in its vector store, not as a readable property, so the
pool is sampled once from Neo4j and reused identically across engines). For MeSH a
single `:MeshTerm(type)` range index is added uniformly on every engine so `type`
(Drug/Organism/Disease/PharmacologicalAction) is the low-cardinality indexed analog
of pole's `Crime.type`.

```bash
python3 -m venv /tmp/pole_venv && /tmp/pole_venv/bin/pip install neo4j falkordb ladybug
SNAP=../../../hs-backend-spot/slater-snapshot/cypher
# MeSH
./setup_hs.sh   mesh      $SNAP/bioalphaengine-companies.cypher MeshTerm:type
./run_bench_hs.sh mesh    bench_mesh.py 340839
./aggregate_hs.py /tmp/bench-hs/results-mesh
# EU-AI-Act
./setup_hs.sh   eu_ai_act $SNAP/eu_ai_act.cypher
./run_bench_hs.sh eu_ai_act bench_vec.py 20766
./aggregate_hs.py /tmp/bench-hs/results-eu_ai_act
```

`setup_hs.sh` builds the slater generation (`slater-build`, stamping the ACL) and
loads Neo4j/Memgraph/FalkorDB from the same primitive-Cypher dump via
`load_cypher.py` (a quote-aware parser → UNWIND batches; FalkorDB needs `vecf32()`
on load and `efRuntime:256` on the vector index for full top-k recall). LadybugDB
is loaded by `load_ladybug.py`: it disables the default hash index, creates ART
indexes on generated `__dump_id__` primary keys, maps multi-label dumped nodes onto
a primary node table, and stores secondary labels for query rewrites.

## Results

The result tables below are the existing four-engine baseline. After rerunning
`setup_hs.sh` / `run_bench_hs.sh`, `aggregate_hs.py` now emits a fifth
**LadybugDB** latency column and RSS row.

### MeSH — 340,839 nodes / 469,438 edges (pure graph)

Latency, mean of 5 runs. Mark on slater: 🟢 sole fastest, ⚪ ties (within 25%).

| query | slater | Neo4j 5 | Memgraph | FalkorDB |
|---|--:|--:|--:|--:|
| count all nodes | **0.52 ms 🟢** | 13.62 ms | 22.02 ms | 14.84 ms |
| label count (Disease) | **0.52 ms 🟢** | 3.32 ms | 19.08 ms | 0.95 ms |
| point lookup (idx meshUi) | 1.77 ms | 4.03 ms | 0.45 ms | 0.44 ms |
| idx-eq count (MeshTerm.type) | **0.52 ms 🟢** | 4.67 ms | 4.36 ms | 1.46 ms |
| 1-hop type→BROADER_THAN | **1.24 ms ⚪** | 5.29 ms | 1.11 ms | 3.34 ms |
| 2-hop BROADER_THAN chain | 22.67 ms | 5.04 ms | 7.92 ms | 15.17 ms |
| group-by type | **19.27 ms 🟢** | 58.66 ms | 60.05 ms | 28.79 ms |
| 3-hop Drug/action/Drug | 1.47 ms | 3.74 ms | 5.35 ms | 0.95 ms |
| full-scan CONTAINS | **0.54 ms 🟢** | 5.04 ms | 22.48 ms | 1.55 ms |
| count DISTINCT type | **19.18 ms 🟢** | 55.69 ms | 58.38 ms | 36.70 ms |

| resident memory | slater | Neo4j 5 | Memgraph | FalkorDB |
|---|--:|--:|--:|--:|
| **peak RSS** | **262 MiB 🟢** | 1127 MiB | 350 MiB | 454 MiB |
| steady RSS | 211 MiB 🟢 | 1115 MiB | 328 MiB | 453 MiB |

slater is the **smallest RSS of the four** (🟢 sole-smallest, 4.3× below Neo4j).
Note its footprint is **not** resident graph: idle RSS is ~16 MiB, and individual
queries peak at 36–75 MiB; the 262 MiB **peak is transient per-query working memory**
across the suite (wide scans filling the 64 MiB block cache + traversal
intermediates) that the glibc allocator holds as a high-water mark rather than
returning to the OS — `MALLOC_TRIM_THRESHOLD_`/jemalloc trims it to ~218 MiB. (An
earlier draft reported 316 MiB; ~54 MiB of that was an artifact of the benchmark's
own `RETURN DISTINCT n.type` setup query materialising 340k rows — now fetched via
the indexed group-by fast path.) slater is **fastest of the four on the count /
group-by / DISTINCT / scan shapes**
(its metadata + index fast paths — often 10–40× the others on the big graph),
ties on the indexed 1-hop, and **trails on the unanchored 2-hop traversal** (it
label-scans all 340k MeshTerm anchors). It also keeps the **smallest RSS** of the
four — 4.3× below Neo4j.

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
- `load_ladybug.py` — primitive-Cypher → embedded LadybugDB (ART primary-key indexes,
  multi-label table mapping + metadata for rewrites).
- `bench_with_rss.py` — wrapper used for embedded LadybugDB run-5 RSS capture.
- `bench_mesh.py` / `bench_vec.py` — the two query suites (`engines.py` = shared
  connection layer; dedicated `-hs` ports 7700/7701/7702/6401).
- `setup_hs.sh` / `run_bench_hs.sh` / `count_hs.py` / `aggregate_hs.py` — orchestration.
