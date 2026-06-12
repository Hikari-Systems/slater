# perf/ — slater vs Neo4j performance work (pole dataset)

This directory holds a staged plan and harness to close slater's cold-execution
performance gap, measured on the "pole" Manchester crime graph (61,521 nodes /
105,840 rels).

- **`PERF_PROGRESS.md`** — START HERE. Cross-context tracker: status table, frozen
  baseline numbers, root causes (file:line), per-stage fix detail, and full
  build/serve/validate instructions. A fresh session reconstructs all state from
  this file alone.
- **`bench.py`** — the benchmark. Measures each query uncached (varying Bolt param,
  real execution) and cached (slater result-cache hit), with an optional Neo4j
  parity column.
- **`cross-engine/`** — the four-engine comparison (slater / Neo4j / Memgraph /
  FalkorDB), mean of 5 runs with a container restart before each. Produces the
  "Cross-engine comparison" table in `PERF_PROGRESS.md`. See `cross-engine/README.md`.
- **`cross-engine-hs/`** — the same four-engine method on two **larger**
  hs-backend-spot reference graphs: a 340,839-node / 469,438-edge MeSH graph (pure
  graph) and a 20,766-node EU-AI-Act graph with 54.8 MiB of 1024-dim embeddings
  (adds a kNN suite). This is where the bounded-memory claim is actually exercised —
  slater is the smallest RSS of the four on both. Includes a `blockCacheBytes` sweep
  showing the kNN latency/RSS tradeoff when vectors no longer fit the cache. See
  `cross-engine-hs/README.md`.

## Quick start

```bash
# driver (host Python is PEP-668 managed → use a venv)
python3 -m venv /tmp/pole_venv && /tmp/pole_venv/bin/pip install neo4j

# against a running slater (+ optional Neo4j for parity)
/tmp/pole_venv/bin/python perf/bench.py --slater-pass polereader \
  --neo4j-uri bolt://localhost:7688 --neo4j-pass polepole12
```

Build slater via `docker build -t slater:local .` (no host Rust toolchain).
Ingest, serve, and validate steps are in `PERF_PROGRESS.md`. Note: a current-source
server build only boots after **Stage 0** (the `requireAclStamp` config fix).
