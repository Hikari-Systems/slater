# perf/cross-engine — slater vs Neo4j / Memgraph / FalkorDB

The harness behind the four-engine comparison table in `../PERF_PROGRESS.md`
("Cross-engine comparison"), the README, and DOCKERHUB. Same pole graph, same
queries, run against four engines; **mean of 5 runs, each engine restarted before
every run** (cold start).

> For the **same pole shapes across all six engines** (this four plus ArcadeDB and
> LadybugDB), see the "Pole — all six engines" table in `../cross-engine-hs/README.md`
> (run via `bench_pole.py` through the six-engine `-hs` harness).

Environment used (host ports; adjust to taste):

| engine | container | endpoint | auth |
|--------|-----------|----------|------|
| slater | `slater-pole` | `bolt://localhost:7687` (db `pole`) | `reporting` / `polereader` |
| Neo4j 5 | `pole-neo4j` | `bolt://localhost:7688` | `neo4j` / `polepole12` |
| Memgraph | `pole-memgraph` | `bolt://localhost:7689` | none |
| FalkorDB | `pole-falkordb` | `localhost:<port>` (RESP), graph `pole` | none |

FalkorDB's host port is read from `/tmp/falkor_port`. Memgraph/FalkorDB persist to
volumes (`CREATE SNAPSHOT` / `SAVE`) so a container restart recovers the data.

```bash
# clients
python3 -m venv .venv && .venv/bin/pip install neo4j falkordb

# stand up Memgraph + FalkorDB (Neo4j + slater assumed already running with pole data)
docker run -d --name pole-memgraph -p 7689:7687 -v pole_memgraph:/var/lib/memgraph \
  memgraph/memgraph:latest --data-recovery-on-startup=true --storage-snapshot-on-exit=true
docker run -d --name pole-falkordb -p 6390:6379 -v pole_falkordb:/data falkordb/falkordb:latest
echo 6390 > /tmp/falkor_port

# load the graph into each from the running Neo4j, then snapshot/SAVE
.venv/bin/python load_graph.py memgraph
.venv/bin/python load_graph.py falkordb

# 5 runs/engine, restart between each, then average
bash run_bench.sh
.venv/bin/python aggregate.py
```

- `load_graph.py <memgraph|falkordb>` — reads (nodes, rels) from the running Neo4j
  and bulk-loads via UNWIND batches; recreates the 12 label/property indexes;
  strips the temp `_N`/`_id` join keys.
- `bench_one.py <engine>` — one engine's suite: 3 warm-ups + 25 measured per query,
  prints `{query: median_ms}` JSON. Bolt engines via the neo4j driver; FalkorDB via
  the `falkordb` client.
- `count.py <engine>` — readiness gate (prints node count or `NA`).
- `run_bench.sh` — the 5×-with-restart loop; writes `results/<engine>.run<N>.json`
  and `results/memory.txt` (cgroup peak/current RSS).
- `aggregate.py` — means the 5 runs, prints the latency table, the slater-vs-each
  25%-parity verdicts, and the memory summary.

These scripts target the specific local setup above (ports, container names,
credentials) — they are a record of how the published numbers were produced, not a
turnkey tool.
