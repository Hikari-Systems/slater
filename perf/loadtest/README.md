# Slater load-testing suite

A concurrency stress + brown-out suite for the slater read service, built on
**Locust** (the standard Python load tool) driving the official **neo4j** Bolt
driver — the same driver `perf/bench.py` uses. It stresses each potentially
fragile area, and an automated **coordinator** ramps load until the service
brown-outs and names the limiter (memory / CPU / connection-cap / query-budget /
message).

Unlike `perf/bench.py` (single-query latency on an idle server), this suite
drives *many concurrent* connections and queries and correlates client-side
latency/throughput with **server-reported health** read live over Bolt.

## How server health is exposed

Slater speaks Bolt, not HTTP, so there is no `/metrics` endpoint. Instead a gated
introspection statement reports health:

```cypher
CALL slater.diagnostics()
```

It returns a `metric` / `value` table: process **RSS** and **CPU**, the **cgroup
memory & CPU limits** (so a report can name the limiter), connection-cap
**headroom and rejections**, per-reason **query-failure** tallies, and **latency
percentiles**. It is **disabled by default** and errors unless the server is
started with:

```json
{ "loadTestDiagnostics": true }
```

When disabled, the extra instrumentation is inert (a single predictable branch
per call site) so the normal hot path is unchanged — never enable it on a
production replica. The same snapshot is available from the CLI:

```bash
slater diagnostics 127.0.0.1 7687 reporting <password>   # prints JSON
```

## Setup

```bash
python3 -m venv .venv && .venv/bin/pip install -r perf/loadtest/requirements.txt
```

Have a graph loaded (the suite defaults to the `pole` crime graph used elsewhere
in `perf/`; schema-specific query shapes are auto-skipped on other graphs, and
schema-agnostic shapes still run). Start slater with diagnostics on, ideally
under a **cgroup limit** so RSS/CPU have a ceiling to hit — e.g.:

```bash
systemd-run --user --scope -p MemoryMax=256M -p CPUQuota=200% \
  ./target/release/slater          # with loadTestDiagnostics: true in config.json
```

## Run a single scenario (raw Locust)

```bash
SLATER_PASS=polereader SLATER_SCENARIO=cpu_fanout \
  .venv/bin/locust -f perf/loadtest/locustfile.py --headless \
    -u 500 -r 50 -t 60s --host bolt://127.0.0.1:7687 \
    --csv perf/loadtest/out/cpu_fanout
```

Locust writes its standard CSV/HTML stats; read them as usual.

## Run the automated brown-out coordinator

```bash
.venv/bin/python perf/loadtest/coordinator.py --scenario mem_cache_churn \
  --pass polereader --users 100,250,500,1000,2000 --step 45
```

It ramps through the user counts, snapshots diagnostics under load at each step,
and prints a ramp table plus a verdict:

```
 users     rps    p50      p99  fail%    rss inflight rejects  status
   100     820     12m      45m   0.0%   120M        2       0  OK
   500    3100     28m      98m   0.0%   180M        9       0  OK
  1000    3400     90m     410m   1.4%   240M       31       0  ⚠ KNEE
------------------------------------------------------------------
BROWN-OUT at ~1000 concurrent users
Limiter: memory (RSS 240M ≈ cgroup limit 256M)
  - RSS 240MB > 90% of cgroup limit 256MB
  - p99 410ms > 4.0x baseline 45ms
```

## Scenarios (one per fragile area)

| Scenario | Targets | What it stresses |
|---|---|---|
| `mixed_soak` | overall | balanced SLA mix; baseline + slow degradation/leaks |
| `cpu_fanout` | cpu | heavy multi-hop/agg; run with `query.maxFanout > 1` |
| `mem_cache_churn` | memory | wide cold scans → block-cache eviction storm, RSS↑ |
| `intermediate_breach` | query-budget | large UNWIND/collect → `fail_budget` climbs |
| `deadline_storm` | query-budget | deep var-length paths → `fail_deadline`, blocking pool |
| `fat_message` | message | large IN-list params near `maxMessageBytes` |
| `shortest_path_bfs` | memory | shortestPath BFS (unbounded when explore cap = 0) |
| `conn_flood` | connection-cap | user≈connection count → `maxConnections`/per-IP caps |
| `pre_auth_loris` | connection-cap | open-then-stall sockets → pre-auth budget, login deadline |

See `scenarios.py` for the exact query mixes and the diagnostics metrics each one
tells you to watch.

## Files

- `queries.py` — query shapes mapped to fragile areas + pool derivation.
- `scenarios.py` — named scenario registry (query mix + watch list).
- `locustfile.py` — the custom Bolt `User` classes (query mix + slow-loris).
- `coordinator.py` — ramp driver + brown-out knee detection + limiter attribution.

## Notes / limits

- The global `maxConnections` cap is **backpressure**, not a countable rejection
  (the server reserves a permit before `accept()`), so `conn_flood` reports it via
  the `conn_in_use` / `conn_limit` gauges rather than a reject counter. Per-IP and
  pre-auth caps *are* counted.
- ACL / generation hot-reload under load is a real fragile area but needs build
  tooling to churn generations; it is left as a manual scenario, not automated
  here.
- For >~1000 users on one box, run Locust in
  [distributed mode](https://docs.locust.io/en/stable/running-distributed.html)
  (`--master` / `--worker`); the coordinator's per-step invocation can be pointed
  at a master instead.
