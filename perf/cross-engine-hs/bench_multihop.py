#!/usr/bin/env python3
"""Task 9 perf probe: uncapped multi-hop expansion (expand_chain_par).

The parallel breadth-first chain walk only engages for an *uncapped* fixed-length
chain (a pushed LIMIT routes to the sequential early-exit path) whose per-hop
frontier exceeds EXPAND_PAR_MIN (64). So this probe:
  * samples high-degree :Entity hubs (degree >= MIN_DEG) as the param pool, and
  * times count-based 2-hop / 3-hop expansions (no LIMIT) over them.

Run the same binary twice — config.fanout1.json (sequential expand_chain) vs
config.fanout8.json (parallel expand_chain_par) — for an apples-to-apples
fanout=1 vs fanout=8 comparison on one build. Prints JSON {query: median_ms}.

Usage: bench_multihop.py [slater]
"""
import sys, os, json, time, statistics as st
from engines import connect

ENGINE = sys.argv[1] if len(sys.argv) > 1 else "slater"
GRAPH = os.environ.get("WIKI_GRAPH", "wikidata")
WARMUP, MEAS = 5, 20
MIN_DEG = int(os.environ.get("MIN_DEG", "70"))   # > EXPAND_PAR_MIN so the pool parallelizes
POOL_N = int(os.environ.get("POOL_N", "40"))     # how many hubs to keep
SAMPLE_N = int(os.environ.get("SAMPLE_N", "1500"))  # candidates to scan for degree

run, rows, close = connect(ENGINE, GRAPH)


def hub_pool():
    """Sample SAMPLE_N entity ids, measure each 1-hop out-degree, keep the
    POOL_N highest with degree >= MIN_DEG (so level-1 frontier > EXPAND_PAR_MIN)."""
    ids = [r[0] for r in rows(
        "MATCH (e:Entity) WHERE e.wikidata_id IS NOT NULL "
        f"RETURN e.wikidata_id AS w LIMIT {SAMPLE_N}")]
    degs = []
    for w in ids:
        try:
            d = rows("MATCH (e:Entity {wikidata_id:$x})-[:LINK]->(m) RETURN count(m) AS d",
                     {"x": w})[0][0]
        except Exception:
            continue
        if d >= MIN_DEG:
            degs.append((d, w))
    degs.sort(reverse=True)
    pool = [w for _, w in degs[:POOL_N]]
    if not pool:
        raise SystemExit(f"no hubs with degree >= {MIN_DEG} in {SAMPLE_N} samples")
    print(f"  hubs: {len(pool)} (deg {degs[0][0]}..{degs[min(POOL_N, len(degs))-1][0]})",
          file=sys.stderr)
    return pool


SUITE = [
    # Uncapped: counts the whole 2-/3-hop neighbourhood, so the parallel walk reads
    # every frontier node's adjacency (no early-exit) — pure overlap, no wasted work.
    ("2-hop count", "MATCH (e:Entity {wikidata_id:$x})-[:LINK]->()-[:LINK]->(m) RETURN count(m) AS c"),
    ("3-hop count", "MATCH (e:Entity {wikidata_id:$x})-[:LINK]->()-[:LINK]->()-[:LINK]->(m) RETURN count(m) AS c"),
]

POOL = hub_pool()
out = {}
for name, q in SUITE:
    for i in range(WARMUP):
        try:
            run(q, {"x": POOL[i % len(POOL)]})
        except Exception:
            pass
    ts, fails, last = [], 0, ""
    for i in range(MEAS):
        x = POOL[(WARMUP + i) % len(POOL)]
        try:
            a = time.perf_counter(); run(q, {"x": x}); ts.append((time.perf_counter() - a) * 1000)
        except Exception as e:
            fails += 1; last = str(e)
    out[name] = round(st.median(ts), 3) if ts else None
    if fails:
        print(f"  {name!r}: {fails}/{MEAS} failed: {last[:120]}", file=sys.stderr)
close()
print(json.dumps(out))
