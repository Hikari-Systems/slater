#!/usr/bin/env python3
"""Bench one engine's uncached latency over the pole crime-graph query suite.

The original four-engine pole benchmark lives in `perf/cross-engine/` (self-contained,
servers only). This is the SAME 10 pole shapes (count / label count / indexed point
lookup / indexed-eq count / 1–3 hop traversals / group-by / count(DISTINCT) / unindexed
substring scan) run through the six-engine `-hs` harness, so LadybugDB (embedded) and
ArcadeDB get pole numbers too. `Crime.type` is given a range index at load time —
uniformly on every engine — as the low-cardinality indexed column (pass `Crime:type` to
setup_hs.sh). LadybugDB has only a primary-key index, so its `nhs_no` / `Crime.type`
lookups scan; its multi-label patterns are rewritten by `engines._ladybug_rewriter`.

Usage: bench_pole.py <slater|neo4j|memgraph|falkordb|arcadedb|ladybug>
Prints JSON {query_name: median_ms}. Params vary every call (no result-cache hit).
"""
import sys, json, time, statistics as st
from engines import connect

ENGINE = sys.argv[1]
GRAPH = "pole"
WARMUP, MEAS = 15, 25

# last_outcome substring terms — outcome phrases present across many pole crimes.
OUTCOME_TERMS = ["suspect", "investigation", "complete", "review", "action", "court",
    "caution", "charged", "unable", "identified", "resolved", "prosecution", "further", "police"]

# (name, query, param_fn(i, pools)) — identical text across engines.
SUITE = [
    ("count all nodes", "MATCH (n) RETURN count(n) AS c, $k AS k", lambda i, p: {"k": i}),
    ("Crime label count", "MATCH (n:Crime) RETURN count(n) AS c, $k AS k", lambda i, p: {"k": i}),
    ("point lookup (idx nhs_no)", "MATCH (p:Person {nhs_no:$x}) RETURN p.name",
        lambda i, p: {"x": p["nhs"][i % len(p["nhs"])]}),
    ("idx-eq count (Crime.type)", "MATCH (c:Crime {type:$t}) RETURN count(c)",
        lambda i, p: {"t": p["types"][i % len(p["types"])]}),
    ("1-hop Crime->Location", "MATCH (c:Crime {type:$t})-[:OCCURRED_AT]->(l:Location) RETURN l.address LIMIT 100",
        lambda i, p: {"t": p["types"][i % len(p["types"])]}),
    ("2-hop Person->Loc->Area", "MATCH (p:Person)-[:CURRENT_ADDRESS]->(l:Location)-[:LOCATION_IN_AREA]->(a:Area) RETURN a.areaCode, $k AS k LIMIT 100",
        lambda i, p: {"k": i}),
    ("agg crimes by type", "MATCH (c:Crime) RETURN c.type AS t, count(*) AS n, $k AS k ORDER BY n DESC LIMIT 10",
        lambda i, p: {"k": i}),
    ("3-hop Officer/Crime/Loc", "MATCH (o:Officer)<-[:INVESTIGATED_BY]-(c:Crime)-[:OCCURRED_AT]->(l:Location) RETURN o.surname, l.postcode, $k AS k LIMIT 100",
        lambda i, p: {"k": i}),
    ("full-scan CONTAINS", "MATCH (c:Crime) WHERE c.last_outcome CONTAINS $w RETURN count(c)",
        lambda i, p: {"w": OUTCOME_TERMS[i % len(OUTCOME_TERMS)]}),
    ("count DISTINCT type", "MATCH (c:Crime) RETURN count(DISTINCT c.type) AS c, $k AS k", lambda i, p: {"k": i}),
]

run, rows, close = connect(ENGINE, GRAPH)


def pools():
    nhs = [r[0] for r in rows("MATCH (p:Person) WHERE p.nhs_no IS NOT NULL RETURN p.nhs_no LIMIT 200")]
    # Distinct `Crime.type` via the indexed group-by fast path — NOT `RETURN DISTINCT`,
    # which materialises every Crime row and would dominate the measured peak RSS.
    types = [r[0] for r in rows("MATCH (c:Crime) RETURN c.type AS t, count(*) AS n ORDER BY n DESC")
             if r[0] is not None]
    return {"nhs": nhs, "types": types}


P = pools()
# Prime page caches after the cold restart (warm even the disk-backed engine).
# Best-effort: slater's query.maxIntermediate budget may reject the unanchored all-rel
# count (>1M intermediates), which must not abort the run before the suite.
for _ in range(5):
    for warm in ("MATCH (n) RETURN count(n)", "MATCH ()-[r]->() RETURN count(r)"):
        try:
            run(warm, {})
        except Exception:
            pass
out = {}
for name, q, pf in SUITE:
    try:
        for i in range(WARMUP):
            run(q, pf(i, P))
        ts = []
        for i in range(MEAS):
            a = time.perf_counter(); run(q, pf(WARMUP + i, P)); ts.append((time.perf_counter() - a) * 1000)
        out[name] = round(st.median(ts), 3)
    except Exception as e:
        out[name] = None
        print(f"  {ENGINE} query {name!r} failed: {str(e)[:140]}", file=sys.stderr)
close()
print(json.dumps(out))
