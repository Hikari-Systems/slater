#!/usr/bin/env python3
"""Bench one engine's uncached latency over the MeSH query suite.

The pole 10-query shapes (count / label count / indexed point lookup / indexed-eq
count / 1–3 hop traversals / group-by / count(DISTINCT) / unindexed substring scan)
mapped onto the MeSH graph (340,839 nodes / 469,438 edges). `type` (Drug / Organism /
Disease / PharmacologicalAction) is given a range index at load time — uniformly on
every engine — so it is the low-cardinality indexed analog of pole's `Crime.type`.

Usage: bench_mesh.py <slater|neo4j|memgraph|falkordb>
Prints JSON {query_name: median_ms}. Params vary every call (no result-cache hit).
"""
import sys, json, time, statistics as st
from engines import connect

ENGINE = sys.argv[1]
GRAPH = "mesh"
WARMUP, MEAS = 15, 25

# scopeNote substring terms — generic medical words present in many MeSH scope notes.
TERMS = ["disease", "syndrome", "cells", "blood", "infection", "chronic", "acute",
         "tissue", "caused", "clinical", "genetic", "bacteria", "inflammation", "protein"]

# (name, query, param_fn(i, pools)) — identical text across engines.
SUITE = [
    ("count all nodes", "MATCH (n) RETURN count(n) AS c, $k AS k", lambda i, p: {"k": i}),
    ("Disease label count", "MATCH (n:Disease) RETURN count(n) AS c, $k AS k", lambda i, p: {"k": i}),
    ("point lookup (idx meshUi)", "MATCH (d:Drug {meshUi:$x}) RETURN d.canonicalName",
        lambda i, p: {"x": p["ui"][i % len(p["ui"])]}),
    ("idx-eq count (MeshTerm.type)", "MATCH (n:MeshTerm {type:$t}) RETURN count(n)",
        lambda i, p: {"t": p["types"][i % len(p["types"])]}),
    ("1-hop type->BROADER_THAN", "MATCH (n:MeshTerm {type:$t})-[:BROADER_THAN]->(m) RETURN m.canonicalName LIMIT 100",
        lambda i, p: {"t": p["types"][i % len(p["types"])]}),
    ("2-hop BROADER_THAN chain", "MATCH (a:MeshTerm)-[:BROADER_THAN]->(b)-[:BROADER_THAN]->(c) RETURN c.meshUi AS u, $k AS k LIMIT 100",
        lambda i, p: {"k": i}),
    ("group-by type", "MATCH (n:MeshTerm) RETURN n.type AS t, count(*) AS n, $k AS k ORDER BY n DESC LIMIT 10",
        lambda i, p: {"k": i}),
    ("3-hop Drug/action/Drug", "MATCH (d:Drug)-[:HAS_PHARMACOLOGICAL_ACTION]->(p)<-[:HAS_PHARMACOLOGICAL_ACTION]-(d2) RETURN d.meshUi, d2.meshUi, $k AS k LIMIT 100",
        lambda i, p: {"k": i}),
    ("full-scan CONTAINS", "MATCH (n:Disease) WHERE n.scopeNote CONTAINS $w RETURN count(n)",
        lambda i, p: {"w": TERMS[i % len(TERMS)]}),
    ("count DISTINCT type", "MATCH (n:MeshTerm) RETURN count(DISTINCT n.type) AS c, $k AS k", lambda i, p: {"k": i}),
]

run, rows, close = connect(ENGINE, GRAPH)


def pools():
    ui = [r[0] for r in rows("MATCH (d:Drug) WHERE d.meshUi IS NOT NULL RETURN d.meshUi LIMIT 200")]
    # Distinct `type` values via the indexed group-by fast path — NOT `RETURN
    # DISTINCT n.type`, which materialises all 340k rows and would dominate the
    # measured peak RSS (a setup-query artifact, not the workload's footprint).
    types = [r[0] for r in rows("MATCH (n:MeshTerm) RETURN n.type AS t, count(*) AS c ORDER BY c DESC")
             if r[0] is not None]
    return {"ui": ui, "types": types}


P = pools()
# Prime page caches after the cold restart (warm even the disk-backed engine).
# Best-effort: slater's query.maxIntermediate budget rejects the unanchored all-rel
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
        # e.g. slater rejecting a query that exceeds its query.maxIntermediate budget
        # (bounded-memory protection) — record None rather than killing the suite.
        out[name] = None
        print(f"  {ENGINE} query {name!r} failed: {str(e)[:140]}", file=sys.stderr)
close()
print(json.dumps(out))
