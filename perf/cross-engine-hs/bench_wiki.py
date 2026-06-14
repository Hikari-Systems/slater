#!/usr/bin/env python3
"""Bench one engine's uncached latency over the Wikidata-1M query suite.

The graph is a single-label `:Entity` Wikidata subset (1,000,000 nodes /
13,826,895 `:LINK` edges, range index on `Entity.wikidata_id`) — the large
*traversal* case: point lookups, 1-3 hop expansions, bounded variable-length, and a
best-effort shortestPath (syntax is per-engine; unsupported engines record null).

Usage: bench_wiki.py <slater|neo4j|memgraph|falkordb|arcadedb|ladybug>
Prints JSON {query_name: median_ms}. Params vary every call (no result-cache hit).
At 1M scale an unbounded multi-hop expansion can exceed slater's query.maxIntermediate
budget (a deliberate bounded-memory guard) — that query then records null, not a crash.
"""
import sys, os, json, time, statistics as st
from engines import connect

ENGINE = sys.argv[1]
GRAPH = os.environ.get("WIKI_GRAPH", "wikidata1m")  # "wikidatafull" for the full 91.6M graph
WARMUP, MEAS = 8, 20


def shortest_path():
    """Bounded shortestPath between two sampled entities. The syntax genuinely
    diverges across engines (this is the gap that motivated slater's MATCH-position
    shortestPath support): slater/Neo4j/ArcadeDB take `MATCH p=shortestPath(...)`;
    Memgraph needs its `*BFS` expansion; FalkorDB only allows shortestPath in
    WITH/RETURN; LadybugDB/Kùzu uses `SHORTEST` in the rel pattern."""
    if ENGINE == "ladybug":
        return ("MATCH (a:Entity {wikidata_id:$x})-[:LINK* SHORTEST 1..6]->(b:Entity {wikidata_id:$y}) "
                "RETURN count(*) AS c")
    if ENGINE == "memgraph":  # Memgraph breadth-first shortest-path expansion
        return ("MATCH (a:Entity {wikidata_id:$x})-[e:LINK *BFS 1..6]->(b:Entity {wikidata_id:$y}) "
                "RETURN size(e) AS c")
    if ENGINE == "falkordb":  # FalkorDB: shortestPath only in WITH/RETURN, not MATCH
        return ("MATCH (a:Entity {wikidata_id:$x}), (b:Entity {wikidata_id:$y}) "
                "RETURN length(shortestPath((a)-[:LINK*..6]->(b))) AS c")
    # MATCH p=shortestPath(...) — slater / Neo4j / ArcadeDB
    return ("MATCH p=shortestPath((a:Entity {wikidata_id:$x})-[:LINK*..6]->(b:Entity {wikidata_id:$y})) "
            "RETURN length(p) AS c")


# (name, query, param_fn(i, pools)) — identical text across engines (bar shortestPath).
SUITE = [
    ("count all nodes", "MATCH (n) RETURN count(n) AS c, $k AS k", lambda i, p: {"k": i}),
    ("point lookup (idx wikidata_id)", "MATCH (e:Entity {wikidata_id:$x}) RETURN e.name",
        lambda i, p: {"x": p["ids"][i % len(p["ids"])]}),
    ("degree (1-hop count)", "MATCH (e:Entity {wikidata_id:$x})-[:LINK]->(m) RETURN count(m)",
        lambda i, p: {"x": p["ids"][i % len(p["ids"])]}),
    ("1-hop neighbours", "MATCH (e:Entity {wikidata_id:$x})-[:LINK]->(m) RETURN m.name LIMIT 100",
        lambda i, p: {"x": p["ids"][i % len(p["ids"])]}),
    ("2-hop", "MATCH (e:Entity {wikidata_id:$x})-[:LINK]->()-[:LINK]->(m) RETURN m.wikidata_id AS w LIMIT 100",
        lambda i, p: {"x": p["ids"][i % len(p["ids"])]}),
    ("3-hop", "MATCH (e:Entity {wikidata_id:$x})-[:LINK]->()-[:LINK]->()-[:LINK]->(m) RETURN m.wikidata_id AS w LIMIT 100",
        lambda i, p: {"x": p["ids"][i % len(p["ids"])]}),
    ("var-length *1..2 distinct", "MATCH (e:Entity {wikidata_id:$x})-[:LINK*1..2]->(m) RETURN count(DISTINCT m) AS c",
        lambda i, p: {"x": p["ids"][i % len(p["ids"])]}),
    ("shortestPath <=6", shortest_path(),
        lambda i, p: {"x": p["ids"][i % len(p["ids"])], "y": p["ids"][(i + 7) % len(p["ids"])]}),
]

# Optionally drop queries by name (';'-separated) — at full-Wikidata scale a
# shortestPath between random *disconnected* entities legitimately explores the
# giant component's whole k-hop neighbourhood (O(V+E), minutes/call), so it is
# measured separately rather than run 20× per restart-cycle. e.g.
# BENCH_SKIP="shortestPath <=6".
_SKIP = {s for s in os.environ.get("BENCH_SKIP", "").split(";") if s}
if _SKIP:
    SUITE = [s for s in SUITE if s[0] not in _SKIP]

run, rows, close = connect(ENGINE, GRAPH)


def pools():
    ids = [r[0] for r in rows("MATCH (e:Entity) WHERE e.wikidata_id IS NOT NULL "
                              "RETURN e.wikidata_id AS w LIMIT 300")]
    return {"ids": ids}


P = pools()
# Prime page caches after the cold restart (best-effort; large counts may hit a budget).
for _ in range(5):
    for warm in ("MATCH (n) RETURN count(n)", "MATCH (e:Entity {wikidata_id:$x}) RETURN e.name"):
        try:
            run(warm, {"x": P["ids"][0]} if "$x" in warm else {})
        except Exception:
            pass
out = {}
for name, q, pf in SUITE:
    # Per-call resilience: at 1M scale a high-degree hub can trip slater's
    # query.maxIntermediate budget on *some* params; record the median of the calls
    # that succeed (with a failure count) rather than nulling the whole query. null
    # only if every call fails (genuinely unsupported syntax or always-over-budget).
    for i in range(WARMUP):
        try:
            run(q, pf(i, P))
        except Exception:
            pass
    ts, fails, last = [], 0, ""
    for i in range(MEAS):
        try:
            a = time.perf_counter(); run(q, pf(WARMUP + i, P)); ts.append((time.perf_counter() - a) * 1000)
        except Exception as e:
            fails += 1; last = str(e)
    out[name] = round(st.median(ts), 3) if ts else None
    if fails:
        print(f"  {ENGINE} {name!r}: {fails}/{MEAS} calls failed: {last[:120]}", file=sys.stderr)
close()
print(json.dumps(out))
