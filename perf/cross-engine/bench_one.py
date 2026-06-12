#!/usr/bin/env python3
"""Bench one engine's uncached latency over the pole query suite.

Usage: bench_one.py <slater|neo4j|memgraph|falkordb>
Prints JSON {query_name: median_ms} to stdout. Params vary every call so each
execution is real (no result-cache hit); medians over MEAS runs after WARMUP.
"""
import sys, json, time, statistics as st

ENGINE = sys.argv[1]
WARMUP, MEAS = 15, 25   # generous warm-up so a cold-restarted page cache (Neo4j) warms

OUTCOME_TERMS = ["suspect","investigation","complete","review","action","court",
    "caution","charged","unable","identified","resolved","prosecution","further","police"]

# (name, query, param_fn(i, pools)) — identical text across engines.
SUITE = [
    ("count all nodes", "MATCH (n) RETURN count(n) AS c, $k AS k", lambda i,p:{"k":i}),
    ("Crime label count", "MATCH (n:Crime) RETURN count(n) AS c, $k AS k", lambda i,p:{"k":i}),
    ("point lookup (idx nhs_no)", "MATCH (p:Person {nhs_no:$x}) RETURN p.name", lambda i,p:{"x":p["nhs"][i%len(p["nhs"])]}),
    ("idx-eq count (Crime.type)", "MATCH (c:Crime {type:$t}) RETURN count(c)", lambda i,p:{"t":p["types"][i%len(p["types"])]}),
    ("1-hop Crime->Location", "MATCH (c:Crime {type:$t})-[:OCCURRED_AT]->(l:Location) RETURN l.address LIMIT 100", lambda i,p:{"t":p["types"][i%len(p["types"])]}),
    ("2-hop Person->Loc->Area", "MATCH (p:Person)-[:CURRENT_ADDRESS]->(l:Location)-[:LOCATION_IN_AREA]->(a:Area) RETURN a.areaCode, $k AS k LIMIT 100", lambda i,p:{"k":i}),
    ("agg crimes by type", "MATCH (c:Crime) RETURN c.type AS t, count(*) AS n, $k AS k ORDER BY n DESC LIMIT 10", lambda i,p:{"k":i}),
    ("3-hop Officer/Crime/Loc", "MATCH (o:Officer)<-[:INVESTIGATED_BY]-(c:Crime)-[:OCCURRED_AT]->(l:Location) RETURN o.surname, l.postcode, $k AS k LIMIT 100", lambda i,p:{"k":i}),
    ("full-scan CONTAINS", "MATCH (c:Crime) WHERE c.last_outcome CONTAINS $w RETURN count(c)", lambda i,p:{"w":OUTCOME_TERMS[i%len(OUTCOME_TERMS)]}),
    ("count DISTINCT type", "MATCH (c:Crime) RETURN count(DISTINCT c.type) AS c, $k AS k", lambda i,p:{"k":i}),
]

# ---- per-engine connection + run(q, params) -> consume all rows -------------
if ENGINE == "falkordb":
    from falkordb import FalkorDB
    port = int(open("/tmp/falkor_port").read().strip())
    g = FalkorDB(host="localhost", port=port).select_graph("pole")
    def run(q, params): g.query(q, params).result_set
    def pools():
        types=[r[0] for r in g.query("MATCH (c:Crime) RETURN DISTINCT c.type").result_set if r[0] is not None]
        nhs=[r[0] for r in g.query("MATCH (p:Person) WHERE p.nhs_no IS NOT NULL RETURN p.nhs_no LIMIT 200").result_set]
        return {"types":types,"nhs":nhs}
    close=lambda:None
else:
    from neo4j import GraphDatabase
    cfg = {
        "slater":   ("bolt://localhost:7687", ("reporting","polereader"), "pole"),
        "neo4j":    ("bolt://localhost:7688", ("neo4j","polepole12"), "neo4j"),
        "memgraph": ("bolt://localhost:7689", ("",""), None),
    }[ENGINE]
    uri, auth, db = cfg
    drv = GraphDatabase.driver(uri, auth=auth)
    sess = drv.session(database=db) if db else drv.session()
    def run(q, params): list(sess.run(q, params))
    def pools():
        types=[r[0] for r in sess.run("MATCH (c:Crime) RETURN DISTINCT c.type") if r[0] is not None]
        nhs=[r[0] for r in sess.run("MATCH (p:Person) WHERE p.nhs_no IS NOT NULL RETURN p.nhs_no LIMIT 200")]
        return {"types":types,"nhs":nhs}
    close=lambda:(sess.close(), drv.close())

P = pools()
# Prime page caches after the cold restart: touch all nodes + all rels a few
# times so a disk-backed engine (Neo4j) reaches warm steady state before timing.
for _ in range(5):
    run("MATCH (n) RETURN count(n)", {})
    run("MATCH ()-[r]->() RETURN count(r)", {})
out = {}
for name, q, pf in SUITE:
    for i in range(WARMUP):
        run(q, pf(i, P))
    ts = []
    for i in range(MEAS):
        a = time.perf_counter(); run(q, pf(WARMUP+i, P)); ts.append((time.perf_counter()-a)*1000)
    out[name] = round(st.median(ts), 3)
close()
print(json.dumps(out))
