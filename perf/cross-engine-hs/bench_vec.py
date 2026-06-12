#!/usr/bin/env python3
"""Bench one engine's uncached kNN-vector latency over the eu_ai_act graph.

eu_ai_act (20,766 nodes / 44,790 edges) carries 15,238 1024-dim fp32 embeddings
(Concept + Chunk), 54.8 MiB of vectors — larger than slater's default 32 MiB vector
cache, so this is the run where "vectors don't all fit in cache" actually bites.

The metric is the same as the MeSH/pole suites — uncached median latency, fresh query
vector every call (no result-cache hit) — but the query type is approximate-NN search
via each engine's native vector procedure (slater/FalkorDB `db.idx.vector.queryNodes`,
Neo4j `db.index.vector.queryNodes`, Memgraph `vector_search.search`), plus a few graph
baselines and a hybrid kNN→1-hop expand. Query vectors are real embeddings sampled
from the graph, so a top hit is the node itself (score≈1) — a built-in index sanity check.

Usage: bench_vec.py <slater|neo4j|memgraph|falkordb>
Prints JSON {query_name: median_ms | null}.  A null = that engine rejected the query.
"""
import sys, json, time, statistics as st
from engines import connect

ENGINE = sys.argv[1]
GRAPH = "eu_ai_act"
WARMUP, MEAS = 10, 25


def knn(lab, k):
    """Per-engine kNN call returning node id; query vector bound as $q."""
    if ENGINE in ("slater", "falkordb"):
        return f"CALL db.idx.vector.queryNodes('{lab}','embedding',{k},vecf32($q)) YIELD node, score RETURN node.id AS id"
    if ENGINE == "neo4j":
        return f"CALL db.index.vector.queryNodes('{lab}_embedding',{k},$q) YIELD node, score RETURN node.id AS id"
    return f"CALL vector_search.search('{lab}_embedding',{k},$q) YIELD node RETURN node.id AS id"


def hybrid():
    """kNN top-10 over Concept, then expand 1 hop and count neighbours."""
    if ENGINE in ("slater", "falkordb"):
        head = "CALL db.idx.vector.queryNodes('Concept','embedding',10,vecf32($q)) YIELD node"
    elif ENGINE == "neo4j":
        head = "CALL db.index.vector.queryNodes('Concept_embedding',10,$q) YIELD node"
    else:
        head = "CALL vector_search.search('Concept_embedding',10,$q) YIELD node"
    return head + " MATCH (node)-[r]->(m) RETURN count(m) AS c"


# (name, query, param_fn(i, pools))
SUITE = [
    ("kNN top-10 Concept", knn("Concept", 10), lambda i, p: {"q": p["cvec"][i % len(p["cvec"])]}),
    ("kNN top-50 Concept", knn("Concept", 50), lambda i, p: {"q": p["cvec"][i % len(p["cvec"])]}),
    ("kNN top-10 Chunk", knn("Chunk", 10), lambda i, p: {"q": p["hvec"][i % len(p["hvec"])]}),
    ("kNN-10 + 1-hop expand", hybrid(), lambda i, p: {"q": p["cvec"][i % len(p["cvec"])]}),
    ("count all nodes", "MATCH (n) RETURN count(n) AS c, $k AS k", lambda i, p: {"k": i}),
    ("Concept label count", "MATCH (n:Concept) RETURN count(n) AS c, $k AS k", lambda i, p: {"k": i}),
    ("point lookup (idx id)", "MATCH (n:Concept {id:$x}) RETURN n.canonicalName",
        lambda i, p: {"x": p["ids"][i % len(p["ids"])]}),
]

run, rows, close = connect(ENGINE, GRAPH)

# Query vectors come from a shared pool file, identical for every engine — slater
# keeps embeddings in its vector store (not readable as an `n.embedding` property),
# so the pool is sampled once (from Neo4j) and reused here. Same query points across
# engines = a fair comparison. Built on first use if the file is absent.
import json, os
POOL_FILE = "/tmp/bench-hs/vec_pool.json"
if not os.path.exists(POOL_FILE):
    _r, nrows, nclose = connect("neo4j", GRAPH)
    pool = {
        "cvec": [list(r[0]) for r in nrows("MATCH (n:Concept) WHERE n.embedding IS NOT NULL RETURN n.embedding LIMIT 200")],
        "hvec": [list(r[0]) for r in nrows("MATCH (n:Chunk) WHERE n.embedding IS NOT NULL RETURN n.embedding LIMIT 200")],
        "ids":  [r[0] for r in nrows("MATCH (n:Concept) WHERE n.id IS NOT NULL RETURN n.id LIMIT 200")],
    }
    nclose()
    os.makedirs(os.path.dirname(POOL_FILE), exist_ok=True)
    json.dump(pool, open(POOL_FILE, "w"))
P = json.load(open(POOL_FILE))
for _ in range(5):
    run("MATCH (n) RETURN count(n)", {})
    run("MATCH ()-[r]->() RETURN count(r)", {})
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
