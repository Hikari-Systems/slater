#!/usr/bin/env python3
"""Shared connection layer for the hs-backend-spot cross-engine benchmark.

One `connect(engine, graph)` returns (run, rows, close): `run(q, params)` consumes
all rows (the timed path); `rows(q, params)` returns the rows as a list (for building
parameter pools / sanity checks). Dedicated `-hs` ports so the pole stack is untouched.
"""

# (uri/port, auth, db) per engine — dedicated -hs port block.
SLATER_USER, SLATER_PASS = "reporting", "polereader"
NEO4J_USER, NEO4J_PASS = "neo4j", "polepole12"
PORTS = {"slater": 7700, "neo4j": 7701, "memgraph": 7702, "falkordb": 6401}


def connect(engine, graph):
    if engine == "falkordb":
        from falkordb import FalkorDB
        g = FalkorDB(host="localhost", port=PORTS["falkordb"]).select_graph(graph)
        def run(q, params=None): g.query(q, params or {}).result_set
        def rows(q, params=None): return g.query(q, params or {}).result_set
        return run, rows, (lambda: None)

    from neo4j import GraphDatabase
    if engine == "slater":
        uri, auth, db = f"bolt://localhost:{PORTS['slater']}", (SLATER_USER, SLATER_PASS), graph
    elif engine == "neo4j":
        uri, auth, db = f"bolt://localhost:{PORTS['neo4j']}", (NEO4J_USER, NEO4J_PASS), "neo4j"
    elif engine == "memgraph":
        uri, auth, db = f"bolt://localhost:{PORTS['memgraph']}", ("", ""), None
    else:
        raise SystemExit(f"unknown engine {engine}")
    drv = GraphDatabase.driver(uri, auth=auth)
    sess = drv.session(database=db) if db else drv.session()
    def run(q, params=None): list(sess.run(q, params or {}))
    def rows(q, params=None): return [list(r) for r in sess.run(q, params or {})]
    return run, rows, (lambda: (sess.close(), drv.close()))
