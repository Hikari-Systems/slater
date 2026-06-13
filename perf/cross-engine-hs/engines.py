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


def _ladybug_rewriter(graph):
    import json
    import os
    import re

    meta_path = os.environ.get(
        "LADYBUG_META",
        f"/tmp/bench-hs/ladybug/{graph}.meta.json",
    )
    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)
    label_to_primary = meta.get("label_to_primary", {})

    label_pat = re.compile(r"\((?P<var>[A-Za-z_][A-Za-z0-9_]*)?:(?P<label>[A-Za-z_][A-Za-z0-9_]*)")

    def rewrite(q):
        filters = []

        def repl(m):
            var = m.group("var") or ""
            label = m.group("label")
            primary = label_to_primary.get(label, label)
            if var and primary != label:
                filters.append(f"{var}.__labels CONTAINS '|{label}|'")
            return f"({var}:{primary}"

        q2 = label_pat.sub(repl, q)
        if not filters:
            return q2
        filt = " AND ".join(filters)
        where = re.search(r"\bWHERE\b", q2, re.IGNORECASE)
        if where:
            return q2[: where.end()] + f" {filt} AND" + q2[where.end():]
        ret = re.search(r"\bRETURN\b", q2, re.IGNORECASE)
        if ret:
            return q2[: ret.start()] + f"WHERE {filt} " + q2[ret.start():]
        return q2 + f" WHERE {filt}"

    return rewrite


def connect(engine, graph):
    if engine == "ladybug":
        import os
        import ladybug as lb

        path = os.environ.get("LADYBUG_DB", f"/tmp/bench-hs/ladybug/{graph}.lbug")
        db = lb.Database(path, read_only=True)
        conn = lb.Connection(db)
        rewrite = _ladybug_rewriter(graph)

        def run(q, params=None):
            res = conn.execute(rewrite(q), params or {})
            if isinstance(res, list):
                for r in res:
                    r.get_all()
            else:
                res.get_all()

        def rows(q, params=None):
            res = conn.execute(rewrite(q), params or {})
            if isinstance(res, list):
                res = res[-1]
            return res.get_all()

        return run, rows, (lambda: (conn.close(), db.close()))

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
