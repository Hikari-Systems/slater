#!/usr/bin/env python3
"""Shared connection layer for the hs-backend-spot cross-engine benchmark.

One `connect(engine, graph)` returns (run, rows, close): `run(q, params)` consumes
all rows (the timed path); `rows(q, params)` returns the rows as a list (for building
parameter pools / sanity checks). Dedicated `-hs` ports so the pole stack is untouched.
"""

import os
# (uri/port, auth, db) per engine — dedicated -hs port block.
SLATER_USER, SLATER_PASS = "reporting", "polereader"
NEO4J_USER, NEO4J_PASS = "neo4j", "polepole12"
ARCADE_USER, ARCADE_PASS = "root", "playwithdata"
# slater/neo4j/memgraph/falkordb/arcadedb are service containers; ladybug is an
# embedded library (no port) run inside the slater-ladybug image. The slater port
# is overridable via SLATER_PORT so a one-at-a-time isolated slater run can use a
# free port while another slater container holds 7700.
PORTS = {"slater": int(os.environ.get("SLATER_PORT", "7700")),
         "neo4j": 7701, "memgraph": 7702, "falkordb": 6401, "arcadedb": 7703}


def _ladybug_rewriter(graph):
    """LadybugDB (Kùzu-derived) gives each node a single primary-table label and
    stores every label in a `__labels` pipe-string. `load_ladybug.py` writes a
    `<graph>.meta.json` with `label_to_primary`; here we rewrite each `(v:Label`
    node pattern to its primary table, and for a *secondary* label inject a
    `v.__labels CONTAINS '|Label|'` filter so multi-label matches stay correct.
    """
    import json
    import os
    import re

    meta_path = os.environ.get("LADYBUG_META", f"/data/ladybug/{graph}.meta.json")
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
    if engine == "falkordb":
        from falkordb import FalkorDB
        g = FalkorDB(host="localhost", port=PORTS["falkordb"]).select_graph(graph)
        def run(q, params=None): g.query(q, params or {}).result_set
        def rows(q, params=None): return g.query(q, params or {}).result_set
        return run, rows, (lambda: None)

    if engine == "ladybug":
        import os
        import real_ladybug as lb  # PyPI `real_ladybug`; `ladybug` is the wrong package.
        path = os.environ.get("LADYBUG_DB", f"/data/ladybug/{graph}.lbug")
        # Cap the buffer pool (default grabs ~80% of RAM) so peak RSS is comparable
        # to the other engines' cache budgets (Neo4j pagecache is 512M here).
        bp = int(os.environ.get("LADYBUG_BUFFER_POOL", str(512 * 1024 * 1024)))
        db = lb.Database(path, read_only=True, buffer_pool_size=bp)
        conn = lb.Connection(db)
        # The query side needs the `vector` extension loaded too (QUERY_VECTOR_INDEX is
        # provided by it). It's baked into the image, so LOAD is offline; harmless on
        # non-vector graphs. Best-effort so a stripped image still serves graph queries.
        try:
            conn.execute("LOAD vector;")
        except Exception:
            pass
        rewrite = _ladybug_rewriter(graph)
        def run(q, params=None):
            res = conn.execute(rewrite(q), params or {})
            for r in (res if isinstance(res, list) else [res]):
                r.get_all()
        def rows(q, params=None):
            res = conn.execute(rewrite(q), params or {})
            if isinstance(res, list):
                res = res[-1]
            return res.get_all()
        return run, rows, (lambda: (conn.close(), db.close()))

    from neo4j import GraphDatabase
    kw = {}
    if engine == "slater":
        uri, auth, db = f"bolt://localhost:{PORTS['slater']}", (SLATER_USER, SLATER_PASS), graph
    elif engine == "neo4j":
        uri, auth, db = f"bolt://localhost:{PORTS['neo4j']}", (NEO4J_USER, NEO4J_PASS), "neo4j"
    elif engine == "memgraph":
        uri, auth, db = f"bolt://localhost:{PORTS['memgraph']}", ("", ""), None
    elif engine == "arcadedb":
        # ArcadeDB's Bolt plugin speaks the Neo4j wire format but has no TLS.
        uri, auth, db = f"bolt://localhost:{PORTS['arcadedb']}", (ARCADE_USER, ARCADE_PASS), "bench"
        kw["encrypted"] = False
    else:
        raise SystemExit(f"unknown engine {engine}")
    drv = GraphDatabase.driver(uri, auth=auth, **kw)
    sess = drv.session(database=db) if db else drv.session()
    def run(q, params=None): list(sess.run(q, params or {}))
    def rows(q, params=None): return [list(r) for r in sess.run(q, params or {})]
    return run, rows, (lambda: (sess.close(), drv.close()))
