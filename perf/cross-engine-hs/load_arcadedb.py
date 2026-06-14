#!/usr/bin/env python3
"""Load a slater primitive-Cypher dump into ArcadeDB.

ArcadeDB models each openCypher label-set as its own composite *type*, and changing
a node's labels (the generic loader's `__DumpVertex__` add-then-strip join trick)
retypes the record into a different bucket — which moves its RID and corrupts every
index on it ("Record #x:y not found"). So ArcadeDB needs a schema-first,
inheritance-based load instead:

  * the label present on every node becomes a super vertex type;
  * every other label becomes a sub-type EXTENDS the super;
  * each node is created as its concrete sub-type (single-label CREATE, no composite);
  * range-indexed properties + a `__dump_id__` join key are declared and indexed on
    the super-type, so the sub-types inherit them;
  * relationships join on the indexed `__dump_id__` via the polymorphic super-type;
  * nothing is stripped afterwards (no retype, no index corruption).

Polymorphic `MATCH (n:Super)` then matches all sub-types and `MATCH (n:Sub)` its
subset — exactly what the cross-engine suite expects. Schema DDL goes over HTTP/SQL
(ArcadeDB's Bolt endpoint is openCypher-only); node/edge data goes over Bolt.

Assumes one label is common to every node (a shared super). Topologies without a
common super, or with several non-super labels per node, need extension.

Usage:
    load_arcadedb.py <dump.cypher> [--http http://localhost:2480] \
        [--bolt bolt://localhost:7703] [--db bench] [--user root] [--pass <pw>]
"""
import argparse
import base64
import json
import sys
import time
import urllib.request
from collections import defaultdict
from functools import reduce

from load_cypher import chunks, parse

BATCH = 2000


def make_sql(http, db, user, password):
    url = f"{http}/api/v1/command/{db}"
    tok = base64.b64encode(f"{user}:{password}".encode()).decode()

    def sql(command, tolerate=False):
        body = json.dumps({"language": "sql", "command": command}).encode()
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Authorization", f"Basic {tok}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            msg = e.read().decode(errors="replace")
            if tolerate:
                return {"_error": msg}
            raise SystemExit(f"ArcadeDB SQL failed: {command}\n  {msg[:300]}")

    return sql


def load(dump, http, bolt, db, user, password):
    from neo4j import GraphDatabase

    nodes, rels, range_idx, _vec_idx = parse(dump)
    print(f"parsed {len(nodes)} nodes / {len(rels)} rels / {len(range_idx)} range-idx",
          flush=True)

    # Super = label on every node (intersection of all label-sets); pick the most
    # globally-common when several qualify.
    from collections import Counter
    counts = Counter(l for _did, labels, _p in nodes for l in labels)
    common = reduce(lambda a, b: a & b, (set(labels) for _did, labels, _p in nodes))
    if not common:
        raise SystemExit("no label common to every node — ArcadeDB inheritance load needs one")
    super_t = max(common, key=lambda l: counts[l])
    print(f"  super-type: {super_t}", flush=True)

    def concrete(labels):
        subs = sorted(set(labels) - {super_t})
        return "_".join(subs) if subs else super_t

    sql = make_sql(http, db, user, password)

    # ---- schema (SQL/HTTP) ----
    # Infer each indexed property's ArcadeDB type from the data — declaring an int
    # column (e.g. wikidata_id) as STRING would break `{prop:$x}` integer lookups.
    def arcade_type(prop):
        for _did, _labels, props in nodes:
            v = props.get(prop)
            if v is None:
                continue
            if isinstance(v, bool):
                return "BOOLEAN"
            if isinstance(v, int):
                return "LONG"
            if isinstance(v, float):
                return "DOUBLE"
            return "STRING"
        return "STRING"

    sql(f"CREATE VERTEX TYPE {super_t} IF NOT EXISTS")
    sql(f"CREATE PROPERTY {super_t}.`__dump_id__` IF NOT EXISTS LONG")
    indexed_props = sorted({prop for _lab, prop in range_idx})
    prop_types = {p: arcade_type(p) for p in indexed_props}
    for prop in indexed_props:
        sql(f"CREATE PROPERTY {super_t}.`{prop}` IF NOT EXISTS {prop_types[prop]}")
    # __dump_id__ (unique) on the super-type backs the relationship-load join. The
    # range-prop indexes are created per concrete sub-type AFTER the load (below):
    # an index on the super-type under-serves polymorphic queries in ArcadeDB.
    sql(f"CREATE INDEX IF NOT EXISTS ON {super_t}(`__dump_id__`) UNIQUE")

    ctypes = {concrete(labels) for _did, labels, _p in nodes}
    for ct in sorted(ctypes):
        if ct != super_t:
            sql(f"CREATE VERTEX TYPE {ct} IF NOT EXISTS EXTENDS {super_t}")
    for typ in sorted({t for _a, _b, t, _p in rels}):
        sql(f"CREATE EDGE TYPE {typ} IF NOT EXISTS")

    # ---- data (Bolt openCypher) ----
    drv = GraphDatabase.driver(bolt, auth=(user, password), encrypted=False)
    sess = drv.session(database=db)
    def run(q, params=None): return sess.run(q, params or {}).consume()

    t0 = time.time()
    by_ctype = defaultdict(list)
    for did, labels, props in nodes:
        by_ctype[concrete(labels)].append({"id": did, "props": props})
    for ct, rws in sorted(by_ctype.items()):
        q = f"UNWIND $rows AS r CREATE (n:{ct}) SET n += r.props SET n.`__dump_id__` = r.id"
        for ch in chunks(rws, BATCH):
            run(q, {"rows": ch})
        print(f"  arcadedb nodes :{ct} -> {len(rws)}", flush=True)

    by_type = defaultdict(list)
    has_props = defaultdict(bool)
    for a, b, t, props in rels:
        by_type[t].append({"a": a, "b": b, "props": props})
        if props:
            has_props[t] = True
    for t, rws in by_type.items():
        setp = " SET e += r.props" if has_props[t] else ""
        q = (f"UNWIND $rows AS r MATCH (a:{super_t} {{`__dump_id__`:r.a}}), "
             f"(b:{super_t} {{`__dump_id__`:r.b}}) CREATE (a)-[e:{t}]->(b){setp}")
        for ch in chunks(rws, BATCH):
            run(q, {"rows": ch})
        print(f"  arcadedb rels :{t} -> {len(rws)}", flush=True)

    sess.close(); drv.close()

    # Build the range-prop indexes on each concrete type that holds data, over the
    # now-loaded records. A super-type range index under-counts polymorphic queries
    # in ArcadeDB (incremental maintenance misses sub-type buckets), so we index the
    # concrete types directly — complete, and used for both sub-type lookups (point
    # lookup on :Drug) and the polymorphic :MeshTerm filters. For a single-label
    # graph the only concrete type IS the super (e.g. :Entity), which is indexed here.
    for ct in sorted(by_ctype):
        for prop in indexed_props:
            sql(f"CREATE INDEX IF NOT EXISTS ON {ct}(`{prop}`) NOTUNIQUE", tolerate=True)

    print(f"loaded arcadedb in {time.time()-t0:.1f}s "
          f"({len(nodes)} nodes / {len(rels)} rels)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump")
    ap.add_argument("--http", default="http://localhost:2480")
    ap.add_argument("--bolt", default="bolt://localhost:7703")
    ap.add_argument("--db", default="bench")
    ap.add_argument("--user", default="root")
    ap.add_argument("--pass", dest="password", default="playwithdata")
    args = ap.parse_args()
    load(args.dump, args.http, args.bolt, args.db, args.user, args.password)


if __name__ == "__main__":
    main()
