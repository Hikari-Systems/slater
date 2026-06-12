#!/usr/bin/env python3
"""Load the pole graph from the running Neo4j into Memgraph or FalkorDB.

Reads (id, labels, props) and (a, b, type) from Neo4j, then bulk-inserts via
UNWIND batches grouped by label-set / rel-type. A temp label `_N` + `_id` prop
(indexed) carry the join keys, then are stripped so the loaded graph matches.
"""
import sys, time
from collections import defaultdict
from neo4j import GraphDatabase

TARGET = sys.argv[1]            # memgraph | falkordb
SRC_URI = "bolt://localhost:7688"
SRC_AUTH = ("neo4j", "polepole12")
BATCH = 5000

# The real (label, property) indexes from the pole dump (minus the temp dump id).
INDEXES = [
    ("Person", "surname"), ("Person", "nhs_no"), ("Person", "name"),
    ("Crime", "last_outcome"), ("Crime", "type"),
    ("Officer", "surname"), ("Officer", "rank"),
    ("Location", "address"), ("Location", "postcode"),
    ("PostCode", "code"), ("Area", "areaCode"), ("Object", "type"),
]

def chunks(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

# ---- read from Neo4j -------------------------------------------------------
src = GraphDatabase.driver(SRC_URI, auth=SRC_AUTH)
with src.session(database="neo4j") as s:
    nodes = [(r["id"], tuple(r["labels"]), dict(r["props"]))
             for r in s.run("MATCH (n) RETURN id(n) AS id, labels(n) AS labels, properties(n) AS props")]
    rels = [(r["a"], r["b"], r["t"])
            for r in s.run("MATCH (a)-[r]->(b) RETURN id(a) AS a, id(b) AS b, type(r) AS t")]
src.close()
print(f"read {len(nodes)} nodes / {len(rels)} rels from Neo4j", flush=True)

# ---- target runner ---------------------------------------------------------
if TARGET == "memgraph":
    drv = GraphDatabase.driver("bolt://localhost:7689", auth=("", ""))
    sess = drv.session()
    def run(q, params=None):
        return sess.run(q, params or {}).consume()
elif TARGET == "falkordb":
    from falkordb import FalkorDB
    g = FalkorDB(host="localhost", port=int(open("/tmp/falkor_port").read().strip())).select_graph("pole")
    def run(q, params=None):
        return g.query(q, params or {})
else:
    sys.exit("target must be memgraph|falkordb")

t0 = time.time()
# temp join index
run("CREATE INDEX ON :_N(_id)")

# nodes grouped by label-set
by_labels = defaultdict(list)
for nid, labels, props in nodes:
    by_labels[labels].append({"id": nid, "props": props})
for labels, rows in by_labels.items():
    lab = ":".join(list(labels) + ["_N"])
    q = f"UNWIND $rows AS r CREATE (n:{lab}) SET n += r.props SET n._id = r.id"
    for ch in chunks(rows, BATCH):
        run(q, {"rows": ch})
    print(f"  nodes :{':'.join(labels)} -> {len(rows)}", flush=True)

# rels grouped by type
by_type = defaultdict(list)
for a, b, t in rels:
    by_type[t].append({"a": a, "b": b})
for t, rows in by_type.items():
    q = (f"UNWIND $rows AS r MATCH (a:_N {{_id:r.a}}), (b:_N {{_id:r.b}}) "
         f"CREATE (a)-[:{t}]->(b)")
    for ch in chunks(rows, BATCH):
        run(q, {"rows": ch})
    print(f"  rels :{t} -> {len(rows)}", flush=True)

# real indexes
for lab, prop in INDEXES:
    try:
        run(f"CREATE INDEX ON :{lab}({prop})")
    except Exception as e:
        print(f"  index {lab}.{prop} failed: {e}", flush=True)

# strip temp join artifacts in batches
ids = [n[0] for n in nodes]
for ch in chunks(ids, BATCH):
    run("UNWIND $ids AS i MATCH (n:_N {_id:i}) REMOVE n:_N, n._id", {"ids": ch})
try:
    run("DROP INDEX ON :_N(_id)")
except Exception as e:
    print(f"  drop temp index: {e}", flush=True)

# verify
nc = run("MATCH (n) RETURN count(n) AS c")
print(f"loaded {TARGET} in {time.time()-t0:.1f}s", flush=True)
