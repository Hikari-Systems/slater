#!/usr/bin/env python3
"""Print the node count for an engine, or 'NA' if not ready. Used to gate on
container readiness + data recovery before benching."""
import sys
e = sys.argv[1]
try:
    if e == "falkordb":
        from falkordb import FalkorDB
        port = int(open("/tmp/falkor_port").read().strip())
        g = FalkorDB(host="localhost", port=port).select_graph("pole")
        print(g.query("MATCH (n) RETURN count(n) AS c").result_set[0][0])
    else:
        from neo4j import GraphDatabase
        cfg = {"slater":("bolt://localhost:7687",("reporting","polereader"),"pole"),
               "neo4j":("bolt://localhost:7688",("neo4j","polepole12"),"neo4j"),
               "memgraph":("bolt://localhost:7689",("",""),None)}[e]
        uri, auth, db = cfg
        d = GraphDatabase.driver(uri, auth=auth)
        s = d.session(database=db) if db else d.session()
        print(s.run("MATCH (n) RETURN count(n) AS c").single()["c"])
        s.close(); d.close()
except Exception:
    print("NA")
