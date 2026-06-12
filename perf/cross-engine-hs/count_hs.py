#!/usr/bin/env python3
"""Print an engine's node count for graph <graph>, or 'NA' if not ready.
Used to gate on container readiness + data recovery before benching.

Usage: count_hs.py <engine> <graph>
"""
import sys
from engines import connect

engine, graph = sys.argv[1], sys.argv[2]
try:
    _, rows, close = connect(engine, graph)
    print(rows("MATCH (n) RETURN count(n) AS c")[0][0])
    close()
except Exception:
    print("NA")
