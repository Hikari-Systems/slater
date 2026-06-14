"""Query library for the slater load-test suite.

Each query is a *shape* that maps to one of slater's potentially fragile areas
(see README). Shapes follow the perf/bench.py convention: stable query TEXT with
a varying Bolt PARAMETER so the result cache MISSES every call (the real
execution path) — except the cache-churn shapes, which deliberately spread reads
across the store, and the soak mix, which lets some hit.

Value pools (crime types, nhs_no samples) are derived from the graph itself, the
same way bench.py does it, so no schema knowledge is hard-coded. If derivation
fails (a non-pole graph), the pools fall back to empty and the schema-specific
shapes are skipped by `shapes_for` — the schema-agnostic shapes (count, label
scan, fat unwind, fat param, deep var-length) always work.
"""

from __future__ import annotations

import random

# Outcome substrings known to occur in Crime.last_outcome — varying the term
# forces a fresh full scan each call (mirrors bench.py).
OUTCOME_TERMS = [
    "suspect", "investigation", "complete", "review", "action", "court",
    "caution", "charged", "unable", "identified", "resolved", "prosecution",
    "further", "police",
]


def derive_pools(driver, db):
    """Pull value pools from the graph itself; best-effort (empty on failure)."""
    types, nhs = [], []
    try:
        with driver.session(database=db) as s:
            types = [r[0] for r in s.run("MATCH (c:Crime) RETURN DISTINCT c.type")
                     if r[0] is not None]
            nhs = [r[0] for r in s.run(
                "MATCH (p:Person) WHERE p.nhs_no IS NOT NULL "
                "RETURN p.nhs_no LIMIT 500")]
    except Exception:
        pass
    return {"types": types, "nhs": nhs}


# A shape: (name, area, schema_specific, query_text, param_fn(i, pools)).
# `area` ties the shape to a fragile area for reporting. `schema_specific` shapes
# are skipped when the pools could not be derived.
def all_shapes():
    return [
        # ── schema-agnostic (always available) ───────────────────────────────
        ("count_all", "scan", False,
         "MATCH (n) RETURN count(n) AS c, $k AS k",
         lambda i, p: {"k": i}),
        ("fat_unwind", "intermediate", False,
         # A large UNWIND + collect materialises a big intermediate — charges the
         # per-query intermediate budget (query.maxIntermediate).
         "UNWIND range(0, $n) AS x WITH collect(x) AS xs RETURN size(xs) AS c, $k AS k",
         lambda i, p: {"n": 200000, "k": i}),
        ("fat_param", "message", False,
         # A large IN-list parameter inflates the Bolt message body (reassembly
         # cap, server.maxMessageBytes) without a huge result.
         "MATCH (n) WHERE id(n) IN $ids RETURN count(n) AS c, $k AS k",
         lambda i, p: {"ids": list(range(0, 20000)), "k": i}),
        ("deep_varlen", "deadline", False,
         # An unbounded-ish variable-length expansion stresses execution time
         # (query.timeoutMs) and the intermediate budget on dense graphs.
         "MATCH p=(n)-[*1..4]-(m) RETURN count(p) AS c, $k AS k",
         lambda i, p: {"k": i}),
        # ── pole-schema shapes (skipped if pools empty) ──────────────────────
        ("point_lookup", "scan", True,
         "MATCH (p:Person {nhs_no:$x}) RETURN p.name AS name, $k AS k",
         lambda i, p: {"x": _pick(p["nhs"], i), "k": i}),
        ("two_hop", "expand", True,
         "MATCH (p:Person)-[:CURRENT_ADDRESS]->(l:Location)-[:LOCATION_IN_AREA]->(a:Area) "
         "RETURN a.areaCode AS area, $k AS k LIMIT 200",
         lambda i, p: {"k": i}),
        ("three_hop", "expand", True,
         "MATCH (o:Officer)<-[:INVESTIGATED_BY]-(c:Crime)-[:OCCURRED_AT]->(l:Location) "
         "RETURN o.surname AS s, l.postcode AS pc, $k AS k LIMIT 200",
         lambda i, p: {"k": i}),
        ("agg_by_type", "aggregation", True,
         "MATCH (c:Crime) RETURN c.type AS t, count(*) AS n, $k AS k ORDER BY n DESC LIMIT 25",
         lambda i, p: {"k": i}),
        ("scan_contains", "scan", True,
         # Full label scan with CONTAINS — reads cold blocks, churns the block
         # cache when run wide (cache.blockCacheBytes).
         "MATCH (c:Crime) WHERE c.last_outcome CONTAINS $w RETURN count(c) AS c, $k AS k",
         lambda i, p: {"w": OUTCOME_TERMS[i % len(OUTCOME_TERMS)], "k": i}),
        ("shortest_path", "shortest_path", True,
         # shortestPath between two indexed people — BFS discovery
         # (query.maxShortestPathExplore).
         "MATCH (a:Person {nhs_no:$x}), (b:Person {nhs_no:$y}), "
         "p = shortestPath((a)-[*..6]-(b)) RETURN length(p) AS len, $k AS k",
         lambda i, p: {"x": _pick(p["nhs"], i), "y": _pick(p["nhs"], i + 7), "k": i}),
    ]


def _pick(pool, i):
    return pool[i % len(pool)] if pool else None


def shapes_for(names, pools):
    """Resolve shape names to callable (name, text, param_fn), dropping any
    schema-specific shape when the pools are empty. `names` may include 'all'."""
    have_schema = bool(pools.get("types")) and bool(pools.get("nhs"))
    chosen = []
    for name, area, schema_specific, text, pf in all_shapes():
        if "all" not in names and name not in names:
            continue
        if schema_specific and not have_schema:
            continue
        chosen.append((name, text, pf))
    return chosen


def rand_seed_iter():
    """A per-user counter seed so different Locust users vary their parameters."""
    return random.randint(0, 1 << 30)
