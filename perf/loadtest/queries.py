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
    types, nhs, wids = [], [], []
    try:
        with driver.session(database=db) as s:
            types = [r[0] for r in s.run("MATCH (c:Crime) RETURN DISTINCT c.type")
                     if r[0] is not None]
            nhs = [r[0] for r in s.run(
                "MATCH (p:Person) WHERE p.nhs_no IS NOT NULL "
                "RETURN p.nhs_no LIMIT 500")]
    except Exception:
        pass
    wids = _derive_wids(driver, db)
    return {"types": types, "nhs": nhs, "wids": wids}


# Wikidata (Entity/LINK, range index on wikidata_id) value pool. Built by reading
# scattered range-index slices so the looked-up nodes spread across the store —
# the point of a cache-churn driver against a graph far larger than the cache.
# A pre-built pool file (cheap, deterministic across workers) is used if present.
import json  # noqa: E402
import os    # noqa: E402

_WID_POOL_FILE = os.environ.get("SLATER_WID_POOL", "/tmp/loadtest/wid_pool.json")


def _derive_wids(driver, db):
    try:
        with open(_WID_POOL_FILE) as fh:
            pool = json.load(fh)
        if pool:
            return pool
    except Exception:
        pass
    pool = []
    try:
        for k in range(20):
            lo = (k * 4999999) % 99000000
            with driver.session(database=db) as s:
                pool += [r["w"] for r in s.run(
                    "MATCH (n:Entity) WHERE n.wikidata_id >= $lo "
                    "RETURN n.wikidata_id AS w LIMIT 1000", {"lo": lo})]
    except Exception:
        pass
    return pool


# A shape: (name, area, needs, query_text, param_fn(i, pools)).
# `area` ties the shape to a fragile area for reporting. `needs` is "" for a
# schema-agnostic shape, or a pool key ("pole" / "wiki"); a shape is skipped when
# the pool it needs could not be derived for the graph under test.
def all_shapes():
    return [
        # ── schema-agnostic (always available) ───────────────────────────────
        ("count_all", "scan", "",
         "MATCH (n) RETURN count(n) AS c, $k AS k",
         lambda i, p: {"k": i}),
        ("fat_unwind", "intermediate", "",
         # A large UNWIND + collect materialises a big intermediate — charges the
         # per-query intermediate budget (query.maxIntermediate).
         "UNWIND range(0, $n) AS x WITH collect(x) AS xs RETURN size(xs) AS c, $k AS k",
         lambda i, p: {"n": 200000, "k": i}),
        ("fat_param", "message", "",
         # A large IN-list parameter inflates the Bolt message body (reassembly
         # cap, server.maxMessageBytes) without a huge result.
         "MATCH (n) WHERE id(n) IN $ids RETURN count(n) AS c, $k AS k",
         lambda i, p: {"ids": list(range(0, 20000)), "k": i}),
        ("deep_varlen", "deadline", "",
         # An unbounded-ish variable-length expansion stresses execution time
         # (query.timeoutMs) and the intermediate budget on dense graphs.
         "MATCH p=(n)-[*1..4]-(m) RETURN count(p) AS c, $k AS k",
         lambda i, p: {"k": i}),
        # ── wikidata-schema shapes (Entity/LINK, range idx on wikidata_id) ────
        # These read across the disk-bound store: a range-index seek plus node /
        # topology block reads, with the key varying over a store-spread pool so
        # the working set exceeds a small block cache → real eviction churn.
        ("wiki_point", "scan", "wiki",
         "MATCH (n:Entity {wikidata_id:$w}) RETURN n.name AS name, $k AS k",
         lambda i, p: {"w": _pick(p["wids"], i), "k": i}),
        ("wiki_1hop", "expand", "wiki",
         # Range-index seek then a 1-hop expansion — reads topology.csr blocks.
         "MATCH (n:Entity {wikidata_id:$w})-[:LINK]-(m) RETURN count(m) AS deg, $k AS k",
         lambda i, p: {"w": _pick(p["wids"], i), "k": i}),
        ("wiki_range", "scan", "wiki",
         # An index range scan over a 50k-wide wikidata_id window at a varying
         # offset — wide cold reads of the range index, churns the block cache.
         "MATCH (n:Entity) WHERE n.wikidata_id >= $lo AND n.wikidata_id < $hi "
         "RETURN count(n) AS c, $k AS k",
         lambda i, p: {"lo": (i * 2654435761) % 99000000,
                       "hi": (i * 2654435761) % 99000000 + 50000, "k": i}),
        ("wiki_2hop", "intermediate", "wiki",
         # A 2-hop expansion from a seeded node fans out past query.maxIntermediate
         # on higher-degree nodes → clean fail_budget instead of unbounded RAM.
         "MATCH (n:Entity {wikidata_id:$w})-[:LINK]-()-[:LINK]-(m) "
         "RETURN count(DISTINCT m) AS c, $k AS k",
         lambda i, p: {"w": _pick(p["wids"], i), "k": i}),
        # ── pole-schema shapes (skipped if pools empty) ──────────────────────
        ("point_lookup", "scan", "pole",
         "MATCH (p:Person {nhs_no:$x}) RETURN p.name AS name, $k AS k",
         lambda i, p: {"x": _pick(p["nhs"], i), "k": i}),
        ("two_hop", "expand", "pole",
         "MATCH (p:Person)-[:CURRENT_ADDRESS]->(l:Location)-[:LOCATION_IN_AREA]->(a:Area) "
         "RETURN a.areaCode AS area, $k AS k LIMIT 200",
         lambda i, p: {"k": i}),
        ("three_hop", "expand", "pole",
         "MATCH (o:Officer)<-[:INVESTIGATED_BY]-(c:Crime)-[:OCCURRED_AT]->(l:Location) "
         "RETURN o.surname AS s, l.postcode AS pc, $k AS k LIMIT 200",
         lambda i, p: {"k": i}),
        ("agg_by_type", "aggregation", "pole",
         "MATCH (c:Crime) RETURN c.type AS t, count(*) AS n, $k AS k ORDER BY n DESC LIMIT 25",
         lambda i, p: {"k": i}),
        ("scan_contains", "scan", "pole",
         # Full label scan with CONTAINS — reads cold blocks, churns the block
         # cache when run wide (cache.blockCacheBytes).
         "MATCH (c:Crime) WHERE c.last_outcome CONTAINS $w RETURN count(c) AS c, $k AS k",
         lambda i, p: {"w": OUTCOME_TERMS[i % len(OUTCOME_TERMS)], "k": i}),
        ("shortest_path", "shortest_path", "pole",
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
    schema-specific shape whose pool is empty for the graph under test. `names`
    may include 'all'."""
    have = {
        "": True,
        "pole": bool(pools.get("types")) and bool(pools.get("nhs")),
        "wiki": bool(pools.get("wids")),
    }
    chosen = []
    for name, area, needs, text, pf in all_shapes():
        if "all" not in names and name not in names:
            continue
        if not have.get(needs, False):
            continue
        chosen.append((name, text, pf))
    return chosen


def rand_seed_iter():
    """A per-user counter seed so different Locust users vary their parameters."""
    return random.randint(0, 1 << 30)
