#!/usr/bin/env python3
"""Slater vs Neo4j latency benchmark for the pole crime graph.

Measures each query in two regimes that matter for an honest read:

  * uncached - stable query TEXT with a Bolt PARAMETER that varies every call.
    Neo4j keeps its plan cached; slater's result cache (keyed on query+params)
    MISSES every call. This is the real execution-engine number.
  * cached   - the identical query+params repeated. slater serves from its
    result cache; Neo4j has no result cache so its "cached" == "uncached".

Why parameters (not literal substitution): varying a literal also busts Neo4j's
PLAN cache, inflating its numbers. A varying parameter with stable text isolates
*execution*. Queries with no natural varying key get a dummy `$k` in RETURN to
force a slater cache miss without changing the plan.

Value pools (crime types, nhs_no samples, outcome substrings) are derived from
slater itself, so no live Neo4j is required for the slater-only run.

Usage:
  python3 perf/bench.py --slater-pass polereader
  python3 perf/bench.py --slater-pass polereader \
      --neo4j-uri bolt://localhost:7688 --neo4j-pass polepole12   # adds parity
  python3 perf/bench.py --slater-pass polereader --slater-pid 564766  # + RSS

Needs the neo4j Python driver:  python3 -m venv .venv && .venv/bin/pip install neo4j
"""

import argparse
import statistics as st
import time

from neo4j import GraphDatabase

# Outcome substrings known to occur in Crime.last_outcome (used by the CONTAINS
# scan; varying the term forces a fresh scan each call).
OUTCOME_TERMS = [
    "suspect", "investigation", "complete", "review", "action", "court",
    "caution", "charged", "unable", "identified", "resolved", "prosecution",
    "further", "police",
]

# Each entry: (name, query_text_with_$params, param_fn(i, pools)). The TEXT is
# stable across iterations; only parameter VALUES change.
def query_suite():
    return [
        ("count all nodes",
         "MATCH (n) RETURN count(n) AS c, $k AS k",
         lambda i, p: {"k": i}),
        ("Crime label count",
         "MATCH (n:Crime) RETURN count(n) AS c, $k AS k",
         lambda i, p: {"k": i}),
        ("point lookup (idx nhs_no)",
         "MATCH (p:Person {nhs_no:$x}) RETURN p.name",
         lambda i, p: {"x": p["nhs"][i % len(p["nhs"])]}),
        ("idx-eq count (Crime.type)",
         "MATCH (c:Crime {type:$t}) RETURN count(c)",
         lambda i, p: {"t": p["types"][i % len(p["types"])]}),
        ("1-hop Crime->Location",
         "MATCH (c:Crime {type:$t})-[:OCCURRED_AT]->(l:Location) RETURN l.address LIMIT 100",
         lambda i, p: {"t": p["types"][i % len(p["types"])]}),
        ("2-hop Person->Loc->Area",
         "MATCH (p:Person)-[:CURRENT_ADDRESS]->(l:Location)-[:LOCATION_IN_AREA]->(a:Area) "
         "RETURN a.areaCode, $k AS k LIMIT 100",
         lambda i, p: {"k": i}),
        ("agg crimes by type",
         "MATCH (c:Crime) RETURN c.type AS t, count(*) AS n, $k AS k ORDER BY n DESC LIMIT 10",
         lambda i, p: {"k": i}),
        ("3-hop Officer/Crime/Loc",
         "MATCH (o:Officer)<-[:INVESTIGATED_BY]-(c:Crime)-[:OCCURRED_AT]->(l:Location) "
         "RETURN o.surname, l.postcode, $k AS k LIMIT 100",
         lambda i, p: {"k": i}),
        ("full-scan CONTAINS",
         "MATCH (c:Crime) WHERE c.last_outcome CONTAINS $w RETURN count(c)",
         lambda i, p: {"w": OUTCOME_TERMS[i % len(OUTCOME_TERMS)]}),
        ("count DISTINCT type",
         "MATCH (c:Crime) RETURN count(DISTINCT c.type) AS c, $k AS k",
         lambda i, p: {"k": i}),
    ]


def derive_pools(driver, db):
    """Pull value pools from the graph itself (works against slater or Neo4j)."""
    with driver.session(database=db) as s:
        types = [r[0] for r in s.run("MATCH (c:Crime) RETURN DISTINCT c.type") if r[0] is not None]
        nhs = [r[0] for r in s.run(
            "MATCH (p:Person) WHERE p.nhs_no IS NOT NULL RETURN p.nhs_no LIMIT 200")]
    if not types or not nhs:
        raise SystemExit("could not derive value pools (is the pole graph loaded?)")
    return {"types": types, "nhs": nhs}


def bench(driver, db, pools, warmup, meas, cached):
    out = {}
    with driver.session(database=db) as s:          # one reused session
        for name, q, pf in query_suite():
            for w in range(warmup):
                list(s.run(q, pf(0 if cached else w, pools)))
            ts = []
            last_rows = 0
            for i in range(meas):
                params = pf(0 if cached else i, pools)
                t0 = time.perf_counter()
                rows = list(s.run(q, params))
                ts.append((time.perf_counter() - t0) * 1000.0)
                last_rows = len(rows)
            out[name] = {
                "median": st.median(ts), "min": min(ts),
                "p95": sorted(ts)[max(0, int(0.95 * len(ts)) - 1)], "rows": last_rows,
            }
    return out


def rss_mb(pid):
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        return None
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--slater-uri", default="bolt://localhost:7687")
    ap.add_argument("--slater-user", default="reporting")
    ap.add_argument("--slater-pass", required=True)
    ap.add_argument("--slater-db", default="pole")
    ap.add_argument("--slater-pid", type=int, help="print slater RSS from /proc/<pid>")
    ap.add_argument("--neo4j-uri", help="optional: add a Neo4j parity column + result check")
    ap.add_argument("--neo4j-user", default="neo4j")
    ap.add_argument("--neo4j-pass")
    ap.add_argument("--neo4j-db", default="neo4j")
    ap.add_argument("--warmup", type=int, default=3)
    ap.add_argument("--meas", type=int, default=25)
    args = ap.parse_args()

    sl = GraphDatabase.driver(args.slater_uri, auth=(args.slater_user, args.slater_pass))
    sl.verify_connectivity()
    pools = derive_pools(sl, args.slater_db)
    print(f"pools: {len(pools['types'])} crime types, {len(pools['nhs'])} nhs_no samples")

    S_un = bench(sl, args.slater_db, pools, args.warmup, args.meas, cached=False)
    S_ca = bench(sl, args.slater_db, pools, args.warmup, args.meas, cached=True)

    ne = None
    if args.neo4j_uri:
        ne = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_pass))
        ne.verify_connectivity()
        N_un = bench(ne, args.neo4j_db, pools, args.warmup, args.meas, cached=False)

    hdr = f"\n{'query':26} | {'slater uncached':>15} {'cached':>8}"
    if ne:
        hdr += f" | {'neo4j':>9} | {'n4j/sl':>7}"
    print(hdr)
    print("-" * (len(hdr) + 4))
    for name, _, _ in query_suite():
        su = S_un[name]; sc = S_ca[name]
        line = (f"{name:26} | {su['median']:8.2f} ({su['min']:6.2f}) {sc['median']:7.3f}")
        if ne:
            nu = N_un[name]
            ratio = nu["median"] / su["median"] if su["median"] else 0
            tag = f"{ratio:5.2f}x" if ratio >= 1 else f"1/{su['median']/nu['median']:4.1f}x"
            line += f" | {nu['median']:7.2f}ms | {tag:>7}"
            if su["rows"] != nu["rows"]:
                line += f"  !rows sl={su['rows']} n4j={nu['rows']}"
        print(line)
    print("-" * (len(hdr) + 4))
    print("uncached = real execution (varying param forces recompute); cached = slater result-cache hit")
    print("medians in ms over", args.meas, "runs after", args.warmup, "warmups")

    if args.slater_pid:
        r = rss_mb(args.slater_pid)
        print(f"\nslater RSS (pid {args.slater_pid}): {r:.0f} MB" if r else
              f"\nslater RSS: pid {args.slater_pid} not readable")
    print("neo4j memory: run  docker stats --no-stream pole-neo4j")

    sl.close()
    if ne:
        ne.close()


if __name__ == "__main__":
    main()
