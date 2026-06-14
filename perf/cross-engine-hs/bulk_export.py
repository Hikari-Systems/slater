#!/usr/bin/env python3
"""Emit nodes.csv + edges.csv from a primitive-Cypher dump, for the native bulk
importers (Neo4j neo4j-admin, Memgraph LOAD CSV, falkordb-bulk-loader, ArcadeDB
importer) — much faster than the uniform UNWIND path on large graphs.

Assumes a single node label and a single relationship type (wikidata-1m: Entity/LINK)
— the case where the bulk path is worth the per-engine plumbing. Writes:
  nodes.csv : header `dump_id,<props...>`  (dump_id is the join key edges reference)
  edges.csv : header `src,dst`
  meta.json : {label, reltype, props, n_nodes, n_rels}
Python's csv writer quotes fields per RFC4180 (doubled quotes), which every importer
below parses — so wikidata names with commas/quotes/unicode round-trip safely.

Usage: bulk_export.py <dump.cypher> --out-dir /tmp/bench-hs/csv
"""
import argparse
import csv
import json
import os

from load_cypher import parse


def export(dump, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    nodes, rels, _range_idx, _vec = parse(dump)
    labels = sorted({l for _d, ls, _p in nodes for l in ls})
    reltypes = sorted({t for _a, _b, t, _p in rels})
    if len(labels) != 1 or len(reltypes) != 1:
        raise SystemExit(f"bulk_export expects 1 label / 1 rel type, got {labels} / {reltypes}")
    # stable union of node property names (insertion order of first appearance)
    props, seen = [], set()
    for _d, _ls, p in nodes:
        for k in p:
            if k not in seen:
                seen.add(k)
                props.append(k)

    npath = os.path.join(out_dir, "nodes.csv")
    with open(npath, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["dump_id"] + props)
        for did, _ls, p in nodes:
            w.writerow([did] + [p.get(k, "") for k in props])
    epath = os.path.join(out_dir, "edges.csv")
    with open(epath, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["src", "dst"])
        for a, b, _t, _p in rels:
            w.writerow([a, b])
    meta = {"label": labels[0], "reltype": reltypes[0], "props": props,
            "n_nodes": len(nodes), "n_rels": len(rels)}
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print(f"exported {len(nodes)} nodes / {len(rels)} rels; label={labels[0]} "
          f"reltype={reltypes[0]} props={props}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump")
    ap.add_argument("--out-dir", default="/tmp/bench-hs/csv")
    args = ap.parse_args()
    export(args.dump, args.out_dir)


if __name__ == "__main__":
    main()
