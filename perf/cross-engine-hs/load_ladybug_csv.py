#!/usr/bin/env python3
"""Load a single-label graph into LadybugDB via Kùzu `COPY FROM` CSV — for graphs
too large to parse the Cypher dump in memory (full Wikidata: 91.6M nodes / 766M
edges). Header-less CSVs: nodes = `dump_id,wikidata_id,name`; edges = `src,dst`
(both referencing the INT64 dump_id primary key). Writes the `<graph>.meta.json`
that `engines._ladybug_rewriter` reads — trivial here (one label maps to itself).

Usage:
    load_ladybug_csv.py --nodes nodes.csv --edges edges.csv --graph wikidatafull \
        [--label Entity] [--reltype LINK] [--out-dir /data/ladybug]
"""
import argparse
import glob
import json
import os
import shutil

import real_ladybug as lb


def load(nodes_csv, edges_csv, graph, out_dir, label, reltype):
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(out_dir, f"{graph}.lbug")
    for p in glob.glob(db_path + "*"):
        shutil.rmtree(p) if os.path.isdir(p) else os.unlink(p)

    # A generous buffer pool for the bulk COPY (spills to disk beyond it). The
    # read-only bench reopens with a small cap (engines.py LADYBUG_BUFFER_POOL).
    # At full-Wikidata scale (766M rels) the rel COPY needs a large pool or it
    # fails with "buffer pool is full"; size it via LADYBUG_LOAD_BUFFER_POOL.
    bp = int(os.environ.get("LADYBUG_LOAD_BUFFER_POOL", str(4 * 1024 * 1024 * 1024)))
    db = lb.Database(db_path, read_only=False, buffer_pool_size=bp)
    conn = lb.Connection(db)
    # dump_id (the CSR id edges reference) is the primary key; wikidata_id/name are
    # plain columns — LadybugDB has no secondary index, so wikidata_id lookups scan.
    conn.execute(
        f"CREATE NODE TABLE {label}(dump_id INT64, wikidata_id INT64, name STRING, "
        f"PRIMARY KEY(dump_id))"
    ).get_all()
    conn.execute(f'COPY {label} FROM "{nodes_csv}" (HEADER=false)').get_all()
    print(f"  ladybug nodes copied from {nodes_csv}", flush=True)
    conn.execute(f"CREATE REL TABLE {reltype}(FROM {label} TO {label})").get_all()
    conn.execute(f'COPY {reltype} FROM "{edges_csv}" (HEADER=false)').get_all()
    print(f"  ladybug rels copied from {edges_csv}", flush=True)
    n = conn.execute(f"MATCH (n:{label}) RETURN count(n)").get_all()[0][0]
    conn.close()
    db.close()
    with open(os.path.join(out_dir, f"{graph}.meta.json"), "w", encoding="utf-8") as f:
        json.dump({"db_path": db_path, "label_to_primary": {label: label},
                   "vector_indexes": []}, f, indent=2)
    print(f"loaded ladybug {db_path}: {n} nodes", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", required=True)
    ap.add_argument("--edges", required=True)
    ap.add_argument("--graph", required=True)
    ap.add_argument("--label", default="Entity")
    ap.add_argument("--reltype", default="LINK")
    ap.add_argument("--out-dir", default="/data/ladybug")
    args = ap.parse_args()
    load(args.nodes, args.edges, args.graph, args.out_dir, args.label, args.reltype)


if __name__ == "__main__":
    main()
