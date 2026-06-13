#!/usr/bin/env python3
"""Load a primitive-Cypher dump into an embedded LadybugDB database.

LadybugDB requires tables before `CREATE` and does not create nodes with multiple
labels. This loader maps each dumped node to one primary node table, stores all
labels in `__labels`, and writes a metadata file used by `engines.py` to rewrite
secondary-label queries back to the primary table.

Usage:
    load_ladybug.py <dump.cypher> --graph <name> [--out-dir /tmp/bench-hs/ladybug]
"""
import argparse
import contextlib
import json
import os
import shutil
from collections import Counter, defaultdict

import ladybug as lb

from load_cypher import chunks, parse

BATCH = 2000


def qid(s):
    return "`" + s.replace("`", "``") + "`"


def cypher_type(values):
    sample = next((v for v in values if v is not None), None)
    if sample is None:
        return "STRING"
    if isinstance(sample, bool):
        return "BOOL"
    if isinstance(sample, int) and not isinstance(sample, bool):
        return "INT64"
    if isinstance(sample, float):
        return "DOUBLE"
    if isinstance(sample, list):
        inner = next((v for v in sample if v is not None), 0.0)
        base = "FLOAT" if isinstance(inner, float) else "INT64"
        return f"{base}[{len(sample)}]" if sample else f"{base}[]"
    return "STRING"


def prop_expr(prop):
    return f"{qid(prop)}: r.props.{qid(prop)}"


def create_props(props):
    parts = ["__dump_id__: r.id"]
    parts.extend(prop_expr(p) for p in props)
    parts.append("__labels: r.labels")
    return "{ " + ", ".join(parts) + " }"


def load(dump, graph, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(out_dir, f"{graph}.lbug")
    meta_path = os.path.join(out_dir, f"{graph}.meta.json")
    if os.path.isdir(db_path):
        shutil.rmtree(db_path)
    else:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(db_path)

    nodes, rels, _range_idx, vec_idx = parse(dump)
    global_label_counts = Counter(label for _did, labels, _props in nodes for label in labels)

    def primary_label(labels):
        return max(labels, key=lambda label: global_label_counts[label]) if labels else "Node"

    node_primary = {did: primary_label(labels) for did, labels, _ in nodes}

    label_counts = defaultdict(Counter)
    for _did, labels, _props in nodes:
        p = primary_label(labels)
        for label in labels:
            label_counts[label][p] += 1
    label_to_primary = {
        label: counts.most_common(1)[0][0] for label, counts in label_counts.items()
    }

    db = lb.Database(db_path, read_only=False)
    conn = lb.Connection(db)
    conn.execute("CALL enable_default_hash_index=false;").get_all()

    by_primary = defaultdict(list)
    for did, labels, props in nodes:
        by_primary[node_primary[did]].append((did, labels, props))

    for table, rows in sorted(by_primary.items()):
        prop_values = defaultdict(list)
        for _did, _labels, props in rows:
            for k, v in props.items():
                prop_values[k].append(v)
        cols = ["__dump_id__ INT64"]
        cols.extend(f"{qid(k)} {cypher_type(v)}" for k, v in sorted(prop_values.items()))
        cols.append("__labels STRING")
        ddl = f"CREATE NODE TABLE {qid(table)}({', '.join(cols)}, PRIMARY KEY(__dump_id__));"
        conn.execute(ddl).get_all()
        conn.execute(f"CREATE ART INDEX FOR (n:{qid(table)}) ON (n.__dump_id__);").get_all()

        props = sorted(prop_values)
        query = f"UNWIND $rows AS r CREATE (n:{qid(table)} {create_props(props)})"
        batch_rows = [
            {
                "id": did,
                "labels": "|" + "|".join(labels) + "|",
                "props": {p: props_map.get(p) for p in props},
            }
            for did, labels, props_map in rows
        ]
        for ch in chunks(batch_rows, BATCH):
            conn.execute(query, {"rows": ch}).get_all()
        print(f"  ladybug nodes :{table} -> {len(rows)}", flush=True)

    rel_pairs = defaultdict(set)
    rel_props = defaultdict(lambda: defaultdict(list))
    for a, b, t, props in rels:
        rel_pairs[t].add((node_primary[a], node_primary[b]))
        for k, v in props.items():
            rel_props[t][k].append(v)

    for typ, pairs in sorted(rel_pairs.items()):
        cols = []
        cols.extend(f"{qid(k)} {cypher_type(v)}" for k, v in sorted(rel_props[typ].items()))
        from_to = ", ".join(f"FROM {qid(a)} TO {qid(b)}" for a, b in sorted(pairs))
        ddl = f"CREATE REL TABLE {qid(typ)}({from_to}{', ' if cols else ''}{', '.join(cols)});"
        conn.execute(ddl).get_all()

    by_rel_shape = defaultdict(list)
    for a, b, typ, props in rels:
        by_rel_shape[(typ, node_primary[a], node_primary[b])].append((a, b, props))
    for (typ, src, dst), rows in sorted(by_rel_shape.items()):
        props = sorted(rel_props[typ])
        prop_map = " " + create_props(props).replace("__dump_id__: r.id, ", "").replace(", __labels: r.labels", "") if props else ""
        query = (
            f"UNWIND $rows AS r MATCH (a:{qid(src)} {{__dump_id__: r.a}}), "
            f"(b:{qid(dst)} {{__dump_id__: r.b}}) CREATE (a)-[e:{qid(typ)}{prop_map}]->(b)"
        )
        batch_rows = [
            {"a": a, "b": b, "props": {p: props_map.get(p) for p in props}}
            for a, b, props_map in rows
        ]
        for ch in chunks(batch_rows, BATCH):
            conn.execute(query, {"rows": ch}).get_all()
        print(f"  ladybug rels :{typ} {src}->{dst} -> {len(rows)}", flush=True)

    conn.close()
    db.close()
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "db_path": db_path,
                "label_to_primary": label_to_primary,
                "vector_indexes": vec_idx,
            },
            f,
            indent=2,
            sort_keys=True,
        )
    print(f"loaded ladybug in {db_path} ({len(nodes)} nodes / {len(rels)} rels)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dump")
    ap.add_argument("--graph", required=True)
    ap.add_argument("--out-dir", default="/tmp/bench-hs/ladybug")
    args = ap.parse_args()
    load(args.dump, args.graph, args.out_dir)


if __name__ == "__main__":
    main()
