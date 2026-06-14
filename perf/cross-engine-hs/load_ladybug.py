#!/usr/bin/env python3
"""Load a slater primitive-Cypher dump into an embedded LadybugDB database.

LadybugDB (Kùzu-derived) requires tables before `CREATE` and does not support
multi-label nodes. This loader maps each dumped node to ONE primary node table
(the globally most-common of its labels), stores all labels in a `__labels`
pipe-string, and writes a `<graph>.meta.json` whose `label_to_primary` map drives
`engines._ladybug_rewriter` (it rewrites secondary-label patterns back to the
primary table + a `__labels CONTAINS '|Label|'` filter).

Adapted from the adsharma/slater fork's loader. Two fork-only DDL calls were
dropped because no published `real_ladybug` (nor the master source) accepts them:
  * `CALL enable_default_hash_index=false;`  -> "Invalid option name"
  * `CREATE ART INDEX FOR (n:T) ON (n.__dump_id__);`  -> parser error
The `PRIMARY KEY(__dump_id__)` already provides the join/lookup index, so the
rels-by-__dump_id__ MATCH still resolves.

Usage:
    load_ladybug.py <dump.cypher> --graph <name> [--out-dir /data/ladybug]
"""
import argparse
import csv
import glob
import json
import os
import shutil
import tempfile
from collections import Counter, defaultdict

import real_ladybug as lb

from load_cypher import chunks, parse

BATCH = 2000


def qid(s):
    return "`" + s.replace("`", "``") + "`"


def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def infer_type(values):
    """Pick a LadybugDB column type that fits EVERY non-null value (not just the
    first sample — the dump has columns that mix numbers and strings, and lists of
    strings, both of which a single-sample guess gets wrong and the loader then
    fails to cast). Falls back to STRING for anything heterogeneous; `coerce()`
    stringifies the stragglers on load."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "STRING"
    if all(isinstance(v, bool) for v in non_null):
        return "BOOL"
    if all(isinstance(v, list) for v in non_null):
        elems = [e for v in non_null for e in v if e is not None]
        if elems and all(isinstance(e, str) for e in elems):
            base = "STRING"
        elif any(isinstance(e, float) for e in elems):
            base = "FLOAT"
        else:
            base = "INT64"
        lengths = {len(v) for v in non_null}
        return f"{base}[{lengths.pop()}]" if len(lengths) == 1 else f"{base}[]"
    if all(_is_int(v) for v in non_null):
        return "INT64"
    if all(_is_int(v) or isinstance(v, float) for v in non_null):
        return "DOUBLE"
    return "STRING"


def coerce(value, ktype):
    """Coerce a Python value to match its declared column type so inserts never
    fail a cast (e.g. a stray number in a STRING column)."""
    if value is None:
        return None
    if ktype == "STRING":
        return value if isinstance(value, str) else json.dumps(value)
    if ktype == "DOUBLE":
        return float(value)
    if ktype == "INT64":
        return int(value)
    if ktype == "BOOL":
        return bool(value)
    return value  # array types: left as-is


def node_create_props(props):
    parts = ["__dump_id__: r.id"]
    parts.extend(f"{qid(p)}: r.props.{qid(p)}" for p in props)
    parts.append("__labels: r.labels")
    return "{ " + ", ".join(parts) + " }"


def rel_create_props(props):
    if not props:
        return ""
    return " { " + ", ".join(f"{qid(p)}: r.props.{qid(p)}" for p in props) + " }"


def load(dump, graph, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(out_dir, f"{graph}.lbug")
    meta_path = os.path.join(out_dir, f"{graph}.meta.json")
    # Clear the DB file and any sidecars (.wal / .tmp) — a stale .wal left by an
    # interrupted load otherwise blocks reopening ("Database ID ... does not match").
    for path in glob.glob(db_path + "*"):
        shutil.rmtree(path) if os.path.isdir(path) else os.unlink(path)

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

    # Bound the buffer pool (default grabs ~80% of host RAM during load).
    db = lb.Database(db_path, read_only=False, buffer_pool_size=2 * 1024 * 1024 * 1024)
    conn = lb.Connection(db)

    by_primary = defaultdict(list)
    for did, labels, props in nodes:
        by_primary[node_primary[did]].append((did, labels, props))

    for table, rws in sorted(by_primary.items()):
        prop_values = defaultdict(list)
        for _did, _labels, props in rws:
            for k, v in props.items():
                prop_values[k].append(v)
        col_types = {k: infer_type(v) for k, v in prop_values.items()}
        cols = ["__dump_id__ INT64"]
        cols.extend(f"{qid(k)} {col_types[k]}" for k in sorted(prop_values))
        cols.append("__labels STRING")
        ddl = f"CREATE NODE TABLE {qid(table)}({', '.join(cols)}, PRIMARY KEY(__dump_id__));"
        conn.execute(ddl).get_all()

        props = sorted(prop_values)
        query = f"UNWIND $rows AS r CREATE (n:{qid(table)} {node_create_props(props)})"
        batch_rows = [
            {
                "id": did,
                "labels": "|" + "|".join(labels) + "|",
                "props": {p: coerce(props_map.get(p), col_types[p]) for p in props},
            }
            for did, labels, props_map in rws
        ]
        for ch in chunks(batch_rows, BATCH):
            conn.execute(query, {"rows": ch}).get_all()
        print(f"  ladybug nodes :{table} -> {len(rws)}", flush=True)

    rel_pairs = defaultdict(set)
    rel_props = defaultdict(lambda: defaultdict(list))
    for a, b, t, props in rels:
        rel_pairs[t].add((node_primary[a], node_primary[b]))
        for k, v in props.items():
            rel_props[t][k].append(v)

    rel_col_types = {
        typ: {k: infer_type(v) for k, v in props.items()} for typ, props in rel_props.items()
    }
    for typ, pairs in sorted(rel_pairs.items()):
        cols = [f"{qid(k)} {rel_col_types[typ][k]}" for k in sorted(rel_props[typ])]
        from_to = ", ".join(f"FROM {qid(a)} TO {qid(b)}" for a, b in sorted(pairs))
        ddl = f"CREATE REL TABLE {qid(typ)}({from_to}{', ' if cols else ''}{', '.join(cols)});"
        conn.execute(ddl).get_all()

    # Relationships go in via COPY FROM a CSV: a per-row `UNWIND ... MATCH (a {pk})`
    # does NOT become a primary-key seek in LadybugDB — it scans the node table per
    # row, i.e. O(nodes x rels) (hours at 340k nodes). COPY joins on the primary key
    # in bulk (sub-second). CSV columns are FROM-pk, TO-pk, then props in sorted
    # order. Only single-(src,dst)-pair, scalar-prop rel types can take this path;
    # multi-pair or array-prop types fall back to the slow per-row insert (rare, and
    # only on the smaller graphs).
    tmpdir = tempfile.mkdtemp(prefix="ladybug-rels-")
    by_rel_shape = defaultdict(list)
    for a, b, typ, props in rels:
        by_rel_shape[(typ, node_primary[a], node_primary[b])].append((a, b, props))
    for (typ, src, dst), rws in sorted(by_rel_shape.items()):
        props = sorted(rel_props[typ])
        single_pair = len(rel_pairs[typ]) == 1
        array_prop = any("[" in rel_col_types[typ][p] for p in props)
        if single_pair and not array_prop:
            path = os.path.join(tmpdir, f"{typ}.csv")
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                for a, b, props_map in rws:
                    w.writerow([a, b] + [coerce(props_map.get(p), rel_col_types[typ][p]) for p in props])
            conn.execute(f"COPY {qid(typ)} FROM '{path}' (HEADER=false)").get_all()
        else:
            query = (
                f"UNWIND $rows AS r MATCH (a:{qid(src)} {{__dump_id__: r.a}}), "
                f"(b:{qid(dst)} {{__dump_id__: r.b}}) "
                f"CREATE (a)-[e:{qid(typ)}{rel_create_props(props)}]->(b)"
            )
            batch_rows = [
                {"a": a, "b": b,
                 "props": {p: coerce(props_map.get(p), rel_col_types[typ][p]) for p in props}}
                for a, b, props_map in rws
            ]
            for ch in chunks(batch_rows, BATCH):
                conn.execute(query, {"rows": ch}).get_all()
        print(f"  ladybug rels :{typ} {src}->{dst} -> {len(rws)}", flush=True)
    shutil.rmtree(tmpdir, ignore_errors=True)

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
    ap.add_argument("--out-dir", default="/data/ladybug")
    args = ap.parse_args()
    load(args.dump, args.graph, args.out_dir)


if __name__ == "__main__":
    main()
