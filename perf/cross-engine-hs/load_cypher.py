#!/usr/bin/env python3
"""Load a slater primitive-Cypher dump into Neo4j / Memgraph / FalkorDB.

The hs-backend-spot `slater-snapshot` reference graphs are emitted as flat
FalkorDB-dialect Cypher (the dialect `slater-build` ingests):

    CREATE (:L1:L2:__DumpVertex__ {__dump_id__: N, ...props});
    MATCH (a:__DumpVertex__ {__dump_id__:X}), (b:__DumpVertex__ {__dump_id__:Y}) CREATE (a)-[:T]->(b);
    CREATE INDEX FOR (n:L) ON (n.p);
    CALL db.idx.vector.createNodeIndex('L', 'embedding', 1024, 'cosine');

slater itself loads this via `slater-build`. The *other* engines have no such
ingester, so this script parses the dump once (a quote-aware tokenizer for the
property map — robust to commas/colons inside string values and to `vecf32([...])`
vector literals) and bulk-loads each target with the same UNWIND-batch pattern as
perf/cross-engine/load_graph.py: a temp `__DumpVertex__(__dump_id__)` join index,
nodes grouped by label-set, rels grouped by type, the real range indexes, then the
join artifacts are stripped. Vector indexes are the only per-engine dialect fork.

Usage:
    load_cypher.py verify  <dump.cypher>
    load_cypher.py neo4j   <dump.cypher> --uri bolt://localhost:7691 --pass <pw>
    load_cypher.py memgraph <dump.cypher> --uri bolt://localhost:7692
    load_cypher.py falkordb <dump.cypher> --port 6401 --graph <name>
"""
import sys, re, time, argparse
from collections import defaultdict

# ---------------------------------------------------------------------------
# Quote-aware parser for a Cypher map literal `{k: v, k: v, ...}`.
# Values: double-quoted string (\" \\ \n escapes), number, [list], true/false/
# null, or vecf32([...]) (treated as a plain list of floats).
# ---------------------------------------------------------------------------
class _P:
    def __init__(self, s, i):
        self.s, self.i = s, i

    def ws(self):
        while self.i < len(self.s) and self.s[self.i] in " \t\r\n":
            self.i += 1

    def map(self):
        self.ws(); assert self.s[self.i] == "{"; self.i += 1
        out = {}
        self.ws()
        if self.s[self.i] == "}":
            self.i += 1; return out
        while True:
            self.ws()
            # key: bare identifier (possibly backtick-quoted), up to ':'
            if self.s[self.i] == "`":
                j = self.s.index("`", self.i + 1); key = self.s[self.i + 1:j]; self.i = j + 1
            else:
                j = self.i
                while self.s[self.i] not in " \t\r\n:":
                    self.i += 1
                key = self.s[j:self.i]
            self.ws(); assert self.s[self.i] == ":"; self.i += 1
            out[key] = self.value()
            self.ws()
            c = self.s[self.i]; self.i += 1
            if c == "}":
                return out
            assert c == ",", f"expected , or }} at {self.i}: {self.s[self.i-1:self.i+20]!r}"

    def value(self):
        self.ws(); c = self.s[self.i]
        if c == '"':
            return self.string()
        if c == "[":
            return self.list()
        if c == "{":
            return self.map()
        # vecf32([...]) -> list
        if self.s.startswith("vecf32", self.i):
            self.i += 6; self.ws(); assert self.s[self.i] == "("; self.i += 1
            self.ws(); v = self.list(); self.ws(); assert self.s[self.i] == ")"; self.i += 1
            return v
        # literal token: number / true / false / null
        j = self.i
        while self.i < len(self.s) and self.s[self.i] not in ",]}) \t\r\n":
            self.i += 1
        tok = self.s[j:self.i]
        if tok == "true":  return True
        if tok == "false": return False
        if tok == "null":  return None
        return float(tok) if ("." in tok or "e" in tok or "E" in tok) else int(tok)

    def string(self):
        assert self.s[self.i] == '"'; self.i += 1; out = []
        while True:
            c = self.s[self.i]; self.i += 1
            if c == "\\":
                e = self.s[self.i]; self.i += 1
                out.append({"n": "\n", "t": "\t", "r": "\r"}.get(e, e))
            elif c == '"':
                return "".join(out)
            else:
                out.append(c)

    def list(self):
        assert self.s[self.i] == "["; self.i += 1; out = []
        self.ws()
        if self.s[self.i] == "]":
            self.i += 1; return out
        while True:
            out.append(self.value()); self.ws()
            c = self.s[self.i]; self.i += 1
            if c == "]":
                return out
            assert c == ",", f"expected , or ] at {self.i}"


_NODE_HEAD = re.compile(r"^CREATE \(:([^ {]+)\s*\{")
_REL = re.compile(
    r"^MATCH \(a:__DumpVertex__ \{__dump_id__:\s*(\d+)\}\), "
    r"\(b:__DumpVertex__ \{__dump_id__:\s*(\d+)\}\) CREATE \(a\)-\[:(\w+)\s*(\{.*\})?\]->\(b\);")
_RANGE_IDX = re.compile(r"^CREATE INDEX FOR \(n:(\w+)\) ON \(n\.(\w+)\);")
_VEC_IDX = re.compile(
    r"^CALL db\.idx\.vector\.createNodeIndex\('(\w+)',\s*'(\w+)',\s*(\d+),\s*'(\w+)'\);")


def parse(path):
    """Stream-parse the dump → (nodes, rels, range_idx, vec_idx).

    nodes: list of (dump_id:int, labels:tuple[str], props:dict)  (labels exclude __DumpVertex__)
    rels:  list of (a_dump_id:int, b_dump_id:int, type:str, props:dict)
    range_idx: list of (label, prop);  vec_idx: list of (label, prop, dim, metric)
    """
    nodes, rels, range_idx, vec_idx = [], [], [], []
    with open(path, encoding="utf-8") as f:
        for ln in f:
            if ln.startswith("CREATE (:"):
                m = _NODE_HEAD.match(ln)
                labels = tuple(l for l in m.group(1).split(":") if l != "__DumpVertex__")
                props = _P(ln, ln.index("{")).map()
                did = props.pop("__dump_id__")
                nodes.append((did, labels, props))
            elif ln.startswith("MATCH (a:__DumpVertex__"):
                m = _REL.match(ln)
                eprops = _P(m.group(4), 0).map() if m.group(4) else {}
                rels.append((int(m.group(1)), int(m.group(2)), m.group(3), eprops))
            elif ln.startswith("CREATE INDEX"):
                m = _RANGE_IDX.match(ln); range_idx.append((m.group(1), m.group(2)))
            elif ln.startswith("CALL db.idx.vector"):
                m = _VEC_IDX.match(ln)
                vec_idx.append((m.group(1), m.group(2), int(m.group(3)), m.group(4)))
    return nodes, rels, range_idx, vec_idx


def chunks(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


BATCH = 2000


def load(target, nodes, rels, range_idx, vec_idx, args):
    # ---- target runner -----------------------------------------------------
    if target == "falkordb":
        from falkordb import FalkorDB
        g = FalkorDB(host="localhost", port=args.port).select_graph(args.graph)
        def run(q, params=None): return g.query(q, params or {})
        def idx(lab, prop): run(f"CREATE INDEX FOR (n:{lab}) ON (n.{prop})")
        join_idx = "CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__)"
        def vindex(lab, prop, dim, metric):
            # FalkorDB v4 dropped the db.idx.vector.createNodeIndex procedure in
            # favour of CREATE VECTOR INDEX … OPTIONS {…}.
            sim = {"cosine": "cosine", "euclidean": "euclidean"}.get(metric, "cosine")
            # efRuntime default is too low here — it returns ~k/2 hits for k up to 50.
            # 256 restores full top-k recall (a correctness floor, not over-tuning).
            run(f"CREATE VECTOR INDEX FOR (n:{lab}) ON (n.{prop}) "
                f"OPTIONS {{dimension:{dim}, similarityFunction:'{sim}', efRuntime:256}}")
    else:
        from neo4j import GraphDatabase
        auth = (args.user, args.password) if target == "neo4j" else ("", "")
        drv = GraphDatabase.driver(args.uri, auth=auth)
        sess = drv.session()
        def run(q, params=None): return sess.run(q, params or {}).consume()
        def idx(lab, prop): run(f"CREATE INDEX FOR (n:{lab}) ON (n.{prop})")
        join_idx = "CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__)"
        if target == "neo4j":
            def vindex(lab, prop, dim, metric):
                sim = {"cosine": "cosine", "euclidean": "euclidean"}.get(metric, "cosine")
                run(f"CREATE VECTOR INDEX {lab}_{prop} IF NOT EXISTS FOR (n:{lab}) ON n.{prop} "
                    f"OPTIONS {{indexConfig:{{`vector.dimensions`:{dim},"
                    f"`vector.similarity_function`:'{sim}'}}}}")
        else:  # memgraph
            def vindex(lab, prop, dim, metric):
                metric_mg = {"cosine": "cos", "euclidean": "l2sq"}.get(metric, "cos")
                cap = 2048
                run(f"CREATE VECTOR INDEX {lab}_{prop} ON :{lab}({prop}) WITH CONFIG "
                    f'{{"dimension":{dim},"metric":"{metric_mg}","capacity":{cap}}}')

    t0 = time.time()
    run(join_idx)
    # nodes grouped by label-set
    by_labels = defaultdict(list)
    for did, labels, props in nodes:
        by_labels[labels].append({"id": did, "props": props})
    for labels, rws in by_labels.items():
        lab = ":".join(list(labels) + ["__DumpVertex__"])
        # FalkorDB indexes vectors only when the property is a vecf32 type; a plain
        # SET from a JSON list stores an un-indexable array. Re-wrap it on load.
        emb = ""
        if target == "falkordb" and "embedding" in rws[0]["props"]:
            emb = " SET n.embedding = vecf32(r.props.embedding)"
        q = f"UNWIND $rows AS r CREATE (n:{lab}) SET n += r.props SET n.__dump_id__ = r.id{emb}"
        for ch in chunks(rws, BATCH):
            run(q, {"rows": ch})
        print(f"  nodes :{':'.join(labels)} -> {len(rws)}", flush=True)
    # rels grouped by type
    by_type = defaultdict(list)
    has_props = defaultdict(bool)
    for a, b, t, props in rels:
        by_type[t].append({"a": a, "b": b, "props": props})
        if props:
            has_props[t] = True
    for t, rows in by_type.items():
        setp = " SET e += r.props" if has_props[t] else ""
        q = (f"UNWIND $rows AS r MATCH (a:__DumpVertex__ {{__dump_id__:r.a}}), "
             f"(b:__DumpVertex__ {{__dump_id__:r.b}}) CREATE (a)-[e:{t}]->(b){setp}")
        for ch in chunks(rows, BATCH):
            run(q, {"rows": ch})
        print(f"  rels :{t} -> {len(rows)}", flush=True)
    # real range indexes
    for lab, prop in range_idx:
        try:
            idx(lab, prop)
        except Exception as e:
            print(f"  range idx {lab}.{prop} failed: {e}", flush=True)
    # vector indexes (per-engine DDL)
    for lab, prop, dim, metric in vec_idx:
        try:
            vindex(lab, prop, dim, metric)
        except Exception as e:
            print(f"  vector idx {lab}.{prop} FAILED: {e}", flush=True)
    # strip temp join artifacts
    ids = [n[0] for n in nodes]
    for ch in chunks(ids, BATCH * 5):
        run("UNWIND $ids AS i MATCH (n:__DumpVertex__ {__dump_id__:i}) "
            "REMOVE n:__DumpVertex__ REMOVE n.__dump_id__", {"ids": ch})
    try:
        if target == "falkordb":
            run("DROP INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__)")
        else:
            run("DROP INDEX __DumpVertex___dump_id__ IF EXISTS") if target == "neo4j" else \
                run("DROP INDEX ON :__DumpVertex__(__dump_id__)")
    except Exception as e:
        print(f"  drop temp idx: {e}", flush=True)
    print(f"loaded {target} in {time.time()-t0:.1f}s "
          f"({len(nodes)} nodes / {len(rels)} rels)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", choices=["verify", "neo4j", "memgraph", "falkordb"])
    ap.add_argument("dump")
    ap.add_argument("--uri", default="bolt://localhost:7687")
    ap.add_argument("--user", default="neo4j")
    ap.add_argument("--pass", dest="password", default="")
    ap.add_argument("--port", type=int, default=6401)
    ap.add_argument("--graph", default="g")
    args = ap.parse_args()

    t0 = time.time()
    nodes, rels, range_idx, vec_idx = parse(args.dump)
    print(f"parsed {len(nodes)} nodes / {len(rels)} rels / {len(range_idx)} range-idx / "
          f"{len(vec_idx)} vec-idx in {time.time()-t0:.1f}s", flush=True)
    if args.target == "verify":
        labset = defaultdict(int)
        for _, labels, _ in nodes:
            labset[labels] += 1
        print("label-sets:")
        for ls, c in sorted(labset.items(), key=lambda x: -x[1])[:12]:
            print(f"  {':'.join(ls):40s} {c}")
        veccount = sum(1 for _, _, p in nodes if "embedding" in p)
        print(f"nodes with embedding: {veccount}")
        return
    load(args.target, nodes, rels, range_idx, vec_idx, args)


if __name__ == "__main__":
    main()
