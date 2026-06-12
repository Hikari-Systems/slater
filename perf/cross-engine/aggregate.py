#!/usr/bin/env python3
"""Average the 5 runs per engine/query and print a comparison table.
Verdict columns are slater-vs-competitor with a 25% parity band."""
import json, glob, statistics as st, os

ENGINES = ["slater", "neo4j", "memgraph", "falkordb"]
LABEL = {"slater":"slater", "neo4j":"Neo4j 5", "memgraph":"Memgraph", "falkordb":"FalkorDB"}
RES = "/tmp/bench/results"

# query order
ORDER = ["count all nodes","Crime label count","point lookup (idx nhs_no)",
    "idx-eq count (Crime.type)","1-hop Crime->Location","2-hop Person->Loc->Area",
    "agg crimes by type","3-hop Officer/Crime/Loc","full-scan CONTAINS","count DISTINCT type"]

avg = {e: {} for e in ENGINES}
nruns = {e: 0 for e in ENGINES}
for e in ENGINES:
    runs = []
    for f in sorted(glob.glob(f"{RES}/{e}.run*.json")):
        try:
            runs.append(json.load(open(f)))
        except Exception:
            pass
    nruns[e] = len(runs)
    for q in ORDER:
        vals = [r[q] for r in runs if q in r]
        avg[e][q] = round(st.mean(vals), 2) if vals else None

def slater_marker(q):
    """🟢 if slater is the sole fastest; ⚪ if slater ties for fastest (another
    engine within 25%); '' if some other engine is clearly faster. Marks slater
    only — competitors carry no marker."""
    vals = {e: avg[e][q] for e in ENGINES if avg[e][q] is not None}
    if "slater" not in vals: return ""
    best = min(vals.values())
    winners = [e for e, v in vals.items() if v <= best * 1.25]   # within 25% of best
    if "slater" not in winners: return ""           # slater beaten
    return "🟢" if len(winners) == 1 else "⚪"        # sole vs tied lead

print(f"runs per engine: {nruns}\n")
# raw averaged table with the slater marker on slater's column
hdr = "| query | " + " | ".join(LABEL[e] for e in ENGINES) + " |"
print(hdr); print("|---|" + "--:|"*len(ENGINES))
for q in ORDER:
    cells = []
    for e in ENGINES:
        v = avg[e][q]
        s = f"{v:.2f}" if v is not None else "—"
        if e == "slater":
            m = slater_marker(q); s = f"{s} {m}".strip()
        cells.append(s)
    print(f"| {q} | " + " | ".join(cells) + " |")

# memory — mark slater the same way as latency (🟢 sole-smallest RSS, ⚪ ties <25%)
print("\n--- memory (run-5 cycle) ---")
mt = os.path.join(RES, "memory.txt")
if os.path.exists(mt):
    mem = {}
    for line in open(mt):
        parts = dict(p.split("=") for p in line.split()[1:]) if len(line.split())>1 else {}
        mem[line.split()[0]] = (int(parts.get("peak",0)), int(parts.get("current",0)))

    def mem_marker(idx):
        vals = {e: m[idx] for e, m in mem.items() if m[idx] > 0}
        if "slater" not in vals: return ""
        best = min(vals.values())
        winners = [e for e, v in vals.items() if v <= best * 1.25]
        if "slater" not in winners: return ""
        return "🟢" if len(winners) == 1 else "⚪"

    pk_mark, cur_mark = mem_marker(0), mem_marker(1)
    for e in ENGINES:
        if e not in mem: continue
        pk, cur = mem[e]
        pkm = f" {pk_mark}" if e == "slater" and pk_mark else ""
        curm = f" {cur_mark}" if e == "slater" and cur_mark else ""
        print(f"{LABEL.get(e,e):10s} peak={pk/1048576:7.0f} MiB{pkm}  current={cur/1048576:7.0f} MiB{curm}")
