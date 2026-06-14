#!/usr/bin/env python3
"""Average the N runs per engine/query and print a comparison table + memory.
Verdict marker is slater-vs-competitors with a 25% parity band (same as the pole
aggregate). Query order is read from the run JSONs (insertion order preserved).

Usage: aggregate_hs.py <results_dir>
"""
import json, glob, statistics as st, os, sys

RES = sys.argv[1] if len(sys.argv) > 1 else "/tmp/bench-hs/results"
ENGINES = ["slater", "neo4j", "memgraph", "falkordb", "arcadedb", "ladybug"]
LABEL = {"slater": "slater", "neo4j": "Neo4j 5", "memgraph": "Memgraph",
         "falkordb": "FalkorDB", "arcadedb": "ArcadeDB", "ladybug": "LadybugDB"}

# discover query order from the first available run file
ORDER = []
for e in ENGINES:
    fs = sorted(glob.glob(f"{RES}/{e}.run*.json"))
    if fs:
        try:
            ORDER = list(json.load(open(fs[0])).keys()); break
        except Exception:
            pass

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
        vals = [r[q] for r in runs if r.get(q) is not None]
        avg[e][q] = round(st.mean(vals), 2) if vals else None


def slater_marker(q):
    vals = {e: avg[e][q] for e in ENGINES if avg[e][q] is not None}
    if "slater" not in vals:
        return ""
    best = min(vals.values())
    winners = [e for e, v in vals.items() if v <= best * 1.25]
    if "slater" not in winners:
        return ""
    return "🟢" if len(winners) == 1 else "⚪"


print(f"runs per engine: {nruns}\n")
hdr = "| query | " + " | ".join(LABEL[e] for e in ENGINES) + " |"
print(hdr)
print("|---|" + "--:|" * len(ENGINES))
for q in ORDER:
    cells = []
    for e in ENGINES:
        v = avg[e][q]
        s = f"{v:.2f} ms" if v is not None else "—"
        if e == "slater":
            m = slater_marker(q); s = f"{s} {m}".strip()
        cells.append(s)
    print(f"| {q} | " + " | ".join(cells) + " |")

print("\n--- memory ---")
mt = os.path.join(RES, "memory.txt")
if os.path.exists(mt):
    mem = {}
    for line in open(mt):
        parts = dict(p.split("=") for p in line.split()[1:]) if len(line.split()) > 1 else {}
        e = line.split()[0]
        mem[e] = (int(parts.get("peak", 0)), int(parts.get("current", 0)))

    def mem_marker(idx):
        """🟢 if slater has the smallest RSS; ⚪ if it ties (another within 25%);
        '' if another engine is clearly smaller. Smaller is better, same band as latency."""
        vals = {e: m[idx] for e, m in mem.items() if m[idx] > 0}
        if "slater" not in vals:
            return ""
        best = min(vals.values())
        winners = [e for e, v in vals.items() if v <= best * 1.25]
        if "slater" not in winners:
            return ""
        return "🟢" if len(winners) == 1 else "⚪"

    pk_mark, cur_mark = mem_marker(0), mem_marker(1)
    for e in ENGINES:
        if e not in mem:
            continue
        pk, cur = mem[e]
        pkm = f" {pk_mark}" if e == "slater" and pk_mark else ""
        curm = f" {cur_mark}" if e == "slater" and cur_mark else ""
        print(f"{LABEL.get(e, e):10s} peak={pk/1048576:7.0f} MiB{pkm}  current={cur/1048576:7.0f} MiB{curm}")
