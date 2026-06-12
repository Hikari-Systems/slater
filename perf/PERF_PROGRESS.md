# Slater cold-execution performance — staged fix tracker

> **READ THIS FIRST (resuming in a fresh/clear context).**
> This file is the single source of truth for this work. To continue:
> 1. Read the **Status** table → find the first stage not `DONE`.
> 2. Read that stage's section under **Stages** (problem, change, files, validate, acceptance).
> 3. Make the change on a branch `perf/stage-N-...`, validate with the harness below.
> 4. Update the stage's **Status** + **Result** here, commit this file with the code, open a PR.
> Do **not** trust ambient memory — everything needed is in this file. Keep the **Baseline**
> table frozen; only append "after" numbers per stage.

## Status

| # | Stage | Risk | Status | Result (uncached median) |
|---|-------|------|--------|--------------------------|
| 0 | Config bool deser (server boots from current source) | low | TODO | — |
| 1 | Parameters → index selection | low | TODO | idx-eq target ≤ ~50 ms (from 901 ms) |
| 2 | Skip redundant per-node label re-check | low | TODO | Crime-label-count target ≈ 14 ms (from 841 ms) |
| 3 | Count fast-paths (metadata / index cardinality) | low | TODO | counts target < 1 ms |
| 4 | Block-cache record read (no per-record copy / less locking) | high | TODO | broad floor drop |
| 5 | Streaming aggregation + projection pushdown + prop memo | med | TODO | — |
| 6 | Traversal frame / streaming (no per-hop binding clone) | med | TODO | 3-hop target low-ms (from 2242 ms) |

Recommended order: **0 → 1 → 2 → 3** (local, high payoff, low risk), then **4 → 5 → 6**.
**Stage 0 is a hard prerequisite** for any server-based validation of Stages 1–6 (see below).

## Background

A faithful export of the "pole" Manchester crime graph (61,521 nodes / 105,840 rels) was loaded
into slater and benchmarked against Neo4j over Bolt. slater wins **memory** (129 MB vs 539 MB at
matched 64 MB cache) and wins **repeated/cached** queries via its result cache (0.4–1.4 ms), but
its **cold/uncached execution is 100–600× slower** than Neo4j on label scans, parameterized
lookups, aggregations and traversals. The causes are local decisions in the planner / executor /
block cache, all fixable without changing slater's architecture or memory story. Each stage below
targets one cause and is independently landable and measurable.

### Root causes (file:line → measured symptom)

1. **Params invisible to index selection.** `crates/slater/src/plan.rs:333` `literal()` matches
   only `Expr::Literal`; `inline_preds` (`plan.rs:203`) and `compare_operands` (`plan.rs:289`)
   drop predicates whose value is a `$param`, so `choose_from_preds` falls to `LabelScan`
   (`plan.rs:246`). → param `{type:$t}` = **901 ms** vs literal `{type:'…'}` = **46 ms**.
2. **Redundant label re-validation.** Candidates already come from the resident label posting
   (`exec.rs:2028` `nodes_with_label`), yet `match_single_pattern` re-checks each via
   `node_ok`→`node_label_ids` (`exec.rs:1748`→`591`), decoding a label record per node.
   `count_all` skips this because its pattern has no label (`exec.rs:2038` guard). →
   `MATCH (c:Crime) RETURN count(c)` = **841 ms** vs `MATCH (n) RETURN count(n)` = **14 ms**.
3. **Per-record global lock + heap copy.** `crates/slater/src/cache.rs:241` `record()` takes a
   single shared `Mutex` (`cache.rs:225`) and returns `rec.to_vec()` (`cache.rs:253`) on every
   node/edge/label/property access. ~30 µs/record dominates every scan and traversal.
4. **Eager full-table materialization; no count fast-path.** `match_single_pattern` builds a
   `Vec<HashMap>`, one clone per row (`exec.rs:1751`); `count` is `indices.len()` over it
   (`exec.rs:2223`). Resident metadata exists (`exec.rs:1332` uses `nodes_with_label().len()`;
   `label_postings` resident at `generation.rs:66`) but the query path ignores it.
5. **Property reads decode the whole map per access, unmemoized.** `node_prop` (`exec.rs:639`)
   decodes the full record and linear-scans for one key; k returned props = k full decodes.
6. **Traversals clone the binding map per hop and buffer all paths** (`exec.rs:1855`, `1892`).
   → 3-hop = **2242 ms** vs Neo4j 3.6 ms.

### Stage 0 prerequisite (why the server won't boot from current source)

`crates/slater/src/config.rs:9-13` documents that `hs_utils::config::load_layered_value()`
stringifies every scalar config leaf. Numeric fields use `deser_*_or_str` helpers, but
`require_acl_stamp: bool` (`config.rs:97`) uses a plain `bool` deserializer, so the stringified
`"true"`/`"false"` fails: `Error: deserialise Slater config … invalid type: string "true",
expected a boolean`. **A current-source build of `slater` (server) therefore cannot start** — the
prebuilt binary used for the baseline predates this. Stage 0 fixes it; until then, server-based
validation of Stages 1–6 is blocked (the baseline harness used the old prebuilt binary).

## Baseline (FROZEN — measured 2026-06-12, matched 64 MB page/block cache)

slater uncached (real execution) vs Neo4j; `perf/bench.py` reproduces these.

| query | slater uncached | slater cached | Neo4j | n4j/slater |
|-------|----------------:|--------------:|------:|-----------:|
| count all nodes | 14 ms | 0.44 ms | 4–9 ms | 1/3.4 |
| Crime label count | 841 ms | 0.44 ms | 2–6 ms | 1/286 |
| point lookup (idx nhs_no) | 13 ms | 0.42 ms | 1.3–5 ms | 1/6 |
| idx-eq count (Crime.type, param) | 901 ms | 0.46 ms | 1.3–5 ms | 1/465 |
| 1-hop Crime→Location (param) | 952–998 ms | 1.34 ms | 2.6–8 ms | 1/245 |
| 2-hop Person→Loc→Area | 45 ms | 1.30 ms | 2.2–8 ms | 1/15 |
| agg crimes by type | 885 ms | 0.53 ms | 9–11 ms | 1/94 |
| 3-hop Officer/Crime/Loc | 2242 ms | 1.33 ms | 3.6–5.5 ms | 1/627 |
| full-scan CONTAINS | 879 ms | 0.44 ms | 5.6–8 ms | 1/158 |
| count DISTINCT type | 890 ms | 0.44 ms | 7–11 ms | 1/120 |

**Memory:** slater **129 MB** RSS vs Neo4j **539 MB** at 64 MB page cache (866 MB at 512 MB).
**Cached note:** slater's result cache cannot be disabled — `cache.rs:422` clamps the budget to
`.max(1)`, so `resultCacheBytes:0` still caches. The "uncached" column uses varying parameters to
force misses; do not "fix" the benchmark by trying to disable the cache.

## Validation harness

### Test data locations (on this machine)

- **slater dump, ready to ingest:** `/home/rickk/git/personal/pole/data/pole-50.slater.cypher`
  (23 MB, Primitive-Cypher, 61,521 nodes / 105,840 rels). Feeds straight into `slater-build`.
- **Source Neo4j dumps:** `/home/rickk/git/personal/pole/data/pole-50.dump` (`-35/-40/-43` too).
- **Exporter (regenerate the slater dump):** `/home/rickk/git/personal/pole/code/neo4j_to_slater.py`.
- **Benchmark:** `perf/bench.py` (this repo).
- **Python driver:** `python3 -m venv /tmp/pole_venv && /tmp/pole_venv/bin/pip install neo4j`
  (host Python is PEP-668 externally-managed; use a venv).

### Build (Docker only — no host Rust toolchain)

```bash
cd /home/rickk/git/hs/slater
docker build -t slater:local .          # compiles current source into the image
# Optional unit tests via a throwaway builder (slow; installs build deps):
docker run --rm -v "$PWD":/src -w /src rust:1-bookworm bash -lc \
  'apt-get update && apt-get install -y cmake clang libclang-dev >/dev/null && cargo test'
```

### Ingest the pole image

```bash
docker run --rm -v slater_pole:/data \
  -v /home/rickk/git/personal/pole/data:/dumps:ro \
  --entrypoint /app/slater-build slater:local \
  --input /dumps/pole-50.slater.cypher --graph pole --data-dir /data
# Acceptance: stdout reports "61521 nodes, 105840 edges".
```

### Serve (requires Stage 0 so the server boots)

Mint a password hash and ACL, then run the image:

```bash
docker run --rm --entrypoint /app/slater slater:local hash-password polereader   # -> $argon2id$...
mkdir -p /tmp/slater-acl
cat > /tmp/slater-acl/acl.json <<'JSON'
{ "users": { "reporting": {
    "passwordArgon2id": "<paste hash>",
    "grants": { "pole": ["read"] } } } }
JSON
docker run -d --name slater-pole -p 7687:7687 \
  -v slater_pole:/data:ro \
  -v /tmp/slater-acl/acl.json:/config/acl.json:ro \
  slater:local
docker exec slater-pole /app/slater healthcheck localhost 7687   # exit 0 = up
```

Known-good hash for password `polereader` (argon2id, standard format):
`$argon2id$v=19$m=19456,t=2,p=1$R3VyS8OJIiG2Q7ihG1WlJQ$WAnkFldoPaMdxe1lAxUt/qio1Ny/jTV5aeo3p2h7ZuU`

> Pre-Stage-0 alternative: the prebuilt `target/release/slater` (old) boots with a
> `config.json` containing `"requireAclStamp": false` and serves the same image. Useful only to
> re-measure the baseline; Stages 1–6 must be validated against a freshly built server.

### Run the benchmark

```bash
/tmp/pole_venv/bin/python perf/bench.py --slater-pass polereader \
  --neo4j-uri bolt://localhost:7688 --neo4j-pass polepole12 \
  --slater-pid "$(docker inspect -f '{{.State.Pid}}' slater-pole)"
```

Compare the `slater uncached` medians against the Baseline table; the `neo4j` column gives parity
(and flags any row-count mismatch → correctness regression). For an idle Neo4j reference:
`docker run -d --name pole-neo4j -p 7688:7687 -e NEO4J_AUTH=neo4j/polepole12
-e NEO4J_server_memory_pagecache_size=64M -v pole_neo4j_data:/data neo4j:5`
(after loading `pole-50.dump`; see Regenerate appendix).

### Per-stage acceptance (apply to every stage)

1. **Correctness:** `slater-build` still reports 61,521 / 105,840; `bench.py` parity column shows
   equal row counts to Neo4j; (optional) `cargo test` green.
2. **Performance:** the stage's target query hits its goal (Status table), no >10% regression on
   other rows.
3. **Bookkeeping:** update this file's Status + Result, keep the Baseline frozen, commit doc+code.

## Stages (detail)

### Stage 0 — Config bool deserialization (server-boot prerequisite)
- **Problem:** `config.rs:97` `require_acl_stamp: bool` can't accept the stringified value the
  layered loader produces → server won't boot (see Stage 0 prerequisite above).
- **Change:** add a `deser_bool_or_str` (mirror `de::u64`/`deser_u16_or_str` visitor pattern,
  accepting `true`/`false` and `"true"`/`"false"`), apply via `#[serde(deserialize_with=…)]` to
  `require_acl_stamp` and audit other bare `bool` config leaves.
- **Files:** `crates/slater/src/config.rs` (helper likely there; if `hs-utils` owns the `de::*`
  family, add it alongside and re-export).
- **Validate:** `docker build` then run the server (Serve steps) → `healthcheck` exits 0; add a
  unit test parsing `{"requireAclStamp":"false"}` and `{"requireAclStamp":false}`.

### Stage 1 — Parameters → index selection
- **Problem:** params bypass index selection (root cause 1) → param equality/range degrades to a
  label scan (901 ms).
- **Change:** make the planner parameter-aware. The RUN message carries params before planning;
  thread a `&Params` (or pre-substituted AST) into `choose_node_scan` → `inline_preds` /
  `collect_where_preds` / `compare_operands`, and have `literal()` resolve `Expr::Param(name)`
  against it to a constant `Value` so `RangeEq`/`RangeRange` is chosen.
- **Files:** `crates/slater/src/plan.rs`; caller `exec.rs:1743` `match_single_pattern`
  (`choose_node_scan(self.gen, start, where_)` needs the param map in scope).
- **Validate:** bench `idx-eq count (Crime.type)` and `1-hop` drop from ~900 ms to tens of ms;
  results unchanged. Add a planner unit test: param equality on an indexed prop picks `RangeEq`.

### Stage 2 — Skip redundant per-node label re-check
- **Problem:** root cause 2 — `node_ok` re-decodes labels for candidates that already came from
  the label posting (841 ms vs 14 ms).
- **Change:** when the anchor scan guarantees the label(s) (a `LabelScan{L}`, or a `RangeEq`/
  `RangeRange` on an index defined for L), skip the label portion of `node_ok` for those labels
  (still check any *additional* labels / inline props). Alternative: test membership against the
  resident `nodes_with_label` posting instead of decoding a record.
- **Files:** `crates/slater/src/exec.rs` (`match_single_pattern` ~1747, `node_ok` ~2037).
- **Validate:** `Crime label count` ≈ `count all nodes` (~14 ms); results unchanged.

### Stage 3 — Count fast-paths (metadata / index cardinality)
- **Problem:** root cause 4 — counts materialize all rows; resident metadata ignored.
- **Change:** recognize an aggregate-only `RETURN count(*)`/`count(n)` with no residual WHERE and
  a single-node pattern: unfiltered → `gen.node_count()`; one label → `nodes_with_label(L).len()`;
  indexed equality (post Stage 1) → `range_index(idx).lookup_eq(v).len()`. Plumb as a planner
  recognition + a short-circuit in `project_aggregated`/`compute_aggregate`.
- **Files:** `crates/slater/src/plan.rs`, `crates/slater/src/exec.rs` (`project_aggregated`
  ~2167, `compute_aggregate` ~2223).
- **Validate:** count queries < 1 ms; results unchanged. Guard: any WHERE/extra pattern disables
  the fast path (add tests for `count(*)` with and without a filter).

### Stage 4 — Block-cache record read (broad constant factor)
- **Problem:** root cause 3 — per-record `Mutex` + `to_vec()` copy.
- **Change:** return a shared/borrowed record (`Arc<[u8]>` or a guard that borrows the cached
  decompressed block) instead of copying; reduce contention with a sharded cache or an `RwLock`
  read path; optionally a batch API to read a contiguous candidate id range under one lock.
- **Files:** `crates/slater/src/cache.rs` (`record` ~241, lock sites ~225); call sites in
  `exec.rs` (`node_props`/`edge_props`/`node_label_ids`/`outgoing`/`incoming` ~571–617).
- **Risk:** lifetime/aliasing churn across the executor — land behind full `cargo test` + bench.
- **Validate:** uniform drop across all scan/traversal rows; no correctness change.

### Stage 5 — Streaming aggregation + projection pushdown + prop memo
- **Problem:** root causes 4 & 5 — `Vec<HashMap>` per row; per-access full prop decode.
- **Change:** count/aggregate by streaming candidates without building a HashMap per row; project
  only RETURN-referenced columns; decode a node's prop record once per row and serve all property
  reads from it.
- **Files:** `crates/slater/src/exec.rs` (`match_single_pattern`, `project*`, `node_prop` ~639).
- **Validate:** `agg crimes by type`, `count DISTINCT type`, multi-property RETURNs improve; rows
  unchanged.

### Stage 6 — Traversal frame / streaming
- **Problem:** root cause 6 — per-hop `binding.clone()` and full path buffering (3-hop 2242 ms).
- **Change:** replace the cloned `HashMap` binding with a compact positional frame or a persistent
  (structurally-shared) map; stream path filtering instead of collecting all paths into a `Vec`.
- **Files:** `crates/slater/src/exec.rs` (`expand_chain` ~1821, `varlen` ~1913).
- **Validate:** 3-hop and 2-hop drop to low ms; results unchanged.

## Appendix — regenerate test data if lost

```bash
# 1. Load the Neo4j dump into a volume, then start Neo4j 5.
cp /home/rickk/git/personal/pole/data/pole-50.dump /tmp/pole_dump/neo4j.dump
docker run --rm -v pole_neo4j_data:/data -v /tmp/pole_dump:/dumps:ro neo4j:5 \
  neo4j-admin database load neo4j --from-path=/dumps --overwrite-destination=true
docker run -d --name pole-neo4j -p 7688:7687 -v pole_neo4j_data:/data \
  -e NEO4J_AUTH=neo4j/polepole12 -e NEO4J_server_memory_pagecache_size=64M neo4j:5
# 2. Re-export to slater Primitive-Cypher.
/tmp/pole_venv/bin/python /home/rickk/git/personal/pole/code/neo4j_to_slater.py \
  --uri bolt://localhost:7688 --user neo4j --password polepole12 \
  --output /home/rickk/git/personal/pole/data/pole-50.slater.cypher
```
