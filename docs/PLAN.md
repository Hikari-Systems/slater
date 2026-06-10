# Slater — a low-memory, read-only, Bolt-speaking graph engine

## Context

The estate runs several GraphRAG read APIs (`eu-ai-act-data-service`,
`bioalphaengine-data-service`/MeSH, and other sibling read services) over a single shared **FalkorDB**
instance. FalkorDB eagerly
loads each whole graph into RAM at boot (>700 MB resident for graphs that are not large), which is
the memory problem we are eliminating. Today those services issue Cypher over **`GRAPH.RO_QUERY --compact`**
(RESP), inlining values (including `vecf32([...])`) into the query string; the snapshot is built
offline by `hs-backend-spot/falkordb-snapshot/build.ts` replaying primitive-Cypher seeds.

**Slater** replaces the read path with two Rust binaries that page in only the blocks a query
touches, cache them under a bounded byte budget, and serve Cypher reads over **Bolt** so the apps can
migrate from the RESP client to a standard neo4j driver. The headline requirement is **flat resident
memory bounded by the cache budget, independent of graph size**.

This document is the build plan. The original prompt is the authoritative spec; this file records the
reconciliations made after surveying the neighbours, the confirmed decisions, and the execution order.

### Confirmed decisions (from the user)
- **Product name: Slater.** Binaries: `slater` (online Bolt server, default container entrypoint) and
  `slater-build` (offline writer). Workspace lives in `/home/rickk/git/hs/slater`.
- **Implement the full large-vector path in v1** — disk-native Vamana graph + PQ codebooks +
  block-relative addressing + beam search + coalesced reads + separate vector-cache pool — not just
  the brute-force path.
- **Container base: house style `debian:bookworm-slim`** (multi-stage `rust:1-bookworm` → slim,
  dep-cache stub, non-root `appuser:1000`, `HEALTHCHECK` subcommand). This permits the C-backed
  `zstd` crate (builds cleanly under glibc; it is self-contained, unlike rocksdb), so we are **not**
  constrained to musl-clean crypto/compression. Pure-Rust is still preferred where it costs nothing.

## Survey findings that shape the design (with paths)

### hs-utils-rs conventions — reuse, do not reinvent
- **Dependency form:** `git + tag`, never path/workspace-member. Example
  (`eu-ai-act-data-service/Cargo.toml`): `hs-utils = { git = "https://github.com/Hikari-Systems/hs-utils-rs", tag = "v0.10.0", features = ["web"] }`.
  `hs-utils-rs/CLAUDE.md`: *"referenced via git + tag … there is no workspace."* Slater is itself an
  internal cargo workspace; each member still pulls `hs-utils` via git+tag.
- **Config:** `hs_utils::config::load_layered_value()` → `serde_json::from_value` (newer pattern, used by
  the sibling read services' `src/config.rs`). JSON, **camelCase** keys, base `config.json`/`/app/config.json`
  + `/sandbox/config.json` deep-merge overlay + `[SECRET]:/path` resolution + `KEY__sub=val` env
  overrides. Numeric/bool fields use `hs_utils::config::deser_*_or_str` (e.g. `deser_u16_or_str`).
- **Logging:** `hs_utils::logging::init(&cfg.log.level)` (EnvFilter string, ANSI off). Call after config load.
- **Healthcheck:** `hs_utils::healthcheck::check_subcommand(port)` — **first line of `main`**, does a bare
  TCP connect to `port` and exits 0/1. Works against a Bolt TCP port unchanged.
- **Not reusable here:** `hs_utils::server::run` / `middleware` are **actix-web HTTP**. Slater's Bolt
  listener is a bespoke `tokio` + `rustls` TCP server. We reuse config/logging/healthcheck only.
- **Release profile (house standard, copy verbatim):** `strip = true, lto = true, codegen-units = 1, opt-level = "s"`.

### The real Cypher surface (widen the floor to this)
Confirmed call sites: the sibling read services' `src/routes/mod.rs` (e.g.
`eu-ai-act-data-service/src/routes/mod.rs`), with literals built in `*/src/cypher.rs` and executed via
`*/src/falkor.rs` `GRAPH.RO_QUERY --compact`. The read subset **must** cover, beyond the spec floor:
- **`WITH` pipelines** (projection, `DISTINCT`, `ORDER BY`, post-`WITH` `WHERE`/having, aggregation
  staging) — pervasive.
- **`UNION`** (up to 4 branches in the expand queries) — required (spec floor wrongly excluded it).
- **Map-projection RETURN** `RETURN { id: n.id, labels: labels(n), ... } AS row` — the **dominant**
  return shape; PackStream must emit Bolt Map values.
- **`CASE WHEN … THEN … ELSE … END`**, label predicates in expressions (`p:Provision`).
- **List predicates** `ANY(x IN coalesce(n.tags, []) WHERE toLower(x) CONTAINS …)`, `NONE`, `ALL`.
- **Variable-length paths** `*1..N` incl. **type alternation** `-[:IN_CHAPTER|IN_SECTION*1..2]->`;
  path variables `p = (...)`, `length(p)`.
- **Functions:** `labels, type, properties, startNode, endNode, id, length, size, coalesce, toLower,
  toUpper, toFloat, toInteger, abs, log, exists`, plus aggregations `count, count(DISTINCT), collect,
  collect(DISTINCT), sum, min, max, avg`.
- **Operators in WHERE:** `=, <>, <, <=, >, >=, IN, CONTAINS, STARTS WITH, ENDS WITH, IS NULL,
  IS NOT NULL, AND/OR/NOT`.
- **Vector KNN:** `CALL db.idx.vector.queryNodes('Label','embedding', k, vecf32([f0,...])) YIELD node, score`
  — cosine, 1024-dim, NODE-level indexes on whichever labels carry an `embedding` property. **Vectors
  arrive inlined as `vecf32([...])` literals**, so Slater must accept inlined vector literals **and**
  Bolt `$param` values (drivers will likely parameterise post-migration).
- **Reject** (write/side-effect): `CREATE/MERGE/SET/DELETE/REMOVE/DROP`, write-`CALL`. **Reject for v1**
  (not observed): `UNWIND`, `CALL {}` subqueries — clear Bolt FAILURE at parse time.

### Scale & graph inventory
- Graphs/dbs: several named graphs (e.g. `eu_ai_act`, `bioalphaengine-companies` (MeSH), and others),
  each a few thousand nodes / tens of thousands of edges with a handful of NODE vector indexes
  (×1024-dim cosine). Names from `hs-backend-spot/falkordb-snapshot/config.json`; per-graph ACL users
  already exist. Treat graph names as data — Slater serves whatever directories exist under `data_dir`.
- All live graphs are **below the 50k-vector ANN threshold** → brute-force cosine is the real path;
  Vamana/PQ is exercised by synthetic tests but is being built per the decision above.
- Embeddings: Bedrock Cohere Embed v4, **1024-dim, cosine**. Slater does **not** embed — the query
  vector arrives in the Cypher. No Bedrock/AWS dependency in Slater.

### Dump/build format to parse (from `hs-backend-spot/falkordb-snapshot/seeds/*.ts` + the sibling loaders' `loader/lib/dump-graph.ts`)
- `CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__)` (marker setup).
- `CREATE (:Label1:Label2:__DumpVertex__ {__dump_id__: <int>, embedding: vecf32([...]), key: val, ...})`.
- `MATCH (a:__DumpVertex__ {__dump_id__: i}), (b:__DumpVertex__ {__dump_id__: j}) CREATE (a)-[:REL {props}]->(b)`.
- Range indexes: `CREATE INDEX FOR (n:Label) ON (n.prop)` and `CREATE INDEX FOR ()-[r:TYPE]->() ON (r.prop)`.
- Vector indexes: in seeds they come from `createNodeVectorIndex(label, dim, 'cosine', property)` /
  the `db.idx.vector.createNodeIndex('Label','prop',dim,'cosine')` Cypher form, and/or a
  `VectorIndexSpec[]` sidecar. Accept **all three**: the Cypher `CALL db.idx.vector.createNodeIndex`,
  the SDK-style helper line, and a `--vector-index-json <file>` sidecar.
- Cleanup: `MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__` / `DROP INDEX … __dump_id__`
  → honoured by simply never persisting the marker label/property.
- Property value types: int, float, bool, null, escaped strings (including large multi-paragraph
  markdown text fields), homogeneous arrays (notably string arrays), and **`vecf32([...])`** as a
  first-class dense-f32 type.

## Workspace layout

```
slater/
  Cargo.toml                 # [workspace] members = ["crates/*"]
  rust-toolchain.toml        # pin stable (match house)
  Dockerfile                 # multi-stage, debian:bookworm-slim
  docker-compose.yml         # slater + mounted /data volume; alt slater-build command
  README.md                  # mounts, env, worked example (a representative graph)
  config.json                # baked default (camelCase, hs-utils layering)
  acl.json                   # sample ACL (argon2id hashes)
  docs/
    PLAN.md                  # the frozen implementation plan (this document, copied in at scaffold)
    PROGRESS.md              # living ledger: per-milestone status, verified tests, decisions, next action
    DECISIONS.md             # running log of // DESIGN: choices + rationale (mirrors in-code comments)
  crates/
    graph-format/            # lib: byte layout, block codec, manifest, dictionary, integrity, crypto
    slater-build/            # bin "slater-build": offline writer (primitive Cypher -> on-disk image)
    slater/                  # bin "slater": online Bolt read server
```

`graph-format` is the single owner of the byte layout; both binaries depend on it so writer and reader
cannot drift.

## Crate: `graph-format` (build first — everything depends on it)

Pure data + codec library. Modules:
- `manifest` — `Manifest` struct (build uuid, format version, content hash, file inventory, per-file
  block size, codec id, encryption header = KDF salt/params + per-block nonces (**never the key**),
  dictionary stats, index catalogue, **vector index descriptors incl. ANN mode** `BruteForce|Vamana`,
  dim, metric, Vamana medoid + params, PQ params). Serde JSON.
- `block` — fixed-size raw block framing; per-file **block directory** (block id → file offset +
  compressed length), held resident at open (tiny). `pread` at aligned offsets; **no mmap**.
- `codec` — zstd compress/decompress (per-`(label,property)` dictionaries for repetitive columns).
  `// DESIGN:` C-backed `zstd` crate is acceptable under debian-slim (self-contained, static-linkable);
  pure-Rust `ruzstd` decode noted as a fallback if we ever need musl.
- `crypto` — optional per-block AEAD (**XChaCha20-Poly1305**, pure-Rust `chacha20poly1305`). Key from
  env/file at runtime; per-generation derived key (BLAKE3/argon2 KDF over salt). Decrypt-before-decompress
  on miss; LRU holds plaintext-decompressed blocks. `// DESIGN:` document the key-management seam.
- `integrity` — **BLAKE3** content hash over generation files; validate on open, refuse on mismatch.
- `columns` — column-oriented property encoding for node & edge props; typed cells (int/float/bool/null/
  string/array/**vecf32**). Dictionary-interned strings/labels/reltypes/keys.
- `topology` — forward + reverse **CSR** adjacency layout.
- `isam` — ISAM range index: sorted (value,id) key-blocks + resident sparse top-level (first key/block).
- `vector` — descriptors + readers for `vectors.f32.blk`, PQ codes file, Vamana block file
  (block-relative `(block_id, slot)` neighbour addressing).
- `ids` — `NodeId`, `EdgeId`, `BlockId`, `Generation` newtypes; `enum Value` for property cells.

On-disk generation directory (exactly as spec): `MANIFEST.json`, `dictionary.blk`, `node_props.blk`,
`edge_props.blk`, `topology.csr.blk`, `vectors.f32.blk`, `labels.post`, `reltypes.post`,
`range/<name>.isam`, `vector/<label>.<prop>.pq`, `vector/<label>.<prop>.vamana`; plus `data/<graph>/current`
pointer. Block size **per file** (default 256 KiB props/topology/dict; 1–2 MiB Vamana). All `.blk` zstd
per-block; optional AEAD per block. Write to temp dir → fsync → hash → MANIFEST last → fsync → atomic
rename → atomic swap `current`.

## Crate: `slater-build` (offline writer, bin `slater-build`)

Parser: **`pest`** grammar (`primitive_cypher.pest`) covering the six dump forms + the property value
grammar incl. `vecf32([...])`. Streaming statement reader (seeds are large; do not hold whole file).

Two-pass build (offline, `HashMap` fine):
1. Ingest nodes → dense `NodeId`; transient `__dump_id__ → NodeId` map; intern labels/reltypes/keys/
   string values into per-graph dictionary; route `vecf32` → vector store, scalars/arrays → columns;
   drop `__DumpVertex__` + `__dump_id__`.
2. Ingest relationships → dense `EdgeId`; build forward + reverse **CSR**; edge props columnar.
3. Range indexes → ISAM (sorted key-blocks + sparse top-level).
4. Vector indexes per `(label,prop)`, mode by cardinality (recorded in MANIFEST):
   - `< --ann-threshold` (default 50k): **brute-force** — persist full f32 vectors + metadata only.
   - `>= threshold`: **Vamana** — (a) build single-layer Vamana (`R`, `alpha` pruning, medoid in
     MANIFEST); (b) **train PQ codebooks** per `(label,prop)`, encode all vectors (~64–128 B/vec) →
     `vector/<l>.<p>.pq`; (c) **locality layout** (BFS-from-medoid / cheap recursive bisection — keep
     v1 simple) packed into 1–2 MiB blocks `[full vec ‖ adjacency]`; (d) rewrite adjacency to
     block-relative `(block_id, slot)` → `vector/<l>.<p>.vamana`.
5. Serialise to fresh generation dir, atomically publish; print generation UUID + content hash.

CLI: `slater-build --input <script|-> --graph <name> --data-dir <dir> [--block-size] [--vector-block-size]
[--encrypt --key-file|--key-env] [--zstd-level] [--ann-threshold] [--vamana-r] [--vamana-alpha]
[--pq-subspaces] [--pq-bits] [--vector-index-json <file>]`. Idempotent — new generation each run, never mutates.
`clap` for args. Each open design choice gets a `// DESIGN:` comment.

## Crate: `slater` (online Bolt server, bin `slater`)

`main` order (house pattern): `healthcheck::check_subcommand(port)` → `config::load()` →
`logging::init` → open graphs → run Bolt listener. Also a `slater hash-password` subcommand.

Modules:
- `config` — `hs_utils::config::load_layered_value()`; struct with `data_dir, bind, port,
  tls{cert,key}, acl_path, block_cache_bytes, vector_cache_bytes, result_cache_bytes,
  vector_index_pins:[(label,prop)], encryption{key_env|key_file}, query{max_rows,timeout_ms},
  vector_query{beam_width,max_hops}, generation_poll_ms, reload_strategy=exit|swap`. `deser_*_or_str` on numerics.
- `bolt` — Bolt **5.x handshake with 4.4 fallback**, **PackStream v2** encode/decode; messages
  `HELLO/LOGON/RUN/PULL/DISCARD/BEGIN(read-only)/COMMIT/ROLLBACK/RESET/GOODBYE/FAILURE/SUCCESS/RECORD`.
  Map values for map-projection returns. One `tokio` task per connection; **TLS via `rustls`**
  (`bolt+s`/direct-TLS; plaintext on loopback for dev). `// DESIGN:` verified against neo4j JS+Python drivers.
- `acl` — JSON ACL; **argon2id** verify at LOGON; per-graph `read` grant enforced on `db` select +
  every query; hot-reload on file change (bad file → keep last-good, log loudly). `hash-password` mints hashes.
- `parser` — read-only Cypher (`pest`) covering the widened subset above; reject writes/UNWIND/CALL{} with FAILURE.
- `plan` — logical plan; `exec` — volcano/iterator executor pulling from `graph-format`. Use ISAM range
  indexes + label/reltype postings for selective MATCH; label scan fallback; CSR traversal; var-length
  expansion with type alternation; aggregation/ORDER BY/SKIP/LIMIT/DISTINCT/UNION/WITH; map projection.
  Run execution on `spawn_blocking`/`rayon`; enforce per-query **row + wall-clock** limits.
- `vector` — KNN entry (`db.idx.vector.queryNodes` + a `similarity()`-style function). Path chosen from
  MANIFEST mode: **brute-force** cosine over label-filtered candidates via block LRU; **Vamana beam
  search** holding PQ codes resident (navigate by PQ-estimated distance in memory), read full vectors/
  adjacency only for frontier + final re-rank, **coalesce reads by `block_id`** (one block read per
  distinct block). `// DESIGN:` resident set for large path is **PQ codes only** — never a full in-memory HNSW.
- `cache` — three pools: **block LRU** keyed `(graph,gen,file_id,block_id)` holding decompressed/
  decrypted blocks, global byte budget; **vector-index pool** (separate budget) for 1–2 MiB Vamana
  blocks + resident PQ codes, with **pin/unpin per `(label,prop)`** (config `vector_index_pins`);
  **result LRU** keyed `(graph,gen,normalised_query,params)` (gen UUID in key → swap orphans stale).
  All budgets runtime-adjustable (config reload / admin signal). Per-pool hit/miss/eviction metrics via tracing.
- `generation` — open `current`, **validate content hash vs MANIFEST** (fail fast, non-zero exit on
  mismatch — copy-completeness guard for NFS rsync). In-flight guard: **poll** `current` on interval
  (not inotify; NFS). On changed UUID mid-flight: default `exit` (log fatal, non-zero → orchestrator
  restart); alternative `swap` (drain in-flight, drop affected caches, atomically swap to new validated
  generation). Never mix two generations within one query.

## Container & ops
- Multi-stage `Dockerfile`: `rust:1-bookworm` builder (dep-cache stub then real build of all three
  crates) → `debian:bookworm-slim` + `ca-certificates`, non-root `appuser:1000`. `slater` is the
  ENTRYPOINT; `slater-build` reachable as an alternate command. `HEALTHCHECK CMD ["/app/slater","healthcheck"]`.
- **Read-only root filesystem.** `/data` = mounted NFS volume (`data_dir`); `acl.json` + TLS material
  mounted read-only; encryption key via env or mounted secret. `docker-compose.yml` mirrors house style
  (`read_only: true`, `/sandbox` overlay, `__`-nested env overrides).
- README: mounts/env table + worked example — build a representative graph from a dump script with
  `slater-build`, connect with neo4j JS **and** Python drivers over `bolt+s`, run a `MATCH … RETURN`
  and a cosine-KNN query.

## Dependencies (pin in each Cargo.toml)
`hs-utils` (git+tag, minimal features — verify which feature gates config/logging/healthcheck; likely
default), `tokio` (full), `rustls` + `tokio-rustls`, `serde`/`serde_json`, `pest`/`pest_derive`, `clap`,
`zstd`, `chacha20poly1305`, `blake3`, `argon2`, `rayon`, `byteorder`/`bytes`, `tracing`, `anyhow`,
`thiserror`, `uuid`. PQ/Vamana hand-rolled in `graph-format`/`slater-build` (no heavy ANN crate) to keep
the byte layout owned locally.

## Compliance & test corpus (build a lot of tests, alongside every milestone)

Treat tests as a first-class deliverable, written **with** each milestone, not after. Two external
suites are the reference for *behaviour we must match* — mine them for cases and expected results, port
the relevant ones as Rust tests, and cite the source case in a comment:
- **FalkorDB** (`https://github.com/falkordb/falkordb`) — the engine we are replacing. Its `tests/flow`
  Python suite is the closest match to our actual surface: Cypher semantics, `db.idx.vector.queryNodes`
  cosine KNN, range indexes, CSR traversal, aggregations, map projections, and the RESP/compact result
  shapes our apps depend on today. Port the read-path flow tests; they define bug-for-bug compatibility
  with what the siblings issue.
- **Memgraph** (`https://github.com/memgraph/memgraph`) — for breadth of **openCypher semantics**. Its
  `tests/` (gqlalchemy/openCypher feature scenarios, query semantics, `ORDER BY`/`SKIP`/`LIMIT`, list
  predicates, `WITH`/`UNION`, null handling) and the upstream **openCypher TCK** feature files it tracks
  are the source for our parser/executor conformance cases.

Organise as: `#[cfg(test)]` unit tests inside each module; `crates/*/tests/` integration tests; a shared
`testdata/` (small fixture graphs + golden generations) and `corpus/` (a curated set of query→expected
records lifted from the two suites, tagged by source). Categories and where they attach:

- **Parser compliance** (`slater::parser`): accept-set = the widened subset (WITH/UNION/map projections/
  CASE/list predicates/var-length+alternation/functions/operators) and the inlined `vecf32([...])`
  literal grammar; reject-set = `CREATE/MERGE/SET/DELETE/REMOVE/DROP/UNWIND/CALL{}` each yielding a clean
  Bolt FAILURE with the right code. Drive from Memgraph/TCK scenario files + every distinct query string
  found across the sibling read services' `src/routes/mod.rs` / `src/cypher.rs`.
- **Executor / Cypher semantics** (`slater::exec`): golden query→records against fixture graphs —
  `MATCH`/`OPTIONAL MATCH`, multi-label, directed/undirected/var-length, `WHERE` operators, `WITH`
  pipelines, `UNION`, `ORDER BY`/`SKIP`/`LIMIT`/`DISTINCT`, aggregations (`count(DISTINCT)`, `collect`,
  `sum/min/max/avg`), map projection, `CASE`, `coalesce`/`toLower`/`toFloat`/`labels`/`type`/`properties`,
  null-propagation semantics (port Memgraph's null-handling cases). Ground truth cross-checked against a
  live FalkorDB where shapes are ambiguous.
- **Vector KNN compliance** (`slater::vector`): brute-force cosine results match a reference numpy/ndarray
  computation exactly (ordering + scores within tolerance); `db.idx.vector.queryNodes` arg parsing and
  `YIELD node, score` shape match FalkorDB. Large path: recall@k vs brute-force ground truth above
  threshold.
- **Format round-trip** (`graph-format`, `slater-build`): every property type incl. large escaped strings
  and 1024-dim `vecf32`; CSR forward/reverse equivalence; ISAM lookups vs linear scan; dictionary
  interning; block-directory offsets; zstd + AEAD round-trip; BLAKE3 integrity accept/reject.
- **Bolt/PackStream wire compliance** (`slater::bolt`): handshake 5.x + 4.4 fallback; PackStream v2
  encode/decode of Null/Bool/Int/Float/String/List/Map/Node/Relationship/Path; `HELLO/LOGON/RUN/PULL/
  DISCARD/RESET/GOODBYE` flows; FAILURE on writes. **Driver interop** integration tests spawn the real
  **neo4j JavaScript and Python** drivers against a running `slater` over TLS (gated behind a feature/
  env so CI can skip if drivers absent).
- **Memory/bounded-resident** (headline): RSS-sampling integration test serving a graph ≫ cache budget;
  assert resident ≤ `block_cache_bytes + vector_cache_bytes` + fixed overhead.
- **Security & ops**: argon2id verify (right/wrong password), per-graph grant enforcement, AEAD wrong/
  absent-key refusal, truncated-generation refusal, mid-flight `current` swap → `exit`/`swap`.

## Working method: on-disk progress ledger & context-clear resumability

This is a large, multi-session build whose full context will not fit in one window. To keep request-
following coherent across context clears, **state lives on disk, not in the conversation**. Three files
under `slater/docs/` are written at scaffold time (milestone 1) and maintained throughout:

- **`PLAN.md`** — a frozen copy of this plan. Read-only reference; the contract for what we are building.
- **`PROGRESS.md`** — the living ledger and single source of truth for "where are we". Structure:
  - A milestone checklist mirroring "Execution order" with explicit status per item:
    `[ ] todo` / `[~] in-progress` / `[x] done & verified` / `[!] blocked (reason)`.
  - For each completed milestone: the date, the crates/modules/files added or changed, the **exact test
    names** added and their pass/fail state (paste the `cargo test` summary line), and any deviations
    from PLAN.md.
  - A **"NEXT ACTION"** block at the top: the single next concrete step, the command to run to confirm
    the current state is green (`cargo build && cargo test -p <crate>`), and any preconditions.
  - A short **"context for resume"** note: anything a fresh session must know that is not obvious from
    the code (open questions, half-finished refactors, why a thing is the way it is).
- **`DECISIONS.md`** — append-only log of every `// DESIGN:` choice with one-line rationale, kept in
  step with the in-code comments so decisions survive even if a file is rewritten.

**Resume protocol (run at the start of every session):**
1. Read `docs/PROGRESS.md` (NEXT ACTION + status) then `docs/PLAN.md` for the relevant milestone only.
2. Run the green-state command named in PROGRESS.md to confirm the tree matches the ledger; if it
   diverges, reconcile the ledger to reality before doing new work.
3. Do the next milestone's work; keep tests green at every commit.
4. **Before ending / before a context clear:** update PROGRESS.md (status, tests, NEXT ACTION) and
   DECISIONS.md, and ensure `cargo build` + the milestone's tests pass. A milestone is only "done" when
   the tree compiles and its tests are green — these are the **only safe context-clear boundaries**.

**Milestones are designed as clean clear-points:** each one leaves the workspace compiling with its
tests passing and the ledger updated, so a brand-new context can resume from `docs/PROGRESS.md` +
`docs/PLAN.md` alone without re-deriving prior reasoning. The in-repo Task list (TaskCreate/TaskUpdate)
may mirror PROGRESS.md within a session for live tracking, but **PROGRESS.md on disk is authoritative**
because the Task list does not survive a context clear.

## Execution order (milestones — each ships with its tests)
1. **Scaffold** workspace, three crates, Cargo metadata, toolchain, release profile; wire `hs-utils`
   git+tag. **Write `docs/PLAN.md` (copy of this plan), `docs/PROGRESS.md` (ledger seeded with the
   milestone checklist + first NEXT ACTION), and `docs/DECISIONS.md`.** Stand up `testdata/` + `corpus/`
   skeleton and the FalkorDB/Memgraph case-porting harness.
2. **`graph-format`** types + block codec + zstd + manifest + integrity + columns + CSR + ISAM (no
   crypto/ANN yet); **format round-trip + CSR + ISAM unit tests**.
3. **`slater-build`** pest parser + two-pass build → emit a brute-force generation (a representative
   graph); **parser accept/reject corpus from the dump grammar + golden round-trip test**.
4. **`slater`** generation open + integrity validation + block LRU + label/reltype postings + CSR;
   Bolt handshake + PackStream + ACL/argon2 + read-only parser + executor for the widened subset.
   **Bolt/PackStream wire tests + parser-compliance corpus (Memgraph/TCK + sibling queries) + executor
   golden tests + neo4j JS/Python driver interop.**
5. **Brute-force vector KNN** + result LRU; **vector-KNN compliance vs reference + representative-graph
   acceptance (MATCH/RETURN + cosine-KNN) + FalkorDB flow-test port**.
6. **Encryption at rest** (per-block AEAD) end-to-end; **wrong/absent-key refusal test**.
7. **Large-vector path**: Vamana build + PQ codebooks + block-relative layout in `slater-build`;
   beam search + coalesced reads + separate vector-cache pool + pin/unpin in `slater`. **Synthetic
   recall@k-vs-brute-force + bounded-memory test.**
8. **Generation guard**: poll, `exit`/`swap` strategies; **truncated-generation refusal + swap/exit
   behaviour tests**.
9. **Memory headline test** under sustained load; **Container**: Dockerfile, compose, README worked
   example; full suite green in CI.

> At the **end of every milestone above**: update `docs/PROGRESS.md` (status, test names + pass state,
> NEXT ACTION) and `docs/DECISIONS.md`, and confirm `cargo build` + that milestone's tests are green.
> These are the safe points to clear context and resume from disk.

## Verification (end-to-end)
- **Round-trip:** `slater-build` a primitive-Cypher dump (multi-label nodes, string arrays, large
  escaped strings, 1024-dim `vecf32`, node+edge range indexes, vector index) → serve with `slater` →
  neo4j JS/Python drivers return correct nodes/rels/properties/vectors and a correct cosine-KNN top-k.
- **Driver interop:** official neo4j JS + Python drivers connect over TLS, authenticate via ACL, select
  `db`, run the widened read subset; write clauses get a clean Bolt FAILURE.
- **Memory (headline):** resident stays bounded by `block_cache_bytes + vector_cache_bytes` (+ small
  fixed overhead) while serving a graph far larger than that budget — assert via RSS sampling under load.
- **Large-vector path:** synthetic set ≫ `--ann-threshold` and ≫ `vector_cache_bytes` → KNN recall vs
  brute-force ground truth acceptable while RSS bounded (PQ codes resident + coalesced block reads only).
- **Integrity:** a truncated/half-copied generation is refused at open; mid-flight `current` swap triggers
  configured `exit`/`swap`.
- **Security:** wrong password and ungranted-graph both fail cleanly; at-rest encryption round-trips with
  the key; wrong/absent key fails to open rather than serving garbage.

## Open items flagged for `// DESIGN:` comments during implementation
- Exact `hs-utils` feature set exposing config/logging/healthcheck (verify against the crate before wiring).
- Whether result-cache keys normalise inlined `vecf32` literals (large keys) or skip caching vector queries.
- Vamana locality partitioner choice (BFS-from-medoid vs recursive bisection) — keep v1 simple.
- Key-management seam (env/file → real secrets store left to operator).
