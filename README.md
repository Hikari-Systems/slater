# Slater

[![CI](https://github.com/Hikari-Systems/slater/actions/workflows/ci.yml/badge.svg)](https://github.com/Hikari-Systems/slater/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Hikari-Systems/slater?sort=semver)](https://github.com/Hikari-Systems/slater/releases/latest)

A low-memory, read-only, Bolt-speaking graph engine.

Slater serves an **immutable, on-disk** graph image over the **Bolt** protocol
(so any standard neo4j driver can talk to it), keeping **resident memory bounded
by its cache budgets — independent of graph size**. It replaces an in-memory
engine whose RSS scaled with the graph: where that engine held the whole graph
resident, Slater holds only bounded caches and reads everything else from disk on
demand, including the disk-native approximate-nearest-neighbour (Vamana/PQ) vector
path.

## Features

| Feature | What it means for you |
|---|---|
| **Bounded, predictable memory** | Resident memory is capped by three cache budgets *you* set — it does **not** grow with graph size. You tune the performance/RAM trade-off instead of provisioning for the whole graph. |
| **Multi-tenant out of the box** | One server hosts many graphs with per-user read grants — multi-database isolation that most graph DBs reserve for a paid/enterprise tier. |
| **Encryption at rest & in transit** | Per-block XChaCha20-Poly1305 sealing (the key is never written to disk) plus optional TLS (`bolt+s://`). GDPR-friendly by construction. |
| **Tiny, dependency-light install** | A ~5 MB stripped static binary in a ~33 MB multi-arch image (amd64/arm64); pure-Rust TLS, no OpenSSL. Pull and run. |
| **Built for periodic publish** | Build a graph offline, serve it immutable, then atomically swap in a new version with zero downtime — ideal for data-warehouse / scheduled-refresh workloads. |
| **Rugged under load** | Written in Rust with no `unsafe`; read-only means no write locks, no GC pauses, no data races. One bad query can't take the server down. |
| **Works with your neo4j tools** | Speaks Bolt 5.4 / 4.4 / 4.1 — use the standard neo4j drivers (JS, Python, Go, Java…), `cypher-shell`, or graph browsers unchanged. |
| **Rich read-only Cypher** | A broad query surface: `MATCH`/`WHERE`/`WITH`/`UNION`, `CALL {…}` subqueries, 70+ functions & aggregations, temporal & geospatial values, and regex. |
| **ISO GQL support (read-only aspects)** | Speaks a read-only subset of **ISO GQL** (ISO/IEC 39075) over the same Bolt connection — quantified paths, path restrictors, shortest-path selectors, label/type boolean expressions, `FOR`, `CAST`, and an optional `GQL`/`CYPHER` dialect prefix — alongside Cypher, in one engine. See [Supported GQL subset](#supported-gql-subset). |
| **Vectors + graph in one engine** | Disk-native ANN vector search (Vamana + PQ) for embeddings/RAG, plus graph algorithms (PageRank, BFS, betweenness, WCC…) — bounded memory even with millions of vectors. |
| **Safe on network storage** | Every file is BLAKE3 content-hashed and verified on open; torn or half-copied images are refused, not served. Designed for NFS/remote volumes (no mmap surprises). |

Two binaries make up the workspace:

| Binary | Role |
| --- | --- |
| `slater` | The online, read-only Bolt server (the container ENTRYPOINT). |
| `slater-build` | The offline writer: turns a primitive-Cypher dump into an immutable, content-hashed generation directory. |

Slater splits writing from serving: the offline `slater-build` does all the heavy
lifting — ingesting your data and compiling it into an immutable generation — so
the serving process carries **none** of the write-side machinery (transaction
logs, locks, GC, index rebalancing) on its hot path. That's what keeps reads fast
and memory light. The serving process is therefore **read-only**, and within that
envelope answers a broad Cypher surface — pattern matching,
`WITH`/`UNION`/`CALL {…}` subqueries, 70+ scalar & aggregate functions, temporal &
geospatial values, graph algorithms (`algo.*`), and disk-native vector KNN
(`db.idx.vector.queryNodes`). To update a graph you build a new generation and
atomically swap the `current` pointer; the running server picks the change up via
its generation guard (see [Generation guard](#generation-guard)).

## Supported GQL subset

Slater also understands a read-only subset of **ISO GQL** (ISO/IEC 39075), the
standardised graph query language. There is no separate endpoint or protocol: GQL
arrives over the same Bolt connection and is parsed by the same engine, so the
standard drivers and tools work unchanged. A statement may optionally carry a
leading `GQL` or `CYPHER` dialect selector (mirroring the `CYPHER 5` / `CYPHER 25`
form) — it is stripped and the remainder is parsed either way, as one parser
serves both languages.

Every GQL form below lowers onto an existing engine capability, so the two
spellings are equivalent and may be mixed freely (even within one query):

| GQL form | Cypher equivalent | Meaning |
|---|---|---|
| `((…)){m,n}` | `…*m..n` | Quantified path — repeat the parenthesised pattern *m* to *n* times. |
| `WALK` / `TRAIL` / `ACYCLIC` / `SIMPLE` | (varies; `*` is `TRAIL`) | Path restrictor over a variable-length match: `WALK` allows repeats, `TRAIL` forbids repeated edges (the default for `*`), `ACYCLIC` forbids any repeated node, `SIMPLE` forbids interior repeats but lets the two endpoints coincide. |
| `ANY SHORTEST` / `ALL SHORTEST` / `SHORTEST k` | `shortestPath(…)` | Shortest-path selector on a match: any one shortest path, all minimum-length paths, or the first *k* by length. |
| `:A & B`, `:A \| B`, `:! A`, `:(A \| B) & C` | `:A:B` (AND), `:A\|B` (OR) | Label / relationship-type boolean expressions — `&`, `\|`, `!` and parentheses. The classic `:A:B` (AND) and `:T1\|T2` (alternation) remain valid as sugar. |
| `FOR x IN list` | `UNWIND list AS x` | Iterate a list, emitting one row per element. |
| `CAST(expr AS TYPE)` | `toInteger(expr)`, `toFloat(…)`, … | Typed-value conversion (`INTEGER`/`INT`, `FLOAT`/`DOUBLE`/`REAL`, `STRING`/`VARCHAR`, `BOOLEAN`/`BOOL`, `DATE`, `LOCALTIME`, `LOCALDATETIME`, `DURATION`). |
| `GQL …` / `CYPHER …` prefix | *(none — implicit)* | Optional dialect selector at the very start of a statement; recorded and stripped, with no change of behaviour today. |

GQL responses also carry additive **GQLSTATUS** status objects in the Bolt
`SUCCESS` / `FAILURE` metadata (`gql_status` + `status_description`) alongside the
existing keys, so GQL-aware clients see standard status codes while older drivers
are unaffected.

### Writing GQL vs Cypher

Because both spellings parse to the same plan, you can use whichever reads better —
and mix them. The pairs below return identical results.

**Quantified paths** — repeat a parenthesised sub-pattern a bounded number of times
(the quantifier sits on the group, `((…)){m,n}`):

```cypher
-- GQL
MATCH (a:Person) ((x)-[:KNOWS]->(y)){1,3} (b:Person) RETURN b
-- Cypher
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b
```

**Path restrictors** — control repeated nodes/edges on a variable-length `-[:R*]->`
match (`WALK` allows repeats, `TRAIL` = no repeated edge — the default for `*` —
`ACYCLIC` = no repeated node, `SIMPLE` = no repeated *interior* node):

```cypher
-- GQL: no node may repeat on the variable-length walk
MATCH ACYCLIC (a)-[:KNOWS*]->(b) RETURN b
-- Cypher `*` is already edge-unique (= TRAIL); ACYCLIC has no plain-Cypher spelling
```

**Shortest-path selectors** — pick the shortest path(s) on a match:

```cypher
-- GQL
MATCH ANY SHORTEST (a {name:'Alice'})-[:KNOWS*]->(b {name:'Carol'}) RETURN b
MATCH ALL SHORTEST (a)-[:KNOWS*]->(b) RETURN b      -- every minimum-length path
MATCH SHORTEST 3 (a)-[:KNOWS*]->(b) RETURN b        -- first 3 by length
-- Cypher: the shortestPath() function (single shortest; endpoints bound first)
MATCH (a {name:'Alice'}), (b {name:'Carol'})
RETURN shortestPath((a)-[:KNOWS*]->(b))
```

**Label / type boolean expressions** — `&`, `|`, `!` and parentheses:

```cypher
-- GQL
MATCH (n:Person & Admin) RETURN n            -- both labels
MATCH (n:Person & !Admin) RETURN n           -- Person but not Admin
MATCH (a)-[r:KNOWS | FOLLOWS]->(b) RETURN r   -- either relationship type
-- Cypher: :Person:Admin is AND sugar; |-alternation is rel-types only; no `!`
MATCH (n:Person:Admin) RETURN n
```

**`FOR` instead of `UNWIND`** — iterate a list:

```cypher
-- GQL
FOR x IN [1, 2, 3] RETURN x
-- Cypher
UNWIND [1, 2, 3] AS x RETURN x
```

**`CAST` instead of the `to*` functions** — typed-value conversion:

```cypher
-- GQL
RETURN CAST('42' AS INTEGER), CAST(3 AS FLOAT), CAST('2024-01-01' AS DATE)
-- Cypher
RETURN toInteger('42'), toFloat(3), date('2024-01-01')
```

**Dialect prefix** — optional; stripped and parsed by the one engine, so it changes
nothing today but lets a GQL-aware client be explicit:

```cypher
GQL MATCH (n:Person) RETURN n
CYPHER MATCH (n:Person) RETURN n
```

> **On the name.** Slater is named after the CIA agent in *Archer* (a great show)
> who insists on going by a single name — "Just… Slater" — and one of my favourite
> characters in it. See the
> [character wiki page](https://archer.fandom.com/wiki/Slater).

## Running with Docker

Slater is designed to be run as a **Docker deployment** — that's the expected way
to use it. Prebuilt multi-arch images (`linux/amd64` + `linux/arm64`) are
published to **Docker Hub** at
[**`hikarisystems/slater`**](https://hub.docker.com/r/hikarisystems/slater),
tagged `:latest` and `:vX.Y.Z` on every release:

```sh
docker pull hikarisystems/slater:latest
```

A Docker-command-only usage, configuration, and operations guide lives in
[`DOCKERHUB.md`](DOCKERHUB.md) (and is mirrored to the Docker Hub overview page) —
**start there if you're deploying.** In short:

```sh
# Build a graph generation with the offline writer:
docker run --rm -v slater-data:/data -v "$PWD/dumps:/dumps:ro" \
  --entrypoint /app/slater-build hikarisystems/slater:latest \
  --input /dumps/people.cypher --graph people --data-dir /data

# Serve it (read-only) over Bolt on 7687:
docker run -d --name slater -p 7687:7687 \
  -v slater-data:/data:ro -v "$PWD/acl.json:/config/acl.json:ro" \
  hikarisystems/slater:latest
```

To build the image locally instead (e.g. for development):

```sh
# Build the image (both binaries).
docker compose build

# Serve (expects generations under the slater-data volume / your /data mount).
docker compose up slater

# Build a generation with the offline writer (profile `build`):
docker compose run --rm builder \
  --input /dumps/people.cypher --graph people --data-dir /data
```

The builder stage installs `cmake`, `clang` and `libclang-dev` for the rustls
`aws-lc-rs` backend; `git` (already in the base image) is required for the
`hs-utils` git+tag dependency, which `.cargo/config.toml` fetches via the git CLI.

The sections below cover the on-disk format, configuration, ACLs, and a
local (non-Docker) worked example.

## How it works

```
            slater-build                         slater (Bolt server)
   dump.cypher ──────────▶ /data/<graph>/<uuid>/ ──────────▶ neo4j driver
   (offline, atomic)        MANIFEST.json, *.blk,            (bolt / bolt+s)
                            range/*.isam, vector/*.{vamana,pq},
                            current → <uuid>
```

* A **generation** is one immutable directory: a `MANIFEST.json` (symbol tables,
  index descriptors, an optional encryption header), columnar block files
  (`node_props.blk`, `node_labels.blk`, `edge_props.blk`, `topology.csr.blk`,
  `vectors.f32.blk`), range indexes (`range/<name>.isam`), above-threshold ANN
  indexes (`vector/<label>.<prop>.{vamana,pq}`), and a `current` text pointer.
* Every block is zstd-compressed and BLAKE3-checksummed; with `--encrypt` each
  block is additionally sealed with XChaCha20-Poly1305 (AEAD at rest).
* The server opens a generation by **re-hashing every file** against the manifest,
  so a half-copied / truncated image — a torn copy onto the data dir, which may be
  remote/network storage — is refused rather than served.
* Reads flow through **three bounded cache pools** — a decompressed-block LRU, a
  vector-index pool (resident PQ codes + a Vamana-block LRU), and a result LRU —
  each with its own byte budget. This is what keeps RSS flat.

### Range indexes (ISAM)

A range index (`range/<name>.isam`, one per indexed `(label, property)`) lets a
`MATCH (n:Label {prop: v})` or `WHERE n.prop <op> v` resolve to the matching node
ids **without scanning the label**. It is an
**[ISAM](https://en.wikipedia.org/wiki/ISAM)** (Indexed Sequential Access Method)
structure — the classic *static, sorted, block-structured* index, which is exactly
the right shape for an immutable generation: there are no inserts to rebalance, so
the simplicity of ISAM buys what a B-tree's mutation machinery would only
complicate.

* Entries `(value, entity_id)` are sorted by value and packed into the same
  zstd-compressed 256 KiB blocks as everything else.
* A small **resident top-level** holds the first key of each block (a sparse
  index). A lookup binary-searches that in-memory top level to find the *one* block
  a key can be in, reads + decompresses that block, and scans it — so an equality
  lookup is **one block read**, and a range scan walks the contiguous run of blocks
  it spans. (This is why a `meshUi`-indexed lookup is single-digit milliseconds
  while the same match on an unindexed property scans the whole label.)
* The planner picks it via `NodeScan::RangeEq` / `RangeRange`; an unindexed
  predicate falls back to a label sweep or full scan, with the executor re-checking
  every predicate either way.

### Vector search (Vamana + PQ)

Vector KNN (`db.idx.vector.queryNodes`) has two execution paths, chosen per index
at build time by the `--ann-threshold` (default 50 000 vectors):

* **Below the threshold — brute force.** The full `f32` vectors live in
  `vectors.f32.blk`; a query scans the index's group and computes exact cosine
  distances. Simple and exact; fine when the vector set is small.
* **At or above the threshold — Vamana + PQ**, the disk-native ANN path that keeps
  resident memory bounded regardless of how many vectors there are:
  * **[Vamana](https://arxiv.org/pdf/2401.11324)** is the graph index from the
    DiskANN line of work: a single proximity graph whose edges are pruned (the
    `--vamana-r` out-degree and `--vamana-alpha` long-edge factor) so a *greedy
    beam search* — start at the medoid, repeatedly hop toward the query, keeping a
    candidate list of width `vectorQuery.beamWidth` — reaches a node's true
    neighbours in few hops, i.e. **few random block reads per query**. The graph
    blocks (`vector/<label>.<prop>.vamana`) are paged in through the vector cache,
    not held wholesale.
  * **[Product quantisation (PQ)](https://medium.com/aiguys/product-quantization-k-nn-for-big-datasets-12431d764c4e)**
    compresses each vector into a short code (`--pq-subspaces` × `--pq-bits`): the
    dimensions are split into subspaces, each independently k-means-clustered, and
    the vector is stored as the tuple of nearest-centroid ids. These codes
    (`vector/<label>.<prop>.pq`) are small enough to keep **resident**, so the
    beam search scores candidates from RAM and only the chosen few full vectors are
    read from disk. That resident PQ set is what the `cache.vectorCacheBytes` pool
    pins.

## Mounts

The container runs with a **read-only root filesystem** and a non-root user
(`appuser:1000`). Everything Slater needs is mounted read-only:

| Path | Purpose | Notes |
| --- | --- | --- |
| `/data` | The graph generations (`<graph>/<uuid>/…` + `current`). | **Read-only**; produced by `slater-build`. May live on remote/network storage (e.g. NFS), so reads are not assumed to be fast local-SSD latencies. |
| `/sandbox` | Per-environment config overlay + secrets. | `/sandbox/config.json` is deep-merged over the baked-in `config.json`; also holds `acl.json`, TLS PEM material, the at-rest key file. |
| `/tmp`, `/run` | Scratch (`tmpfs`). | Slater itself never writes to disk. |

## Environment / configuration

Config is loaded by the house-standard layered loader: the baked-in `config.json`,
then `/sandbox/config.json` deep-merged over it, then `KEY__sub` environment
overrides (double underscore for nesting; keys match the camelCase config).

| Key | Env override | Default | Meaning |
| --- | --- | --- | --- |
| `server.bind` | `server__bind` | `0.0.0.0` | Bind address. |
| `server.port` | `server__port` | `7687` | Bolt port. |
| `dataDir` | `dataDir` | `/data` | Root holding `<graph>/<generation>/`. |
| `aclPath` | `aclPath` | `/config/acl.json` | JSON ACL (users → per-graph read grants). |
| `requireAclStamp` | `requireAclStamp` | `true` | Refuse a generation with no `aclBlake3` stamp (closes the stamp-strip downgrade); build images with `--acl`. A generation with no manifest MAC is always refused when a master key is configured — that check has no off switch. |
| `cache.blockCacheBytes` | `cache__blockCacheBytes` | 64 MiB | Decompressed block LRU budget. |
| `cache.vectorCacheBytes` | `cache__vectorCacheBytes` | 32 MiB | Vector pool (resident PQ + Vamana-block LRU) budget. |
| `cache.resultCacheBytes` | `cache__resultCacheBytes` | 16 MiB | Result LRU budget. |
| `tls.cert` / `tls.key` | `tls__cert` / `tls__key` | _(empty)_ | PEM material; both set ⇒ `bolt+s`. Empty ⇒ plaintext (loopback dev). |
| `encryption.keyFile` | `encryption__keyFile` | _(empty)_ | File holding the hex at-rest master key. Must live **outside** `dataDir` and any attacker-writable path (server refuses to start if it resolves inside `dataDir`); see `THREAT_MODEL.md` "Trust boundary". |
| `encryption.keyEnv` | `encryption__keyEnv` | _(empty)_ | Env var holding the hex at-rest master key. |
| `query.maxRows` | `query__maxRows` | 100000 | Per-query row cap. |
| `query.timeoutMs` | `query__timeoutMs` | 30000 | Per-query wall-clock deadline (0 ⇒ none). |
| `query.maxIntermediate` | `query__maxIntermediate` | 1000000 | Per-query intermediate-element budget (0 ⇒ none); ~48 B/element, so the default bounds one query at ≈48 MB. |
| `vectorQuery.beamWidth` | `vectorQuery__beamWidth` | 64 | Vamana beam-search list size. |
| `generationPollMs` | `generationPollMs` | 5000 | How often to poll each graph's `current`. |
| `reloadStrategy` | `reloadStrategy` | `exit` | `exit` or `swap` on a generation change. |

**Resident memory** is approximately
`blockCacheBytes + vectorCacheBytes + resultCacheBytes` + a small fixed overhead,
**independent of graph size** — that is the headline guarantee, exercised by the
`rss_stays_bounded_under_sustained_knn_load` integration test.

### Generation guard

Slater polls each graph's `current` pointer every `generationPollMs`
(**poll, not inotify** — the data dir may be remote/network storage like NFS,
where filesystem change events are unreliable). When it changes:

* `reloadStrategy=exit` (default): the server logs fatal and exits non-zero so the
  orchestrator restarts it cleanly against the new generation.
* `reloadStrategy=swap`: the server opens **and validates** the new generation
  (same content-hash guard as boot), atomically swaps it in, and lets in-flight
  queries finish on the old one. A corrupt/incomplete new image is refused and the
  old generation keeps serving.

## ACL

`acl.json` maps users to argon2id password hashes and per-graph **read** grants.
Mint a hash (never store cleartext) with:

```sh
slater hash-password 's3cret'        # prints a $argon2id$… string for acl.json
```

## Health check

The `slater` binary doubles as its own liveness probe: `slater healthcheck [host]
[port]` performs a **Bolt handshake** (not an HTTP request) against the server and
exits `0` if it negotiates a protocol version, `1` otherwise — defaulting to
`localhost` and the configured Bolt port. This is what the container
`HEALTHCHECK` runs, so orchestrators see a truly Bolt-ready server, not just an
open socket:

```sh
slater healthcheck localhost 7687    # exit 0 = healthy
docker exec slater /app/slater healthcheck   # inside the container
```

## Worked example

Build a small graph, serve it, and query it with the neo4j **JavaScript** and
**Python** drivers.

### 1. Build a generation

`people.cypher` (primitive-Cypher dump dialect — what `slater-build` consumes):

```cypher
CREATE (:Person {name: 'Alice', age: 30, embedding: vecf32([0.1, 0.2, 0.3])});
CREATE (:Person {name: 'Bob',   age: 25, embedding: vecf32([0.2, 0.1, 0.0])});
CREATE (:Person {name: 'Carol', age: 40, embedding: vecf32([0.9, 0.8, 0.7])});
CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'});
CREATE INDEX FOR (p:Person) ON (p.name);
CALL db.idx.vector.createNodeIndex('Person', 'embedding', 3, 'COSINE');
```

```sh
slater-build \
  --input people.cypher \
  --graph people \
  --data-dir ./data
# prints the new generation UUID + content hash; writes ./data/people/<uuid>/
# and ./data/people/current
```

Mint an ACL entry and start the server (plaintext, for local dev):

```sh
slater hash-password 'pw'   # paste the hash into acl.json under users.reporting
slater                      # reads ./config.json (dataDir ./data, port 7687)
```

### 2. Connect with the neo4j JavaScript driver

```js
import neo4j from 'neo4j-driver';

// Use 'bolt://' for plaintext dev, 'bolt+s://' when TLS is configured.
const driver = neo4j.driver('bolt://localhost:7687',
  neo4j.auth.basic('reporting', 'pw'));
const session = driver.session({ database: 'people' });

// A plain MATCH … RETURN.
const r1 = await session.run(
  'MATCH (p:Person) WHERE p.age >= $min RETURN p.name AS name ORDER BY name',
  { min: 28 });
console.log(r1.records.map(rec => rec.get('name')));   // [ 'Alice', 'Carol' ]

// A cosine-KNN query (the one permitted procedure).
const r2 = await session.run(
  `CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([0.1, 0.2, 0.3]))
   YIELD node, score RETURN node.name AS name, score ORDER BY score`);
console.log(r2.records.map(rec => [rec.get('name'), rec.get('score')]));

await session.close();
await driver.close();
```

### 3. Connect with the neo4j Python driver

```python
from neo4j import GraphDatabase

# 'bolt://' plaintext for dev, 'bolt+s://' once TLS is configured.
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("reporting", "pw"))

with driver.session(database="people") as session:
    # A plain MATCH … RETURN.
    rows = session.run(
        "MATCH (p:Person) WHERE p.age >= $min RETURN p.name AS name ORDER BY name",
        min=28)
    print([r["name"] for r in rows])            # ['Alice', 'Carol']

    # A cosine-KNN query.
    knn = session.run(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, "
        "vecf32([0.1, 0.2, 0.3])) "
        "YIELD node, score RETURN node.name AS name, score ORDER BY score")
    print([(r["name"], r["score"]) for r in knn])

driver.close()
```

The KNN `score` is the **cosine distance** (ascending — nearest first).

## Development

```sh
export PATH="$HOME/.cargo/bin:$PATH"
cargo build
cargo test            # unit + the bounded-RSS headline integration test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

See `docs/PLAN.md`, `docs/PROGRESS.md` and `docs/DECISIONS.md` for the design,
the milestone ledger, and the decision log.

## Benchmarking

`perf/` ships the harnesses behind the headline claim — **resident memory bounded by
cache budgets, not graph size, at comparable query speed.** A tiny pole smoke-test
([`perf/`](perf/)) plus a cross-engine sweep
([`perf/cross-engine-hs/`](perf/cross-engine-hs/)) over MeSH, an EU-AI-Act vector graph,
and Wikidata at 1M and 91.6M nodes — the same query suite on **slater, Neo4j 5,
Memgraph, FalkorDB** (single client, latency medians, each engine restarted + warmed,
mean of 5 runs, varying parameters so the result cache always misses). It is a
correctness-and-footprint check, not a throughput benchmark.

The result is **one story in two regimes**: memory stays bounded whether the graph
**fits in RAM** or is **far larger than it**.

**Memory is bounded as the graph grows ~1,500×.** Peak RSS while serving, slater on
default cache budgets:

| graph (nodes / edges) | slater | Neo4j 5 | Memgraph | FalkorDB |
|---|--:|--:|--:|--:|
| pole — 62k / 106k | **82 MiB** | 774 | 115 | 140 |
| MeSH — 340k / 469k | **262 MiB** | 1,117 | 355 | 454 |
| Wikidata — 1M / 13.8M | **645 MiB** | 2,012 | 2,716 | 1,506 |
| Wikidata — 91.6M / 766M | **~0.9 GiB †** | 2,911 | cannot-load | cannot-load |

† anon high-water (at 14 GB on disk the OS page cache dominates the cgroup peak; anon is
the engine's own footprint). slater's RSS tracks the **query working set**, not the
graph — idle is ~16–70 MiB at every scale. The in-memory engines grow ~linearly with the
data and, on the 766M-edge graph (working set ≫ the 15 GiB host), **Memgraph and
FalkorDB can't load it at all** — only the disk-backed engines serve it.

**…at comparable-to-faster speed, in both regimes** (latency, ms, median):

| in-RAM — pole 62k / 106k | slater | Neo4j 5 | Memgraph | FalkorDB |
|---|--:|--:|--:|--:|
| count(*) all nodes | **0.6** | 6.0 | 3.5 | 3.6 |
| indexed point lookup | **0.6** | 4.2 | 0.5 | 0.5 |
| 1-hop traversal | 2.4 | 6.9 | 1.4 | 0.8 |
| group-by / count(DISTINCT) | **2.8** | 7.7–9.4 | 6.8–7.2 | 3.9–4.5 |

| disk-bound — Wikidata 91.6M / 766M | slater | Neo4j 5 |
|---|--:|--:|
| count all nodes | **0.58** | ~4,000 |
| point lookup (indexed) | **1.30** | 9.7 |
| 1-hop neighbours | **4.25** | 12.3 |
| 3-hop | **26.7** | 74.9 |
| shortestPath ≤6 | **52.6** | 131.9 |

When the graph **fits in RAM** the latencies are close — slater wins counts /
aggregations / `DISTINCT` via its metadata + index fast paths and trails the in-memory
engines on raw multi-hop. When the graph is **far larger than RAM** slater matches or
beats the only other engine that can load it, at **~⅓ the RAM**: `count` is
metadata-served (0.58 ms vs a ~4 s disk scan) and per-query `maxFanout` parallelism
carries shortestPath 82.6 → 52.6 ms. (Vector kNN is the one shape slater trails today —
an exact brute-force scan vs the others' resident HNSW: an algorithmic gap, not memory.)

Full per-engine tables — MeSH, the EU-AI-Act vector suite + the `blockCacheBytes`
RAM↔latency dial, Wikidata 1M & 91.6M, and the full-Wikidata bulk-load + parallelism
figures — are in [`perf/cross-engine-hs/README.md`](perf/cross-engine-hs/README.md); the
pole harness and method are in [`perf/PERF_PROGRESS.md`](perf/PERF_PROGRESS.md).

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE) for the
full text and [`NOTICE`](NOTICE) for attribution. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this work,
as defined in the Apache 2.0 license, shall be licensed as above, without any
additional terms or conditions.

SPDX-License-Identifier: Apache-2.0
