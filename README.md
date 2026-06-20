# Slater

[![CI](https://github.com/Hikari-Systems/slater/actions/workflows/ci.yml/badge.svg)](https://github.com/Hikari-Systems/slater/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Hikari-Systems/slater?sort=semver)](https://github.com/Hikari-Systems/slater/releases/latest)

> **In one line:** Slater serves a graph database the way SQLite serves a relational one — you bake the data into an immutable file, ship it anywhere, and serve it read-only with **memory cost that's flat no matter how big the graph gets**.

---

**Shortcuts**

|  |  |  |  |
|:--|:--|:--|:--|
| [Why Slater exists](#why-slater-exists) | [What you get](#what-you-get) | [Features](#features) | [GQL subset](#supported-gql-subset) |
 | [Running with Docker](#running-with-docker) | [How it works](#how-it-works) | [Mounts](#mounts) | [Configuration](#environment--configuration) |
| [ACL](#acl) | [Health check](#health-check) | [Worked example](#worked-example) | [Development](#development) |
| [Performance](#performance) | [License](#license) |
## Why Slater exists

A **graph database** stores data as *things* (nodes) and the *relationships between them* (edges), with the relationships as first-class citizens. That's what you want when your questions are about connections rather than rows — "who's within three hops of this account?", "what's the full dependency chain behind this build?", "which accounts share a device, an address, and a card?" — the queries that become a swamp of recursive joins in SQL but fall out naturally in a graph.

The catch with most graph databases (neo4j, Memgraph, FalkorDB) is that they're built to *write* as much as read: transactional, clustered, and holding the whole graph resident in RAM. A 40&nbsp;GB graph wants 40&nbsp;GB of memory — *per instance*. Want a replica per region, per tenant, or per pod? Multiply the bill.

Slater is built for the other half of the problem: graphs that are **mostly read and rebuilt in batches** — knowledge graphs for RAG, recommendation and identity graphs, dependency graphs. You build the graph once, offline, into an immutable on-disk image; then any number of Slater servers serve it read-only over **Bolt** (so your existing neo4j drivers just work) while holding only a fixed cache budget in memory. **A 4&nbsp;GB graph and a 400&nbsp;GB graph cost the same RAM to serve.**

> **On the name.** Slater is named after the CIA agent in *Archer* (a great show)
> who insists on going by a single name — "Just… Slater" — and one of my favourite
> characters in it. See the
> [character wiki page](https://archer.fandom.com/wiki/Slater).

### What you get

- **RAM set by your cache budget, not your graph size** — fan out as many read replicas as you like.
- **A drop-in for the read path** — speaks Bolt, so any standard neo4j driver (JS, Python, Go…) works unchanged. It's the read subset of Cypher; nothing new to learn.
- **Deployment by file swap** — build a new content-hashed *generation* offline, atomically flip the `current` pointer, and servers pick it up. Every block is checksummed, so a half-copied image is refused rather than served.
- **Vector search built in** — disk-native approximate-nearest-neighbour (cosine KNN) sits right next to your graph, for when this is the retrieval layer behind a RAG pipeline.
- **Locked down by design** — read-only by construction, optional at-rest encryption, TLS Bolt, argon2id-hashed ACLs, read-only container rootfs.

**When *not* to use it:** if you need live writes, transactional mutation, or full Cypher write semantics, Slater isn't your engine — it's deliberately the serving half. Pair it with whatever builds your graph upstream.

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

## Storage backends (filesystem / S3)

Every generation file is opened through an **`ObjectStore`** abstraction rather
than `std::fs` directly, so the *same* on-disk byte format — blocks, indexes,
manifest, `current` pointer — can be served from different storage without
changing the readers, the query engine, or the integrity checks. Only *where the
bytes come from* differs. The hot path is positional reads (`read_exact_at`),
which map onto a `pread` on a local file and an HTTP `Range` GET on an object
store; Slater never mmaps, so the explicit, bounded-read model is identical
across backends. The backend is selected by `dataBackend.kind`:

* **`fs` (default) — local filesystem**, rooted at `dataDir`. This is the right
  choice for the overwhelming majority of deployments: a generation on a local
  SSD (or an NFS/EBS mount) served read-only. Integrity is a BLAKE3 re-hash of
  every file at open.

* **`s3` — an S3 (or S3-compatible: MinIO, localstack) bucket.** Generation files
  are objects under a key prefix; the readers' positional reads become `Range`
  GETs. The published image is built with the `s3` feature, so this needs no
  custom build — just configuration. Integrity is verified from S3's
  **server-computed SHA-256 object checksum** via a metadata `HEAD` (no body
  read): `slater-build` sends each object's SHA-256 on upload (S3 validates and
  stores it), and the server reads it back at open and compares it to the
  manifest. Credentials come from the standard AWS chain (env / profile /
  instance role).

**When is S3 appropriate?** Reach for it when you want generations to live in
durable, central object storage rather than on a node's disk — typically:
publish once and fan out to many stateless, disk-less server replicas that all
read the same bucket; decouple the build host from the serve hosts; or lean on
S3's durability/versioning/lifecycle instead of managing volumes. The trade-off
is latency: a cold block is a network round-trip (~10–50 ms) instead of a local
read (~0.1 ms). Slater hides much of this with the in-memory block cache and
concurrent read-ahead, **and** with the optional disk cache below — but if your
generations already sit on fast local storage and you don't need the
central-bucket model, `fs` is simpler and faster. The byte format is identical,
so you can switch backends without rebuilding a generation.

### S3 local-disk block cache (second tier)

The in-memory `BlockCache` is deliberately small (bounded RSS is the headline
guarantee), so on a working set larger than RAM the same blocks would be
re-fetched from S3 on every spill. An **optional local-SSD second cache tier**
fixes that: a block evicted from RAM is served from local disk (~0.1 ms) instead
of a fresh S3 GET, surviving in-memory eviction and cutting S3 request
count/cost — bringing an S3-backed node close to local-filesystem performance
once warm. It is **opt-in** for the `s3` backend, enabled by setting
`dataBackend.s3.diskCacheBytes > 0` and a writable `diskCacheDir`.

* It caches the **sealed** S3 bytes exactly as fetched — already compressed, and
  (for `--encrypt` generations) still AEAD-sealed — *below* decrypt/decompress.
  The cache layer never holds the encryption key and never re-encrypts, so
  at-rest status is preserved for free: an encrypted generation lands on disk
  still sealed.
* Writes are **write-behind**: a miss returns the S3 bytes to the query
  immediately, then a background thread does the disk write and LRU trim, so the
  query path never blocks on disk I/O. Eviction keeps the cache within its byte
  budget; a per-file checksum verified on every read self-heals a corrupt cache
  file to a miss (→ S3 refetch).
* `diskCacheDir` **must point at a real writable volume — never `tmpfs`** (tmpfs
  is RAM and would defeat the bounded-RSS guarantee). The in-memory index that
  tracks it costs a little RAM (~tens of bytes per cached block), which counts
  against your RSS ceiling — size the directory ≫ the in-memory block cache.

### Publishing to S3

`slater-build` always writes the finished generation to `--data-dir` first (its
local staging area), and can **additionally** upload it to a bucket with the
`--publish-s3-*` flags — the remote `current` pointer is written last, so a
serving node never sees a half-published generation:

```sh
slater-build --input people.cypher --graph people --data-dir /data \
  --publish-s3-bucket slater --publish-s3-region eu-west-2 \
  --publish-s3-prefix prod            # add --publish-s3-endpoint / --publish-s3-path-style for MinIO
```

## Mounts

The container runs with a **read-only root filesystem** and a non-root user
(`appuser:1000`). Everything Slater needs is mounted read-only:

| Path | Purpose | Notes |
| --- | --- | --- |
| `/data` | The graph generations (`<graph>/<uuid>/…` + `current`). | **Read-only**; produced by `slater-build`. May live on remote/network storage (e.g. NFS), so reads are not assumed to be fast local-SSD latencies. |
| `/sandbox` | Per-environment config overlay + secrets. | `/sandbox/config.json` is deep-merged over the baked-in `config.json`; also holds `acl.json`, TLS PEM material, the at-rest key file. |
| `/tmp`, `/run` | Scratch (`tmpfs`). | Slater itself never writes to disk by default. |
| _(optional)_ disk cache | The S3 local-disk block cache, when `dataBackend.s3.diskCacheBytes > 0`. | **Writable**, and a **real volume — not `tmpfs`**. Only used by the `s3` backend; see [Storage backends](#storage-backends-filesystem--s3). |

## Environment / configuration

Config is loaded by the house-standard layered loader: the baked-in `config.json`,
then `/sandbox/config.json` deep-merged over it, then `KEY__sub` environment
overrides (double underscore for nesting; keys match the camelCase config).

| Key | Env override | Default | Meaning |
| --- | --- | --- | --- |
| `server.bind` | `server__bind` | `0.0.0.0` | Bind address. |
| `server.port` | `server__port` | `7687` | Bolt port. |
| `server.maxConnections` | `server__maxConnections` | 16384 | Global concurrent-connection cap (0 ⇒ unlimited). A permit is taken **before `accept()`**, so at capacity back-pressure lands in the kernel listen backlog instead of the heap — this is what keeps resident memory bounded under adversarial connection load. |
| `server.maxPreAuthConnections` | `server__maxPreAuthConnections` | 4096 | Cap on connections not yet past `LOGON` (0 ⇒ unlimited). Smaller than `maxConnections` so an anonymous flood cannot starve authenticated readers. |
| `server.maxConnectionsPerIp` | `server__maxConnectionsPerIp` | 1024 | Per-source concurrent-connection cap (0 ⇒ unlimited); keyed on the /32 for IPv4 and the /64 for IPv6. |
| `server.maxPreAuthBytes` | `server__maxPreAuthBytes` | 65536 | Largest Bolt message accepted **before `LOGON`** (only `HELLO`/`LOGON` arrive then — a few hundred bytes). Ratchets up to `maxMessageBytes` on successful auth, back down on `LOGOFF`. |
| `server.maxMessageBytes` | `server__maxMessageBytes` | 67108864 | Largest Bolt message accepted from an **authenticated** reader (64 MiB). |
| `server.loginTimeoutMs` | `server__loginTimeoutMs` | 10000 | Deadline for an unauthenticated peer to finish handshake → `LOGON` (0 ⇒ none); closes the slow-loris a byte cap alone leaves open. |
| `server.idleTimeoutMs` | `server__idleTimeoutMs` | 0 | Idle read timeout for an **authenticated** connection (0 ⇒ none, the default — pooled drivers legitimately hold idle connections). |
| `dataDir` | `dataDir` | `/data` | Root holding `<graph>/<generation>/` (the `fs` backend, and the local staging area for `slater-build`). |
| `dataBackend.kind` | `dataBackend__kind` | `fs` | Storage backend: `fs` (local filesystem) or `s3` (object store). See [Storage backends](#storage-backends-filesystem--s3). |
| `dataBackend.verifyIntegrity` | `dataBackend__verifyIntegrity` | `true` | Verify each generation file against the manifest at open (a cheap metadata check on every backend). |
| `dataBackend.s3.bucket` | `dataBackend__s3__bucket` | _(empty)_ | S3 bucket name (required when `kind=s3`). |
| `dataBackend.s3.region` | `dataBackend__s3__region` | _(empty)_ | AWS region (e.g. `eu-west-2`); empty ⇒ resolved from the environment. |
| `dataBackend.s3.endpoint` | `dataBackend__s3__endpoint` | _(empty)_ | Custom endpoint URL for an S3-compatible store (MinIO, localstack); empty ⇒ standard AWS endpoint. |
| `dataBackend.s3.prefix` | `dataBackend__s3__prefix` | _(empty)_ | Key prefix every generation key is joined under; empty ⇒ bucket root. |
| `dataBackend.s3.pathStyle` | `dataBackend__s3__pathStyle` | `false` | Path-style addressing (`endpoint/bucket/key`); required by most S3-compatible servers. |
| `dataBackend.s3.diskCacheBytes` | `dataBackend__s3__diskCacheBytes` | `0` | Byte budget for the **local-disk block cache** (second tier). `0` ⇒ disabled. When `> 0`, `diskCacheDir` is required. Size it ≫ `blockCacheBytes`; the in-memory index counts against the RSS ceiling. |
| `dataBackend.s3.diskCacheDir` | `dataBackend__s3__diskCacheDir` | _(empty)_ | Directory for the disk cache (used iff `diskCacheBytes > 0`). Must be a **real writable volume — never `tmpfs`**. |
| `aclPath` | `aclPath` | `/config/acl.json` | JSON ACL (users → per-graph read grants). |
| `requireAclStamp` | `requireAclStamp` | `true` | Refuse a generation with no `aclBlake3` stamp (closes the stamp-strip downgrade); build images with `--acl`. A generation with no manifest MAC is always refused when a master key is configured — that check has no off switch. |
| `cache.blockCacheBytes` | `cache__blockCacheBytes` | 64 MiB | Decompressed block LRU budget. |
| `cache.vectorCacheBytes` | `cache__vectorCacheBytes` | 64 MiB | Vector pool budget: resident brute-force kNN matrix (pre-normalised, no-gather scan) + resident PQ + Vamana-block LRU. kNN falls back to the block-cache gather path for any group that does not fit. |
| `cache.resultCacheBytes` | `cache__resultCacheBytes` | 16 MiB | Result LRU budget. |
| `tls.cert` / `tls.key` | `tls__cert` / `tls__key` | _(empty)_ | PEM material; both set ⇒ `bolt+s`. Empty ⇒ plaintext (loopback dev). |
| `encryption.keyFile` | `encryption__keyFile` | _(empty)_ | File holding the hex at-rest master key. Must live **outside** `dataDir` and any attacker-writable path (server refuses to start if it resolves inside `dataDir`); see `THREAT_MODEL.md` "Trust boundary". |
| `encryption.keyEnv` | `encryption__keyEnv` | _(empty)_ | Env var holding the hex at-rest master key. |
| `query.maxRows` | `query__maxRows` | 100000 | Per-query row cap. |
| `query.timeoutMs` | `query__timeoutMs` | 30000 | Per-query wall-clock deadline (0 ⇒ none). |
| `query.maxIntermediate` | `query__maxIntermediate` | 1000000 | Per-query intermediate-element budget (0 ⇒ none); ~48 B/element, so the default bounds one query at ≈48 MB. |
| `query.maxIntermediateGlobal` | `query__maxIntermediateGlobal` | 8000000 | Server-wide ceiling on the sum of all in-flight queries' intermediate elements (0 ⇒ none). Bounds the aggregate so `N` concurrent heavy queries can't multiply the per-query budget into an OOM; ~48 B/element ⇒ ≈384 MB. |
| `vectorQuery.beamWidth` | `vectorQuery__beamWidth` | 64 | Vamana beam-search list size. |
| `generationPollMs` | `generationPollMs` | 5000 | How often to poll each graph's `current`. |
| `reloadStrategy` | `reloadStrategy` | `exit` | `exit` or `swap` on a generation change. |

**Resident memory** is approximately
`blockCacheBytes + vectorCacheBytes + resultCacheBytes` + a small fixed overhead,
**independent of graph size** — that is the headline guarantee, exercised by the
`rss_stays_bounded_under_sustained_knn_load` integration test. Per-connection
buffers live *outside* the cache budgets, so the guarantee holds under adversarial
load only because `server.maxConnections` bounds how many can exist at once.

### Network posture

Slater is a read replica handle; the **primary** connection-security control is the
network, not the binary. Bind it to a private interface, restrict source ranges at
the network layer (security groups / NetworkPolicy), and — if it faces anything but
trusted clients — front it with a connection-limiting L4 proxy (HAProxy `maxconn` +
a per-source `stick-table`, or nftables `connlimit` + `hashlimit`). That sits before
the file descriptor is ever handed to the process, so it is the most robust limit.

The in-binary limits above (`maxConnections`, `maxPreAuthConnections`,
`maxConnectionsPerIp`, the differential byte caps, and `loginTimeoutMs`) are
**defence-in-depth**: they default on and generous so they are invisible to a
legitimate client population, but they make the bounded-RSS guarantee hold even when
the proxy is forgotten. See **[`docs/HARDENING.md`](docs/HARDENING.md)** for the full
defensive posture, and `THREAT_MODEL.md` / `SECURITY_WORKLIST.md` for the canonical
detail.

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

First mint a password hash and write the `acl.json` — it has to exist
*before* the build, because `slater-build --acl` stamps the file's BLAKE3
digest into the manifest and the server refuses a generation whose stamp
doesn't match the live ACL (`requireAclStamp` is on by default):

```sh
slater hash-password 'pw'   # prints a $argon2id$… string — copy it
```

`acl.json` (next to `config.json`), granting the `myuser` user **read**
on the `people` graph:

```json
{
  "users": {
    "myuser": {
      "passwordArgon2id": "$argon2id$v=19$m=19456,t=2,p=1$…paste the hash here…",
      "grants": {
        "people": ["read"]
      }
    }
  }
}
```

Now build the generation, stamping that ACL into the manifest:

```sh
slater-build \
  --input people.cypher \
  --graph people \
  --data-dir ./data \
  --acl ./acl.json
# prints the new generation UUID + content hash; writes ./data/people/<uuid>/
# and ./data/people/current
```

Then start the server:

```sh
slater    # looks for configuration options in ./config.json (default is dataDir ./data, port 7687)
```

### 2. Connect with the neo4j JavaScript driver

```js
import neo4j from 'neo4j-driver';

// Use 'bolt://' for plaintext dev, 'bolt+s://' when TLS is configured.
const driver = neo4j.driver('bolt://localhost:7687',
  neo4j.auth.basic('myuser', 'pw'));
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
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("myuser", "pw"))

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

## Performance

Six engines, one single-client suite, five graphs from a 62k-node toy to **Wikidata
91.6M nodes / 766M edges**. Each engine is **measured in isolation** (every other container
stopped — RSS and latency are its own footprint). **slater is current `main`**; the other
engines are the established cross-engine run (their performance is unchanged). All figures
are medians (ms) or peak resident memory (MiB). **Lower is better everywhere; bold = best in
row.**

| engine | class | memory bound |
|---|---|---|
| **slater** | disk-backed, paged | `query.maxIntermediate` caps the working set automatically |
| Neo4j 5 | disk-backed, JVM | ~2 GiB heap + off-heap, committed regardless of query |
| Memgraph · FalkorDB | in-memory | whole graph resident in RAM |
| ArcadeDB | in-memory, JVM | whole graph resident; heaviest |
| LadybugDB | embedded, columnar | manual buffer pool that must exceed the query |

The three engines that **page from disk** — slater, Neo4j 5, and LadybugDB — load all five
graphs. The **in-memory trio** (Memgraph · FalkorDB · ArcadeDB) cannot hold the 766M graph at
all (it needs ~64–128 GiB resident), and ArcadeDB's importer can't finish it either.

### Resident memory (MiB) — bounded as the graph grows ~1,500×

Each figure is **committed working memory** — what the OS cannot reclaim. Every engine *except*
slater holds its graph in committed anonymous memory (own heap, Neo4j's off-heap page cache, or
a buffer pool), so its peak RSS *is* its committed footprint. slater alone serves from the
**reclaimable OS page cache** of its on-disk store, so its figure is the anon working set; the
store's page cache (evictable under pressure — slater keeps serving) is excluded, and shown as
*total* in parentheses for the two wiki graphs. **Bold = lowest.**

| graph (nodes / edges) | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| pole — 62k / 106k | **11** | 746 | 114 | 140 | 1,556 | 198 |
| MeSH — 341k / 469k | **63** | 1,083 | 358 | 455 | 1,631 | 121 |
| EU-AI-Act — 21k / 45k (+55 MiB vec) | **99** | 729 | 229 | 312 | 1,948 | 286 |
| Wikidata — 1M / 13.8M | **33** *(295 total)* | ~2,330 | 2,716 | 1,506 | 2,247 | ~774 |
| Wikidata — 91.6M / 766M | **584** *(4,595 total)* | ~2,900 | cannot-load | cannot-load | cannot-load | ~652 † |

slater is the **lowest at every scale** and grows ~50× while the graph grows ~1,500× — its
footprint tracks the *query working set*, not the graph (idle ~16–71 MiB throughout). The
in-memory trio grows ~linearly and can't load the 766M graph; Neo4j commits a ~2 GiB heap
regardless of query. († LadybugDB on the bounded shapes only — its hub / var-length /
shortestPath traversals at 766M need its read pool raised to ≥2 GiB, vs slater's automatic
`maxIntermediate` cap.) The build-time value→count histograms add negligible resident memory —
a few KB for a low-cardinality indexed column, and *zero* for unique-key graphs like Wikidata
(`wikidata_id` exceeds the histogram cardinality cap, so none is stored) — so these figures are
unchanged by that feature.

### Latency (median ms) — graph fits in RAM (MeSH, 341k / 469k)

| shape | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| count(*) all nodes | **0.57** | 15.0 | 23.8 | 16.4 | 82.0 | 2.2 |
| label count | **0.58** | 4.2 | 20.7 | 1.1 | 4.4 | 4.3 |
| indexed point lookup | 1.95 | 3.9 | **0.48** | **0.48** | 0.65 | 8.8 |
| idx-eq count | **0.60** | 4.9 | 5.0 | 2.0 | 381 | 2.5 |
| 1-hop (indexed anchor) | 1.32 | 5.8 | **1.21** | 4.1 | 390 | 4.9 |
| 2-hop (unanchored) | **1.9** | 5.6 | 8.5 | 16.7 | 444 | 6.4 |
| group-by / count(DISTINCT) | **0.50** | 47–51 | 63–64 | 31–39 | 411 | 5.3 |
| full-scan `CONTAINS` | **0.59** | 5.4 | 24.1 | 1.7 | 16.3 | 4.1 |

slater owns the **metadata / index / scan** shapes (count, label, idx-eq, scan — 10–150× the
service engines), the **unanchored multi-hop** (2-hop 1.9 ms via the relationship-type scan,
fastest in the field), and — via a build-time value→count histogram on the indexed grouping key —
the **whole-label group-by / count(DISTINCT)** (0.5 ms, ahead of LadybugDB's columnar 5.3 ms). The
in-memory servers keep only **point lookups & raw 1-hop** (0.5 ms vs slater's 1–2 ms). (pole
62k/106k looks the same: slater sole-fastest on count/scan, ~2–3 ms on hops.)

### Latency (median ms) — vectors (EU-AI-Act kNN, 15k × 1024-dim)

| shape | slater | Neo4j 5 | Memgraph | FalkorDB | LadybugDB |
|---|--:|--:|--:|--:|--:|
| kNN top-10 Concept | 3.1 | 8.6 | 1.9 | **1.2** | 2.8 |
| kNN top-10 Chunk | 2.2 | 5.7 | 1.9 | **1.5** | 3.2 |

slater answers kNN with an **exact brute-force** scan (these sets are below its 50k-vector
ANN threshold) where the others use an approximate resident HNSW — so slater's results are
exact (recall 1.0). A SIMD distance kernel + a resident, pre-normalised vector matrix
(v0.9.x) took Concept from ~23 → ~3.1 ms and Chunk from ~10 → ~2.2 ms, so slater now beats
Neo4j and LadybugDB and is within ~1.4× of Memgraph, trailing only FalkorDB — while exact.

### Latency (median ms) — graph ≫ RAM (Wikidata 91.6M / 766M)

Only the disk-backed engines load it (Memgraph / FalkorDB / ArcadeDB: cannot-load).

| shape | slater | Neo4j 5 | LadybugDB |
|---|--:|--:|--:|
| count(*) all nodes | **0.58** | ~4,000 | 34 |
| point lookup (indexed) | **1.30** | 9.7 | ~2,337 † |
| 1-hop neighbours | **4.25** | 12.3 | 22.9 |
| 2-hop | **17.4** ‡ | 17.4 | over-budget |
| 3-hop | **26.7** | 74.9 | over-budget |
| var-length `*1..2` distinct | **9.1** | 116 | over-budget |
| shortestPath ≤6 | **52.6** (fan 8) | 131.9 | over-budget |

slater is **sole-fastest on every shape** at a fraction of the RAM — `count` is metadata-served
(0.58 ms vs Neo4j's ~4 s disk scan, ≈7000×). († LadybugDB builds no secondary index → the point
lookup is a full columnar scan; its hub/var-length shapes need its read pool raised to ≥2 GiB.
‡ cold 2-hop is the one ~tie with Neo4j.)

### Multi-hop `count(*)` — memory decoupled from result size

Uncapped multi-hop `RETURN count(*)` counts *during* expansion instead of materialising
the matched rows. Same hub anchors on the 91.6M graph, `maxIntermediate=20M`:

| 3-hop count(*) @ 91.6M | fanout=1 | fanout=8 |
|---|--:|--:|
| latency / peak working set | **554 ms / 0.66 GiB** | **298 ms / 1.9 GiB** |

The count holds O(1) rows. Charging is unchanged, so a mega-hub count still trips
`maxIntermediate` on *compute* (adjacency reads), bounded as before.

### Per-query parallelism (`maxFanout`)

Raising `query.maxFanout` overlaps a query's **cold, I/O-bound** block reads across cores —
it helps large-cold-working-set disk-bound shapes and is flat on warm shapes. On the 766M
graph: shortestPath ≤6 **918 → 608 ms** (1.5×, largest search 6,269 → 2,350 ms, 2.7×);
3-hop count **547 → 298 ms**. `maxFanout=1` is the default (throughput-oriented); `8` is the
latency dial, at more transient worker memory.

### Where slater wins / trails

| dimension | slater | best of the field | verdict |
|---|---|---|---|
| resident memory, any scale | 11–584 MiB (62k → 91.6M) | in-memory 1.5–2.7 GiB; can't load 766M | **slater** |
| count / metadata / scan | ~0.6 ms | service engines 5–80 ms | **slater** (10–150×) |
| indexed point / 1-hop | 1–2 ms | Memgraph · FalkorDB **0.5 ms** | trails the in-memory pair |
| unanchored multi-hop (rows) | **1.9 ms** (MeSH 2-hop) | Neo4j 5.6 ms | **slater** (relationship-type scan) |
| aggregation (group-by / DISTINCT) | **0.5 ms** | LadybugDB 5 ms (columnar) | **slater** (build-time histogram) |
| kNN | 2–3 ms (exact) | FalkorDB **1.2 ms** (HNSW) | beats Neo4j/Ladybug; ~1.4× off Memgraph; exact |
| traversal at 91.6M (≫ RAM) | 0.6–53 ms | Neo4j 10–4,000 ms; in-mem can't load | **slater** |
| multi-hop `count(*)` at scale | 0.3–0.6 GiB | in-memory engines materialise the row set | **slater**, bounded |

Full per-engine tables (pole, MeSH, EU-AI-Act + the `blockCacheBytes` RAM↔latency dial,
Wikidata 1M & 91.6M) are in
[`perf/cross-engine-hs/README.md`](perf/cross-engine-hs/README.md); the fresh slater-only pass
(both fanouts, every dataset) is in [`perf/PERF_CURRENT_STATUS.md`](perf/PERF_CURRENT_STATUS.md).

### Concurrency & brown-out (load testing)

The benchmarks above are single-client. The complementary axis — **behaviour under many
concurrent clients** — has its own harness, [`perf/loadtest/`](perf/loadtest/): a Locust
driver over Bolt plus a coordinator that ramps load, reads `CALL slater.diagnostics()`,
finds the capacity knee, and names the limiter (full method in
[`docs/LOAD-TESTING.md`](docs/LOAD-TESTING.md)). Headlines from a 256 MiB-cache run on the
Wikidata-1M graph (one 16-core box):

| result | measurement |
|---|---|
| Holds to **1000 concurrent clients, zero failures** | throughput peaks ~3k rps; the latency knee (p99 40 → 520 ms) is queueing under core contention, not a hard cap |
| Block cache **bounded and effective** | 100% hit rate, 0 evictions, 50 MB resident for a cache-fitting working set |
| RSS held under sustained load | `MALLOC_ARENA_MAX=2` + trim threshold holds RSS to ~0.5–0.6 GB (was ~2.7 GB retained allocator high-water — not a leak) |
| Aggregate memory bounded | server-wide **`query.maxIntermediateGlobal`** + adjacency-charged expansion hold the `wiki_budget` 2-hop flood at 1000 clients without OOM (RSS ~0.6 GB; the guard sheds ~60% of hub queries as retryable budget errors) |

Both memory issues the load test surfaced are now closed; all tracked in the load-testing doc.

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE) for the
full text and [`NOTICE`](NOTICE) for attribution. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this work,
as defined in the Apache 2.0 license, shall be licensed as above, without any
additional terms or conditions.

SPDX-License-Identifier: Apache-2.0
