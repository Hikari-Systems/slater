# Slater

[![CI](https://github.com/Hikari-Systems/slater/actions/workflows/ci.yml/badge.svg)](https://github.com/Hikari-Systems/slater/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Hikari-Systems/slater?sort=semver)](https://github.com/Hikari-Systems/slater/releases/latest)

> **In one line:** Slater serves **graphs that don't fit in memory** — hundreds of millions of nodes and billions of edges in low hundreds of MB of RAM — over standard **Bolt**, so any neo4j driver just works, with disk-native vector search sitting next to the graph, and it takes **live, durable writes** without giving that up. Resident memory is set by a cache budget you choose, **not** by the size of the graph.

---

**Shortcuts**

|  |  |  |  |
|:--|:--|:--|:--|
| [Why Slater exists](#why-slater-exists) | [Reads and writes](#reads-and-writes) | [What you get](#what-you-get) | [Features](#features) | [Running with Docker](#running-with-docker) | 
| [How it works](#how-it-works) | [The writable layer](#the-writable-layer) | [Storage backends](#storage-backends-filesystem--s3--gcs) | [Mounts](#mounts) | [Configuration](#environment--configuration) |
| [ACL](#acl) | [Health check](#health-check) | [Worked example](#worked-example) | [Development](#development) |
| [Performance](#performance) | [License](#license) |
## Why Slater exists

A **graph database** stores data as *things* (nodes) and the *relationships between them* (edges), with the relationships as first-class citizens. That's what you want when your questions are about connections rather than rows — "who's within three hops of this account?", "what's the full dependency chain behind this build?", "which accounts share a device, an address, and a card?" — the queries that become a swamp of recursive joins in SQL but fall out naturally in a graph.

**The most common complaint about graph databases is that they don't scale past what you can hold in RAM.** Many of them (eg neo4j, Memgraph, FalkorDB, etc) keep the whole graph resident: a 40&nbsp;GB graph wants 40&nbsp;GB of memory — *per instance*. Want a replica per region, per tenant, or per pod? Multiply the bill. And past a certain size they simply won't load: eg the 90 million node / 1.5B-edge Wikidata graph needs ~64–128&nbsp;GiB resident, so the in-memory engines can't open it at all.

Slater is the rebuttal. It serves that same 91.6M-node graph from **a few hundred MB of RAM**, because it pages the graph from an on-disk image on demand instead of holding it resident — so the graph size and the memory bill are decoupled.

Slater takes the opposite approach. You compile the graph once, offline, into a content-addressed on-disk image with `slater-build`; then any number of Slater servers serve it over **Bolt** (so your existing neo4j drivers just work) while holding only a fixed cache budget in memory. **A 4&nbsp;GB graph and a 400&nbsp;GB graph cost the same RAM to serve** — you fan out cheap, stateless read replicas and let the store, not the heap, hold the graph.

That makes it a natural fit for knowledge graphs behind RAG, recommendation and identity graphs, dependency graphs — anything large and connected you want to query cheaply and often. Disk-native vector search lives right next to the graph, so the same engine is the retrieval layer for embeddings too.

### Reads and writes

Slater is a **read-write** graph database. You don't rebuild the whole image to correct one property, add a node, or retract an edge — you write the change directly, over Bolt, and it lands durably. The trick is that writes never tax the read path.

Writes accumulate in a **log-structured-merge (LSM) layer over the immutable core**: a write-ahead log and an in-memory table, spilling to immutable delta segments, folded back into a fresh core by a periodic **consolidation**. What that buys you:

- **Reads over an unwritten graph cost exactly what they did before.** An empty delta is a single predictable branch, not a merge — the read path is byte-identical whether or not the writable layer is on.
- **The read cost of a write scales with the size of the delta, not the size of the graph.** Whole-graph answers — `count(*)`, the label and relationship-type marginals — stay *metadata reads* even with writes outstanding: the delta keeps its own counters, so a `count(*)` over a 91.6M-node core with half a million pending writes still answers in tens of milliseconds without touching a single block.
- **Acknowledged means durable.** A single writer drains the queue and returns `SUCCESS` only after the `fsync` that covers the write. Group your writes and they're cheap — a write-`UNWIND` commits one `fsync` per batch rather than per row.
- **Business-key writes, in either dialect.** `MERGE` / `MATCH … SET` / `DELETE` (and `CREATE` / `REMOVE`, detach delete, relationship writes) keyed on a node's identity property — or the equivalent ISO GQL data-modifying statements (`INSERT` / `SET` / `REMOVE` / `DELETE`), which lower onto the same path. Correct, insert, upsert and retract, over nodes and edges, addressed the way your data already is.

The writable layer is opt-in (`delta.enabled`); with it off, Slater serves the pure immutable core and refuses writes. See [The writable layer](#the-writable-layer) for the full model.

> **On the name.** Slater is named after the CIA agent in *Archer* (a great show)
> who insists on going by a single name — "Just… Slater" — and one of my favourite
> characters in it. See the
> [character wiki page](https://archer.fandom.com/wiki/Slater).

### What you get

- **RAM set by your cache budget, not your graph size** — fan out as many read replicas as you like; the graph never has to fit in memory.
- **A drop-in for the graph** — speaks Bolt, so any standard neo4j driver (JS, Python, Go…) works unchanged. It's Cypher (plus a slice of ISO GQL, reads and writes); nothing new to learn.
- **Live, durable writes** — an opt-in LSM layer over the immutable core: business-key `MERGE` / `SET` / `DELETE` over nodes and edges, group-committed and `fsync`-durable, folded back into a fresh core by consolidation. Reads don't pay for it.
- **Deployment by file swap** — build a new content-hashed *generation* offline, atomically flip the `current` pointer, and servers pick it up. Every block is checksummed, so a half-copied image is refused rather than served.
- **Vector search built in** — disk-native approximate-nearest-neighbour (cosine KNN) sits right next to your graph, for when this is the retrieval layer behind a RAG pipeline.
- **Locked down by design** — read and write grants are independent, plus optional at-rest encryption, TLS Bolt, argon2id-hashed ACLs, and a read-only container rootfs for read replicas.

## Features

| Feature | What it means for you |
|---|---|
| **Bounded, predictable memory** | Resident memory is capped by three cache budgets *you* set — it does **not** grow with graph size; you tune the performance/RAM trade-off instead of provisioning for the whole graph. A jemalloc allocator with background purge returns freed memory to the OS after heavy query bursts, so resident size falls back toward its idle floor rather than staying pinned at the post-burst high-water mark. |
| **Multi-tenant out of the box** | One server hosts many graphs with per-user read grants — multi-database isolation that most graph DBs reserve for a paid/enterprise tier. |
| **Encryption at rest & in transit** | Per-block XChaCha20-Poly1305 sealing (the key is never written to disk) plus optional TLS (`bolt+s://`). GDPR-friendly by construction. |
| **Tiny, dependency-light install** | A small stripped binary on a distroless glibc base (no shell/apt) — the multi-arch (amd64/arm64) image pulls at ~22 MB, or ~12 MB for the server-only `slater:latest-lite` tag; pure-Rust TLS, no OpenSSL. Pull and run. |
| **Built for periodic publish** | Build a graph offline, serve it immutable, then atomically swap in a new version with zero downtime — ideal for data-warehouse / scheduled-refresh workloads. |
| **Rugged under load** | The server and offline builder both compile with `#![forbid(unsafe_code)]` — the engine's only `unsafe` lives in the audited jemalloc allocator crate. The core is immutable, so reads take no locks and never wait on a writer; a single writer serialises mutations behind the write path alone. No GC pauses, no data races. One bad query can't take the server down. |
| **Works with your neo4j tools** | Speaks Bolt 5.4 / 4.4 / 4.1 — use the standard neo4j drivers (JS, Python, Go, Java…), `cypher-shell`, or graph browsers unchanged. |
| **Rich Cypher query surface** | A broad read surface: `MATCH`/`WHERE`/`WITH`/`UNION`, `CALL {…}` subqueries, 70+ functions & aggregations, temporal & geospatial values, and regex. |
| **Live, durable writes** | An opt-in single-writer LSM layer over the immutable core (`delta.enabled`): business-key `MERGE` / `SET` / `DELETE` / `CREATE` / `REMOVE` over nodes and relationships, batched write-`UNWIND` (one `fsync` per batch), and `CALL slater.consolidate()` — group-committed, `fsync`-durable, and folded back into a fresh core by consolidation. The read path is byte-identical when the delta is empty. |
| **ISO GQL, read and write** | Speaks a subset of **ISO GQL** (ISO/IEC 39075) over the same Bolt connection — quantified paths, path restrictors, shortest-path selectors, label/type boolean expressions, `FOR`, `CAST`, an optional `GQL`/`CYPHER` dialect prefix — and, with the writable layer on, GQL's data-modifying statements (`INSERT` / `SET` / `REMOVE` / `[DETACH] DELETE`) lower onto the same durable write path. Cypher and GQL, reads and writes, in one engine. |
| **Vectors + graph in one engine** | Disk-native ANN vector search (Vamana + PQ) for embeddings/RAG, plus graph algorithms (PageRank, BFS, betweenness, WCC…) — bounded memory even with millions of vectors. |
| **Safe on network storage** | Every file is BLAKE3 content-hashed and verified on open; torn or half-copied images are refused, not served. Designed for NFS/remote volumes (no mmap surprises). |
| **Pluggable storage backends** | Serve the same generation format from a local filesystem, an S3 (S3-compatible) bucket, **or** a Google Cloud Storage bucket — publish once, fan out to stateless replicas — with an optional local-SSD cache tier in front of the object store. See [Storage backends](#storage-backends-filesystem--s3--gcs). |

Two binaries make up the workspace:

| Binary | Role |
| --- | --- |
| `slater` | The online Bolt server (the container ENTRYPOINT): serves reads and, with `delta.enabled`, the single-writer durable write path. |
| `slater-build` | The offline compiler: turns a primitive-Cypher dump into an immutable, content-hashed generation directory. |

Slater splits *bulk building* from *serving*: `slater-build` does the heavy lifting
offline — ingesting your data and compiling it into an immutable generation — so a
cold graph is never assembled on the serving hot path. Within the server, the read
surface answers a broad Cypher slice — pattern matching, `WITH`/`UNION`/`CALL {…}`
subqueries, 70+ scalar & aggregate functions, temporal & geospatial values, graph
algorithms (`algo.*`), and disk-native vector KNN (`db.idx.vector.queryNodes`) —
while the writable layer's delta overlay sits *below* that surface and is zero-cost
when empty, so reads never carry the write-side machinery. You can update a graph
two ways: write to it live over Bolt (see [The writable layer](#the-writable-layer)),
or build a new generation offline and atomically swap the `current` pointer, which
the running server picks up via its generation guard (see
[Generation guard](#generation-guard)).

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

# Serve it over Bolt on 7687 (read-only unless `delta.enabled`):
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

### The writable layer

With `delta.enabled`, the immutable generation becomes the **fully-compacted bottom
level (the "core")** of a small log-structured-merge tree, and live writes ride on
top of it:

```
   write (Bolt)                                    read (Bolt)
        │                                               │
        ▼                                               ▼
   ┌──────────────┐  flush   ┌──────────────┐    ┌──────────────────────┐
   │ WAL + active │ ───────▶ │  L0 delta    │    │  a query pins one    │
   │  memtable    │          │  segments    │    │  (core, delta) view  │
   └──────────────┘          └──────┬───────┘    │  and reads the merge │
        (fsync = ack)               │            └──────────────────────┘
                          consolidation │  (folds core + delta → fresh core)
                                        ▼
                                 ┌─────────────┐
                                 │  new core   │  (atomic `current` swap)
                                 └─────────────┘
```

* **Durability floor — the WAL.** Each mutation is serialised behind a single
  writer per graph, appended to a per-graph write-ahead log, and `fsync`ed before
  the Bolt `SUCCESS` is returned — so *acknowledged ⇒ durable*, and a torn tail is
  dropped on replay. A batched write-`UNWIND` appends its rows and commits **one**
  `fsync` for the whole batch. The WAL is **local disk only** (it is not routed
  through the storage backend), which makes a *writer* node stateful: it needs a
  durable local volume at `delta.walDir`. Read replicas stay stateless.
* **Memtable → L0 → consolidation.** Writes accumulate in an in-RAM memtable
  (bounded by `delta.memtableBytes`); when it fills it flushes to an immutable L0
  delta segment. A **consolidation** folds `{core + delta}` into a fresh core by
  serialising the merged view back through `slater-build` and swapping `current`
  atomically — the same content-hash guard as any published generation. Trigger it
  by hand with `CALL slater.consolidate()`, automatically at `delta.deltaCorePercent`
  of the core's size (optionally gated to an off-peak `delta.consolidateWindow`), or
  let the `delta.deltaHardBytes` throttle backstop runaway growth.
* **The overlay sits below the read surface.** The executor reads through a
  `ReadView` that is either the bare core (delta always empty) or a merged
  `(core, delta)` view; the engine is monomorphised over it, so an empty delta
  compiles to a single predictable branch and the read-only path is byte-identical.
  Whole-graph counters (`count(*)`, label/reltype marginals) are served from the
  delta's own live counters, so they stay metadata reads even with writes pending.
* **A query sees a stable snapshot.** It pins one `(core, delta)` tuple for its
  whole life. There are no multi-statement transactions and no rollback — a write is
  a durable, business-key-addressed correction, not an OLTP transaction.

The exact write grammar and knobs are in the [Configuration](#environment--configuration)
table (`delta.*`) and the [Worked example](#worked-example) below.

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

## Storage backends (filesystem / S3 / GCS)

Every generation file is opened through an **`ObjectStore`** abstraction rather
than `std::fs` directly, so the *same* on-disk byte format — blocks, indexes,
manifest, `current` pointer — is served unchanged from any backend; only *where
the bytes come from* differs, never the readers, the query engine, or the
integrity checks. The hot path is positional reads (`read_exact_at`), which map
onto a `pread` on a local file and an HTTP byte-range request on an object store
— Slater never mmaps, so the explicit, bounded-read model is identical
everywhere.

**Three first-class backends**, selected by `dataBackend.kind`. The filesystem is
the simple default; **Amazon S3 and Google Cloud Storage are equal, fully
supported object-store backends** — the published image ships with both compiled
in, so each is configuration-only, and a generation built once can be served from
any of them (even migrated `fs` → S3 → GCS) without a rebuild.

| `dataBackend.kind` | Positional read | Integrity at open (no body download) | Credentials |
| --- | --- | --- | --- |
| `fs` *(default)* | `pread` | full BLAKE3 re-hash of each file | — |
| `s3` | HTTP `Range` GET | server **SHA-256** via `HEAD` (→ byte-size check if absent) | config keys, AWS chain, or IAM role |
| `gcs` | HTTP range read | server **CRC32C** via `get_object` (→ byte-size check if absent) | ADC / Workload Identity, or service-account JSON |

Both object stores verify integrity from the **checksum the store already
computes and keeps**, fetched as object metadata: `slater-build` sends the
checksum on upload (the store validates the bytes against it and stores it), and
the server reads it back at open and compares it to the manifest — one metadata
request per file, no body download. It is content-grade and identical in spirit
across S3 (SHA-256) and GCS (CRC32C).

### Filesystem (`fs`)

The default, rooted at `dataBackend.fs.dir`. The right choice for most
deployments: a generation on a local SSD (or an NFS/EBS mount) served read-only.
Integrity is a full BLAKE3 re-hash of every file at open.

### Amazon S3 (`s3`)

An S3 or S3-compatible bucket (AWS, MinIO, localstack). Credentials come **first**
from config (`dataBackend.s3.awsAccessKey` / `awsSecretKey`, plus
`awsSessionToken` for temporary STS credentials) and fall back to the standard AWS
chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env, shared profile, or
instance/IRSA role) when left empty.

```sh
# serve from S3 (env-var form; see the config table for every key)
dataBackend__kind=s3
dataBackend__s3__bucket=slater
dataBackend__s3__region=eu-west-2
dataBackend__s3__awsAccessKey=…        # omit to use the AWS chain / instance role
dataBackend__s3__awsSecretKey=…
# S3-compatible (e.g. MinIO): also set
dataBackend__s3__endpoint=http://minio:9000
dataBackend__s3__pathStyle=true        # required by most S3-compatible servers
```
```sh
# publish a generation into the bucket (remote `current` pointer written last)
slater-build --input people.cypher --graph people --data-dir /data \
  --publish-s3-bucket slater --publish-s3-region eu-west-2 --publish-s3-prefix prod
#   MinIO: add  --publish-s3-endpoint http://localhost:9000 --publish-s3-path-style
```

### Google Cloud Storage (`gcs`)

A GCS bucket, reached over the JSON API. Authorization is GCP-native: by default
it resolves **Application Default Credentials** — GKE Workload Identity, the GCE
metadata server, or a `gcloud` / `GOOGLE_APPLICATION_CREDENTIALS` key. Set
`dataBackend.gcs.credentialsPath` (a service-account JSON key file) or inline
`credentialsJson` for an explicit key. `dataBackend.gcs.endpoint` points at a
`fake-gcs-server` emulator, and `dataBackend.gcs.anonymous=true` enables
unauthenticated access **for that emulator only** — never against real GCS.

```sh
# serve from GCS (env-var form; see the config table for every key)
dataBackend__kind=gcs
dataBackend__gcs__bucket=slater
dataBackend__gcs__prefix=prod
dataBackend__gcs__credentialsPath=/secrets/sa.json   # omit for ADC / Workload Identity
```
```sh
# publish a generation into the bucket (remote `current` pointer written last)
slater-build --input people.cypher --graph people --data-dir /data \
  --publish-gcs-bucket slater --publish-gcs-prefix prod
#   explicit key: add  --publish-gcs-credentials /secrets/sa.json
```

In all cases `slater-build` writes the finished generation to `--data-dir` first
(its local staging area) and **additionally** uploads it to the bucket; the remote
`current` pointer is written last, so a serving node never sees a half-published
generation.

### When to use an object store (S3 or GCS)

Reach for `s3` or `gcs` when you want generations in durable, central object
storage rather than on a node's disk — typically: publish once and fan out to many
stateless, disk-less server replicas that all read the same bucket; decouple the
build host from the serve hosts; or lean on the store's
durability/versioning/lifecycle instead of managing volumes. The trade-off is
latency: a cold block is a network round-trip (~10–50 ms) instead of a local read
(~0.1 ms). Slater hides most of it with the in-memory block cache, concurrent
read-ahead, **and** the optional disk cache below. If your generations already sit
on fast local storage and you don't need the central-bucket model, `fs` is simpler
and faster.

### Local-disk block cache (object-store second tier)

The in-memory `BlockCache` is deliberately small (bounded RSS is the headline
guarantee), so on a working set larger than RAM the same blocks would be
re-fetched from the object store on every spill. An **optional local-SSD second
cache tier** fixes that: a block evicted from RAM is served from local disk
(~0.1 ms) instead of a fresh object GET, surviving in-memory eviction and cutting
object-store request count/cost — bringing an object-store-backed node close to
local-filesystem performance once warm. It is **opt-in** for both `s3` and `gcs`,
enabled by setting `dataBackend.<s3|gcs>.diskCacheBytes > 0` and a writable
`diskCacheDir`.

* It caches the **sealed** bytes exactly as fetched — already compressed, and
  (for `--encrypt` generations) still AEAD-sealed — *below* decrypt/decompress.
  The cache layer never holds the encryption key and never re-encrypts, so
  at-rest status is preserved for free: an encrypted generation lands on disk
  still sealed.
* Writes are **write-behind**: a miss returns the fetched bytes to the query
  immediately, then a background thread does the disk write and LRU trim, so the
  query path never blocks on disk I/O. Eviction keeps the cache within its byte
  budget; a per-file checksum verified on every read self-heals a corrupt cache
  file to a miss (→ refetch from the object store).
* `diskCacheDir` **must point at a real writable volume — never `tmpfs`** (tmpfs
  is RAM and would defeat the bounded-RSS guarantee). The in-memory index that
  tracks it costs a little RAM (~tens of bytes per cached block), which counts
  against your RSS ceiling — size the directory ≫ the in-memory block cache.

## Mounts

A **read replica** runs with a **read-only root filesystem** and a non-root user
(`appuser:1000`) — everything it needs is mounted read-only. A **writer**
(`delta.enabled`) additionally needs one durable, writable volume for its WAL.

| Path | Purpose | Notes |
| --- | --- | --- |
| `/data` | The graph generations (`<graph>/<uuid>/…` + `current`). | **Read-only** for replicas; produced by `slater-build`. May live on remote/network storage (e.g. NFS), so reads are not assumed to be fast local-SSD latencies. |
| `/sandbox` | Per-environment config overlay + secrets. | `/sandbox/config.json` is deep-merged over the baked-in `config.json`; also holds `acl.json`, TLS PEM material, the at-rest key file. |
| `/tmp`, `/run` | Scratch (`tmpfs`). | A read replica never writes to disk by default. |
| _(writer)_ `delta.walDir` | The write-ahead log + L0 delta segments, when `delta.enabled`. | **Writable**, and a **durable, real volume — never `tmpfs`** (it is the durability floor). A relative path resolves under the data dir; give a writer its own persistent volume here. |
| _(optional)_ disk cache | The local-disk block cache, when `dataBackend.s3.diskCacheBytes` / `dataBackend.gcs.diskCacheBytes > 0`. | **Writable**, and a **real volume — not `tmpfs`**. Used by the `s3` and `gcs` backends; see [Storage backends](#storage-backends-filesystem--s3--gcs). |

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
| `server.loginTimeoutMs` | `server__loginTimeoutMs` | 10000 | Deadline for an unauthenticated peer to finish TLS handshake → Bolt handshake → `LOGON` (0 ⇒ none); armed at `accept()`, so it bounds the whole pre-auth window as one budget and closes the slow-loris a byte cap alone leaves open. |
| `server.idleTimeoutMs` | `server__idleTimeoutMs` | 0 | Idle read timeout for an **authenticated** connection (0 ⇒ none, the default — pooled drivers legitimately hold idle connections). |
| `server.tlsHandshakeTimeoutMs` | `server__tlsHandshakeTimeoutMs` | 5000 | Deadline for the **TLS handshake** alone, on top of `loginTimeoutMs` — whichever lands first wins (0 ⇒ none, not recommended). A handshake is a 2-RTT machine exchange, so it warrants a tighter bound than a login window; and unlike `loginTimeoutMs` it must never lapse, because a peer stalled mid-ClientHello holds a connection slot while sitting behind every guard that lives after the handshake. |
| `server.maxConcurrentAuth` | `server__maxConcurrentAuth` | 4 | Cap on argon2id password verifies running **at once** (0 ⇒ unlimited, not recommended). Each verify costs ~19 MiB and tens of ms; it runs on a blocking thread, never on the reactor, and this bounds how much of the blocking pool (shared with query execution) a `LOGON` flood can take. Even 4 sustains ~100 logins/s. |
| `server.maxAuthFailures` | `server__maxAuthFailures` | 3 | Failed `LOGON`s one connection may make before the server closes it (0 ⇒ unlimited). Per *connection*, never per account, so it cannot be used to lock a user out. |
| `dataBackend.kind` | `dataBackend__kind` | `fs` | Storage backend: `fs` (local filesystem), `s3` (object store), or `gcs` (Google Cloud Storage). See [Storage backends](#storage-backends-filesystem--s3--gcs). |
| `dataBackend.fs.dir` | `dataBackend__fs__dir` | `/data` | Root holding `<graph>/<generation>/` for the `fs` backend (and the local area the at-rest key file must stay outside of). |
| `dataBackend.verifyIntegrity` | `dataBackend__verifyIntegrity` | `true` | Verify each generation file against the manifest at open (a cheap metadata check on every backend). |
| `dataBackend.s3.bucket` | `dataBackend__s3__bucket` | _(empty)_ | S3 bucket name (required when `kind=s3`). |
| `dataBackend.s3.region` | `dataBackend__s3__region` | _(empty)_ | AWS region (e.g. `eu-west-2`); empty ⇒ resolved from the environment. |
| `dataBackend.s3.endpoint` | `dataBackend__s3__endpoint` | _(empty)_ | Custom endpoint URL for an S3-compatible store (MinIO, localstack); empty ⇒ standard AWS endpoint. |
| `dataBackend.s3.prefix` | `dataBackend__s3__prefix` | _(empty)_ | Key prefix every generation key is joined under; empty ⇒ bucket root. |
| `dataBackend.s3.pathStyle` | `dataBackend__s3__pathStyle` | `false` | Path-style addressing (`endpoint/bucket/key`); required by most S3-compatible servers. |
| `dataBackend.s3.awsAccessKey` | `dataBackend__s3__awsAccessKey` | _(empty)_ | **Preferred** way to supply the S3 access key id. Empty ⇒ fall back to the standard AWS credential chain (`AWS_ACCESS_KEY_ID` env, shared profile, or instance role). |
| `dataBackend.s3.awsSecretKey` | `dataBackend__s3__awsSecretKey` | _(empty)_ | **Preferred** way to supply the S3 secret access key, paired with `awsAccessKey`. Empty ⇒ AWS chain. |
| `dataBackend.s3.awsSessionToken` | `dataBackend__s3__awsSessionToken` | _(empty)_ | Optional session token for temporary (STS) credentials; only used when `awsAccessKey`/`awsSecretKey` are set. |
| `dataBackend.s3.diskCacheBytes` | `dataBackend__s3__diskCacheBytes` | `0` | Byte budget for the **local-disk block cache** (second tier). `0` ⇒ disabled. When `> 0`, `diskCacheDir` is required. Size it ≫ `blockCacheBytes`; the in-memory index counts against the RSS ceiling. |
| `dataBackend.s3.diskCacheDir` | `dataBackend__s3__diskCacheDir` | _(empty)_ | Directory for the disk cache (used iff `diskCacheBytes > 0`). Must be a **real writable volume — never `tmpfs`**. |
| `dataBackend.gcs.bucket` | `dataBackend__gcs__bucket` | _(empty)_ | GCS bucket name (required when `kind=gcs`). |
| `dataBackend.gcs.prefix` | `dataBackend__gcs__prefix` | _(empty)_ | Key prefix every generation key is joined under; empty ⇒ bucket root. |
| `dataBackend.gcs.endpoint` | `dataBackend__gcs__endpoint` | _(empty)_ | Custom endpoint URL for a GCS emulator (`fake-gcs-server`); empty ⇒ standard GCS endpoint. |
| `dataBackend.gcs.credentialsPath` | `dataBackend__gcs__credentialsPath` | _(empty)_ | Path to a **service-account JSON key file**. Empty ⇒ Application Default Credentials (Workload Identity / GCE metadata / `gcloud`). |
| `dataBackend.gcs.credentialsJson` | `dataBackend__gcs__credentialsJson` | _(empty)_ | Inline service-account JSON key; takes precedence over `credentialsPath`. Empty ⇒ `credentialsPath`, else ADC. |
| `dataBackend.gcs.anonymous` | `dataBackend__gcs__anonymous` | `false` | Use unauthenticated access — a local GCS emulator (`fake-gcs-server`) **only**, never against real GCS. Overrides every other credential source. |
| `dataBackend.gcs.diskCacheBytes` | `dataBackend__gcs__diskCacheBytes` | `0` | Byte budget for the **local-disk block cache** (second tier), identical to the S3 setting. `0` ⇒ disabled; when `> 0`, `diskCacheDir` is required. |
| `dataBackend.gcs.diskCacheDir` | `dataBackend__gcs__diskCacheDir` | _(empty)_ | Directory for the GCS disk cache (used iff `diskCacheBytes > 0`). Must be a **real writable volume — never `tmpfs`**. |
| `aclPath` | `aclPath` | `/config/acl.json` | JSON ACL (users → per-graph read grants). |
| `requireAclStamp` | `requireAclStamp` | `true` | Refuse a generation with no `aclBlake3` stamp (closes the stamp-strip downgrade); build images with `--acl`. A generation with no manifest MAC is always refused when a master key is configured — that check has no off switch. |
| `cache.blockCacheBytes` | `cache__blockCacheBytes` | 64 MiB | Decompressed block LRU budget. |
| `cache.vectorCacheBytes` | `cache__vectorCacheBytes` | 64 MiB | Vector pool budget: resident brute-force kNN matrix (pre-normalised, no-gather scan) + resident PQ + Vamana-block LRU. kNN falls back to the block-cache gather path for any group that does not fit. |
| `cache.resultCacheBytes` | `cache__resultCacheBytes` | 16 MiB | Result LRU budget. |
| `cache.cacheTtlMs` | `cache__cacheTtlMs` | 1800000 (30 min) | Idle TTL: a cached entry untouched this long is reclaimed by a background sweep, freeing memory below the budgets when the working set goes quiet. `0` or negative disables the sweep. |
| `cache.degreeColumn` | `cache__degreeColumn` | `lazy` | Residency of the dense per-node degree column (backs the degree-sum `count(endpoint)` fast path): `lazy` faults per-id chunks on touch and frees them on the idle sweep; `pinned` holds the whole column resident. |
| `tls.cert` / `tls.key` | `tls__cert` / `tls__key` | _(empty)_ | PEM material; both set ⇒ `bolt+s`. Empty ⇒ plaintext (loopback dev). |
| `encryption.keyFile` | `encryption__keyFile` | _(empty)_ | File holding the hex at-rest master key. Must live **outside** `dataBackend.fs.dir` and any attacker-writable path (server refuses to start if it resolves inside it); see `THREAT_MODEL.md` "Trust boundary". |
| `encryption.keyEnv` | `encryption__keyEnv` | _(empty)_ | Env var holding the hex at-rest master key. |
| `query.maxRows` | `query__maxRows` | 100000 | Per-query row cap. |
| `query.timeoutMs` | `query__timeoutMs` | 30000 | Per-query wall-clock deadline (0 ⇒ none). |
| `query.maxIntermediate` | `query__maxIntermediate` | 1000000 | Per-query intermediate-element budget (0 ⇒ none); ~48 B/element, so the default bounds one query at ≈48 MB. |
| `query.maxIntermediateGlobal` | `query__maxIntermediateGlobal` | 8000000 | Server-wide ceiling on the sum of all in-flight queries' intermediate elements (0 ⇒ none). Bounds the aggregate so `N` concurrent heavy queries can't multiply the per-query budget into an OOM; ~48 B/element ⇒ ≈384 MB. |
| `vectorQuery.beamWidth` | `vectorQuery__beamWidth` | 64 | Vamana beam-search list size. |
| `generationPollMs` | `generationPollMs` | 5000 | How often to poll each graph's `current`. |
| `reloadStrategy` | `reloadStrategy` | `exit` | `exit` or `swap` on a generation change. |
| `delta.enabled` | `delta__enabled` | `false` | Master switch for the writable layer. Off ⇒ every query serves the pure immutable core (no WAL opened) and write statements are refused. |
| `delta.walDir` | `delta__walDir` | `wal` | Directory holding per-graph WAL segments (the durability floor). A relative path resolves under the data dir; one graph's segments live under `<walDir>/<graph>/`. Must be a **durable local volume — never `tmpfs`** or ephemeral instance storage. |
| `delta.memtableBytes` | `delta__memtableBytes` | 64 MiB | Byte budget for a graph's in-RAM active memtable before it flushes to an immutable L0 delta segment (bounds resident memtable RAM). |
| `delta.deltaCorePercent` | `delta__deltaCorePercent` | `0` (off) | Auto-consolidation threshold as a **percent of the core's entity count**: once the delta's changed-entity count reaches this fraction, a background consolidation folds it into a fresh core. A rebuild is O(core), so keep it rare (typical opt-in 5–25); `0` ⇒ only manual / scheduled consolidation. |
| `delta.consolidateWindow` | `delta__consolidateWindow` | _(empty)_ | Off-peak cron window (server-local, hour granularity, `min hour dom mon dow`) gating the `deltaCorePercent` auto-consolidation. Empty ⇒ fire whenever due. Example: `0 1-5 * * *` = 01:00–05:59 daily. |
| `delta.deltaHardBytes` | `delta__deltaHardBytes` | `0` (off) | Hard cap on total resident delta bytes: a write past it **throttles** (waits for a draining consolidation) — the OOM backstop. Set well above the `deltaCorePercent` working set. |
| `cacheWarmingQuery` | `cacheWarmingQuery` | _(empty)_ | Cypher query run once at boot against every served graph, results discarded — faults the blocks needed to answer it into the block/vector cache so the first matching client query is served warm. Empty ⇒ disabled. A parse error is logged and warming is skipped; a per-graph execution error is logged and that graph skipped (the query need not be valid against every graph). Bounded by the same `query.*` limits and `query.timeoutMs` as a real query. |

The writable layer carries a handful of further advanced knobs for tuning
compaction (`delta.l0CompactionTrigger`, `delta.segmentFlushBytes`,
`delta.maxUpperSegments`, `delta.offHeapL0`, `delta.segmentGcGraceSecs`); their
defaults are sensible and they are documented in `docs/WRITABLE-PLAN.md`.

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

`acl.json` maps users to argon2id password hashes and per-graph **`read`** / **`write`**
grants. Mint a hash (never store cleartext) with:

```sh
slater hash-password 's3cret'        # prints a $argon2id$… string for acl.json
```

A starter `acl.json` ships at the repo root; its shape is:

```json
{
  "users": {
    "reporting": {
      "passwordArgon2id": "$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>",
      "grants": {
        "people": ["read"],
        "products": ["read", "write"]
      }
    }
  }
}
```

* **`users`** — one entry per login, keyed by username.
* **`passwordArgon2id`** — the `$argon2id$…` string from `slater hash-password`
  (never cleartext; the file itself is plain JSON and lives on shared storage).
* **`grants`** — per-graph capability lists. Two permissions are meaningful:
  * **`read`** — query the graph. A graph absent from a user's grants is invisible to them.
  * **`write`** — mutate the graph through the writable layer (`delta.enabled`): the
    `MERGE` / `SET` / `DELETE` statements and `CALL slater.consolidate()`.

  They are **independent: a `read` grant confers no write access.** Turning the writable
  layer on therefore cannot promote your existing readers into writers. A writer needs
  both — `["read", "write"]` — because resolving a business key to write it is a read.
  Unrecognised permission strings are ignored (they grant nothing).

Mount it read-only at the path named by `aclPath` (default `/config/acl.json`).
The server reloads it on each generation hot-swap, and the at-rest ACL stamp is
re-checked on every reload (see [`requireAclStamp`](#environment--configuration)).

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

## One-shot query

For scripting, CI checks, and quick lookups, `slater query` mounts a graph's
current generation, runs a single read-only Cypher query in-process, prints the
result as a JSON object, and exits — no server, no Bolt connection. It honours
the same config as the server (storage backend, encryption key, query budgets):

```sh
# GRAPH defaults to `defaultGraph`. Without -q, normal datestamped logging
# (config, "opened generation", …) is written to stdout alongside the result.
slater query mygraph 'MATCH (n) RETURN count(n) AS c'

# -q/--quiet ⇒ logging suppressed, so stdout is *only* the compact result JSON
slater query mygraph -q 'MATCH (c:Company) RETURN c.ticker AS t LIMIT 3' | jq
# {"columns":["t"],"rows":[["AUPH"],["KYMR"],["MREO"]]}
```

Nodes and relationships expand to their labels/type and properties. Use `-q`
when you want machine-parseable output (the result JSON is the only thing on
stdout); omit it for an operator-facing run with logs. Without `-q` a
metrics-only summary is logged after each run — e.g.

```text
INFO query executed cost=2389 resultCount=10 execMs=441 limitRowCount=10
```

carrying the query `cost` (elements charged), `resultCount`, `execMs`, and
`limitRowCount` (only when the query specifies a `LIMIT`) — never the query text
or any result value. Exit status is `0` on success, `1` on a parse/open/execute
error (message on stderr).

## Export a graph (`slater dump`)

`slater dump` exports a graph from a **running** server as business-key `MERGE`
Cypher — the same dialect `slater-build` ingests — so a graph round-trips
(dump → `slater-build` → new generation) for migration or text backup. Unlike
`slater query`, it connects over **Bolt**, authenticates, and honours per-graph
ACLs, so it needs no disk access to the server. The password is read from
`SLATER_DUMP_PASSWORD` or stdin (never a flag, keeping it out of `ps`/history).

```sh
# List the graphs the authenticated user may read.
SLATER_DUMP_PASSWORD=pw slater dump --list -u reporting

# Dump a graph to a file (identity keys inferred from range indexes).
SLATER_DUMP_PASSWORD=pw slater dump people -u reporting -o people.cypher

# Rebuild it into a fresh generation.
slater-build --input people.cypher --graph people --data-dir ./data
```

Each label's identity key is the property carried by its range index; override
with `--key Label=prop` (repeatable) or a global `--pk <field>`. `CREATE INDEX`
DDL is emitted first so the rebuild recreates the indexes. A multi-label node
keeps **every** label — it is emitted as `MERGE (n:Ident:Other {key: v})`, with
the identity label (the one supplying the business key) first and the rest
sorted; the merge is keyed on the identity label alone, so the trailing labels
are written onto the node without creating another one. Vectors (and other
values with no Cypher-literal spelling) cannot ride a `MERGE` dump and are dropped
with a warning on stderr. Exit status is `0` on success, `1` on error.

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
slater    # looks for configuration options in ./config.json (default fs dir ./data, port 7687)
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

### 4. Write to the graph (opt-in)

Writes are off until you enable the delta layer and grant `write`. Give `myuser`
both grants in `acl.json` — a writer needs `read` too, because resolving a business
key to write it is itself a read:

```json
"grants": { "people": ["read", "write"] }
```

Rebuild the generation so the manifest stamps the updated ACL
(`slater-build … --acl ./acl.json`), then start the server with the writable layer
on and a durable WAL directory:

```sh
delta__enabled=true delta__walDir=./wal slater
```

Now correct, insert and retract over the *same* Bolt session — no rebuild:

```js
// Upsert a node by its business key, then set a property.
await session.run(
  "MERGE (p:Person {name: 'Dave'}) SET p.age = 33");

// Batch many rows into one group-committed, fsync-durable write.
await session.run(
  `UNWIND $rows AS r MERGE (p:Person {name: r.name}) SET p.age = r.age`,
  { rows: [ { name: 'Erin', age: 28 }, { name: 'Frank', age: 52 } ] });

// Read it straight back — the delta overlays the immutable core.
const r = await session.run(
  "MATCH (p:Person {name: 'Dave'}) RETURN p.age AS age");
console.log(r.records[0].get('age'));   // 33

// Fold the accumulated delta back into a fresh immutable core (optional;
// also runs automatically per delta.deltaCorePercent / delta.consolidateWindow).
await session.run('CALL slater.consolidate()');
```

`SUCCESS` on a write returns only after the `fsync` that makes it durable. The
write grammar is business-key `MERGE` / `MATCH … SET` / `DELETE` (plus `CREATE`,
`REMOVE`, detach delete and relationship writes) — enough to correct, insert,
upsert and retract, addressed by the identity property your data already carries.

## Development

```sh
export PATH="$HOME/.cargo/bin:$PATH"
cargo build
cargo test            # unit + the bounded-RSS headline integration test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

### Object-store backends are opt-in cargo features

A plain `cargo build` produces a **filesystem-only** binary — the `s3` and `gcs`
backends are gated behind cargo features so the default build stays small (no AWS
or Google SDK, no async runtime). Enable whichever you need on **both** `slater`
(serve) and `slater-build` (publish):

```sh
# S3 only / GCS only / both
cargo build -p slater -p slater-build --features s3
cargo build -p slater -p slater-build --features gcs
cargo build -p slater -p slater-build --features s3,gcs
```

Each crate exposes matching `s3` / `gcs` features that forward to
`graph-format/{s3,gcs}`. Requesting a backend at runtime
(`dataBackend.kind=s3|gcs`, or `slater-build --publish-{s3,gcs}-*`) without its
feature compiled in fails fast with a clear "built without the … feature" error.
The **published Docker image enables both** (Dockerfile `CARGO_FEATURES`), so
prebuilt images need no extra flags — this only matters when building from source.
The integration tests are likewise gated: `--features s3 --test s3_minio`,
`--features gcs --test gcs_emulator` (a `fake-gcs-server`), and `--features gcs
--test gcs_real` (real GCS via ADC); each skips unless its `SLATER_*` env vars are
set.

See `docs/PLAN.md`, `docs/PROGRESS.md` and `docs/DECISIONS.md` for the design,
the milestone ledger, and the decision log.

## Performance

Up to six engines, one single-client suite, graphs from a 62k-node toy to **Wikidata
91.6M nodes / 1.5B edges**. Each engine is **measured in isolation** (every other container
stopped — RSS and latency are its own footprint). The **latency tables** below were
**re-measured on Slater 0.21.0** (the writeable build): the small/medium graphs (MeSH, EU-AI-Act)
freshly, and the 91.6M graph as a fresh **same-box, shared-anchor** slater-vs-Neo4j pass (see
that table). The **resident-memory** figures carry forward from the earlier pass (measured via
container cgroup; the read path is byte-identical with the writable layer idle). The other
engines' numbers are the established cross-engine run (their versions/performance are unchanged).
All figures are medians (ms) or peak resident memory (MiB). **Lower is better everywhere; bold =
best in row.** slater was run on its **local-filesystem (`fs`) backend**; the S3 and GCS backends trade local-read
latency for object-store round-trips (mitigated by the in-memory caches and the optional local-disk
cache tier), so these figures characterise the engine, not a network-storage deployment.

| engine | class | memory bound |
|---|---|---|
| **slater** | disk-backed, paged | `query.maxIntermediate` caps the working set automatically |
| Neo4j 5 | disk-backed, JVM | ~2 GiB heap + off-heap, committed regardless of query |
| Memgraph · FalkorDB | in-memory | whole graph resident in RAM |
| ArcadeDB | in-memory, JVM | whole graph resident; heaviest |
| LadybugDB | embedded, columnar | manual buffer pool that must exceed the query |

The three engines that **page from disk** — slater, Neo4j 5, and LadybugDB — load all five
graphs. The **in-memory trio** (Memgraph · FalkorDB · ArcadeDB) cannot hold the 1.5B edge graph at
all (it needs ~64–128 GiB resident), and ArcadeDB's importer can't finish it either.

### Resident memory (MiB) — bounded as the graph grows ~1,500×

Each figure is **committed working memory** — what the OS cannot reclaim. Every engine *except*
slater holds its graph in committed anonymous memory (own heap, Neo4j's off-heap page cache, or
a buffer pool), so its peak RSS *is* its committed footprint. slater alone serves from the
**reclaimable OS page cache** of its on-disk store, so its figure is the anon working set; the
store's page cache (evictable under pressure — slater keeps serving) is excluded, and shown as
*total* in parentheses for the 91.6M graph. **Bold = lowest.**

| graph (nodes / edges) | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| pole — 62k / 106k | **11** | 746 | 114 | 140 | 1,556 | 198 |
| MeSH — 341k / 469k | **63** | 1,083 | 358 | 455 | 1,631 | 121 |
| EU-AI-Act — 21k / 45k (+55 MiB vec) | **99** | 729 | 229 | 312 | 1,948 | 286 |
| Wikidata — 91.6M / 1.5B | **584** *(4,595 total)* | ~2,900 | cannot-load | cannot-load | cannot-load | ~652 † |

slater is the **lowest at every scale** and grows ~50× while the graph grows ~1,500× — its
footprint tracks the *query working set*, not the graph (idle ~16–71 MiB throughout). The
in-memory trio grows ~linearly and can't load the 1.5B graph; Neo4j commits a ~2 GiB heap
regardless of query. († LadybugDB on the bounded shapes only — its hub / var-length /
shortestPath traversals at 1.5B edges need its read pool raised to ≥2 GiB, vs slater's automatic
`maxIntermediate` cap.) The build-time value→count histograms add negligible resident memory —
a few KB for a low-cardinality indexed column, and *zero* for unique-key graphs like Wikidata
(`wikidata_id` exceeds the histogram cardinality cap, so none is stored) — so these figures are
unchanged by that feature.

### Latency (median ms) — graph fits in RAM (MeSH, 341k / 469k)

| shape | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| count(*) all nodes | **0.41** | 15.0 | 23.8 | 16.4 | 82.0 | 2.2 |
| label count | **0.42** | 4.2 | 20.7 | 1.1 | 4.4 | 4.3 |
| indexed point lookup | **0.43** | 3.9 | 0.48 | 0.48 | 0.65 | 8.8 |
| idx-eq count | **0.42** | 4.9 | 5.0 | 2.0 | 381 | 2.5 |
| 1-hop (indexed anchor) | 1.28 | 5.8 | **1.21** | 4.1 | 390 | 4.9 |
| 2-hop (unanchored) | **1.40** | 5.6 | 8.5 | 16.7 | 444 | 6.4 |
| group-by / count(DISTINCT) | **0.45** | 47–51 | 63–64 | 31–39 | 411 | 5.3 |
| full-scan `CONTAINS` | **0.43** | 5.4 | 24.1 | 1.7 | 16.3 | 4.1 |

slater owns the **metadata / index / scan** shapes (count, label, idx-eq, scan — ~0.4 ms, 10–200× the
service engines), the **indexed point lookup** (0.43 ms, now edging the in-memory pair's 0.48 ms),
the **unanchored multi-hop** (2-hop 1.40 ms via the relationship-type scan, fastest in the field),
and — via a build-time value→count histogram on the indexed grouping key — the **whole-label
group-by / count(DISTINCT)** (0.45 ms, ahead of LadybugDB's columnar 5.3 ms). The in-memory servers
keep only **raw 1-hop** (Memgraph 1.21 ms vs slater's 1.28 ms). (pole 62k/106k looks the same:
slater sole-fastest on count/scan ~0.4 ms, ~1.3–2.6 ms on hops.)

### Latency (median ms) — vectors (EU-AI-Act kNN, 15k × 1024-dim)

| shape | slater | Neo4j 5 | Memgraph | FalkorDB | LadybugDB |
|---|--:|--:|--:|--:|--:|
| kNN top-10 Concept | 2.9 | 8.6 | 1.9 | **1.2** | 2.8 |
| kNN top-10 Chunk | 2.4 | 5.7 | 1.9 | **1.5** | 3.2 |

slater answers kNN with an **exact brute-force** scan (these sets are below its 50k-vector
ANN threshold) where the others use an approximate resident HNSW — so slater's results are
exact (recall 1.0). A SIMD distance kernel + a resident, pre-normalised vector matrix
took Concept from ~23 → ~2.9 ms and Chunk from ~10 → ~2.4 ms, so slater now beats
Neo4j and LadybugDB and is within ~1.4× of Memgraph, trailing only FalkorDB — while exact.

### Latency (median ms) — graph ≫ RAM (Wikidata 91.6M / 1.5B)

The in-memory engines (Memgraph / FalkorDB / ArcadeDB) **cannot load** this graph at all
(~64–128 GiB resident). Only slater and Neo4j 5 do. This is a **fresh same-box, same-day pass
against a shared, fixed anchor set** — every query hits the *identical* nodes on both engines,
so the head-to-head is apples-to-apples (a common `wikidata_id` pool of moderate-degree anchors;
see the note below on why that matters). slater is shown at both fanouts (`query.maxFanout` 1 =
throughput default, 8 = the latency dial that overlaps cold block reads). **Bold = best in row.**

| shape | slater (fan 1) | slater (fan 8) | Neo4j 5 |
|---|--:|--:|--:|
| count(*) all nodes | **0.41** | 0.41 | 3606 |
| point lookup (indexed) | 0.72 | **0.49** | 6.3 |
| degree (1-hop count) | **0.43** | 0.44 | 6.0 |
| 1-hop neighbours | 9.8 | **4.5** | 10.1 |
| 2-hop | 37 | **23** | 34.5 |
| 3-hop | 32 | **25** | 74 |
| var-length `*1..2` distinct | 985 | 1056 | **47** |

The honest picture: slater **dominates the metadata / index shapes** — `count(*)` is
metadata-served (**0.41 ms vs Neo4j's 3.6 s** disk scan, ~8800×), and point-lookup / degree /
3-hop run ~2–10× faster — is **even with Neo4j on 1–2-hop** (fanout 8 pulls ahead on cold reads),
but **loses `var-length *1..2 distinct` decisively (≈1 s vs Neo4j's 47 ms)**: slater's
variable-length distinct expansion is materially slower here, a real weakness worth its own
investigation. All of that at a few hundred MB of RSS versus Neo4j's committed ~2 GiB heap.

> **On the anchors.** These traversal numbers depend heavily on *which* nodes you start from —
> a node one link from a Wikidata mega-hub ("human", "country") has a millions-strong 2-hop
> neighbourhood, so var-length/hop cost swings by orders of magnitude with anchor choice. The
> earlier edition of this table sampled each engine's *own* "first N by scan," which is neither
> stable nor comparable; this pass fixes a single shared, degree-bounded anchor set for both
> engines. (shortestPath is omitted from this pass — between two arbitrary anchors it is
> path-existence-dependent and too high-variance to median meaningfully.)

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
it helps large-cold-working-set disk-bound shapes and is flat on warm shapes. On the 1.5B
graph: shortestPath ≤6 **918 → 608 ms** (1.5×, largest search 6,269 → 2,350 ms, 2.7×);
3-hop count **547 → 298 ms**. `maxFanout=1` is the default (throughput-oriented); `8` is the
latency dial, at more transient worker memory.

### Where slater wins / trails

| dimension | slater | best of the field | verdict |
|---|---|---|---|
| resident memory, any scale | 11–584 MiB (62k → 91.6M) | in-memory 1.5–2.7 GiB; can't load 1.5B | **slater** |
| count / metadata / scan | ~0.4 ms | service engines 5–80 ms | **slater** (10–200×) |
| indexed point lookup | **0.43 ms** (MeSH) | Memgraph · FalkorDB 0.48 ms | **slater** (edges the in-memory pair) |
| unanchored multi-hop (rows) | **1.40 ms** (MeSH 2-hop) | Neo4j 5.6 ms | **slater** (relationship-type scan) |
| aggregation (group-by / DISTINCT) | **0.45 ms** | LadybugDB 5 ms (columnar) | **slater** (build-time histogram) |
| kNN | 2.4–2.9 ms (exact) | FalkorDB **1.2 ms** (HNSW) | beats Neo4j/Ladybug; ~1.4× off Memgraph; exact |
| 91.6M metadata / point / degree / 3-hop | 0.4–32 ms | Neo4j 6–3,600 ms | **slater** (2–8800×) |
| 91.6M 1–2-hop | 4.5–23 ms (fan 8) | Neo4j 10–35 ms | ~even |
| 91.6M var-length `*1..2` distinct | ~1 s | Neo4j **47 ms** | **Neo4j** (a real slater weak spot) |
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
| Holds to **1000 concurrent clients, zero failures** | throughput peaks ~2.5k rps; the latency knee sets in around 750 clients (p99 51 → 750 ms) — queueing under core contention, not a hard cap (single-run, WSL2) |
| Block cache **bounded and effective** | 100% hit rate, 0 evictions, 50 MB resident for a cache-fitting working set |
| RSS held under sustained load | the jemalloc allocator holds RSS to ~0.6 GB across a 100→500-client `wiki_cache_churn` ramp — cache-bound and stable, with **no** `MALLOC_*` tuning (the former `MALLOC_ARENA_MAX=2` + trim threshold is retired); its background purge also returns the post-burst high-water instead of leaving it pinned |
| Aggregate memory bounded | server-wide **`query.maxIntermediateGlobal`** + adjacency-charged expansion hold the `wiki_budget` 2-hop flood at 1000 clients without OOM (RSS ~0.6 GB; the guard sheds ~60% of hub queries as retryable budget errors) |

Both memory issues the load test surfaced are now closed; all tracked in the load-testing doc.

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE) for the
full text and [`NOTICE`](NOTICE) for attribution. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this work,
as defined in the Apache 2.0 license, shall be licensed as above, without any
additional terms or conditions.

SPDX-License-Identifier: Apache-2.0
