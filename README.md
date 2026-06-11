# Slater

A low-memory, read-only, Bolt-speaking graph engine.

> **On the name.** Slater is named after the CIA agent in *Archer* (a great show)
> who insists on going by a single name — "Just… Slater" — and one of my favourite
> characters in it. See the
> [character wiki page](https://archer.fandom.com/wiki/Slater).

Slater serves an **immutable, on-disk** graph image over the **Bolt** protocol
(so any standard neo4j driver can talk to it), keeping **resident memory bounded
by its cache budgets — independent of graph size**. It replaces an in-memory
engine whose RSS scaled with the graph: where that engine held the whole graph
resident, Slater holds only bounded caches and reads everything else from disk on
demand, including the disk-native approximate-nearest-neighbour (Vamana/PQ) vector
path.

Two binaries make up the workspace:

| Binary | Role |
| --- | --- |
| `slater` | The online, read-only Bolt server (the container ENTRYPOINT). |
| `slater-build` | The offline writer: turns a primitive-Cypher dump into an immutable, content-hashed generation directory. |

Slater is **read-only**: no `CREATE`/`MERGE`/`SET`/`DELETE`/`REMOVE`/`DROP`, and
the only permitted procedure is `db.idx.vector.queryNodes` (cosine KNN). Writes
happen offline, by building a new generation and atomically swapping the
`current` pointer; the running server picks the change up via its generation
guard (see [Generation guard](#generation-guard)).

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
| `cache.blockCacheBytes` | `cache__blockCacheBytes` | 256 MiB | Decompressed block LRU budget. |
| `cache.vectorCacheBytes` | `cache__vectorCacheBytes` | 128 MiB | Vector pool (resident PQ + Vamana-block LRU) budget. |
| `cache.resultCacheBytes` | `cache__resultCacheBytes` | 32 MiB | Result LRU budget. |
| `tls.cert` / `tls.key` | `tls__cert` / `tls__key` | _(empty)_ | PEM material; both set ⇒ `bolt+s`. Empty ⇒ plaintext (loopback dev). |
| `encryption.keyFile` | `encryption__keyFile` | _(empty)_ | File holding the hex at-rest master key. |
| `encryption.keyEnv` | `encryption__keyEnv` | _(empty)_ | Env var holding the hex at-rest master key. |
| `query.maxRows` | `query__maxRows` | 100000 | Per-query row cap. |
| `query.timeoutMs` | `query__timeoutMs` | 30000 | Per-query wall-clock deadline (0 ⇒ none). |
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

The KNN `score` is the **cosine distance** (ascending — nearest first), matching
FalkorDB's `db.idx.vector.queryNodes` contract.

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

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE) for the
full text and [`NOTICE`](NOTICE) for attribution. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this work,
as defined in the Apache 2.0 license, shall be licensed as above, without any
additional terms or conditions.

SPDX-License-Identifier: Apache-2.0
