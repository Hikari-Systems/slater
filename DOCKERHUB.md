# Slater

**A low-memory, Bolt-speaking graph + vector engine that serves graphs bigger than RAM — and takes live, durable writes.**

Slater serves a graph over the **Bolt protocol** (port `7687`), so any standard
**neo4j driver** (JavaScript, Python, Go, Java, …) or `cypher-shell` can query it.
Its headline property: **resident memory stays bounded by fixed cache budgets,
independent of graph size** — it reads decompressed blocks on demand from disk
(local or network/NFS) rather than holding the whole graph in RAM. That is why it
serves graphs the in-memory engines cannot even load (Wikidata 91.6M nodes / 1.5B
edges from a few hundred MB of RAM).

It answers a broad slice of Cypher — pattern matching, `WITH`/`UNION`/`CALL {…}`
subqueries, 70+ functions & aggregations, temporal & geospatial values,
label/property index seeks, and disk-native vector KNN
(`CALL db.idx.vector.queryNodes(...)`). Graphs are **compiled offline** by
`slater-build` into immutable, content-hashed generations; the read hot path
carries none of the write-side machinery (logs, locks, GC), which is what keeps
reads fast and memory bounded. On top of that immutable core sits an **opt-in
single-writer write layer** (`delta.enabled`): business-key `MERGE` / `SET` /
`DELETE` over Bolt, group-committed and `fsync`-durable, folded back into a fresh
core by consolidation — so you correct, insert and retract without rebuilding the
graph, and reads stay byte-identical when no writes are pending.

📦 **Source, issues & full documentation:**
[github.com/Hikari-Systems/slater](https://github.com/Hikari-Systems/slater)

---

## Features

| Feature | What it means for you |
|---|---|
| **Bounded, predictable memory** | Resident memory is capped by three cache budgets *you* set — it does **not** grow with graph size. You tune the performance/RAM trade-off instead of provisioning for the whole graph. |
| **Multi-tenant out of the box** | One server hosts many graphs with per-user read grants — multi-database isolation that most graph DBs reserve for a paid/enterprise tier. |
| **Encryption at rest & in transit** | Per-block XChaCha20-Poly1305 sealing (the key is never written to disk) plus optional TLS (`bolt+s://`). GDPR-friendly by construction. |
| **Tiny, dependency-light install** | A small stripped binary on a distroless glibc base (no shell/apt) — the multi-arch (amd64/arm64) image pulls at ~22 MB, or ~12 MB for the server-only `slater:latest-lite` tag; pure-Rust TLS, no OpenSSL. Pull and run. |
| **Built for periodic publish** | Build a graph offline, serve it immutable, then atomically swap in a new version with zero downtime — ideal for data-warehouse / scheduled-refresh workloads. |
| **Rugged under load** | The server and offline builder both compile with `#![forbid(unsafe_code)]` — the engine's only `unsafe` lives in the audited jemalloc allocator crate. The immutable core means reads take no locks and never wait; a single writer serialises mutations behind the write path alone — no GC pauses, no data races. One bad query can't take the server down. |
| **Works with your neo4j tools** | Speaks Bolt 5.4 / 4.4 / 4.1 — use the standard neo4j drivers (JS, Python, Go, Java…), `cypher-shell`, or graph browsers unchanged. |
| **Rich Cypher query surface** | A broad read surface: `MATCH`/`WHERE`/`WITH`/`UNION`, `CALL {…}` subqueries, 70+ functions & aggregations, temporal & geospatial values, and regex. |
| **Live, durable writes** | An opt-in single-writer LSM layer over the immutable core (`delta.enabled`): business-key `MERGE` / `SET` / `DELETE` over nodes and relationships, batched write-`UNWIND` (one `fsync` per batch), and `CALL slater.consolidate()` — group-committed, `fsync`-durable, folded back into a fresh core. Reads are byte-identical when the delta is empty. See [Writing to a graph](#writing-to-a-graph). |
| **ISO GQL, read and write** | Speaks a subset of **ISO GQL** (ISO/IEC 39075) over the same Bolt connection — quantified paths, path restrictors, shortest-path selectors, label/type boolean expressions, `FOR`, `CAST`, an optional `GQL`/`CYPHER` dialect prefix — and, with the writable layer on, GQL's data-modifying statements (`INSERT` / `SET` / `REMOVE` / `DELETE`) lower onto the same durable write path. Cypher and GQL, in one engine. See [Querying with GQL](#querying-with-gql). |
| **Vectors + graph in one engine** | Disk-native ANN vector search (Vamana + PQ; cosine / L2 / dot) for embeddings/RAG, plus graph algorithms (PageRank, BFS, betweenness, WCC…) — bounded memory even with millions of vectors. Embeddings are **writable** in place (a FreshDiskANN-style write ladder): insert / update / delete a vector and it is KNN-visible at once, folded into the base without an offline rebuild. |
| **Safe on network storage** | Every file is BLAKE3 content-hashed and verified on open; torn or half-copied images are refused, not served. Designed for NFS/remote volumes (no mmap surprises). |
| **Pluggable storage backends** | Serve the same generation format from a local volume, an S3 (S3-compatible) bucket, **or** a Google Cloud Storage bucket — publish once, fan out to stateless replicas — with an optional local-disk cache tier in front of the object store. See [Storage backends](#storage-backends-filesystem--s3--gcs). |

---

## Two variants: full and `-lite` (same `slater` repo)

Each release publishes two flavours, as **tags of the one `hikarisystems/slater`
repo** (multi-arch amd64 + arm64):

| Tag | Contains | Storage backends | Use it when |
|---|---|---|---|
| **`:latest`** / **`:vX.Y.Z`** (full) | both binaries (`slater` + `slater-build`) | filesystem **+ S3 + GCS** | the default — you want to build generations in-container and/or serve from (and publish to) S3 or GCS object storage. |
| **`:latest-lite`** / **`:vX.Y.Z-lite`** | the server only (`slater`) | filesystem **only** | a smaller image / smaller dependency surface for the common serve-only case: serve a generation built elsewhere from a local or mounted volume. No object-store backends, no `slater-build`. |

Everything below uses the full tag but applies equally to the `-lite` tag for the
server bits (the `slater-build` and S3/GCS sections are full-only).

```bash
docker pull hikarisystems/slater:latest   # full
docker pull hikarisystems/slater:latest-lite     # server only, fs backend
```

## What's in the image

The full image bundles **two binaries**:

| Binary | Role | How you run it |
|---|---|---|
| `slater` | The online **Bolt server**: serves reads, plus the single-writer durable write path when `delta.enabled`. | Default entrypoint — just `docker run` the image. |
| `slater-build` | The offline **compiler**: turns a Cypher dump into an immutable generation. | Override the entrypoint: `--entrypoint /app/slater-build`. |

`slater-build` compiles the immutable generations the server serves, on a shared `/data`
volume. A read replica never writes to disk; a writer (`delta.enabled`) additionally keeps
its write-ahead log on a durable local volume. (The `:latest-lite` tag ships only the
`slater` server.)

```
  dump.cypher ──[ slater-build ]──▶ /data/<graph>/<uuid>/ + `current` ──[ slater ]──▶ neo4j driver
```

---

## Quick start

### 1. Mint a password hash (for the ACL)

Passwords are stored as argon2id hashes, never cleartext:

```bash
docker run --rm hikarisystems/slater:latest hash-password 'choose-a-password'
# → $argon2id$v=19$m=19456,t=2,p=1$....
```

Create `acl.json` next to you, pasting that hash in:

```json
{
  "users": {
    "reporting": {
      "passwordArgon2id": "$argon2id$v=19$...your-hash...",
      "grants": { "people": ["read"] }
    }
  }
}
```

`grants` maps **graph name → permissions**: `read` (query the graph) and `write`
(mutate it through the writable layer). They are independent — a `read` grant confers no
write access — so a writer needs both, `["read", "write"]`. A user can only see graphs
they are granted. This quick start grants `read` only; see [Writing to a graph](#writing-to-a-graph)
to enable writes.

### 2. Build a graph generation

Put a primitive-Cypher creation script (`CREATE (...)`, `CREATE (a)-[...]->(b)`)
in a local `./dumps` folder, then write it into a named Docker volume:

```bash
docker volume create slater-data

docker run --rm \
  -v slater-data:/data \
  -v "$PWD/dumps:/dumps:ro" \
  --entrypoint /app/slater-build \
  hikarisystems/slater:latest \
  --input /dumps/people.cypher --graph people --data-dir /data
```

This writes `/data/people/<uuid>/…` and a `current` pointer.

### 3. Run the server

```bash
docker run -d --name slater \
  -p 7687:7687 \
  -v slater-data:/data:ro \
  -v "$PWD/acl.json:/config/acl.json:ro" \
  hikarisystems/slater:latest
```

The server reads `/data` (read-only) and the ACL at `/config/acl.json` (the
baked-in default path). It listens for Bolt on `7687`.

### 4. Connect and query

Any neo4j driver works; the **database name is the graph name**. Example with
`cypher-shell` from a container on the same Docker network:

```bash
docker run --rm -it --network host neo4j:5 \
  cypher-shell -a bolt://localhost:7687 -u reporting -p 'choose-a-password' -d people \
  "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
```

From application code use `bolt://<host>:7687` (or `bolt+s://` with TLS), basic
auth, and set the session `database` to the graph you want.

### Querying with GQL

Alongside Cypher, Slater understands a subset of **ISO GQL** (ISO/IEC 39075) over the
**same Bolt connection** — no separate endpoint, no driver change. A statement may
optionally start with a `GQL`/`CYPHER` dialect selector (like Neo4j's `CYPHER 5`); it is
stripped and the one engine parses the rest. Every GQL form lowers onto an existing
capability, so GQL and Cypher spellings are equivalent and mixable — reads always, and
GQL's data-modifying statements too when the writable layer is on.

| GQL | Cypher equivalent |
|---|---|
| `MATCH (a) ((x)-[:R]->(y)){1,3} (b)` | `MATCH (a)-[:R*1..3]->(b)` (quantified path) |
| `MATCH ACYCLIC (a)-[:R*]->(b)` | path restrictor (`WALK`/`TRAIL`/`ACYCLIC`/`SIMPLE`) over `-[:R*]->` |
| `MATCH ANY SHORTEST (a)-[:R*]->(b)` | `shortestPath((a)-[:R*]->(b))` (also `ALL SHORTEST`, `SHORTEST k`) |
| `MATCH (n:Person & !Admin)` | label/type booleans `&` `\|` `!`; `:A:B` stays AND sugar |
| `FOR x IN [1,2,3] RETURN x` | `UNWIND [1,2,3] AS x RETURN x` |
| `RETURN CAST('42' AS INTEGER)` | `RETURN toInteger('42')` |
| `INSERT (n:Person {name:'Zoe'})` | `CREATE (n:Person {name:'Zoe'})` (writable layer; GQL's `SET`/`REMOVE`/`DELETE` are spelled as in Cypher) |
| `GQL MATCH (n) RETURN n` | optional dialect prefix (no-op routing) |

Responses also carry **GQLSTATUS** objects in the Bolt `SUCCESS`/`FAILURE` metadata
(`gql_status` + `status_description`), added alongside the existing keys so older
drivers are unaffected.

---

## Writing to a graph

Writes are **off until you turn them on**. Enable the writable layer with
`delta__enabled=true`, grant the user `write` (alongside `read`), and give the container a
**durable, writable volume for the write-ahead log** — a writer is *not* the read-only-rootfs
shape a read replica uses. Then you correct, insert and retract over Bolt, live, without
rebuilding the graph.

```bash
docker volume create slater-wal        # durable — NOT tmpfs

docker run -d --name slater-writer \
  -p 7687:7687 \
  -v slater-data:/data \
  -v slater-wal:/wal \
  -v "$PWD/acl.json:/config/acl.json:ro" \
  -e delta__enabled=true \
  -e delta__walDir=/wal \
  hikarisystems/slater:latest
```

`acl.json` must grant both permissions to the writer:

```json
{ "users": { "editor": {
  "passwordArgon2id": "$argon2id$v=19$...",
  "grants": { "people": ["read", "write"] } } } }
```

Now write over the same Bolt connection any neo4j driver uses:

```cypher
-- Upsert by business key, then set a property.
MERGE (p:Person {name: 'Dave'}) SET p.age = 33;

-- Batch many rows into one group-committed, fsync-durable write.
UNWIND $rows AS r MERGE (p:Person {name: r.name}) SET p.age = r.age;

-- Retract.
MATCH (p:Person {name: 'Dave'}) DETACH DELETE p;

-- Fold the accumulated delta back into a fresh immutable core (optional; also
-- runs automatically per delta.deltaCorePercent / delta.consolidateWindow).
CALL slater.consolidate();
```

How it behaves:

- **A single writer, durable on ack.** Mutations serialise behind one writer per graph; the
  Bolt `SUCCESS` returns only after the `fsync` that makes the write durable. Group your
  writes (a write-`UNWIND` commits one `fsync` for the whole batch) and they're cheap.
- **Reads don't pay for it.** The delta overlay sits below the read surface and is
  byte-identical to the pure-core read path when empty; whole-graph counters (`count(*)`,
  label/type marginals) stay metadata reads even with writes pending.
- **The write grammar is business-key-addressed:** `MERGE` / `MATCH … SET` / `DELETE` (plus
  `CREATE`, `REMOVE`, detach delete, relationship writes) keyed on a node's identity property.
- **Consolidation folds the delta into a new core** by rebuilding through `slater-build` and
  swapping `current` atomically — the same content-hash guard as any published generation.

Tune it with the `delta.*` knobs in [Configuration](#configuration) below.

---

## Configuration

Slater reads a baked-in `/app/config.json` and lets you override **any field two
ways**, both Docker-friendly:

1. **A config overlay file** — mount your own JSON at `/sandbox/config.json`; it
   is deep-merged over the defaults at startup.
2. **Environment variables** — `KEY__sub` form (**double underscore** for
   nesting), keys matching the camelCase config. Env wins over files.

So these are equivalent ways to set the block-cache budget:

```bash
-e cache__blockCacheBytes=536870912           # env override
# …or in /sandbox/config.json: { "cache": { "blockCacheBytes": 536870912 } }
```

### Key knobs

| Setting | Env var | Default | What it does |
|---|---|---|---|
| `dataBackend.kind` | `dataBackend__kind` | `fs` | Storage backend: `fs` (local filesystem), `s3` (object store), or `gcs` (Google Cloud Storage). See [Storage backends](#storage-backends-filesystem--s3--gcs). |
| `dataBackend.fs.dir` | `dataBackend__fs__dir` | `/data` | Root dir of generations (`<graph>/<uuid>/`) for the `fs` backend. |
| `dataBackend.s3.*` | `dataBackend__s3__*` | _(empty)_ | S3 settings: `bucket`, `region`, `endpoint`, `prefix`, `pathStyle`, `awsAccessKey`/`awsSecretKey`/`awsSessionToken` (else the standard AWS chain), and `diskCacheBytes`/`diskCacheDir` (local-disk second tier). See [Storage backends](#storage-backends-filesystem--s3--gcs). |
| `dataBackend.gcs.*` | `dataBackend__gcs__*` | _(empty)_ | GCS settings: `bucket`, `prefix`, `endpoint`, `credentialsPath`/`credentialsJson` (else ADC), `anonymous` (emulator only), and `diskCacheBytes`/`diskCacheDir`. See [Storage backends](#storage-backends-filesystem--s3--gcs). |
| `aclPath` | `aclPath` | `/config/acl.json` | Path to the ACL file. |
| `server.bind` | `server__bind` | `0.0.0.0` | Bind address. |
| `server.port` | `server__port` | `7687` | Bolt port. |
| `cache.blockCacheBytes` | `cache__blockCacheBytes` | `268435456` (256 MiB) | Decompressed graph-block LRU. |
| `cache.vectorCacheBytes` | `cache__vectorCacheBytes` | `134217728` (128 MiB) | Resident PQ codes + Vamana block LRU. |
| `cache.resultCacheBytes` | `cache__resultCacheBytes` | `33554432` (32 MiB) | Query-result LRU. |
| `cache.cacheTtlMs` | `cache__cacheTtlMs` | `1800000` (30 min) | **Idle TTL**: a cached entry untouched for this long is reclaimed by a background sweep, freeing memory below the budgets. A **negative** value (or `0`) disables the sweep. |
| `query.maxRows` | `query__maxRows` | `100000` | Max rows per result. |
| `query.timeoutMs` | `query__timeoutMs` | `30000` | Per-query timeout (`0` = none). |
| `vectorQuery.beamWidth` | `vectorQuery__beamWidth` | `64` | Vamana beam-search width. |
| `generationPollMs` | `generationPollMs` | `5000` | How often to poll each graph's `current` pointer. |
| `reloadStrategy` | `reloadStrategy` | `exit` | On a generation change: `exit` (let the orchestrator restart) or `swap` (hot-swap in place). |
| `cacheWarmingQuery` | `cacheWarmingQuery` | _(empty)_ | Cypher run once at boot per graph (results discarded) to pre-fault its blocks into the caches so the first matching client query is warm. Empty = off. |
| `delta.enabled` | `delta__enabled` | `false` | Master switch for the writable layer. Off ⇒ pure immutable core; writes refused. See [Writing to a graph](#writing-to-a-graph). |
| `delta.walDir` | `delta__walDir` | `wal` | Directory for per-graph WAL segments (the durability floor). Must be a **durable, writable volume — never `tmpfs`**. Relative paths resolve under the data dir. |
| `delta.memtableBytes` | `delta__memtableBytes` | `67108864` (64 MiB) | In-RAM active memtable budget before it flushes to an immutable L0 delta segment. |
| `delta.deltaCorePercent` | `delta__deltaCorePercent` | `0` (off) | Auto-consolidate once the delta reaches this % of the core's entity count (rebuild is O(core) — keep rare; typical 5–25). `0` ⇒ manual/scheduled only. |
| `delta.consolidateWindow` | `delta__consolidateWindow` | _(empty)_ | Off-peak cron window gating auto-consolidation, e.g. `0 1-5 * * *`. Empty ⇒ whenever due. |
| `delta.deltaHardBytes` | `delta__deltaHardBytes` | `0` (off) | Hard cap on resident delta bytes; a write past it throttles until a consolidation drains — the OOM backstop. |
| `log.level` | `log__level` | `info` | Log level. |

Example — a memory-tight server with a 10-minute idle TTL:

```bash
docker run -d --name slater -p 7687:7687 \
  -v slater-data:/data:ro -v "$PWD/acl.json:/config/acl.json:ro" \
  -e cache__blockCacheBytes=134217728 \
  -e cache__vectorCacheBytes=67108864 \
  -e cache__cacheTtlMs=600000 \
  hikarisystems/slater:latest
```

---

## Memory & cache behaviour

Resident memory ≈ `blockCacheBytes + vectorCacheBytes + resultCacheBytes` plus a
small fixed overhead — it does **not** grow with graph size. The three pools are
isolated on purpose so a vector-heavy query can't evict hot graph blocks (and
vice versa); tune the split with the budgets above.

The **idle TTL** (`cacheTtlMs`, default 30 min) reclaims pool memory once the
working set goes quiet, so you can set generous budgets without paying for idle
RAM. It only affects idle time — under concurrent load the budgets still cap RSS.

**Watch the pools live** over Bolt — `SHOW STORAGE INFO` appends per-pool metrics
(`block_cache_hits` / `misses` / `evictions`, and the same for `vector_cache_*` and
`result_cache_*`). High misses/evictions on one pool while another sits idle means it's
time to rebalance the budgets.

---

## Storage backends (filesystem / S3 / GCS)

Slater serves the **same** immutable generation byte-format from either local
storage or an object store — only `dataBackend.kind` changes, never the data.

- **`fs` (default)** — generations live under `/data` (the mounted volume). Right for
  almost everyone: build, mount read-only, serve. Nothing else to configure.
- **`s3`** — generations live in an **S3 (or S3-compatible: MinIO/localstack) bucket**;
  S3 support ships in the image (config-only). Integrity from S3's server-computed
  SHA-256 via a metadata request (no body download). Credentials from the standard AWS
  chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or an instance role).
- **`gcs`** — generations live in a **Google Cloud Storage bucket** (config-only).
  Integrity from GCS's server-computed CRC32C via a metadata request. Credentials are
  GCP-native: Application Default Credentials, or a service-account JSON key.

**Use S3 or GCS when** you want generations in durable, central object storage that many
stateless replicas can read — publish once, fan out — or to decouple build from serve
hosts. The cost is latency (a cold block is a network round-trip); the in-memory caches
and the **local-disk block cache** below hide most of it. If your data sits on a fast
local/NFS/EBS volume, `fs` is simpler and faster.

```bash
docker run -d --name slater-s3 -p 7687:7687 \
  -v "$PWD/acl.json:/config/acl.json:ro" \
  -e dataBackend__kind=s3 \
  -e dataBackend__s3__bucket=slater \
  -e dataBackend__s3__region=eu-west-2 \
  -e AWS_ACCESS_KEY_ID=… -e AWS_SECRET_ACCESS_KEY=… \
  hikarisystems/slater:latest
```

### Local-disk block cache (S3/GCS second tier)

Optional, opt-in: set `dataBackend.s3.diskCacheBytes > 0` and mount a **writable
volume** for `diskCacheDir`. A block evicted from the in-memory cache is then
served from local SSD (~0.1 ms) instead of a fresh S3 GET — so an S3-backed node
performs close to a local one once warm, and your S3 request count/cost drops.
It caches the bytes exactly as fetched (still compressed and, for encrypted
generations, still sealed — it never holds the key), writes them behind the query
on a background thread, evicts to stay within budget, and self-heals a corrupt
cache file with a checksum verified on every read.

> **Must be a real writable volume, never `tmpfs`.** tmpfs is RAM and would break
> the bounded-memory guarantee. Size the cache **≫ `blockCacheBytes`**.

Add to the S3 `docker run` above: a writable cache volume and the two flags —

```bash
  -v slater-s3-cache:/var/cache/slater \
  -e dataBackend__s3__diskCacheBytes=10737418240 \
  -e dataBackend__s3__diskCacheDir=/var/cache/slater
```

### Publishing a generation to S3 or GCS

`slater-build` writes to `--data-dir` first, then optionally uploads to a bucket
(the remote `current` pointer is written last, so a serving node never sees a
half-published generation). The image ships with **both** the `s3` and `gcs`
backends compiled in, so publishing to either needs no special image — just the
flags.

**S3** (credentials via the `AWS_*` env, an instance role, or
`--publish-s3-*` flags for an S3-compatible endpoint):

```bash
docker run --rm -v slater-data:/data -v "$PWD/dumps:/dumps:ro" \
  -e AWS_ACCESS_KEY_ID=… -e AWS_SECRET_ACCESS_KEY=… \
  --entrypoint /app/slater-build hikarisystems/slater:latest \
  --input /dumps/people.cypher --graph people --data-dir /data \
  --publish-s3-bucket slater --publish-s3-region eu-west-2
#   MinIO/localstack: add  --publish-s3-endpoint http://host:9000 --publish-s3-path-style
```

**GCS** — same shape, swapping in `--publish-gcs-bucket <b>` (plus optional
`--publish-gcs-prefix`). Credentials use Application Default Credentials, or mount a
service-account key and add `--publish-gcs-credentials /secrets/sa.json`.

---

## Authentication

- Mint hashes with `docker run --rm hikarisystems/slater hash-password '<pw>'`.
- Put users + per-graph `grants` in `acl.json`, mount it at `/config/acl.json`
  (or point `aclPath` elsewhere).
- The ACL file is **hot-reloaded** — edit it and the server picks up changes
  without a restart (a malformed file keeps the last good version).

---

## Encryption at rest (optional)

Generations can be sealed per-block with XChaCha20-Poly1305. Build encrypted:

```bash
docker run --rm \
  -v slater-data:/data -v "$PWD/dumps:/dumps:ro" \
  -e MASTER_KEY="$(openssl rand -hex 32)" \
  --entrypoint /app/slater-build \
  hikarisystems/slater:latest \
  --input /dumps/people.cypher --graph people --data-dir /data \
  --encrypt --key-env MASTER_KEY
```

Serve it by giving the server the same key — either an env var or a mounted file:

```bash
-e encryption__keyEnv=MASTER_KEY -e MASTER_KEY=<hex>
# …or:
-e encryption__keyFile=/run/secrets/slater-key   # mount the hex key there
```

## TLS (optional, `bolt+s://`)

Mount PEM material and point at it:

```bash
-v "$PWD/tls:/sandbox/tls:ro" \
-e tls__cert=/sandbox/tls/server.crt \
-e tls__key=/sandbox/tls/server.key
```

---

## Health check

The container `HEALTHCHECK` is built in — it performs a **Bolt handshake** (not
HTTP) against the configured port. You can also run it manually:

```bash
docker exec slater /app/slater healthcheck localhost 7687   # exit 0 = healthy
```

---

## `slater-build` reference

```
--input <path|->          Creation script, or - for stdin            (required)
--graph <name>            Logical graph name                          (required)
--data-dir <dir>          Root data dir to write <graph>/<uuid>/      (required)
--zstd-level <n>          zstd level for all files                    (default 3)
--vector-spec <path>      JSON sidecar declaring vector indexes       (optional)
--encrypt  --key-env <name> | --key-file <path>   Encrypt blocks at rest
--publish-s3-*  /  --publish-gcs-*    Also upload the generation to S3 / GCS
```

Block-size and vector (Vamana/PQ) tuning flags are omitted above; both object-store
backends are compiled into the published image, so the `--publish-s3-*` /
`--publish-gcs-*` flags work out of the box. Run `--help` for the authoritative list:

```bash
docker run --rm --entrypoint /app/slater-build hikarisystems/slater:latest --help
```

---

## Updating a live graph

Build a **new** generation into the same `/data` volume (same `--graph`, new
uuid + `current` pointer). The running server polls `current` every
`generationPollMs` and applies `reloadStrategy`:

- `exit` (default) — the server exits non-zero so your orchestrator restarts it
  cleanly against the new generation.
- `swap` — the server validates and hot-swaps the new generation in place,
  letting in-flight queries finish on the old one.

---

## docker compose example

```yaml
services:
  slater:
    image: hikarisystems/slater:latest
    read_only: true
    tmpfs: ["/tmp", "/run"]
    ports: ["7687:7687"]
    volumes:
      - slater-data:/data:ro
      - ./acl.json:/config/acl.json:ro
    environment:
      # cache budgets default to 256/128/32 MiB — override any cache__* here as needed
      cache__cacheTtlMs: "1800000"
      reloadStrategy: "exit"
    healthcheck:
      test: ["CMD", "/app/slater", "healthcheck"]
      interval: 10s
      start_period: 15s

  # A WRITER (opt-in): writable layer on, a durable WAL volume, NOT a read-only rootfs.
  #   docker compose --profile write up slater-writer
  slater-writer:
    image: hikarisystems/slater:latest
    profiles: ["write"]
    ports: ["7688:7687"]
    volumes:
      - slater-data:/data          # writable, not :ro — consolidation rebuilds here
      - slater-wal:/wal            # durable WAL volume — never tmpfs
      - ./acl.json:/config/acl.json:ro
    environment:
      delta__enabled: "true"
      delta__walDir: "/wal"
    healthcheck:
      test: ["CMD", "/app/slater", "healthcheck"]
      interval: 10s
      start_period: 15s

  # Run a build on demand:
  #   docker compose run --rm builder --input /dumps/x.cypher --graph x --data-dir /data
  builder:
    image: hikarisystems/slater:latest
    profiles: ["build"]
    entrypoint: ["/app/slater-build"]
    volumes:
      - slater-data:/data
      - ./dumps:/dumps:ro

volumes:
  slater-data:
  slater-wal:
```

---

## Performance

Up to six engines, one single-client suite, graphs from a 62k-node toy to **Wikidata
91.6M nodes / 1.5B edges**, each **measured in isolation**. slater is the **lowest-RSS
engine at every scale** and the only one whose footprint tracks the *query working set*,
not the graph. The latency figures are a fresh Slater 0.21.0 pass (the 91.6M row is a
same-box, shared-anchor slater-vs-Neo4j comparison); resident-memory figures carry forward
from the earlier cgroup-measured pass. Figures are medians (ms) or peak resident memory
(MiB), slater on its local-filesystem (`fs`) backend.

### Resident memory (MiB) — bounded as the graph grows ~1,500×

Committed working memory (what the OS cannot reclaim). Every engine except slater holds its
graph in committed memory; slater serves from the **reclaimable OS page cache** of its
on-disk store, so its figure is the anon working set (store page cache shown as *total* in
parentheses for the two wiki graphs). **Bold = lowest.**

| graph (nodes / edges) | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| pole — 62k / 106k | **11** | 746 | 114 | 140 | 1,556 | 198 |
| MeSH — 341k / 469k | **63** | 1,083 | 358 | 455 | 1,631 | 121 |
| EU-AI-Act — 21k / 45k (+55 MiB vec) | **99** | 729 | 229 | 312 | 1,948 | 286 |
| Wikidata — 91.6M / 1.5B | **584** *(4,595 total)* | ~2,900 | cannot-load | cannot-load | cannot-load | ~652 |

The in-memory trio (Memgraph · FalkorDB · ArcadeDB) cannot hold the 1.5B graph at all
(~64–128 GiB resident); Neo4j commits a ~2 GiB heap regardless of query.

### Latency highlights (median ms)

slater owns the **metadata / index / scan** shapes (count, label, idx-eq, scan ~0.4 ms,
10–200× the service engines), the **unanchored multi-hop** (MeSH 2-hop 1.40 ms), the
**indexed point lookup** (0.43 ms, edging the in-memory pair), and whole-label **group-by /
count(DISTINCT)** (0.45 ms, build-time histogram). At **91.6M ≫ RAM** — where only the
disk-backed engines load the graph at all — a fresh same-box, shared-anchor pass has slater
crushing `count` (**0.41 ms vs Neo4j 3.6 s**, ~8800×), point/degree/3-hop (~2–10×), even with
Neo4j on 1–2-hop (fanout 8 pulls ahead), but **losing var-length `*1..2` distinct (~1 s vs
Neo4j's 47 ms)** — an honest slater weak spot. kNN is 2.4–2.9 ms **exact** (recall 1.0) vs
FalkorDB's 1.2 ms approximate HNSW.

Full per-engine tables (every shape, every graph) are in the
[cross-engine benchmark README](https://github.com/Hikari-Systems/slater/tree/main/perf/cross-engine-hs).

---

## Tags

- `:latest` — the most recent release.
- `:vX.Y.Z` — a specific release (e.g. `:v0.1.4`).

Multi-arch: **linux/amd64** and **linux/arm64**.
```bash
docker pull hikarisystems/slater:latest
```

---

## Links

- **GitHub repository:** <https://github.com/Hikari-Systems/slater>
- **Issues & bug reports:** <https://github.com/Hikari-Systems/slater/issues>
- **Releases & changelog:** <https://github.com/Hikari-Systems/slater/releases>
- **Full README & design docs:** <https://github.com/Hikari-Systems/slater#readme>
- **License:** Apache-2.0

This page is generated from
[`DOCKERHUB.md`](https://github.com/Hikari-Systems/slater/blob/main/DOCKERHUB.md)
in the repository and synced on each release.
