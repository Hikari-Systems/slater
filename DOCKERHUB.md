# Slater

**A low-memory, read-only, Bolt-speaking graph + vector engine.**

Slater serves an immutable graph over the **Bolt protocol** (port `7687`), so any
standard **neo4j driver** (JavaScript, Python, Go, Java, …) or `cypher-shell` can
query it. Its headline property: **resident memory stays bounded by fixed cache
budgets, independent of graph size** — it reads decompressed blocks on demand
from disk (local or network/NFS) rather than holding the whole graph in RAM.

It answers a broad read-only slice of Cypher — pattern
matching, `WITH`/`UNION`/`CALL {…}` subqueries, 70+ functions & aggregations,
temporal & geospatial values, label/property index seeks, and disk-native vector
KNN (`CALL db.idx.vector.queryNodes(...)`). Graphs are **built offline** by
`slater-build` and published as immutable generations, so the serving process
carries none of the write-side machinery (logs, locks, GC) — that's what keeps
reads fast and memory bounded.

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
| **Rugged under load** | The server and offline builder both compile with `#![forbid(unsafe_code)]` — the engine's only `unsafe` lives in the audited jemalloc allocator crate. Read-only means no write locks, no GC pauses, no data races. One bad query can't take the server down. |
| **Works with your neo4j tools** | Speaks Bolt 5.4 / 4.4 / 4.1 — use the standard neo4j drivers (JS, Python, Go, Java…), `cypher-shell`, or graph browsers unchanged. |
| **Rich read-only Cypher** | A broad query surface: `MATCH`/`WHERE`/`WITH`/`UNION`, `CALL {…}` subqueries, 70+ functions & aggregations, temporal & geospatial values, and regex. |
| **ISO GQL support (read-only aspects)** | Speaks a read-only subset of **ISO GQL** (ISO/IEC 39075) over the same Bolt connection — quantified paths, path restrictors, shortest-path selectors, label/type boolean expressions, `FOR`, `CAST`, and an optional `GQL`/`CYPHER` dialect prefix — alongside Cypher, in one engine. See [Querying with GQL](#querying-with-gql). |
| **Vectors + graph in one engine** | Disk-native ANN vector search (Vamana + PQ) for embeddings/RAG, plus graph algorithms (PageRank, BFS, betweenness, WCC…) — bounded memory even with millions of vectors. |
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
| `slater` | The online **Bolt server** (read-only). | Default entrypoint — just `docker run` the image. |
| `slater-build` | The offline **writer**: turns a Cypher dump into an immutable generation. | Override the entrypoint: `--entrypoint /app/slater-build`. |

`slater` never writes to disk; `slater-build` produces the generations `slater`
serves, on a shared `/data` volume. (The `:latest-lite` tag ships only the
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

`grants` maps **graph name → permissions** (`read` is the only permission). A user
can only see graphs they are granted.

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

Alongside read-only Cypher, Slater understands a read-only subset of **ISO GQL**
(ISO/IEC 39075) over the **same Bolt connection** — no separate endpoint, no driver
change. A statement may optionally start with a `GQL` or `CYPHER` dialect selector
(like Neo4j's `CYPHER 5` / `CYPHER 25`); it is stripped and the one engine parses the
rest, so the prefix changes nothing today. Every GQL form lowers onto an existing
capability, so GQL and Cypher spellings are equivalent and may be mixed.

| GQL | Cypher equivalent |
|---|---|
| `MATCH (a) ((x)-[:R]->(y)){1,3} (b)` | `MATCH (a)-[:R*1..3]->(b)` (quantified path) |
| `MATCH ACYCLIC (a)-[:R*]->(b)` | path restrictor (`WALK`/`TRAIL`/`ACYCLIC`/`SIMPLE`) over `-[:R*]->` |
| `MATCH ANY SHORTEST (a)-[:R*]->(b)` | `shortestPath((a)-[:R*]->(b))` (also `ALL SHORTEST`, `SHORTEST k`) |
| `MATCH (n:Person & !Admin)` | label/type booleans `&` `\|` `!`; `:A:B` stays AND sugar |
| `FOR x IN [1,2,3] RETURN x` | `UNWIND [1,2,3] AS x RETURN x` |
| `RETURN CAST('42' AS INTEGER)` | `RETURN toInteger('42')` |
| `GQL MATCH (n) RETURN n` | optional dialect prefix (no-op routing) |

Responses additionally carry **GQLSTATUS** status objects in the Bolt
`SUCCESS`/`FAILURE` metadata (`gql_status` + `status_description`), added alongside
the existing keys so older drivers are unaffected. For source and full
documentation see the [project repository](https://github.com/Hikari-Systems/slater).

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
| `dataBackend.s3.bucket` | `dataBackend__s3__bucket` | _(empty)_ | S3 bucket (required when `kind=s3`). |
| `dataBackend.s3.region` | `dataBackend__s3__region` | _(empty)_ | AWS region (e.g. `eu-west-2`); empty ⇒ from the environment. |
| `dataBackend.s3.endpoint` | `dataBackend__s3__endpoint` | _(empty)_ | Custom endpoint for an S3-compatible store (MinIO/localstack). |
| `dataBackend.s3.prefix` | `dataBackend__s3__prefix` | _(empty)_ | Key prefix under which generations live in the bucket. |
| `dataBackend.s3.pathStyle` | `dataBackend__s3__pathStyle` | `false` | Path-style addressing; required by most S3-compatible servers. |
| `dataBackend.s3.awsAccessKey` | `dataBackend__s3__awsAccessKey` | _(empty)_ | **Preferred** way to set the S3 access key id. Empty ⇒ standard AWS chain (`AWS_ACCESS_KEY_ID` env / profile / instance role). |
| `dataBackend.s3.awsSecretKey` | `dataBackend__s3__awsSecretKey` | _(empty)_ | **Preferred** way to set the S3 secret key, paired with `awsAccessKey`. Empty ⇒ AWS chain. |
| `dataBackend.s3.awsSessionToken` | `dataBackend__s3__awsSessionToken` | _(empty)_ | Optional STS session token; used only with `awsAccessKey`/`awsSecretKey`. |
| `dataBackend.s3.diskCacheBytes` | `dataBackend__s3__diskCacheBytes` | `0` | Local-disk block cache budget (second tier). `0` = off; when set, also set `diskCacheDir`. |
| `dataBackend.s3.diskCacheDir` | `dataBackend__s3__diskCacheDir` | _(empty)_ | Writable dir for the disk cache — a **real volume, not `tmpfs`**. |
| `dataBackend.gcs.bucket` | `dataBackend__gcs__bucket` | _(empty)_ | GCS bucket (required when `kind=gcs`). |
| `dataBackend.gcs.prefix` | `dataBackend__gcs__prefix` | _(empty)_ | Key prefix under which generations live in the bucket. |
| `dataBackend.gcs.endpoint` | `dataBackend__gcs__endpoint` | _(empty)_ | Custom endpoint for a GCS emulator (`fake-gcs-server`); empty ⇒ standard GCS endpoint. |
| `dataBackend.gcs.credentialsPath` | `dataBackend__gcs__credentialsPath` | _(empty)_ | Service-account JSON key file path. Empty ⇒ Application Default Credentials (Workload Identity / GCE metadata / `gcloud`). |
| `dataBackend.gcs.credentialsJson` | `dataBackend__gcs__credentialsJson` | _(empty)_ | Inline service-account JSON key; precedence over `credentialsPath`. Empty ⇒ `credentialsPath`, else ADC. |
| `dataBackend.gcs.anonymous` | `dataBackend__gcs__anonymous` | `false` | Unauthenticated access — a local GCS emulator (`fake-gcs-server`) **only**, never real GCS. |
| `dataBackend.gcs.diskCacheBytes` | `dataBackend__gcs__diskCacheBytes` | `0` | Local-disk block cache budget (second tier), same as the S3 setting. `0` = off; when set, also set `diskCacheDir`. |
| `dataBackend.gcs.diskCacheDir` | `dataBackend__gcs__diskCacheDir` | _(empty)_ | Writable dir for the GCS disk cache — a **real volume, not `tmpfs`**. |
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
| `cacheWarmingQuery` | `cacheWarmingQuery` | _(empty)_ | Cypher query run once at boot against every served graph, results discarded — pre-faults the blocks it touches into the caches so the first matching client query is warm. Empty = off. Bounded by the same `query.*` limits/timeout as a real query. |
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

You can **watch the pools live** over Bolt — `SHOW STORAGE INFO` appends per-pool
metrics:

```
block_cache_bytes / block_cache_entries / block_cache_hits / block_cache_misses / block_cache_evictions
vector_cache_…    result_cache_…
```

High `misses`/`evictions` on one pool while another sits idle means it's time to
rebalance the budgets.

---

## Storage backends (filesystem / S3 / GCS)

Slater serves the **same** immutable generation byte-format from either local
storage or an object store — only `dataBackend.kind` changes, never the data.

- **`fs` (default)** — generations live under `/data` (the mounted volume). This
  is the right choice for almost everyone: build a generation, mount it
  read-only, serve it. Nothing else to configure.
- **`s3`** — generations live in an **S3 (or S3-compatible: MinIO/localstack)
  bucket**; the image already ships with S3 support, so this is config-only.
  Integrity is checked from S3's server-computed SHA-256 via a metadata request
  (no body download). Credentials come from the standard AWS chain — pass
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (or use an instance role).
- **`gcs`** — generations live in a **Google Cloud Storage bucket**; the image
  ships with GCS support, so this is config-only. Integrity is checked from GCS's
  server-computed CRC32C via a metadata request (no body download). Credentials
  are GCP-native: Application Default Credentials by default (Workload Identity /
  GCE metadata / `gcloud`), or a service-account JSON key via
  `dataBackend__gcs__credentialsPath`.

**Use S3 or GCS when** you want generations in durable, central object storage that many
stateless, disk-less server replicas can all read — publish once, fan out — or to
decouple the build host from the serve hosts. The cost is latency: a cold block
is a network round-trip instead of a local read. The in-memory caches hide most
of it; the **local-disk block cache** below hides the rest. If your data already
sits on a fast local/NFS/EBS volume, `fs` is simpler and faster.

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

```bash
docker run -d --name slater-s3 -p 7687:7687 \
  -v "$PWD/acl.json:/config/acl.json:ro" \
  -v slater-s3-cache:/var/cache/slater \
  -e dataBackend__kind=s3 \
  -e dataBackend__s3__bucket=slater \
  -e dataBackend__s3__region=eu-west-2 \
  -e dataBackend__s3__diskCacheBytes=10737418240 \
  -e dataBackend__s3__diskCacheDir=/var/cache/slater \
  -e AWS_ACCESS_KEY_ID=… -e AWS_SECRET_ACCESS_KEY=… \
  hikarisystems/slater:latest
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

**GCS** (credentials via Application Default Credentials — mount a
service-account key and point `GOOGLE_APPLICATION_CREDENTIALS` at it, or use
`--publish-gcs-credentials`):

```bash
docker run --rm -v slater-data:/data -v "$PWD/dumps:/dumps:ro" \
  -v "$PWD/sa.json:/secrets/sa.json:ro" -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/sa.json \
  --entrypoint /app/slater-build hikarisystems/slater:latest \
  --input /dumps/people.cypher --graph people --data-dir /data \
  --publish-gcs-bucket slater --publish-gcs-prefix prod
#   or pass the key directly:  --publish-gcs-credentials /secrets/sa.json
```

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
--block-size <bytes>      Block size for prop/label/topology files    (default 262144)
--vector-block-size <n>   Block size for the vector store             (default 262144)
--zstd-level <n>          zstd level for all files                    (default 3)
--vector-spec <path>      JSON sidecar declaring vector indexes       (optional)
--vamana-threshold <n>    ≥ this many vectors ⇒ disk-native Vamana/PQ (default 50000)
--vamana-r <n>            Vamana out-degree bound R                    (default 32)
--vamana-alpha <f>        Vamana robust-prune factor alpha            (default 1.2)
--pq-subspaces <m>        PQ subspaces (must divide the dimension)     (default 16)
--pq-bits <n>             PQ bits per subspace (1..=8)                 (default 8)
--encrypt                 Encrypt every block at rest (needs a key)
--key-file <path>         Hex master key file        (with --encrypt)
--key-env <name>          Env var holding the hex key (with --encrypt)

  # Publish to S3 (in addition to the local --data-dir):
--publish-s3-bucket <b>   Also upload the generation to this S3 bucket
--publish-s3-region <r>   AWS region (e.g. eu-west-2)
--publish-s3-endpoint <u> Custom endpoint for an S3-compatible store (MinIO…)
--publish-s3-prefix <p>   Key prefix under which the generation is published
--publish-s3-path-style   Path-style addressing (most S3-compatible servers)

  # Publish to GCS (in addition to the local --data-dir):
--publish-gcs-bucket <b>  Also upload the generation to this GCS bucket
--publish-gcs-prefix <p>  Key prefix under which the generation is published
--publish-gcs-credentials Service-account JSON key file (else ADC / Workload Id)
--publish-gcs-endpoint <u> Custom endpoint for a GCS emulator (fake-gcs-server)
```

Both object-store backends are compiled into the published image, so the
`--publish-s3-*` / `--publish-gcs-*` flags work out of the box. Run `--help` for
the authoritative list:

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
      cache__blockCacheBytes: "268435456"
      cache__vectorCacheBytes: "134217728"
      cache__resultCacheBytes: "33554432"
      cache__cacheTtlMs: "1800000"
      reloadStrategy: "exit"
      log__level: "info"
    healthcheck:
      test: ["CMD", "/app/slater", "healthcheck"]
      interval: 10s
      timeout: 5s
      start_period: 15s
      retries: 3

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
```

---

## Performance

Six engines, one single-client suite, five graphs from a 62k-node toy to **Wikidata
91.6M nodes / 766M edges**, each engine **measured in isolation** (every other container
stopped — RSS and latency are its own footprint). slater is the **lowest-RSS engine at
every scale** and the only one whose footprint tracks the *query working set* rather than
the graph — it grows ~50× while the graph grows ~1,500×. Figures are medians (ms) or peak
resident memory (MiB); slater on its local-filesystem (`fs`) backend.

### Resident memory (MiB) — bounded as the graph grows ~1,500×

Committed working memory (what the OS cannot reclaim). Every engine except slater holds its
graph in committed memory (own heap, Neo4j's off-heap page cache, or a buffer pool); slater
serves from the **reclaimable OS page cache** of its on-disk store, so its figure is the anon
working set, with the store's page cache shown as *total* in parentheses for the two wiki
graphs. **Bold = lowest.**

| graph (nodes / edges) | slater | Neo4j 5 | Memgraph | FalkorDB | ArcadeDB | LadybugDB |
|---|--:|--:|--:|--:|--:|--:|
| pole — 62k / 106k | **11** | 746 | 114 | 140 | 1,556 | 198 |
| MeSH — 341k / 469k | **63** | 1,083 | 358 | 455 | 1,631 | 121 |
| EU-AI-Act — 21k / 45k (+55 MiB vec) | **99** | 729 | 229 | 312 | 1,948 | 286 |
| Wikidata — 1M / 13.8M | **33** *(295 total)* | ~2,330 | 2,716 | 1,506 | 2,247 | ~774 |
| Wikidata — 91.6M / 766M | **584** *(4,595 total)* | ~2,900 | cannot-load | cannot-load | cannot-load | ~652 |

The in-memory trio (Memgraph · FalkorDB · ArcadeDB) cannot hold the 766M graph at all
(~64–128 GiB resident); Neo4j commits a ~2 GiB heap regardless of query.

### Latency highlights (median ms)

slater owns the **metadata / index / scan** shapes (count, label, idx-eq, scan — ~0.6 ms,
10–150× the service engines), the **unanchored multi-hop** (MeSH 2-hop 1.9 ms, fastest in the
field), and whole-label **group-by / count(DISTINCT)** (0.5 ms, via a build-time histogram).
At **91.6M ≫ RAM** it is sole-fastest on every traversal shape (count 0.58 ms vs Neo4j ~4 s;
1-hop 4.25 ms; 3-hop 26.7 ms; shortestPath ≤6 52.6 ms) — and only the disk-backed engines load
that graph at all. The in-memory pair keep raw point lookups & 1-hop (0.5 ms vs slater's
1–2 ms); kNN is 2–3 ms **exact** (brute-force, recall 1.0) vs FalkorDB's 1.2 ms approximate
HNSW. Uncapped multi-hop `count(*)` counts during expansion (3-hop @ 91.6M: 554 ms / 0.66 GiB
at fanout 1), and `query.maxFanout` overlaps cold I/O-bound block reads across cores
(shortestPath ≤6 918 → 608 ms).

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
