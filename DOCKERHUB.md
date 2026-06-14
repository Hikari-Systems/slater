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
| **Tiny, dependency-light install** | A ~5 MB stripped static binary in a ~33 MB multi-arch image (amd64/arm64); pure-Rust TLS, no OpenSSL. Pull and run. |
| **Built for periodic publish** | Build a graph offline, serve it immutable, then atomically swap in a new version with zero downtime — ideal for data-warehouse / scheduled-refresh workloads. |
| **Rugged under load** | Written in Rust with no `unsafe`; read-only means no write locks, no GC pauses, no data races. One bad query can't take the server down. |
| **Works with your neo4j tools** | Speaks Bolt 5.4 / 4.4 / 4.1 — use the standard neo4j drivers (JS, Python, Go, Java…), `cypher-shell`, or graph browsers unchanged. |
| **Rich read-only Cypher** | A broad query surface: `MATCH`/`WHERE`/`WITH`/`UNION`, `CALL {…}` subqueries, 70+ functions & aggregations, temporal & geospatial values, and regex. |
| **ISO GQL support (read-only aspects)** | Speaks a read-only subset of **ISO GQL** (ISO/IEC 39075) over the same Bolt connection — quantified paths, path restrictors, shortest-path selectors, label/type boolean expressions, `FOR`, `CAST`, and an optional `GQL`/`CYPHER` dialect prefix — alongside Cypher, in one engine. See [Querying with GQL](#querying-with-gql). |
| **Vectors + graph in one engine** | Disk-native ANN vector search (Vamana + PQ) for embeddings/RAG, plus graph algorithms (PageRank, BFS, betweenness, WCC…) — bounded memory even with millions of vectors. |
| **Safe on network storage** | Every file is BLAKE3 content-hashed and verified on open; torn or half-copied images are refused, not served. Designed for NFS/remote volumes (no mmap surprises). |

---

## What's in the image

The image bundles **two binaries**:

| Binary | Role | How you run it |
|---|---|---|
| `slater` | The online **Bolt server** (read-only). | Default entrypoint — just `docker run` the image. |
| `slater-build` | The offline **writer**: turns a Cypher dump into an immutable generation. | Override the entrypoint: `--entrypoint /app/slater-build`. |

`slater` never writes to disk; `slater-build` produces the generations `slater`
serves, on a shared `/data` volume.

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
the existing keys so older drivers are unaffected. The full mapping with examples is
in the [README](https://github.com/Hikari-Systems/slater#supported-gql-subset).

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
| `dataDir` | `dataDir` | `/data` | Root dir of generations (`<graph>/<uuid>/`). |
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
```

Run `--help` for the authoritative list:

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

## Benchmarking

The repo's `perf/` harnesses back the headline claim — **resident memory bounded by
cache budgets, not graph size, at comparable query speed.** The same query suite runs on
**slater, Neo4j 5, Memgraph, FalkorDB** across a tiny pole graph and a cross-engine sweep
over MeSH, an EU-AI-Act vector graph, and Wikidata at 1M and 91.6M nodes (single client,
latency medians, each engine restarted + warmed, mean of 5 runs, varying parameters so
the result cache always misses). It is a correctness-and-footprint check, not a
throughput benchmark — **one story in two regimes**: memory stays bounded whether the
graph **fits in RAM** or is **far larger than it**.

**Memory is bounded as the graph grows ~1,500×.** Peak RSS while serving, slater on
default cache budgets:

| graph (nodes / edges) | slater | Neo4j 5 | Memgraph | FalkorDB |
|---|--:|--:|--:|--:|
| pole — 62k / 106k | **82 MiB** | 774 | 115 | 140 |
| MeSH — 340k / 469k | **258 MiB** | 1,117 | 355 | 454 |
| Wikidata — 1M / 13.8M | **~150 MiB** | 2,012 | 2,716 | 1,506 |
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
an exact brute-force scan vs the others' resident HNSW: an algorithmic gap, not memory;
`blockCacheBytes` is the dial that trades kNN latency for footprint.) Full per-engine
tables, methodology, and the cache-budget sweep are in the
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
