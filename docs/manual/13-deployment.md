# 13 · Deployment

This page covers running the `slater` server: how it is configured, how to run it
in a container, and how it reloads generations, handles TLS, and reports what it
is doing.

## The server binary

`slater` is a Bolt server. It listens on a single TCP port (default **7687**),
speaks the Bolt protocol, and answers queries against the generations it finds
under its data directory. **It takes no command-line flags** — everything is
configuration. The binary also carries a few stdlib-only subcommands that run
*without* starting the server: `slater hash-password`, `slater healthcheck`,
`slater diagnostics`, `slater query`, and `slater dump`.

A normal startup looks like this:

```
INFO starting slater (Bolt graph engine) version="0.24.1" bind=0.0.0.0 port=7687 data_dir=/tmp/slater-data tls=false writable=true
INFO opened generation graph="social" generation=7c6560e5-… nodes=12 edges=17 labels=3 reltypes=4 range_indexes=3 vector_indexes=0 segments=0
INFO writable layer enabled wal_dir=wal off_heap_l0=false
INFO slater Bolt listener ready bind=0.0.0.0 port=7687 tls=false graphs=2 poll_ms=5000 reload_strategy=exit max_connections=16384 …
```

## Configuration model

Configuration is **layered**, resolved once at startup:

1. A base `config.json` (in the working directory, or `/app/config.json` in the
   container image).
2. An optional `/sandbox/config.json` overlay.
3. `[SECRET]:` references, resolved from the environment.
4. **Environment-variable overrides** of any field: the config path with `.`
   replaced by `__` (double underscore). `cache.blockCacheBytes` becomes
   `cache__blockCacheBytes`; `dataBackend.fs.dir` becomes `dataBackend__fs__dir`.

That last mechanism is how you configure a container without baking a config file.
The quickstart runs the server entirely from env overrides:

```sh
export dataBackend__fs__dir=/tmp/slater-data
export requireAclStamp=false
export aclPath=/tmp/slater-serve/acl.json
export delta__enabled=true          # turn on the writable layer
slater
```

Every knob, its env-var form, and its default is catalogued in
[14 Configuration reference](14-configuration-reference.md).

## Docker

Two images are provided:

| Image | Contents | Use |
|---|---|---|
| **`Dockerfile`** | `slater` **and** `slater-build`, with the `s3` and `gcs` backends compiled in | Full image: build graphs and serve from any backend. |
| **`Dockerfile.lite`** | `slater` only, filesystem backend only | Minimal, lower-CVE image for serving a pre-built generation from a mounted volume. |

Both build on a `distroless/cc` runtime, run as non-root `USER 1000:1000`, `EXPOSE
7687`, and declare a Bolt `HEALTHCHECK`. The entrypoint is `/app/slater`; run the
offline builder in the full image with `--entrypoint /app/slater-build`.

Configure the container through `KEY__sub` environment variables (or a mounted
`/sandbox/config.json`). The bundled `docker-compose.yml` has four profiles:

- **default** — filesystem backend serving `/data` on `7687`.
- **build** — runs `slater-build` to compile a dump.
- **s3** — a MinIO service plus a slater serving `dataBackend__kind=s3` with a
  local disk cache.
- **gcs** — a fake-gcs-server plus a slater serving `dataBackend__kind=gcs`.

Storage backend selection and credentials are covered in [12 Storage](12-storage.md).

## TLS

TLS is off until you supply both a certificate chain and a private key:

```sh
export tls__cert=/config/server.pem
export tls__key=/config/server.key
```

Leaving either empty disables TLS. The handshake has its own timeout
(`server.tlsHandshakeTimeoutMs`, default 5000 ms).

## Generations and hot-reload

Each graph's live generation is named by its `current` pointer file
(`<graph>/current`). The server runs a background **generation guard** that polls
that pointer every `generationPollMs` (default **5000 ms**) — polling, not
inotify, so it works over network and object storage. When the pointer changes,
`reloadStrategy` decides what happens:

| `reloadStrategy` | Behaviour |
|---|---|
| `exit` (default) | Log and exit non-zero, so an orchestrator restarts the process against the new generation. |
| `swap` | Open and validate the new generation, then atomically swap it in while in-flight queries drain on the old one. A corrupt or incomplete new generation is refused and the old one keeps serving. |

Because the `current` pointer is always written *last*, after a generation is
fully published, a crash mid-publish never exposes a half-written image. See
[12 Storage](12-storage.md).

An optional `cacheWarmingQuery` runs once at boot, before the listener opens, to
pull hot blocks into cache.

## Health checks

The port speaks Bolt, not HTTP, so the health probe is a **Bolt handshake**, not a
`GET`. The container `HEALTHCHECK` runs:

```sh
slater healthcheck [HOST] [PORT]      # exit 0 = healthy, 1 = not
```

## Logging and observability

`log.level` (default `info`) controls verbosity. At `info`, every query emits a
one-line summary after it runs:

```
INFO query executed graph=social cost=0 rows=1 result_cache="miss" exec_ms=0.0 encode_ms=0.0 total_ms=0.0 blk_hits=0 blk_misses=0 blk_hit_ratio=0.00 blk_evicted=0 query=MATCH (n) RETURN count(*) AS nodes
```

`debug` adds wire- and SDK-level tracing. Failed queries log at `warn`.

Log lines emitted inside a request or a build phase are prefixed with that span,
e.g. `bolt.run{otel.kind="server"}: query executed …` or
`slater_build.run:slater_build.phase{otel.name=build.pass1}: ▶ starting pass 1`.
The prefix is there whether or not OpenTelemetry export is enabled.

**There is no HTTP `/metrics` endpoint.** Metrics are *pushed* over OTLP when
OpenTelemetry is enabled (below). For load testing, `CALL slater.diagnostics()`
(or the `slater diagnostics` subcommand) is gated behind
`loadTestDiagnostics=true`; see
[09 Procedures & algorithms](09-procedures-and-algorithms.md) and
[15 Security](15-security.md).

### OpenTelemetry (traces and metrics)

Both `slater` and `slater-build` can export traces and metrics over OTLP/HTTP
(protobuf) to Honeycomb, or to any OpenTelemetry Collector. Export is **off by
default**: until `otel.enabled` is set, no exporter thread is started and nothing
leaves the process.

```json
{
  "otel": {
    "enabled": "true",
    "endpoint": "https://api.honeycomb.io",
    "apiKey": "[SECRET]:/run/secrets/honeycomb-api-key",
    "serviceName": "slater",
    "environment": "preview",
    "sampleRatio": "1.0"
  }
}
```

The same keys work as environment overrides (`otel__enabled=true`,
`otel__endpoint=…`). Use the `[SECRET]:` file indirection for `apiKey`, never an
inline key. To send to a local Collector that holds the key itself, set
`"endpoint": "http://collector:4318"` and `"apiKeyRequired": "false"`. Every key
is listed in [14 Configuration reference](14-configuration-reference.md#otel).

**What a trace looks like.** Each Bolt `RUN` is one trace, and so is each
authenticating `HELLO`/`LOGON`. A connection is *not* a span: driver pools hold
connections open for hours, which would bury every request in one huge trace.

```
bolt.run                  server span: db.namespace, db.operation.name, db.query.text,
│                                      enduser.id, client.address, error.type
├─ slater.parse           parse, on the blocking pool (slater.parse.ok)
├─ slater.execute         result-cache hit, cost, rows, block-cache hits/misses
├─ slater.encode          PackStream encoding of the result
├─ slater.write           a write statement: WAL append + fsync + memtable apply
└─ slater.consolidate     CALL slater.consolidate() (or an automatic one, as its own root)
   └─ slater.builder      the slater-build child process (process.exit.code)
      └─ slater_build.run                ← exported by the child, same trace
         ├─ build.ingest / build.pass1 / build.resolve / build.cluster
         ├─ build.emit.topology / build.emit.node_stores / …
         └─ build.publish
bolt.logon                └─ slater.auth.verify (argon2id)
slater.generation.swap    a reloadStrategy=swap generation swap (background root)
slater.delta.flush / slater.delta.compact   writable-layer maintenance
```

A consolidation is **one trace across two processes**. The server passes its
trace context to the `slater-build` child in the `TRACEPARENT` environment
variable, and the child reads the same `otel` block (it inherits the server's
working directory and environment) and reports as `<serviceName>-build`.

**Errors.** Span status is set to ERROR only for server failures, which covers
every execution failure: budget trips, timeouts, a task that died. Bad input
(wrong credentials, a missing grant, an unknown graph, a syntax error, a write on
a read-only connection) is recorded as `error.type` but leaves the status unset,
so the error rate counts what the server got wrong.

**Metrics.** Pushed every `metricsIntervalSecs` (default 60):

| Metric | Unit | Attributes |
|---|---|---|
| `slater.request.duration` | s | `db.namespace`, `db.operation.name`, `outcome` (`ok`/`client_error`/`error`) |
| `slater.block_cache.accesses` | `{access}` | `db.namespace`, `result` (`hit`/`miss`) |
| `slater.result_cache.lookups` | `{lookup}` | `db.namespace`, `result` |
| `slater.consolidation.duration` | s | `db.namespace`, `outcome` |

The process and container CPU, memory, disk I/O and pressure-stall gauges
(`processMetrics`) come with the exporter, and are reported by the build too:
a long consolidation shows its resource curve in the build's own service. Metric
attributes are bounded: graphs are the served set, operations a fixed list.

**What is exported.** Query text is exported in the same one-line form, truncated
to 160 characters, that the `query executed` log line already prints. Query
**parameters are never recorded as attributes**. A failed execution's error message
(truncated to 512 characters) becomes the span status, and like the `query failed`
log line it can quote a value the query touched, parameters included. Spans also
carry the authenticated user (`enduser.id`, recorded only once a `LOGON` succeeds)
and the client address. See [15 Security](15-security.md).

**Shutdown.** With export enabled, `SIGTERM`/`SIGINT` flush queued spans and
metrics before the process exits (with the signal's usual status, 143 or 130). The
last seconds before a stop are usually the ones you want. With export off, a
signal behaves exactly as before.

## Next

- Configure every knob: [14 Configuration reference](14-configuration-reference.md).
- Lock it down: [15 Security](15-security.md).
- Tune it: [16 Performance tuning](16-performance-tuning.md).
- Storage backends: [12 Storage](12-storage.md).
