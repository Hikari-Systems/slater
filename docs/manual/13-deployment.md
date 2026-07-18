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

**There is no HTTP `/metrics` endpoint.** Operational metrics are exposed only
through `CALL slater.diagnostics()` (or the `slater diagnostics` subcommand),
gated behind `loadTestDiagnostics=true`. See
[09 Procedures & algorithms](09-procedures-and-algorithms.md) and
[15 Security](15-security.md).

## Next

- Configure every knob: [14 Configuration reference](14-configuration-reference.md).
- Lock it down: [15 Security](15-security.md).
- Tune it: [16 Performance tuning](16-performance-tuning.md).
- Storage backends: [12 Storage](12-storage.md).
