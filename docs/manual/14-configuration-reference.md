# 14 · Configuration reference

Every configuration field, its environment-variable form, and its default. Keys
are camelCase JSON paths; the env-var form replaces each `.` with `__` (double
underscore), so `cache.blockCacheBytes` is set with `cache__blockCacheBytes`. The
server reads config once at startup ([13 Deployment](13-deployment.md) explains
the layering); it takes no CLI flags.

## Top-level

| Key | Env | Default | Purpose |
|---|---|---|---|
| `aclPath` | `aclPath` | `/config/acl.json` | Path to the ACL file (users + grants). |
| `requireAclStamp` | `requireAclStamp` | `true` | Refuse any generation whose manifest lacks an `aclBlake3` stamp. |
| `loadTestDiagnostics` | `loadTestDiagnostics` | `false` | Maintain extra counters and answer `CALL slater.diagnostics()`. |
| `generationPollMs` | `generationPollMs` | `5000` | How often the guard polls each graph's `current` pointer. |
| `reloadStrategy` | `reloadStrategy` | `exit` | On a new generation: `exit` (restart) or `swap` (atomic in-place). |
| `defaultGraph` | `defaultGraph` | (empty) | Display metadata only; never auto-selects a graph. |
| `cacheWarmingQuery` | `cacheWarmingQuery` | (empty) | A query run once at boot to warm the cache. |
| `vectorIndexPins` | — | `[]` | Array of `{label, property}` vector indexes to pin resident. |

## `server.*`

| Key | Default | Purpose |
|---|---|---|
| `server.bind` | `0.0.0.0` | Listen address. |
| `server.port` | `7687` | Bolt port. |
| `server.maxMessageBytes` | `67108864` (64 MiB) | Largest reassembled authenticated Bolt message. |
| `server.maxPreAuthBytes` | `65536` (64 KiB) | Largest pre-`LOGON` message. |
| `server.loginTimeoutMs` | `10000` | Deadline to authenticate (`0` = none). |
| `server.tlsHandshakeTimeoutMs` | `5000` | TLS handshake deadline (`0` = none). |
| `server.idleTimeoutMs` | `0` (off) | Idle-connection deadline. |
| `server.maxConnections` | `16384` | Global connection cap (reserved before `accept()`). |
| `server.maxPreAuthConnections` | `4096` | Cap on un-authenticated connections (`0` = unlimited; must be `< maxConnections`). |
| `server.maxConnectionsPerIp` | `1024` | Per-source cap (`/32` IPv4, `/64` IPv6; `0` = unlimited). |
| `server.maxBlockingThreads` | `0` | Tokio blocking-pool cap (`0` = tokio default 512). |
| `server.maxConcurrentAuth` | `4` | Concurrent argon2id verifications. |
| `server.maxAuthFailures` | `3` | Auth failures per connection before drop. |
| `server.maxConcurrentWrites` | `4` | Concurrent write statements. |

## `log.*` / `tls.*`

| Key | Default | Purpose |
|---|---|---|
| `log.level` | `info` | Log level (`info` emits the per-query summary; `debug` adds wire tracing). |
| `tls.cert` | (empty) | PEM certificate chain; empty disables TLS. |
| `tls.key` | (empty) | PEM private key; empty disables TLS. |

## `dataBackend.*`

| Key | Default | Purpose |
|---|---|---|
| `dataBackend.kind` | `fs` | Storage backend: `fs`, `s3`, or `gcs`. |
| `dataBackend.verifyIntegrity` | `true` | Verify each generation file against the manifest at open. |

### `dataBackend.fs.*`

| Key | Default | Purpose |
|---|---|---|
| `dataBackend.fs.dir` | `/data` | Root holding `<graph>/<generation>/` images and `current` pointers. |

### `dataBackend.s3.*` (requires the `s3` build feature)

| Key | Default | Purpose |
|---|---|---|
| `dataBackend.s3.bucket` | — | Bucket name. |
| `dataBackend.s3.region` | (empty) | Region, e.g. `eu-west-2`. |
| `dataBackend.s3.endpoint` | (empty) | Custom S3-compatible endpoint (MinIO/localstack). |
| `dataBackend.s3.prefix` | (empty) | Key prefix. |
| `dataBackend.s3.pathStyle` | `false` | Path-style addressing (needed by MinIO). |
| `dataBackend.s3.awsAccessKey` | (empty) | Access key; empty falls back to the AWS credential chain. |
| `dataBackend.s3.awsSecretKey` | (empty) | Secret key. |
| `dataBackend.s3.awsSessionToken` | (empty) | STS session token. |
| `dataBackend.s3.diskCacheBytes` | `0` | Local disk L2 cache budget (`0` disables). |
| `dataBackend.s3.diskCacheDir` | (empty) | Disk-cache directory (required if bytes > 0; must not be tmpfs). |

### `dataBackend.gcs.*` (requires the `gcs` build feature)

| Key | Default | Purpose |
|---|---|---|
| `dataBackend.gcs.bucket` | — | Bucket name (no `gs://`). |
| `dataBackend.gcs.prefix` | (empty) | Key prefix. |
| `dataBackend.gcs.endpoint` | (empty) | Emulator endpoint (fake-gcs-server). |
| `dataBackend.gcs.credentialsPath` | (empty) | Service-account JSON path; empty ⇒ Application Default Credentials. |
| `dataBackend.gcs.credentialsJson` | (empty) | Inline service-account JSON (precedence over path). |
| `dataBackend.gcs.anonymous` | `false` | Emulator-only; overrides all credentials. |
| `dataBackend.gcs.diskCacheBytes` | `0` | Local disk L2 cache budget (`0` disables). |
| `dataBackend.gcs.diskCacheDir` | (empty) | Disk-cache directory (required if bytes > 0; never tmpfs). |

## `cache.*`

| Key | Default | Purpose |
|---|---|---|
| `cache.blockCacheBytes` | `67108864` (64 MiB) | Resident block-cache pool. |
| `cache.vectorCacheBytes` | `67108864` (64 MiB) | Vamana blocks + PQ codes pool. |
| `cache.resultCacheBytes` | `16777216` (16 MiB) | Result cache (`≤0` disables). |
| `cache.rangeIndexCacheBytes` | `16777216` (16 MiB) | Range-index cache (`0` disables). |
| `cache.cacheTtlMs` | `1800000` (30 min) | Idle-eviction sweep interval (`≤0` disables). |
| `cache.degreeColumn` | `lazy` | Degree-column residency: `lazy` (fault + evict) or `pinned` (prefault all). |
| `cache.degreeColumnBytes` | `268435456` (256 MiB) | Soft cap under `lazy` (`0` = uncapped; ignored under `pinned`). |

## `query.*`

| Key | Default | Purpose |
|---|---|---|
| `query.maxRows` | `100000` | Max rows returned per query. |
| `query.timeoutMs` | `30000` | Per-query wall-clock timeout. |
| `query.maxIntermediate` | `1000000` | Per-query cap on retained intermediate elements (`0` disables). |
| `query.maxScan` | `500000000` | Per-query transient walk-work budget (`0` disables). |
| `query.maxIntermediateGlobal` | `8000000` | Server-wide sum of retained intermediates (`0` disables). |
| `query.maxShortestPathExplore` | `0` (unlimited) | Cap on nodes one `shortestPath()` BFS may discover. |
| `query.maxFanout` | `1` | Per-query parallelism worker cap (`1` = sequential). |
| `query.adjStreamThreshold` | `8192` | Degree at/above which adjacency is streamed in chunks. |
| `query.adjStreamChunk` | `8192` | Edges per streamed chunk. |

## `vectorQuery.*`

| Key | Default | Purpose |
|---|---|---|
| `vectorQuery.beamWidth` | `64` | Beam-search list size `L` for the base/segment ANN. |
| `vectorQuery.tempBeamWidth` | `128` | Wider beam for per-segment temp indexes. |
| `vectorQuery.maxHops` | `256` | Beam-search hop cap. |
| `vectorQuery.rwIndex.enabled` | `true` | Live mutable Vamana over the write delta (kill switch → brute-force). |
| `vectorQuery.rwIndex.minVectors` | `2000` | Floor below which the delta arm stays brute-force. |
| `vectorQuery.rwIndex.maxVectors` | `50000` | Ceiling above which the delta refuses an in-memory index. |

## `encryption.*`

| Key | Default | Purpose |
|---|---|---|
| `encryption.keyEnv` | (empty) | Env var holding the at-rest master key (hex); empty ⇒ unencrypted. |
| `encryption.keyFile` | (empty) | File holding the key (hex); precedence over `keyEnv`; must live outside the data dir. |

## `delta.*` (the writable layer)

| Key | Default | Purpose |
|---|---|---|
| `delta.enabled` | `false` | Master switch for the writable layer. |
| `delta.walDir` | `wal` | Per-graph WAL directory (resolved under the data dir). |
| `delta.memtableBytes` | `67108864` (64 MiB) | In-memory delta size before flush. |
| `delta.l0CompactionTrigger` | `4` | L0 segments before compaction (`0` disables). |
| `delta.segmentFlushBytes` | `0` (off) | Byte threshold to seal a segment. |
| `delta.maxUpperSegments` | `8` | Stacked segments before forced compaction (`0` disables). |
| `delta.deltaCorePercent` | `0` (off) | Auto-consolidate when the delta reaches this % of core. |
| `delta.deltaHardBytes` | `0` (off) | Resident-delta throttle. |
| `delta.consolidateWindow` | (empty) | Cron-style off-peak consolidation window. |
| `delta.builderBin` | `slater-build` | Builder binary for consolidation — must resolve on `PATH` or be an absolute path. |
| `delta.offHeapL0` | `false` | Keep L0 off-heap. |
| `delta.segmentGcGraceSecs` | `0` (off) | Grace period before GCing superseded segments. |

> **Consolidation gotcha:** `CALL slater.consolidate()` spawns `delta.builderBin`.
> If it is left as the bare name `slater-build` and that is not on `PATH`,
> consolidation fails with `spawn builder 'slater-build': No such file or
> directory`. Set it to an absolute path in most deployments. See
> [18 Troubleshooting](18-troubleshooting.md).

## Next

- Apply these safely: [15 Security](15-security.md).
- Tune the performance-related knobs: [16 Performance tuning](16-performance-tuning.md).
