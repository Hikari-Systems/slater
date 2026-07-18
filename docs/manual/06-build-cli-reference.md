# 06 · Build CLI reference

Every `slater-build` flag, its default, and what it does. For the narrative — dump
formats, build-time expressions, publishing — see
[05 Building graphs](05-building-graphs.md). `slater-build` is configured entirely
by these flags (it takes no config file).

## Required inputs

| Flag | Default | Purpose |
|---|---|---|
| `--input <PATH>` | — (required) | Dump script path, or `-` for stdin. |
| `--graph <NAME>` | — (required) | Logical graph name; selects `<data-dir>/<graph>/`. |
| `--data-dir <PATH>` | — (required) | Root data directory the generation is written under. |

## Identity and input format

| Flag | Default | Purpose |
|---|---|---|
| `--pk <FIELD>` | *(off → business-key MERGE mode)* | Use `<FIELD>` as a single global integer node identity across the dump; `--pk __dump_id__` ingests legacy FalkorDB `GRAPH.DUMP`. |
| `--input-format <cypher\|slater-dump>` | `cypher` | `cypher` parses a dump script; `slater-dump` ingests a binary consolidation dump. `--pk` may not be combined with `slater-dump`. |

## Block sizes

| Flag | Default | Purpose |
|---|---|---|
| `--block-size <BYTES>` | `262144` (256 KiB) | Target block size for property/label/topology files. |
| `--range-block-size <BYTES>` | `16384` (16 KiB) | Leaf-block size for range (ISAM) indexes; smaller by design. |
| `--vector-block-size <BYTES>` | `262144` (256 KiB) | Block size for the vector store. |

## Compression

| Flag | Default | Purpose |
|---|---|---|
| `--compression-profile <auto\|local\|remote\|max>` | `auto` | Backend-aware zstd profile: `local`=level 9, `remote`=19, `max`=22. `auto` picks `remote` when publishing to an object store, else `local`. |
| `--zstd-level <N>` | *(profile-derived)* | Explicit zstd level for all published files; overrides the profile. |
| `--degree-zstd-margin <F>` | *(profile: local 0.5, remote/max 1.0)* | Degree-column zstd-vs-Elias–Fano selection penalty; a chunk uses zstd only when its size ≤ margin × the decompress-free size. |

## Histograms and hub degrees

| Flag | Default | Purpose |
|---|---|---|
| `--histogram-max-distinct <N>` | *(format default)* | Cap on a per-(label,property) value→count histogram's distinct keys; `0` disables histograms. |
| `--hub-degree-floor <N>` | *(format default)* | Degree at/above which a node is recorded in the hub-degree sidecar; `0` records every node. |

## Vector / ANN index knobs

| Flag | Default | Purpose |
|---|---|---|
| `--vector-index-json <PATH>` | *(none)* | JSON sidecar declaring vector indexes (`[{label, property, dim, metric}]`). |
| `--ann-threshold <N>` | `50000` | Indexes with ≥ N vectors build as disk-native Vamana + PQ; smaller stay exact brute-force. |
| `--vamana-r <N>` | `32` | Vamana graph out-degree bound `R`. |
| `--vamana-alpha <F>` | `1.2` | Vamana robust-prune long-edge factor `alpha`. |
| `--pq-subspaces <N>` | `16` | Product-quantisation subspace count `m` (must divide the dimension). |
| `--pq-bits <N>` | `8` | PQ bits per subspace (`k = 2^bits`, 1–8). |

> There is **no `--vector-spec` flag** — the flag to declare vector indexes from a
> file is `--vector-index-json`.

## Encryption and ACL

| Flag | Default | Purpose |
|---|---|---|
| `--encrypt` | off | Encrypt every data block at rest (XChaCha20-Poly1305); requires exactly one of `--key-file` / `--key-env`. |
| `--key-file <PATH>` | *(none)* | File holding the at-rest master key as hex. |
| `--key-env <VAR>` | *(none)* | Environment variable holding the at-rest master key as hex. |
| `--acl <PATH>` | *(none)* | Path to `acl.json`; its BLAKE3 digest is stamped into the manifest (`aclBlake3`). See [15 Security](15-security.md). |

## Memory, scratch, and clustering

| Flag | Default | Purpose |
|---|---|---|
| `--max-memory <SIZE>` | `4g` | Working-memory budget for the external build (accepts `k`/`m`/`g` suffixes); the build aborts rather than exceed it. |
| `--temp-dir <PATH>` | *(scratch under graph dir)* | Scratch directory for spill files. |
| `--cluster <ldg\|none>` | `ldg` | Node-id reordering for on-disk locality; `ldg` clusters graph-proximate nodes, `none` keeps dump order. |
| `--cluster-passes <N>` | `3` | LDG refinement passes when `--cluster=ldg`. |
| `--keep-temp` | off | Keep the scratch directory after a successful build (debugging). |
| `--resume` | off | Resume an interrupted build from surviving scratch (`BUILD-STATE.json`). |

## Threads, logging, diagnostics

| Flag | Default | Purpose |
|---|---|---|
| `--threads <N>` / `-j` | `max(cores − 2, 1)` | Worker-thread cap for parallel stages and the spill pool. |
| `--quiet` / `-q` | off | Suppress the progress log (errors still surface). |
| `--diagnostics` | off | Sample resource counters to a JSONL trace (also enabled by `SLATER_BUILD_DIAG=1`). |
| `--diagnostics-log <PATH>` | *(under data-dir)* | Where to write the diagnostics JSONL. |
| `--diagnostics-interval-ms <N>` | `1000` | Diagnostics sampling interval. |
| `--object-checksums` | off (auto when publishing) | Also record each file's SHA-256 and CRC32C in the manifest. |

## Remote publish — S3 (requires the `s3` cargo feature)

| Flag | Default | Purpose |
|---|---|---|
| `--publish-s3-bucket <NAME>` | — | Publish the finished generation to this S3 bucket. |
| `--publish-s3-region <REGION>` | `""` | Bucket region. |
| `--publish-s3-endpoint <URL>` | `""` | Custom S3-compatible endpoint (MinIO / localstack). |
| `--publish-s3-prefix <PREFIX>` | `""` | Key prefix. |
| `--publish-s3-path-style` | off | Path-style addressing (needed by MinIO). |

## Remote publish — GCS (requires the `gcs` cargo feature; mutually exclusive with S3)

| Flag | Default | Purpose |
|---|---|---|
| `--publish-gcs-bucket <NAME>` | — | Publish to this GCS bucket (no `gs://` scheme). |
| `--publish-gcs-prefix <PREFIX>` | `""` | Key prefix. |
| `--publish-gcs-credentials <PATH>` | `""` | Service-account JSON path (empty ⇒ Application Default Credentials). |
| `--publish-gcs-endpoint <URL>` | `""` | Emulator endpoint (fake-gcs-server). |
| `--publish-gcs-anonymous` | off | Anonymous access (emulator only). |

## Environment variables

| Variable | Purpose |
|---|---|
| `SLATER_BUILD_DIAG=1` | Enable diagnostics (same as `--diagnostics`). |
| `SLATER_BUILD_FAIL_AFTER=<phase>` | Test-only: crash after a named phase (for exercising `--resume`). |
| `SLATER_SHARD_BYTES=<N>` | Pass-1 shard target size. |

## Cargo features

| Feature | Effect |
|---|---|
| `s3` | Enables the `--publish-s3-*` flags and the S3 backend. |
| `gcs` | Enables the `--publish-gcs-*` flags and the GCS backend. |
| `profiling` | Swaps in the dhat heap profiler in place of jemalloc. |

## Next

- The narrative build guide: [05 Building graphs](05-building-graphs.md).
- Storage layout and codecs the flags shape: [12 Storage](12-storage.md).
