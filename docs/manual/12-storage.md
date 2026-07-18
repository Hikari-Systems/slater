# 12 · Storage

Storage in Slater has two faces: **where the bytes live** (the backend — a local
filesystem, S3, or GCS) and **how the bytes are laid out** (the on-disk engine —
generations, blocks, indexes, and the codecs a build chooses). This page covers
the choices you make about each. The byte format is identical across all
backends; only the source of the bytes differs.

---

## Part 1 · Storage backends

Select the backend with `dataBackend.kind` — `fs` (default), `s3`, or `gcs`. The
`s3` and `gcs` backends require the corresponding build feature to be compiled in
(they are, in the default full image; see [13 Deployment](13-deployment.md)).

### Filesystem (`fs`)

```json
{"dataBackend": {"kind": "fs", "fs": {"dir": "/data"}}}
```

`dataBackend.fs.dir` is the root that holds `<graph>/<generation>/` images and the
`current` pointers. This is the simplest backend and the one the sample graphs
use.

### S3 (`s3`)

```json
{"dataBackend": {"kind": "s3", "s3": {
  "bucket": "my-graphs", "region": "eu-west-2", "prefix": "prod/",
  "diskCacheBytes": 1073741824, "diskCacheDir": "/var/cache/slater"}}}
```

| Field | Purpose |
|---|---|
| `bucket`, `region`, `prefix` | Object location |
| `endpoint`, `pathStyle` | For S3-compatible stores (MinIO, localstack); `pathStyle` is usually required for MinIO |
| `awsAccessKey`, `awsSecretKey`, `awsSessionToken` | Explicit credentials |
| `diskCacheBytes`, `diskCacheDir` | Local L2 cache (below) |

If the explicit keys are empty, Slater falls back to the standard AWS credential
chain (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, a shared profile, or an
instance role).

### GCS (`gcs`)

```json
{"dataBackend": {"kind": "gcs", "gcs": {
  "bucket": "my-graphs", "credentialsPath": "/etc/slater/sa.json"}}}
```

| Field | Purpose |
|---|---|
| `bucket`, `prefix` | Object location (no `gs://` scheme) |
| `endpoint` | For an emulator (fake-gcs-server) |
| `credentialsPath` / `credentialsJson` | Service-account JSON (path, or inline; inline wins). Empty ⇒ Application Default Credentials |
| `anonymous` | Emulator only — overrides all credentials |
| `diskCacheBytes`, `diskCacheDir` | Local L2 cache (below) |

### The disk-cache tier

For the object-store backends, a local-SSD **L2 cache** serves a block that has
fallen out of the in-memory cache from local disk (~0.1 ms) instead of issuing a
fresh GET. Enable it by setting `diskCacheBytes > 0` and a `diskCacheDir`.

- The directory **must be real disk, not tmpfs** — tmpfs would double-count the
  bytes against RAM, defeating the purpose.
- The cache index costs a little RAM against your memory ceiling.

### Integrity verification

`dataBackend.verifyIntegrity` (default **on**) checks each generation file against
the manifest when it is opened — a guard against a partial copy or a corrupted
object. On `fs` it re-hashes bytes with BLAKE3; on S3/GCS it reads the
server-computed SHA-256 object checksum with a single HEAD (no body download).

---

## Part 2 · The on-disk engine

### Layout

A graph directory contains immutable, content-hashed generations plus the pointers
that select which one is live:

```
<data-dir>/<graph>/
  current                     # text file naming the live set/generation
  <generation-uuid>/          # an immutable base generation
    MANIFEST.json
    node_props.blk  edge_props.blk  node_labels.blk
    topology.csr.blk  node_degrees.blk  hub_degrees.blk
    reltype_src.post  reltype_tgt.post  prop_hist.blk
    <range-index files>  <label>.<prop>.vamana  <label>.<prop>.pq
  sets/<uuid>.json            # base + stacked segments (writable layer)
  segments/<uuid>/            # sealed delta segments (writable layer)
```

The **`current` pointer is written last**, after a generation is fully published
and fsynced, so a crash mid-publish never exposes a half-written image. The server
polls it and hot-reloads when it changes ([13 Deployment](13-deployment.md)).

### The manifest

`MANIFEST.json` is written last by the builder and validated first by the reader.
Fields worth knowing:

- `formatVersion` — currently **8**. Slater has **no backwards compatibility**: a
  reader refuses any generation whose format it does not understand, with a "must
  be rebuilt" message, rather than mis-reading it.
- `contentHash` — a BLAKE3 hash over the file inventory; the generation's identity.
- `files` — the inventory, each with a per-file SHA-256 (used by integrity checks).
- `aclBlake3` — the ACL stamp, present when built with `--acl`; a stamp-requiring
  server refuses an unstamped generation ([15 Security](15-security.md)).
- `mac` — a keyed-BLAKE3 MAC over the manifest, present when at-rest encryption is
  configured; a keyed server refuses a MAC-less generation.

### Sets, segments, and the delta

With the writable layer on, a **set manifest** stacks the base generation plus any
sealed **segments**, and an in-memory **delta** (backed by the WAL) sits on top.
Reads merge all three. `CALL slater.consolidate()` collapses the stack back into a
single base ([11 Writing data](11-writing-data.md)).

### Encoding choices a build makes

The builder shapes the on-disk bytes; the knobs below trade build cost and file
size against query IO. Full flag details are in
[06 Build CLI reference](06-build-cli-reference.md).

| Choice | Flag | Tradeoff |
|---|---|---|
| Compression profile | `--compression-profile` (`local` z9 / `remote` z19 / `max` z22), or `--zstd-level` | Higher levels shrink files (good for object stores) at more build CPU |
| Block sizes | `--block-size`, `--range-block-size`, `--vector-block-size` | Larger blocks compress better; smaller blocks reduce read amplification |
| Clustering | `--cluster ldg\|none`, `--cluster-passes` | LDG reorders nodes so graph-neighbours are near on disk, cutting random reads; costs build time |
| Degree column | `--degree-zstd-margin` | Per-chunk Elias-Fano vs zstd; the margin sets when zstd's smaller size is worth the decode |
| Hub degrees | `--hub-degree-floor` | Degree at which a node's exact degree is recorded in the always-resident sidecar |
| Histograms | `--histogram-max-distinct` | Per-(label,property) value histograms for planning; `0` disables |

At serve time, degree-column residency is a runtime choice too:
`cache.degreeColumn` = `lazy` (fault chunks on touch, evict cold ones — the
default) or `pinned` (prefault the whole column, never evict). See
[16 Performance tuning](16-performance-tuning.md).

### What the blocks hold

- `node_props.blk` / `edge_props.blk` — properties, in **plane/codec** block
  containers (per-column encoding chosen for size).
- `node_labels.blk` — per-node label sets (a bitmask for ≤ 64 labels, else varint).
- `topology.csr.blk` — forward and reverse CSR adjacency.
- `node_degrees.blk` — the dense degree column; `hub_degrees.blk` — the mega-hub
  sidecar.
- `reltype_src.post` / `reltype_tgt.post` — per-relationship-type endpoint
  postings.
- Range (ISAM) index files — sorted business-key indexes.
- `<label>.<prop>.vamana` + `.pq` — the vector ANN graph and PQ codes
  ([10 Vector search](10-vector-search.md)).

## Next

- Run and reload generations: [13 Deployment](13-deployment.md).
- Every storage and cache knob: [14 Configuration reference](14-configuration-reference.md).
- Tune residency and caches for your memory budget: [16 Performance tuning](16-performance-tuning.md).
