# Plan: S3 local-disk block cache (second cache tier)

Branch: `feat/s3-storage-backend` (builds on the pluggable storage backend +
SHA-256 integrity already committed there as `84e363a`).

## Context & goal

The S3 backend serves generation blocks over the network: each cold block is an
HTTP range GET (~10–50 ms RTT). The in-memory `BlockCache` is small (bounded-RSS
is the headline guarantee, default 256 MiB), so on any workload whose working set
exceeds RAM the same blocks are re-fetched from S3 repeatedly on every spill.

Add an **optional local-SSD second cache tier** for the S3 backend so a cold-from-
RAM block is served from local disk (~0.1 ms) instead of S3, and survives in-memory
eviction. Goal: make an S3-backed node perform close to a local-filesystem node
once warm, and cut S3 request count/cost.

## Design decisions (locked — see rationale below)

1. **Inclusive read-through of the *sealed* S3 bytes, NOT a victim cache.**
   The cache stores exactly the bytes `read_exact_at` returns from S3 — the
   already-compressed, already-AEAD-sealed object bytes — keyed below the reader.
   - **No re-encryption, ever.** Encrypted generations land on disk still sealed
     (matches S3); plaintext generations land plaintext (also matches S3). At-rest
     status is preserved for free, with no new key/nonce handling.
   - A victim cache (write-on-RAM-eviction) was rejected: the RAM `BlockCache`
     holds *decompressed plaintext*, so spilling it would force a re-encrypt on
     every eviction to keep the at-rest claim — the undesirable side effect we
     explicitly avoid.

2. **Duplication is accepted and is negligible.** Inclusive means a hot block is
   in both RAM and disk. The overlap is bounded by the *RAM* budget (only resident
   blocks are duplicated), which is a rounding error against a disk cache that is
   meant to be ≫ RAM (e.g. 256 MiB duplicated on a 100 GiB cache = 0.25%). Going
   exclusive would reclaim only that sliver at the cost of per-spill re-encryption.

3. **No new crypto/CPU on the hot path.** Decrypt+decompress already happen on
   every RAM-miss fill today (on the S3 read path). The disk cache only swaps the
   *fetch source* (slow S3 GET → fast local read); the decrypt/decompress stay
   exactly where they are. RAM hits remain free (plaintext).

4. **Write-behind on a background thread, after the response is served.** A disk
   miss returns the S3 bytes to the caller immediately, then enqueues them to a
   **bounded** channel; a background writer does the disk write + LRU trim. The
   query thread never does disk I/O. Bounded channel → drops under pressure rather
   than stalling queries (a dropped write just re-fetches later).

5. **Disk eviction is owned by the background writer**, which trims its own LRU to
   the byte budget on each insert (deletes coldest files). No reaper in the normal
   path; an optional periodic sweep only reclaims crash-orphans / stale
   old-generation files after a swap.

6. **Self-heal via a per-file checksum** (CRC32C in the cache-file header), verified
   on every read; mismatch → evict the entry and return a miss (→ S3 refetch). This
   covers both encrypted and plaintext generations and is key-free (the cache layer
   never holds the generation cipher). Without it, a corrupt sealed block would only
   fail later at decrypt (encrypted) or be served as garbage (plaintext).

7. **Opt-in.** Enabled iff `dataBackend.s3.diskCacheBytes > 0`; default off. Must
   point at a **real writable volume — never tmpfs** (tmpfs is RAM and would break
   bounded-RSS).

## Architecture

The cache is a decorator at the `RandomReadAt` / byte-range level, wrapping the S3
`ObjectStore`. It is invisible to `BlockFileReader`, the `BlockCache`, and the
executor.

```
executor → BlockCache (RAM, decompressed plaintext)        ← unchanged
              └─ miss → BlockFileReader.read_block
                          └─ decrypt+decompress             ← unchanged (above cache)
                              └─ src.read_exact_at  ────────┐
                                                            ▼
                              CachingRandomReadAt (NEW): check disk cache
                                ├─ hit  → return sealed bytes from local SSD
                                └─ miss → S3 range GET → return → write-behind to disk
```

Keying: `(object_key, offset, len)`. The object key is `<graph>/<uuid>/<file>`, so
the key already embeds the generation UUID → a generation swap orphans old entries
(they age out via LRU; the optional sweep reclaims them eagerly). Block reads are
always at a stable `(offset, comp_len)` from the block directory, so the key is
stable per block.

### Data flow
- **read_exact_at(buf, offset):** look up `(key, offset, buf.len())` → on hit read
  the cache file (off-lock), verify CRC32C, copy into `buf`. On miss:
  `inner.read_exact_at` (S3), copy into `buf`, then `cache.put_async(key, offset,
  bytes)` (non-blocking enqueue).
- **read_ranges(ranges):** per-range cache lookup; misses fetched from
  `inner.read_ranges` (already concurrent); write-behind the freshly-fetched
  ranges; reassemble in request order.
- **Background writer:** drains the channel; for each write: temp file → fsync →
  atomic rename → insert into index → while `total_bytes > budget` evict LRU tail
  (remove index entry + delete file). Also handles evict/delete requests from
  self-heal.

## Components & cache-file format

New module `crates/graph-format/src/store/diskcache.rs` (behind the `s3` feature):

- `pub struct CachingObjectStore { inner: Arc<dyn ObjectStore>, cache: Arc<DiskCache> }`
  - `open(key)` → `inner.open(key)?` wrapped in `CachingRandomReadAt { key, inner, cache }`.
  - `read_all`/`list`/`exists`/`put`/`verify_file` delegate to `inner` (cold paths;
    `verify_file` stays the metadata HEAD — not cached).
- `struct CachingRandomReadAt { key: String, inner: Arc<dyn RandomReadAt>, cache: Arc<DiskCache> }`
- `struct DiskCache { dir, budget_bytes, index: Mutex<Lru>, writer_tx: SyncSender<Req>, .. }`
  - `get(key, offset, len) -> Option<Vec<u8>>` — lock to look up + bump recency,
    read file off-lock, CRC verify, evict-on-failure.
  - `put_async(key, offset, bytes)` — `try_send` to the bounded channel; drop if full.
  - `flush()` (test hook) — drain the channel and wait, so tests can assert a write
    landed.
  - background writer thread: owns all disk mutations (writes, LRU evictions,
    self-heal deletes).

Cache-file naming: `blake3(key‖offset‖len)` hex, sharded by first 2 hex chars
(`<dir>/ab/abcd…`). Flat-per-shard avoids huge directories.

Cache-file format (self-describing, enables optional restart rebuild):
```
magic(4) ‖ version(1) ‖ key_len(u16) ‖ key bytes ‖ offset(u64) ‖ len(u32)
        ‖ crc32c(u32 of payload) ‖ payload (the sealed S3 bytes)
```

## Config & wiring

`crates/slater/src/config.rs` — add to `S3BackendConfig`:
- `disk_cache_bytes: usize` (`diskCacheBytes`, default 0 = disabled, `de::usize`).
- `disk_cache_dir: String` (`diskCacheDir`, default empty; required when bytes > 0).

`crates/slater/src/server.rs` `build_store`: when `kind == "s3"` and
`disk_cache_bytes > 0`, construct `DiskCache::open(dir, bytes)?` and wrap:
`Arc::new(CachingObjectStore::new(s3_store, cache))`. Otherwise return the bare S3
store. (The wrapper is generic over any `ObjectStore`, but only wired for S3.)

## Ops

`docker-compose.yml` `slater-s3` service:
- Add a **named volume** (or host bind) mounted at e.g. `/var/cache/slater`
  (writable; the read-only rootfs already allows specific writable mounts —
  **do not** put it under the tmpfs `/tmp`).
- Env: `dataBackend__s3__diskCacheBytes` (e.g. `10737418240` = 10 GiB),
  `dataBackend__s3__diskCacheDir: /var/cache/slater`.

`Dockerfile`: no build change needed; the dir is a runtime mount.

**RSS note:** the in-memory LRU index costs RAM proportional to entry count
(~tens of bytes/entry → ~25 MiB for a 100 GiB cache of 256 KiB blocks). Bounded,
but it must be counted against the configured RSS ceiling.

## Concurrency & correctness invariants

- Cache layer is **key-free** — it never decrypts; it stores/returns S3's exact
  sealed bytes. Decrypt/decompress happen above it, unchanged.
- Index is a `Mutex<Lru>` (shard later if contended); file reads happen off-lock.
- Concurrent misses of the same block each fetch + enqueue a write; identical
  content + atomic rename makes duplicate writes idempotent (optional in-flight
  dedup set is a later optimisation).
- Self-heal: CRC mismatch on read → evict + miss → S3 refetch; corruption is never
  served.
- Swap-safe: keys embed the generation UUID; old-gen entries orphan and age out.
- Bounded write-behind channel: never blocks queries; sheds under pressure.

## Phasing

All phases below are **implemented and tested** on `feat/s3-storage-backend`.

1. ✅ **DiskCache core** — struct, file format, `get`/`put_async`/`flush`, background
   writer with LRU trim, CRC self-heal (`crc32fast`; integrity-only, not a MAC).
   Unit tests in a tempdir (no S3): cold-miss/hit round-trip, miss-on-unknown-key,
   budget-eviction, recency-protects-hot-block, corrupt-file → miss + self-heal
   delete, flush ordering.
2. ✅ **CachingObjectStore / CachingRandomReadAt** — `read_exact_at` + `read_ranges`
   + write-behind; delegate the rest. Offline tests: `CachingObjectStore` over a
   **counting** `ObjectStore` wrapper over `MemObjectStore` — read a block twice
   with `flush()` between, assert inner read count == 1 (second served from disk);
   plus a mixed hit/miss `read_ranges` batch asserting only the misses reach inner.
3. ✅ **Config + wiring** — `S3BackendConfig.disk_cache_{bytes,dir}`
   (`diskCacheBytes`/`diskCacheDir`); `build_store` wraps S3 in `CachingObjectStore`
   when `diskCacheBytes > 0`, erroring if `diskCacheDir` is empty.
4. ✅ **Ops** — `docker-compose.yml` `slater-s3`: writable named volume
   `slater-s3-cache` at `/var/cache/slater` + `diskCacheBytes`/`diskCacheDir` env.
5. ✅ **MinIO integration test** (gated, in `tests/s3_minio.rs`):
   `disk_cache_absorbs_warm_reads_over_s3` — disk cache over the real S3 store,
   read a block twice, assert the S3 read count stays at 1 on the warm pass (via a
   wrapping counter store). Validated against a live MinIO.

**Note on the checksum:** the plan named CRC32C; the implementation uses
`crc32fast` (IEEE CRC-32), the dep listed in *Files to touch*. Either is sound for
self-heal (corruption detection only — the sealed bytes carry their own AEAD).

### Not yet done (still the "Out of scope / follow-ups" list)

Persistent index across restart, in-flight dedup set, periodic orphan sweep,
diagnostics counters, and query-time prefetch into the disk tier remain follow-ups.

## Files to touch

- New: `crates/graph-format/src/store/diskcache.rs`
- `crates/graph-format/src/store.rs` — `#[cfg(feature = "s3")] pub mod diskcache;`
- `crates/graph-format/Cargo.toml` — `crc32fast` dep under the `s3` feature
- `crates/slater/src/config.rs` — `S3BackendConfig.disk_cache_{bytes,dir}`
- `crates/slater/src/server.rs` — `build_store` wraps S3 in `CachingObjectStore`
- `docker-compose.yml` — writable cache volume + env on `slater-s3`
- Tests: graph-format unit tests + gated MinIO integration

## Out of scope / follow-ups

- **Persistent index across restart** (rebuild the LRU by scanning cache-file
  headers at startup; recency from mtime). The file format above already carries
  the metadata for this; v1 may start with a cold (empty) in-memory index and add
  rebuild later. This is the main "blocks survive a restart" win — worth doing
  early if restart-warmth matters.
- In-flight dedup set (avoid duplicate concurrent write-behind of the same block).
- Periodic orphan/old-generation sweep.
- Hit/miss/eviction counters surfaced via `CALL slater.diagnostics()`.
- Query-time prefetch into the disk tier at coalescing points (the read-ahead
  primitive already exists).

## Appendix: performance rationale (so it isn't re-litigated)

Per block (256 KiB raw → ~64–128 KiB sealed), typical x86+NVMe / aarch64:
- Cold NVMe block read ≈ 80–120 µs; page-cache-warm ≈ 5–15 µs.
- ChaCha20-Poly1305 decrypt ≈ 40–70 µs (≈ 0.4–0.8× a cold disk read).
- zstd decompress ≈ 100–150 µs (the larger CPU term).
- S3 GET ≈ 10,000–50,000 µs — what the cache eliminates.

So decrypt+decompress (~150–220 µs) is ≤ ~2% of the avoided S3 RTT, and in the
store-sealed design they are **not added cost** (already paid on every RAM-miss
fill). Crypto throughput is governed by **runtime** SIMD dispatch (RustCrypto
`cpufeatures` → AVX2 on Haswell+, NEON on aarch64), independent of the Docker
`-Ctarget-cpu=x86-64-v3` flag (which exists for the vector-kNN dot kernel). zstd is
C (`libzstd`), compiled separately, also independent of that flag. Only a CPU with
no AVX2 running a baseline build drops ChaCha to ~1 GB/s (decrypt ≈ a full disk
read) — still ≪ S3, and x86-64-v3 mandates Haswell so the CI binary never hits it.
```
