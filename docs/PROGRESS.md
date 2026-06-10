# Slater — progress ledger

**Authoritative, on-disk record of where the build is.** A fresh session resumes
from this file + `docs/PLAN.md` alone (see `docs/RESUME.md` for the copy-paste
resume prompt). Update it (and `docs/DECISIONS.md`) at the end of every milestone,
and keep `cargo build` + that milestone's tests green — milestone boundaries are the
only safe context-clear points.

---

## NEXT ACTION

> **Milestone 9 is COMPLETE & green — the milestone plan (M1–M9) is fully
> delivered.** Two deliverables landed:
> 1. **Bounded-RSS headline test** — `crates/slater/tests/memory_headline.rs`, the
>    project's raison d'être made into a real-OS-RSS assertion (D34). `slater` is now
>    a **library + thin binary** (`src/lib.rs` exposes the modules; `main.rs` just
>    loads config/logging + runtime and calls `server::serve`) so an integration
>    crate can drive the real server **in-process** and sample `/proc/self/statm`.
>    `serve` was split into a bind step + `serve_with_listener(cfg, listener)`; the
>    test binds an ephemeral `127.0.0.1:0` port, hands the listener over, and so runs
>    the production wiring (graph open + validate, ACL, the three cache pools at the
>    *configured* tiny budgets, resident-PQ pinning, generation guard). It builds a
>    synthetic above-threshold Vamana/PQ generation whose `.vamana` store (~1.2 MiB)
>    is ~5× the vector-cache budget (256 KiB) — so the pool must page — warms the
>    caches, then drives 150 distinct cosine-KNN (+ occasional `MATCH`) queries over
>    a real in-process Bolt client and asserts: recall@10 ≥ 0.7 vs brute force, peak−
>    warm-up RSS ≤ budgets + 48 MiB slack (observed growth ~0), and peak RSS < 512
>    MiB. (Observed: growth 0.0 MiB, recall 1.000.) The bound is deliberately
>    generous + growth-based, not a tight absolute formula, because OS RSS is
>    baseline-dominated and would otherwise flake (the M7 reason unit RSS was
>    rejected).
> 2. **Container & ops** (D35) — top-level `Dockerfile` (multi-stage workspace build:
>    `rust:1-bookworm` builder installing `cmake`/`clang`/`libclang-dev` for rustls
>    `aws-lc-rs` (D5), a workspace dep-cache stub layer, then `--bin slater --bin
>    slater-build` → `debian:bookworm-slim` + `ca-certificates`, `appuser:1000`,
>    `slater` ENTRYPOINT, `HEALTHCHECK ["/app/slater","healthcheck"]`),
>    `docker-compose.yml` (house style: `read_only: true` + `tmpfs`, `/sandbox`
>    overlay, `slater-data:/data:ro` NFS mount, `KEY__sub` env overrides, a
>    `profiles:[build]` `builder` service for the offline writer), and `README.md`
>    (mounts/env table + worked example: build with `slater-build`, connect with the
>    neo4j JS **and** Python drivers, run a `MATCH … RETURN` and a cosine-KNN query).
>
> **Next: no remaining milestone.** The one outstanding item from PLAN.md
> "Verification" is the **feature-gated neo4j JS/Python driver-interop integration
> tests** over `bolt+s` (deferred since M4 — the in-process Bolt client covers the
> wire path, but the real drivers + a cert-bearing TLS handshake are not yet
> exercised in CI; gate behind a feature/env so CI skips when the drivers/Node/
> Python are absent). The README's worked example already documents the exact JS +
> Python snippets to port. Everything else in the plan is delivered and green.
>
> **Green-state check (all currently pass — 170 tests: 41 graph-format + 116 slater
> unit + 1 slater bounded-RSS integration + 10 slater-build + 2 golden round-trip):**
> ```
> cd /home/rickk/git/hs/slater
> export PATH="$HOME/.cargo/bin:$PATH"
> cargo build && cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --all -- --check
> ```
> Cargo is NOT on PATH by default here — always `export PATH="$HOME/.cargo/bin:$PATH"` first.
>
> **The on-disk generation layout (the reader's input):**
> `MANIFEST.json` (+ `property_keys`, `vectorIndexes[].firstRecord`, optional
> `encryption` header = aead/kdf/salt), `node_props.blk`, `node_labels.blk`,
> `edge_props.blk`, `topology.csr.blk`, `vectors.f32.blk` (brute-force groups — D10),
> `range/<name>.isam`, `vector/<l>.<p>.{pq,vamana}` (above-threshold ANN indexes —
> M7), and a `current` text pointer. Symbol tables are MANIFEST `Vec<String>`; ids =
> index. All `.blk`/index/ANN files honour the optional M6 AEAD when `--encrypt` is set.
> `slater-build` CLI is fully wired (`--input/--graph/--data-dir/--block-size/
> --vector-block-size/--zstd-level/--vector-index-json/--encrypt/--key-file/--key-env/
> --ann-threshold/--vamana-r/--vamana-alpha/--pq-subspaces/--pq-bits`).

---

## (previous milestone)

> **Milestone 7 is COMPLETE & green.** The disk-native large-vector Vamana/PQ path
> is end-to-end. `graph-format` gained two modules through the existing `blockfile`
> seam (so they inherit zstd + the M6 AEAD for free): `pq` — deterministic k-means
> PQ codebooks, encode, asymmetric (ADC) squared-L2 estimate, and a `.pq` store
> loaded resident as `ResidentPq`; and `vamana` — single-layer Vamana build (robust
> prune over `R`/`alpha`, mean-point medoid), BFS-from-medoid locality layout, a
> `.vamana` block store `[node_id ‖ full vec ‖ adjacency]` (neighbours as **global
> indices**, mapped to `(block, slot)` by the blockfile directory — D30), and a
> **generic `beam_search`**. `slater-build` routes each `(label, property)` by
> cardinality: at/above `--ann-threshold` (default 50k) it builds Vamana + trains PQ
> (cosine-only in v1, `pq_subspaces | dim`, else brute-force fallback); below it
> stays the M5 brute-force path. `slater` opens those readers + loads resident PQ,
> adds the **vector-index cache pool** (`cache::VectorIndexCache` — pinned resident
> PQ + a Vamana-block LRU under `vector_cache_bytes`, D32), and the executor's
> `apply_vector_call` dispatches on `AnnMode` — the new `Engine::vamana_knn` arm
> navigates by the resident PQ ADC estimate (no IO), reads frontier blocks coalesced
> through the pool, and re-ranks exactly. See D29–D32. (Superseded by the M8 NEXT
> ACTION above; kept for context.)

---

## Milestone status

- [x] **M1 — Scaffold workspace + docs ledger** ✅ done & verified
- [x] **M2 — graph-format core** (no crypto/ANN): manifest, block codec+zstd, integrity, columns, CSR, ISAM ✅ done & verified
- [x] **M3 — slater-build** offline writer: pest grammar + two-pass build + brute-force generation ✅ done & verified
- [x] **M4 — slater** Bolt server + read engine: generation open, block LRU, Bolt/PackStream, ACL, parser, planner, executor,
      tokio listener (+TLS, HELLO→LOGON→RUN→PULL state machine, Val→PackStream incl. Node/Relationship/Map) ✅ done & verified
- [x] **M5 — Brute-force vector KNN** + result LRU: `vector` module (pure brute-force cosine), the one
      permitted `CALL db.idx.vector.queryNodes` (parser + `VectorCall` exec clause), `vecf32`/`similarity`
      functions, vector reads through the block LRU, and the `ResultCache` pool wired into the server ✅ done & verified
- [x] **M6 — Encryption at rest** (per-block AEAD): `graph-format` `crypto` module
      (XChaCha20-Poly1305 + BLAKE3 KDF, pure-Rust), per-block random nonces in the
      `blockfile`/`isam` directories (sealed ISAM top-level too), decrypt-before-
      decompress at the `BlockFileReader::read_block` choke point, `slater-build
      --encrypt --key-file|--key-env`, and key derivation via `Generation::open_with_key`
      / `config.encryption{keyFile|keyEnv}` ✅ done & verified
- [x] **M7 — Large-vector Vamana/PQ path**: `graph-format` `pq` (k-means codebooks +
      ADC) & `vamana` (build/robust-prune + BFS layout + block store + generic beam
      search) through the blockfile seam; `slater-build` routes by `--ann-threshold`
      and writes `vector/<l>.<p>.{vamana,pq}`; `slater` opens resident PQ + the
      `VectorIndexCache` pool and the executor's `AnnMode::Vamana` beam-search arm ✅ done & verified
- [x] **M8 — Generation guard** (poll, exit/swap): `Graphs` holds
      `RwLock<Arc<Generation>>` per graph; a background task polls each `current` on
      `generation_poll_ms` and applies `reload_strategy` — `exit` signals a clean
      non-zero shutdown, `swap` opens+validates the new generation and atomically
      swaps it in (pin new PQ → swap `Arc` → unpin old), refusing a corrupt copy ✅ done & verified
- [x] **M9 — Memory headline test + container**: `slater` is now lib+bin so the
      bounded-RSS integration test (`crates/slater/tests/memory_headline.rs`) drives
      the real server in-process via `server::serve_with_listener` over a loopback
      port and samples `/proc/self/statm` under sustained KNN load (store ≫ budgets
      → growth ~0); multi-stage `Dockerfile` (two binaries, `cmake`/`clang`/
      `libclang-dev`), house-style `docker-compose.yml`, and a `README.md` worked
      example (neo4j JS + Python). See D34/D35 ✅ done & verified

---

## Log

### M9 — Memory headline test + container — DONE (2026-06-10) — **PLAN COMPLETE**

Delivered the bounded-RSS *headline* test (the project's raison d'être) and the
container/ops surface. See D34/D35.

**`slater` → library + thin binary** (`lib.rs` + `main.rs`)
- New `src/lib.rs` exposes the modules as `pub mod` (server/bolt/cache/exec/…);
  `main.rs` shrinks to the stdlib subcommands + config/logging + runtime + a call
  to `server::serve`. This lets a `tests/` integration crate link the engine and
  drive it in-process (a binary-only crate exposes nothing). `testgen` stays
  `#[cfg(test)]`-private to the crate.

**server** (`server.rs`)
- `serve` split into a bind step + `pub async fn serve_with_listener(cfg,
  listener)`, so a caller (the RSS test) can bind an ephemeral `127.0.0.1:0` port,
  learn the address, and run the **production wiring** (graph open + validate, ACL,
  the three cache pools at the configured budgets, resident-PQ pinning, generation
  guard). A `// DESIGN:` comment marks the split.

**Bounded-RSS integration test** (`crates/slater/tests/memory_headline.rs`, new)
- Builds a synthetic above-threshold Vamana/PQ generation from the public
  `graph-format` API (mirroring `slater-build`) whose `.vamana` store (~1.2 MiB) is
  ~5× the 256 KiB vector-cache budget; stands up `serve_with_listener` over loopback;
  drives a minimal in-process Bolt client (reusing `slater::bolt`) through HELLO/
  LOGON then a 30-query warm-up + 150 distinct inline-`vecf32` cosine-KNN (+ occasional
  `MATCH`) queries; samples `/proc/self/statm`. Asserts recall@10 ≥ 0.7 vs brute
  force, peak−warm-up RSS ≤ budgets + 48 MiB slack, and peak RSS < 512 MiB.

**Container & ops** (top-level `Dockerfile`, `docker-compose.yml`, `README.md`, new)
- `Dockerfile`: multi-stage workspace build — `rust:1-bookworm` builder installs
  `cmake`/`clang`/`libclang-dev` (rustls aws-lc-rs, D5), a workspace dep-cache stub
  layer, then `--bin slater --bin slater-build`; `debian:bookworm-slim` runtime +
  `ca-certificates`, `appuser:1000`, `slater` ENTRYPOINT, Bolt `HEALTHCHECK`.
- `docker-compose.yml`: house style — `read_only: true` + `tmpfs`, `/sandbox`
  overlay, `slater-data:/data:ro` mount, `KEY__sub` env overrides, a
  `profiles:[build]` `builder` service for the offline writer.
- `README.md`: mounts/env table + worked example (build with `slater-build`,
  connect with the neo4j JS **and** Python drivers, `MATCH … RETURN` + cosine-KNN).

**Tests (1 new, passes)** — slater integration (1):
`rss_stays_bounded_under_sustained_knn_load` (store ~5× the vector budget served
under sustained KNN load → observed RSS growth 0.0 MiB, recall@10 1.000).

**Verified** — `cargo test` **170 passed** (41 graph-format + 116 slater unit + 1
slater bounded-RSS integration + 10 slater-build + 2 golden); clippy `-D warnings`
clean; `fmt --check` clean. `docker compose config` validates.

**Deviations from PLAN.md** — (1) the RSS assertion is **growth-bounded** (peak −
warm-up ≤ budgets + slack) plus a generous absolute ceiling, not a tight absolute
`≤ budgets + fixed overhead` formula: real-OS RSS is dominated by an unpredictable
process baseline (tokio/rustls/allocator), the very reason M7 deemed unit RSS
sampling flaky; the growth bound proves the same property (no unbounded
accumulation once caches saturate) without flaking. (2) `N` is kept modest
(4 000) because the **fixture's** Vamana build dominates wall-clock, not the
property under test — the bound holds identically at any scale. (3) The remaining
PLAN "Verification" item — **feature-gated neo4j JS/Python driver-interop over
`bolt+s`** — is still deferred (the in-process Bolt client covers the wire path);
it is the only outstanding item and the README documents the snippets to port.

### M8 — Generation guard (poll, exit/swap) — DONE (2026-06-10)

Built the in-flight guard for a `current` pointer that changes under a running
server, end-to-end. **Poll, not inotify** (NFS — D14/D16). See D33.

**config** (`config.rs`)
- New `ReloadStrategy { Exit, Swap }` enum + `AppConfig::reload_strategy()` (errors
  on an unknown value so a fat-fingered config fails at boot) and
  `generation_poll_interval()`. `generation_poll_ms` + `reload_strategy` fields were
  already present from M1.

**generation** (`generation.rs`)
- New `Generation::current_uuid(data_dir, graph)` — reads/parses only the small
  `current` pointer file (no open/validate), so the guard's poll is cheap.

**server** (`server.rs`)
- `Graphs` now holds `HashMap<String, RwLock<Arc<Generation>>>` + the retained
  `data_dir` + `master_key`. `get()` clones an `Arc<Generation>` snapshot a query
  holds for its whole life (a swap never mixes two generations in one query);
  `current_generations()` replaces `generations()` for the startup PQ-pin loop.
- `Graphs::swap_if_changed` — compares on-disk vs live UUID; on a change opens +
  **validates** the new generation (same content-hash guard as boot), pins its
  resident PQ into the `VectorIndexCache`, atomically swaps the slot's `Arc`, then
  unpins the old gen's PQ. Returns `Ok(None)`/`Ok(Some(uuid))`/`Err` (refused; old
  kept). In-flight queries hold their own `Arc<Generation>` (+ PQ `Arc`), so the
  unpin is safe and the gen-UUID cache keys orphan stale entries (D18/D27/D32).
- `guard_sweep` (pure, synchronous) → `SweepAction::{Continue, Shutdown(name)}`;
  `spawn_generation_guard` wraps it with a `tokio::time::interval` (first immediate
  tick consumed) + `spawn_blocking` for the sweep's blocking IO. `serve` parses the
  strategy up front, spawns the guard, and `select!`s the accept loop against a
  `oneshot`; an `exit`-strategy change `bail!`s out of `serve` → `main` returns
  `Err` → non-zero process exit (no `process::exit`, so the core stays testable).

**Tests (6 new, all pass)** — server (6):
`swap_refuses_a_truncated_new_generation` (a half-copied generation errors at open;
the live one is untouched), `swap_applies_a_valid_new_generation_while_in_flight_reads_the_old`
(new queries see the new gen; a handle taken before the swap still reads the old;
a second no-change swap is a no-op), `exit_strategy_guard_sweep_signals_shutdown_on_change`
(no change → `Continue`; a changed `current` → `Shutdown("people")`),
`swap_strategy_guard_sweep_swaps_in_place`,
`swap_moves_pinned_pq_from_the_old_generation_to_the_new` (Vamana fixture: after a
swap the new gen's resident PQ is pinned and the old gen's unpinned),
`exit_strategy_guard_task_signals_shutdown_over_oneshot` (async: the spawned guard
fires the shutdown `oneshot` with the graph name within the timeout). New test
helpers `copy_dir_all` + `publish_copy_as_new_generation` (copy the live generation
to a fresh UUID, optionally truncating a file, and republish `current`).

**Verified** — `cargo test` **169 passed** (41 graph-format + 116 slater + 10
slater-build + 2 golden); clippy `-D warnings` clean; `fmt --check` clean.

**Deviations from PLAN.md** — none material. The plan suggested `ArcSwap` *or* a
`RwLock`; we use a plain `std::sync::RwLock<Arc<Generation>>` to avoid adding the
`arc-swap` dependency (the lock is held only for the pointer clone/replace). `exit`
signals shutdown via a `oneshot` + `serve` `bail!` rather than `std::process::exit`,
so the same non-zero exit happens through `main`'s normal `Result` path and the
decision core is unit-testable. The guard is one task sweeping all graphs (the plan
offered per-graph as an alternative).

### M7 — Large-vector Vamana/PQ path — DONE (2026-06-10)

Built the disk-native ANN path end-to-end so a `(label, property)` index at/above
`--ann-threshold` (default 50k) is served by a Vamana graph + PQ codes instead of
brute force, while RSS stays bounded by the cache budgets. See D29–D32.

**graph-format `pq`** (`crates/graph-format/src/pq.rs`, new + `mod pq;`)
- `PqParams`/`Codebook` (subspaces × k × dsub, `bits ≤ 8` ⇒ one byte/code),
  deterministic-LCG k-means `train_codebooks`, `Codebook::encode`, and `AdcTable`
  (asymmetric squared-L2 estimate from a per-query lookup table). A `.pq` blockfile
  store (`PqWriter`/`PqReader`) — codebook header (record 0) + per-vector code
  records — loaded resident as `ResidentPq` (`load_resident`, block-by-block).

**graph-format `vamana`** (`crates/graph-format/src/vamana.rs`, new + `mod vamana;`)
- `build_vamana` (random R-regular init → two-pass robust prune over `R`/`alpha`,
  mean-point medoid, trivial complete graph when `n ≤ R+1`), `bfs_order` (locality
  layout), the `.vamana` block store (`VamanaWriter`/`VamanaReader`/`decode_node`:
  `node_id ‖ full vec ‖ neighbours-as-global-index` — D30), and a **generic**
  `beam_search` parameterised over a PQ estimate + block-fetch + exact re-rank.

**slater-build** (`build.rs` + `main.rs`)
- New flags `--ann-threshold/--vamana-r/--vamana-alpha/--pq-subspaces/--pq-bits`.
  The vector section now gathers each index's vectors (`PendingIndex`) then routes by
  cardinality: at/above the threshold `build_vamana_index` normalises (D29), builds
  Vamana, BFS-relabels, writes `vector/<l>.<p>.vamana` + trains/writes `.pq` (same
  layout order), records `AnnMode::Vamana`; below it stays brute-force in
  `vectors.f32.blk`. Cosine-only in v1 + `pq_subspaces | dim`, else brute-force
  fallback (D29/D31). The new files join the inventory/block-sizes.

**slater** (`generation.rs` + `cache.rs` + `exec.rs` + `vector.rs` + `config`/`server.rs`)
- `Generation` opens a `VamanaReader` + resident `ResidentPq` per Vamana index
  (`vamana_index`/`vamana_indexes`). `cache::VectorIndexCache` is the second pool
  (D32): pinned resident PQ (charged, never evicted) + a Vamana-block LRU under
  `vector_cache_bytes`, gen-UUID-keyed, with `pin/unpin/resident_pq/record`. The
  executor's `apply_vector_call` dispatches on `AnnMode`; the new `Engine::vamana_knn`
  arm builds the ADC table over the normalised query, runs `vamana::beam_search`
  (navigate by resident PQ, read frontier blocks coalesced via the pool, re-rank by
  the exact metric distance), and returns the same ascending-`score` shape as brute
  force. `vector::distance` made public for the shared re-rank. `server::serve` builds
  the pool and pins every generation's PQ at startup; `run_query` threads it +
  `vectorQuery.beamWidth` into the executing `Engine`.

**Tests (13 new, all pass)** — graph-format pq (5):
`params_validate_divisibility_and_bits`,
`encode_assigns_clustered_points_to_distinct_codes`,
`adc_estimate_tracks_true_distance_ordering`,
`pq_store_roundtrips_codebook_and_codes`, `pq_store_roundtrips_under_encryption`.
graph-format vamana (3): `build_produces_bounded_degree_and_reachable_graph`,
`vamana_store_roundtrips_nodes_and_adjacency`,
`beam_search_recall_matches_brute_force` (recall@10 ≥ 0.85 vs brute force — the
scorer-in-isolation test). slater-build (2):
`above_threshold_builds_vamana_and_pq_files_with_acceptable_recall` (build → read
back via the readers → recall@10 ≥ 0.8), `below_threshold_stays_brute_force`.
slater cache (2): `vector_index_cache_pins_pq_and_serves_blocks`,
`vector_index_cache_evicts_blocks_but_keeps_pinned_pq`. slater exec (1):
`vamana_knn_matches_brute_force_with_bounded_vector_cache` (2000-vector index ≫ the
vector-cache budget → recall@10 ≥ 0.8 over the full executor path while the pool
never pages in the whole store — the headline recall+bounded-RSS test).

**Verified** — `cargo test` **163 passed** (41 graph-format + 110 slater + 10
slater-build + 2 golden); clippy `-D warnings` clean; `fmt --check` clean.

**Deviations from PLAN.md** — (1) Vamana adjacency stores **global indices**, not
on-disk `(block_id, slot)` pairs; the reader derives the pair from the blockfile's
resident directory (`locate`) and coalesces by block — variable-width records make
the on-disk pair circular to size (D30). (2) Above-threshold vectors live only in the
`.vamana` blocks, not also in `vectors.f32.blk` (the Vamana arm never reads it — D31).
(3) v1 builds Vamana for **cosine** indexes only and requires `pq_subspaces | dim`,
else falls back to brute force with a note (D29). (4) the bounded-memory assertion is
on the pool's accounted resident bytes + block count (deterministic) rather than OS
RSS sampling (flaky in a unit test) — it proves the same property: residency is
capped by the budget, independent of index size. `vector_index_pins` config exists and
the pool supports pin/unpin; v1 pins every Vamana index's PQ at startup (the resident
set is required to search), so the config is not yet consulted per-entry.

### M6 — Encryption at rest (per-block AEAD) — DONE (2026-06-10)

Built optional at-rest encryption end-to-end, sealed **per block after
compression** so the block LRU keeps holding plaintext-decompressed blocks and the
executor / KNN / result-cache paths are unchanged. See D28.

**graph-format `crypto` module** (`crates/graph-format/src/crypto.rs`, new + `mod
crypto;`)
- `BlockCipher` over pure-Rust `chacha20poly1305::XChaCha20Poly1305` (NOT the
  C/aws-lc stack): `encrypt`/`decrypt` (clear error on a bad tag, never a panic),
  `random_nonce`, `from_master(master_key, salt)` deriving the per-generation key
  via `BLAKE3::derive_key` over (master key ‖ salt). `random_salt` + `hex_encode`/
  `hex_decode` helpers (no `hex` crate in the tree).

**blockfile** (`blockfile.rs`)
- Encrypted magic `SLBLKE01`; directory entries are 24 bytes wider, carrying each
  block's random nonce. `BlockFileWriter::create_with_cipher` seals each compressed
  block; `read_block` does `pread → decrypt(nonce) → decompress` on a miss.
  `open_with_cipher` refuses an encrypted file with no key, and ignores a key on a
  plaintext file. Plaintext ctors delegate with `None` (byte layout unchanged).

**isam** (`isam.rs`)
- Encrypted magic `SLISME01`: every data block sealed under its own nonce **and**
  the whole resident top-level sealed under one more nonce (widened footer), since
  the top-level holds each block's first key in the clear — otherwise it would leak
  at rest. A wrong key therefore fails at *open* (top-level tag check).

**typed stores** (`columns`/`nodelabels`/`topology`/`vectors`)
- Each gained an `_with_cipher` writer ctor + `open_with_cipher` reader, threading
  `Option<Arc<BlockCipher>>`; plaintext ctors delegate with `None`.

**slater-build** (`build.rs` + `main.rs`)
- `--encrypt --key-file|--key-env` (hex master key); `BuildOptions.encryption_key`
  derives a fresh-salt cipher, threads it into every writer, and records the
  `EncryptionHeader` (aead/kdf/salt — never the key) in the MANIFEST. Absent
  `--encrypt` writes plaintext (M2–M5 fixtures + golden unchanged).

**slater** (`generation.rs` + `config.rs` + `server.rs`)
- `Generation::open_with_key(data_dir, graph, master_key)` derives the cipher from
  the MANIFEST header + runtime key (refusing an unknown AEAD/KDF, or an encrypted
  generation with no key) and hands it to every reader; `open` delegates `None`.
  `EncryptionConfig::load_key()` resolves `keyFile`/`keyEnv` (hex) → bytes;
  `Graphs::open_all` takes the key and `serve` loads it from config.

**Tests (14 new, all pass)** — crypto (5):
`hex_roundtrips_and_rejects_garbage`, `derive_key_is_deterministic_and_salt_sensitive`,
`encrypt_then_decrypt_roundtrips`, `wrong_key_refuses_cleanly`,
`tampered_ciphertext_refuses_cleanly`. blockfile (4):
`encrypted_records_roundtrip_across_blocks`,
`encrypted_block_bytes_are_not_plaintext_on_disk`,
`wrong_key_and_absent_key_are_refused`, `plaintext_file_opens_with_a_key_present`.
isam (1): `encrypted_index_roundtrips_and_refuses_wrong_or_absent_key`.
generation (3): `encrypted_generation_opens_with_the_right_key`,
`encrypted_generation_refuses_absent_and_wrong_key`,
`plaintext_generation_opens_even_with_a_key_configured`. golden (1):
`encrypted_build_then_reopen_with_key` (real `slater-build --encrypt` →
re-open every reader with the derived cipher; absent-key refusal).

**Verified** — `cargo test` **150 passed** (33 graph-format + 107 slater + 8
slater-build + 2 golden); clippy `-D warnings` clean; `fmt --check` clean.

**Deviations from PLAN.md** — one deliberate superset: the milestone named the
`blockfile` choke point, but ISAM range indexes are encrypted too (and their sparse
top-level sealed), because a half-encrypted generation with plaintext range-index
values is not "encryption at rest" — an `--encrypt` image now has no plaintext data
file (D28). Vamana/PQ encryption surface is naturally covered (those readers will go
through the same `blockfile` seam) and remains M7.

### M5 — Brute-force vector KNN + result LRU — DONE (2026-06-10)

Built the vector KNN read path and the third cache pool end-to-end, keeping the
brute-force-only scope (Vamana/PQ remains M7). See D26/D27.

**graph-format** (`crates/graph-format/src/vectors.rs`)
- Exposed `VectorStoreReader::inner()` + public `decode_vector` (mirroring
  `columns`/`nodelabels`/`topology`) so the KNN scan reads index-group records
  through the block LRU rather than uncached `pread`s.

**slater `vector` module** (`crates/slater/src/vector.rs`, new + `mod vector;`)
- Pure `cosine_similarity` and `brute_force_knn(entries, query, k, metric)`:
  score = distance (`1 - cosine_similarity` for cosine), ascending, ties by node
  id; dimension mismatch is a hard error; zero-norm → similarity 0.

**parser** (`cypher.pest` + `parser.rs`)
- The one allowed `CALL`: `forbidden_clause`'s `call` branch gained a negative
  lookahead `!(ws+ ~ vector_proc)`, and `reading_clause` gained `vector_call_clause`
  (`CALL db.idx.vector.queryNodes(args) YIELD … [WHERE …]`). New `Clause::VectorCall`
  + `VectorCallClause` AST and its lowering (string-literal label/property; `k` and
  query vector as exprs; `YIELD` output→bound-var pairs; optional WHERE). `call`/
  `yield` are reserved; `kw_call`/`kw_yield` filtered in lowering.

**exec** (`exec.rs`)
- `apply_vector_call` resolves the `(label, property)` `VectorIndexDesc`, reads its
  group through `BlockCache` (`vector_group` over `VectorStoreReader::inner()`),
  runs `brute_force_knn`, binds `YIELD node/score`, applies the optional WHERE.
  New `vecf32()` (builds `Val::Vector`) and `similarity()` scalar functions; `as_vector` helper.
- Extended the shared `testgen` fixture: all three Person nodes now carry
  embeddings (vector index `count: 3`) so KNN has a real candidate set.

**cache** (`cache.rs`)
- `ResultCache<V>` — the third pool: generic, byte-budgeted LRU (same tick +
  `BTreeMap` machinery + atomic metrics as the block cache), keyed
  `ResultKey { gen, query }`. `insert` charges the key's length to the budget;
  keeps ≥1 entry; `get` records hit/miss.

**server** (`server.rs`)
- `ConnCtx` gains `result_cache: Arc<ResultCache<QueryResult>>` (built from
  `cfg.cache.result_cache_bytes`). `run_query` now consults the result cache first
  (gen-UUID-keyed, normalised query + params), executes + inserts on a miss, then
  re-encodes the version-independent `QueryResult` for the connection's Bolt
  version. New `result_query_key` + `estimate_result_bytes`/`val_bytes`.

**Tests (21 new, all pass)** — vector unit (5):
`cosine_similarity_matches_hand_computation`, `zero_norm_vector_is_maximally_distant`,
`knn_orders_by_distance_with_scores_matching_reference`, `k_larger_than_group_returns_all`,
`dimension_mismatch_is_an_error`. parser (2): `accepts_the_vector_knn_procedure`,
`rejects_malformed_vector_calls` (+ the existing reject test now also covers a
non-vector `db.idx.fulltext` CALL). exec (8):
`vector_knn_returns_k_nearest_ordered_with_reference_scores`,
`vector_knn_yield_alias_and_node_projection`, `vector_knn_yield_where_filters_rows`,
`vector_knn_unknown_index_is_an_error`, `vector_knn_dimension_mismatch_is_an_error`,
`vector_knn_query_vector_from_parameter`, `vector_knn_reads_route_through_the_block_cache`,
`similarity_and_vecf32_scalar_functions`. cache (4): `result_cache_hit_then_miss`,
`result_cache_evicts_least_recently_used`, `result_cache_generation_swap_orphans_stale_entries`,
`result_cache_single_oversized_result_is_retained`. server (2):
`vector_knn_query_returns_nodes_and_scores_over_bolt`,
`identical_query_is_served_from_the_result_cache`.

**Verified** — `cargo test` **136 passed** (23 graph-format + 104 slater + 8
slater-build + 1 golden); clippy `-D warnings` clean; `fmt --check` clean.

**Deviations from PLAN.md** — none material. The KNN `score` is the cosine
*distance* (ascending), matching FalkorDB's documented `queryNodes` contract; the
`similarity()` scalar returns the complementary similarity (D26). The PLAN footnote
on result-cache keys is resolved in favour of normalising whitespace + serialising
params and caching all queries (the key's bytes are charged to the budget), rather
than skipping vector queries (D27). The vector-index cache pool (pinned Vamana
blocks + PQ codes) remains M7, as planned — M5 reads vectors through the block LRU.

### M4 — slater Bolt server + read engine — DONE (2026-06-10)

#### Sub-step 1 — `generation` module — DONE (2026-06-10)

`crates/slater/src/generation.rs` + `mod generation;` in `main.rs`. The reader's
entry point: `Generation::open(data_dir, graph)`.

- Resolves `<data_dir>/<graph>/current` → generation UUID → generation dir.
- Parses the `Manifest`; sniffs magic + `formatVersion` (refuses an unknown
  version) and checks `graph` matches the directory it sits under.
- **Copy-completeness guard**: re-hashes every `files[]` entry from disk via
  `integrity::hash_file`, refuses on the first per-file mismatch (precise "which
  file" error), then asserts `Manifest::verify_content_hash` (inventory
  self-consistency). This is the half-copied-NFS-rsync guard from the plan.
- Opens every reader eagerly — `PropsReader` ×2 (node/edge), `NodeLabelsReader`,
  `TopologyReader`, `VectorStoreReader`, and one `IsamReader` per range index —
  footer-only, so block bytes stay lazy via `pread` (**D16**).
- Builds the in-memory inverted postings (**D17**): `label_id → ascending node
  ids` and `reltype_id → ascending edge ids`, plus `name → id` inverses of the
  three MANIFEST symbol tables. Accessors: `nodes_with_label`,
  `edges_with_reltype`, the readers, symbol lookups, identity/metadata.

**Tests (6, all pass)** — `open_validates_and_exposes_readers`,
`symbol_tables_invert`, `inverted_postings_are_built`,
`rejects_content_hash_mismatch` (corrupts a `.blk` after manifest write),
`rejects_unknown_format_version`, `rejects_missing_current`. The fixture builds a
representative generation directly with the graph-format writers (no dependency on
the `slater-build` binary): 3 nodes (Person/Company), 2 typed edges, a routed
vector + vector index, and a range ISAM, then publishes a `current` pointer.

**Verified** — `cargo test` **38 passed** (23 graph-format + 6 slater + 8
slater-build + 1 golden); clippy `-D warnings` clean; `fmt --check` clean.

#### Sub-step 2 — `cache` (block LRU) — DONE (2026-06-10)

`crates/slater/src/cache.rs` + `mod cache;` in `main.rs`. A byte-budgeted LRU over
**decompressed** blocks, safe to share across Bolt tasks.

- `BlockKey { gen: u128, file: u32, block: u32 }` — keyed on the generation UUID
  (globally unique → subsumes graph; swap orphans stale entries), with `FileKind`
  encoding the fixed files (0–4) and `Range(i)` behind a flag bit (D18).
- `BlockCache::get_or_try_insert(key, load)` — hit returns the cached `Arc`; miss
  runs `load` **outside** the lock (no IO under the mutex), inserts, evicts true
  LRU (tick + `BTreeMap`) to the byte budget, keeping ≥1 block so an oversized
  block stays returnable. Atomic hit/miss/eviction counters → `metrics()`.
- `BlockCache::record(reader, gen, file, global)` — the routing path: `locate` →
  cached block → `parse_block`/`record_from_block` slice. This is what the M4.5
  executor calls instead of the readers' uncached `read_record_global`.

**Tests (5, all pass)** — `hit_then_miss_counts_and_returns_same_bytes`,
`evicts_least_recently_used_over_budget`, `single_oversized_block_is_retained`,
`generation_id_isolates_keys`, `record_reads_through_cache_against_a_real_blockfile`
(builds a real multi-block file, sweeps it twice, asserts the second sweep is all
hits).

**Verified** — `cargo test` **43 passed** (23 graph-format + 11 slater + 8
slater-build + 1 golden); clippy `-D warnings` clean; `fmt --check` clean.

#### Sub-step 3 — `bolt` wire layer — DONE (2026-06-10)

`crates/slater/src/bolt/` (`mod.rs` + four modules) + `mod bolt;` in `main.rs`.
The deterministic, socket-free wire core (D19):

- `packstream.rs` — PackStream v2 `PsValue` codec (encode/decode), big-endian,
  smallest-int, ordered maps, tiny-struct messages; `Decoder` cursor.
- `handshake.rs` — `PREAMBLE`, `SUPPORTED = [(5,4),(4,4)]`, `Proposal` (range-aware),
  `negotiate` / `handle_client_hello` → agreed-version reply or four zero bytes.
- `chunk.rs` — `frame`/`decode_message`/`decode_complete`: 2-byte length chunks +
  `00 00` terminator; partial buffers return `None` (read more), not an error.
- `message.rs` — `tag` constants; `decode_request` (de-chunked body → `Request`);
  `success`/`record`/`failure`/`ignored` builders; `to_wire` (packstream + frame).

**Tests (25, all pass)** — packstream known-encodings + int/float/string/list/map
boundary round-trips + nested struct + reject trailing/unknown (7); handshake
5.4/4.4/range/preference/no-version/bad-preamble (6); chunk single/empty/large/
partial/back-to-back (5); message decode HELLO/LOGON/RUN/control/PULL + response
encode + failure + reject-unknown-tag/arity (7).

**Verified** — `cargo test` **68 passed** (23 graph-format + 36 slater + 8
slater-build + 1 golden); clippy `-D warnings` clean; `fmt --check` clean.

#### Sub-step 4 — `acl` (argon2id auth + grants) — DONE (2026-06-10)

`crates/slater/src/acl.rs` + `mod acl;` and the `hash-password` subcommand wired
into `main.rs` (before the runtime starts).

- `Acl` / `UserEntry` — parse the JSON ACL (`users → { passwordArgon2id, grants }`);
  unknown keys (`_comment`) ignored. `verify(user, pw)` (argon2id PHC verify; dummy
  verify on the unknown-user path to flatten timing; malformed hash → reject+log),
  `can_read(user, graph)`, `readable_graphs(user)`.
- `hash_password(pw)` — random-salt argon2id PHC string; `hash_password_subcommand`
  reads the password from `argv[2]` or stdin and prints the hash.
- `AclHandle` — `RwLock<Arc<Acl>>`; `snapshot()` per request; `reload()` keeps the
  last-good ACL on a bad file (logs loudly); `poll()` mtime-gates reload for the
  background poller. Initial `load()` errors (no server without a usable ACL). (D20)

**Tests (6, all pass)** — `hash_is_argon2id_and_verifies`,
`verify_checks_user_and_password`, `grants_are_per_graph_and_read_only`,
`parses_sample_file_shape_with_comment`, `hot_reload_keeps_last_good_on_malformed_file`,
`missing_initial_acl_is_an_error`. The `hash-password` CLI was also smoke-checked
(`cargo run -p slater -- hash-password …` → a real `$argon2id$…` string).

**Verified** — `cargo test` **74 passed** (23 graph-format + 42 slater + 8
slater-build + 1 golden); clippy `-D warnings` clean; `fmt --check` clean.

#### Sub-step 5a — `parser` (read-only Cypher → AST) — DONE (2026-06-10)

`crates/slater/src/cypher.pest` + `parser.rs` + `mod parser;`. The online query
grammar (separate from `slater-build`'s dump dialect) and its lowering to a typed
AST (`parser::ast`).

- Grammar covers the widened read subset: MATCH/OPTIONAL MATCH, WHERE, WITH,
  RETURN, UNION[ ALL]; relationship patterns with type alternation (`:A|B`) and
  variable length (`*1..3`); map projection (`n {.name}`), CASE, list predicates
  (`any/all/none/single`), function calls/aggregations, ORDER BY/SKIP/LIMIT/DISTINCT;
  full expression precedence. Write/procedure clauses parse then reject as read-only.
- **Hard-won grammar fixes (D21)**: keywords must be **atomic** (`@{}`) with a
  `!ident_cont` boundary, else implicit whitespace breaks the boundary and `or`
  matches inside `ORDER` (and `1 OR 2` fails); atomic keyword tokens are filtered out
  in lowering via `kids()`. `forbidden_query` consumes the rest so writes reject with
  a clear message. `$limit`-style params use an unreserved name rule.

**Tests (7, all pass)** — `accepts_the_read_subset` (24-query corpus incl. the
sibling services' shapes), `rejects_writes_and_procedures_with_read_only_message`,
`rejects_syntax_errors`, `lowers_pattern_and_projection_structurally`,
`lowers_union_and_distinct`, `lowers_expression_precedence`, `string_literals_unescape`.

**Verified** — `cargo test` **81 passed** (23 graph-format + 49 slater + 8
slater-build + 1 golden); clippy `-D warnings` clean; `fmt --check` clean.

#### Sub-step 5b — `plan` (scan-strategy planner) — DONE (2026-06-10)

`crates/slater/src/plan.rs` + `mod plan;`. A pure `choose_node_scan(gen, node,
where_) -> NodeScan` picking the anchor's candidate generator (D22):
range-index **equality** → range-index **range** → smallest **label posting** →
full **AllNodes** sweep. Extracts constant predicates from inline `{prop: lit}`
maps and top-level `WHERE` `AND` conjuncts (`var.prop <op> literal`, mirrored when
flipped), resolving an index only when one is open over `(label ∈ node.labels,
prop)`. Plans on literals only; parameters fall through to a scan + executor
filter — correctness never depends on the plan (the executor re-checks all
predicates).

**Tests (5, all pass)** — `inline_equality_on_indexed_property_picks_range_eq`,
`where_equality_on_indexed_property_picks_range_eq`,
`range_predicate_on_indexed_property_picks_range_range`,
`unindexed_label_falls_back_to_smallest_label_posting`,
`no_label_no_index_falls_back_to_all_nodes`.

#### Sub-step 5c — `exec` (volcano executor) — DONE (2026-06-10)

`crates/slater/src/exec.rs` + `mod exec;`, plus a shared `#[cfg(test)] mod
testgen;` fixture (5 nodes Person/Company, 5 typed edges, name+age range indexes,
a routed embedding). `Engine::run(query)` over `Generation` + `BlockCache` (D23):

- **Reads route through the block cache** — new `graph-format` surface
  (`PropsReader/NodeLabelsReader/TopologyReader::inner()` + public
  `decode_props`/`decode_labels`/`decode_adj`) lets the executor call
  `BlockCache::record` then slice the record from the cached decompressed block.
- Runtime `Val` (stored `Value` + `Node`/`Rel`/`Map`) with total-order `cmp_total`
  and three-valued `loose_eq`. Backtracking matcher (anchor via planner, CSR chain
  walk, direction + type alternation + rel-prop filters), var-length DFS with
  relationship uniqueness (`*` capped at 15 hops), `OPTIONAL MATCH` null-fill.
- Projection pipeline (shared by `WITH`/`RETURN`): star-expand → simple/aggregated
  → `DISTINCT` → (`WITH`) `WHERE` → `ORDER BY` → `SKIP` → `LIMIT`. Aggregation
  groups by non-aggregate items (`BTreeMap` → deterministic order); aggregates
  nested in expressions handled via `collect_aggregates` + an `AggCursor` replay.
  Full expression evaluator (arith incl. string/list `+`, comparisons, `IN`,
  `STARTS/ENDS/CONTAINS`, `IS NULL`, `CASE`, list predicates, map projection, a
  scalar-function set + `count/sum/avg/min/max/collect`). `UNION[ ALL]`. `max_rows`
  + optional wall-clock deadline guards.

**Tests (21, all pass)** — all-nodes/label/range-eq/range-range scans, forward &
incoming traversal, rel-property predicate, var-length, type alternation, `WITH`
aggregation + `HAVING`, the aggregate-function suite + `collect DISTINCT`,
`DISTINCT` projection, `SKIP`/`LIMIT`, map projection, `CASE`, `IN` + string ops,
`UNION`, `OPTIONAL MATCH` nulls, **reads-route-through-the-cache** (second run = 0
new misses), parameter substitution, `max_rows` enforcement.

**Verified** — `cargo test` **107 passed** (23 graph-format + 75 slater + 8
slater-build + 1 golden); clippy `-D warnings` clean; `fmt --check` clean.

#### Sub-step 6 — `server` (tokio Bolt listener) — DONE (2026-06-10) — **M4 COMPLETE**

`crates/slater/src/server.rs` + `mod server;` in `main.rs`; `main` now builds the
multi-thread tokio runtime and hands off to `server::serve` (the scaffold stub is
gone). The piece that ties everything together (D24):

- **Listener + connection state machine** — `serve` opens every graph under
  `data_dir` (`Graphs::open_all`, fail-fast on a bad generation), loads the ACL,
  builds **one shared** `BlockCache`, optionally a `rustls` `TlsAcceptor`, then
  `accept`s and spawns a task per connection. `handle_connection<S>` (generic over
  plain TCP / TLS) runs the handshake then the message loop: HELLO (+ 4.4-embedded
  auth), LOGON, RUN→PULL/DISCARD, BEGIN/COMMIT/ROLLBACK (read-only no-ops), RESET,
  GOODBYE; a FAILURE drops the connection into the IGNORED-until-RESET state.
- **Auth + grant** — `authenticate` verifies `basic` creds against the ACL (after a
  cheap `poll()` for hot edits); `RUN` selects a graph (`db` metadata, else the
  user's sole readable graph) and enforces `can_read` before parsing. Status codes
  per D24.
- **Execute + encode off the reactor** — `RUN` parses synchronously (clean
  syntax/read-only classification), then `spawn_blocking` runs `exec::Engine`
  (config `maxRows`/`timeoutMs`) **and** encodes the rows to PackStream; `PULL`
  drains the buffered `RECORD`s + `SUCCESS {has_more}`. `encode_val` maps `exec::Val`
  → `PsValue` incl. Node (`0x4E`) / Relationship (`0x52`) / Map, with Bolt-5
  element-id fields gated on the negotiated version; `Val::Rel` was extended to
  carry stored endpoints + type so a relationship is materialisable, also enabling
  `type()` (D25).

**Tests (8 new, all pass)** — `exec::relationship_value_carries_type_and_stored_endpoints`
(outgoing + incoming walk report the same src→dst direction); server (7):
`full_handshake_logon_run_pull_returns_records`,
`returns_node_and_relationship_structures` (asserts the `N`/`R` struct shapes,
endpoints, type, props), `hello_embedded_auth_authenticates_the_4_4_fallback`,
`bad_password_fails_and_run_before_logon_fails`, `write_query_is_rejected_read_only`
(+ FAILED→IGNORED→RESET), `open_all_discovers_the_fixture_graph`,
`tls_acceptor_is_none_when_disabled`. The socket tests stand up the handler on a
loopback `TcpListener` and drive a real Bolt client over plaintext.

**Verified** — `cargo test` **115 passed** (23 graph-format + 83 slater + 8
slater-build + 1 golden); clippy `-D warnings` clean; `fmt --check` clean.

**Deviations from PLAN.md** — none material for M4. The `vector` KNN module and the
**result-cache** pool are M5 (the block LRU is the only cache pool wired so far).
Driver-interop tests against the real neo4j JS/Python drivers remain the planned
feature-gated addition (the wire path is covered here by an in-process Bolt client).
TLS is implemented but exercised only via `build_tls_acceptor` (cert-bearing
handshake belongs with the gated driver-interop test).

### M3 — slater-build offline writer — DONE (2026-06-10)

Implemented the offline writer end-to-end: primitive-Cypher dump script in, an
immutable, content-hashed, atomically-published generation directory out.

**graph-format additions** (`crates/graph-format/src/`)
- `manifest`: added `property_keys: Vec<String>` (the bounded key symbol table,
  resident — D7) and `VectorIndexDesc.first_record` (group offset into the vector
  store — D10).
- `vectors` — `VectorStoreWriter`/`VectorStoreReader` for `vectors.f32.blk`; one
  `node_id ‖ dim ‖ dim×f32` record per vector, grouped by index (D10).
- `nodelabels` — `NodeLabelsWriter`/`NodeLabelsReader` for `node_labels.blk`; the
  forward per-node label-id store (D11; inverted postings deferred to M4).

**slater-build** (`crates/slater-build/src/`)
- `primitive_cypher.pest` — the dump dialect: node/edge create, node/edge range
  index, the two vector-index forms, + ignorable marker/cleanup/drop (D15).
- `parser` — `StatementReader` (streaming top-level-`;` splitter, byte-level,
  string-aware — D13) + `parse_statement` (pest pairs → typed `Statement`).
- `model` — `Statement`/`NodeStmt`/`EdgeStmt`/`RangeIndexStmt`/`VectorIndexStmt`
  (property values reuse `graph_format::ids::Value`, so `vecf32` is `Value::Vector`).
- `build` — two-pass build (intern labels/reltypes/keys first-seen; `__DumpVertex__`
  and `__dump_id__` dropped; `vecf32` routed to the store only when an index covers
  it — D12; CSR via `topology`; range ISAMs; brute-force `VectorIndexDesc`); atomic
  publish + `current` swap (D14). `--vector-index-json` sidecar supported.
- `main` — full `clap` CLI wired (`--input/--graph/--data-dir/--block-size/
  --vector-block-size/--zstd-level/--vector-index-json`); prints generation UUID +
  content hash.

**Verified** — `cargo test` **32 passed** (23 graph-format unit + 8 slater-build
parser unit + 1 golden round-trip integration); `cargo clippy --all-targets -D
warnings` clean; `cargo fmt --all -- --check` clean. The golden test runs the real
binary on a representative dump (multi-label nodes, string array, escaped string
with an embedded `;`, `vecf32`, node range + vector index, one relationship,
marker/cleanup lines), then re-opens **every** graph-format reader and the MANIFEST
and asserts nodes/labels/edge/topology/vectors/range-index all round-trip, plus
re-hashes every inventory file against the manifest.

**Deviations from PLAN.md** — none material. `AnnMode::BruteForce` only (Vamana/PQ
is M7, as planned). `dictionary.blk` still not emitted (D7). `labels.post`/
`reltypes.post` inverted postings deferred to M4 (D11) — M3 emits the forward
`node_labels.blk` instead.

**Context for resume**
- A node's `vecf32` is in `vectors.f32.blk` (not `node_props.blk`) **iff** a vector
  index is declared on a `(label, prop)` it carries (D12); otherwise it is an inline
  `Value::Vector` column. M4's reader must fetch `n.embedding` from the vector store.
- Edge endpoints are resolved by **CREATE variable name**, not MATCH order, so a
  `CREATE (b)-[...]->(a)` reverses src/dst correctly (test:
  `edge_endpoint_order_follows_create_vars`).
- M4 needs inverted label/reltype postings for selective scans; build them in the
  `generation` open path (D11 notes the forward store is what M3 produced).

### M2 — graph-format core — DONE (2026-06-10)

Implemented the full on-disk storage substrate and typed structures (no crypto,
no ANN — both deferred to M6/M7 per plan).

**Modules added** (`crates/graph-format/src/`)
- `codec` — zstd compress/decompress.
- `blockfile` — generic block container: packs length-delimited records into
  fixed-size zstd blocks; resident block directory (offset/len/**rec_count**) +
  footer; `pread` (no mmap); **global record addressing** (`locate`,
  `read_record_global`) via a tiny prefix-sum index (D9).
- `integrity` — BLAKE3 `hash_file` + order-sensitive `content_hash` over the
  file inventory.
- `manifest` — `Manifest` (+ `AnnMode::{BruteForce,Vamana}`, `Metric`,
  `RangeIndexDesc`, `VectorIndexDesc`, `EncryptionHeader`, `FileEntry`),
  `verify_content_hash`, JSON read/write.
- `wire` — LEB128 varints + zig-zag + inline `Value` codec (D7).
- `columns` — `PropsWriter`/`PropsReader`, row-per-entity property maps (D6).
- `topology` — `write_csr` + `TopologyReader`, forward+reverse CSR in one file (D8).
- `isam` — sorted blocked range index, resident sparse top-level; `lookup_eq` +
  `lookup_range` (cross-block duplicate/boundary handling), `Value::cmp_key` total order.

**Verified** — `cargo test` **21 passed**; `cargo clippy --all-targets -D warnings`
clean; `cargo fmt --all -- --check` clean. Tests cross-check every reader against
its writer (round-trip), CSR forward/reverse equivalence, and ISAM eq/range against
linear scans over multi-block fixtures.

**Deviations from PLAN.md** — see DECISIONS D6–D9: row-per-entity props (not strict
column orientation); inline string values + symbol tables in the MANIFEST (no
`dictionary.blk` in v1); single `topology.csr.blk` for both directions.

**Context for resume**
- The `vectors.f32.blk` writer is NOT built yet — M3 adds it (or reuses
  `BlockFileWriter`). One record per vector, grouped by `(label,property)`.
- Symbol tables (labels/reltypes/propertyKeys) are MANIFEST `Vec<String>`; the
  builder assigns ids = index. NOTE: `Manifest` currently has `labels` + `reltypes`
  but NOT `property_keys` — M3 must add a `property_keys: Vec<String>` field (and a
  per-node `labels: Vec<u32>` store, since `columns` only holds properties, not which
  labels a node has). Decide the node-labels store in M3 (e.g. a `node_labels.blk`
  bitmap/postings or a small per-node label-id list record).

### M1 — Scaffold workspace + docs ledger — DONE (2026-06-10)

Created the `slater` cargo workspace and a compiling, tested skeleton.

**Files added**
- `Cargo.toml` — `[workspace]` (resolver 2), `[workspace.package]`, centralised
  `[workspace.dependencies]` (incl. `hs-utils` git+tag `v0.16.0`, `graph-format`
  path), house release profile (`strip/lto/codegen-units=1/opt-level="s"`).
- `.cargo/config.toml` — `net.git-fetch-with-cli = true` (forces the git CLI for
  the `hs-utils` fetch; libgit2 transport is unreliable here, the git CLI works).
- `rust-toolchain.toml` — stable + rustfmt + clippy. `.gitignore` (keeps Cargo.lock).
- `crates/graph-format/` — lib with `FORMAT_VERSION=1`, `MAGIC=b"SLATER01"`, and a
  real `ids` module: `NodeId/EdgeId/BlockId/Generation` newtypes + `Value` enum
  (incl. first-class `Vector(Vec<f32>)`). 3 unit tests.
- `crates/slater-build/` — bin `slater-build`, clap CLI (`--input/--graph/--data-dir`),
  pipeline stubbed (`bail!`) until M3.
- `crates/slater/` — bin `slater`; `config.rs` (full `AppConfig` via
  `hs_utils::config::load_layered_value`, camelCase, `deser_*_or_str` + local
  `de::u64`/`de::usize`); `health.rs` (Bolt-native handshake probe, NOT HTTP);
  `main.rs` wires healthcheck → config → `hs_utils::logging::init`.
- `config.json`, `acl.json` (argon2id placeholder), `docs/PLAN.md` (frozen plan),
  this `PROGRESS.md`, `DECISIONS.md`, `testdata/` + `corpus/` skeletons.

**Verified**
- `cargo build` — clean, 0 warnings, ~17s cold (hs-utils git+tag fetched OK).
- `cargo test` — `test result: ok. 3 passed` (graph-format unit tests).
- `cargo clippy --all-targets -- -D warnings` — clean.
- `cargo fmt --all -- --check` — clean.

**Deviations from PLAN.md**
- Healthcheck: PLAN.md said `hs_utils::healthcheck::check_subcommand` works against
  a Bolt port unchanged. It does NOT — that helper speaks HTTP and checks for
  `HTTP/1.1 200`. Replaced with a Bolt-native handshake probe in `slater::health`.
  See DECISIONS.md.
- `hs-utils` pinned to `v0.16.0` (latest), not the siblings' `v0.10.0`; the
  config/logging API we use is identical and present in 0.16.0.

**Context for resume**
- `config`, `logging`, `healthcheck` in `hs-utils` are NOT feature-gated → we use
  `default-features = false` and avoid the actix/sqlx stack entirely.
- The rustls default backend pulls `aws-lc-rs`, which needs `cmake` + `clang` +
  `libclang-dev` at build time (the M9 Dockerfile builder stage must install them,
  matching `bioalphaengine-data-service`).
