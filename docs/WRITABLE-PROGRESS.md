# Slater writable layer — progress ledger

Running ledger for the `writeable` track. Pairs with the design in
`docs/WRITABLE-PLAN.md` (stable design) and the decisions in `docs/DECISIONS.md`
(D43+). **This file is the resume anchor** — read it first after a context clear.

---

## How to resume (read this after a context clear)

- **Branch:** `writeable` (off `main`). Long-lived track; do **not** fast-forward
  into `main` without the user's say-so. Many small commits.
- **Build/test target dir is redirected** — `target/` has some root-owned
  artefacts, so always export:
  ```
  export CARGO_TARGET_DIR=/tmp/claude-1000/-home-rickk-git-hs-slater/6a6f382f-eb59-4b50-8ebb-050f63801623/scratchpad/target
  ```
  (If that scratch path is gone after a session reset, pick any writable dir and
  set `CARGO_TARGET_DIR` to it — a fresh full compile is the only cost.)
- **Green gates (run before every commit):**
  `cargo test -p slater -p slater-delta`, `cargo clippy -p slater -p slater-delta
  --all-targets -- -D warnings`, `cargo fmt -p slater -p slater-delta -- --check`.
  Full workspace determinism gate: `cargo test --workspace` (golden/emit-determinism/
  resume tests in `slater-build`).
- **Empty-delta no-regression bench:**
  `cargo bench -p slater --features testkit --bench delta_overlay`.
- **British English everywhere.** `#![forbid(unsafe_code)]` via `[lints.rust]` in
  each crate's Cargo.toml (not a source attribute).

## Architecture cheat-sheet (where things live)

- `crates/slater-delta/` — owns delta byte formats + read-merge fold logic.
  - `identity.rs` — `NodeIdentity`/`EdgeIdentity` (delta-local `SymbolId`s + `Value`),
    type-exact `canonical_key()` via `graph_format::wire::write_value`.
  - `interner.rs` — first-seen delta-local symbol interner.
  - `memtable.rs` — `NodeDelta`/`EdgeDelta` (LWW `BTreeMap` patches + tombstone),
    single-writer `Memtable`, read-side `DeltaSnapshot` (`is_empty` fast path).
  - `wal.rs` — WAL; two-seam durability (see D44). Currently a `Seq` placeholder.
- `crates/slater/src/read_view.rs` — `ReadView` trait; `Generation` identity impl;
  `MergedView { core, delta }`. `Engine<'g, V: ReadView>` is generic (D43).
- `crates/slater/src/exec.rs` — executor; reads via `self.gen: &V`. Node
  materialisation: `node_record`/`node_props(id)` (~L1411–1490). Scan choke point:
  `scan_candidates` (~L4928). The property overlay hooks in here (Phase 1c).
- `crates/slater/src/parser.rs` — write rejection at `lower_single_query` (~L697)
  and `lower_call_clause` (~L820); relax here for write ingestion (Phase 1c).
- `crates/slater/src/cache.rs` — `ResultKey` keys on `gen.uuid()`; add a delta
  epoch (Phase 1c).
- `crates/slater/src/server.rs` — generation guard (`swap_if_changed` ~L320,
  `guard_sweep` ~L386), per-query `Arc<Generation>` pin (`Graphs::get` ~L279),
  Bolt node/rel emission (~L2504–2660, `element_id`). Write ingestion + orchestrator
  land here (Phase 1c/1d).
- `crates/slater-build/src/build_external.rs` — `--consolidate` mode goes here
  (Phase 1d/4). `common.rs::write_manifest_and_publish` is the atomic swap (D14).

## Key implementation decisions (beyond D43/D44)

- **WAL record is `slater-delta`'s own type, NOT `slater-build::model::Statement`.**
  `slater-build` depends on `slater-delta`, so the dep cannot be reversed. The
  builder's grammar is "reused" at the *consolidation output* level: Phase-4a
  serialises the frozen delta to business-key `MERGE` text and re-parses it. WAL
  records carry symbol **names** inline (self-describing) so replay re-interns to the
  same delta-local ids deterministically.
- **Group-commit boundary = an explicit commit marker frame.** `WalSink::commit()`
  writes the batch's record frames + a commit-marker frame, then fsyncs; the Bolt
  ack happens after `commit()` returns. Replay keeps records **only up to the last
  complete commit marker**, so a torn/un-fsynced tail is dropped — giving exactly
  "the writes whose batch fsync completed, and no more".
- **Reads are O(1) via a resolved dense-id index, not id→identity reconstruction.**
  The writer resolves each write's business key to the current-core dense id **once**
  (ISAM equality probe) and stores the delta under that dense id for the current
  core generation. `MergedView` node reads then consult `resolved[dense_id]` — no
  need to reconstruct a node's business key from its dense id. The business-key map
  stays authoritative for WAL replay + consolidation + cross-swap identity. (Delta
  is retired at consolidation, so the resolved index is rebuilt-empty after a swap.)
- **Phase 1 writes require the business-key property to be range-indexed** (so the
  write can resolve to a dense id). If unindexed, reject with a clear error for now;
  a labelled-scan fallback is a later refinement.

## Phase status

- **Phase 0 — scaffolding. ✅ DONE** (commits `9187665`, `b2fccf0`).
  `slater-delta` crate; `ReadView`/`MergedView`/generic `Engine`; `testkit` +
  `delta_overlay` bench (empty-delta within noise); WAL two-seam correction folded
  into docs. Whole workspace green.

- **Phase 1 — durable property overwrites + dump-and-rebuild consolidation. 🔨 IN PROGRESS.**
  Sub-milestones (each independently green + committed = a safe context-clear point):
  - **1a — `WalSink` local floor. ✅ DONE.** `wal.rs`: segment format
    (`MAGIC ‖ frame*`, frame = `len:u32 ‖ crc32c:u32 ‖ payload`), `WalOp::UpsertNode`
    (names inline), `WalSink::{append,commit,seal}` (commit marker + fsync = ack
    barrier), `replay_segment`/`replay_dir` (keep only to last commit marker). 6 unit
    tests incl. dropped-uncommitted-tail + torn-frame truncation. `crc32c` dep added.
  - **1b — memtable mutation + resolved index. ✅ DONE.** `memtable.rs`:
    `Memtable::upsert_node` (LWW fold, patches name-keyed, identity interned +
    stored for name recovery), `by_dense: dense_id → canonical_key` read index
    (`resolved` passed in by the caller — no `Generation` needed for unit tests),
    `apply(&WalOp, resolved)` shared by live writes + replay, `node_patch(dense_id)`,
    `iter_nodes()` (consolidation input). 18 slater-delta tests green.
  - **1c — server integration. ✅ DONE** (commits `193fe17`, `d17d98f`, +this).
    Shipped in three green sub-slices:
    - **1c-A** (`193fe17`): read overlay in `exec.rs` (`node_prop_par` single-prop +
      `overlay_node_props` for `node_record`/`all_properties`, name-space LWW,
      empty-delta fast path); `delta_writer::DeltaWriter` (single-writer WAL floor +
      authoritative `Memtable` + published `RwLock<Arc<Memtable>>` snapshot + epoch;
      `write` = append+commit(fsync ack)+apply+publish; `open` replays WAL, opens a
      fresh segment); `config::DeltaConfig` (off by default); `Memtable: Clone`.
    - **1c-B** (`d17d98f`): `parse_statement` → `ast::Statement::{Read,Write}`; a
      narrow `write_statement` grammar (`MATCH (n:L {k:v}) SET n.p = <lit|param> …`)
      tried before the read grammar; `lower_write_statement` enforces the shape.
      `parse` unchanged (still rejects SET read-only when the layer is off).
    - **1c-C** (this commit): per-graph `DeltaWriter` registry in `Graphs`
      (`enable_writable_layer`, boot-gated on `delta.enabled`); RUN-handler dispatch
      (write → `execute_write`: resolve business key to dense id via ISAM →
      WAL commit → memtable apply → ack; read → `MergedView` over the pinned delta);
      `ResultKey` delta epoch; `delta_for_read` uuid guard (fail safe to pure core on
      a superseded generation). Read-your-writes + reopen-durability + error + epoch
      tests. Whole workspace green.
    - **Deferred out of 1c** (each a clean later refinement, none blocking 1d):
      `RETURN` after `SET` (rejected for now — read back with a separate `MATCH…RETURN`);
      re-resolving a live delta across a hot-reload swap (run `reloadStrategy=exit`);
      group-commit batching (WAL already supports it, writer commits per-op);
      labelled-scan fallback for an unindexed business key; edge + tombstone deltas
      (Phases 2–3).
  - **1d — consolidation (4a) + orchestrator.** `slater-build --consolidate`
    (dump-and-rebuild); freeze → spawn builder → retire delta on exit 0; end-to-end
    "write → read merged → consolidate → value in core, delta gone" + crash test. ⬜ TODO

- Phases 2–5: see `docs/WRITABLE-PLAN.md`.

- **Parallel workstream — per-graph dump CLI (`slater dump`). 📋 PLANNED, not started.**
  See `docs/WRITABLE-PLAN.md` §"Per-graph dump CLI". Independent of Phases 0–5 (does
  not gate them). **Decided:** Bolt-client transport (user/pass, honours ACLs — reuse
  `BoltConn` from `health.rs`, promote to shared); identity keys inferred from range
  indexes with `--key Label=prop` / `--pk` overrides; clap-derive args, password via
  stdin/env (no plaintext flag). Also a `-l`/`--list` mode: print the graphs the
  authed user can read (backed by `Acl::readable_graphs`, surfaced via a Bolt
  list-graphs call — verify/add). Distinct in code from Phase 4a's offline
  generation→MERGE serialiser (shares only the text format). NB: `vecf32` props can't
  ride a MERGE dump (vectors non-goal).

## Recommended context-clear points

Best stops are **right after a sub-milestone commit with all gates green**. In
descending preference:
1. After **1a** (WAL floor done) — clean, self-contained, easy to resume at 1b.
2. After **1b** (memtable+resolver done) — the pure `slater-delta` layer is then
   complete; 1c/1d are the server/builder integration.
3. After **1c** or **1d** — larger, but each leaves an end-to-end capability.

When stopping: ensure this file's Phase status checkboxes + the "next action" line
below are current, and that the latest commit hash is noted.

## Next action

Implement **Phase 1d — consolidation (4a) + orchestrator** (`slater-build` +
`crates/slater/`). Phase 1c gives durable property overwrites read-your-writes;
1d folds the frozen delta back into a fresh core so the memtable can be retired.
Steps:
1. `slater-build --consolidate` (dump-and-rebuild): serialise the frozen delta to
   business-key `MERGE` text (`Memtable::iter_nodes` is the input) and re-parse it
   through the existing overlay/merge build path (`overlay.rs` / `merge_build.rs`),
   layered on the current core generation → a new generation via
   `common::write_manifest_and_publish` (D14 atomic swap).
2. Orchestrator in the server: freeze the memtable (seal the WAL segment, open a
   fresh one), spawn the builder, and on exit 0 retire the frozen delta + rebuild
   the writer's `by_dense` empty against the new core (dense ids permute across the
   rebuild — see `DeltaWriter::core_uuid` and the `delta_for_read` uuid guard).
3. End-to-end test: write → read merged → consolidate → value is in the core and
   the delta is gone; plus a crash-between-freeze-and-swap replay test (the WAL
   still replays the write, so no data loss on a mid-consolidation kill).

Handy resume detail: the write flow lands in `server.rs` (`execute_write`,
`resolve_dense_id`, `delta_for_read`, `run_query`'s `ReadOverlay`); the writer is
`crates/slater/src/delta_writer.rs`; the read overlay is `exec.rs`
(`overlay_node_props`, `node_prop_par`).

