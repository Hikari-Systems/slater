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

- **Phase 1 — durable property overwrites + dump-and-rebuild consolidation. ✅ DONE.**
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
  - **1d — consolidation (4a) + orchestrator. ✅ DONE.**
    - **1d-A — merged-view → MERGE dump serialiser. ✅ DONE** (commit `ed16742`).
      `consolidate::serialise_merge_dump` reads a `ReadView`, so pointing it at a
      `MergedView` folds the delta in for free — the dump *is* the consolidated
      state and the builder runs unchanged (**key deviation from the plan: the
      serialiser lives in `slater` and reads the merged view, rather than the
      builder reading the generation offline — far less code and the delta fold is
      automatic**). Emits `CREATE INDEX` DDL + business-key `MERGE` nodes/edges in
      slater-build's default dialect; grammar-exact Cypher escaper; refuses (never
      corrupts) a node whose identity isn't recoverable from a range index. New
      `Engine::outgoing_adj`; `testgen::write_indexed_people` fixture.
    - **1d-B — orchestrator + end-to-end + crash test. ✅ DONE** (this commit).
      `DeltaWriter::freeze()` seals the live WAL segment, opens a fresh one, and
      returns `Frozen { snapshot, consumed }` (non-destructive — reads keep
      overlaying, so a failure/crash before publish loses nothing);
      `DeltaWriter::retire(consumed, new_core_uuid)` deletes the consumed segments,
      resets the memtable empty, and re-binds `core_uuid` (now `RwLock<GenId>`,
      published empty-snapshot-before-rebind so a lock-free reader never overlays a
      stale delta on the new core). `Graphs::consolidate_graph(name, cache,
      vector_cache, data_dir, build)`: freeze → dump the `MergedView(core ⊕ delta)`
      via `serialise_merge_dump` to `<data_dir>/<graph>/.consolidate.cypher` →
      `build(dump, graph, data_dir)` → `swap_if_changed` picks up + validates the new
      generation → `retire`. A builder failure is non-destructive (old core keeps
      serving, delta stays live, scratch dump removed). The `build` seam is an
      injected `Fn(&Path,&str,&Path)->Result<()>`; production = `run_builder`
      (spawns `delta.builder_bin`, default `slater-build`, `--input/--graph/--data-dir`);
      the automated test injects a closure that inspects the dump then publishes a
      known-correct generation via `testgen::write_indexed_people_at` (no
      impl-vs-impl parity). Tests: `consolidate_folds_delta_into_fresh_generation`
      (e2e orchestration), `failed_consolidation_preserves_the_write_and_old_core`
      (crash window = builder error before the `current` swap; WAL replays the write,
      old core served), `#[ignore] consolidate_via_real_builder` (spawns the real
      binary via `SLATER_BUILD_BIN`; verified green). Also hardened `wal.rs`:
      `WalSink::create` flushes the magic immediately + `replay_bytes` tolerates a
      0-byte segment (a fresh unflushed/torn-on-power-loss segment no longer wedges
      `replay_dir`). Whole workspace green; clippy + fmt clean.
      **Deferred (Phase 4/5):** the Bolt trigger (`CALL slater.consolidate()`) + the
      automatic L0-soft-cap trigger — `consolidate_graph` is a callable server method
      only for now; group-commit; the freeze-to-a-live-memtable "writes never block"
      admission control (Phase 1 runs consolidation on the single-writer path).

- **Phase 2 — new nodes + deletes (tombstones) + index overlay. 🔨 IN PROGRESS.**
  The overlay cases (`docs/WRITABLE-PLAN.md` §Read-merge overlay): tombstones
  suppress the core row on read; delta-born nodes get synthetic dense ids in
  `[core.node_count, …)`; range-index probes union core ISAM hits with matching
  delta nodes minus tombstones. Sub-milestones (each independently green + committed):
  - **2a — WAL delete op + memtable tombstone path (pure `slater-delta`). ✅ DONE**
    (this commit). `WalOp::DeleteNode { label, key, value }` (op-tag 2, encode/decode/
    replay round-trip) + `WalOp::business_key()` (variant-agnostic `(label,key,value)`
    accessor — `resolve_dense_id` + the test resolver no longer irrefutable-let on
    `UpsertNode`); `Memtable::delete_node` (tombstone the entry, drop its patches,
    index by dense id) + `apply` `DeleteNode` arm (shared live/replay path);
    `upsert_node` now clears the tombstone (LWW resurrect). No read-path effect yet —
    that's 2b. slater-delta tests: WAL delete round-trip, tombstone-then-resurrect,
    apply-vs-direct parity. Whole slater+slater-delta green, clippy+fmt clean.
  - **2b — tombstone read overlay + DELETE write path. ✅ DONE** (this commit).
    Grammar: `write_statement` now alternates `set_clause | delete_clause`
    (`[DETACH] DELETE var`); `WriteStmt.sets` → `WriteStmt.op: WriteOp::{Set(..),
    Delete{detach}}`; `execute_write` dispatches Delete → `WalOp::DeleteNode`
    (`resolve_dense_id` uses the new `business_key()`, so it resolves a delete's
    anchor unchanged). Read overlay (`exec.rs`): `scan_candidates` post-filters
    tombstoned dense ids via new `DeltaSnapshot::is_tombstoned` (covers every anchor
    scan — IdSeek/RangeEq/RangeRange/LabelScan/AllNodes/RelTypeScan) behind the
    empty-delta fast path; `run_single` gates **all** count/metadata fast paths on
    `delta.is_empty()` — with any live delta present it falls through to full
    execution (a tombstone removes a node from a count; a property patch on an indexed
    key would move it in the index — both make the manifest/index shortcuts wrong).
    Consolidation (`consolidate.rs`): `emit_node`/`emit_edges_from` skip a tombstoned
    node and its incident edges, so a delete survives a rebuild. Tests: parser
    lowers/rejects `DELETE`, WAL/memtable delete (2a), read-your-deletes +
    whole-label-count-drops + reopen-durability (`server.rs`), serialiser drops a
    tombstoned node+edge. **Known gap → Phase 3:** a *core* edge pointing at a
    tombstoned node still lets traversal reach it (`MATCH (a)-->(b)` where `b` is
    deleted) — the topology overlay is Phase 3; direct scans/lookups/counts are
    correct now. Whole slater+slater-delta+slater-build green; clippy+fmt clean.
  - **2c — delta-born nodes (synthetic dense ids). ✅ DONE** (this commit). A new
    **`MERGE`** anchor keyword is the create spelling (user decision): `MERGE (n:L
    {k:v}) SET n.p = x` = create-if-absent / patch-if-present; `MATCH … SET` stays
    update-only (absent → error, pointing at MERGE); `MERGE … DELETE` rejected.
    Memtable (`slater-delta`): `Memtable::with_synthetic_base(base)` (= core
    `node_count`); `upsert_node(resolved=None)` allocates one synthetic dense id
    `synthetic_base + born.len()` per identity, once (stable across re-upsert), pushed
    into a `born: Vec<ck>` (index = id offset, so allocation = WAL-replay order =
    deterministic); `by_dense` now holds synthetic ids too so `node_patch` resolves
    them uniformly; `node_identity_by_dense` (recover label/key/value) +
    `born_ids_with_label` + `synthetic_base`/`born_count` accessors on
    `Memtable`/`DeltaSnapshot`. Read overlay (`exec.rs`, all gated so the empty-delta
    path is untouched): `MergedView::node_count() = core + born_count`;
    `scan_candidates` LabelScan appends `born_ids_with_label` (AllNodes covered by the
    grown `node_count`); a synthetic id (`>= core.node_count()`) routes **all** reads
    to the delta only — `node_prop_par` (business key from identity, patches, else
    Null), `node_label_ids_par` (single identity label via `gen.label_id`),
    `node_props` (empty core props), `overlay_node_props` (seed the business-key prop
    then fold patches), `outgoing_adj` (empty — no edges yet). Writer: `DeltaWriter::
    open`/`retire` take the core `node_count` to seed/re-base `synthetic_base`.
    `execute_write`: `resolve_business_key` → `KeyResolution::{Unique,Absent,Ambiguous,
    Unindexed}`; a `MERGE` + `Absent` writes with `resolved=None` (create), every
    other absent/ambiguous/unindexed is a clear error. Consolidation: no code change —
    the `0..node_count` loop + synthetic-aware `node_record`/`outgoing_adj` emit a
    born node for free (test added). Tests: memtable (stable/replay-deterministic
    alloc, label filter, delete-survives), parser (MERGE lowers to upsert, MERGE+DELETE
    reject), server (`merge_creates_delta_born_node_and_survives_reopen`:
    create→label-scan-read-back→count-grows→patch-existing-no-dup→reopen-durability),
    consolidate (`serialise_emits_a_delta_born_node`). Whole slater+slater-delta+
    slater-build green; clippy+fmt clean; empty-delta bench within noise.
    **Known gap → 2d:** addressing a born node by an *indexed key seek*
    (`MATCH (n:L {k:v})`) misses it — the range-index probe overlay is 2d; a born node
    is found by a label scan / AllNodes until then. Also deferred: deleting a born node
    by business key (the core probe returns Absent → rejected; the memtable
    `delete_node` already handles it, just needs `execute_write` to resolve against the
    delta).
  - **2d — range-index probe overlay. ✅ DONE** (this commit). A range-index seek now
    overlays the delta: an equality/range seek finds a **delta-born** node and unions
    it into the core ISAM hits, and drops a tombstoned hit. Memtable (`slater-delta`):
    `born_ids_in_index_eq`/`born_ids_in_index_range` (+ private `born_ids_in_index`
    driver and `born_index_value`) return the synthetic ids of born nodes carrying the
    index's `label` whose indexed `property` satisfies the seek; comparison is
    `Value::cmp_key` (the ISAM total order), and the indexed value follows the read
    overlay's precedence — a patch wins over the business key (matches `node_prop_par`),
    else the business-key value when `property` *is* the key, else the node is absent
    from the index. Exposed on `DeltaSnapshot`. Read overlay (`exec.rs`,
    `scan_candidates` `RangeEq`/`RangeRange` arms, both behind the empty-delta fast
    path, mirroring 2c's `LabelScan` born-append): append the born ids matching the
    index predicate; new `node_index_label_prop(index)` maps an index name →
    `(label, property)` from the manifest. Born ids sort after every core id, so the
    ascending `scan_candidates` order holds; **tombstone drop on `RangeEq`/`RangeRange`
    was already in place since 2b** (the final `suppress_tombstoned` wraps every arm) —
    2d confirms it with a test. Tests: memtable (`born_index_overlay_eq_and_range`,
    `born_index_overlay_patch_wins_over_business_key`,
    `born_index_overlay_includes_tombstoned_for_caller_suppression`); server
    (`range_index_seek_overlays_born_and_tombstoned`: seek-finds-born +
    seek-drops-tombstoned + range-includes-born on the `write_indexed_people` fixture's
    `(Person, name)` index). Whole slater+slater-delta+workspace green; clippy+fmt
    clean; empty-delta bench within noise. **Known gap → follow-up (moved indexed
    value):** a *core* node whose property patch changes an **indexed** value is not
    relocated in the index — `RangeEq`/`RangeRange` still read the stale core ISAM
    membership for a patched core node (found at its old value, missed at its new one).
    The value *read back* is already corrected by the property overlay; only index
    *membership* is stale. Closing it needs the memtable to track each patched node's
    indexed value per index (remove-old/add-new) — deferred, as the plan anticipated.
    (Also still deferred from 2c: deleting a born node by business key — the core probe
    returns Absent → rejected; `delete_node` already handles it, just needs
    `execute_write` to resolve against the delta.)
  - **2e — consolidation folds delta-born nodes.** `serialise_merge_dump` already
    skips tombstoned nodes (done in 2b); the remaining work is emitting delta-born
    nodes — the `0..node_count` node/edge iteration must extend over the synthetic id
    range once 2c lands. (Small once 2c–2d are in.)

- **Phase 3 — topology (edge) overlay. ✅ DONE.** Closed the two open Phase-2 gaps: a
  core edge to a tombstoned node still traversed (2b), and delta-born nodes had no
  edges (2c). Relationships can now be created/deleted through the delta, are walkable
  in every traversal path, and survive consolidation. Sub-milestones:
  - **3a — edge WAL ops + memtable edge overlay (pure `slater-delta`). ✅ DONE**
    (this commit). WAL: `WalOp::{UpsertEdge,DeleteEdge}` (op-tags 3/4, names inline,
    encode/decode/replay round-trip; `patches` on `UpsertEdge` reserved for a later
    edge-property overlay); `WalOp::node_key()`/`edge_keys()` replace the old
    variant-total `business_key()` (a node op returns its single key, an edge op its
    `(src, reltype, dst)` — the two are mutually exclusive `Option`s). Memtable: an
    `edges: HashMap<edge-ck, EdgeEntry>` authoritative store keyed by `EdgeIdentity`
    `(src, reltype, dst)` names, with `out_adj`/`in_adj` dense-id read indexes and a
    `born_edges` allocation vector; `with_bases(node_base, edge_base)` seeds both
    synthetic id spaces (`edge_synthetic_base` = core `edge_count`, so a born edge id
    never collides with a core edge id `rel_record` reads). `upsert_edge` (idempotent
    by edge identity; **creates delta-born endpoint nodes** when an endpoint key is
    absent from the core — the MERGE-edge endpoint-create path — via
    `endpoint_dense_or_create`) and `delete_edge` (tombstone-only entry with no
    synthetic edge id to suppress a **core** edge, or flip a born edge; a no-op when an
    endpoint exists nowhere, resolved via `born_endpoint_dense`). Read accessors
    `out_edges`/`in_edges` return `DeltaEdge { other, reltype-name, edge_id: Option,
    tombstoned }` (reltype by **name** — the exec reader maps it to a core id, keeping
    the memtable core-agnostic); `iter_edges` recovers identity names for
    consolidation. `apply` now dispatches on a new `OpResolution::{Node(Option<u64>),
    Edge{src,dst}}` — the caller-resolved dense-id context (the memtable never touches
    the core); the `slater` resolver (`server::resolve_op`), `DeltaWriter::open`/`write`
    (`Fn(&WalOp)->OpResolution`, `write(op, OpResolution)`) and `open`/`retire`
    node/edge-count threading are updated to match, all still driven only by node ops
    (the edge write grammar is 3c). 10 new slater-delta tests (WAL edge round-trip,
    both-way indexing + synthetic id, idempotent re-MERGE, born-endpoint creation,
    core-edge tombstone-only, MERGE-then-DELETE, absent-endpoint no-op, resurrect,
    apply-vs-direct parity, `iter_edges` name recovery). Whole workspace green;
    clippy + fmt clean. **No read-path effect yet** — traversal overlay is 3b.
  - **3b — exec traversal read overlay. ✅ DONE** (this commit). Two new exec free
    fns: `overlay_adj(gen, node, outgoing, core)` folds the edge delta into a core
    adjacency list — drops core edges a delta tombstone suppresses (matched on
    `(reltype-id, neighbour)`) and any edge whose neighbour is a tombstoned **node**
    (closing the 2b core-edge-to-deleted-node gap), then appends the node's born edges
    (reltype **name** → core id via `gen.reltype_id`, skipped if the reltype is absent
    from the core — the write path requires it to pre-exist, mirroring born-node
    labels); `read_adj_overlaid(gen, cache, node, outgoing)` is the single overlay-aware
    directional reader (a born node has an empty core adjacency), behind the
    `delta.is_empty()` fast path. `Engine::{outgoing,incoming}` and the free
    `hops_par`/`neighbours_par` (parallel multi-hop + shortestPath BFS) now route
    through it, so every traversal path — sequential and parallel — applies the
    identical overlay; `Engine::outgoing_adj` (consolidation edge walk) delegates to
    `outgoing`, so 3d's born/tombstoned-edge folding falls out for free.
    `MergedView::edge_count` adds `born_edge_count`; `edge_props`/`edge_prop_par` return
    empty/Null for a born edge id (`>= core edge_count`), so a traversed born edge
    materialises as a `Relationship` with its type and no properties. Tests
    (`server.rs`, driving edges through `DeltaWriter::write` since the grammar is 3c):
    `edge_overlay_folds_born_and_deleted_edges` (born edge walkable both directions;
    deleted core edge stops walking both directions; unrelated delete leaves the born
    edge) and `edge_overlay_suppresses_edge_to_tombstoned_node` (a core edge to a
    `DELETE`d node vanishes on read). Whole slater+slater-delta green; clippy+fmt clean;
    empty-delta bench within noise of core.
  - **3c — relationship write grammar + write path. ✅ DONE** (this commit). Grammar
    (`cypher.pest`): `write_statement` now tries an `edge_write` alt first
    (`edge_merge = MERGE (a)-rel->(b)` create, `edge_delete = MATCH (a)-[r:R]->(b)
    DELETE r`) before the node arm — the shared `(node)` prefix means a node write
    only reaches its arm when no relationship follows. Reuses the read grammar's
    `rel_pattern`, validated at lowering (must be a single directed `-[:R]->`, one
    type, no var-length/props). Parser: `ast::{EndpointPat, EdgeWriteStmt, EdgeWriteOp}`
    + `Statement::WriteEdge`; `lower_edge_write` + a shared `endpoint()` helper (single
    label + one constant business-key prop, like the node anchor); a `DELETE` names the
    bound rel var (required), `DETACH`/undirected/var-length/edge-props rejected with
    clear messages. Write path (`server::execute_edge_write`): the reltype must
    pre-exist (`gen.reltype_id`, so the overlay can map it — mirrors born-node labels),
    both endpoints resolve via `resolve_endpoint` (`Unique`→dense, `Absent`→`None` for a
    MERGE born-endpoint create / DELETE no-op, ambiguous/unindexed→error); a MERGE whose
    endpoints are both core nodes is deduped against the existing **core** edge
    (`core_edge_exists` scans the src's `outgoing_adj` over an empty-delta view — a born
    duplicate is already prevented by the memtable's identity idempotency), so a re-MERGE
    of a core edge is a no-op. `writer.write(op, OpResolution::Edge{src,dst})`. Tests:
    parser (MERGE-edge create, params + ignored rel var, DELETE rel-var check, rejected
    shapes) + server (`edge_write_grammar_end_to_end`: create + walk, idempotent MERGE of
    a core edge, born-endpoint auto-create, DELETE, unknown-reltype reject;
    `edge_writes_survive_a_reopen`: created + deleted edges durable across a WAL replay).
    Whole slater+slater-delta+workspace green; clippy+fmt clean. See D46.
  - **3d — consolidation folds delta edges. ✅ DONE** (this commit). No production
    code change was needed: `serialise_merge_dump` walks `Engine::outgoing_adj`, which
    3b made overlay-aware, so born edges emit as `MERGE (…)-[:R]->(…)` lines and
    deleted / incident-to-tombstoned-node edges are dropped, and born nodes emit via the
    existing `0..node_count` loop. Tests added (`consolidate.rs`):
    `serialise_emits_a_delta_born_edge` (a born edge between two core nodes *and* one to
    a born endpoint node both round-trip, alongside the surviving core edge) and
    `serialise_drops_a_deleted_edge` (a deleted core edge is gone while both endpoint
    nodes survive). Refreshed the now-stale "Phase 3" comment in `emit_edges_from`.
    Whole slater+slater-delta+workspace green (determinism goldens included); clippy+fmt
    clean.

- **Phase 5 — Bolt consolidation trigger `CALL slater.consolidate()`. ✅ DONE** (this
  commit). The orchestrator (`Graphs::consolidate_graph` + `run_builder`) is now reachable
  from a client. Grammar (`cypher.pest`): a SOI/EOI-anchored `consolidate_call` /
  `consolidate_proc` rule — deliberately **not** in the read-only `read_proc` whitelist
  (consolidation mutates; see D47), so it is tried only in `parser::parse_statement`
  (writable-layer path) and, with the layer off, the read parser rejects the `CALL` as a
  forbidden write. Parser: new `ast::Statement::Consolidate`, returned by `parse_statement`
  when the input is exactly the trigger. Server: the RUN handler dispatches
  `Statement::Consolidate` → `execute_consolidate`, which clones the ctx seams and runs
  `consolidate_graph(…, run_builder)` on a `spawn_blocking` thread (never parks the Bolt
  reactor), returning the new generation id as a single `generation` column; a builder
  failure surfaces as a query `Failure`, non-destructively. `ConnCtx` gains `data_dir` +
  `builder_bin` (from `config.delta`). Tests: parser
  (`parse_statement_recognises_the_consolidate_trigger` — accepts the exact shape
  case/whitespace-insensitively, rejects args/YIELD/longer-name, and confirms the
  layer-off read parser rejects it); server (`bolt_consolidate_surfaces_a_builder_failure`
  — wiring + non-destructive error via a missing builder binary;
  `#[ignore] bolt_consolidate_trigger_folds_delta_via_real_builder` — true end-to-end
  through the real `slater-build`, verified green). Whole workspace green; clippy + fmt
  clean; empty-delta bench unaffected (the trigger is off the read path). See D47.

- **Phase 4 — L0 flush + backpressure. ✅ DONE.** Bounds delta growth and lets writes
  continue while a consolidation rebuilds the core, so the layer takes sustained write volume.
  Shipped as a **two-tier** compaction design (revised mid-phase after the O(core)-rebuild review
  — see the 4d bullets + D49/D50): cheap, frequent flush + L0→L0 compaction absorb the churn
  (O(delta)), and the expensive O(core) consolidation fires only rarely, at a **fraction of core
  size** (opt-in). User-confirmed scope: correctness foundation (4a) first, then the full L0 LSM,
  then admission/backpressure. Plan `~/.claude/plans/wise-wobbling-puppy.md`; design in
  `docs/WRITABLE-PLAN.md` §"Write path, admission, consolidation". Sub-milestones:
  - **4a — writes survive a concurrent consolidation. ✅ DONE** (this commit). Removes the
    Phase-1 "no writes during a build" restriction. `DeltaWriter::retire` no longer resets
    the memtable to empty (which dropped any write that arrived between `freeze()` and
    `retire()` from RAM); it now **rebuilds** the live memtable by `replay_dir` over the
    surviving *post-freeze* segments (the consumed set is the pre-freeze segments — freeze
    already rotated to a fresh one), applying each op through a new `resolve: impl Fn(&WalOp)
    -> OpResolution` param **bound to the new core**. WAL records are self-describing
    (business-key names), so re-resolution is automatic and a pre-freeze delta-born node
    (synthetic id) folded into the new core re-binds to its now-real dense id. No seal/rotate
    inside `retire` — a committed record is already fsync-durable, so the still-open segment
    replays fine and keeps taking appends. Rebuilt-snapshot-published-before-core-uuid-rebind
    (a reader seeing the new `core_uuid` also sees the re-resolved overlay). `consolidate_graph`
    passes `|op| resolve_op(new_gen, op)` using the freshly-swapped generation. No read-path
    change (freeze does not swap the live memtable; only the *dump* uses the frozen clone).
    Tests: `writes_during_consolidation_survive` + `post_freeze_write_reresolves_a_born_node_
    to_the_new_core` (`delta_writer.rs`); `consolidate_folds_delta_into_fresh_generation` +
    the `#[ignore]` `consolidate_via_real_builder` both now apply a post-freeze write inside
    the build closure and assert it is carried forward onto the new core. Whole
    slater+slater-delta+workspace green; clippy+fmt clean. See D48.
  - **4b — L0 segment format + reader. ✅ DONE** (this commit; pure `slater-delta`). An L0
    segment is a **frozen memtable spilled to disk**. `Memtable::serialise()` /
    `Memtable::deserialise()` (`memtable.rs`) round-trip the whole folded delta — interner
    name table (so identities' delta-local `SymbolId`s survive), every node/edge entry, the
    derived read indexes (`by_dense`/`out_adj`/`in_adj`) and the born-order vectors verbatim,
    entries + patches emitted in sorted/`BTreeMap` order so equal memtables serialise to
    identical bytes (a determinism property). New `crates/slater-delta/src/l0.rs`:
    `L0Segment::{write,open}` frames the body as `MAGIC("SLL0SEG1") ‖ crc32c ‖ body`, writes
    temp-then-`rename`+fsync (no torn reads), and verifies magic+crc+version on open, reloading
    the segment as an immutable `Arc<Memtable>` — so it answers the **full `DeltaSnapshot`
    read surface via the existing memtable methods** (no reimplementation). **Deliberate
    deviation from the plan:** the reloaded body is held **resident** (whole-file load), not
    off-heap `pread`+sparse-index; RSS is still bounded by the delta byte budget (never grows
    with core size), and the off-heap variant is a later pure-RSS refinement, not a correctness
    concern (and the format is freely changeable — no back-compat, an L0 segment lives only
    between a flush and the next consolidation). Synthetic-id stacking across levels is carried
    by each segment persisting its own `synthetic_base`/`edge_synthetic_base` + born counts, so
    4c rebases the active memtable past all levels. Tests (`l0.rs`): serialise/deserialise
    round-trips every read over a memtable exercising core-patch + born node + tombstone + born
    edge + core-edge-tombstone; empty round-trip; segment write→open; magic/checksum rejection.
    42 slater-delta tests green; clippy+fmt clean; slater unaffected (no wiring yet).
  - **4c-A — multi-level read merge in `DeltaSnapshot`. ✅ DONE** (this commit; pure
    `slater-delta`). `DeltaSnapshot` grows from a single `Arc<Memtable>` to
    `{ mem: Arc<Memtable>, l0: Vec<Arc<Memtable>> }` (sealed segments newest-first) and folds
    every read accessor across levels with last-writer-wins precedence `mem ⊕ newer-L0 ⊕
    older-L0`, behind the preserved empty fast path (`is_empty` = mem empty **and** every L0
    empty; the common no-flush path leaves `l0` an empty vector so it stays a single check).
    Two private level iterators (`levels_newest_first`/`levels_oldest_first`) drive the fold:
    **`node_patch`** now returns an **owned** merged `NodeDelta` — a core dense id's patches
    split across levels merge per-property newest-wins, a tombstone clears+deletes and a newer
    upsert resurrects (LSM tombstone semantics); a synthetic id lives in one level so its entry
    passes through (single-level fast path just clones the sole memtable's entry). The two exec
    call sites (`node_prop_par` @590, `overlay_node_props` @1642) already `if let Some(nd)`, so
    owned needed no change. **`is_tombstoned`** folds newest-first over just the tombstone flags
    (no patch clone — it's the hot suppression-filter path). Born-id sets (`born_ids_with_label`/
    `born_ids_in_index_{eq,range}`) **union** oldest-first (stacked synthetic ranges stay
    ascending, matching the core scan order); `born_count`/`born_edge_count`/`node_delta_count`
    **sum**; `synthetic_base`/`edge_synthetic_base` are the **min** (= core count);
    `node_identity_by_dense` takes the newest level touching the id. **`out_edges`/`in_edges`**
    (new `merge_edges`) dedup by `(reltype, neighbour)` newest-wins, so a born edge flushed to
    L0 and later deleted surfaces once tombstoned (the traversal overlay then suppresses it —
    no double-count/resurrect); output order is deterministic. New constructor
    `DeltaSnapshot::with_levels(mem, l0)` for 4c-B to publish flushed segments;
    `from_memtable`/`empty` keep `l0` empty. 8 new tests (`memtable.rs`) stacking two memtables
    directly (no flush needed): per-property merge, delete-newest-wins, re-MERGE-shadows-older-
    tombstone, born-id/born-index union across levels, edge LWW merge, is-empty fold, single-
    level parity. Whole workspace green (50 slater-delta + 569 slater); clippy+fmt clean;
    empty-delta bench: no change on every arm.
  - **4c-B — memtable→L0 flush + write-path born resolution + wiring. ✅ DONE** (this commit).
    `DeltaWriter::flush_to_l0()` seals the active memtable to an `L0Segment` under
    `<wal_dir>/<graph>/l0/<n>.l0` (fsync-durable), prepends it to the writer's L0 stack, rebases a
    fresh active memtable past every level (node **and** edge synthetic bases), rotates the WAL
    (seal + fresh segment) and deletes the pre-flush WAL segments (durable in the L0 file); a no-op
    on an empty memtable. The writer now publishes the whole delta as **one atomic**
    `RwLock<DeltaSnapshot>` (`republish`), so a lock-free reader can never straddle a flush (datum
    in neither/both levels, or a born id double-listed) — this replaces the Phase-1
    `RwLock<Arc<Memtable>>`; `snapshot()` still returns the active memtable via new
    `DeltaSnapshot::active_memtable`, `delta_snapshot()` returns the full pinned view for
    `delta_for_read`, and `l0_len`/`total_bytes` are diagnostics. `open` reloads existing L0 files
    (sorted, oldest→newest), seeds the active memtable's bases past them (max over levels), then
    replays the live WAL tail. **Write-path born resolution (crux part 2):** new non-mutating
    `Memtable::born_synthetic_for_identity` (resolves names via `Interner::get`, short-circuits a
    name absent from the interner) folded over the L0 levels by
    `DeltaWriter::born_synthetic_for_identity`; `execute_write`'s MERGE-`Absent` branch and
    `execute_edge_write`'s born-endpoint fallback (**after** the core-only duplicate check, which
    must see genuine core ids) consult it and write `resolved = Some(l0_synthetic_id)` on a hit,
    and the identical substitution runs on the WAL-tail replay (`resolve_with_l0` in `open`) so a
    reopen never duplicates. Consolidation folds L0 for free: `freeze` captures the levels into
    `Frozen.l0` (+ `consumed_l0`), the dump reads through `DeltaSnapshot::with_levels`, and
    `retire` (new `consumed_l0` param) deletes the consumed L0 files and clears the level stack.
    Born-edge re-`MERGE` is not separately de-duplicated (the read merge dedups edges by
    `(reltype, neighbour)` newest-wins; residue is a harmless `edge_count` over-estimate, gated off
    the count fast paths when the delta is non-empty). Tests: slater-delta
    (`born_synthetic_for_identity_resolves_only_born_nodes`); `delta_writer.rs`
    (`flush_to_l0_seals_memtable_and_reopen_reloads_l0`,
    `remerge_of_a_flushed_born_node_reuses_its_synthetic_id`); `server.rs`
    (`flush_to_l0_overlay_reads_and_born_reuse_survive_reopen` — index seek + label scan + core
    patch read back through the L0 level, re-MERGE reuse, reopen durability;
    `consolidation_folds_a_flushed_l0_level` — dump carries the flushed born node + core patch,
    retire deletes the L0 file + clears the stack). Whole slater+slater-delta+workspace green;
    clippy+fmt clean. Empty-delta read path is **cost-identical by construction** (the atomic
    publish clones one `Arc` + an empty `Vec`, exactly the old `from_memtable(snapshot())`, and
    the 4c-A per-node accessors are untouched); the `delta_overlay` numbers are
    machine-jitter-dominated on this WSL2 box (the /1000 arm swung +58%→+11% between two runs, wide
    CIs), so the no-cost claim rests on the code, not the noisy criterion delta. See D49.
    **Deferred to 4d:** a flush is not admitted during an in-flight consolidation (the in-flight
    guard), and the auto flush/soft-cap triggers.
  - **4d — two-tier compaction + admission/backpressure** (revised after the O(core)-rebuild design
    review; full rationale in `~/.claude/plans/wise-wobbling-puppy.md` §4d). Fold-into-core is a **full `slater-build`
    rebuild = O(core), not O(delta)** (~1h / ~180 GB dump on the 91M core), so a fixed-byte soft
    cap that auto-rebuilds is a lose-lose (frequent → write amplification; rare → hundreds of L0
    levels → read amplification). Split into two tiers:
    - **4d-i — L0→L0 compaction. ✅ DONE** (this commit; pure `slater-delta` + writer wiring, no
      auto-trigger yet — that's 4d-ii). `Memtable::merge_levels(newest_first)` folds a contiguous,
      stacked run of L0 levels into one equivalent memtable, **preserving every born id** (keeps the
      oldest level's base; born-id ranges are disjoint + stacked, checked by `debug_assert`). It
      folds newest-wins by an **interner-independent** identity key (`node_name_key`/`edge_name_key`
      = names + type-exact value bytes, so levels with different local symbol tables combine), then
      **replays** the folded state through the ordinary `upsert_node`/`delete_node`/`upsert_edge`/
      `delete_edge` paths — born entities in ascending id order with endpoints resolved explicitly
      (none re-allocated), core rows sorted by dense id, core-edge tombstones sorted by endpoint —
      so allocation + byte accounting reuse the single tested path and the output is deterministic.
      `DeltaWriter::compact_l0()` merges **all** current L0 levels into one (so there is only ever
      ≤1 L0 file → segment number and age agree, no reconciliation), writes the merged file,
      publishes the collapsed one-segment stack atomically (`republish`), and deletes the consumed
      files once the merged file is fsynced; a no-op with <2 levels. The active memtable + core are
      untouched, so born ids and any dense id already handed to a reader stay valid. Tests:
      slater-delta `merge_levels_matches_the_snapshot_fold` (read-equivalence vs
      `DeltaSnapshot::with_levels` over a 3-level run exercising core-patch/re-patch/delete, core
      tombstone, born nodes across levels, born-node re-MERGE, born edge + its delete — the only
      benign divergence is a tombstoned edge's unobservable `edge_id`, masked in the check) +
      `merge_levels_is_deterministic`; `delta_writer` `compact_l0_collapses_the_stack_preserving_reads`
      (2 levels → 1, reads unchanged, reopen). Whole slater+slater-delta green; clippy+fmt clean.
      **Policy note:** merge-all was the first-cut compaction policy; the size-tiered partial-run
      policy (with number-vs-stack-order reconciliation via oldest-slot reuse) landed as a later
      refinement — see the "Deferred follow-ups (post-Phase-5)" section.
    - **4d-ii-a — in-flight guard + auto flush + auto compaction. ✅ DONE** (this commit). The write
      path now self-maintains the cheap tiers. `DeltaWriter` gains a `consolidating: AtomicBool` +
      `begin_consolidation()`/`end_consolidation()`/`is_consolidating()`; `flush_to_l0`/`compact_l0`
      check it **under the inner lock** and no-op while set, so nothing mutates the L0 stack across a
      consolidation's freeze→retire window (which `retire` clears wholesale). `consolidate_graph`
      claims the guard before freeze (refusing an overlap) and releases it on every exit via an RAII
      `ConsolidationGuard` — covering both the auto trigger and the manual `CALL slater.consolidate()`.
      New `DeltaConfig.l0_compaction_trigger` (default 4; `memtableBytes` is the flush cap, default
      64 MiB) threaded through `ConnCtx`; after `execute_write`/`execute_edge_write` the RUN handler
      calls `maybe_maintain_delta` — flush the memtable if ≥ cap, then compact if the L0 stack ≥
      trigger, both on `spawn_blocking` (they fsync) and both failure-swallowing (the write already
      acked durably) and skipped while consolidating. Tests: `delta_writer`
      `consolidation_guard_suppresses_flush_and_compact` (guard no-ops flush+compact, second claim
      refused, resumes after release); `server` `write_path_auto_flushes_and_compacts` (1-byte cap +
      3-segment trigger drives flush-per-write then a collapse, born rows survive). Whole
      slater+slater-delta+workspace green; clippy+fmt clean.
    - **4d-ii-b — rare fraction-of-core consolidation + hard-cap throttle. ✅ DONE** (this commit).
      `maybe_maintain_delta` gains a third tier after flush/compact: when
      `delta_entity_count() ≥ deltaCorePercent% × core_entities` (core = the served generation's
      `node_count()+edge_count()`; `consolidation_due` does the `u128`-safe fraction maths) it
      **spawns a detached background consolidation** (`spawn_auto_consolidation` → the existing
      `execute_consolidate` path), so the ack never waits on the O(core) rebuild and 4a carries any
      writes that land during it. Expressed as a **fraction of core** (not a fixed byte count) so
      write amplification stays bounded independent of core size; **off by default**
      (`deltaCorePercent = 0`) because an auto-fired ~hour-long rebuild must be opt-in — operators
      set it, or keep using manual `CALL slater.consolidate()`. The `begin_consolidation` claim
      inside `consolidate_graph` is the real single-flight guard; a lost race surfaces as a benign
      "already in progress" (logged `debug`). New `deltaHardBytes` **hard cap**: a write that pushes
      total resident delta past it calls `throttle_until_drained` — ensure a consolidation is
      draining (kick one if not), then `await` headroom (yields the reactor; a client that blocks too
      long times out = the correct "saturated" signal), with a generous bound so a wedged rebuild
      can't hang a writer forever (then it proceeds over-cap with a loud `warn` — for a very large
      core the hard cap is advisory, the fraction trigger is what keeps the delta from getting
      there). Off by default (`deltaHardBytes = 0`). New `DeltaConfig.{delta_core_percent,
      delta_hard_bytes}` (+ `delta_entity_count()`/`edge_delta_count()` accessors) threaded through
      `ConnCtx`. Tests: `consolidation_due_is_a_fraction_of_core` (threshold logic incl. disabled /
      tiny-core / near-`u64::MAX` cases); `#[ignore] write_path_auto_consolidates_at_core_fraction`
      (full write→trigger→real-`slater-build`→drain→fresh generation, verified green). Whole
      slater+slater-delta+workspace green; clippy+fmt clean. **This completes Phase 4.** Deferred
      refinements: an off-peak *schedule* knob; size-tiered partial-L0 compaction; off-heap `pread`
      L0 (bounded-RSS reads without whole-file residency).

- **Parallel workstream — per-graph dump CLI (`slater dump`). ✅ DONE.**
  See `docs/WRITABLE-PLAN.md` §"Per-graph dump CLI". Independent of Phases 0–5 (does
  not gate them). **Decided:** Bolt-client transport (user/pass, honours ACLs — reuse
  `BoltConn` from `health.rs`, promote to shared); identity keys inferred from range
  indexes with `--key Label=prop` / `--pk` overrides; clap-derive args, password via
  stdin/env (no plaintext flag). Also a `-l`/`--list` mode: print the graphs the
  authed user can read (backed by `Acl::readable_graphs`, surfaced via a Bolt
  list-graphs call — verify/add). Distinct in code from Phase 4a's offline
  generation→MERGE serialiser (shares only the text format). NB: `vecf32` props can't
  ride a MERGE dump (vectors non-goal). Sub-milestones (each independently green +
  committed):
  - **dump-a — shared Bolt client + clap scaffolding + `--list`. ✅ DONE** (commit
    `998ec09`). Promoted `health.rs`'s private `BoltConn` to a shared
    `crate::bolt::client::BoltClient` (`connect(host,port,timeout)`,
    `login(agent,user,pass)` = HELLO+LOGON basic, `run_pull(query, db) -> (columns,
    rows)` = RUN[+`db` selector in `extra`]+PULL-all, plus `send`/`recv`/`request`);
    `health.rs::fetch_diagnostics` refactored onto it (old `BoltConn` deleted). New
    `crate::dump` module + `dump_subcommand(default_port)` wired into `main.rs` beside
    `diagnostics` (blocking Bolt client, runs before the tokio runtime). clap-derive
    `DumpArgs` (`graph` positional, `-l/--list`, `--host`, `--port`, `-u/--user`,
    `--password-stdin`); password from `SLATER_DUMP_PASSWORD` else stdin (never a
    flag). `--list` = `run_pull("SHOW DATABASES")` → print the `name` column (server
    already ACL-filters to the caller's read grants — no new proc needed). Graph selection
    rides the RUN `extra.db` key (`select_graph`). The graph data dump (schema + nodes +
    edges) bails with a clear "next milestone" message for now (`-o/--out`, `--key`,
    `--pk` land with it — kept off the struct until used so each commit is warning-clean).
    Tests: `dump.rs` unit (CLI-definition `debug_assert`, arg parsing, missing-target
    reject, env-var password). Verified e2e against a live server: `--list` prints the
    graph, wrong password fails cleanly, the dump path emits its stub. 582 slater + 53
    slater-delta green; whole workspace green; clippy `-D warnings` + fmt clean.
  - **dump-b+c — schema + identity-key resolution + node/edge dump + PsValue escaper.
    ✅ DONE** (commit `8adcbff`; planned dump-b and dump-c folded — they share the arg
    surface and the single buffered output sink, and splitting them would either
    leave a partial dump on disk or make `--out`/`--key`/`--pk` dead code between the
    two commits). Args grew `-o/--out`, `--key Label=prop` (repeatable), `--pk`.
    `fetch_schema` reads `SHOW INDEXES` (RANGE only; `entityType` NODE→inferred
    identity key + node `CREATE INDEX`, RELATIONSHIP→rel `CREATE INDEX`) + `CALL
    db.labels()`, then folds each label's resolved identity `(label, key)` into the
    node-index set so a `--key`/`--pk` key that is not itself range-indexed still gets
    a `CREATE INDEX` (the rebuild needs it indexed to resolve the business key).
    `Schema::key_for` precedence: `--key` > `--pk` > inferred. `dump_graph` buffers
    the whole dump (so a mid-dump failure never truncates a file): `CREATE INDEX` DDL
    first (nodes then rels, sorted → deterministic), then `MATCH (n) RETURN n` →
    `MERGE (n:L {k:v}) SET …` (identity label chosen from the node's labels in **sorted**
    order for determinism; a node with no resolvable key is a hard error naming the
    labels), then `MATCH (a)-[r]->(b) RETURN a, r, b` → `MERGE (a)-[r:T]->(b) SET …`
    (both endpoints' business keys, rel type + props from the Bolt `Relationship`
    struct). New `literal(&PsValue) -> Option<String>` + `format_float`/`quote_str`
    **mirror `consolidate::literal` exactly** (same builder dialect, different value
    type); a value with no MERGE-dump spelling (vector/temporal struct, map, bytes)
    yields `None` → the property is dropped from the `SET` with a stderr warning (the
    identity key value must be representable, else a hard error). **No header comment:**
    `slater-build` splits on `;` and has no comment syntax, so the dump is pure
    rebuildable statements. Tests: 9 new unit tests (escaper-vs-dialect, key-override
    parse + `key_for` precedence, node/edge emission over synthetic Bolt `Node`/`Rel`
    values incl. sorted-label pick, no-key error, vector-prop drop-with-warning, SET
    property sort). Verified e2e against a live server: `dump` reproduces the source
    dump verbatim and **round-trips content-hash-identical** through `slater-build`
    (`56529eec…`); `--pk`/`--key` overrides confirmed (a `--key` on a non-indexed
    property adds its `CREATE INDEX`). 591 slater + 53 slater-delta green; clippy
    `-D warnings` + fmt clean.
  - **dump-d — reproducible round-trip e2e + docs. ✅ DONE** (commit `db52515`). New
    `#[ignore]` integration test `crates/slater/tests/dump_roundtrip.rs`
    (`dump_round_trips_through_the_real_builder`): lays down the `write_indexed_people`
    fixture, serves it via `server::serve_with_listener` on an ephemeral loopback
    port (production wiring), runs the **real `slater dump` binary** over Bolt
    (`env!("CARGO_BIN_EXE_slater")`, password via `SLATER_DUMP_PASSWORD`, on a
    `spawn_blocking` task so the async server keeps running), asserts the emitted dump
    **byte-for-byte** against a known `EXPECTED_DUMP`, then — when `SLATER_BUILD_BIN`
    is set — feeds it to the **real `slater-build`** and asserts a fresh `current`
    generation is produced (closing the dump→build loop). Registered as a
    `required-features = ["testkit"]` `[[test]]` so `cargo test --workspace` skips it
    without the fixture feature. Verified green with the real builder (rebuilt 3
    nodes/1 edge). Run:
    `SLATER_BUILD_BIN=$CARGO_TARGET_DIR/debug/slater-build cargo test -p slater
    --features testkit --test dump_roundtrip -- --ignored`. **This completes the
    `slater dump` workstream.** README + `WRITABLE-PLAN.md` dump section carry a
    "STATUS: implemented" note. Whole workspace green; clippy (`--features testkit
    --all-targets -D warnings`) + fmt clean.

## Deferred follow-ups (post-Phase-5)

The "Smaller follow-ups" listed further down, each closed one small commit at a time
(independent of one another; ledger updated in the same commit).

- **Delete a delta-born node by business key** (deferred from 2c). ✅ DONE (`2a06264`).
  A `MERGE`-created (delta-born) node can now be `DELETE`d by its business key. Its DELETE
  anchor's core probe returns `Absent` (no core row), so `execute_write` used to reject it;
  it now resolves the born synthetic id from the delta and tombstones it. Resolution folds
  the **whole** delta (active memtable + every L0 level) via new
  `DeltaSnapshot::born_synthetic_for_identity` (newest-first `find_map`) →
  `DeltaWriter::born_synthetic_in_delta` — deliberately distinct from the L0-only
  `born_synthetic_for_identity` the MERGE-create reuse path uses: create relies on the active
  memtable's `upsert_node` idempotency, but a DELETE must also *see* an unflushed born node
  **and** recover the synthetic id of a **flushed** one, so the tombstone's `by_dense` mapping
  suppresses it on read (a flushed born node's live entry sits in an L0 level, not the active
  tombstone). Absent from core *and* delta ⇒ a clear no-such-node error; a `MATCH … SET` on an
  absent key is unchanged ("no … node to update … use MERGE"). The reopen path was already
  correct — `resolve_with_l0` in `open` refines a replayed `DeleteNode`'s `None` to the L0
  synthetic id via `op.node_key()` — and `Memtable::delete_node` was already the tombstone path
  (no change to either). Tests: `server::delete_removes_a_delta_born_node_by_key`
  (create → delete → gone from reads + whole-label count → absent-key errors → reopen-durable),
  `consolidate::serialise_drops_a_delta_born_then_deleted_node` (a born-then-deleted node is
  dropped from the dump, so the delete survives a rebuild). Whole slater (593) + slater-delta
  (53) + workspace green; clippy `-D warnings` + fmt clean.

- **Moved indexed value** (deferred from 2d). ✅ DONE (`a551907`). A **core** node whose
  property patch changes an **indexed** value is now relocated in the range index — a
  `RangeEq`/`RangeRange` seek finds it at the new value and drops it at the old. (The value
  *read back* was already corrected by the property overlay; only index *membership* was stale.)
  The overlay lives in `slater-delta`, mirroring 2d's born-index overlay, and the `exec`
  `scan_candidates` `RangeEq`/`RangeRange` arms just call it: **drop** a core ISAM hit whose
  patched value moved out of the predicate (`DeltaSnapshot::core_hit_survives_{eq,range}`), then
  **add** core nodes whose patched value moved in (`moved_core_ids_in_index_{eq,range}`, inserted
  in sorted position so the ascending scan order holds), before appending born ids. The **add**
  is the load-bearing case — a relocated node is absent from the core ISAM at its new value, so
  without the overlay it is never a candidate and a residual filter never sees it; the **drop** is
  defence-in-depth (a residual re-filter usually masks a stale hit, but a covering/count-cover
  scan has none — those are gated off with a live delta anyway). Membership uses the **merged**
  (newest-wins across levels) patched value via `node_patch`, so a node patched across L0 levels
  is judged once; candidates come from new `Memtable::core_ids_patched_on_index`. A shared
  `value_in_range` helper is factored out (born range now uses it too). All gated behind the
  empty-delta fast path (read-only path pays nothing). Tests: slater-delta
  `moved_indexed_value_relocates_a_patched_core_node` +
  `moved_indexed_value_uses_the_merged_value_across_levels` (single- and multi-level, eq + range,
  label/prop discrimination); `server::moved_indexed_value_relocates_a_patched_core_node` (patch
  Alice's indexed `name`→'Alicia': eq seek finds new / misses old, range relocates, reopen-durable).
  Whole slater (594) + slater-delta (55) + workspace green; clippy `-D warnings` + fmt clean;
  empty-delta read path untouched by construction (overlay is inside the `!is_empty` guard).

- **Edge properties** (deferred from 3c). ✅ DONE (`80f976b`). `MERGE (a)-[r:R]->(b) SET
  r.p = <lit|param>` now gives a **delta-born** relationship properties; a re-`MERGE` patches
  them in place, they read back via `RETURN r.p` (and materialise on a full `RETURN r`), and
  consolidation carries them into the rebuild. Grammar (`cypher.pest`): `edge_merge` gains a
  trailing `set_clause?`. Parser: `EdgeWriteStmt` gains `sets: Vec<(String, Expr)>`;
  `lower_edge_write` validates each `SET` targets the **named** relationship variable with
  literal/parameter values (unnamed rel, wrong var, or non-constant value → clear error).
  Write path (`server::execute_edge_write`): evaluates the patches onto `WalOp::UpsertEdge.patches`
  (already WAL-encoded + replayed). Read overlay: new `Memtable::edge_delta_by_id` (born edge id →
  its `EdgeDelta`, owning-level-scoped) + `DeltaSnapshot::{edge_patch_value, edge_patches}` (fold to
  the level whose stacked born-edge id range owns the id; tombstoned → empty); `exec.rs`
  `edge_prop_par` returns a born edge's patch value by name, `edge_props` maps its patches to
  `(key_id, value)` (so `rel_record`/consolidation carry them — a name absent from the core symbol
  table is still readable by `RETURN r.p` but drops from the id-keyed full view). `Memtable::
  upsert_edge` already stored patches (reserved 3a) — no memtable-write change. `DeltaEdge` is
  unchanged (props are fetched lazily by edge id, not carried on the traversal record).
  **Scope / deliberate deferral:** only **delta-born** edges carry editable properties. Patching a
  **core** edge's properties *in place* needs a distinct core-edge-id patch overlay (like the node
  patch overlay) + a write-path dedup change + replay resolution; a `MERGE` that carries a `SET`
  onto an existing core edge is **rejected** with a clear message (a bare re-`MERGE` of a core edge
  stays an idempotent no-op). Tests: parser (`merge_edge_lowers_set_properties`,
  `edge_set_requires_a_named_rel_var_and_constant_values`); slater-delta
  (`edge_properties_read_back_through_the_overlay`, `edge_properties_patch_then_tombstone`,
  `edge_properties_resolve_from_the_owning_level`); server (`edge_properties_end_to_end`:
  create-with-prop → read → re-MERGE-patch + second prop → core-edge-patch-rejected → reopen-durable);
  consolidate (`serialise_carries_delta_born_edge_properties`). Whole slater (598) + slater-delta
  (58) + workspace green; clippy `-D warnings` + fmt clean.

- **Off-peak consolidation schedule knob** (deferred from Phase 4d-ii-b). ✅ DONE (`215b3d4`).
  A new `delta.consolidateWindow` gates the **fraction-of-core** auto-consolidation to an off-peak
  window: when the delta reaches `deltaCorePercent`% of core but the current **server-local** time
  is *outside* the window, the background consolidation is deferred (logged `debug`); inside the
  window (or with no window set) it fires as before. The `deltaHardBytes` **hard-cap throttle is
  unaffected** — it still fires anytime as the OOM backstop, so a deferral can never OOM. **User
  decisions:** cron-ish expression, **server-local** time. New module `crate::cron_window`
  (`CronWindow::parse` → 5-field `minute hour day-of-month month day-of-week`; `*`/number/range
  `a-b`/list `a,c`/step `*/n`; dow `0`|`7` = Sunday; **hour granularity** — the minute field is
  validated for cron compatibility but does not narrow the window, so `"0 1-5 * * *"` and
  `"* 1-5 * * *"` both mean 01:00–05:59 daily). **Clock seam:** `CronWindow::contains(hour, dom,
  month, dow)` is a pure, clock-free predicate (tests drive it directly); the real clock is read
  only in `cron_window::local_now_hms` (chrono `Local`, so `chrono`'s `clock` feature is enabled for
  `slater`), and the server-side gate `window_permits(&window, now)` is likewise pure over an
  injected time. Config: `DeltaConfig.consolidate_window: String` (camelCase `consolidateWindow`,
  empty default = no gating), parsed once at startup in `serve_with_listener` so a malformed spec
  **fails at boot** (not silently); threaded onto `ConnCtx.consolidate_window: Option<CronWindow>`;
  `maybe_maintain_delta` consults it only for the fraction trigger. Tests: `cron_window` unit
  tests (hour-range window, lists/steps/weekday, dow-7, dom+month, empty=no-window, malformed
  rejects); `config::delta_consolidate_window_deserialises_and_defaults_empty`;
  `server::window_permits_gates_the_fraction_trigger` (None=always, inside fires, outside defers,
  weekday gating). Whole slater (606) + slater-delta (58) + workspace green; clippy `-D warnings` +
  fmt clean; read path + empty-delta bench untouched (write-path maintenance + config only).

- **Size-tiered partial-L0 compaction** (deferred from Phase 4d-i). ✅ DONE (`aea6f36`).
  `DeltaWriter::compact_l0` no longer merges the **whole** L0 stack; it now merges only a
  contiguous run of **similar-sized** levels, so a large level is not repeatedly rewritten with
  tiny new ones (write amplification). New pure `select_compaction_run(&sizes)` picks the longest
  maximal run whose byte sizes are all within `SIZE_TIER_RATIO` (=4)× of the run's smallest
  (length ≥ 2; ties → the oldest run); `None` for a healthy size ladder (no adjacent same-tier
  pair) — the policy is self-balancing (equal-sized flushes form same-tier runs that merge, and
  the merged results are themselves same-tier and merge in turn, bounding fan-out).
  **Number-vs-stack-order reconciliation:** a partial merge leaves several L0 files, so their
  on-disk numbers must still agree with age / born-id-base order (`open` sorts by number). The
  merged segment **reuses the run's oldest (minimum) file number** (overwriting that slot via
  `L0Segment::write`'s temp+rename) — its number *and* born-id base are the run's minimum, which
  is exactly the merged segment's base (`merge_levels` keeps the oldest level's base) — so number
  order stays == base order with **no change to `open`**. `merge_levels` (already generic over a
  contiguous run) is called on the sub-slice; born ids are preserved; the active memtable + core
  are untouched. Crash posture is unchanged from the merge-all policy (publish-before-delete
  protects live readers; a crash between writing the merged file and deleting the run's newer
  members leaves a redundant born-id range until the next compaction — a pre-existing limitation,
  not worsened). Tests: `delta_writer::select_compaction_run_picks_a_same_size_tier` (pure policy:
  ladder→None, equal→whole, big-newest excluded, oldest-tie, 4× boundary) and
  `compact_l0_merges_only_the_matching_size_tier` (three levels — two small + one 64-node big;
  only the small tier merges → `l0_len` 3→2, big untouched, born ids + reads preserved, a further
  compaction still correct, reopen-durable). Existing merge-all-shaped tests
  (`compact_l0_collapses_the_stack_preserving_reads`, the auto flush/compact server test) still
  pass (equal-sized levels are one tier → merged wholesale). Whole slater (608) + slater-delta
  (58) + workspace green; clippy `-D warnings` + fmt clean (MSRV 1.80 — `map_or`, not `is_none_or`);
  read path untouched (compaction is write-path maintenance; empty-delta bench unaffected).

- **Group-commit batching + write-`UNWIND`** (from the 1M-smoke fsync finding; the chosen
  alternative to the deferred off-heap L0 item). ✅ DONE (`6ed7bec` primitive, `bcb109d` surface). Bulk writes were
  one-fsync-per-statement (the 300K-delete smoke run took ~1h on WSL2, fsync-bound). Two pieces:
  - **`DeltaWriter::write_batch(&[(WalOp, OpResolution)])`** — the group-commit primitive: append
    every record, do **one** `commit` (a single fsync — the ack barrier for the whole batch),
    fold all ops into the memtable, and publish **once** (one epoch). Atomic on failure (the
    memtable is applied only after the fsync, so a failed append/commit rejects the batch whole —
    the un-committed records are dropped on replay). Empty batch = no-op.
  - **Write-`UNWIND`** (the client surface, and the first step of FalkorDB-parity UNWIND-as-write):
    `UNWIND $rows AS r MERGE|MATCH (n:L {k: r.k}) SET n.p = r.p` / `… DELETE n`. Grammar
    (`cypher.pest`): an optional leading `unwind_clause` on `write_statement`. Parser: `WriteStmt`
    gains `unwind: Option<(Expr, String)>` (source list + alias); constant-value enforcement is
    relaxed when an UNWIND is present (values may reference the alias's fields). Server:
    `execute_write` dispatches to `execute_write_batch`, which evaluates the `$rows` parameter list,
    builds one WAL op per row (key + SET values via `eval_row_value` — literal / param / `r` /
    `r.field`), resolves each via the **shared** `resolve_node_op` (factored out of `execute_write`
    so single and batched writes share the born-create / delete-born / error semantics), and
    applies the lot through `write_batch`. **Scope:** node writes over a **parameter** list; a
    within-batch create-then-delete of the same *new* key is not resolved (resolution is against
    core ⊕ delta as-of-batch-start — independent rows, the bulk-import case, are unaffected).
    **Deferred:** edge write-UNWIND, a literal-list source (needs a public `Value`→`Val`), a
    params-carrying Bolt client for a 1M throughput smoke, and the broader Falkor write grammar
    (CREATE, ON CREATE/ON MATCH, multi-clause). Tests: `delta_writer::write_batch_group_commits_and_survives_reopen`;
    `parser::write_unwind_lowers_a_batched_node_write`; `server::write_unwind_batches_node_writes_under_one_commit`
    (batched create → one epoch → per-row SET → reopen-durable → batched DELETE → one epoch → gone).
    Whole slater (611) + slater-delta (58) + workspace green; clippy `-D warnings` + fmt clean.

- **In-place core-edge property patching** (deferred from 3c). ✅ DONE (this commit). `MERGE
  (a)-[r:R]->(b) SET r.p = …` on an edge that **already exists in the core** now patches that core
  edge's properties **in place** (was rejected). It reads back via `RETURN r.p` / a full `RETURN r`,
  a bare re-`MERGE` stays an idempotent no-op, and consolidation carries the new value into the
  rebuild. Because a relationship is materialised only from an **edge id** (never its identity), the
  patch is keyed **by core edge id** — the node-patch overlay shape (`by_dense`) transposed to edges.
  slater-delta (`memtable.rs`): new `Memtable::patch_core_edge` stores the patch on a
  `synthetic_edge = None` entry (distinct from a **born** edge, which uses `synthetic_edge`, and from
  a tombstone-only entry, which carries neither), a new `by_edge_id` (core edge id → identity key)
  indexes it, and `EdgeEntry.core_edge` records the id (persisted; `by_edge_id` is **rebuilt** from it
  on `deserialise`, not serialised). `edge_delta_by_id` resolves a core edge id through `by_edge_id`;
  `DeltaSnapshot::{edge_patch_value, edge_patches}` now **fold newest-wins across levels** (a core edge
  may be patched in several L0 levels — unlike a born edge, whose id lives in exactly one). `OpResolution::Edge`
  gains `edge_id: Option<u64>`; `apply` routes `Some` → `patch_core_edge`, `None` → `upsert_edge`
  (born). `merge_levels` reconstructs a folded patch-only core edge via `patch_core_edge`. Write path
  (`server.rs`): `core_edge_exists` → `find_core_edge_id` (returns the id); `execute_edge_write`
  patches on a core-edge hit with `SET`, and **`resolve_op` re-resolves the core edge id against the
  *current* core on every replay** (never stored in the WAL — so a born edge folded into a fresh core
  correctly becomes a core-edge patch after consolidation). Read overlay (`exec.rs`): `edge_prop_par`
  overlays a core edge's patch over the core value; `edge_props` folds patches over the core record
  (replace/append) — so `rel_record`/consolidation carry them for free. A core-edge patch **does not**
  touch the born vector or the adjacency indexes (topology unchanged; only properties overlay). See
  D51. Tests: slater-delta (`core_edge_patch_reads_back_and_folds_across_levels`,
  `core_edge_patch_survives_serialise_roundtrip`, `merge_levels_preserves_a_core_edge_patch`); server
  (`edge_properties_end_to_end` extended: core-edge patch → read-back → bare-re-MERGE-no-op →
  reopen-durable); consolidate (`serialise_carries_a_core_edge_patch`). Whole slater (612) +
  slater-delta (61) + workspace green; clippy `-D warnings` + fmt clean; empty-delta bench within
  noise (all overlay code is behind the `!is_empty` guard).

## Perf finding — bulk-delete resolve is ISAM-block-decompress bound (2026-07-09)

**Context:** reran the `smoke_1m.rs` `delete_thirty_percent_segment` stress on the **1M-node
Wikidata core** (`SLATER_SMOKE_DATADIR=/home/rickk/perf-gens/wiki1m`, graph `wiki1m`), this time
through the **group-committed write-`UNWIND`** path (commit `04beb9e`): the test's `BoltClient`
gained `run_pull_params` (it previously could not send `$rows`, so it could not drive a batched
write at all — the deferred "params-carrying Bolt client" gap), and the delete loop now issues
`UNWIND $rows AS r MATCH (n:Entity {wikidata_id: r.id}) DELETE n` in 10 000-id batches
(`SLATER_SMOKE_DELETE_CHUNK`).

**Result:** ✅ PASSED — 300000/1000000 deleted in **30 group-committed batches** (30 fsyncs, not
300K), range seek over the segment → 0 rows, whole-label count → exactly 700000. Post-delete reads
stay fast (~1.5s each over the overlaid 1M core).

**Timing / finding:** wall-clock **875s (~14.6 min)** for the deletes — ~**4× faster** than the
per-statement ~1h, but now **CPU-bound at 100%, not fsync-bound**. Group commit collapsed the fsyncs
and thereby *unmasked* a pre-existing per-row cost: `resolve_business_key` (`server.rs`) →
`IsamReader::lookup_eq` → `read_block` (`crates/graph-format/src/isam.rs`) **decompresses a full
zstd index block (and decodes every entry) on every probe, with no cache**. The victims are a
contiguous `wikidata_id` range, so the *same* block is re-decompressed once per key in it — the
~875s floor is ~300K block decompressions for only a few thousand distinct blocks. This is
**orthogonal to group commit and to the core-edge / delta work**; it lives in the shared ISAM
equality-probe path.

**Recommended follow-up (well-scoped, not yet done):** a **batch-local resolve pass** in
`execute_write_batch` (`server.rs`) — collect the batch's key `Value`s, sort them, and resolve them
in **one indexed merge-join** that decompresses each ISAM block at most once, instead of an
independent uncached `resolve_business_key` per row. Keeps the concurrent single-probe read path
untouched (so no shared-cache thrash) and should bring the 30%-delete from ~15 min to seconds. A
lighter alternative — a small last-block cache on `IsamReader` — risks thrashing under concurrent
query reads (a single shared reader across threads), so the batch-local pass is preferred. NB the
`smoke_1m` deletes are **core-node** deletes (resolve to `Unique` core dense ids); the born-node /
whole-delta fold path (`born_synthetic_in_delta`) is *not* on this hot path.

**✅ RESOLVED (2026-07-09) — decoded-block cache + binary search (D52).** Profiling
(a `#[ignore]` `bench_resolve_business_key` micro-bench, since WSL2 ptrace is locked) showed the real
shape: the `wikidata_id` index is **27 blocks of ~37 000 entries**, so a resolve's cost is the
**decode + linear scan** of a whole block, *not* the read+decompress — a raw-byte cache gave only
**~15%**. Fixed by caching **decoded** blocks (`IsamReader` now holds a `DecodedBlockCache` —
byte-budgeted LRU of `Arc<Vec<(Value,u64)>>`, one per generation, `cache.rangeIndexCacheBytes`
default 16 MiB) and **binary-searching** the sorted cached block in `lookup_eq`/`lookup_range`.
Measured: **~2.6 ms → ~1.5 µs/resolve (~1750×)**; the full 30%-delete smoke **875s → 13.2s (~66×)**,
now bound by the 30 batch fsyncs, not the resolve. Transparent to `resolve_business_key`/
`scan_candidates` (reader-internal); off (`None` budget) for every non-server opener. The
**batch-local merge-join** idea above is now unnecessary for this workload (the cache handles the
contiguous re-probe).

**✅ Also done (D53) — smaller range-index leaf blocks.** The builder sized range ISAMs with the same
256 KiB `--block-size` as columnar files (→ ~37K entries/leaf); range indexes now take their own
`--range-block-size` (default **16 KiB**). Complementary to D52: the decoded cache makes a *warm* probe
O(log n) regardless of block size; smaller leaves make the *cold* path cheap — 1M int keys: 256 KiB →
~2836 µs/uncached lookup, 16 KiB → **~182 µs (~15×)** — and shrink each cache entry so the budget holds
more blocks. Only affects newly built generations (existing images unchanged until rebuilt);
determinism goldens are invariant. `bench_range_block_size_point_lookups` (`isam.rs`, `#[ignore]`).

## Off-heap L0 reads — the big deferred RSS item (🔨 IN PROGRESS)

Plan: `~/.claude/plans/offheap-l0.md`. **Goal:** stop holding whole sealed L0 delta
segments resident. Today `L0Segment::open` deserialises a whole segment into
`Arc<Memtable>` and `DeltaSnapshot` holds `Vec<Arc<Memtable>>`, so the L0 stack's RSS grows
with the delta byte budget. Make sealed levels read **off-heap**: resident sparse per-axis
directory + `pread`'d blocks through the shared columnar `BlockCache` (**user decision: one
shared cache/eviction domain, not a dedicated delta cache**). `#![forbid(unsafe_code)]` ⇒
`pread`, no mmap. No L0 back-compat (segments are ephemeral). Pure-RSS refinement, **not a
correctness concern**. Three green slices:

- **Phase A — the `LevelRead` seam. ✅ DONE** (this commit). Extracted the per-level read
  surface `DeltaSnapshot` folds over into a `trait LevelRead` (`slater-delta::memtable`,
  re-exported), with **owned** returns so an off-heap level can never leak a borrow into an
  evictable block; the hot tombstone-suppression path gets a dedicated `node_tombstoned(id)
  -> Option<bool>` that reads a single flag and never clones a patch set. `impl LevelRead for
  Memtable` (delegates to the inherent borrow-returning methods, cloning only on the two
  value accessors). `DeltaSnapshot.l0` is now `Vec<Arc<dyn LevelRead>>` and the fold
  iterators (`levels_newest_first`/`levels_oldest_first`) yield `&dyn LevelRead`;
  `with_levels`/`l0_levels` take/return the trait object. `DeltaSnapshot::node_identity_by_dense`
  now returns **owned** `Option<(String,String,Value)>` (3 `exec.rs` call sites adjusted:
  `label_id(&label)`, `kname.as_str() == key`, `overlay_named(named, &kname, …)`). The active
  memtable stays a concrete `Arc<Memtable>` (the writer mutates/clones it), so the empty-delta
  fast path and the `if self.l0.is_empty()` single-memtable fast paths are untouched — the
  read-path cost is unchanged by construction (a non-empty single-memtable delta pays at most
  one extra virtual call on the count/base/identity accessors, all gated behind
  `!delta.is_empty()`). No on-disk format change; L0 segments are still resident memtables
  coerced to `Arc<dyn LevelRead>`. Whole slater (612) + slater-delta (61) + workspace green
  (27 result groups, 0 failed, determinism goldens incl.); clippy `-D warnings` + fmt clean;
  empty-delta bench baseline re-established (~326 µs/3.5 ms arms, fast path unchanged).
- **Phase B — block-addressable off-heap format + `L0Reader: LevelRead`. ✅ DONE** (this
  commit; pure `slater-delta`, no server wiring yet). New `l0_offheap.rs`. **A segment is a
  directory** of block files + a resident `meta.bin`: `node.blk`/`adj_out.blk`/`adj_in.blk`/
  `edge.blk` are `graph_format::blockfile::BlockFile`s (one record per patched/born node,
  per node-with-edges, per delta-carrying edge — record order = key order), read through the
  **shared `graph_format::blockcache::BlockCache`** via `cache.record(reader, scope, sub, idx)`.
  Each section has a **resident sorted `u64` key column** (dense/edge id → record index); a
  point read binary-searches it — a **miss costs no I/O** (the hot tombstone/patch path), a
  **hit pages+caches one block**. `meta.bin` (MAGIC ‖ crc32c ‖ body) holds the scalars, the
  four key columns, and the secondary indexes. `write_segment` writes temp-dir-then-`rename`
  (atomic); `L0Reader::open(dir, scope, cache)` verifies+loads meta and opens the four readers.
  `Memtable::to_segment_data()` gathers the whole delta **through the memtable's own read
  methods** (`out_edges`/`in_edges`/`edge_delta_by_id`/born-index precedence), so the off-heap
  segment answers reads **identically** — proven by `offheap_reader_matches_resident_memtable`,
  a read-for-read parity assertion over the full `LevelRead` surface (node patch/tombstone/
  identity incl. misses, out/in edges, edge deltas, born-by-label, born-index eq+range,
  core-ids-patched, born-synthetic-for-identity) on a fixture exercising core-patch +
  moved-indexed-value + born nodes + tombstone + born edge w/ property + core-edge tombstone
  + core-edge patch. Plus empty round-trip + crc-rejection tests. **Scope/deferred:** the
  **secondary indexes stay resident** (born-by-label/index/identity, core-patched) — they back
  scan-planning + `MERGE`-reuse, are proportionally smaller, and re-hold born *values* resident
  (the only unbounded term left, insert-heavy only); blocking them too is a mechanical
  follow-up. The **hot read path and every per-entity payload are fully off-heap**. 64
  slater-delta tests green; clippy `-D warnings` + fmt clean.
- **Phase C — wire into writer/server. ✅ DONE** (this commit; **completes off-heap L0**).
  Gated behind a new `delta.offHeapL0` config flag (**default off** → the resident path is
  byte-identical to before). `DeltaWriter.WriterInner.l0` is now `Vec<L0Level>` where
  `enum L0Level { Resident(L0Segment), OffHeap { reader: Arc<L0Reader>, dir, bytes } }`, with
  `as_level()`/`level_arc()` (`&dyn`/`Arc<dyn LevelRead>` for bases/publish), `path()`
  (file for resident, **directory** for off-heap), `bytes()`, `resident_memtable()`
  (compaction only). Call sites: `open_with_cache` (new; `open` delegates with off/None — so
  the ~12 existing `open` call sites are untouched) reloads each segment in **whichever
  format it is on disk** (`open_l0_level`: a dir ⇒ off-heap via the cache + a path-derived
  scope, a file ⇒ resident) so the flag need not agree with the disk; `flush_to_l0` writes
  `l0_offheap::write_segment` (a directory) vs `L0Segment::write` per flag; `compact_l0` is a
  **no-op in off-heap mode** (`merge_levels` needs resident memtables — consolidation bounds
  the level count instead, so no L0→L0 compaction off-heap); `freeze` captures `Frozen.l0:
  Vec<Arc<dyn LevelRead>>` (dump reads through the merged view, no resident memtable);
  `retire`/`remove_if_present` now delete a **file or a directory**; `published_snapshot`/
  `resolve_with_l0`/`total_bytes` go through `L0Level`. **Shared cache** (user decision):
  `crate::cache::BlockCache` now holds `inner: Arc<GfBlockCache>` + a `gf()` accessor; the
  server creates the columnar cache **before** `enable_writable_layer` and threads `cache.gf()`
  in, so off-heap L0 blocks page through the **same budget + eviction domain** as columnar
  blocks (per-segment cache scope = a fixed-seed hash of the segment dir path, disjoint from
  the generation-UUID columnar scopes; a retired segment's scope is simply never queried again
  → its blocks age out of the LRU). New `DeltaConfig.off_heap_l0` (camelCase `offHeapL0`).
  Tests: all resident-path tests unchanged (614 slater + 64 slater-delta green);
  `offheap_flush_writes_a_directory_and_reads_survive_reopen` (flush → directory segment,
  core-patch + born-node reads via the off-heap level, compaction no-op, reopen-durable) +
  `offheap_freeze_captures_levels_for_the_dump` (freeze captures the sealed level as a
  `dyn LevelRead`, merged-view read). clippy `-D warnings` + fmt clean. See D54.

  **Deferred (documented):** the secondary scan/write indexes remain resident (Phase B
  note — insert-heavy born *values*). A cleanly-scoped follow-up, not a correctness gap.

- **Off-heap L0→L0 compaction (the disk-native streaming merge). ✅ DONE** (this commit;
  option (b) from the follow-up). `compact_l0` no longer no-ops in off-heap mode — it now
  merges a size-tier run via `slater_delta::l0_offheap::merge_run`, a **disk-native
  streaming** merge: the run is folded through a `DeltaSnapshot` (an empty active memtable
  carrying the oldest member's bases keeps `synthetic_base = min` correct) and each merged
  record is written out immediately through the new incremental `OffheapSegmentWriter`, so
  the merged payloads are **never all held resident** — peak RSS is the key columns +
  secondaries + a block window (not the whole merged result). Reuses the tested read/fold
  semantics (`node_patch`/`out_edges`/`edge_patches`) rather than re-implementing them;
  born ids preserved (disjoint + stacked); secondary indexes concatenated oldest-first
  (exact per-level semantics). **Cache-scope correctness:** a merge reuses the run's oldest
  segment *directory*, so a path-derived cache scope would collide with the pre-merge
  segment's stale blocks — fixed by persisting a **fresh unique scope** (a v4 UUID) in each
  segment's `meta.bin` at write, read back by `L0Reader::open(dir, cache)` (the `scope` param
  is gone), so a reopen is stable and every (re)write gets a distinct scope. Tests:
  `l0_offheap::merge_run_matches_the_snapshot_fold` (a stacked 2-level off-heap run merged →
  read-for-read equal to the `DeltaSnapshot` fold over the run, tiny blocks so payloads span
  multiple paged blocks) + `delta_writer::offheap_compaction_streams_and_collapses_the_stack`
  (3 same-tier flushes → compact collapses the stack, cross-level core-patch fold + disjoint
  born ids preserved, reopen-durable). 615 slater + 65 slater-delta green; clippy + fmt clean.

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

**Resume state:** on branch `writeable`, **all pushed** to `origin/writeable` (through `53c3a82`). **Phases 0–5 + `slater dump` CLI are ALL DONE.** The **bulk-delete
ISAM-resolve floor is RESOLVED** (D52 decoded-block cache + D53 smaller range-index blocks;
875s→13.2s — the batch-local merge-join resolve is now unnecessary). **Off-heap L0 reads
(the big deferred RSS item) are ✅ DONE — all three phases** (see the "Off-heap L0 reads"
section above): A the `LevelRead` seam (`65d3aa5`), B the block-addressable format + `L0Reader`
with proven read-for-read parity (`3194cb6`), C the writer/server wiring behind `delta.offHeapL0`
(default off) sharing the columnar `BlockCache` (`016aff3`, D54), plus off-heap **L0→L0
compaction** as a disk-native streaming merge (`merge_run`, option (b)). **Open follow-ups**
(all optional, none blocking): block the resident **secondary indexes** for insert-heavy
deltas; and (independent) removing the now-dead `FileKind::Range` cache variant. Latest commits:
- `016aff3` feat(delta): wire off-heap L0 into the writer/server (Phase C) — D54
- `3194cb6` feat(delta): off-heap L0 segment format + reader (Phase B)
- `65d3aa5` refactor(delta): LevelRead seam for off-heap L0 (Phase A)
- `79e9a51` perf(build): give range (ISAM) indexes their own smaller leaf-block size (D53)
- `549c276` perf(isam): cache decoded range-index blocks + binary-search them (D52)
- `04beb9e` test(delta): drive the 30%-delete smoke via group-committed write-UNWIND (found the ISAM-resolve floor)
- `136f316` feat(delta): in-place core-edge property patching (SET r.p on an existing core edge)
- `bcb109d` feat(delta): write-UNWIND batched node writes (group-commit surface)
- `6ed7bec` feat(delta): write_batch group-commit primitive
- `aea6f36` feat(delta): size-tiered partial-L0 compaction
- `215b3d4` feat(delta): off-peak window for the fraction-of-core auto-consolidation
- `80f976b` feat(delta): edge properties on delta-born relationships (SET r.p on an edge MERGE)
- `a551907` feat(delta): relocate a patched core node in the range index (moved indexed value)
- `2a06264` feat(delta): delete a delta-born node by business key (follow-up from 2c)
- `db52515` feat(dump): reproducible round-trip e2e test (dump-d)
- `8adcbff` feat(dump): schema + identity-key resolution + node/edge dump (dump-b+c)
- `998ec09` feat(dump): shared Bolt client + `slater dump --list` (dump-a)
- `8b0afac` feat(delta): fraction-of-core auto-consolidation + hard-cap throttle (Phase 4d-ii-b) — completes Phase 4
- `8c0f49b` feat(delta): in-flight guard + auto flush/compaction on the write path (Phase 4d-ii-a)

**No blocking next task.** Both the Phase 0–5 delta track and the `slater dump` workstream are
complete; all gates green (`cargo test -p slater -p slater-delta` = 612 + 61; `cargo test --workspace`;
clippy `-D warnings` incl. `--features testkit`; fmt; the `#[ignore]` real-builder e2es incl. the new
`dump_roundtrip`). If continuing, confirm scope with the user first. Remaining work is
optional/independent:
- **Deferred refinements** (each cleanly scoped, none blocking):
  - **off-heap `pread` L0 reads** — *materially larger than items 1–5*: `L0Segment::open` currently
    deserialises the **whole** segment into an `Arc<Memtable>` and `DeltaSnapshot` holds
    `Vec<Arc<Memtable>>`, calling `Memtable` methods per level. An off-heap variant means either
    reimplementing the **entire per-level read surface** (`node_patch`, `is_tombstoned`,
    `born_ids_*`, `core_ids_patched_on_index`, `out_edges`/`in_edges`, `edge_delta_by_id`, bases,
    counts …) against `pread` + a sparse in-RAM index, or refactoring `DeltaSnapshot` to hold a
    `dyn LevelRead` (resident **or** off-heap) and dispatch every accessor. Pure-RSS (the ledger
    notes L0 RSS is already bounded by the delta byte budget and never grows with core size; size-
    tiered compaction + the fraction trigger bound it further), **not a correctness concern** —
    recommended for its **own dedicated session**, not a tail-of-batch rush.
  - ~~in-place **core-edge** property patching~~ ✅ DONE — see the "Deferred follow-ups
    (post-Phase-5)" section above (`SET r.p` on an existing core edge patches it in place, keyed by
    core edge id; re-resolved against the current core on replay). See D51.
  - ~~**group-commit batching** for bulk writes~~ ✅ DONE — `write_batch` primitive + write-`UNWIND`
    surface (see "Deferred follow-ups" below). Edge write-UNWIND + the broader Falkor write grammar
    (CREATE, ON CREATE/ON MATCH) remain.
  **delete-a-born-node-by-key, moved-indexed-value, edge-properties, the off-peak
  consolidation-window knob, and size-tiered partial-L0 compaction are now ✅ DONE** (see "Deferred
  follow-ups" below). See the "Smaller follow-ups" list below.
Export
`CARGO_TARGET_DIR=/tmp/claude-1000/-home-rickk-git-hs-slater/6a6f382f-eb59-4b50-8ebb-050f63801623/scratchpad/target`
before building (if that scratch dir is gone, any writable dir works — a fresh full compile is
the only cost).

**Phase 4b is complete**: the L0 delta-segment format lands in `slater-delta`.
`Memtable::{serialise,deserialise}` round-trip the whole folded delta (interner names, all
node/edge entries, derived indexes, born vectors) deterministically, and `l0::L0Segment::
{write,open}` frame it on disk as `MAGIC ‖ crc32c ‖ body` (temp+rename+fsync, magic/crc/version
checked on open), reloading as an immutable `Arc<Memtable>` that answers the full
`DeltaSnapshot` read surface via the existing memtable methods. The reloaded body is held
resident (bounded by the delta byte budget); the off-heap `pread` variant is a deferred RSS
refinement (see the Phase-4 ledger note). No `slater` wiring yet.

**Phase 4c is complete** (A: multi-level read merge; B: flush + write-path born resolution +
wiring). `DeltaSnapshot` folds `mem ⊕ L0*` newest-wins (owned merged `node_patch`, LWW edge
merge, union born-id sets, min bases) behind the preserved empty fast path. The writer publishes
the whole `DeltaSnapshot` as one atomic `RwLock` swap (no reader can straddle a flush);
`flush_to_l0` seals the memtable to `<wal_dir>/<graph>/l0/<n>.l0`, rebases past all levels,
rotates+trims the WAL, and `open` reloads L0 (sorted) before the WAL tail. Born identity resolves
across levels via `Memtable::born_synthetic_for_identity` (non-mutating, interner-`get`-based)
folded by `DeltaWriter::born_synthetic_for_identity` — consulted by the live write path
(`execute_write` MERGE-Absent, `execute_edge_write` born-endpoint fallback after the core-only
dup check) and the replay path (`resolve_with_l0` in `open`), so re-`MERGE` of a flushed born
entity never duplicates. Consolidation folds+retires the levels (`Frozen.{l0,consumed_l0}`,
`retire(consumed_l0, …)`). Deferred (as in 4c-A): a born node whose **indexed** property is
patched in a newer level than where it was born is not relocated in the index (same class as the
2d "moved indexed value" gap; the value read back is still correct). **Rejected alternative:**
*partial-flush* (only core-keyed deltas spill; born entities stay resident) — dodges the
write-path change but degrades to no-L0 for insert-heavy loads, so it does not serve the
sustained-write goal L0 exists for.

Handy Phase-4c-B resume detail (landed): `DeltaWriter::{flush_to_l0() -> bool, delta_snapshot()
-> DeltaSnapshot, born_synthetic_for_identity(l,k,v), l0_len(), total_bytes(), republish()}`
(`delta_writer.rs`, published state is now `RwLock<DeltaSnapshot>`; `snapshot()` returns the
active memtable via `DeltaSnapshot::active_memtable`); free fns `published_snapshot`,
`resolve_with_l0`, `remove_if_present`, `l0_segment_paths_sorted`, `next_l0_number`. `Frozen`
grows `l0: Vec<Arc<Memtable>>` + `consumed_l0: Vec<PathBuf>`; `retire` takes `consumed_l0`.
`Memtable::born_synthetic_for_identity` + `DeltaSnapshot::{active_memtable, l0_levels}`
(`memtable.rs`). Server: `delta_for_read` → `writer.delta_snapshot()`; `consolidate_graph`
dump via `with_levels(frozen.snapshot, frozen.l0)`. See D49.

Handy Phase-4b resume detail (landed): `Memtable::serialise() -> Vec<u8>` /
`Memtable::deserialise(&[u8]) -> Result<Memtable>` (`slater-delta/memtable.rs`, with private
`w_*`/`r_*` wire helpers; format version `L0_FORMAT_VERSION = 1`); `l0::L0Segment`
(`slater-delta/src/l0.rs`, re-exported as `slater_delta::L0Segment`) — `write(&Memtable, path)`,
`open(path) -> L0Segment`, `.memtable() -> &Arc<Memtable>`, `.path()`. Tests in `l0.rs`.

Handy Phase-4a resume detail (landed): `DeltaWriter::retire(consumed, consumed_l0, new_uuid,
new_node_count, new_edge_count, resolve)` (`delta_writer.rs`; `consumed_l0` added in 4c-B) — the `resolve` param is
`|op| resolve_op(new_gen, op)` supplied by `Graphs::consolidate_graph` (`server.rs`) from
`self.get(name)` post-swap. `freeze` unchanged (seals + rotates; `consumed` = pre-freeze
segments). Tests in `delta_writer.rs` + the two server-side consolidation tests. See D48.

Handy Phase-3 resume detail (all landed): memtable edges
`Memtable::{upsert_edge,delete_edge,out_edges,in_edges,iter_edges,with_bases}` +
`DeltaEdge`/`OpResolution` (`slater-delta/memtable.rs`); read overlay
`overlay_adj`/`read_adj_overlaid` (`exec.rs`, used by
`Engine::{outgoing,incoming,outgoing_adj}` + `hops_par`/`neighbours_par`); grammar
`edge_write`/`edge_merge`/`edge_delete` (`cypher.pest`) → `parser::lower_edge_write`
→ `ast::EdgeWriteStmt` (`Statement::WriteEdge`); write path
`server::{execute_edge_write, resolve_endpoint, core_edge_exists}`; consolidation is
overlay-transparent (`consolidate.rs` unchanged). See D45 (MERGE-vs-MATCH), D46 (edge
write grammar).

Smaller follow-ups that are **not** the recommended next step but are cleanly scoped:
- ~~**edge properties** (deferred from 3c)~~ ✅ DONE — see the "Deferred follow-ups
  (post-Phase-5)" section above (`SET r.p` on an edge `MERGE` gives a **delta-born** edge
  properties; core-edge in-place patch stays deferred).
- ~~**moved indexed value** (deferred from 2d)~~ ✅ DONE — see the "Deferred follow-ups
  (post-Phase-5)" section above (`scan_candidates` drops moved-out core hits and adds
  moved-in ones via the `slater-delta` overlay; membership uses the merged patched value).
- ~~**delete a born node by business key** (deferred from 2c)~~ ✅ DONE — see the
  "Deferred follow-ups (post-Phase-5)" section above (`execute_write` resolves a `DELETE`
  anchor against the whole delta when the core probe returns Absent).
- ~~off-peak *schedule* knob for consolidation~~ ✅ DONE — see the "Deferred follow-ups
  (post-Phase-5)" section above (`delta.consolidateWindow`, cron-style, server-local, gates
  the fraction trigger; hard-cap throttle unaffected).
- Phase 4 auto L0-soft-cap trigger (the manual trigger now exists — see below); the
  independent `slater dump` CLI (§ above).

Handy Phase-5 resume detail (all landed): grammar `consolidate_call`/`consolidate_proc`
(`cypher.pest`, not in `read_proc`) → `parser::parse_statement` → `ast::Statement::
Consolidate`; RUN-handler dispatch → `server::execute_consolidate` (clones ctx seams,
`spawn_blocking` → `Graphs::consolidate_graph(…, run_builder)`, returns a `generation`
column); `ConnCtx.{data_dir,builder_bin}` supply the seam. See D47.

Handy resume detail (2d landed): `Memtable::born_ids_in_index_eq`/`born_ids_in_index_range`
(+ `born_ids_in_index`/`born_index_value`) in `slater-delta/memtable.rs`, exposed on
`DeltaSnapshot`; read overlay in `exec.rs` `scan_candidates` `RangeEq`/`RangeRange`
arms + `node_index_label_prop(index)` (manifest name → `(label, property)`), gated by
the empty-delta fast path; tombstone drop is the pre-existing final
`suppress_tombstoned`. Grammar/write-path resume detail for born nodes (2c): MERGE in
`cypher.pest` (`kw_merge`, `write_statement` anchor alt) + `WriteStmt.upsert` +
`lower_write_statement` (`parser.rs`); `resolve_business_key`→`KeyResolution` +
`execute_write` create path + `DeltaWriter::open`/`retire` node_count threading
(`server.rs`/`delta_writer.rs`). See D45 for MERGE-vs-MATCH semantics.

