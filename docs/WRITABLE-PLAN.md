# Slater writable layer — a read-favouring LSM over the immutable core

Living design doc for the `writeable` track (a long-lived branch off `main`).
Mirrors the style of `docs/PLAN.md`; grows phase by phase. Newest decisions are
also mirrored as `### D<N>` entries in `docs/DECISIONS.md` (D43, D44 so far).

## Context

Slater is a read-only graph engine: `slater-build` compiles a business-key `MERGE`
dump into an immutable, content-hashed *generation* directory, and the `slater`
Bolt server serves that generation read-only with bounded resident memory. This
track adds a **writable layer that still favours reads** — a log-structured-merge
(LSM) tree layered over the existing immutable generation as the fully-compacted
**core**:

- the immutable generation = the **core** (bottom, fully-compacted level);
- writes accumulate in a **WAL + in-RAM memtable**, spilling to zero or more
  immutable **L0 delta segments** when the memtable fills;
- a **consolidation rebuild** (major compaction, fan-in two) folds
  `{core + frozen delta layers}` into a new core, published via the atomic
  generation swap that already exists.

Reads over the core stay exactly as fast and memory-flat as today; the only read
tax is a small merge proportional to *delta* size, not graph size. Writes are cheap
(WAL append + memtable upsert) and are explicitly **not** optimised beyond "allowed
and durable". An **empty delta is a zero-cost read fast path**.

### Confirmed decisions (from the user)

- **Single writer** for the core track (drain a channel on one thread). Multi-writer
  / shared transport is a deferred appendix, gated behind a later explicit decision.
- **Durability: batch-fsync (group commit).** Bolt `SUCCESS` is returned strictly
  after the fsync that covers the write, so *acknowledged ⇒ durable*. No
  per-statement knob until a deployment asks for one.
- **Consolidation trigger: automatic on the L0 soft cap + a manual lever.**
- **Full deletes (tombstones):** node and edge deletes as tombstone records,
  suppressed on read across memtable/L0/core, applied as real removal at
  consolidation.
- **Edge writes land in Phase 3** (topology overlay is the hardest read-path piece,
  de-risked after the simpler property/new-node/delete cases ship).

## Non-negotiable invariants (confirmed against the code)

1. **Dense ids are per-generation and unstable** (`graph-format/src/ids.rs`;
   `cluster` permutes them). Delta records **never** reference core entities by
   dense id.
2. **Identity is the business key**, stored as a real indexable property: node =
   `(label, key-prop, value)`, edge = `(src-key, reltype, dst-key)`
   (`docs/MERGE-DUMP-FORMAT.md`, `merge_build.rs`). Business-key → dense id is one
   ISAM equality probe (`NodeScan::RangeEq` → `IsamReader::lookup_eq`).
3. **The core is immutable for a generation's life**; a new state is a new
   generation. Caches key on the generation UUID (`cache.rs`).
4. **The atomic swap already exists — reuse it** (`common::write_manifest_and_publish`,
   D14: temp → fsync → rename → swap `current` last; server guard `swap_if_changed`
   / `guard_sweep`; one `Arc<Generation>` pinned per query).
5. **Bounded resident memory** — the delta layer carries its own explicit byte
   budgets and never grows resident memory with core size.

### Two value comparators — pick deliberately

- `Value::cmp_key` (`ids.rs`) — numeric-coercing (`1` collates with `1.0`); the
  ISAM / range-index / `ORDER BY` order. Used for the memtable's per-index range
  structure.
- `value_cmp_exact` (`merge_build.rs`) — type-exact (`{id:1}` ≠ `{id:1.0}`);
  business-key identity. Used for identity-keyed memtable maps and consolidation
  dedup. The delta's canonical identity encoding (`slater-delta/src/identity.rs`)
  uses `graph_format::wire::write_value`, whose byte image already distinguishes
  `Int(1)` from `Float(1.0)`.

## Crate layout

New crate **`slater-delta`** — the shared owner of the delta byte formats (WAL
record format, L0 segment format, delta-index format) and the read-merge fold
logic, analogous to how `graph-format` owns the core format. Both binaries depend
on it. Forbids `unsafe`. Modules:

- `identity` — business-key `NodeIdentity`/`EdgeIdentity` + canonical type-exact
  encoding. *(landed, Phase 0)*
- `interner` — delta-local first-seen symbol interner (mirror of
  `slater-build`'s `shared::Interner`), reconciled at consolidation. *(landed)*
- `memtable` — `NodeDelta`/`EdgeDelta` (LWW patches + tombstone), the single-writer
  `Memtable`, and the read-side `DeltaSnapshot` with the zero-cost `is_empty` fast
  path. *(landed as the read-side scaffold; mutation lands Phase 1)*
- `wal` — the WAL; see **WAL durability tiers** below. *(placeholder; Phase 1)*
- `l0` / `segment` — ISAM-shaped L0 segment format. *(Phase 4)*

## Read-merge overlay — the `ReadView` seam (D43)

The overlay lives *below* the executor's read surface (option A — storage-reader
overlay). `crates/slater/src/read_view.rs` defines `ReadView`, the trait surface the
executor (`exec.rs`) and planner (`plan.rs`) read through:

- `Generation` implements it as an identity pass-through (delta always empty);
- `MergedView { core: &Generation, delta: DeltaSnapshot }` overlays the delta.

`Engine` is **generic** (`Engine<'g, V: ReadView>`), not `&dyn`: the read-only path
monomorphises to `Engine<'_, Generation>` (byte-identical codegen to before, no
vtable); the empty-delta path inlines its forwards. Proven by the `delta_overlay`
bench (`cargo bench -p slater --features testkit --bench delta_overlay`): the
empty-delta arm is within noise of the core arm.

Cases layered into `MergedView`'s method bodies over the phases:

- **Property overwrite on a core node** (Phase 1) — probe delta by business key;
  overlay patched keys.
- **Delta-born nodes** (Phase 2) — synthetic dense ids in
  `[core.node_count, core.node_count + new_count)`; scans yield core ids then
  synthetic; property/label reads for a synthetic id route to the delta.
- **Tombstones** (Phase 2) — suppress the core row on read; real removal only at
  consolidation.
- **Range-index probes** (Phase 2) — union core ISAM hits with matching delta
  nodes, minus tombstoned; equality is a hashmap probe, range a merge-walk against
  the memtable's per-index `BTreeMap`.
- **Topology / new edges** (Phase 3) — concatenate the core CSR record with an
  in-memory adjacency delta keyed by the resolved current-core dense id (resolve
  each endpoint's business key once via ISAM, cache for the delta's lifetime).
- ~~**Vectors** — out of scope for the delta; served from the core (the write grammar
  rejects `vecf32`).~~ **Superseded (D63).** Embeddings are writable and span the whole
  ladder: `SET n.embedding = vecf32([…])` lands in the delta, is immediately KNN-visible
  with *exact* rank, survives a T2 flush and a T3 merge, and is carried through a
  consolidation. `REMOVE n.embedding` takes the node out of the index for good. The one
  thing a row cannot express is a *removal* — an indexed embedding was never in the props
  record (D12), so a row that lacks one is ambiguous — hence the `vec.meta` sidecar. Note
  the *text* `MERGE` dump still cannot carry an embedding and refuses a vector-carrying
  graph outright; consolidation takes the binary path.

**Cache correctness:** the block cache is safe (core blocks immutable, keyed on core
uuid). The *result* cache keys on `gen.uuid()` only, so a delta mutation without a
uuid change would serve stale query results — Phase 1 extends `ResultKey` with a
monotonic delta epoch.

## WAL durability tiers (correction; D44)

The WAL is split across **two seams with contradictory contracts** — do not fold the
WAL into the backend contract.

- **`WalSink` — the local durability floor.** Ordered, append-structured,
  fsync-durable at sub-ms. Local disk is the only medium that honours this contract;
  **it is not parameterised by the storage backend.** A record never travels through
  `ObjectStore`. A write is acked to the Bolt client only after `sync()`
  (group-commit fsync) resolves.

  ```
  WalSink { append(records) -> local append;  sync() -> group-commit fsync (the ack point);
            seal_current_segment() -> roll to a fresh segment, return the immutable one }
  ```

- **`ObjectStore` — shipping of sealed segments.** Sealed segments ship as
  **numbered, immutable, content-addressed** objects (never one growing object —
  S3/GCS have no append), with `wal/HEAD` written **last** as the copy-completeness
  barrier — the same pointer-last discipline as `current`. Reuses `ObjectStore::put`
  verbatim; **no WAL-shaped methods are added to the trait.**

  ```
  ship_segment(store, seg):  store.put("wal/<seq>.seg", seg.bytes, seg.sha256)   // immutable, once
                             store.put("wal/HEAD", head_pointer(seg.seq))        // pointer-last barrier
  recovery: read HEAD → list segments up to it → replay in order (idempotent: business-key
            upserts + tombstones are LWW, sequence numbers dedup, so replay-twice is a no-op)
  ```

So `fs`/`s3`/`gcs` governs **only the shipping tier**; the floor is always local.
Load-bearing consequences:

- **Truncation gate** — a local segment is not retired until its object-store PUT is
  acked (trivially satisfied on a local-disk-only deployment: the floor *is* the
  durable store).
- **Freeze forces a flush** — consolidation (§consolidation) reads the frozen delta
  from the object store, so freeze ships the frozen WAL tail (and any un-shipped L0)
  *before* spawning the builder, overriding the periodic timer.
- **The writeback interval is one knob with two faces** — object-store RPO **and**
  cross-replica read-visibility lag. A process crash loses nothing (local replay);
  losing the writer's local volume loses at most one interval of un-shipped writes.
- **Deployment** — the writer node is stateful: it needs a **durable local volume**
  for the WAL floor (not ephemeral instance storage). Read replicas stay stateless.

Acceptance: `WalSink` append/fsync touches only local disk; on `s3`/`gcs`, sealed
segments appear as numbered immutable objects with `HEAD` last, and a crash after a
segment PUT but before `HEAD` leaves recovery ignoring the orphan; kill-9 after a
local `sync()` but before shipping still replays the exact acked state from local
disk.

## Write path, admission, consolidation (Phases 1–5, summarised)

- **Write ingestion** (`slater`) — relax the parser rejection
  (`parser.rs::lower_single_query` / `lower_call_clause`); parse to the `model.rs`
  AST → WAL append → batch-fsync → memtable apply via the single writer → SUCCESS.
  Minimal, business-key-shaped grammar; not general Cypher writes.
- **Admission control** — WAL append is unconditional first; within budget → accept;
  over budget & L0 < hard cap → flush-and-rotate to L0; else (hard cap + a
  consolidation already in flight) → throttle. "Full" means rotate, never block.
- **Consolidation orchestrator** (`slater`) — freeze (ArcSwap a fresh memtable, seal
  the WAL segment, **ship the frozen tail**, snapshot the input set) → spawn
  `slater-build --consolidate` as a child process (fork+exec, never `libc::fork`) →
  on exit 0 the child has renamed the new generation and swapped `current`; retire
  consumed WAL + L0, keep post-freeze L0 (business-key keyed, stacks onto the new
  core). Non-zero exit keeps serving the old core.
- **Builder consolidation** (`slater-build --consolidate`) — Phase 4a
  dump-and-rebuild (serialise core → business-key `MERGE`, append the frozen delta,
  run `build_external` unchanged); Phase 4b generation-as-input streaming merge (the
  real version; needs the interner promoted out of `slater-build`). Both preserve
  byte-identical determinism and carry `CREATE INDEX` DDL forward.
- **Client-facing ids** — expose the business key as `element_id`; keep the numeric
  id ephemeral and generation-scoped.

## Per-graph dump CLI (`slater dump`) — operator tool

> **STATUS: implemented** (`crates/slater/src/dump.rs`; shared client
> `crates/slater/src/bolt/client.rs`; e2e `crates/slater/tests/dump_roundtrip.rs`).
> `--list` + full schema/node/edge dump ship; round-trip verified
> content-hash-identical through `slater-build`. See the `slater dump` sub-milestones
> (dump-a … dump-d) in `docs/WRITABLE-PROGRESS.md`. The text below is the original
> design; where the implementation deviated it is noted in the ledger (notably: no
> header comment — `slater-build` has no comment syntax; `--list` reuses `SHOW
> DATABASES` rather than a new proc; the escaper mirrors `consolidate::literal` on
> `PsValue`).

A per-graph exporter that dumps a graph from a **running** server to
slater-build-compatible **business-key `MERGE` Cypher**, so a graph can be
round-tripped (dump → `slater-build` → new generation), migrated, or backed up in
text form. Folded in at the user's request; it is an **independent operator-tool
workstream** (does not gate Phases 0–5) that shares the `MERGE` dump *format* with,
but is distinct in code from, the Phase 4a consolidation serialiser.

**Transport — Bolt client (decided).** Not offline/in-process: it connects over
Bolt, authenticates, and honours per-graph ACLs, so it works against a live
deployment without disk access. Reuses `BoltConn` (currently private in
`health.rs` — promote to a shared `bolt` module) and the existing `basic`-scheme
flow: `HELLO → LOGON {scheme:"basic", principal, credentials} → RUN/PULL`, checked
by `Acl::verify`/`can_read`. *(Consolidation Phase 4a needs an **offline**
generation→`MERGE` serialiser instead — same text format, different data source
(generation readers, not Bolt records); built separately there.)*

**Args (clap-derive; align the workspace on one arg style).** The existing `slater
query` subcommand hand-rolls `std::env::args()` while `slater-build` uses
clap-derive — standardise new tools on clap-derive and (recommended) migrate
`slater query` to match, sharing flag names:
- `graph` — positional or `--graph` (required **unless** `--list`).
- `-l`/`--list` — connect + authenticate, then print the graph names the
  authenticated user may read (one per line, to stdout) and exit; no `graph` arg
  needed. Backed server-side by `Acl::readable_graphs(user)` (`acl.rs:88`) —
  surfaced over Bolt by a list-graphs call (verify an existing one, e.g.
  `SHOW DATABASES` / `CALL db.info` / a `slater.graphs()` procedure; else add a
  small read-only introspection proc that returns the caller's readable graphs).
- `--host` (default `localhost`), `--port` (default config `server.port` / 7687).
- `-u`/`--user` (required).
- **password** — from **stdin** (default, tty-safe) or env `SLATER_DUMP_PASSWORD`;
  no plaintext `--password` flag (avoid `ps`/shell-history leaks). A
  `--password-stdin` toggle mirrors Docker's convention.
- `-o`/`--out` — output file (default stdout).
- `--key Label=prop` (repeatable) and global `--pk <field>` — identity-key
  overrides (see below).
- TLS passthrough if the server requires it (`--tls`, `--ca-cert`).

**Identity-key resolution — infer from range indexes + override (decided).** The
core does **not** record which property is a node's business key. Default: treat
each label's range-indexed property (`manifest.range_indexes` → exposed over Bolt
via schema introspection, e.g. `SHOW INDEXES` / `CALL db.indexes()` — verify
`introspect.rs` supports this, else add) as its identity key. `--key Label=prop`
overrides per label; `--pk <field>` selects a global dump_id-style key. Emits
`MERGE (n:Label {key: value})`.

**Dump procedure.** (1) connect + auth; (2) enumerate labels → identity key (schema
+ overrides); (3) emit `CREATE INDEX` DDL so the rebuild recreates indexes; (4)
nodes — per node emit `MERGE (n:Label {key: value}) SET n.p = …;` for the remaining
properties; (5) edges — `MATCH (a…),(b…) MERGE (a)-[:TYPE {props}]->(b);` using both
endpoints' business keys (so the edge query returns endpoint labels + key props);
(6) a Cypher-literal escaper for string/number/bool/null/list values.

**Known limitation — vectors in the `MERGE` dump.** `vecf32` properties cannot be
carried in a `MERGE` dump: the build path rejects vector values outright
(`merge_build::reject_vector`). `serialise_merge_dump` therefore **refuses** a graph
that declares vector indexes rather than emitting each embedding as a `null` literal,
which is what it used to do — silently rebuilding the graph without its vectors. A text
dump of a vector-carrying graph must use the `--pk`/`CREATE`-style offline path.

This is a limitation of the *text* dialect only. **Consolidation carries vectors.** It
takes the binary path (`serialise_binary_dump`), whose dump has a `vectors.blk` stream
and a `vector_indexes` section in `meta.json`; the builder re-attaches each embedding to
its node and rebuilds the index, re-routing by cardinality against its own
`ann_threshold`. Embeddings need their own stream because an *indexed* one is routed
**out** of the column store (D12) and so is absent from a node's property record — which
is exactly why this went unnoticed: the dumper simply never saw them.

**Net-new pieces / dependencies:** promote `BoltConn` to shared; a schema-introspection
query for range indexes; a **list-readable-graphs** call over Bolt for `-l`/`--list`
(backed by `Acl::readable_graphs`); the `MERGE` serialiser + Cypher-literal escaper;
clap args (+ optional `slater query` migration).

## Consistency & correctness model

- **Snapshot isolation across a swap** is free: a query pins one `(core Arc, delta
  snapshot)` tuple at start and holds both.
- **Read-your-writes** within the single writer session after WAL fsync + memtable
  apply.
- **Crash safety:** nothing mutated in place until an atomic rename; a crashed
  consolidation loses nothing (`current` unmoved, WAL intact).

## Execution order (milestones — each ships green with its tests)

- **Phase 0 — scaffolding.** `slater-delta` crate; `ReadView`/`MergedView`;
  generic `Engine`; empty-delta fast path + `delta_overlay` bench. **✅ landed.**
- **Phase 1 — durable property overwrites + dump-and-rebuild consolidation.**
- **Phase 2 — new nodes + deletes (tombstones) + index overlay.**
- **Phase 3 — edge deltas (topology overlay).**
- **Phase 4 — L0 flush + backpressure**, then **4b — generation-as-input** consolidation.
- **Phase 5 — Bolt write surface + `element_id` + ops docs.**

## Verification (end-to-end)

- **Golden / content-hash parity** — consolidation determinism is the primary gate
  (extend the `emit_determinism.rs` two-build byte-identity pattern).
- **Merge-equivalence property test** — `read(core + delta)` == `read(single build
  of the union)` for a random write sequence.
- **Crash-safety** — `SLATER_*_FAIL_AFTER` for consolidation; kill-during-batch for
  the WAL (mirror `slater-build/tests/resume.rs`).
- **No-regression** — the `delta_overlay` empty-delta bench stays within noise of
  the core arm.

## Progress ledger

- **Phase 0 (landed):** `slater-delta` crate (identity/interner/memtable/wal stubs);
  `ReadView` trait + `MergedView` + generic `Engine`; `testkit` feature exposing
  `testgen`; `delta_overlay` bench (empty-delta within noise). Whole workspace green;
  524 slater unit tests pass; clippy `-D warnings` clean. D43, D44 recorded.
