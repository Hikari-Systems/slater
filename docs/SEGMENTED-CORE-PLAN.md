<!-- SPDX-License-Identifier: Apache-2.0 -->
# Segmented core — an additive at-rest format for slater

> Canonical plan + progress ledger for the additive-core track. Committed so it
> survives context clears. **If you are resuming, read the "RESUME HERE" section
> at the bottom first.**

## Why

Consolidation (folding the write-delta into the immutable core) is O(core): the
server reads the whole core back out and `slater-build` rebuilds a fresh generation.
Measured on a 10M-node / 103.66M-edge core: consolidation is **375s** (Phase 0),
**309s** (Phase 0.5), of which the builder side is only ~70s — the remaining ~239s
is the server reading the entire core back out through the read-path to re-serialise.
That read-out is the floor for a single-image format; only an **additive** core (a
bounded LSM stack of immutable segments) removes it, by making a routine fold write
one small segment (O(delta), no base read) instead of rewriting everything.

The read-side machinery to merge N newest-wins levels already exists in slater (the
delta's `LevelRead`/`DeltaSnapshot` fold, `merge_levels`, interner-independent
identity keys). The additive core is that machinery extended *downward* past the
delta, not a new subsystem.

No backwards compatibility with shipped installs (there are none); format breaks are
fine. Correctness is asserted against an independently-derived model oracle, never
impl-vs-impl.

## Design summary

**Generation set** = one large clustered **base segment** (today's generation, still
built by `slater-build`) + a bounded stack (≤ `maxUpperSegments` ~8) of small
immutable **upper core segments**, each the O(delta) at-rest product of a flush.

- **Stable banded ids.** New entities get appended id bands `[b, b+k)`; existing ids
  never move. Only the rare full rebuild may renumber. A resident extent table
  (`sorted Vec<(band_base, segment)>`, binary-searched) routes id → owning segment.
- **Props/labels/postings/ISAM are additive for free** — id-indexed row stores or
  sorted runs; a new segment holds a new band / a new sorted run merged at read.
  A written node's segment carries its **full** property row, so property reads never
  fold (newest segment holding the id wins, 1 read).
- **Topology is the hard part.** A flush writes only born/removed edges as adjacency
  **fragments** (never rewriting a node's whole neighbour list). A per-segment
  **presence filter** (roaring bitmap / id-band fence) lets an untouched node skip all
  upper segments in O(#segments) resident checks → 1 block read (today's cost). Only
  written nodes fan out; **tiered compaction** caps live segments (~8) so fan-out and
  write-amplification stay bounded, and compaction is incremental — never O(whole
  core) at once.
- **Signed marginals** per segment (Δ counts) sum at open; anything not provably exact
  is *declined* (the established "empty ⇒ decline, never wrong" discipline).
- **Compaction ladder:** T0 memtable→L0, T1 L0↔L0 (both exist) · **T2 L0→core-segment
  flush (new, O(delta))** · **T3 segment↔segment merge (new, O(inputs))** · **T4 full
  rebuild** (rare, optional; only rung that re-clusters + reclaims base tombstones —
  uses the Phase 0/0.5 direct-dump path).

## Phases

- **Phase 0 — Direct binary-dump consolidation.** DONE, committed `134e2e4`. Binary
  dump (dense ids + global symbols) → builder ingests directly (skips
  parse/dedup/resolve). Files: `graph-format/consolidate_dump.rs`,
  `slater/consolidate.rs::serialise_binary_dump`, `slater-build/direct_ingest.rs` +
  `build_external.rs` front-half branch + `--input-format`, `server.rs`.
- **Phase 0.5 — Byte-copy untouched entities.** DONE, committed `a6e4d34`. Symbol
  tables seeded from the base manifest; untouched entities byte-copy their raw
  records (no decode/String-alloc/re-encode). `Engine::raw_node_labels/raw_node_props/
  raw_edge_props` + `DumpWriter::append_node_raw/append_edge_raw`.
- **Phase 1 — Set manifest + plumbing (no data-file format change).** DONE, committed
  `4c80c6b` (HP1: type) + HP2 (reader/builder). `slater-build` publishes
  `sets/<uuid>.json` (local + remote, before `current`); `Generation` resolves
  `current`→set→base with an implicit-singleton fallback, carries a `base_uuid` field
  (== `uuid()` in a singleton), `base_uuid()` accessor. Server/ResultKey unchanged
  (set uuid == gen uuid). graph-format + slater (698 lib) + slater-build suites green,
  clippy clean; real-builder consolidation round-trips through the set manifest.
  Introduce `<graph>/sets/<set-uuid>.json` and open the core through it, always a
  singleton (1 base, 0 segments) so behaviour is identical. **Design decision: in a
  singleton `set_uuid == base_uuid == gen_uuid`, so `current` stays a gen uuid and
  nothing that reads `current`/the gen dir (testgen fixtures, golden tests) breaks;
  the reader reads `sets/<uuid>.json` if present else falls back to an implicit
  singleton.** The set/base split lives in `Generation` (a `base_uuid` field ≠ the
  set `uuid()`), ready for Phase 4 where a flush makes a new set over the same base.
  - *Exit:* full suite + conformance green over fs and mem stores; `delta_overlay`
    bench within noise; a graph whose `current` names a set with an unknown
    magic/version fails cleanly.
- **Phase 2 — Core-segment format.** `graph-format/segment.rs` (sections, key columns,
  fences, tombstones), ISAM fragments + removal sidecar, posting fragments,
  `SEGMENT.json` signed marginals, encryption/MAC parity, `extents.rs` routing table.
- **Phase 3 — Read path over a stacked set.** `LevelRead` extensions + at-rest adapter;
  `MergedView` routing (full-row short-circuit, adjacency fan-out gating, index-probe
  union, count summation, histogram decline). Four exec.rs seams: `node_record`,
  `read_adj_overlaid`/`overlay_adj`, `scan_candidates`, count fast paths.
- **Phase 4 — T2 flush.** `DeltaWriter::flush_to_segment`, publish/retire crash-safety,
  exact marginals, memtable base preservation (no re-resolution).
- **Phase 5 — T3 segment compaction + admission.** Size-tiered merges, tombstone
  reclamation, adjacency collapse, `maxUpperSegments`, scheduling; DECISIONS.md D50
  update to the four-rung ladder.
- **Phase 6 — Batch resolve + fences on the write path.** Merge-join batch resolve;
  fences/blooms on resolve.
- **Phase 7 — T4 retarget + GC.** `consolidate_graph` collapses a set to a singleton
  via the Phase-0 direct path; retired sets/segments GC'd after a grace period.
- **Phase 8 — Bench harness + hardening + docs.** Read-amp harness (point lookup,
  2-hop, label scan, counts) over fs and S3, 0/2/4/8 segments, cold+warm.

## Correctness discipline

Model oracle from the op log, property-tested across interleavings; hand-computed
codec goldens; `slater diag --recount` marginal audit; open-time invariants (bands
tile, routing monotone, Σ deltas + base = declared totals). Benches gate performance,
never correctness.

## Reusable scale assets (see memory `reusable-10m-wikidata-sample`)

- `/home/rickk/wd-full/wikidata-10m-merge.cypher` (9.4GB, 10M nodes / 112M edge lines).
- Prebuilt gen `/home/rickk/perf-gens/wd10m-gen` (10M / 103.66M edges) + `perf-gens/wiki1m` (1M).
- Build/test invocation: `CARGO_TARGET_DIR=/home/rickk/.cache/slater-target cargo …`
  with `dangerouslyDisableSandbox` (default `target/` is sandbox-denied — see memory
  `build-target-dir-sandbox`).

---

## RESUME HERE

**Branch:** `writeable`. **Committed through:** Phase 5 slice 5.1 (HP17). **Phases 1–4
DONE; Phase 5 IN PROGRESS.**

**Phase 5 (T3 segment compaction) — slice 5.1 DONE.** A new merge writer
(`crate::merge_segment::write_merge_segment`) folds a **contiguous run** of upper segments
(oldest→newest) into one merged segment that reads *identically* to the run — newest-wins per
dimension: node/edge rows (newest input's full row; a **within-run** born tombstone is
reclaimed, a **below-run** tombstone kept), adjacency fragments (per node, `removed` cancels a
within-run born append else is carried), index fragments (per `(label,prop)`, entry id-sets +
removal sidecars fold newest-wins; each live id's value is read from the merged full-row node —
segments have no `(value,id)` iterator — and a below-run removal is carried), postings (union).
**Marginals are the *sum* of the inputs'** (`marginals_exact` = AND) — the merged segment must
contribute the same Δcounts as the run it replaces, and born-then-deleted ids net to zero.
`Graphs::compact_graph_segments(name, vc, data_dir, start, end)` picks the run, writes the
merged segment, publishes a new set (segments below the run + merged + segments above), uploads
(remote store), swaps, and **rebinds** the delta (`DeltaWriter::rebind_core_uuid`) — compaction
touches neither base nor delta and the merged band unions the run's, so `extents().total()` is
invariant and the delta's resolved ids stay valid (no freeze/replay/rebase, unlike `retire`,
which also clears L0). The run's old segment dirs are left for a later GC (Phase 7). Run
selection is explicit (`start..end`); **admission policy — `maxUpperSegments`, size-tiered
selection, scheduling — is slice 5.3.** An auto-trigger is still Phase-6-gated (as for flush).

**(Phase 4 — flush writer, all slices DONE.)** The flush writer (`Graphs::flush_graph_to_segment` +
`crate::flush_segment::write_flush_segment`) now materialises **born nodes/edges, core-resolved
node patches, deletes, AND core-edge patches** — **every write op can now flush** (the last
per-op `bail!` in the edge-row loop is closed). A `SET`/`REMOVE` on a base node folds into the
upper segment as a full replace-row (base-below row read through the stack, delta overlaid)
with the index **removal sidecars** that supersede its stale base/lower-segment values. A
**delete** is a full-row **tombstone** (the effective-row-empty case of a patch: node/label
marginals net down, every base-indexed value moves to `removals`) plus incident-edge **removal
fragments**: the writer reads the deleted node's *effective adjacency* (base folded with every
lower segment, via `flush_segment::effective_adj`, mirroring `overlay_segment_adj`) and writes
a `removed` fragment by edge id on each *surviving* neighbour's side, netting the edge/reltype
marginals; an explicit `DELETE r` on a core edge is resolved to its id(s) the same way and
removed on both live endpoints. A **core-edge patch** (`SET r.p = v` on an existing core edge)
folds into the upper segment as a full **replace** edge row — the edge's base props (a lower
segment's winning `resolve_edge_row`, else the base generation's `edge_props`) overlaid by the
patch — that `resolve_edge_row` serves over the base, with **no marginal change** (topology is
untouched). Publishes/swaps/retires and reads back identically — surviving a reopen. **A
flush over an encrypted core now writes an encrypted segment** — a fresh per-segment cipher +
KDF header is derived from the runtime `master_key`, `manifest.encryption` is stamped and the
MAC sealed, and the read side re-derives the same cipher on reopen (the `master_key.is_some()`
bail is gone). **A flush over a stacked L0 now folds** — when the freeze captures sealed L0
levels beneath the active memtable, they merge newest-wins into one segment via
`Memtable::merge_levels([snapshot, l0…])` (the active memtable is newest, `frozen.l0` is
newest-first); born ids tile contiguously above the shared base and the merged `synthetic_base`
stays `== prior_node_total`. **A flush against a remote store now uploads through it** — the
segment is staged locally, then (when `self.store` is not the local fs — `ObjectStore::is_local_fs`)
`upload_flush_to_store` publishes every segment file (with its SHA-256), `SEGMENT.json`, the set
manifest, then `current` **last** (the copy-completeness barrier), mirroring the builder's
`upload_generation`; this precedes the swap, which reads `current` from `self.store`. **Slice
4.4 is COMPLETE — the whole deployment bundle is in.** (Flush auto-trigger wiring is
Phase-6-gated — needs segment-aware write-path resolve; do NOT wire before then.) Still
deferred and `bail!`ed in the *flush* writer: patch-**then-delete** of the same core edge in one
delta (an adjacency-removal concern the patch materialiser doesn't own), and a flush over an
**off-heap** L0 level (resident L0 folds via `LevelRead::as_memtable`; off-heap stores a block
image, not a memtable, so it needs a memtable rebuild the lossy trait can't give —
`as_memtable()` returns `None`).

**Phase 5 NEXT after 5.1:** **5.2** — hardening the merge over the cases 5.1's test doesn't
exercise (a base-node **delete** folded across the run; a **partial** run `[i,j)` with segments
above/below it; an **encrypted** merge; a **remote-store** merge; a merge whose inputs include a
zero-width band). **5.3** — the **admission policy**: `maxUpperSegments`, size-tiered run
selection, scheduling, and (Phase-6-gated) an auto-compaction trigger; DECISIONS.md D50 → the
four-rung ladder. Deferred leanness (each benign, matching the flush writer's noted follow-ups):
a born-then-deleted **edge** leaves an orphan edge row in the merged segment (its adjacency is
suppressed by the fold, so it is never read); postings are a union (a stale driving hit is
filtered by adjacency).

### Phase 5 slice log
- **5.1 DONE** (HP17): the **T3 merge writer** + orchestrator + rebind, end-to-end. New
  `crate::merge_segment::write_merge_segment(inputs, &MergeInputs)` folds a contiguous run of
  `&LoadedSegment`s (oldest→newest) into one segment — enumerating each reader's key columns
  (`node_ids`/`edge_ids`/`adj_out_ids`/`adj_in_ids`, `reltypes`, `indexed`) and point-reading
  the rows, newest-wins with within-run reclamation per the module scope. Marginals **sum** the
  inputs' manifests (`marginals_exact` = AND). `Graphs::compact_graph_segments` picks the run,
  writes the merged segment, publishes the spliced set, uploads (remote), `swap_if_changed`,
  asserts `extents().total()` is invariant, and `DeltaWriter::rebind_core_uuid`s the delta (a
  new lightweight rebind: `inner`-locked `core_uuid` set + epoch bump, no replay/rebase/L0-clear
  — unlike `retire`). Shared with the flush writer: `flush_segment::{inventory, SEG_BLOCK_BYTES,
  SEG_ZSTD_LEVEL}` (now `pub(crate)`) and `upload_flush_to_store`. One e2e oracle
  (`compact_segments_folds_a_run_into_one`, `write_basic` fixture): two flushes stack two
  segments (born Dave+edge, base Carol age 40→99; born Frank, base Carol 99→77); a 10-probe
  battery — cross-segment base override (Carol 77; base 40 + intermediate 99 both suppressed),
  born-age index seeks, summed count, forward+reverse born edge — reads **identically** before
  and after the compaction, the stack shrinks 2→1, the id space is invariant, the delta is
  rebound, and every probe survives a reopen. NB the override targets a **base** node (Carol),
  not a flushed born node (Dave) — a born key can't be re-resolved by the write path until
  Phase 6 (the 4.1 note (e) limitation). 720 slater lib + 140 graph-format + 78 slater-delta +
  full workspace green, clippy + fmt clean.

### Phase 4 slice log
- **4.4-d DONE** (HP16): **object-store upload** — the last of the deployment bundle. A flush
  against a store that is not the local filesystem (S3/GCS/in-memory) now publishes through it
  instead of only writing local fs. The segment is staged locally by `write_flush_segment` as
  before; then, gated on a new `ObjectStore::is_local_fs()` (default `false`, `true` only for
  `FsObjectStore` — for which the direct `std::fs` writes already *are* the store), a new
  `upload_flush_to_store` uploads every segment file (with its SHA-256, so S3 validates the body
  and stores the object checksum) under `<graph>/segments/<uuid>/`, then `SEGMENT.json`, then the
  set manifest, then `current` **last** — the copy-completeness barrier, mirroring the builder's
  `upload_generation`. The upload runs **before** `swap_if_changed`, because the swap reads
  `current` from `self.store`; a remote store must see the new pointer for the swap to observe a
  change (else it bails "current was unchanged"). Local atomic publish (`publish_set_and_current`,
  tmp-then-rename + fsync) is kept — it stages the local copy and is the fs-backend's crash-safe
  publish (`FsObjectStore::put` is a plain non-atomic `std::fs::write`). One new e2e oracle
  (`flush_to_segment_uploads_to_an_object_store`): seed a `MemObjectStore` from a local base
  fixture, open the served graph through it, flush a born node + core patch, assert the store now
  holds the set manifest + updated `current` + the segment's `SEGMENT.json`, then reopen reading
  **only** through the mem store (no local fs) and serve the flushed data. 719 slater lib + 140
  graph-format + full workspace green, clippy + fmt clean.
- **4.4-c DONE** (HP15): **stacked L0 fold**. A flush over a freeze that captured sealed L0
  levels (active memtable + `frozen.l0`, previously `bail!`ed) now folds them into one segment.
  When `frozen.l0` is non-empty, `flush_graph_to_segment` builds `[snapshot, l0[0]…l0[n]]`
  (active memtable newest, `frozen.l0` newest-first — matching `DeltaSnapshot::with_levels`) and
  calls `Memtable::merge_levels`, then flushes the merged memtable through the unchanged
  `write_flush_segment`. The no-L0 fast path is untouched (snapshot flushed directly — no merge
  clone paid). Correctness rests on stacked born-id allocation: `flush_to_l0`
  (`delta_writer.rs:545`) rebases each new active memtable to `base + born_count`, so born ids
  across levels tile `[base, base+total)` and the oldest level's `synthetic_base` is the global
  base (= `prior_node_total`) — keeping the writer's Phase-3.2 band assertion true. The empty
  guard widened to `snapshot.is_empty() && l0.all(is_empty)`, and `retire` already consumed
  `frozen.consumed_l0`. **Off-heap L0 is deferred**: `merge_levels` needs concrete `Memtable`s,
  so a new `LevelRead::as_memtable()` seam (`Some(self)` for a resident `Memtable`, default
  `None`) downcasts each level; an off-heap level (a block image, not a memtable) returns `None`
  and the flush `bail!`s — a rebuild the lossy `LevelRead` trait can't cheaply give. One new e2e
  oracle (`flush_to_segment_folds_a_stacked_l0`): a core node patched in **all three** levels
  (99→77→55) resolves newest-wins to 55; born Dave (sealed L0) + born Eve (active) tile above
  the base; a born edge Alice→Dave (core + same-level born endpoints) traverses; all read back
  through an empty delta and survive a reopen. 718 slater lib + full workspace green, clippy +
  fmt clean.
- **4.4-b DONE** (HP14): **encryption parity**. A flush over a core that is encrypted at rest
  now writes an encrypted segment instead of bailing. The caller
  (`Graphs::flush_graph_to_segment`) derives a **fresh per-segment** `BlockCipher` + manifest
  `EncryptionHeader` (KDF salt only, never the key) from `self.master_key` — mirroring the
  builder's `slater-build::common::derive_cipher` and `generation.rs:derive_cipher`, each
  segment gets its own salt — and threads `cipher`/`master_key` into `FlushInputs` alongside a
  new `encryption_header` field. The writer already routed every section through
  `SegmentWriter::create_with_cipher` and sealed the MAC via `seal_mac(inp.master_key)`; the
  **only writer gap was the manifest stamp** — `flush_segment.rs` hard-coded
  `manifest.encryption = None`, now set from `inp.encryption_header`. The **read side needed no
  change** (`segstack::load` → `derive_segment_cipher` already reads
  `manifest.encryption.{aead,kdf,salt_hex}` + `master_key`, MAC-verifies). The
  `master_key.is_some()` bail at the top of `flush_graph_to_segment` is removed. Fixture work:
  `testgen::write_indexed_people_at` refactored to thread an optional key (new
  `write_indexed_people_at_keyed` / `write_indexed_people_keyed`) so a keyed core fixture exists
  — every section written through the cipher, manifest sealed — plaintext path unchanged
  (`None` reduces to the old fixture). One new e2e oracle
  (`flush_to_segment_encrypts_the_segment_under_a_master_key`): keyed core, born node + edge
  flushed, assert the segment manifest carries an encryption header + MAC, read the born node
  back decrypted through an empty delta, reopen the whole data dir WITH the key (born edge
  traverses), and assert reopening WITHOUT the key is refused. 717 slater lib + full workspace
  green, clippy clean.
- **4.4-a DONE** (HP13): **core-edge patches** — closes the last per-op `bail!` in
  `write_flush_segment`'s edge-row loop, so every write op can now flush. A `SET r.p = v` on a
  core edge (id below the edge synthetic base, in the memtable's `by_edge_id`) is materialised
  as a full **replace** `EdgeRow` the segment overrides the base with: the writer reads the
  edge's base-below props via new `read_base_edge_row` (mirror of `read_base_node_row` — a
  lower segment's winning `CoreStack::resolve_edge_row`, else `core.edge_props().props(eid)`
  name-mapped), overlays `edelta.patches` (LWW), and pushes the row at the core edge id. **The
  read side already supported it** (`resolve_edge_row` binary-searches by id gated by
  `may_hold_edge`, derived from the *pushed* edge ids — so a core id below the band is found;
  the `edge_prop_par` seam resolves segment-row-over-base). **The gap was the writer:**
  `to_segment_data` surfaced only the patch delta into `data.edges` (endpoints + reltype
  dropped), so `SegmentData` gained a new `core_patched_edges: Vec<(edge_id, src_dense,
  dst_dense, reltype_name)>` field, populated from the `by_edge_id` `EdgeEntry`s (a patch
  leaves topology alone, so the endpoints are absent from `adj_out`). **No marginal delta** (a
  patch changes no edge/reltype count) and **no index sidecar** (slater carries no relationship
  range index consulted at query time). Core-patch ids push before born ids (ascending), so the
  edge fence widens to include them. A patch-**then-delete** of the same core edge in one delta
  (a tombstoned `by_edge_id` entry) is refused — its removal is an adjacency concern the patch
  materialiser doesn't own. One new e2e oracle test
  (`flush_to_segment_materialises_a_core_edge_patch`): patch `since 2020→2099` + a fresh `note`
  on `Alice-KNOWS->Bob`, flush, read both back through an empty delta + reopen, base value gone,
  endpoints/counts unchanged. 716 slater lib + full workspace (140 graph-format, 78 slater-delta)
  green, clippy clean.
- **4.3 DONE** (HP12): **deletes**. `write_flush_segment` lifts the node/edge-tombstone
  `bail!`s. A core-node delete is materialised as the effective-row-empty case of a core
  patch — a `NodeRow` tombstone, `removals` for every base-indexed value (grouped under the
  identity label, cross-layer), node-count −1, each base label −1 — reusing the 4.2 base=Some
  index/label-marginal path (the only add is a `node_count_delta -= 1` for a tombstoned base
  node). Incident edges are found by reading the deleted node's **effective adjacency** (new
  `effective_adj` helper: base CSR `topology().outgoing/incoming` folded with every lower
  segment's `out_adj/in_adj` fragment, oldest→newest — the write-time mirror of
  `overlay_segment_adj`, recovering the concrete core edge id the delta's node tombstone never
  carried), and a `removed` adjacency fragment is emitted by edge id on each **surviving**
  endpoint's side (a dropped node's own side is never read); the edge/reltype marginals net
  each out. An explicit `DELETE r` on a core edge (carried in `adj_out` as an adjacency
  tombstone with no edge id) is resolved to its id(s) against the source's effective adjacency
  — **all** parallel `(reltype, neighbour)` matches, mirroring `overlay_adj`'s suppression —
  and removed on *both* live endpoints. A born edge incident to a node deleted in the same
  delta is dropped wholesale (never reaches a lower layer). Born edges + suppressed removals
  are merged per node into `out_frags/in_frags` and pushed together (node 0 both gains a born
  edge and loses a core one in the same fragment). Two new e2e oracle tests
  (`flush_to_segment_materialises_a_node_delete`, `…_an_edge_delete`) drive real read paths
  (index seek, label/reltype count, both traversal directions) through an empty delta and a
  reopen. 715 slater lib tests + full workspace green, clippy clean.
  - **Follow-ups deferred:** a delete emits `removals` for *every* base prop, including
    non-indexed ones (spurious idx fragments — benign, never consulted since the planner only
    picks base-existing indexes; mirrors 4.2, leanness TODO); core-edge patch materialisation
    still `bail!`ed (no by-id base endpoint reader).
- **4.2 DONE** (HP11): `write_flush_segment` (renamed from `write_births_segment`) now also
  materialises **core-resolved node patches**. `FlushInputs` carries `core: &Generation`;
  each patched node (id below the synthetic base, non-tombstoned) has its base-below-delta
  row read via `read_base_node_row` (`CoreStack::resolve_node_row` winning row, else the base
  `node_props`/`node_labels` record), the delta overlaid into a full row by `core_patch_props`
  / `core_patch_labels` (line-for-line mirrors of `overlay_node_props` + `node_label_ids_par`),
  and a **minimal index diff** emitted: a `removal` for every base-indexed prop the effective
  row changed/dropped (grouped under the identity label — cross-layer, so it supersedes a
  *lower segment's* entry too), a fresh entry for every changed/added prop. Node-count delta
  is births-only; label deltas net each patch's effective-vs-base label set (dropped when
  zero); `marginals_exact` stays true. Two new oracle tests
  (`flush_to_segment_materialises_core_node_patches`,
  `flush_to_segment_supersedes_a_lower_segment_value`). 713 slater lib tests + full workspace
  green, clippy clean.
  - **Follow-ups deferred:** core-edge patch materialisation (no by-id base endpoint reader);
    a patch-only flush writes a zero-width node band (supported by `Extents::from_lengths`).
- **4.1 DONE** (HP10): `flush_segment.rs` materialiser (`write_births_segment`) — born
  nodes/edges → full rows, adjacency fragments, ISAM index fragments (shared prop derivation
  with the node row so they can't diverge), posting fragments, and *exact* births-only
  marginals. `Graphs::flush_graph_to_segment` orchestrates freeze → write segment → publish
  set + flip `current` (crash barrier, mirrors the builder's local publish) → `swap_if_changed`
  → **reuses `DeltaWriter::retire`** (base preserved, so retire's re-base/re-resolve is passed
  the *set* total via `extents().total()`, not the base-only `node_count()`). Refuses (bails)
  a core patch/tombstone, a stacked L0 level, or an encrypted core — all later slices. **Not
  wired to an auto-trigger** (invoked explicitly, like Phase 2's "don't wire reads yet").
  - **Follow-ups the slice deferred (each a later slice):** (a) core-patch/-delete full-row
    materialisation needs the merged-view base read + removal sidecars; (b) L0-level fold (a
    flush over a prior `flush_to_l0` stack) needs a cross-level `DeltaSnapshot` walk (no
    unified `iter_nodes` on `DeltaSnapshot` — drive `l0_levels()` + the born-* folded
    helpers); (c) encryption parity (write the segment under the core's cipher + seal the
    MAC — currently refused when `master_key.is_some()`); (d) s3/object-store upload of the
    segment + set (currently local-fs publish only, like the builder before its upload step);
    (e) **write-path resolve is not yet segment-aware** — a *concurrent* re-`MERGE` of a
    just-flushed born key during the freeze→retire window would not find it in the segment
    (resolve folds only L0, not segments), risking a duplicate; the 4.1 test is synchronous so
    it does not hit this, but the auto-trigger MUST NOT ship before Phase 6's segment-aware
    resolve (or a flush-time write barrier).

**(historical)** Phase 4 was the first *writer* of a core segment. Everything below Phase 3
is the read side; before slice 4.1 nothing produced a segment, so all of Phase 3 was
exercised only by hand-built fixtures.

### Phase 4 entry notes (obligations Phase 3 recorded — the flush writer MUST honour these)
- **Synthetic id allocation:** the write-delta must allocate born ids above the *stack top*
  (`core_stack().extents().nodes.total()` / `.edges.total()`), NOT merely above the base
  count — else a delta-born id collides with a segment's band and `resolve_node_row` returns
  the wrong row. (Today the delta's `synthetic_base` == base count; a flush over a stacked
  set must lift it.)
- **Removal sidecars are cross-layer:** a segment's `idx.meta` `removals` must list every id
  whose indexed value it supersedes — base *or an older segment's* fragment entry — so the
  oldest→newest `fold_index_*` retain gives newest-wins.
- **Node-delete → incident-edge removals:** deleting a node must write `removed` adjacency
  fragments for *every* incident edge on the neighbour's side (the read path drops a dead
  edge via the removal fragment, NOT via a per-neighbour segment-tombstone check), and the
  `edge_count_delta` / `reltype_edge_deltas` marginals must net those out. (See the
  `write_basic_with_segment` vs `write_basic_with_born_segment` test fixtures: the former is
  adjacency/scan-shaped and deliberately NOT edge-count-consistent; the latter is a clean
  births-only segment used for the count oracle.)
- **`marginals_exact`:** set it only when the flush can prove every marginal; the read path
  declines all count fast paths to full execution when any segment is inexact.

### Phase 3 design (decided)

Segments are **immutable-core-shaped** (full rows, *replace* semantics), not delta-shaped
(patch-fold). So they form a **core stack** *between* the base `Generation` and the
write-delta — NOT a `LevelRead` level. Effective read precedence, newest-wins:
1. **delta** (`MergedView.delta`) — patches / tombstones / born rows (top).
2. **upper segments newest→oldest** — first segment whose `may_hold_node(id)` fence passes
   and `node_row(id)`/`edge_row(id)` returns `Some` wins as a *full row* (its `tombstoned`
   flag = deleted); no cross-segment fold.
3. **base** generation readers (bottom).

The merge lives at the **four exec.rs seams** (the trait methods like `node_props()` return
single readers, so the fold can't live in `ReadView`). Each seam resolves the base row from
the stack *before* applying the existing delta overlay. The stack + routing are reached via
`ReadView::core_stack()` → `crate::segstack::CoreStack` (`segments()` oldest→newest,
`extents()` id→`SegmentOrd`). Segment readers page through their own held `BlockCache`, so a
resolver needs only `&CoreStack` + the id.

**Slices (each its own green commit; test every slice against a hand-built stacked-set
fixture — `segstack.rs::tests::write_segment` + `Generation::open` over an fs set):**
- **3.1 DONE** (`057fec2` store-native segment opens; `1cc6b55` `CoreStack` load+route+
  `core_stack()`, wired into `Generation::open`, INERT — no read consults it yet).
- **3.2 DONE** (`ad005a8`): `CoreStack::resolve_node_row/resolve_edge_row`; seams
  `node_label_ids_par`, `node_prop_par`, `edge_prop_par` resolve segment full-row over base
  before the delta; name-space `core_named_props`/`core_named_edge_props` (used by
  `node_record`/`rel_record`/`all_properties`) preserve non-core-symbol keys. Precedence
  delta>segment>base. **Invariant for Phase 4: the delta must allocate synthetic ids above
  the *stack top*, not just above the base** (else a born id collides with a segment band).
- **3.3 DONE** (`a8057f2`): `overlay_segment_adj` folds each segment's `out_adj/in_adj`
  fragment into the base list (oldest→newest; `removed` suppresses by edge_id, born
  appends) in `read_adj_overlaid`, then the delta. Gated by NEW adjacency fences
  `may_hold_out_adj/in_adj` (the node fence is wrong — an adjacency-only-touched node has
  no node row). Merge order base→segments→delta.
- **3.4 DONE** (`1851b49`): `CoreStack::fold_index_eq/fold_index_range` (oldest→newest,
  removals-suppress then lookup-union = newest-wins), `fold_label_scan` (membership
  recomputed from effective rows), `is_node_tombstoned`; `scan_candidates` folds all four
  variants + re-sorts for the delta overlay; `suppress_tombstoned` drops segment tombstones
  (now `Result`). **Phase-4 obligation: a segment's `removals` must cover every id whose
  indexed value it supersedes (base OR older segment), not just base ids.**
- **3.5 DONE** (`6e2c3a7`): `MergedView`/identity `live_*` sum the stack's `SegmentManifest`
  deltas (node/label/edge/reltype), decline (→ None) on inexact marginals; `node_count()`/
  `edge_count()` use `extents().total()` so `AllNodes` covers born bands. Gates:
  `try_count_fast_path` (declines inexact), `try_reltype_meta_fast_path` (routes stacked
  sets through `live_reltype_edge_groups`), `try_label_meta_fast_path` + grouped-index/
  count-walk (decline over a stacked set — **histogram decline landed here, not 3.6**).
- **3.6 DONE** (`85d68ff`): full workspace suite + clippy + fuzz build green; mem-store
  conformance (a stacked set opens + queries end-to-end store-natively). An adversarial
  review of the merge seams verified all five invariants and the singleton/delta-only
  byte-identity; it surfaced two ungated base-marginal *result* reads (both also pre-existing
  delta-unaware), now fixed: `Engine::build_view` (algo.* subgraph) selects nodes via
  `scan_candidates`, and `meta_stats` reports `live_*` counts. `plan.rs` `choose_node_scan`
  reads base counts for **cost only** (the executor re-filters) — correct, left as-is.
  **Phase 3 COMPLETE.**

**Reference — the delta-overlay mirror targets** (Phase 3 seams mimic these for segments):
`MergedView` in `read_view.rs` (`live_*` signed marginals); `exec.rs` `overlay_node_props`
(:1698), `overlay_adj` (:342)/`read_adj_overlaid` (:388), `scan_candidates` (:5362) with
`born_ids_in_index_eq/range`; `DeltaSnapshot` fold in `slater-delta/memtable.rs`.

**Phase 2 artifacts (all in `graph-format/src/`, format only — NOT wired to reads):**
`extents.rs` (id→segment routing), `segment.rs` (node/adj/edge sections + fences +
public codecs), `segindex.rs` (ISAM fragments + removal sidecar), `segpostings.rs`
(endpoint driving-set fragments), `segmanifest.rs` (`SEGMENT.json`), plus
`SegmentRef::from_manifest` in `setmanifest.rs`. Fuzz: `fuzz/fuzz_targets/segment_decode.rs`.

**Safe handoff points (each is a green commit — clear context freely at any of these):**
- HP0 — Phase 0.5 committed (`a6e4d34`).
- HP1 — `SetManifest` type + graph-format tests, committed (`4c80c6b`). ✓
- HP2 — builder writes singleton set + reader opens through it (implicit-singleton
  fallback), 698 slater lib + slater-build suites green, clippy clean, committed. ✓
- HP3 — Phase 2 segment format, 5 slices, committed through `35f0c0d`. ✓ **Phase 2 complete.**
- HP4 — Phase 3 slice 3.1: store-native segment opens (`057fec2`) + `CoreStack`
  load/route/`core_stack()` wired into `Generation::open`, INERT (`1cc6b55`); 140
  graph-format + 702 slater lib tests green, clippy clean. ✓
- HP5 — Phase 3 slice 3.2: node/edge full-row resolution seam (`ad005a8`); 704 slater lib
  tests green (2 stacked-set oracle tests), clippy clean. ✓
- HP6 — Phase 3 slice 3.3: adjacency fan-out gating (`a8057f2`); 705 slater lib +
  graph-format segment tests green, clippy clean. ✓
- HP7 — Phase 3 slice 3.4: index-probe union + segment-aware scans (`1851b49`); 707 slater
  lib tests green (3 scan oracle tests), clippy clean. ✓
- HP8 — Phase 3 slice 3.5: count summation via signed marginals + histogram decline
  (`6e2c3a7`); 708 slater lib tests green (count oracle + decline), clippy clean. ✓
- HP9 — Phase 3 slice 3.6: hardening + conformance + review fixes (`85d68ff`); full
  workspace suite green (710 slater lib), clippy clean, fuzz builds; mem-store conformance.
  ✓ **Phase 3 COMPLETE.**
- HP10 — Phase 4 slice 4.1: births-only T2 flush writer end-to-end. New
  `slater/src/flush_segment.rs` (`write_births_segment`) + `Graphs::flush_graph_to_segment`
  (freeze → segment → publish set/current → swap → reuse `retire`); one new e2e oracle test
  (`flush_to_segment_folds_births_into_a_core_segment`). 711 slater lib tests green, clippy
  clean.
- HP11 — Phase 4 slice 4.2: core-resolved **node**-patch full-row materialisation.
  `write_flush_segment` reads each patched node's base-below row through the stack, overlays
  the delta into a full replace-row, and emits cross-layer index removal sidecars + a fresh
  entry per changed prop; label marginals net effective-vs-base. `FlushInputs.core` added;
  `read_base_node_row`/`core_patch_props`/`core_patch_labels` helpers. Two new oracle tests
  (base-layer patches; second flush superseding a lower segment's value). 713 slater lib
  tests + full workspace green, clippy clean.
- HP12 — Phase 4 slice 4.3: **deletes** end-to-end. `write_flush_segment` materialises core
  node/edge tombstones as full-row tombstones + incident-edge removal fragments (new
  `effective_adj` helper recovers the incident edge ids from base+segment adjacency) with
  netted node/edge/label/reltype marginals; born edges + suppressed removals merged per node.
  Two new e2e oracle tests (node delete, edge delete) through an empty delta + reopen. 715
  slater lib tests + full workspace green, clippy clean.
- HP13 — Phase 4 slice 4.4-a: **core-edge patches** — closes the last per-op `bail!`; every
  write op now flushes. `SET r.p = v` on a core edge folds into the upper segment as a full
  replace `EdgeRow` (base props via new `read_base_edge_row` overlaid by the patch, served by
  `resolve_edge_row`), no marginal change, no index sidecar (no live rel index). `SegmentData`
  gained `core_patched_edges` (endpoints a patch omits from `adj_out`). One new e2e oracle test
  (`flush_to_segment_materialises_a_core_edge_patch`). 716 slater lib + full workspace green,
  clippy clean.
- HP14/HP15/HP16 — Phase 4 slices 4.4-b/-c/-d (encryption parity, stacked-L0 fold, object-store
  upload); see the Phase 4 slice log above. Slice 4.4 COMPLETE; the flush writer is
  feature-complete (717→719 slater lib).
- HP17 — Phase 5 slice 5.1: **T3 segment compaction** writer + orchestrator + delta rebind.
  New `crate::merge_segment::write_merge_segment` (newest-wins fold of a contiguous run into one
  segment, summed marginals), `Graphs::compact_graph_segments` (pick run → merge → publish
  spliced set → upload → swap → `rebind_core_uuid`), `DeltaWriter::rebind_core_uuid` (lightweight
  id-space-invariant rebind). One e2e oracle (`compact_segments_folds_a_run_into_one`): a 10-probe
  battery reads identically before/after a 2→1 compaction and survives a reopen. 720 slater lib +
  full workspace green, clippy + fmt clean. ← current baseline; next is slice 5.2 (merge
  hardening: delete-across-run, partial run, encrypted/remote, zero-width band).

**Phase 2 slice log (all DONE — historical record of the core-segment format work):**
  1. `extents.rs` — resident routing table `sorted Vec<(band_base, segment_ord)>` for
     node & edge id → segment, binary-searched; unit tests. (isolated, safe first slice)
     **DONE** — `ExtentTable`/`Extents`/`SegmentOrd`, `partition_point` routing, tiling
     invariant validated at construction, `Extents::from_set`; 11 tests green, clippy clean.
  2. Segment writer/reader: sections `node.blk`/`adj_out.blk`/`adj_in.blk`/`edge.blk`
     as off-heap-L0-style resident sorted key columns over BlockCache-paged payloads
     (template: `slater-delta/src/l0_offheap.rs`); full-row node/edge records +
     tombstone flags; min/max id fences.
     **DONE** — `graph-format/src/segment.rs`: `SegmentWriter`/`SegmentReader`,
     `NodeRow`/`EdgeRow`/`AdjEdge`, four block sections + resident sorted key columns,
     `may_hold_node`/`node_fence` id-band fences, plaintext + AEAD (block-section
     encryption via `create_with_cipher`/`open_with_cipher`, absent-key refusal),
     `meta.bin` MAGIC+crc32c+version. 8 tests (round-trip, tiny-block multi-page,
     encrypted, empty, corrupt/foreign-magic reject) green, clippy clean.
     NOTE: `meta.bin` self-MAC + `SEGMENT.json` marginals are slice 4, not here.
  3. ISAM fragment + removal sidecar (reuse `write_isam_sorted`); posting fragments.
     **3a DONE** — `graph-format/src/segindex.rs`: `write_index_fragments` +
     `SegmentIndexReader`, one ISAM per `(label, prop)` over the segment's born/patched
     `(value, id)` pairs (reuses `write_isam_with_cipher`/`IsamReader`) + resident
     delta-varint removal sidecar in `idx.meta` (MAGIC+crc+version); `lookup_eq`/
     `lookup_range`/`removals`/`indexed`, `open_if_present` for the no-index case,
     plaintext + encrypted (absent-key refusal). 6 tests green, clippy clean.
     **3b DONE** — `graph-format/src/segpostings.rs`: `write_posting_fragments` +
     `SegmentPostingsReader`, resident `post.meta` (MAGIC+crc+version) of per-reltype
     ascending-distinct born src/tgt endpoint ids (reuses `encode/decode_endpoint_posting`);
     `src_ids`/`tgt_ids`/`reltypes`, `open_if_present`. Removals NOT tracked (a driving-set
     superset stays correct; edge removal handled by the adjacency fold). 5 tests green,
     clippy clean. **Slice 3 COMPLETE.**
  4. `SEGMENT.json` (signed marginal deltas as i64, per-index dirty bits, bands,
     inventory+hashes, encryption/MAC parity with `manifest.rs`).
     **DONE** — `graph-format/src/segmanifest.rs`: `SegmentManifest` parallel to
     `Manifest` — bands, i64 `node/edge_count_delta` + sparse per-reltype/-label deltas +
     `marginals_exact` decline flag, `dirty_indexes` (per-index dirty bits w/ fragment
     name), `FileEntry` inventory + `content_hash`, `EncryptionHeader`, keyed-BLAKE3 `mac`
     (`seal_mac`/`verify_mac` reuse `derive_manifest_mac_key`). `verify_marginals`
     enforces Σ reltype-edge-deltas == edge_count_delta when exact; `validate` on
     magic/version; `read_via`/`key` under `segments/<uuid>/SEGMENT.json`. 10 tests
     (roundtrip, content-hash + MAC tamper across fields, wrong-key/absent, negative
     deltas, defaults, store I/O) green, clippy clean.
  5. Populate `SegmentRef` in the set manifest (already forward-shaped) + codec goldens
     + fuzz targets.
     **DONE** — `SegmentRef::from_manifest(&SegmentManifest)` (uuid/bands/content_hash
     bridge; a set built from it tiles via `Extents::from_set`); public panic-safe codec
     surface `NodeRow/EdgeRow::encode/decode`, `encode/decode_adj_fragment`,
     `decode_segment_meta` (decoders no longer pre-size from untrusted counts); hand-
     computed byte goldens for node/edge/adj records + a meta round-trip; new fuzz target
     `fuzz/fuzz_targets/segment_decode.rs` (+ graph-format fuzz dep), type-checks.
     137 graph-format lib tests green, clippy clean, whole workspace builds.
Exit: round-trip + hand-computed codec goldens + fuzz green; encrypted segment
open/refuse parity with generation fixtures. Do NOT wire the read path yet — that's
Phase 3. **ALL EXIT CRITERIA MET — Phase 2 COMPLETE.**

**Resume prompt to paste after a context clear:**
> Resume the segmented-core work on branch `writeable`. Read `docs/SEGMENTED-CORE-PLAN.md`
> "RESUME HERE" + the Phase 5 slice log first. **Committed through HP17 (Phase 5 slice 5.1, T3
> segment compaction).** Phases 1–4 DONE (the T2-flush writer is feature-complete). **Phase 5
> (T3 segment↔segment merge) IN PROGRESS.** Slice 5.1 shipped the **merge writer**
> (`crate::merge_segment::write_merge_segment` — folds a contiguous run of upper segments
> newest-wins into one, summed marginals), the **orchestrator**
> (`Graphs::compact_graph_segments` — pick run → merge → publish spliced set → upload → swap →
> rebind), and a lightweight **delta rebind** (`DeltaWriter::rebind_core_uuid` — compaction
> preserves `extents().total()`, so no freeze/replay/rebase). Run selection is explicit.
> Baseline: **720 slater lib tests** (140 graph-format, 78 slater-delta), clippy + fmt clean.
>
> NEXT: **slice 5.2** — harden the merge over the cases 5.1's test doesn't exercise (a base-node
> **delete** folded across the run; a **partial** run `[i,j)`; an **encrypted** merge; a
> **remote-store** merge; a zero-width band). Then **slice 5.3** — the **admission policy**
> (`maxUpperSegments`, size-tiered run selection, scheduling; DECISIONS.md D50 → four-rung
> ladder). An auto-compaction trigger, like the flush auto-trigger, is Phase-6-gated. Deferred
> leanness (benign): a born-then-deleted edge leaves an orphan edge row (adjacency-suppressed,
> never read); postings are a union.
>
> DISCIPLINE: `CARGO_TARGET_DIR=/home/rickk/.cache/slater-target cargo …` +
> `dangerouslyDisableSandbox`. Full workspace + clippy green; `cargo fmt --all` before
> commit. Commit to `writeable` (no PRs). Update this "RESUME HERE" + slice log + add an HP
> per sub-slice.

**Key files for Phase 5:** `slater/src/{merge_segment.rs (the merge writer), server.rs
(compact_graph_segments + upload_flush_to_store + publish_set_and_current), delta_writer.rs
(rebind_core_uuid, freeze, retire), segstack.rs (LoadedSegment readers, extents), flush_segment.rs
(shared inventory + block consts)}`, `graph-format/src/{segment.rs,segindex.rs,segpostings.rs
(reader enumeration APIs the fold walks), segmanifest.rs,setmanifest.rs (the manifests spliced)}`,
the read-side fold in `slater/src/{read_view.rs,exec.rs}` (the merge's output must satisfy it).
