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

**Branch:** `writeable`. **Committed through:** Phase 3 slice 3.6 (`85d68ff`). **Phases
1–3 DONE.** Next: **Phase 4 — the T2 flush (`DeltaWriter::flush_to_segment`)**, the first
*writer* of a core segment. Everything below Phase 3 is the read side; nothing produces a
segment yet, so all of Phase 3 is exercised only by hand-built fixtures.

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
  ✓ **Phase 3 COMPLETE.** ← current baseline; next track is Phase 4 (T2 flush writer).

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
> Resume the segmented-core track for slater (branch `writeable`). Read
> `docs/SEGMENTED-CORE-PLAN.md`, especially "RESUME HERE", and the task list. Phases 1–3
> are done (read path over a stacked set); continue from the next handoff point (Phase 4 —
> the T2 flush, `DeltaWriter::flush_to_segment`, the first *writer* of a core segment).
> Honour the "Phase 4 entry notes" obligations. Build/test with
> `CARGO_TARGET_DIR=/home/rickk/.cache/slater-target cargo …` and `dangerouslyDisableSandbox`.
> Commit at each safe handoff point and update "RESUME HERE" as you go.

**Key files for Phase 4:** `slater/src/{delta_writer.rs,segstack.rs,generation.rs,
server.rs,consolidate.rs}`, `slater-delta/src/memtable.rs` (delta level → segment
materialisation; `synthetic_base`), `graph-format/src/{segment.rs,segindex.rs,
segpostings.rs,segmanifest.rs,setmanifest.rs}` (the writers), the read-side fold in
`slater/src/{read_view.rs,exec.rs}` (the merge Phase 4's output must satisfy).
