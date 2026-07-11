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

**Branch:** `writeable`. **Committed through:** Phase 2 slice 2 (`segment.rs` section
format). **Phase 1 is DONE.** In progress: Phase 2 (core-segment format) — slices 1–2
done, next is slice 3 (ISAM fragment + removal sidecar; posting fragments).

**Safe handoff points (each is a green commit — clear context freely at any of these):**
- HP0 — Phase 0.5 committed (`a6e4d34`).
- HP1 — `SetManifest` type + graph-format tests, committed (`4c80c6b`). ✓
- HP2 — builder writes singleton set + reader opens through it (implicit-singleton
  fallback), 698 slater lib + slater-build suites green, clippy clean, committed. ✓
  ← current baseline; **Phase 1 complete.**
- HP3 (next track) — Phase 2 segment format landing in slices (see below).

**Immediate next step — start Phase 2 (core-segment format).** Build
`graph-format/src/segment.rs` incrementally, each slice its own green commit:
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
     **3b TODO** — posting fragments (per-reltype born src/tgt endpoint id lists).
  4. `SEGMENT.json` (signed marginal deltas as i64, per-index dirty bits, bands,
     inventory+hashes, encryption/MAC parity with `manifest.rs`).
  5. Populate `SegmentRef` in the set manifest (already forward-shaped) + codec goldens
     + fuzz targets.
Exit: round-trip + hand-computed codec goldens + fuzz green; encrypted segment
open/refuse parity with generation fixtures. Do NOT wire the read path yet — that's
Phase 3.

**Resume prompt to paste after a context clear:**
> Resume the segmented-core track for slater (branch `writeable`). Read
> `docs/SEGMENTED-CORE-PLAN.md`, especially "RESUME HERE", and the task list. Phase 1
> is done; continue from the next handoff point (Phase 2, core-segment format —
> `graph-format/src/segment.rs`, template `slater-delta/src/l0_offheap.rs`). Build/test
> with `CARGO_TARGET_DIR=/home/rickk/.cache/slater-target cargo …` and
> `dangerouslyDisableSandbox`. Commit at each safe handoff point and update
> "RESUME HERE" as you go.

**Key files:** `graph-format/src/{segment.rs(new),extents.rs(new),setmanifest.rs,
manifest.rs,isam.rs,blockfile.rs,ids.rs}`, `slater-delta/src/l0_offheap.rs` (template),
`slater/src/{generation.rs,read_view.rs,exec.rs,server.rs,cache.rs}`,
`slater-build/src/common.rs`.
