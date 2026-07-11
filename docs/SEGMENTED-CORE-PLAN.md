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
- **Phase 6 — Batch resolve + fences on the write path.** DONE. Slice 6.1 DONE
  (`HP20`): segment-aware `resolve_business_key` (folds the core stack — the note-(e) closure /
  T2·T3 auto-trigger gate). Slice 6.2 DONE (`HP21`): per-fragment value fence (idx.meta v2) on
  the resolve fold. Slice 6.3 DONE (`HP22`): merge-join batch resolve (one block decompress per
  touched block for a whole write batch). Slice 6.4 DONE (`HP23`): T2/T3 auto-trigger wire-up in
  `maybe_maintain_delta` (`segmentFlushBytes` flushes the delta into a core segment; the served
  stack over `maxUpperSegments` folds a run — beside the L0-internal rungs).
- **Phase 7 — T4 retarget + GC.** Functionally complete. `consolidate_graph` collapses a set to
  a singleton via the Phase-0 direct path; retired sets/segments GC'd after a grace period.
  Slice 7.1 DONE (`HP24`): segment-aware consolidation dump (the retarget correctness gate —
  `serialise_binary_dump` folds the core stack). Slice 7.2 DONE (`HP25`): orphan segment/set GC
  core (`Graphs::gc_orphan_segments`, local-fs, marker-based grace). Slice 7.3 DONE (`HP26`): GC
  config (`deltaConfig.segmentGcGraceSecs`) + write-path wiring (post-compaction, post-retarget).
  Slice 7.4 (optional): remote-store GC parity (`ObjectStore::delete`).
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

**Branch:** `writeable`. **Committed through:** Phase 7 slice 7.3 (HP26). **Phases 1–6 DONE;
Phase 7 functionally complete** (7.1 retarget gate + 7.2 GC core + 7.3 config/wiring). The only
work left is the **optional slice 7.4** — remote-store GC parity: add an `ObjectStore::delete`
and have `gc_orphan_segments` reclaim a remote set/segment's objects (today it is a no-op on a
non-local-fs store), with the grace covering lazy block re-fetch (the local-fs fd-persistence
argument does not hold on a remote backend). If 7.4 is not pursued, **Phase 8** (bench harness +
hardening + docs) is next. The retarget path is `consolidate_graph` (already segment-aware after
7.1); the GC entry point is `Graphs::gc_orphan_segments(name, data_dir, grace_secs)`, wired into
`maybe_maintain_delta` (post-compaction) and `execute_consolidate` (post-retarget) under
`deltaConfig.segmentGcGraceSecs > 0`.

**Phase 7 (T4 retarget + GC) — slice 7.1 DONE (segment-aware consolidation dump — the retarget
correctness gate).** `consolidate_graph` already freezes the delta, dumps `MergedView(core, delta)`
to a scratch binary dump, rebuilds via the Phase-0 direct-ingest builder, swaps, and retires — so it
*structurally* collapses a set to a singleton. But `serialise_binary_dump` was **not stack-aware**:
its Phase-0.5 byte-copy fast paths gated only on the write-delta (`node_patch` / `is_tombstoned` /
`edge_patches`), and `raw_node_labels`/`raw_edge_props` read the **base** block store keyed on the
base uuid — so over a stacked set a base id patched/tombstoned **only in a segment** would byte-copy
stale base bytes (losing the segment write) and a segment tombstone would not elide the id from the
dense renumbering. Fixed by folding the stack into the serialiser: a **combined tombstone set**
(delta ∪ segment, built single-pass as the node loop skips ids — so `compact_id` still matches the
node append positions and born-then-deleted rows are reclaimed at the dense-id level, the leanness
Phase 5 deferred), the node byte-copy fast path additionally gated on
`stack.resolve_node_row(id).is_none()` (a segment-overridden base id takes the decode-through-stack
slow path, already segment-aware via `node_record`/`node_label_ids_par`/`core_named_props`), and the
edge fast path gated on `stack.resolve_edge_row(id).is_none()` with the dst skip + `compact_id` over
the combined set. The **singleton set is byte-for-byte unchanged** (`resolve_node_row` is an instant
`None` on an empty segment list, gated further behind a `!stacked` check so a non-flushed
consolidation pays nothing). Tested: `consolidation_dump_folds_the_segment_stack` (dump of a stacked
gen carries the segment-patched age not stale base, reclaims the tombstoned node with gapless
renumbering, carries the born node/edge with compacted endpoints) +
`consolidate_over_a_stacked_set_collapses_to_a_singleton` (the retarget folds a stacked core back to
a singleton, re-binds the writer, carries a post-freeze write forward). **742 slater lib** (+2) + 141
graph-format + 78 slater-delta + full workspace green (28 suites), clippy + fmt clean. **NEXT: slice
7.2** — orphan segment/set GC: flush (4.4-d) and compaction (5.1) leave superseded `segments/<uuid>/`
dirs + `sets/<uuid>.json` on disk, and a retarget orphans the whole prior set; a grace-period sweep
reclaims the unreferenced ones (live set computed from `current` → set → base + segment uuids).

**Phase 6 (write-path resolve) — slice 6.4 DONE (T2/T3 auto-trigger wire-up — Phase 6
CLOSED).** The two segment-tier rungs of the D50 ladder now auto-fire from the write path,
beside the existing L0-internal rungs, safe because the 6.1 segment-aware resolve gate is met (a
concurrent re-`MERGE` of a just-flushed key resolves through the new segment instead of
duplicating). In `maybe_maintain_delta` (`server.rs`), under the same `!is_consolidating()` guard
and after the memtable→L0 / L0→L0 rungs: **T2** — when the *whole* delta (`writer.total_bytes()` =
memtable + every L0 level) reaches the new **`deltaConfig.segmentFlushBytes`** (distinct from
`memtableBytes`, which drains only the active memtable → an L0 level), `flush_graph_to_segment`
folds the entire delta into one durable core segment (the cheap O(delta) drain that keeps the delta
small without an O(core) consolidation); **off by default (0)** like `deltaCorePercent`, and
suppressed under `offHeapL0` (that flush still bails — no warn-spam). **T3** — when the served set
carries **more than `maxUpperSegments`** upper segments (pre-checked cheaply on the resident
`stack().segments().len()`, the selector's own admission predicate — so no blocking task spawns per
write), `compact_graph_segments_auto` folds a size-tiered run; runs *after* the T2 flush so a
freshly appended segment that tips the stack over budget folds in the same pass. Both run on the
blocking pool and take the `begin_consolidation` single-flight claim inside `Graphs` (so they never
overlap each other or a consolidation); a lost race bails "already in progress" (new helper
`is_already_in_progress`, logged at **debug** not warn). The L0 rungs still run regardless, so the
memtable always drains even if a flush bails — the T2 flush's redundant L0 write before it folds the
whole delta is the cheap price. New `ConnCtx` fields `segment_flush_bytes` / `max_upper_segments` /
`off_heap_l0` (plumbed from `deltaConfig`); `maxUpperSegments`'s config doc + D50 rung-3/4 text
lose their "Phase-6-gated" caveats. Tested: one new e2e oracle
(`write_path_auto_flushes_and_compacts_segments`: `segmentFlushBytes=1` ⇒ every write folds the
delta into a segment [stack 1→2→3], `maxUpperSegments=2` ⇒ the third write's pass compacts the run
back within budget; every born row reads through the compacted stack **and** survives a from-disk
reopen with no delta). **740 slater lib** (+1) + 141 graph-format + 78 slater-delta + full workspace
green (28 suites), clippy + fmt clean. **NEXT: Phase 7** (T4 retarget: collapse a set to a singleton
via the Phase-0 direct path; GC retired sets/segments after a grace period — the born-then-deleted
orphan edge row + union postings noted in Phase 5 are the reclaimable leanness). Deferred `bail!`s
still open in the *flush* writer (each Phase-6-independent): patch-then-delete of the same core edge
in one delta, and a flush over an off-heap L0 level (the T2 auto-trigger is gated off under
`offHeapL0` for exactly this reason).

**Phase 6 (write-path resolve) — slice 6.3 DONE (merge-join batch resolve).** The batched
write path (`execute_write_batch`) resolved each `UNWIND` row's business key one-at-a-time,
re-decompressing the same ISAM leaf blocks per row — the bulk-write floor (memory
`bulk-delete-isam-resolve-floor`); the 6.2 fence only skips a *segment* that cannot hold a given
key, so a batch of many distinct keys still touches many blocks. Now the batch's keys resolve in
**one merge-join sweep**. `(label, key)` is **fixed across a batch** (only the value varies), so
the values are deduped + sorted once and streamed against the sorted base ISAM and each segment
fragment: `IsamReader::lookup_eq_sorted(&[&Value])` walks the leaf blocks in one forward pass —
each touched block decoded once, a decoded-block memo carrying it across the ascending keys;
`SegmentIndexReader::lookup_eq_sorted` fence-prunes the keys then sweeps the in-fence set;
`CoreStack::fold_index_eq_batch` folds the stack oldest→newest (per-segment removal suppress on
every key's id vec — by id, value-independent — then the fence-gated fragment sweep unioned in),
carrying **exactly** the 6.1/6.2 suppress-then-union + fence semantics. `resolve_business_keys_batch`
(server) drives it: base sweep → batch fold → sort/dedup → per-value `Absent`/`Unique`/`Ambiguous`
verdict, byte-identical to N `resolve_business_key` calls (the singleton set short-circuits to the
base sweep, an unindexed pair or any read failure collapses **all** values to `Unindexed` — never
`Absent`, so a read failure can't manufacture a duplicate). `resolve_node_op` /
`merge_creates_node` were split into `_from(resolution)` variants so the batch path resolves the
core **once** per distinct key and still shares the born-id / create-vs-match / delete decisions
with the single path (they can't drift); `KeyResolution` is now `Copy`. The core probe reads `gen`
only (never the accumulating delta), so hoisting every row's resolution to the top of the batch is
sound. **NEXT (closing Phase 6 slice):** wire the T2/T3 auto-triggers into `maybe_maintain_delta`
(`server.rs`) — the 6.1 correctness gate is met; see the note below on its design load. Tested: an
`isam` sweep-vs-point-lookup equivalence over the int + string shapes (present/absent/boundary-
spanning keys); the fence-gated batch sweep folded into the three `segindex` round-trips; a
`CoreStack` oracle (`fold_index_eq_batch_matches_point_folds`, batch == N point folds); an e2e
oracle (`batch_resolve_through_the_stack_reuses_flushed_keys_no_duplicate`: one `UNWIND … MERGE …
SET` batch over a flushed segment reuses a segment-born key, patches a base key, borns an absent
key, honours a within-batch duplicate — duplicate-free across a second flush + reopen + re-batch);
`bench_resolve_business_key_over_the_segment` extended with a batch-vs-per-row timing (same
verdicts). **739 slater lib** (+2) + **141 graph-format** (+1) + 78 slater-delta + full workspace
green (28 suites), clippy + fmt clean.

**Phase 6 (write-path resolve) — slice 6.2 DONE (per-fragment value fence on the resolve
fold).** The resolve fold (`CoreStack::fold_index_eq` / `fold_index_range`, shared by the write
path and every read-path index probe) probed *every* segment's ISAM fragment for `(label, prop)`,
each an uncached leaf-block decompress — the ISAM floor (memory `bulk-delete-isam-resolve-floor`).
Now each fragment carries a **resident value fence**: the `cmp_key` min/max of its entries, written
into `idx.meta` (bumped to **v2**: `… ‖ removals ‖ fence`, `fence = 0 | 1 ‖ min ‖ max`) by
`write_index_fragments` (derived from `entries`, `None` for a removal-only fragment). The fold now
gates the fragment lookup on `SegmentIndexReader::may_hold_eq` / `may_hold_range` — a probe whose
key/range falls outside the fence is a **provable miss** and skips the leaf-block read entirely, at
no I/O. The fence gates **only** the fragment `lookup_*`, never the removal sidecar (removals
suppress base ids by *id*, independent of the probed value, so they are always applied). Results
are byte-identical to the un-fenced fold — the whole slater lib + graph-format suites are unchanged
(the fence can only skip a lookup that would have returned empty). Fence min/max and eq/range
overlap (inclusivity + unbounded sides + cross-type `cmp_key`) are unit-tested in the three
`segindex` round-trips (plaintext / object-store / encrypted, so the fence survives every backend
and the cipher); a `CoreStack`-level oracle (`fold_index_eq_gates_on_the_fence_and_suppresses_
removals`) drives a real stacked segment through the gated fold (moved-away value → gone, new value
→ patched id, born id → born value, below-fence key → skipped-and-empty). **737 slater lib** (+1) +
**140 graph-format** (fence assertions folded into the existing round-trips) + 78 slater-delta +
full workspace green, clippy + fmt clean. **NEXT in Phase 6:** the merge-join **batch** resolve
(sort the write batch's keys once, stream-merge against the sorted base + each segment fragment in
one pass — kills the per-row ISAM decompress), then the small slice that wires the T2/T3
auto-triggers now that the gate (6.1) is met.

**Phase 6 (write-path resolve) — slice 6.1 DONE (segment-aware `resolve_business_key`).**
The single write-path resolver — `server::resolve_business_key`, the choke point for *every*
business-key resolution (node `MERGE` via `resolve_op`, edge endpoints via `resolve_endpoint`)
— now **folds the core stack** over the base equality probe, closing the 4.1 note (e) gap: it
reads the base ids (`gen.range_index(idx).lookup_eq`, the index descriptor still comes from the
base manifest via `index_for`), then, when the served set carries segments
(`!stack.is_singleton()`), calls `CoreStack::fold_index_eq` (the same oldest→newest
suppress-then-union fold the read path uses) and sort+dedups before the `[] → Absent /
[one] → Unique / _ → Ambiguous` verdict. Effect: a `MERGE` of a key **flushed into a segment**
resolves to the segment id and patches it (no duplicate born node); a base key **deleted into a
segment** resolves `Absent` (its base index entry sits in the segment's `removals`, so a
re-`MERGE` reborns it); a key **relocated by a segment patch** resolves only under its new
value; a fold read error collapses to `Unindexed` (never `Absent` — matching the base probe's
`Err`, so a read failure can't manufacture a duplicate). The **singleton set short-circuits to
the base ids**, so a non-flushed graph is byte-for-byte unchanged. Edge-id resolution
(`find_core_edge_id`) already went through the segment-aware read path (`outgoing_adj` over a
`MergedView`), so only the node-key probe was the gap. **This is the correctness gate the T2/T3
auto-triggers were waiting on** — with resolve now segment-aware, a concurrent re-`MERGE` of a
just-flushed key during the freeze→retire window finds it in the segment instead of duplicating
(and the flush/consolidation retire's own WAL-tail re-resolve, already *documented* as
segment-aware at `server.rs:748`, actually is now). Two e2e oracles
(`resolve_through_the_stack_reuses_a_flushed_key_no_duplicate`: flush 2 born nodes + a born
edge, then re-`MERGE` the born key → patched not duplicated, re-`MERGE` a base key, add an edge
off a segment-born endpoint, count stays 5 across a second flush + a reopen + a post-reopen
re-`MERGE`; `resolve_reborns_a_key_deleted_into_a_segment`: delete a base node into a segment ⇒
re-`MERGE` reborns it, a second `MERGE` is memtable-idempotent). **736 slater lib** (+2) + 140
graph-format + 78 slater-delta + full workspace green, clippy + fmt clean. **NEXT in Phase 6:**
fences/blooms on the resolve fold (skip a segment whose value-range can't hold the key) and the
merge-join **batch** resolve (the bulk-write ISAM floor — memory `bulk-delete-isam-resolve-floor`);
then the small slice that wires the T2/T3 auto-triggers now that this gate is met.

**Phase 5 (T3 segment compaction) — slice 5.3 DONE (admission policy).** The fourth rung of
the D50 ladder is in: a **size-tiered run selector** and a policy entry point that drives the
5.1 writer. `crate::merge_segment::select_compaction_run(sizes, max_upper_segments) ->
Option<(start, end)>` is the **pure** admission+selection predicate — admission is by *segment
count* (a point read may consult every upper segment, so the stack is compacted only once it
exceeds `maxUpperSegments`; `0` disables — the explicit `compact_graph_segments(start,end)`
path is untouched), and selection is **size-tiered**: the *longest* contiguous run of same-tier
segments (largest ≤ `SIZE_TIER_RATIO=4`× smallest) — it reduces fan-out most while rewriting
each byte once — tie-broken by the *smallest* total bytes (prefer the cheaper, smaller tier);
if no two adjacent segments share a tier (sizes escalate by >4× at every step) it falls back to
the *cheapest adjacent pair* so the count still drops (progress guaranteed while over budget).
Per-start scan is O(n²) over the segment count (tens at most) *because dropping a run's smallest
member can raise its floor and admit a longer run to its right* — a greedy-from-each-index scan
would miss it (a unit test pins this). `Graphs::compact_graph_segments_auto(name, vc, data_dir,
max_upper_segments)` reads the served stack's per-segment on-disk sizes (`Σ manifest.files.bytes`
— the write-amplification proxy), calls the selector, and folds the chosen run via 5.1's
`compact_graph_segments` (or returns `Ok(None)` — a true no-op, nothing published/swapped). New
config knob `deltaConfig.maxUpperSegments` (default **8**, on like `l0CompactionTrigger`); D50 in
DECISIONS.md is rewritten from two tiers to the **four-rung ladder** (memtable→L0, L0→L0,
L0→segment T2 flush, segment→segment T3 compaction) above the terminal O(core) rebuild. **Both T3
rungs' auto-firing from the write path stays Phase-6-gated** (needs a segment-aware write
resolve) — `compact_graph_segments_auto` is the explicit driver until then, exactly as
`flush_graph_to_segment` is for T2. Eight new pure-selector unit tests
(`merge_segment::tests`: disabled, within-budget, uniform-whole-stack, longest-wins,
tie-to-smaller-tier, dropped-floor-admits-a-longer-run, escalating-fallback-pair,
zero-width-joins) + one e2e (`auto_compaction_admits_only_when_over_budget`: 3 flushes ⇒ 3
segments; `auto` at threshold ≥3 and at 0 are no-ops; at 2 admits and folds the one-tier run into
one; 1-segment re-check is a no-op; every read identical across the no-ops, the fold, and a
reopen). **734 slater lib** (+9) + 140 graph-format + 78 slater-delta + full workspace green,
clippy + fmt clean.

**Phase 5 (T3 segment compaction) — slice 5.2 DONE (merge hardening).** Five new e2e oracle
tests exercise the cases 5.1's single test did not — and **the 5.1 merge writer + orchestrator
handled all five with no code change** (a hardening slice that confirmed, not patched, the
design): a **base-node delete folded across the run** (a below-run tombstone + its incident-edge
`removed` fragments are *carried*, not reclaimed — Bob and his two KNOWS edges stay gone, summed
marginals net the delete); a **partial run `[1,3)`** with a segment below (seg 0) and above (seg
3) — Carol is patched in every segment (11→22→33→44) and still resolves to seg 3's 44 (above wins)
while seg 0's below-run 11 stays superseded via the merged segment's carried index removal, and
below/within/above-run born nodes all survive (the splice `segments[..start] + merged +
segments[end..]` preserves precedence and the bands still tile); a **zero-width band** in the run
(seg 0 is a patch-only flush ⇒ empty node/edge bands — the contiguity check accepts the zero-width
tile, the patched row + carried removal survive); an **encrypted** merge (fresh per-segment cipher
+ KDF header, sealed MAC, decrypts on read, reopens only WITH the key); and a **remote-store**
merge (the merged segment + spliced set + `current` upload through the `ObjectStore`; the run's two
pre-merge dirs remain for a later GC, so the store holds three segment dirs; a store-native reopen
serves the fold). **725 slater lib** (+5) + 140 graph-format + 78 slater-delta + full workspace
green, clippy + fmt clean.

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

**Phase 6 CLOSED (after 6.4):** the **note-(e) gate is met** (6.1), both resolve-fold performance
floors are closed — the **per-fragment value fence** (6.2) and the **merge-join batch resolve**
(6.3) — and the **T2/T3 auto-triggers are wired** (6.4): `maybe_maintain_delta` fires
`flush_graph_to_segment` once the whole delta reaches `segmentFlushBytes` (a threshold distinct from
`memtableBytes`, which drains only the active memtable → L0) and `compact_graph_segments_auto` once
the served stack exceeds `maxUpperSegments`, beside the pre-existing L0-internal
memtable→L0/L0→L0 rungs. `vector_cache`/`data_dir` were already on `ConnCtx`; the slice added
`segment_flush_bytes` / `max_upper_segments` / `off_heap_l0`. **Phase 5 stays functionally
complete** (writer 5.1 + hardening 5.2 + admission 5.3); deferred leanness carried from 5.1 (each
benign, reclaimable in Phase 7): a born-then-deleted **edge** leaves an orphan edge row in the
merged segment (its adjacency is suppressed by the fold, so it is never read); postings are a union
(a stale driving hit is filtered by adjacency).

### Phase 7 slice log (T4 retarget + GC — IN PROGRESS)

**Scope (decided).** The terminal D50 rung + the disk-reclamation the earlier phases deferred.
Two slices:
- **7.1 DONE** (HP24) — **segment-aware consolidation dump (the retarget correctness gate).**
  `consolidate_graph`
  already folds `MergedView(core, delta)` to a scratch binary dump → builder → swap → retire, so it
  *structurally* collapses a set to a singleton via the Phase-0 direct path. But
  `serialise_binary_dump` is **not stack-aware**: its Phase-0.5 byte-copy fast paths gate only on the
  *write-delta* (`delta.node_patch`/`is_tombstoned`/`edge_patches`), never on the segment stack, and
  `raw_node_labels`/`raw_edge_props` read the **base** block store keyed on the base uuid. So over a
  stacked set a base id **patched or tombstoned only in a segment** would byte-copy stale base bytes
  (losing the segment write), and a segment tombstone would not elide the id from the dense
  renumbering. The fix folds the stack into the serialiser: a **combined tombstone set** (delta ∪
  segment, built single-pass as the node loop skips ids — so `compact_id` still matches node append
  positions and born-then-deleted rows are reclaimed at the dense-id level), the node byte-copy fast
  path additionally gated on `stack.resolve_node_row(id).is_none()` (a segment-overridden base id
  takes the decode-through-stack slow path, which is already segment-aware via `node_record` /
  `node_label_ids_par` / `core_named_props`), and the edge fast path gated on
  `stack.resolve_edge_row(id).is_none()` with the dst skip + `compact_id` over the combined set. The
  singleton set is byte-for-byte unchanged (`resolve_node_row` is an instant `None` on an empty
  segment list, gated further behind a `!stacked` check so a non-flushed consolidation pays nothing).
  Two new e2e oracles (`consolidation_dump_folds_the_segment_stack`: flush a base patch + base
  delete + born node + born edge into one segment, then the dump of the stacked gen shows the
  segment-patched age — not the stale base — reclaims the tombstoned node with gapless renumbering,
  and carries the born node/edge with compacted endpoints; `consolidate_over_a_stacked_set_collapses
  _to_a_singleton`: the retarget folds a stacked core back to a singleton via the direct dump path,
  re-binds the writer, carries a post-freeze write forward). **742 slater lib** (+2) + 141
  graph-format + 78 slater-delta + full workspace green, clippy + fmt clean.
- **7.2 DONE** (HP25) — **orphan segment/set GC core (grace-period sweep).** Flush (4.4-d) and
  compaction (5.1) intentionally leave the superseded `segments/<uuid>/` dirs and `sets/<uuid>.json`
  files on disk for a later sweep; a retarget (7.1) to a singleton orphans the whole prior set + all
  its segments. New `Graphs::gc_orphan_segments(name, data_dir, grace_secs) -> SegmentGcReport`
  computes the live reference set from `current` (→ set → its segment uuids; a bare-gen `current`
  after a retarget has no set file ⇒ nothing live ⇒ every set/segment is an orphan) and reclaims the
  unreferenced `segments/` dirs + `sets/*.json` files. **Reader safety** via a grace measured from the
  *retirement observation*, not file creation: the first sweep drops a `.gcmark` marker (its mtime is
  the observation), a later sweep deletes once the marker ages past `grace_secs` (`0` = immediate).
  Local-fs readers hold the segment files open, so `remove_dir_all` is safe under an in-flight reader
  (the inode outlives the unlink); the grace covers a reader mid-open and a future remote backend.
  **Single-flight**: takes the `begin_consolidation` claim (when a writable layer exists) so it never
  races a flush/compaction/consolidation — with the claim held, no in-flight op is publishing a
  segment `current` does not yet name, so even immediate mode is safe. Never touches a **generation**
  directory (base or the retargeted singleton) — only `segments/` and `sets/`. **Local-fs only** for
  now (deletion is `std::fs` under `data_dir`; the `ObjectStore` trait has no delete — a remote sweep
  is slice 7.4). Three e2e oracles (`gc_reclaims_stale_sets_and_compacted_segments`,
  `gc_respects_the_grace_before_reclaiming`, `gc_after_retarget_reclaims_the_prior_set`). **745 slater
  lib** (+3) + 141 graph-format + 78 slater-delta + full workspace green, clippy + fmt clean.
- **7.3 DONE** (HP26) — **GC config + write-path wiring.** New `deltaConfig.segmentGcGraceSecs`
  knob (`u64` seconds; **`0` disables**, off by default) plumbed onto `ConnCtx`, and the sweep wired to
  fire after the orphan-creating events — a **T3 compaction** (in `maybe_maintain_delta`, only when a
  run actually folded, so it is not paid per write) and a **consolidation/retarget** (at the end of
  `execute_consolidate`). Both run on the blocking pool, are guarded by the knob, and treat a lost
  single-flight race / transient error as benign (debug/warn — never fails the compaction or the
  already-published consolidation). One e2e (`write_path_auto_gc_marks_orphans_after_compaction`:
  four auto-flushes drive a compaction whose orphaned run dirs the wired sweep marks with `.gcmark`
  within the grace, then an immediate explicit sweep reclaims them; every born row survives). **746
  slater lib** (+1) + 141 graph-format + 78 slater-delta + full workspace green, clippy + fmt clean.
- **7.4 — remote-store GC parity (optional).** Add an `ObjectStore::delete` and reclaim a remote
  set/segment's objects (with the grace covering lazy block re-fetch, where the fd-persistence
  argument does not hold).

### Phase 6 slice log
- **6.4 DONE** (HP23): **T2/T3 auto-trigger wire-up** — Phase 6 CLOSED. `maybe_maintain_delta`
  (`server.rs`) now fires the two segment-tier rungs beside the L0-internal ones, under the same
  `!is_consolidating()` guard, safe because the 6.1 segment-aware resolve gate is met. **T2**: whole
  delta (`total_bytes()` = memtable + every L0 level) ≥ new `deltaConfig.segmentFlushBytes` ⇒
  `flush_graph_to_segment` folds it into a core segment; off by default (0, like `deltaCorePercent`),
  suppressed under `offHeapL0` (that flush bails). **T3**: served `stack().segments().len()` >
  `maxUpperSegments` (cheap resident pre-check = the selector's admission predicate) ⇒
  `compact_graph_segments_auto` folds a run, *after* the T2 flush so a fresh segment tips-and-folds
  in one pass. Both on the blocking pool + the `begin_consolidation` single-flight claim; a lost
  race bails via new `is_already_in_progress` (debug, not warn). L0 rungs still run regardless (the
  memtable always drains even if a flush bails). New `ConnCtx` fields `segment_flush_bytes` /
  `max_upper_segments` / `off_heap_l0`; `maxUpperSegments` config doc + D50 rung-3/4 shed their
  "Phase-6-gated" caveats. One e2e oracle (`write_path_auto_flushes_and_compacts_segments`: stack
  1→2→3 then compacts within budget; born rows survive the fold + a from-disk reopen). **740 slater
  lib** (+1) + 141 graph-format + 78 slater-delta + full workspace green, clippy + fmt clean. ←
  current baseline; next is Phase 7 (T4 retarget + GC).
- **6.3 DONE** (HP22): **merge-join batch resolve** — the bulk-write ISAM floor (memory
  `bulk-delete-isam-resolve-floor`). `execute_write_batch` resolved each `UNWIND` row's key with a
  per-row ISAM point probe (re-decompressing the same blocks; the 6.2 fence only skips a segment,
  not a block). Now, because `(label, key)` is fixed across a batch, the values are deduped +
  sorted once and streamed against the sorted index in one pass: new
  `IsamReader::lookup_eq_sorted(&[&Value])` (one forward block-walk, each touched block decoded
  once via a sweep-local memo), `SegmentIndexReader::lookup_eq_sorted` (fence-prune the keys, sweep
  the in-fence set, scatter back), `CoreStack::fold_index_eq_batch` (oldest→newest per-segment
  removal-suppress on every key's id vec + fence-gated fragment sweep union — carries 6.1/6.2
  semantics exactly), and `resolve_business_keys_batch` (server: base sweep → batch fold →
  sort/dedup → per-value verdict, byte-identical to N `resolve_business_key`; unindexed/read-fail
  ⇒ all `Unindexed`, never `Absent`). `resolve_node_op` / `merge_creates_node` split into
  `_from(resolution)` variants (batch resolves the core once per distinct key, shares the born-id /
  create / delete decisions with the single path); `KeyResolution` is now `Copy`. The core probe
  reads `gen` only, so hoisting every row's resolution up front is sound. Tested: `isam`
  sweep-vs-point equivalence (int + string, present/absent/boundary-spanning); the fence-gated
  batch sweep in the three `segindex` round-trips; a `CoreStack` oracle
  (`fold_index_eq_batch_matches_point_folds`); an e2e oracle
  (`batch_resolve_through_the_stack_reuses_flushed_keys_no_duplicate`); the resolve bench extended
  with a batch-vs-per-row timing. **739 slater lib** (+2) + **141 graph-format** (+1) + 78
  slater-delta + full workspace green, clippy + fmt clean. ← current baseline; next is the closing
  Phase-6 slice — the T2/T3 auto-trigger wire-up.
- **6.2 DONE** (HP21): **per-fragment value fence on the resolve fold**. `idx.meta` → **v2**
  (`… ‖ removals ‖ fence`, `fence = 0 | 1 ‖ min ‖ max`); `write_index_fragments` derives each
  fragment's `cmp_key` min/max from its entries (`None` for a removal-only fragment).
  `SegmentIndexReader::may_hold_eq` / `may_hold_range` gate the fold's fragment `lookup_*`
  (`CoreStack::fold_index_eq` / `fold_index_range`) — a key/range outside the fence is a provable
  miss and skips the leaf-block decompress (the ISAM floor) at no I/O. The fence gates only the
  fragment lookup, never the removal suppress (removals are by id, not value). Byte-identical
  results (a skipped lookup would have returned empty). Fence + eq/range overlap unit-tested in the
  three `segindex` round-trips; a `CoreStack` oracle
  (`fold_index_eq_gates_on_the_fence_and_suppresses_removals`) drives a real segment through the
  gated fold. **737 slater lib** (+1) + **140 graph-format** + 78 slater-delta + full workspace
  green, clippy + fmt clean.
- **6.1 DONE** (HP20): **segment-aware `resolve_business_key`** — the note-(e) closure and the
  T2/T3 auto-trigger gate. The write path's single business-key resolver now folds the core stack
  over the base equality probe (`CoreStack::fold_index_eq`, the read path's oldest→newest
  suppress-then-union fold), sort+dedups, then verdicts `Absent`/`Unique`/`Ambiguous`; a fold read
  error collapses to `Unindexed` (never `Absent`, so a read failure can't manufacture a duplicate);
  the singleton set short-circuits to the base ids (a non-flushed graph is unchanged). Edge-id
  resolution (`find_core_edge_id`) already used the segment-aware read path, so only the node-key
  probe was the gap. Two e2e oracles
  (`resolve_through_the_stack_reuses_a_flushed_key_no_duplicate`,
  `resolve_reborns_a_key_deleted_into_a_segment`). **736 slater lib** (+2) + 140 graph-format + 78
  slater-delta + full workspace green, clippy + fmt clean.

### Phase 5 slice log
- **5.3 DONE** (HP19): **admission policy** — the fourth D50 rung. New pure predicate
  `merge_segment::select_compaction_run(sizes, max_upper_segments) -> Option<(start,end)>`
  (admission by segment count vs `maxUpperSegments`, `0` disables; size-tiered selection: longest
  same-tier run within `SIZE_TIER_RATIO=4`×, tie → smallest total bytes; escalating-sizes fallback
  to the cheapest adjacent pair; O(n²) per-start scan because a dropped floor can admit a longer
  run to the right). New orchestrator `Graphs::compact_graph_segments_auto` (reads
  `Σ manifest.files.bytes` per segment, selects, folds via 5.1's `compact_graph_segments`, or
  `Ok(None)` no-op). New config `deltaConfig.maxUpperSegments` (default 8). DECISIONS.md D50
  rewritten two-tier → four-rung ladder. Auto-firing both T3 rungs from the write path stays
  Phase-6-gated. 8 selector unit tests + 1 e2e (`auto_compaction_admits_only_when_over_budget`).
  **734 slater lib** (+9) + 140 graph-format + 78 slater-delta + full workspace green, clippy + fmt
  clean. ← current baseline; next real work is Phase 6, then a small T2/T3 auto-trigger wire-up.
- **5.2 DONE** (HP18): **merge hardening** — five new e2e oracle tests over the cases 5.1's test
  did not exercise, all passing against the **unchanged** 5.1 writer/orchestrator (a hardening
  slice that confirmed the design rather than patching it):
  `compact_folds_a_base_delete_across_the_run` (a below-run node tombstone + incident-edge
  `removed` fragments are carried through the fold, summed marginals net the delete),
  `compact_a_partial_run_preserves_precedence` (4 segments, compact the middle `[1,3)`; Carol
  patched in all four 11→22→33→44 resolves to seg 3's 44 — above the run — while seg 0's below-run
  11 stays superseded, and below/within/above-run born nodes all survive the splice),
  `compact_folds_a_zero_width_band` (a patch-only seg 0 ⇒ empty node/edge bands folds with a
  births-carrying seg 1; the contiguity check accepts the zero-width tile),
  `compact_encrypts_the_merged_segment` (fresh per-segment cipher + KDF header + sealed MAC;
  decrypts on read; reopens only WITH the key; without → refused), and
  `compact_uploads_to_an_object_store` (merged segment + spliced set + `current` upload through a
  `MemObjectStore`; the two pre-merge dirs stay for a later GC ⇒ three segment dirs; store-native
  reopen serves the fold). Each read-probe battery is identical before the compaction, after it,
  and after a reopen. **725 slater lib** (+5) + 140 graph-format + 78 slater-delta + full
  workspace green, clippy + fmt clean.
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
  full workspace green, clippy + fmt clean.
- HP18 — Phase 5 slice 5.2: **merge hardening**. Five new e2e oracle tests
  (`compact_folds_a_base_delete_across_the_run`, `compact_a_partial_run_preserves_precedence`,
  `compact_folds_a_zero_width_band`, `compact_encrypts_the_merged_segment`,
  `compact_uploads_to_an_object_store`), all green against the **unchanged** 5.1 writer +
  orchestrator. 725 slater lib + full workspace green, clippy + fmt clean.
- HP19 — Phase 5 slice 5.3: **admission policy**. Pure size-tiered run selector
  `merge_segment::select_compaction_run` (count-based admission vs `maxUpperSegments`;
  longest-same-tier-run selection, `SIZE_TIER_RATIO=4`×, cheapest-tie, escalating fallback pair;
  O(n²) per-start scan) + orchestrator `Graphs::compact_graph_segments_auto` (size-driven, folds
  via 5.1 or `Ok(None)`) + config `deltaConfig.maxUpperSegments` (default 8). DECISIONS.md D50
  rewritten to the four-rung ladder; both T3 auto-firings stay Phase-6-gated. 8 selector unit tests
  + 1 e2e (`auto_compaction_admits_only_when_over_budget`). 734 slater lib + 140 graph-format + 78
  slater-delta + full workspace green, clippy + fmt clean. **Phase 5 functionally complete.**
- HP20 — Phase 6 slice 6.1: **segment-aware `resolve_business_key`** — the note-(e) closure /
  T2·T3 auto-trigger gate. The write path's business-key resolver folds the core stack
  (`CoreStack::fold_index_eq`) over the base probe, so a `MERGE` of a flushed key resolves to its
  segment id (no duplicate), a key deleted-into-a-segment resolves `Absent` (reborns), and a
  singleton set is unchanged. Two e2e oracles
  (`resolve_through_the_stack_reuses_a_flushed_key_no_duplicate`,
  `resolve_reborns_a_key_deleted_into_a_segment`). 736 slater lib + 140 graph-format + 78
  slater-delta + full workspace green, clippy + fmt clean.
- HP21 — Phase 6 slice 6.2: **per-fragment value fence on the resolve fold**. `idx.meta` → v2
  (`… ‖ removals ‖ fence`); `write_index_fragments` derives each fragment's `cmp_key` min/max;
  `SegmentIndexReader::may_hold_eq` / `may_hold_range` gate the fold's fragment `lookup_*`
  (`CoreStack::fold_index_eq` / `fold_index_range`) so a probe outside the fence skips the
  leaf-block decompress (the ISAM floor) — the removal suppress is never gated. Byte-identical
  results. Fence + overlap unit-tested in the three `segindex` round-trips; a `CoreStack` oracle
  (`fold_index_eq_gates_on_the_fence_and_suppresses_removals`). 737 slater lib + 140 graph-format +
  78 slater-delta + full workspace green, clippy + fmt clean.
- HP22 — Phase 6 slice 6.3: **merge-join batch resolve** — the bulk-write ISAM floor (memory
  `bulk-delete-isam-resolve-floor`). `execute_write_batch` now resolves the whole batch's keys in
  one merge-join sweep instead of a per-row point probe: `IsamReader::lookup_eq_sorted` (forward
  block-walk, each touched block decoded once), `SegmentIndexReader::lookup_eq_sorted` (fence-prune
  + sweep), `CoreStack::fold_index_eq_batch` (oldest→newest suppress-then-union, fence-gated), and
  `resolve_business_keys_batch` (server; byte-identical verdicts to N `resolve_business_key`).
  `resolve_node_op` / `merge_creates_node` split into `_from(resolution)` variants; `KeyResolution`
  is `Copy`. Tests: `isam` sweep-vs-point equivalence; the fence-gated batch sweep in the three
  `segindex` round-trips; `fold_index_eq_batch_matches_point_folds`;
  `batch_resolve_through_the_stack_reuses_flushed_keys_no_duplicate`; the resolve bench extended
  with a batch-vs-per-row timing. 739 slater lib + 141 graph-format + 78 slater-delta + full
  workspace green, clippy + fmt clean.
- HP23 — Phase 6 slice 6.4: **T2/T3 auto-trigger wire-up** — Phase 6 CLOSED. `maybe_maintain_delta`
  (`server.rs`) fires the two segment-tier rungs beside the L0-internal ones (same
  `!is_consolidating()` guard): **T2** `flush_graph_to_segment` when the whole delta
  (`total_bytes()`) ≥ new `deltaConfig.segmentFlushBytes` (off by default, suppressed under
  `offHeapL0`); **T3** `compact_graph_segments_auto` when served `stack().segments().len()` >
  `maxUpperSegments` (cheap resident pre-check), after the T2 flush so a fresh segment folds in the
  same pass. Both on the blocking pool + `begin_consolidation` claim; a lost race bails via new
  `is_already_in_progress` (debug). New `ConnCtx` fields `segment_flush_bytes` / `max_upper_segments`
  / `off_heap_l0`; `maxUpperSegments` doc + D50 rung-3/4 lose their Phase-6-gated caveats. One e2e
  oracle (`write_path_auto_flushes_and_compacts_segments`). 740 slater lib + 141 graph-format + 78
  slater-delta + full workspace green, clippy + fmt clean.
- HP24 — Phase 7 slice 7.1: **segment-aware consolidation dump** — the T4 retarget correctness gate.
  `serialise_binary_dump` (`consolidate.rs`) now folds the core stack so `consolidate_graph` over a
  *stacked* set collapses it to a correct singleton via the Phase-0 direct dump path. The Phase-0.5
  byte-copy fast paths gated only on the write-delta, and `raw_node_labels`/`raw_edge_props` read the
  base block store — so a base id patched/tombstoned **only in a segment** would byte-copy stale base
  bytes and a segment tombstone would not elide the id from the dense renumbering. Fix: a **combined
  tombstone set** (delta ∪ segment, built single-pass as the node loop skips ids, so `compact_id`
  matches the node append positions and born-then-deleted rows reclaim); the node fast path also
  gated on `stack.resolve_node_row(id).is_none()` (a segment-overridden base id takes the
  already-segment-aware `node_record` slow path); the edge fast path gated on
  `resolve_edge_row(id).is_none()` with the dst skip + `compact_id` over the combined set. Singleton
  byte-for-byte unchanged (`!stacked` short-circuit). Two e2e oracles
  (`consolidation_dump_folds_the_segment_stack`,
  `consolidate_over_a_stacked_set_collapses_to_a_singleton`). **742 slater lib** (+2) + 141
  graph-format + 78 slater-delta + full workspace green, clippy + fmt clean.
- HP25 — Phase 7 slice 7.2: **orphan segment/set GC core**. New
  `Graphs::gc_orphan_segments(name, data_dir, grace_secs) -> SegmentGcReport` (`server.rs`) reclaims
  the `segments/<uuid>/` dirs + `sets/<uuid>.json` files the current served set no longer references —
  the disk the flush (4.4-d) and compaction (5.1) slices leave behind, plus everything a retarget
  (7.1) orphans when it collapses a stacked set to a singleton (a bare-gen `current` ⇒ nothing live).
  **Reader-safe grace** measured from the retirement *observation*, not file creation: sweep 1 stamps
  a `.gcmark` marker, a later sweep deletes once it ages past `grace_secs` (`0` = immediate; safe here
  because the sweep holds the `begin_consolidation` claim, so no in-flight flush/compaction is
  publishing a segment `current` does not yet name; local-fs readers hold the files open so a delete
  under them is safe via inode persistence). Never touches a **generation** dir — only `segments/` and
  `sets/`. **Local-fs only** (deletion is `std::fs`; the `ObjectStore` trait has no delete — remote is
  slice 7.4). Three e2e oracles (`gc_reclaims_stale_sets_and_compacted_segments`,
  `gc_respects_the_grace_before_reclaiming`, `gc_after_retarget_reclaims_the_prior_set`). **745 slater
  lib** (+3) + 141 graph-format + 78 slater-delta + full workspace green, clippy + fmt clean.
- HP26 — Phase 7 slice 7.3: **GC config + write-path wiring**. New `deltaConfig.segmentGcGraceSecs`
  (`u64` seconds; `0` disables, off by default) on `DeltaConfig` + `ConnCtx`, and
  `gc_orphan_segments` wired to fire after the orphan-creating events: a **T3 compaction** (in
  `maybe_maintain_delta`, only when a run actually folded — captured via the `compact_auto` `Some`
  result, so GC is not paid per write) and a **consolidation/retarget** (end of `execute_consolidate`).
  Both on the blocking pool, guarded by the knob, a lost single-flight race / transient error benign
  (debug/warn — never fails the fold or the already-published consolidation). `build_writable_ctx_caps`
  gained a grace arg. One e2e (`write_path_auto_gc_marks_orphans_after_compaction`). **746 slater lib**
  (+1) + 141 graph-format + 78 slater-delta + full workspace green, clippy + fmt clean. ← current
  baseline; Phase 7 functionally complete (7.1 retarget gate + 7.2 GC core + 7.3 wiring); only the
  optional 7.4 (remote-store `ObjectStore::delete` parity) remains.

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
> "RESUME HERE" + the Phase 5 slice log first. **Committed through HP18 (Phase 5 slice 5.2, T3
> merge hardening).** Phases 1–4 DONE (the T2-flush writer is feature-complete). **Phase 5
> (T3 segment↔segment merge) IN PROGRESS.** Slice 5.1 shipped the **merge writer**
> (`crate::merge_segment::write_merge_segment` — folds a contiguous run of upper segments
> newest-wins into one, summed marginals), the **orchestrator**
> (`Graphs::compact_graph_segments` — pick run → merge → publish spliced set → upload → swap →
> rebind), and a lightweight **delta rebind** (`DeltaWriter::rebind_core_uuid` — compaction
> preserves `extents().total()`, so no freeze/replay/rebase). Slice 5.2 added **five e2e merge
> hardening tests** (base-delete-across-run, partial run `[1,3)`, zero-width band, encrypted,
> remote-store) — all green against the **unchanged** 5.1 writer + orchestrator (no code change).
> Run selection is explicit. Baseline: **725 slater lib tests** (140 graph-format, 78
> slater-delta), clippy + fmt clean.
>
> NEXT: **slice 5.3** — the **admission policy** (`maxUpperSegments`, size-tiered run selection,
> scheduling; DECISIONS.md D50 → four-rung ladder). An auto-compaction trigger, like the flush
> auto-trigger, is Phase-6-gated. Deferred leanness (benign): a born-then-deleted edge leaves an
> orphan edge row (adjacency-suppressed, never read); postings are a union.
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
