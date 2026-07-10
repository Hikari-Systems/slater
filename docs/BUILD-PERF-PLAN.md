# Build performance — plan for the highest-leverage fixes

Living plan for `slater-build`'s remaining wall-clock and memory problems, written against a
full **91.6M-node / 1.49B-edge** Wikidata build measured on 2026-07-09. Mirrors the style of
`docs/PLAN.md`. Newest decisions get a `### D<N>` entry in `docs/DECISIONS.md`.

## Provenance — the measurement this plan is built on

```sh
slater-build --input  wikidata-full-merge.cypher        # 133 GB business-key MERGE dump
             --graph  wd91m_wr --data-dir data-wd91m-writeable \
             --diagnostics --diagnostics-log wd91m-writeable-build-diag.jsonl \
             --diagnostics-interval-ms 250
```

16 cores, `--threads 14`, `--max-memory 4 GiB` (the default). **53m 46s wall**, 756% average
CPU, **8.47 GB peak RSS**, 12,795 diagnostic samples. Raw numbers and the per-phase table live
in `perf/PERF_CURRENT_STATUS.md`; this document is the *plan*, not the measurement.

Determinism baseline: that build is byte-identical to the v0.21.0 core in 8 of 9 emitted files;
only `range/node_Entity_wikidata_id.isam` differs, which is exactly D53 (smaller range-index
leaf blocks). **Content-hash `5e8e7307…` is the fixed point every change below must preserve**
unless it deliberately changes emitted bytes.

## Where the time and memory actually go

| phase | wall | % | cpu/wall | peak RSS | verdict |
|---|--:|--:|--:|--:|---|
| pass1 (parse + metadata) | 11.1m | 21% | **14.2×** | 1.95 G | scales; leave alone |
| dedup keys | 1.9m | 4% | 1.6× | 0.62 G | near-serial |
| resolve edge endpoints | 11.3m | 21% | **10.3×** | 2.42 G | scales; leave alone |
| cluster (locality reorder) | 8.7m | 16% | 2.9× | 1.60 G | **serial tail** |
| emit node stores | 2.8m | 5% | 1.4× | 1.27 G | near-serial |
| emit topology (CSR + edges) | 12.2m | 23% | 7.7× | **8.06 G** | **RSS blowout** |
| emit.graph_summaries | 4.3m | 8% | 1.3× | 3.83 G | **serial** |
| emit range indexes | 0.3m | 1% | 1.0× | 2.50 G | negligible |
| publish (hash + manifest) | 1.0m | 2% | 1.0× | 2.50 G | **fully serial** |

Two structural facts drive everything below:

1. **`--max-memory` is not a budget.** Peak RSS is 8.06 GB against a 4 GiB cap; 20.1% of all
   samples exceed the cap and *every one of them* is in `emit.topology`.
2. **~35% of wall clock (18.9 min) runs on roughly one core** of sixteen — `cluster`,
   `emit.graph_summaries`, `emit.node_stores`, `dedup`, `publish`. `pass1` and `resolve` already
   scale at 14.2× and 10.3×, so the parallel-extsort work landed; the headroom is entirely in
   the phases it never touched.

## B1 — `emit.topology` must respect `--max-memory` *(highest leverage)*

**Symptom.** Peak RSS 8.06 GB against a 4 GiB budget, reached at t=41 min in
`op = "emit forward CSR + edge_props per band"`. This is the phase that OOMs a memory-capped
box, and it is simultaneously the wall-clock leader (23%) and the only phase under real
pressure (PSI cpu 8.8 / io 11.5).

**Root cause — the budget is divided into independent fractions that sum past the cap.**
`opts.max_memory_bytes` is never held by a single arbiter; each consumer takes its own slice of
the *whole* number (`crates/slater-build/src/build_external.rs`):

| consumer | budget expression | ~value at 4 GiB / 14 threads |
|---|---|--:|
| `src_post_mx` posting sorter (:1601) | `max_memory / 16` | 256 MB |
| `tgt_post_mx` posting sorter (:1606) | `max_memory / 16` | 256 MB |
| `range_sorters` (one per range index) | `max_memory / 16` each | 256 MB × N |
| per-worker band sorter (:1613) | `max_memory / 16 / threads` (min 8 MB) | 14 × 19 MB ≈ 268 MB |
| band batching buffers (:1528) | `max_memory / 32 / (nbands × threads)` | bounded, small |

Live simultaneously, plus each `ExtSorter`'s run buffer, the compression scratch, and whatever
the allocator retains. Nothing enforces the sum. The same pattern recurs at :1022, :1064, :1302
and :1815.

**Change.**

1. Introduce a **`MemoryBudget` accountant**. It has to live in **`graph-format`**, beside
   `ExtSorter` (`crates/graph-format/src/extsort.rs:182`), because that is where the budget is
   spent — `slater-build` merely owns the instance and passes `&MemoryBudget` down. A counted
   semaphore over `max_memory_bytes` handing out RAII reservations
   (`budget.reserve(bytes) -> Reservation`). `ExtSorter::new` / `new_inline` (:204, :221) take a
   `&MemoryBudget` in place of the bare `budget_bytes: usize`, and size the run buffer from what
   they are actually granted rather than from a fraction of a number nobody is tracking.
2. Make the forward-band phase reserve **per worker**, so `threads` is throttled by memory
   rather than by core count when the budget is tight (a worker waits for a reservation instead
   of over-committing).
3. Add a `diag` counter `budget_reserved_bytes` alongside `rss_bytes`, so a diagnostics run
   shows reserved-vs-resident directly. Divergence between the two is then a bug you can see.

**Acceptance.** A full 91.6M build at `--max-memory 4g` keeps peak RSS ≤ ~1.25× the cap (some
allocator slack is honest), with **zero** samples above 2×. Content-hash unchanged
(`5e8e7307…`). Wall clock within 5% of the 53m 46s baseline.

**Risks.** Throttling workers on reservations can serialise the band phase if the budget is set
absurdly low; the reservation must be `min(request, budget/threads)`-shaped with a floor, and a
build that cannot make progress must fail loudly rather than deadlock. Add a starvation test at
a tiny `--max-memory`.

**Effort.** Medium — one new module, mechanical threading of `&MemoryBudget` through the six
call sites above.

## B2 — parallelise `emit.graph_summaries`

**Symptom.** 4.3 min (8% of wall) at median 100% CPU; 75% of its samples are below 1.5 cores.

**Root cause.** `compute_graph_summaries` (`build_external.rs:2491`) is a single sequential
sweep over all 91.6M nodes: it walks the topology, tallies `reltype_edge`, `reltype_self`,
`label_node`, `first_label` into flat `Vec`s and `src_marg` / `tgt_marg` into `HashMap`s, and
spills `(dst, src_label, reltype)` triples into one `ExtSorter` for the target-label join.

**Change.** The tally is a **map-reduce over disjoint node ranges** — every accumulator is a sum:

1. Shard `0..node_count` into `threads` contiguous ranges (contiguity keeps the windowed
   block-cache locality the comment at :2515 is careful about).
2. Each worker keeps private `Vec` counters + private `HashMap` marginals + its **own**
   `ExtSorter` for the triple spill (reserved through B1's budget).
3. Reduce: element-wise add the vectors, merge the maps, and **merge the per-worker sorted runs**
   into the existing merge-join for the target-label resolution — the sorters already produce
   sorted runs, so this is a k-way merge, not a re-sort.

Determinism: addition is commutative over `u64`, and the triple join consumes a *sorted* merge,
so the emitted summaries are order-independent. The determinism gate (`emit_determinism.rs`)
covers this.

**Acceptance.** ≥6× cpu/wall on this phase (4.3 min → ≲1 min). Byte-identical
`graph_summaries` output; content-hash unchanged.

**Effort.** Medium-low. This is the cleanest win per line changed.

## B3 — parallelise `publish` (hash + manifest)

**Symptom.** 1.0 min at **99% CPU for 100% of its samples** — the only phase that is serial for
its entire duration. It reads 23.4 GB at 411 MB/s.

**Root cause.** `common.rs:~152` loops over the emitted files and calls
`hash_file_blake3_sha256_crc32c(&path)` one file at a time. That helper
(`crates/graph-format/src/integrity.rs:43`) makes **one read pass** but then runs all three
digests *sequentially* over each chunk — `b3.update`, `sha.update`, `crc32c_append` — through a
**64 KiB** stack buffer. The file set is dominated by a single member:

| file | size |
|---|--:|
| `topology.csr.blk` | 16.78 GB |
| `edge_props.blk` | 3.67 GB |
| `node_props.blk` | 2.97 GB |
| `range/…isam` | 0.59 GB |
| `node_labels.blk` | 0.23 GB |

So **parallelising across files buys ≤1.4×** — `topology.csr.blk` alone is 71% of the bytes. The
win has to come from inside one file.

**Change — three parts, in order of value.**

1. **Don't compute checksums nobody asked for.** SHA-256 and CRC32C exist so a generation served
   from S3/GCS can be verified from object metadata (see the comment at `common.rs:~147`). A
   filesystem-backend build needs only BLAKE3. Gate them: compute SHA-256/CRC32C when the build
   publishes to (or is flagged for) an object store, otherwise skip. *This alone should remove
   most of the phase*, because SHA-256 is the slowest of the three.
2. **Parallel BLAKE3 within a file.** BLAKE3 is a Merkle tree: `Hasher::update_rayon` hashes one
   buffer across the pool. The workspace currently pulls `blake3 = "1"` with default features
   (`Cargo.toml:46`), so this needs the **`rayon` feature enabled** first. Also raise the 64 KiB
   read buffer to a few MiB — `update_rayon` has nothing to parallelise across at 64 KiB.
3. **Parallel across files** with `rayon` for the residue.

When 1 is not applicable (object-store publish), run the three digests **concurrently over a
shared chunk stream** rather than sequentially per chunk: wall becomes `max(blake3, sha256,
crc32c)` instead of their sum. SHA-256 has no tree structure and stays the floor; prefer
`sha2`'s `asm`/SHA-NI path.

**Acceptance.** `publish` ≤ 15s on the fs backend at 91.6M.

**Correction (measured).** This section claimed part 1 "changes `MANIFEST.json` for fs builds and
therefore the content-hash", needing a re-baseline of `5e8e7307…`. **That is wrong.** `content_hash`
is a digest over the inventory's `(name, blake3)` pairs only, and `MANIFEST.json` is not itself in the
inventory — so dropping the `sha256`/`crc32c` keys is invisible to it. A 1M build before and after the
change both hash to `6cbc6508…`. Part 1 is hash-preserving like parts 2 and 3, and no re-baseline was
performed. Shipped as **D56**, with an `--object-checksums` flag for a generation that will reach an
object store by other means (without it, that backend falls back to a size-only completeness check).

**Effort.** Low (parts 2+3), Low-medium (part 1, mostly plumbing a flag + a decision).

## B4 — chase `cluster`'s serial tail

**Symptom.** 8.7 min (16% of wall) at 2.9× cpu/wall. It reports 14 active workers and bursts to
1336% CPU at p90, yet spends **67% of its time below 1.5 cores**. So it is not un-parallelised —
it is parallel work punctuated by a long serial phase.

**Root cause — not yet established.** The op label never changes (`"block-parallel LDG cluster"`,
`build_external.rs:1263`), so the diagnostics cannot separate the parallel LDG passes from
whatever runs between them. Candidates: the per-pass permutation rebuild / prefix-sum, the
sequential read of the previous pass's assignment, or the `cluster_passes` loop barrier.

**Change.** *Measure before cutting.*

1. Split the op labels per sub-step so the next `--diagnostics` run attributes the serial 67% to a
   specific step. This is a small change and must land first.
2. Then parallelise whatever it names.

**Stage 1 reported (1M nodes, `--diagnostics-interval-ms 50`).** The serial tail is **not** the LDG
passes and **not** a barrier between them. It is `route adjacency into stripes`:

| cluster sub-op | wall | cpu%avg |
|---|--:|--:|
| build undirected adjacency (external sort) | 0.7s | 148% |
| **route adjacency into stripes** | **2.7s (69%)** | **116%** |
| ldg pass 0 / 1 / 2 | 0.1 / 0.2 / 0.2s | 616 / 421 / 511% |
| build final permutation | <0.05s | — |

One thread drains the adjacency sorter's k-way merge and zstd-writes 1,398 stripe files. The LDG passes
— the part this section guessed at — are already parallel and account for 13% of the phase.

**Stage 2, as shipped: the cause was not local to `cluster`.** The same one-thread-does-the-zstd shape
explains `dedup`'s drain (84% CPU), `emit.node_stores`' drain (99%) and `emit.topology`'s stitch (79%).
So the fix went into `BlockFileWriter` itself: block sealing (zstd + AEAD) moved onto a shared bounded
pool, drained in block order so the bytes cannot move (**D57**). The permutation is untouched — no
iteration order changed anywhere — and the 1M content hash is identical before and after.

Measured at 1M: total build **30.6s → 24.4s**; `cluster` route 2.7s@116% → 2.0s@164%;
`emit.node_stores` drain 0.9s@99% → 0.5s@243%; `emit.topology` forward band 5.6s → 3.8s.

**What is left in `cluster`.** The route's residual serial cost is the *read* side — the k-way merge
decompressing run blocks on the consuming thread. Removing it means inverting the phase: route unsorted
`(node, nbr)` pairs into per-stripe files in parallel, then sort each stripe in parallel, since
`ldg_stripe_pass` only ever needs its own stripe ordered by node. Not attempted; the tripwire (1M
content hash) makes it a safe follow-up.

**Risks.** `cluster` decides dense-id assignment; any change to iteration order changes the permutation,
changes every emitted file, and changes the content-hash. D57 changes no iteration order at all. The
`emit_determinism.rs` two-build byte-identity gate plus the 1M content hash are the tripwires.

## Also worth doing (small, uncontroversial)

- ~~**`dedup` (1.9 min, 1.6×) and `emit.node_stores` (2.8 min, 1.4×)** are the same shape as B2 — a
  sequential sweep that reduces. Fold them in once B2's map-reduce shape exists. Combined ~4.7 min.~~
  **Struck: measured false.** Per-op diagnostics at 1M put ~10% of each phase in the scan and ~90% in
  the *drain* (`dedup` 0.1s scan / 0.8s drain; `emit.node_stores` 0.1s / 0.9s). The drain emits in
  global order — deduped nodes by ascending prov id, node stores by ascending final id — so it is not a
  reducible sweep and a map-reduce over node ranges cannot touch it. What the drain was actually
  spending its time on was serial zstd in `BlockFileWriter`; that is fixed by D57 above, which is why
  `emit.node_stores`' drain went to 243% CPU without either phase being restructured.
- **IO amplification is 3.1× read / 2.5× write** (406 GB read, 337 GB written against a 133 GB
  input); `resolve` alone writes 158 GB. That is the price of the spill-based external sort and is
  *not* a bug — but it means the build is only ~2× off being IO-bound on this disk. Any CPU win
  above shortens wall clock only until IO becomes the floor; re-measure PSI io after B1–B4 before
  chasing more CPU.

## Ordering

```
B1 (memory budget)  ──┬─→ B2 (summaries map-reduce)
                      └─→ B3 parts 2+3 (hash-preserving)
B4 stage 1 (instrument)  ──→ B4 stage 2 (only if stage 1 justifies it)
B3 part 1 (skip S3/GCS checksums)  ── needs a D<N> decision
```

B1 first, because B2's per-worker `ExtSorter`s need a real budget to reserve against — doing B2
first would make the RSS overshoot worse, on the phase that already peaks at 8 GB.

*Executed in that order.* B1 → B2 → B3 (all three parts) → B4 stage 1 → B4 stage 2 (which stage 1
redirected from `cluster` into `BlockFileWriter`). `dedup + node_stores` was struck on measurement.

## Non-goals

- Re-architecting the external sort. `pass1` (14.2×) and `resolve` (10.3×) prove the current
  design scales; the problem is the phases that never adopted it.
- Reducing IO amplification. See above — measure first.
- Making `emit.topology` faster. It is 23% of wall at 7.7× parallelism, which is respectable. B1
  is about its *memory*, not its speed.

## Verification protocol (every item)

1. `cargo test --workspace` + `cargo clippy --workspace --all-targets -- -D warnings` green.
2. `emit_determinism.rs` two-build byte-identity still passes.
3. Full 91.6M rebuild with `--diagnostics`; compare the per-phase table against
   `perf/PERF_CURRENT_STATUS.md`.
4. **Content-hash is `5e8e7307…`** unless the item explicitly re-baselines it (only B3 part 1
   does), in which case record the new hash and the reason in `docs/DECISIONS.md`.
5. Update `perf/PERF_CURRENT_STATUS.md` with the new table.

## Progress ledger

Written 2026-07-09 from the `wd91m-writeable-build-diag.jsonl` run. Landed 2026-07-10 and verified by two
full 91.6M rebuilds (`wd91m-b1v2-diag.jsonl` on glibc, `wd91m-jem-diag.jsonl` on jemalloc):
**47.0 min wall (was 53.8), peak RSS 5.66 GB (was 8.47), content hash `5e8e7307…` unchanged.**
New per-phase table in `perf/PERF_CURRENT_STATUS.md`.

- **B1 — `MemoryBudget` accountant.** Done, and **partially met** (see acceptance below). Shipped as
  **D58**. New `graph-format/src/membudget.rs`: a counted semaphore over `--max-memory` handing out RAII
  `Reservation`s. `ExtSorter::new`/`new_inline` take a `Reservation` instead of a bare byte count and
  size the run buffer from what they were granted; the reservation passes to the `SortedIter` so it is
  held across the merge (which spends one decompressed block per run). The band workers and `resolve`'s
  partition workers draw from a `Reservation::into_sub_budget()` pool, so a tight cap throttles workers
  instead of over-committing, and `reserve` blocks only on a *peer* guaranteed to release.
  `Reservation::split_off` exists because `resolve`'s stage 2 holds two sorters at once and reserving
  twice inside a pool would deadlock. `budget_reserved_bytes` is emitted beside `rss_bytes`.
  New `tests/memory_budget.rs`: a 1 MiB cap fails loudly (not a hang), a 48 MiB cap throttles and emits
  byte-identical output to a 4 GiB one.

  **Two things the plan did not anticipate, both found by the full-scale run:**
  1. *The accountant was honest; its inputs were not.* `ExtSorter` budgeted with `SortRecord::size_hint()`
     — documented as the **encoded** size. `EndpointRef` reports 24 bytes and resides in 56. The old
     `/16/threads` fractions (~18 MB per sorter) hid the 3-4× under-count; granting the real budget
     multiplied it, and the first full 91.6M run put `resolve` at **15.11 GB against 4.29 GB reserved** —
     worse than the 8.06 GB it was meant to fix. Fixed with `SortRecord::resident_hint()` and
     capacity-aware accounting in `push`. `resolve` → 5.62 GB, `dedup` 4.89 → 1.53 GB. The bug was found
     by `budget_reserved_bytes`, exactly as this plan predicted it would be.
  2. *Inline vs pooled spill is a property of the data.* Switching band sorters to `new_inline` is right
     at 91.6M (88 bands / 14 workers) but wrong at 1M, where there is **one** band and inline left 13
     cores idle (`emit.topology` 6.5s → 9.95s). Now `ExtSorter::new_for_pool(…, nbands >= threads)`.

  **Acceptance: met.** Peak **reserved** = 4.29 GB = **1.00× the cap**; **zero** samples above 2× the cap
  (was 20.1% above the cap outright). Peak **RSS** 4.60 GB = **1.07× the cap** in every budgeted phase,
  once **D59** (jemalloc) removed the glibc arena retention that the accountant cannot see. Wall clock
  improved rather than merely staying within 5%. Content hash `5e8e7307…` unchanged.

  On glibc the peak had stayed at 1.93× cap despite reserved being exactly 1.00×, and the gap was never
  live memory — `stitch` held 6.25 GB resident against 0.81 GB reserved while only concatenating finished
  files. That was ~1.5B small `props_blob` allocations freed into per-thread arenas glibc never returns.
  See **D59**.
- **B2 — `emit.graph_summaries` map-reduce.** Done. Tally sharded over contiguous node ranges; triples
  routed by `dst`-range so the target-label join *also* parallelises (one sort-merge join per range)
  rather than staying a single serial pass after the tally, as the plan proposed. Needed a new
  `BlockFileReader::for_each_record_in(lo, hi, …)` to give each worker a contiguous record range.
  At 91.6M: **4.3m@1.3× → 1.36m@9.6×** — ≥6× met, byte-identical output.
- **B3 — publish hashing, all three parts.** Done (**D56**). SHA-256/CRC32C gated on an object-store
  publish or the new `--object-checksums`; `blake3` gains the `rayon` feature and `update_rayon`; the
  read buffer goes 64 KiB → 8 MiB; files are hashed with `par_iter`. **The content hash did not move**
  (see the correction under B3) — `5e8e7307…` stands, confirmed on a full 91.6M rebuild. At 91.6M:
  `publish` **1.0m@1.0× → 0.19m@2.2×**, comfortably inside the ≤15s target.
- **B4 stage 1 — instrument `cluster`.** Done, and it named the step: `route adjacency into stripes`,
  69% of the phase at 116% CPU. `dedup` and `emit.node_stores` were split into scan/drain ops too.
- **B4 stage 2 — parallel block sealing.** Done (**D57**), in `BlockFileWriter` rather than in
  `cluster`, because stage 1 showed the same serial-zstd shape in four phases. Byte-identical output.
  At 91.6M: `emit.node_stores` **2.8m@1.4× → 1.68m@3.4×**, `dedup` 1.9m → 1.61m, `emit.topology`
  7.7× → 9.4×.
- **`dedup` + `emit.node_stores` map-reduce.** Struck; the premise was measured false (see above).

### Follow-ups, named and measured, not attempted

1. **`cluster` / `route adjacency into stripes`** — 272.8s of the phase's 512s (54%) at **120% CPU**, and
   D57 did *not* help it: at scale the step is bounded by the k-way merge **decompressing** run blocks on
   the consuming thread, not by compression. Invert the phase: route unsorted `(node, nbr)` pairs into
   per-stripe files in parallel, then sort each stripe in parallel. `ldg_stripe_pass` only ever needs its
   own stripe ordered by node, so the stripe files come out byte-identical and the permutation cannot
   move. The 1M content hash (`6cbc6508…`) is the tripwire.
2. **`emit.topology` / `stitch`** — now 247.8s (35% of the phase) at 85% CPU, a verbatim block-concat of
   176 band files into 20.4 GB. IO-bound; it became the phase's largest serial step only because the band
   passes got 1.6× faster.
3. ~~**Allocator retention** — the last 2× of peak RSS.~~ **Done (D59).** `slater-build` took
   `tikv-jemallocator` as its `#[global_allocator]` on Linux. Peak RSS 8.13 → 5.66 GB, `emit.topology`
   8.29 → 4.60 G, `stitch` 6.25 → 2.53 G against the same 0.81 G reserved, and `emit.node_stores` got
   *faster* (1.68 → 0.98 min) because jemalloc also services the churn better. Wall 48.08 → 47.04 min,
   hash unchanged. Not `malloc_trim` — this crate forbids `unsafe`, and `slater` had already migrated off
   it for that reason.

   It exposed the real last peak: `emit.prop_hist`, a five-second phase reserving nothing, whose
   `derive_histogram_from_isam` materialised **every** distinct `(Value, count)` pair before checking the
   `max_distinct` cap and discarding them. On near-unique `node_Entity_wikidata_id` that was one 5.78 GB
   sample — the whole build's peak. Now `distinct_key_counts_bounded` abandons mid-scan (D59).

   Still open: jemalloc treats the *symptom*. The churn is ~1.5B small `props_blob` `Vec<u8>`
   allocations, one per edge. A per-band bump arena, or inlining short blobs into the record, would remove
   them and cut CPU as well as RSS.
4. **Unbudgeted resident consumers.** `emit.prop_hist` was one (now fixed). The k-way merge's
   `#runs × 256 KiB` of decompressed blocks is another, and `cluster`'s O(n) partition maps are reserved
   but the stripe readers are not. None currently drives the peak; `budget_reserved_bytes` vs `rss_bytes`
   in a `--diagnostics` run is how you find the next one.
5. **`pass1` writers** — `BlockFileWriter` hands every block to D57's seal pool, which is pure overhead
   for a phase already at 14.1× cpu/wall. An inline-seal constructor for writers driven from inside a
   saturated pool would remove it, mirroring `ExtSorter::new_for_pool`. (Measured within noise at 91.6M:
   11.1m → 10.77m, so this is speculative.)
