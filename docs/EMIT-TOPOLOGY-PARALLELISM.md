# Parallelising the single-threaded build phases (emit.topology et al.)

Diagnostics (`--diagnostics`, see [build-diagnostics-mode]) on the 1M-node
wikidata set showed `emit.topology` (~31% of build wall) and `cluster` (~19%) run
at ~1 core while `pass1` (parallel) uses ~5–6. This doc records the parallelisation
options considered so we can revisit B/C after shipping A.

## Why it's safe
The external-sort keys are **total orders with no ties**:
- `EdgeFwd::cmp_key` = `(final_src, final_dst, prov_edge_id)`; `prov_edge_id` is
  unique per edge (`build_external.rs`).
- `EdgeRev::cmp_key` = `(final_dst, final_edge_id, …)`; `final_edge_id` unique.

So the merged order is fully determined by the key regardless of how/where runs are
formed → identical final edge-ids → **identical content hash**. The golden roundtrip
+ histogram-parity tests are the regression gate ("faster" and "bit-identical" are
not in tension here).

## What is actually serial in emit.topology
The single emit thread (`build_external.rs` ~1293–1393) runs **five concurrent
external sorts + output-block compression on one core**: forms `fwd_sorter` runs,
then in the merge loop per edge does heap-pop → `csr.push` (encode + zstd output
blocks) → `edge_props_w.append_raw` (zstd) → push into `src_post_sorter`,
`tgt_post_sorter`, and `rev_sorter` (each doing its own sort+zstd run-formation).

## Option A — async spillers inside `ExtSorter` (SHIPPED — graph-format/src/extsort.rs)

**Implemented.** A process-wide bounded spill pool (`SLATER_EXTSORT_SPILL_THREADS`,
default = online cores capped at 16; `1` = inline/original) executes sort+compress+
write off the push thread. Each sorter splits its byte budget across
`max_inflight + 1` smaller buffers (peak RAM ≈ budget preserved) with a per-sorter
backpressure semaphore, and completed runs are re-sorted into dispatch order so the
merge is bit-identical regardless of completion order (safe even for non-total keys
like `cluster`'s `AdjPair`).

**Measured (1M wikidata, --max-memory 1g, 16-core WSL2):**

| phase | baseline | Option A | speedup |
|---|---|---|---|
| emit.topology | 13.9s @0.9c | **7.1s @1.7c** (peak 2.3c) | **2.0×** |
| cluster | 8.6s @1.0c | 5.2s @1.4c | 1.6× |
| emit.node_stores | 0.9s | 0.6s | 1.6× |
| **total build** | **44.4s** | **31.1s** | **1.4×** |

Content-hash unchanged (`420536081afe…`), stable across repeated runs; all golden /
parity tests pass. **Overall peak RSS dropped 650→367 MB** (budget split shrank the
cluster footprint) — no memory regression. `cluster`/`node_stores` improved for free
(same primitive).

**Why only ~2× on emit.topology, not ~8×:** Option A parallelises *run formation*
only. The k-way merge plus the serial CSR encode + output-block zstd + edge_props
write remain single-threaded and now dominate the phase — exactly what Options B
(parallel merge + parallel CSR via range partition) and C (off-thread output
compression) target. Next lever is C, then B.

### Original design notes
`ExtSorter::spill_run` sorts+compresses **inline** on the push thread. Make it hand
each full buffer to a bounded worker pool (sort + zstd + write a run file) while the
push thread keeps filling the next buffer; `sorted()` (k-way merge) unchanged.
- One localized change in `graph-format/src/extsort.rs` parallelises the sort+zstd
  cost of **all five** sorters at once; no caller changes; determinism trivial.
- **Memory faithfulness:** keep the bounded-memory guarantee by splitting the
  sorter's budget across (in-flight + filling) buffers — peak RAM unchanged, just
  more, smaller runs (bigger but still single-pass merge heap).
- This is the "N core-local sorted run files then merge" idea at the primitive level.

## Option B — range-partitioned sort + CSR + concat ("cursor assembly")  [SHIPPED — build_external.rs]

**Implemented and validated at full scale.** `emit.topology` now range-partitions the
resolved edges into fixed `BAND_NODES` (=2²⁰) node bands and works each band in
parallel under the `--threads` cap, then stitches the per-band block files with
`concat_block_files`. Four sub-phases: a parallel **partition** (route edges by
`final_src` band, count per band → prefix-sum `base_b`), a parallel **forward** pass
(per band: sort → forward CSR half + `edge_props` slice + global postings/edge-range
sinks + route reverse records by `final_dst` band), a parallel **reverse** pass (per
dst-band: sort routed records → reverse CSR half), and a serial **stitch** (concat the
CSR halves forward-then-reverse + `edge_props` in band order, drain the postings).

`final_edge_id = base_b + i` (band prefix-sum + sorted position) is **bit-identical to
the serial forward-merge position** because bands partition by the primary sort key
`final_src`; only the *block layout* differs (boundaries fall at band edges), so the
content hash changed once (the documented re-baseline) while the logical content,
postings, and range ISAMs are identical. Determinism is locked by
`tests/emit_determinism.rs` (two fresh builds → byte-identical store files, with
`SLATER_EMIT_BAND_NODES=1` forcing many bands + cross-band reverse routing, for both
`ldg` and `none`, including an edge range index).

**Measured (full 91.6M-node / 1.533B-edge wikidata, `--max-memory 4g`, 16-core WSL2,
snapshot-resume of the emit phase):**

| metric | serial baseline | Option B | change |
|---|---|---|---|
| emit.topology wall | 1334 s | **646.7 s** | **2.06×** |
| emit.topology avg cores | 1.4 | **5.75** | 4.1× |

Sub-phase split: partition ~52 s, forward ~239 s (peak ~11.7 cores), reverse ~119 s,
serial stitch/postings-drain ~236 s. Content-hash `03384068…` (was `b7dca485…`), stable
across a repeat run. The **serial postings drain** (`write_endpoint_postings_from_sorted`
over the two global 1.5 B-record sinks) is now the dominant remaining serial tail (~36%
of the phase) — the next lever is to band the postings too (node-disjoint per-band
distinct-node lists concatenated per reltype, no global merge), plus reduce forward-band
skew. The plan/design notes that follow are retained for that follow-up.

### Original design notes
Partition the **node-id space** into P contiguous, edge-count-balanced ranges. Route
each edge by `final_src` into partition p; each partition independently sorts AND
writes its own forward sub-CSR for nodes `[a_p, b_p)`. Because ranges are disjoint
and contiguous, the full forward CSR is `part0 ‖ part1 ‖ …` — a cheap **block-file
concat that rewrites the offset index** (the cursor-marker assembly). No cross-
partition k-way merge.
- Forward partitioned by `final_src`, **reverse by `final_dst`** — two independent
  partitioned passes that can run concurrently ("one for forward, one for reverse").
- Biggest win: parallelises run-formation **and** merge **and** CSR encode+compress
  — ~P cores nearly end-to-end; serial part is only the concat.
- Complexity / open items:
  - CSR writer that emits a node-range sub-file + a block-file concat that rewrites
    record offsets (the real engineering).
  - `final_edge_id` is the forward-merge position today → give each partition a
    contiguous edge-id range (running base = Σ earlier-partition edge counts) so
    numbering stays deterministic; each partition writes its own `edge_props` run,
    concat in partition order.
  - Postings (`reltype_src/tgt.post`) are keyed by `(reltype, node)` — a different
    partitioning; keep feeding global postings sorters (cheap drain) or partition by
    reltype separately.
  - Skew: choose partition boundaries by **edge count** (degree prefix-sum), not node
    count, so partitions are balanced against mega-degree nodes.
- Bonus: the same node-range partition+concat machinery applies to
  `emit.node_stores` and conceptually to the LDG `cluster` phase (the other
  single-threaded hot spots).

### Option B — concrete implementation plan (IN PROGRESS)

**Keystones done + tested** (graph-format):
- `blockfile::concat_block_files(out, &[parts])` — copies each part's block region
  verbatim (no re-compress/re-encrypt) and rebuilds the directory offsets; O(total
  bytes); plaintext + encrypted. Test: `concat_preserves_records_in_order`.
- `topology::CsrHalfWriter` — writes one CSR half for a node band `[lo, hi)`
  (`hi-lo` records, empty for gaps). Test `banded_half_writers_concat_matches_csr`
  proves **banded forward+reverse halves + concat == a streamed CSR, logically
  identical** (all `outgoing`/`incoming` match) — the core correctness proof.

**Remaining**: the orchestration in `build_external.rs` (partition resolved edges by
`final_src` → parallel forward bands write partial CSR/edge_props/postings + route
reverse records by `final_dst` → parallel reverse bands → `concat_block_files` stitch
+ postings merge), wired under `--threads` with diag.

Data flow (P bands over the final node-id space, `BAND_NODES` fixed → thread-count-
independent, like cluster's stripes):
1. **Partition** the resolved edge bucket by `final_src / BAND_NODES` into P spill
   files, counting edges per band → prefix-sum `base_b` (deterministic edge-id ranges).
2. **Forward (parallel over bands)** — each band b: sort its edges by
   `(final_src, final_dst, prov_edge_id)`; assign `final_edge_id = base_b + i`; write
   a **partial forward CSR** (records for nodes `[lo_b, hi_b)`) via a plain
   `BlockFileWriter` + `encode_adj` (replicating `CsrStreamWriter`'s empty-record-for-
   gaps logic over the band range); write **partial `edge_props`** (ids
   `[base_b, base_b+count_b)`); accumulate partial postings; and route each edge's
   reverse record `(final_dst, final_edge_id, final_src, reltype)` to a **dst-band**
   spill file (by `final_dst / BAND_NODES`).
3. **Reverse (parallel over dst-bands)** — each band b: sort by
   `(final_dst, final_edge_id)`; write a **partial reverse CSR** for nodes `[lo_b,hi_b)`.
4. **Stitch (serial, cheap)** — `concat_block_files(topology.csr.blk, [fwd_0..fwd_{P-1},
   rev_0..rev_{P-1}])` (forward half then reverse half = the 2N-record CSR);
   `concat_block_files(edge_props.blk, [eprops_0..])`; merge partial postings by reltype.

Parallelises the merge, CSR encode, zstd, and edge_props write — the whole serial
tail — to ~P cores.

**Hash note (correction):** the cheap byte-concat means block *boundaries* fall at band
edges instead of wherever the serial stream packed them, so `topology.csr.blk` /
`edge_props.blk` bytes differ → **content-hash changes once** (logical content
identical; the golden roundtrip/parity tests are layout-agnostic and pass). This is
the same category of one-time re-baseline as cluster — NOT bit-identical. (Staying
byte-identical would require a serial re-pack, which defeats the parallelism.)

## Option C — pipeline overlap (cheap, complements A/B)  [REVISIT]
- Move output-block zstd off the merge thread: a dedicated compressor/writer thread
  (or a block-file writer compressing blocks on a small pool); the merge hands raw
  blocks across a channel. Output compression (CSR 464 MB + edge_props) is a real
  serial chunk.
- Split forward-merge and reverse-run-formation onto two threads (they're already
  interleaved on one) for a small, safe overlap.

## Recommended staging
1. **A** (async spillers) — SHIPPED. Best effort/reward, near-zero risk, no format changes.
2. **B** (range-partitioned parallel CSR + concat) — SHIPPED. 2.06× on the full build
   (emit.topology 1334 s → 647 s @ 5.75 cores). Its concat machinery is reusable for
   node_stores/cluster.
3. **Next**: band the serial postings drain (now the dominant tail), then **C**
   (off-thread output compression) for whatever serial work remains.

## How to validate
Re-run the `--diagnostics` build on the 1M set; watch `emit.topology` **avg-cores**
(target ≫1) and the printed **content-hash** (must stay
`420536081afe19e18c5782ded29af884214e14549b018b38f5765dfdbefc6493`). Gate on the
golden roundtrip + histogram-parity tests.
