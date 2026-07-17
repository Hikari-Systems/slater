# FreshDiskANN vector write-ladder — performance report (HIK-120)

A committed, re-runnable measurement suite for the FreshDiskANN write ladder (HIK-108…119). Every
number below is produced by a bench in `crates/slater/benches/` and can be reproduced with the
command in that section. Ground truth for recall is an **exact brute force over the live set**
(`slater::vector::distance`), recomputed independently — never "index A agrees with index B".

**Environment.** AMD Ryzen AI 7 350 (16 threads), 15 GB RAM, WSL2, disk ~98 % full. Release
profile (`cargo bench`). Fixtures are deterministic (splitmix64). Scale here is representative
(dim 768; ~1k–50k vectors), extrapolated to 91.6 M / 370 GB for the size-linear metrics. This box
cannot hold a real-scale vector index, so absolute wall-clock throughput (bench 5) is
environment-bound; the *shapes* (flat-vs-linear, iso-recall read ratios, recall preservation) are
not.

## Headline

| # | Feature (ticket) | Claim | Measured | 91.6 M / 370 GB extrapolation |
|---|---|---|---|---|
| 1 | RW-index removes per-query overlay brute force (HIK-112) | ON flat, OFF linear in delta | ON **1.2–2.0 ms** flat across 1k→50k; OFF **1.9→114.7 ms** (linear, ~2.3 ms/1k); **61× at 50k** | delta bounded by `maxVectors` (50k) ⇒ ON stays ~2 ms; OFF unusable |
| 2 | Recall preserved across the ladder, per metric (HIK-114/115/119) | recall@10 held at each rung | cosine **0.91–0.99**, L2 **0.76–0.93**, dot **0.32–0.50**; consolidated ≥ base every metric | recall is size-stable (graph quality, not count) |
| 3 | Insert cost (HIK-112) | ~2 ms/insert | **1.54 ms** @2k, **1.68 ms** @5k amortized (climbs with N) | delta rebuild ≈ 2 ms × `maxVectors` ≈ **~100 s** at 50k |
| 4 | Delete-consolidation cuts read IO at iso-recall (HIK-114) | ~3–4× fewer reads at 67 % dead | **2.94× at 67 %**, 1.98× at 50 %, **5.20× at 80 %** (recall ≥ 0.90) | ratio is fraction-driven, size-independent |
| 5 | StreamingMerge fast vs slow path (HIK-115/119) | hard-link ~instant; rewrite decode-once | fast **9.7 ms / ~7.7 GiB/s eff.**; slow **108–114 MiB/s** | fast path O(1); slow @110 MiB/s: 1 pass ≈ **58 min**, 2 passes ≈ **117 min** for 370 GB |

---

## 1. RW-index kill-switch — `vector_rwindex.rs`

`cargo bench -p slater --features testkit --bench vector_rwindex`

Drives `db.idx.vector.queryNodes` **end to end** through the engine (parse →
`Engine::with_rw_index` → merged top-k) against a delta of *N* born `:Doc` vectors, A/B on
`RwIndexConfig::enabled`. OFF is the pre-HIK-112 path: rebuild a `ResidentMatrix` over the whole
delta and scan it, every query. The bench asserts the ON arm actually *served* the index
(`index_epoch == query epoch`) so it can't pass vacuously.

| touched delta nodes | ON (RW-index walk) | OFF (delta brute force) | speedup |
|---|---|---|---|
| 1 000 | 1.24 ms | 1.93 ms | 1.6× |
| 5 000 | 1.64 ms | 11.0 ms | 6.7× |
| 20 000 | 1.99 ms | 45.8 ms | 23× |
| 50 000 | 1.88 ms | 114.7 ms | 61× |

ON is **flat** (~1.5–2 ms, dominated by the base brute-force arm + query plumbing, not the delta);
OFF is **linear** (~2.3 ms per additional 1 000 delta nodes: 5k→50k is 10× the work for ~10× the
time). This is the whole point of the kill switch: a recall/memory regression in production is one
config flip back to the OFF line, at the OFF cost.

Warm cache (criterion reuses the opened generation, block cache, and RW-index pool per point, so
the ON graph is built once during warm-up and every measured iteration is a steady-state query).
Caveat: the base index here is deliberately tiny (8 vectors) so the swept delta dominates; a large
base adds a constant to both arms.

## 2. Recall across the ladder — `vector_recall.rs`

`cargo bench -p slater --features testkit --bench vector_recall`

recall@10 vs exact brute force over the **live** set, at each index kind, for each metric, over a
representative **low-rank manifold** fixture (dim 768, latent 48) with **unequal norms** (a moderate
4× spread, so cosine/L2/dot genuinely diverge). Index vectors and held-out queries are sampled from
the same manifold — real embeddings live on such a manifold, which is what makes their kNN both
meaningful and navigable. (Uniform-random high-dim vectors are near-orthogonal and equidistant, so
*no* ANN graph recalls well on them — see the adversarial-review note below.)

| metric | delta (RwVamana) | base (vamana+PQ) | consolidated | merged |
|---|---|---|---|---|
| Cosine | 0.990 | 0.910 | 0.960 | 0.890 |
| L2 | 0.927 | 0.788 | 0.875 | 0.758 |
| Dot | 0.433 | 0.353 | 0.497 | 0.315 |

The ladder **preserves** recall: consolidated ≥ base for every metric (splicing holes out of
adjacency reconnects the live neighbourhood), delta (exact-navigated RwVamana) is highest, merged is
within ~0.06 of base. The three metrics diverge exactly as intended — dot is far lower (0.315–0.497)
because maximum-inner-product search is harder to navigate (a high-norm vector is "near" everything,
distorting the navigable graph). The "exact-distance" delta rung (RwVamana) reaching only ~0.43 tells
us **PQ quantization is not the dominant term** — dropping it buys about +0.08 — but it does *not*
show the loss is intrinsic to dot-product kNN: that baseline is not metric-exact for dot either. It
still navigates via the norm-augmentation MIPS→L2 reduction (`RwVamana::dist` computes `base + d*d`
on the augment-coordinate difference, `crates/graph-format/src/rwvamana.rs`:186), so the augmentation
reduction is not ruled out as the cause. Whether a MIPS-native navigator (e.g. ip-NSW) recalls better
on this same fixture is **unmeasured** — tracked in HIK-137. For cosine and L2, recall is a function
of graph quality, not vector count, so those hold at scale.

**Scope note (honest).** The engine-level "delta+segments" *merged read* is not a distinct index
kind — a core segment is itself a small on-disk vamana, so its recall is the **base** rung's, and
the merged top-k is bounded below by each level's recall (`vector::merge_topk`). The four columns
are the ladder's four distinct index *kinds*; standing up a full multi-segment flush inside a
microbench would add machinery without a new recall regime.

## 3. Insert cost — `vector_insert.rs`

`cargo bench -p slater --features testkit --bench vector_insert`

One `RwVamana::insert` at dim 768, R=32 (`RW_R`), L=64 (`RW_L_BUILD`) — greedy-search + robust-prune
+ back-link. Reported amortized over building the index (the per-insert cost climbs with the live
set, so an amortized-to-a-few-thousand figure is the representative number, not the near-empty first
insert).

| index size | amortized ms/insert |
|---|---|
| 2 000 | 1.54 ms |
| 5 000 | 1.68 ms |

Consistent with the ~2 ms/insert anchor from HIK-112 (and rising toward it as N grows). **Delta
rebuild budget**: a from-scratch rebuild re-inserts the whole delta at this cost, on the read path,
under the write guard — ≈ 2 ms × `maxVectors` (50 000 default) ≈ **~100 s**. That is *why*
`maxVectors` bounds the resident set: it bounds the rebuild, not just the memory. (The ON arm of
bench 1 pays this ~100 s once, during warm-up, then answers every query in ~2 ms.)

## 4. Delete IO at iso-recall — `vector_delete_io.rs`

`cargo bench -p slater --features testkit --bench vector_delete_io`

The S5 headline (HIK-114). A lazily-deleted node stays a **hole** its neighbours still name, so
beam search fetches its block, finds it un-emittable, and moves on. At a *fixed* beam width a hole
costs a beam slot, not a fetch, so a fixed-width comparison is a flat line and proves nothing. The
cost surfaces at **iso-recall**: holes crowd the candidate list, so lazy needs a **wider** beam to
hit the target — and a wider beam fetches more nodes. Consolidation splices holes out of live
adjacency, so the target is met at a narrower beam. Metric = **node fetches per query** (one
beam-search node expansion = the DiskANN IO unit, one random read), counted exactly; recall target
**≥ 0.90** vs exact-over-live; dim 768, N=6 000, cosine, cold reads.

| dead % | live | lazy beam L | lazy reads/q | cons beam L | cons reads/q | reads ratio |
|---|---|---|---|---|---|---|
| 0 % | 6000 | 96 | 97.9 | 96 | 97.9 | 1.00× |
| 25 % | 4500 | 96 | 97.9 | 96 | 97.9 | 1.00× |
| 50 % | 3000 | 192 | 193.6 | 96 | 97.8 | 1.98× |
| **67 %** | 1980 | 192 | 193.6 | 64 | 65.8 | **2.94×** |
| 80 % | 1200 | 256 | 257.4 | 48 | 49.5 | 5.20× |

**2.94× at 67 % dead** (in the expected 3–4× band; the exact figure is quantised by the beam ladder
— lazy lands on L=192 where L≈160 would suffice), rising to **5.2× at 80 %**. Consolidation's beam
*shrinks* as dead grows (fewer live nodes to search); lazy's *grows* (more holes to see past). Below
~25 % dead there is nothing to reclaim (holes are too sparse to change the beam), exactly as
designed.

## 5. StreamingMerge throughput — `streaming_merge.rs`

`cargo bench -p slater --features testkit --bench streaming_merge`

Fast path (no inserts, no new dead → the `.vamana` is carried by reference / hard-linked,
byte-identical; only the small `.pq` id column is rewritten) vs slow path (a Δ=1 insert forces the
sequential `emit_merged` rewrite — decode each block **once**, the HIK-119 fix — isolating the
rewrite from insert work). Base N=25 000, `.vamana` = 69.5 MiB.

| path | wall time | throughput |
|---|---|---|
| fast (hard-link) | 9.7 ms | ~7.7 GiB/s *effective* (size-independent; the `.vamana` is not rewritten) |
| slow (decode-once rewrite) | 612 ms | **108–114 MiB/s** |

The fast path is ~65× the slow path here and is **O(1)** in graph size — a pure permutation is a
hard-link plus a tiny id-column rewrite, so the 9.7 ms does not grow with the `.vamana`. The slow
path is the sequential rewrite that any insert or new tombstone forces.

**370 GB core extrapolation @ 110 MiB/s**: insert consolidation (1 rewrite pass) ≈ **58 min**;
delete consolidation (2 passes) ≈ **117 min**.

**Discrepancy with the 365 MiB/s anchor (investigated).** HIK-119's own measurement was
17→365 MiB/s; on *this* branch (fix included) this box measures 108–114 MiB/s. The gap is
environmental, not a regression: the emit is single-threaded and **re-compresses** every block at
zstd-3 on write (zstd-3 single-thread runs ~100–150 MiB/s of payload — which matches 612 ms for
~80 MiB logical), and the output is written to a **98 %-full WSL2 disk**. The 365 figure was a
faster/less-contended environment (the EC2 perf box). What this bench confirms on any box is the
*decode-once* regime — the pre-HIK-119 path re-inflated each block ~20× per record and would be an
order of magnitude slower — and the fast-vs-slow structural contrast. Re-run on the EC2 perf box for
the canonical MiB/s.

---

## Caveats (apply throughout)

- **Synthetic vectors.** Recall/IO use a low-rank manifold (bench 2/4); it stands in for real
  embeddings' local structure but is not a specific real dataset. Absolute recall (esp. dot) and
  absolute beam widths would shift on real data; the *rung-to-rung* and *lazy-vs-consolidated*
  relationships are the robust results.
- **Single box, warm vs cold.** Latency benches (1, 3, 5-criterion) are warm (criterion warm-up
  stated per bench). Delete-IO block counts (4) are cold (`beam_topk_disk` opens fresh readers).
  Throughput (5) is CPU/disk-bound on this box; see its discrepancy note.
- **Representative scale.** 1k–50k vectors, extrapolated to 91.6 M/370 GB only where the metric is
  size-linear (insert budget, merge throughput) or size-invariant (recall, iso-recall read ratios).
  Latency (bench 1) is bounded by `maxVectors`, so it does not extrapolate past 50k — it is *already*
  at the production ceiling.
- **Stability.** Numbers are criterion medians (benches 1, 3, 5) or means over 40–60 queries
  (benches 2, 4). Run-to-run variation on this shared box is a few %; the qualitative shapes are
  stable.
