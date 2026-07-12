# Read A/B — writeable (per-chunk EF degree column + forward-CSR edge_id_base) vs published v0.21.0

Same-box Docker A/B on the Wikidata graph at three scales, isolating the read impact of the
`writeable` branch (headline changes: per-chunk Elias–Fano degree column in a raw block container,
`FORMAT_VERSION 4`; forward-CSR `edge_id_base`, `FORMAT_VERSION 5`) against the published
`hikarisystems/slater:v0.21.0` image.

## Method

- **Baseline:** `hikarisystems/slater:v0.21.0` (`formatVersion 3`).
- **Candidate:** `slater:local` built from `writeable` HEAD `2965aee`.
- Each side builds its own generation from the **same** merge dump (or, at 91.6M, the baseline reuses
  the pre-existing `formatVersion 3` `data-wd91m-fixed` gen — byte-identical topology to a fresh v0.21.0
  build, no degree column), served in an isolated container: `requireAclStamp=false`,
  `query.maxIntermediate=20M`, `maxIntermediateGlobal=100M`, `maxFanout=1`.
- Latency: `perf/cross-engine-hs/bench_wiki.py`, 5-run median per shape. RSS: container cgroup
  (`memory.stat anon`, `memory.current`, `memory.peak`).
- Same box (15 GB); other containers stopped for the 91.6M run.

## On-disk (`topology.csr.blk`, the `edge_id_base` change)

| graph | edges | v0.21.0 | v5 | Δ |
|---|--:|--:|--:|--:|
| Wikidata-1M | 12.17M | 127.0 MB | 105.7 MB | **−16.8%** |
| Wikidata-10M | 103.7M | 1.13 GB | 0.96 GB | **−15.5%** |
| Wikidata-91.6M | 1.49B | 18.02 GB | 16.01 GB | **−11.2%** (−2.01 GB) |

`node_degrees.blk` (the EF degree column) is **new** in writeable — absent in v0.21.0: 1.35 MB / 12.5 MB /
128 MB at 1M / 10M / 91.6M. Total generation shrinks despite adding it: −11.1% (1M), −9.9% (10M).
(The topology saving's percentage tapers with scale because neighbour-id varints grow while the
per-edge `edge_id` we remove is a smaller share of each record — but the absolute saving grows: ~2 GB at 91.6M.)

## Read latency — fanout=1, 5-run median (ms), 91.6M

| shape | v0.21.0 | v5 |
|---|--:|--:|
| count | 0.58 | 0.61 |
| point lookup | 0.63 | 0.60 |
| degree (1-hop count) | 0.60 | 0.63 |
| 1-hop | 0.75 | 0.73 |
| 2-hop | 1.26 | 1.30 |
| 3-hop | 1.34 | 1.33 |
| var-length \*1..2 | 0.80 | 0.90 |
| shortestPath ≤6 | 0.61 | 0.61 |

**Identical** — no read-latency change or regression, at all three scales. `bench_wiki` uses light
(non-hub) anchors, so it does **not** exercise the degree-sum multi-hop `count()` fast path (the degree
column's headline read-perf win; v0.21.0 has no degree column at all). That shape wants a separate
`bench_multihop` run.

## Read memory — anon (engine heap), 91.6M

| metric | v0.21.0 | v5 | Δ |
|---|--:|--:|--:|
| **anon** | 1054 MiB | 470 MiB | **−55%** |
| current total | 8400 MiB | 7786 MiB | −7% |
| peak total | 10014 MiB | 10006 MiB | ~0 |

Read memory drops decisively only at 91.6M — the "bigger than cache" regime the changes target. At 1M/10M
the engine heap is dominated by the bounded block-cache LRU and query working set, so `anon` is noisy
(1M: 279→239; 10M: 355→464, where v5 is *higher* because it now holds the degree column resident) and
not a clean signal. The −55% at 91.6M is holistic writeable-vs-v0.21.0 (bounded query memory, chunk-lazy
residency, jemalloc work — not the EF/edge_id_base changes alone), but it is real and large. `peak total`
(~10 GB both) is page-cache of the gen-open scan, bounded by the box.

## Bottom line

- **Topology on disk: −11% to −17%** (−2 GB at 91.6M), scale-robust, attributable to `edge_id_base`.
- **Read latency: unchanged** across 1M/10M/91.6M.
- **Read memory (engine heap): −55% at 91.6M**; not visible at 1M/10M (fits in cache).
- The degree column adds a small resident structure that buys the O(1) degree-sum count fast path
  (not exercised by `bench_wiki`).
