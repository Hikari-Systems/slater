<!-- SPDX-License-Identifier: Apache-2.0 -->
# Segmented core — read amplification

> Phase 8 harness + measured characteristics. The bench that produces this is
> `crates/slater/benches/segment_read_amp.rs`; the fixture/reader support is the
> testkit-gated `slater::benchkit` module.

## Why measure read-amp

The additive core's whole premise is that a routine flush is **O(delta)** — it writes one
small upper segment instead of rewriting the base — *without* inflating read cost. That only
holds if a read touching an **untouched** id skips the whole upper-segment stack cheaply. The
design mechanism is a per-segment **presence fence** (id-band + value fence): an id/key outside
a segment's band never touches its blocks. This harness measures whether that holds as the
stack deepens.

**Metric — read amplification = cold-cache block misses.** With an empty write-delta, a read
folds only the immutable core stack, so the number of distinct blocks it decompresses (base
cache misses + segment-stack cache misses, both read from a cold open) *is* its read
amplification. This is deterministic and far more meaningful than wall-time for the presence
claim. `slater::segstack::CoreStack::cache_metrics()` exposes the segment-side counter;
`slater::cache::BlockCache::metrics()` the base side.

## Harness

`benchkit::build_stacked(tag, n, segments)` builds a scaled `scale` graph (`n` `:Person`
nodes, `name = "p{i:07}"` business key with a range index, `age = i % 100`, a `:KNOWS` ring)
and folds exactly `segments` upper segments over it. Each segment is the O(delta) product of
patching one **small, contiguous, disjoint** band of ~16 node names near the **top** of the id
space — so the vast base stays untouched and each segment's name-fence is narrow. A read
anchored **below** the patched bands therefore exercises the fence skip at increasing depth.

Four read shapes, each at 0 / 2 / 4 / 8 segments:

- **point_lookup** — index seek + node row for an untouched name;
- **two_hop** — a 2-hop ring traversal from an untouched anchor;
- **label_scan** — the first 1 000 rows of a `:Person` scan;
- **count** — `count(:Person)`, served from resident marginals.

Run:

```
CARGO_TARGET_DIR=/home/rickk/.cache/slater-target \
  cargo bench -p slater --features testkit --bench segment_read_amp
```

## Measured read amplification (N = 50 000, cold-cache block misses `base+seg=total`)

| shape          | seg=0 | seg=2  | seg=4  | seg=8   |
|----------------|-------|--------|--------|---------|
| point_lookup   | 1+0=1 | 1+0=1  | 1+0=1  | 1+0=1   |
| two_hop        | 2+0=2 | 2+0=2  | 2+0=2  | 2+0=2   |
| label_scan     | 4+0=4 | 4+2=6  | 4+4=8  | 4+8=12  |
| count          | 0+0=0 | 0+0=0  | 0+0=0  | 0+0=0   |

Warm latency (steady-state, caches primed) tracks the same story: `point_lookup` is ~31.5 µs
flat across all four depths.

### Reading the result

- **point_lookup / two_hop stay perfectly flat.** An untouched id's index seek, node row and
  ring adjacency pull the *same* base blocks at depth 8 as at depth 0, and **zero** segment
  blocks — the presence fence skips every upper segment at no I/O. This is the headline: an
  untouched read pays nothing for stack depth, so a flush that adds a segment does not tax the
  reads that don't touch it.
- **count is zero-read at every depth.** The patches net Δ = 0 to the label/node marginals, so
  `count(:Person)` is answered from the resident manifest sum with no block reads regardless of
  stack depth (the "empty ⇒ decline, never wrong" marginal discipline serves the exact case).
- **label_scan fans out — linearly, but bounded.** A full label scan grows by exactly **one
  segment block per segment** (`4 + d`). This is expected in kind — a scan must consult each
  segment for any overridden rows in its range — but here the scanned prefix (ids 0..1000) is
  entirely untouched and every segment's node band is at the *top* of the id space, so a
  band/range-overlap gate could prune those reads to zero. See the follow-up below. The cost is
  bounded by `maxUpperSegments` (~8) by construction, so it never runs away, but tightening it
  is the obvious next optimization.

## Follow-up (bounded, correctness-neutral)

**Range-gate the label-scan stack fold.** The label scan reads one block per upper segment even
when the scanned id range lies entirely outside every segment's node band. The extents routing
table already knows each segment's band `[base, base+k)`; gating the per-segment scan
contribution on a band/scan-range overlap check would drop `label_scan` back to its base-only
read-amp (`4+0`) for a scan disjoint from the written bands. Not done in slice 8.1 (it is
bounded by `maxUpperSegments` and touches the read-path scan seam); tracked here as the one
read-amp inefficiency the harness surfaced.
