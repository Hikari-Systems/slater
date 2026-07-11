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
  entirely untouched. The **membership fold** cost of this — decoding a node block per segment
  to re-check labels — is now zeroed by the label-scan gate (below) for the common case; the
  residual `+1/segment` in the matrix is the query *materialising* `p.name`, whose value lives
  in the patched segment rows (see the gate section). Either way the cost is bounded by
  `maxUpperSegments` (~8) by construction, so it never runs away.

## Backend invariance (fs vs object store)

Read amplification is the **number of blocks** a read pulls, which is a property of the format
and the read path, not the backend — a base or segment block miss fetches through whatever
`ObjectStore` serves the graph (local fs, S3, GCS, in-memory). So the matrix above is
**identical** served through an object store; `benchkit::build_stacked_store` mirrors an
fs-built stacked set into an in-memory store and `read_amp_cold_store` reads it back, and the
`read_amp_parity_fs_vs_object_store` unit test pins the two miss counts equal for every shape.
The bench prints both matrices side by side.

What changes on a **real S3** backend is only the per-block *latency* (a cold miss is a network
GET, ~tens of ms, versus a local page fault). That is an EC2, in-region measurement — never the
laptop, never MinIO (see the `s3-benchmark-methodology` note) — and it multiplies these same
block counts by the per-GET cost. The presence-fence result therefore matters *most* on S3: an
untouched point lookup that stays at 1 block is one GET regardless of stack depth, whereas a
naïve per-segment fan-out would be one GET per segment.

The read path is generic over `ObjectStore`, so **no per-backend code exists** — S3 and GCS
serve the fold through the identical `Reader::open_store` / `read_amp_cold_store`, only the
store constructor differs. The `tests/object_store_readamp.rs` integration test proves this
against real network backends (MinIO for S3, `fake-gcs-server` for GCS — both correctness-only):
it builds a depth-4 stacked set on fs, mirrors it into the store, and asserts the base+segment
block-miss counts match fs shape-for-shape. Both arms are verified — e.g. `label_scan` reads
`2+4` blocks over fs, MinIO, and the GCS emulator alike; the segment fold is genuinely served
over the network, not short-circuited. The arms skip unless `SLATER_S3_TEST_ENDPOINT` /
`SLATER_GCS_TEST_ENDPOINT` are set, so an ordinary `cargo test` is unaffected.

## The label-scan membership gate

The harness surfaced that a whole-graph label scan's *membership fold* (`CoreStack::
fold_label_scan`) decoded a node block per segment — `resolve_node_row` for every id the stack
touches — to re-check each row's labels. A naïve id-band/range gate is **unsound**: a segment
can preserve a label's *count* (its band fence, its `label_node_deltas`) while still changing
which ids hold it — a label *swap* (drop `:X` from A, add `:X` to B) leaves both invariant yet
moves membership. So the gate is driven by a writer-computed fact, not a value fence.

Each segment manifest carries `label_membership_touch: Option<Vec<String>>` — the sorted set of
labels whose node **membership** the segment changes (a node gains or loses the label, is born
carrying it, or is tombstoned while carrying it), computed exactly by the flush writer as it
materialises each row (and unioned across a run by the T3 merge). `None` means *unknown* (a
manifest predating the field, or a decline) — conservatively "may touch anything", so
correctness never depends on the gate. `fold_label_scan(label)` then **skips the entire stack**
when no segment's touch set lists `label`: every touched id keeps its base membership, so the
base scan is already exact — **zero** segment block reads for the fold, versus one per segment
before. For the common **property-only patch** workload (a `SET n.prop`, which never changes
labels) every touch set is empty, so all label scans skip the stack.

**What the gate does and does not cover.** It zeroes the *membership fold's* block reads. A
query that also *materialises* a segment-resident property of the scanned nodes (e.g. `RETURN
p.name` where `name` was patched into a segment) still reads those rows for output — that is a
property read, not the fold, and is unavoidable while the value lives in the segment. So the
gate's block-read win is realised for a scan of an **untouched label** (segments that changed
only other labels), a fold whose consumer is a count/aggregate, or any scan that does not emit
the patched rows; it also always saves the fold's per-id CPU. The `label_scan` row in the
matrix above (`RETURN p.name`) is materialisation-bound, so its end-to-end number is unchanged
by the gate — `benchkit::label_scan_membership_gate_reads_no_segment_blocks` measures the fold
in isolation (0 segment blocks at depth 4), and
`label_scan_gate_folds_a_membership_changing_segment` pins the safety-critical direction (a
born `:Person` is still folded in).
