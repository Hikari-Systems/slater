# Per-(label, property) value→count histogram — implementation plan

**Goal.** Make `MATCH (n:Label) RETURN n.prop, count(*)` and
`MATCH (n:Label) RETURN count(DISTINCT n.prop)` sub-millisecond on a
low-cardinality indexed property, down from ~24–25 ms today on MeSH
(`MeshTerm.type`, 340,839 nodes, 4 distinct values).

This is **Level 1** (per the brief). Level 2 (index-only aggregation pushdown
with a WHERE filter) is described at the end as a follow-up and is **not** built
here.

---

## Key finding from the exploration — this is much smaller than it looks

The query-recognition machinery **already exists** and already ships. `exec.rs`
`run_single` Stage 7 calls `try_grouped_index_fast_path` (`exec.rs:1931`), which:

- recognises *exactly* the two target shapes (group-by `n.p` + `count(*)`/`count(n)`,
  and `count(DISTINCT n.p)`), with all the correct guards: one non-OPTIONAL MATCH,
  no WHERE, single-node pattern (no rels), exactly one positive label, no inline
  props, grouping key is a bare stored `n.p`, the only aggregate is `count`;
- computes the null group (`label_node_count − Σ indexed counts`) for group-by;
- applies `ORDER BY` / `SKIP` / `LIMIT` to the small grouped output;
- already requires an **open range index over `(label, prop)`**.

Its entire cost is **one line** — `exec.rs:2007`:

```rust
let groups = reader.distinct_key_counts()?;   // Vec<(Value, u64)>, ascending key
```

`IsamReader::distinct_key_counts()` (`isam.rs:465`) walks **every block** of the
ISAM index and run-length-counts equal keys — O(index entries) of zstd
decompression. On 340k `MeshTerm.type` keys that is the ~24 ms. The answer it
returns is tiny (4 pairs).

So Level 1 is precisely: **precompute that `Vec<(Value, u64)>` at build time and
hand it back in O(distinct) instead of recomputing it from the full index at query
time.** The stored histogram is *byte-for-byte the same vector* the scan produces,
so correctness is preserved by construction and the parity test is trivial.

Nothing about the planner, the shape detection, the null-group math, or
ORDER BY/LIMIT changes. We add a precomputed store, a reader accessor, and a
3-line "use the precompute if present, else fall back to the walk" at `exec.rs:2007`.

A histogram exists **per node range index** — `RangeIndexDesc` for
`EntityKind::Node` is exactly a `(label_or_type, property)` pair, and the exec
path already keys off the index `name`. So histograms align 1:1 with node range
indexes, identified by the same `name` stem.

---

## Precedent followed

This mirrors the just-shipped `feat(query): relationship-type scan` (f92429b)
end-to-end: format version bump, a new graph-format module, a manifest descriptor
vector, emission in **both** builders, inventory in `common.rs`, a reader accessor
in `generation.rs`, an executor fast path, and tests at each layer including a
build-parity test and a feature-on/off identical-results test.

---

## Design

### Derivation (the cheap, uniform, parity-guaranteed way)

For each **node** range index, after its `.isam` is written to `tmp_dir`, re-open
it with the build cipher and call `distinct_key_counts()` — the *same* function the
query path calls. This guarantees the stored histogram is identical to the
query-time result in **both** builders with **zero** duplicated counting logic.
Build time is offline; one extra sequential decompress per indexed property is
negligible.

- If `distinct_count ≤ HISTOGRAM_MAX_DISTINCT` → encode and store the histogram.
- Else → **skip** it (a 340k-distinct `name` histogram is as big as the index and
  useless), and `log::info!` the skip with label, property, and distinct count
  (no silent caps — per the brief).

The cap is a **build config option**, not a hard-coded const:

- `BuildOptions.histogram_max_distinct: u64`, default **4096** (covers real
  low-card group-by columns; `type` = 4). The `Default` impl and graph-format
  expose `4096` as the documented default.
- CLI flag `--histogram-max-distinct <N>` in `main.rs` (`#[arg(long,
  default_value_t = 4096)]` → `cli.histogram_max_distinct` →
  `BuildOptions.histogram_max_distinct`), threaded into both build paths exactly
  like `block_size` / `ann_threshold`.
- `N = 0` ⇒ **histograms disabled** for the build (every node index is skipped and
  logged). This is the knob that produces a clean "histogram-off" generation for
  the live identical-rows verification (step 3 below) — no recompile needed.

Edge range indexes get **no** histogram (the target shapes are node-anchored only).

### On-disk store — `prop_hist.blk`

One blockfile beside `topology.csr.blk`, following `postings.rs` exactly: **one
record per stored histogram**, record index = position in the new manifest
descriptor list `property_histograms`. Record encoding (new
`graph-format/src/histogram.rs`, mirroring `encode/decode_endpoint_posting`):

```
record = uvarint(count) ‖ for each pair: wire::write_value(value) ‖ uvarint(run_count)
```

Values use the existing `wire::write_value` / `read_value` (the same total-order
`Value` the ISAM stores). Pairs are emitted in ascending key order (as
`distinct_key_counts` returns them), so `decode` returns the identical
`Vec<(Value, u64)>`.

A separate file (not stuffed into the manifest JSON) keeps the manifest small and
matches how every other data store is handled; the descriptors in the manifest
stay tiny.

### Manifest

New `manifest.rs`:

```rust
/// Descriptor for one stored (label, property) value→count histogram. Aligned by
/// position with the `prop_hist.blk` records. Present ⇒ the generation can answer
/// whole-label group-by / count(DISTINCT) on this index from O(distinct) instead
/// of an O(index) walk. Absent (over the cardinality cap, or an edge index) ⇒ the
/// query path falls back to `distinct_key_counts()` — slower, never incorrect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PropertyHistogramDesc {
    /// Range-index file stem this histogram derives from (= `RangeIndexDesc.name`).
    pub index_name: String,
    pub label: String,
    pub property: String,
    pub distinct_count: u64,
}

// added to `Manifest`:
#[serde(default)]
pub property_histograms: Vec<PropertyHistogramDesc>,
```

`#[serde(default)]` ⇒ old (v2) generations deserialize with an empty vector and
simply use the fallback path. Format bumps **v2 → v3** in
`graph-format/src/lib.rs` (forces a rebuild; the version gate already enforces it).
`prop_hist.blk` added to the inventory in `common.rs` and to `PublishInputs`.

### Reader (`generation.rs`)

Mirror the range-index opening: load all histograms resident at `open` into

```rust
prop_histograms: HashMap<String, Vec<(Value, u64)>>,   // keyed by index name
```

(they are tiny and few). Read each `prop_hist.blk` record via `BlockFileReader`
and `histogram::decode`, keyed by `index_name` from `manifest.property_histograms`.
Public accessor:

```rust
pub fn property_histogram(&self, index_name: &str) -> Option<&[(Value, u64)]>;
```

Returns `None` when no precompute exists (gates the fast path; never wrong).

### Executor (`exec.rs:2007`) — the only behavioural change

Replace the single line with:

```rust
let groups: Vec<(Value, u64)> = match self.gen.property_histogram(&idx_name) {
    Some(h) => h.to_vec(),
    None => reader.distinct_key_counts()?,
};
```

Everything below (`is_distinct` branch, `groups.len()`, null-group sum,
`order_skip_limit_no_input`) is unchanged. Same `groups` ⇒ identical rows. The
histogram is consulted **only** inside a function whose guards already enforce all
the brief's boundaries (whole-label anchor, no WHERE, no second pattern, grouping
key a stored prop of the anchor label, aggregate is `count(*)`). min/max of the
grouping key are not in this shape and are unaffected; sum/avg of another property
never reach here.

### Builders

- **In-memory** (`build.rs`, after the `write_isam_with_cipher` loop ~line 500):
  for each emitted **node** `RangeIndexDesc`, re-open `range/<name>.isam` with
  `cipher`, `distinct_key_counts()`, apply the cap, collect `(desc, record)`.
  Then write `prop_hist.blk` and pass `property_histograms` into `PublishInputs`.
- **External** (`build_external.rs`, after the `write_isam_sorted` loop ~line
  1390): identical post-pass over the written node `.isam` files. Because the
  derivation reads the *finished* ISAM (not the in-RAM vs ext-sorted stream), both
  paths are guaranteed to agree — no separate counting code per builder.

A shared helper in `histogram.rs`:

```rust
pub fn derive_histogram_from_isam(
    isam_path, cipher, max_distinct,
) -> Result<Option<Vec<(Value, u64)>>>   // None ⇒ over cap (skip)
```

keeps both builders to a single call each.

---

## Files touched

| Layer | File | Change |
|---|---|---|
| Format | `graph-format/src/lib.rs` | `FORMAT_VERSION` 2→3; `pub mod histogram`; const test |
| Format | `graph-format/src/histogram.rs` | **new**: `encode`/`decode`, `derive_histogram_from_isam(.., max_distinct)`, `DEFAULT_HISTOGRAM_MAX_DISTINCT = 4096`, writer for `prop_hist.blk` |
| Format | `graph-format/src/manifest.rs` | `PropertyHistogramDesc`; `property_histograms` field; test fixtures |
| Build | `slater-build/src/build.rs` | `BuildOptions.histogram_max_distinct` field + `Default` |
| Build | `slater-build/src/main.rs` | `--histogram-max-distinct` CLI flag (default 4096; 0 disables) |
| Build | `slater-build/src/common.rs` | `prop_hist.blk` in inventory; `property_histograms` in `PublishInputs` |
| Build | `slater-build/src/build.rs` | post-pass: derive + write histograms (in-memory) |
| Build | `slater-build/src/build_external.rs` | post-pass: derive + write histograms (external) |
| Server | `slater/src/generation.rs` | open `prop_hist.blk`; `property_histogram()` accessor |
| Server | `slater/src/exec.rs` | 3-line precompute-or-fallback swap at the `distinct_key_counts` call |
| Tests | `slater-build/tests/property_histograms.rs` | **new**: build-parity + ground-truth |
| Tests | `slater/src/testgen.rs` | `with_histogram` fixture variant |
| Tests | `slater/src/exec.rs` (tests) | histogram-on vs histogram-off identical-rows |

---

## Tests

1. **graph-format unit** (`histogram.rs`): `encode`→`decode` round-trips an
   arbitrary `Vec<(Value, u64)>` across `Value` variants; cap boundary
   (`distinct == cap` stored, `cap + 1` skipped).
2. **Build parity** (`slater-build/tests/property_histograms.rs`): build the same
   dump via in-memory and external paths; decode `prop_hist.blk`; assert the
   per-index histograms are equal aligned by `index_name`, and equal to a ground
   truth derived independently from the node property records. Mirrors
   `endpoint_postings.rs`.
3. **Reader**: `property_histogram(name)` returns the decoded pairs; `None` for an
   over-cap / absent index.
4. **End-to-end identical results** (the correctness proof): one fixture built
   `with_histogram = true`, one `false` (identical data, postings omitted — the
   `testgen` two-variant pattern). For each of
   `MATCH (n:L) RETURN n.p, count(*) ORDER BY count(*) DESC LIMIT k`,
   `MATCH (n:L) RETURN n.p, count(*)` (incl. the null group), and
   `MATCH (n:L) RETURN count(DISTINCT n.p)`, assert **identical rows** from both
   generations (histogram path == `distinct_key_counts` path). Also assert a
   property *over* the cap (no histogram) still returns correct rows via fallback.
5. **Boundary negatives** (already covered by existing guards; assert they still
   fall back to the scan and stay correct): a WHERE present, a second pattern, an
   expression grouping key, `sum`/`avg` of another property, multi-label anchor.
6. Existing suites stay green (`memory_headline.rs` etc.).

---

## Verification (build + measure)

1. `cargo build --release` (workspace) and `cargo test` (graph-format,
   slater-build, slater).
2. Build a generation with a low-card indexed node property. Adapt
   `perf/cross-engine-hs/` — it already loads MeSH with a range index on
   `MeshTerm.type` (`setup_hs.sh` / `load_cypher.py`), which is the exact target.
3. Correctness in the live server: run both queries and confirm the rows match the
   pre-change output (and a one-off build with `--histogram-max-distinct 0`, i.e.
   histogram disabled, returns identical rows — the live analog of test 4):
   - `MATCH (n:MeshTerm) RETURN n.type, count(*) ORDER BY count(*) DESC LIMIT 10`
   - `MATCH (n:MeshTerm) RETURN count(DISTINCT n.type)`
4. Latency: `perf/cross-engine-hs/bench_mesh.py slater` — its suite already
   contains `"group-by type"` and `"count DISTINCT type"`. Expect ~24–25 ms →
   sub-millisecond on both; confirm every other shape (count, label count, point
   lookup, traversals, CONTAINS) is unchanged. Report before/after medians.

---

## Boundaries (honest statement)

Level 1 fires **only** inside `try_grouped_index_fast_path`, whose existing guards
already enforce: whole-label anchor, no WHERE, single-node pattern (no second
pattern / rels), exactly one positive label, grouping key a bare stored property
`n.p` of that label with an open range index, and the only aggregate `count(*)` /
`count(n)` / `count(DISTINCT n.p)`. Anything else — a filter, an expression key, a
relationship property, `sum`/`avg`, multi-label — never reaches the histogram and
runs today's path. The precompute only ever returns the **same** `groups` vector
the index walk would, so it is strictly a faster path to an identical answer.

---

## Level 2 (follow-up — NOT built here)

Index-only aggregation pushdown for grouping/counting on an indexed property *with*
a WHERE filter (ranges, partial filters): walk the sorted ISAM keys in range and
aggregate without per-node property decode. Turns the ~24 ms scan into low-single-
digit ms and covers far more shapes, but does **not** reach O(1) and needs a real
index-scan aggregation operator in the executor (a new plan node + exec operator,
charging/budget integration, ORDER-BY-on-aggregate handling). Larger and more
invasive; tracked separately.
