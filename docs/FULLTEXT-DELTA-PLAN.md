# Full-text delta plan — closing the relationship overlay gap

Status: **planned, not started.** Written 2026-08-15, against `main` at v0.25.0.

## Context

v0.25.0 shipped full-text indexes with a **node** overlay arm: a document written
through the writable layer is findable immediately, because the built index serves the
base generation while the write delta and sealed segments are served by an overlay that
analyses each affected document's *current* text at query time
(`crates/slater/src/exec/fulltext.rs`).

**Relationships got no overlay arm.** `fulltext_overlay_ids` returns an empty set when
`relationships` is true, so a relationship index is served from the base generation alone
and a fact created or edited since the build keeps its old text until
`CALL slater.consolidate()`. That is deliberate and explicit rather than emergent — but it
makes consolidation scheduling a retrieval-quality concern on a write-heavy graph, and it
is the last knowingly-degraded surface in the full-text feature.

This document plans closing it.

## What the code actually says, versus what it needs

The comment at `exec/fulltext.rs` justifying the empty set says the writable layer's edge
rows "carry patches rather than a scannable identity set". That is half true, and the half
that is not is the good news: the delta is identity-addressed for *authoring*, but a dense
edge id space already exists beside it.

| Capability | Location | Status |
|---|---|---|
| Born edges occupy a contiguous synthetic id range | `slater-delta/src/memtable.rs` `edge_synthetic_base()`, `born_edge_count()` | exists |
| Patched core edges, carrying their real edge id | `memtable.rs` `core_patched_edges() -> Vec<(u64,u64,u64,String)>` | exists |
| Id → edge delta lookup | `memtable.rs` `edge_delta_by_id` | exists, **private** |
| Sealed segment edge ids | `graph-format/src/segment.rs` `SegmentReader::edge_ids()` | exists, public |
| Off-heap L0 edge ids | `slater-delta/src/l0_offheap.rs` `edge_ids()` | exists |
| Reading an overlay edge's current text | `slater/src/exec/access.rs` `edge_prop(id, key)`, merged view | exists |
| **Suppression of a deleted edge by id** | — | **missing** |

So the candidate set is constructible today, mirroring the node arm exactly:

```
born edge id range  ∪  core_patched_edges() ids  ∪  ⋃ segment.edge_ids()
```

### The real blocker is suppression, and it is shared

`fulltext_dead` returns `false` for relationships because there is no
`is_edge_tombstoned(id)`. The delta suppresses a deleted edge by `(reltype, neighbour)` in
the traversal overlay, never by id.

That is **the same missing primitive** that forces the keyed `DELETE` refusal
(`MATCH (a)-[r:R {uuid:$u}]->(b) DELETE r`): the overlay cannot spare an edge's siblings
because it cannot name one. Building suppress-by-id once buys both. This is the sequencing
insight the plan turns on.

## Slices

Ordered. Slice 1 carries all the design risk; the rest are mechanical once it lands.

### 1 · Suppress an edge by id

A tombstone that names a resolved core edge id, and a suppress-by-id set on the traversal
overlay beside its existing suppress-by-pair one. Then:

- `slater/src/segstack.rs` — `is_edge_tombstoned(id)` beside `is_node_tombstoned(id)`.
- `slater-delta/src/memtable.rs` — `DeltaSnapshot::is_edge_tombstoned(id)` beside
  `is_tombstoned(dense_id)`.

Land this **alone**. Keyed `DELETE` rides on top of it as a separate change, so the two are
not entangled in one diff and each gets its own red test.

### 2 · `DeltaSnapshot::edge_dense_ids()`

Public, mirroring `node_dense_ids()`, composed from the three sources in the table above.
Returns a **set**: a patched core edge can also appear in a segment, and scoring it twice
is exactly the double-count the node arm's suppression exists to prevent.

### 3 · Generalise the overlay over `EntityKind`

`fulltext_overlay_ids` drops its early return. `fulltext_overlay_hits` currently hardcodes
two node-shaped things:

- `node_label_ids(id).contains(&label_id)` — becomes a relationship-type check for the
  edge arm.
- The document-frequency lookup at the bottom passes `EntityKind::Node` **literally**. It
  must take the call's kind. Left as-is, an edge query would score its terms against *node*
  corpus statistics and produce a plausible-looking ranking that is wrong — the failure
  mode here is silent, not loud, so it needs its own assertion.

### 4 · Endpoints on an overlay hit

A yielded relationship needs its endpoints. `fulltext_doc_entry` returns `endpoints: None`
for anything not in `.docs.blk`. For a born edge they come from the delta's identity; for a
patched core edge, from the core row.

**Do slice 0 below before this one**, or it inherits a linear scan.

### 0 · (Prerequisite, independent) A reverse lookup for `.docs.blk`

`fulltext_doc_entry` finds a relationship's document record by **linear scan over the whole
`.docs.blk`** — `for d in 0..r.doc_count()`, per hit — because the file is docid → entity
with no reverse map. That is O(hits × documents) on every relationship query *today*, before
any of this work.

It is an independent bug and deserves its own ticket, but the edge overlay makes it far more
visible, so schedule it first.

## Scoring

Keep the single reconciled idf: both arms must put a term on one scale, or results reorder
by how recently something was written, which reads as a ranking opinion rather than a bug.

Two edge-specific wrinkles:

- A **born** edge has no core document, so its terms get `df = 0` and score as maximally
  rare. That is defensible when the core index is large. For an edge index it may not be: a
  graph built with few relationships of that type has `desc.doc_count` near zero, `n_docs`
  collapses to the overlay size, and ranking is driven almost entirely by the fallback.
  **Check `bm25::idf(n_docs, 0)` at small `n_docs` before trusting it.**
- The existing downward bias on recently-edited terms applies unchanged and still vanishes
  at consolidation. Do not try to fix it here: subtracting a superseded document needs its
  old text, which is not retained. State it in the module note as the node arm already does.

## Cost, stated rather than hidden

The overlay analyses every candidate's text **per query**, bounded by delta + segment size
rather than graph size. Adding edges roughly doubles that constant on a write-heavy graph.

A maintained incremental overlay index would remove it, and is explicitly **not** this work.
Put the bound in the docs the way the node arm did, so the next person meets it in the
manual rather than in production.

## Tests

**The suite currently asserts the bug.** `hs-memory/scripts/verify.py` asserts that edge
search finds nothing for delta-written facts — the honest current behaviour, pinned so it
could not regress unnoticed in either direction. It will fail when this lands. That failure
is the signal. Update it in the same change.

Red tests to observe failing before fixing:

1. A fact written through the delta is found by BM25 with no consolidation.
2. An edited fact is found by its **new** text and not by its old.
3. A deleted edge stops being found. *(Fails until slice 1 — it is the slice-1 test.)*
4. A fact present in both core and delta is scored **once**, from current text.
5. An edge query's term weights come from the **edge** index's statistics, not the node
   index's. (Guards slice 3's silent failure mode.)

## Out of scope

- A maintained/incremental overlay index.
- Stemming (still reserved in the manifest, still deliberately unimplemented).
- Block-max WAND (the `max_impact` bytes are written and unread).
- Phrase queries.
