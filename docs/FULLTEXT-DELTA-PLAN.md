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
| Asking whether a given edge is dead | see the corrected section below — answerable today | exists |
| *Suppressing one parallel edge while sparing its siblings* | — | **missing**, but only keyed `DELETE` needs it |

So the candidate set is constructible today, mirroring the node arm exactly:

```
born edge id range  ∪  core_patched_edges() ids  ∪  ⋃ segment.edge_ids()
```

### The suppression question — corrected

> **Correction, 2026-08-15.** The first draft of this plan claimed the blocker was a
> missing `is_edge_tombstoned(id)` primitive, and that building it would also unblock the
> keyed `DELETE` refusal, so the two should be sequenced together. **That is wrong**, and
> it was found by starting the implementation rather than by re-reading. The two needs are
> different sizes and are not coupled. The original claim is left visible here because the
> mistake is instructive: it inferred a shared prerequisite from a shared *symptom*
> ("suppression is by pair, not by id") without checking what each consumer actually needs.

What full text needs is narrower than what keyed `DELETE` needs.

- **Keyed `DELETE`** must *spare an edge's siblings* — suppress one parallel edge and leave
  the others live. That genuinely requires naming an edge by id at write time, and it is a
  real piece of work (write-path resolution, a new tombstone shape, the L0 fold).
- **Full text** only has to *ask whether a given edge is dead*. It already holds the edge
  id (from a core index hit) and the document record already carries the endpoints. It
  never needs to spare a sibling, because it is filtering, not deleting.

And the second question is answerable with APIs that already exist:

| Question | Existing API |
|---|---|
| Did a segment tombstone this edge? | `SegStack::resolve_edge_row(id)` → `EdgeRow.tombstoned` |
| Did the live delta tombstone it? | `delta.out_edges(src)`, matching `tombstoned && other == dst && reltype == T` — exactly the match the traversal overlay makes |
| Is an endpoint deleted? | `delta.is_tombstoned(node)`, `stack.is_node_tombstoned(node)` |

The pair-shaped delta match is not an approximation here: suppressing *every* parallel edge
of that type to that neighbour is precisely what a delete does today, so full text agrees
with traversal by construction. When keyed `DELETE` later narrows that, full text inherits
the narrowing for free, because it is asking the same question through the same overlay.

**Consequence for sequencing:** the relationship overlay does **not** block on keyed
`DELETE`, needs no delta format change, no WAL change and no write-path change. Keyed
`DELETE` becomes independent follow-up work rather than slice 1.

The one hard prerequisite is the `.docs.blk` reverse lookup (slice 0), because answering
"is this edge dead" needs the document's endpoints, and fetching those is currently a
linear scan.

## Slices

Ordered. Slice 0 is a hard prerequisite; slice 1 stands alone and fixes the worse defect;
slices 2-4 build the overlay arm.

### 1 · `fulltext_dead` answers for relationships

Compose the three existing checks in the table above into the `relationships` arm of
`fulltext_dead`, which today returns a hardcoded `false`. No new primitive.

Its own red test: an edge deleted through the writable layer stops being returned by
`db.idx.fulltext.queryRelationships` *before* any consolidation. That test fails today and
is the whole point of the slice.

Note this slice makes the **core** arm correct under deletion. It is independent of slices
2–4, which make the **overlay** arm exist at all, and it is worth landing first because a
full-text index that returns deleted facts is a worse defect than one that misses new ones.

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

Slice 0 is a prerequisite, or this inherits a linear scan.

### 0 · (Prerequisite, do first) A reverse lookup for `.docs.blk`

`fulltext_doc_entry` finds a relationship's document record by **linear scan over the whole
`.docs.blk`** — `for d in 0..r.doc_count()`, per hit — because the file is docid → entity
with no reverse map. That is O(hits × documents) on every relationship query *today*, before
any of this work.

It is an independent bug and deserves its own ticket. It is also a hard prerequisite for
slices 1 and 4, both of which need a document's endpoints per hit, so it goes first.

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
3. A deleted edge stops being found. *(The slice-1 test; fails today.)*
4. A fact present in both core and delta is scored **once**, from current text.
5. An edge query's term weights come from the **edge** index's statistics, not the node
   index's. (Guards slice 3's silent failure mode.)

## Out of scope

- A maintained/incremental overlay index.
- Stemming (still reserved in the manifest, still deliberately unimplemented).
- Block-max WAND (the `max_impact` bytes are written and unread).
- Phrase queries.
