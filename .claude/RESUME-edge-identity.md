# Done: edge identity by an identifying edge property

Resume note. This was the deferred item on `feat/graphiti-write-compat`; it has landed.
What follows is what shipped, what deliberately did not, and what is next.

## What shipped

`MERGE (a)-[e:R {uuid: $u}]->(b)` — the inline relationship property map is now the
edge's **identity**, not decoration. `EdgeIdentity` (`crates/slater-delta/src/identity.rs`)
carries an optional `(key: SymbolId, value: Value)` beside `(src, reltype, dst)`, appended
to `canonical_key` only when present — so a keyless edge's byte image is exactly what it
always was, and a keyed one can never collide with it.

Graphiti's several `RELATES_TO` edges between the same entity pair therefore stay distinct
instead of collapsing into one and silently losing facts. Verified end to end, including
across a `CALL slater.consolidate()`.

Alongside it, four things the same op had to grow to support:

- **`SET r = $map` on a relationship.** `WalOp::UpsertEdge` gained a `replace` flag and
  `EdgeDelta` a `replaced` field, so the write can say *replace* where it used to be
  refused for silently meaning *merge*. All the whole-map spellings (`SET r = {…}`,
  `SET r += {…}`, and their `$param` forms) now lower; the write path folds the `SET`
  sequence into the one `(patches, replace)` pair the edge op carries.
- **`RETURN` after a relationship write.** `MERGE (a)-[e:R {…}]->(b) SET e = $d RETURN
  e.uuid` — graphiti's `RELATES_TO` save needs it, and the node write already had it. The
  edge is resolved over the *post-write* merged view (`find_merged_edge_id`), so a
  delta-born edge projects its synthetic id and a patched core edge its own.
- **A keyless MERGE adopts a keyed identity.** `MERGE (a)-[r:R]->(b)` means "any `R`
  between the pair", so it matches an edge the delta already holds under an identifying
  property rather than standing beside it. Without this the same statement pair answered
  two ways: two edges before a consolidation, one after (the core probe is an adjacency
  scan, which always matched any `R`). See `DeltaWriter::edge_identity_key_between`.
- **The identifying property reads back.** It is seeded into the delta's patches, so
  `r.uuid` answers off a born edge (which has no core row to carry it) and survives a
  `SET r = $map` that omits it. A `SET` that would assign the identity property a
  *different* value is refused — an edge that stops answering to the name it is stored
  under is unfindable.

### Deliberately refused, not silently approximated

- **A keyed `DELETE`** (`MATCH (a)-[r:R {uuid:$u}]->(b) DELETE r`). The traversal overlay
  suppresses a core edge by `(reltype, neighbour)`, so it cannot spare that edge's
  siblings. Deleting every `R` between the pair is exactly the loss this work exists to
  prevent, so the parser refuses with a message saying so. To lift it, the tombstone has
  to name a resolved core edge id and the overlay needs a suppress-by-id set beside its
  suppress-by-pair one.
- **More than one inline property.** Which one is the identity would be a guess.
- **A `MERGE` text dump of a graph with parallel edges.** `serialise_merge_dump` now
  refuses, mirroring how it already refuses a vector-carrying graph. The builder's
  `edge_overwrite` locates an edge by `(src, dst, reltype)` and *ignores* an inline
  relationship property map, so the dialect genuinely cannot spell two `R` edges between
  one pair — emitting them would fuse them on rebuild. Consolidation itself takes the
  binary path, which carries each edge by id and is unaffected.

### On the WAL, and why there is no magic bump

The earlier design pass was right that a version hook exists (`SLWAL001`, `wal.rs:94`) and
wrong that one had to be added. Neither was needed: the extra fields ride **new op tags**
(`OP_UPSERT_EDGE_V2` / `OP_DELETE_EDGE_V2`), emitted only when a record actually carries an
identifying property or a replace. A write that was expressible before encodes to the same
bytes as before (there is a test pinning this), so an existing WAL replays unchanged and no
operator story is needed. An older binary meeting a v2 tag fails closed on the
unknown-op-tag path, which is the right way round.

The L0 segment body went to `L0_FORMAT_VERSION = 4`, which needs no story at all — a
segment lives only between a flush and the next consolidation, and a version mismatch is
already a hard error on open.

## Verification

- **1560 lib tests green**, clippy + fmt clean.
- Rust regression tests: two same-pair edges with distinct keys survive a write, a flush, a
  `merge_levels` fold and an L0 round-trip (`memtable.rs`); the keyless canonical encoding
  is unchanged (`identity.rs`); a keyed WAL op round-trips and a keyless one still encodes
  under the v1 tag (`wal.rs`); the parser accepts graphiti's verbatim statements and
  refuses what it cannot identify.
- **End-to-end: 10 of 10 pass.** The harness is `hs-memory/scripts/verify.py` +
  `hs-memory/schema/graphiti-schema.cypher`; it runs graphiti-core 0.29.3's real statements
  against a live Slater over the neo4j Python driver 6.2.0. Both edge cases the note asked
  for are in it, plus the two new ones (`RELATES_TO` verbatim, and that `SET e = $map`
  really replaces).

To run it: `slater-build --input schema/graphiti-schema.cypher --graph graphiti --data-dir
./data`, start `slater` beside a `config.json` on port 7699 with a `graphiti` ACL user, then
`python scripts/verify.py`.

## Empirical findings that are still not in git

1. **D12 is real and measured.** An *indexed* embedding reads back as `Null` from a column
   (`exec/access.rs:186` — routed out to the vector store). So graphiti's FalkorDB
   similarity leg, an inline `vec.cosineDistance` over a label scan, returns **zero rows**
   on Slater. `CALL db.idx.vector.queryNodes` finds the same vectors fine, including ones
   written through the delta moments earlier. The `graphiti-slater` adapter therefore
   *must* route similarity through graphiti's `driver.search_interface` hook; this is not
   optional and not a performance nicety.
2. **The rollback warning reaches applications.** Attaching it to the offending `RUN` (not
   just COMMIT/ROLLBACK, whose metadata the drivers discard) works: the Python driver
   surfaces it as a `GqlStatusObject`, `severity=WARNING`, `gql_status='01N42'`.
3. **Temporal parameters round-trip as ISO-8601 strings**, and `properties(n)` returns
   `created_at` as a `str`, which is what graphiti's `parse_db_date` expects.

## Then — W8, **now done**

`graphiti-slater` and the `hs-memory` compose stack both landed after this note was
written. W8 — UNWIND-batched relationship writes — has since landed too, and the stack
now runs end to end: a real `add_memory` extracts on Bedrock, embeds on Voyage, persists
through Slater, and reads back through MCP. 1054 lib tests green, `verify.py` 13/13 at
dim 1024.

The edge grammar gained the `unwind_clause?` prefix the node arm already had (plus
`with_clause*` for graphiti's no-op `WITH r, edge`), `EdgeWriteStmt` gained `unwind`, and
`execute_edge_write_batch` commits the whole list through one `DeltaWriter::write_batch`.
The core-edge probe, the idempotent-re-MERGE decision and the keyless-identity adoption
were extracted into `plan_edge_merge`, and `edge_set_patches` now takes the same
evaluator-closure pair the node path does — so single and batched writes share one
implementation. `edge_delete` deliberately got **no** `UNWIND` prefix, for the same reason
a keyed `DELETE` is refused: the overlay suppresses by `(reltype, neighbour)` and cannot
spare an edge's siblings.

Full detail: `~/.claude/plans/using-slater-as-a-soft-twilight.md`.

What it was, for the record:

```cypher
UNWIND $entity_edges AS edge
MATCH (source:Entity {uuid: edge.source_node_uuid})
MATCH (target:Entity {uuid: edge.target_node_uuid})
MERGE (source)-[r:RELATES_TO {uuid: edge.uuid}]->(target)
SET r = edge
SET r.fact_embedding = vecf32(edge.fact_embedding)
WITH r, edge
RETURN edge.uuid AS uuid
```

Refused as an unsupported write, and graphiti uses the batched form for both `MENTIONS`
and `RELATES_TO` — so nothing it extracted persisted. Both statements are now in
`hs-memory/scripts/verify.py` verbatim, and both pass.

## What is actually left

Nothing on this track blocks the stack. What remains is work stream 2 — **fulltext**
(`db.idx.fulltext.*`), ~5,600 LOC, untouched. Until it lands, graphiti's BM25 leg returns
empty and hybrid search runs on the similarity list alone: worse recall on exact-name
matches, everything else working. Also still open: communities / `HAS_MEMBER` (a
label-less endpoint the relationship write does not accept), and a keyed `DELETE`, which
needs a tombstone naming a resolved core edge id and a suppress-by-id set in the overlay.

How to run the stack, and the full history: `~/.claude/plans/using-slater-as-a-soft-twilight.md`.
