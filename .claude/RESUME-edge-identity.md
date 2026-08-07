# Next: edge identity by an indexed edge property

Resume note. Everything else about the Graphiti compatibility work is committed on
`feat/graphiti-write-compat` (8 commits, working tree clean, 1042 lib tests green,
clippy + fmt clean). This is the one deliberately deferred item.

## The gap, reproduced

Graphiti emits (verbatim, `graphiti_core/models/edges/edge_db_queries.py:19-27`):

```cypher
MATCH (episode:Episodic {uuid: $episode_uuid})
MATCH (node:Entity {uuid: $entity_uuid})
MERGE (episode)-[e:MENTIONS {uuid: $uuid}]->(node)
SET e.group_id = $group_id, e.created_at = $created_at
```

Slater answers `relationship properties are not yet supported in a write`
(`parser.rs`, in `lower_edge_write`: `if !rel.props.is_empty()`).

Everything else in that statement already works — both MATCH-bound endpoints and the
repeated `SET` clauses landed in `982112f`. The inline `{uuid: $uuid}` is the whole gap.

**Why it matters, and why it is not cosmetic.** The map is the edge's *identity*:
create-if-no-edge-with-this-uuid, not create-if-no-edge-between-these-nodes. Slater's
`EdgeIdentity` is `(src, reltype, dst)`, so Graphiti's several `RELATES_TO` edges between
the same entity pair collapse to one and **facts are silently lost**. Silent, not loud —
which is what makes it worth doing properly rather than approximating.

## Sizing — smaller than the earlier estimate

- `EdgeIdentity` (`crates/slater-delta/src/identity.rs:41`) is `{src: NodeIdentity,
  reltype: SymbolId, dst: NodeIdentity}`. **11 usages across 3 files**, all inside
  `slater-delta` (`identity.rs`, `lib.rs`, `memtable.rs`). The blast radius is contained.
- The shape to add is the one nodes already have: an optional `(key: SymbolId, value:
  Value)` beside the triple, keyed off an **edge** range index. The machinery exists —
  `RangeIndexDesc` already carries `entity: EntityKind::Edge`, and `consolidate.rs:578`
  already re-emits `CREATE INDEX FOR ()-[r:T]->() ON (r.prop)`.
- `canonical_key` (`identity.rs:77`) is the function that changes; it is a byte encoding
  used as a map key, so any change to it changes every key in the memtable.

**Correction to carry forward:** an earlier design pass claimed `wal.rs` has no record
version and that one must be added. That is wrong at the file level — the WAL magic is
`SLWAL001` (`wal.rs:94`), with `SLWALE01` for the sealed variant, and an unrecognised
magic already fails closed with `bad or missing WAL magic` (`wal.rs:1067-1071`). So the
versioning hook exists; the question is only whether to bump it and what to do with an
in-flight WAL written by the older binary. Decide that deliberately — a bump means a
replay of an existing WAL is refused, which needs an operator story.

## Also worth doing in the same change

`SET r = $map` on a relationship is still rejected, and the reason is adjacent:
`WalOp::UpsertEdge` (`crates/slater-delta/src/wal.rs:191`) carries merge-semantics
`patches` with no replace flag, so honouring it would silently *merge* where the statement
says replace. Graphiti emits `SET e = $edge_data`, so edges cannot be written without it.
Both this and edge identity are changes to the same op, so they belong together.

## What "done" looks like

The end-to-end harness is at
`/tmp/claude-1000/.../scratchpad/e2e/` (`verify.py` + `graphiti-schema.cypher`) — move it
to `hs-memory/scripts/`. It runs graphiti-core 0.29.3's real statements against a live
Slater with the neo4j Python driver 6.2.0. Currently **7 of 8 pass**; the failing case is
`MENTIONS edge, graphiti verbatim (inline edge property key)`. That case going green is
the acceptance test.

Add a Rust test too: two same-pair `RELATES_TO` edges with distinct `uuid`s must both
survive a consolidation round-trip. That is the regression the collapse would cause.

## Empirical findings from this session that are not in git

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

## Then

`graphiti-slater` (driver + `SearchInterface`) and the `hs-memory` compose stack — tasks 8
and 9 in the plan at `~/.claude/plans/using-slater-as-a-soft-twilight.md`.
