# 11 · Writing data

Slater serves read-only generations by default. When you need to mutate a live
graph, you enable the **writable layer**: writes land in an in-memory delta backed
by a write-ahead log, are visible to the very next read, and are later folded back
into the base with `CALL slater.consolidate()`. This page is the full write
surface.

## Enabling the writable layer

The layer is **off by default**. Turn it on with `delta.enabled=true` (config or
the `delta__enabled` environment variable) and restart the server. On startup you
will see `writable=true` and, if a WAL already exists, a replay line:

```
INFO starting slater (Bolt graph engine) … writable=true
INFO writable layer replayed WAL graph=social node_deltas=4
INFO writable layer enabled wal_dir=wal …
```

While it is off, any write clause is rejected:

```
this slater connection is read-only: the writable layer is not enabled (set delta.enabled)
```

Two more gates apply even when the layer is on:

- **ACL:** the connected user needs a `write` grant on the graph. A `read` grant
  does **not** imply write. Without it you get `write access to graph '…' is not
  granted`.
- **Statement shape:** only the clauses below are accepted. Anything else — an
  unsupported write form — is rejected with:
  `unsupported write: the writable layer accepts business-key MERGE / SET /
  REMOVE / [DETACH] DELETE, CREATE / INSERT (GQL), and relationship writes only`.

## Creating and updating nodes

A node write anchors **one node by its business key** — a single label and a
single inline key property that must be range-indexed and unique.

```cypher
-- MERGE: create if absent, else match; then apply SET
MERGE (p:Person {email:'linus@example.com'}) SET p.name='Linus Torvalds', p.age=54;

-- CREATE: like MERGE but the key must be a fresh, range-indexed business key
CREATE (p:Person {email:'ken@example.com'});

-- INSERT: the ISO GQL spelling, equivalent to CREATE
INSERT (p:Person {email:'ken@example.com'});
```

`MERGE` also supports `ON CREATE SET …` / `ON MATCH SET …` to apply different
assignments depending on whether the node was created or matched.

### `SET` forms

| Form | Meaning |
|---|---|
| `SET n.prop = value` | Set one property |
| `SET n += {k: v, …}` | Merge a map into existing properties |
| `SET n = {k: v, …}` | Replace **all** properties with the map |
| `SET n = $props` | Replace all properties with a map supplied whole |
| `SET n += $props` | Merge a map supplied whole |
| `SET n:Label` | Add a (pre-existing) label |
| `SET n.embedding = vecf32([…])` | Write an indexed vector ([10 Vector search](10-vector-search.md)) |

Values must be constants — a literal, a `$parameter`, or a field of one (`$p.field`,
`$p.a.b`). The path form is for the common shape where a client sends one map per entity
and addresses into it rather than flattening every field into its own parameter:

```cypher
MERGE (n:Entity {uuid: $data.uuid}) SET n.name = $data.name;
```

It resolves entirely from the bound parameters, so it is as constant as `$data` itself. A
property access on a *graph* variable (`n.other`) is a read, and stays rejected. A field
the map does not carry reads as `null`, exactly as a parameter bound to `null` would.

The map on the right of `SET n = …` / `SET n += …` may itself be supplied whole, as a
parameter rather than a literal — the usual shape when a client holds an entity as one
map:

```cypher
MERGE (n:Entity {uuid: $data.uuid}) SET n = $data;
```

Its keys are then known only once the parameter is bound, so a source that is not a map,
or a value the store cannot hold, is reported at execution and names the offending key.
In a batched write the source may also be the `UNWIND` row (`SET n = row`).

Re-setting the business-key property relocates the node in its index. A replace that
*omits* the key is fine: the key is never stored as an ordinary property, and reads
re-seed it from the node's identity.

Items may be comma-separated in one `SET`, or split across consecutive `SET` clauses —
the two spellings are the same write, folded in source order with the last write winning:

```cypher
MERGE (n:Entity {uuid: $uuid})
SET n:Person
SET n = $props
SET n.embedding = vecf32($embedding);
```

What a statement cannot do is combine *kinds* of updating clause — a `SET` beside a
`REMOVE` or a `DELETE`. A write carries one operation against one anchor, so that
combination is rejected by name rather than partly honoured; issue the clauses as
separate statements.

A `WITH` may appear between updating clauses, as a marker between phases of one write:

```cypher
UNWIND $rows AS row
MERGE (n:Entity {uuid: row.uuid}) SET n = row
WITH n, row
SET n.embedding = vecf32(row.embedding);
```

It is accepted and ignored, because in the single-anchor write model it provably does
nothing: there is no intermediate relation to reshape, so re-projecting what is already
bound changes nothing. A `WITH` that *would* mean something — `DISTINCT`, a `WHERE`,
`ORDER BY`/`SKIP`/`LIMIT`, or an alias introducing a new binding — is rejected by name
rather than quietly dropped.

### Reading back what you wrote

A node write may end in `RETURN`, projecting the node it just wrote:

```cypher
MERGE (n:Entity {uuid: $uuid}) SET n.name = $name RETURN n.uuid AS uuid;
```

The projection runs over the post-commit view, so it reports the values the write
produced — including for a `MERGE` that created the node, which had no identity until the
commit allocated one. Anything a read can project works here: aliases, `properties(n)`,
`labels(n)`, `id(n)`.

A batched write returns **one row per input row, in input order**, so results line up
against the list that was sent:

```cypher
UNWIND $rows AS r MERGE (n:Entity {uuid: r.uuid}) SET n.name = r.name RETURN n.uuid AS uuid;
```

Relationship writes do not yet project a `RETURN`; read them back with a separate
`MATCH … RETURN`.

## Removing data

```cypher
MATCH (p:Person {email:'ken@example.com'}) REMOVE p.age;      -- drop a property
MATCH (p:Person {email:'ken@example.com'}) REMOVE p:Robot;    -- drop a label
MATCH (p:Person {email:'ken@example.com'}) DETACH DELETE p;   -- delete node + its edges
```

A plain `DELETE` of a node that still has relationships is **rejected** — this is a
guard, not a limitation:

```
Cannot delete node, because it still has relationships. To delete it and its relationships, use DETACH DELETE.
```

Use `DETACH DELETE` to remove a node and its incident edges together. The
business-key property cannot be `REMOVE`d, and a newly-created node's identity
label cannot be removed.

## Relationships

```cypher
-- Create/ensure a relationship between two business-keyed endpoints
MERGE (a:Person {email:'ada@example.com'})-[r:KNOWS]->(b:Person {email:'alan@example.com'})
SET r.since = 1936;

-- Delete a relationship (name the edge variable)
MATCH (a:Person {email:'ada@example.com'})-[r:KNOWS]->(b) DELETE r;
```

`MERGE` on a relationship resolves both endpoints by their business keys, creating
absent endpoints as needed. Re-merging an existing edge is an idempotent no-op.

An endpoint may instead be bound by a leading `MATCH` and named in the `MERGE` — the same
write, factored the way generated Cypher usually spells it:

```cypher
MATCH (a:Person {email:'ada@example.com'})
MATCH (b:Person {email:'alan@example.com'})
MERGE (a)-[r:KNOWS]->(b) SET r.since = 1936;
```

Relationship writes take property assignments across as many `SET` clauses as you like,
including the whole-map forms: `SET r = $map` **replaces** the edge's properties and
`SET r += $map` merges over them. (Whole-map replace was once refused, because the durable
op carried merge-semantics patches with no replace flag and honouring it would have
silently merged where the statement said replace. The op now carries the flag.)

A `RETURN` after a relationship write projects what it just wrote, resolved over the
post-write view — so a `MERGE` that *created* the edge still projects it:

```cypher
MERGE (a:Person {email:'ada@example.com'})-[r:KNOWS]->(b:Person {email:'alan@example.com'})
SET r.since = 1936
RETURN r.since;
```

### An inline property can identify the relationship

By default `MERGE (a)-[r:R]->(b)` means *any* `R` between that pair, so re-merging matches
the existing edge. Giving the pattern one inline property makes that property the edge's
**identity** instead:

```cypher
MERGE (a:Entity {uuid:$src})-[r:RELATES_TO {uuid:$edge}]->(b:Entity {uuid:$dst})
SET r = $fact;
```

Two `RELATES_TO` edges between the same pair with different `uuid`s stay two distinct
edges rather than collapsing into one — which is what a graph that records several facts
about the same pair needs. The property reads back off the edge, survives a `SET r = $map`
that omits it, and survives consolidation.

A keyless `MERGE` still means "any `R`", so it adopts an edge already stored under an
identifying property rather than standing beside it — the same statement pair answers the
same way either side of a consolidation.

Two things are refused rather than guessed at: **more than one inline property** (which
one is the identity would be a guess), and a **keyed `DELETE`** —
`MATCH (a)-[r:R {uuid:$u}]->(b) DELETE r`. The traversal overlay suppresses a core edge by
`(type, neighbour)`, so it cannot spare that edge's siblings; deleting every `R` between
the pair is exactly the loss identifying properties exist to prevent, so the parser says so
instead.

## Batched writes with `UNWIND`

For bulk loads, drive many rows through one statement. The source **must be a
parameter list**, and per-row values reference `r` or `r.field`. The whole batch
commits atomically under a single group commit (one fsync):

```cypher
UNWIND $rows AS r
MERGE (p:Person {email: r.email}) SET p.name = r.name
```

```python
s.run(q, rows=[{"email": "margaret@example.com", "name": "Margaret Hamilton"},
               {"email": "barbara@example.com",  "name": "Barbara Liskov"}])
```

If any row fails to evaluate or resolve, the entire batch is rejected before
commit.

Relationships batch the same way, including the identifying-property form, and a
`RETURN` may project a field of the row:

```cypher
UNWIND $edges AS edge
MATCH (source:Entity {uuid: edge.source_uuid})
MATCH (target:Entity {uuid: edge.target_uuid})
MERGE (source)-[r:RELATES_TO {uuid: edge.uuid}]->(target)
SET r = edge
RETURN edge.uuid AS uuid
```

`DELETE` deliberately takes no `UNWIND` prefix, for the same reason a keyed `DELETE` is
refused: a batched delete could not spare an edge's siblings either.

## What you cannot write

These are enforced with clear errors — they keep the served schema stable:

| Attempt | Result |
|---|---|
| Add a label not already in the graph | `cannot add label ':Robot' — it is not defined in the graph (only pre-existing labels can be set)` |
| Write a relationship type not in the graph | rejected — the type must already exist |
| `MERGE`/`CREATE` on a non-range-indexed key | rejected — add a range index at build time |
| `DELETE` a node that still has relationships | rejected — use `DETACH DELETE` |
| `DELETE` naming an inline edge property (`DELETE r` on `-[r:R {uuid:$u}]->`) | rejected — the overlay cannot spare the edge's siblings; see above |
| More than one inline property on a relationship pattern | rejected — which one is the identity would be a guess |

New labels and relationship types come only from a rebuild; the writable layer
works within the schema the base generation already defines.

## Transactions and durability

Each write statement is its own **autocommit** group commit — the fsync is the
acknowledgement barrier, so a returned write is durable. Bolt `BEGIN`/`COMMIT` is
accepted but only opens a **read** transaction; there is no multi-statement write
transaction. Concurrent writes to one graph serialise behind that graph's writer
(bounded by `server.maxConcurrentWrites`, default 4).

## When a write is refused for reasons that are not about the statement

Two failures come from the *state of the server* rather than from the Cypher, and both are
loud on purpose:

| Message | Meaning |
|---|---|
| `the writable layer is bound to generation X but graph 'g' is now serving Y … has NOT been applied` | A new generation was swapped in under the writer, so the delta's internal ids no longer line up. The write was **not** applied. Consolidate or restart to rebind the delta. |
| `refusing to swap graph 'g' to generation X: the writable layer holds N pending entities …` | The generation guard found a new generation on disk while the delta held unconsolidated writes, and kept the current one serving rather than abandoning them. Run `CALL slater.consolidate()`, then publish again. |

The second is why publishing a new generation under a live writable server does not
silently discard the delta. If you are republishing deliberately — a schema rebuild, say —
consolidate first so the delta is folded in, or stop the server.

## Consolidation

Writes accumulate in the delta and (optionally) sealed segments. A query merges
the base plus its segments plus the in-memory delta, so latency stays flat as
writes accumulate. To fold everything back into a single fresh base generation:

```cypher
CALL slater.consolidate()      -- => {generation: '<new-uuid>'}
```

Consolidation **spawns the `slater-build` binary**, so `delta.builderBin` must
resolve to it — an absolute path, or a name on the server's `PATH`. If it does
not, consolidation fails with:

```
consolidation failed: … spawn builder 'slater-build': No such file or directory (os error 2)
```

Consolidation can also run automatically on a size trigger or an off-peak window
(`delta.deltaCorePercent`, `delta.consolidateWindow`); see
[14 Configuration reference](14-configuration-reference.md).

### Consolidation and the ACL stamp

On a graph built with `--acl`, the rebuild is stamped with the ACL the server is
**enforcing** — the one it loaded and is serving, not whatever `acl.json` happens to
contain when the rebuild runs. If the file has changed underneath, the consolidation is
refused rather than blessing it:

```
refusing to consolidate 'g': /etc/slater/acl.json hashes to <b> but the ACL in force
is <a>. The rebuild would be stamped against an acl.json this server never accepted…
```

That is the stamp doing its job. An edit to `acl.json` takes effect when the server adopts
it through the stamp gate ([15 Security](15-security.md)), not because a rebuild happened
to read the file mid-edit. Restore the file, or restart so the new ACL is adopted, then
consolidate.

If a rebuild is refused at swap-in for any reason, the `current` pointer is rolled back to
the generation still serving, so a failed consolidation leaves a bootable graph.

## Next

- Vector writes specifically: [10 Vector search](10-vector-search.md).
- Grants and the write gate: [15 Security](15-security.md).
- Delta/segment/consolidation knobs: [14 Configuration reference](14-configuration-reference.md).
