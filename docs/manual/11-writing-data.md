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
  unsupported write form, or a trailing `RETURN` — is rejected with:
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
| `SET n:Label` | Add a (pre-existing) label |
| `SET n.embedding = vecf32([…])` | Write an indexed vector ([10 Vector search](10-vector-search.md)) |

Values must be constants (a literal or a `$parameter`). Re-setting the business-key
property relocates the node in its index.

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

## What you cannot write

These are enforced with clear errors — they keep the served schema stable:

| Attempt | Result |
|---|---|
| Add a label not already in the graph | `cannot add label ':Robot' — it is not defined in the graph (only pre-existing labels can be set)` |
| Write a relationship type not in the graph | rejected — the type must already exist |
| `MERGE`/`CREATE` on a non-range-indexed key | rejected — add a range index at build time |
| `DELETE` a node that still has relationships | rejected — use `DETACH DELETE` |
| `RETURN` after a write | rejected — run a separate `MATCH … RETURN` |

New labels and relationship types come only from a rebuild; the writable layer
works within the schema the base generation already defines.

## Transactions and durability

Each write statement is its own **autocommit** group commit — the fsync is the
acknowledgement barrier, so a returned write is durable. Bolt `BEGIN`/`COMMIT` is
accepted but only opens a **read** transaction; there is no multi-statement write
transaction. Concurrent writes to one graph serialise behind that graph's writer
(bounded by `server.maxConcurrentWrites`, default 4).

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

## Next

- Vector writes specifically: [10 Vector search](10-vector-search.md).
- Grants and the write gate: [15 Security](15-security.md).
- Delta/segment/consolidation knobs: [14 Configuration reference](14-configuration-reference.md).
