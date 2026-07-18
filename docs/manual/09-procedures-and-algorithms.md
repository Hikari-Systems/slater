# 09 · Procedures & algorithms

Slater exposes a fixed, whitelisted set of procedures you invoke with `CALL`:
vector search, six graph algorithms, schema introspection, and two `slater.*`
admin procedures. There is no user-defined-procedure mechanism — the list on this
page is the whole surface, and any `CALL` outside it is rejected as read-only.

## Two places procedures run

This distinction matters in practice, so it comes first.

- **Engine procedures** run inside the query engine. They participate in a larger
  query, support `YIELD … WHERE`, and work both over a Bolt connection *and*
  through the in-process `slater query` tool. These are `db.idx.vector.queryNodes`,
  the `algo.*` family, `db.meta.stats`, `db.constraints`, `dbms.procedures`, and
  `dbms.functions`.
- **Server interceptions** are answered by the Bolt server *before* the query
  parser runs, straight from the resident manifest. They only work **over a live
  Bolt connection** — not through `slater query`, whose parser rejects them.
  These are `db.labels`, `db.relationshipTypes`, `db.propertyKeys`, the
  `SHOW INDEXES` / `db.indexes` family, and the server-info commands
  (`SHOW DATABASES`, `SHOW VERSION`, `dbms.components`, and so on).

The practical consequence:

```sh
# Over Bolt: works
CALL db.labels()            # => [{label: 'Person'}, {label: 'Company'}, {label: 'Product'}]

# Through `slater query`: rejected, because db.labels() is a server interception
slater query social 'CALL db.labels()'
# => parse query: Slater is read-only; the 'CALL' clause is not permitted
```

`db.meta.stats` gives you the same counts as an engine procedure, so it works in
both contexts.

## Vector search

```cypher
CALL db.idx.vector.queryNodes('Product', 'embedding', 3, vecf32([0.9, 0.8, 0.1, 0.05]))
YIELD node, score
RETURN node.sku, node.title, score
```

```json
{"columns": ["node.sku", "node.title", "score"],
 "rows": [["CMP-1", "A-0 Compiler", -4.06e-8],
          ["NAV-1", "Orbital Calculator", 0.00224],
          ["BMB-1", "Bombe", 0.524]]}
```

The four arguments are `(label, property, k, queryVector)`; `YIELD node, score` is
mandatory and `score` is the metric distance in **ascending** order (nearest
first). This is the one procedure with its own dedicated `CALL … YIELD` shape, and
it needs a pre-built vector index or it errors. The full treatment — metrics,
index creation, tuning — is in [10 Vector search](10-vector-search.md).

## Graph algorithms (`algo.*`)

Six algorithms, all read-only engine procedures. Each returns one row per node
(except `BFS`, which returns a single row describing the traversal).

```cypher
CALL algo.pageRank('Person', 'KNOWS')
YIELD node, score
RETURN node.name, score ORDER BY score DESC LIMIT 3
```

```json
{"columns": ["node.name", "score"],
 "rows": [["Katherine Johnson", 0.301],
          ["Edsger Dijkstra", 0.259],
          ["Grace Hopper", 0.209]]}
```

| Procedure | Arguments | `YIELD` columns |
|---|---|---|
| `algo.BFS` | `(source: Node, maxLevel: Int, relationshipType: String\|null)` — `maxLevel ≤ 0` = unlimited | `nodes`, `edges` (lists; one row) |
| `algo.WCC` | `([config])` — optional `{nodeLabels, relationshipTypes}` | `node`, `componentId` |
| `algo.pageRank` | `(label: String\|null, relationshipType: String\|null)` | `node`, `score` |
| `algo.HarmonicCentrality` | `([config])` | `node`, `score`, `reachable` |
| `algo.betweenness` | `([config])` — exact Brandes; `samplingSize`/`samplingSeed` accepted but ignored | `node`, `score` |
| `algo.labelPropagation` | `([config])` — optional `maxIterations` (default 10) | `node`, `communityId` |

`componentId` and `communityId` are the smallest internal node id in the group, so
they are stable labels rather than sequential counters. In a config map, an unknown
*key* is an error, but an unknown label/relationship-type *name* is silently
dropped.

There is no `shortestPath` procedure — shortest paths are a query-language
construct instead ([07 Querying](07-querying.md)).

## Introspection

### Engine: `db.meta.stats`

Whole-graph counts, served from the manifest with no scan:

```cypher
CALL db.meta.stats() YIELD nodeCount, relCount, labelCount
RETURN nodeCount, relCount, labelCount
```

```json
{"columns": ["nodeCount", "relCount", "labelCount"], "rows": [[12, 17, 3]]}
```

It also yields `labels` and `relTypes` (name→count maps), `relTypeCount`, and
`propertyKeyCount`.

### Server interceptions (Bolt only)

| Statement | Columns |
|---|---|
| `CALL db.labels()` | `label` |
| `CALL db.relationshipTypes()` | `relationshipType` |
| `CALL db.propertyKeys()` | `propertyKey` |
| `SHOW INDEXES` | `id`, `name`, `state`, `type`, `entityType`, `labelsOrTypes`, `properties`, `indexProvider`, … |
| `CALL db.indexes()` | Neo4j-4.x shape of the same |
| `SHOW DATABASES` / `SHOW VERSION` / `dbms.components()` | server/database metadata |

```json
// SHOW INDEXES on the social graph
[{"name": "node_Person_email", "type": "RANGE", "entityType": "NODE",
  "labelsOrTypes": ["Person"], "properties": ["email"], "indexProvider": "range-1.0"},
 …]
```

Range indexes report provider `range-1.0`; vector indexes appear as
`vector_<label>_<property>` with provider `vector-2.0`. Slater enforces no
uniqueness/existence constraints, so `SHOW CONSTRAINTS` and `db.constraints()` are
always empty.

The compatibility surface (which client expects which `SHOW` spelling, including
the Memgraph variants like `SHOW STORAGE INFO`) is detailed in
[17 Client compatibility](17-client-compatibility.md).

## `slater.*` procedures

### `slater.consolidate()`

Folds the writable delta into a fresh base generation and returns its UUID:

```cypher
CALL slater.consolidate()
```

```json
{"columns": ["generation"], "rows": [["e448f67b-6bb2-4b23-a66a-1b3ee3a23935"]]}
```

It is an admin operation available only when the writable layer is enabled, and it
spawns the `slater-build` binary — so `delta.builderBin` must resolve to it. See
[11 Writing data](11-writing-data.md).

### `slater.diagnostics()`

A load-test health snapshot (also spelled `SHOW SERVER DIAGNOSTICS`), yielding
`metric`, `value` rows — uptime, RSS, connection occupancy, query counters, and a
latency histogram. It is **gated**: it errors unless `loadTestDiagnostics=true`,
which also prints a startup warning not to enable it on a production replica. See
[13 Deployment](13-deployment.md) and [16 Performance tuning](16-performance-tuning.md).

## Not supported

- **Full-text index procedures** (`db.idx.fulltext.*`) — not implemented; a `CALL`
  is rejected.
- **User-defined / APOC procedures** — there is no plugin mechanism.

## Next

- Search embeddings in depth: [10 Vector search](10-vector-search.md).
- The query language the algorithms complement: [07 Querying](07-querying.md).
- Client-specific `SHOW` commands: [17 Client compatibility](17-client-compatibility.md).
