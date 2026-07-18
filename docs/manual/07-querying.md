# 07 · Querying

Slater answers queries in a large, Neo4j-compatible subset of Cypher, extended
with several ISO GQL constructs. This page covers **reading** data: the clauses
that shape a query and the patterns that match the graph. Functions, operators,
and expressions have their own page — [08 Functions & expressions](08-functions-and-expressions.md).

## Read-only by default

A Slater connection is **read-only** unless the writable layer is enabled. Write
clauses — `CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE`, and non-whitelisted
`CALL` — are rejected by the read parser with a clear message:

```
Slater is read-only; the 'CREATE' clause is not permitted
```

Enabling writes is a server-side decision (`delta.enabled`) and a separate topic —
see [11 Writing data](11-writing-data.md). Everything on this page works on a
plain read-only connection.

## Reading clauses

A read query is a sequence of reading clauses ending in `RETURN` (or a bare
`CALL`). All the familiar Cypher clauses are supported:

| Clause | Notes |
|---|---|
| `MATCH` | One or more comma-separated patterns. |
| `OPTIONAL MATCH` | Rows survive with `null`s when the pattern does not match. |
| `WHERE` | Attaches to `MATCH`, `WITH`, comprehensions, and `CALL`. |
| `WITH` | Chains query parts; supports `WITH DISTINCT …` and a trailing `WHERE`. |
| `RETURN` | Supports `RETURN DISTINCT` and `RETURN *`. |
| `UNWIND … AS x` | Expands a list into rows. The GQL spelling `FOR x IN …` is equivalent. |
| `ORDER BY` | Multiple sort keys, each `ASC`/`DESC`. |
| `SKIP` / `LIMIT` | Take an expression. |
| `UNION` / `UNION ALL` | Combine result sets. |
| `CALL { … }` | A correlated read-only subquery. |

A worked example against the sample `social` graph:

```cypher
MATCH (p:Person)
RETURN p.name, p.age
ORDER BY p.age DESC
LIMIT 3
```

```json
{ "columns": ["p.name", "p.age"],
  "rows": [ ["Edsger Dijkstra", 52], ["Katherine Johnson", 48], ["Grace Hopper", 45] ] }
```

Aggregation and grouping are implicit, as in Cypher — non-aggregated return items
form the grouping key:

```cypher
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
RETURN c.name, count(*) AS staff
ORDER BY staff DESC
```

```json
{ "columns": ["c.name", "staff"],
  "rows": [ ["Remington Systems", 3], ["Analytical Engines", 1], ["Bletchley Compute", 1] ] }
```

## Pattern syntax

### Nodes and relationships

- **Node pattern** — `(p:Person {email:'ada@example.com'})`: an optional variable,
  optional labels, and an optional inline property map.
- **Relationship pattern** — `-[r:WORKS_AT {role:'Analyst'}]->`: an optional
  variable, optional types, an optional length range, and optional properties.
- **Direction** — `-->` (left-to-right), `<--` (right-to-left), or undirected
  `--` (either direction).
- **Label / type expressions** — classic `:A:B` (both) and `:T1|T2` (either) are
  supported, as are boolean label expressions (`:A&!B`).

```cypher
MATCH (p:Person {email:'ada@example.com'})-[:KNOWS]->(f)
RETURN f.name
```

### Variable-length paths

`-[:KNOWS*1..3]->` matches a chain of 1 to 3 hops. The bounds may be written
`*`, `*2`, `*1..3`, `*..3`, or `*2..`. Bind the path to a variable to inspect it
with `nodes()`, `relationships()`, and `length()`:

```cypher
MATCH p = (a:Person {email:'ada@example.com'})-[:KNOWS*1..3]->(b)
RETURN b.name, length(p) AS hops
```

```json
{ "columns": ["b.name", "hops"],
  "rows": [ ["Alan Turing", 1], ["Grace Hopper", 2], ["Edsger Dijkstra", 3] ] }
```

### GQL quantified path patterns

Slater accepts ISO GQL quantified paths — a parenthesised sub-pattern with a
quantifier:

```cypher
MATCH ((x:Person)-[:KNOWS]->(y:Person)){1,3}
RETURN y.name
```

**Only bounded quantifiers execute** — `{m,n}` and `{m}`. Unbounded forms (`+`,
`*`, `{m,}`, `{,n}`) parse but are rejected at execution; give an explicit upper
bound.

### Shortest paths

- **Selectors** (GQL) — prefix a pattern with `ANY SHORTEST`, `ALL SHORTEST`, or
  `SHORTEST k` to pick shortest matches.
- **Restrictors** (GQL) — `WALK`, `TRAIL`, `ACYCLIC`, or `SIMPLE` constrain how a
  variable-length pattern may repeat nodes/edges.
- **`shortestPath((a)-[*]->(b))`** — the openCypher spelling is supported in
  `MATCH` position and as an expression. `allShortestPaths(...)` works in `MATCH`
  position only.

```cypher
MATCH p = shortestPath((a:Person {email:'ada@example.com'})-[:KNOWS*]->(b:Person {email:'edsger@example.com'}))
RETURN length(p) AS hops
```

## Type casting

`CAST(expr AS type)` converts a value to a named type. Supported target types
include `integer`/`int`, `float`/`double`/`real`, `string`/`varchar`,
`boolean`/`bool`, and the temporal types `date`, `localtime`, `localdatetime`,
`duration`. See [04 Types & values](04-types-and-values.md) for conversion
semantics.

## Notes and limits

- Identifiers may be backtick-quoted, and there is no reserved-word guard, so
  `MATCH (n:Order) RETURN n.end` is legal.
- A single trailing `;` is tolerated; multi-statement batches are not — send one
  statement per request.
- Only a fixed set of procedures is callable (`db.idx.vector.queryNodes`, the
  `algo.*` algorithms, and the metadata procedures); every other `CALL` is
  rejected. See [09 Procedures & algorithms](09-procedures-and-algorithms.md).

## Reference: clause summary

| Category | Constructs |
|---|---|
| Reading | `MATCH`, `OPTIONAL MATCH`, `WHERE`, `WITH [DISTINCT]`, `UNWIND` / `FOR`, `CALL { }` |
| Projection | `RETURN [DISTINCT]`, `RETURN *`, `ORDER BY`, `SKIP`, `LIMIT` |
| Set ops | `UNION`, `UNION ALL` |
| Patterns | node/rel patterns, `-->`/`<--`/`--`, `*m..n`, GQL `(){m,n}` (bounded), `ANY/ALL SHORTEST`, `WALK/TRAIL/ACYCLIC/SIMPLE`, `shortestPath` |
| Conversion | `CAST(x AS type)` |

## Next

- Functions, operators, and expressions: [08 Functions & expressions](08-functions-and-expressions.md).
- Procedures and graph algorithms: [09 Procedures & algorithms](09-procedures-and-algorithms.md).
- Vector similarity search: [10 Vector search](10-vector-search.md).
