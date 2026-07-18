# 08 · Functions & expressions

This page catalogues the scalar and aggregate functions, the operators and
predicates, and the expression forms available in a query. Function names are
**case-insensitive** (`toInteger` and `tointeger` are the same). For the clauses
that host these expressions, see [07 Querying](07-querying.md); for the value
types they operate on, see [04 Types & values](04-types-and-values.md).

## Scalar functions

### String

`toLower` / `lower`, `toUpper` / `upper`, `trim`, `ltrim`, `rtrim`, `reverse`,
`left(s, n)`, `right(s, n)`, `substring(s, start [, len])`, `split(s, sep)`,
`replace(s, from, to)`, `string.join(list, sep)`, `string.matchRegex(s, re)`,
`string.replaceRegex(s, re, repl)`.

### Numeric

`abs`, `ceil`, `floor`, `round`, `sign`, `sqrt`, `exp`, `log`, `log10`, `pow(x, y)`,
`e()`, `pi()`, and the trigonometric family `sin`, `cos`, `tan`, `cot`, `asin`,
`acos`, `atan`, `atan2`, `degrees`, `radians`, `haversin`.

### List and map

`size` / `length`, `head`, `last`, `tail`, `range(start, end [, step])`,
`reverse`, `keys(map|node|rel)`, `properties(node|rel)`. List helpers:
`list.dedup`, `list.sort`, `list.remove`, `list.insert`, `list.insertListElements`.

### Conversion

`toString` / `toStringOrNull`, `toInteger` / `toIntegerOrNull`, `toFloat` /
`toFloatOrNull`, `toBoolean` / `toBooleanOrNull`, and the list forms
`toStringList`, `toIntegerList`, `toFloatList`, `toBooleanList`. The `…OrNull`
variants return `null` instead of erroring on out-of-range/non-finite numeric
input; see [04 Types & values](04-types-and-values.md) for the exact semantics.

### Type and introspection

`typeOf(x)`, `isEmpty(x)`, `exists(x)`, `coalesce(a, b, …)`.

### Node, relationship, and path

`id(x)`, `labels(node)`, `type(rel)`, `startNode(rel)`, `endNode(rel)`,
`hasLabels(node, labels)`, `indegree(node)`, `outdegree(node)`,
`nodes(path)`, `relationships(path)`.

### Spatial

`point({latitude, longitude})` and `distance(p1, p2)`. Points are WGS-84
(SRID 4326) and are runtime-only — they cannot be stored as a property.

### Temporal

`date(…)`, `localtime(…)`, `localdatetime(…)`, `duration(…)`. Temporals carry
whole-second precision and are runtime-only.

### Vector

`vecf32(list)` builds a vector; `similarity(a, b)` / `vec.cosineSimilarity(a, b)`,
`vec.cosineDistance(a, b)`, and `vec.euclideanDistance(a, b)` compare two vectors.
See [10 Vector search](10-vector-search.md).

### Non-deterministic

`rand()`, `randomUUID()`, `timestamp()`. (These are query-only; they are not
allowed in build-time expressions — see [05 Building graphs](05-building-graphs.md).)

## Aggregate functions

`count`, `sum`, `avg`, `min`, `max`, `collect`, `stdev`, `stdevp`,
`percentileCont(x, p)`, `percentileDisc(x, p)`. `count(*)` counts rows;
`count(DISTINCT expr)` (and `DISTINCT` on any aggregate) deduplicates first.
Percentiles take a value and a percentile in `[0, 1]`. Aggregates may not be
nested inside one another.

```cypher
MATCH (p:Person)
RETURN count(*) AS people, avg(p.age) AS mean_age, collect(p.name) AS names
```

## Operators and predicates

| Group | Operators |
|---|---|
| Boolean | `AND`, `OR`, `XOR`, `NOT` |
| Comparison | `=`, `<>`, `<`, `<=`, `>`, `>=`, `=~` (regex match) |
| String | `STARTS WITH`, `ENDS WITH`, `CONTAINS` |
| List membership | `IN` |
| Null test | `IS NULL`, `IS NOT NULL` |
| Arithmetic | `+`, `-`, `*`, `/`, `%`, `^` (exponent; always yields a float) |
| String / list | `+` concatenates strings and appends to lists |
| Access | `list[i]`, `list[i..j]`, `map.key`, `n:Label` (label predicate) |

```cypher
MATCH (p:Person)
WHERE p.name STARTS WITH 'A' AND p.age IN [36, 41]
RETURN p.name
```

## Expression forms

- **`CASE`** — simple (`CASE x WHEN 1 THEN … END`) and searched
  (`CASE WHEN x > 0 THEN … ELSE … END`).
- **List comprehension** — `[x IN list WHERE pred | expr]`.
- **Pattern comprehension** — `[(a)-[:R]->(b) WHERE pred | expr]`.
- **`reduce`** — `reduce(acc = 0, x IN list | acc + x)`.
- **Quantified list predicates** — `any`, `all`, `none`, `single`
  (`any(x IN list WHERE pred)`).
- **Existential subquery** — `EXISTS { (a)-[:R]->(b) [WHERE …] }`.
- **Map projection** — `n{.name, .age, alias: expr, .*}`.
- **Parameters** — `$name` (and positional `$0`, `$1`), supplied by the driver.

```cypher
MATCH (p:Person)
RETURN p.name,
       [s IN p.skills WHERE size(s) > 4 | toUpper(s)] AS long_skills,
       CASE WHEN p.active THEN 'active' ELSE 'inactive' END AS status
```

## Next

- The clauses that host these expressions: [07 Querying](07-querying.md).
- Procedures and algorithms: [09 Procedures & algorithms](09-procedures-and-algorithms.md).
