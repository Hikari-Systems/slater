# 04 · Types & values

This page describes the values a query can hold and return, which of them can be
*stored* as properties, and the numeric, string, and ordering semantics you can
rely on.

## Two tiers: stored values vs runtime values

Slater distinguishes what can live **on disk as a property** from what a query can
**compute and return**. The stored set is deliberately narrow; the runtime set is
wider.

### Storable value types (can be a property)

| Type | Notes |
|---|---|
| `null` | Absence. A missing property also reads as `null`. |
| Boolean | `true` / `false`. |
| Integer | Signed 64-bit (`i64`). |
| Float | IEEE-754 double (`f64`). |
| String | UTF-8. |
| List | Homogeneous list of values (the builder rejects ragged/mixed arrays). |
| **Vector** | A dense `f32` vector, written `vecf32([...])`. A first-class type, **distinct from a list of floats**, routed to the vector index. See [10 Vector search](10-vector-search.md). |

### Runtime-only value types (computed, never stored)

Queries can produce these, but they **cannot be stored** as a property — an
attempt to `SET` one is not representable on disk:

- **Map** — `{k: v, …}`, from map literals and map projections.
- **Node**, **Relationship**, **Path** — graph references and walks.
- **Point** — `point({latitude, longitude})`, WGS-84 (SRID 4326) only.
- **Temporals** — `Date`, `LocalTime`, `LocalDateTime`, `Duration`.

If you need to persist a temporal or a point, store its components (e.g. an epoch
integer, or latitude/longitude floats) instead.

## Integer and float semantics

Integers are `i64`; floats are `f64`. The important guarantee is that **integer
arithmetic never silently wraps or saturates**:

- `+`, `-`, `*`, unary negation, and `sum()` are all checked. On overflow the
  query fails with an `ArithmeticOverflow` error rather than returning a wrong
  number. Division by zero errors too.
- Converting a float to an integer is also checked. `toInteger(x)` on a
  non-finite or out-of-range float is a **hard error**, while
  `toIntegerOrNull(x)` returns `null` for the same input. (A malformed *string*
  is `null` for both spellings — that asymmetry is deliberate.)

```cypher
RETURN toInteger('42')      AS a,   // 42
       toInteger(3.9)       AS b,   // 3   (truncates toward zero)
       toIntegerOrNull('x') AS c    // null
```

`toFloat` / `toFloatOrNull` and `toBoolean` / `toBooleanOrNull` follow the same
pattern. See [08 Functions & expressions](08-functions-and-expressions.md) for
the full conversion list.

## Strings and collation

- Strings are **UTF-8**. Length and indexing count **Unicode scalar values**
  (characters), not bytes: `size('café')` is 4, and `substring`/slicing work by
  character.
- Comparison and `ORDER BY` on strings are **byte-wise on the UTF-8 encoding**
  (equivalent to Unicode code-point order) and are **not locale-aware** — there is
  no collation table. So `'Z' < 'a'` (uppercase sorts before lowercase).
- Function *names* are case-insensitive (`toInteger` = `tointeger`), but string
  *values* compare case-sensitively.

## Ordering across mixed types

When a single `ORDER BY` column mixes types (or when values are compared), Slater
uses a total order with this type ranking:

```
null  <  boolean  <  number  <  string  <  list  <  vector
```

Numbers compare numerically (an integer and a float compare by value); `NaN` sorts
deterministically rather than being unordered.

## Next

- Use these values in queries: [07 Querying](07-querying.md) and
  [08 Functions & expressions](08-functions-and-expressions.md).
- Store them by building a graph: [05 Building graphs](05-building-graphs.md).
