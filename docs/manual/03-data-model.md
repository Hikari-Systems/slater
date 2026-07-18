# 03 · Data model

Slater is a **labelled property graph**, the same model Cypher assumes: nodes
carry labels and properties, relationships connect two nodes with a type and
properties. This page covers the shape of the data and — the part that is
Slater-specific — how nodes are *identified*.

## Nodes

A node has:

- **Zero or more labels.** Multiple labels per node are supported. In a dump the
  first label is the node's *identity* label (it locates the node); any further
  labels are extra tags. `labels(n)` returns them all.
- **Properties.** A map of string keys to values. Properties are **schemaless**:
  there is no per-label property schema, each property cell carries its own type,
  and a missing property reads as `null`. The value types a property may hold are
  covered in [04 Types & values](04-types-and-values.md).

```cypher
MATCH (n:Person {email:'ada@example.com'})
RETURN labels(n) AS labels, keys(n) AS keys
```

## Relationships

A relationship has:

- Exactly **one type** (e.g. `WORKS_AT`). Unlike labels, a relationship cannot
  have multiple types.
- A **direction** — it is stored from a source node to a target node. A query may
  traverse it in either direction (`-->`, `<--`, or undirected `--`), but the
  stored direction is preserved: `startNode(r)` / `endNode(r)` always report the
  stored source and target regardless of how you walked it.
- **Properties**, like nodes.

Slater is a **multigraph**: parallel edges (several relationships of the same or
different type between the same two nodes) are supported, and so are **self-loops**
(a relationship from a node to itself).

## Identity: business keys

This is the key Slater-specific idea. Slater does not invent surrogate node ids
for you to reference across a dump; instead, **each node is identified by a
business key you choose** — a label plus one property whose value is unique among
nodes of that label. In the sample graph the keys are `Person.email`,
`Company.name`, and `Product.sku`.

Business keys do real work:

- **In a dump**, `MERGE (p:Person {email:'ada@example.com'})` means "the Person
  whose email is ada@…", so a second statement with the same key updates the same
  node, and an edge statement resolves its endpoints by their keys. This is how a
  streaming dump stays self-contained without surrogate ids.
- **At query time** (with the writable layer on), the business key must be
  **range-indexed** for `MERGE`/`CREATE` to resolve or create the node — which is
  why dumps declare `CREATE INDEX FOR (n:Person) ON (n.email)`. A write against a
  non-indexed key is rejected ([11 Writing data](11-writing-data.md)).

### The `--pk` alternative

Some datasets already have a single, global, integer identifier for every node
(for example, a legacy FalkorDB `GRAPH.DUMP`). For those, `slater-build --pk
<field>` uses that one field as a label-agnostic identity across the whole dump,
stored as an ordinary queryable property. `--pk __dump_id__` ingests FalkorDB
dumps directly. This is the format the sample [`products-vec.cypher`](examples/)
uses. See [05 Building graphs](05-building-graphs.md).

## Internal ids and the `id()` function

Within a generation, every node and relationship also has a **dense internal id**
(`0, 1, 2, …`), assigned by the builder. `id(n)` and `id(r)` expose it:

```cypher
MATCH (p:Person {email:'ada@example.com'}) RETURN id(p)
```

Two things to know about internal ids:

- They are **stable only within one generation**. A rebuild — or a
  `CALL slater.consolidate()` — produces a *new* generation with fresh ids, so a
  node's `id()` can change. **Do not persist or reference internal ids across
  builds; use your business key for durable identity.**
- They are the reason the on-disk format is compact and the vector index can be
  carried by reference across a consolidation — neighbours are addressed by
  layout position, not by a stored id.

## Round-tripping

`slater dump` re-exports a served graph as business-key `MERGE` Cypher, so a graph
can round-trip `dump → slater-build → new generation`. The business key survives
the round trip; internal ids do not.

## Next

- The value and type system: [04 Types & values](04-types-and-values.md).
- Turning a dump into a graph: [05 Building graphs](05-building-graphs.md).
