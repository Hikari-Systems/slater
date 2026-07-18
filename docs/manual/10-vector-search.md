# 10 · Vector search

Slater indexes dense `f32` embeddings and answers **k-nearest-neighbour (KNN)**
queries over them, alongside — and joinable with — the rest of your graph. You
supply the vectors; Slater does not compute embeddings.

## How it works

A vector index is chosen per index at build time between two execution paths, by
the `--ann-threshold` (default **50,000** vectors):

- **Below the threshold — brute force.** The full `f32` vectors are kept and each
  query computes the exact distance to every candidate. Simple and exact; ideal
  for small vector sets. The sample `products` graph (four vectors) uses this
  path.
- **At or above the threshold — Vamana + PQ**, the disk-native approximate path.
  A [Vamana](https://arxiv.org/pdf/2401.11324) proximity graph is walked by a
  greedy beam search (few random block reads per query), and
  **product-quantised (PQ)** codes — small enough to stay resident — score
  candidates from RAM, reading only the chosen few full vectors from disk. This
  keeps resident memory bounded regardless of how many vectors there are.

## Distance metrics

An index is built with one metric. The accepted keyword synonyms are:

| Metric | Keywords (case-insensitive) |
|---|---|
| Cosine | `cosine` |
| Euclidean (L2) | `euclidean`, `l2` |
| Dot / inner product (MIPS) | `ip`, `dot`, `dotproduct`, `inner_product` |

The default when unspecified is `cosine`.

> **Use cosine unless you have a specific reason not to.** Cosine is the metric the
> engine is validated and tuned against (measured recall@10 ≈ 0.91–0.99). L2 and
> dot are supported but far less exercised — the performance report measures dot
> recall@10 at roughly 0.32–0.50 on the same data. If you need L2 or dot, validate
> recall on your own vectors before relying on it.

The KNN `score` is the metric **distance**, returned in ascending order (nearest
first). For cosine it is `1 − cosineSimilarity`.

## Creating a vector index (build time)

Vector indexes are declared **when you build the graph**, not at query time.
There are three equivalent ways to declare one; all produce the same index.

```cypher
-- In a dump: the CALL form (label, property, dim, metric)
CALL db.idx.vector.createNodeIndex('Product', 'embedding', 4, 'cosine');

-- In a dump: the SDK-style helper (label, dim, metric, property) — note the order
createNodeVectorIndex('Product', 4, 'cosine', 'embedding');
```

```json
// Or a JSON sidecar passed as --vector-index-json vectors.json
[{"label": "Product", "property": "embedding", "dim": 4, "metric": "cosine"}]
```

The dimension is fixed at build time; a later write with a different-length vector
is rejected (see below). Vector indexes are node-only.

> **Correctness note.** Business-key `MERGE` dumps **cannot carry vector values** —
> `SET p.embedding = vecf32([…])` in a MERGE dump is rejected. Vectors enter a
> graph one of two ways: through the **CREATE / `--pk`** build form (as in the
> sample [`products-vec.cypher`](examples/products-vec.cypher)), or through the
> **writable layer** at serve time (below). See
> [05 Building graphs](05-building-graphs.md).

## Querying: `db.idx.vector.queryNodes`

```cypher
CALL db.idx.vector.queryNodes('Product', 'embedding', 3, vecf32([0.9, 0.8, 0.1, 0.05]))
YIELD node, score
RETURN node.sku, node.title, score
```

```json
{"columns": ["node.sku", "node.title", "score"],
 "rows": [["CMP-1", "A-0 Compiler",       -4.06e-8],
          ["NAV-1", "Orbital Calculator",  0.00224],
          ["BMB-1", "Bombe",               0.524]]}
```

The arguments are `(label, property, k, queryVector)`. The query vector is a
`vecf32([…])` literal or a `$parameter` (drivers with no vector type may send a
plain list of numbers, coerced against the index). `YIELD node, score` is
required; you can post-filter with `YIELD … WHERE …` and continue the query — for
example, join the matched nodes back into the graph:

```cypher
CALL db.idx.vector.queryNodes('Product', 'embedding', 5, vecf32([0.9,0.8,0.1,0.05]))
YIELD node, score
MATCH (c:Company)-[:MAKES]->(node)
RETURN c.name, node.title, score ORDER BY score
```

## Scalar vector functions

For ad-hoc distances outside the index, these expression functions are available:

| Function | Returns |
|---|---|
| `vecf32(list)` | Build a vector value from a list of numbers |
| `similarity(a, b)` / `vec.cosineSimilarity(a, b)` | Cosine similarity in [−1, 1] |
| `vec.cosineDistance(a, b)` | `1 − cosineSimilarity` |
| `vec.euclideanDistance(a, b)` | L2 distance |

## Writing embeddings — the write ladder

With the writable layer on, an indexed embedding is a first-class writable value:

```cypher
MERGE (p:Product {sku:'CMP-1'}) SET p.embedding = vecf32([0.91, 0.79, 0.11, 0.06]);
MATCH (p:Product {sku:'CMP-1'}) REMOVE p.embedding;   -- drop it from the index
```

- A `SET` lands in the write delta and is **immediately visible to KNN with exact
  rank**, then survives segment flushes and merges.
- A delete leaves a *hole*: the node stops being returned but stays a navigational
  waypoint until a background delete-consolidation splices it out, so deletes do
  not cost query IO.
- Because the on-disk graph addresses neighbours by layout position rather than by
  id, `CALL slater.consolidate()` carries the Vamana graph **by reference** —
  folding vector writes into the base **without** rebuilding the graph.

A write whose vector length differs from the index dimension is rejected, and
non-finite components (`NaN`, `±inf`) are never stored. The full write surface is
in [11 Writing data](11-writing-data.md).

## Tuning

Query-time (`vectorQuery.*`, [14 Configuration reference](14-configuration-reference.md)):

| Knob | Default | Effect |
|---|---|---|
| `vectorQuery.beamWidth` | 64 | Beam-search list size; higher = better recall, more work |
| `vectorQuery.tempBeamWidth` | 128 | Beam width for per-segment temp indexes |
| `vectorQuery.rwIndex.enabled` | true | Live mutable index over the write delta; off = brute-force the delta |
| `vectorQuery.rwIndex.maxVectors` | 50000 | Delta row cap before the delta arm brute-forces |

Build-time (`slater-build`, [06 Build CLI reference](06-build-cli-reference.md)):
`--ann-threshold`, `--vamana-r`, `--vamana-alpha`, `--pq-subspaces`, `--pq-bits`.
The resident PQ pool is sized by `cache.vectorCacheBytes`.

## Next

- Mutate embeddings live: [11 Writing data](11-writing-data.md).
- Declare indexes during a build: [05 Building graphs](05-building-graphs.md).
- Size the vector cache and beam: [16 Performance tuning](16-performance-tuning.md).
