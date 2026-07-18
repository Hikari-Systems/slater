# 05 · Building graphs

A served graph starts as a **dump** — a text (or binary) description of nodes,
edges, and indexes — which `slater-build` compiles into an immutable on-disk
generation. This page covers the dump formats, build-time expression evaluation,
declaring vector indexes, and the operational features (resume, diagnostics,
remote publishing). For the exhaustive flag list, see
[06 Build CLI reference](06-build-cli-reference.md).

## The shape of a build

Every build reads one input and writes one generation:

```sh
slater-build --input docs/manual/examples/social.cypher \
             --graph social --data-dir /tmp/slater-data
```

- `--input` — the dump script (or `-` to read from stdin).
- `--graph` — the logical graph name; the image is written under
  `<data-dir>/<graph>/<generation-uuid>/`.
- `--data-dir` — the root data directory.

On success it prints the generation UUID, the node/edge counts, and a content
hash:

```
built graph 'social' generation 7c6560e5-… (12 nodes, 17 edges)
content-hash 6a1f6cd0…
dir /tmp/slater-data/social/7c6560e5-…
```

Each build produces a **fresh, immutable generation**. There is no in-place
incremental build against a prior generation — incremental change is expressed
through the writable layer and consolidation ([11 Writing data](11-writing-data.md))
or through overlay dumps (below). Dump files take **no comments** (`//` or `/* */`
are rejected); a dump is bare statements separated by `;`.

## Input dump formats

### Business-key MERGE (the default)

The default import is a stream of `MERGE` statements where each node carries an
inline **business key** that identifies it (see
[03 Data model](03-data-model.md)). A node MERGE optionally sets more properties;
an edge MERGE resolves both endpoints by their keys:

```cypher
CREATE INDEX FOR (n:Person) ON (n.email);
CREATE INDEX FOR (n:Company) ON (n.name);

MERGE (p:Person {email: 'ada@example.com'}) SET p.name = 'Ada Lovelace', p.age = 36;
MERGE (c:Company {name: 'Analytical Engines'}) SET c.founded = 1843;
MERGE (a:Person {email: 'ada@example.com'})-[r:WORKS_AT]->(b:Company {name: 'Analytical Engines'}) SET r.role = 'Analyst';
```

Repeated MERGEs of the same identity collapse into one node, folding their `SET`
assignments together. A node's identity label may be followed by extra labels
(`MERGE (n:Person:Employee {email: …})`). The dump must be self-contained: an edge
can only reference nodes the dump also defines.

> **Vectors are not allowed in a MERGE dump.** A `SET n.embedding = vecf32([...])`
> inside a business-key MERGE dump is rejected. Vectors enter a graph through the
> CREATE / `--pk` build form (below) or through the writable layer at serve time
> ([10 Vector search](10-vector-search.md)).

### Single-global-key (`--pk <field>`)

When every node already has one global, integer identifier, `--pk <field>` uses
that field as the node identity across the whole dump. Nodes are `CREATE`d and
edges reference endpoints by the id field. This form **does** carry `vecf32`
values, so it is how you build a graph with embeddings offline:

```cypher
CREATE INDEX FOR (n:Product) ON (n.sku);
CALL db.idx.vector.createNodeIndex('Product', 'embedding', 4, 'cosine');

CREATE (:Product {__dump_id__: 0, sku: 'ENG-1', title: 'Difference Engine', price: 4500.0, embedding: vecf32([0.10, 0.20, 0.30, 0.40])});
CREATE (:Product {__dump_id__: 1, sku: 'BMB-1', title: 'Bombe',             price: 9900.0, embedding: vecf32([0.20, 0.10, 0.40, 0.30])});
```

```sh
slater-build --input docs/manual/examples/products-vec.cypher \
             --graph products --data-dir /tmp/slater-data --pk __dump_id__
```

`--pk __dump_id__` ingests a legacy FalkorDB `GRAPH.DUMP` directly (its
`CREATE (:…:__DumpVertex__ {__dump_id__: n, …})` node lines and
`MATCH … CREATE (a)-[:REL]->(b)` edge lines). The id field is stored as an ordinary
queryable property.

### Binary consolidation dump (`--input-format slater-dump`)

`--input-format slater-dump` ingests a binary dump directory that already carries
dense ids and global symbols — the server produces these during a direct
consolidation. It skips parsing, dedup, and endpoint resolution, entering the
pipeline at the clustering stage. You will not usually author this by hand; it is
the machinery behind `CALL slater.consolidate()`.

### Overlay / patch dumps

A single dump may mix creation statements with **overwrite** statements
(`MERGE|MATCH (n:L {k: v}) SET n.a = …`) that mutate nodes or edges created
*earlier in the same build*, matched by label + property key. This is an in-run
patch pass — useful for layering a set of property updates onto a base dump in one
build. `SET` is last-writer-wins per key. An overlay edge patch that matches no
existing edge is an error (overlay does not create edges on absence).

## Build-time expression evaluation

A node `SET` right-hand side is not limited to a literal — it can be a **pure
scalar expression** evaluated against the node's accumulated properties. This lets
a dump compute derived values and build accumulators as it folds repeated MERGEs.
Supported forms:

- **Pure functions** — an allowlist mirroring the query engine's deterministic
  scalar functions: `coalesce`; string functions (`toLower`/`upper`, `trim`,
  `left`, `right`, `substring`, `split`, `replace`, `string.join`, `reverse`);
  size/list helpers (`size`/`length`, `head`, `last`, `tail`, `isEmpty`,
  `list.dedup`, `list.sort`, `list.remove`, `list.insert`); conversions
  (`toString`, `toInteger`, `toFloat`, `toBoolean`, and their `…OrNull` and list
  variants); and numeric functions (`abs`, `ceil`, `floor`, `round`, `sqrt`,
  `log`, `exp`, `pow`, `sign`, trig, `degrees`/`radians`, …).
- **Infix operators** — `+ - * / %` (with checked integer arithmetic, and `+`
  concatenating strings or extending lists) and comparisons `= <> < <= > >=`,
  combined with `AND` / `OR` / `NOT`.
- **`CASE`** — searched (`CASE WHEN … THEN … ELSE … END`) or simple
  (`CASE x WHEN … END`).

A common pattern is a semicolon-joined accumulator that grows across repeated
MERGEs of the same node:

```cypher
MERGE (p:Person {email:'ada@example.com'})
  SET p.tags = CASE WHEN coalesce(p.tags,'') = '' THEN 'math'
                    ELSE p.tags + '; ' + 'math' END;
```

Impure or non-`Value` functions (`rand`, `timestamp`, `id`, `labels`, `point`,
`vecf32`, temporals) are **rejected** at build time — they live only in the query
engine. On an **edge** `SET`, and in overlay-patch dumps, only literal values are
allowed. The evaluator and its allowlist are shared with the query engine via the
`slater-scalar` crate, so build-time and query-time semantics agree.

## Declaring vector indexes

A vector index is declared at build time (there is no runtime "create index"
procedure). Three equivalent forms:

```cypher
-- Cypher CALL form: (label, property, dimension, metric)
CALL db.idx.vector.createNodeIndex('Product', 'embedding', 4, 'cosine');

-- SDK-style helper: (label, dimension, metric, property)  — note the argument order
createNodeVectorIndex('Product', 4, 'cosine', 'embedding');
```

Or as a JSON sidecar passed with `--vector-index-json vecs.json`:

```json
[{ "label": "Product", "property": "embedding", "dim": 4, "metric": "cosine" }]
```

The metric keyword accepts synonyms: `cosine`; `euclidean` / `l2`; and `ip` /
`dot` / `dotproduct` / `inner_product`. Indexes at or above `--ann-threshold`
(default 50 000 vectors) are built as a disk-native Vamana + PQ graph tuned by
`--vamana-r`, `--vamana-alpha`, `--pq-subspaces`, `--pq-bits`; smaller ones stay
exact brute-force. See [10 Vector search](10-vector-search.md).

## Resume and diagnostics

- **`--resume`** — an interrupted build can be resumed. The builder checkpoints a
  `BUILD-STATE.json` in its scratch directory after each phase (`pass1`, `dedup`,
  `resolve`, `cluster`), and on resume skips the phases that already completed. The
  build is deterministic, so regenerated artifacts are byte-identical. (A build
  reading from a stdin *pipe* cannot resume a mid-pass-1 interruption, since a pipe
  cannot be re-read.)
- **`--diagnostics`** (or `SLATER_BUILD_DIAG=1`) — writes a per-sample JSONL trace
  of RSS, CPU, IO, and PSI stall counters with per-phase summaries, for
  diagnosing a slow build. `--diagnostics-log` sets the path;
  `--diagnostics-interval-ms` the sampling cadence.

## Publishing to object storage

With the `s3` or `gcs` cargo feature, a build can publish the finished generation
straight to an object store instead of (or as well as) the local disk:

```sh
slater-build --input dump.cypher --graph g --data-dir /data \
  --publish-s3-bucket my-bucket --publish-s3-region eu-west-2 --publish-s3-prefix graphs/
```

Publishing auto-selects the `remote` compression profile (higher zstd level) and
records per-file object checksums. The GCS equivalents are `--publish-gcs-bucket`
and friends. See [12 Storage](12-storage.md) for how the server then reads it.

## Reference: a representative build command

```sh
cat dump.cypher | slater-build \
  --input - --graph g --data-dir /data \
  --vector-index-json vecs.json --ann-threshold 50000 \
  --threads 8 --max-memory 8g --diagnostics
```

Every flag is catalogued in [06 Build CLI reference](06-build-cli-reference.md).

## Next

- The full flag and environment reference: [06 Build CLI reference](06-build-cli-reference.md).
- Where the built bytes live and how they are encoded: [12 Storage](12-storage.md).
- Mutating a graph without a rebuild: [11 Writing data](11-writing-data.md).
