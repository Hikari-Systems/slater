# Slater manual

Slater is a Bolt-speaking graph engine that **serves compiled, read-only graph
generations that can be larger than RAM**, with an **optional writable layer**
for live inserts, updates, and deletes. You build a graph offline into an
immutable on-disk image with `slater-build`, then serve it with `slater` and
query it over the Bolt protocol using any Neo4j-compatible driver.

This manual is the how-to reference for **every** user-facing feature: the query
language, the build toolchain, vector search, the writable layer, storage,
deployment, configuration, security, and tuning. Each page follows the same
shape — **what the feature is and why it exists**, then **how to use it** with
worked examples, then a **reference** table of the exact syntax, flags, or knobs.

> The internal engineering design documents live in `docs/` alongside this
> folder (`docs/PLAN.md`, `docs/WRITABLE-PLAN.md`, the perf reports, and so on).
> Those describe *how Slater is built*; this manual describes *how to use it*.

## Reading paths

**If you operate Slater** (run and configure the server) →
[01 Introduction](01-introduction.md) →
[13 Deployment](13-deployment.md) →
[12 Storage](12-storage.md) →
[14 Configuration reference](14-configuration-reference.md) →
[15 Security](15-security.md) →
[16 Performance tuning](16-performance-tuning.md).

**If you write queries** (connect and read/write data) →
[02 Quickstart](02-quickstart.md) →
[03 Data model](03-data-model.md) →
[04 Types & values](04-types-and-values.md) →
[07 Querying](07-querying.md) →
[08 Functions & expressions](08-functions-and-expressions.md) →
[09 Procedures & algorithms](09-procedures-and-algorithms.md) →
[10 Vector search](10-vector-search.md) →
[11 Writing data](11-writing-data.md).

**If you build graphs** (compile data into a served image) →
[05 Building graphs](05-building-graphs.md) →
[06 Build CLI reference](06-build-cli-reference.md) →
[03 Data model](03-data-model.md) →
[12 Storage](12-storage.md).

## Contents

### Concepts
- [01 Introduction](01-introduction.md) — what Slater is, the build→serve
  lifecycle, and the architecture at a glance.
- [02 Quickstart](02-quickstart.md) — build a small graph, serve it, run your
  first query, end to end.
- [03 Data model](03-data-model.md) — nodes, labels, relationships, multigraph,
  and how nodes are identified.
- [04 Types & values](04-types-and-values.md) — the value/type system and its
  numeric, string, and collation semantics.

### Building graphs
- [05 Building graphs](05-building-graphs.md) — the `slater-build` toolchain and
  its input dump formats.
- [06 Build CLI reference](06-build-cli-reference.md) — every `slater-build`
  flag and environment variable.

### Querying
- [07 Querying](07-querying.md) — the Cypher/GQL read query language: clauses and
  patterns.
- [08 Functions & expressions](08-functions-and-expressions.md) — every scalar
  and aggregate function, operator, and predicate.
- [09 Procedures & algorithms](09-procedures-and-algorithms.md) — the `CALL`
  surface, graph algorithms, and introspection.

### Vectors & writes
- [10 Vector search](10-vector-search.md) — embeddings, distance metrics, KNN,
  and the vector write ladder.
- [11 Writing data](11-writing-data.md) — the optional writable layer:
  inserts, updates, deletes, and consolidation.

### Storage & operations
- [12 Storage](12-storage.md) — storage backends (filesystem / S3 / GCS) and the
  on-disk engine, with the choices you make about each.
- [13 Deployment](13-deployment.md) — running the server, Docker images, TLS,
  hot-reload, and observability.
- [14 Configuration reference](14-configuration-reference.md) — every
  configuration knob, its environment-variable form, and its default.
- [15 Security](15-security.md) — authentication, access control, encryption,
  and resource limits.
- [16 Performance tuning](16-performance-tuning.md) — fast paths, memory
  bounding, parallelism, and cache sizing.

### Reference
- [17 Client compatibility](17-client-compatibility.md) — Bolt versions, tested
  drivers, and the introspection commands clients expect.
- [18 Troubleshooting](18-troubleshooting.md) — common error messages and what
  they mean.

## Example dataset

Every worked example in this manual runs against a small, self-contained sample
graph in [`examples/`](examples/). See [`examples/README.md`](examples/README.md)
for the data and the exact build commands. You can copy any example query
straight into a session against the sample graph and get the output shown.
