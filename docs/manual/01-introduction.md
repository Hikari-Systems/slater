# 01 · Introduction

## What Slater is

Slater is a **graph database that serves compiled, read-only graph images which
can be much larger than RAM**, spoken to over the **Bolt protocol** — the same
wire protocol as Neo4j — so you can connect with any Neo4j-compatible driver or
tool. On top of that read-only core sits an **optional writable layer** for live
inserts, updates, and deletes.

The design splits cleanly into two programs:

- **`slater-build`** — an *offline* compiler. It reads a dump script (or a binary
  dump) and produces an immutable, content-hashed, generation-numbered on-disk
  image: the blocks, indexes, and manifest that make up one graph *generation*.
  See [05 Building graphs](05-building-graphs.md).
- **`slater`** — the *server*. It memory-maps a generation and answers Bolt
  queries against it, paging blocks in on demand so resident memory stays bounded
  no matter how large the graph is. See [13 Deployment](13-deployment.md).

This separation is the whole point: the expensive work of laying out a graph for
locality, building indexes, and compressing blocks happens once, offline; serving
is then cheap, parallel, and memory-bounded.

## Why it exists

Most graph databases keep the whole working set in RAM, so the graph you can
serve is capped by the RAM you can afford. Slater instead compiles a graph into a
disk-native format that is *read* efficiently a block at a time, which lets a
modest server answer queries over a graph with billions of edges. The tradeoffs
that buys — an immutable base image, an offline build step — are made explicit
rather than hidden.

Read-only is the **default and the fast path**. When you do need to mutate a live
graph, you turn on the writable layer ([11 Writing data](11-writing-data.md)),
which absorbs writes into a delta and folds them back into the base without a full
rebuild.

## The build → serve lifecycle

```
   dump script            slater-build            slater server         Bolt client
  (MERGE / CREATE)   ──▶  compile offline   ──▶   serve generation  ◀──  driver / query
                          (immutable image)       (read, + optional
                                                    writable layer)
```

1. **Author a dump** describing the graph — business-key `MERGE` statements are
   the default form ([05 Building graphs](05-building-graphs.md)).
2. **Compile** it with `slater-build` into `<data-dir>/<graph>/<generation>/`.
3. **Serve** it with `slater`, which publishes and reloads generations as they
   appear.
4. **Query** it over Bolt. If the writable layer is on, writes land in a delta and
   are visible immediately; `CALL slater.consolidate()` folds them into a new
   generation.

## Architecture at a glance

A few concepts recur throughout the manual:

- **Generation** — one immutable, content-hashed on-disk image of a graph:
  `<graph>/<generation-uuid>/` holding `.blk` data files plus a `MANIFEST.json`.
  Internal node/relationship ids are dense and stable *within* a generation, but
  a rebuild produces a new generation with fresh ids (see
  [03 Data model](03-data-model.md)).
- **The `current` pointer** — a tiny file, `<graph>/current`, naming the live
  generation. It is written last, after the generation is fully published, so a
  crash mid-publish never exposes a half-written image. The server polls it and
  hot-reloads when it changes ([13 Deployment](13-deployment.md)).
- **Sets, segments, and the delta** — when the writable layer is on, writes
  accumulate in an in-memory **delta** backed by a write-ahead log, are sealed
  into on-disk **segments**, and a **set manifest** stacks the base generation
  plus its segments into what the server serves. Consolidation collapses the
  stack back into a single base ([11 Writing data](11-writing-data.md)).
- **Format version** — the on-disk format carries a `FORMAT_VERSION` (currently
  **8**). Slater has **no backwards compatibility**: a server refuses any
  generation whose format it does not understand, with a "must be rebuilt"
  message, rather than silently mis-reading it ([12 Storage](12-storage.md)).

## What Slater is not

- It is **not** a drop-in Neo4j server, even though it speaks Bolt and reports a
  `Neo4j/`-prefixed agent string so drivers feature-gate it correctly. The query
  language is a large, compatible subset ([07 Querying](07-querying.md)); the
  administrative surface is Slater's own.
- It does **not** compute embeddings. You supply vectors; Slater indexes and
  searches them ([10 Vector search](10-vector-search.md)).
- It does **not** enforce schema constraints (uniqueness, existence). Identity
  comes from business keys you define at build time
  ([03 Data model](03-data-model.md)).

## Next

- Get hands-on: [02 Quickstart](02-quickstart.md).
- Understand the data: [03 Data model](03-data-model.md).
