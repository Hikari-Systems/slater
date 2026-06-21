# Dump formats (default merge; `--pk` for dump_id-style)

`slater-build` accepts two input dump identity models, selected by the **`--pk`** flag:

- **merge** (default, no `--pk`) — the dump is *entirely* business-key `MERGE` statements.
  The **per-pattern** business key (label + the property in the pattern) is the node
  identity; edges resolve their endpoints by it. There is no `__dump_id__`. Each label
  has its own natural key (`Company.ticker`, `Source.sourceId`, `Indication.meshUi`, …).
  Use this to build a graph from scratch out of MERGE statements (e.g. a back-population
  corpus).
- **`--pk <FIELD>`** — single-global-key ("dump_id style") import: `<FIELD>` is the unique
  node identity across the *whole* dump (label-agnostic, integer-valued), and edges
  reference endpoints by it. This is the `GRAPH.DUMP` shape generalized: `--pk __dump_id__`
  ingests legacy FalkorDB dump files. The dump uses the CREATE / `MATCH … CREATE` grammar
  and may carry an overlay patch section (`MERGE|MATCH … SET`). `<FIELD>` is **stored** as
  a queryable node property (it is not consumed).

The two identity models do not mix: the default merge build rejects `__dump_id__`/CREATE
statements (pass `--pk` to import those), and a `--pk` build expects an integer key field.

## Statement forms

Statements are `;`-terminated; string literals are single-quoted (`\'` for a literal
quote, inner `"` is literal). Each node/edge pattern carries exactly one label and one
business-key property.

1. **Node create-on-absent (+ SET):**
   ```
   MERGE (n:Source {sourceId: '0001104659-24-124081'}) SET n.companyTicker = 'ATXI', n.formType = '8-K';
   MERGE (n:Company {ticker: 'A'}) SET n.canonicalName = 'Agilent Technologies Inc.';
   ```
2. **Node create-on-absent (no SET):**
   ```
   MERGE (n:Company {ticker: 'ATXI'});
   ```
3. **Edge create-on-absent with props:**
   ```
   MERGE (a:Source {sourceId: '0001…'})-[r:PUBLISHED_BY]->(b:Company {ticker: 'ATXI'})
     SET r.confidence = 'exact', r.designations = ['ORPHAN'];
   ```
4. **Edge create-on-absent without props (bare):**
   ```
   MERGE (a:Person {id: '9e…'})-[r:SOURCED_FROM]->(b:Source {sourceId: '0001…'});
   ```

Value literals: single-quoted strings, ints/floats/bools/null, and lists (`['ORPHAN']`).
Vector (`vecf32`) values are not supported in merge dumps.

## Semantics

- **Node identity** = `(label, business-key property, value)`. Multiple node MERGEs with
  the same identity collapse to **one** node; SET props fold **last-writer-wins** in input
  order (an unset prior key survives).
- **Value equality is type-exact**: `{id: 1}` (Int) does not resolve against `{id: 1.0}`
  (Float). Business keys are identifiers, so no numeric coercion.
- **Edge endpoints** resolve by business key against **all** nodes built this run. Dumps
  must be **self-contained**: every endpoint must appear as a node MERGE in the same
  input. An unresolved endpoint is a hard error.
- **Edge identity** = `(src, reltype, dst)`. Identical relationships collapse to one edge;
  SET props fold last-writer-wins in input order. (No multigraph by default.)
- `CREATE INDEX FOR (n:Label) ON (n.prop)` range-index DDL is honoured as usual.

## How it builds (bounded memory, streaming)

The build is the same bucketed `pass1 → resolve → emit` pipeline as `dump-id`, with two
merge-specific phases (`crates/slater-build/src/merge_build.rs`), each built on the
external sorter so peak memory is independent of node/edge count:

1. **pass 1** spills each node/edge MERGE into per-shard buckets (local symbol ids).
2. **dedup** (phase `Deduped`) external-sorts node MERGEs by identity, collapses each
   group (SET props last-wins), and writes the deduped node bucket plus a
   `(identity → prov)` key stream — both in identity order, so node ids are dense and
   deterministic and the key stream is pre-sorted for the join.
3. **resolve** spills two endpoint refs per edge, sort-merge-joins them against the node
   key stream to resolve endpoints by business key, reassembles each edge, then collapses
   identical `(src, reltype, dst)` edges into the final edge bucket.
4. **cluster + emit** are unchanged — they consume the deduped node bucket and the
   resolved edge bucket like any other build.

Determinism: node prov ids are assigned in identity sort order and edge ids in
`(src, reltype, dst, input-order)` order — independent of worker scheduling — so two
builds of the same dump produce byte-identical stores. The `Deduped` / `Resolved`
checkpoints make `--resume` skip completed phases.
