# perf/datasets — DuckDB → slater-build conversion

`wikidata_csr.duckdb` (8.7 GB) is a Wikidata graph stored in **symmetric CSR**
adjacency form: **91,600,404 nodes / 1,533,008,048 directed edges** (the graph is
undirected, so each edge is stored in both directions). `convert.sql` +
`duckdb_to_slater.sh` turn it into the primitive-Cypher dump that `slater-build`
ingests.

📥 **Download the DuckDB source** (`wikidata_csr.duckdb`) from
<https://huggingface.co/datasets/ladybugdb/wikidata-20260401/tree/main>.

## How it works

The data flow has **no intermediate format** — DuckDB writes the final Cypher text
directly:

```
wikidata_csr.duckdb ──[ duckdb runs convert.sql ]──> primitive-Cypher dump ──[ slater-build ]──> on-disk image
```

`convert.sql` is the converter logic, expressed as SQL. Each `COPY (SELECT
'<a formatted Cypher line>') TO '/dev/stdout'` makes DuckDB emit one finished
statement per output row, so the entire job — expanding 1.5 B CSR edges (via an
`ASOF JOIN` against the `indptr` array) and escaping ~91.6 M node names — runs
inside DuckDB's vectorised C++ engine. The shell wrapper only sets a few variables,
prints the static index-DDL header / cleanup footer, and routes stdout. No
host-language loop ever touches a row, so throughput is IO-bound regardless.

The DuckDB source tables:

| table | meaning |
|---|---|
| `wikidata_mapping_node(csr_index, original_node_id)` | dense id ↔ sparse wikidata id |
| `wikidata_nodes_node(id, name)` | wikidata id → label string |
| `wikidata_indptr_rel(ptr)` | CSR row pointers, `n_nodes+1` rows; `rowid k` = `ptr[k]` |
| `wikidata_indices_rel(target)` | flat CSR column array; `rowid` = position |

Each node becomes `(:Entity:__DumpVertex__ {__dump_id__: <csr_index>, wikidata_id:
<id>, name: '<escaped>'})`; `__dump_id__` is the dense `csr_index`, so edge
endpoints resolve with no lookup table. Edges become `[:LINK]`.

## Requirements

- The **`duckdb` CLI** on PATH, **or** Docker (the wrapper falls back to the pinned
  `datacatering/duckdb:v1.5.3` image — it must match the `libduckdb` version that
  wrote the file). Override with `--image` / `--duckdb-bin`.
- For `--build`: a `slater-build` binary (`cargo build -p slater-build --release`,
  or `--slater-build <path>`).

## Usage

```bash
cd perf/datasets

# Stream an induced subgraph (first N csr_indices + edges among them) to a file
./duckdb_to_slater.sh --limit 100000 --output wikidata-100k.cypher

# Build a subgraph straight into a slater data dir (no intermediate file)
./duckdb_to_slater.sh --limit 100000 --build /tmp/data --graph wikidata

# Full graph, undirected-deduped (one direction per edge), to a file
./duckdb_to_slater.sh --dedup --output wikidata-full.cypher
```

### Flags

| flag | default | meaning |
|---|---|---|
| `--limit N` | `0` (full) | induced subgraph on `csr_index < N` — the practical knob for sizing a perf dataset |
| `--dedup` | off | keep only the `src<dst` direction; halves the symmetric duplication (drops self-loops; preserves parallel multi-edges) |
| `--label L` | `Entity` | node label |
| `--reltype R` | `LINK` | relationship type |
| `--output FILE` | stdout | write the dump to a file |
| `--build DIR` + `--graph NAME` | — | pipe the dump straight into `slater-build` |
| `--no-ddl` / `--no-cleanup` | both on | omit the index-DDL header / marker-cleanup footer |
| `--db PATH` | `./wikidata_csr.duckdb` | source DuckDB file |
| `--slater-build PATH` | `../../target/release/slater-build` | builder binary |
| `--image IMG` / `--duckdb-bin BIN` | `datacatering/duckdb:v1.5.3` | DuckDB engine to use |

## Scale warning

The **full** graph is enormous: the faithful dump is on the order of **~150 GB of
text** and **1.533 B edge statements**, and building it is a multi-hour, large-RSS
offline job. Start with `--limit` to produce a tractable perf graph (e.g. a few
hundred thousand nodes) and only go full once the pipeline is validated. `--dedup`
roughly halves the edge volume if you only need each undirected edge once.

## Verified

The pipeline is validated end-to-end against the real `duckdb` CLI and the release
`slater-build`: e.g. `--limit 2000` builds deterministically to **2000 nodes /
34,318 edges**, name escaping is byte-correct (a literal `\` in a name → `\\`),
`--dedup` halves the edge count with zero `src>=dst` edges, and the `ASOF`
expansion reproduces the CSR adjacency exactly.
