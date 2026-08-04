#!/usr/bin/env bash
# duckdb_to_slater.sh — convert a DuckDB CSR graph into a slater-build dump.
#
# Thin wrapper around convert.sql: it sets the SQL variables, frames the DuckDB
# output with the (static) index DDL header and cleanup footer, and routes the
# stream to stdout, a file, or straight into slater-build. The DuckDB engine does
# all the per-row work, so this stays a few-line orchestrator regardless of scale.
#
# DuckDB is run via the `duckdb` CLI if on PATH, else the pinned Docker image
# (the .duckdb file was written by libduckdb 1.5.3 — the image must match).
#
# Examples:
#   # Stream a 100k-node induced subgraph to a file
#   ./duckdb_to_slater.sh --limit 100000 --output wikidata-100k.cypher
#
#   # Build that subgraph directly (no intermediate file)
#   ./duckdb_to_slater.sh --limit 100000 --build /data --graph wikidata
#
#   # Degree-capped 10M subgraph: bounds the 2-hop frontier at cap^2 so a
#   # var-length traversal cannot blow up on a Wikidata superhub (max degree in
#   # the uncapped first-10M induced subgraph is 2,409,783).
#   ./duckdb_to_slater.sh --limit 10000000 --degree-cap 1024 --output wd10m-cap1024.cypher
#
#   # Full graph (91.6M nodes / 1.533B edges — huge), undirected-deduped, to a file
#   ./duckdb_to_slater.sh --dedup --output wikidata-full.cypher
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# --- defaults ----------------------------------------------------------------
DB="$SCRIPT_DIR/wikidata_csr.duckdb"
CONVERT_SQL="$SCRIPT_DIR/convert.sql"
LIMIT=0
LABEL="Entity"
RELTYPE="LINK"
DEDUP=0
DEGREE_CAP=0
MEM_LIMIT="4GB"
THREADS=4
OVERLAY=0
WITH_DDL=1
WITH_CLEANUP=1
OUTPUT=""
BUILD_DIR=""
GRAPH="wikidata"
SLATER_BUILD="$REPO_ROOT/target/release/slater-build"
IMAGE="datacatering/duckdb:v1.5.3"
DUCKDB_BIN=""

usage() { sed -n '2,30p' "$0"; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --db)            DB="$2"; shift 2;;
    --limit)         LIMIT="$2"; shift 2;;
    --label)         LABEL="$2"; shift 2;;
    --reltype)       RELTYPE="$2"; shift 2;;
    --dedup)         DEDUP=1; shift;;
    --degree-cap)    DEGREE_CAP="$2"; shift 2;;
    --mem-limit)     MEM_LIMIT="$2"; shift 2;;
    --threads)       THREADS="$2"; shift 2;;
    --overlay)       OVERLAY=1; shift;;
    --no-ddl)        WITH_DDL=0; shift;;
    --no-cleanup)    WITH_CLEANUP=0; shift;;
    --output)        OUTPUT="$2"; shift 2;;
    --build)         BUILD_DIR="$2"; shift 2;;
    --graph)         GRAPH="$2"; shift 2;;
    --slater-build)  SLATER_BUILD="$2"; shift 2;;
    --image)         IMAGE="$2"; shift 2;;
    --duckdb-bin)    DUCKDB_BIN="$2"; shift 2;;
    -h|--help)       usage 0;;
    *) echo "unknown arg: $1" >&2; usage 1;;
  esac
done

# --- validation --------------------------------------------------------------
[ -f "$DB" ] || { echo "DB not found: $DB" >&2; exit 1; }
[ -f "$CONVERT_SQL" ] || { echo "convert.sql not found: $CONVERT_SQL" >&2; exit 1; }
case "$LIMIT" in (*[!0-9]*|'') echo "--limit must be a non-negative integer" >&2; exit 1;; esac
case "$DEGREE_CAP" in (*[!0-9]*|'') echo "--degree-cap must be a non-negative integer (0 = uncapped)" >&2; exit 1;; esac
# Labels/reltypes go verbatim into SQL and Cypher; slater-build only accepts these.
[[ "$LABEL"   =~ ^[A-Za-z0-9_]+$ ]] || { echo "--label must match [A-Za-z0-9_]+" >&2; exit 1; }
[[ "$RELTYPE" =~ ^[A-Za-z0-9_]+$ ]] || { echo "--reltype must match [A-Za-z0-9_]+" >&2; exit 1; }
if [ -n "$BUILD_DIR" ]; then
  [ -x "$SLATER_BUILD" ] || { echo "slater-build not executable: $SLATER_BUILD (build it or pass --slater-build)" >&2; exit 1; }
fi
if [ -z "$DUCKDB_BIN" ] && ! command -v duckdb >/dev/null 2>&1; then
  command -v docker >/dev/null 2>&1 || { echo "need either the 'duckdb' CLI or 'docker' on PATH" >&2; exit 1; }
elif [ -z "$DUCKDB_BIN" ]; then
  DUCKDB_BIN="duckdb"   # found on PATH
fi

# --- DuckDB invocation -------------------------------------------------------
# DuckDB defaults its memory limit to ~80% of system RAM. On a 15 GB dev box a
# 112M-row windowed aggregate at that ceiling competes with everything else and
# has taken the machine down. Bound it explicitly and let DuckDB spill instead —
# it is an out-of-core engine, so a low ceiling costs time, never correctness.
SQL="SET memory_limit = '$MEM_LIMIT';
SET threads = $THREADS;
SET VARIABLE node_limit = $LIMIT;
SET VARIABLE label = '$LABEL';
SET VARIABLE reltype = '$RELTYPE';
SET VARIABLE dedup = $DEDUP;
SET VARIABLE degree_cap = $DEGREE_CAP;
SET VARIABLE overlay = $OVERLAY;
$(cat "$CONVERT_SQL")"

run_duckdb() {
  if [ -n "$DUCKDB_BIN" ]; then
    "$DUCKDB_BIN" -readonly "$DB" -c "$SQL"
  else
    local dbdir dbbase
    dbdir="$(cd "$(dirname "$DB")" && pwd)"
    dbbase="$(basename "$DB")"
    docker run --rm -i -v "$dbdir:/data:ro" "$IMAGE" -readonly "/data/$dbbase" -c "$SQL"
  fi
}

# The DDL header and cleanup footer are static text, so the shell emits them
# directly — DuckDB only produces the node + edge bodies, in that order.
emit() {
  [ "$WITH_DDL" = 1 ] && printf 'CREATE INDEX FOR (n:%s) ON (n.wikidata_id);\n' "$LABEL"
  run_duckdb
  if [ "$WITH_CLEANUP" = 1 ]; then
    printf 'MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;\n'
    printf 'DROP INDEX ON :__DumpVertex__(__dump_id__);\n'
  fi
}

# --- route -------------------------------------------------------------------
if [ -n "$BUILD_DIR" ]; then
  echo "building graph '$GRAPH' into $BUILD_DIR ..." >&2
  emit | "$SLATER_BUILD" --input - --graph "$GRAPH" --data-dir "$BUILD_DIR"
elif [ -n "$OUTPUT" ]; then
  # Pipe through `cat` so DuckDB's `COPY TO '/dev/stdout'` sees an append-only pipe.
  # Writing straight to a regular file (`> "$OUTPUT"`) makes each COPY statement reopen
  # /dev/stdout with its own offset, so multiple COPY blocks overwrite/interleave.
  emit | cat > "$OUTPUT"
  echo "wrote $OUTPUT" >&2
else
  emit
fi
