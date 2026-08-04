#!/usr/bin/env bash
# monarch_to_slater.sh — convert the Monarch KG DuckDB into a slater-build dump.
#
# Thin wrapper around convert_monarch.sql: sets the SQL variable, frames the DuckDB
# output with the (static) index-DDL header and cleanup footer, and routes the stream
# to stdout, a file, or straight into slater-build (`--pk __dump_id__`). DuckDB does all
# the per-row work, so this stays a few-line orchestrator regardless of scale.
#
# DuckDB is run via the `duckdb` CLI if on PATH, else the pinned Docker image (the
# .duckdb file was written by libduckdb 1.5.3 — the image must match).
#
# Examples:
#   # Stream a 5k-node smoke subset to a file
#   ./monarch_to_slater.sh --db ~/wd-full/monarch-kg.duckdb --limit 5000 --output /tmp/monarch-5k.cypher
#
#   # Full graph (1.46M nodes / 15.21M edges) to a file (inspectable, resumable build)
#   ./monarch_to_slater.sh --db ~/wd-full/monarch-kg.duckdb --output ~/wd-full/monarch-kg.cypher
#
#   # Build directly, no intermediate file (pass build flags through after --)
#   ./monarch_to_slater.sh --db ~/wd-full/monarch-kg.duckdb --build ~/wd-full/data-monarch --graph monarch -- --diagnostics
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# --- defaults ----------------------------------------------------------------
DB="$SCRIPT_DIR/monarch-kg.duckdb"
CONVERT_SQL="$SCRIPT_DIR/convert_monarch.sql"
LIMIT=0
WITH_DDL=1
WITH_CLEANUP=1
OUTPUT=""
BUILD_DIR=""
GRAPH="monarch"
SLATER_BUILD="$REPO_ROOT/target/release/slater-build"
IMAGE="datacatering/duckdb:v1.5.3"
DUCKDB_BIN=""
BUILD_ARGS=()   # extra slater-build flags, collected after `--`

usage() { sed -n '2,28p' "$0"; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --db)            DB="$2"; shift 2;;
    --limit)         LIMIT="$2"; shift 2;;
    --no-ddl)        WITH_DDL=0; shift;;
    --no-cleanup)    WITH_CLEANUP=0; shift;;
    --output)        OUTPUT="$2"; shift 2;;
    --build)         BUILD_DIR="$2"; shift 2;;
    --graph)         GRAPH="$2"; shift 2;;
    --slater-build)  SLATER_BUILD="$2"; shift 2;;
    --image)         IMAGE="$2"; shift 2;;
    --duckdb-bin)    DUCKDB_BIN="$2"; shift 2;;
    --)              shift; BUILD_ARGS=("$@"); break;;
    -h|--help)       usage 0;;
    *) echo "unknown arg: $1" >&2; usage 1;;
  esac
done

# --- validation --------------------------------------------------------------
[ -f "$DB" ] || { echo "DB not found: $DB" >&2; exit 1; }
[ -f "$CONVERT_SQL" ] || { echo "convert_monarch.sql not found: $CONVERT_SQL" >&2; exit 1; }
case "$LIMIT" in (*[!0-9]*|'') echo "--limit must be a non-negative integer" >&2; exit 1;; esac
if [ -n "$BUILD_DIR" ]; then
  [ -x "$SLATER_BUILD" ] || { echo "slater-build not executable: $SLATER_BUILD (build it or pass --slater-build)" >&2; exit 1; }
fi
if [ -z "$DUCKDB_BIN" ] && ! command -v duckdb >/dev/null 2>&1; then
  command -v docker >/dev/null 2>&1 || { echo "need either the 'duckdb' CLI or 'docker' on PATH" >&2; exit 1; }
elif [ -z "$DUCKDB_BIN" ]; then
  DUCKDB_BIN="duckdb"   # found on PATH
fi

# --- DuckDB invocation -------------------------------------------------------
# Run against an in-memory default catalog and ATTACH the source file read-only as `src`
# (the engine's read-only mode forbids the macros/TEMP table in convert_monarch.sql if the
# file is the default catalog). $1 is the db path as the engine sees it (local vs Docker).
build_sql() {
  printf "ATTACH '%s' AS src (READ_ONLY);\nSET VARIABLE node_limit = %s;\n%s" \
    "$1" "$LIMIT" "$(cat "$CONVERT_SQL")"
}

# NO_COLOR keeps the CLI from wrapping any diagnostic in ANSI codes (which would land on
# stdout and corrupt the dump). convert_monarch.sql uses non-deprecated `lambda x:` syntax
# so DuckDB emits no warnings, but this is belt-and-braces.
run_duckdb() {
  if [ -n "$DUCKDB_BIN" ]; then
    NO_COLOR=1 "$DUCKDB_BIN" -c "$(build_sql "$DB")"
  else
    local dbdir dbbase
    dbdir="$(cd "$(dirname "$DB")" && pwd)"
    dbbase="$(basename "$DB")"
    docker run --rm -i -e NO_COLOR=1 -v "$dbdir:/data:ro" "$IMAGE" -c "$(build_sql "/data/$dbbase")"
  fi
}

# The header (index DDL) and footer (strip the linking marker) are static text — see
# golden_external_roundtrip.rs for the `--pk __dump_id__` convention. The __dump_id__
# index speeds build-time endpoint resolution; the `Node(id)` index leaves the CURIE
# range-queryable in the served graph. DuckDB produces only the node + edge bodies.
emit() {
  if [ "$WITH_DDL" = 1 ]; then
    printf 'CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);\n'
    printf 'CREATE INDEX FOR (n:Node) ON (n.id);\n'
  fi
  run_duckdb
  if [ "$WITH_CLEANUP" = 1 ]; then
    printf 'MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;\n'
    printf 'DROP INDEX ON :__DumpVertex__(__dump_id__);\n'
  fi
}

# --- route -------------------------------------------------------------------
if [ -n "$BUILD_DIR" ]; then
  echo "building graph '$GRAPH' into $BUILD_DIR (--pk __dump_id__) ..." >&2
  emit | "$SLATER_BUILD" --input - --graph "$GRAPH" --data-dir "$BUILD_DIR" --pk __dump_id__ "${BUILD_ARGS[@]}"
elif [ -n "$OUTPUT" ]; then
  # Pipe through `cat` so DuckDB's `COPY TO '/dev/stdout'` sees an append-only pipe;
  # writing straight to a regular file makes each COPY reopen /dev/stdout at offset 0.
  emit | cat > "$OUTPUT"
  echo "wrote $OUTPUT" >&2
else
  emit
fi
