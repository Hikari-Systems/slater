#!/usr/bin/env bash
# 5 runs per engine, restarting service containers before each, then bench. Captures
# peak+current RSS per engine from the cgroup after the final (run-5) cycle. LadybugDB
# is embedded in the benchmark Python process, so run 5 records that process' RSS.
#
# Usage: run_bench_hs.sh <graph> <bench_script> <expected_node_count>
#   e.g. run_bench_hs.sh mesh bench_mesh.py 340839
set -u
GRAPH="$1"; BENCH="$2"; EXPECT="$3"
PY=/tmp/pole_venv/bin/python
HERE="$(cd "$(dirname "$0")" && pwd)"
RES="/tmp/bench-hs/results-${GRAPH}"
mkdir -p "$RES"; rm -f "$RES"/*
ENGINES="slater neo4j memgraph falkordb ladybug"
declare -A CONT=( [slater]=slater-hs [neo4j]=neo4j-hs [memgraph]=memgraph-hs [falkordb]=falkordb-hs )

wait_ready () {  # $1 engine
  for i in $(seq 1 90); do
    c=$($PY "$HERE/count_hs.py" "$1" "$GRAPH" 2>/dev/null)
    [ "$c" = "$EXPECT" ] && return 0
    sleep 2
  done
  echo "TIMEOUT waiting for $1 (last=$c, want=$EXPECT)" >&2; return 1
}

for run in 1 2 3 4 5; do
  for e in $ENGINES; do
    if [ "$e" != "ladybug" ]; then
      docker restart "${CONT[$e]}" >/dev/null 2>&1
    fi
    wait_ready "$e" || continue
    if [ "$e" = "ladybug" ] && [ "$run" = "5" ]; then
      $PY "$HERE/bench_with_rss.py" "$HERE/$BENCH" "$e" "$RES/memory.txt" > "$RES/${e}.run${run}.json" 2>"$RES/${e}.run${run}.err"
    else
      $PY "$HERE/$BENCH" "$e" > "$RES/${e}.run${run}.json" 2>"$RES/${e}.run${run}.err"
    fi
    echo "run $run $e -> $(head -c 90 "$RES/${e}.run${run}.json" 2>/dev/null)"
  done
done

echo "=== memory (run-5 cycle) ==="
cg=/sys/fs/cgroup
for e in $ENGINES; do
  [ "$e" = "ladybug" ] && continue
  id=$(docker inspect -f '{{.Id}}' "${CONT[$e]}")
  peak=""; cur=""
  for base in "$cg/docker/$id" "$cg/system.slice/docker-$id.scope"; do
    [ -f "$base/memory.peak" ]    && peak=$(cat "$base/memory.peak")
    [ -f "$base/memory.current" ] && cur=$(cat "$base/memory.current")
  done
  echo "$e peak=${peak:-0} current=${cur:-0}" | tee -a "$RES/memory.txt"
done
echo "DONE $GRAPH"
