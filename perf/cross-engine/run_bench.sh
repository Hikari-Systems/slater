#!/usr/bin/env bash
# 5 runs per engine, restarting the container before each run, then bench.
set -u
PY=/tmp/pole_venv/bin/python
RES=/tmp/bench/results
mkdir -p "$RES"; rm -f "$RES"/*
ENGINES="slater neo4j memgraph falkordb"
declare -A CONT=( [slater]=slater-pole [neo4j]=pole-neo4j [memgraph]=pole-memgraph [falkordb]=pole-falkordb )

wait_ready () {  # $1 engine
  for i in $(seq 1 90); do
    c=$($PY /tmp/bench/count.py "$1" 2>/dev/null)
    [ "$c" = "61521" ] && return 0
    sleep 2
  done
  echo "TIMEOUT waiting for $1 (last=$c)" >&2; return 1
}

for run in 1 2 3 4 5; do
  for e in $ENGINES; do
    docker restart "${CONT[$e]}" >/dev/null 2>&1
    wait_ready "$e" || continue
    $PY /tmp/bench/bench_one.py "$e" > "$RES/${e}.run${run}.json" 2>"$RES/${e}.run${run}.err"
    echo "run $run $e -> $(cat "$RES/${e}.run${run}.json" 2>/dev/null | head -c 80)"
  done
done

# Capture peak + current RSS per engine from the cgroup (reflects the last,
# run-5, recovery+bench cycle since each container was restarted before it).
echo "=== memory (run-5 cycle) ==="
for e in $ENGINES; do
  id=$(docker inspect -f '{{.Id}}' "${CONT[$e]}")
  peak=$(cat /sys/fs/cgroup/docker/$id/memory.peak 2>/dev/null)
  cur=$(cat /sys/fs/cgroup/docker/$id/memory.current 2>/dev/null)
  echo "$e peak=$peak current=$cur" | tee -a "$RES/memory.txt"
done
echo "DONE"
