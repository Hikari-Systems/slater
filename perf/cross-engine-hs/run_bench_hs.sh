#!/usr/bin/env bash
# 5 runs per engine, restarting the container before each, then bench. Captures
# peak+current RSS per engine from the cgroup after the final (run-5) cycle.
#
# Usage: run_bench_hs.sh <graph> <bench_script> <expected_node_count>
#   e.g. run_bench_hs.sh mesh bench_mesh.py 340839
set -u
GRAPH="$1"; BENCH="$2"; EXPECT="$3"
PY=/tmp/pole_venv/bin/python
HERE="$(cd "$(dirname "$0")" && pwd)"
RES="/tmp/bench-hs/results-${GRAPH}"
mkdir -p "$RES"
# Clear only the result files for the engines being run this invocation, so the
# one-at-a-time full-Wikidata sweep (each engine benched in isolation) doesn't wipe
# the others' results. memory.txt is appended; the aggregator keeps the last line/engine.
# Engine set is overridable via env so a run can target a subset (e.g. the full
# Wikidata sweep, where only the disk-backed engines slater/neo4j/ladybug can load).
ENGINES="${ENGINES:-slater neo4j memgraph falkordb arcadedb ladybug}"
# ladybug is embedded (no container/port): run inside the slater-ladybug image.
declare -A CONT=( [slater]=slater-hs [neo4j]=neo4j-hs [memgraph]=memgraph-hs \
                  [falkordb]=falkordb-hs [arcadedb]=arcadedb-hs )
LBVOL=/tmp/bench-hs/ladybug
# Run the embedded-ladybug container as the host user so files it writes (notably
# memory.txt) are host-owned — otherwise the later cgroup `tee -a` can't append.
# Mount /tmp/bench-hs so the shared kNN query-vector pool (vec_pool.json) is visible:
# the embedded container has no host network to reach Neo4j, but an earlier engine
# (slater/neo4j) has already built the pool on the host, so ladybug reads the same one.
LB_MOUNTS="--user $(id -u):$(id -g) -v $HERE:/app -v $LBVOL:/data/ladybug -v /tmp/bench-hs:/tmp/bench-hs"
# Propagate the graph name into the embedded-ladybug container (its bench resolves
# /data/ladybug/<graph>.lbug from WIKI_GRAPH; without this it defaults to wikidata1m).
LB_ENV="-e WIKI_GRAPH=${WIKI_GRAPH:-wikidata1m}"
for e in $ENGINES; do rm -f "$RES/${e}".run*.json "$RES/${e}".run*.err; done
touch "$RES/memory.txt"  # host-owned up front, so every appender can write it

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
    if [ "$e" = "ladybug" ]; then
      # Embedded: each `docker run` is a fresh process — no restart needed. On the
      # final run, capture the bench process' peak RSS into the shared memory.txt.
      if [ "$run" = "5" ]; then
        docker run --rm $LB_MOUNTS $LB_ENV -v "$RES":/results -w /app slater-ladybug:local \
          python /app/bench_with_rss.py "/app/$BENCH" ladybug /results/memory.txt \
          > "$RES/${e}.run${run}.json" 2>"$RES/${e}.run${run}.err"
      else
        docker run --rm $LB_MOUNTS $LB_ENV -w /app slater-ladybug:local \
          python "/app/$BENCH" ladybug > "$RES/${e}.run${run}.json" 2>"$RES/${e}.run${run}.err"
      fi
    else
      docker restart "${CONT[$e]}" >/dev/null 2>&1
      wait_ready "$e" || continue
      $PY "$HERE/$BENCH" "$e" > "$RES/${e}.run${run}.json" 2>"$RES/${e}.run${run}.err"
    fi
    echo "run $run $e -> $(head -c 90 "$RES/${e}.run${run}.json" 2>/dev/null)"
  done
done

echo "=== memory (run-5 cycle) ==="
cg=/sys/fs/cgroup
for e in $ENGINES; do
  [ "$e" = "ladybug" ] && continue  # ladybug RSS already written by bench_with_rss.py
  id=$(docker inspect -f '{{.Id}}' "${CONT[$e]}")
  peak=""; cur=""
  for base in "$cg/docker/$id" "$cg/system.slice/docker-$id.scope"; do
    [ -f "$base/memory.peak" ]    && peak=$(cat "$base/memory.peak")
    [ -f "$base/memory.current" ] && cur=$(cat "$base/memory.current")
  done
  echo "$e peak=${peak:-0} current=${cur:-0}" | tee -a "$RES/memory.txt"
done
echo "DONE $GRAPH"
