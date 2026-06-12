#!/usr/bin/env bash
# Stand up the 4-engine -hs stack for ONE hs-backend-spot reference graph and load
# all four. Dedicated -hs container names + port block (7700/7701/7702/6401) so the
# pole stack on 7687-7689/6390 is never touched.
#
# Usage: setup_hs.sh <graph> <cypher_src> [extra_index "Label:prop"]
#   <graph>       logical graph name (also slater --graph and the ACL grant key)
#   <cypher_src>  path to the primitive-Cypher dump
#   extra_index   optional "Label:prop" range index appended uniformly to every engine
set -eu
GRAPH="$1"; SRC="$2"; EXTRA="${3:-}"
PY=/tmp/pole_venv/bin/python
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=/tmp/bench-hs
mkdir -p "$WORK/cypher"
# argon2id hash for password "polereader" (known-good, from PERF_PROGRESS).
HASH='$argon2id$v=19$m=19456,t=2,p=1$R3VyS8OJIiG2Q7ihG1WlJQ$WAnkFldoPaMdxe1lAxUt/qio1Ny/jTV5aeo3p2h7ZuU'

echo "### teardown any prior -hs containers"
docker rm -f slater-hs neo4j-hs memgraph-hs falkordb-hs >/dev/null 2>&1 || true
docker volume rm slater_hs >/dev/null 2>&1 || true

echo "### stage working cypher (+ optional extra index)"
WCY="$WORK/cypher/${GRAPH}.cypher"
cp "$SRC" "$WCY"
if [ -n "$EXTRA" ]; then
  L="${EXTRA%%:*}"; P="${EXTRA##*:}"
  printf 'CREATE INDEX FOR (n:%s) ON (n.%s);\n' "$L" "$P" >> "$WCY"
  echo "  appended CREATE INDEX FOR (n:$L) ON (n.$P)"
fi

echo "### write acl.json (grant reporting:read on $GRAPH) — stamped into the generation"
cat > "$WORK/acl.json" <<JSON
{ "users": { "reporting": {
    "passwordArgon2id": "$HASH",
    "grants": { "$GRAPH": ["read"] } } } }
JSON

echo "### slater-build $GRAPH -> volume slater_hs (stamping --acl)"
docker run --rm --user 0 -v slater_hs:/data -v "$WORK/cypher":/dumps:ro \
  -v "$WORK/acl.json":/acl.json:ro \
  --entrypoint /app/slater-build slater:local \
  --input "/dumps/${GRAPH}.cypher" --graph "$GRAPH" --data-dir /data --acl /acl.json

echo "### launch engines"
docker run -d --name slater-hs -p 7700:7687 \
  -v slater_hs:/data:ro -v "$WORK/acl.json":/config/acl.json:ro slater:local >/dev/null
docker run -d --name neo4j-hs -p 7701:7687 \
  -e NEO4J_AUTH=neo4j/polepole12 -e NEO4J_server_memory_pagecache_size=512M neo4j:5 >/dev/null
docker run -d --name memgraph-hs -p 7702:7687 \
  memgraph/memgraph:latest --telemetry-enabled=false >/dev/null
docker run -d --name falkordb-hs -p 6401:6379 falkordb/falkordb:latest >/dev/null

echo "### wait for Bolt/RESP readiness"
for i in $(seq 1 60); do
  ok=1
  docker exec slater-hs /app/slater healthcheck localhost 7687 >/dev/null 2>&1 || ok=0
  $PY "$HERE/count_hs.py" neo4j "$GRAPH"    >/dev/null 2>&1 || true
  sleep 2
  [ "$ok" = 1 ] && break
done

echo "### load neo4j / memgraph / falkordb from $WCY"
$PY "$HERE/load_cypher.py" neo4j    "$WCY" --uri bolt://localhost:7701 --pass polepole12
$PY "$HERE/load_cypher.py" memgraph "$WCY" --uri bolt://localhost:7702
$PY "$HERE/load_cypher.py" falkordb "$WCY" --port 6401 --graph "$GRAPH"

echo "### persist in-memory engines so they recover across the restart-bench loop"
# Memgraph is in-memory; snapshot so a restart recovers the data.
$PY -c "from neo4j import GraphDatabase as G; s=G.driver('bolt://localhost:7702',auth=('','')).session(); list(s.run('CREATE SNAPSHOT')); print('  memgraph snapshot ok')" || echo "  memgraph snapshot FAILED"
# FalkorDB persists via RDB; force a save.
docker exec falkordb-hs redis-cli SAVE >/dev/null 2>&1 && echo "  falkordb SAVE ok" || echo "  falkordb SAVE FAILED"

echo "### verify node counts per engine"
for e in slater neo4j memgraph falkordb; do
  printf '  %-9s %s\n' "$e" "$($PY "$HERE/count_hs.py" "$e" "$GRAPH")"
done
echo "### setup done for $GRAPH"
