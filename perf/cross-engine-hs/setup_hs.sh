#!/usr/bin/env bash
# Stand up the 6-engine -hs stack for ONE hs-backend-spot reference graph and load
# them all. Dedicated -hs container names + port block (7700/7701/7702/6401/7703) so
# the pole stack on 7687-7689/6390 is never touched. LadybugDB is embedded (no port).
#
# Usage: setup_hs.sh <graph> <cypher_src> [extra_index "Label:prop"]
#   <graph>       logical graph name (also slater --graph and the ACL grant key)
#   <cypher_src>  path to the primitive-Cypher dump
#   extra_index   optional "Label:prop" range index appended uniformly to every engine
# Env: SKIP_ARCADE=1 leaves an already-loaded arcadedb-hs untouched (it loads slowly,
#      so this lets you reuse it across runs); the other engines are still rebuilt.
set -eu
GRAPH="$1"; SRC="$2"; EXTRA="${3:-}"
PY=/tmp/pole_venv/bin/python
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK=/tmp/bench-hs
SKIP_ARCADE="${SKIP_ARCADE:-0}"
# slater image to build the generation with and serve — override to benchmark a
# published release, e.g. SLATER_IMG=hikarisystems/slater:v0.8.0
SLATER_IMG="${SLATER_IMG:-slater:local}"
mkdir -p "$WORK/cypher"
# argon2id hash for password "polereader" (known-good, from PERF_PROGRESS).
HASH='$argon2id$v=19$m=19456,t=2,p=1$R3VyS8OJIiG2Q7ihG1WlJQ$WAnkFldoPaMdxe1lAxUt/qio1Ny/jTV5aeo3p2h7ZuU'

echo "### teardown any prior -hs containers"
ARC_CONT="arcadedb-hs"; [ "$SKIP_ARCADE" = 1 ] && ARC_CONT=""
docker rm -f slater-hs neo4j-hs memgraph-hs falkordb-hs $ARC_CONT >/dev/null 2>&1 || true
docker volume rm slater_hs >/dev/null 2>&1 || true
# LadybugDB files are written by the container as root; the host can't rm them, and
# the loader clears its own .lbug* on load anyway — so this cleanup must not be fatal.
mkdir -p "$WORK/ladybug"
rm -rf "$WORK/ladybug"/* 2>/dev/null || true

echo "### build slater-ladybug image (embedded LadybugDB; real_ladybug wheel)"
docker build -q -t slater-ladybug:local -f "$HERE/Dockerfile.ladybug" "$HERE" >/dev/null

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
  --entrypoint /app/slater-build "$SLATER_IMG" \
  --input "/dumps/${GRAPH}.cypher" --graph "$GRAPH" --data-dir /data --acl /acl.json

echo "### launch engines"
docker run -d --name slater-hs -p 7700:7687 \
  -v slater_hs:/data:ro -v "$WORK/acl.json":/config/acl.json:ro "$SLATER_IMG" >/dev/null
docker run -d --name neo4j-hs -p 7701:7687 \
  -e NEO4J_AUTH=neo4j/polepole12 -e NEO4J_server_memory_pagecache_size=512M neo4j:5 >/dev/null
docker run -d --name memgraph-hs -p 7702:7687 \
  memgraph/memgraph:latest --telemetry-enabled=false >/dev/null
docker run -d --name falkordb-hs -p 6401:6379 falkordb/falkordb:latest >/dev/null
if [ "$SKIP_ARCADE" != 1 ]; then
docker run -d --name arcadedb-hs -p 7703:7687 -p 2480:2480 \
  -e JAVA_OPTS="-Darcadedb.server.rootPassword=playwithdata \
    -Darcadedb.server.plugins=Bolt:com.arcadedb.bolt.BoltProtocolPlugin \
    -Darcadedb.server.defaultDatabases=bench[root]" \
  arcadedata/arcadedb:latest >/dev/null
fi

echo "### wait for Bolt/RESP readiness"
for i in $(seq 1 60); do
  ok=1
  docker exec slater-hs /app/slater healthcheck localhost 7687 >/dev/null 2>&1 || ok=0
  $PY "$HERE/count_hs.py" neo4j "$GRAPH"    >/dev/null 2>&1 || true
  sleep 2
  [ "$ok" = 1 ] && break
done

if [ "$SKIP_ARCADE" != 1 ]; then
echo "### wait for arcadedb Bolt readiness"
for i in $(seq 1 60); do
  $PY -c "from neo4j import GraphDatabase as G; G.driver('bolt://localhost:7703',auth=('root','playwithdata'),encrypted=False).session(database='bench').run('RETURN 1').consume()" >/dev/null 2>&1 && break
  sleep 2
done
fi

echo "### wait for neo4j Bolt readiness"
for i in $(seq 1 60); do
  $PY -c "from neo4j import GraphDatabase as G; G.driver('bolt://localhost:7701',auth=('neo4j','polepole12')).session().run('RETURN 1').consume()" >/dev/null 2>&1 && break
  sleep 2
done

echo "### load neo4j / memgraph / falkordb from $WCY"
$PY "$HERE/load_cypher.py" neo4j    "$WCY" --uri bolt://localhost:7701 --pass polepole12
$PY "$HERE/load_cypher.py" memgraph "$WCY" --uri bolt://localhost:7702
$PY "$HERE/load_cypher.py" falkordb "$WCY" --port 6401 --graph "$GRAPH"

if [ "$SKIP_ARCADE" != 1 ]; then
echo "### load arcadedb (schema-first inheritance loader)"
$PY "$HERE/load_arcadedb.py" "$WCY" --http http://localhost:2480 --bolt bolt://localhost:7703 \
  --db bench --user root --pass playwithdata
else
echo "### SKIP_ARCADE=1 — leaving existing arcadedb-hs untouched"
fi

echo "### load ladybug (embedded; loader runs inside slater-ladybug image)"
docker run --rm -v "$HERE":/app -v "$WORK/ladybug":/data/ladybug -v "$WORK/cypher":/dumps:ro \
  -w /app slater-ladybug:local \
  python /app/load_ladybug.py "/dumps/${GRAPH}.cypher" --graph "$GRAPH" --out-dir /data/ladybug

echo "### persist in-memory engines so they recover across the restart-bench loop"
# Memgraph is in-memory; snapshot so a restart recovers the data.
$PY -c "from neo4j import GraphDatabase as G; s=G.driver('bolt://localhost:7702',auth=('','')).session(); list(s.run('CREATE SNAPSHOT')); print('  memgraph snapshot ok')" || echo "  memgraph snapshot FAILED"
# FalkorDB persists via RDB; force a save.
docker exec falkordb-hs redis-cli SAVE >/dev/null 2>&1 && echo "  falkordb SAVE ok" || echo "  falkordb SAVE FAILED"

echo "### verify node counts per engine"
VERIFY_ENGINES="slater neo4j memgraph falkordb"
[ "$SKIP_ARCADE" = 1 ] || VERIFY_ENGINES="$VERIFY_ENGINES arcadedb"
for e in $VERIFY_ENGINES; do
  printf '  %-9s %s\n' "$e" "$($PY "$HERE/count_hs.py" "$e" "$GRAPH")"
done
printf '  %-9s %s\n' "ladybug" \
  "$(docker run --rm -v "$HERE":/app -v "$WORK/ladybug":/data/ladybug -w /app slater-ladybug:local \
       python /app/count_hs.py ladybug "$GRAPH" 2>/dev/null)"
echo "### setup done for $GRAPH"
