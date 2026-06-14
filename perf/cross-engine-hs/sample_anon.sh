#!/usr/bin/env bash
# Sample a container's cgroup *anonymous* memory high-water (MiB) until killed.
# At graph-larger-than-cache scale the cgroup `memory.peak` is dominated by the
# reclaimable OS page cache of the engine's mmap'd store, which misrepresents the
# engine's own footprint. The anonymous pages (`memory.stat` `anon`) are the
# engine's actual allocations — heap, caches, query working memory — and are the
# honest "resident memory" metric for the disk-bound sweep. Writes the running max
# (bytes) to <out>, overwriting each tick, so the last value is the peak.
#
# Usage: sample_anon.sh <container> <out_file> [interval_s]
set -u
CONT="$1"; OUT="$2"; IV="${3:-0.5}"
cid=$(docker inspect -f '{{.Id}}' "$CONT" 2>/dev/null) || exit 0
peak=0
for base in "/sys/fs/cgroup/docker/$cid" "/sys/fs/cgroup/system.slice/docker-$cid.scope"; do
  [ -f "$base/memory.stat" ] && STAT="$base/memory.stat"
done
[ -z "${STAT:-}" ] && exit 0
while :; do
  a=$(awk '/^anon /{print $2}' "$STAT" 2>/dev/null)
  [ -n "${a:-}" ] && [ "$a" -gt "$peak" ] && { peak=$a; echo "$peak" > "$OUT"; }
  sleep "$IV"
done
