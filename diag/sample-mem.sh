#!/usr/bin/env bash
# Step-1 probe: does PG memory + client-backend count climb under load
# (the suspected path to the cgroup OOM)?
#
# Run this in one terminal while a normal k6 load test runs in another.
# Every INTERVAL sec it logs, per PG node container:
#   - memory usage + % of the container limit   (docker stats)
#   - live client-backend count                  (pg_stat_activity)
# If the OOM theory holds, both climb together toward 100% / the cap.
#
# Usage: sample-mem.sh [DURATION_SEC] [INTERVAL_SEC]   (default 1200 / 10)
set -uo pipefail
cd "$(dirname "$0")/.."

DUR="${1:-1200}"; INT="${2:-10}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="diag/mem-${STAMP}.log"

PW=$(grep -E '^POSTGRES_PASSWORD=' .env | cut -d= -f2- | tr -d '\r')
USR=$(grep -E '^POSTGRES_USER='    .env | cut -d= -f2- | tr -d '\r')
DB=$(grep -E '^POSTGRES_DB='       .env | cut -d= -f2- | tr -d '\r')

# Discover running PG node containers (shard0/shard1, node-a/b).
mapfile -t PGC < <(docker ps --format '{{.Names}}' | grep -E 'pg-shard[0-9]+-node-' | sort)
if [ "${#PGC[@]}" -eq 0 ]; then echo "no PG node containers running"; exit 1; fi

echo "PG containers: ${PGC[*]}"        | tee    "$OUT"
echo "sampling -> $OUT  (${DUR}s @ ${INT}s)" | tee -a "$OUT"

backends() { # $1=container -> client-backend count on that node
  docker exec -e PGPASSWORD="$PW" "$1" \
    psql -h 127.0.0.1 -U "$USR" -d "$DB" -tAc \
    "select count(*) from pg_stat_activity where backend_type='client backend'" \
    2>/dev/null | tr -d '[:space:]'
}

END=$(( $(date +%s) + DUR ))
while [ "$(date +%s)" -lt "$END" ]; do
  TS=$(date -u +%H:%M:%S)
  STATS=$(docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.MemPerc}}' "${PGC[@]}" 2>/dev/null)
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    name=$(echo "$line" | awk '{print $1}')
    b=$(backends "$name")
    echo "[$TS] ${line}  backends=${b:-?}" >> "$OUT"
  done <<< "$STATS"
  echo "[$TS] ----" >> "$OUT"
  sleep "$INT"
done
echo "DONE -> $OUT" | tee -a "$OUT"
