#!/usr/bin/env bash
# Sample resource usage AND pipeline backlog during a load test.
# Run this in one terminal, start k6 in another; stop with Ctrl-C or let it time out.
#
# Usage: sample-stats.sh LABEL [DURATION_SEC] [INTERVAL_SEC]
#   LABEL         tag for output files (e.g. profileC, profileB)
#   DURATION_SEC  total window (default 960 = 16 min, covers a 15-min test)
#   INTERVAL_SEC  seconds between snapshots (default 5)
#
# Writes two CSVs into diag/:
#   <LABEL>-stats-<stamp>.csv    ts,name,cpu_perc,mem_usage,mem_perc,block_io,net_io
#   <LABEL>-backlog-<stamp>.csv  ts,source,key,value
# block_io / net_io are cumulative-since-start (diff between rows to get a rate).
set -uo pipefail
cd "$(dirname "$0")/.."

LABEL="${1:-run}"; DUR="${2:-960}"; INT="${3:-5}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
STATS="diag/${LABEL}-stats-${STAMP}.csv"
BACK="diag/${LABEL}-backlog-${STAMP}.csv"

REDIS_PW=$(grep -E '^REDIS_PASSWORD=' .env 2>/dev/null | cut -d= -f2- | tr -d '\r')

echo "ts,name,cpu_perc,mem_usage,mem_perc,block_io,net_io" > "$STATS"
echo "ts,source,key,value" > "$BACK"
echo "Sampling -> $STATS  and  $BACK   (every ${INT}s for ${DUR}s)"

END=$(( $(date +%s) + DUR ))
while [ "$(date +%s)" -lt "$END" ]; do
  TS=$(date -u +%H:%M:%S)

  # 1) per-container resource usage (incl. disk + net I/O)
  docker stats --no-stream \
    --format "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}|{{.BlockIO}}|{{.NetIO}}" 2>/dev/null \
    | sed "s/^/${TS}|/" | tr '|' ',' >> "$STATS" || true

  # 2) RabbitMQ queue depth (stage-3 consumer backlog)
  docker exec peakload-rabbitmq rabbitmqctl list_queues -s name messages 2>/dev/null \
    | awk -v ts="$TS" 'NF==2 && $2 ~ /^[0-9]+$/ {print ts",rabbitmq,"$1","$2}' >> "$BACK" || true

  # 3) Redis pending-list depth (stage-2 intake backlog)
  if [ -n "${REDIS_PW:-}" ]; then
    for K in $(docker exec peakload-redis-master redis-cli -a "$REDIS_PW" --no-auth-warning \
                 --scan --pattern 'idempotency:pending*' 2>/dev/null | tr -d '\r'); do
      LEN=$(docker exec peakload-redis-master redis-cli -a "$REDIS_PW" --no-auth-warning \
              LLEN "$K" 2>/dev/null | tr -d '\r')
      [ -n "$LEN" ] && echo "${TS},redis,${K},${LEN}" >> "$BACK"
    done
  fi

  sleep "$INT"
done
echo "DONE -> $STATS  and  $BACK"
