#!/usr/bin/env bash
# Decompose the cross-shard slowness. Run alongside a normal k6 load
# test. Every INTERVAL sec, per shard primary, it logs:
#   pending   - outbox rows still waiting (backlog depth)
#   done/iv   - rows COMPLETED in the last interval (drain throughput)
#   lat p50/p95/max ms - age (completed_at - created_at) of those rows,
#                        i.e. how long the cross-shard saga actually took
#
# Reading it:
#   * pending stays low + lat ~250-500ms  -> POLL FLOOR (fix: event-driven drain)
#   * pending grows unbounded             -> drain can't keep up (throughput wall)
#   * pending a large STANDING queue + lat = pending/throughput seconds
#                                         -> backlog-bound; check if commits are disk-bound
#
# Needs the step-1 retention (completed rows kept ~5 min), which they are.
#
# Usage: sample-crossshard.sh [DURATION_SEC] [INTERVAL_SEC]   (default 960 / 5)
set -uo pipefail
cd "$(dirname "$0")/.."

DUR="${1:-960}"; INT="${2:-5}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="diag/crossshard-${STAMP}.log"

PW=$(grep -E '^POSTGRES_PASSWORD=' .env | cut -d= -f2- | tr -d '\r')
USR=$(grep -E '^POSTGRES_USER='    .env | cut -d= -f2- | tr -d '\r')
DB=$(grep -E '^POSTGRES_DB='       .env | cut -d= -f2- | tr -d '\r')

# One row: pending | done_in_window | p50ms | p95ms | maxms
# The latency window matches the sample interval so done/iv is a rate.
SQL=$(cat <<SQL
SELECT
  (SELECT count(*) FROM cross_shard_outbox WHERE status='pending'),
  c.n, c.p50, c.p95, c.maxms
FROM (
  SELECT count(*) n,
         round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ms)) p50,
         round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)) p95,
         round(max(ms)) maxms
  FROM (
    SELECT EXTRACT(EPOCH FROM (completed_at - created_at)) * 1000 AS ms
    FROM cross_shard_outbox
    WHERE status = 'completed'
      AND completed_at > NOW() - (INTERVAL '1 second' * ${INT})
  ) x
) c;
SQL
)

psql_q() { # $1=port
  docker exec -e PGPASSWORD="$PW" peakload-pg-shard0-node-a \
    psql -h pg-haproxy -p "$1" -U "$USR" -d "$DB" -At -F '|' -c "$SQL" 2>&1
}

echo "sampling -> $OUT  (${DUR}s @ ${INT}s)" | tee "$OUT"
echo "cols: [time shard@port] pending | done/iv | p50ms | p95ms | maxms" | tee -a "$OUT"
END=$(( $(date +%s) + DUR ))
while [ "$(date +%s)" -lt "$END" ]; do
  TS=$(date -u +%H:%M:%S)
  for P in 5000 5001; do
    R=$(psql_q "$P")
    echo "[$TS shard@$P] $R" >> "$OUT"
  done
  sleep "$INT"
done
echo "DONE -> $OUT" | tee -a "$OUT"
