#!/usr/bin/env bash
# Confirm the cross-shard lock-contention hypothesis under load.
# Samples, on BOTH shard primaries (pg-haproxy :5000 / :5001):
#   1. cross_shard_outbox depth by status  (is a backlog building?)
#   2. live Lock-wait blocking chains       (who blocks the credit?)
#
# Run in one terminal, start k6 in another. ~5 min is plenty to see
# the steady-state pattern.
#
# Usage: sample-locks.sh [DURATION_SEC] [INTERVAL_SEC]   (default 300 / 2)
set -uo pipefail
cd "$(dirname "$0")/.."

DUR="${1:-300}"; INT="${2:-2}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="diag/profileC-locks-${STAMP}.log"

PW=$(grep -E '^POSTGRES_PASSWORD='  .env | cut -d= -f2- | tr -d '\r')
USR=$(grep -E '^POSTGRES_USER='     .env | cut -d= -f2- | tr -d '\r')
DB=$(grep -E '^POSTGRES_DB='        .env | cut -d= -f2- | tr -d '\r')

DEPTH_SQL="SELECT status||'='||count(*) FROM cross_shard_outbox GROUP BY status ORDER BY status;"

# Blocked Lock-waiters and who holds the lock. All app backends are
# the same role, so query text is visible without superuser.
BLOCK_SQL="SELECT 'blocked['||blocked.pid||'] '||coalesce(blocked.wait_event,'?')||' :: '||left(regexp_replace(blocked.query,'\s+',' ','g'),64)||'  <== blocked_by['||bp.pid||'] '||left(regexp_replace(blocking.query,'\s+',' ','g'),64) FROM pg_stat_activity blocked JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) AS bp(pid) ON true JOIN pg_stat_activity blocking ON blocking.pid=bp.pid WHERE blocked.wait_event_type='Lock';"

psql_q() { # $1=port  $2=sql
  docker exec -e PGPASSWORD="$PW" peakload-pg-shard0-node-a \
    psql -h pg-haproxy -p "$1" -U "$USR" -d "$DB" -At -c "$2" 2>&1
}

echo "sampling -> $OUT   (${DUR}s @ ${INT}s)"
END=$(( $(date +%s) + DUR ))
while [ "$(date +%s)" -lt "$END" ]; do
  TS=$(date -u +%H:%M:%S)
  for P in 5000 5001; do
    D=$(psql_q "$P" "$DEPTH_SQL" | tr '\n' ' ')
    echo "[$TS shard@$P] outbox: ${D:-<empty>}" >> "$OUT"
    B=$(psql_q "$P" "$BLOCK_SQL")
    if [ -n "$B" ]; then
      echo "[$TS shard@$P] LOCK-WAITS:" >> "$OUT"
      echo "$B" | sed 's/^/    /' >> "$OUT"
    fi
  done
  sleep "$INT"
done
echo "DONE -> $OUT"
