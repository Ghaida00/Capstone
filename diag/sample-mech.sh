#!/usr/bin/env bash
# Mechanism probe: WHY does the cross-shard drain's throughput collapse
# under load (≈220/s/shard idle -> ≈50/s/shard loaded) while CPU stays low?
#
# It discriminates between the two candidate fixes by sampling, per shard
# primary (via HAProxy), every INTERVAL sec:
#
#   ACT  total | active | idle_in_txn | drain_active
#     - total vs the 80 pgBouncer server-pool ceiling: total pinned ~80
#       = pgBouncer server pool exhausted (clients queue upstream).
#     - drain_active = backends currently running a cross_shard_* statement,
#       compared against the configured CROSS_SHARD_APPLY_CONCURRENCY (12):
#         drain_active << 12  -> drain is CONNECTION-STARVED (can't get its
#                                concurrency) => the limit is connections.
#         drain_active ~= 12  -> drain runs full-width but each apply is slow
#                                => the limit is per-apply latency.
#   WAIT <event>=<n> ...   wait-event histogram of ACTIVE client backends
#     - IO:WAL* / LWLock:WAL*    -> commit/fsync-bound   => BATCHING wins
#     - Lock:transactionid/tuple -> row-lock contention  => neither; reorder
#     - Client:ClientRead/(run)  -> not DB-bound (in-app sqlx pool starvation)
#
# Decision rule (pre-registered, given the 2×40=80=DEFAULT_POOL_SIZE sizing
# has ZERO connection headroom):
#   drain_active ~12 + waits dominated by WAL  -> commit-bound  => BATCHING
#   drain_active <<12 / total pinned ~80       -> conn-starved; a dedicated
#       drain pool needs MORE pgBouncer+pg capacity, so batching is still the
#       lower-risk first move (it cuts commits without adding connections).
#
# Run alongside a SHORT k6 load (~4-5 min); the backlog diverges ~80-120s in.
# Usage: sample-mech.sh [DURATION_SEC] [INTERVAL_SEC]   (default 300 / 5)
set -uo pipefail
cd "$(dirname "$0")/.."

DUR="${1:-300}"; INT="${2:-5}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="diag/mech-${STAMP}.log"

PW=$(grep -E '^POSTGRES_PASSWORD=' .env | cut -d= -f2- | tr -d '\r')
USR=$(grep -E '^POSTGRES_USER='    .env | cut -d= -f2- | tr -d '\r')
DB=$(grep -E '^POSTGRES_DB='       .env | cut -d= -f2- | tr -d '\r')

# total | active | idle_in_txn | drain_active (running a cross_shard_* stmt)
SQL_ACT="SELECT
 (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend'),
 (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND state='active'),
 (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND state='idle in transaction'),
 (SELECT count(*) FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid() AND query ILIKE '%cross_shard_outbox%');"

# wait-event histogram of active client backends, as one line
SQL_WAIT="SELECT string_agg(we||'='||c, ' ' ORDER BY c DESC) FROM (
  SELECT coalesce(wait_event_type,'(run)')||':'||coalesce(wait_event,'-') we, count(*) c
  FROM pg_stat_activity
  WHERE backend_type='client backend' AND state='active' AND pid<>pg_backend_pid()
  GROUP BY 1) s;"

# Who is BLOCKING lock-waiters right now: for every backend stuck on a Lock,
# name its blocker's state + a snippet of the blocker's current/last query.
# This says whether the lock holder is another drain chunk (UPDATE users from
# the apply) or the local consumer's batch apply — i.e. drain-vs-drain vs
# drain-vs-consumer contention.
SQL_BLOCK="SELECT string_agg(b.state||' :: '||left(regexp_replace(b.query,'\\s+',' ','g'),48), ' | ')
  FROM pg_stat_activity w
  JOIN LATERAL unnest(pg_blocking_pids(w.pid)) AS bp(pid) ON true
  JOIN pg_stat_activity b ON b.pid = bp.pid
  WHERE w.wait_event_type='Lock';"

pg_q() { # $1=port  $2=sql
  docker exec -e PGPASSWORD="$PW" peakload-pg-shard0-node-a \
    psql -h pg-haproxy -p "$1" -U "$USR" -d "$DB" -At -F '|' -c "$2" 2>&1 | tr -d '\r'
}

echo "probing -> $OUT  (${DUR}s @ ${INT}s)" | tee "$OUT"
END=$(( $(date +%s) + DUR ))
while [ "$(date +%s)" -lt "$END" ]; do
  TS=$(date -u +%H:%M:%S)
  for P in 5000 5001; do
    echo "[$TS pg@$P] ACT $(pg_q "$P" "$SQL_ACT")"    >> "$OUT"
    echo "[$TS pg@$P] WAIT $(pg_q "$P" "$SQL_WAIT")"  >> "$OUT"
    BLK="$(pg_q "$P" "$SQL_BLOCK")"
    [ -n "$BLK" ] && echo "[$TS pg@$P] BLOCK $BLK"    >> "$OUT"
  done
  sleep "$INT"
done
echo "DONE -> $OUT" | tee -a "$OUT"
