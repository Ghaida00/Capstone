#!/usr/bin/env bash
# Read-side mechanism probe: WHY does GET /balance have a 104ms p95 tail
# (p50 is 2ms) — i.e. the cache-MISS reads that hit the replica are slow.
#
# The live read path is Tier 1: app reader pool (DB_READ_POOL_SIZE=80 per
# shard × 2 app instances = up to 160 conns) -> HAProxy /replica frontend
# (:5010 shard0, :5011 shard1) -> the replica node directly. No read
# pgBouncer multiplexing, and the replica's max_connections is 120.
#
# Samples each replica (via the /replica frontend) every INTERVAL sec:
#   ACT  total | active | idle    client backends on the replica.
#     - total pinned near ~100-120 = replica connection-saturated (Tier-1
#       demand 160 > max_connections 120, no pool buffer).
#   WAIT <event>=<n> ...   wait-event histogram of ACTIVE backends.
#     - IO:* / LWLock:*  -> replica CPU/IO bound serving the read load.
#     - mostly (run)/ClientRead -> not DB-bound; the wait is upstream
#       (app reader-pool acquire / HAProxy), i.e. pure connection pressure.
#   LAG  <seconds>   replica replay lag (now - last replayed xact).
#     - climbing -> replication apply is competing with reads.
#   QMIX status=<n> balance=<n> list=<n> other=<n>   what the active reads
#     ARE, so we can attribute the load:
#     - status-dominated -> the uncached transient status polls (our cache
#       fix) fanning out to both replicas are the driver
#       => re-add a short (1s) transient status cache.
#     - balance/list-dominated -> genuine read demand
#       => add read capacity (enable Option B read pgBouncer / raise replica
#          max_connections / lower DB_READ_POOL_SIZE to fit 2*N <= 120).
#
# Run alongside a SHORT k6 load (~5-6 min). Usage:
#   sample-read.sh [DURATION_SEC] [INTERVAL_SEC]   (default 360 / 5)
set -uo pipefail
cd "$(dirname "$0")/.."

DUR="${1:-360}"; INT="${2:-5}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="diag/read-${STAMP}.log"

PW=$(grep -E '^POSTGRES_PASSWORD=' .env | cut -d= -f2- | tr -d '\r')
USR=$(grep -E '^POSTGRES_USER='    .env | cut -d= -f2- | tr -d '\r')
DB=$(grep -E '^POSTGRES_DB='       .env | cut -d= -f2- | tr -d '\r')

# total | active | idle client backends on this replica.
SQL_ACT="SELECT
 (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend'),
 (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND state='active'),
 (SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND state='idle');"

SQL_WAIT="SELECT string_agg(we||'='||c, ' ' ORDER BY c DESC) FROM (
  SELECT coalesce(wait_event_type,'(run)')||':'||coalesce(wait_event,'-') we, count(*) c
  FROM pg_stat_activity
  WHERE backend_type='client backend' AND state='active' AND pid<>pg_backend_pid()
  GROUP BY 1) s;"

# Replay lag in BYTES (received WAL not yet replayed). 0 = caught up,
# regardless of write activity — unlike a timestamp delta, which just grows
# when the primary is idle. A growing value under load = replay falling
# behind, which can stall/conflict reads on the hot standby.
SQL_LAG="SELECT coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::bigint, 0);"

# Classify the active reads by their query shape so the load can be
# attributed: status fan-out vs balance point-read vs transaction list.
SQL_QMIX="SELECT
  count(*) FILTER (WHERE query ILIKE '%from transactions where reference_id%'),
  count(*) FILTER (WHERE query ILIKE '%from users where account_number%'),
  count(*) FILTER (WHERE query ILIKE '%from transactions%order by%'),
  count(*) FILTER (WHERE query NOT ILIKE '%from transactions where reference_id%'
                     AND query NOT ILIKE '%from users where account_number%'
                     AND query NOT ILIKE '%from transactions%order by%')
  FROM pg_stat_activity
  WHERE backend_type='client backend' AND state='active' AND pid<>pg_backend_pid();"

pg_q() { # $1=port  $2=sql
  docker exec -e PGPASSWORD="$PW" peakload-pg-shard0-node-a \
    psql -h pg-haproxy -p "$1" -U "$USR" -d "$DB" -At -F '|' -c "$2" 2>&1 | tr -d '\r'
}

echo "probing replicas -> $OUT  (${DUR}s @ ${INT}s)" | tee "$OUT"
END=$(( $(date +%s) + DUR ))
while [ "$(date +%s)" -lt "$END" ]; do
  TS=$(date -u +%H:%M:%S)
  # 5010 = shard0 replica, 5011 = shard1 replica (/replica frontends)
  for P in 5010 5011; do
    echo "[$TS rep@$P] ACT  $(pg_q "$P" "$SQL_ACT")"    >> "$OUT"
    echo "[$TS rep@$P] WAIT $(pg_q "$P" "$SQL_WAIT")"   >> "$OUT"
    echo "[$TS rep@$P] LAG  $(pg_q "$P" "$SQL_LAG")B"   >> "$OUT"
    echo "[$TS rep@$P] QMIX status|balance|list|other = $(pg_q "$P" "$SQL_QMIX")" >> "$OUT"
  done
  sleep "$INT"
done
echo "DONE -> $OUT" | tee -a "$OUT"
