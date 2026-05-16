#!/bin/sh
# Regression test for D-1/R-3 (WP1b). Spins up a throwaway Postgres +
# the project's exact pgBouncer image, runs the REAL bootstrap-schema.sh
# against it, then asserts statement_timeout is enforced THROUGH
# pgBouncer (SQLSTATE 57014). The reverted WP1 would fail this because
# pgBouncer rejects the libpq `options` startup parameter.
#
# Exit 0 = pass. Any other exit = fail. Self-cleaning.
set -eu

NET=wp1btest-net
PG=wp1btest-pg
PGB=wp1btest-pgb
PGIMG=postgres:18.3-alpine3.23
PGBIMG=edoburu/pgbouncer:v1.25.1-p0
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

cleanup() { docker rm -f "$PGB" "$PG" >/dev/null 2>&1 || true
            docker network rm "$NET" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

docker network create "$NET" >/dev/null
docker run -d --name "$PG" --network "$NET" \
  -e POSTGRES_USER=peakload_user -e POSTGRES_PASSWORD=peakload_pass \
  -e POSTGRES_DB=peakload_db "$PGIMG" >/dev/null

i=0; until docker exec "$PG" pg_isready -U peakload_user -d peakload_db -q 2>/dev/null; do
  i=$((i+1)); [ "$i" -ge 30 ] && { echo "FAIL: postgres never ready"; exit 1; }; sleep 1; done

docker run --rm --network "$NET" \
  -e POSTGRES_USER=peakload_user -e POSTGRES_PASSWORD=peakload_pass \
  -e POSTGRES_DB=peakload_db \
  -e PG_HAPROXY_HOST="$PG" -e PG_HAPROXY_PORTS=5432 \
  -e DB_STATEMENT_TIMEOUT_MS=500 -e DB_LOCK_TIMEOUT_MS=500 \
  -e DB_IDLE_IN_TX_TIMEOUT_MS=5000 \
  -v "$SCRIPT_DIR/bootstrap-schema.sh:/opt/bootstrap-schema.sh:ro" \
  -v "$SCRIPT_DIR/../init.sql:/sql/init.sql:ro" \
  --entrypoint /bin/sh "$PGIMG" /opt/bootstrap-schema.sh

docker run -d --name "$PGB" --network "$NET" \
  -e DATABASE_URL="postgres://peakload_user:peakload_pass@$PG:5432/peakload_db" \
  -e AUTH_TYPE=scram-sha-256 -e POOL_MODE=transaction "$PGBIMG" >/dev/null

i=0; until docker run --rm --network "$NET" "$PGIMG" \
  pg_isready -h "$PGB" -p 5432 -U peakload_user -q 2>/dev/null; do
  i=$((i+1)); [ "$i" -ge 30 ] && { echo "FAIL: pgbouncer never ready"; exit 1; }; sleep 1; done

PSQL="docker run --rm --network $NET -e PGPASSWORD=peakload_pass $PGIMG \
  psql -h $PGB -p 5432 -U peakload_user -d peakload_db -tA"

SHOWN=$($PSQL -c "SHOW statement_timeout" | tr -d '[:space:]')
echo "statement_timeout through pgBouncer = '$SHOWN'"
[ "$SHOWN" != "0" ] || { echo "FAIL: statement_timeout is 0 through pgBouncer"; exit 1; }

$PSQL -c "BEGIN; SELECT 1; COMMIT;" >/dev/null 2>&1 || true
OUT=$($PSQL -c "SELECT pg_sleep(2)" 2>&1 || true)
echo "$OUT" | grep -q "canceling statement due to statement timeout" \
  || { echo "FAIL: pg_sleep(2) not cancelled through pgBouncer; got: $OUT"; exit 1; }

echo "PASS: statement_timeout enforced through pgBouncer transaction pool"
