#!/bin/sh
# ============================================================
# One-shot schema applier — HA-tool agnostic
# ============================================================
#
# Patroni (like the pg_auto_failover setup that preceded it)
# manages its nodes through its own entrypoint and bypasses
# PostgreSQL's stock `/docker-entrypoint-initdb.d/` mechanism,
# so our `init.sql` never auto-runs on fresh data dirs.
#
# This job fills that gap. It is idempotent: waits until each
# shard's HAProxy frontend is answering, checks whether the
# schema has already been applied (via a canary object), and
# applies `init.sql` only if missing.
#
# The container uses `restart: "no"` and exits after success;
# the `app` service depends on it with
# `condition: service_completed_successfully` so the API
# never runs against an unpopulated database.
#
# Required environment:
#   POSTGRES_USER
#   POSTGRES_PASSWORD
#   POSTGRES_DB
#   PG_HAPROXY_HOST    Usually `pg-haproxy`
#   PG_HAPROXY_PORTS   Space-separated shard primary ports,
#                      e.g. "5000 5001 5002"
#
# This script lives under `db/bootstrap/` rather than
# `db/patroni/` precisely because it is **not** tied to the
# HA orchestrator — it talks only to pg-haproxy. If we ever
# swap Patroni for something else, this file does not move.
# ============================================================

set -eu

: "${POSTGRES_USER:?}"
: "${POSTGRES_PASSWORD:?}"
: "${POSTGRES_DB:?}"
: "${PG_HAPROXY_HOST:=pg-haproxy}"
: "${PG_HAPROXY_PORTS:=5000 5001 5002}"

export PGPASSWORD="$POSTGRES_PASSWORD"

INIT_SQL=/sql/init.sql

# Canary: a table created by init.sql. If it already exists,
# we know the schema has been applied — skip re-running.
CANARY_TABLE='transactions'

wait_for_primary() {
    port=$1
    # HAProxy only marks a backend UP when the node returns HTTP
    # 200 on /primary (see haproxy.cfg). So once the frontend
    # accepts TCP AND pg_isready returns 0, we are truly talking
    # to a writable primary.
    echo "[bootstrap-schema] Waiting for writable primary on ${PG_HAPROXY_HOST}:${port}..."
    attempts=0
    max_attempts=60
    until pg_isready -h "$PG_HAPROXY_HOST" -p "$port" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge "$max_attempts" ]; then
            echo "[bootstrap-schema] ERROR: no primary on port $port after ${max_attempts}s" >&2
            return 1
        fi
        sleep 1
    done
    echo "[bootstrap-schema] Primary up on ${PG_HAPROXY_HOST}:${port}."
}

schema_already_applied() {
    port=$1
    # to_regclass returns NULL when the object doesn't exist.
    # We count non-null results.
    present=$(psql -h "$PG_HAPROXY_HOST" -p "$port" \
                   -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
                   -tA -w \
                   -c "SELECT to_regclass('public.${CANARY_TABLE}') IS NOT NULL" 2>/dev/null || echo "f")
    [ "$present" = "t" ]
}

apply_schema() {
    port=$1
    echo "[bootstrap-schema] Applying ${INIT_SQL} to ${PG_HAPROXY_HOST}:${port}..."
    psql -h "$PG_HAPROXY_HOST" -p "$port" \
         -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
         -v ON_ERROR_STOP=1 \
         -w \
         -f "$INIT_SQL"
    echo "[bootstrap-schema] Schema applied on port ${port}."
}

# ------------------------------------------------------------
# Main loop — one pass per shard.
# ------------------------------------------------------------
for port in $PG_HAPROXY_PORTS; do
    wait_for_primary "$port"

    if schema_already_applied "$port"; then
        echo "[bootstrap-schema] Shard on port ${port} already has schema — skipping."
    else
        apply_schema "$port"
    fi
done

echo "[bootstrap-schema] All shards ready."
