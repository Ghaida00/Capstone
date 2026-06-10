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
#                      e.g. "5000 5001" (shard 2 currently
#                      disabled; restore "5002" when shard 2
#                      is re-enabled).
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
# Shard 2 disabled — was "5000 5001 5002". The compose file
# already overrides PG_HAPROXY_PORTS, so this default only
# matters for ad-hoc invocations.
: "${PG_HAPROXY_PORTS:=5000 5001}"

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
    #
    # This loop is the REAL gate for "the primary is ready": pg-haproxy
    # now starts on `service_started` (not `service_healthy`) of the PG
    # nodes, so HAProxy — and therefore this job — can come up well
    # before Patroni has bootstrapped a writable primary. 180s covers a
    # cold `down -v` bootstrap of the larger high-memory profiles.
    echo "[bootstrap-schema] Waiting for writable primary on ${PG_HAPROXY_HOST}:${port}..."
    attempts=0
    max_attempts=180
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
# Server-side statement/lock/idle-in-tx timeouts (D-1/R-3).
#
# Set as DATABASE-level defaults via ALTER DATABASE so they are
# inherited by every backend the app opens — including via
# pgBouncer in transaction pooling mode, where libpq startup
# `options` are rejected and session `SET` is wiped by
# DISCARD ALL between transactions. RESET ALL returns a GUC to
# its default, and ALTER DATABASE IS that default, so the
# timeout survives pooling. Replicated to standbys via WAL, so
# the direct-connected read replicas inherit it too.
#
# Runs UNCONDITIONALLY every bootstrap pass (NOT gated by the
# canary like the schema) because ALTER DATABASE ... SET is
# naturally idempotent and must take effect on already-populated
# clusters where apply_schema is skipped.
# ------------------------------------------------------------
: "${DB_STATEMENT_TIMEOUT_MS:=2000}"
: "${DB_LOCK_TIMEOUT_MS:=500}"
: "${DB_IDLE_IN_TX_TIMEOUT_MS:=5000}"

apply_db_timeouts() {
    port=$1
    echo "[bootstrap-schema] Applying DB timeout defaults to ${PG_HAPROXY_HOST}:${port} (statement=${DB_STATEMENT_TIMEOUT_MS}ms lock=${DB_LOCK_TIMEOUT_MS}ms idle_in_tx=${DB_IDLE_IN_TX_TIMEOUT_MS}ms)..."
    psql -h "$PG_HAPROXY_HOST" -p "$port" \
         -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
         -v ON_ERROR_STOP=1 -w \
         -c "ALTER DATABASE \"${POSTGRES_DB}\" SET statement_timeout = '${DB_STATEMENT_TIMEOUT_MS}'" \
         -c "ALTER DATABASE \"${POSTGRES_DB}\" SET lock_timeout = '${DB_LOCK_TIMEOUT_MS}'" \
         -c "ALTER DATABASE \"${POSTGRES_DB}\" SET idle_in_transaction_session_timeout = '${DB_IDLE_IN_TX_TIMEOUT_MS}'"
    echo "[bootstrap-schema] DB timeout defaults applied on port ${port}."
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

    # Always (re)assert the DB-level timeout defaults — idempotent,
    # and must run even when the schema step is skipped.
    apply_db_timeouts "$port"
done

echo "[bootstrap-schema] All shards ready."
