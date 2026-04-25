#!/bin/sh
# ============================================================
# Patroni post_bootstrap hook
# ============================================================
#
# Runs exactly ONCE, on the first primary of each shard scope,
# after Patroni has:
#   1. Executed initdb
#   2. Created the users declared under `bootstrap.users` in
#      patroni.yml.tmpl (the app user)
#   3. Applied the bootstrap.pg_hba entries
# …and BEFORE the node reports itself to etcd as healthy, so
# HAProxy will only start routing traffic here once this exits 0.
#
# The sole responsibility of this script is to create the
# application database owned by ${POSTGRES_USER}. Patroni's
# declarative `users:` block does not create databases — this
# gap is what the hook fills.
#
# Schema inside that database (tables, indexes, triggers) is
# applied later by db/bootstrap/bootstrap-schema.sh, which
# talks to pg-haproxy rather than to any particular node and
# is therefore HA-tool-agnostic. Keeping "create DB" here and
# "create tables" there is intentional: the DB exists for the
# lifetime of the cluster, while tables are in the normal
# migration / schema-management lane.
#
# The hook receives Postgres connection args as positional
# parameters from Patroni (already wired to the local node's
# superuser on the unix socket), so psql just needs `-c`.
# ============================================================

set -eu

: "${POSTGRES_DB:?POSTGRES_DB must be set}"
: "${POSTGRES_USER:?POSTGRES_USER must be set}"

echo "[post-bootstrap] Creating database ${POSTGRES_DB} owned by ${POSTGRES_USER} (if missing)..."

# Patroni's post_bootstrap hook is invoked by calling the
# script with psql-style args pre-appended. That means $@
# contains things like `-h /tmp -d postgres` already. We can
# piggyback on it to talk to the local cluster.
psql "$@" <<SQL
SELECT 'CREATE DATABASE ${POSTGRES_DB} OWNER ${POSTGRES_USER}'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = '${POSTGRES_DB}'
)\gexec
SQL

echo "[post-bootstrap] Done."
