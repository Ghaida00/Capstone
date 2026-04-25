#!/bin/sh
# ============================================================
# Patroni node entrypoint
# ============================================================
#
# Single entrypoint used by every Postgres NODE in the stack.
# Reads a handful of env vars, renders the Patroni YAML
# config from a template, and execs Patroni which then owns
# the PG process for the rest of the container's life.
#
# Required environment (set per-service in docker-compose.yml):
#   PGDATA                Absolute path to PG data dir (volume)
#   PATRONI_SCOPE         Cluster scope = per-shard label
#                         (e.g. peakload-shard0). Every node of
#                         a given shard MUST share the same
#                         scope so they converge via etcd.
#   PATRONI_NAME          Unique node name within the scope
#                         (e.g. shard0-a, shard0-b). Shows up
#                         in `patronictl list` output.
#   PATRONI_HOSTNAME      Docker service name this container
#                         is reachable at from the other node
#                         and from HAProxy (equals the compose
#                         service name, e.g. pg-shard0-node-a).
#   ETCD_HOSTS            Comma-separated etcd endpoints.
#                         Typically etcd-1:2379,etcd-2:2379,
#                         etcd-3:2379.
#   POSTGRES_SUPERUSER_PASSWORD
#                         Password for the `postgres` role
#                         that Patroni will create + use for
#                         its own admin queries.
#   POSTGRES_USER         App DB user created on bootstrap
#                         by Patroni's `users:` block.
#   POSTGRES_PASSWORD     App DB user password.
#   POSTGRES_DB           App database (created by the
#                         post_bootstrap hook; see the
#                         template).
#   REPL_PASSWORD         Password for the `replicator` role
#                         Patroni creates for streaming
#                         replication between the two nodes
#                         of a shard.
#
# The template at /etc/patroni/patroni.yml.tmpl is rendered
# once per container start via `envsubst` and written to
# /var/lib/patroni/patroni.yml. Re-renders on restart are
# cheap and idempotent — Patroni re-reads the config each
# time it boots.
# ============================================================

set -eu

: "${PGDATA:?PGDATA must be set}"
: "${PATRONI_SCOPE:?PATRONI_SCOPE must be set (e.g. peakload-shard0)}"
: "${PATRONI_NAME:?PATRONI_NAME must be set (e.g. shard0-a)}"
: "${PATRONI_HOSTNAME:?PATRONI_HOSTNAME must be set (docker service name)}"
: "${ETCD_HOSTS:?ETCD_HOSTS must be set (comma-separated)}"
: "${POSTGRES_SUPERUSER_PASSWORD:?POSTGRES_SUPERUSER_PASSWORD must be set}"
: "${POSTGRES_USER:?POSTGRES_USER must be set}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set}"
: "${POSTGRES_DB:?POSTGRES_DB must be set}"
: "${REPL_PASSWORD:?REPL_PASSWORD must be set}"

export PGDATA PATRONI_SCOPE PATRONI_NAME PATRONI_HOSTNAME ETCD_HOSTS \
       POSTGRES_SUPERUSER_PASSWORD POSTGRES_USER POSTGRES_PASSWORD \
       POSTGRES_DB REPL_PASSWORD

# ------------------------------------------------------------
# 1. Render the Patroni config from the template.
# ------------------------------------------------------------
# envsubst only substitutes the vars we explicitly list, which
# prevents YAML `$(foo)` or `$foo` fragments from being eaten
# accidentally (Patroni's template does not use any such, but
# being explicit is defensive and cheap).
# ------------------------------------------------------------
RENDERED=/var/lib/patroni/patroni.yml
echo "[patroni-entrypoint] Rendering config → $RENDERED"
envsubst '
    ${PGDATA}
    ${PATRONI_SCOPE}
    ${PATRONI_NAME}
    ${PATRONI_HOSTNAME}
    ${ETCD_HOSTS}
    ${POSTGRES_SUPERUSER_PASSWORD}
    ${POSTGRES_USER}
    ${POSTGRES_PASSWORD}
    ${POSTGRES_DB}
    ${REPL_PASSWORD}
' < /etc/patroni/patroni.yml.tmpl > "$RENDERED"

# ------------------------------------------------------------
# 2. Hand off to Patroni.
# ------------------------------------------------------------
# `exec` so Patroni becomes PID 1 in the container and receives
# SIGTERM directly from Docker on stack shutdown. That in turn
# lets Patroni do an orderly pg_ctl stop -m fast of Postgres.
# ------------------------------------------------------------
echo "[patroni-entrypoint] Launching patroni..."
exec patroni "$RENDERED"
