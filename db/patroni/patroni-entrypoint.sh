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
# Optional environment (safe defaults applied below if unset):
#   PG_SYNCHRONOUS_COMMIT   Durability posture rendered into
#                           the node's local PG parameters.
#                           Default: remote_write.
#   PG_COMMIT_DELAY         Group-commit nap, microseconds.
#                           Default: 0.
#   PG_COMMIT_SIBLINGS      commit_delay concurrency floor.
#                           Default: 5.
#   PG_WAL_COMPRESSION      Compress WAL full-page images so the
#                           log writes fewer bytes — trades CPU
#                           for disk write throughput.
#                           Default: off.
#   BACKUP_ENABLED          true|false (default false). When true,
#                           WAL archiving turns on and every
#                           completed segment is pushed to the
#                           pgBackRest repo described by the
#                           BACKUP_* vars below; base backups are
#                           then driven externally (host cron →
#                           scripts/backup-shard.sh). When false
#                           the node behaves byte-identically to a
#                           stack without the feature.
#   BACKUP_REPO_TYPE        s3 (default) or posix.
#   BACKUP_S3_ENDPOINT/_BUCKET/_REGION/_KEY/_KEY_SECRET
#                           Object-storage target; all five are
#                           required when BACKUP_ENABLED=true and
#                           repo type is s3.
#   BACKUP_CIPHER_PASS      Optional; when set the repo is
#                           encrypted at rest (aes-256-cbc).
#   BACKUP_POSIX_PATH       posix repo path (default
#                           /var/lib/pgbackrest) — for local
#                           testing without object storage.
#   BACKUP_RETENTION_FULL   Full backups retained (default 2);
#                           expired fulls take their WAL with them.
#   PG_ARCHIVE_TIMEOUT      Idle-RPO bound, seconds: a non-full
#                           WAL segment is force-switched and
#                           archived after this long. Default 300
#                           when backups are on; forced to 0 when
#                           off. Keep >= 60 — every switch uploads
#                           a segment.
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

# ------------------------------------------------------------
# Optional PG durability knobs — env-driven, with defaults.
# ------------------------------------------------------------
# Unlike the required vars above these have safe defaults, so
# a stack brought up without them in .env still boots. They
# render into the LOCAL `postgresql.parameters` block of the
# template (not bootstrap.dcs), which is what makes them take
# effect on every container recreate rather than only at the
# first cluster bootstrap.
# ------------------------------------------------------------
: "${PG_SYNCHRONOUS_COMMIT:=remote_write}"
: "${PG_COMMIT_DELAY:=0}"
: "${PG_COMMIT_SIBLINGS:=5}"
: "${PG_WAL_COMPRESSION:=off}"
: "${PG_MAX_CONNECTIONS:=120}"
: "${PG_SHARED_BUFFERS:=64MB}"
: "${PG_MAX_WAL_SIZE:=2GB}"
: "${PG_MIN_WAL_SIZE:=512MB}"
: "${PG_CHECKPOINT_TIMEOUT:=15min}"
: "${PG_CHECKPOINT_COMPLETION_TARGET:=0.9}"

# Fail fast on a typo'd posture — otherwise Postgres rejects
# the GUC with a less obvious error several layers down.
case "$PG_SYNCHRONOUS_COMMIT" in
    on|off|local|remote_write|remote_apply) ;;
    *)
        echo "[patroni-entrypoint] FATAL: PG_SYNCHRONOUS_COMMIT='$PG_SYNCHRONOUS_COMMIT'" \
             "must be one of: on off local remote_write remote_apply" >&2
        exit 1
        ;;
esac

# Fail fast on a bad WAL-compression method. lz4/zstd need a
# server built with that support — the bookworm postgres image
# this stage builds on has both.
case "$PG_WAL_COMPRESSION" in
    on|off|pglz|lz4|zstd) ;;
    *)
        echo "[patroni-entrypoint] FATAL: PG_WAL_COMPRESSION='$PG_WAL_COMPRESSION'" \
             "must be one of: on off pglz lz4 zstd" >&2
        exit 1
        ;;
esac

# Sanity-check PG_MAX_CONNECTIONS is a sensible integer; a
# typo here would surface as "FATAL: invalid value for
# parameter max_connections" deep in Patroni's startup log.
case "$PG_MAX_CONNECTIONS" in
    ''|*[!0-9]*)
        echo "[patroni-entrypoint] FATAL: PG_MAX_CONNECTIONS='$PG_MAX_CONNECTIONS'" \
             "must be a positive integer" >&2
        exit 1
        ;;
esac
if [ "$PG_MAX_CONNECTIONS" -lt 50 ] || [ "$PG_MAX_CONNECTIONS" -gt 1000 ]; then
    echo "[patroni-entrypoint] FATAL: PG_MAX_CONNECTIONS=$PG_MAX_CONNECTIONS" \
         "must be in range 50..1000 (pgBouncer + replication + admin headroom)" >&2
    exit 1
fi

# Sanity-check PG_SHARED_BUFFERS is in the PG-accepted form
# (positive integer + unit suffix, kB/MB/GB). Wrong format
# would surface as "FATAL: invalid value for parameter
# shared_buffers" deep in PG startup.
case "$PG_SHARED_BUFFERS" in
    *[0-9]kB|*[0-9]MB|*[0-9]GB) ;;
    *)
        echo "[patroni-entrypoint] FATAL: PG_SHARED_BUFFERS='$PG_SHARED_BUFFERS'" \
             "must be of the form '<N><unit>' where unit is kB, MB, or GB (e.g. 256MB)" >&2
        exit 1
        ;;
esac

# Sanity-check the WAL size ceilings use the PG size form
# (positive integer + kB/MB/GB), same failure mode as
# shared_buffers if mistyped. checkpoint_timeout /
# checkpoint_completion_target are passed through — PG rejects a
# bad value loudly at startup.
case "$PG_MAX_WAL_SIZE" in
    *[0-9]kB|*[0-9]MB|*[0-9]GB) ;;
    *)
        echo "[patroni-entrypoint] FATAL: PG_MAX_WAL_SIZE='$PG_MAX_WAL_SIZE'" \
             "must be of the form '<N><unit>' where unit is kB, MB, or GB (e.g. 4GB)" >&2
        exit 1
        ;;
esac
case "$PG_MIN_WAL_SIZE" in
    *[0-9]kB|*[0-9]MB|*[0-9]GB) ;;
    *)
        echo "[patroni-entrypoint] FATAL: PG_MIN_WAL_SIZE='$PG_MIN_WAL_SIZE'" \
             "must be of the form '<N><unit>' where unit is kB, MB, or GB (e.g. 512MB)" >&2
        exit 1
        ;;
esac

# ------------------------------------------------------------
# Backup / WAL-archiving toggle (BACKUP_ENABLED)
# ------------------------------------------------------------
# Resolves the BACKUP_* env vars into the PG_ARCHIVE_* values
# the template renders. Off (the default) means archive_mode=
# off and no pgbackrest.conf — zero archiver work, identical
# to a stack without the feature. On means archive_mode=on
# with pgbackrest archive-push as the archive_command, against
# a config rendered here.
#
# The operational footgun this block guards: archive_mode=on
# with a misconfigured repo makes every archive attempt fail,
# and Postgres then RETAINS unarchived WAL until the disk
# fills. Hence fail-fast on missing s3 settings instead of
# letting a half-configured node boot.
# ------------------------------------------------------------
: "${BACKUP_ENABLED:=false}"
: "${BACKUP_REPO_TYPE:=s3}"
: "${BACKUP_POSIX_PATH:=/var/lib/pgbackrest}"
: "${BACKUP_RETENTION_FULL:=2}"
: "${BACKUP_CIPHER_PASS:=}"

case "$BACKUP_ENABLED" in
    true|false) ;;
    *)
        echo "[patroni-entrypoint] FATAL: BACKUP_ENABLED='$BACKUP_ENABLED'" \
             "must be 'true' or 'false'" >&2
        exit 1
        ;;
esac

if [ "$BACKUP_ENABLED" = "true" ]; then
    : "${PG_ARCHIVE_TIMEOUT:=300}"
    PG_ARCHIVE_MODE=on
    PG_ARCHIVE_COMMAND="pgbackrest --stanza=${PATRONI_SCOPE} archive-push %p"

    BACKREST_CONF=/etc/pgbackrest/pgbackrest.conf
    case "$BACKUP_REPO_TYPE" in
        s3)
            : "${BACKUP_S3_ENDPOINT:?BACKUP_S3_ENDPOINT required when BACKUP_ENABLED=true (s3 repo)}"
            : "${BACKUP_S3_BUCKET:?BACKUP_S3_BUCKET required when BACKUP_ENABLED=true (s3 repo)}"
            : "${BACKUP_S3_REGION:?BACKUP_S3_REGION required when BACKUP_ENABLED=true (s3 repo)}"
            : "${BACKUP_S3_KEY:?BACKUP_S3_KEY required when BACKUP_ENABLED=true (s3 repo)}"
            : "${BACKUP_S3_KEY_SECRET:?BACKUP_S3_KEY_SECRET required when BACKUP_ENABLED=true (s3 repo)}"
            REPO_LINES="repo1-type=s3
repo1-s3-endpoint=${BACKUP_S3_ENDPOINT}
repo1-s3-bucket=${BACKUP_S3_BUCKET}
repo1-s3-region=${BACKUP_S3_REGION}
repo1-s3-key=${BACKUP_S3_KEY}
repo1-s3-key-secret=${BACKUP_S3_KEY_SECRET}
repo1-s3-uri-style=path
repo1-path=/peakload"
            ;;
        posix)
            REPO_LINES="repo1-type=posix
repo1-path=${BACKUP_POSIX_PATH}"
            ;;
        *)
            echo "[patroni-entrypoint] FATAL: BACKUP_REPO_TYPE='$BACKUP_REPO_TYPE'" \
                 "must be 's3' or 'posix'" >&2
            exit 1
            ;;
    esac

    CIPHER_LINES=""
    if [ -n "$BACKUP_CIPHER_PASS" ]; then
        CIPHER_LINES="repo1-cipher-type=aes-256-cbc
repo1-cipher-pass=${BACKUP_CIPHER_PASS}"
    fi

    echo "[patroni-entrypoint] BACKUP_ENABLED=true — rendering $BACKREST_CONF (repo: $BACKUP_REPO_TYPE, stanza: $PATRONI_SCOPE)"
    cat > "$BACKREST_CONF" <<EOF
# Rendered by patroni-entrypoint.sh on every container start.
# Do not edit in place — change the BACKUP_* vars in .env and
# recreate the node.
[global]
${REPO_LINES}
${CIPHER_LINES}
repo1-retention-full=${BACKUP_RETENTION_FULL}
compress-type=lz4
process-max=1
start-fast=y
log-level-console=info
log-level-file=detail
log-path=/var/log/pgbackrest

[${PATRONI_SCOPE}]
pg1-path=${PGDATA}
pg1-socket-path=/var/run/postgresql
pg1-user=postgres
EOF
    chown postgres:postgres "$BACKREST_CONF"
    chmod 0600 "$BACKREST_CONF"
else
    # Forced regardless of any PG_ARCHIVE_TIMEOUT in .env: a
    # timeout without archiving would switch segments for
    # nothing. /bin/true (not empty) so the rendered YAML stays
    # a valid scalar.
    PG_ARCHIVE_MODE=off
    PG_ARCHIVE_COMMAND=/bin/true
    PG_ARCHIVE_TIMEOUT=0
    rm -f /etc/pgbackrest/pgbackrest.conf
fi

export PGDATA PATRONI_SCOPE PATRONI_NAME PATRONI_HOSTNAME ETCD_HOSTS \
       POSTGRES_SUPERUSER_PASSWORD POSTGRES_USER POSTGRES_PASSWORD \
       POSTGRES_DB REPL_PASSWORD \
       PG_SYNCHRONOUS_COMMIT PG_COMMIT_DELAY PG_COMMIT_SIBLINGS \
       PG_WAL_COMPRESSION PG_MAX_CONNECTIONS PG_SHARED_BUFFERS \
       PG_MAX_WAL_SIZE PG_MIN_WAL_SIZE PG_CHECKPOINT_TIMEOUT \
       PG_CHECKPOINT_COMPLETION_TARGET \
       PG_ARCHIVE_MODE PG_ARCHIVE_COMMAND PG_ARCHIVE_TIMEOUT

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
    ${PG_SYNCHRONOUS_COMMIT}
    ${PG_COMMIT_DELAY}
    ${PG_COMMIT_SIBLINGS}
    ${PG_WAL_COMPRESSION}
    ${PG_MAX_CONNECTIONS}
    ${PG_SHARED_BUFFERS}
    ${PG_MAX_WAL_SIZE}
    ${PG_MIN_WAL_SIZE}
    ${PG_CHECKPOINT_TIMEOUT}
    ${PG_CHECKPOINT_COMPLETION_TARGET}
    ${PG_ARCHIVE_MODE}
    ${PG_ARCHIVE_COMMAND}
    ${PG_ARCHIVE_TIMEOUT}
' < /etc/patroni/patroni.yml.tmpl > "$RENDERED"

# ------------------------------------------------------------
# 2. Ensure PGDATA (and its parent) are owned by postgres.
# ------------------------------------------------------------
# This script runs as root specifically so we can fix volume
# ownership here. On Docker Desktop / WSL2 a freshly-created
# named volume lands as root-owned regardless of the image's
# mountpoint perms, which makes initdb fail with:
#     "could not change permissions of directory
#      /var/lib/postgresql/data: Operation not permitted"
# Chown is idempotent and cheap on a populated PGDATA.
#
# The compose mount sits at /var/lib/postgresql (the parent of
# PGDATA) to absorb the base image's inherited
# `VOLUME /var/lib/postgresql` declaration — without that,
# Docker would create a fresh anonymous volume for the parent
# on every recreate. The parent dir is also `postgres`'s HOME,
# so it needs to be owned by postgres for tools that touch
# HOME (psql history, etc.). Non-recursive on the parent to
# stay cheap; -R on PGDATA only.
# ------------------------------------------------------------
echo "[patroni-entrypoint] Ensuring $PGDATA owned by postgres"
chown postgres:postgres /var/lib/postgresql
mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"

# ------------------------------------------------------------
# 3. Hand off to Patroni as the postgres user.
# ------------------------------------------------------------
# `exec gosu` so Patroni becomes PID 1 in the container and
# receives SIGTERM directly from Docker on stack shutdown.
# That in turn lets Patroni do an orderly pg_ctl stop -m fast
# of Postgres. gosu drops privileges cleanly without the
# signal-forwarding pitfalls of su/sudo.
# ------------------------------------------------------------
echo "[patroni-entrypoint] Launching patroni as postgres..."
exec gosu postgres patroni "$RENDERED"
