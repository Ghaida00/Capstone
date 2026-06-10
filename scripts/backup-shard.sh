#!/usr/bin/env bash
# ============================================================
# backup-shard.sh — run a pgBackRest base backup for one shard
# ============================================================
#
# Host-side wrapper around `pgbackrest backup`, meant to be
# driven by cron on the deploy host (NOT inside a container).
# pgBackRest must read PGDATA files directly, so the backup
# executes inside whichever PG container is currently the
# shard's PRIMARY — this script asks each node's Patroni REST
# API (GET /primary → 200 only on the leader) and execs there.
#
# Requires BACKUP_ENABLED=true in .env (the entrypoint renders
# /etc/pgbackrest/pgbackrest.conf and turns archiving on; this
# script fails with "config missing" when the toggle is off).
#
# Usage:
#   scripts/backup-shard.sh <shard0|shard1|shard2> [full|diff|incr]
#
# Type defaults to diff. The first backup of a fresh stanza is
# automatically taken as full regardless of the requested type
# (pgBackRest upgrades it); stanza-create is idempotent and
# cheap, so it runs unconditionally first.
#
# Suggested crontab on the cloud host (stagger the shards so
# their I/O doesn't overlap; adjust to the active topology):
#   0 3 * * 0   /path/to/scripts/backup-shard.sh shard0 full
#   0 3 * * 1-6 /path/to/scripts/backup-shard.sh shard0 diff
#   20 3 * * 0  /path/to/scripts/backup-shard.sh shard1 full
#   20 3 * * 1-6 /path/to/scripts/backup-shard.sh shard1 diff
#
# Verify afterwards:
#   docker exec peakload-pg-<shard>-node-<x> gosu postgres \
#     pgbackrest --stanza=peakload-<shard> info
# ============================================================

set -euo pipefail

SHARD="${1:?usage: backup-shard.sh <shard0|shard1|shard2> [full|diff|incr]}"
TYPE="${2:-diff}"

case "$SHARD" in
    shard0|shard1|shard2) ;;
    *) echo "FATAL: unknown shard '$SHARD' (expected shard0|shard1|shard2)" >&2; exit 1 ;;
esac
case "$TYPE" in
    full|diff|incr) ;;
    *) echo "FATAL: unknown backup type '$TYPE' (expected full|diff|incr)" >&2; exit 1 ;;
esac

STANZA="peakload-${SHARD}"

# Find the current primary: Patroni answers 200 on GET /primary
# only on the leader. Probed via python3 (which Patroni itself
# runs on) because the image ships neither curl nor wget; run
# from inside each container so it works without the REST port
# published to the host. urlopen raises on Patroni's 503, so a
# replica exits nonzero.
PRIMARY=""
for node in a b; do
    CONTAINER="peakload-pg-${SHARD}-node-${node}"
    if docker exec "$CONTAINER" python3 -c \
        "import urllib.request; urllib.request.urlopen('http://localhost:8008/primary', timeout=3)" \
        2>/dev/null; then
        PRIMARY="$CONTAINER"
        break
    fi
done

if [ -z "$PRIMARY" ]; then
    echo "FATAL: no primary found for ${SHARD} (are the containers up and healthy?)" >&2
    exit 1
fi

echo "[backup-shard] ${SHARD}: primary is ${PRIMARY}; running ${TYPE} backup (stanza ${STANZA})"

# stanza-create is a no-op when the stanza already exists.
docker exec "$PRIMARY" gosu postgres pgbackrest --stanza="$STANZA" stanza-create

docker exec "$PRIMARY" gosu postgres pgbackrest --stanza="$STANZA" --type="$TYPE" backup

echo "[backup-shard] ${SHARD}: ${TYPE} backup complete"
docker exec "$PRIMARY" gosu postgres pgbackrest --stanza="$STANZA" info
