#!/bin/sh
set -e

PGDATA=/var/lib/postgresql/data
# PRIMARY_HOST is passed via environment variable (e.g., pg-shard0-primary)
PRIMARY_HOST=${PRIMARY_HOST:-postgres-primary}
# Fix #6: Use env var for replication password instead of hardcoded value
REPL_PASSWORD=${REPL_PASSWORD:-repl_secure_pass}

# If data directory is empty, do a base backup from primary
if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
    echo "Replica: Initializing from primary ($PRIMARY_HOST) via pg_basebackup..."

    # Ensure data directory ownership before pg_basebackup writes to it.
    # Docker volumes are created as root; pg_basebackup runs as postgres.
    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"

    until pg_isready -h "$PRIMARY_HOST" -p 5432 -U peakload_user; do
        echo "Waiting for primary ($PRIMARY_HOST) to be ready..."
        sleep 2
    done

    # pg_basebackup needs the replication user's password
    # Run as postgres user — directory is already owned by postgres
    PGPASSWORD="$REPL_PASSWORD" gosu postgres pg_basebackup \
        -h "$PRIMARY_HOST" \
        -p 5432 \
        -U replication_user \
        -D "$PGDATA" \
        -Fp -Xs -P -R

    # Ensure standby.signal exists (PostgreSQL 12+)
    gosu postgres touch "$PGDATA/standby.signal"

    # Configure replica connection
    gosu postgres sh -c "cat >> '$PGDATA/postgresql.auto.conf' <<EOF
primary_conninfo = 'host=$PRIMARY_HOST port=5432 user=replication_user password=$REPL_PASSWORD'
hot_standby = on
EOF"

    # Final ownership guarantee
    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
fi

# Fix #11: Match primary's memory settings to avoid OOM inside 256MB container
echo "Replica: Starting PostgreSQL in standby mode (primary=$PRIMARY_HOST)..."
exec gosu postgres postgres \
    -D "$PGDATA" \
    -c hot_standby=on \
    -c shared_buffers=64MB \
    -c effective_cache_size=192MB \
    -c max_connections=300
