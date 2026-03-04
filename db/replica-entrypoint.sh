#!/bin/sh
set -e

PGDATA=/var/lib/postgresql/data
# PRIMARY_HOST is passed via environment variable (e.g., pg-shard0-primary)
PRIMARY_HOST=${PRIMARY_HOST:-postgres-primary}

# If data directory is empty, do a base backup from primary
if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
    echo "Replica: Initializing from primary ($PRIMARY_HOST) via pg_basebackup..."
    
    until pg_isready -h "$PRIMARY_HOST" -p 5432 -U gn_user; do
        echo "Waiting for primary ($PRIMARY_HOST) to be ready..."
        sleep 2
    done

    # pg_basebackup needs the replication user's password
    PGPASSWORD=repl_secure_pass gosu postgres pg_basebackup \
        -h "$PRIMARY_HOST" \
        -p 5432 \
        -U replication_user \
        -D "$PGDATA" \
        -Fp -Xs -P -R

    # Ensure standby.signal exists (PostgreSQL 12+)
    gosu postgres touch "$PGDATA/standby.signal"
    
    # Configure replica connection
    gosu postgres sh -c "cat >> '$PGDATA/postgresql.auto.conf' <<EOF
primary_conninfo = 'host=$PRIMARY_HOST port=5432 user=replication_user password=repl_secure_pass'
hot_standby = on
EOF"
    
    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
fi

echo "Replica: Starting PostgreSQL in standby mode (primary=$PRIMARY_HOST)..."
exec gosu postgres postgres \
    -D "$PGDATA" \
    -c hot_standby=on \
    -c shared_buffers=256MB \
    -c effective_cache_size=768MB \
    -c max_connections=300
