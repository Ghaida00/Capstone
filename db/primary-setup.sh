#!/bin/bash
set -e

# Fix #6: Create replication_user with password from env var (never hardcoded).
# This script runs after init.sql (02- prefix) during postgres first-boot.
: "${REPL_PASSWORD:?REPL_PASSWORD must be set for primary setup}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replication_user') THEN
            CREATE ROLE replication_user WITH REPLICATION LOGIN PASSWORD '$REPL_PASSWORD';
        ELSE
            ALTER ROLE replication_user WITH PASSWORD '$REPL_PASSWORD';
        END IF;
    END
    \$\$;
EOSQL

# Configure pg_hba for replication
echo "host replication replication_user 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"
echo "host all all 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"

# Reload PostgreSQL config
pg_ctl reload -D "$PGDATA"
