#!/bin/bash
set -e

# Configure pg_hba for replication
echo "host replication replication_user 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"
echo "host all all 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"

# Reload PostgreSQL config
pg_ctl reload -D "$PGDATA"
