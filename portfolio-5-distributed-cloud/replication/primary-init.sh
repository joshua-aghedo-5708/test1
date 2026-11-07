#!/bin/bash
# Runs once on first start of pg-primary: create the replication role and allow it in pg_hba.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'repl_dev';
SQL
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
