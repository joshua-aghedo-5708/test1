#!/bin/bash
# Hot-standby bootstrap: clone the primary with pg_basebackup (-R writes standby.signal
# and primary_conninfo), create a physical replication slot, then start in recovery mode.
set -e
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -h pg-primary -U nairaflow -q; do echo "waiting for primary"; sleep 1; done
  sleep 3   # let the primary finish its init scripts
  export PGPASSWORD=repl_dev
  until pg_basebackup -h pg-primary -U replicator -D "$PGDATA" -R -X stream -C -S "$SLOT" -P; do
    echo "basebackup failed, retrying"; rm -rf "$PGDATA"/*; sleep 2
  done
  echo "primary_conninfo = 'host=pg-primary user=replicator password=repl_dev application_name=$APP_NAME'" >> "$PGDATA/postgresql.auto.conf"
  echo "primary_slot_name = '$SLOT'" >> "$PGDATA/postgresql.auto.conf"
  chmod 700 "$PGDATA"
fi
exec postgres -c hot_standby=on -c hot_standby_feedback=on
