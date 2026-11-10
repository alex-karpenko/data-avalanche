#!/usr/bin/env bash
# An init script of `postgres-primary` and `postgres-sync-primary` (run once, by initdb): the
# image's pg_hba admits no replication connection over TCP, which the standby's base backup and
# WAL stream are. With STANDBY_SLOT set, the physical slot the standby streams through: the one
# `synchronized_standby_slots` names, so the primary's failover slots wait for the standby.
set -euo pipefail
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
if [ -n "${STANDBY_SLOT:-}" ]; then
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "SELECT pg_create_physical_replication_slot('$STANDBY_SLOT')"
fi
