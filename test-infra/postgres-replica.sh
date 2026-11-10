#!/usr/bin/env bash
# Entrypoint of `postgres-replica` and `postgres-sync-replica`: a streaming standby of PRIMARY
# (default `postgres-primary`), made from its base backup at the first start (-R: standby.signal
# and primary_conninfo), then PostgreSQL with the service's command ("$@"). Its configuration and
# pg_hba come with the backup. With STANDBY_SLOT set, it streams through that physical slot, and
# primary_conninfo names the database, which slot sync (`sync_replication_slots`) needs.
set -euo pipefail
export PGPASSWORD=avalanche
primary=${PRIMARY:-postgres-primary}
data=/var/lib/postgresql/standby
until pg_isready -q -h "$primary" -U avalanche -d avalanche; do sleep 1; done
mkdir -p "$data"
chown postgres:postgres "$data"
chmod 700 "$data"
if [ ! -s "$data/PG_VERSION" ]; then
  if [ -n "${STANDBY_SLOT:-}" ]; then
    gosu postgres pg_basebackup -d "host=$primary user=avalanche dbname=avalanche" -D "$data" -R \
      -X stream -c fast -S "$STANDBY_SLOT"
  else
    gosu postgres pg_basebackup -h "$primary" -U avalanche -D "$data" -R -X stream -c fast
  fi
fi
[ $# -gt 0 ] && shift
exec gosu postgres postgres -D "$data" "$@"
