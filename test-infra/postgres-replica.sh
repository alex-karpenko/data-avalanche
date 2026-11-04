#!/usr/bin/env bash
# Entrypoint of `postgres-replica`: a streaming standby of `postgres-primary`, made from its base
# backup at the first start (-R: standby.signal and primary_conninfo), then PostgreSQL with the
# compose anchor's command ("$@"). Its configuration and pg_hba come with the backup.
set -euo pipefail
export PGPASSWORD=avalanche
data=/var/lib/postgresql/standby
until pg_isready -q -h postgres-primary -U avalanche -d avalanche; do sleep 1; done
mkdir -p "$data"
chown postgres:postgres "$data"
chmod 700 "$data"
if [ ! -s "$data/PG_VERSION" ]; then
  gosu postgres pg_basebackup -h postgres-primary -U avalanche -D "$data" -R -X stream -c fast
fi
[ $# -gt 0 ] && shift
exec gosu postgres postgres -D "$data" "$@"
