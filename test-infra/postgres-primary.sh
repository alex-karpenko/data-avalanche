#!/usr/bin/env bash
# An init script of `postgres-primary` (run once, by initdb): the image's pg_hba admits no
# replication connection over TCP, which `postgres-replica`'s base backup and WAL stream are.
set -euo pipefail
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
