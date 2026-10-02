#!/usr/bin/env bash
# Entrypoint of the `postgres-tls` service: a fresh throwaway CA, server certificate (localhost
# only, so a connection by 127.0.0.1 is a name mismatch) and client certificate (CN
# avalanche_cert) at every start, then PostgreSQL with TLS required for every TCP connection. The
# CA and the client pair are copied to /tls (bind-mounted from the git-ignored test-infra/.tls)
# for the tests; the server key stays in the container.
set -euo pipefail
server=/var/lib/postgresql/tls
mkdir -p "$server" /tls
cd "$server"
rm -f ./*
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 30 \
  -subj "/CN=data-avalanche test CA" -keyout ca.key -out ca.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=localhost" \
  -keyout server.key -out server.csr
printf 'subjectAltName=DNS:localhost\n' > server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 \
  -extfile server.ext -out server.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=avalanche_cert" \
  -keyout client.key -out client.csr
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 -out client.crt
chown postgres:postgres server.key && chmod 600 server.key
# 644: on a Linux host the bind-mounted files are root-owned, and the tests read them as another
# user. Throwaway material, granting nothing the test password doesn't.
cp ca.crt client.crt client.key /tls/ && chmod 644 /tls/*
cat > "$server/pg_hba.conf" <<'HBA'
local     all  all             trust
hostssl   all  avalanche_cert  all  cert
hostssl   all  all             all  scram-sha-256
hostnossl all  all             all  reject
HBA
# The role the client certificate maps to. initdb runs these scripts once, on the first start;
# the role then persists with the data directory (a volume kept across restarts).
mkdir -p /docker-entrypoint-initdb.d
echo "CREATE ROLE avalanche_cert SUPERUSER LOGIN REPLICATION PASSWORD 'unused';" \
  > /docker-entrypoint-initdb.d/cert-role.sql
exec docker-entrypoint.sh postgres -c wal_level=logical -c max_replication_slots=20 \
  -c max_wal_senders=20 -c ssl=on -c ssl_cert_file="$server/server.crt" \
  -c ssl_key_file="$server/server.key" -c ssl_ca_file="$server/ca.crt" \
  -c hba_file="$server/pg_hba.conf"
