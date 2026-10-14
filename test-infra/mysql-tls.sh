#!/usr/bin/env bash
# Entrypoint of the `mysql-tls` service: a fresh throwaway CA, server certificate (localhost
# only, so a connection by 127.0.0.1 is a name mismatch) and client certificate (CN
# avalanche_cert) at every start, then MySQL 8.4 with TLS required for every TCP connection. The
# CA and the client pair are copied to /tls/mysql (bind-mounted from the git-ignored
# test-infra/.tls) for the tests; the server key stays in the container.
set -euo pipefail
server=/etc/mysql-tls
mkdir -p "$server" /tls/mysql
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
chown mysql:mysql server.key && chmod 600 server.key
# 644: on a Linux host the bind-mounted files are root-owned, and the tests read them as another
# user. Throwaway material, granting nothing the test password doesn't.
cp ca.crt client.crt client.key /tls/mysql/ && chmod 644 /tls/mysql/*
# The user the client certificate identifies: its password alone doesn't log in. The init scripts
# run once, on the first start; the user then persists with the data directory.
mkdir -p /docker-entrypoint-initdb.d
cat > /docker-entrypoint-initdb.d/cert-user.sql <<'SQL'
CREATE USER 'avalanche_cert'@'%' IDENTIFIED BY 'unused' REQUIRE SUBJECT '/CN=avalanche_cert';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'avalanche_cert'@'%';
SQL
exec docker-entrypoint.sh mysqld --server-id=1 --log-bin=mysql-bin --binlog-format=ROW \
  --binlog-row-image=FULL --binlog-row-metadata=FULL --gtid-mode=ON --enforce-gtid-consistency=ON \
  --ssl-ca="$server/ca.crt" --ssl-cert="$server/server.crt" --ssl-key="$server/server.key" \
  --require-secure-transport=ON
