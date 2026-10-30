---
title: Getting started
description: Replicate one PostgreSQL database into Delta tables on S3 with a single node.
sidebar:
  order: 1
---

This page runs one node with one pipeline: a PostgreSQL database replicated into Delta tables in
an S3 bucket. The [configuration guide](/data-avalanche/configuration/overview/) covers MySQL,
MariaDB, the other clouds and clusters.

## 1. Prepare the source

The database needs logical replication on, and a role that may replicate and read the tables:

```sql
-- postgresql.conf: wal_level = logical (a server restart applies it)
CREATE ROLE cdc LOGIN REPLICATION PASSWORD '…';
GRANT CREATE ON DATABASE shop TO cdc;          -- the pipeline creates its publication
GRANT USAGE ON SCHEMA public TO cdc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc;
```

The pipeline creates its publication, and PostgreSQL lets only a table's owner add the table to
one: `cdc` must own the tables (or be a superuser). Otherwise the pipeline stops in `error`, "must
be owner of table …". For a role that doesn't own them, a DBA creates the publication and the slot:
see [Source privileges](/data-avalanche/runbooks/source-privileges/).

Every table needs a primary key (or a `tables.primary_key` override), and its `REPLICA IDENTITY`
must not be `NOTHING`.

## 2. Prepare the storage

Two buckets, or one for both: one for the cluster's own state, one for the Delta tables. The node
takes its credentials from the cloud's default chain (an instance role, workload identity, or the
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` variables); the config holds none. It needs to read,
write, list and delete objects in both:
[Storage](/data-avalanche/configuration/storage/) lists the permissions per cloud.

## 3. Write the config

```yaml
config_version: 1
globals:
  cloud: aws
  state:
    bucket: example-cdc-state
pipelines:
  shop:
    id: shop
    source:
      type: postgres
      host: shop-db.example.internal
      db: shop
      user: cdc
      password: ${SHOP_DB_PASSWORD}
      postgres:
        publication: shop_pub
        slot: shop_slot
    tables:
      include: [public.*]
      snapshots:
        mode: initial
    sink:
      type: delta
      bucket: example-lake
```

- `id` is the pipeline's identity: its state, its lease and its default table prefix. Don't change
  it once the pipeline runs.
- `snapshots.mode: initial` copies each table's existing rows once, at its first start; the default
  (`never`) streams only changes from now on.
- `${SHOP_DB_PASSWORD}` is read from the environment.
- Each table lands at `s3://example-lake/shop/<schema>/<table>/`.

## 4. Run it

```sh
export SHOP_DB_PASSWORD=…  AWS_REGION=eu-west-1
data-avalanche run --config config.yaml
```

or with the container image (see [Container image](/data-avalanche/deployment/container/)):

```sh
docker run --rm -p 8080:8080 -e SHOP_DB_PASSWORD -e AWS_REGION \
  -v "$PWD/config.yaml:/etc/data-avalanche/config.yaml:ro" \
  ghcr.io/alex-karpenko/data-avalanche:<version>
```

The node checks the whole config before it starts anything: a mistake is reported with its
rule (`V-…`, see [Validation rules](/data-avalanche/reference/validation-rules/)) and the path of
the key, and the node exits. Check a config without running it with `POST /v1/config/validate`
on a running node.

## 5. Watch it

```sh
curl -s localhost:8080/v1/pipelines/shop        # desired and actual state, position, halted tables
curl -s localhost:8080/metrics | grep data_avalanche_replication_lag_seconds
```

The pipeline's `actual_state` is `running` once it streams. Its tables appear under the sink's prefix as the first
flush commits, within `sink.flush.seconds` (30 s by default) of a change. Read them with any Delta
reader: Spark, DuckDB's `delta_scan`, Polars, delta-rs.

## Next

- Change the config of a running node: [Config changes](/data-avalanche/runbooks/config-changes/).
- Run more than one node: [Clusters](/data-avalanche/configuration/cluster/) and
  [Kubernetes](/data-avalanche/deployment/kubernetes/).
- Know what to watch: [Observability](/data-avalanche/deployment/observability/).
