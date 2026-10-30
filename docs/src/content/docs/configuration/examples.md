---
title: Examples
description: Complete config files for Google Cloud Storage, Azure, API authentication, tracing and split state.
sidebar:
  order: 6
---

Each example is a whole config file. The repository's tests load every one of them, so they stay
valid as the config changes. `${…}` values come from the environment.

## MySQL into Google Cloud Storage, with JWT auth

The API takes bearer tokens from an identity provider and maps its groups to the local roles.

```yaml
config_version: 1
globals:
  cloud: google
  state:
    bucket: example-cdc-state
  api:
    address: 0.0.0.0:8080
    auth: jwt
    jwt:
      issuer: https://idp.example.com/
      audience: data-avalanche
      jwks_url: https://idp.example.com/.well-known/jwks.json
      roles_claim: groups
    role_mapping:
      viewer: [cdc-readers]
      operator: [cdc-operators]
      admin: [cdc-admins]
  log:
    level: info
    format: json
pipelines:
  inventory:
    id: inventory
    source:
      type: mysql
      host: inventory-db.example.internal
      db: inventory
      credentials_file: ${INVENTORY_DB_CREDENTIALS}
      ssl_mode: verify-full
      mysql:
        replication_type: gtid
        binlog_purge_action: re_snapshot
    tables:
      include: ["*"]
      exclude: [audit_*]
      snapshots:
        mode: initial
        parallelism: 2
    sink:
      type: delta
      bucket: example-lake
      prefix: inventory
      flush:
        seconds: 60
        rows: 500000
```

`credentials_file` names a JSON file `{"user": …, "password": …}` that a mounted Secret keeps
current: replace it and reload to rotate the password without a restart.

## PostgreSQL into Azure, with basic auth over TLS

```yaml
config_version: 1
globals:
  cloud: azure
  azure:
    account_name: examplelake
  state:
    bucket: cdc-state
  api:
    auth: basic
    user: ops
    password: ${API_PASSWORD}
    tls:
      enabled: true
      cert: ${API_TLS_CERT}
      key: ${API_TLS_KEY}
pipelines:
  payments:
    id: payments
    source:
      type: postgres
      host: payments.postgres.database.azure.com
      db: payments
      user: cdc
      password: ${PAYMENTS_DB_PASSWORD}
      ssl_mode: verify-full
      ssl_ca: ${PAYMENTS_DB_CA}
      postgres:
        publication: payments_pub
        slot: payments_slot
        max_wal_lag_bytes: 50GiB
    tables:
      include: [public.payments, public.refunds]
      primary_key:
        public.refunds: [payment_id, refund_no]
    sink:
      type: delta
      bucket: lake
      delta:
        timestamp_ntz_as: timestamp
```

`API_TLS_CERT`, `API_TLS_KEY` and `PAYMENTS_DB_CA` hold PEM text (or a path to a PEM file). On
Azure `bucket` is a container of the `examplelake` account.

## Shared defaults, split state and tracing

Two pipelines on one PostgreSQL server; their state lives in a bucket of its own, and flushes are
traced to an OpenTelemetry collector.

```yaml
config_version: 1
globals:
  cloud: aws
  state:
    bucket: example-cdc-state
  tracing:
    otlp_endpoint: http://otel-collector.observability:4318
    sample_ratio: 0.1
defaults:
  source:
    type: postgres
    host: erp-db.example.internal
    db: erp
    user: cdc
    password: ${ERP_DB_PASSWORD}
    postgres:
      max_wal_lag_bytes: 20GiB
  tables:
    snapshots:
      mode: initial
  sink:
    type: delta
    bucket: example-lake
    metrics:
      per_table: true
  state:
    bucket: example-pipeline-state
    prefix: erp
pipelines:
  erp-sales:
    id: erp-sales
    source:
      postgres:
        publication: erp_sales_pub
        slot: erp_sales_slot
    tables:
      include: [sales.*]
  erp-stock:
    id: erp-stock
    source:
      postgres:
        publication: erp_stock_pub
        slot: erp_stock_slot
    tables:
      include: [stock.*]
      snapshots:
        mode: never
```

`erp-sales` keeps its state at `example-pipeline-state/erp/erp-sales/`, `erp-stock` at
`example-pipeline-state/erp/erp-stock/`; their tables at `example-lake/erp-sales/…` and
`example-lake/erp-stock/…`. `erp-stock` overrides only `snapshots.mode`: the other `tables`
keys still come from `defaults`.
