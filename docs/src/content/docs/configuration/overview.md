---
title: The config file
description: The config file's structure, how defaults merge, environment variables, secrets, and when a change applies.
sidebar:
  order: 1
---

A node reads one YAML file, named by `--config`. The file holds the cluster's settings and every
pipeline; a node runs every pipeline the ring gives it. The
[configuration reference](/data-avalanche/reference/configuration/) lists every key.

## Structure

```yaml
config_version: 1
globals:
  cloud: aws
  state:
    bucket: example-cdc-state
defaults:
  source:
    type: postgres
    host: analytics-db.example.internal
    db: analytics
    user: cdc
    password: ${ANALYTICS_DB_PASSWORD}
  sink:
    type: delta
    bucket: example-lake
pipelines:
  orders:
    id: orders
    source:
      postgres:
        publication: orders_pub
        slot: orders_slot
    tables:
      include: [sales.orders, sales.order_lines]
  customers:
    id: customers
    source:
      postgres:
        publication: customers_pub
        slot: customers_slot
    tables:
      include: [crm.*]
      exclude: [crm.tmp_*]
```

- **`config_version`** is required; `1` is the only version.
- **`globals`** holds what the whole cluster shares: the cloud, where the cluster keeps its state,
  the API, logging, the cluster's own settings, tracing.
- **`defaults`** is shaped like a pipeline. It is merged into every pipeline, and the pipeline
  wins. It may not hold a pipeline's `id`, its publication, its slot or its
  `source.mysql.server_id`, which must be unique (V-060). Its `state.prefix` is a parent prefix,
  under which each pipeline's state goes, not a value copied as it is (see
  [Storage](/data-avalanche/configuration/storage/)).
- **`pipelines`** maps a pipeline's name to its config. It may be empty (`pipelines: {}`): the
  cluster runs and waits.

The parser is strict: an unknown key is an error, at every level. A misspelt key never silently
falls back to its default.

## A pipeline's name and its id

The map key (`orders` above) is the pipeline's **name**: the `_pipeline` column of every row and
the `pipeline` label of its metrics. Renaming it is harmless.

`id` is the pipeline's **identity**: its state, its lease, its place on the ring, its default
table prefix (`sink.prefix`) and the default MySQL `server_id` derive from it. Changing `id`
deletes the pipeline and creates a new one: a new state, and a new snapshot per its
`snapshots.mode`. Lower-case letters, digits, `_` and `-`, at most 63 characters (V-055).

:::caution[Reusing an id or its resources]
When a pipeline is removed and another one takes its `id`, slot, publication, sink prefix or state
location, start the new one only after the old one's lease is released (watch
`GET /v1/cluster/status`), or in the same reload as the removal. A node that starts or joins in
between, or a second reload, can start the new pipeline while the old one still drains.
:::

## How defaults merge

- **Values and maps** merge key by key; the pipeline's value wins.
- **Lists are replaced**, never concatenated: a pipeline's `tables.include` replaces the one in
  `defaults`.
- **The engine and format blocks are alternatives.** `source.postgres` and `source.mysql` are
  chosen by `source.type`, `sink.delta` and `sink.iceberg` by `sink.type`: a pipeline that sets
  `source.type: mysql` drops a `postgres` block it inherits, and keeps the common keys (`host`,
  `port`, `user`, …).
- A value that resolves to empty (`${VAR:-}` with the variable unset) counts as absent and
  inherits, except a `prefix`, where empty means the bucket's root.

## Values

- **Durations** are `humantime` strings: `200ms`, `30s`, `1h30m`, `30d`.
- **Byte sizes** take units, `512MiB` (binary) or `512MB` (decimal), or a bare number of bytes.
- **`${VAR}`** in any string is replaced by the environment variable, whole or in part
  (`bucket: ${ACCOUNT}-lake`). An unset variable is an error (V-063); `${VAR:-text}` gives a
  default, and `$${VAR}` writes a literal `${VAR}`. The result is then read as the key's type, so
  `port: ${DB_PORT}` is a number.
- **TLS material** (`cert`, `key`, `ca`, `ssl_cert`, …) is a file path, or the PEM text itself (a
  value starting `-----BEGIN`), or a `${VAR}` holding either.

## Secrets

The file never needs a secret in clear. Two ways in:

- **`${VAR}`**, from the process's environment (a Kubernetes Secret through `env`). A changed
  variable takes a process restart.
- **A credentials file**, `credentials_file` in place of `user` and `password`: a JSON file
  `{"user": "…", "password": "…"}`, which a secret manager's sidecar or a mounted Secret keeps
  current. Replace its contents and reload: the reload compares the resolved values, sees the new
  credentials and applies them without restarting the process.

A resolved secret stays in memory: it never reaches the logs, the state in object storage, or the
answers of the API, which show a templated value as its `${VAR}` text.

## Validation

A node checks the whole file before it applies anything, in three passes: the YAML and its
keys; the rules over the keys as written (structure, required keys, value ranges, cross-key
rules); then every `${VAR}`, file and certificate resolved and checked. Any **error** rejects the
whole file: a starting node exits, a reload leaves the running config in force. A **warning** is
logged and the file loads. Each finding names its rule and the key's path; the
[validation rules](/data-avalanche/reference/validation-rules/) list them all.

What validation can't see waits for a pipeline's start: an unreachable host, a wrong password, a
missing privilege, a table without a key. Those stop that pipeline or that table, and the others
run.

## When a change applies

A running cluster takes a new config through `POST /v1/config/reload`, never by itself. Each key
of the [reference](/data-avalanche/reference/configuration/) has a reload class:

| On reload | What happens |
|---|---|
| applied | in place, at once |
| pipeline restart | that pipeline stops, drops what it hasn't committed, and starts again from its last checkpoint with the new value; the replay is idempotent, and the other pipelines don't notice |
| pipeline restart, if the table set changes | a restart only when the tables the patterns match change |
| process restart | the reload is refused, naming the key (V-061); see [Config changes](/data-avalanche/runbooks/config-changes/#keys-a-reload-refuses) |

A node that restarts takes the config the cluster runs, not its own file: see
[Config changes](/data-avalanche/runbooks/config-changes/).
