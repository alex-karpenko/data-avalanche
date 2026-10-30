---
title: Storage
description: The clouds, where the tables and the state are written, the credentials and permissions a node needs, and S3-compatible stores.
sidebar:
  order: 3
---

Everything a node keeps is in object storage: the Delta tables, each pipeline's state (positions,
snapshot progress, halted tables), and the cluster's own state (its anchor, the config
generations, the members, the leases). A node writes nothing to its local disk.

## The cloud

`globals.cloud` names the provider for every bucket of the file: `aws` (the default), `google` or
`azure`. On Azure every `bucket` is a container, and `globals.azure.account_name` names the storage
account when the credentials don't.

The file holds no storage credentials. A node takes them from the provider's default chain:

| Cloud | Where the credentials come from |
|---|---|
| `aws` | an IAM role (EKS IRSA or Pod Identity, an EC2 instance profile), or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`; the region from `AWS_REGION` |
| `google` | Workload Identity or the instance's service account, or a service-account key named by `GOOGLE_SERVICE_ACCOUNT_PATH` |
| `azure` | Workload Identity or a managed identity, or `AZURE_STORAGE_ACCOUNT_NAME` with `AZURE_STORAGE_ACCOUNT_KEY` |

The variables are the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate's,
which reads the rest of its settings from the environment too.

## Permissions

| Cloud | On every bucket the file names |
|---|---|
| `aws` | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on the objects; `s3:ListBucket` on the bucket |
| `google` | `storage.objects.create`, `.get`, `.delete`, `.list` (the `Storage Object User` role holds them) |
| `azure` | the `Storage Blob Data Contributor` role on the container |

The leases and the state documents are compare-and-swap writes (`If-Match`, `If-None-Match`, GCS
generation preconditions), and every Delta commit is a create-if-absent write; none needs a
permission beyond writing an object, but the store must honour the conditions: one that ignores
them lets two writers both win.

## S3-compatible stores

Point the S3 client at another endpoint through the environment:

```sh
AWS_ENDPOINT_URL=https://s3.example.internal:9000
AWS_REGION=us-east-1            # any region the store accepts
AWS_ALLOW_HTTP=true             # only for a plain-HTTP endpoint
AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
```

The project's tests run against [RustFS](https://github.com/rustfs/rustfs) this way. The store must
support conditional writes: `If-None-Match: *` and `If-Match` on `PutObject`.

## Where things are written

| What | Where |
|---|---|
| a table | `<sink.bucket>/<sink.prefix>/<schema>/<table>/`: `_delta_log/` and the Parquet files |
| a pipeline's state, by default | `<sink.bucket>/<sink.prefix>/.pipeline-state/` |
| a pipeline's state, with `state.bucket` | `<state.bucket>/[<defaults.state.prefix>/]<state.prefix>/` |
| the cluster's state | `<globals.state.bucket>/<globals.state.prefix>/` |

- `sink.prefix` and `state.prefix` default to the pipeline's `id`.
- `<schema>` is the PostgreSQL schema; for MySQL and MariaDB it is `source.db`.
- A table location that already holds another table format's metadata is refused.
- Moving a pipeline's state (`state.bucket`, `state.prefix`) on a running pipeline leaves its
  history behind: the pipeline starts over at the new location as if new.

data-avalanche never deletes a table, its data files or its catalog entry: removing a pipeline
leaves its tables. It writes the Delta checkpoints (every `sink.delta.checkpoint_interval`
commits) and removes expired log entries (`log_retention`, 30 days); compaction and `VACUUM` are
yours to run.

## Names outside ASCII

Table and column names are written as the source has them, non-ASCII letters included. Some
readers can't open such a table: DuckDB 1.5.6's `delta_scan` refuses a path with non-ASCII text.
Check your engines before replicating such a table.
