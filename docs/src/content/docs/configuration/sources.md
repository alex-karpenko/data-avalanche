---
title: Sources
description: What PostgreSQL, MySQL and MariaDB need, and the source settings of a pipeline.
sidebar:
  order: 2
---

A pipeline reads one database: a PostgreSQL database, or one MySQL or MariaDB database (schema).
`source.type` picks the engine; the common keys (`host`, `port`, `db`, `user`, `password` or
`credentials_file`, `ssl_mode`, `timeouts`) are the same for both. `ssl_mode` defaults to
`require`.

The server settings and privileges below are checked when the pipeline starts. A missing one stops
that pipeline (or that table) with a message naming it; the other pipelines run.
[Source privileges](/data-avalanche/runbooks/source-privileges/) has the grants.

## PostgreSQL

Tested on PostgreSQL 14 to 18.

**Server settings**

- `wal_level = logical`.
- A free replication slot and WAL sender per pipeline (`max_replication_slots`,
  `max_wal_senders`).

**Each table**

- has a primary key, or a `tables.primary_key` override naming NOT NULL, non-floating columns;
- has a `REPLICA IDENTITY` other than `NOTHING` that covers the key (the default, the primary key,
  does);
- is a logged table: PostgreSQL doesn't publish `UNLOGGED` tables.

A table without a key that a pattern matches is skipped with a warning; one named exactly halts.

**The slot and the publication.** `source.postgres.publication` and `slot` name them; both are
required and unique per database. By default (`slot_management: managed`) the pipeline creates the
slot and the publication, keeps the publication's tables equal to the tables it replicates, and
drops both when the pipeline is removed (`auto_cleanup: true`). With
`slot_management: require_existing` it creates neither: a DBA creates them, and the pipeline
refuses to start without them (and needs `auto_cleanup: false`). The managed publication needs
the pipeline's role to own the tables;
[Source privileges](/data-avalanche/runbooks/source-privileges/) has both set-ups.

**Partitioned tables.** Name the partitions, or a pattern that matches them: each partition
replicates as its own table. A publication that publishes through the partition root
(`publish_via_partition_root = true`) is refused, since the partitions' changes would arrive under
the parent's name.

**Large values.** PostgreSQL leaves an unchanged TOASTed value out of an update. With
`toast_strategy: carry_forward` (the default) the pipeline keeps the value it wrote last;
`re_query` reads the row again from the database.

**The WAL a slot holds.** A slot keeps the server's WAL until the pipeline confirms it, so a
stopped or failing pipeline grows the server's disk. `data_avalanche_wal_lag_bytes` measures it;
`source.postgres.max_wal_lag_bytes` halts the pipeline when it passes a bound (the slot stays).
Set the server's `max_slot_wal_keep_size` too, so a forgotten slot can't fill the disk: a slot past
it is invalidated, and the pipeline then starts over with a new slot and a snapshot (see
[A lost position](/data-avalanche/runbooks/position-lost/)).

## MySQL

Tested on MySQL 8.4.

**Server settings**

- `log_bin` on, `binlog_format = ROW`, `binlog_row_image = FULL`, `binlog_row_metadata = FULL`;
- for `replication_type: gtid` (the default): `gtid_mode = ON` and
  `enforce_gtid_consistency = ON`;
- no binlog filter (`binlog_do_db`, `binlog_ignore_db` empty): a filter drops statements by the
  session's database, so changes to the pipeline's database made from another one would never
  reach it;
- on a server that replicates from another, `log_replica_updates = ON`.

**Tables.** `tables.include` and `exclude` take bare table names (`orders`, `payment_*`), never
`db.table`: `source.db` is the one database the pipeline reads. Each table needs a primary key or
a `tables.primary_key` override.

**Positions.** `source.mysql.replication_type`:

- `gtid` (the default) follows GTIDs, which survive a failover of the source to a replica.
- `binlog` follows a file and an offset, for servers without GTIDs. A position is only valid on
  the server that wrote it: when the pipeline finds another server behind the host (another
  `server_uuid`), it halts with `BINLOG_SOURCE_CHANGED`.

**`server_id`.** The binlog stream registers as a replica with a server id, and the server ends any
other stream with the same id. Each pipeline takes its own, derived from its `id` (between
2147483648 and 4294967295); set `source.mysql.server_id` when that range collides with the
server's replicas.

**A purged binlog.** When the server has purged the binlog a pipeline still needs,
`binlog_purge_action: error` (the default) stops it for the operator;
`re_snapshot` starts again from the server's current position and snapshots every table.

**`TINYINT(1)`** is read as a boolean (`tinyint1_as_bool: true`); set it `false` to keep the
number.

```yaml
config_version: 1
globals:
  cloud: aws
  state:
    bucket: example-cdc-state
pipelines:
  billing:
    id: billing
    source:
      type: mysql
      host: billing-db.example.internal
      db: billing
      user: cdc
      password: ${BILLING_DB_PASSWORD}
      mysql:
        replication_type: gtid
    tables:
      include: [invoices, payment_*]
      snapshots:
        mode: initial
    sink:
      type: delta
      bucket: example-lake
```

## MariaDB

MariaDB is read through the MySQL source (`type: mysql`); tested on MariaDB 10.11, 11.4 and 11.8.
What differs:

- GTIDs need `gtid_strict_mode = 1`; positions are MariaDB's `domain-server-sequence` GTIDs.
- `log_slave_updates` is off by default; turn it on for a server that replicates from another.
- Reading a replica's status takes `SLAVE MONITOR` (10.5 and later). Without it the pipeline
  can't check `log_slave_updates`, and warns.

## Connections

- `timeouts.connect` (10 s) bounds the whole connection setup; `timeouts.command` (30 s) each
  statement; `timeouts.snapshot` (6 h) one table's snapshot in one run.
- A lost connection is retried with backoff (1 s to 60 s), and the stream resumes from the last
  committed position.
- `heartbeat` (60 s) keeps an idle stream alive. A stream silent for three heartbeats, whose
  connection the server no longer shows as streaming, is reconnected.
