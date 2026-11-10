---
title: Sources
description: What PostgreSQL, MySQL and MariaDB need, and the source settings of a pipeline.
sidebar:
  order: 2
---

A pipeline reads one database: a PostgreSQL database, or one MySQL or MariaDB database (schema).
`source.type` picks the engine; the common keys (`host`, `port`, `db`, `user`, `password` or
`credentials_file`, `ssl_mode`, `timeouts`) are the same for both. `ssl_mode` defaults to
`require`. On MySQL and MariaDB, `verify-ca` checks the server's host name as well, as
`verify-full` does (a warning, V-124): use a certificate that names the host you connect to.

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
required. A slot name is unique per source server (host and port: V-058), a publication per
database. By default (`slot_management: managed`) the pipeline creates the
slot and the publication, keeps the publication's tables equal to the tables it replicates, and
drops both when the pipeline is removed (`auto_cleanup: true`). With
`slot_management: require_existing` it creates neither: a DBA creates them, and the pipeline
refuses to start without them (and needs `auto_cleanup: false`). The managed publication needs
the pipeline's role to own the tables;
[Source privileges](/data-avalanche/runbooks/source-privileges/) has both set-ups. On PostgreSQL 17
and later a managed slot is a failover slot, which a standby with `sync_replication_slots` keeps,
so a promotion can continue the stream (see **A failover** below). Such a slot waits
for every standby the primary names in `synchronized_standby_slots`: a standby that is down
stalls the pipeline. A slot made before an upgrade to 17 stays a plain one.

**What the publication publishes.** At each start the pipeline checks what the publication
publishes of its tables. Under `require_existing`, or a publication `FOR ALL TABLES`, a tracked
table published with a row filter or a column list (PostgreSQL 15 and later) **halts itself**
(`TABLE_NOT_REPLICABLE`) and the other tables go on. The changes the filter dropped never reached
the stream, so fixing the publication isn't enough: publish the table whole in one transaction
(`BEGIN; ALTER PUBLICATION … DROP TABLE …; ALTER PUBLICATION … ADD TABLE …; COMMIT;`), then copy it
again (see [A halted table](/data-avalanche/runbooks/halted-table/)). Stored generated columns
(`publish_generated_columns = stored`, PostgreSQL 18) are refused for the whole pipeline when a
replicated table has one, naming the `ALTER PUBLICATION` that fixes them.

Under `managed` the pipeline makes the publication's tables exactly the tracked set: it drops a
row filter, a column list, a `TABLES IN SCHEMA` member and any other table, and sets
`publish_generated_columns = none`. A change to what is published of a tracked table means the WAL
since the checkpoint was decoded under the old publication, so the pipeline re-snapshots its tables
as `snapshots.mode` says. A tracked table that wasn't published at all is added and copied alone,
as `snapshots.mode` says. One that was published through its schema, or through its partitioned
root with `publish_via_partition_root = false`, is added by name, with no copy.
Under `require_existing` (or `FOR ALL TABLES`), tables the publication publishes but the pipeline
doesn't replicate are a warning: their changes are decoded and dropped.

**Partitioned tables.** Name the partitions, or a pattern that matches them: each partition
replicates as its own table. Under `require_existing` (or `FOR ALL TABLES`), a publication that
publishes a replicated partition only through its root (`publish_via_partition_root = true`) is
refused, since the partition's changes would arrive under the parent's name. A managed publication
is set to `false`, and the pipeline copies the partitions the root published, as `snapshots.mode`
says; the other tables stream on.

**Large values.** PostgreSQL leaves an unchanged TOASTed value out of an update. With
`toast_strategy: carry_forward` (the default) the pipeline keeps the value it wrote last;
`re_query` reads the row again from the database.

**The WAL a slot holds.** A slot keeps the server's WAL until the pipeline confirms it, so a
stopped or failing pipeline grows the server's disk. `data_avalanche_wal_lag_bytes` measures it;
`source.postgres.max_wal_lag_bytes` halts the pipeline when it passes a bound (the slot stays).
Set the server's `max_slot_wal_keep_size` too, so a forgotten slot can't fill the disk: a slot past
it is invalidated, and the pipeline then starts over with a new slot and a snapshot as
`snapshots.mode` says, none under `never` (see
[A lost position](/data-avalanche/runbooks/position-lost/)).

**A failover.** The checkpoint records the server it was read on: its system identifier and its
timeline. A promoted replica keeps the identifier but starts a timeline of its own, and its LSNs
can repeat ones the lake has already marked. So a start on another server, or a stream reconnect
that reaches one, halts the pipeline with `WAL_SOURCE_CHANGED`
(`source.postgres.source_change_action: error`, the default). The check runs before the pipeline
drops or makes a slot there. With `re_snapshot` the pipeline starts again, moves its tables onto
the new server's position and copies them as `snapshots.mode` says. Under either, a promotion the
pipeline's failover slot followed (PostgreSQL 17 and later, the standby's copy of the slot
synchronized exactly up to the checkpoint) continues the stream on the new timeline, with a warning: a
reconnect to the promoted server starts the pipeline again, and that start continues. See
[A lost position](/data-avalanche/runbooks/position-lost/#a-failover-wal_source_changed).

**A standby.** Point the pipeline at the primary: a standby (a server in recovery) is refused on
every version. The source reads the primary's WAL position and may change the publication, which
a read-only standby can't. Decoding on a standby (PostgreSQL 16 and later) is planned for a later
release.

## MySQL

Tested on MySQL 8.4.

**Server settings**

- `log_bin` on, `binlog_format = ROW`, `binlog_row_image = FULL`, `binlog_row_metadata = FULL`;
- for `replication_type: gtid` (the default): `gtid_mode = ON` and
  `enforce_gtid_consistency = ON`;
- no binlog filter (`binlog_do_db`, `binlog_ignore_db` empty): a filter drops statements by the
  session's database, so changes to the pipeline's database made from another one would never
  reach it;
- on a server that replicates from another, `log_replica_updates = ON`. A MariaDB Galera node is
  one (see [MariaDB](#mariadb)).

**Tables.** `tables.include` and `exclude` take bare table names (`orders`, `payment_*`), never
`db.table`: `source.db` is the one database the pipeline reads. Each table needs a primary key or
a `tables.primary_key` override.

**Positions.** `source.mysql.replication_type`:

- `gtid` (the default) follows GTIDs, which survive a failover of the source to a replica.
- `binlog` follows a file and an offset, for servers without GTIDs. A position is only valid on
  the server that wrote it, so the pipeline checks at each start (and at a reconnect) that the
  server is the one that wrote the checkpoint:
  - servers are told apart by `server_uuid` on MySQL and by `server_id` on MariaDB; on MariaDB the
    position must also be an event boundary of a binlog file the server holds
    (`BINLOG_GTID_POS`);
  - a checkpoint's file the server doesn't list is taken as purged only when it is older than the
    server's oldest binlog; a file with another base name, or after the newest, is another
    server's;
  - `binlog_purge_action` decides. `error` (the default) halts the pipeline with
    `BINLOG_SOURCE_CHANGED`. `re_snapshot` starts from the new server's position and copies the
    tables as `snapshots.mode` says; found at a reconnect, the pipeline runs its start again to
    do so (see [A lost position](/data-avalanche/runbooks/position-lost/#the-server-changed-binlog_source_changed)).

  Two cases can't be told apart. On MariaDB, a server with the same `server_id` and the same
  binlog file names, whose file has an event boundary at the checkpoint's offset. On MySQL, a copy
  of the server with the same `server_uuid` and the same file names. `gtid` doesn't depend on any
  of this: prefer it.

**`server_id`.** The binlog stream registers as a replica with a server id, and the server ends any
other stream with the same id. Each pipeline takes its own, derived from its `id` (between
2147483648 and 4294967295); set `source.mysql.server_id` when that range collides with the
server's replicas.

**A purged binlog.** When the server has purged the binlog a pipeline still needs,
`binlog_purge_action: error` (the default) stops it for the operator;
`re_snapshot` starts again from the server's current position and snapshots the tables as
`snapshots.mode` says: under `never`, the default, none, and the log warns, so start a snapshot
job for the tables.

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
- A Galera node (`wsrep_on = ON`) applies the other nodes' writes without a replica connection, so
  it needs `log_slave_updates = ON` too, or it is refused: its binlog would lack those writes.
- Reading a replica's status takes `SLAVE MONITOR` (10.5 and later). Without it the pipeline
  can't check `log_slave_updates`, and warns.
- **Savepoints.** In a transaction that also writes a non-transactional table (MyISAM, Aria),
  MariaDB logs the rows a `ROLLBACK TO SAVEPOINT` or a `ROLLBACK` undid. The pipeline holds such a
  transaction's rows until its end, up to `sink.max_transaction_buffer_bytes` (counted as the
  decoded rows), drops the rows the rollback undid, and keeps the non-transactional table's rows,
  which stand. It halts the pipeline where it can't tell which rows stand: a rollback over rows
  past that limit, a rollback that undid a non-transactional table's row, or a `ROLLBACK TO` a
  savepoint it didn't see logged.

## Connections

- `timeouts.connect` (10 s) bounds the whole connection setup; `timeouts.command` (30 s) each
  statement; `timeouts.snapshot` (6 h) one table's snapshot in one run.
- A lost connection is retried with backoff (1 s to 60 s), and the stream resumes from the last
  committed position.
- `heartbeat` (60 s) keeps an idle stream alive. A stream silent for three heartbeats, whose
  connection the server no longer shows as streaming, is reconnected.
