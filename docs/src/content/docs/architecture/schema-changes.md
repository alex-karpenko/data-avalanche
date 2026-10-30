---
title: Schema changes
description: How a pipeline notices a source schema change, which changes the Delta table follows, and which halt the table.
sidebar:
  order: 5
---

A pipeline keeps the schema of each table it replicates, and applies a change at its place in the
stream: it flushes what came before, changes the Delta table, and goes on. A change the Delta table
can't follow halts that table, with its reason; the other tables go on.

## Noticing a change

- **At every start**, the pipeline reads each table's schema from the source's catalog and compares
  it with the one it kept.
- **In the stream.** MySQL and MariaDB write DDL to the binlog, and each row event carries its
  table's columns. PostgreSQL writes no DDL to the replication stream: a changed table is noticed
  when its next row arrives with a new column list.
- **On a timer.** Every `schema_evolution.rescan_interval` (60 minutes) the pipeline checks the
  tables that had no changes, which is how PostgreSQL notices a dropped table.

The pipeline then reads the table's catalog entry to tell a rename from a drop and an add, and to
get each column's exact type.

## What the Delta table does

| Source change | The Delta table |
|---|---|
| add a column | the column is added, nullable; older rows read NULL |
| drop a column | **kept**, NULL from then on (a soft drop, with a warning) |
| rename a column | the new name is added, the old one soft-dropped; history stays under the old name |
| change a column's type | **halts** the table (`EVOLUTION_UNSUPPORTED` or `UNSAFE_DDL`): Delta can't widen a column in place |
| `NULL` ↔ `NOT NULL` | nothing: every non-key column is nullable |
| change the primary key, or a `REPLICA IDENTITY` that no longer covers it | **halts** the table (`UNSAFE_DDL`) |
| add or rename to a lineage column's name | **halts** the table (`RESERVED_COLUMN_COLLISION`) |
| drop the table | **halts** the table (`TABLE_DROPPED`); the Delta table is kept |
| MySQL: reorder or remove `ENUM` / `SET` values | **halts** the table |
| MySQL: a partition or tablespace operation that changes rows without row events (`TRUNCATE PARTITION`, `EXCHANGE PARTITION`, …) | **halts** the table |
| a DDL the pipeline can't parse, on a tracked table | **halts** the table |
| indexes, comments, storage parameters, `AUTO_INCREMENT` | nothing |

`data_avalanche_sink_soft_dropped_columns` counts the soft-dropped columns: drop them yourself, if
you want, with your Delta engine once nothing reads them.

`schema_evolution.enabled: false` halts a table at any schema change (`EVOLUTION_DISABLED`).

## Online migrations

MySQL tools that copy a table to a shadow and swap it in (gh-ost's `_t_gho`, pt-online-schema-change's
`_t_new`) are followed: the swap, one `RENAME TABLE t TO …, _t_gho TO t` statement, is read as the
column changes between the two tables, each handled as above. Any other rename of a tracked table
away halts it as dropped.

## A re-created table

A table dropped and created again under the same name is a new table: the pipeline keeps it halted
(`TABLE_DROPPED`) until a snapshot job copies it afresh. See
[A halted table](/data-avalanche/runbooks/halted-table/).
