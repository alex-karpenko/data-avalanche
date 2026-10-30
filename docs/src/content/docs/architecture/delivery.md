---
title: Delivery guarantees
description: At-least-once delivery with an exactly-once effect, what makes a replay harmless, and what a reader of the tables can rely on.
sidebar:
  order: 4
---

data-avalanche delivers every change **at least once** and makes the result **as if once**: a
table always converges to the source's rows, however many times a change is replayed.

## Why a replay is harmless

A pipeline replays the changes since its last checkpoint after a crash, a failover, a restart, or
a lost connection. Three things make that safe, the first one alone sufficient:

1. **Idempotent writes.** Every table is keyed by its primary key (or `tables.primary_key`), and a
   flush writes each key's last state: an upsert, or a delete. Writing the same change set twice
   leaves the same rows.
2. **The position guard.** Each of the pipeline's Delta commits records the source position it
   covers. Before a commit, the sink reads the table's last own commit (walking past commits other
   writers made, such as an `OPTIMIZE`) and skips a change set that commit already covers.
3. **The epoch fence.** Each commit also records the lease term that wrote it, and a commit from
   an older term is refused: a node that lost its lease while stalled can't write over its
   successor.

## What a reader can rely on

- **Each table's commit is atomic.** A reader sees all of a flush's changes to a table, or none.
- **Each table converges.** After the source stops changing and the pipeline has flushed, every
  table holds the source's rows.
- **No atomicity across tables.** A source transaction that wrote two tables is committed to each
  in its own Delta commit; a reader may see it in one before the other.
- **A row may be rewritten with the same values.** After a replay, a row's Delta file may change
  although its values don't; `_lsn` and `_seq` stay those of the source change.
- **A large transaction may be seen in parts** when `max_transaction_batch_rows` splits it.

## What stops a table instead

When a change can't be applied correctly (an unsafe schema change, a value the table can't hold,
a key column that is NULL) the pipeline **halts that table** and records why: the table keeps the
rows it had, and the pipeline's other tables go on. It never skips the change and carries on. See
[A halted table](/data-avalanche/runbooks/halted-table/).

## Snapshots

A snapshot row is written as an upsert too, and a change streamed while the snapshot runs wins over
the snapshot's copy of the same row (see [Snapshots](/data-avalanche/architecture/snapshots/)). A
snapshot never deletes: a row deleted at the source while its table wasn't being streamed stays in
the lake, through a copy too. To remove such rows, drop or move the Delta table before copying it.
