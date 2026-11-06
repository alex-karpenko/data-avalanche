---
title: Nodes, leases and failover
description: A node down, a lease that isn't renewed, a pipeline no node runs, taking a node out for maintenance.
sidebar:
  order: 4
---

How the cluster places pipelines and moves them is in
[Cluster and failover](/data-avalanche/architecture/cluster/). Start from what the cluster sees:

```sh
curl -s https://cdc.example.internal/v1/cluster/status
```

It lists the members this node sees, with their state, and each pipeline's lease holder. Ask more
than one node: they should agree. A holder marked `holder_stale` (in `GET /v1/pipelines/<id>`:
`holder.stale`) isn't a member this node sees, or is an earlier process of a restarted node: its
lease still reads as held until it expires and another node takes it over. `holder.suspect` means
the gossip suspects the holder of having failed. Both come from this node's view of the
membership, not from clocks.

## A node died

Nothing to do for the data. Each of its pipelines is taken over by the next node on the ring once
its lease expires (about 20 s at the defaults), and resumes from its last checkpoint; the
replication lag rises for that time (v1's target: back under 60 s within 2 minutes). When the node comes
back under the same `node_name` it rejoins and gets its pipelines back after
`rebalance_stabilization`.

If a pipeline stays without a holder:

- check that the surviving nodes see each other (`members` on each);
- check `data_avalanche_owned_pipelines` on each node: a node that owns it but can't start it logs
  why (its source unreachable, its storage refusing);
- check that every node can write `globals.state`: a node that can't take leases can't own
  anything.

## A lease isn't renewed

**Symptom:** `DataAvalancheLeaseDegraded`, a rising `data_avalanche_lease_renew_failures_total` or
`data_avalanche_lease_renew_latency_seconds`.

A holder that can't renew keeps writing only until its safety deadline (`lease_duration ×
(1 − lease_safety_margin)` after its last renewal, 12.75 s by default), then stops the pipeline
so that another node can take it safely. A lease degraded for minutes means the storage of
`globals.state` is slow or failing for that node: look at its network path and the storage's
status. If renewals fail often, a longer `lease_duration` tolerates more, at the price of a
slower failover; it is fixed for the life of a cluster's `globals.state`.

## The nodes split into groups

Nodes that formed groups that don't see each other (each group's `members` lists only its own)
merge on their own: every node resolves its seeds again every 5 s and asks those that aren't
members. Groups that stay apart for more than a few seconds can't reach each other: check the
network between them, and that each node's `advertise_addr` is the address its peers reach.

## Taking a node out

```sh
curl -s -X POST https://cdc-2.example.internal/v1/cluster/leave    # admin
```

The node hands its pipelines off (flush, commit, checkpoint, release) and stays up, not ready,
still answering the API, until you stop it; a restart joins again. `POST /v1/admin/shutdown` (or
`SIGTERM`) leaves and then exits.

:::caution[A node that left keeps its last config]
A node that left the cluster no longer follows the config generations: a later reload's
credentials or `role_mapping` don't reach it, and it still accepts the pipeline commands. Stop a
node soon after it leaves, and send commands to the nodes that are members.
:::
