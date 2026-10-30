---
title: Clusters
description: One node or several, node identity, discovery, and the values that differ per node.
sidebar:
  order: 4
---

Every node is a member of a cluster, and one node alone is a cluster of one: the same code, with
its lease. Nodes that share a `globals.state` location are one cluster. They find each other by
gossip (SWIM over UDP, port 7946), place every pipeline on a hash ring, and the node a pipeline
lands on takes its lease in object storage before it reads a row. How that holds up when nodes
fail is in [Cluster and failover](/data-avalanche/architecture/cluster/).

## One node

Leave `globals.cluster` out. The node's name is its hostname, it has no peers, and it runs every
pipeline. A second process started on the same `globals.state` by mistake (an overlapping
deploy) has no peer to join, so it places every pipeline on itself too; but each lease has one
holder, so it waits for the leases the first one holds and never writes those pipelines.

## Several nodes

Every node runs **the same config**, the cluster's config generation in `globals.state`, not its
own file: the file only bootstraps the first generation and is the text a reload publishes (see
[Config changes](/data-avalanche/runbooks/config-changes/)). So the values that differ per node are
written as `${VAR}`, which each node resolves from its own environment:

- `globals.cluster.memberlist.node_name`: the node's identity, and its place on the ring. It must
  stay the same across the node's restarts: a pod name of a StatefulSet, or a name kept on the
  host, never a pod IP or a Deployment's random pod name.
- `memberlist.advertise_addr` (and `advertise_port`): the address the other nodes reach this one
  at.
- the API's `globals.api.address`, when nodes share a host.

A value written as a literal in one node's file is ignored once the cluster has a generation: the
node takes the generation's, and warns at startup naming each key that differs.

```yaml
config_version: 1
globals:
  cloud: aws
  state:
    bucket: example-cdc-state
    prefix: cdc
  cluster:
    discovery: static
    memberlist:
      node_name: ${NODE_NAME}
      advertise_addr: ${NODE_IP}
      join_members:
        - cdc-1.example.internal:7946
        - cdc-2.example.internal:7946
        - cdc-3.example.internal:7946
pipelines: {}
```

## Discovery

- `discovery: static` (the default) joins the addresses in `memberlist.join_members`. Each is a
  peer's advertised address, not a load balancer's or a NAT's, whose packets the gossip drops.
- `discovery: dns` resolves `discovery_dns_name`, a Kubernetes headless Service, and joins every
  address it returns. The [Helm chart](/data-avalanche/deployment/kubernetes/) sets it up.

:::caution[Pods started together with DNS discovery]
A node asks DNS again only while it has no peer. When four or more pods start at once, nodes that
each resolved some of the others can settle in separate groups that never merge: each group
places every pipeline, the leases keep one writer per pipeline, but the groups contend for them.
After a cold start, check that `GET /v1/cluster/status` on any node lists every pod. If it
doesn't, restart the pods of the smaller group one at a time: a starting node resolves the name
again.
:::

## Settings fixed for the cluster's life

The first node of a cluster writes its **anchor** to `globals.state`: `tokens` (virtual nodes per
member, 128), `lock_backend` (`storage`) and `lease_duration` (15 s). A node whose config has
other values refuses to start. They can't change for that `globals.state`.

The gossip's packets carry a MAC keyed from the anchor, so a node of another cluster can't join
or disturb this one. They aren't encrypted: they carry node names, addresses and states, never
data or secrets.

## Leases and hand-offs

| Key | Default | |
|---|---|---|
| `lease_duration` | `15s` | how long a dead node's pipelines wait before another node takes them |
| `lease_renewal_interval` | `5s` | at most a third of `lease_duration` |
| `drain_timeout` | `60s` | how long a node leaving gracefully spends flushing a pipeline before it lets it go |
| `rebalance_stabilization` | `15s` | how long membership must be stable before pipelines move to a node that joined |
| `max_concurrent_handoffs` | `2` | pipelines a leaving node drains at once |
| `max_concurrent_takeovers` | `4` | pipelines a node starts at once after another node died |

A node that leaves gracefully (`SIGTERM`, `POST /v1/cluster/leave`) drains its pipelines: it
flushes and commits each one, then releases its lease, and the next owner starts at once. Size the
time a supervisor waits before it kills the process (Kubernetes' `terminationGracePeriodSeconds`)
for the worst case:

```text
drain_timeout × ceil(pipelines on the node / max_concurrent_handoffs) + 2 × lease_renewal_interval + drain_timeout
```

130 s for one pipeline at the defaults. A node killed earlier isn't a loss: its pipelines wait
for their leases to expire and the next owner replays from the last checkpoint.
