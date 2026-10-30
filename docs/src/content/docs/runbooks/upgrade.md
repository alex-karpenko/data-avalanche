---
title: Upgrades
description: Upgrading the nodes of a cluster, and what v1 does and doesn't promise across versions.
sidebar:
  order: 8
---

:::caution[No compatibility promise before 1.0]
Until 1.0 a release may change the config, the state it writes to object storage, or its metrics.
Read the release notes of every version between yours and the target before upgrading, and keep
the old version's image and chart at hand.
:::

## Before

- Check `GET /v1/pipelines`: no pipeline in `error`, no halted table you haven't dealt with.
- Once the first node runs the new version, `POST /v1/config/validate` on it checks your config
  against the new version's rules, before the other nodes roll.

## Rolling the nodes

On Kubernetes, `helm upgrade` with the new chart (or `--set image.tag=<version>`) replaces the pods
one at a time, the highest ordinal first. Each pod hands its pipelines off before it exits, its
pipelines run on the others meanwhile, and they come back once the new pod has been a member for
`rebalance_stabilization`. The chart's `minReadySeconds` paces the rollout so that they have.

Outside Kubernetes do the same by hand: stop a node with `SIGTERM` (or
`POST /v1/admin/shutdown`), wait for it to exit, start it on the new version, wait until
`GET /v1/cluster/status` lists it and its pipelines are back, then the next node.

During the roll both versions run the cluster's one config generation and share its leases. If
the release notes say a version can't run beside its predecessor, stop every node first and start
them on the new version.

## A new config with the upgrade

`helm upgrade` doesn't apply a changed config (see
[Config changes](/data-avalanche/runbooks/config-changes/)). Upgrade first, then reload the new
config once every node runs the new version, so that every node can apply it.

## Rolling back

Roll the nodes back to the old version the same way. The state documents are strict: a version
refuses a document with a field it doesn't know, so state that a newer version wrote may stop an
older one. The release notes say when that happens.
