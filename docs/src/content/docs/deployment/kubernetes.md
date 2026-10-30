---
title: Kubernetes
description: "Running a cluster with the Helm chart: the StatefulSet, discovery, secrets, the config, probes and rolling updates."
sidebar:
  order: 2
---

The chart runs the nodes as a **StatefulSet**: a pod's name is its node name, so a restarted pod
keeps its place on the ring. The pods find each other through a headless Service
(`discovery: dns`). They have no volumes: their state is in object storage.

```sh
helm install cdc oci://ghcr.io/alex-karpenko/charts/data-avalanche --version <version> \
  --set-file config=config.yaml -f values.yaml
```

Until the first release, install it from a checkout: `helm install cdc deploy/helm/data-avalanche
…`. The chart's [README](https://github.com/alex-karpenko/data-avalanche/tree/main/deploy/helm/data-avalanche)
lists every value.

## The config

`config` is the config, as a map in the values or as the file's text (`--set-file`); or
`existingConfigMap` names a ConfigMap of your own. The chart puts in every pod's environment
`POD_NAME`, `POD_NAMESPACE`, `POD_IP` and `DATA_AVALANCHE_DISCOVERY_DNS_NAME`, so the cluster
block reads:

```yaml
config_version: 1
globals:
  cloud: aws
  state:
    bucket: example-cdc-state
  cluster:
    discovery: dns
    discovery_dns_name: ${DATA_AVALANCHE_DISCOVERY_DNS_NAME}
    memberlist:
      node_name: ${POD_NAME}
      advertise_addr: ${POD_IP}
pipelines: {}
```

Secrets reach the config's `${VAR}`s through `env` or `envFrom`, and a cloud identity through
`serviceAccount.annotations` (an EKS role, a GKE or Azure workload identity):

```yaml
env:
  - name: SHOP_DB_PASSWORD
    valueFrom:
      secretKeyRef: { name: shop-db, key: password }
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/data-avalanche
```

:::caution[`helm upgrade` doesn't change the config]
A pod that starts takes the cluster's config generation, not its file, and the chart doesn't
restart pods when the config changes. After `helm upgrade` with a new config, wait for the
kubelet to update the mounted ConfigMap (up to about a minute), check it with
`POST /v1/config/validate`, then apply it with `POST /v1/config/reload`. See
[Config changes](/data-avalanche/runbooks/config-changes/).
:::

## Stopping a pod

A deleted or evicted pod gets `SIGTERM` and hands its pipelines off before it exits. The chart's
`terminationGracePeriodSeconds` is 90 s, above the default `drain_timeout` (60 s). The worst case
of a hand-off is longer (130 s for one pipeline, more for many: the formula is in
[Clusters](/data-avalanche/configuration/cluster/#leases-and-hand-offs)); a pod killed before it
finishes isn't a loss, its pipelines fail over and replay. Raise the grace with `drain_timeout` and
with the number of pipelines per pod.

The chart's PodDisruptionBudget lets one pod go at a time.

## Probes

| Probe | Path | Settings |
|---|---|---|
| startup | `/health/live` | every 5 s, up to 60 failures (5 minutes to start) |
| liveness | `/health/live` | every 10 s, 3 failures |
| readiness | `/health/ready` | every 5 s, 2 failures: ready once the node runs the cluster's config, not ready while it leaves |

A node is ready before its pipelines run: readiness says the node serves the API, not that it has
caught up. Watch the [metrics](/data-avalanche/deployment/observability/) for that.

## Rolling updates

The StatefulSet replaces its pods one at a time, the highest ordinal first, each once the one before
is ready. The pipelines of the pod being replaced move to the others (a planned hand-off), and come
back once the new pod has been a member for `rebalance_stabilization` (15 s). `minReadySeconds`
(30 s by default) keeps the rollout from moving on before they have: lower it to roll faster, at
the cost of moving some pipelines twice.

The pods start in parallel (`podManagementPolicy: Parallel`), since a pod's name, not its start
order, places it. With four or more pods started at once, check after a cold start that every node
sees every other: see [the caution on DNS discovery](/data-avalanche/configuration/cluster/#discovery).

## TLS for the API

With `globals.api.tls.enabled: true` the API and the probes speak HTTPS: set `apiTls: true` when the
config isn't a map the chart can read it from. Pass the certificate and key as `${VAR}`s from a
Secret, or mount them with `extraVolumes` and name their paths.
