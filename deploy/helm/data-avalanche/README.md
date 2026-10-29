# The data-avalanche Helm chart

The data-avalanche nodes as a StatefulSet: a pod's name is its node name (the Downward API's
`POD_NAME`), so a restarted pod keeps its place on the ring (config §6.5.1, D-1). The pods find
each other through a headless Service (`discovery: dns`); their state, the leases and the config
generations are in object storage, so the pods have no volumes.

```sh
helm install cdc oci://ghcr.io/alex-karpenko/charts/data-avalanche --version 0.1.0 \
  --set-file config=config.yaml -f values.yaml
```

## The config

`config` is the data-avalanche config, a map in the values or the file's text (`--set-file`);
`existingConfigMap` names a ConfigMap of your own instead. Every pod has in its environment:

| Variable | Value |
|---|---|
| `POD_NAME`, `POD_NAMESPACE`, `POD_IP` | the Downward API's |
| `DATA_AVALANCHE_DISCOVERY_DNS_NAME` | the headless Service's DNS name |

so the config's cluster block reads:

```yaml
globals:
  cluster:
    discovery: dns
    discovery_dns_name: ${DATA_AVALANCHE_DISCOVERY_DNS_NAME}
    memberlist:
      node_name: ${POD_NAME}
      advertise_addr: ${POD_IP}
```

Secrets reach the config's `${VAR}`s through `env` or `envFrom`; a cloud identity through
`serviceAccount.annotations`. `ci/example-values.yaml` is a complete example, `ci/tls-values.yaml`
one with the API's TLS on.

A changed config reaches a running cluster through the API's reload (`POST /v1/config/reload`),
not through `helm upgrade` alone: a starting node adopts the cluster's latest config generation,
not its file (config §12). The chart has no checksum annotation that would restart the pods.

## Values

| Key | Default | |
|---|---|---|
| `config` | `{}` | The config, a map or text. Required unless `existingConfigMap` |
| `existingConfigMap`, `configKey` | `""`, `config.yaml` | A ConfigMap of your own, and its key |
| `ports.api`, `ports.gossip`, `apiTls` | `8080`, `7946`, `false` | When the config isn't a map that names them (`globals.api.address`, `memberlist.bind_port`, `globals.api.tls.enabled`) |
| `replicaCount` | `3` | |
| `image.repository`, `image.tag`, `image.pullPolicy` | `ghcr.io/alex-karpenko/data-avalanche`, the chart's `appVersion`, `IfNotPresent` | |
| `env`, `envFrom` | `[]` | The secrets the config names |
| `serviceAccount.create`, `.name`, `.annotations` | `true`, `""`, `{}` | |
| `service.type`, `.nodePort`, `.annotations` | `ClusterIP`, `null`, `{}` | The API, `/metrics` and the probes |
| `terminationGracePeriodSeconds` | `90` | Above `drain_timeout` (60 s): a deleted pod hands its pipelines off first |
| `livenessProbe`, `readinessProbe`, `startupProbe` | `/health/live`, `/health/ready`, `/health/live` | HTTPS when the API's TLS is on |
| `resources` | requests 500m, 1Gi; limit 2Gi | |
| `podDisruptionBudget.enabled`, `.maxUnavailable` | `true`, `1` | |
| `podSecurityContext`, `securityContext` | non-root (65532), read-only root, no capabilities | `/tmp` is an `emptyDir` |
| `hostAliases` | `[]` | Names the pods resolve beside the cluster's DNS |
| `podAnnotations`, `podLabels`, `nodeSelector`, `tolerations`, `affinity`, `topologySpreadConstraints`, `extraVolumes`, `extraVolumeMounts`, `imagePullSecrets` | empty | |
