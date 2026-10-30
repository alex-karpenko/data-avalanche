---
title: Config changes
description: Validating and reloading a config, why a restart doesn't apply one, rotating secrets, and the keys a reload refuses.
sidebar:
  order: 7
---

## Change the config

1. Change the file a node reads (on Kubernetes: the ConfigMap, then wait up to about a minute for
   the kubelet to update the pod's copy).
2. Check it on that node:

   ```sh
   curl -s -X POST https://cdc-0.example.internal/v1/config/validate
   ```

   With no body it checks the node's file; with a YAML body, that text. The answer is the report
   (each error and warning with its rule and key) and what a reload would change: which pipelines
   are added, removed, restarted, or changed in place. Nothing is applied.
3. Apply it:

   ```sh
   curl -s -X POST https://cdc-0.example.internal/v1/config/reload
   ```

   The node validates its file again and publishes it as the cluster's next config generation;
   every node adopts it within seconds. A file with any error is refused whole, and the running
   config stays.
4. Check that every node runs it: `GET /v1/config/status` lists the generation and each member's.

A reload doesn't restart a pipeline in `error`, even when it fixes the cause: once the new
generation runs, `POST /v1/pipelines/<id>/restart` it.

Both calls need the `operator` role. Any node will do, but it publishes **its own file**: send the
reload to a node whose file is the one you changed.

## A restart doesn't apply a config

A node that starts takes the cluster's latest config generation from `globals.state`, not its own
file. The file matters only for the very first start of a cluster, and as the text a reload
publishes. So editing the file and restarting the node changes nothing, and the node warns that
it runs the generation, not its file. On Kubernetes, `helm upgrade` with a new config is the same:
follow it with a reload.

The per-node values are the exception: a node resolves the generation's `${VAR}`s with its own
environment, so `node_name`, `advertise_addr` and secrets passed as `${VAR}` are each node's own.
A per-node key written as a literal that differs from the generation is ignored, with a warning
naming it, and a reload from that node is refused until the file matches.

## Rotate a secret

- **A credentials file** (`credentials_file`): replace its contents on every node (a mounted
  Secret does), then reload. The reload sees the new value and applies it as the key's class says
  (the source's credentials restart that pipeline; the API's apply in place).
- **An environment variable** (`${VAR}`): a running process can't see a new value. Update it and
  restart the nodes one at a time.
- **The API's TLS certificate** (`globals.api.tls.cert` and `key`): replace the files or the
  variables, then restart the nodes one at a time. A reload meanwhile is refused, naming the key.

## Keys a reload refuses

A reload refuses a change to a key whose class is *process restart*
([the reference](/data-avalanche/reference/configuration/) marks them), naming it (V-061). Because
a restarted node adopts the cluster's generation, restarting doesn't apply such a change either:

- `tokens`, `lock_backend` and `lease_duration` are pinned in the cluster's anchor and never
  change for that `globals.state`.
- `cloud`, `globals.state` and `globals.azure` say where the cluster is: changing them makes a new
  cluster.
- The rest (`lease_renewal_interval`, `lease_safety_margin`, `discovery`, `discovery_dns_name`,
  `join_members`, the gossip's ports and timers, `globals.api.tls`) change only through each
  node's environment. Write the ones you may want to change as `${VAR}`, with a
  default, from the start, and change the variable with a rolling restart:

  ```yaml
  globals:
    cluster:
      lease_renewal_interval: ${LEASE_RENEWAL_INTERVAL:-5s}
  ```

  A key already written as a literal can't be changed in a running cluster. The way out is a new
  cluster: stop every node, then start them with a new `globals.state.prefix`, so that the file
  bootstraps a new anchor and generation. Never run nodes on the old and the new location at
  once: they would be two clusters holding the same pipelines. This path isn't covered by the
  project's tests yet.

`node_name` and `globals.api.address` are read from each node's own file when it starts, so a
restart applies a changed value; a reload is still refused while the generation holds another
one. Write them as `${VAR}` too.
