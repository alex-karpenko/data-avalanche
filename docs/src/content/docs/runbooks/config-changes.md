---
title: Config changes
description: Validating and reloading a config, why a restart doesn't apply one, removing a pipeline, rotating secrets, and the keys a reload refuses or applies at the next start.
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

A reload whose change to a pipeline is of class *pipeline restart* restarts it, which clears
`error`. A change of class *applied* (for example `slot_on_orphan`) reaches the running pipeline
without a restart, so a pipeline in `error` stays there: once the new generation runs,
`POST /v1/pipelines/<id>/restart` it.

Both calls need the `operator` role. Any node will do, but it publishes **its own file**: send the
reload to a node whose file is the one you changed. A node checks at most 5 posted configs at once,
then one a second: past that, the validate answers `429` with `Retry-After`. Checking the node's
own file (no body) has no limit.

## A restart doesn't apply a config

A node that starts takes the cluster's latest config generation from `globals.state`, not its own
file. The file matters only for the very first start of a cluster, and as the text a reload
publishes. So editing the file and restarting the node changes nothing, and the node warns that
it runs the generation, not its file. On Kubernetes, `helm upgrade` with a new config is the same:
follow it with a reload.

The per-node values are the exception: a node resolves the generation's `${VAR}`s with its own
environment, so `node_name`, `advertise_addr` and secrets passed as `${VAR}` are each node's own.
A gossip address (`bind_addr`, `bind_port`, `advertise_addr`, `advertise_port`) written as a
literal that differs from the generation is ignored, with a warning naming it; `node_name` and
`api.address` are the node's file's. A reload from a node is refused until its file matches the
generation on these keys.

## Remove a pipeline

Delete it from the file and reload. The node that adopts the generation first writes a removal
record in `globals.state` (`removals/<id>.json`); the pipeline's owner then drains it, drops its
slot and publication as `auto_cleanup` says, and marks the record done. The sink's tables stay.

The record is what makes a removal survive a node that is down: if no node holds the pipeline
when the generation lands (its owner was stopped, or the lease was between two nodes), the node
the ring places the id on finds the pending record within 30 s, takes the lease once it is free,
and tears the pipeline down itself. A pipeline added back with the same id, or a new one on the
removed one's slot, publication, sink or state location, waits for the record to be done before
it starts, whichever node it lands on. A record that can't be read or resolved is waited for, and
the node logs why.

## Rotate a secret

- **A credentials file** (`credentials_file`): replace its contents on every node (a mounted
  Secret does), then reload. The reload sees the new value and applies it as the key's class says
  (the source's credentials restart that pipeline; the API's apply in place).
- **An environment variable** (`${VAR}`): a running process can't see a new value. Update it and
  restart the nodes one at a time.
- **The API's TLS certificate** (`globals.api.tls.cert` and `key`): replace the files or the
  variables, then restart the nodes one at a time. A reload meanwhile is refused, naming the key.

## Keys applied at the next start

A reload refuses a change to a key whose class is *process restart*
([the reference](/data-avalanche/reference/configuration/) marks them), naming it (V-061). Most
of them (`lease_renewal_interval`, `lease_safety_margin`, `discovery`, `discovery_dns_name`,
`join_members`, the gossip's ports and timers, `globals.api.tls`) can still be changed:

1. Reload with `restart=accept`:

   ```sh
   curl -s -X POST 'https://cdc-0.example.internal/v1/config/reload?restart=accept'
   ```

   The generation is published. Every running node adopts it but keeps the values it started
   with for those keys; everything else in the reload applies as usual.
2. Restart the nodes one at a time. Each one takes the new values as it starts.
3. `GET /v1/config/status` on each node lists, in `restart_pending`, the keys it still runs with
   the old value. The rollout is done when every node's list is empty.

Some keys are refused even with `restart=accept`, because the cluster keeps them for good:

- `tokens`, `lock_backend` and `lease_duration` are pinned in the cluster's anchor and never
  change for that `globals.state`.
- `config_version`, `cloud`, `globals.state` and `globals.azure` say what and where the cluster
  is: changing them makes a new cluster. Stop every node, then start them with a new
  `globals.state.prefix`, so that the file bootstraps a new anchor and generation. Never run
  nodes on the old and the new location at once: they would be two clusters holding the same
  pipelines. This path isn't covered by the project's tests yet.

`node_name` and `globals.api.address` are read from each node's own file when it starts, so a
restart applies a changed value; a reload is still refused while the generation holds another
one. Write them as `${VAR}` too.
