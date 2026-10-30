---
title: Container image and CLI
description: The container image, its tags and platforms, the command line, signals and exit codes.
sidebar:
  order: 1
---

## The image

`ghcr.io/alex-karpenko/data-avalanche`, for `linux/amd64` and `linux/arm64`, is published by each
release:

| Tag | |
|---|---|
| `X.Y.Z` | that release |
| `X.Y`, `latest` | the latest release of that line, and the latest release; a pre-release moves neither |

The image is built on `gcr.io/distroless/cc-debian13:nonroot`: no shell, user 65532, nothing
written but to object storage, so it runs with a read-only root filesystem. It listens on 8080/tcp
(the API) and 7946/udp (the gossip). Each release carries a build-provenance attestation and an
SPDX SBOM per platform.

Its entrypoint is `data-avalanche`, with the default arguments
`run --config /etc/data-avalanche/config.yaml`: mount the config there, or pass other arguments.

Until the first release, build it from the repository:

```sh
docker build -f deploy/docker/Dockerfile -t data-avalanche:dev .
```

## The command line

```text
data-avalanche run --config <file> [--log-level <filter>] [--log-format json|compact|pretty]
```

- `--config` names the YAML file. Nothing else is read from the disk.
- `--log-level` overrides `globals.log.level`: a level (`info`) or a filter
  (`data_avalanche=debug,warn`).
- `--log-format` overrides `globals.log.format`.

The log goes to stderr. `data-avalanche --version` prints the version.

## Signals and exit codes

`SIGTERM` or `SIGINT` (or `POST /v1/admin/shutdown`) makes the node leave the cluster: it hands
off its pipelines (flush, commit, checkpoint, release each lease), then exits. Give it the time
[Clusters](/data-avalanche/configuration/cluster/#leases-and-hand-offs) works out before a
`SIGKILL`.

| Exit code | |
|---|---|
| `0` | stopped by a signal or the API |
| `1` | couldn't start: the state storage is unreachable, the cluster's anchor disagrees, … |
| `2` | the config is invalid (each error is printed with its rule and key), or the command line is |
