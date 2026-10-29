---
updated: 2026-10-29
domain: Architecture
tags: [v1, implementation-plan, m11a, packaging, docker, distroless, helm, kubernetes, statefulset, k3s, release, ghcr, sbom]
resolves: []
---

# M11a — Packaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The first half of roadmap M11 (Q-M11-A): data-avalanche as an operator installs it.
- a container image: the release binary on distroless, as a non-root user, for linux/amd64 and
  linux/arm64;
- a Helm chart: a StatefulSet whose pod names are the node names (D-1), a headless Service for
  the gossip's DNS discovery, the config in a ConfigMap, the probes, a PodDisruptionBudget;
- a release workflow: on a `v*` tag, the image and the chart to GHCR, with an SBOM and a
  provenance attestation, and a GitHub release;
- the exit: `helm lint` and `helm template` pass, and a smoke deploy of three pods on k3s runs a
  pipeline end to end, a deleted pod handing it to the others (the 3-node container run moved
  from M8, Q-M8-4).

M11b (the documentation site, the `xtask` config reference, the runbooks) is the second half: its
own plan, branch and PR, written once this one has landed.

**Architecture:**

- **The image** (Task 1, `deploy/docker/Dockerfile`): a build stage on `rust:1.98.1-trixie`
  (the workspace's toolchain) builds `data-avalanche` with `--release --locked`, its registry and
  target directory as BuildKit caches, and strips the debug info (the symbols stay, for a
  backtrace). The runtime stage is `gcr.io/distroless/cc-debian13:nonroot` (glibc 2.41 as the
  build's Debian 13, the CA bundle for rustls's platform verifier, uid 65532, no shell). The
  entrypoint is the binary, the command `run --config /etc/data-avalanche/config.yaml`.
- **The chart** (Task 2, `deploy/helm/data-avalanche`): a StatefulSet (`podManagementPolicy:
  Parallel`) whose pods carry `POD_NAME`, `POD_NAMESPACE`, `POD_IP` (the Downward API) and
  `DATA_AVALANCHE_DISCOVERY_DNS_NAME` (the headless Service's name), which the config's cluster
  block takes through `${VAR}` (config §6.5.1: a node runs the cluster's generation, its per-node
  values from its environment). The config is a value (a map, or the file's text through
  `--set-file`) rendered into a ConfigMap, or a ConfigMap of the operator's. The API's and the
  gossip's ports and the probes' scheme follow the config when it is a map. The pods run
  read-only as 65532 with no capabilities, `/tmp` an `emptyDir`; the grace period (90 s) exceeds
  the drain (60 s).
- **The smoke deploy** (Task 3): test-infra's new `k3s` service (Q-M11-4), a one-node k3s on the
  compose network beside PostgreSQL 18 and RustFS; `test-infra/k3s.sh` starts it, builds the image
  and imports it into k3s's containerd. The E2E suite's `k3s` scenario installs the chart with
  three pods, reaches `postgres-18` and `rustfs` through the chart's `hostAliases` (k3s's DNS
  doesn't forward to Docker's), follows the pipeline through the API's NodePort, deletes the pod
  that holds it, and watches another take it over and the deleted pod come back.
- **The release** (Task 4, `.github/workflows/release.yml`, Q-M11-1, Q-M11-3): a `v*` tag checks
  that the tag, the workspace's version and the chart's agree; builds each platform on a runner of
  its own architecture, pushed by digest with an SBOM and provenance; joins them under the
  version's tags; attests the image; pushes the chart to `oci://ghcr.io/<owner>/charts`; and makes
  a GitHub release with the chart and an SPDX SBOM. A manual run builds and packages without
  pushing. `weekly.yml` builds the arm64 image, so a break shows before a tag.

**Tech Stack:** Rust 1.98.1; Docker BuildKit (`docker/build-push-action` v7,
`docker/setup-buildx-action` v4, `docker/login-action` v4); `rust:1.98.1-trixie`,
`gcr.io/distroless/cc-debian13:nonroot`; Helm 4.3.0 (`azure/setup-helm` v5); kubeconform 0.8.0
(`ghcr.io/yannh/kubeconform:v0.8.0`, the Kubernetes 1.37.0 schemas); actionlint 1.7.12
(`rhysd/actionlint:1.7.12`); k3s `rancher/k3s:v1.37.1-k3s1` (2026-09-30);
`actions/attest-build-provenance` v4, `anchore/sbom-action` v0.24.3, `actions/upload-artifact` v7,
`actions/download-artifact` v8.

**Spec:**
- `doc/specs/System-Overview.md`: Clustering (Kubernetes pods, a StatefulSet, the node identity
  from the Downward API), the Management REST API (the probes and `/metrics` unauthenticated).
- `doc/specs/Configuration_Specification.md`: §4.3 `${VAR}` substitution, §6.3 `globals.api`
  (`address`, `tls`), §6.5 `globals.cluster` (`discovery: dns`, `discovery_dns_name`,
  `drain_timeout`), §6.5.1 `memberlist` (`node_name`, `advertise_addr`, the D-1 sourcing rule),
  §12 (a reload, not a restart, applies a changed config).
- `doc/research/Clustering_Architecture_Research.md`: D-1 (a StatefulSet), §9.3 (a node runs the
  cluster's config generation).
- `doc/v1.0/00-roadmap.md`: §2 (the layout: `deploy/docker`, `deploy/helm`), §4 M11, §5 (test
  gates, CI), §8 (the acceptance checklist), Q-M8-4.

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **No Rust code changes outside tests.** The binary, its config and its API stay as M10 left
  them; this milestone packages them.
- **Pins:** every image by its exact tag (`rust:1.98.1-trixie`, `cc-debian13:nonroot`,
  `rancher/k3s:v1.37.1-k3s1`, `kubeconform:v0.8.0`, `actionlint:1.7.12`); every action by its
  major, as the existing workflows do; Helm `v4.3.0`.
- **The image's build stage is the workspace's toolchain** (`rust-toolchain.toml`'s channel),
  and its runtime the same Debian release (13): a test checks both.
- **The chart's `version` and `appVersion` are the workspace's version** (`0.1.0` now; M12 makes
  it `1.0.0` with the tag): a test checks it, and the release refuses a tag that differs.
- **Nothing is published by this milestone.** The release workflow runs on a `v*` tag (M12's
  `v1.0.0`) or by hand; a manual run pushes nothing. PR CI builds the amd64 image (the `k3s`
  job), `weekly.yml` the arm64 one.
- **Test gates:**
  - Plain `cargo test` stays hermetic: `tests/packaging.rs` reads the repository's files and the
    config crate, nothing else.
  - The `k3s` scenario needs `DATA_AVALANCHE_IT=1`, `DATA_AVALANCHE_K3S=1`, the `e2e-hooks`
    feature, `test-infra/up.sh` (`postgres-18`, `rustfs`) and `test-infra/k3s.sh`; it runs on
    PostgreSQL only and with `--test-threads=1`.
  - The chart's lint, render and schema checks need Helm and Docker (kubeconform), the workflow
    check Docker (actionlint): CI's `check` job runs them.
- Code blocks are the verified content, already formatted. Every block is a `git apply`-ready
  diff, one per file; a step's diffs apply in the order given, after the earlier steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the conditions most likely to hurt an operator who installs the chart. Each is pinned
by a test in the task after the arrow.

1. **A pod deleted or evicted** (a node drain, a rolling upgrade, `kubectl delete`). Its
   pipelines must move to the other pods without a gap in the lake, and the pod must come back
   under its name, keeping its place on the ring; the grace period must outlast the drain, or
   Kubernetes kills the drain half-way → Task 3
   (`three_pods_run_a_pipeline_and_a_deleted_pod_hands_it_off`), Task 2
   (`a_deleted_pod_has_longer_than_the_drain_to_hand_off`).
2. **The API with TLS on** (`globals.api.tls.enabled`). The probes must speak HTTPS, or every pod
   is killed as unhealthy and the StatefulSet never settles → Task 2 (CI renders
   `ci/tls-values.yaml` and counts three `scheme: HTTPS` probes;
   `the_examples_configs_validate_with_the_pod_s_environment`).
3. **Object storage over TLS from the distroless image** (S3, GCS, Azure). rustls's platform
   verifier reads the system's CA bundle; an image without one fails every storage call with an
   unknown issuer → Task 3 (CI's `The image's CA bundle` step), Task 1's verification.
4. **A config given as text** (`--set-file config=config.yaml`) or as the operator's own
   ConfigMap. The chart can't read the ports from it, so the pods must still listen where the
   Services and the probes point → Task 3 (the scenario installs its config as text, on the
   default ports), Task 2 (`helm template` with the example map).
5. **An operator who edits the config and runs `helm upgrade`**, expecting it applied. A
   starting node adopts the cluster's config generation, not its file (config §12), so the chart
   restarts nothing and must say that the reload applies it → Task 2
   (`a_changed_config_is_said_to_go_through_the_reload`).

## Decisions and rulings

**Asked and answered** (2026-10-29, Q-M11-A…3 each the "Recommended" answer; Q-M11-4 Oleksii's
choice while planning). Oleksii approved the plan 2026-10-29; it needs no spec edit.

| # | Question | Answer |
|---|---|---|
| Q-M11-A | One plan for M11, or two? | **Two: M11a packaging, M11b documentation**, each its own branch and PR. M11b's deploy pages describe the chart that landed |
| Q-M11-1 | Where and when are the image and the chart published? | **GHCR, on a `v*` tag**: the image to `ghcr.io/<owner>/data-avalanche`, the chart as an OCI artifact to `ghcr.io/<owner>/charts`, with an SBOM and a provenance attestation. PR CI only builds; nothing is published before M12's `v1.0.0` |
| Q-M11-2 | Where does the documentation site live? | **GitHub Pages, from main** (M11b) |
| Q-M11-3 | Which runtime image, which architectures? | **distroless `cc`, linux/amd64 and linux/arm64**, each built on a runner of its own architecture (no QEMU: `aws-lc-sys` and DataFusion are slow to emulate) |
| Q-M11-4 | kind or k3d for the smoke deploy? | **k3s from test-infra's compose file, no cluster CLI** (k3d's image, `rancher/k3s`, run directly): one more compose service, on the network of the source and the store; no tool to install beside Docker, `kubectl` and `helm` |

**Rulings where the plan decides** (each pinned by a test or a measurement):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M11a.1 | **The image**: build on `rust:1.98.1-trixie` (the toolchain file's channel), run on `gcr.io/distroless/cc-debian13:nonroot` (the same Debian release, so the same glibc); `cargo build --release --locked -p data-avalanche` with the registry and `target/` as BuildKit caches; `strip --strip-debug` (250 → 235 MB binary, the image 263 MB); OCI labels with `VERSION` from a build argument; `EXPOSE 8080/tcp 7946/udp`; the entrypoint the binary, the command `run --config /etc/data-avalanche/config.yaml`. `.dockerignore` lets in only the manifests and `crates/` | The symbols stay so that a panic's backtrace names its functions, for an operator's bug report; a full strip (178 MB) would lose them. The cargo profile's `strip` given through `CARGO_PROFILE_RELEASE_STRIP` didn't relink a cached binary (measured), so the strip is explicit. The context leaves out `target/`, `prototypes/` and `test-infra/` (its throwaway TLS material among them) | Task 1 (`the_image_builds_with_the_workspace_s_toolchain`; the build, `--version`, the user 65532, the CA bundle) |
| D-M11a.2 | **The pod's identity**: a StatefulSet, `podManagementPolicy: Parallel`; the env `POD_NAME`, `POD_NAMESPACE`, `POD_IP` (the Downward API) and `DATA_AVALANCHE_DISCOVERY_DNS_NAME` (`<headless>.<ns>.svc.cluster.local`); the config names them through `${VAR}`. The headless Service publishes not-ready addresses | D-1 and config §6.5.1 (a stable `node_name`, the pod's name; `discovery: dns` on a headless Service); a node runs the cluster's generation with its own environment (§9.3). Parallel: the ring places a pod by its name, not its start order. A starting pod must find the others before it is ready | Task 2 (`the_chart_sets_the_environment_its_values_document`), Task 3 |
| D-M11a.3 | **The config**: `config` is a map (rendered with `toYaml`) or the file's text (`--set-file`), into a ConfigMap; `existingConfigMap` names the operator's own. A missing config fails `helm template` (`fail`). The API's and the gossip's ports and the probes' scheme follow the config when it is a map (`globals.api.address`, `memberlist.bind_port`, `globals.api.tls.enabled`), else `ports.*` and `apiTls` | An operator keeps one config file and the chart must not need a second copy of its ports; the text form keeps comments and `${VAR}`s as written | Task 2 (`helm template` with each example, the HTTPS count), Task 3 (text) |
| D-M11a.4 | **No restart on a changed config**: no checksum annotation; NOTES and the README say a changed config goes through `POST /v1/config/reload` | Config §12 and System-Overview ("a restart does not apply config-file changes"): restarting the pods would apply nothing and cost a drain per pod | Task 2 (`a_changed_config_is_said_to_go_through_the_reload`) |
| D-M11a.5 | **The pod's defaults**: run as 65532, read-only root, no capabilities, `RuntimeDefault` seccomp, no service-account token; `/tmp` an `emptyDir`; grace 90 s (the drain is 60 s); liveness and startup on `/health/live`, readiness on `/health/ready` (false while the node leaves); requests 500m and 1 GiB, a 2 GiB limit; a PodDisruptionBudget of one; `hostAliases` and the usual scheduling values | The binary writes nothing but to object storage; a pod deleted must hand off before it is killed (Review Focus 1); readiness takes a leaving node out of the API Service | Task 2 (`a_deleted_pod_has_longer_than_the_drain_to_hand_off`), Task 3 (the pods run under these defaults) |
| D-M11a.6 | **The smoke deploy on k3s** (Q-M11-4): `rancher/k3s:v1.37.1-k3s1` in `test-infra/compose.yaml`, profile `k3s`, privileged; its API on 56443, the chart's NodePort 30080 on 58080; node name `k3s` (and `hostname: k3s`); the kubelet evicting only under 1% free disk; Traefik, the metrics server and ServiceLB off. `test-infra/k3s.sh` starts it, waits for the node, builds the image (unless `--no-build`) and imports it with `ctr images import` | k3s's default eviction thresholds tainted the node `disk-pressure` on a Docker disk 96% full, and nothing scheduled (measured); a recreated container under a new hostname left a stale node holding CoreDNS (measured). The pods can't resolve compose names (NXDOMAIN; k3s forwards to the host's resolver, not Docker's 127.0.0.11, measured) but reach the containers' addresses: the scenario sets `hostAliases` from `docker inspect` | Task 3 (the scenario) |
| D-M11a.7 | **The scenario**: `E2e::new` (the source's tables, the run's prefix in RustFS) and its config, moved into the cluster (the source `postgres-18:5432`, the API on `0.0.0.0:8080`, DNS discovery, `${POD_NAME}`, `${POD_IP}`); `helm upgrade --install --wait`; three members through the NodePort; the snapshot converged; the holder's pod deleted; a higher epoch on another pod; new rows converged; the deleted pod back under its name (a new UID), a member and Ready; the release uninstalled when the test ends, failed or not | The roadmap's exit, as an operator would see it: the API and the lake, not the pods' logs | Task 3 (a headless Service selecting no pod fails it: "timed out waiting until three members") |
| D-M11a.8 | **The release** (Q-M11-1, Q-M11-3): on a `v*` tag, the workspace's version, the chart's `version` and `appVersion` and the tag must agree; each platform built on its own runner, pushed by digest with `sbom: true` and `provenance: mode=max`; the digests joined as `X.Y.Z`, `X.Y` and `latest` (a pre-release takes its own tag only); `attest-build-provenance` on the joined digest; the chart pushed to `oci://ghcr.io/<owner>/charts`; a GitHub release with the chart and an SPDX SBOM (`anchore/sbom-action`). A manual run builds both images and packages the chart, pushing nothing. `weekly.yml` builds the arm64 image and runs `--version` in it | A tag is the only publication, M12's `v1.0.0` the first. A break of the arm64 build would otherwise show only at the tag | Task 4 (`the_release_publishes_the_image_the_chart_pulls`, `the_chart_s_versions_are_the_workspace_s`, actionlint) |
| D-M11a.9 | **CI** (`ci.yml`): the `check` job lints and renders the chart with each `ci/*-values.yaml`, validates the objects with kubeconform (Kubernetes 1.37.0), checks the TLS example's HTTPS probes, and runs actionlint on every workflow; a new `k3s` job builds the amd64 image (cached per scope), checks its CA bundle, starts k3s and runs the scenario | The chart's structure is checked without a cluster on every change; the cluster run is one job beside the E2E jobs | Task 2, Task 3 |

**Measured while planning** (2026-10-29, on this branch's base, an x86_64 Mac with Docker
Desktop 29.9.2, 12 CPUs, 8 GiB; recorded in the tasks):

- Every task was built in a scratch workspace, test first, then replayed onto the base, stage by
  stage: each test stage committed and run (Step 2's output), then its implementation (Step 4's).
  The plan's diffs are those commits: the 32 blocks apply in order onto the base with `git
  apply` and give a tree identical to the replay's last commit.
- The image: the first build 20 min 16 s (DataFusion and `aws-lc-sys` dominate), a rebuild from
  the BuildKit caches 4 min 22 s; 263 MB; `--version` prints `data-avalanche 0.1.0`; the user
  65532; it starts on a read-only root (`docker run --read-only`); the CA bundle at
  `/etc/ssl/certs/ca-certificates.crt`. The toolchain file's components (rustfmt, clippy) are
  fetched by rustup inside the build, a few seconds.
- The binary: 250 MB, 235 MB without its debug info, 178 MB without its symbols too.
  `CARGO_PROFILE_RELEASE_STRIP=debuginfo` with a warm `target/` cache finished in 3 s and left the
  binary as it was: the strip is a step of its own.
- Helm 4.3.0: `helm lint --strict` logs a template's `fail` at INFO and passes; `helm template`
  fails with it. CI therefore renders each example, not only lints it.
- kubeconform 0.8.0, Kubernetes 1.37.0's schemas: the six objects of each example valid.
- actionlint 1.7.12: the three existing workflows clean; it caught an unquoted `$(printf …)` in
  the release's manifest step (SC2046) and a `k3s::` test filter on a plain `run:` line (read as a
  YAML mapping), both fixed in the blocks below.
- k3s v1.37.1+k3s1 in Docker:
  - With the kubelet's default eviction thresholds and the Docker disk 96% full, the node was
    tainted `disk-pressure` and nothing scheduled; under 1% the node took the pods.
  - Recreating the container under a new hostname left the old node in the cluster's state (its
    anonymous volumes), CoreDNS still on it: the node name is fixed.
  - A pod resolves the cluster's names (`kubernetes.default` → 10.43.0.1) but not the compose
    services' (`postgres-18`: NXDOMAIN); it reaches the containers' addresses (TCP 5432 open).
  - The scenario: 17–22 s in three runs (the image local, the harness's 6 s lease), e.g.
    `smoke-data-avalanche-1` (epoch 1) handed off to `smoke-data-avalanche-0` (epoch 2), the
    deleted pod back under its name. The replay caught `k3s.sh` naming the compose file from the
    repository's root; the block below runs from `test-infra/`.
- The workspace: 1332 tests at the base, 1339 after Task 4 (the seven of `packaging.rs`; the
  E2E suite's scenario runs under `e2e-hooks` only).
- Mutation checks, each run against its test:
  - Task 1: the build stage on `rust:1.98.0-trixie` fails
    `the_image_builds_with_the_workspace_s_toolchain`.
  - Task 2: `terminationGracePeriodSeconds: 60` fails
    `a_deleted_pod_has_longer_than_the_drain_to_hand_off` ("60 s, the drain 60s"); `version:
    0.2.0` fails `the_chart_s_versions_are_the_workspace_s`; the example without
    `discovery_dns_name` fails `the_examples_configs_validate_with_the_pod_s_environment`
    (V-096).
  - Task 3: a headless Service selecting no pod fails the scenario ("timed out waiting until
    three members", 127 s).
  - Task 4: the arm64 image on `ubuntu-24.04` fails
    `the_release_publishes_the_image_the_chart_pulls`.

**Known edges, accepted:**

- The release workflow can't run before it is on `main`, and a tag is its first full run (M12's
  `v1.0.0`). After the merge, a manual run on `main` proves both builds and the chart's package
  (it pushes nothing). The first push creates the GHCR packages private: making them public is a
  one-time step in the repository's package settings (new gap, DA-OPS-11).
- The scenario doesn't tell a drained hand-off from a takeover after the lease expired: both give
  another pod the lease at a higher epoch. The grace (90 s) against the drain (60 s) is pinned by
  Task 2's test; M8's E2E suite covers the drain itself.
- The image is 263 MB, most of it the binary; M12's compile-time and dependency review
  (Q-M5-24) is where it shrinks, if it does.
- `ServiceMonitor` and the Prometheus Operator's objects aren't in the chart: the pods take
  `podAnnotations` for a scrape config, and `/metrics` is on the API's port.

**Documents this plan changes:**

- Applied at plan approval, in an `infra:` and a `gap:` commit:
  - Roadmap:
    - §2: `deploy/docker/Dockerfile`, `deploy/helm/data-avalanche/`, `test-infra/k3s.sh`; `xtask/`
      and `docs/` stay M11b's.
    - §4: M11 split into M11a (packaging, this plan; its exit: `helm lint` and `helm template`,
      the k3s smoke deploy with a pod deleted, the arm64 build weekly) and M11b (the MkDocs site,
      the API and config references, the runbooks; its exit: `mkdocs build --strict`, every
      config example of the docs validated by `xtask`).
    - §5.2: the `k3s` service; §5.4: the chart and workflow checks, the `k3s` job, the weekly
      arm64 build, `release.yml`.
    - §8: "kind smoke deploy green" → "k3s smoke deploy green".
    - §9: D-IMPL-22 for D-M11a.1…9; §9.1 Q-M11-A, Q-M11-1…4.
  - `doc/v1.0/README.md`: the M11a row.
  - `CDC_Design_Gap_Analysis_v1.md`, §46 for M11a's plan: DA-OPS-11 🆕 (the release's first run
    is the tag's; the GHCR packages start private).
- No spec edit: System-Overview's Clustering bullets (a StatefulSet, the Downward API) and config
  §6.5.1's sourcing rule are what the chart builds.
- `test-infra/README.md`: the `k3s` service and the smoke deploy (Task 3).

---

## File Structure

| File | Responsibility |
|---|---|
| `deploy/docker/Dockerfile`, `.dockerignore` (new) | The image (Task 1) |
| `crates/data-avalanche/tests/packaging.rs` (new) | The image's toolchain, the chart's versions, examples, environment, grace and reload note, the release's names (Tasks 1, 2, 4) |
| `deploy/helm/data-avalanche/` (new): `Chart.yaml`, `values.yaml`, `.helmignore`, `README.md`, `templates/{_helpers.tpl,configmap.yaml,serviceaccount.yaml,service.yaml,pdb.yaml,statefulset.yaml,NOTES.txt}`, `ci/{example,tls}-values.yaml` | The chart (Task 2) |
| `.github/workflows/ci.yml` | The chart and workflow checks (Task 2); the `k3s` job (Task 3) |
| `crates/data-avalanche-testkit/src/lib.rs` | `k3s_enabled`, `require_k3s!` (Task 3) |
| `crates/data-avalanche/tests/e2e/{harness,main}.rs`, `k3s.rs` (new) | `get_json`; the smoke deploy's scenario (Task 3) |
| `test-infra/compose.yaml`, `k3s.sh` (new), `k3s/values.yaml` (new), `down.sh`, `README.md`, `.gitignore` | The `k3s` service, its start script, the chart's values for it (Task 3) |
| `.github/workflows/release.yml` (new), `weekly.yml` | The release; the weekly arm64 build (Task 4) |

---

### Task 1: The image (`deploy/docker`)

**Files:**
- Create: `deploy/docker/Dockerfile`, `.dockerignore`
- Test: `crates/data-avalanche/tests/packaging.rs` (new)

**Interfaces:**
- Consumes: the binary `data-avalanche` and its `run --config <file>` (M6b); `rust-toolchain.toml`.
- Produces: the image, built from the repository's root with `docker build -f
  deploy/docker/Dockerfile .`: the binary at `/usr/local/bin/data-avalanche`, the user 65532, the
  command `run --config /etc/data-avalanche/config.yaml`, ports 8080/tcp and 7946/udp, the
  build argument `VERSION` (the `org.opencontainers.image.version` label, `dev` by default).
  `packaging.rs`'s helpers `repo(path)` and `value(text, prefix)`, which Tasks 2 and 4 extend.

- [ ] **Step 1: Write the failing test**

`crates/data-avalanche/tests/packaging.rs`:

```diff
diff --git a/crates/data-avalanche/tests/packaging.rs b/crates/data-avalanche/tests/packaging.rs
new file mode 100644
index 0000000..909d6c9
--- /dev/null
+++ b/crates/data-avalanche/tests/packaging.rs
@@ -0,0 +1,34 @@
+//! The image, the Helm chart and the release against the workspace (roadmap §4 M11a): the
+//! Dockerfile's toolchain, the chart's versions, its example config and its defaults, what the
+//! release publishes. `helm lint`, `helm template` and kubeconform check the chart itself in CI,
+//! and the E2E suite's k3s scenario runs it.
+
+use std::path::PathBuf;
+
+/// A file of the repository, by its path from the root.
+fn repo(file: &str) -> String {
+    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
+        .join("../..")
+        .join(file);
+    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
+}
+
+/// The value of the first line `<prefix><value>` of `text`, quotes taken off.
+fn value(text: &str, prefix: &str) -> String {
+    text.lines()
+        .find_map(|l| l.strip_prefix(prefix))
+        .unwrap_or_else(|| panic!("no {prefix:?}"))
+        .trim_matches('"')
+        .to_owned()
+}
+
+#[test]
+fn the_image_builds_with_the_workspace_s_toolchain() {
+    let channel = value(&repo("rust-toolchain.toml"), "channel = ");
+    let dockerfile = repo("deploy/docker/Dockerfile");
+    assert!(
+        dockerfile.contains(&format!("FROM rust:{channel}-trixie AS build\n")),
+        "the build stage isn't rust:{channel}"
+    );
+    assert!(dockerfile.contains("FROM gcr.io/distroless/cc-debian13:nonroot\n"));
+}
```


- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p data-avalanche --test packaging`
Expected: FAIL: `the_image_builds_with_the_workspace_s_toolchain` panics with
`…/deploy/docker/Dockerfile: No such file or directory`.

- [ ] **Step 3: Write the Dockerfile and its context's filter**

`.dockerignore`:

```diff
diff --git a/.dockerignore b/.dockerignore
new file mode 100644
index 0000000..f473bda
--- /dev/null
+++ b/.dockerignore
@@ -0,0 +1,7 @@
+# Only what the build needs goes into the image's build context.
+**
+!Cargo.toml
+!Cargo.lock
+!rust-toolchain.toml
+!crates/
+crates/**/target/
```

`deploy/docker/Dockerfile`:

```diff
diff --git a/deploy/docker/Dockerfile b/deploy/docker/Dockerfile
new file mode 100644
index 0000000..145c58c
--- /dev/null
+++ b/deploy/docker/Dockerfile
@@ -0,0 +1,26 @@
+# syntax=docker/dockerfile:1.7
+# The data-avalanche image (roadmap §4 M11a): the release binary on distroless, as the
+# non-root user. Built from the repository root:
+#   docker build -f deploy/docker/Dockerfile -t data-avalanche .
+
+FROM rust:1.98.1-trixie AS build
+WORKDIR /src
+COPY . .
+# The target directory is a cache across builds. Debug info is stripped, the symbols kept: a
+# panic's backtrace still names its functions.
+RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
+    --mount=type=cache,target=/src/target,sharing=locked \
+    cargo build --release --locked -p data-avalanche --bin data-avalanche \
+    && cp target/release/data-avalanche /data-avalanche \
+    && strip --strip-debug /data-avalanche
+
+FROM gcr.io/distroless/cc-debian13:nonroot
+ARG VERSION=dev
+LABEL org.opencontainers.image.title="data-avalanche" \
+      org.opencontainers.image.description="Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake" \
+      org.opencontainers.image.source="https://github.com/alex-karpenko/data-avalanche" \
+      org.opencontainers.image.version="${VERSION}"
+COPY --from=build /data-avalanche /usr/local/bin/data-avalanche
+EXPOSE 8080/tcp 7946/udp
+ENTRYPOINT ["/usr/local/bin/data-avalanche"]
+CMD ["run", "--config", "/etc/data-avalanche/config.yaml"]
```


- [ ] **Step 4: Run the test, build the image and run it**

Run: `cargo test -p data-avalanche --test packaging`
Expected: PASS (1 test).

Run:
```sh
docker build -f deploy/docker/Dockerfile -t data-avalanche:dev .
docker run --rm data-avalanche:dev --version
docker inspect data-avalanche:dev --format '{{.Config.User}}'
id=$(docker create data-avalanche:dev); docker cp "$id:/etc/ssl/certs/ca-certificates.crt" - | tar -tv; docker rm "$id"
```
Expected: the build succeeds (about 20 minutes the first time, minutes from its caches);
`data-avalanche 0.1.0`; `65532`; the CA bundle listed.

- [ ] **Step 5: Commit**

```bash
git add .dockerignore deploy/docker/Dockerfile crates/data-avalanche/tests/packaging.rs
git commit -m "build(deploy): the image — the release binary on distroless cc (Debian 13) as the non-root user, debug info stripped"
```

### Task 2: The Helm chart (`deploy/helm/data-avalanche`)

**Files:**
- Create: `deploy/helm/data-avalanche/{Chart.yaml,values.yaml,.helmignore,README.md}`,
  `deploy/helm/data-avalanche/templates/{_helpers.tpl,configmap.yaml,serviceaccount.yaml,service.yaml,pdb.yaml,statefulset.yaml,NOTES.txt}`,
  `deploy/helm/data-avalanche/ci/{example,tls}-values.yaml`
- Modify: `.github/workflows/ci.yml`
- Test: `crates/data-avalanche/tests/packaging.rs`

**Interfaces:**
- Consumes: Task 1's image and `packaging.rs` helpers; `data_avalanche_config::{validate_str,
  load_str, FingerprintKey}` (M1); the dev-dependency `rcgen` (a throwaway certificate).
- Produces:
  - the release `<name>-data-avalanche`: the StatefulSet (pods `<fullname>-0…n`), the Service
    `<fullname>` (port `api`), the headless Service `<fullname>-headless` (port `gossip`, UDP),
    the ConfigMap `<fullname>` (key `configKey`), the ServiceAccount, the PodDisruptionBudget;
  - the pods' environment: `POD_NAME`, `POD_NAMESPACE`, `POD_IP`,
    `DATA_AVALANCHE_DISCOVERY_DNS_NAME`;
  - the values Task 3 sets: `image.{repository,tag,pullPolicy}`, `service.{type,nodePort}`, `env`,
    `hostAliases`, `resources`, `podDisruptionBudget.enabled`, `config` (text);
  - CI's `check` steps `Helm chart` and `Workflows` (actionlint, which Task 4's workflow passes).

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche/tests/packaging.rs`:

```diff
diff --git a/crates/data-avalanche/tests/packaging.rs b/crates/data-avalanche/tests/packaging.rs
index 909d6c9..85c9412 100644
--- a/crates/data-avalanche/tests/packaging.rs
+++ b/crates/data-avalanche/tests/packaging.rs
@@ -3,8 +3,11 @@
 //! release publishes. `helm lint`, `helm template` and kubeconform check the chart itself in CI,
 //! and the E2E suite's k3s scenario runs it.
 
+use std::collections::BTreeMap;
 use std::path::PathBuf;
 
+use data_avalanche_config::{FingerprintKey, load_str, validate_str};
+
 /// A file of the repository, by its path from the root.
 fn repo(file: &str) -> String {
     let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
@@ -13,6 +16,10 @@ fn repo(file: &str) -> String {
     std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
 }
 
+fn chart(file: &str) -> String {
+    repo(&format!("deploy/helm/data-avalanche/{file}"))
+}
+
 /// The value of the first line `<prefix><value>` of `text`, quotes taken off.
 fn value(text: &str, prefix: &str) -> String {
     text.lines()
@@ -32,3 +39,95 @@ fn the_image_builds_with_the_workspace_s_toolchain() {
     );
     assert!(dockerfile.contains("FROM gcr.io/distroless/cc-debian13:nonroot\n"));
 }
+
+#[test]
+fn the_chart_s_versions_are_the_workspace_s() {
+    // The release refuses a tag unless all three agree (.github/workflows/release.yml).
+    let workspace = value(&repo("Cargo.toml"), "version = ");
+    let chart = chart("Chart.yaml");
+    assert_eq!(value(&chart, "version: "), workspace);
+    assert_eq!(value(&chart, "appVersion: "), workspace);
+    assert_eq!(workspace, env!("CARGO_PKG_VERSION"));
+}
+
+/// The value of a top-level key written as a block: the lines under it, two spaces taken off.
+fn block(values: &str, key: &str) -> String {
+    let start = format!("{key}:");
+    let mut lines = values.lines().skip_while(|l| *l != start).skip(1);
+    let mut text = String::new();
+    for line in lines.by_ref() {
+        if !line.is_empty() && !line.starts_with(' ') {
+            break;
+        }
+        text.push_str(line.strip_prefix("  ").unwrap_or(line));
+        text.push('\n');
+    }
+    assert!(!text.trim().is_empty(), "no {key} block");
+    text
+}
+
+/// What the chart puts in every pod's environment, and the secrets the examples' configs name:
+/// a password, and the API's certificate and key as PEM text, made for the test.
+fn pod_env() -> BTreeMap<String, String> {
+    let tls = rcgen::generate_simple_self_signed(vec!["data-avalanche".into()]).unwrap();
+    [
+        ("POD_NAME", "data-avalanche-0".to_owned()),
+        ("POD_NAMESPACE", "cdc".to_owned()),
+        ("POD_IP", "10.0.0.5".to_owned()),
+        (
+            "DATA_AVALANCHE_DISCOVERY_DNS_NAME",
+            "data-avalanche-headless.cdc.svc.cluster.local".to_owned(),
+        ),
+        ("ORDERS_DB_PASSWORD", "secret".to_owned()),
+        ("API_TLS_CERT", tls.cert.pem()),
+        ("API_TLS_KEY", tls.signing_key.serialize_pem()),
+    ]
+    .into_iter()
+    .map(|(k, v)| (k.to_owned(), v))
+    .collect()
+}
+
+#[test]
+fn the_examples_configs_validate_with_the_pod_s_environment() {
+    for example in ["ci/example-values.yaml", "ci/tls-values.yaml"] {
+        let config = block(&chart(example), "config");
+        let report = validate_str(&config, &pod_env());
+        assert!(report.is_ok(), "{example}: {:#?}\n{config}", report.errors);
+    }
+}
+
+#[test]
+fn the_chart_sets_the_environment_its_values_document() {
+    let statefulset = chart("templates/statefulset.yaml");
+    let values = chart("values.yaml");
+    for name in [
+        "POD_NAME",
+        "POD_NAMESPACE",
+        "POD_IP",
+        "DATA_AVALANCHE_DISCOVERY_DNS_NAME",
+    ] {
+        assert!(statefulset.contains(&format!("- name: {name}\n")), "{name}");
+        assert!(values.contains(name), "values.yaml doesn't document {name}");
+    }
+}
+
+#[test]
+fn a_deleted_pod_has_longer_than_the_drain_to_hand_off() {
+    let config = block(&chart("ci/example-values.yaml"), "config");
+    let loaded = load_str(&config, &pod_env(), &FingerprintKey::from_bytes([0; 32])).unwrap();
+    let drain = loaded.config.globals.cluster.drain_timeout;
+    let grace: u64 = value(&chart("values.yaml"), "terminationGracePeriodSeconds: ")
+        .parse()
+        .unwrap();
+    assert!(grace > drain.as_secs(), "{grace} s, the drain {drain:?}");
+}
+
+#[test]
+fn a_changed_config_is_said_to_go_through_the_reload() {
+    // A starting node adopts the cluster's config generation, not its file (config §12): the
+    // chart restarts nothing on a changed ConfigMap, and tells the operator so.
+    for text in [chart("templates/NOTES.txt"), chart("README.md")] {
+        assert!(text.contains("POST /v1/config/reload"), "{text}");
+    }
+    assert!(!chart("templates/statefulset.yaml").contains("checksum/"));
+}
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche --test packaging`
Expected: FAIL: five of six, each with `…/deploy/helm/data-avalanche/…: No such file or
directory` (`Chart.yaml`, `ci/example-values.yaml`, `templates/statefulset.yaml`,
`templates/NOTES.txt`); the image's test passes.

- [ ] **Step 3: Write the chart and its CI checks**

`.github/workflows/ci.yml`:

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index f6c8ae4..f274602 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -40,6 +40,23 @@ jobs:
         run: >-
           docker run --rm -v "$PWD/deploy/prometheus:/rules:ro" --entrypoint promtool
           prom/prometheus:v3.15.0 test rules /rules/alerts.test.yaml
+      # The Helm chart (M11a): lint, render with its example values, and validate the objects
+      # against the Kubernetes API's schemas; chart.rs validates the example's config.
+      - uses: azure/setup-helm@v5
+        with:
+          version: v4.3.0
+      - name: Helm chart
+        run: |
+          chart=deploy/helm/data-avalanche
+          for values in "$chart"/ci/*-values.yaml; do
+            helm lint --strict "$chart" -f "$values"
+            helm template ci "$chart" -f "$values" \
+              | docker run --rm -i ghcr.io/yannh/kubeconform:v0.8.0 -strict -summary -kubernetes-version 1.37.0 -
+          done
+          # The API's TLS turns the probes to HTTPS.
+          [ "$(helm template ci "$chart" -f "$chart/ci/tls-values.yaml" | grep -c 'scheme: HTTPS')" = 3 ]
+      - name: Workflows
+        run: docker run --rm -v "$PWD:/repo:ro" --workdir /repo rhysd/actionlint:1.7.12 -no-color
       # RustSec advisory audit of Cargo.lock. Non-blocking by design: a vulnerable dependency is
       # reported (warning annotation + failed step) but never fails the job.
       - name: Install cargo-audit
@@ -154,3 +171,4 @@ jobs:
       - name: Stop the containers
         if: always()
         run: docker compose -f test-infra/compose.yaml down -v
+
```

`deploy/helm/data-avalanche/.helmignore`:

```diff
diff --git a/deploy/helm/data-avalanche/.helmignore b/deploy/helm/data-avalanche/.helmignore
new file mode 100644
index 0000000..41d5d09
--- /dev/null
+++ b/deploy/helm/data-avalanche/.helmignore
@@ -0,0 +1,2 @@
+ci/
+*.tgz
```

`deploy/helm/data-avalanche/Chart.yaml`:

```diff
diff --git a/deploy/helm/data-avalanche/Chart.yaml b/deploy/helm/data-avalanche/Chart.yaml
new file mode 100644
index 0000000..0dd7c0b
--- /dev/null
+++ b/deploy/helm/data-avalanche/Chart.yaml
@@ -0,0 +1,9 @@
+apiVersion: v2
+name: data-avalanche
+description: Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake tables
+type: application
+version: 0.1.0
+appVersion: "0.1.0"
+home: https://github.com/alex-karpenko/data-avalanche
+sources:
+  - https://github.com/alex-karpenko/data-avalanche
```

`deploy/helm/data-avalanche/README.md`:

```diff
diff --git a/deploy/helm/data-avalanche/README.md b/deploy/helm/data-avalanche/README.md
new file mode 100644
index 0000000..7902fb0
--- /dev/null
+++ b/deploy/helm/data-avalanche/README.md
@@ -0,0 +1,61 @@
+# The data-avalanche Helm chart
+
+The data-avalanche nodes as a StatefulSet: a pod's name is its node name (the Downward API's
+`POD_NAME`), so a restarted pod keeps its place on the ring (config §6.5.1, D-1). The pods find
+each other through a headless Service (`discovery: dns`); their state, the leases and the config
+generations are in object storage, so the pods have no volumes.
+
+```sh
+helm install cdc oci://ghcr.io/alex-karpenko/charts/data-avalanche --version 0.1.0 \
+  --set-file config=config.yaml -f values.yaml
+```
+
+## The config
+
+`config` is the data-avalanche config, a map in the values or the file's text (`--set-file`);
+`existingConfigMap` names a ConfigMap of your own instead. Every pod has in its environment:
+
+| Variable | Value |
+|---|---|
+| `POD_NAME`, `POD_NAMESPACE`, `POD_IP` | the Downward API's |
+| `DATA_AVALANCHE_DISCOVERY_DNS_NAME` | the headless Service's DNS name |
+
+so the config's cluster block reads:
+
+```yaml
+globals:
+  cluster:
+    discovery: dns
+    discovery_dns_name: ${DATA_AVALANCHE_DISCOVERY_DNS_NAME}
+    memberlist:
+      node_name: ${POD_NAME}
+      advertise_addr: ${POD_IP}
+```
+
+Secrets reach the config's `${VAR}`s through `env` or `envFrom`; a cloud identity through
+`serviceAccount.annotations`. `ci/example-values.yaml` is a complete example, `ci/tls-values.yaml`
+one with the API's TLS on.
+
+A changed config reaches a running cluster through the API's reload (`POST /v1/config/reload`),
+not through `helm upgrade` alone: a starting node adopts the cluster's latest config generation,
+not its file (config §12). The chart has no checksum annotation that would restart the pods.
+
+## Values
+
+| Key | Default | |
+|---|---|---|
+| `config` | `{}` | The config, a map or text. Required unless `existingConfigMap` |
+| `existingConfigMap`, `configKey` | `""`, `config.yaml` | A ConfigMap of your own, and its key |
+| `ports.api`, `ports.gossip`, `apiTls` | `8080`, `7946`, `false` | When the config isn't a map that names them (`globals.api.address`, `memberlist.bind_port`, `globals.api.tls.enabled`) |
+| `replicaCount` | `3` | |
+| `image.repository`, `image.tag`, `image.pullPolicy` | `ghcr.io/alex-karpenko/data-avalanche`, the chart's `appVersion`, `IfNotPresent` | |
+| `env`, `envFrom` | `[]` | The secrets the config names |
+| `serviceAccount.create`, `.name`, `.annotations` | `true`, `""`, `{}` | |
+| `service.type`, `.nodePort`, `.annotations` | `ClusterIP`, `null`, `{}` | The API, `/metrics` and the probes |
+| `terminationGracePeriodSeconds` | `90` | Above `drain_timeout` (60 s): a deleted pod hands its pipelines off first |
+| `livenessProbe`, `readinessProbe`, `startupProbe` | `/health/live`, `/health/ready`, `/health/live` | HTTPS when the API's TLS is on |
+| `resources` | requests 500m, 1Gi; limit 2Gi | |
+| `podDisruptionBudget.enabled`, `.maxUnavailable` | `true`, `1` | |
+| `podSecurityContext`, `securityContext` | non-root (65532), read-only root, no capabilities | `/tmp` is an `emptyDir` |
+| `hostAliases` | `[]` | Names the pods resolve beside the cluster's DNS |
+| `podAnnotations`, `podLabels`, `nodeSelector`, `tolerations`, `affinity`, `topologySpreadConstraints`, `extraVolumes`, `extraVolumeMounts`, `imagePullSecrets` | empty | |
```

`deploy/helm/data-avalanche/ci/example-values.yaml`:

```diff
diff --git a/deploy/helm/data-avalanche/ci/example-values.yaml b/deploy/helm/data-avalanche/ci/example-values.yaml
new file mode 100644
index 0000000..041df81
--- /dev/null
+++ b/deploy/helm/data-avalanche/ci/example-values.yaml
@@ -0,0 +1,40 @@
+# A three-node cluster replicating a PostgreSQL database into Delta tables on S3. Checked by
+# `helm lint`, `helm template` and kubeconform in CI, and its config validated by the
+# data-avalanche config crate (crates/data-avalanche/tests/chart.rs).
+replicaCount: 3
+env:
+  - name: ORDERS_DB_PASSWORD
+    valueFrom:
+      secretKeyRef: { name: orders-db, key: password }
+serviceAccount:
+  annotations:
+    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/data-avalanche
+config:
+  config_version: 1
+  globals:
+    cloud: aws
+    state: { bucket: example-cdc-state, prefix: cdc }
+    api: { address: "0.0.0.0:8080" }
+    log: { level: info, format: json }
+    cluster:
+      discovery: dns
+      discovery_dns_name: ${DATA_AVALANCHE_DISCOVERY_DNS_NAME}
+      memberlist:
+        node_name: ${POD_NAME}
+        advertise_addr: ${POD_IP}
+        bind_port: 7946
+        advertise_port: 7946
+  pipelines:
+    orders:
+      id: orders
+      source:
+        host: orders-db.example.internal
+        db: orders
+        user: cdc
+        password: ${ORDERS_DB_PASSWORD}
+        postgres: { publication: orders_pub, slot: orders_slot }
+      tables: { include: [public.*] }
+      sink:
+        type: delta
+        bucket: example-lake
+        prefix: orders
```

`deploy/helm/data-avalanche/ci/tls-values.yaml`:

```diff
diff --git a/deploy/helm/data-avalanche/ci/tls-values.yaml b/deploy/helm/data-avalanche/ci/tls-values.yaml
new file mode 100644
index 0000000..2204db5
--- /dev/null
+++ b/deploy/helm/data-avalanche/ci/tls-values.yaml
@@ -0,0 +1,30 @@
+# The API with TLS on: the probes must speak HTTPS (CI renders this file and checks it). The
+# certificate and key are PEM text from a Secret (config §4.4).
+config:
+  config_version: 1
+  globals:
+    cloud: aws
+    state: { bucket: example-cdc-state, prefix: cdc }
+    api:
+      address: "0.0.0.0:8443"
+      tls: { enabled: true, cert: "${API_TLS_CERT}", key: "${API_TLS_KEY}" }
+    cluster:
+      discovery: dns
+      discovery_dns_name: ${DATA_AVALANCHE_DISCOVERY_DNS_NAME}
+      memberlist: { node_name: "${POD_NAME}", advertise_addr: "${POD_IP}" }
+  pipelines:
+    orders:
+      id: orders
+      source: { host: orders-db.example.internal, db: orders, user: cdc, password: "${ORDERS_DB_PASSWORD}", postgres: { publication: orders_pub, slot: orders_slot } }
+      tables: { include: [public.*] }
+      sink: { type: delta, bucket: example-lake, prefix: orders }
+env:
+  - name: ORDERS_DB_PASSWORD
+    valueFrom:
+      secretKeyRef: { name: orders-db, key: password }
+  - name: API_TLS_CERT
+    valueFrom:
+      secretKeyRef: { name: data-avalanche-tls, key: tls.crt }
+  - name: API_TLS_KEY
+    valueFrom:
+      secretKeyRef: { name: data-avalanche-tls, key: tls.key }
```

`deploy/helm/data-avalanche/templates/NOTES.txt`:

```diff
diff --git a/deploy/helm/data-avalanche/templates/NOTES.txt b/deploy/helm/data-avalanche/templates/NOTES.txt
new file mode 100644
index 0000000..becb7ce
--- /dev/null
+++ b/deploy/helm/data-avalanche/templates/NOTES.txt
@@ -0,0 +1,8 @@
+{{ .Values.replicaCount }} data-avalanche node(s) start as the StatefulSet {{ include "data-avalanche.fullname" . }}.
+
+The API, /metrics and the probes: {{ include "data-avalanche.fullname" . }}.{{ .Release.Namespace }}:{{ include "data-avalanche.apiPort" . }}
+  kubectl -n {{ .Release.Namespace }} port-forward svc/{{ include "data-avalanche.fullname" . }} {{ include "data-avalanche.apiPort" . }}
+  curl localhost:{{ include "data-avalanche.apiPort" . }}/v1/cluster/status
+
+A changed config reaches the running cluster through the API's reload (POST /v1/config/reload),
+not through a restart: a starting node adopts the cluster's latest config generation.
```

`deploy/helm/data-avalanche/templates/_helpers.tpl`:

```diff
diff --git a/deploy/helm/data-avalanche/templates/_helpers.tpl b/deploy/helm/data-avalanche/templates/_helpers.tpl
new file mode 100644
index 0000000..f04f0bc
--- /dev/null
+++ b/deploy/helm/data-avalanche/templates/_helpers.tpl
@@ -0,0 +1,73 @@
+{{/* The release's name for its objects, at most 63 characters. */}}
+{{- define "data-avalanche.fullname" -}}
+{{- if contains .Chart.Name .Release.Name -}}
+{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
+{{- else -}}
+{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
+{{- end -}}
+{{- end -}}
+
+{{- define "data-avalanche.headless" -}}
+{{- printf "%s-headless" (include "data-avalanche.fullname" .) | trunc 63 | trimSuffix "-" -}}
+{{- end -}}
+
+{{- define "data-avalanche.selectorLabels" -}}
+app.kubernetes.io/name: {{ .Chart.Name }}
+app.kubernetes.io/instance: {{ .Release.Name }}
+{{- end -}}
+
+{{- define "data-avalanche.labels" -}}
+{{ include "data-avalanche.selectorLabels" . }}
+app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
+app.kubernetes.io/managed-by: {{ .Release.Service }}
+helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
+{{- end -}}
+
+{{- define "data-avalanche.serviceAccountName" -}}
+{{- if .Values.serviceAccount.create -}}
+{{- .Values.serviceAccount.name | default (include "data-avalanche.fullname" .) -}}
+{{- else -}}
+{{- .Values.serviceAccount.name | default "default" -}}
+{{- end -}}
+{{- end -}}
+
+{{- define "data-avalanche.configMap" -}}
+{{- .Values.existingConfigMap | default (include "data-avalanche.fullname" .) -}}
+{{- end -}}
+
+{{/* The config as a map: {} when it is given as text (ports and TLS then come from values). */}}
+{{- define "data-avalanche.configMap.values" -}}
+{{- if kindIs "map" .Values.config -}}{{- toYaml .Values.config -}}{{- else -}}{}{{- end -}}
+{{- end -}}
+
+{{/* The API's port: globals.api.address's, else ports.api. */}}
+{{- define "data-avalanche.apiPort" -}}
+{{- $config := include "data-avalanche.configMap.values" . | fromYaml -}}
+{{- $address := dig "globals" "api" "address" "" $config -}}
+{{- if $address -}}
+{{- last (splitList ":" $address) -}}
+{{- else -}}
+{{- .Values.ports.api -}}
+{{- end -}}
+{{- end -}}
+
+{{/* The gossip's port: memberlist.bind_port's, else ports.gossip. */}}
+{{- define "data-avalanche.gossipPort" -}}
+{{- $config := include "data-avalanche.configMap.values" . | fromYaml -}}
+{{- dig "globals" "cluster" "memberlist" "bind_port" .Values.ports.gossip $config -}}
+{{- end -}}
+
+{{/* HTTPS when the config turns the API's TLS on. */}}
+{{- define "data-avalanche.scheme" -}}
+{{- $config := include "data-avalanche.configMap.values" . | fromYaml -}}
+{{- if dig "globals" "api" "tls" "enabled" (.Values.apiTls | default false) $config -}}HTTPS{{- else -}}HTTP{{- end -}}
+{{- end -}}
+
+{{/* A probe from values, its scheme the API's. */}}
+{{- define "data-avalanche.probe" -}}
+{{- $probe := deepCopy .probe -}}
+{{- if hasKey $probe "httpGet" -}}
+{{- $_ := set $probe.httpGet "scheme" (include "data-avalanche.scheme" .root) -}}
+{{- end -}}
+{{- toYaml $probe -}}
+{{- end -}}
```

`deploy/helm/data-avalanche/templates/configmap.yaml`:

```diff
diff --git a/deploy/helm/data-avalanche/templates/configmap.yaml b/deploy/helm/data-avalanche/templates/configmap.yaml
new file mode 100644
index 0000000..a5808cd
--- /dev/null
+++ b/deploy/helm/data-avalanche/templates/configmap.yaml
@@ -0,0 +1,18 @@
+{{- if not .Values.existingConfigMap }}
+{{- if not .Values.config }}
+{{- fail "config is required (the data-avalanche config), unless existingConfigMap names a ConfigMap of your own" }}
+{{- end }}
+apiVersion: v1
+kind: ConfigMap
+metadata:
+  name: {{ include "data-avalanche.fullname" . }}
+  labels:
+    {{- include "data-avalanche.labels" . | nindent 4 }}
+data:
+  {{ .Values.configKey }}: |
+    {{- if kindIs "string" .Values.config }}
+    {{- .Values.config | nindent 4 }}
+    {{- else }}
+    {{- toYaml .Values.config | nindent 4 }}
+    {{- end }}
+{{- end }}
```

`deploy/helm/data-avalanche/templates/pdb.yaml`:

```diff
diff --git a/deploy/helm/data-avalanche/templates/pdb.yaml b/deploy/helm/data-avalanche/templates/pdb.yaml
new file mode 100644
index 0000000..54830cd
--- /dev/null
+++ b/deploy/helm/data-avalanche/templates/pdb.yaml
@@ -0,0 +1,13 @@
+{{- if .Values.podDisruptionBudget.enabled }}
+apiVersion: policy/v1
+kind: PodDisruptionBudget
+metadata:
+  name: {{ include "data-avalanche.fullname" . }}
+  labels:
+    {{- include "data-avalanche.labels" . | nindent 4 }}
+spec:
+  maxUnavailable: {{ .Values.podDisruptionBudget.maxUnavailable }}
+  selector:
+    matchLabels:
+      {{- include "data-avalanche.selectorLabels" . | nindent 6 }}
+{{- end }}
```

`deploy/helm/data-avalanche/templates/service.yaml`:

```diff
diff --git a/deploy/helm/data-avalanche/templates/service.yaml b/deploy/helm/data-avalanche/templates/service.yaml
new file mode 100644
index 0000000..7468ecd
--- /dev/null
+++ b/deploy/helm/data-avalanche/templates/service.yaml
@@ -0,0 +1,40 @@
+apiVersion: v1
+kind: Service
+metadata:
+  name: {{ include "data-avalanche.fullname" . }}
+  labels:
+    {{- include "data-avalanche.labels" . | nindent 4 }}
+  {{- with .Values.service.annotations }}
+  annotations:
+    {{- toYaml . | nindent 4 }}
+  {{- end }}
+spec:
+  type: {{ .Values.service.type }}
+  selector:
+    {{- include "data-avalanche.selectorLabels" . | nindent 4 }}
+  ports:
+    - name: api
+      port: {{ include "data-avalanche.apiPort" . }}
+      targetPort: api
+      protocol: TCP
+      {{- if and (eq .Values.service.type "NodePort") .Values.service.nodePort }}
+      nodePort: {{ .Values.service.nodePort }}
+      {{- end }}
+---
+# The gossip's peers: every pod's address, ready or not, so a starting pod finds the others.
+apiVersion: v1
+kind: Service
+metadata:
+  name: {{ include "data-avalanche.headless" . }}
+  labels:
+    {{- include "data-avalanche.labels" . | nindent 4 }}
+spec:
+  clusterIP: None
+  publishNotReadyAddresses: true
+  selector:
+    {{- include "data-avalanche.selectorLabels" . | nindent 4 }}
+  ports:
+    - name: gossip
+      port: {{ include "data-avalanche.gossipPort" . }}
+      targetPort: gossip
+      protocol: UDP
```

`deploy/helm/data-avalanche/templates/serviceaccount.yaml`:

```diff
diff --git a/deploy/helm/data-avalanche/templates/serviceaccount.yaml b/deploy/helm/data-avalanche/templates/serviceaccount.yaml
new file mode 100644
index 0000000..b251d82
--- /dev/null
+++ b/deploy/helm/data-avalanche/templates/serviceaccount.yaml
@@ -0,0 +1,13 @@
+{{- if .Values.serviceAccount.create }}
+apiVersion: v1
+kind: ServiceAccount
+metadata:
+  name: {{ include "data-avalanche.serviceAccountName" . }}
+  labels:
+    {{- include "data-avalanche.labels" . | nindent 4 }}
+  {{- with .Values.serviceAccount.annotations }}
+  annotations:
+    {{- toYaml . | nindent 4 }}
+  {{- end }}
+automountServiceAccountToken: false
+{{- end }}
```

`deploy/helm/data-avalanche/templates/statefulset.yaml`:

```diff
diff --git a/deploy/helm/data-avalanche/templates/statefulset.yaml b/deploy/helm/data-avalanche/templates/statefulset.yaml
new file mode 100644
index 0000000..f6012ef
--- /dev/null
+++ b/deploy/helm/data-avalanche/templates/statefulset.yaml
@@ -0,0 +1,122 @@
+apiVersion: apps/v1
+kind: StatefulSet
+metadata:
+  name: {{ include "data-avalanche.fullname" . }}
+  labels:
+    {{- include "data-avalanche.labels" . | nindent 4 }}
+spec:
+  replicas: {{ .Values.replicaCount }}
+  serviceName: {{ include "data-avalanche.headless" . }}
+  # Every pod starts at once: a pod's name, not its start order, places it on the ring (D-1).
+  podManagementPolicy: Parallel
+  selector:
+    matchLabels:
+      {{- include "data-avalanche.selectorLabels" . | nindent 6 }}
+  template:
+    metadata:
+      labels:
+        {{- include "data-avalanche.labels" . | nindent 8 }}
+        {{- with .Values.podLabels }}
+        {{- toYaml . | nindent 8 }}
+        {{- end }}
+      {{- with .Values.podAnnotations }}
+      annotations:
+        {{- toYaml . | nindent 8 }}
+      {{- end }}
+    spec:
+      serviceAccountName: {{ include "data-avalanche.serviceAccountName" . }}
+      {{- with .Values.imagePullSecrets }}
+      imagePullSecrets:
+        {{- toYaml . | nindent 8 }}
+      {{- end }}
+      {{- with .Values.hostAliases }}
+      hostAliases:
+        {{- toYaml . | nindent 8 }}
+      {{- end }}
+      terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
+      securityContext:
+        {{- toYaml .Values.podSecurityContext | nindent 8 }}
+      containers:
+        - name: data-avalanche
+          image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
+          imagePullPolicy: {{ .Values.image.pullPolicy }}
+          args: ["run", "--config", "/etc/data-avalanche/{{ .Values.configKey }}"]
+          securityContext:
+            {{- toYaml .Values.securityContext | nindent 12 }}
+          env:
+            - name: POD_NAME
+              valueFrom:
+                fieldRef:
+                  fieldPath: metadata.name
+            - name: POD_NAMESPACE
+              valueFrom:
+                fieldRef:
+                  fieldPath: metadata.namespace
+            - name: POD_IP
+              valueFrom:
+                fieldRef:
+                  fieldPath: status.podIP
+            - name: DATA_AVALANCHE_DISCOVERY_DNS_NAME
+              value: "{{ include "data-avalanche.headless" . }}.$(POD_NAMESPACE).svc.cluster.local"
+            {{- with .Values.env }}
+            {{- toYaml . | nindent 12 }}
+            {{- end }}
+          {{- with .Values.envFrom }}
+          envFrom:
+            {{- toYaml . | nindent 12 }}
+          {{- end }}
+          ports:
+            - name: api
+              containerPort: {{ include "data-avalanche.apiPort" . }}
+              protocol: TCP
+            - name: gossip
+              containerPort: {{ include "data-avalanche.gossipPort" . }}
+              protocol: UDP
+          {{- with .Values.livenessProbe }}
+          livenessProbe:
+            {{- include "data-avalanche.probe" (dict "probe" . "root" $) | nindent 12 }}
+          {{- end }}
+          {{- with .Values.readinessProbe }}
+          readinessProbe:
+            {{- include "data-avalanche.probe" (dict "probe" . "root" $) | nindent 12 }}
+          {{- end }}
+          {{- with .Values.startupProbe }}
+          startupProbe:
+            {{- include "data-avalanche.probe" (dict "probe" . "root" $) | nindent 12 }}
+          {{- end }}
+          resources:
+            {{- toYaml .Values.resources | nindent 12 }}
+          volumeMounts:
+            - name: config
+              mountPath: /etc/data-avalanche
+              readOnly: true
+            - name: tmp
+              mountPath: /tmp
+            {{- with .Values.extraVolumeMounts }}
+            {{- toYaml . | nindent 12 }}
+            {{- end }}
+      volumes:
+        - name: config
+          configMap:
+            name: {{ include "data-avalanche.configMap" . }}
+        - name: tmp
+          emptyDir: {}
+        {{- with .Values.extraVolumes }}
+        {{- toYaml . | nindent 8 }}
+        {{- end }}
+      {{- with .Values.nodeSelector }}
+      nodeSelector:
+        {{- toYaml . | nindent 8 }}
+      {{- end }}
+      {{- with .Values.affinity }}
+      affinity:
+        {{- toYaml . | nindent 8 }}
+      {{- end }}
+      {{- with .Values.tolerations }}
+      tolerations:
+        {{- toYaml . | nindent 8 }}
+      {{- end }}
+      {{- with .Values.topologySpreadConstraints }}
+      topologySpreadConstraints:
+        {{- toYaml . | nindent 8 }}
+      {{- end }}
```

`deploy/helm/data-avalanche/values.yaml`:

```diff
diff --git a/deploy/helm/data-avalanche/values.yaml b/deploy/helm/data-avalanche/values.yaml
new file mode 100644
index 0000000..b8e41cb
--- /dev/null
+++ b/deploy/helm/data-avalanche/values.yaml
@@ -0,0 +1,116 @@
+# The data-avalanche config (config_version 1), rendered into the ConfigMap as config.yaml: a
+# map, or the file's text (helm install --set-file config=config.yaml). Required unless
+# existingConfigMap is set. Per-pod values come from the environment the chart
+# sets on every pod:
+#   POD_NAME, POD_IP, POD_NAMESPACE           from the Downward API
+#   DATA_AVALANCHE_DISCOVERY_DNS_NAME          the headless Service's DNS name
+# so the cluster block reads:
+#   globals:
+#     cluster:
+#       discovery: dns
+#       discovery_dns_name: ${DATA_AVALANCHE_DISCOVERY_DNS_NAME}
+#       memberlist:
+#         node_name: ${POD_NAME}
+#         advertise_addr: ${POD_IP}
+# A changed config reaches a running cluster through the API's reload, not a pod restart: a
+# starting node adopts the cluster's latest config generation, not its file.
+config: {}
+
+# A ConfigMap of your own that holds the config under configKey; config is then ignored.
+existingConfigMap: ""
+configKey: config.yaml
+
+# The ports the pods listen on, when the config isn't a map that names them (text, or
+# existingConfigMap): the API (globals.api.address, also /metrics and the probes) and the gossip
+# (memberlist.bind_port, UDP). apiTls: the probes use HTTPS (globals.api.tls.enabled).
+ports:
+  api: 8080
+  gossip: 7946
+apiTls: false
+
+replicaCount: 3
+
+image:
+  repository: ghcr.io/alex-karpenko/data-avalanche
+  # Defaults to the chart's appVersion.
+  tag: ""
+  pullPolicy: IfNotPresent
+imagePullSecrets: []
+
+# Extra environment: the secrets the config's ${VAR}s name.
+env: []
+envFrom: []
+
+serviceAccount:
+  create: true
+  name: ""
+  # For a cloud identity: eks.amazonaws.com/role-arn, iam.gke.io/gcp-service-account, …
+  annotations: {}
+
+service:
+  type: ClusterIP
+  # With type NodePort: the node's port, or empty for one Kubernetes picks.
+  nodePort: null
+  annotations: {}
+
+podAnnotations: {}
+podLabels: {}
+
+podSecurityContext:
+  runAsNonRoot: true
+  runAsUser: 65532
+  runAsGroup: 65532
+  fsGroup: 65532
+  seccompProfile:
+    type: RuntimeDefault
+
+securityContext:
+  allowPrivilegeEscalation: false
+  readOnlyRootFilesystem: true
+  capabilities:
+    drop: [ALL]
+
+resources:
+  requests:
+    cpu: 500m
+    memory: 1Gi
+  limits:
+    memory: 2Gi
+
+# Above globals.cluster.drain_timeout (60s by default): a deleted pod hands its pipelines off
+# before it is killed.
+terminationGracePeriodSeconds: 90
+
+livenessProbe:
+  httpGet:
+    path: /health/live
+    port: api
+  periodSeconds: 10
+  failureThreshold: 3
+readinessProbe:
+  httpGet:
+    path: /health/ready
+    port: api
+  periodSeconds: 5
+  failureThreshold: 2
+startupProbe:
+  httpGet:
+    path: /health/live
+    port: api
+  periodSeconds: 5
+  failureThreshold: 60
+
+podDisruptionBudget:
+  enabled: true
+  maxUnavailable: 1
+
+# Names the pods resolve to the addresses given, beside the cluster's DNS (e.g. a database
+# outside Kubernetes without a DNS name).
+hostAliases: []
+
+nodeSelector: {}
+tolerations: []
+affinity: {}
+topologySpreadConstraints: []
+extraVolumes: []
+extraVolumeMounts: []
```


- [ ] **Step 4: Run the tests and the chart's checks**

Run: `cargo test -p data-avalanche --test packaging`
Expected: PASS (6 tests).

Run, from the repository's root:
```sh
chart=deploy/helm/data-avalanche
for values in "$chart"/ci/*-values.yaml; do
  helm lint --strict "$chart" -f "$values"
  helm template ci "$chart" -f "$values" | docker run --rm -i ghcr.io/yannh/kubeconform:v0.8.0 -strict -summary -kubernetes-version 1.37.0 -
done
helm template ci "$chart" -f "$chart/ci/tls-values.yaml" | grep -c 'scheme: HTTPS'
helm template t "$chart"
docker run --rm -v "$PWD:/repo:ro" --workdir /repo rhysd/actionlint:1.7.12 -no-color
```
Expected: `1 chart(s) linted, 0 chart(s) failed` and `Valid: 6, Invalid: 0, Errors: 0` for each
example; `3`; `Error: execution error at (data-avalanche/templates/configmap.yaml…): config is
required …`; actionlint prints nothing and exits 0.

- [ ] **Step 5: Commit**

```bash
git add deploy/helm .github/workflows/ci.yml crates/data-avalanche/tests/packaging.rs
git commit -m "feat(deploy): the Helm chart — a StatefulSet named for the ring, DNS discovery through a headless Service, the config as a map or text, probes over the API's TLS; linted, rendered and schema-checked in CI"
```

### Task 3: The smoke deploy on k3s (`test-infra`, the E2E suite)

**Files:**
- Create: `test-infra/k3s.sh`, `test-infra/k3s/values.yaml`,
  `crates/data-avalanche/tests/e2e/k3s.rs`
- Modify: `test-infra/compose.yaml`, `test-infra/down.sh`, `test-infra/README.md`, `.gitignore`,
  `.github/workflows/ci.yml`, `crates/data-avalanche-testkit/src/lib.rs`,
  `crates/data-avalanche/tests/e2e/{harness,main}.rs`

**Interfaces:**
- Consumes: Task 1's image, Task 2's chart and values; the harness's `E2e::new`, `config`,
  `converged`, `lease`, `exec`, `ids`, `cleanup`, `eventually`, `postgres_only!` (M6b–M10).
- Produces:
  - `data_avalanche_testkit::{k3s_enabled, require_k3s!}` (`DATA_AVALANCHE_IT` and
    `DATA_AVALANCHE_K3S`);
  - `harness::get_json(port, path) -> Option<(u16, serde_json::Value)>`;
  - the compose service `k3s` (profile `k3s`; API 56443, NodePort 30080 on 58080; kubeconfig
    `test-infra/.k3s/kubeconfig.yaml`), `test-infra/k3s.sh [--no-build]` (the image
    `data-avalanche:k3s` imported);
  - CI's `k3s` job.

- [ ] **Step 1: Write the failing scenario**

`crates/data-avalanche-testkit/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/lib.rs b/crates/data-avalanche-testkit/src/lib.rs
index f235a44..25c636e 100644
--- a/crates/data-avalanche-testkit/src/lib.rs
+++ b/crates/data-avalanche-testkit/src/lib.rs
@@ -32,6 +32,12 @@ pub fn gcs_enabled() -> bool {
     it_enabled() && !env_set("DATA_AVALANCHE_SKIP_GCS")
 }
 
+/// True when integration tests are enabled and `DATA_AVALANCHE_K3S` is set: the smoke deploy's
+/// k3s runs (`test-infra/k3s.sh`).
+pub fn k3s_enabled() -> bool {
+    it_enabled() && env_set("DATA_AVALANCHE_K3S")
+}
+
 /// True when `DATA_AVALANCHE_CLOUD` (a comma-separated list, e.g. `s3` or `s3,azure`) names
 /// `cloud`. Real-cloud tests run only in `cloud.yml` or by hand, never in `ci.yml`.
 pub fn cloud_enabled(cloud: &str) -> bool {
@@ -68,6 +74,19 @@ macro_rules! require_it {
     };
 }
 
+/// Returns early unless the smoke deploy's tests are enabled.
+#[macro_export]
+macro_rules! require_k3s {
+    () => {
+        if !$crate::k3s_enabled() {
+            eprintln!(
+                "skipped: set DATA_AVALANCHE_IT=1 and DATA_AVALANCHE_K3S=1 after running test-infra/k3s.sh"
+            );
+            return;
+        }
+    };
+}
+
 /// Returns early unless GCS tests are enabled. When they are enabled, `stores::gcs()` fails loudly on
 /// missing configuration instead of skipping, so a misconfigured CI can't pass silently.
 #[macro_export]
```

`crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index 9db0b76..b19a742 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -903,6 +903,21 @@ pub struct Proc {
     token: Option<String>,
 }
 
+/// `GET path` on the API at `127.0.0.1:port`, plain HTTP: the status code and the JSON body,
+/// `None` when nothing answers or the body isn't JSON.
+pub async fn get_json(port: u16, path: &str) -> Option<(u16, serde_json::Value)> {
+    let request = format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
+    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
+        .await
+        .ok()?;
+    let out = exchange(tcp, request.as_bytes()).await?;
+    let (head, body) = out.split_once("\r\n\r\n")?;
+    Some((
+        head.get(9..12)?.parse().ok()?,
+        serde_json::from_str(body).ok()?,
+    ))
+}
+
 /// One HTTP/1.1 exchange on `s`: the answer's text.
 async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, request: &[u8]) -> Option<String> {
     s.write_all(request).await.ok()?;
```

`crates/data-avalanche/tests/e2e/k3s.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/k3s.rs b/crates/data-avalanche/tests/e2e/k3s.rs
new file mode 100644
index 0000000..4ef5e4a
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/k3s.rs
@@ -0,0 +1,246 @@
+//! The Helm chart on Kubernetes (roadmap §4 M11a; the 3-node run moved from M8, Q-M8-4): three
+//! pods of the chart's StatefulSet find each other through the headless Service, run one
+//! pipeline end to end, and a deleted pod hands it to the others. The cluster is test-infra's
+//! `k3s` service, beside PostgreSQL 18 and RustFS on the compose network; the pods reach those by
+//! the chart's `hostAliases`, since k3s's DNS doesn't forward to Docker's. Needs
+//! `DATA_AVALANCHE_K3S=1` and `test-infra/k3s.sh` (k3s, the image imported).
+
+use std::process::Command;
+use std::time::Duration;
+
+use data_avalanche_testkit::{require_it, require_k3s};
+
+use crate::harness::{E2e, Options, eventually, get_json};
+
+const RELEASE: &str = "smoke";
+/// The chart's API Service, as the node's port 30080 maps it on the host (compose.yaml).
+const API: u16 = 58080;
+
+fn repo(path: &str) -> String {
+    format!("{}/../../{path}", env!("CARGO_MANIFEST_DIR"))
+}
+
+/// Runs `program` against k3s with `args`; its stdout, or a panic with its stderr.
+fn run(program: &str, args: &[&str]) -> String {
+    let out = Command::new(program)
+        .args(args)
+        .env("KUBECONFIG", repo("test-infra/.k3s/kubeconfig.yaml"))
+        .output()
+        .unwrap_or_else(|e| panic!("{program}: {e}"));
+    assert!(
+        out.status.success(),
+        "{program} {}: {}",
+        args.join(" "),
+        String::from_utf8_lossy(&out.stderr)
+    );
+    String::from_utf8_lossy(&out.stdout).into_owned()
+}
+
+/// A compose service's address on the compose network.
+fn address(service: &str) -> String {
+    let ip = run(
+        "docker",
+        &[
+            "inspect",
+            "-f",
+            "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
+            &format!("data-avalanche-test-{service}-1"),
+        ],
+    );
+    ip.split_whitespace()
+        .next()
+        .unwrap_or_else(|| panic!("{service} has no address"))
+        .to_owned()
+}
+
+/// A pod's UID, `None` while it doesn't exist.
+fn uid(pod: &str) -> Option<String> {
+    let out = Command::new("kubectl")
+        .args(["get", "pod", pod, "-o", "jsonpath={.metadata.uid}"])
+        .env("KUBECONFIG", repo("test-infra/.k3s/kubeconfig.yaml"))
+        .output()
+        .ok()?;
+    let uid = String::from_utf8_lossy(&out.stdout).trim().to_owned();
+    (out.status.success() && !uid.is_empty()).then_some(uid)
+}
+
+/// Uninstalls the release when the scenario ends, failed or not.
+struct Release;
+
+impl Drop for Release {
+    fn drop(&mut self) {
+        let _ = Command::new("helm")
+            .args(["uninstall", RELEASE, "--wait"])
+            .env("KUBECONFIG", repo("test-infra/.k3s/kubeconfig.yaml"))
+            .output();
+    }
+}
+
+/// The scenario's config as the pods run it: the source by its service name, the API on every
+/// address, the peers from the headless Service, each pod named by itself.
+fn in_cluster(config: &str) -> String {
+    let mut out = String::new();
+    for line in config.lines() {
+        let trimmed = line.trim_start();
+        let indent = &line[..line.len() - trimmed.len()];
+        let line = if trimmed.starts_with("api: ") {
+            format!("{indent}api: {{ address: \"0.0.0.0:8080\" }}")
+        } else if trimmed == "cluster:" {
+            format!(
+                "{line}\n{indent}  discovery: dns\n{indent}  discovery_dns_name: ${{DATA_AVALANCHE_DISCOVERY_DNS_NAME}}"
+            )
+        } else if trimmed.starts_with("memberlist: ") {
+            format!(
+                "{indent}memberlist: {{ node_name: \"${{POD_NAME}}\", advertise_addr: \"${{POD_IP}}\" }}"
+            )
+        } else if trimmed == "host: 127.0.0.1" {
+            format!("{indent}host: postgres-18")
+        } else if trimmed == "port: 55418" {
+            format!("{indent}port: 5432")
+        } else {
+            line.to_owned()
+        };
+        out.push_str(&line);
+        out.push('\n');
+    }
+    for moved in [
+        "0.0.0.0:8080",
+        "discovery: dns",
+        "${POD_NAME}",
+        "host: postgres-18",
+        "port: 5432",
+    ] {
+        assert!(out.contains(moved), "{moved} isn't in:\n{out}");
+    }
+    out
+}
+
+/// The values the scenario adds: where postgres-18 and rustfs are, RustFS as the S3 endpoint.
+fn addresses() -> String {
+    format!(
+        "hostAliases:
+  - {{ ip: \"{pg}\", hostnames: [postgres-18] }}
+  - {{ ip: \"{rustfs}\", hostnames: [rustfs] }}
+env:
+  - {{ name: AWS_ACCESS_KEY_ID, value: avalanche }}
+  - {{ name: AWS_SECRET_ACCESS_KEY, value: avalanche-secret }}
+  - {{ name: AWS_REGION, value: us-east-1 }}
+  - {{ name: AWS_ENDPOINT_URL, value: \"http://rustfs:9000\" }}
+  - {{ name: AWS_ALLOW_HTTP, value: \"true\" }}
+  - {{ name: RUST_BACKTRACE, value: \"1\" }}
+",
+        pg = address("postgres-18"),
+        rustfs = address("rustfs"),
+    )
+}
+
+/// The members `GET /v1/cluster/status` names, through the API Service.
+async fn members() -> Vec<String> {
+    let Some((200, v)) = get_json(API, "/v1/cluster/status").await else {
+        return vec![];
+    };
+    v["members"]
+        .as_array()
+        .into_iter()
+        .flatten()
+        .filter_map(|m| m["node"].as_str().map(str::to_owned))
+        .collect()
+}
+
+#[tokio::test]
+async fn three_pods_run_a_pipeline_and_a_deleted_pod_hands_it_off() {
+    require_it!();
+    require_k3s!();
+    crate::postgres_only!();
+    let e = E2e::new(
+        "k3s",
+        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
+         INSERT INTO items VALUES (1, 'a'), (2, 'b');",
+        Options {
+            tables: ", snapshots: { mode: initial }",
+            ..Options::default()
+        },
+    )
+    .await;
+    let dir = tempfile::tempdir().unwrap();
+    let config = dir.path().join("config.yaml");
+    std::fs::write(&config, in_cluster(&e.config())).unwrap();
+    let more = dir.path().join("addresses.yaml");
+    std::fs::write(&more, addresses()).unwrap();
+
+    let release = Release;
+    run(
+        "helm",
+        &[
+            "upgrade",
+            "--install",
+            RELEASE,
+            &repo("deploy/helm/data-avalanche"),
+            "-f",
+            &repo("test-infra/k3s/values.yaml"),
+            "-f",
+            &more.display().to_string(),
+            "--set-file",
+            &format!("config={}", config.display()),
+            "--wait",
+            "--timeout",
+            "5m",
+        ],
+    );
+    let pods = [0, 1, 2].map(|i| format!("{RELEASE}-data-avalanche-{i}"));
+    eventually("three members", Duration::from_secs(120), async || {
+        let mut m = members().await;
+        m.sort();
+        m == pods
+    })
+    .await;
+
+    e.converged("items").await;
+    let lease = e.lease().await.expect("a lease").doc;
+    let holder = lease.holder_node_id;
+    assert!(pods.contains(&holder), "{holder}");
+
+    // A deleted pod leaves: it hands the pipeline off before it stops (the chart's grace is
+    // longer than the drain), and the StatefulSet starts it again under its name.
+    let before = uid(&holder).expect("the holder's pod");
+    run("kubectl", &["delete", "pod", &holder, "--wait=false"]);
+    eventually(
+        "another pod holds it",
+        Duration::from_secs(120),
+        async || {
+            e.lease().await.is_some_and(|l| {
+                !l.doc.released
+                    && l.doc.holder_node_id != holder
+                    && l.doc.owner_epoch > lease.owner_epoch
+            })
+        },
+    )
+    .await;
+    let taker = e.lease().await.unwrap().doc;
+    eprintln!(
+        "{holder} (epoch {}) handed off to {} (epoch {})",
+        lease.owner_epoch, taker.holder_node_id, taker.owner_epoch
+    );
+    e.exec("INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'A' WHERE id = 1")
+        .await;
+    e.converged("items").await;
+    assert_eq!(e.ids("items").await, [1, 2, 3]);
+    eventually(
+        "the deleted pod back, a member again",
+        Duration::from_secs(180),
+        async || uid(&holder).is_some_and(|u| u != before) && members().await.contains(&holder),
+    )
+    .await;
+    run(
+        "kubectl",
+        &[
+            "wait",
+            "--for=condition=Ready",
+            "pod",
+            &holder,
+            "--timeout=120s",
+        ],
+    );
+    drop(release);
+    e.cleanup().await;
+}
```

`crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index cecd134..85a1371 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -14,6 +14,7 @@ mod guards;
 mod harness;
 mod idp;
 mod jobs;
+mod k3s;
 mod lifecycle;
 mod mysql;
 mod observability;
```


- [ ] **Step 2: Run the scenario to verify it fails**

Run, with `test-infra/up.sh` running:
`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_K3S=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- k3s:: --test-threads=1`
Expected: FAIL: `helm upgrade --install smoke …` fails with `Error: kubernetes cluster
unreachable` (no k3s: without its kubeconfig helm tries `localhost:8080`). Without
`DATA_AVALANCHE_K3S` the scenario skips.

- [ ] **Step 3: Write the k3s service, its script and the CI job**

`.github/workflows/ci.yml`:

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index f274602..dd34b83 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -172,3 +172,57 @@ jobs:
         if: always()
         run: docker compose -f test-infra/compose.yaml down -v
 
+  # The smoke deploy (M11a): the image built from deploy/docker/Dockerfile, imported into
+  # test-infra's k3s, and the Helm chart's three pods running a pipeline from PostgreSQL 18 into
+  # RustFS; a deleted pod hands the pipeline to the others. No secrets.
+  k3s:
+    runs-on: ubuntu-latest
+    timeout-minutes: 60
+    needs: check
+    env:
+      DATA_AVALANCHE_IT: "1"
+      DATA_AVALANCHE_SKIP_GCS: "1"
+      DATA_AVALANCHE_K3S: "1"
+      KUBECONFIG: ${{ github.workspace }}/test-infra/.k3s/kubeconfig.yaml
+    steps:
+      - name: Free runner disk
+        run: sudo rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc /opt/hostedtoolcache/CodeQL
+      - uses: actions/checkout@v7
+      - uses: actions-rust-lang/setup-rust-toolchain@v2
+      - uses: azure/setup-helm@v5
+        with:
+          version: v4.3.0
+      - uses: docker/setup-buildx-action@v4
+      - name: Start PostgreSQL 18 and RustFS
+        run: |
+          docker compose -f test-infra/compose.yaml up -d --wait postgres-18 rustfs
+          docker compose -f test-infra/compose.yaml --profile init run --rm rustfs-init
+      - name: The image
+        uses: docker/build-push-action@v7
+        with:
+          context: .
+          file: deploy/docker/Dockerfile
+          tags: data-avalanche:k3s
+          load: true
+          cache-from: type=gha,scope=k3s
+          cache-to: type=gha,mode=max,scope=k3s
+      # rustls's platform verifier reads the distroless CA bundle: S3, GCS and Azure over TLS.
+      - name: The image's CA bundle
+        run: |
+          id=$(docker create data-avalanche:k3s)
+          docker cp "$id:/etc/ssl/certs/ca-certificates.crt" - | tar -tv
+          docker rm "$id"
+      - name: Start k3s, import the image
+        run: test-infra/k3s.sh --no-build
+      - name: The smoke deploy
+        run: >-
+          cargo test --locked -p data-avalanche --features e2e-hooks --test e2e --
+          k3s:: --test-threads=1
+      - name: Pods and logs on failure
+        if: failure()
+        run: |
+          kubectl get pods -o wide || true
+          kubectl logs -l app.kubernetes.io/name=data-avalanche --prefix --tail=200 || true
+      - name: Stop the containers
+        if: always()
+        run: docker compose -f test-infra/compose.yaml --profile k3s down -v
```

`.gitignore`:

```diff
diff --git a/.gitignore b/.gitignore
index f1a7a38..4beca48 100644
--- a/.gitignore
+++ b/.gitignore
@@ -1,3 +1,4 @@
 target/
 prototypes/*/target/
 test-infra/.tls/
+test-infra/.k3s/
```

`test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
index 992f6ff..06a9e8e 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -19,6 +19,7 @@ on `PATH`; it fetches DuckDB itself.
 | rustfs (S3) | rustfs/rustfs:1.0.0 | 59000 | avalanche / avalanche-secret | default |
 | azurite (Azure Blob) | mcr.microsoft.com/azure-storage/azurite:3.37.0 | 10000 | Azurite's public dev account | default |
 | jaeger (OTLP/HTTP collector, query API) | jaegertracing/jaeger:2.21.0 | 54318 (OTLP), 56686 (query) | — | default |
+| k3s (the smoke deploy's Kubernetes) | rancher/k3s:v1.37.1-k3s1, privileged | 56443 (API), 58080 (the chart's API NodePort) | kubeconfig in `test-infra/.k3s/` | k3s |
 
 Bucket / container `data-avalanche-test` is created by the `init` profile (`up.sh` runs it). Azurite keeps
 port 10000 because `object_store`'s emulator mode always targets `127.0.0.1:10000`.
@@ -109,6 +110,26 @@ through a node that doesn't hold it, and leaves and shuts a node down through th
 snapshot-job scenarios (M9b, `jobs.rs`) need nothing more either: they ask for jobs through the
 API, drop and create a table again to halt it, and `kill -9` the process while a job reads.
 
+## The smoke deploy (k3s)
+
+The Helm chart's scenario (`crates/data-avalanche/tests/e2e/k3s.rs`, M11a) installs the chart
+with three pods on a one-node k3s, runs a pipeline from `postgres-18` into RustFS, and deletes the
+pod that holds it. `test-infra/k3s.sh` starts the `k3s` service (profile `k3s`), waits for its
+node, builds the image from `deploy/docker/Dockerfile` (about 20 minutes the first time) and
+imports it; k3s writes its kubeconfig to `test-infra/.k3s/` (git-ignored). The pods reach
+`postgres-18` and `rustfs` through the chart's `hostAliases`, which the scenario sets from
+`docker inspect`: k3s's DNS doesn't forward to Docker's.
+
+```sh
+test-infra/up.sh && test-infra/k3s.sh
+DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_K3S=1 \
+  cargo test -p data-avalanche --features e2e-hooks --test e2e -- k3s:: --test-threads=1
+```
+
+The kubelet evicts only under 1% free disk (`compose.yaml`): a laptop's Docker disk is often
+fuller than the default thresholds (10–15%) allow, which taints the node and schedules nothing.
+`test-infra/down.sh` stops k3s with the rest.
+
 ## GCS: a real bucket, not an emulator
 
 `fake-gcs-server` 1.56.1 rejects `object_store`'s path-style PUT (`400 invalid uploadType`) and
```

`test-infra/compose.yaml`:

```diff
diff --git a/test-infra/compose.yaml b/test-infra/compose.yaml
index b4e9871..8a97cfa 100644
--- a/test-infra/compose.yaml
+++ b/test-infra/compose.yaml
@@ -88,6 +88,31 @@ services:
     image: "jaegertracing/jaeger:2.21.0"
     ports: ["54318:4318", "56686:16686"]
 
+  # The smoke deploy's Kubernetes (M11a): one k3s server, privileged as k3s in a container needs.
+  # Its API on host port 56443, its kubeconfig written to test-infra/.k3s/; the chart's API
+  # NodePort 30080 on host port 58080. Profile k3s: test-infra/k3s.sh starts it.
+  k3s:
+    image: "rancher/k3s:v1.37.1-k3s1"
+    profiles: [k3s]
+    privileged: true
+    # One node name across recreations: the cluster's state outlives the container.
+    hostname: k3s
+    # A test cluster schedules while the host's disk is nearly full (a laptop's Docker VM, a CI
+    # runner): the kubelet evicts only under 1% free.
+    command:
+      - server
+      - --https-listen-port=56443
+      - --disable=traefik,metrics-server,servicelb
+      - --tls-san=127.0.0.1
+      - --node-name=k3s
+      - --kubelet-arg=eviction-hard=nodefs.available<1%,imagefs.available<1%
+      - --kubelet-arg=image-gc-high-threshold=99
+      - --kubelet-arg=image-gc-low-threshold=98
+    environment: { K3S_KUBECONFIG_OUTPUT: /output/kubeconfig.yaml, K3S_KUBECONFIG_MODE: "644" }
+    tmpfs: [/run, /var/run]
+    volumes: ["./.k3s:/output"]
+    ports: ["56443:56443", "58080:30080"]
+
   rustfs-init:
     image: "amazon/aws-cli:2.37.4"
     profiles: [init]
```

`test-infra/down.sh`:

```diff
diff --git a/test-infra/down.sh b/test-infra/down.sh
index c8e8d2d..018f784 100755
--- a/test-infra/down.sh
+++ b/test-infra/down.sh
@@ -2,4 +2,4 @@
 # Stop everything and drop volumes (all profiles).
 set -euo pipefail
 cd "$(dirname "$0")"
-docker compose --profile matrix --profile init down -v --remove-orphans
+docker compose --profile matrix --profile init --profile k3s down -v --remove-orphans
```

`test-infra/k3s.sh`:

```diff
diff --git a/test-infra/k3s.sh b/test-infra/k3s.sh
new file mode 100755
index 0000000..524ae98
--- /dev/null
+++ b/test-infra/k3s.sh
@@ -0,0 +1,17 @@
+#!/usr/bin/env bash
+# Starts the smoke deploy's k3s (compose profile k3s) beside test-infra's containers, waits for
+# its node, builds the image (unless --no-build: CI builds it with its cache) and imports it into
+# k3s's containerd. test-infra/up.sh runs first. The kubeconfig: test-infra/.k3s/kubeconfig.yaml.
+set -euo pipefail
+cd "$(dirname "$0")"
+docker compose --profile k3s up -d k3s
+export KUBECONFIG="$PWD/.k3s/kubeconfig.yaml"
+for _ in $(seq 1 90); do
+  [ -f "$KUBECONFIG" ] && kubectl wait --for=condition=Ready node/k3s --timeout=2s >/dev/null 2>&1 && break
+  sleep 2
+done
+kubectl wait --for=condition=Ready node/k3s --timeout=10s
+if [ "${1:-}" != --no-build ]; then
+  docker build -f ../deploy/docker/Dockerfile -t data-avalanche:k3s ..
+fi
+docker save data-avalanche:k3s | docker compose --profile k3s exec -T k3s ctr images import -
```

`test-infra/k3s/values.yaml`:

```diff
diff --git a/test-infra/k3s/values.yaml b/test-infra/k3s/values.yaml
new file mode 100644
index 0000000..8d8d955
--- /dev/null
+++ b/test-infra/k3s/values.yaml
@@ -0,0 +1,14 @@
+# The chart's values for the smoke deploy: the image test-infra/k3s.sh imported, the API on the
+# node's port 30080, small requests. The scenario adds the RustFS endpoint and the config.
+image:
+  repository: data-avalanche
+  tag: k3s
+  pullPolicy: Never
+service:
+  type: NodePort
+  nodePort: 30080
+resources:
+  requests: { cpu: 100m, memory: 256Mi }
+  limits: { memory: 1Gi }
+podDisruptionBudget:
+  enabled: false
```


- [ ] **Step 4: Start k3s and run the scenario**

Run: `test-infra/k3s.sh`, then the command of Step 2.
Expected: the node `k3s` Ready, the image imported; PASS, the scenario printing
`smoke-data-avalanche-<i> (epoch 1) handed off to smoke-data-avalanche-<j> (epoch 2)`.

- [ ] **Step 5: Commit**

```bash
git add test-infra .gitignore .github/workflows/ci.yml crates/data-avalanche-testkit/src/lib.rs crates/data-avalanche/tests/e2e
git commit -m "test(e2e): the chart's three pods on k3s run a pipeline, and a deleted pod hands it to the others (the 3-node run moved from M8)"
```

### Task 4: The release (`.github/workflows`)

**Files:**
- Create: `.github/workflows/release.yml`
- Modify: `.github/workflows/weekly.yml`
- Test: `crates/data-avalanche/tests/packaging.rs`

**Interfaces:**
- Consumes: Task 1's Dockerfile (its `VERSION` argument), Task 2's chart and its versions, Task 2's
  actionlint step.
- Produces: `release.yml` (on `v*` tags and by hand); the weekly `image-arm64` job.

- [ ] **Step 1: Write the failing test**

`crates/data-avalanche/tests/packaging.rs`:

```diff
diff --git a/crates/data-avalanche/tests/packaging.rs b/crates/data-avalanche/tests/packaging.rs
index 85c9412..6e4c05e 100644
--- a/crates/data-avalanche/tests/packaging.rs
+++ b/crates/data-avalanche/tests/packaging.rs
@@ -131,3 +131,28 @@ fn a_changed_config_is_said_to_go_through_the_reload() {
     }
     assert!(!chart("templates/statefulset.yaml").contains("checksum/"));
 }
+
+#[test]
+fn the_release_publishes_the_image_the_chart_pulls() {
+    let release = repo(".github/workflows/release.yml");
+    let pulled = value(&chart("values.yaml"), "  repository: ");
+    let owner = pulled
+        .strip_prefix("ghcr.io/")
+        .and_then(|r| r.strip_suffix("/data-avalanche"))
+        .unwrap_or_else(|| panic!("{pulled}"));
+    assert_eq!(owner, "alex-karpenko");
+    assert!(release.contains("IMAGE: ghcr.io/${{ github.repository_owner }}/data-avalanche\n"));
+    assert!(release.contains("CHARTS: oci://ghcr.io/${{ github.repository_owner }}/charts\n"));
+    // Each platform on a runner of its own architecture, as the weekly arm64 build.
+    for (platform, runner) in [
+        ("linux/amd64", "ubuntu-24.04"),
+        ("linux/arm64", "ubuntu-24.04-arm"),
+    ] {
+        assert!(
+            release.contains(&format!(
+                "- platform: {platform}\n            runner: {runner}\n"
+            )),
+            "{platform}"
+        );
+    }
+}
```


- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p data-avalanche --test packaging`
Expected: FAIL: `the_release_publishes_the_image_the_chart_pulls` panics with
`…/.github/workflows/release.yml: No such file or directory`.

- [ ] **Step 3: Write the release workflow and the weekly arm64 build**

`.github/workflows/release.yml`:

```diff
diff --git a/.github/workflows/release.yml b/.github/workflows/release.yml
new file mode 100644
index 0000000..98d2b26
--- /dev/null
+++ b/.github/workflows/release.yml
@@ -0,0 +1,183 @@
+name: release
+
+# A v* tag publishes the release (Q-M11-1): the image for linux/amd64 and linux/arm64, each built
+# on a runner of its own architecture, to ghcr.io/<owner>/data-avalanche with an SBOM and a
+# provenance attestation; the Helm chart as an OCI artifact to ghcr.io/<owner>/charts; and a
+# GitHub release with the SBOM. The tag must equal the workspace's version and the chart's.
+# A manual run builds both images and packages the chart without pushing anything.
+on:
+  push:
+    tags: ["v*"]
+  workflow_dispatch:
+
+permissions:
+  contents: read
+
+env:
+  IMAGE: ghcr.io/${{ github.repository_owner }}/data-avalanche
+  CHARTS: oci://ghcr.io/${{ github.repository_owner }}/charts
+  PUBLISH: ${{ github.event_name == 'push' }}
+
+jobs:
+  version:
+    runs-on: ubuntu-latest
+    outputs:
+      version: ${{ steps.version.outputs.version }}
+    steps:
+      - uses: actions/checkout@v7
+      - id: version
+        name: The tag, the workspace and the chart agree
+        run: |
+          crate=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml)
+          chart=$(sed -n 's/^version: //p' deploy/helm/data-avalanche/Chart.yaml)
+          app=$(sed -n 's/^appVersion: "\(.*\)"/\1/p' deploy/helm/data-avalanche/Chart.yaml)
+          echo "workspace $crate, chart $chart, appVersion $app"
+          [ "$crate" = "$chart" ] && [ "$crate" = "$app" ] || { echo "::error::the workspace, chart and appVersion versions differ"; exit 1; }
+          if [ "$PUBLISH" = true ] && [ "${GITHUB_REF_NAME}" != "v$crate" ]; then
+            echo "::error::tag ${GITHUB_REF_NAME} isn't v$crate"; exit 1
+          fi
+          echo "version=$crate" >> "$GITHUB_OUTPUT"
+
+  image:
+    needs: version
+    strategy:
+      matrix:
+        include:
+          - platform: linux/amd64
+            runner: ubuntu-24.04
+          - platform: linux/arm64
+            runner: ubuntu-24.04-arm
+    runs-on: ${{ matrix.runner }}
+    timeout-minutes: 90
+    permissions:
+      contents: read
+      packages: write
+    steps:
+      - uses: actions/checkout@v7
+      - uses: docker/setup-buildx-action@v4
+      - if: env.PUBLISH == 'true'
+        uses: docker/login-action@v4
+        with:
+          registry: ghcr.io
+          username: ${{ github.actor }}
+          password: ${{ secrets.GITHUB_TOKEN }}
+      - id: build
+        uses: docker/build-push-action@v7
+        with:
+          context: .
+          file: deploy/docker/Dockerfile
+          platforms: ${{ matrix.platform }}
+          build-args: VERSION=${{ needs.version.outputs.version }}
+          outputs: type=image,name=${{ env.IMAGE }},push-by-digest=true,name-canonical=true,push=${{ env.PUBLISH }}
+          sbom: true
+          provenance: mode=max
+          cache-from: type=gha,scope=${{ matrix.platform }}
+          cache-to: type=gha,mode=max,scope=${{ matrix.platform }}
+      - if: env.PUBLISH == 'true'
+        name: The digest
+        run: |
+          mkdir -p digests
+          touch "digests/${DIGEST#sha256:}"
+        env:
+          DIGEST: ${{ steps.build.outputs.digest }}
+      - if: env.PUBLISH == 'true'
+        uses: actions/upload-artifact@v7
+        with:
+          name: digest-${{ strategy.job-index }}
+          path: digests/*
+          if-no-files-found: error
+
+  manifest:
+    needs: [version, image]
+    if: github.event_name == 'push'
+    runs-on: ubuntu-latest
+    permissions:
+      contents: read
+      packages: write
+      id-token: write
+      attestations: write
+    outputs:
+      digest: ${{ steps.push.outputs.digest }}
+    steps:
+      - uses: actions/download-artifact@v8
+        with:
+          pattern: digest-*
+          path: digests
+          merge-multiple: true
+      - uses: docker/setup-buildx-action@v4
+      - uses: docker/login-action@v4
+        with:
+          registry: ghcr.io
+          username: ${{ github.actor }}
+          password: ${{ secrets.GITHUB_TOKEN }}
+      - id: push
+        name: The image's tags, over both platforms
+        working-directory: digests
+        env:
+          VERSION: ${{ needs.version.outputs.version }}
+        run: |
+          tags=(-t "$IMAGE:$VERSION")
+          case "$VERSION" in
+            *-*) ;;  # a pre-release takes its own tag only
+            *) tags+=(-t "$IMAGE:${VERSION%.*}" -t "$IMAGE:latest") ;;
+          esac
+          refs=()
+          for d in *; do refs+=("$IMAGE@sha256:$d"); done
+          docker buildx imagetools create "${tags[@]}" "${refs[@]}"
+          digest=$(docker buildx imagetools inspect "$IMAGE:$VERSION" --format '{{json .Manifest}}' | jq -r .digest)
+          echo "digest=$digest" >> "$GITHUB_OUTPUT"
+      - uses: actions/attest-build-provenance@v4
+        with:
+          subject-name: ${{ env.IMAGE }}
+          subject-digest: ${{ steps.push.outputs.digest }}
+          push-to-registry: true
+
+  chart:
+    needs: version
+    runs-on: ubuntu-latest
+    permissions:
+      contents: read
+      packages: write
+    steps:
+      - uses: actions/checkout@v7
+      - uses: azure/setup-helm@v5
+        with:
+          version: v4.3.0
+      - name: Package
+        run: helm package deploy/helm/data-avalanche --destination dist
+      - if: env.PUBLISH == 'true'
+        name: Push
+        run: |
+          echo "${{ secrets.GITHUB_TOKEN }}" | helm registry login ghcr.io -u "${{ github.actor }}" --password-stdin
+          helm push "dist/data-avalanche-${{ needs.version.outputs.version }}.tgz" "$CHARTS"
+      - uses: actions/upload-artifact@v7
+        with:
+          name: chart
+          path: dist/*.tgz
+          if-no-files-found: error
+
+  github-release:
+    needs: [version, manifest, chart]
+    runs-on: ubuntu-latest
+    permissions:
+      contents: write
+    steps:
+      - uses: actions/download-artifact@v8
+        with:
+          name: chart
+          path: dist
+      - uses: anchore/sbom-action@v0.24.3
+        with:
+          image: ${{ env.IMAGE }}@${{ needs.manifest.outputs.digest }}
+          format: spdx-json
+          output-file: dist/data-avalanche-${{ needs.version.outputs.version }}.spdx.json
+          upload-artifact: false
+      - name: The release
+        env:
+          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
+          VERSION: ${{ needs.version.outputs.version }}
+        run: |
+          prerelease=()
+          case "$VERSION" in *-*) prerelease=(--prerelease) ;; esac
+          gh release create "$GITHUB_REF_NAME" --repo "$GITHUB_REPOSITORY" --generate-notes \
+            --verify-tag "${prerelease[@]}" dist/*
```

`.github/workflows/weekly.yml`:

```diff
diff --git a/.github/workflows/weekly.yml b/.github/workflows/weekly.yml
index 0fa7eab..7b087a9 100644
--- a/.github/workflows/weekly.yml
+++ b/.github/workflows/weekly.yml
@@ -8,7 +8,8 @@ name: weekly
 # DATA_AVALANCHE_MYSQL. The slice-1 E2E suite on MariaDB 11.8 in file/position mode and on 11.4
 # and 10.11 (Q-M7b-2; ci.yml runs it on MySQL 8.4 and MariaDB 11.8 per change).
 # Weekly (Mondays 03:41 UTC) and on manual dispatch. The TLS tests need `postgres-tls`
-# (PostgreSQL 18) or `mysql-tls` (MySQL 8.4) and run in ci.yml only. No secrets.
+# (PostgreSQL 18) or `mysql-tls` (MySQL 8.4) and run in ci.yml only. The image is built on linux/arm64
+# (M11a). No secrets.
 
 on:
   schedule:
@@ -111,3 +112,24 @@ jobs:
       - name: Stop the containers
         if: always()
         run: docker compose -f test-infra/compose.yaml --profile matrix down -v
+
+  # The image on linux/arm64 (Q-M11-3), built on an arm64 runner as the release builds it, so a
+  # break shows before a tag; PR CI's k3s job builds linux/amd64. Nothing is pushed.
+  image-arm64:
+    runs-on: ubuntu-24.04-arm
+    timeout-minutes: 90
+    steps:
+      - uses: actions/checkout@v7
+      - uses: docker/setup-buildx-action@v4
+      - uses: docker/build-push-action@v7
+        with:
+          context: .
+          file: deploy/docker/Dockerfile
+          platforms: linux/arm64
+          tags: data-avalanche:arm64
+          load: true
+          cache-from: type=gha,scope=linux/arm64
+          cache-to: type=gha,mode=max,scope=linux/arm64
+      - name: The binary runs
+        run: docker run --rm data-avalanche:arm64 --version
+
```


- [ ] **Step 4: Run the tests and the workflow check**

Run: `cargo test -p data-avalanche --test packaging`, then
`docker run --rm -v "$PWD:/repo:ro" --workdir /repo rhysd/actionlint:1.7.12 -no-color`.
Expected: PASS (7 tests); actionlint prints nothing and exits 0.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release.yml .github/workflows/weekly.yml crates/data-avalanche/tests/packaging.rs
git commit -m "ci: the release — on a v* tag, the image for amd64 and arm64 with its SBOM and provenance and the chart to GHCR, a GitHub release; the arm64 image built weekly"
```

## Exit

- [ ] `helm lint` and `helm template` pass on each example, and kubeconform finds the objects
      valid (CI's `check` job).
- [ ] The k3s smoke deploy: three pods run one pipeline end to end, and a deleted pod hands it
      to the others (CI's `k3s` job).
- [ ] The image builds on linux/amd64 (the `k3s` job) and linux/arm64 (`weekly.yml`'s
      `image-arm64`, dispatched by hand on the PR's branch); `release.yml` passes actionlint.
- [ ] The whole-branch review's findings fixed or recorded; the PR's CI green.
- [ ] After the merge: a manual run of `release.yml` on `main` builds both images and packages the
      chart (DA-OPS-11).

## Deviations during execution

- Task 2: two comments of Step 3's blocks name the tests' file `chart.rs`, its name early in
  planning (`.github/workflows/ci.yml`, the chart step's comment; `ci/example-values.yaml`, its
  header): the commit names `packaging.rs`. The block of `ci.yml` also ends the file with a blank
  line, which the commit leaves out.
- Task 3: Step 3's block of `ci.yml` takes that blank line as context, so it doesn't apply after
  Task 2's commit; the commit adds the same 55 lines, the blank line before the `k3s` job among
  them.
- Task 4 (the task review's Important finding): the `github-release` job's SBOM step pulls the
  joined image from GHCR, which starts private (DA-OPS-11), with no login; the job takes
  `packages: read` and logs in to `ghcr.io` before it, as the image's jobs do. Step 3's block of
  `weekly.yml` also ends the file with a blank line, which the commit leaves out.
