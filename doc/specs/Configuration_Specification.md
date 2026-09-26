---
updated: 2026-09-26
domain: Architecture
status: Specification (supersedes the Configuration_file_model.md draft)
tags: [architecture, configuration, spec, validation, multi-sink, iceberg, delta-lake]
sources: []
---

# CDC System — Configuration Specification

This document is the authoritative specification for the YAML configuration file of the
CDC system described in `System-Overview.md` and `Data-Flow.md`. It supersedes the
`Configuration_file_model.md` draft. It is the contract for three things:

1. The config **loader / parser** (structure, types, defaults, strictness).
2. The config **validation** API endpoint (the master validation table, §13 — gap G-1f).
3. The config **reload / reconcile** engine (mutability classes, §12).

Where this spec deviates from the prior draft, every change is listed in §15.

---

## 1. Conventions used in this document

### 1.1 Field notation

Each field is described with the following attributes:

| Attribute | Meaning |
|---|---|
| **Type** | The value type (see §4 for the type vocabulary). |
| **Required** | `yes` / `no` / `conditional` (with the condition). |
| **Default** | The value applied when the field is absent. `—` means no default (must be supplied). |
| **Mutability** | Reload behavior class: `R`, `PR`, or `PROC` (see §1.2). |
| **Scope** | `globals` / `defaults` / `pipeline` — where the field may appear. |

A field marked **Required: conditional** is mandatory only when a stated condition holds
(e.g., `slot` is required only when `source.type = postgres`).

"Resolvable" means: present either directly on the pipeline **or** inherited from `defaults`
after the merge in §8. A required field that is resolvable may be omitted on the pipeline if
`defaults` supplies it.

### 1.2 Mutability classes (reload behavior)

The system reloads config on the fly (`POST /v1/config/reload`), diffs old vs new, and
reconciles. Every field belongs to exactly one mutability class that defines what a change
to it triggers:

| Class | Name | On change during a live reload |
|---|---|---|
| **R** | Reloadable | Applied in place, no interruption. (e.g., `log.level`, `sink.flush.*`, `tables.include`.) |
| **PR** | Pipeline-restart | The affected pipeline(s) are restarted per the restart policy (§12.4): drain/flush, release resources, re-create. Other pipelines keep running. (e.g., `source.host`, `sink.iceberg.catalog.*`.) |
| **PROC** | Process-restart | Cannot be hot-applied. The reload is **rejected** with an error naming the field; the operator must restart the worker process. (e.g., `cluster.tokens`, `api.address`, memberlist binding.) |

The reload engine computes the highest class touched per scope and acts accordingly. A reload
that touches **only** R-class fields never interrupts data flow.

A field may be **conditionally** classed: its effective class depends on whether the change
alters a *derived* value rather than just the literal text. For example, editing
`tables.include` is a no-op when the resolved table set is unchanged but PR when it adds or
removes tables.

### 1.3 Severity vocabulary (validation)

- **error** — a config-time validation failure (structural or resolution); the **entire config**
  is rejected (§1.4).
- **warning** — the config loads; a warning is logged (and surfaced by the validation
  endpoint). The system continues.

### 1.4 Failure blast radius

There are two distinct phases, with deliberately different blast radii.

**Config-time validation (this spec's §13 rules — structural Phase 1 *and* resolution Phase 2,
see §11) — all-or-nothing.** The config file is a single
authored artifact representing one coherent intent. Any **error**, whether it is global
(`globals.*`, malformed file, bad `config_version`) or located inside a single pipeline
(missing required field, bad enum, mutual-exclusion or cross-field violation, charset/identity
violation, or a reference that fails to resolve — unset env var, unreadable/invalid
`credentials_file`, malformed PEM), rejects the **entire** config:

- On **startup**: the process refuses to start. A partial fleet is never launched.
- On **reload**: the new config is rejected and the **previously running config stays fully in
  effect**. Nothing is changed.

There is **no partial or per-pipeline application** of a statically-invalid config — valid
pipelines are *not* loaded while a broken one is skipped. Applying part of a config the operator
got wrong is more dangerous than rejecting all of it: on reload it would silently leave some
pipelines on the new config and one on the old (or unstarted), drifting the cluster away from
the operator's intent. The validation API endpoint exists precisely so an operator can confirm
validity before applying, so this strictness costs nothing during iteration. **Warnings** never
block loading.

**Runtime — isolated per pipeline/table.** Once a *statically valid* config is applied, failures
that can only manifest at runtime are **not** validation concerns and are isolated to the
smallest unit so one bad pipeline never downs the fleet:

- Unreachable source host, wrong credentials, a replication slot that already exists, an
  unreachable catalog, insufficient source privileges or server settings (the start-time source
  preconditions in §11.2), etc. → the affected **pipeline** halts with an error, surfaced via the
  API and metrics, while other pipelines keep running.
- A glob-discovered table with no primary key, an unsafe DDL change → the affected **table**
  halts (skip + warning) while the rest of the pipeline continues.

This mirrors the per-table halt philosophy in `System-Overview.md`. The boundary is the rule:
**statically invalid config never reaches runtime; runtime failures never reach validation.**

---

## 2. File format & loading

- **Format:** YAML 1.2, UTF-8, a single document.
- **Path:** supplied via a command-line parameter. No config is read from any implicit
  location.
- **Reload:** triggered by `POST /v1/config/reload`; the worker re-reads the **same** file
  path, parses, validates, diffs against the running config, and reconciles (§12).
- **No local persistence:** nothing derived from the config is written to the local
  filesystem. Operational state goes to cloud storage per `System-Overview.md`.
- **Unknown keys:** the parser runs in **strict mode** — any key not defined in this spec is an
  **error** (serde `deny_unknown_fields`). This is the primary typo-protection mechanism.
  Strictness applies at every object level (top level, `globals`, each pipeline, every
  sub-block).
- **`config_version`:** a top-level integer (see §3) gates structural compatibility.

---

## 3. Top-level structure

```yaml
config_version: 1        # mandatory; structural compatibility gate
globals:   { ... }       # mandatory; non-pipeline parameters
defaults:  { ... }       # optional; pipeline defaults (merged into each pipeline)
pipelines: { ... }       # mandatory; map of pipeline name -> pipeline config (may be empty {})
```

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `config_version` | integer | yes | — | PROC | Must equal a version this binary understands. Unknown/old versions are rejected with a clear migration message. Current: `1`. |
| `globals` | object | yes | — | mixed | §6. |
| `defaults` | object | no | `{}` | mixed | §7. Pipeline-shaped defaults. |
| `pipelines` | map<string,object> | yes | — | mixed | §9. Key = human pipeline name. May be an empty map (`{}`) — a valid, idle deployment. |

> **NEW (vs draft):** `config_version` did not exist. It is mandatory.

---

## 4. Value-type vocabulary & common conventions

These conventions apply to **every** field of the corresponding type.

### 4.1 Duration

Human-readable duration strings parsed by the `humantime` grammar: `200ms`, `5s`, `60s`,
`6h`, `1h30m`. Bare integers are **not** durations except where a field is explicitly an
integer count (e.g., `flush.seconds`, `flush.rows`). All `*_interval`, `*_timeout`,
`heartbeat`, and `timeouts.*` fields are Durations.

### 4.2 Byte size

Human-readable size strings: `512MiB`, `256MB`, `1GiB`, or a bare integer interpreted as
**bytes**. Binary units (`KiB`/`MiB`/`GiB`) and decimal units (`KB`/`MB`/`GB`) are both
accepted and are **not** equal — `512MiB = 536,870,912`, `512MB = 512,000,000`.

> **CHANGE (vs draft):** the draft used a bare `512000000` with a comment reading "512Mb",
> which is internally inconsistent (512 MB ≠ 512 MiB). Size fields now accept unit suffixes,
> and the documented default for `flush.bytes` is `512MiB`.

### 4.3 Environment-variable substitution

Any **string** value (whole or partial) may reference environment variables. Substitution
runs **after** YAML parsing, on string leaf values, and the result is then coerced into the
field's declared type.

Grammar:

| Form | Meaning |
|---|---|
| `${VAR}` | Replaced by the value of `VAR`. If `VAR` is unset → **load error** (V-063). |
| `${VAR:-default}` | Value of `VAR`, or `default` if `VAR` is unset/empty. |
| `$${...}` | A literal `${...}` (escape; the doubled `$` is consumed). |

Partial substitution is allowed: `bucket: ${ACCOUNT_ID}-state` → `12345-state`.
Substitution is **not** recursive (the substituted value is not re-scanned for `${...}`).
After substitution the resulting string is parsed into the target type, so `port: ${PG_PORT}`
where `PG_PORT=5432` yields the integer `5432`; a non-numeric value is a type error.

> **NEW (vs draft):** the draft documented only the bare `${VAR}` syntax with no rule for
> undefined variables, defaults, escaping, ordering, or type coercion.

### 4.4 TLS / PEM material

Fields named `cert`, `key`, `ca` accept **either** a filesystem path **or** inline PEM data
**or** an `${ENV}` reference resolving to either. Detection: a value beginning with
`-----BEGIN` is treated as inline PEM; otherwise it is a path. This applies uniformly to
`globals.api.tls`, `globals.cluster.memberlist.tls`, `source.ssl_*`, and `sink.iceberg.catalog.*.tls`.

### 4.5 Booleans, lists, maps

Standard YAML. Lists are ordered and, under merge, are **replaced** (not concatenated) — see
§8.3.

### 4.6 Credential files (`credentials_file`, `token_file`)

Anywhere a `user`/`password` pair or an OAuth2 `client_id`/`client_secret` pair is accepted, a
`credentials_file` field is offered as a **mutually exclusive** alternative: instead of the two
explicit fields, give the path to a JSON file holding the pair.

- `user`/`password` contexts (`globals.api`, `source`, `sink.iceberg.catalog.hive`):
  `{ "user": "...", "password": "..." }`
- OAuth2 context (`sink.iceberg.catalog.rest.oauth2`):
  `{ "client_id": "...", "client_secret": "..." }`

The `credentials_file` **value is a path** and is itself subject to env-var substitution (§4.3)
— e.g. `credentials_file: ${DB_CREDS_FILE}` or a literal `/run/secrets/db.json`. The file is read
during **resolution** (§11 phase 2); its JSON must parse and contain the two required, non-empty
fields (V-067).

Where a **single bearer token** is accepted (`sink.iceberg.catalog.rest`, `auth: bearer`), the parallel field
is **`token_file`**: a path (also env-var-substitutable, e.g. `token_file: ${CATALOG_TOKEN_FILE}`)
to a file whose **raw contents are the token**. It is **not** JSON — a bearer token is a single
opaque value, so the file holds just the token (any trailing newline/whitespace is trimmed). It is
read during resolution and must be non-empty (V-071); it is mutually exclusive with `token`
(V-070).

**Why a file rather than just `${ENV}`:** environment variables are fixed for a process's
lifetime — rotating an env-var secret requires a full process restart (a PROC change). A file's
*contents* can be replaced in place by an external secret manager, and the new credentials/token
are picked up on the next config reload without a process restart: the reconcile diff is computed
on **resolved** values (§11.1), so a content change is detected and applied per the field's
mutability class (R or PR depending on the field).

`credentials_file` / `token_file` and their explicit counterparts are mutually exclusive (V-065 /
V-066 / V-070): provide exactly one. Which source satisfies a credential requirement is checked by
the per-context requirement rules (V-010, V-040, V-044, V-045, V-049).

---

## 5. Reserved metadata column names

The accumulator injects lineage columns (`_op`, `_ts`, `_lsn`, `_seq`, `_source_db`, `_pipeline`;
see `Data-Flow.md`). The `__avalanche_` prefix is reserved for sink-internal working columns (e.g.
the Delta MERGE delete flag); they are never persisted. Source columns whose names collide with these are a runtime concern, not
a config concern, but operators should be aware the `_`-prefixed names are reserved.

---

## 6. `globals` section

Parameters not tied to any single pipeline: cloud target, application state location,
management API, logging, cluster membership, tracing, and provider-specific settings.

```yaml
globals:
  cloud: aws
  state:    { ... }
  api:      { ... }
  log:      { ... }
  cluster:  { ... }
  tracing:  { ... }     # optional (§6.6)
  azure:    { ... }     # optional, cloud = azure only (§6.7)
```

### 6.1 `globals.cloud`

| Field | Type | Required | Default | Mutability |
|---|---|---|---|---|
| `cloud` | enum `aws` \| `google` \| `azure` | no | `aws` | PROC |

Selects the object-storage provider and SDK credential chain. **No explicit credentials are
configured** — the provider SDK's default credential chain is used (instance profile / IAM
role, environment variables, workload identity, managed identity). For **Azure**, every
`bucket` field across the config is interpreted as a **container** name (§10); the storage account
comes from the credential chain or `globals.azure.account_name` (§6.7).

Affects which `sink.iceberg.catalog` and `sigv4` options are valid (§6, §9.5): `sink.iceberg.catalog.glue` and
`rest.auth = sigv4` require `cloud = aws`.

### 6.2 `globals.state`

Application-level state: config-generation pointer, cluster-membership cache, and (when
designed) the lease registry. This is **distinct** from per-pipeline operational state (§9.6,
§1.4 of `Data-Flow.md`).

```yaml
state:
  bucket: ${ACCOUNT_ID}-config-state-bucket
  prefix: cdc-app          # optional
```

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `bucket` | string | yes | — | PROC | Bucket (Azure: container) for application state. |
| `prefix` | string | no | `""` (root) | PROC | Key prefix under the bucket. See §10 for joining rules. |

> Boundary rule (resolves gap 1.4): `globals.state` holds **cross-pipeline** data only —
> config generation pointer, cluster membership cache, token-count anchor, lease registry.
> **Per-pipeline** offsets/cursors/snapshot status live under the pipeline `state` location
> (§9.6). Implementations must not write per-pipeline offsets into `globals.state`.

### 6.3 `globals.api`

The management REST API listener and its authentication.

```yaml
api:
  address: 0.0.0.0:8080
  auth: none
  user: admin
  password: ${API_PASSWORD}
  jwt:    { ... }     # when auth: jwt
  oauth2: { ... }     # when auth: oauth2
  tls:    { ... }
```

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `address` | host:port | no | `0.0.0.0:8080` | PROC | IPv4/IPv6/DNS host + port. Listener binding cannot hot-change. |
| `auth` | enum `none` \| `basic` \| `jwt` \| `oauth2` | no | `none` | R | Auth mode for **authenticated** endpoints. |
| `user` | string | conditional | — | R | For `auth = basic`: required unless `credentials_file` is given (V-049); mutually exclusive with it (V-065). |
| `password` | string | conditional | — | R | For `auth = basic`: required unless `credentials_file` is given (V-049); mutually exclusive with it (V-065). Use `${ENV}`. |
| `credentials_file` | path | conditional | — | R | Alternative to `user`/`password` for `auth = basic` (§4.6): path to JSON `{user, password}`. Mutually exclusive with `user`/`password` (V-065). Enables runtime rotation by replacing the file's contents. |
| `jwt` | object | conditional | — | R | Required when `auth = jwt`. §6.3.1. |
| `oauth2` | object | conditional | — | R | Required when `auth = oauth2`. §6.3.1. |
| `role_mapping` | map<role, list<string>> | no | identity | R | Maps IdP role strings to the three local roles `viewer` ⊂ `operator` ⊂ `admin` (`research/API_RBAC_and_Versioning.md` §4). Only meaningful for `auth ∈ {jwt, oauth2}` (V-100). Omitted = identity (a claim value `viewer`/`operator`/`admin` maps to itself). Unmapped IdP roles are ignored; the effective role is the **maximum** mapped role. |
| `tls` | object | no | `{enabled:false}` | PROC | §6.3.2. |

Endpoints that are **always unauthenticated** regardless of `auth` (per `System-Overview.md`):
`/health/live`, `/health/ready`, `/metrics`.

#### 6.3.1 `globals.api.jwt` / `globals.api.oauth2` (verification parameters)

> **NEW (vs draft):** the draft listed `auth: jwt | oauth2` but defined no verification
> parameters. These blocks supply the schema. The role→permission matrix (G-16) is defined in
> `research/API_RBAC_and_Versioning.md` §2; IdP role strings are mapped via `role_mapping` (§6.3).

```yaml
jwt:
  issuer: https://idp.example.com/realms/cdc      # mandatory; expected `iss`
  audience: cdc-management-api                     # optional; expected `aud`
  jwks_url: https://idp.example.com/realms/cdc/protocol/openid-connect/certs  # mandatory
  roles_claim: realm_access.roles                  # optional; JSON path to roles claim (default "roles")

oauth2:                                            # OIDC; superset of jwt for token introspection / discovery
  issuer: https://idp.example.com/realms/cdc       # mandatory
  audience: cdc-management-api                      # optional
  jwks_url: https://idp.example.com/realms/cdc/protocol/openid-connect/certs   # optional if discovery used
  discovery_url: https://idp.example.com/realms/cdc/.well-known/openid-configuration  # optional
  roles_claim: realm_access.roles                   # optional (default "roles")
```

`roles_claim` tells the verifier **where** to read roles; `role_mapping` (§6.3) maps them to the
local roles; **what** each role may call is the endpoint → minimum-role matrix in
`research/API_RBAC_and_Versioning.md` §2 (`admin ⊇ operator ⊇ viewer`, functional endpoints under
`/v1`; health and metrics unversioned and unauthenticated). Binding per mode: `none` = open, `basic` =
`admin`, `jwt`/`oauth2` = mapped claim (401 unauthenticated vs 403 insufficient role).

#### 6.3.2 `globals.api.tls`

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `enabled` | bool | no | `false` | PROC | |
| `cert` | PEM/path | conditional | — | PROC | Required when `enabled`. §4.4. |
| `key` | PEM/path | conditional | — | PROC | Required when `enabled`. §4.4. |

If TLS is terminated by an upstream proxy/sidecar, leave `enabled: false` and rely on the
proxy. The "non-anonymous auth without TLS" check (V-052) is therefore a **warning**, not an
error, to accommodate that topology.

### 6.4 `globals.log`

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `level` | string | no | `info` | R | `trace`\|`debug`\|`info`\|`warn`\|`error`; per-module granularity allowed (`tracing` EnvFilter syntax, e.g. `info,rustcdc::sink=debug`). Hot-changeable. |
| `format` | enum `json`\|`compact`\|`pretty` | no | `json` | R | Output format on stderr. |

> Note: `System-Overview.md` states log level/format are command-line parameters. This spec
> treats them as config fields that the CLI flags **override** at startup (CLI wins). If you
> prefer CLI-only, drop this block; the validation table (V-016/V-017) still applies to the
> CLI values.

### 6.5 `globals.cluster`

```yaml
cluster:
  tokens: 128
  lock_backend: storage
  lease_duration: 15s
  lease_renewal_interval: 5s
  lease_safety_margin: 0.15
  rebalance_stabilization: 15s
  max_concurrent_handoffs: 2
  max_concurrent_takeovers: 4
  drain_timeout: 60s
  discovery: static            # static | dns
  memberlist: { ... }
```

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `tokens` | integer | no | `128` | PROC | Virtual nodes per physical node on the consistent-hash ring. **Must be identical cluster-wide** (V-026, V-062): the first node writes the established value to `globals.state`; a node whose `tokens` differs refuses to start. Recommended band 64–256; out-of-band values warn; <1 or >4096 error. |
| `lock_backend` | enum | no | `storage` | PROC | Mutual-exclusion lease backend (D-2, `Clustering_Architecture_Research.md` §5.6). `storage` = object-storage CAS lease (default, portable). `kubernetes` = K8s Lease API — **reserved, not yet implemented**; only `storage` is valid in v1. **Must be identical cluster-wide** (V-075, V-076): pinned in the `globals.state` cluster anchor; a node whose `lock_backend` differs refuses to start, because a mixed-backend cluster has two independent arbiters and is split-brain by construction. |
| `lease_duration` | Duration | no | `15s` | PROC | Per-pipeline lease TTL — the safety boundary and the worst-case unowned window after an unplanned death. **Cluster-wide consistent**: anchored in `globals.state` with `tokens`; a node whose value differs refuses to start (V-094). Balanced profile ratified (D-6, `Clustering_Architecture_Research.md` §18). |
| `lease_renewal_interval` | Duration | no | `5s` | PROC | Must be ≤ `lease_duration / 3` (V-073, hard error). Recommended equal cluster-wide. |
| `lease_safety_margin` | float (0–0.5) | no | `0.15` | PROC | Fraction of `lease_duration` held back as the renewal safety-deadline guard (§19.3 of the clustering doc; V-072). |
| `rebalance_stabilization` | Duration | no | `15s` | R | Membership must be stable this long before proactive rebalancing; applies symmetrically to additions incl. rejoins (flap immunity, D-11). Does **not** delay dead-node failover. |
| `max_concurrent_handoffs` | integer | no | `2` | R | Per-node cap on simultaneous graceful drains (V-074). Local knob, not cluster-wide. |
| `max_concurrent_takeovers` | integer | no | `4` | R | Per-node cap on simultaneous Phase-0 starts on the unplanned-takeover path, with jitter (D-10; V-074). Local knob. |
| `drain_timeout` | Duration | no | `60s` | R | Max graceful drain (flush → commit → checkpoint → release lease) before falling back to unplanned replay. |
| `discovery` | enum `static`\|`dns` | no | `static` | PROC | `static` = `memberlist.join_members`; `dns` = resolve `discovery_dns_name` (k8s headless Service). V-095. |
| `discovery_dns_name` | string | conditional | — | PROC | Required when `discovery = dns` (V-096). |
| `memberlist` | object | no | (defaults below) | PROC | §6.5.1. |

A deployment with no peers (empty `join_members`, none discovered) is the supported
**single-node mode**: the ring contains only self, all pipelines are owned locally, gossip is
idle, and the lease (`Clustering_Architecture_Research.md` §5) still runs to guard against a second
process accidentally attaching to the same `globals.state` bucket (e.g. a rolling-deploy overlap).

#### 6.5.1 `globals.cluster.memberlist`

SWIM gossip transport (the `memberlist` crate). All fields are **PROC** (transport binding and
gossip timers are fixed for a process lifetime) unless noted.

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `join_members` | list<host:port> | no | `[]` | Seed addresses to join. Empty = single-node (or wait for inbound joins). |
| `node_name` | string | no | OS hostname | Unique, **stable** node identity; this is also the `node_id` used for consistent-hash ring placement (`Clustering_Architecture_Research.md` §3.4). See the sourcing rule below. |
| `advertise_addr` | string | no | `""` (bind_addr or first private IP) | Address advertised to peers. |
| `advertise_port` | integer | no | `7946` | Port advertised to peers. |
| `bind_addr` | list<ip> | no | `[0.0.0.0]` | Listen addresses. |
| `bind_port` | integer | no | `7946` | UDP/TCP gossip port. |
| `packet_dial_timeout` | Duration | no | `5s` | |
| `packet_write_timeout` | Duration | no | `5s` | |
| `compression_enabled` | bool | no | `true` | Snappy compression for gossip. |
| `gossip_interval` | Duration | no | `200ms` | |
| `gossip_nodes` | integer | no | `3` | Peers gossiped per interval. |
| `probe_interval` | Duration | no | `1s` | |
| `probe_timeout` | Duration | no | `500ms` | |
| `dead_node_reclaim_time` | Duration | no | `30s` | Bounds the stale-membership window (relevant to split-brain, G-8). |
| `push_pull_interval` | Duration | no | `30s` | Full-state sync cadence. |
| `retransmit_factor` | integer | no | `4` | Retransmit multiplier. |
| `lifeguard_enabled` | bool | no | `true` | Lifeguard extensions to SWIM (local health aware probing/suspicion; G-23). Recommended equal cluster-wide. |
| `tls` | object | no | `{enabled:false}` | §6.5.2. |

**`node_name` sourcing (D-1).** `node_name` is the node's stable identity for both gossip and
consistent-hash ring placement, so it must be **stable across restarts of the same logical worker**
and **must not embed an ephemeral pod IP**. On **Kubernetes, run workers as a StatefulSet (not a
Deployment)** and set `node_name` to the pod name via the Downward API — a Deployment's random,
changing pod names would reshuffle the derived ring on every restart. For **standalone** deployments,
use a persisted-once UUID on a durable path, falling back to `${HOSTNAME}` only when no durable volume
exists. A restarted process keeps its `node_name` but bumps a per-process `incarnation` so peers
discard its stale in-flight gossip.

#### 6.5.2 `globals.cluster.memberlist.tls`

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `enabled` | bool | no | `false` | |
| `cert` | PEM/path | conditional | — | Required when `enabled`. |
| `key` | PEM/path | conditional | — | Required when `enabled`. |
| `ca` | PEM/path | no | — | Custom CA. Presence implies peer-certificate verification (mutual TLS); absence = server trust via system store. |

> Fixes the draft's `cat:` → `ca:` typo (already corrected upstream; restated here as the
> canonical field name).

### 6.6 `globals.tracing`

Distributed tracing (G-32, `research/Observability_Metrics_and_Tracing.md` §3). Spans are emitted at
batch/commit granularity (flush → sink commit, snapshot chunk, DDL apply), never per source event.
When tracing is active, `trace_id` / `span_id` are injected into JSON log records.

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `otlp_endpoint` | URL | no | — | R | OTLP collector endpoint. **Absent ⇒ tracing disabled** (zero overhead, no error). |
| `sample_ratio` | float [0,1] | no | `0.0` (no endpoint) / `0.05` (endpoint set) | R | Head sampling ratio (V-097). `> 0` without an endpoint warns (V-098). |

### 6.7 `globals.azure` (`cloud = azure` only)

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `account_name` | string | no | from credential chain | PROC | Storage account for all containers (G-34). Credentials still come from the SDK default chain (managed / workload identity) — no secret fields. Valid storage-account name (V-101). |

---

## 7. `defaults` section

`defaults` is pipeline-shaped: it may contain any subset of the fields valid under a single
pipeline **except** pipeline identity (`id`) and per-pipeline resource names that must be
unique (`source.postgres.publication`, `source.postgres.slot`). Specifying a unique resource
name in `defaults` is an error (V-058 area) because it would force a collision across
pipelines.

Defaults are merged into every pipeline per §8. The example default values in §14 are the
**actual** system defaults; a field absent from both `defaults` and the pipeline falls back to
the value in this spec's tables.

`defaults.sink.iceberg.catalog` may set the full Iceberg catalog block (DRY); a pipeline overrides it
per the union rule in §8.4. Both `defaults.sink.iceberg.catalog.hive` and
`defaults.sink.iceberg.catalog.rest` carry the same protocol-correct auth schema as the pipeline-level
blocks (§9.5). `defaults.sink.type` may be set — it is still an explicit choice in the config file,
which is all V-091 requires.

---

## 8. Merge & override semantics

This section is normative for the reconcile engine. The draft stated the rule for the catalog
only; the rules below cover every shape.

### 8.1 Direction

Effective pipeline value = `defaults` deep-merged with the pipeline block, **pipeline wins**
on every conflict.

### 8.2 Scalars and maps

Scalars (strings, numbers, bools, durations, sizes, enums) and nested **maps/objects** are
deep-merged key by key. A key present in the pipeline overrides the same key in `defaults`;
keys present only in `defaults` are inherited; keys present only in the pipeline are added.

### 8.3 Lists

Lists are **replaced wholesale**, never merged element-wise. If a pipeline specifies
`tables.include`, it fully replaces any `defaults.tables.include`. Same for
`memberlist.join_members`, `bind_addr`, `tables.exclude`, etc.

> **NEW (vs draft):** list-merge behavior was unspecified. Replacement is the rule.

### 8.4 Discriminated-union subtrees

Three subtrees are unions; the **active branch** replaces the entire subtree, and the sibling
branch from `defaults` is **dropped** (no field-level merge across branches):

- **`source` engine block** — branches `postgres` / `mysql`, selected by `source.type`. If
  `defaults.source.type = postgres` (with a `postgres:` block) and a pipeline sets
  `source.type = mysql`, the defaulted `postgres:` block is dropped and the pipeline's `mysql:`
  block applies. The common `source` fields (`host`, `port`, `db`, `user`, `password`,
  `ssl_*`, `timeouts`) still merge normally.
- **`sink` format block** — branches `iceberg` / `delta`, selected by `sink.type`. If `defaults`
  carries `sink.iceberg` and a pipeline sets `sink.type = delta`, the defaulted `iceberg:` block is
  dropped. The common `sink` fields (`type`, `write_mode`, `bucket`, `prefix`, `flush`, …) still
  merge normally.
- **`sink.iceberg.catalog`** — branches `glue` / `hive` / `rest`. If
  `defaults.sink.iceberg.catalog.glue` is set and a pipeline sets `sink.iceberg.catalog.rest`, the
  pipeline gets `rest` and the defaulted `glue` is discarded entirely. The common catalog fields
  (`type`, `retry`) still merge normally.

`schema_evolution` is a pipeline-level block (§9.7), not part of any union; it merges normally.

### 8.5 Worked example

```yaml
defaults:
  source: { type: postgres, host: db.internal, port: 5432, ssl_mode: require }
  sink:
    type: iceberg
    write_mode: mirror
    flush: { seconds: 60, rows: 100000, bytes: 512MiB }
    iceberg:
      strategy: mor
      catalog:
        glue: { name: "123456789012", database: analytics }

pipelines:
  orders:
    id: orders
    source: { db: orders, user: cdc, password: ${PW}, postgres: { publication: p, slot: s } }
    sink:
      bucket: lake
      flush: { rows: 50000 }
      iceberg:
        catalog: { rest: { url: https://cat.example.com, auth: oauth2, oauth2: {...} } }
  clicks:
    id: clicks
    source: { db: clicks, user: cdc, password: ${PW}, postgres: { publication: pc, slot: sc } }
    sink:
      type: delta
      bucket: lake
      delta: { checkpoint_interval: 20 }
```

Effective `orders`:
- `source`: host/port/ssl_mode/type from defaults; db/user/password/postgres from pipeline.
- `sink.type: iceberg`, `write_mode: mirror` inherited; `sink.iceberg.strategy: mor` inherited
  (map merge §8.2); `sink.iceberg.catalog`: **`rest` only** — the defaulted `glue` is gone (union
  rule §8.4).
- `sink.flush`: `seconds:60` and `bytes:512MiB` from defaults, `rows:50000` from pipeline.

Effective `clicks`:
- `sink.type: delta` — the defaulted `sink.iceberg` block (strategy + catalog) is **dropped**
  (union rule §8.4); `sink.delta` from the pipeline; `flush` and `write_mode` inherited.

---

## 9. `pipelines` section

A map of human-readable pipeline name → pipeline config.

### 9.1 Pipeline identity: name vs `id`

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| *(map key)* | string | yes | — | R | Human name. Used in the `_pipeline` lineage column and metric labels. Renaming the key **without** changing `id` is a pure rename. Charset: `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$` (V-057). |
| `id` | string | yes | — | special | **Immutable identity** for state keying and resource lifecycle. Changing `id` = delete-and-recreate the pipeline (drop slots/publications per `auto_cleanup`, abandon old state prefix, snapshot per `snapshot_mode`). Charset `^[a-z0-9][a-z0-9_-]{0,62}$`, ≤ 63 chars (V-055); unique across all pipelines (V-056). |

The `id` is used to build the state prefix (`pipelines/<id>/`, see `Data-Flow.md`) and may be
used to derive object-storage prefixes; keep it within the conservative charset above so it is
always a safe path segment and metric-label value. If future versions auto-derive DB resource
names from `id`, hyphens will be translated to underscores (PG slot names forbid hyphens).

### 9.2 `source`

Common fields plus exactly one engine block (`postgres` xor `mysql`), selected by `type`.

```yaml
source:
  type: postgres
  host: db.example.com
  port: 5432
  db: analytics
  user: cdc
  password: ${DB_PASSWORD}
  ssl_mode: require
  ssl_cert: /path/or/PEM
  ssl_key:  /path/or/PEM
  ssl_ca:   /path/or/PEM
  timeouts: { connect: 10s, command: 30s, snapshot: 6h }
  postgres: { ... }     # iff type=postgres
  mysql:    { ... }     # iff type=mysql
```

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `type` | enum `postgres`\|`mysql` | no | `postgres` | PR | Selects the engine block. |
| `host` | string | yes (resolvable) | — | PR | |
| `port` | integer | no | `5432` (pg) / `3306` (mysql) | PR | Default depends on `type`. |
| `db` | string | yes (resolvable) | — | PR | Database name. **PostgreSQL:** the single database this pipeline replicates (slot/publication are database-scoped); table patterns are `schema.table` within it. **MySQL:** the single database the pipeline replicates; table patterns are bare names relative to it (one MySQL database per pipeline). |
| `user` | string | yes (resolvable) | — | PR | Required unless `credentials_file` is given (V-010); mutually exclusive with it (V-065). |
| `password` | string | yes (resolvable) | — | PR | Required unless `credentials_file` is given (V-010); mutually exclusive with it (V-065). Use `${ENV}`. Re-read on reconnect to support rotation. |
| `credentials_file` | path | yes (resolvable) | — | PR | Alternative to `user`/`password` (§4.6): path to JSON `{user, password}`. Mutually exclusive with them (V-065). Rotating the file's contents + reload rotates the DB credentials without a process restart. |
| `ssl_mode` | enum | no | `require` | PR | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full`. `allow` is **invalid for MySQL** (V-030). |
| `ssl_cert` / `ssl_key` / `ssl_ca` | PEM/path | no | — | PR | §4.4. |
| `timeouts.connect` | Duration | no | `10s` | PR | |
| `timeouts.command` | Duration | no | `30s` | PR | |
| `timeouts.snapshot` | Duration | no | `6h` | PR | **Per-table** snapshot timeout. |
| `toast_strategy` | enum `carry_forward`\|`re_query` | no | `carry_forward` | PR | **PostgreSQL only.** Handling of unchanged TOAST(out-of-line) columns in UPDATE events: `carry_forward` reuses the last known value; `re_query` re-fetches the row over the control connection. `re_query` on high-volume snapshots may saturate the control pool (V-077 warning). Ignored for MySQL. See `research/Type_Mapping.md`. |

> **CHANGE (vs draft):** `ssl-mode`/`ssl-cert`/`ssl-key`/`ssl-ca` renamed to snake_case
> (`ssl_mode`, …) for consistency with the rest of the schema.

#### 9.2.1 `source.postgres` (iff `type = postgres`)

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `publication` | string | yes | — | PR | PG publication name. Must be a valid identifier; unique per source DB across pipelines (V-059). Auto-created/maintained by the app. |
| `slot` | string | yes | — | PR | Logical replication slot name. Charset `^[a-z_][a-z0-9_]{0,62}$` (PG constraint); **unique** per source host+db across pipelines (V-058). |
| `auto_cleanup` | bool | no | `true` | R | Drop the slot & publication when the pipeline is deleted. Teardown-time policy flag only — consulted at deletion/`id`-change, not on the live path, so a change applies in place with no restart. |
| `heartbeat` | Duration | no | `60s` | R | StandbyStatusUpdate interval; keeps the connection alive and prevents WAL-lag-as-inactivity. |
| `slot_management` | enum `managed`\|`require_existing` | no | `managed` | PR | `managed`: create the slot if absent, reuse if present, drop on delete per `auto_cleanup`. `require_existing`: never create (DBA-owned slot; error if absent) and **requires `auto_cleanup: false`** (V-103). `research/Postgres_Slot_and_Publication_Lifecycle.md` §2. |
| `slot_on_orphan` | enum `halt`\|`adopt_resnapshot` | no | `halt` | R | Slot exists but the pipeline has no state for it. `halt`: refuse to start, naming the slot. `adopt_resnapshot`: take ownership, restart from a fresh consistent point and incrementally re-snapshot all tables. Resume-from-slot without a snapshot is never offered. (Slot-lifecycle doc §1.1; named `slot_on_orphan` because `slot` is the slot-name string.) |
| `max_wal_lag_bytes` | ByteSize \| null | no | `null` | R | WAL-retention guard (G-22). `cdc_wal_lag_bytes` is always emitted; when set and exceeded the pipeline **halts** to protect the source disk. `null` = alert only. ≥ 64 MiB when set (V-104). |

#### 9.2.2 `source.mysql` (iff `type = mysql`)

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `replication_type` | enum `binlog`\|`gtid` | no | `binlog` | PR | `gtid` is preferred (position-independent, gap-free). |
| `tinyint1_as_bool` | bool | no | `true` | PR | Map `TINYINT(1)` columns to logical `bool`. When `true`, requires the display-width from the catalog query (channel C); when `false`, `TINYINT(1)` maps as `TINYINT SIGNED` → `Int16`. See `research/Type_Mapping.md`. |
| `tinyint1_detection` | enum `information_schema`\|`none` | no | `information_schema` | PR | Source of the `TINYINT(1)` display-width signal. `none` disables the detection (all `TINYINT` treated as integer); `information_schema` uses the catalog query (always available via channel C). |
| `binlog_purge_action` | enum `error`\|`re_snapshot` | no | `error` | R | Resume position purged on the server (G-26). `error`: halt with an error (operator decides). `re_snapshot`: automatic recovery via incremental re-snapshot. GTID `replication_type` remains recommended. |

### 9.3 `tables`

```yaml
tables:
  include: [ public.*, data.analytics, data.sources_* ]   # mandatory, non-empty
  exclude: [ public.temp_* ]                               # optional
  snapshots:
    mode: never
    parallelism: 1
    chunk_size: 10000
    overrides: { public.table_1: always, data.analytics: initial }
  primary_key:                                           # optional per-table identifier override
    data.analytics: [ tenant_id, event_id ]
  partitioning:                                          # optional per-table partition spec
    public.orders: [ { column: _ts, transform: day } ]
    data.analytics: [ { column: tenant_id, transform: bucket, n: 16 } ]
```

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `include` | list<glob> | yes | — | PR (cond.) | Non-empty (V-007). **PostgreSQL:** globs over `schema.table`, schema **mandatory** (e.g. `public.*`, `data.orders`). **MySQL:** bare table-name globs, **no** `schema.`/`database.` prefix — the database is fixed by `source.db` (e.g. `orders`, `payment_*`, `*`); a prefix is a validation error (V-064). Resolved against the live schema at (re)start. **Conditional:** PR when the edit changes the **resolved** table set — added tables go through Phase-0 snapshot evaluation, removed tables are drained and the publication is updated; a no-op when the resolved set is unchanged. |
| `exclude` | list<glob> | no | `[]` | PR (cond.) | Subtracted from the include set; same per-engine form as `include` (PG `schema.table`, MySQL bare table name). Same conditional rule: restart only when the resolved set actually changes. |
| `snapshots.mode` | enum | no | `never` | PR | `never`\|`initial`\|`always`\|`dump_always`\|`dump_initial` (semantics in `System-Overview.md`). |
| `snapshots.parallelism` | integer | no | `1` | R | ≥ 1 (V-027). Concurrent snapshot jobs. **> 4 warns** (V-038): each PG snapshot job uses ≥ 3 connections. The limit is read by the snapshot scheduler at job-launch time; a change affects only **future** scheduling — in-flight and already-queued jobs are unaffected — so it applies in place. Implies an adjustable permit count, not a pool sized once at pipeline start. |
| `snapshots.chunk_size` | integer | no | `10000` | R | Rows per keyset chunk of the incremental snapshot; the resume cursor is checkpointed per chunk (`research/Snapshot_Resumability_and_Consistency.md` §5). ≥ 1; very large values warn (V-108). |
| `snapshots.overrides` | map<table,mode> | no | `{}` | PR | Per-table mode override. Keys are full `schema.table` for PostgreSQL / bare table name for MySQL (V-064), **no globs** (V-033). A key not in the resolved set **warns** at load (V-034). Not allowed in `defaults`. |
| `column_type_overrides` | map<column, logical-type> | no | `{}` | PR | Per-column target-type override applied at schema resolution (e.g. `public.orders.amount: "decimal(38,10)"`). Values use the **sink-neutral logical type grammar** — `bool`, `int16`, `int32`, `int64`, `float32`, `float64`, `decimal(P,S)`, `string`, `binary`, `date`, `time`, `timestamp_tz`, `timestamp_ntz`, `uuid`, `json`, `list<T>` — never a format-specific type name (Iceberg `timestamp` = without tz, Delta `timestamp` = with tz). Each sink maps it to its physical type (`research/Type_Mapping.md` Hop 3). Must be valid (V-078) and mappable by the pipeline's sink (V-087). First step of the unsupported-type fallback chain. |
| `numeric_overflow_action` | enum `string`\|`error` | no | `string` | PR | Action when a `numeric(P,S)` with `P > 38` (beyond Iceberg `decimal` range) is encountered: `string` serializes it as text; `error` halts the table. With `error`, unscaled `numeric` columns may halt unexpectedly (V-079 warning). |
| `primary_key` | map<table, list<column>> | no | `{}` | PR (re-snapshot) | Identifier override per table (G-13): replaces the inferred source PK as the upsert/delete key — needed for PG `REPLICA IDENTITY USING INDEX` on a non-PK unique index. Selects existing identifying columns; it is **not** a surrogate key (`tables.surrogate_key` was rejected, G-30). Keys: same form as `snapshots.overrides` (V-033/V-064), no globs; unknown key warns (V-106). Column constraints (exist, top-level, primitive, NOT NULL, non-floating; PG ⊆ REPLICA IDENTITY) are checked at start (§11.2). Changing it on a live table = PK change ⇒ re-snapshot. `research/Ghost_Row_Dedup_and_Primary_Keys.md` §1/§7. |
| `partitioning` | map<table, list<field>> | no | `{}` (unpartitioned) | PR | Per-table partition spec (G-24). A field is `{ column, transform }` with `transform ∈ {identity, bucket, truncate, year, month, day, hour}` (+ `n` for `bucket`, `width` for `truncate`; V-107). `_ts` is a valid source (commit-time pruning; `day(_ts)` is the common opt-in). Transforms must be supported by the pipeline's sink (V-086 — Delta v1: `identity` only). A change applies to **new data only** (Iceberg spec evolution; no rewrite of history). Keys as for `primary_key`. Column existence / temporal-type checks at start (§11.2). `research/Iceberg_Partitioning_and_Layout_Evolution.md`. |

> **Per-table maps.** The research docs sketched `primary_key` and `partitioning` as bare lists; they
> are specified here as **per-table maps** (keyed like `snapshots.overrides`) because an identifier or
> partition column is a property of one table, not of a whole glob-resolved table set.

Glob grammar: `*` matches within a single name segment (it does not cross the `.` separator);
matching is case-sensitive. The match target depends on the engine — **PostgreSQL** matches
against `schema.table` (`public.*` = all tables in schema `public`; `*.orders` = `orders` in any
schema); **MySQL** matches against the bare table name (`payment_*` = every table prefixed
`payment_` in `source.db`).

### 9.4 `sink`

The pipeline's target table format and storage location. Common fields plus exactly one format block
(`iceberg` xor `delta`), selected by `type`. One sink per pipeline. See
`research/Multi_Sink_Architecture_and_Delta_Lake.md` (decisions D-MS.1–D-MS.15, R-1).

```yaml
sink:
  type: delta                 # REQUIRED — iceberg | delta; no default (V-091)
  write_mode: mirror          # mirror (v1) | changelog (reserved, V-089)
  bucket: lake-bucket
  prefix: orders
  flush: { seconds: 60, rows: 100000, bytes: 512MiB }
  iceberg: { ... }            # iff type = iceberg (§9.4.2)
  delta:   { ... }            # iff type = delta   (§9.4.3)
```

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `type` | enum `iceberg`\|`delta` | **yes** | **— (no default)** | **PR, re-seed** | Must be set explicitly (pipeline or `defaults`) — V-091. There is deliberately no default, so a pipeline's target format never changes implicitly when the runtime binary is rebuilt with a different compiled-in sink set. The sink must be compiled into the binary (V-083). Pipeline state records the sink type; changing it re-seeds the pipeline's tables (fresh positions, snapshot per `snapshot_mode`) and requires table locations that hold no other format's metadata (start-time check). |
| `write_mode` | enum `mirror`\|`changelog` | no | `mirror` | PR, re-seed | `mirror` = the target mirrors the source (upserts + deletes). `changelog` (append-only `_op` log; PK-less tables allowed) is **reserved** — error in v1 (V-089). A sink that does not declare the mode refuses the pipeline at start (§11.2). |
| `bucket` | string | yes (resolvable) | — | PR | Object-storage bucket (Azure: container). |
| `prefix` | string | no | pipeline `id` | PR | Root prefix for this pipeline's tables and co-located state. See §10 for the full path layout. |
| `flush` | object | no | (below) | R | Flush triggers (§9.4.1). |
| `max_transaction_batch_rows` | integer \| null | no | `null` | R | Split-commit threshold for oversized source transactions (G-29). `null` = never split (strict atomic visibility). When set, a larger transaction is committed in slices; the resume checkpoint stays pinned at the transaction start, so a crash replays it idempotently. ≥ 1; small values warn (V-109). `research/Large_Transaction_Handling.md`. |
| `max_transaction_buffer_bytes` | ByteSize | no | `1GiB` | R | Absolute per-transaction memory ceiling. Splitting disabled ⇒ reaching it **halts the pipeline** (never OOMs the worker); splitting enabled ⇒ an early flush trigger. ≥ 16 MiB (V-110). |
| `buffer_capacity_rows` | integer | no | `200000` | R | Capacity of the bounded inter-task channels (G-27). All channels are bounded; on persistent sink failure the pipeline is **DEGRADED** — retries with backoff and stops consuming once buffers fill. ≥ 1 (V-111). |
| `metrics.per_table` | bool | no | `false` | R | Add the `table` label to per-table metrics (G-33). Default labels are `{pipeline, source_host, source_db}`; enabling this multiplies series count on large fleets. |
| `iceberg` / `delta` | object | exactly one, matching `type` | — | PR | Format block (§9.4.2 / §9.4.3). The foreign block is forbidden (V-084). |

> **CHANGE (2026-09-26, multi-sink):** `sink.strategy` moved to `sink.iceberg.strategy`; the
> pipeline-level `catalog` moved to `sink.iceberg.catalog` (§9.5); `catalog.schema_evolution` moved to
> the pipeline-level `schema_evolution` (§9.7).

#### 9.4.1 `sink.flush` (OR semantics, tri-state per field)

A flush fires when **any** enabled trigger is satisfied (first-wins). Each trigger has three
states: **absent** → use default; explicit **`null`** → disabled; **value** → that threshold.

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `seconds` | integer \| null | no | `60` | R | Max age of the oldest buffered event. `0` = flush at **every transaction boundary** (not per-row). `null` = time-based flushing **disabled**. `< 5` (and > 0) **warns** (write amplification, V-037). |
| `rows` | integer \| null | no | `100000` | R | Max buffered rows. `null` = disabled. |
| `bytes` | ByteSize \| null | no | `512MiB` | R | Max buffered size. `null` = disabled. |

Constraints: all three `null` is an **error** (V-035). `seconds: null` with at least one of
`rows`/`bytes` set is valid but **warns** (possible long flush latency, V-036).

> **Implementation note (tri-state):** serde collapses "absent" and explicit `null` into
> `None`. To honor the "absent = default, null = disabled" distinction, deserialize these as
> `Option<Option<T>>` (e.g. `serde_with::rust::double_option`): outer `None` = absent (apply
> default), `Some(None)` = explicit null (disable), `Some(Some(v))` = value.

#### 9.4.2 `sink.iceberg` (iff `type = iceberg`)

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `strategy` | enum `mor`\|`cow` | no | `mor` | PR | Applies to **new** tables only; changing it does not migrate existing tables (G-12a). Valid only with `write_mode: mirror` (V-085). |
| `catalog` | union glue/hive/rest | yes (resolvable) | — | PR | Where Iceberg table metadata is registered — §9.5. |

> **Capability note.** Iceberg `mirror` needs a delete-capable commit that no published iceberg-rust
> provides (gap DA-SINK-1i). The config is valid; the pipeline is refused at start by the capability
> assertion (§11.2) until the pinned crate gains it.

#### 9.4.3 `sink.delta` (iff `type = delta`)

Settings apply when the sink **creates** a table; existing tables keep their properties.

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `column_mapping` | enum `none`\|`name` | no | `none` | PR (new tables) | `name` is **rejected in v1** (V-092): delta-rs 1.0 refuses ADD COLUMN on column-mapped tables, which would disable the only Delta schema-evolution op (probe P7b). |
| `checkpoint_interval` | integer | no | `10` | PR (new tables) | → `delta.checkpointInterval`. ≥ 1. |
| `log_retention` | Duration | no | `30d` | PR (new tables) | → `delta.logRetentionDuration`. Expired-log cleanup runs as a post-commit hook (format metadata housekeeping). |
| `timestamp_ntz_as` | enum `timestamp_ntz`\|`timestamp`\|`string` | no | `timestamp_ntz` | PR (new tables) | Delta type for source `timestamp without time zone` columns (DA-MS-8). `timestamp_ntz`: exact semantics, but the table is created at **reader v3 / writer v7** (`timestampNtz` table feature) — engines without table-feature support cannot read it. `timestamp`: the wall-clock value stored as a UTC instant; keeps the protocol at reader 1 / writer 2, loses the no-time-zone marker. `string`: `YYYY-MM-DD HH:MM:SS.ffffff`. A per-column `tables.column_type_overrides` entry takes precedence. Existing tables keep their column type. V-114. |
| `table_properties` | map<string,string> | no | `{}` | PR (new tables) | Passthrough of allow-listed `delta.*` table properties (V-093). |
| `catalog.type` | enum `none`\|`glue`\|`unity`\|`hive` | no | `none` | PR | Catalog **registration** of Delta tables. v1 implements `none` (path-based tables); other values are reserved (V-088). |

Delta maintenance in v1: checkpoints + expired-log cleanup are built-in (post-commit hooks);
OPTIMIZE and VACUUM are external (never delete data files — `System-Overview.md`).

### 9.5 `sink.iceberg.catalog` (Iceberg sink only)

Where Iceberg table schemas/metadata are registered. Exactly one of `glue` / `hive` / `rest`.
(Moved from the pipeline-level `catalog` on 2026-09-26; field semantics unchanged.)

```yaml
sink:
  type: iceberg
  iceberg:
    catalog:
      type: rest            # optional discriminator: glue | hive | rest
      glue: { ... }         # OR
      hive: { ... }         # OR
      rest: { ... }
```

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `type` | enum `glue`\|`hive`\|`rest` | no | inferred | PR | **NEW (vs draft).** Optional explicit discriminator, symmetric with `source.type`. If present it must match the single present sub-block (V-002); if absent it is inferred from the one present sub-block (0 or >1 present = error). |
| `glue` / `hive` / `rest` | object | one required (resolvable) | — | PR | §9.5.1–9.5.3. |
| `retry` | object | no | `{max_attempts: 5, base_backoff: 1s, max_backoff: 30s}` | R | Catalog-operation retry policy, separate from object storage (G-35): exponential backoff + jitter, then **DEGRADED** (alert via `cdc_pipeline_status`), with a circuit breaker so a down catalog is not hammered. V-112. Not part of the union; merges normally. |

#### 9.5.1 `sink.iceberg.catalog.glue` (valid only if `globals.cloud = aws`, V-028)

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `name` | string | yes | — | PR | Glue catalog id (AWS account id or registered catalog name). |
| `database` | string | no | `source.db` | PR | Glue database mapped to the source schema/db. |
| `strip_schema` | bool | no | `false` | PR | Glue table naming. `false`: `<schema>__<table>` (double underscore; collision-free). `true`: bare `<table>` — two source schemas with the same table name then **collide** and the pipeline is refused when its table set is resolved, naming the colliding tables (V-113). |

#### 9.5.2 `sink.iceberg.catalog.hive` (Thrift / SASL)

> **Rejected on the pinned crate (V-115, DA-CAT-6):** iceberg-rust's HMS catalog cannot commit. The
> block is specified for when it can; until then a pipeline configuring it fails validation.

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `url` | string | yes | — | PR | `thrift://host:9083` or bare `host:port`. Default port 9083. |
| `auth` | enum `none`\|`ldap`\|`kerberos` | no | `none` | PR | SASL mode. Mode change is structural (sub-blocks added/removed) — kept PR; see the credential-rotation note below §9.5.3. |
| `user` | string | conditional | — | R | For `ldap`: required unless `credentials_file` is given (V-040); mutually exclusive with it (V-065). Rotation applied via catalog-client reconnect (SASL is connection-bound). |
| `password` | string | conditional | — | R | For `ldap`: required unless `credentials_file` is given (V-040); mutually exclusive with it (V-065). Rotation applied via reconnect. |
| `credentials_file` | path | conditional | — | R | Alternative to `user`/`password` for `ldap` (§4.6): path to JSON `{user, password}`. Mutually exclusive with them (V-065). |
| `kerberos.principal` | string | conditional | — | R | Required for `kerberos` (V-041). Applied via reconnect. |
| `kerberos.keytab` | path | conditional | — | R | Required for `kerberos` (V-041). Re-read on reconnect — supports keytab rotation. |
| `kerberos.service_name` | string | no | `hive` | R | Kerberos service name on the HMS host. Applied via reconnect. |
| `tls.enabled` | bool | no | `false` | PR | Wrap Thrift in TLS. Recommended for `ldap` (else cleartext password, V-042). |
| `tls.ca` | PEM/path | no | — | PR | Custom CA for self-signed HMS certs. |
| `database` | string | no | `source.db` | PR | Hive database mapping (catalog target, not a credential). |

#### 9.5.3 `sink.iceberg.catalog.rest` (HTTP)

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `url` | string | yes | — | PR | Base URL (no trailing `/v1`). Endpoint/target change. |
| `warehouse` | string | no | — | PR | Logical warehouse id (Polaris, Lakekeeper, Unity, some Nessie). Catalog target, not a credential. |
| `prefix` | string | no | — | PR | URL path prefix before `/v1/` (e.g. `/iceberg` for HMS REST). Distinct from `sink.prefix`. |
| `auth` | enum `none`\|`bearer`\|`oauth2`\|`sigv4` | no | `none` | PR | HTTP auth mode. Mode change is structural — kept PR; see the credential-rotation note below. `sigv4` is **rejected on the pinned crate** (V-116, DA-CAT-7). |
| `token` | string | conditional | — | R | For `bearer`: required unless `token_file` is given (V-044); mutually exclusive with it (V-070). `Authorization: Bearer <token>`. Rotation applies to the next request. |
| `token_file` | path | conditional | — | R | Alternative to `token` for `bearer` (§4.6): path to a file whose raw contents are the token (not JSON). Mutually exclusive with `token` (V-070). Replacing the file's contents + reload rotates the bearer token without a process restart. |
| `oauth2.client_id` | string | conditional | — | R | For `oauth2`: required unless `oauth2.credentials_file` is given (V-045); mutually exclusive with it (V-066). Applied at the next token refresh (force a refresh on change). |
| `oauth2.client_secret` | string | conditional | — | R | For `oauth2`: required unless `oauth2.credentials_file` is given (V-045); mutually exclusive with it (V-066). Rotation applied at next token refresh. |
| `oauth2.credentials_file` | path | conditional | — | R | Alternative to `oauth2.client_id`/`client_secret` (§4.6): path to JSON `{client_id, client_secret}`. Mutually exclusive with them (V-066). `token_url` is always given explicitly (it is an endpoint, not a credential). |
| `oauth2.token_url` | string | conditional | — | R | Required for `oauth2` (V-045). **Point at an external IdP**, not `<url>/v1/oauth/tokens` (deprecated in Iceberg 1.6, removed in 2.0; V-048 warning). Change invalidates the current token and re-auths against the new endpoint. |
| `oauth2.scope` | string | no | — | R | Space-separated scopes (e.g. `catalog:read catalog:write`; Lakekeeper `lakekeeper`). Change forces a token re-fetch with the new scopes. |
| `sigv4.region` | string | conditional | — | R | Required for `sigv4` (V-046). Signing parameter; next request signs with the new value. |
| `sigv4.service` | string | conditional | — | R | Required for `sigv4` (V-046); `glue` or `execute-api`. `sigv4` requires `globals.cloud = aws` (V-047). Signing parameter; next request picks it up. |
| `tls.ca` | PEM/path | no | system store | PR | Custom CA bundle for internal/self-signed certs. |

> **Catalog credential-rotation note (R semantics for the auth fields above).** The catalog
> client is used only by the Iceberg Writer (schema load, metadata commit); rotating a
> credential never touches the source connection, the replication reader, or the accumulator,
> so the data path is uninterrupted and no pipeline restart is needed — hence **R**, not PR.
> The *mechanism* differs by protocol:
> - **REST (HTTP):** the new credential is applied on the next request (`bearer`) or the next
>   token refresh (`oauth2`); SigV4 `region`/`service` are signing parameters consumed
>   per-request. Truly zero-disruption. On `oauth2`/`token_url`/`scope` changes the writer
>   should invalidate the cached token and re-auth.
> - **Hive (Thrift/SASL):** authentication is negotiated at connection establishment and bound
>   to the transport, so a credential change cannot take effect on the live connection — it is
>   applied by a **transparent reconnect** of the catalog client. Source streaming and
>   buffering continue; back-pressure absorbs the brief commit pause.
>
> Changing the auth **mode** (`auth`) stays **PR**: it is structural (sub-blocks are added or
> removed), rare, and more error-prone, so a clean restart that surfaces misconfiguration is
> worth the heavier blast radius. The catalog *target* fields (`url`, `warehouse`, `prefix`,
> `database`) and `tls.*` also stay PR — they re-point or re-establish the connection rather
> than merely refresh credentials.

### 9.6 `state` (per-pipeline)

Optional override of where this pipeline's operational state (offsets, snapshot cursors,
snapshot status, per-pipeline schema snapshots) is stored. Defaults to the `sink` location.

```yaml
state:
  bucket: state-bucket
  prefix: pipeline-state
```

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `bucket` | string | conditional | `sink.bucket` | PR | If omitted, state co-locates with `sink` (under `.pipeline-state`, §10). Mandatory only if you split state from sink and it is not set in `defaults`. |
| `prefix` | string | no | pipeline `id` | PR | Prefixed by `defaults.state.prefix` when that is defined (§10). |

Relocating state on a running pipeline (PR) is dangerous (the new location has no history);
treat such a change as effectively a re-seed and review before applying.

### 9.7 `schema_evolution` (per-pipeline)

Sink-neutral schema-evolution policy (moved from `catalog.schema_evolution`, 2026-09-26). The sink
applies it within its declared capabilities: an operation the sink cannot perform halts the table
(`Schema_Change_Detection_and_Evolution.md` §7).

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `enabled` | bool | no | `true` | R | When disabled, or on an unsafe change, the changed table halts (log error); other tables continue (G-19b). |
| `rescan_interval` | Duration | no | `60m` | R | Idle-table rescan cadence (detection channel D only). `0` disables the idle rescan. Always applicable. |

> Detection and evolution are defined by `Schema_Change_Detection_and_Evolution.md` (the
> four-channel trigger→augment→diff model and the safe/unsafe DDL classification, §7.2 there) —
> this spec only carries the configuration surface. There is no `method` field: detection is the
> fixed four-channel model, not an operator choice. Source-side requirements for detection
> (e.g. SELECT on the source catalog) are operational start-time preconditions, not config rules
> — see §11.2.

---

## 10. Storage path construction

Path segments are joined with `/`; empty segments are skipped; a single separator is used
regardless of leading/trailing slashes in any segment. For **Azure**, `bucket` means
`container`.

**Per-pipeline data & metadata** (under `sink`):

```
<sink.bucket>/<sink.prefix>/<schema>/<table>/           ← table root (common to all sinks)
    sink.type = iceberg:  data/   ← Parquet data (+ delete) files
                          meta/   ← Iceberg metadata
    sink.type = delta:    _delta_log/          ← Delta transaction log + checkpoints
                          part-*.parquet       ← data files (partition dirs when partitioned)
```
A table root that already holds the **other** format's metadata is refused at pipeline start
(no silent format mixing).
`sink.prefix` defaults to the pipeline `id`. The `<schema>` segment is the PostgreSQL schema;
for **MySQL** — which has no schema layer and uses bare table names in config (§9.3) — this
segment is filled internally with the database (`source.db`), so the layout stays uniform across
engines and consistent with `Data-Flow.md`'s `TableRef { schema, name }` and
`snapshots/<schema>.<table>.json` (for MySQL, `<schema>` = `source.db`).

**Per-pipeline operational state** (under `state`, default = sink):

```
# default (state co-located with sink):
<sink.bucket>/<sink.prefix>/.pipeline-state/...

# explicit pipeline state location:
<state.bucket>/[<defaults.state.prefix>/]<state.prefix>/...
```
The location is already scoped to a single pipeline (`sink.prefix` / `state.prefix` default to
the pipeline `id`, which is unique per V-056), so the internal layout from `Data-Flow.md`
(`pipeline-state.json`, `replication/slot.json` + `gtid.json`, `snapshots/<schema>.<table>.json`)
sits **directly** under this root — no `pipelines/<id>/` namespacing segment. The
`.pipeline-state/` marker in the co-located case is what separates state from the table data
that shares `sink.prefix`; in the explicit case the entire location is pipeline state, so no
marker is needed.

> **Reconcile with `Data-Flow.md`:** its state-layout diagram still roots per-pipeline state at
> `<state_prefix>/pipelines/<pipeline-id>/` under a prefix it labels "global." That nesting
> belongs to a shared-prefix model; under the per-pipeline `state`/`sink` location used here
> (and the `globals.state` boundary in §6.2, which reserves the global prefix for cross-pipeline
> data only), the `pipelines/<id>/` segment is the same redundancy removed above. Data-Flow's
> diagram should be re-rooted at the per-pipeline state location and drop that segment.

**Application-level state** (under `globals.state`):

```
<globals.state.bucket>/<globals.state.prefix>/...      ← config generation, membership, leases
```

> **Clarification (vs draft):** prefix joining, slash normalization, and the
> `defaults.state.prefix` + pipeline `state.prefix` composition were described only in
> scattered comments. This is the single normative description.

---

## 11. Validation & resolution model

Loading (or reloading) a config runs as an ordered pipeline; each phase must fully succeed
before the next begins. All of it is **config-time** work, distinct from pipeline runtime (§1.4).

**Phase 0 — Parse (§2).** Strict YAML with `deny_unknown_fields`. Syntax errors and unknown keys
stop here.

**Phase 1 — Structural validation.** The §13.1–13.8 rules, evaluated on the parsed config with
references **unresolved** — `${VAR}`, file paths, and `credentials_file` paths are still literal
text. This phase touches neither the environment nor the filesystem. It covers structure and
mutual exclusion (including `credentials_file` vs explicit credentials — V-065/V-066), required
presence, types, value domains, cross-field constraints, and identity/naming.

**Phase 2 — Resolution & materialization (§13.9).** Every reference is replaced by its actual
value, and the result is checked for **presence** (non-empty) and **content validity**:

- `${VAR}` substitution (§4.3); an unset variable with no default is an error (V-063).
- `credentials_file` (§4.6): resolve the (possibly env-var) path, read the file, parse JSON,
  extract the required fields. A missing/unreadable file, empty content, invalid JSON, or
  missing/empty required field is an error (V-067).
- `token_file` (§4.6): resolve the path, read the file; the raw contents (trailing whitespace
  trimmed) are the bearer token and must be non-empty (V-071).
- TLS material (`cert`/`key`/`ca`, §4.4): resolve inline/path/env and verify the result is
  non-empty, valid PEM (V-068).
- Any other referenced file (e.g. `kerberos.keytab`) must resolve to an existing, readable,
  non-empty file (V-069).
- Any required value that resolves to empty is an error.

**Phase 3 — Resolved config.** The output is a fully **resolved** configuration in which every
reference has been replaced by its materialized value. This is the only representation the
runtime uses (§11.1).

**Blast radius & severity (per §1.4) apply across every phase.** Any **error** in any phase
rejects the **entire** config — on startup the process refuses to start; on reload the new config
is rejected and the previous *resolved* config stays fully in effect. There is no partial or
per-pipeline application. **Warnings** never block. The whole pipeline runs identically on initial
load and on reload, and is exposed (without applying) by the validation API endpoint — which runs
inside the worker, so it performs both Phase 1 and Phase 2.

Other notes:
- Errors and warnings carry an ID (V-nnn, §13).
- **Runtime failures are out of validation scope.** Conditions that cannot be checked at config
  time — unreachable host, *wrong* (but well-formed) credentials, pre-existing slot, unreachable
  catalog, a PK-less table found via glob — are isolated per pipeline/table at runtime and never
  block other pipelines. (Resolution checks that a credential is *present and well-formed*, not
  that it is *accepted* by the server.)
- On reload, a change to any **PROC** field is rejected (V-061) with a message naming the field;
  the previous config stays in effect (a reload-time instance of whole-config rejection).

### 11.1 Resolved config at runtime

Runtime components never see unresolved references. After Phase 3 the system holds a resolved
config — environment variables substituted, credential files read into in-memory secrets, TLS
material parsed into key/cert objects, paths made concrete — and **all reconciliation and pipeline
execution operate on this resolved form**, not on the raw file. Consequences:

- **The reconcile diff is computed on resolved values.** Replacing a `credentials_file`'s contents
  (or any referenced secret/cert) and triggering a reload is therefore *detected* as a change and
  applied per the field's mutability class. This is the rotation path that environment-variable
  secrets cannot offer: an env-var change requires a full process restart (PROC), whereas a
  file-content change is picked up by a reload (R or PR per the field).
- **Resolved secrets live in memory only** — never written to local disk (§2), and excluded from
  logs, the reconcile diff output, and the validation endpoint's response. Diffs over secret
  fields report *changed / unchanged*, never the values.
- **Reload re-runs the whole pipeline** (Phase 0 → 1 → 2 → 3); only if it fully succeeds does the
  diff/reconcile in §12 proceed, against the new resolved config.

### 11.2 Operational (start-time) source preconditions

These are **not** config-time validation rules — they require a live source connection and so are
checked when a pipeline starts/connects, and a failure is an ordinary **runtime** error handled
per §1.4 (the affected pipeline halts — or warns — while other pipelines keep running). They have
no V-IDs. They are listed here for completeness; the authority is
`Schema_Change_Detection_and_Evolution.md` §9, which the detection model (channels A/C/D) depends
on.

| Engine | Requirement | Severity (at start) | Rationale |
|---|---|---|---|
| Both | Configured source credentials hold **SELECT on `information_schema` / system catalogs** (over a regular, non-replication connection) | error → halt pipeline | Detection channels A/C/D query the source catalog; the replication credential is reused — no separate read role. |
| PostgreSQL | **`REPLICA IDENTITY` ≠ `NOTHING`** on tracked tables | error → halt table | Delete/update keying (pre-existing requirement). |
| MySQL | **`binlog_format = ROW`** | error → halt pipeline | Granular row-level change capture. |
| MySQL | **`binlog_row_image = FULL`** | error → halt pipeline | Complete before-images / equality-delete keys. |
| MySQL | **`binlog_row_metadata = FULL`** | warning → continue | Column names/PK/ENUM strings in `TABLE_MAP`; if absent, names fall back to the catalog query (channel C). |
| MySQL | **`REPLICATION CLIENT` / `REPLICATION SLAVE`** privilege | error → halt pipeline | Binlog dump access. |
| Both | Each table has an **identifier**: an inferred PK, or a `tables.primary_key` override whose columns exist and are top-level, primitive, NOT NULL, non-floating | error → halt table (explicit table) / warning + skip (glob-matched PK-less table) | Upsert/delete keying (G-13, G-30). |
| PostgreSQL | Identifier columns ⊆ **REPLICA IDENTITY** columns | error → halt table, naming the missing columns | Before-image must carry the old key (`Ghost_Row…` §2). |
| Both | `tables.partitioning` columns exist; temporal transforms target a date/time column | error → halt table | Partition spec validity (G-24). |

Because these are runtime checks, they sit outside the all-or-nothing config-validation contract:
a privilege or server-setting problem on one pipeline's source does not reject the config or stop
other pipelines.

**Sink (start-time) preconditions** — same runtime class (no V-IDs; the affected pipeline is refused,
others keep running). Authority: `Data-Flow.md` Phase 0 step 4 and
`research/Multi_Sink_Architecture_and_Delta_Lake.md` §6.4.

| Sink | Requirement | Severity (at start) | Rationale |
|---|---|---|---|
| All | **Capability assertion** — the sink declares every capability the pipeline's config needs (`write_mode`, partition transforms, logical types of the resolved tables) | error → refuse pipeline, naming the capability | Fail fast instead of a first-write surprise. Example: `sink.type: iceberg` + `write_mode: mirror` is refused until the pinned iceberg-rust has a delete-capable commit (DA-SINK-1i). |
| All | Pipeline state's recorded `sink_type` equals `sink.type` | error → refuse pipeline | Positions and markers are sink-specific; a changed `sink.type` must re-seed. |
| All | No table root holds the **other** format's metadata (`metadata/` vs `_delta_log/`) | error → halt table | No silent format mixing. |
| All | Storage supports **atomic create-if-absent** (conditional PUT) on the sink location | error → refuse pipeline | Iceberg file-based commits and every Delta commit rely on it (same capability as the storage lease). |

---

## 12. Reload & reconciliation behavior

### 12.1 Trigger & flow

`POST /v1/config/reload` → worker re-reads the file → parse (strict) → **structural validation
(Phase 1) → resolution (Phase 2) → resolved config (Phase 3)** per §11 → diff the new resolved
config against the running resolved config → reconcile. Because the diff is over **resolved**
values (§11.1), a change limited to a `credentials_file`'s contents or any other referenced
secret/cert is detected here even though the config text is unchanged. Cross-worker propagation of
the reload (so peers also re-read) is a **design item (G-2 / G-17)** and is out of scope for this
schema; this section defines only what one worker does with the new file.

### 12.2 Diff granularity

The diff is computed per pipeline (keyed by `id`) and per global field. For each changed
field the engine looks up its mutability class (§1.2) and the change set resolves to the
highest class touched.

### 12.3 Pipeline add / remove / modify

- **Added** (`id` new): start it (Phase 0, `Data-Flow.md`), snapshot per `snapshot_mode`.
- **Removed** (`id` gone): stop, drain/flush, drop DB resources if `auto_cleanup`, retain
  Iceberg artifacts (never deleted, per `System-Overview.md`). Orphan-cleanup of resources for
  a tombstoned `id` whose owner was offline is a design item (gap 2.4).
- **Renamed** (key changed, `id` same): pure rename; the `_pipeline` label changes, no
  resource churn.
- **`id` changed**: delete-and-recreate (drop old resources per `auto_cleanup`, start fresh).
- **Modified, only R fields**: applied in place.
- **Modified, any PR field**: restart that pipeline per §12.4.
- **Modified, any PROC field**: reload rejected (V-061).

### 12.4 Pipeline restart policy (in-flight batches)

The default for a restart (PR change, explicit `restart`/`stop`, or `id` change) is
**discard-and-replay**: drop unflushed in-memory batches and replay from the last confirmed
offset (the Phase-4 exactly-once guard deduplicates). A graceful-flush option (flush before
restart) is a planned flag on the restart path (gap 2.2). The replay window is bounded by the
checkpoint interval (flush-driven, per `Data-Flow.md`).

> The detailed pipeline **state machine** (RUNNING/PAUSED/STOPPED, which transitions flush /
> release leases / persist to cloud) is a design item (G-NEW-15) and not fixed by this schema.

---

## 13. Master validation table (G-1f)

The complete, consolidated rule set.

**How to read the action:** per §1.4, the **action is governed by Severity, not by locus** — any
`error` rejects the **entire** config (startup: refuse to start; reload: keep the previous
config), and any `warning` loads and logs. There is no partial or per-pipeline application of a
statically-invalid config.

The **Locus** column says *where* the problem is, so the validation endpoint can group its
report (`config`-level vs a named `pipeline`) and point the operator to the right place. A
`pipeline`-locus **error still rejects the whole config** — locus is diagnostic, not a blast
radius. `reload` locus marks a rule that can only fire on reload.

### 13.1 Structure & mutual exclusion

| ID | Path / Scope | Rule | Severity | Locus |
|---|---|---|---|---|
| V-001 | `source` | Exactly one engine block (`postgres` xor `mysql`) after merge, matching `type`. | error | pipeline |
| V-002 | `sink.iceberg.catalog` | Exactly one sub-block (`glue`/`hive`/`rest`); if `type` is set it must equal the present block. | error | pipeline |
| V-084 | `sink.{iceberg,delta}` | Exactly the format block matching `sink.type` after merge; the foreign block is forbidden. | error | pipeline |
| V-003 | `source.{postgres,mysql}` | Engine block present must match `type`; the foreign engine block is forbidden. | error | pipeline |
| V-004 | top | `pipelines` key present (may be `{}`). | error | config |
| V-005 | top | `globals` present. | error | config |
| V-065 | `globals.api`, `source`, `sink.iceberg.catalog.hive` | `credentials_file` is mutually exclusive with `user`/`password` in the same block — providing both (any of `user`/`password` alongside `credentials_file`) is an error. | error | config / pipeline |
| V-066 | `sink.iceberg.catalog.rest.oauth2` | `oauth2.credentials_file` is mutually exclusive with `oauth2.client_id`/`client_secret` — providing both is an error. | error | pipeline |
| V-070 | `sink.iceberg.catalog.rest` | `token_file` is mutually exclusive with `token` — providing both is an error. | error | pipeline |

### 13.2 Requiredness / presence

| ID | Path / Scope | Rule | Severity | Locus |
|---|---|---|---|---|
| V-006 | `globals.state.bucket` | Present. | error | config |
| V-007 | `tables.include` | Present and non-empty. | error | pipeline |
| V-008 | `source.host` | Resolvable (pipeline or defaults). | error | pipeline |
| V-009 | `source.db` | Resolvable. | error | pipeline |
| V-010 | `source` credentials | Resolvable: either `user`+`password`, or `credentials_file` (exactly one — V-065). | error | pipeline |
| V-011 | `source.postgres.{publication,slot}` | Present when `type=postgres`. | error | pipeline |
| V-012 | `sink.bucket` | Resolvable. | error | pipeline |
| V-013 | `sink.iceberg.catalog` | When `sink.type = iceberg`: a catalog block resolvable (pipeline or defaults). | error | pipeline |
| V-091 | `sink.type` | Present after merge (pipeline or `defaults`). **No implicit default.** | error | pipeline |
| V-014 | `config_version` | Present and supported by this binary. | error | config |

### 13.3 Value domain

| ID | Path / Scope | Rule | Severity | Locus |
|---|---|---|---|---|
| V-015 | `globals.cloud` | ∈ {aws, google, azure}. | error | config |
| V-016 | `globals.api.auth` | ∈ {none, basic, jwt, oauth2}. | error | config |
| V-017 | `globals.log.level` | Valid level / EnvFilter syntax. | error | config |
| V-018 | `globals.log.format` | ∈ {json, compact, pretty}. | error | config |
| V-019 | `source.type` | ∈ {postgres, mysql}. | error | pipeline |
| V-020 | `source.ssl_mode` | ∈ {disable, allow, prefer, require, verify-ca, verify-full}. | error | pipeline |
| V-021 | `source.mysql.replication_type` | ∈ {binlog, gtid}. | error | pipeline |
| V-022 | `tables.snapshots.mode` (and overrides values) | ∈ {never, initial, always, dump_always, dump_initial}. | error | pipeline |
| V-023 | `sink.iceberg.strategy` | ∈ {mor, cow}. | error | pipeline |
| V-083 | `sink.type` | ∈ {iceberg, delta} **and** compiled into this binary (Cargo feature `sink-iceberg` / `sink-delta`). | error | pipeline |
| V-089 | `sink.write_mode` | ∈ {mirror, changelog}; `changelog` is reserved → `error` in v1 (D-MS.5). | error | pipeline |
| V-092 | `sink.delta.column_mapping` | ∈ {none, name}; `name` → `error` in v1 (delta-rs refuses ADD COLUMN under column mapping; R-1). | error | pipeline |
| V-088 | `sink.delta.catalog.type` | ∈ {none, glue, unity, hive}; anything but `none` is reserved → `error` in v1 (D-MS.12). | error | pipeline |
| V-025 | `sink.iceberg.catalog.hive.auth` / `sink.iceberg.catalog.rest.auth` | hive ∈ {none,ldap,kerberos}; rest ∈ {none,bearer,oauth2,sigv4}. | error | pipeline |
| V-026 | `globals.cluster.tokens` | 1 ≤ tokens ≤ 4096. Outside 64–256 → also `warning`. | error / warning | config |
| V-027 | `tables.snapshots.parallelism` | ≥ 1. | error | pipeline |
| V-072 | `globals.cluster.lease_safety_margin` | 0 ≤ margin ≤ 0.5. Outside 0.05–0.30 → also `warning`. Fraction of `lease_duration` held back as the renewal safety-deadline `guard` (Clustering doc §19.3/§19.6, D-8). | error / warning | config |
| V-074 | `globals.cluster.max_concurrent_handoffs` / `globals.cluster.max_concurrent_takeovers` | ≥ 1 (per-node concurrency caps; local resource knobs, not cluster-wide-consistent). Clustering doc §8.5/§20.2 (G-9, D-10). | error | config |
| V-075 | `globals.cluster.lock_backend` | ∈ {storage, kubernetes}. `kubernetes` is reserved / not yet implemented → `error` in v1. Clustering doc §5.6 (D-2). | error | config |
| V-080 | `source.toast_strategy` | ∈ {carry_forward, re_query} (PostgreSQL only; ignored for MySQL). `research/Type_Mapping.md`. | error | pipeline |
| V-081 | `source.mysql.tinyint1_detection` | ∈ {information_schema, none}. | error | pipeline |
| V-082 | `tables.numeric_overflow_action` | ∈ {string, error}. | error | pipeline |
| V-095 | `globals.cluster.discovery` | ∈ {static, dns}. | error | config |
| V-097 | `globals.tracing.sample_ratio` | ∈ [0, 1]. | error | config |
| V-101 | `globals.azure.account_name` | Valid storage-account name (3–24 chars, lowercase letters and digits). | error | config |
| V-102 | `source.postgres.slot_management` / `slot_on_orphan` | slot_management ∈ {managed, require_existing}; slot_on_orphan ∈ {halt, adopt_resnapshot}. | error | pipeline |
| V-104 | `source.postgres.max_wal_lag_bytes` | ≥ 64 MiB when set. | error | pipeline |
| V-105 | `source.mysql.binlog_purge_action` | ∈ {error, re_snapshot}. | error | pipeline |
| V-107 | `tables.partitioning` fields | `transform` ∈ {identity, bucket, truncate, year, month, day, hour}; `bucket` requires `n ≥ 1`; `truncate` requires `width ≥ 1`. | error | pipeline |
| V-108 | `tables.snapshots.chunk_size` | ≥ 1. > 1,000,000 → also `warning` (long chunk transactions, large memory). | error / warning | pipeline |
| V-109 | `sink.max_transaction_batch_rows` | ≥ 1 when set. < 1000 → also `warning` (commit amplification). | error / warning | pipeline |
| V-110 | `sink.max_transaction_buffer_bytes` | ≥ 16 MiB. | error | pipeline |
| V-111 | `sink.buffer_capacity_rows` | ≥ 1. | error | pipeline |
| V-112 | `sink.iceberg.catalog.retry` | `max_attempts ≥ 1`; `base_backoff ≤ max_backoff`. | error | pipeline |
| V-114 | `sink.delta.timestamp_ntz_as` | ∈ {timestamp_ntz, timestamp, string}. | error | pipeline |
| V-115 | `sink.iceberg.catalog.type` / `sink.iceberg.catalog.hive` | `hive` → `error`: the pinned iceberg-rust HMS catalog cannot commit (`update_table` is `FeatureUnsupported`; DA-CAT-6). Lifted when a pinned release implements it and the iceberg-capability probe passes against a real HMS. | error | pipeline |
| V-116 | `sink.iceberg.catalog.rest.auth` | `sigv4` → `error`: the pinned iceberg-rust REST client has no SigV4 signing (DA-CAT-7; upstream #3082/#3092). `none`/`bearer`/`oauth2` unaffected; on AWS use `catalog.glue`. Lifted when a pinned release carries SigV4 and it is probed against the Glue or S3 Tables REST endpoint. | error | pipeline |

### 13.4 Cross-field / conditional

| ID | Path / Scope | Rule | Severity | Locus |
|---|---|---|---|---|
| V-028 | `sink.iceberg.catalog.glue` | Requires `globals.cloud = aws`. | error | pipeline |
| V-029 | `sink.iceberg.catalog.rest.auth = sigv4` | Requires `globals.cloud = aws`. | error | pipeline |
| V-030 | `source.ssl_mode = allow` | Invalid for MySQL. | error | pipeline |
| V-073 | `globals.cluster.lease_renewal_interval` | Must be ≤ `lease_duration / 3` (the holder must get ≥2 renew attempts inside the renewal safety deadline). **Hard error** — promoted from the previously-advisory guidance (Clustering doc §18.2/§19.6, D-8). | error | config |
| V-033 | `tables.snapshots.overrides` keys | Must be full `schema.table`; globs forbidden. | error | pipeline |
| V-034 | `tables.snapshots.overrides` keys | Key not in the resolved include/exclude set. | warning | pipeline |
| V-035 | `sink.flush` | All of `seconds`/`rows`/`bytes` null/disabled. | error | pipeline |
| V-036 | `sink.flush.seconds = null` | With at least one other trigger set (time-based disabled). | warning | pipeline |
| V-037 | `sink.flush.seconds` | 0 < seconds < 5 (write amplification). | warning | pipeline |
| V-038 | `tables.snapshots.parallelism` | > 4 (connection budget). | warning | pipeline |
| V-064 | `tables.include`/`exclude` globs and `snapshots.overrides` keys, when `source.type = mysql` | Must be bare table names — no `.` namespace prefix (the database is fixed by `source.db`). | error | pipeline |
| V-085 | `sink.iceberg.strategy` | Only meaningful with `write_mode: mirror`. | error | pipeline |
| V-086 | `tables.partitioning` transforms | Each transform supported by the pipeline's sink (Delta v1: `identity` only). Active once `tables.partitioning` is promoted from §16. | error | pipeline |
| V-087 | `tables.column_type_overrides` values | Logical type mappable by the pipeline's sink. | error | pipeline |
| V-093 | `sink.delta.table_properties` keys | Only allow-listed `delta.*` keys; `delta.columnMapping.mode` / `delta.enableDeletionVectors` / `delta.appendOnly` are managed by the sink and forbidden here. | error | pipeline |
| V-096 | `globals.cluster.discovery = dns` | Requires `discovery_dns_name`. | error | config |
| V-098 | `globals.tracing.sample_ratio > 0` without `otlp_endpoint` | Sampling configured but tracing disabled. | warning | config |
| V-103 | `source.postgres.slot_management = require_existing` | Requires `auto_cleanup = false` (the app must not drop a slot it did not create). | error | pipeline |
| V-106 | `tables.primary_key` / `tables.partitioning` keys | Same key form as `snapshots.overrides` (full `schema.table` / MySQL bare name, no globs — V-033/V-064); column lists non-empty. A key not in the resolved table set → `warning` (as V-034). | error / warning | pipeline |
| V-113 | `sink.iceberg.catalog.glue.strip_schema = true` | Two resolved tables share a bare name across schemas. Evaluated when the table set is resolved (start / table-set change); refuses the pipeline naming the collision. | error | pipeline (resolution) |
| V-077 | `source.toast_strategy = re_query` with `tables.snapshots.mode = always` | `re_query` on high-volume snapshots may saturate the control connection pool. | warning | pipeline |

### 13.5 Catalog authentication (the 9 rules from `Catalog_Auth_Research.md`)

| ID | Path / Scope | Rule | Severity | Locus |
|---|---|---|---|---|
| V-040 | `sink.iceberg.catalog.hive.auth = ldap` | Requires `user`+`password`, or `credentials_file` (exactly one — V-065). | error | pipeline |
| V-041 | `sink.iceberg.catalog.hive.auth = kerberos` | Requires `kerberos.principal` + `kerberos.keytab`. | error | pipeline |
| V-042 | `sink.iceberg.catalog.hive.auth = ldap` without `tls.enabled` | Password in cleartext. | warning | pipeline |
| V-043 | `sink.iceberg.catalog.hive.tls.enabled = true` w/ private CA but no `tls.ca` | May fail cert validation against system store. | warning | pipeline |
| V-044 | `sink.iceberg.catalog.rest.auth = bearer` | Requires `token` or `token_file` (exactly one — V-070). | error | pipeline |
| V-045 | `sink.iceberg.catalog.rest.auth = oauth2` | Requires `token_url`, and either `oauth2.client_id`+`client_secret` or `oauth2.credentials_file` (exactly one — V-066). | error | pipeline |
| V-046 | `sink.iceberg.catalog.rest.auth = sigv4` | Requires `sigv4.region` + `service`. | error | pipeline |
| V-047 | `sink.iceberg.catalog.rest.auth = sigv4` | Requires `globals.cloud = aws`. (Same constraint as V-029.) | error | pipeline |
| V-048 | `sink.iceberg.catalog.rest.auth = oauth2`, `token_url` = `<url>/v1/oauth/tokens` | Deprecated built-in endpoint (Iceberg ≥1.6, removed 2.0). | warning | pipeline |

### 13.6 Management-API security

| ID | Path / Scope | Rule | Severity | Locus |
|---|---|---|---|---|
| V-049 | `globals.api.auth = basic` | Requires `user`+`password`, or `credentials_file` (exactly one — V-065). | error | config |
| V-050 | `globals.api.auth = jwt` | Requires `jwt.issuer` + `jwt.jwks_url`. | error | config |
| V-051 | `globals.api.auth = oauth2` | Requires `oauth2.issuer` + (`jwks_url` or `discovery_url`). | error | config |
| V-052 | `globals.api.auth ≠ none` without `tls.enabled` | Credentials/tokens in cleartext unless TLS terminated upstream. | warning | config |
| V-099 | `globals.api.role_mapping` | Keys ∈ {viewer, operator, admin}; values are lists of strings. | error | config |
| V-100 | `globals.api.role_mapping` with `auth ∈ {none, basic}` | Ignored — roles come from the open mode / single basic principal. | warning | config |
| V-053 | `globals.api.tls.enabled = true` | Requires `cert` + `key`. | error | config |
| V-054 | `globals.cluster.memberlist.tls.enabled = true` | Requires `cert` + `key`. | error | config |

### 13.7 Identity / naming

| ID | Path / Scope | Rule | Severity | Locus |
|---|---|---|---|---|
| V-055 | `pipeline.id` | Matches `^[a-z0-9][a-z0-9_-]{0,62}$`, ≤ 63 chars. | error | pipeline |
| V-056 | `pipeline.id` | Unique across all pipelines. | error | config |
| V-057 | pipeline name (map key) | Matches `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`. | error | pipeline |
| V-058 | `source.postgres.slot` | Matches `^[a-z_][a-z0-9_]{0,62}$`; unique per source host+db across pipelines. | error | config |
| V-059 | `source.postgres.publication` | Valid identifier; uniqueness per source DB recommended. | warning | pipeline |
| V-060 | `defaults` | Must not set per-pipeline-unique resource names (`postgres.publication`, `postgres.slot`, `id`). | error | config |

### 13.8 Reload-time & cluster

| ID | Path / Scope | Rule | Severity | Locus |
|---|---|---|---|---|
| V-061 | any PROC-class field | Changed during a live reload. | error | reload |
| V-062 | `globals.cluster.tokens` | Differs from the cluster-established value in `globals.state`. | error | config (startup) |
| V-076 | `globals.cluster.lock_backend` | Differs from the cluster-established value in the `globals.state` anchor. | error | config (startup) |
| V-094 | `globals.cluster.lease_duration` | Differs from the cluster-established value in the `globals.state` anchor (`lease_duration_ms`). | error | config (startup) |

### 13.9 Resolution / materialization (Phase 2 — §11)

These fire during the resolution phase, after every reference is dereferenced. They check that
referenced values are **present and well-formed** — not that a server will *accept* them (that is
a runtime concern, §1.4).

| ID | Path / Scope | Rule | Severity | Locus |
|---|---|---|---|---|
| V-063 | any `${VAR}` | Unset with no `:-default`. | error | config |
| V-067 | any `credentials_file` (incl. `oauth2.credentials_file`) | Resolved path exists and is readable; content is non-empty, valid JSON, and contains the required non-empty fields (`user`+`password`, or `client_id`+`client_secret`). | error | config / pipeline |
| V-071 | `sink.iceberg.catalog.rest.token_file` | Resolved path exists and is readable; content (the raw token, trailing whitespace trimmed) is non-empty. | error | pipeline |
| V-068 | TLS material (`cert`/`key`/`ca` across `globals.api`, `memberlist`, `source.ssl_*`, `sink.iceberg.catalog.*`) | Resolved value (inline / file / env) is non-empty and parses as valid PEM (and a private key where a key is expected). | error | config / pipeline |
| V-069 | any other referenced file (e.g. `sink.iceberg.catalog.hive.kerberos.keytab`) | Resolved path exists, is readable, and is non-empty. | error | pipeline |
| V-078 | `tables.column_type_overrides` values | Each value parses in the sink-neutral logical type grammar (§9.3); else error `"invalid logical type <value> in column_type_overrides for <table>.<column>"`. | error | pipeline |
| V-079 | `tables.numeric_overflow_action = error` with any source `numeric` column lacking declared precision | Such a column may halt the table unexpectedly under `error`; consider a `column_type_overrides` entry. (Emitted at schema resolution.) | warning | pipeline |

> **Reconciliation note (G-NEW-21).** An earlier Type_Mapping draft proposed a warning tying
> `tinyint1_detection` to `schema_evolution.method: events`. That field was **removed** (detection is
> the fixed four-channel model), and the catalog query is always available via channel C, so the
> warning is **dropped** — `tinyint1_as_bool` works regardless of detection method.

---

## 14. Complete annotated example

A full, valid configuration exercising every section. All values shown are either the
documented defaults or representative examples.

```yaml
config_version: 1

globals:
  cloud: aws                                   # aws (default) | google | azure
  state:
    bucket: ${ACCOUNT_ID}-cdc-app-state
    prefix: cdc-app
  api:
    address: 0.0.0.0:8080
    auth: jwt
    jwt:
      issuer: https://idp.example.com/realms/cdc
      audience: cdc-management-api
      jwks_url: https://idp.example.com/realms/cdc/protocol/openid-connect/certs
      roles_claim: realm_access.roles
    tls:
      enabled: true
      cert: /etc/cdc/tls/api.crt
      key: ${API_TLS_KEY}
  log:
    level: info                                # rust module syntax allowed: info,rustcdc::sink=debug
    format: json
  cluster:
    tokens: 128                                # must match cluster-wide
    lease_duration: 15s                        # must match cluster-wide (anchored, V-094)
    lease_renewal_interval: 5s                 # ≤ lease_duration / 3 (V-073)
    discovery: dns
    discovery_dns_name: cdc-headless.svc.cluster.local
    memberlist:
      join_members:
        - cdc-headless.svc.cluster.local:7946
      bind_port: 7946
      advertise_port: 7946
      gossip_interval: 200ms
      probe_interval: 1s
      probe_timeout: 500ms
      push_pull_interval: 30s
      dead_node_reclaim_time: 30s
      compression_enabled: true
      tls:
        enabled: true
        cert: /etc/cdc/tls/gossip.crt
        key: ${GOSSIP_TLS_KEY}
        ca: /etc/cdc/tls/gossip-ca.crt
  tracing:
    otlp_endpoint: http://otel-collector.observability:4317
    sample_ratio: 0.05

defaults:
  source:
    type: postgres
    port: 5432
    ssl_mode: require
    timeouts: { connect: 10s, command: 30s, snapshot: 6h }
    postgres:
      auto_cleanup: true
      heartbeat: 60s
  tables:
    snapshots: { mode: never, parallelism: 1 }
  schema_evolution: { enabled: true, rescan_interval: 60m }
  sink:
    type: iceberg                              # REQUIRED somewhere (V-091); set once here
    write_mode: mirror
    flush: { seconds: 60, rows: 100000, bytes: 512MiB }
    iceberg:
      strategy: mor
      catalog:
        rest:
          url: https://catalog.example.com
          warehouse: analytics
          auth: oauth2
          oauth2:
            client_id: ${CATALOG_CLIENT_ID}      # or: credentials_file: ${CATALOG_OAUTH_CREDS_FILE} (JSON {client_id, client_secret}) — V-066
            client_secret: ${CATALOG_CLIENT_SECRET}
            token_url: https://idp.example.com/realms/cdc/protocol/openid-connect/token
            scope: catalog:read catalog:write
  state:
    prefix: pipeline-state

pipelines:
  orders-pg:
    id: orders_pg
    source:
      host: pg-orders.example.com
      db: orders
      user: cdc
      password: ${ORDERS_DB_PASSWORD}
      postgres:
        publication: orders_pub
        slot: orders_slot
    tables:
      include: [ public.* ]
      exclude: [ public.temp_* ]
      snapshots:
        mode: initial
        parallelism: 2
        overrides: { public.orders: always }
      partitioning:
        public.orders: [ { column: _ts, transform: day } ]
    sink:
      bucket: ${ACCOUNT_ID}-lake
      prefix: orders_pg
      flush: { rows: 50000 }                   # merges over defaults (seconds:60, bytes:512MiB kept)

  events-mysql:
    id: events_mysql
    source:
      type: mysql                              # drops the defaulted postgres block (union rule §8.4)
      host: mysql-events.example.com
      port: 3306
      db: events
      credentials_file: ${EVENTS_DB_CREDS_FILE}  # JSON {user, password}; mutually exclusive with user/password (V-065); rotatable in place
      ssl_mode: require
      mysql:
        replication_type: gtid
    tables:
      include: [ orders, payment_* ]           # MySQL: bare table names (db = source.db = events); a prefix would violate V-064
    schema_evolution: { enabled: true }       # rescan_interval merges from defaults (60m)
    sink:
      type: delta                              # drops the defaulted iceberg block (union rule §8.4)
      bucket: ${ACCOUNT_ID}-lake
      delta:
        checkpoint_interval: 10
        log_retention: 30d
        timestamp_ntz_as: timestamp_ntz          # default; `timestamp` keeps reader v1 (DA-MS-8)
        catalog: { type: none }                # path-based Delta tables (v1)
```

---

## 15. Changes from the `Configuration_file_model.md` draft

Every deviation, so nothing is silent:

1. **Added** top-level `config_version` (mandatory).
2. **Strict parsing** — unknown keys are now errors (`deny_unknown_fields`).
3. **snake_case standardization** — `ssl-mode/ssl-cert/ssl-key/ssl-ca` → `ssl_mode/…`.
4. **Byte-size units** — size fields accept `512MiB`-style suffixes; fixed the `512000000`
   "= 512Mb" inconsistency; default for `flush.bytes` documented as `512MiB`.
5. **Env-var grammar** — defined `${VAR}`, `${VAR:-default}`, `$$` escape, undefined-error,
   post-parse-then-coerce ordering.
6. **Merge semantics generalized** — explicit rules for scalars/maps (merge), lists (replace),
   and the `source` engine-block union (drop sibling on type switch); previously only
   `catalog` was specified.
7. **`catalog.type` discriminator** added (optional, inferred if absent), symmetric with
   `source.type`.
8. **Mutability classes** (R / PR / PROC) assigned to every field for the reload engine.
9. **`globals.api.jwt` / `oauth2` verification sub-blocks** added (issuer/audience/jwks_url/
   roles_claim). The RBAC matrix remains a design item.
10. **Flush tri-state** clarified (absent vs null vs value) with the `double_option`
    implementation note.
11. **Identity constraints** — `pipeline.id`, pipeline-name, and PG `slot`/`publication`
    charset + uniqueness rules made explicit.
12. **Path construction** consolidated into one normative section (§10).
13. **Validation rules consolidated** into the master table (§13) — including the 9 catalog
    rules and several new cross-field rules (V-030 ssl-allow-on-MySQL, V-052 auth-without-TLS, etc.).
14. **Defaults restriction** — `defaults` may not set unique resource names (V-060).
15. **MySQL table patterns are bare names** — `tables.include`/`exclude` globs and
    `snapshots.overrides` keys for a MySQL source carry no schema/database prefix (the database
    is `source.db`); a prefix is an error (V-064). PostgreSQL's `schema.table` rules are
    unchanged. Internally the storage / `TableRef` `<schema>` slot is filled with `source.db`
    for MySQL (so the layout stays uniform with PG and consistent with `Data-Flow.md`).
16. **`credentials_file` / `token_file` added** (§4.6). `credentials_file` sits beside every
    `user`/`password` pair (`globals.api`, `source`, `catalog.hive`) and the
    `catalog.rest.oauth2` `client_id`/`client_secret` pair — a path to a JSON file holding the
    pair. `token_file` sits beside the `catalog.rest` bearer `token` — a path to a file whose raw
    contents are the token. Each is mutually exclusive with its explicit counterpart (V-065 /
    V-066 / V-070) and required-as-an-alternative by V-010 / V-040 / V-044 / V-045 / V-049. Their
    purpose is rotation: unlike an `${ENV}` secret (fixed for the process lifetime, PROC to
    change), a file's contents can be replaced and picked up on reload.
17. **Two-phase config-time validation + resolved runtime config** (§11, §11.1) — validation is
    now explicitly *structural* (Phase 1, references unresolved) then *resolution/materialization*
    (Phase 2): `${VAR}`, `credentials_file`, and TLS/keytab material are dereferenced and checked
    for presence and content validity (V-063, V-067–V-069, V-071). Runtime uses only the **resolved**
    config (secrets/PEM materialized in memory), and the reconcile diff is computed on resolved
    values so secret rotation via file content is detected without a process restart.
18. **`schema_evolution` contract aligned to `Schema_Change_Detection_and_Evolution.md`** — §9.5.1 (now §9.7)
    reduces to two fields: `enabled` and `rescan_interval` (renamed from `interval`, default
    `60s` → `60m`, always applicable, `0` disables). The `method` field is **removed** (detection
    is the fixed four-channel model, not an operator choice), and validation rules **V-024**,
    **V-031**, **V-032** are deleted. Detection/evolution semantics are owned by the schema doc;
    §9.5.1 only carries the config surface. The source-side requirements that detection depends on
    (SELECT on the source catalog, `binlog_format`/`binlog_row_image`/`binlog_row_metadata`,
    REPLICATION privileges, `REPLICA IDENTITY`) are documented as **operational start-time
    preconditions** (§11.2) — runtime checks handled per §1.4, **not** config-validation rules.

19. **Multi-sink (2026-09-26)** — `sink.type` (**required, no default**) selects the target table
    format (`iceberg` | `delta`); format blocks `sink.iceberg` / `sink.delta` (union rule §8.4);
    `sink.strategy` → `sink.iceberg.strategy`; pipeline-level `catalog` → `sink.iceberg.catalog`
    (all catalog rule paths relocated, semantics unchanged); `catalog.schema_evolution` →
    pipeline-level `schema_evolution` (§9.7); `sink.write_mode` (`changelog` reserved);
    `column_type_overrides` switched to a sink-neutral logical type grammar; new rules
    **V-083…V-093**. Source: `research/Multi_Sink_Architecture_and_Delta_Lake.md`.

20. **Research-field promotion (2026-09-26, gap report §19.1 C-1…C-16)** — cluster lease /
    rebalance / discovery fields (§6.5, `lifeguard_enabled` §6.5.1); `globals.tracing` (§6.6);
    `globals.azure.account_name` (§6.7); `globals.api.role_mapping` + RBAC pointer (§6.3);
    `source.postgres.slot_management` / `slot_on_orphan` / `max_wal_lag_bytes`;
    `source.mysql.binlog_purge_action`; `tables.snapshots.chunk_size`, `tables.primary_key`,
    `tables.partitioning` (per-table maps); `sink.max_transaction_batch_rows` /
    `max_transaction_buffer_bytes` / `buffer_capacity_rows` / `metrics.per_table`;
    `sink.iceberg.catalog.retry` and `glue.strip_schema`; identifier / partition start-time checks
    (§11.2); rules **V-094…V-113**. Later the same day: `sink.delta.timestamp_ntz_as` (DA-MS-8, V-114); crate-capability rules V-115
    (`catalog.hive` — HMS cannot commit, DA-CAT-6) and V-116 (`rest.auth: sigv4` — no SigV4 client,
    DA-CAT-7), from probe G2c / crate source review. `tables.surrogate_key` (G-30 rejected) and
    `source.consistent_snapshot` (G-7 moot) struck from §16.

None of these change the **decided** catalog-auth schema (hive Thrift `none/ldap/kerberos`;
rest HTTP `none/bearer/oauth2/sigv4`) — those are reproduced verbatim from
`Catalog_Auth_Research.md`.

---

## 16. Config surface deliberately deferred to design

Reserved names for designed-but-not-built (or not-yet-designed) surface, so the schema can grow
without churn. Fields that exist in this spec but reject some values in v1 (`lock_backend:
kubernetes` V-075, `sink.write_mode: changelog` V-089, `sink.delta.catalog.type ≠ none` V-088,
`sink.delta.column_mapping: name` V-092) are not repeated here.

| Future field(s) | Section it will extend | Gap / decision |
|---|---|---|
| `sink.iceberg.maintenance { enabled, expire_snapshots, compaction, remove_orphans, schedule }` | `sink.iceberg` | G-25 — built-in Iceberg maintenance; gated on iceberg-rust rewrite/compaction support (DA-SINK-10). v1 maintenance is external. |
| `sink.delta.maintenance { optimize, … }` | `sink.delta` | D-MS.13 — built-in OPTIMIZE; v1 runs only checkpoints + expired-log cleanup. |
| `sink.large_txn_mode: split \| spill` | `sink` | G-29 §5 / DA-SINK-8 — spill-to-disk for oversized transactions. |
| per-source-endpoint takeover concurrency group | `globals.cluster` | DA-CLUS-13 — v1 cap is per-node global (`max_concurrent_takeovers`). |

**Struck (2026-09-26):** `tables.surrogate_key` — G-30 rejected (a PK-less table halts;
`Ghost_Row_Dedup_and_Primary_Keys.md` §3); `source.consistent_snapshot` — G-7 moot (the incremental
snapshot has no weaker mode; `Snapshot_Resumability_and_Consistency.md` §3). All other previously
reserved fields were promoted (§15 item 20).

---

## 17. Related documents

- `System-Overview.md` — high-level requirements and snapshot-mode semantics.
- `Data-Flow.md` — pipeline stages, state layout under `pipelines/<id>/`, offset acknowledgment.
- `Catalog_Auth_Research.md` — source of the catalog auth schema and the 9 catalog rules.
- `research/Multi_Sink_Architecture_and_Delta_Lake.md` — sink abstraction, `sink.type`, Delta Lake
  block, V-083…V-093.
- `research/Clustering_Architecture_Research.md`, `Postgres_Slot_and_Publication_Lifecycle.md`,
  `Snapshot_Resumability_and_Consistency.md`, `Large_Transaction_Handling.md`,
  `Operational_and_Scope_Gaps_Batch.md`, `Observability_Metrics_and_Tracing.md`,
  `API_RBAC_and_Versioning.md`, `Iceberg_Partitioning_and_Layout_Evolution.md`,
  `Ghost_Row_Dedup_and_Primary_Keys.md` — sources of the fields promoted in §15 item 20.
- `Schema_Change_Detection_and_Evolution.md` — authority for the four-channel detection model,
  DDL safe/unsafe classification, and the source-side preconditions (§11.2); owns the
  `schema_evolution` semantics that §9.7 configures.
- `CDC_Gap_Analysis_Report.md` — gap IDs referenced throughout (G-1f is closed by §13).
