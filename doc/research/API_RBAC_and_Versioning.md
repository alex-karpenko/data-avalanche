---
updated: 2026-06-20
domain: Auth
tags: [rbac, api, authorization, roles, jwt, oauth2, versioning, management-api, cdc]
resolves: [G-16, G-9.1]
---

# Management API: RBAC Role Matrix & Versioning

## Scope

Closes **G-16** (RBAC role→permission matrix) and **G-9.1** (API versioning). The auth *mechanism* is
already specified (config §6.3.1: `none|basic|jwt|oauth2`, `roles_claim` JSON path, JWKS/issuer/
audience verification). This document defines the **roles**, the **endpoint→minimum-role matrix**, how
roles bind per auth mode, and the URL versioning scheme.

---

## §1 — Roles (linear hierarchy)

Three roles, strictly nested: **`admin` ⊇ `operator` ⊇ `viewer`**.

- **`viewer`** — read-only status and observability.
- **`operator`** — `viewer` + day-to-day pipeline lifecycle and config reload.
- **`admin`** — `operator` + cluster-destructive operations.

A principal holding a higher role implicitly satisfies any lower requirement, so authorization is a
single comparison `principal.max_role >= endpoint.min_role`.

---

## §2 — Endpoint → minimum-role matrix (G-16) + versioning (G-9.1)

Authenticated and functional endpoints live under **`/v1`**. Liveness/readiness and metrics are
**unversioned and unauthenticated** (operational probes must work without credentials and across
version skew).

| Endpoint | Method · Path | Min role |
|---|---|---|
| Config validate (dry-run) | `POST /v1/config/validate` | **operator** |
| Config reload | `POST /v1/config/reload` | **operator** |
| Config status | `GET /v1/config/status` | viewer |
| Cluster status | `GET /v1/cluster/status` | viewer |
| Trigger snapshot | `POST /v1/pipelines/{id}/snapshots` | operator |
| Snapshot job status | `GET /v1/snapshots/{job_id}` | viewer |
| Pipeline pause | `POST /v1/pipelines/{id}/pause` | operator |
| Pipeline resume | `POST /v1/pipelines/{id}/resume` | operator |
| Pipeline restart | `POST /v1/pipelines/{id}/restart` | operator |
| Pipeline stop | `POST /v1/pipelines/{id}/stop` | operator |
| Pipeline flush | `POST /v1/pipelines/{id}/flush` | operator |
| Leave cluster | `POST /v1/cluster/leave` | **admin** |
| Shutdown | `POST /v1/admin/shutdown` | **admin** |
| Liveness | `GET /health/live` | none (unversioned) |
| Readiness | `GET /health/ready` | none (unversioned) |
| Metrics | `GET /metrics` | none (unversioned, per System-Overview) |

Notes:

- **`config/validate` is operator, not viewer** — it echoes the *resolved* config, which can contain
  resolved secret material; it must not be readable by a status-only principal.
- **Config reload = operator** (routine ops). Cluster-destructive ops (`leave`, `shutdown`) = admin.
- **Pipeline `stop`** releases DB connections/leases but is recoverable, so it stays operator; only
  node-level teardown is admin.

---

## §3 — Role binding per auth mode (D-16.3)

| `globals.api.auth` | Authorization behavior |
|---|---|
| `none` | No identity exists → **all authenticated endpoints are open**. Dev / trusted-network only; paired with the V-052 cleartext-without-TLS warning. Roles cannot be enforced without an identity. |
| `basic` | Single principal → mapped to **`admin`** (basic auth is inherently single-user). For least-privilege, use `jwt`/`oauth2`. |
| `jwt` / `oauth2` | Roles read from `roles_claim`; the principal's role set is mapped via §4; access granted iff `max(mapped roles) >= endpoint.min_role`. A token with no recognized role is treated as **unauthorized** (403), not viewer. |

A missing/invalid token on an authenticated endpoint → **401**; a valid token lacking the required role
→ **403**. Unauthenticated probes (`/health/*`, `/metrics`) never challenge.

---

## §4 — IdP role mapping (D-16.3 — configurable)

IdP role strings differ across providers (`cdc-admin`, `realm-admin`, group DNs, …), so literal
`viewer`/`operator`/`admin` in the claim is brittle. A configurable mapping is added:

```yaml
globals:
  api:
    role_mapping:               # optional; default = identity on the three role names
      viewer:   [cdc-viewer, readonly]
      operator: [cdc-operator]
      admin:    [cdc-admin, realm-admin]
```

- **Default** (block omitted) = identity: a claim value of `viewer`/`operator`/`admin` maps to itself.
- The verifier reads the roles at `roles_claim` (default `roles`; e.g. `realm_access.roles` for
  Keycloak), maps each to a local role via `role_mapping`, and takes the **maximum**.
- Unmapped IdP roles are ignored (not an error) — a token may carry unrelated roles.

---

## §5 — TLS-and-auth rule (kept as warning)

The existing **V-052** (`auth ≠ none` without `tls.enabled` → **warning**) is retained as a *warning*,
not promoted to an error: in Kubernetes, TLS is frequently terminated upstream by an ingress or a
sidecar (mTLS service mesh), so the API process legitimately speaks plaintext on the pod loopback.
Forcing an error would break that common, secure topology. The warning still flags the genuinely
insecure "auth over plaintext, no upstream TLS" case.

---

## §6 — Config & validation impact

**Added field**

- `globals.api.role_mapping: { viewer: [str], operator: [str], admin: [str] }` (R, optional; default
  identity). Only meaningful for `auth ∈ {jwt, oauth2}`.

**New validation rules**

1. `role_mapping` keys ∈ {viewer, operator, admin}; values are string lists — else error.
2. `role_mapping` present with `auth ∈ {none, basic}` → **warning** (ignored; roles come from the
   single principal / open mode).
3. (Carried) V-052 unchanged — `auth ≠ none` without TLS → warning.

**Doc edits owed (spec, propose-only):** System-Overview API section to list the `/v1` paths and the
min-role column; the new lifecycle endpoints (G-NEW-15) inherit operator from this matrix.

---

## §7 — Resolved decision summary

| Decision | Resolution |
|---|---|
| D-16.1 roles | `admin ⊇ operator ⊇ viewer`; reload/validate = operator; leave/shutdown = admin |
| D-16.2 matrix | §2 endpoint table; `/v1` for functional endpoints, health/metrics unversioned+unauth (G-9.1) |
| D-16.3 binding | none = open; basic = admin; jwt/oauth2 = mapped claim roles, max wins; 401 vs 403 distinguished |
| D-16.4 IdP mapping | configurable `role_mapping`, default identity on the three names |
| D-16.5 TLS rule | V-052 stays a **warning** (upstream TLS termination is common in k8s) |
