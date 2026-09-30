# Catalog Authentication — Research & Config Proposals

**Scope:** Apache Iceberg REST Catalog and Hive Metastore catalog authentication  
**Purpose:** Fill gap G-1g (catalog JWT/OAuth2 auth fields) from the CDC Gap Analysis  
**Relates to:** `Configuration_file_model.md` → `catalog.hive` and `catalog.rest` sections

---

## 1. The Current Draft's Gaps

The current config draft declares three auth modes for both catalog types — `basic`, `jwt`, and `aws` — but leaves jwt and aws as open TODOs, and `basic` is a poor fit for how either catalog actually works:

```yaml
# current draft — incomplete and partially wrong
hive:
  url: https://hive.meta.store/
  auth: basic          # closest is "ldap" in Thrift parlance
  user: user
  password: ${hive_password}
  # elaborate to explore jwt auth parameters  ← still a TODO

rest:
  url: https://api.schema.store/
  auth: basic          # HTTP Basic isn't in the REST Catalog spec
  user: user
  password: ${rest_store_password}
  # elaborate to explore jwt auth parameters  ← still a TODO
```

The two catalogs need substantially different auth models because they use fundamentally different protocols: Hive uses the **Thrift binary protocol**, and the REST catalog uses **HTTP**.

---

## 2. Hive Metastore Catalog (`catalog.hive`)

### 2.1 What the protocol is

The `hive` catalog type in iceberg-rust connects to Hive Metastore (HMS) via the **Thrift binary protocol** on the standard port 9083. This is the classic HMS interface used by Hadoop-era tooling. It is not HTTP and does not use HTTP Basic or Bearer tokens.

Modern versions of HMS (4.0+) also expose an **Iceberg REST API endpoint** on a separate port. However, if you want to connect to that, you use `catalog.rest`, not `catalog.hive`. The `hive` type is exclusively Thrift.

### 2.2 Authentication modes

The Thrift transport layer wraps authentication via **SASL** (Simple Authentication and Security Layer). Three modes are relevant in practice:

| Mode | Mechanism | When to use |
|---|---|---|
| `none` | NOSASL — plain TCP, no auth framing | Local dev, fully trusted internal network |
| `ldap` | SASL PLAIN — username+password validated against LDAP/AD | Production without Kerberos, requires TLS |
| `kerberos` | SASL GSSAPI — Kerberos ticket from a keytab file or kinit cache | Production Hadoop/Kerberized clusters |

**`none` (NOSASL):** The Thrift transport opens without SASL negotiation. No credentials exchanged. HMS must be configured with `hive.metastore.sasl.enabled=false` on the server side. Used only in dev or closed internal networks.

**`ldap`:** Client sends username + password over a SASL PLAIN exchange. HMS validates credentials against a configured LDAP or Active Directory directory. Because password is transmitted in plaintext within SASL PLAIN, TLS on the Thrift connection is mandatory in practice. This is the mode that maps most closely to the current draft's `auth: basic`.

**`kerberos`:** Client authenticates with a Kerberos ticket (Kerberos 5 / GSSAPI). The client needs:
- A valid Kerberos principal (e.g., `cdc-service@MYREALM.COM`)
- Either a keytab file (for unattended service operation) or a pre-existing ticket from `kinit`
- The HMS server's service principal (e.g., `hive/metastore-host.example.com@MYREALM.COM`)

This is the standard auth mode for production Hadoop deployments. The CDC system would need a service keytab provisioned by the cluster Kerberos admin.

### 2.3 TLS for Thrift

Separately from SASL authentication, the Thrift transport can be wrapped in TLS. This is required when using `ldap` to avoid password exposure, and optional with `kerberos` (which provides cryptographic mutual auth at the SASL layer even without TLS). TLS is configured with a cert/key/ca similar to the pattern already in the draft for source DB and memberlist.

### 2.4 What "jwt" and "aws" do NOT mean for Hive Thrift

- **JWT** is not a Thrift HMS auth mechanism. If an HMS deployment exposes its newer Iceberg REST endpoint and uses JWT, you'd access it as `catalog.rest`, not `catalog.hive`.
- **AWS SigV4** signing is irrelevant to the Thrift protocol — it only applies to HTTP-based APIs.

---

## 3. Iceberg REST Catalog (`catalog.rest`)

### 3.1 What the protocol is

The `rest` catalog type connects via HTTP/HTTPS to any server implementing the [Apache Iceberg REST Catalog spec](https://iceberg.apache.org/rest-catalog-spec/). This includes: Apache Polaris, Lakekeeper, Project Nessie (REST mode), AWS Glue (as of 2023), Databricks Unity Catalog, Hive Metastore's Iceberg REST endpoint, and any custom implementation.

All catalog operations (`GET /v1/namespaces`, `POST .../tables`, commit transactions, etc.) go over standard HTTPS.

### 3.2 Authentication modes

| Mode | Mechanism | When to use |
|---|---|---|
| `none` | No `Authorization` header sent | Open/unauthenticated catalogs (dev only) |
| `bearer` | `Authorization: Bearer <static_token>` | Databricks PAT, R2 token, any platform-issued token |
| `oauth2` | OAuth2 client credentials flow; token auto-refreshed | Polaris, Lakekeeper, Keycloak-protected catalogs |
| `sigv4` | AWS Signature Version 4 request signing | AWS Glue REST endpoint; uses ambient IAM credentials |

#### `none`

No credentials sent. The catalog must be configured for anonymous access. This is the default for the HMS Docker image test environment. Not suitable for production.

#### `bearer` (maps to what the draft called `jwt`)

A static, long-lived token is injected into every request as `Authorization: Bearer <token>`. The catalog validates it directly without any token exchange. Common examples:
- Databricks Unity Catalog: personal access token (PAT)
- Cloudflare R2: R2 API token
- Hive Metastore REST endpoint with `metastore.catalog.servlet.auth=jwt`: a pre-issued JWT

The name "bearer" is more accurate than "jwt" because the token is opaque to the client — it may or may not be a JWT internally; the client just forwards it as-is.

#### `oauth2` (the missing half of what the draft called `jwt`)

This is the standard production auth mode for fully-featured REST catalogs. The flow:

1. Client holds `client_id` and `client_secret` (a service-to-service credential pair, similar to an API key pair).
2. On startup (and before each token expiry), client sends an HTTP POST to the `token_url` — an external OAuth2 provider like Keycloak, Auth0, Okta, or AWS Cognito.
3. The provider returns a short-lived `access_token` (typically valid for 15 minutes to 1 hour).
4. Client includes `Authorization: Bearer <access_token>` on every catalog request.
5. iceberg-rust handles token refresh transparently.

The external OAuth2 provider is critical: the Iceberg REST spec originally included a built-in `/v1/oauth/tokens` endpoint on the catalog itself, but **this endpoint is deprecated as of Iceberg Java 1.6.0 and will be removed in Iceberg 2.0**. New implementations must point to an external provider via `token_url`.

Required parameters:
- `client_id` — OAuth2 client identifier (issued by the IdP)
- `client_secret` — OAuth2 client secret (issued by the IdP)
- `token_url` — Full URL of the OAuth2 token endpoint (e.g., `https://keycloak.example.com/realms/myrealm/protocol/openid-connect/token`)
- `scope` — Optional space-separated OAuth2 scopes (some catalogs require specific scopes, e.g., `catalog:write` or `lakekeeper`)

#### `sigv4` (maps to what the draft called `aws`)

Used exclusively with AWS services that implement the REST Catalog spec — primarily **AWS Glue**. Instead of an OAuth2 token, every HTTP request is signed using [AWS Signature Version 4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html) HMAC. The signing uses the ambient AWS credentials from the standard credential chain (instance profile, environment variables, IAM role) — no separate catalog credentials are needed, so there is no `client_id`/`client_secret`.

Required parameters:
- `region` — AWS region (e.g., `us-east-1`)
- `service` — AWS service name for signing (e.g., `glue` for AWS Glue; `execute-api` for API Gateway-fronted catalogs)

This mode is only valid when `globals.cloud = aws`.

### 3.3 The `warehouse` parameter

Several REST catalog implementations (Polaris, Lakekeeper, Unity Catalog, Nessie) require a `warehouse` identifier on catalog initialization. It selects which logical warehouse (a named root location in object storage) the catalog operates on. This is separate from the `bucket`/`prefix` in `sink` — the warehouse tells the catalog server which set of tables to expose, while `sink` tells the CDC writer where to store Parquet files. Some catalogs infer the warehouse from OAuth2 claims; others require it explicitly.

### 3.4 TLS custom CA

Because many internal REST catalog deployments use self-signed certificates or private CAs, the REST catalog client needs a way to specify a trusted CA certificate bundle rather than relying solely on the system trust store.

### 3.5 Credential vending (informational)

The REST Catalog spec includes an optional **credential vending** mechanism: when the client loads a table, the catalog can return short-lived, table-scoped object storage credentials (AWS STS temporary credentials, GCS access tokens, etc.) alongside the table metadata. This eliminates the need for the CDC writer to hold long-lived storage credentials at all.

Credential vending is controlled server-side; the client simply uses whatever credentials are returned with the table response. No special config is needed beyond using `auth: oauth2` or `auth: sigv4` to authenticate with the catalog. It is mentioned here because it interacts with the `sink` credential model — if credential vending is enabled on the catalog, the `globals.cloud` credential chain for object storage writes becomes optional.

---

## 4. Proposed Configuration Changes

### 4.1 `catalog.hive` — revised schema

```yaml
catalog:
  hive:
    url: thrift://hive-metastore.example.com:9083  # mandatory
                                                    # default port 9083; thrift:// or host:port both accepted.
    auth: none  # optional, none (default), ldap, kerberos
    
    # --- for auth: ldap ---
    user: svc-cdc              # mandatory for ldap
    password: ${HMS_PASSWORD}  # mandatory for ldap
    
    # --- for auth: kerberos ---
    kerberos:
      principal: cdc-service@MYREALM.COM              # mandatory; Kerberos principal for this service
      keytab: /etc/security/keytabs/cdc.keytab        # mandatory; path to keytab file
      service_name: hive                              # optional; Kerberos service name on the HMS host, default "hive"
    
    # --- TLS (optional; recommended for ldap to protect password in transit) ---
    tls:
      enabled: false  # default false; set true to wrap Thrift in TLS
      ca: /path/to/ca.pem  # optional; custom CA for self-signed HMS certs. PEM inline or file path.
      # client cert auth for mTLS is not standard for HMS Thrift, so no cert/key here.
    
    # --- database mapping ---
    database: analytics  # optional; default is source.db
```

**Changes from draft:**
- `auth` values changed: `basic` → `ldap`, `jwt` removed (not applicable to Thrift), `aws` removed.
- Added `kerberos:` sub-block for Kerberos auth.
- Added `tls:` block for optional Thrift transport encryption.
- URL clarified as Thrift endpoint (`thrift://` scheme or bare `host:port`).

### 4.2 `catalog.rest` — revised schema

```yaml
catalog:
  rest:
    url: https://catalog.example.com  # mandatory; base URL of the REST Catalog server (no trailing /v1)
    warehouse: my-warehouse  # optional; required by Polaris, Lakekeeper, Unity Catalog, some Nessie setups
    prefix: /iceberg          # optional; URL path prefix inserted before /v1/ (e.g., for Hive REST: /iceberg)
    auth: none  # optional, none (default), bearer, oauth2, sigv4
    
    # --- for auth: bearer ---
    # A static, long-lived token sent as "Authorization: Bearer <token>".
    # Suitable for Databricks PAT, R2 tokens, pre-issued JWTs from an identity provider.
    token: ${CATALOG_BEARER_TOKEN}  # mandatory for bearer
    
    # --- for auth: oauth2 ---
    # OAuth2 client credentials flow. The client_id:client_secret are exchanged at token_url
    # for a short-lived access token, which is refreshed automatically before expiry.
    # Point token_url at an external identity provider — not at /v1/oauth/tokens on the catalog
    # itself, which is deprecated in Iceberg 1.6 and will be removed in Iceberg 2.0.
    oauth2:
      client_id: ${CATALOG_CLIENT_ID}        # mandatory; OAuth2 client identifier
      client_secret: ${CATALOG_CLIENT_SECRET} # mandatory; OAuth2 client secret
      token_url: https://keycloak.example.com/realms/my-realm/protocol/openid-connect/token
                                              # mandatory; external IdP token endpoint
      scope: catalog:read catalog:write       # optional; space-separated; required by some catalogs
                                              # e.g., Lakekeeper requires "lakekeeper",
                                              #        Polaris requires the principal role scope
    
    # --- for auth: sigv4 (valid only when globals.cloud = aws) ---
    # Requests signed with AWS Signature Version 4 using the ambient credential chain
    # (instance profile, env vars, IAM role). No separate catalog credentials needed.
    # Required for the AWS Glue REST endpoint.
    sigv4:
      region: us-east-1   # mandatory; AWS region of the catalog service
      service: glue        # mandatory; AWS service name for signing
                           # "glue" for AWS Glue, "execute-api" for API Gateway-fronted catalogs
    
    # --- TLS ---
    tls:
      ca: /path/to/ca.pem  # optional; custom CA bundle for internal/self-signed certs.
                           # PEM inline or file path. Uses system trust store when absent.
```

**Changes from draft:**
- `auth: basic` removed — HTTP Basic Auth is not part of the Iceberg REST spec.
- `auth: jwt` renamed to `auth: bearer` — more accurate; the token type is opaque to the client.
- `auth: aws` renamed to `auth: sigv4` — matches the actual mechanism name.
- `auth: oauth2` added as a first-class mode with a proper `oauth2:` sub-block.
- `token:` field added for `bearer` mode (replaces the `user`/`password` pair).
- `oauth2:` block added with `client_id`, `client_secret`, `token_url`, `scope`.
- `sigv4:` block added with `region` and `service`.
- `warehouse:` and `prefix:` fields added.
- `tls.ca:` added for custom CA bundles.
- Former `user`/`password` fields removed (they were HTTP Basic credentials; no standard REST catalog uses these).

---

## 5. Validation Rules Implied by the Above

These should be added to the validation rules table (gap G-1f):

| Constraint | Severity |
|---|---|
| `catalog.hive.auth = ldap` requires `user` and `password` | error |
| `catalog.hive.auth = kerberos` requires `kerberos.principal` and `kerberos.keytab` | error |
| `catalog.hive.tls.enabled = true` requires `tls.ca` when not using system trust store | warning |
| `catalog.hive.auth = ldap` without `tls.enabled = true` | warning (password in cleartext) |
| `catalog.rest.auth = bearer` requires `token` | error |
| `catalog.rest.auth = oauth2` requires `oauth2.client_id`, `oauth2.client_secret`, `oauth2.token_url` | error |
| `catalog.rest.auth = sigv4` requires `sigv4.region` and `sigv4.service` | error |
| `catalog.rest.auth = sigv4` requires `globals.cloud = aws` | error |
| `catalog.rest.auth = oauth2` with `oauth2.token_url` pointing to `<catalog_url>/v1/oauth/tokens` | warning (deprecated endpoint) |

---

## 6. Summary of Renaming / Mapping

| Draft value | Real mechanism | Proposed name | Applies to |
|---|---|---|---|
| `auth: basic` | SASL PLAIN (LDAP-validated) | `auth: ldap` | `hive` |
| `auth: jwt` | Not applicable to Thrift HMS | removed | `hive` |
| `auth: aws` | Not applicable to Thrift HMS | removed | `hive` |
| *(missing)* | SASL GSSAPI (Kerberos) | `auth: kerberos` | `hive` |
| `auth: basic` | HTTP Basic (not in spec) | removed | `rest` |
| `auth: jwt` | Static bearer token | `auth: bearer` | `rest` |
| `auth: aws` | AWS Signature Version 4 | `auth: sigv4` | `rest` |
| *(missing)* | OAuth2 client credentials | `auth: oauth2` | `rest` |
| *(missing)* | No authentication | `auth: none` | both |

---

## 7. Gap Status Update

This research fully resolves **G-1g** (catalog JWT/OAuth2 auth fields — previously marked 🔴 Blocker).

No new gaps are introduced. The new `sigv4` constraint (`globals.cloud = aws`) should be added to the validation rules list (G-1f).

---

## 8. Crate capability check (2026-09-26)

Read against the pinned `iceberg-catalog-*` **0.10.1** sources (and `main`), plus a live probe for Glue
(`prototypes/iceberg-capability/`, G2c):

| Catalog | Commit (`update_table`) | Auth as specified here | Status |
|---|---|---|---|
| `glue` (native) | client-side requirement check + Glue `UpdateTable` with `VersionId` (optimistic lock) | AWS SDK chain / static keys / profile | ✅ **probed on AWS**: CAS holds under 8-way concurrency |
| `hive` | **`FeatureUnsupported`** (0.10.1 and `main`) | NOSASL / SASL PLAIN / GSSAPI (§2) — moot | 🟥 **cannot commit** — DA-CAT-6 |
| `rest` | server-side requirements, 409 → `CatalogCommitConflicts` | `none` / `bearer` / `oauth2` available; **`sigv4` absent** (open PRs #3082, #3092) | 🟥 `sigv4` unrealizable — DA-CAT-7 |
| `s3tables` (native, not in config) | `UpdateTableMetadataLocation` with `versionToken` | AWS SDK chain | source-read only |

The auth model in §2–§4 stays correct as a design; the Iceberg sink's capability assertion must refuse
`hive` and `rest` + `sigv4` while the pinned crate lacks them.
