---
updated: 2026-11-15
domain: Architecture
tags: [v1.1, implementation-plan, m13b, console, nodes, gossip, forwarding, config-generations, api, cluster, config, telemetry]
---

# M13b — The Console's Nodes and Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The second server milestone of the 1.1.0 admin console (Admin_Console §14): what the
Cluster, node and Config pages read, and the node actions through any node:
- each node's API address and its own config file's hash travel in the gossip (the MAC label's
  v2); `globals.api.advertise_url` names the address when the derived one won't do;
- any node fetches another node's live detail, and asks another node to leave or shut down, with
  the caller's own credentials and a time limit of its own (§10); no node-to-node trust is added;
- a node's process figures (memory and CPU against the container's limits) and gossip figures
  (peers, the last probe and its round trip, suspicions raised and received);
- the config generations: who published each and why, the text of any one with its secrets
  masked, the changes between any two with their classes, and which generation each node runs
  with whether its own file matches it;
- a member that was up before this node turned active is no longer recorded as joining (M13a's
  F4);
- the site, the chart and the E2E suite follow.

**Architecture:**

- **Config** (Task 1, `data-avalanche-config`): `globals.api.advertise_url`, optional, PROC; V-138.
- **Gossip v2** (Task 2, `data-avalanche-cluster`, `data-avalanche`): `NodeMeta` gains `api_url`
  and `file_hash` (the first 16 hex digits of the BLAKE3 hash of the node's file, as
  `config_hash` writes them); the MAC key's derivation label becomes `… gossip packet MAC v2`, so
  a 1.0 node and a 1.1 node drop each other's packets instead of misreading them. The node hashes
  its file at start and every 15 s, and gossips a change. `GET /v1/cluster/status` gains each
  member's `api_url` and file status, and each pipeline's ring owner and lease expiry.
- **Gossip figures** (Task 3, `data-avalanche-cluster`, `data-avalanche-telemetry`): foca's
  `unstable-notifications` (same 2.0.0) gives the packets sent and received and the probes that
  failed; the gossip task keeps the last probe, its round trip and a 24 h window of suspicions
  raised and received. The membership recorder waits for a settled view before it records (F4).
- **Process figures** (Task 4, `data-avalanche-telemetry`): `/proc/self` and the cgroup (v2, else
  v1) on Linux, nothing elsewhere; sampled every 15 s for the CPU's rate; four metrics.
- **Peers and node routes** (Task 5, `data-avalanche-api`, `data-avalanche`): a `Peers` client
  (reqwest on rustls: the platform's roots and this node's own certificate chain) sends a request to
  a member's `api_url` with the caller's `Authorization` header and body, marked as forwarded, so
  it is never forwarded again. `GET /v1/nodes/{node}` answers locally or fetches it;
  `POST /v1/nodes/{node}/leave` and `…/shutdown` are forwarded to the node's own
  `/v1/cluster/leave` and `/v1/admin/shutdown`, which verify the caller and record the action.
- **Generations** (Task 6, `data-avalanche-state`, `-config`, `-cluster`, `-api`): each publish
  writes a record `config/history/<generation:06>.json` (who, role, note, from which node, the
  summary). The generation's text is shown with its secret values masked in place;
  `GET /v1/config/generations` and `GET /v1/config/generations/{generation}`.
- **Diff and the nodes' files** (Task 7, `-config`, `-api`): the changes between two generations'
  texts, field by field, each with its class and masked values; `GET /v1/config/nodes`: each
  member's generation, whether its file matches it (from the gossip), and its restart-pending
  fields, asked of each node within 2 s.
- **The site and the chart** (Task 8): the pages, the reference, the upgrade note on the gossip's
  v2, and the chart's `advertise_url`.
- **The E2E suite** (Task 9): three nodes; a node's detail and its leave through another node;
  generations, a diff and a node's changed file.

**Tech Stack:** Rust 1.100.0 (edition 2024). No new crate: foca gains its `unstable-notifications`
feature (same `=2.0.0`); the telemetry crate takes the workspace's `tokio` (`sync`, `time`) and
`tempfile` for its tests; the cluster crate's `tokio` gains `fs`. The API crate's `reqwest` was
there already.

**Spec:**
- `doc/research/Admin_Console.md`: §4 (Cluster, node, Config pages), §5 (node actions through any
  node), §8 (live from memory and gossip; each source its own time limit), §10 (forwarding with the
  caller's credentials; the API address in the gossip), §12 (new figures; config generations and
  diff), §13 (endpoint paths are the builder's), §14.
- The approved mockups `doc/v1.1/mockups/03-cluster.html`, `04-node.html`, `10-config.html`,
  `11-node-actions-and-degraded-states.html`.
- `doc/specs/System-Overview.md`: §Management API endpoints, §Observability catalogue, §Cluster.
- `doc/specs/Configuration_Specification.md`: §6.3 `globals.api`, §6.5.1 `memberlist`, §12
  (reload, mutability classes).
- `doc/research/Clustering_Architecture_Research.md` (SWIM, suspicion, the ring),
  `doc/research/API_RBAC_and_Versioning.md` §2 (roles).
- `doc/v1.0/M8a-membership.md` (gossip, Q-M8-2's MAC), `M8b-config-generations.md`,
  `M9a-management-api.md`; `doc/v1.1/M13a-console-events.md` (the recorder, attribution).

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`; `telemetry` beside them. `testkit` is a dev-dependency only.
- **Upgrading from 1.0.x:** the gossip's v2 is a deliberate break (approved, mockup round):
  1.0 and 1.1 nodes can't open each other's packets, so each side would place the pipelines among
  its own members (the leases keep it from writing twice, not from moving pipelines back and
  forth). The upgrade page says to stop every node, then start them all on 1.1. New state documents (`config/history/`) are new keys no 1.0 node
  reads; no existing document gains a field.
- **No node-to-node trust:** a forwarded request carries the caller's own credentials and is
  verified and recorded by the node it reaches; the serving node adds nothing a caller couldn't
  send itself, and never forwards a request that was forwarded to it.
- **Each source its own time limit** (Admin_Console §8): a node's detail 3 s, a node action 10 s,
  each node's config status 2 s; a node that doesn't answer is named, never waited on past that.
- **Secrets never leave a node in clear:** a generation's text and a diff mask a secret's literal
  value (`••••••`, with a short keyed fingerprint); a `${VAR}` reference is shown as written.
- **Time in tests:** the gossip's, the sampler's and the membership recorder's tests run on
  tokio's paused clock where they wait; the API's tests and the E2E suite run in real time.
- **Test gates:** plain `cargo test` stays hermetic. The E2E suite needs `DATA_AVALANCHE_IT=1` and
  the `e2e-hooks` feature, and runs with `--test-threads=1`.
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`).
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `thiserror` types.
- Code blocks are the verified content, already `rustfmt`-formatted. Every block is a `git
  apply`-ready diff, one per file; a step's diffs apply in the order given, after the earlier
  steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

The inputs most likely to hurt a person running 1.1.0 in production, each pinned by a test in the
task after the arrow.

1. **The node asked about is down, partitioned or slow.** Asking another node for its detail, or
   to leave, must answer within its time limit and name the node (`502` it didn't answer, `504`
   too slow), never hang the page → Task 5 (`a_peer_that_doesnt_answer_is_named_within_the_limit`,
   `a_slow_peer_is_cut_off_at_the_limit`).
2. **A stale address** (a pod rescheduled under the same name, an address reused by another
   node). A forwarded request that reaches a node other than its target must be refused, not acted
   on, and never forwarded on → Task 5 (`a_request_forwarded_to_the_wrong_node_is_refused`).
3. **Rights through another node.** A viewer asking node A to shut node B down is refused at A
   (and recorded there); B gets the caller's own credentials and nothing of A's, so A can't lend
   its rights → Task 5 (`a_viewer_cant_leave_another_node`,
   `another_node_s_detail_is_fetched_with_the_callers_own_credentials`).
4. **Secrets in a generation's text**: a literal password, one inside a map of headers, a
   `${VAR}`, a rotated literal. Never shown in clear, in the text or in a diff; a rotation shows
   as a changed fingerprint → Task 6 (`a_generations_secrets_are_masked_in_place`), Task 7
   (`a_rotated_literal_secret_shows_only_its_fingerprint`).
5. **A container without limits, or a host that isn't Linux.** The figures say "no limit" or
   nothing, never `0` or a bogus number (cgroup v2's `max`, v1's huge sentinel) → Task 4
   (`no_limit_reads_as_none`, `cgroup_v1_s_sentinel_is_no_limit`).

## Decisions and rulings

Settled with Oleksii before this plan: the API address goes into the gossip with a version bump
(mockup round, 2026-11-14); M13b is split in two, nodes and config here and the pipelines' detail
in a new M13c (2026-11-15); a node finds a peer's API at `globals.api.advertise_url`, by default
derived from the gossip's advertised address, and verifies it with ordinary hostname checks
(2026-11-15). The rest are Claude's calls under Admin_Console §13.

- **D-M13b.1 The milestones of 1.1.0, again.** M13a (merged, #27); M13b (this plan); M13c (the
  owner-served pipeline detail and per-table figures, source detail, all tables, all snapshot
  jobs, job cancel and retry with the "copy cancelled" halt); M13d (history, the Prometheus
  reader, the SSE stream); M14 (sign-in and sessions); M15 (the frame and Overview, serving
  `/ui/`); M16 (the detail pages and actions). Nothing is tagged until M16 merges. The charts of
  the node page (lease renewal p99, memory and CPU over 6 h) read M13d's history; M13b serves the
  current values.
- **D-M13b.2 The API address.** `globals.api.advertise_url` when set (V-138: an absolute `http` or
  `https` URL with a host, no path but `/`, no query or fragment). Otherwise
  `<scheme>://<host>:<port>`: the scheme `https` when `api.tls` is on; the host the listener's
  when `api.address` names one that isn't unspecified (`0.0.0.0`, `::`), else the gossip's
  advertised address (an IPv6 one in brackets); the port `api.address`'s. PROC: the address is
  gossiped from the start.
- **D-M13b.3 Gossip v2.** `NodeMeta` gains `api_url: String` and `file_hash: String`, the first 16
  hex digits of `config_hash` of the node's own file (empty when it can't be read). The MAC key's
  derivation label becomes `data-avalanche 2026-10-20 gossip packet MAC v2`. A node hashes its file
  at start and every 15 s (`FILE_CHECK`) and gossips a change (one identity revision). Packets keep
  `MAX_PAYLOAD`; each identity grows by at most 90 bytes, so foca piggybacks fewer updates per
  packet.
- **D-M13b.4 Forwarding.** `Peers::send(node, api_url, path_and_query, asked, limit)` sends to the
  member's `api_url`; only `Authorization`, `Content-Type` and the body travel (`Asked`), with
  `x-data-avalanche-target: <node>` and `x-data-avalanche-forwarded-by: <serving node>`; no proxy,
  no redirect, an answer of at most 8 MiB. The answer comes back as the target gave it,
  with `x-data-avalanche-node: <target>`. The serving node answers `404` for a name that isn't a
  member, `503` for a member without an `api_url`, `502` when the target doesn't answer (connect,
  TLS), `504` past the limit. A request carrying the forwarded header is never forwarded: a
  `/v1/nodes/{node}/…` route reached that way whose `{node}` isn't the receiving node answers `421`.
  TLS trusts the platform's roots (reqwest's rustls platform verifier) and every certificate of
  this node's own `api.tls.cert`, with hostname verification. The serving node also refuses, before
  anything reads it, a request whose `x-data-avalanche-target` isn't itself (`421`): an action sent
  to an address since reused never runs on the wrong node.
- **D-M13b.5 The node routes.** `GET /v1/nodes/{node}` (viewer): the node's detail, answered
  locally for this node, else fetched (3 s). `POST /v1/nodes/{node}/leave`, `…/shutdown` (admin),
  `POST /v1/nodes/{node}/config/validate`, `…/config/reload` (operator) and
  `GET /v1/nodes/{node}/config/status` (viewer) go to the node's own `/v1/cluster/leave`,
  `/v1/admin/shutdown`, `/v1/config/validate`, `/v1/config/reload` (with its query) and
  `/v1/config/status`, the receiving node included, so the target verifies the caller and records
  the action (10 s; 2 s for the status). The serving node records only a `403` of its own gate.
- **D-M13b.6 A node's detail.** Its name, `api_url`, gossip address, build, incarnation and start,
  phase, generation; its file's hash and whether it matches its generation's; its restart-pending
  fields; `process` (D-M13b.7); `gossip` (D-M13b.8); the leases it holds, with each one's epoch and
  ring owner.
- **D-M13b.7 Process figures.** On Linux: resident memory from `/proc/self/status` (`VmRSS`),
  CPU seconds from `/proc/self/stat` (utime + stime over Linux's `USER_HZ`, 100: `sysconf` would
  need `unsafe`); the limits from cgroup v2 at the process's own cgroup (`/proc/self/cgroup`'s
  `0::<path>`) else at the hierarchy's root (`memory.max`, `cpu.max`; `max` is none), else v1
  (`memory.limit_in_bytes` at or over 2^60 is none; `cpu.cfs_quota_us` −1 is none). Elsewhere
  every figure is `None`. A sampler every 15 s (`PROCESS_SAMPLE`) keeps the CPU's rate in cores.
  Metrics `process_resident_memory_bytes`, `process_cpu_seconds_total` (the names every
  Prometheus client gives them, outside the `data_avalanche` prefix),
  `data_avalanche_process_memory_limit_bytes`, `data_avalanche_process_cpu_limit_cores` (a series
  only while there is a limit).
- **D-M13b.8 Gossip figures.** The peers and their state; the last probe (a `Ping` foca sent, to
  whom, when) and its round trip (to its `Ack`); suspicions in the last 24 h (`SUSPICIONS_KEPT`):
  *raised*, a probe of a member that failed (`ProbeFailed`), with the member; *received*, this node
  suspected by the others, seen as a higher incarnation of itself in its own packets (foca refutes
  that way) or as a `Rejoin` (declared down first). foca's `unstable-notifications` (same 2.0.0)
  gives the packets and the failed probes. Metrics `data_avalanche_gossip_probe_rtt_seconds`
  (histogram, 0.5 ms to ~1 s) and `data_avalanche_gossip_suspicions_total{direction}` (`raised`,
  `received`).
- **D-M13b.9 Generation records.** After its pointer lands, a publish writes
  `config/history/<generation:06>.json` (`Document` kind `config history`, version 1: generation,
  hash, node, time, the caller's name and role, the note, the change summary); a record already
  there (a retried publish) is kept. Bootstrap writes generation 1's, `by` none, summary
  `bootstrap`. A failed record write is logged; the generation stands. A missing record (a 1.0
  publisher, a crash between the two writes) leaves the generation listed from its snapshot: its
  hash is the pointer's when it is the current one, else its only snapshot's, else unknown. A
  snapshot newer than the pointer (a publish that lost its race) is no generation. The reload
  passes the caller and the note (the recording layer hands the note on as `Noted`) to
  `ConfigCoordinator::reload_as`; `reload` is `reload_as` with no one. `GET
  /v1/config/generations` (viewer): newest first, `limit` 1–100 (20), `before`, and `next` when
  the page is full. `GET /v1/config/generations/{generation}`: the record and the masked text;
  `400` not a number, `404` unknown, `409` when its hash is unknown, `500` when the text can't be
  masked (D-M13b.10).
- **D-M13b.10 Masking.** The secrets are the keys of one list, `SECRET_KEYS` (the API's password
  and OAuth2 client secret, its TLS key, the tracing and Prometheus headers, the session key, the
  Prometheus password and bearer token, a source's password and `ssl_key`, the Iceberg catalogs'
  password, token and client secret; a pipeline's apply under `defaults` too). A test ties the list
  to the model: every key in it exists, and every key named like a secret is in it. A raw-model
  walk (`Walk::leaves`, generated with `drop_empty` and `check_values`, so no field can be left
  out) yields each leaf with its path and where its text is written (an alias's anchor, from
  serde-saphyr's `defined` span). Each secret is replaced in place, so comments and layout stay:
  a `${VAR}` as written; a key or `ssl_key` that is a path, as written; a literal as
  `"•••••• blake3:<8 hex>"`, the start of its fingerprint under the cluster's key, so two
  generations show whether it changed. Quoted scalars are replaced with their quotes, a block
  scalar from its `|`/`>` to its last line, a plain one (folded over lines too) without its
  comment. The masked text is parsed again: every secret must then be a mask or a reference;
  otherwise `MaskError` and nothing is shown. Only the secret keys are masked: a secret's text also
  written as another key's value (a `user` that is the `password`) stays shown there, as it is in
  the resolved config. (Ruling: a search for each secret's text anywhere in the masked text was
  tried, and refused the E2E's generations, whose sources write `user: avalanche`,
  `password: avalanche`.)
- **D-M13b.11 The diff.** `GET /v1/config/diff?from=&to=` (viewer): both texts parsed into the raw
  model without the environment (`diff_written`), each leaf flattened to a dotted path (a pipeline
  by its `id`, else its key; a list element as `[i]`), compared as written. Pipelines added and
  removed are named by `id`, their keys not listed. Each other change: path, class (`fixed` for
  `FIXED_FIELDS`, else from `key_mutability`: `in_place`, `tables` for a table pattern,
  `pipeline_restart`, `node_restart`; a `defaults` key classed as a pipeline's), before and after
  as written (`null` where absent), secrets as D-M13b.10 shows them. A `${VAR}` whose value
  changed is no change here. `400` for `from`/`to` missing or not a number; `404`/`409` as a
  generation's. What a change did when applied comes from the events (M13a), not from here.
- **D-M13b.12 The nodes' files.** `GET /v1/config/nodes` (viewer): the generation and hash the
  pointer names, and for each member its generation, its file (`same` when its gossiped hash is
  the first 16 hex digits of the generation's, `differs`, `unknown` when it gossips none), and its
  `restart_pending`: this node's own, another's from its `/v1/config/status` asked with the
  caller's credentials, all at once (2 s); `null` with `error` when it doesn't answer, answers
  other than 200, or gossips no API address.
- **D-M13b.13 The cluster status.** `GET /v1/cluster/status` gains each member's `api_url` and
  `file_hash`, and each pipeline's `ring_owner` and lease `expires_at`; the page derives "away from
  its owner" and "renews in".
- **D-M13b.14 The membership recorder settles (F4).** It records nothing until this node has been
  `Active` for 10 s (`MEMBERS_SETTLE`), and takes the view then as its baseline: a member that was
  up before is never recorded as joining.

### Spec patch for approval (S-M13b.1)

Approved by Oleksii 2026-11-15 with the plan; applied as execution starts. These edits to two
spec files are what the code below implements; Task 1's tests
need the first file's V-138 row (the registry is checked against the spec's table), so the patch
lands first, as its own commit `spec: S-M13b.1 — globals.api.advertise_url and V-138; the node and
generation endpoints; the process and gossip metrics`, once Oleksii approves it.

1. `doc/specs/Configuration_Specification.md`: §6.3 gains the `advertise_url` row; §13.6 gains
   V-138.
2. `doc/specs/System-Overview.md`: the endpoint table's cluster status row says what it now
   holds, and gains six rows (node detail; node leave and shutdown; node config; config
   generations; config diff; config nodes); the observability catalogue gains the Process and
   Gossip rows.

`doc/specs/Configuration_Specification.md`:

````diff
diff --git a/doc/specs/Configuration_Specification.md b/doc/specs/Configuration_Specification.md
index 389bcef..fb7b2b8 100644
--- a/doc/specs/Configuration_Specification.md
+++ b/doc/specs/Configuration_Specification.md
@@ -335,6 +335,7 @@ api:
 | Field | Type | Required | Default | Mutability | Notes |
 |---|---|---|---|---|---|
 | `address` | host:port | no | `0.0.0.0:8080` | PROC | IPv4/IPv6/DNS host + port. Listener binding cannot hot-change. |
+| `advertise_url` | URL | no | derived | PROC | Where the other nodes reach this node's API, for the console's pages and node actions served by another node (a node's detail, leave, shutdown). Derived: `https` when `tls.enabled`, else `http`; `address`'s host, or the gossip's `advertise_addr` when `address` listens on all addresses; `address`'s port. With TLS, set it to a name the certificate covers (a pod's headless-Service name): the other nodes verify it as any HTTPS client does. Absolute `http(s)` URL of a host and port only (V-138). |
 | `auth` | enum `none` \| `basic` \| `jwt` \| `oauth2` | no | `none` | R | Auth mode for **authenticated** endpoints. |
 | `user` | string | conditional | — | R | For `auth = basic`: required unless `credentials_file` is given (V-049); mutually exclusive with it (V-065). |
 | `password` | string | conditional | — | R | For `auth = basic`: required unless `credentials_file` is given (V-049); mutually exclusive with it (V-065). Use `${ENV}`. |
@@ -1419,6 +1420,7 @@ errors.
 | V-135 | `globals.console.enabled = true` with `api.auth ∈ {basic, oauth2}` | Requires `globals.console.session_key`. | error | config |
 | V-136 | `globals.console.enabled = true` with `api.auth = oauth2` | Requires `globals.console.public_url` and `globals.api.oauth2.client_id`. | error | config |
 | V-137 | `globals.console.prometheus` | Requires `url`; `basic` requires `user` and `password`. | error | config |
+| V-138 | `globals.api.advertise_url` | An absolute `http`/`https` URL with a host, without a path, query or fragment. | error | config |
 
 ### 13.7 Identity / naming
 
````

`doc/specs/System-Overview.md`:

````diff
diff --git a/doc/specs/System-Overview.md b/doc/specs/System-Overview.md
index 3f66ee6..51ac767 100644
--- a/doc/specs/System-Overview.md
+++ b/doc/specs/System-Overview.md
@@ -128,7 +128,13 @@ Functional endpoints are versioned under `/v1`; probes and metrics are unversion
 | Config validate | `POST /v1/config/validate` | operator | dry-run of the full load pipeline (parse → validate → resolve) of the body, or of the node's file when the body is empty: the report and what a reload would change; the resolved config is not returned; nothing is published. A node takes posted bodies at 1 per second after a burst of 5: past it, `429` with `Retry-After` |
 | Config reload | `POST /v1/config/reload` | operator | publishes a new config generation cluster-wide; a process-restart field is refused unless `?restart=accept`, and then applied at each node's next start (`restart_pending`) |
 | Config status | `GET /v1/config/status` | viewer | |
-| Cluster status | `GET /v1/cluster/status` | viewer | |
+| Cluster status | `GET /v1/cluster/status` | viewer | each member's state, API URL and config file hash; each pipeline's lease holder, its ring owner and when the lease expires |
+| Node detail | `GET /v1/nodes/{node}` | viewer | the node's build, start, phase, config generation and whether its file is that generation's, its restart-pending fields, process and gossip figures, and the pipelines it holds; another node's is fetched from it with the caller's credentials (`404` no such member, `503` no API address, `502`/`504` no answer) |
+| Node leave / shutdown | `POST /v1/nodes/{node}/leave`, `POST /v1/nodes/{node}/shutdown` | admin | the node's own `POST /v1/cluster/leave` or `POST /v1/admin/shutdown`, asked of any node and passed on with the caller's credentials; the node that acts records it. A request passed on names its node: any other node refuses it (`421`) |
+| Node config | `POST /v1/nodes/{node}/config/validate`, `POST /v1/nodes/{node}/config/reload`, `GET /v1/nodes/{node}/config/status` | operator (status: viewer) | the node's own config routes, asked of any node: the check and the reload read that node's file |
+| Config generations | `GET /v1/config/generations`, `GET /v1/config/generations/{generation}` | viewer | newest first, each with who published it, its note and what it changed; `limit` 1–100 (20), `before`. One generation's text has its secrets masked: a `${VAR}` as written, a literal as the start of its fingerprint |
+| Config diff | `GET /v1/config/diff?from=&to=` | viewer | what differs between two generations as written, each change with what a reload of it does; secrets masked |
+| Config nodes | `GET /v1/config/nodes` | viewer | each member's generation, whether its own file is the cluster's generation, and its restart-pending fields |
 | Pipelines status | `GET /v1/pipelines`, `GET /v1/pipelines/{id}` | viewer | `desired_state`, `actual_state`, positions, per-table status (halted tables with their reason); the lease holder flagged `stale` when it isn't a member the node's gossip sees (gone, or an earlier process of a restarted node), `suspect` while gossip suspects it |
 | Request snapshot | `POST /v1/pipelines/{id}/snapshots` | operator | tables list (the per-table config keys), no globs; refused (`409`) unless the desired state is `RUNNING` and a node holds the lease, as a restart (`ERROR` included), or while an unfinished job holds one of the tables; `202` with the job's id. The job runs in a restarted worker. It upserts: a row deleted at the source before the job stays, unless the job is asked with `truncate: true`, which empties each sink table first. A pipeline keeps its 100 newest ended jobs |
 | Snapshot job status | `GET /v1/snapshots/{job_id}` | viewer | |
@@ -160,6 +166,8 @@ Names are the exposed sample names (OpenMetrics): a counter's name ends in `_tot
 | Clustering | `data_avalanche_lease_state`, `data_avalanche_lease_renew_failures_total`, `data_avalanche_lease_renew_latency_seconds`, `data_avalanche_cluster_members`, `data_avalanche_owned_pipelines`, `data_avalanche_ring_generation` |
 | Status | `data_avalanche_pipeline_status` (running / paused / stopped / error), `data_avalanche_halted_tables` |
 | Events | `data_avalanche_events_written_total`, `data_avalanche_events_dropped_total` (the node's writes to the cluster's timeline; no labels) |
+| Process | `process_resident_memory_bytes`, `process_cpu_seconds_total` (Linux), `data_avalanche_process_memory_limit_bytes`, `data_avalanche_process_cpu_limit_cores` (the cgroup's limits, absent without one; no labels) |
+| Gossip | `data_avalanche_gossip_probe_rtt_seconds`, `data_avalanche_gossip_suspicions_total{direction}` (`raised`, `received`; no other labels) |
 
 **Definitions:** `replication_lag_seconds` is the age of the oldest change read but not yet durable in the sink: now minus the source commit time of the oldest transaction read since the last checkpoint, `0` when nothing is pending; snapshot rows don't count. `binlog_lag_seconds` is the age of the last transaction the stream read, `0` at the server's head; once a server heartbeat has shown the stream caught up, it counts from that heartbeat as soon as the server writes past the head the stream was at; a MySQL pipeline has no `wal_lag_bytes`, a PostgreSQL one no `binlog_lag_seconds`. `rows_read_total{op}` counts row changes read (`insert`, `update`, `delete`) and snapshot rows (`snapshot`). `snapshot_state{state}` is `running` or `waiting` (held by the source, or behind the previous term's writers). `sink_commit_failures_total` counts every failed attempt, retried ones included.
 
````

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-config/src/{raw,walk,resolved,build,mutability,field_docs,registry}.rs`, `rules/domain.rs`, `tests/{load,rules/domain}.rs` | `globals.api.advertise_url`, V-138 (Task 1) |
| `crates/data-avalanche-cluster/src/advertised.rs` (new), `gossip.rs`, `wire.rs`, `lib.rs`, `Cargo.toml`, the `NodeMeta` sites | The API URL, the file's hash and its watch; the gossip's v2 (Task 2) |
| `crates/data-avalanche-api/src/{cluster,pipelines,test_support}.rs`, `crates/data-avalanche/tests/e2e/cluster.rs` | The cluster status's new fields; the test node's meta; the E2E's pipeline row (Task 2) |
| `Cargo.toml`, `crates/data-avalanche-cluster/src/{gossip,events}.rs`, `crates/data-avalanche-telemetry/src/lib.rs`, `tests/{metrics,catalogue}.rs` | foca's notifications; the gossip's figures and series; the settled membership view (Task 3) |
| `crates/data-avalanche-telemetry/src/process.rs` (new), `lib.rs`, `Cargo.toml`, `tests/metrics.rs` | The process's figures, the sampler, the four series (Task 4) |
| `crates/data-avalanche-api/src/{peers,nodes}.rs` (new), `lib.rs`, `actions.rs`, `openapi.rs`, `openapi.json`, `test_support.rs`; `crates/data-avalanche-cluster/src/coordinator.rs` | Forwarding, the node routes, the `421` middleware (Task 5) |
| `xtask/src/api.rs`, `docs/src/content/docs/reference/api.md`, `docs/public/openapi.json` | The API reference's `nodes` section and new routes (Tasks 5–7) |
| `crates/data-avalanche-state/src/history.rs` (new), `generation.rs`, `lib.rs` | Generation records, the history listing (Task 6) |
| `crates/data-avalanche-config/src/mask.rs` (new), `leaf.rs`, `walk.rs`, `lib.rs`, `diff.rs` | Masking in place; the written diff (Tasks 6, 7) |
| `crates/data-avalanche-cluster/src/coordinator.rs`, `lib.rs` | `Attribution`, `reload_as`, the record after a publish; `generations`, `mask`, `diff_written` (Tasks 6, 7) |
| `crates/data-avalanche-api/src/generations.rs` (new), `config.rs`, `actions.rs` (`Noted`), `nodes.rs` (`files`) | The generations, diff and config nodes routes; the reload's attribution (Tasks 6, 7) |
| `crates/data-avalanche/src/run.rs` | The node's API URL, file watch, gossip report, sampler and peers, wired (Tasks 2–5) |
| `deploy/helm/data-avalanche/{values.yaml,templates/statefulset.yaml,ci/tls-values.yaml}`, `crates/data-avalanche/tests/packaging.rs` | The pod's DNS name and the TLS example's `advertise_url` (Task 8) |
| `docs/src/content/docs/{runbooks/upgrade,deployment/kubernetes,deployment/observability}.md` | 1.0 → 1.1, the advertised URL, the new series (Task 8) |
| `crates/data-avalanche/tests/e2e/{console_nodes,main}.rs` | The three-node scenario (Task 9) |

---

### Task 1: The API's advertised address (`data-avalanche-config`)

**Files:**
- Modify: `crates/data-avalanche-config/src/build.rs`, `crates/data-avalanche-config/src/field_docs.rs`, `crates/data-avalanche-config/src/mutability.rs`, `crates/data-avalanche-config/src/raw.rs`, `crates/data-avalanche-config/src/registry.rs`, `crates/data-avalanche-config/src/resolved.rs`, `crates/data-avalanche-config/src/rules/domain.rs`, `crates/data-avalanche-config/src/walk.rs`, `crates/data-avalanche-config/tests/load.rs`, `crates/data-avalanche-config/tests/rules/domain.rs`, `docs/src/content/docs/reference/configuration.md`, `docs/src/content/docs/reference/validation-rules.md`

**Interfaces:**
- Consumes: the config crate's raw/walk/resolved/build pattern, the rule registry, `FIELD_DOCS`,
  the mutability table; the spec's V-138 row (S-M13b.1, applied first).
- Produces (re-exported at the crate root): `Api::advertise_url: Option<String>` (trimmed; `None`
  when absent or empty), mutability PROC, its field doc (default `Note("derived")`), and V-138
  (`origin_url`: `http(s)://host[:port]` with an optional trailing `/`, nothing else). The registry
  holds 132 rules.

- [ ] **Step 1: Write the failing tests**

The tests: `advertise_url_is_absent_by_default_and_stored_trimmed` (`tests/load.rs`); V-138's four cases (`tests/rules/domain.rs`); the registry's count, 132.

`crates/data-avalanche-config/src/registry.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index 952dc7f..10eb62c 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1161,7 +1169,7 @@ mod tests {
 
     #[test]
     fn has_every_spec_rule_once_in_order() {
-        assert_eq!(RULES.len(), 131);
+        assert_eq!(RULES.len(), 132);
         assert!(
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
````

`crates/data-avalanche-config/tests/load.rs`:

````diff
diff --git a/crates/data-avalanche-config/tests/load.rs b/crates/data-avalanche-config/tests/load.rs
index f0d8402..7b914f2 100644
--- a/crates/data-avalanche-config/tests/load.rs
+++ b/crates/data-avalanche-config/tests/load.rs
@@ -1228,6 +1228,26 @@ fn console_urls_are_stored_trimmed() {
     );
 }
 
+/// Config §6.3: `advertise_url` is absent unless written, and stored trimmed.
+#[test]
+fn advertise_url_is_absent_by_default_and_stored_trimmed() {
+    let cfg = load_str(MINIMAL, &env(&[("PW", "pw")]), &fp_key())
+        .unwrap_or_else(|r| panic!("{r:#?}"))
+        .config;
+    assert_eq!(cfg.globals.api.advertise_url, None);
+    let yaml = MINIMAL.replace(
+        "globals:\n  state: { bucket: app-state }",
+        "globals:\n  state: { bucket: app-state }\n  api:\n    advertise_url: \" https://cdc-0.cdc:8080 \"",
+    );
+    let cfg = load_str(&yaml, &env(&[("PW", "pw")]), &fp_key())
+        .unwrap_or_else(|r| panic!("{r:#?}"))
+        .config;
+    assert_eq!(
+        cfg.globals.api.advertise_url.as_deref(),
+        Some("https://cdc-0.cdc:8080")
+    );
+}
+
 /// Config §6.8: the console's secrets resolve as secrets (shown by fingerprint), Prometheus takes
 /// its bearer token and labels, and the oauth2 sign-in fields keep their defaults.
 #[test]
````

`crates/data-avalanche-config/tests/rules/domain.rs`:

````diff
diff --git a/crates/data-avalanche-config/tests/rules/domain.rs b/crates/data-avalanche-config/tests/rules/domain.rs
index 03c667a..755d868 100644
--- a/crates/data-avalanche-config/tests/rules/domain.rs
+++ b/crates/data-avalanche-config/tests/rules/domain.rs
@@ -340,6 +340,32 @@ pub fn cases() -> Vec<Case> {
                 )
             },
         ),
+        case("V-138", Error, "an advertise_url with a path", || {
+            pg_delta().set(
+                "/globals/api/advertise_url",
+                json!("https://cdc-0.cdc.ns.svc:8080/api"),
+            )
+        }),
+        case("V-138", Error, "an advertise_url without a scheme", || {
+            pg_delta().set("/globals/api/advertise_url", json!("cdc-0.cdc.ns.svc:8080"))
+        }),
+        case("V-138", Error, "an advertise_url with credentials", || {
+            pg_delta().set(
+                "/globals/api/advertise_url",
+                json!("https://u:p@cdc-0:8080"),
+            )
+        }),
+        case(
+            "V-138",
+            Silent,
+            "an advertise_url of a host and port",
+            || {
+                pg_delta().set(
+                    "/globals/api/advertise_url",
+                    json!("https://cdc-0.cdc.ns.svc:8080/"),
+                )
+            },
+        ),
         case("V-131", Error, "basic and a bearer token", || {
             pg_delta().set("/globals/console/prometheus", json!({"url": "https://prom", "basic": {"user": "u", "password": "p"}, "bearer_token": "t"}))
         }),
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-config`
Expected: the build fails: no field `advertise_url` on `Api`.

- [ ] **Step 3: Implement**

The reference pages are regenerated by `cargo xtask docs` (their diffs below are its output).

`crates/data-avalanche-config/src/build.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/build.rs b/crates/data-avalanche-config/src/build.rs
index c02bd92..c738ec7 100644
--- a/crates/data-avalanche-config/src/build.rs
+++ b/crates/data-avalanche-config/src/build.rs
@@ -447,6 +447,7 @@ fn api(a: &RawApi, at: &At, ctx: &mut Ctx) -> Option<Api> {
             port: 8080,
         },
     );
+    let advertise_url = opt(ctx, &a.advertise_url, at, "advertise_url");
     let auth = or(ctx, &a.auth, at, "auth", ApiAuth::None);
     // §11 Phase 2 (G4): `credentials_file` is only in effect for `auth = basic` — the file itself is
     // read only then — but every present leaf still gets `${VAR}` substituted either way (V-063).
@@ -540,6 +541,7 @@ fn api(a: &RawApi, at: &At, ctx: &mut Ctx) -> Option<Api> {
     };
     Some(Api {
         address: address?,
+        advertise_url: advertise_url?.map(|u| u.trim().to_owned()),
         auth,
         credentials,
         jwt,
````

`crates/data-avalanche-config/src/field_docs.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
index 1a244f3..f7aac6a 100644
--- a/crates/data-avalanche-config/src/field_docs.rs
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -63,6 +63,11 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
         Value("0.0.0.0:8080"),
         "Where the management API, `/metrics` and the health probes listen.",
     ),
+    f(
+        "globals.api.advertise_url",
+        Note("derived"),
+        "Where the other nodes reach this node's API (`https://cdc-0.cdc.ns.svc:8080`), for a console page served by another node. Derived: `https` with `tls`, else `http`; `address`'s host, or the gossip's `advertise_addr` when it listens on all addresses; `address`'s port. With TLS, write a name the certificate covers (V-138).",
+    ),
     f(
         "globals.api.auth",
         Value("none"),
````

`crates/data-avalanche-config/src/mutability.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/mutability.rs b/crates/data-avalanche-config/src/mutability.rs
index d686dfc..68ef934 100644
--- a/crates/data-avalanche-config/src/mutability.rs
+++ b/crates/data-avalanche-config/src/mutability.rs
@@ -27,6 +27,7 @@ pub(crate) const CLASSES: &[(&str, Mutability)] = &[
     ("globals.cloud", Proc),
     ("globals.state", Proc),
     ("globals.api.address", Proc),
+    ("globals.api.advertise_url", Proc),
     ("globals.api.auth", R),
     ("globals.api.credentials", R),
     ("globals.api.jwt", R),
````

`crates/data-avalanche-config/src/raw.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/raw.rs b/crates/data-avalanche-config/src/raw.rs
index d688545..fef2dcd 100644
--- a/crates/data-avalanche-config/src/raw.rs
+++ b/crates/data-avalanche-config/src/raw.rs
@@ -52,6 +52,7 @@ pub(crate) struct RawGlobalState {
 #[serde(deny_unknown_fields)]
 pub(crate) struct RawApi {
     pub address: L<HostPort>,
+    pub advertise_url: L<String>,
     pub auth: L<ApiAuth>,
     pub user: L<String>,
     pub password: L<String>,
````

`crates/data-avalanche-config/src/registry.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index 952dc7f..10eb62c 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1148,6 +1148,14 @@ pub const RULES: &[RuleMeta] = &[
         r#"globals.console.prometheus"#,
         r#"Requires url; basic requires user and password."#,
     ),
+    rule(
+        "V-138",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.api.advertise_url"#,
+        r#"An absolute http/https URL with a host, without a path, query or fragment."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
````

`crates/data-avalanche-config/src/resolved.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/resolved.rs b/crates/data-avalanche-config/src/resolved.rs
index fc5b5b4..9b3ee9c 100644
--- a/crates/data-avalanche-config/src/resolved.rs
+++ b/crates/data-avalanche-config/src/resolved.rs
@@ -92,6 +92,9 @@ pub struct TlsIdentity {
 #[derive(Debug, Clone, PartialEq, Serialize)]
 pub struct Api {
     pub address: HostPort,
+    /// Where the other nodes reach this node's API; `None` = derived from the gossip's
+    /// advertised address, `address`'s port and `tls`.
+    pub advertise_url: Option<String>,
     pub auth: ApiAuth,
     /// Set when `auth = basic`.
     pub credentials: Option<Credentials>,
````

`crates/data-avalanche-config/src/rules/domain.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/rules/domain.rs b/crates/data-avalanche-config/src/rules/domain.rs
index 78235f5..ee31b93 100644
--- a/crates/data-avalanche-config/src/rules/domain.rs
+++ b/crates/data-avalanche-config/src/rules/domain.rs
@@ -234,6 +234,19 @@ fn globals(g: &RawGlobals, ctx: &mut Ctx) {
     if let Some(c) = &g.console {
         console(c, &root.key("console"), ctx);
     }
+    // V-138
+    if let Some(l) = g.api.as_ref().and_then(|a| a.advertise_url.as_ref())
+        && let Some(url) = ctx.text(l, &root.key("api").key("advertise_url"))
+        && !origin_url(url.trim())
+    {
+        ctx.error(
+            "V-138",
+            &root.key("api").key("advertise_url"),
+            l.loc(),
+            "advertise_url must be an absolute http or https URL with a host and nothing after it \
+             (no path, query or fragment)",
+        );
+    }
     // V-101
     if let Some(a) = &g.azure
         && let Some((name, loc)) = val(ctx, &a.account_name, &root.key("azure").key("account_name"))
@@ -622,6 +635,20 @@ fn http_headers(ctx: &mut Ctx, headers: &BTreeMap<String, Leaf<String>>, at: &At
 }
 
 /// Whether `text` is an absolute `http`/`https` URL with a host.
+/// An `http(s)://host[:port]` URL, a trailing `/` at most: where a node is reached, not a page.
+fn origin_url(text: &str) -> bool {
+    let lower = text.to_ascii_lowercase();
+    let rest = lower
+        .strip_prefix("https://")
+        .or_else(|| lower.strip_prefix("http://"));
+    rest.is_some_and(|r| {
+        let host = r.strip_suffix('/').unwrap_or(r);
+        !host.is_empty()
+            && !host.contains(['/', '?', '#', '@'])
+            && !host.contains(char::is_whitespace)
+    })
+}
+
 fn http_url(text: &str) -> bool {
     let lower = text.to_ascii_lowercase();
     let rest = lower
````

`crates/data-avalanche-config/src/walk.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/walk.rs b/crates/data-avalanche-config/src/walk.rs
index f707723..c6815d7 100644
--- a/crates/data-avalanche-config/src/walk.rs
+++ b/crates/data-avalanche-config/src/walk.rs
@@ -244,6 +244,7 @@ walk_fields!(RawGlobals {
 walk_fields!(RawGlobalState { bucket, prefix });
 walk_fields!(RawApi {
     address,
+    advertise_url,
     auth,
     user,
     password,
````

`docs/src/content/docs/reference/configuration.md`:

````diff
diff --git a/docs/src/content/docs/reference/configuration.md b/docs/src/content/docs/reference/configuration.md
index f9efa79..2f00276 100644
--- a/docs/src/content/docs/reference/configuration.md
+++ b/docs/src/content/docs/reference/configuration.md
@@ -42,6 +42,7 @@ The management API, `/metrics` and the health probes.
 | Key | Type | Default | On reload | Rules | Description |
 |---|---|---|---|---|---|
 | `address` | host:port | `0.0.0.0:8080` | process restart |  | Where the management API, `/metrics` and the health probes listen. |
+| `advertise_url` | string | derived | process restart | [V-138](/data-avalanche/reference/validation-rules/#v-138) | Where the other nodes reach this node's API (`https://cdc-0.cdc.ns.svc:8080`), for a console page served by another node. Derived: `https` with `tls`, else `http`; `address`'s host, or the gossip's `advertise_addr` when it listens on all addresses; `address`'s port. With TLS, write a name the certificate covers (V-138). |
 | `auth` | `none` \| `basic` \| `jwt` \| `oauth2` | `none` | applied | [V-016](/data-avalanche/reference/validation-rules/#v-016) [V-049](/data-avalanche/reference/validation-rules/#v-049) [V-050](/data-avalanche/reference/validation-rules/#v-050) [V-051](/data-avalanche/reference/validation-rules/#v-051) [V-052](/data-avalanche/reference/validation-rules/#v-052) | How the API authenticates a caller: `none` (open), `basic` (one user, `admin`), or a bearer JWT (`jwt`, `oauth2`) whose roles map through `role_mapping`. The health probes and `/metrics` never ask. |
 | `user` | string | — | applied |  | The user for `auth: basic`, or give `credentials_file`. |
 | `password` | string | — | applied |  | The password for `auth: basic`; write it as `${VAR}`. |
````

`docs/src/content/docs/reference/validation-rules.md`:

````diff
diff --git a/docs/src/content/docs/reference/validation-rules.md b/docs/src/content/docs/reference/validation-rules.md
index 696f28c..38ec7b5 100644
--- a/docs/src/content/docs/reference/validation-rules.md
+++ b/docs/src/content/docs/reference/validation-rules.md
@@ -143,3 +143,4 @@ A config with any **error** is rejected whole: at startup the node refuses to st
 | <span id="v-135"></span>V-135 | error | `globals.console.enabled = true with api.auth ∈ {basic, oauth2}` | Requires globals.console.session_key. |
 | <span id="v-136"></span>V-136 | error | `globals.console.enabled = true with api.auth = oauth2` | Requires globals.console.public_url and globals.api.oauth2.client_id. |
 | <span id="v-137"></span>V-137 | error | `globals.console.prometheus` | Requires url; basic requires user and password. |
+| <span id="v-138"></span>V-138 | error | `globals.api.advertise_url` | An absolute http/https URL with a host, without a path, query or fragment. |
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-config`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/build.rs crates/data-avalanche-config/src/field_docs.rs crates/data-avalanche-config/src/mutability.rs crates/data-avalanche-config/src/raw.rs crates/data-avalanche-config/src/registry.rs crates/data-avalanche-config/src/resolved.rs crates/data-avalanche-config/src/rules/domain.rs crates/data-avalanche-config/src/walk.rs crates/data-avalanche-config/tests/load.rs crates/data-avalanche-config/tests/rules/domain.rs docs/src/content/docs/reference/configuration.md docs/src/content/docs/reference/validation-rules.md
git commit -m "feat(config): globals.api.advertise_url, where the other nodes reach a node's API (V-138)"
```

---

### Task 2: Gossip v2: the API address and the file's hash (`data-avalanche-cluster`, `-api`, `data-avalanche`)

**Files:**
- Create: `crates/data-avalanche-cluster/src/advertised.rs`
- Modify: `crates/data-avalanche-api/src/cluster.rs`, `crates/data-avalanche-api/src/pipelines.rs`, `crates/data-avalanche-api/src/test_support.rs`, `crates/data-avalanche-cluster/Cargo.toml`, `crates/data-avalanche-cluster/src/coordinator.rs`, `crates/data-avalanche-cluster/src/discovery.rs`, `crates/data-avalanche-cluster/src/events.rs`, `crates/data-avalanche-cluster/src/gossip.rs`, `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-cluster/src/node.rs`, `crates/data-avalanche-cluster/src/test_support.rs`, `crates/data-avalanche-cluster/src/wire.rs`, `crates/data-avalanche/src/run.rs`, `crates/data-avalanche/tests/e2e/cluster.rs`

**Interfaces:**
- Consumes: `Api::advertise_url` (Task 1); the gossip's `NodeMeta` and `Advertiser`, `wire.rs`'s
  MAC (M8a); `config_hash` (M8b).
- Produces (cluster crate root): `FILE_CHECK: Duration` (15 s); `api_url(&Api, IpAddr) -> String`
  (D-M13b.2); `file_hash(&str) -> String` (16 hex digits); `watch_file(PathBuf, String, Advertiser)`
  (async, never ends); `NodeMeta::{api_url, file_hash}: String`; `Advertiser::set_file_hash(String)
  -> bool` (`false` once the gossip is gone); `wire.rs`'s `CONTEXT` ends in `MAC v2`. `GET
  /v1/cluster/status` gains `members[].api_url`, `members[].file_hash`, `pipelines[].ring_owner`
  and `pipelines[].expires_at`. `run.rs` gossips both from the start and spawns `watch_file`.

- [ ] **Step 1: Write the failing tests**

The tests: `a_file_hash_reaches_every_peer`, `a_v1_packet_doesnt_open`, `advertised`'s three tests (in the new module, Step 3), the cluster status's members and pipelines; the E2E cluster scenario's pipeline row (`any_nodes_api_drives_the_holder`, run in Task 9's suite).

`crates/data-avalanche-api/src/cluster.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/cluster.rs b/crates/data-avalanche-api/src/cluster.rs
index a955239..300d4d0 100644
--- a/crates/data-avalanche-api/src/cluster.rs
+++ b/crates/data-avalanche-api/src/cluster.rs
@@ -192,9 +210,17 @@ mod tests {
         assert_eq!(v["members"][0]["node"], "node-a");
         assert_eq!(v["members"][0]["phase"], "active");
         assert_eq!(v["members"][0]["config_generation"], 1);
+        assert_eq!(v["members"][0]["api_url"], "http://127.0.0.1:8080");
+        assert_eq!(
+            v["members"][0]["file_hash"],
+            data_avalanche_cluster::file_hash(CONFIG)
+        );
         assert_eq!(
             v["pipelines"],
-            json!([{ "id": "orders", "holder": null, "owner_epoch": null, "holder_stale": null }])
+            json!([{
+                "id": "orders", "holder": null, "owner_epoch": null, "holder_stale": null,
+                "expires_at": null, "ring_owner": "node-a",
+            }])
         );
         let holder = Holder {
             node_id: "node-b".into(),
@@ -205,9 +231,22 @@ mod tests {
             .await
             .unwrap();
         let v = json_of(addr, "GET", "/v1/cluster/status").await.1;
+        let p = &v["pipelines"][0];
         assert_eq!(
-            v["pipelines"],
-            json!([{ "id": "orders", "holder": "node-b", "owner_epoch": 1, "holder_stale": true }])
+            (
+                &p["holder"],
+                &p["owner_epoch"],
+                &p["holder_stale"],
+                &p["ring_owner"]
+            ),
+            (&json!("node-b"), &json!(1), &json!(true), &json!("node-a")),
+            "held away from its owner: {p}"
+        );
+        let expires: chrono::DateTime<Utc> = p["expires_at"].as_str().unwrap().parse().unwrap();
+        let left = expires - Utc::now();
+        assert!(
+            left > chrono::Duration::seconds(25) && left <= chrono::Duration::seconds(30),
+            "{left}"
         );
     }
 
````

`crates/data-avalanche-api/src/pipelines.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index b2876e9..36f0e95 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -589,6 +589,8 @@ mod tests {
                 phase: data_avalanche_cluster::Phase::Active,
                 config_generation: 1,
                 build: "1.0.0".into(),
+                api_url: String::new(),
+                file_hash: String::new(),
             },
             suspect,
         };
````

`crates/data-avalanche-cluster/src/coordinator.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 1f1fa12..a556903 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -916,6 +916,8 @@ mod tests {
                 phase: Phase::Active,
                 config_generation: 1,
                 build: "1.0.0".into(),
+                api_url: String::new(),
+                file_hash: String::new(),
             },
         };
         let gossip = Gossip::start(
````

`crates/data-avalanche-cluster/src/discovery.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/discovery.rs b/crates/data-avalanche-cluster/src/discovery.rs
index 80061e0..4526de5 100644
--- a/crates/data-avalanche-cluster/src/discovery.rs
+++ b/crates/data-avalanche-cluster/src/discovery.rs
@@ -252,6 +252,8 @@ mod tests {
                     phase: crate::Phase::Active,
                     config_generation: 1,
                     build: "1.0.0".into(),
+                    api_url: String::new(),
+                    file_hash: String::new(),
                 },
             };
             let key = crate::GossipKey::derive(&[7; 32]);
@@ -306,6 +308,8 @@ mod tests {
                     phase: crate::Phase::Active,
                     config_generation: 1,
                     build: "1.0.0".into(),
+                    api_url: String::new(),
+                    file_hash: String::new(),
                 },
             };
             let key = crate::GossipKey::derive(&[7; 32]);
````

`crates/data-avalanche-cluster/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
index 14ae529..bdd7d7f 100644
--- a/crates/data-avalanche-cluster/src/events.rs
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -706,6 +706,8 @@ mod tests {
                                 phase: *phase,
                                 config_generation: 1,
                                 build: "1.1.0".into(),
+                                api_url: String::new(),
+                                file_hash: String::new(),
                             },
                             suspect: *suspect,
                         },
````

`crates/data-avalanche-cluster/src/gossip.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index dee4777..725d8ca 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -510,7 +535,9 @@ mod tests {
             meta: NodeMeta {
                 phase: Phase::Active,
                 config_generation: 1,
-                build: "1.0.0".into(),
+                build: "1.1.0".into(),
+                api_url: format!("http://{}", SocketAddr::new(addr.ip(), 8080)),
+                file_hash: "0123456789abcdef".into(),
             },
         }
     }
@@ -614,6 +641,24 @@ mod tests {
         );
     }
 
+    /// A node's file changed under it: its new hash reaches every peer (D-M13b.3), and its API
+    /// address travels with it.
+    #[tokio::test(start_paused = true)]
+    async fn a_file_hash_reaches_every_peer() {
+        let hub = Hub::new();
+        let (a, b, c) = three(&hub).await;
+        let meta = |g: &Gossip| g.members().borrow().nodes.get("b").map(|m| m.meta.clone());
+        assert_eq!(meta(&a).unwrap().api_url, "http://10.0.0.2:8080");
+        assert!(b.advertiser().set_file_hash("fedcba9876543210".into()));
+        eventually("b's new hash on a and c", || {
+            [&a, &c]
+                .iter()
+                .all(|g| meta(g).is_some_and(|m| m.file_hash == "fedcba9876543210"))
+        })
+        .await;
+        assert_eq!(names(&a), ["a", "b", "c"], "a new hash is no departure");
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_leave_is_seen_at_once() {
         let hub = Hub::new();
````

`crates/data-avalanche-cluster/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 3ebd504..0688f57 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -22,6 +23,7 @@ pub mod ring;
 mod test_support;
 pub mod wire;
 
+pub use advertised::{FILE_CHECK, api_url, file_hash, watch_file};
 pub use claim::{Claim, Claimed, release};
 pub use controller::{
     Actual, Configured, ControllerDeps, ControllerSettings, ControllerStatus, CurrentPipeline,
````

`crates/data-avalanche-cluster/src/node.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/node.rs b/crates/data-avalanche-cluster/src/node.rs
index 8a2a37a..53d9256 100644
--- a/crates/data-avalanche-cluster/src/node.rs
+++ b/crates/data-avalanche-cluster/src/node.rs
@@ -90,6 +90,8 @@ mod tests {
                 phase: Phase::Joining,
                 config_generation: 1,
                 build: "1.0.0".into(),
+                api_url: String::new(),
+                file_hash: String::new(),
             },
         };
         let key = crate::GossipKey::derive(&[7; 32]);
````

`crates/data-avalanche-cluster/src/wire.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/wire.rs b/crates/data-avalanche-cluster/src/wire.rs
index 37ff5a4..945a861 100644
--- a/crates/data-avalanche-cluster/src/wire.rs
+++ b/crates/data-avalanche-cluster/src/wire.rs
@@ -184,6 +186,20 @@ mod tests {
         assert_eq!(format!("{ours:?}"), "GossipKey(..)");
     }
 
+    /// A 1.0 node seals under the v1 label: a 1.1 node drops its packets rather than misread
+    /// the metadata 1.1 added (D-M13b.3).
+    #[test]
+    fn a_v1_packet_doesnt_open() {
+        let anchor = [7; 32];
+        let v1 = GossipKey(blake3::derive_key(
+            "data-avalanche 2026-10-20 gossip packet MAC v1",
+            &anchor,
+        ));
+        let v2 = GossipKey::derive(&anchor);
+        assert_eq!(v2.open(&v1.seal(b"ping")), None);
+        assert_eq!(v1.open(&v2.seal(b"ping")), None);
+    }
+
     #[tokio::test]
     async fn an_address_taken_is_an_error_naming_it() {
         let (a, _in) = Udp::bind(&[SocketAddr::from(([127, 0, 0, 1], 0))])
````

`crates/data-avalanche/tests/e2e/cluster.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/cluster.rs b/crates/data-avalanche/tests/e2e/cluster.rs
index 93d7bad..bcb542b 100644
--- a/crates/data-avalanche/tests/e2e/cluster.rs
+++ b/crates/data-avalanche/tests/e2e/cluster.rs
@@ -372,10 +372,14 @@ async fn any_nodes_api_drives_the_holder() {
         .iter()
         .find(|p| p["id"] == json!(id))
         .cloned();
-    // A live holder isn't stale (DA-API-13).
+    // A live holder isn't stale (DA-API-13); in a settled cluster it is the ring's owner, and the
+    // lease's expiry is a time.
+    let mut row = row.expect("the pipeline's row");
+    assert!(row["expires_at"].is_string(), "{row}");
+    row.as_object_mut().unwrap().remove("expires_at");
     assert_eq!(
         row,
-        Some(json!({ "id": id, "holder": holder, "owner_epoch": epoch, "holder_stale": false }))
+        json!({ "id": id, "holder": holder, "owner_epoch": epoch, "holder_stale": false, "ring_owner": holder })
     );
     c.landed().await;
     c.stop().await;
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-cluster -p data-avalanche-api`
Expected: the build fails: no fields `api_url`, `file_hash` in `NodeMeta`; no `file_hash` in the crate root.

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/cluster.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/cluster.rs b/crates/data-avalanche-api/src/cluster.rs
index a955239..300d4d0 100644
--- a/crates/data-avalanche-api/src/cluster.rs
+++ b/crates/data-avalanche-api/src/cluster.rs
@@ -8,7 +8,7 @@ use axum::Extension;
 use axum::extract::State;
 use axum::http::StatusCode;
 use axum::response::Response;
-use data_avalanche_cluster::Phase;
+use data_avalanche_cluster::{Phase, Ring};
 use serde_json::{Value, json};
 use tokio::sync::watch;
 
@@ -60,7 +60,9 @@ fn phase(p: Phase) -> &'static str {
     }
 }
 
-/// The members this node sees, and each pipeline's lease holder.
+/// The members this node sees, with where each one's API is and its file's hash; each
+/// pipeline's lease holder, epoch and expiry, and its owner on the ring of the active members
+/// (where it goes once the ring has settled, D-M13b.13).
 pub(crate) async fn status(State(api): State<Api>) -> Response {
     let Some(node) = api.node.get() else {
         return starting();
@@ -77,10 +79,20 @@ pub(crate) async fn status(State(api): State<Api>) -> Response {
                 "suspect": m.suspect,
                 "config_generation": m.meta.config_generation,
                 "build": m.meta.build,
+                "api_url": (!m.meta.api_url.is_empty()).then_some(&m.meta.api_url),
+                "file_hash": (!m.meta.file_hash.is_empty()).then_some(&m.meta.file_hash),
             })
         })
         .collect();
     let config = node.coordinator.config().await;
+    let ring = Ring::new(
+        members
+            .nodes
+            .iter()
+            .filter(|(_, m)| m.meta.phase == Phase::Active)
+            .map(|(name, _)| name.as_str()),
+        config.globals.cluster.tokens,
+    );
     let mut pipelines = vec![];
     let mut ids: Vec<&str> = config.pipelines.values().map(|p| p.id.as_str()).collect();
     ids.sort_unstable();
@@ -101,6 +113,12 @@ pub(crate) async fn status(State(api): State<Api>) -> Response {
             "holder_stale": lease
                 .as_ref()
                 .map(|l| liveness(&members, l) == Liveness::Stale),
+            "expires_at": lease.as_ref().map(|l| {
+                l.renewed_at + chrono::Duration::milliseconds(
+                    i64::try_from(l.lease_duration_ms).unwrap_or(i64::MAX),
+                )
+            }),
+            "ring_owner": ring.owner(id),
         }));
     }
     body(
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index f7a47a7..0139afa 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -202,7 +202,9 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
         meta: NodeMeta {
             phase: Phase::Active,
             config_generation: 1,
-            build: "1.0.0".into(),
+            build: "1.1.0".into(),
+            api_url: "http://127.0.0.1:8080".into(),
+            file_hash: data_avalanche_cluster::file_hash(text),
         },
     };
     let gossip = Gossip::start(
````

`crates/data-avalanche-cluster/Cargo.toml`:

````diff
diff --git a/crates/data-avalanche-cluster/Cargo.toml b/crates/data-avalanche-cluster/Cargo.toml
index 2b73f41..d2ef726 100644
--- a/crates/data-avalanche-cluster/Cargo.toml
+++ b/crates/data-avalanche-cluster/Cargo.toml
@@ -26,7 +26,7 @@ if-addrs.workspace = true
 rand.workspace = true
 serde.workspace = true
 thiserror.workspace = true
-tokio = { workspace = true, features = ["net", "sync"] }
+tokio = { workspace = true, features = ["fs", "net", "sync"] }
 tracing.workspace = true
 xxhash-rust.workspace = true
 
````

`crates/data-avalanche-cluster/src/advertised.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/advertised.rs b/crates/data-avalanche-cluster/src/advertised.rs
new file mode 100644
index 0000000..96e430f
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/advertised.rs
@@ -0,0 +1,145 @@
+//! What a node tells the others about itself beyond its phase (Admin_Console §10, D-M13b.2–3):
+//! where its API is, and a short hash of its own config file, so another node's console page can
+//! reach it and say whether its file still matches the generation it runs.
+
+use std::net::{IpAddr, SocketAddr};
+use std::path::PathBuf;
+use std::time::Duration;
+
+use data_avalanche_config::Api;
+use data_avalanche_state::config_hash;
+
+use crate::Advertiser;
+
+/// How often a node reads its file again for its hash.
+pub const FILE_CHECK: Duration = Duration::from_secs(15);
+
+/// The URL the other nodes reach this node's API at (D-M13b.2): `api.advertise_url`, else the
+/// scheme its TLS sets, the listener's host (`advertised`, the gossip's address, when it listens
+/// on all addresses) and the listener's port.
+pub fn api_url(api: &Api, advertised: IpAddr) -> String {
+    if let Some(url) = &api.advertise_url {
+        return url.trim_end_matches('/').to_owned();
+    }
+    let scheme = if api.tls.is_some() { "https" } else { "http" };
+    let host = api
+        .address
+        .host
+        .trim_start_matches('[')
+        .trim_end_matches(']');
+    match host.parse::<IpAddr>() {
+        Ok(ip) if ip.is_unspecified() => {
+            format!(
+                "{scheme}://{}",
+                SocketAddr::new(advertised, api.address.port)
+            )
+        }
+        Ok(ip) => format!("{scheme}://{}", SocketAddr::new(ip, api.address.port)),
+        Err(_) => format!("{scheme}://{host}:{}", api.address.port),
+    }
+}
+
+/// The first 16 hex digits of the text's `config_hash`: enough to tell two files apart, short
+/// enough for every identity in a gossip packet.
+pub fn file_hash(text: &str) -> String {
+    let hash = config_hash(text);
+    let hex = hash.strip_prefix("blake3:").unwrap_or(&hash);
+    hex[..16].to_owned()
+}
+
+/// The hash of the file at `path`; empty when it can't be read.
+async fn hash_of(path: &PathBuf) -> String {
+    match tokio::fs::read_to_string(path).await {
+        Ok(text) => file_hash(&text),
+        Err(e) => {
+            tracing::debug!(path = %path.display(), "the config file for its hash: {e}");
+            String::new()
+        }
+    }
+}
+
+/// Reads the node's file every [`FILE_CHECK`] and gossips its hash when it changed from `last`
+/// (a ConfigMap updated under a running node). Runs until the gossip is gone.
+pub async fn watch_file(path: PathBuf, mut last: String, advertiser: Advertiser) {
+    loop {
+        tokio::time::sleep(FILE_CHECK).await;
+        let now = hash_of(&path).await;
+        if now != last {
+            tracing::info!(path = %path.display(), hash = %now, "the config file changed");
+            if !advertiser.set_file_hash(now.clone()) {
+                return;
+            }
+            last = now;
+        }
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use data_avalanche_config::{
+        ApiAuth, FingerprintKey, HostPort, Origin, Pem, Secret, TlsIdentity,
+    };
+
+    use super::*;
+
+    fn api(address: &str, tls: bool, advertise_url: Option<&str>) -> Api {
+        let pem = || Pem {
+            origin: Origin::Inline,
+            text: Secret::new("x", &FingerprintKey::random()),
+        };
+        Api {
+            address: address.parse::<HostPort>().unwrap(),
+            advertise_url: advertise_url.map(str::to_owned),
+            auth: ApiAuth::None,
+            credentials: None,
+            jwt: None,
+            oauth2: None,
+            role_mapping: Default::default(),
+            tls: tls.then(|| TlsIdentity {
+                cert: pem(),
+                key: pem(),
+            }),
+        }
+    }
+
+    const GOSSIP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 1, 12));
+
+    #[test]
+    fn the_api_url_is_derived_from_the_listener_and_the_gossip() {
+        assert_eq!(
+            api_url(&api("0.0.0.0:8080", false, None), GOSSIP),
+            "http://10.0.1.12:8080"
+        );
+        assert_eq!(
+            api_url(&api("[::]:8443", true, None), GOSSIP),
+            "https://10.0.1.12:8443"
+        );
+        assert_eq!(
+            api_url(&api("127.0.0.1:9000", false, None), GOSSIP),
+            "http://127.0.0.1:9000"
+        );
+        let v6 = IpAddr::V6("fd00::12".parse().unwrap());
+        assert_eq!(
+            api_url(&api("0.0.0.0:8080", false, None), v6),
+            "http://[fd00::12]:8080"
+        );
+        assert_eq!(
+            api_url(&api("cdc.local:8080", true, None), GOSSIP),
+            "https://cdc.local:8080"
+        );
+    }
+
+    #[test]
+    fn advertise_url_wins_without_its_trailing_slash() {
+        let a = api("0.0.0.0:8080", true, Some("https://cdc-0.cdc.ns.svc:8080/"));
+        assert_eq!(api_url(&a, GOSSIP), "https://cdc-0.cdc.ns.svc:8080");
+    }
+
+    #[test]
+    fn a_file_hash_is_the_config_hash_s_first_16_digits() {
+        let h = file_hash("config_version: 1\n");
+        assert_eq!(h.len(), 16);
+        assert!(config_hash("config_version: 1\n").starts_with(&format!("blake3:{h}")));
+        assert_ne!(h, file_hash("config_version: 1\n# changed\n"));
+    }
+}
````

`crates/data-avalanche-cluster/src/gossip.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index dee4777..725d8ca 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -43,6 +43,10 @@ pub struct NodeMeta {
     pub config_generation: u64,
     /// The build, e.g. `1.0.0`.
     pub build: String,
+    /// Where the others reach its API (`https://cdc-0.cdc.ns.svc:8080`, D-M13b.2).
+    pub api_url: String,
+    /// The first 16 hex digits of its own config file's hash; empty when it can't read it.
+    pub file_hash: String,
 }
 
 /// One incarnation of a node, as foca knows it.
@@ -227,6 +231,7 @@ enum Command {
     Announce(Vec<SocketAddr>),
     Phase(Phase),
     Generation(u64),
+    FileHash(String),
     Leave(oneshot::Sender<()>),
 }
 
@@ -250,6 +255,11 @@ impl Advertiser {
     pub fn set_config_generation(&self, generation: u64) {
         let _ = self.0.send(Command::Generation(generation));
     }
+
+    /// Tells the others the hash of this node's config file; false once the gossip is gone.
+    pub fn set_file_hash(&self, hash: String) -> bool {
+        self.0.send(Command::FileHash(hash)).is_ok()
+    }
 }
 
 /// A running gossip. Dropping it stops the gossip without a leave, as a crash would.
@@ -409,6 +419,19 @@ impl Actor {
                         log(self.swim.change_identity(next, &mut rt));
                         None
                     }
+                    Some(Command::FileHash(file_hash)) => {
+                        let me = self.swim.identity();
+                        let next = NodeIdentity {
+                            rev: me.rev.wrapping_add(1),
+                            meta: NodeMeta {
+                                file_hash,
+                                ..me.meta.clone()
+                            },
+                            ..me.clone()
+                        };
+                        log(self.swim.change_identity(next, &mut rt));
+                        None
+                    }
                     Some(Command::Leave(done)) => {
                         log(self.swim.leave_cluster(&mut rt));
                         Some(Some(done))
@@ -485,6 +508,8 @@ fn seed(addr: SocketAddr) -> NodeIdentity {
             phase: Phase::Joining,
             config_generation: 0,
             build: String::new(),
+            api_url: String::new(),
+            file_hash: String::new(),
         },
     }
 }
````

`crates/data-avalanche-cluster/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 3ebd504..0688f57 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -3,6 +3,7 @@
 //! commit guard it answers, the controller that runs a pipeline's worker while it holds the
 //! lease, and the reconciler that tells each controller whether to own.
 
+pub mod advertised;
 pub mod claim;
 #[cfg(test)]
 mod cluster_tests;
````

`crates/data-avalanche-cluster/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index ad6e2d2..a262574 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -675,6 +675,8 @@ pub(crate) fn members(me: &str, nodes: &[(&str, crate::Phase, bool)]) -> crate::
                     phase: *phase,
                     config_generation: 1,
                     build: "1.0.0".into(),
+                    api_url: String::new(),
+                    file_hash: String::new(),
                 },
                 suspect: *suspect,
             };
@@ -883,6 +885,8 @@ impl TestCluster {
                 phase: crate::Phase::Joining,
                 config_generation: first.pointer.doc.generation,
                 build: "1.0.0".into(),
+                api_url: String::new(),
+                file_hash: String::new(),
             },
         };
         let gossip = crate::Gossip::start(
````

`crates/data-avalanche-cluster/src/wire.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/wire.rs b/crates/data-avalanche-cluster/src/wire.rs
index 37ff5a4..945a861 100644
--- a/crates/data-avalanche-cluster/src/wire.rs
+++ b/crates/data-avalanche-cluster/src/wire.rs
@@ -26,7 +26,9 @@ pub const MAC_LEN: usize = 32;
 pub const MAX_PAYLOAD: usize = 1400;
 
 /// The BLAKE3 key-derivation context of the gossip MAC key.
-const CONTEXT: &str = "data-avalanche 2026-10-20 gossip packet MAC v1";
+/// v2 (1.1.0): the node's metadata gained `api_url` and `file_hash`, which a 1.0 node's codec
+/// can't read; under another key, the two versions drop each other's packets instead.
+const CONTEXT: &str = "data-avalanche 2026-10-20 gossip packet MAC v2";
 
 /// The key gossip packets are authenticated with.
 #[derive(Clone, PartialEq, Eq)]
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 8e30e54..3605945 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -11,8 +11,9 @@ use data_avalanche_api::{Api, Auth, Control, Node, RateLimit, ServeError, Stop,
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
-    ReconcilerSettings, Recorder, Seeds, Udp, advertise, discover, expire_events, interfaces,
-    join as join_gossip, leave, node_identity, record_members, removed_pipelines,
+    ReconcilerSettings, Recorder, Seeds, Udp, advertise, api_url, discover, expire_events,
+    file_hash, interfaces, join as join_gossip, leave, node_identity, record_members,
+    removed_pipelines, watch_file,
 };
 use data_avalanche_config::{
     Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
@@ -211,11 +212,19 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             phase: Phase::Joining,
             config_generation: joined.generation.doc.generation,
             build: env!("CARGO_PKG_VERSION").to_owned(),
+            api_url: api_url(&config.globals.api, addr.ip()),
+            file_hash: file_hash(&text),
         },
     };
     let key = GossipKey::derive(&joined.fingerprint_key);
     let gossip = Gossip::start(me, key, GossipSettings::new(m), Arc::new(udp), inbox);
     tracing::info!(%addr, bound = ?bind, "gossip started");
+    // The file's hash, gossiped again when the file changes under the node (D-M13b.3).
+    let _file = tokio::spawn(watch_file(
+        args.config.clone(),
+        file_hash(&text),
+        gossip.advertiser(),
+    ));
     // Joins, suspicions and departures, each recorded by one node (Admin_Console §6).
     let _membership = tokio::spawn(record_members(recorder.clone(), gossip.members()));
     let seeds = Seeds::new(c);
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-cluster -p data-avalanche-api`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/cluster.rs crates/data-avalanche-api/src/pipelines.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche-cluster/Cargo.toml crates/data-avalanche-cluster/src/advertised.rs crates/data-avalanche-cluster/src/coordinator.rs crates/data-avalanche-cluster/src/discovery.rs crates/data-avalanche-cluster/src/events.rs crates/data-avalanche-cluster/src/gossip.rs crates/data-avalanche-cluster/src/lib.rs crates/data-avalanche-cluster/src/node.rs crates/data-avalanche-cluster/src/test_support.rs crates/data-avalanche-cluster/src/wire.rs crates/data-avalanche/src/run.rs crates/data-avalanche/tests/e2e/cluster.rs
git commit -m "feat(cluster): the gossip's v2, each node's API address and its file's hash; the cluster status's ring owners and lease expiries"
```

---

### Task 3: Gossip figures and a settled membership view (`data-avalanche-cluster`, `-telemetry`)

**Files:**
- Modify: `Cargo.toml`, `crates/data-avalanche-cluster/src/events.rs`, `crates/data-avalanche-cluster/src/gossip.rs`, `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-telemetry/src/lib.rs`, `crates/data-avalanche-telemetry/tests/catalogue.rs`, `crates/data-avalanche-telemetry/tests/metrics.rs`, `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: the gossip actor (M8a); `Metrics` (M7); the membership recorder (M13a).
- Produces: telemetry `Suspicion { Raised, Received }`, `Metrics::gossip_probe_rtt(Duration)`,
  `Metrics::gossip_suspicion(Suspicion)` (series `data_avalanche_gossip_probe_rtt_seconds`,
  `data_avalanche_gossip_suspicions_total{direction}`); cluster `GossipFigures { last_probe:
  Option<(String, DateTime<Utc>)>, probe_rtt: Option<Duration>, raised: Vec<(String,
  DateTime<Utc>)>, received: Vec<DateTime<Utc>> }`, `SUSPICIONS_KEPT` (24 h),
  `Gossip::figures() -> watch::Receiver<GossipFigures>`, `Gossip::report_to(Metrics)`;
  `MEMBERS_SETTLE` (10 s). The workspace's foca gains `unstable-notifications`.

- [ ] **Step 1: Write the failing tests**

The tests: `the_figures_keep_a_probe_s_round_trip_and_suspicions_each_way`, `suspicions_older_than_a_day_are_forgotten`, `a_new_node_records_nothing_until_its_view_settles`, `the_gossip_series_count`, the catalogue's new entries.

`crates/data-avalanche-cluster/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
index bdd7d7f..84bafba 100644
--- a/crates/data-avalanche-cluster/src/events.rs
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -806,6 +820,44 @@ mod tests {
     }
 
     /// The texts of every stored event, newest first.
+    /// F4: a node that turned active on its first peer sees the rest of a long-running cluster
+    /// arrive; none of them is a join. A member that joins once it has settled is.
+    #[tokio::test(start_paused = true)]
+    async fn a_new_node_records_nothing_until_its_view_settles() {
+        use NodePhase::{Active, Joining};
+        let events = store(0);
+        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", Metrics::new());
+        let (view, rx) = watch::channel(members("a", &[("a", 1, false, Joining)]));
+        let _recording = tokio::spawn(record_members(r.clone(), rx));
+        tokio::task::yield_now().await;
+        view.send_replace(members(
+            "a",
+            &[("a", 1, false, Active), ("c", 1, false, Active)],
+        ));
+        tokio::time::sleep(Duration::from_secs(2)).await;
+        view.send_replace(members(
+            "a",
+            &[
+                ("a", 1, false, Active),
+                ("b", 1, false, Active),
+                ("c", 1, false, Active),
+            ],
+        ));
+        tokio::time::sleep(MEMBERS_SETTLE).await;
+        view.send_replace(members(
+            "a",
+            &[
+                ("a", 1, false, Active),
+                ("b", 1, false, Active),
+                ("c", 1, false, Active),
+                ("d", 1, false, Joining),
+            ],
+        ));
+        tokio::time::sleep(Duration::from_millis(10)).await;
+        assert!(r.flush(Duration::from_secs(1)).await);
+        assert_eq!(stored(&events).await, ["d joined (1.1.0)"]);
+    }
+
     async fn stored(events: &Events) -> Vec<String> {
         let page = events.list(&EventQuery::default()).await.unwrap();
         page.events.into_iter().map(|(_, e)| e.text).collect()
````

`crates/data-avalanche-cluster/src/gossip.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index 725d8ca..ef87fc3 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -757,6 +881,56 @@ mod tests {
         );
     }
 
+    /// D-M13b.8: a probe's ping and ack give its round trip. A cut makes each side suspect the
+    /// other (raised); once healed, a suspicion reaches its subject (received), which refutes it
+    /// or, declared down first, rejoins: which side, the dissemination decides.
+    #[tokio::test(start_paused = true)]
+    async fn the_figures_keep_a_probe_s_round_trip_and_suspicions_each_way() {
+        let hub = Hub::new();
+        let (a, b, c) = three(&hub).await;
+        let metrics = Metrics::new();
+        a.report_to(metrics.clone());
+        eventually("a probed and was acked", || {
+            let f = a.figures().borrow().clone();
+            f.last_probe.is_some() && f.probe_rtt.is_some()
+        })
+        .await;
+        hub.cut(&[at(1), at(2)], &[at(3)]);
+        eventually("a or b suspects c", || {
+            [&a, &b]
+                .iter()
+                .any(|g| g.figures().borrow().raised.iter().any(|(n, _)| n == "c"))
+        })
+        .await;
+        hub.heal();
+        eventually("a suspicion received", || {
+            [&a, &b, &c]
+                .iter()
+                .any(|g| !g.figures().borrow().received.is_empty())
+        })
+        .await;
+        eventually("c back", || names(&a) == ["a", "b", "c"]).await;
+        let text = metrics.encode();
+        assert!(
+            text.contains("data_avalanche_gossip_probe_rtt_seconds_count"),
+            "{text}"
+        );
+    }
+
+    #[test]
+    fn suspicions_older_than_a_day_are_forgotten() {
+        let now = Utc::now();
+        let old = now - chrono::Duration::hours(25);
+        let mut f = GossipFigures {
+            raised: vec![("b".into(), old), ("c".into(), now)],
+            received: vec![old, now],
+            ..GossipFigures::default()
+        };
+        f.forget_before(now);
+        assert_eq!(f.raised, [("c".to_owned(), now)]);
+        assert_eq!(f.received, [now]);
+    }
+
     /// A node on another anchor has another key: its packets don't open, so it never joins.
     #[tokio::test(start_paused = true)]
     async fn a_node_with_another_key_cant_join() {
````

`crates/data-avalanche-cluster/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 0688f57..0ad6b55 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -37,10 +37,12 @@ pub use coordinator::{
 pub use discovery::{Seeds, advertise, discover, interfaces};
 pub use error::ClusterError;
 pub use events::{
-    EVENT_QUEUE, PRUNE_EVERY, Recorder, WRITE_RETRIES, expire_events, record_members,
+    EVENT_QUEUE, MEMBERS_SETTLE, PRUNE_EVERY, Recorder, WRITE_RETRIES, expire_events,
+    record_members,
 };
 pub use gossip::{
-    Advertiser, Announcer, Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase,
+    Advertiser, Announcer, Gossip, GossipFigures, GossipSettings, Member, Members, NodeIdentity,
+    NodeMeta, Phase, SUSPICIONS_KEPT,
 };
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
 pub use node::{JOIN_WAIT, join, leave};
````

`crates/data-avalanche-telemetry/tests/catalogue.rs`:

````diff
diff --git a/crates/data-avalanche-telemetry/tests/catalogue.rs b/crates/data-avalanche-telemetry/tests/catalogue.rs
index d148002..aaa1dc9 100644
--- a/crates/data-avalanche-telemetry/tests/catalogue.rs
+++ b/crates/data-avalanche-telemetry/tests/catalogue.rs
@@ -65,6 +65,9 @@ const CATALOGUE: &[(&str, Option<&[&str]>)] = &[
     // Events (Admin_Console §6)
     ("data_avalanche_events_written_total", None),
     ("data_avalanche_events_dropped_total", None),
+    // Gossip (D-M13b.8); `gossip_suspicions_total{direction}`, a node series with a label of
+    // its own, is checked in `metrics.rs`.
+    ("data_avalanche_gossip_probe_rtt_seconds_count", None),
 ];
 
 /// The per-table series `sink.metrics.per_table` adds `table` to, with their other labels.
@@ -133,6 +136,7 @@ fn everything(per_table: bool) -> Metrics {
     m.owned_by(1);
     m.set_cluster_members(1);
     m.set_ring_generation(1);
+    m.gossip_probe_rtt(Duration::from_millis(1));
     m
 }
 
````

`crates/data-avalanche-telemetry/tests/metrics.rs`:

````diff
diff --git a/crates/data-avalanche-telemetry/tests/metrics.rs b/crates/data-avalanche-telemetry/tests/metrics.rs
index 6cfdf69..2ff2694 100644
--- a/crates/data-avalanche-telemetry/tests/metrics.rs
+++ b/crates/data-avalanche-telemetry/tests/metrics.rs
@@ -2,7 +2,7 @@ use std::time::Duration;
 
 use data_avalanche_telemetry::{
     LeaseState, Metrics, PipelineLabels, PipelineStatus, RenewFailure, RowsRead, SnapshotState,
-    Totals,
+    Suspicion, Totals,
 };
 
 const L: &str = r#"pipeline="orders",source_host="db1",source_db="shop""#;
@@ -372,6 +372,28 @@ fn the_node_reports_its_config_generation() {
     );
 }
 
+/// M13b: a probe's round trip, and the suspicions each way.
+#[test]
+fn the_gossip_series_count() {
+    let m = Metrics::new();
+    m.gossip_probe_rtt(Duration::from_millis(2));
+    m.gossip_suspicion(Suspicion::Raised);
+    m.gossip_suspicion(Suspicion::Raised);
+    m.gossip_suspicion(Suspicion::Received);
+    let text = m.encode();
+    assert_eq!(
+        samples(&text, "data_avalanche_gossip_probe_rtt_seconds_count"),
+        ["data_avalanche_gossip_probe_rtt_seconds_count 1"]
+    );
+    assert_eq!(
+        samples(&text, "data_avalanche_gossip_suspicions_total"),
+        [
+            "data_avalanche_gossip_suspicions_total{direction=\"raised\"} 2",
+            "data_avalanche_gossip_suspicions_total{direction=\"received\"} 1"
+        ]
+    );
+}
+
 /// M13a: the event recorder's two node counters, without labels.
 #[test]
 fn the_event_counters_count() {
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-cluster -p data-avalanche-telemetry`
Expected: the build fails: no `Suspicion`, `gossip_probe_rtt`, `figures` or `MEMBERS_SETTLE`.

- [ ] **Step 3: Implement**

`Cargo.toml`:

````diff
diff --git a/Cargo.toml b/Cargo.toml
index ea819f9..51da7c5 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -109,7 +109,7 @@ tracing-opentelemetry = { version = "=0.34.0", default-features = false }
 # the host's addresses, for an unset `advertise_addr`.
 xxhash-rust = { version = "0.8", features = ["xxh3"] }
 if-addrs = "0.15"
-foca = { version = "=2.0.0", default-features = false, features = ["std", "serde", "postcard-codec"] }
+foca = { version = "=2.0.0", default-features = false, features = ["std", "serde", "postcard-codec", "unstable-notifications"] }
 rand = { version = "0.10", default-features = false, features = ["std", "std_rng", "sys_rng"] }
 
 # Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
````

`crates/data-avalanche-cluster/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
index bdd7d7f..84bafba 100644
--- a/crates/data-avalanche-cluster/src/events.rs
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -403,11 +403,6 @@ pub(crate) fn member_changes(before: &Members, after: &Members) -> Vec<(String,
 /// first by name of the active, unsuspected members other than `subject`. Views converge, so
 /// one node records it; a view that hasn't may record it twice or not at all.
 pub(crate) fn records(before: &Members, after: &Members, subject: &str) -> bool {
-    let active = |m: &Members| {
-        m.nodes
-            .get(&m.me)
-            .is_some_and(|n| n.meta.phase == NodePhase::Active)
-    };
     active(before)
         && active(after)
         && after
@@ -417,25 +412,44 @@ pub(crate) fn records(before: &Members, after: &Members, subject: &str) -> bool
             .is_some_and(|(name, _)| *name == after.me)
 }
 
+/// How long a node is `Active` before it records membership changes (D-M13b.14).
+pub const MEMBERS_SETTLE: Duration = Duration::from_secs(10);
+
 /// Records the members' changes this node is the one to record, until the gossip stops. A node
-/// turns `Active` once it sees one peer, before its view has converged: when the rest of a
-/// long-running cluster arrives in its later views, it can record `member.joined` for members
-/// that were up long before it.
+/// turns `Active` once it sees one peer, before its view has converged, so it records nothing
+/// until it has been `Active` for [`MEMBERS_SETTLE`]: the rest of a long-running cluster
+/// arriving in its later views is its baseline, not a run of joins.
 pub async fn record_members(recorder: Recorder, mut members: watch::Receiver<Members>) {
     let mut before = members.borrow_and_update().clone();
+    let mut active_since = active(&before).then(tokio::time::Instant::now);
     while members.changed().await.is_ok() {
         let after = members.borrow_and_update().clone();
-        for (subject, c) in member_changes(&before, &after) {
-            if records(&before, &after, &subject) {
-                let mut e = recorder.event(c.kind, c.severity, c.text);
-                e.object = c.object;
-                recorder.record(e);
+        let now = tokio::time::Instant::now();
+        active_since = if active(&after) {
+            Some(active_since.unwrap_or(now))
+        } else {
+            None
+        };
+        if active_since.is_some_and(|since| now.duration_since(since) >= MEMBERS_SETTLE) {
+            for (subject, c) in member_changes(&before, &after) {
+                if records(&before, &after, &subject) {
+                    let mut e = recorder.event(c.kind, c.severity, c.text);
+                    e.object = c.object;
+                    recorder.record(e);
+                }
             }
         }
         before = after;
     }
 }
 
+/// This node is `Active` in its own view.
+fn active(m: &Members) -> bool {
+    m.nodes
+        .get(&m.me)
+        .is_some_and(|n| n.meta.phase == NodePhase::Active)
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::atomic::{AtomicUsize, Ordering};
````

`crates/data-avalanche-cluster/src/gossip.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index 725d8ca..ef87fc3 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -15,7 +15,12 @@ use std::sync::Arc;
 use std::time::Duration;
 
 use bytes::Bytes;
-use foca::{AccumulatingRuntime, Foca, NoCustomBroadcast, PeriodicParams, PostcardCodec, State};
+use chrono::{DateTime, Utc};
+use data_avalanche_telemetry::{Metrics, Suspicion};
+use foca::{
+    AccumulatingRuntime, Foca, Incarnation, Message, NoCustomBroadcast, OwnedNotification,
+    PeriodicParams, PostcardCodec, ProbeNumber, State,
+};
 use rand::rngs::StdRng;
 use serde::{Deserialize, Serialize};
 use tokio::sync::{mpsc, oneshot, watch};
@@ -49,6 +54,37 @@ pub struct NodeMeta {
     pub file_hash: String,
 }
 
+/// What a node's gossip saw of its probes, for its page (D-M13b.8).
+#[derive(Debug, Clone, Default, PartialEq, Eq)]
+pub struct GossipFigures {
+    /// The member the last probe pinged, and when.
+    pub last_probe: Option<(String, DateTime<Utc>)>,
+    /// That probe's round trip, once its ack came.
+    pub probe_rtt: Option<Duration>,
+    /// The members this node suspected (its probe of them failed), and when, in the last
+    /// [`SUSPICIONS_KEPT`].
+    pub raised: Vec<(String, DateTime<Utc>)>,
+    /// When the others suspected this node, in the last [`SUSPICIONS_KEPT`]: it refuted (foca
+    /// raises its own incarnation, which its packets carry), or it was declared down and
+    /// rejoined.
+    pub received: Vec<DateTime<Utc>>,
+}
+
+/// How long the figures keep a suspicion.
+pub const SUSPICIONS_KEPT: Duration = Duration::from_secs(24 * 3600);
+
+impl GossipFigures {
+    /// Forgets the suspicions older than [`SUSPICIONS_KEPT`] before `now`.
+    fn forget_before(&mut self, now: DateTime<Utc>) {
+        let kept = chrono::Duration::from_std(SUSPICIONS_KEPT).unwrap_or(chrono::Duration::MAX);
+        let since = now
+            .checked_sub_signed(kept)
+            .unwrap_or(DateTime::<Utc>::MIN_UTC);
+        self.raised.retain(|(_, at)| *at >= since);
+        self.received.retain(|at| *at >= since);
+    }
+}
+
 /// One incarnation of a node, as foca knows it.
 #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
 pub struct NodeIdentity {
@@ -232,6 +268,7 @@ enum Command {
     Phase(Phase),
     Generation(u64),
     FileHash(String),
+    Metrics(Metrics),
     Leave(oneshot::Sender<()>),
 }
 
@@ -267,6 +304,7 @@ impl Advertiser {
 pub struct Gossip {
     commands: mpsc::UnboundedSender<Command>,
     members: watch::Receiver<Members>,
+    figures: watch::Receiver<GossipFigures>,
     task: tokio::task::JoinHandle<()>,
 }
 
@@ -294,6 +332,7 @@ impl Gossip {
             PostcardCodec,
         );
         let (members_tx, members) = watch::channel(members_of(&me, std::iter::empty()));
+        let (figures_tx, figures) = watch::channel(GossipFigures::default());
         let (commands, rx) = mpsc::unbounded_channel();
         let actor = Actor {
             swim,
@@ -301,6 +340,10 @@ impl Gossip {
             settings,
             wire,
             members: members_tx,
+            figures: figures_tx,
+            metrics: None,
+            probe: None,
+            incarnation: None,
             size: 1,
             dropped: 0,
             warned: None,
@@ -309,10 +352,21 @@ impl Gossip {
         Self {
             commands,
             members,
+            figures,
             task,
         }
     }
 
+    /// What the gossip saw of its probes, as it changes.
+    pub fn figures(&self) -> watch::Receiver<GossipFigures> {
+        self.figures.clone()
+    }
+
+    /// Counts the probes' round trips and the suspicions in `metrics` from now on.
+    pub fn report_to(&self, metrics: Metrics) {
+        let _ = self.commands.send(Command::Metrics(metrics));
+    }
+
     /// The members, as they change.
     pub fn members(&self) -> watch::Receiver<Members> {
         self.members.clone()
@@ -362,6 +416,12 @@ struct Actor {
     settings: GossipSettings,
     wire: Arc<dyn Wire>,
     members: watch::Sender<Members>,
+    figures: watch::Sender<GossipFigures>,
+    metrics: Option<Metrics>,
+    /// The last probe's ping not yet acked: its number and when it went.
+    probe: Option<(ProbeNumber, Instant)>,
+    /// This node's incarnation as its last packet carried it.
+    incarnation: Option<Incarnation>,
     /// The cluster size foca's settings were last computed for.
     size: usize,
     dropped: u64,
@@ -432,6 +492,10 @@ impl Actor {
                         log(self.swim.change_identity(next, &mut rt));
                         None
                     }
+                    Some(Command::Metrics(metrics)) => {
+                        self.metrics = Some(metrics);
+                        None
+                    }
                     Some(Command::Leave(done)) => {
                         log(self.swim.leave_cluster(&mut rt));
                         Some(Some(done))
@@ -451,7 +515,7 @@ impl Actor {
                 });
             }
             while let Some(n) = rt.to_notify() {
-                tracing::debug!("gossip: {n:?}");
+                self.notified(n);
             }
             if let Some(done) = done {
                 if let Some(done) = done {
@@ -483,6 +547,66 @@ impl Actor {
         });
     }
 
+    /// Keeps the figures (D-M13b.8): a ping this node sends starts a probe, its ack ends it; a
+    /// failed probe is a suspicion raised; a packet carrying a higher incarnation of this node
+    /// is a suspicion of it refuted, and a rejoin one that ended in its being declared down.
+    fn notified(&mut self, n: OwnedNotification<NodeIdentity>) {
+        let now = Utc::now();
+        match n {
+            OwnedNotification::DataSent(h) => {
+                if let Message::Ping(probe) = h.message {
+                    self.probe = Some((probe, Instant::now()));
+                    self.figures.send_modify(|f| {
+                        f.last_probe = Some((h.dst.node_id.clone(), now));
+                        f.probe_rtt = None;
+                    });
+                }
+                if self.incarnation.is_some_and(|i| h.src_incarnation > i) {
+                    self.suspected(now);
+                }
+                self.incarnation = Some(h.src_incarnation);
+            }
+            OwnedNotification::DataReceived(h) => {
+                if let (Message::Ack(probe), Some((sent, at))) = (h.message, self.probe)
+                    && probe == sent
+                {
+                    self.probe = None;
+                    let rtt = at.elapsed();
+                    self.figures.send_modify(|f| f.probe_rtt = Some(rtt));
+                    if let Some(m) = &self.metrics {
+                        m.gossip_probe_rtt(rtt);
+                    }
+                }
+            }
+            OwnedNotification::ProbeFailed(_, member) => {
+                self.figures.send_modify(|f| {
+                    f.raised.push((member.node_id, now));
+                    f.forget_before(now);
+                });
+                if let Some(m) = &self.metrics {
+                    m.gossip_suspicion(Suspicion::Raised);
+                }
+            }
+            OwnedNotification::Rejoin(me) => {
+                tracing::info!("gossip: declared down by the others; rejoined as {me:?}");
+                self.incarnation = None;
+                self.suspected(now);
+            }
+            other => tracing::debug!("gossip: {other:?}"),
+        }
+    }
+
+    /// The others suspected this node.
+    fn suspected(&mut self, now: DateTime<Utc>) {
+        self.figures.send_modify(|f| {
+            f.received.push(now);
+            f.forget_before(now);
+        });
+        if let Some(m) = &self.metrics {
+            m.gossip_suspicion(Suspicion::Received);
+        }
+    }
+
     /// A packet that doesn't open: another cluster's, a corrupted one, or forged.
     fn dropped(&mut self) {
         self.dropped += 1;
````

`crates/data-avalanche-telemetry/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-telemetry/src/lib.rs b/crates/data-avalanche-telemetry/src/lib.rs
index 84fed04..dfa8b60 100644
--- a/crates/data-avalanche-telemetry/src/lib.rs
+++ b/crates/data-avalanche-telemetry/src/lib.rs
@@ -76,6 +76,24 @@ impl LeaseState {
     }
 }
 
+/// `data_avalanche_gossip_suspicions_total{direction}` (D-M13b.8).
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum Suspicion {
+    /// This node's probe of a member failed: it suspects the member.
+    Raised,
+    /// Another member suspected this node, and it refuted.
+    Received,
+}
+
+impl Suspicion {
+    fn label(self) -> &'static str {
+        match self {
+            Self::Raised => "raised",
+            Self::Received => "received",
+        }
+    }
+}
+
 /// `data_avalanche_lease_renew_failures_total{class}` (Clustering §19).
 #[derive(Debug, Clone, Copy, PartialEq, Eq)]
 pub enum RenewFailure {
@@ -213,6 +231,8 @@ struct Families {
     ring_generation: Gauge,
     events_written: Counter,
     events_dropped: Counter,
+    gossip_probe_rtt: Histogram,
+    gossip_suspicions: Family<Labels, Counter>,
     wal_lag_bytes: Family<Labels, Gauge>,
     source_reconnects: Family<Labels, Counter>,
     snapshot_rows_done: Family<Labels, Gauge>,
@@ -235,6 +255,11 @@ struct Families {
     sink_soft_dropped_columns: Family<Labels, Gauge>,
 }
 
+fn probe_rtt() -> Histogram {
+    // 0.5 ms … ~1 s.
+    Histogram::new(exponential_buckets(0.0005, 2.0, 12))
+}
+
 fn renew_latency() -> Histogram {
     // 5 ms … ~10 s.
     Histogram::new(exponential_buckets(0.005, 2.0, 12))
@@ -276,6 +301,8 @@ impl Metrics {
             ring_generation: Gauge::default(),
             events_written: Counter::default(),
             events_dropped: Counter::default(),
+            gossip_probe_rtt: probe_rtt(),
+            gossip_suspicions: Family::default(),
             wal_lag_bytes: Family::default(),
             source_reconnects: Family::default(),
             snapshot_rows_done: Family::default(),
@@ -339,6 +366,16 @@ impl Metrics {
             "Events this node couldn't write: its queue was full, or the store kept failing",
             f.events_dropped.clone(),
         );
+        r.register(
+            "gossip_probe_rtt_seconds",
+            "A gossip probe's round trip, from its ping to its ack",
+            f.gossip_probe_rtt.clone(),
+        );
+        r.register(
+            "gossip_suspicions",
+            "Suspicions this node raised of a member, or received of itself and refuted",
+            f.gossip_suspicions.clone(),
+        );
         r.register(
             "config_generation",
             "The config generation this node applies",
@@ -481,6 +518,18 @@ impl Metrics {
         self.families.events_dropped.inc();
     }
 
+    /// A gossip probe's round trip.
+    pub fn gossip_probe_rtt(&self, rtt: Duration) {
+        self.families.gossip_probe_rtt.observe(rtt.as_secs_f64());
+    }
+
+    pub fn gossip_suspicion(&self, direction: Suspicion) {
+        self.families
+            .gossip_suspicions
+            .get_or_create(&vec![("direction", direction.label().to_owned())])
+            .inc();
+    }
+
     pub fn set_ring_generation(&self, generation: u64) {
         self.families.ring_generation.set(gauge(generation));
     }
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 3605945..754daf7 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -219,6 +219,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     let key = GossipKey::derive(&joined.fingerprint_key);
     let gossip = Gossip::start(me, key, GossipSettings::new(m), Arc::new(udp), inbox);
     tracing::info!(%addr, bound = ?bind, "gossip started");
+    gossip.report_to(metrics.clone());
     // The file's hash, gossiped again when the file changes under the node (D-M13b.3).
     let _file = tokio::spawn(watch_file(
         args.config.clone(),
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-cluster -p data-avalanche-telemetry`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/data-avalanche-cluster/src/events.rs crates/data-avalanche-cluster/src/gossip.rs crates/data-avalanche-cluster/src/lib.rs crates/data-avalanche-telemetry/src/lib.rs crates/data-avalanche-telemetry/tests/catalogue.rs crates/data-avalanche-telemetry/tests/metrics.rs crates/data-avalanche/src/run.rs
git commit -m "feat(cluster): the gossip's probes and suspicions; the membership recorder waits for a settled view"
```

---

### Task 4: Process figures (`data-avalanche-telemetry`)

**Files:**
- Create: `crates/data-avalanche-telemetry/src/process.rs`
- Modify: `Cargo.lock`, `crates/data-avalanche-telemetry/Cargo.toml`, `crates/data-avalanche-telemetry/src/lib.rs`, `crates/data-avalanche-telemetry/tests/metrics.rs`, `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: `Metrics` (M7).
- Produces (telemetry crate root): `PROCESS_SAMPLE` (15 s); `Reading { memory_bytes:
  Option<u64>, memory_limit_bytes: Option<u64>, cpu_seconds: Option<f64>, cpu_limit_cores:
  Option<f64> }`; `ProcessFigures { reading: Reading, cpu_cores: Option<f64> }` and
  `ProcessFigures::next(&self, Reading, Duration) -> Self`; `read_process_at(&Path) -> Reading`,
  `read_process() -> Reading`; `sample_process(Metrics, watch::Sender<ProcessFigures>)` (async,
  never ends); `Metrics::process(&Reading)`. `run.rs` spawns the sampler.

- [ ] **Step 1: Write the failing tests**

The tests: `the_process_series_follow_the_readings` (`tests/metrics.rs`); in the new module (Step 3): `/proc`, cgroup v2 and v1, `no_limit_reads_as_none`, `cgroup_v1_s_sentinel_is_no_limit`, the CPU's rate.

`crates/data-avalanche-telemetry/tests/metrics.rs`:

````diff
diff --git a/crates/data-avalanche-telemetry/tests/metrics.rs b/crates/data-avalanche-telemetry/tests/metrics.rs
index 2ff2694..6d15249 100644
--- a/crates/data-avalanche-telemetry/tests/metrics.rs
+++ b/crates/data-avalanche-telemetry/tests/metrics.rs
@@ -1,8 +1,8 @@
 use std::time::Duration;
 
 use data_avalanche_telemetry::{
-    LeaseState, Metrics, PipelineLabels, PipelineStatus, RenewFailure, RowsRead, SnapshotState,
-    Suspicion, Totals,
+    LeaseState, Metrics, PipelineLabels, PipelineStatus, Reading, RenewFailure, RowsRead,
+    SnapshotState, Suspicion, Totals,
 };
 
 const L: &str = r#"pipeline="orders",source_host="db1",source_db="shop""#;
@@ -372,6 +372,40 @@ fn the_node_reports_its_config_generation() {
     );
 }
 
+/// M13b: the process figures under their usual names; a limit only when there is one.
+#[test]
+fn the_process_series_follow_the_readings() {
+    let m = Metrics::new();
+    m.process(&Reading {
+        memory_bytes: Some(1 << 30),
+        memory_limit_bytes: Some(2 << 30),
+        cpu_seconds: Some(12.5),
+        cpu_limit_cores: Some(2.0),
+    });
+    m.process(&Reading {
+        cpu_seconds: Some(14.0),
+        memory_limit_bytes: Some(2 << 30),
+        ..Reading::default()
+    });
+    let text = m.encode();
+    assert_eq!(
+        samples(&text, "process_resident_memory_bytes"),
+        ["process_resident_memory_bytes 1073741824"]
+    );
+    assert_eq!(
+        samples(&text, "process_cpu_seconds_total"),
+        ["process_cpu_seconds_total 14.0"]
+    );
+    assert_eq!(
+        samples(&text, "data_avalanche_process_memory_limit_bytes"),
+        ["data_avalanche_process_memory_limit_bytes 2147483648"]
+    );
+    assert!(
+        samples(&text, "data_avalanche_process_cpu_limit_cores").is_empty(),
+        "no CPU limit in the last reading: {text}"
+    );
+}
+
 /// M13b: a probe's round trip, and the suspicions each way.
 #[test]
 fn the_gossip_series_count() {
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-telemetry`
Expected: the build fails: no `Reading` or `Metrics::process`.

- [ ] **Step 3: Implement**

`Cargo.lock`:

````diff
diff --git a/Cargo.lock b/Cargo.lock
index ddf946a..4f065b6 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -2023,6 +2023,7 @@ dependencies = [
  "prometheus-client",
  "rcgen",
  "reqwest 0.13.5",
+ "tempfile",
  "thiserror 2.0.21",
  "tokio",
  "tracing",
````

`crates/data-avalanche-telemetry/Cargo.toml`:

````diff
diff --git a/crates/data-avalanche-telemetry/Cargo.toml b/crates/data-avalanche-telemetry/Cargo.toml
index ceaac9b..fe0ccc9 100644
--- a/crates/data-avalanche-telemetry/Cargo.toml
+++ b/crates/data-avalanche-telemetry/Cargo.toml
@@ -15,6 +15,7 @@ opentelemetry-otlp.workspace = true
 opentelemetry_sdk.workspace = true
 prometheus-client.workspace = true
 thiserror.workspace = true
+tokio = { workspace = true, features = ["sync", "time"] }
 tracing.workspace = true
 tracing-opentelemetry.workspace = true
 tracing-subscriber = { workspace = true, features = ["registry"] }
@@ -22,5 +23,6 @@ tracing-subscriber = { workspace = true, features = ["registry"] }
 [dev-dependencies]
 data-avalanche-testkit.workspace = true
 rcgen.workspace = true
+tempfile.workspace = true
 reqwest = { workspace = true, features = ["blocking"] }
 tokio = { workspace = true, features = ["rt", "macros", "net", "io-util", "time"] }
````

`crates/data-avalanche-telemetry/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-telemetry/src/lib.rs b/crates/data-avalanche-telemetry/src/lib.rs
index dfa8b60..6939a80 100644
--- a/crates/data-avalanche-telemetry/src/lib.rs
+++ b/crates/data-avalanche-telemetry/src/lib.rs
@@ -11,8 +11,12 @@ use std::sync::atomic::AtomicU64;
 use std::sync::{Arc, Mutex, PoisonError};
 use std::time::Duration;
 
+mod process;
 mod traces;
 
+pub use process::{
+    PROCESS_SAMPLE, ProcessFigures, Reading, read_process, read_process_at, sample_process,
+};
 pub use traces::{Traces, TracesError, shown, trace_ids};
 
 use prometheus_client::encoding::text::encode;
@@ -233,6 +237,12 @@ struct Families {
     events_dropped: Counter,
     gossip_probe_rtt: Histogram,
     gossip_suspicions: Family<Labels, Counter>,
+    process_resident_memory: Gauge,
+    process_cpu_seconds: Counter<f64, AtomicU64>,
+    /// The CPU seconds counted so far: a reading adds what is new.
+    process_cpu_counted: Mutex<f64>,
+    process_memory_limit: Family<Labels, Gauge>,
+    process_cpu_limit: Family<Labels, Seconds>,
     wal_lag_bytes: Family<Labels, Gauge>,
     source_reconnects: Family<Labels, Counter>,
     snapshot_rows_done: Family<Labels, Gauge>,
@@ -303,6 +313,11 @@ impl Metrics {
             events_dropped: Counter::default(),
             gossip_probe_rtt: probe_rtt(),
             gossip_suspicions: Family::default(),
+            process_resident_memory: Gauge::default(),
+            process_cpu_seconds: Counter::default(),
+            process_cpu_counted: Mutex::new(0.0),
+            process_memory_limit: Family::default(),
+            process_cpu_limit: Family::default(),
             wal_lag_bytes: Family::default(),
             source_reconnects: Family::default(),
             snapshot_rows_done: Family::default(),
@@ -325,7 +340,30 @@ impl Metrics {
             sink_soft_dropped_columns: Family::default(),
         };
         let f = &families;
-        let mut r = Registry::with_prefix("data_avalanche");
+        // The process figures carry the names every Prometheus client gives them; the rest are
+        // the node's own, under the prefix.
+        let mut root = Registry::default();
+        root.register(
+            "process_resident_memory_bytes",
+            "Resident memory of the process",
+            f.process_resident_memory.clone(),
+        );
+        root.register(
+            "process_cpu_seconds",
+            "User and system CPU time the process used",
+            f.process_cpu_seconds.clone(),
+        );
+        let r = root.sub_registry_with_prefix("data_avalanche");
+        r.register(
+            "process_memory_limit_bytes",
+            "The container's memory limit, when it has one",
+            f.process_memory_limit.clone(),
+        );
+        r.register(
+            "process_cpu_limit_cores",
+            "The container's CPU limit in cores, when it has one",
+            f.process_cpu_limit.clone(),
+        );
         r.register(
             "pipeline_status",
             "The pipeline's state: 1 on its current status",
@@ -483,7 +521,7 @@ impl Metrics {
             f.sink_soft_dropped_columns.clone(),
         );
         Self {
-            registry: Arc::new(r),
+            registry: Arc::new(root),
             families: Arc::new(families),
         }
     }
@@ -518,6 +556,39 @@ impl Metrics {
         self.families.events_dropped.inc();
     }
 
+    /// A reading of the process figures (D-M13b.7). A limit the container doesn't have is no
+    /// series at all.
+    pub fn process(&self, r: &Reading) {
+        let f = &self.families;
+        if let Some(bytes) = r.memory_bytes {
+            f.process_resident_memory.set(gauge(bytes));
+        }
+        if let Some(seconds) = r.cpu_seconds {
+            let mut counted = f
+                .process_cpu_counted
+                .lock()
+                .unwrap_or_else(PoisonError::into_inner);
+            if seconds > *counted {
+                f.process_cpu_seconds.inc_by(seconds - *counted);
+                *counted = seconds;
+            }
+        }
+        match r.memory_limit_bytes {
+            Some(bytes) => {
+                f.process_memory_limit
+                    .get_or_create(&vec![])
+                    .set(gauge(bytes));
+            }
+            None => f.process_memory_limit.clear(),
+        }
+        match r.cpu_limit_cores {
+            Some(cores) => {
+                f.process_cpu_limit.get_or_create(&vec![]).set(cores);
+            }
+            None => f.process_cpu_limit.clear(),
+        }
+    }
+
     /// A gossip probe's round trip.
     pub fn gossip_probe_rtt(&self, rtt: Duration) {
         self.families.gossip_probe_rtt.observe(rtt.as_secs_f64());
````

`crates/data-avalanche-telemetry/src/process.rs`:

````diff
diff --git a/crates/data-avalanche-telemetry/src/process.rs b/crates/data-avalanche-telemetry/src/process.rs
new file mode 100644
index 0000000..37e2a03
--- /dev/null
+++ b/crates/data-avalanche-telemetry/src/process.rs
@@ -0,0 +1,272 @@
+//! The process's memory and CPU, and its container's limits (Admin_Console §12, D-M13b.7): read
+//! from `/proc/self` and the cgroup on Linux, absent elsewhere. Every figure is optional: a host
+//! without a limit has none, never a `0` or a sentinel standing in for "unlimited".
+
+use std::path::Path;
+use std::time::{Duration, Instant};
+
+use tokio::sync::watch;
+
+use crate::Metrics;
+
+/// How often the sampler reads the figures; the CPU's rate is over this interval.
+pub const PROCESS_SAMPLE: Duration = Duration::from_secs(15);
+
+/// Linux's `USER_HZ`, the unit of `/proc/self/stat`'s CPU times, on every architecture it runs
+/// on (the process can't ask `sysconf` without `unsafe`).
+const TICKS_PER_SECOND: f64 = 100.0;
+
+/// A cgroup v1 memory limit at or over this is the kernel's "no limit" (`PAGE_COUNTER_MAX`
+/// pages, rounded).
+const V1_NO_LIMIT: u64 = 1 << 60;
+
+/// One reading of the figures.
+#[derive(Debug, Clone, Copy, Default, PartialEq)]
+pub struct Reading {
+    pub memory_bytes: Option<u64>,
+    pub memory_limit_bytes: Option<u64>,
+    /// User and system CPU time since the process started.
+    pub cpu_seconds: Option<f64>,
+    pub cpu_limit_cores: Option<f64>,
+}
+
+/// What a node's page shows: the last reading, and the CPU used over the interval before it.
+#[derive(Debug, Clone, Copy, Default, PartialEq)]
+pub struct ProcessFigures {
+    pub reading: Reading,
+    /// Cores used on average over the last [`PROCESS_SAMPLE`]; `None` until two readings.
+    pub cpu_cores: Option<f64>,
+}
+
+/// `VmRSS` of `/proc/self/status`, in bytes.
+fn resident(status: &str) -> Option<u64> {
+    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
+    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
+    kb.checked_mul(1024)
+}
+
+/// `utime + stime` of `/proc/self/stat`, in seconds. The fields are counted after the command's
+/// closing parenthesis, which a command name may itself contain.
+fn cpu_seconds(stat: &str) -> Option<f64> {
+    let rest = &stat[stat.rfind(')')? + 1..];
+    let fields: Vec<&str> = rest.split_whitespace().collect();
+    // After the `)`: state is field 3 of the line, so utime (14) and stime (15) are at 11, 12.
+    let utime: f64 = fields.get(11)?.parse().ok()?;
+    let stime: f64 = fields.get(12)?.parse().ok()?;
+    Some((utime + stime) / TICKS_PER_SECOND)
+}
+
+/// cgroup v2's `memory.max`: bytes, or `max` for none.
+fn v2_memory(text: &str) -> Option<u64> {
+    text.trim().parse().ok()
+}
+
+/// cgroup v2's `cpu.max`: `<quota> <period>` in microseconds, or `max <period>` for none.
+fn v2_cpu(text: &str) -> Option<f64> {
+    let mut parts = text.split_whitespace();
+    let quota: f64 = parts.next()?.parse().ok()?;
+    let period: f64 = parts.next()?.parse().ok()?;
+    (period > 0.0).then(|| quota / period)
+}
+
+/// cgroup v1's `memory.limit_in_bytes`: a huge number for none.
+fn v1_memory(text: &str) -> Option<u64> {
+    text.trim().parse().ok().filter(|n| *n < V1_NO_LIMIT)
+}
+
+/// cgroup v1's `cpu.cfs_quota_us` (`-1` for none) over `cpu.cfs_period_us`.
+fn v1_cpu(quota: &str, period: &str) -> Option<f64> {
+    let quota: f64 = quota.trim().parse().ok()?;
+    let period: f64 = period.trim().parse().ok()?;
+    (quota > 0.0 && period > 0.0).then(|| quota / period)
+}
+
+/// The figures under `root` (`/` on a real host), so a test can lay out a fake `/proc` and
+/// `/sys/fs/cgroup`. cgroup v2 is read at the process's own cgroup (`/proc/self/cgroup`'s
+/// `0::<path>`), then at the hierarchy's root; v1 at its controllers' roots.
+pub fn read_process_at(root: &Path) -> Reading {
+    let read = |p: &str| std::fs::read_to_string(root.join(p.trim_start_matches('/'))).ok();
+    let mut r = Reading {
+        memory_bytes: read("/proc/self/status").as_deref().and_then(resident),
+        cpu_seconds: read("/proc/self/stat").as_deref().and_then(cpu_seconds),
+        ..Reading::default()
+    };
+    let own = read("/proc/self/cgroup")
+        .and_then(|t| {
+            t.lines()
+                .find_map(|l| l.strip_prefix("0::").map(|p| p.trim().to_owned()))
+        })
+        .unwrap_or_default();
+    let v2 = [format!("/sys/fs/cgroup{own}"), "/sys/fs/cgroup".to_owned()]
+        .into_iter()
+        .find(|dir| read(&format!("{dir}/cgroup.controllers")).is_some());
+    match v2 {
+        Some(dir) => {
+            r.memory_limit_bytes = read(&format!("{dir}/memory.max"))
+                .as_deref()
+                .and_then(v2_memory);
+            r.cpu_limit_cores = read(&format!("{dir}/cpu.max")).as_deref().and_then(v2_cpu);
+        }
+        None => {
+            r.memory_limit_bytes = read("/sys/fs/cgroup/memory/memory.limit_in_bytes")
+                .as_deref()
+                .and_then(v1_memory);
+            r.cpu_limit_cores = read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
+                .zip(read("/sys/fs/cgroup/cpu/cpu.cfs_period_us"))
+                .and_then(|(q, p)| v1_cpu(&q, &p));
+        }
+    }
+    r
+}
+
+/// The figures of this process: on Linux from the real files, elsewhere none.
+pub fn read_process() -> Reading {
+    if cfg!(target_os = "linux") {
+        read_process_at(Path::new("/"))
+    } else {
+        Reading::default()
+    }
+}
+
+impl ProcessFigures {
+    /// The figures after `now`, read `elapsed` after the previous one.
+    pub fn next(&self, now: Reading, elapsed: Duration) -> Self {
+        let cpu_cores = match (self.reading.cpu_seconds, now.cpu_seconds) {
+            (Some(before), Some(after)) if !elapsed.is_zero() && after >= before => {
+                Some((after - before) / elapsed.as_secs_f64())
+            }
+            _ => None,
+        };
+        Self {
+            reading: now,
+            cpu_cores,
+        }
+    }
+}
+
+/// Reads the figures every [`PROCESS_SAMPLE`], publishes them and sets the process metrics, until
+/// every receiver is gone.
+pub async fn sample_process(metrics: Metrics, figures: watch::Sender<ProcessFigures>) {
+    let mut at = Instant::now();
+    loop {
+        let now = read_process();
+        metrics.process(&now);
+        let elapsed = at.elapsed();
+        at = Instant::now();
+        figures.send_modify(|f| *f = f.next(now, elapsed));
+        if figures.is_closed() {
+            return;
+        }
+        tokio::time::sleep(PROCESS_SAMPLE).await;
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+
+    /// A fake root: `files` laid out under a temporary directory.
+    fn root(files: &[(&str, &str)]) -> tempfile::TempDir {
+        let dir = tempfile::tempdir().unwrap();
+        for (path, text) in files {
+            let p = dir.path().join(path.trim_start_matches('/'));
+            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
+            std::fs::write(p, text).unwrap();
+        }
+        dir
+    }
+
+    const STATUS: &str = "Name:\tdata-avalanche\nVmPeak:\t 2000000 kB\nVmRSS:\t 1363148 kB\n";
+    // A command name with a space and a parenthesis; utime 4500, stime 500 ticks.
+    const STAT: &str = "42 (data (av)) S 1 42 42 0 -1 4194560 100 0 0 0 4500 500 0 0 20 0 9 0 \
+                        100 2000000 340787 18446744073709551615";
+
+    #[test]
+    fn memory_and_cpu_come_from_proc_self() {
+        assert_eq!(resident(STATUS), Some(1_363_148 * 1024));
+        assert_eq!(cpu_seconds(STAT), Some(50.0));
+        assert_eq!(resident("Name:\tx\n"), None);
+        assert_eq!(cpu_seconds("42 (x) S 1"), None);
+    }
+
+    #[test]
+    fn a_cgroup_v2_limit_is_read_at_the_process_s_own_cgroup() {
+        let dir = root(&[
+            ("/proc/self/status", STATUS),
+            ("/proc/self/stat", STAT),
+            ("/proc/self/cgroup", "0::/kubepods/pod1/cdc\n"),
+            ("/sys/fs/cgroup/cgroup.controllers", "cpu memory"),
+            ("/sys/fs/cgroup/memory.max", "max\n"),
+            (
+                "/sys/fs/cgroup/kubepods/pod1/cdc/cgroup.controllers",
+                "cpu memory",
+            ),
+            (
+                "/sys/fs/cgroup/kubepods/pod1/cdc/memory.max",
+                "2147483648\n",
+            ),
+            (
+                "/sys/fs/cgroup/kubepods/pod1/cdc/cpu.max",
+                "200000 100000\n",
+            ),
+        ]);
+        let r = read_process_at(dir.path());
+        assert_eq!(r.memory_limit_bytes, Some(2 << 30));
+        assert_eq!(r.cpu_limit_cores, Some(2.0));
+        assert_eq!(r.memory_bytes, Some(1_363_148 * 1024));
+    }
+
+    #[test]
+    fn no_limit_reads_as_none() {
+        let dir = root(&[
+            ("/proc/self/cgroup", "0::/\n"),
+            ("/sys/fs/cgroup/cgroup.controllers", "cpu memory"),
+            ("/sys/fs/cgroup/memory.max", "max\n"),
+            ("/sys/fs/cgroup/cpu.max", "max 100000\n"),
+        ]);
+        let r = read_process_at(dir.path());
+        assert_eq!((r.memory_limit_bytes, r.cpu_limit_cores), (None, None));
+        assert_eq!(read_process_at(root(&[]).path()), Reading::default());
+    }
+
+    #[test]
+    fn cgroup_v1_s_sentinel_is_no_limit() {
+        let unlimited = root(&[
+            (
+                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
+                "9223372036854771712\n",
+            ),
+            ("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "-1\n"),
+            ("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "100000\n"),
+        ]);
+        let r = read_process_at(unlimited.path());
+        assert_eq!((r.memory_limit_bytes, r.cpu_limit_cores), (None, None));
+        let limited = root(&[
+            (
+                "/sys/fs/cgroup/memory/memory.limit_in_bytes",
+                "1073741824\n",
+            ),
+            ("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "50000\n"),
+            ("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "100000\n"),
+        ]);
+        let r = read_process_at(limited.path());
+        assert_eq!(
+            (r.memory_limit_bytes, r.cpu_limit_cores),
+            (Some(1 << 30), Some(0.5))
+        );
+    }
+
+    #[test]
+    fn the_cpu_rate_is_over_the_interval_between_two_readings() {
+        let at = |s: f64| Reading {
+            cpu_seconds: Some(s),
+            ..Reading::default()
+        };
+        let first = ProcessFigures::default().next(at(10.0), Duration::from_secs(15));
+        assert_eq!(first.cpu_cores, None, "one reading has no rate");
+        let second = first.next(at(19.0), Duration::from_secs(15));
+        assert_eq!(second.cpu_cores, Some(0.6));
+        let none = second.next(Reading::default(), Duration::from_secs(15));
+        assert_eq!(none.cpu_cores, None);
+    }
+}
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 754daf7..30f8038 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -20,7 +20,7 @@ use data_avalanche_config::{
 };
 use data_avalanche_pipeline::{Backoff, Tuning};
 use data_avalanche_state::{ConfigGenerations, Events, Leases, Removals};
-use data_avalanche_telemetry::Metrics;
+use data_avalanche_telemetry::{Metrics, ProcessFigures, sample_process};
 use thiserror::Error;
 use tokio::sync::{Semaphore, oneshot, watch};
 
@@ -118,6 +118,9 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     }
     // The listener answers from here: live at once, ready once every controller runs.
     let metrics = Metrics::new();
+    // The process's memory and CPU every 15 s, for /metrics and the node's page (D-M13b.7).
+    let (sampled, _process) = watch::channel(ProcessFigures::default());
+    let _sampler = tokio::spawn(sample_process(metrics.clone(), sampled));
     let ready = Arc::new(AtomicBool::new(false));
     let node = Arc::new(OnceLock::new());
     let api = &first.config.globals.api;
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-telemetry`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock crates/data-avalanche-telemetry/Cargo.toml crates/data-avalanche-telemetry/src/lib.rs crates/data-avalanche-telemetry/src/process.rs crates/data-avalanche-telemetry/tests/metrics.rs crates/data-avalanche/src/run.rs
git commit -m "feat(telemetry): the process's memory and CPU against the container's limits"
```

---

### Task 5: Peers and the node routes (`data-avalanche-api`, `data-avalanche`)

**Files:**
- Create: `crates/data-avalanche-api/src/nodes.rs`, `crates/data-avalanche-api/src/peers.rs`
- Modify: `crates/data-avalanche-api/openapi.json`, `crates/data-avalanche-api/src/actions.rs`, `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-api/src/openapi.rs`, `crates/data-avalanche-api/src/test_support.rs`, `crates/data-avalanche-cluster/src/coordinator.rs`, `crates/data-avalanche/src/run.rs`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`, `xtask/src/api.rs`

**Interfaces:**
- Consumes: `NodeMeta::{api_url, file_hash}` (Task 2), `GossipFigures` (Task 3),
  `ProcessFigures` (Task 4); the recording layer and the role gate (M13a).
- Produces: api `Peers::new(me, Option<&TlsIdentity>) -> Result<Peers, String>`,
  `Peers::send(node, api_url, path_and_query, Asked, Duration) -> Response`, `Asked::from(Method,
  &HeaderMap, Bytes)`; `TARGET`, `FORWARDED_BY`, `ANSWERED_BY`, `DETAIL_LIMIT` (3 s),
  `ACTION_LIMIT` (10 s), `STATUS_LIMIT` (2 s), `MAX_ANSWER` (8 MiB); `Node::{peers, process,
  gossip}`; the six `/v1/nodes/{node}…` routes and the `421` middleware (D-M13b.4, .5, .6);
  `nodes::short(&str) -> &str`; cluster `ConfigCoordinator::generation() -> GenerationDoc`. The
  OpenAPI document gains the `nodes` tag.

- [ ] **Step 1: Write the failing tests**

The tests: the route table's roles (`lib.rs`), the OpenAPI document's `node` parameter; in the new `nodes.rs` (Step 3): the nine node-route tests, the Review Focus's among them.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 33c37fc..17dd5c5 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -576,6 +744,12 @@ mod tests {
                 ("GET", "/v1/snapshots/{job_id}", Viewer),
                 ("POST", "/v1/cluster/leave", Admin),
                 ("POST", "/v1/admin/shutdown", Admin),
+                ("GET", "/v1/nodes/{node}", Viewer),
+                ("POST", "/v1/nodes/{node}/leave", Admin),
+                ("POST", "/v1/nodes/{node}/shutdown", Admin),
+                ("POST", "/v1/nodes/{node}/config/validate", Operator),
+                ("POST", "/v1/nodes/{node}/config/reload", Operator),
+                ("GET", "/v1/nodes/{node}/config/status", Viewer),
                 ("GET", "/v1/events", Viewer),
                 ("GET", "/v1/openapi.json", Viewer),
             ]
````

`crates/data-avalanche-api/src/openapi.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index f657da9..80befb6 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -221,7 +222,7 @@ mod tests {
                         "{method} {path} {code}"
                     );
                 }
-                for name in ["id", "job_id"] {
+                for name in ["id", "job_id", "node"] {
                     if path.contains(&format!("{{{name}}}")) {
                         assert_eq!(op["parameters"][0]["name"], name, "{method} {path}");
                     }
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api`
Expected: the route table's test fails: no route `/v1/nodes/{node}`.

- [ ] **Step 3: Implement**

`openapi.json` is re-blessed with `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi`, and the site's copy and reference by `cargo xtask docs`: their diffs below are that output.

`crates/data-avalanche-api/openapi.json`:

````diff
diff --git a/crates/data-avalanche-api/openapi.json b/crates/data-avalanche-api/openapi.json
index 5703a99..9243a2b 100644
--- a/crates/data-avalanche-api/openapi.json
+++ b/crates/data-avalanche-api/openapi.json
@@ -504,6 +504,573 @@
         "x-min-role": "viewer"
       }
     },
+    "/v1/nodes/{node}": {
+      "get": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "A node's detail: its build and start, its file against its generation, its process and gossip figures, the leases it holds; another node's is fetched from it",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the node's detail",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
+    "/v1/nodes/{node}/config/reload": {
+      "post": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "Publish a node's own file as the next generation, through any node: its own `POST /v1/config/reload`, with the query",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
+        "responses": {
+          "200": {
+            "description": "the file is the generation the cluster runs",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "202": {
+            "description": "published",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "other nodes published first, every time",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "413": {
+            "description": "the body is over 1 MiB",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "422": {
+            "description": "invalid, or a restart-only field without restart=accept; or the note is too long, not a string, holds a control character, or the body isn't a JSON object",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "operator"
+      }
+    },
+    "/v1/nodes/{node}/config/status": {
+      "get": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "A node's generation and its own restart-pending fields: its own `GET /v1/config/status`",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the node's config status",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
+    "/v1/nodes/{node}/config/validate": {
+      "post": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "Check a node's own file (an empty body), or the body, on that node: its own `POST /v1/config/validate`",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "requestBody": {
+          "content": {
+            "application/yaml": {}
+          },
+          "required": false
+        },
+        "responses": {
+          "200": {
+            "description": "it loads: its warnings and the changes",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "422": {
+            "description": "invalid, or it changes a restart-only field: the report",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "429": {
+            "description": "posted bodies past the node's rate",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "operator"
+      }
+    },
+    "/v1/nodes/{node}/leave": {
+      "post": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "Ask a node to leave the cluster, through any node: its own `POST /v1/cluster/leave`",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
+        "responses": {
+          "202": {
+            "description": "the leave is under way",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "leaving already",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "413": {
+            "description": "the body is over 1 MiB",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "422": {
+            "description": "the note is too long, not a string, holds a control character, or the body isn't a JSON object",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "admin"
+      }
+    },
+    "/v1/nodes/{node}/shutdown": {
+      "post": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "Ask a node to shut down, through any node: its own `POST /v1/admin/shutdown`",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
+        "responses": {
+          "202": {
+            "description": "the shutdown is under way",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "shutting down already",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "413": {
+            "description": "the body is over 1 MiB",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "422": {
+            "description": "the note is too long, not a string, holds a control character, or the body isn't a JSON object",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "admin"
+      }
+    },
     "/v1/openapi.json": {
       "get": {
         "tags": [
````

`crates/data-avalanche-api/src/actions.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/actions.rs b/crates/data-avalanche-api/src/actions.rs
index 15126c1..e21cedb 100644
--- a/crates/data-avalanche-api/src/actions.rs
+++ b/crates/data-avalanche-api/src/actions.rs
@@ -263,6 +263,9 @@ pub(crate) async fn denied(gate: &Gate, name: &str, role: &str, path: &str) {
         None if path.starts_with("/v1/cluster/") || path.starts_with("/v1/admin/") => {
             recorder.node().to_owned()
         }
+        None if let Some(rest) = path.strip_prefix("/v1/nodes/") => {
+            clipped(rest.split('/').next().unwrap_or_default(), MAX_TARGET).into_owned()
+        }
         None => String::new(),
     };
     let mut event = recorder.event(
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 33c37fc..17dd5c5 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -14,7 +14,9 @@ mod cluster;
 mod config;
 mod events;
 mod limit;
+mod nodes;
 mod openapi;
+mod peers;
 mod pipelines;
 mod server;
 mod snapshots;
@@ -31,10 +33,10 @@ use axum::http::{HeaderValue, StatusCode, header};
 use axum::middleware::{Next, from_fn_with_state};
 use axum::response::{IntoResponse, Response};
 use axum::routing::{MethodRouter, get, post};
-use data_avalanche_cluster::{ConfigCoordinator, DocsFn, Members, Recorder};
+use data_avalanche_cluster::{ConfigCoordinator, DocsFn, GossipFigures, Members, Recorder};
 use data_avalanche_config::Role;
 use data_avalanche_state::{Events, Leases};
-use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
+use data_avalanche_telemetry::{CONTENT_TYPE, Metrics, ProcessFigures};
 use serde_json::{Value, json};
 use tokio::sync::watch;
 
@@ -43,6 +45,9 @@ pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, R
 pub use cluster::{Control, Stop};
 pub use limit::{RateLimit, VALIDATE_BURST, VALIDATE_PER_SECOND};
 pub use openapi::openapi;
+pub use peers::{
+    ACTION_LIMIT, ANSWERED_BY, DETAIL_LIMIT, FORWARDED_BY, MAX_ANSWER, Peers, STATUS_LIMIT, TARGET,
+};
 pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
 
 use crate::auth::Denied;
@@ -79,6 +84,12 @@ pub struct Node {
     pub events: Arc<Events>,
     /// The refused attempts recorded: [`RateLimit::denials`].
     pub denials: Arc<RateLimit>,
+    /// Asks another node on a caller's behalf (D-M13b.4).
+    pub peers: Peers,
+    /// The process's memory and CPU, sampled.
+    pub process: watch::Receiver<ProcessFigures>,
+    /// What the gossip saw of its probes.
+    pub gossip: watch::Receiver<GossipFigures>,
 }
 
 impl std::fmt::Debug for Node {
@@ -105,6 +116,9 @@ pub struct Route {
     pub answers: &'static [(u16, &'static str)],
     /// What it records, when it changes something (Admin_Console §5).
     pub records: Option<Recorded>,
+    /// Sent on to the node it names (`/v1/nodes/{node}/…`), which records the action itself:
+    /// this node records only a refused attempt of its own gate.
+    pub forwards: bool,
 }
 
 /// What a route's gate and recording know: the auth, the route, and the node once it runs.
@@ -130,7 +144,19 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
         body: None,
         answers,
         records: None,
+        forwards: false,
     };
+    // A node's route, sent on to the node it names.
+    let fwd = |route: Route| Route {
+        forwards: true,
+        ..route
+    };
+    const FORWARDED: [(u16, &str); 4] = [
+        (404, "no member of that name in this node's view"),
+        (421, "a request forwarded to a node that isn't its target"),
+        (502, "the node didn't answer"),
+        (504, "the node didn't answer within the time limit"),
+    ];
     // A mutating route: an optional JSON body with a `note`, and what it records.
     let act = |kind, verb, route: Route| Route {
         body: Some(route.body.unwrap_or("application/json")),
@@ -401,6 +427,130 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             post(cluster::shutdown),
         ),
+        (
+            r(
+                "GET",
+                "/v1/nodes/{node}",
+                Viewer,
+                "A node's detail: its build and start, its file against its generation, its process and gossip figures, the leases it holds; another node's is fetched from it",
+                &[
+                    (200, "the node's detail"),
+                    FORWARDED[0],
+                    FORWARDED[1],
+                    FORWARDED[2],
+                    FORWARDED[3],
+                ],
+            ),
+            get(nodes::get),
+        ),
+        (
+            fwd(act(
+                "node.leave",
+                "leave the cluster:",
+                r(
+                    "POST",
+                    "/v1/nodes/{node}/leave",
+                    Admin,
+                    "Ask a node to leave the cluster, through any node: its own `POST /v1/cluster/leave`",
+                    &[
+                        (202, "the leave is under way"),
+                        (409, "leaving already"),
+                        FORWARDED[0],
+                        FORWARDED[1],
+                        FORWARDED[2],
+                        FORWARDED[3],
+                    ],
+                ),
+            )),
+            post(nodes::leave),
+        ),
+        (
+            fwd(act(
+                "node.shutdown",
+                "shut down",
+                r(
+                    "POST",
+                    "/v1/nodes/{node}/shutdown",
+                    Admin,
+                    "Ask a node to shut down, through any node: its own `POST /v1/admin/shutdown`",
+                    &[
+                        (202, "the shutdown is under way"),
+                        (409, "shutting down already"),
+                        FORWARDED[0],
+                        FORWARDED[1],
+                        FORWARDED[2],
+                        FORWARDED[3],
+                    ],
+                ),
+            )),
+            post(nodes::shutdown),
+        ),
+        (
+            fwd(Route {
+                body: Some("application/yaml"),
+                ..r(
+                    "POST",
+                    "/v1/nodes/{node}/config/validate",
+                    Operator,
+                    "Check a node's own file (an empty body), or the body, on that node: its own `POST /v1/config/validate`",
+                    &[
+                        (200, "it loads: its warnings and the changes"),
+                        (
+                            422,
+                            "invalid, or it changes a restart-only field: the report",
+                        ),
+                        (429, "posted bodies past the node's rate"),
+                        FORWARDED[0],
+                        FORWARDED[1],
+                        FORWARDED[2],
+                        FORWARDED[3],
+                    ],
+                )
+            }),
+            post(nodes::validate),
+        ),
+        (
+            fwd(act(
+                "config.reload",
+                "reload the config",
+                r(
+                    "POST",
+                    "/v1/nodes/{node}/config/reload",
+                    Operator,
+                    "Publish a node's own file as the next generation, through any node: its own `POST /v1/config/reload`, with the query",
+                    &[
+                        (200, "the file is the generation the cluster runs"),
+                        (202, "published"),
+                        (409, "other nodes published first, every time"),
+                        (
+                            422,
+                            "invalid, or a restart-only field without restart=accept",
+                        ),
+                        FORWARDED[0],
+                        FORWARDED[1],
+                        FORWARDED[2],
+                        FORWARDED[3],
+                    ],
+                ),
+            )),
+            post(nodes::reload),
+        ),
+        (
+            fwd(r(
+                "GET",
+                "/v1/nodes/{node}/config/status",
+                Viewer,
+                "A node's generation and its own restart-pending fields: its own `GET /v1/config/status`",
+                &[
+                    (200, "the node's config status"),
+                    FORWARDED[0],
+                    FORWARDED[1],
+                    FORWARDED[2],
+                    FORWARDED[3],
+                ],
+            )),
+            get(nodes::config_status),
+        ),
         (
             r(
                 "GET",
@@ -444,7 +594,7 @@ pub fn router(api: Api) -> Router {
             node: Arc::clone(&api.node),
         };
         // The gate runs first (the outer layer), then the recording.
-        let handler = if r.records.is_some() {
+        let handler = if r.records.is_some() && !r.forwards {
             handler.route_layer(from_fn_with_state(gate.clone(), actions::record))
         } else {
             handler
@@ -454,7 +604,25 @@ pub fn router(api: Api) -> Router {
             handler.route_layer(from_fn_with_state(gate, authorize)),
         );
     }
-    router.with_state(api)
+    router
+        .layer(from_fn_with_state(api.clone(), misdirected))
+        .with_state(api)
+}
+
+/// A request forwarded to this node for another one (D-M13b.4: an address since reused, a
+/// renamed pod) is refused before anything reads it.
+async fn misdirected(State(api): State<Api>, req: Request, next: Next) -> Response {
+    let target = req.headers().get(peers::TARGET).cloned();
+    if let (Some(target), Some(node)) = (target, api.node.get()) {
+        let me = node.members.borrow().me.clone();
+        if target.as_bytes() != me.as_bytes() {
+            return body(
+                StatusCode::MISDIRECTED_REQUEST,
+                &json!({ "error": format!("this is {me}, not the node the request was sent to") }),
+            );
+        }
+    }
+    next.run(req).await
 }
 
 /// Lets a caller through to a route that takes `role` or less, with its [`Principal`] in the
````

`crates/data-avalanche-api/src/nodes.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/nodes.rs b/crates/data-avalanche-api/src/nodes.rs
new file mode 100644
index 0000000..1a6e8c6
--- /dev/null
+++ b/crates/data-avalanche-api/src/nodes.rs
@@ -0,0 +1,619 @@
+//! `/v1/nodes/{node}` (Admin_Console §4, §5, §10; D-M13b.5–6): a node's detail, and the actions
+//! only that node takes, asked of any node. The detail of this node is answered here; another
+//! node's is fetched from it. An action goes to the node's own route, this node's included, so
+//! the node that acts verifies the caller and records it.
+
+use axum::body::Bytes;
+use axum::extract::{Path, RawQuery, State};
+use axum::http::{HeaderMap, Method, StatusCode};
+use axum::response::Response;
+use chrono::{DateTime, Utc};
+use data_avalanche_cluster::{Phase, Ring};
+use serde_json::{Value, json};
+
+use crate::peers::{ACTION_LIMIT, Asked, DETAIL_LIMIT, FORWARDED_BY, STATUS_LIMIT};
+use crate::{Api, Node, body, starting};
+
+/// The first 16 hex digits of a `blake3:<hex>` hash, as a node gossips its file's.
+pub(crate) fn short(hash: &str) -> &str {
+    let hex = hash.strip_prefix("blake3:").unwrap_or(hash);
+    hex.get(..16).unwrap_or(hex)
+}
+
+fn phase(p: Phase) -> &'static str {
+    match p {
+        Phase::Joining => "joining",
+        Phase::Active => "active",
+        Phase::Draining => "draining",
+        Phase::Leaving => "leaving",
+    }
+}
+
+/// This node's detail (D-M13b.6).
+async fn detail(node: &Node) -> Value {
+    let members = node.members.borrow().clone();
+    let me = members.nodes.get(&members.me);
+    let generation = node.coordinator.generation().await;
+    let file_hash = me.map(|m| m.meta.file_hash.as_str()).unwrap_or_default();
+    let config = node.coordinator.config().await;
+    let ring = Ring::new(
+        members
+            .nodes
+            .iter()
+            .filter(|(_, m)| m.meta.phase == Phase::Active)
+            .map(|(name, _)| name.as_str()),
+        config.globals.cluster.tokens,
+    );
+    let mut ids: Vec<&str> = config.pipelines.values().map(|p| p.id.as_str()).collect();
+    ids.sort_unstable();
+    let mut held = Some(vec![]);
+    for id in ids {
+        match node.leases.observe(id).await {
+            Ok(Some(l)) if !l.doc.released && l.doc.holder_node_id == members.me => {
+                if let Some(h) = held.as_mut() {
+                    h.push(json!({
+                        "id": id,
+                        "owner_epoch": l.doc.owner_epoch,
+                        "ring_owner": ring.owner(id),
+                    }));
+                }
+            }
+            Ok(_) => {}
+            Err(e) => {
+                tracing::debug!("a node's detail, the lease of {id}: {e}");
+                held = None;
+                break;
+            }
+        }
+    }
+    let p = *node.process.borrow();
+    let g = node.gossip.borrow().clone();
+    let peers: Vec<Value> = members
+        .nodes
+        .iter()
+        .filter(|(name, _)| **name != members.me)
+        .map(|(name, m)| {
+            json!({
+                "node": name,
+                "state": if m.suspect { "suspect" } else { "alive" },
+                "phase": phase(m.meta.phase),
+            })
+        })
+        .collect();
+    let started = me.and_then(|m| {
+        i64::try_from(m.incarnation)
+            .ok()
+            .and_then(DateTime::<Utc>::from_timestamp_millis)
+    });
+    json!({
+        "node": members.me,
+        "api_url": me.map(|m| m.meta.api_url.as_str()),
+        "gossip_addr": me.map(|m| m.addr.to_string()),
+        "build": me.map(|m| m.meta.build.as_str()),
+        "incarnation": me.map(|m| m.incarnation),
+        "started_at": started,
+        "phase": me.map(|m| phase(m.meta.phase)),
+        "config_generation": generation.generation,
+        "file": {
+            "hash": (!file_hash.is_empty()).then_some(file_hash),
+            "generation_hash": short(&generation.hash),
+            "matches": (!file_hash.is_empty()).then(|| file_hash == short(&generation.hash)),
+        },
+        "restart_pending": node.coordinator.restart_pending().await,
+        "process": {
+            "memory_bytes": p.reading.memory_bytes,
+            "memory_limit_bytes": p.reading.memory_limit_bytes,
+            "cpu_seconds": p.reading.cpu_seconds,
+            "cpu_cores": p.cpu_cores,
+            "cpu_limit_cores": p.reading.cpu_limit_cores,
+        },
+        "gossip": {
+            "peers": peers,
+            "last_probe": g.last_probe.map(|(n, at)| json!({ "node": n, "at": at })),
+            "probe_rtt_ms": g.probe_rtt.map(|d| d.as_secs_f64() * 1000.0),
+            "suspicions": {
+                "raised": g.raised.iter().map(|(n, at)| json!({ "node": n, "at": at })).collect::<Vec<_>>(),
+                "received": g.received,
+            },
+        },
+        "pipelines": held,
+    })
+}
+
+/// Sends the caller's request on to `node`'s `path`: `404` for a name no member has, `503` for
+/// a member that gossips no API address, and never a request that was forwarded already.
+async fn forward(
+    node: &Node,
+    headers: &HeaderMap,
+    to: &str,
+    path: &str,
+    asked: Asked,
+    limit: std::time::Duration,
+) -> Response {
+    if headers.contains_key(FORWARDED_BY) {
+        return body(
+            StatusCode::MISDIRECTED_REQUEST,
+            &json!({ "error": format!("{to} isn't this node, and a forwarded request isn't forwarded again") }),
+        );
+    }
+    let member = node.members.borrow().nodes.get(to).cloned();
+    match member {
+        None => body(
+            StatusCode::NOT_FOUND,
+            &json!({ "error": format!("no member {to:?} in this node's view of the cluster") }),
+        ),
+        Some(m) if m.meta.api_url.is_empty() => body(
+            StatusCode::SERVICE_UNAVAILABLE,
+            &json!({ "error": format!("{to} gossips no API address") }),
+        ),
+        Some(m) => {
+            node.peers
+                .send(to, &m.meta.api_url, path, asked, limit)
+                .await
+        }
+    }
+}
+
+/// `GET /v1/nodes/{node}`.
+pub(crate) async fn get(
+    State(api): State<Api>,
+    Path(to): Path<String>,
+    headers: HeaderMap,
+) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    if to == node.members.borrow().me {
+        return body(StatusCode::OK, &detail(node).await);
+    }
+    let asked = Asked::from(Method::GET, &headers, Bytes::new());
+    let path = format!("/v1/nodes/{to}");
+    forward(node, &headers, &to, &path, asked, DETAIL_LIMIT).await
+}
+
+/// A node action, sent to the node's own `path`, this node's included.
+async fn act(
+    api: &Api,
+    to: &str,
+    headers: &HeaderMap,
+    method: Method,
+    path: &str,
+    bytes: Bytes,
+    limit: std::time::Duration,
+) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let asked = Asked::from(method, headers, bytes);
+    forward(node, headers, to, path, asked, limit).await
+}
+
+/// `POST /v1/nodes/{node}/leave` → the node's `POST /v1/cluster/leave`.
+pub(crate) async fn leave(
+    State(api): State<Api>,
+    Path(to): Path<String>,
+    headers: HeaderMap,
+    bytes: Bytes,
+) -> Response {
+    act(
+        &api,
+        &to,
+        &headers,
+        Method::POST,
+        "/v1/cluster/leave",
+        bytes,
+        ACTION_LIMIT,
+    )
+    .await
+}
+
+/// `POST /v1/nodes/{node}/shutdown` → the node's `POST /v1/admin/shutdown`.
+pub(crate) async fn shutdown(
+    State(api): State<Api>,
+    Path(to): Path<String>,
+    headers: HeaderMap,
+    bytes: Bytes,
+) -> Response {
+    act(
+        &api,
+        &to,
+        &headers,
+        Method::POST,
+        "/v1/admin/shutdown",
+        bytes,
+        ACTION_LIMIT,
+    )
+    .await
+}
+
+/// `POST /v1/nodes/{node}/config/validate` → the node's `POST /v1/config/validate`: its own
+/// file when the body is empty.
+pub(crate) async fn validate(
+    State(api): State<Api>,
+    Path(to): Path<String>,
+    headers: HeaderMap,
+    bytes: Bytes,
+) -> Response {
+    act(
+        &api,
+        &to,
+        &headers,
+        Method::POST,
+        "/v1/config/validate",
+        bytes,
+        ACTION_LIMIT,
+    )
+    .await
+}
+
+/// `POST /v1/nodes/{node}/config/reload` → the node's `POST /v1/config/reload`, with the query:
+/// the node publishes its own file.
+pub(crate) async fn reload(
+    State(api): State<Api>,
+    Path(to): Path<String>,
+    RawQuery(query): RawQuery,
+    headers: HeaderMap,
+    bytes: Bytes,
+) -> Response {
+    let path = match query {
+        Some(q) => format!("/v1/config/reload?{q}"),
+        None => "/v1/config/reload".to_owned(),
+    };
+    act(
+        &api,
+        &to,
+        &headers,
+        Method::POST,
+        &path,
+        bytes,
+        ACTION_LIMIT,
+    )
+    .await
+}
+
+/// `GET /v1/nodes/{node}/config/status` → the node's `GET /v1/config/status`: its generation
+/// and its own restart-pending fields.
+pub(crate) async fn config_status(
+    State(api): State<Api>,
+    Path(to): Path<String>,
+    headers: HeaderMap,
+) -> Response {
+    act(
+        &api,
+        &to,
+        &headers,
+        Method::GET,
+        "/v1/config/status",
+        Bytes::new(),
+        STATUS_LIMIT,
+    )
+    .await
+}
+
+#[cfg(test)]
+mod tests {
+    use std::net::SocketAddr;
+    use std::sync::{Arc, Mutex};
+    use std::time::{Duration, Instant};
+
+    use axum::Router;
+    use axum::http::Uri;
+    use data_avalanche_cluster::{Member, Members, NodeMeta, file_hash};
+    use data_avalanche_config::{FingerprintKey, Origin, Pem, Secret, TlsIdentity};
+    use data_avalanche_state::{EventDoc, EventQuery, Events};
+    use tempfile::TempDir;
+    use tokio::sync::watch;
+
+    use super::*;
+    use crate::peers::{ANSWERED_BY, Peers, TARGET};
+    use crate::test_support::{
+        CONFIG, Issuer, Lone, USERNAME_CLAIM, api, lone, open, request, serve,
+    };
+    use crate::{Auth, Stop};
+
+    /// What a fake node saw of one request.
+    #[derive(Debug, Clone)]
+    struct Seen {
+        method: Method,
+        uri: String,
+        authorization: Option<String>,
+        target: Option<String>,
+        forwarded_by: Option<String>,
+        body: String,
+    }
+
+    type Log = Arc<Mutex<Vec<Seen>>>;
+
+    /// A fake node: answers every request `202 {"node": "node-b"}` after `delay`, keeping what it
+    /// saw.
+    async fn fake(delay: Duration) -> (String, Log) {
+        let log: Log = Arc::default();
+        let kept = Arc::clone(&log);
+        let app = Router::new().fallback(
+            move |method: Method, uri: Uri, headers: HeaderMap, bytes: Bytes| {
+                let kept = Arc::clone(&kept);
+                async move {
+                    let h = |n: &str| headers.get(n).map(|v| v.to_str().unwrap().to_owned());
+                    kept.lock().unwrap().push(Seen {
+                        method,
+                        uri: uri.to_string(),
+                        authorization: h("authorization"),
+                        target: h(TARGET),
+                        forwarded_by: h(FORWARDED_BY),
+                        body: String::from_utf8(bytes.to_vec()).unwrap(),
+                    });
+                    tokio::time::sleep(delay).await;
+                    body(StatusCode::ACCEPTED, &json!({ "node": "node-b" }))
+                }
+            },
+        );
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let url = format!("http://{}", listener.local_addr().unwrap());
+        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
+        (url, log)
+    }
+
+    fn member(api_url: &str, phase: Phase) -> Member {
+        Member {
+            addr: "127.0.0.1:7946".parse().unwrap(),
+            incarnation: 1_731_600_000_000,
+            meta: NodeMeta {
+                phase,
+                config_generation: 1,
+                build: "1.1.0".into(),
+                api_url: api_url.into(),
+                file_hash: file_hash(CONFIG),
+            },
+            suspect: false,
+        }
+    }
+
+    /// node-a's API under `auth`; its view holds itself at its own address and node-b at
+    /// `b_url`.
+    async fn served(b_url: &str, auth: Arc<Auth>) -> (SocketAddr, Lone, TempDir) {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, CONFIG).unwrap();
+        let mut lone = lone(file).await;
+        let (view, members) = watch::channel(Members {
+            me: "node-a".into(),
+            nodes: Default::default(),
+        });
+        lone.node.members = members;
+        let a = api(auth);
+        a.node.set(lone.node.clone()).unwrap();
+        let addr = serve(a).await;
+        view.send_replace(Members {
+            me: "node-a".into(),
+            nodes: [
+                (
+                    "node-a".to_owned(),
+                    member(&format!("http://{addr}"), Phase::Active),
+                ),
+                ("node-b".to_owned(), member(b_url, Phase::Active)),
+            ]
+            .into(),
+        });
+        std::mem::forget(view);
+        (addr, lone, dir)
+    }
+
+    fn json(text: &str) -> Value {
+        serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
+    }
+
+    async fn recorded(events: &Events) -> Vec<EventDoc> {
+        let page = events.list(&EventQuery::default()).await.unwrap();
+        page.events.into_iter().map(|(_, e)| e).collect()
+    }
+
+    #[tokio::test]
+    async fn this_node_s_detail_is_answered_here() {
+        let (b, log) = fake(Duration::ZERO).await;
+        let (addr, _lone, _dir) = served(&b, open()).await;
+        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-a", &[], "").await;
+        assert_eq!(code, 200, "{text}");
+        let v = json(&text);
+        assert_eq!(v["node"], "node-a");
+        assert_eq!(v["api_url"], format!("http://{addr}"));
+        assert_eq!(v["build"], "1.1.0");
+        assert_eq!(v["started_at"], "2024-11-14T16:00:00Z");
+        assert_eq!(v["config_generation"], 1);
+        assert_eq!(v["file"]["hash"], file_hash(CONFIG));
+        assert_eq!(v["file"]["matches"], true);
+        assert_eq!(
+            v["gossip"]["peers"],
+            json!([{ "node": "node-b", "state": "alive", "phase": "active" }])
+        );
+        assert_eq!(v["pipelines"], json!([]));
+        assert!(
+            v["process"].is_object() && v["restart_pending"].is_array(),
+            "{v}"
+        );
+        assert!(log.lock().unwrap().is_empty(), "nothing forwarded");
+    }
+
+    #[tokio::test]
+    async fn another_node_s_detail_is_fetched_with_the_callers_own_credentials() {
+        let (b, log) = fake(Duration::ZERO).await;
+        let (addr, _lone, _dir) = served(&b, open()).await;
+        let auth = [("Authorization", "Bearer the-callers-token")];
+        let (code, head, text) = request(addr, "GET", "/v1/nodes/node-b", &auth, "").await;
+        assert_eq!((code, json(&text)), (202, json!({ "node": "node-b" })));
+        assert!(
+            head.to_ascii_lowercase()
+                .contains(&format!("{ANSWERED_BY}: node-b")),
+            "{head}"
+        );
+        let seen = log.lock().unwrap()[0].clone();
+        assert_eq!(
+            (seen.method, seen.uri.as_str()),
+            (Method::GET, "/v1/nodes/node-b")
+        );
+        assert_eq!(
+            seen.authorization.as_deref(),
+            Some("Bearer the-callers-token")
+        );
+        assert_eq!(seen.target.as_deref(), Some("node-b"));
+        assert_eq!(seen.forwarded_by.as_deref(), Some("node-a"));
+    }
+
+    #[tokio::test]
+    async fn a_peer_that_doesnt_answer_is_named_within_the_limit() {
+        let (addr, _lone, _dir) = served("http://127.0.0.1:1", open()).await;
+        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-b", &[], "").await;
+        assert_eq!(code, 502, "{text}");
+        let v = json(&text);
+        assert_eq!(v["node"], "node-b");
+        assert!(
+            v["error"]
+                .as_str()
+                .unwrap()
+                .starts_with("node-b didn't answer"),
+            "{v}"
+        );
+        let (code, _, _) = request(addr, "GET", "/v1/nodes/node-z", &[], "").await;
+        assert_eq!(code, 404, "no such member");
+    }
+
+    #[tokio::test]
+    async fn a_slow_peer_is_cut_off_at_the_limit() {
+        let (b, _log) = fake(Duration::from_secs(30)).await;
+        let (addr, _lone, _dir) = served(&b, open()).await;
+        let t = Instant::now();
+        let (code, _, text) = request(addr, "GET", "/v1/nodes/node-b", &[], "").await;
+        assert_eq!(code, 504, "{text}");
+        assert!(
+            t.elapsed() >= DETAIL_LIMIT && t.elapsed() < DETAIL_LIMIT * 2,
+            "{:?}",
+            t.elapsed()
+        );
+    }
+
+    #[tokio::test]
+    async fn a_request_forwarded_to_the_wrong_node_is_refused() {
+        let (b, log) = fake(Duration::ZERO).await;
+        let (addr, lone, _dir) = served(&b, open()).await;
+        let to_b = [(TARGET, "node-b")];
+        let (code, _, text) = request(addr, "POST", "/v1/cluster/leave", &to_b, "").await;
+        assert_eq!(code, 421, "{text}");
+        assert_eq!(*lone.stops.borrow(), Stop::Run, "nothing acted on");
+        let again = [(FORWARDED_BY, "node-c")];
+        let (code, _, _) = request(addr, "GET", "/v1/nodes/node-b", &again, "").await;
+        assert_eq!(code, 421, "never forwarded twice");
+        assert!(log.lock().unwrap().is_empty());
+    }
+
+    #[tokio::test]
+    async fn a_leave_goes_to_the_node_named_which_records_it() {
+        let (b, log) = fake(Duration::ZERO).await;
+        let (addr, lone, _dir) = served(&b, open()).await;
+        let note = r#"{"note": "node pool upgrade"}"#;
+        let (code, _, _) = request(addr, "POST", "/v1/nodes/node-b/leave", &[], note).await;
+        assert_eq!(code, 202);
+        let seen = log.lock().unwrap()[0].clone();
+        assert_eq!(
+            (seen.method, seen.uri.as_str()),
+            (Method::POST, "/v1/cluster/leave")
+        );
+        assert_eq!(seen.body, note);
+        assert_eq!(*lone.stops.borrow(), Stop::Run, "node-a stays");
+
+        // Through itself: its own route acts and records, once.
+        let (code, _, text) = request(addr, "POST", "/v1/nodes/node-a/leave", &[], note).await;
+        assert_eq!(
+            (code, json(&text)),
+            (202, json!({ "leaving": true, "node": "node-a" }))
+        );
+        assert_eq!(*lone.stops.borrow(), Stop::Leave);
+        assert!(lone.node.recorder.flush(Duration::from_secs(5)).await);
+        let got = recorded(&lone.events).await;
+        let kinds: Vec<&str> = got.iter().map(|e| e.kind.as_str()).collect();
+        assert_eq!(kinds, ["action.node.leave"], "{got:?}");
+        assert_eq!(got[0].note.as_deref(), Some("node pool upgrade"));
+    }
+
+    #[tokio::test]
+    async fn the_config_routes_go_to_the_node_s_own() {
+        let (b, log) = fake(Duration::ZERO).await;
+        let (addr, _lone, _dir) = served(&b, open()).await;
+        for (method, path, to) in [
+            (
+                "POST",
+                "/v1/nodes/node-b/config/validate",
+                "/v1/config/validate",
+            ),
+            (
+                "POST",
+                "/v1/nodes/node-b/config/reload?restart=accept",
+                "/v1/config/reload?restart=accept",
+            ),
+            ("GET", "/v1/nodes/node-b/config/status", "/v1/config/status"),
+            ("POST", "/v1/nodes/node-b/shutdown", "/v1/admin/shutdown"),
+        ] {
+            let (code, _, _) = request(addr, method, path, &[], "").await;
+            assert_eq!(code, 202, "{path}");
+            assert_eq!(log.lock().unwrap().last().unwrap().uri, to);
+        }
+    }
+
+    #[tokio::test]
+    async fn a_viewer_cant_leave_another_node() {
+        let idp = Issuer::start().await;
+        let (b, log) = fake(Duration::ZERO).await;
+        let (addr, lone, _dir) =
+            served(&b, Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
+        let token = format!("Bearer {}", idp.token(json!(["viewer"])));
+        let auth = [("Authorization", token.as_str())];
+        let (code, _, _) = request(addr, "POST", "/v1/nodes/node-b/leave", &auth, "").await;
+        assert_eq!(code, 403);
+        assert!(log.lock().unwrap().is_empty(), "refused before it was sent");
+        assert!(lone.node.recorder.flush(Duration::from_secs(5)).await);
+        let got = recorded(&lone.events).await;
+        assert_eq!(got.len(), 1, "{got:?}");
+        assert_eq!(got[0].kind, "action.denied");
+        assert_eq!(
+            got[0].text,
+            "leave the cluster: node-b refused: needs admin"
+        );
+    }
+
+    /// The cluster's nodes share a CA: a peer trusts every certificate of its own `api.tls`.
+    #[tokio::test]
+    async fn peers_trust_this_node_s_own_certificate() {
+        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
+        let key = FingerprintKey::random();
+        let pem = |text: String| Pem {
+            origin: Origin::Inline,
+            text: Secret::new(text, &key),
+        };
+        let tls = TlsIdentity {
+            cert: pem(cert.cert.pem()),
+            key: pem(cert.signing_key.serialize_pem()),
+        };
+        let bound = crate::bind(
+            &data_avalanche_config::HostPort {
+                host: "127.0.0.1".into(),
+                port: 0,
+            },
+            Some(&tls),
+        )
+        .await
+        .unwrap();
+        let port = bound.local_addr().unwrap().port();
+        let app = Router::new().fallback(|| async { body(StatusCode::OK, &json!({})) });
+        tokio::spawn(bound.serve(app, std::future::pending()));
+        let url = format!("https://localhost:{port}");
+        let asked = || Asked::from(Method::GET, &HeaderMap::new(), Bytes::new());
+        let ours = Peers::new("node-a", Some(&tls)).unwrap();
+        let r = ours
+            .send("node-b", &url, "/v1/x", asked(), DETAIL_LIMIT)
+            .await;
+        assert_eq!(r.status(), 200);
+        let strangers = Peers::new("node-a", None).unwrap();
+        let r = strangers
+            .send("node-b", &url, "/v1/x", asked(), DETAIL_LIMIT)
+            .await;
+        assert_eq!(r.status(), 502, "a certificate nobody vouches for");
+    }
+}
````

`crates/data-avalanche-api/src/openapi.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index f657da9..80befb6 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -45,6 +45,7 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
     for (name, description) in [
         ("id", "the pipeline's id"),
         ("job_id", "the snapshot job's id, `<pipeline id>~<suffix>`"),
+        ("node", "a member's name, as the gossip knows it"),
     ] {
         if r.path.contains(&format!("{{{name}}}")) {
             op = op.parameter(
````

`crates/data-avalanche-api/src/peers.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/peers.rs b/crates/data-avalanche-api/src/peers.rs
new file mode 100644
index 0000000..1a87942
--- /dev/null
+++ b/crates/data-avalanche-api/src/peers.rs
@@ -0,0 +1,177 @@
+//! Another node's API, asked on a caller's behalf (Admin_Console §10, D-M13b.4): a node's live
+//! detail, and the actions only the node itself takes (leave, shut down, check and reload its own
+//! file). The request carries the caller's own credentials and nothing of this node's, so the
+//! node it reaches verifies the caller, and records the action, as if the caller had come
+//! straight to it: no node-to-node trust. It names its target, so a node that isn't it (an
+//! address since reused) refuses it, and it is never forwarded again.
+
+use std::time::Duration;
+
+use axum::body::Bytes;
+use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
+use axum::response::{IntoResponse, Response};
+use data_avalanche_config::TlsIdentity;
+use serde_json::json;
+
+use crate::body;
+
+/// The node a forwarded request is for: a node that isn't it answers `421`.
+pub const TARGET: &str = "x-data-avalanche-target";
+/// The node that forwarded a request: a request carrying it is never forwarded again.
+pub const FORWARDED_BY: &str = "x-data-avalanche-forwarded-by";
+/// On a forwarded answer: the node that gave it.
+pub const ANSWERED_BY: &str = "x-data-avalanche-node";
+
+/// How long a node's detail may take (Admin_Console §8: each source its own limit).
+pub const DETAIL_LIMIT: Duration = Duration::from_secs(3);
+/// How long a node action may take to be answered (the action itself goes on after).
+pub const ACTION_LIMIT: Duration = Duration::from_secs(10);
+/// How long a node's config status may take, asked of every node at once.
+pub const STATUS_LIMIT: Duration = Duration::from_secs(2);
+/// The largest answer passed back.
+pub const MAX_ANSWER: usize = 8 << 20;
+
+/// The client every forwarded request goes through.
+#[derive(Debug, Clone)]
+pub struct Peers {
+    me: String,
+    http: reqwest::Client,
+}
+
+/// A request to forward: the caller's method, credentials, content type and body.
+#[derive(Debug, Clone)]
+pub(crate) struct Asked {
+    pub method: Method,
+    pub authorization: Option<HeaderValue>,
+    pub content_type: Option<HeaderValue>,
+    pub body: Bytes,
+}
+
+impl Asked {
+    /// What the caller sent, for forwarding.
+    pub(crate) fn from(method: Method, headers: &HeaderMap, body: Bytes) -> Self {
+        Self {
+            method,
+            authorization: headers.get(header::AUTHORIZATION).cloned(),
+            content_type: headers.get(header::CONTENT_TYPE).cloned(),
+            body,
+        }
+    }
+}
+
+impl Peers {
+    /// The client of node `me`. Over HTTPS it trusts the platform's roots and every certificate
+    /// of this node's own `api.tls.cert` (the cluster's nodes usually share a CA), with the usual
+    /// hostname checks; redirects and proxies are not followed.
+    ///
+    /// # Errors
+    /// The certificate isn't PEM, or the client can't be built.
+    pub fn new(me: impl Into<String>, tls: Option<&TlsIdentity>) -> Result<Self, String> {
+        let mut b = reqwest::Client::builder()
+            .redirect(reqwest::redirect::Policy::none())
+            .no_proxy();
+        if let Some(tls) = tls {
+            let certs = reqwest::Certificate::from_pem_bundle(tls.cert.text.expose().as_bytes())
+                .map_err(|e| format!("the API certificate as a peer's trust: {e}"))?;
+            b = b.tls_certs_merge(certs);
+        }
+        Ok(Self {
+            me: me.into(),
+            http: b
+                .build()
+                .map_err(|e| format!("the peers' HTTP client: {e}"))?,
+        })
+    }
+
+    /// Sends `asked` to `path_and_query` on node `node` at `api_url`, within `limit`: its answer
+    /// as it gave it (with [`ANSWERED_BY`]), or `502` when it can't be reached, `504` past the
+    /// limit.
+    pub(crate) async fn send(
+        &self,
+        node: &str,
+        api_url: &str,
+        path_and_query: &str,
+        asked: Asked,
+        limit: Duration,
+    ) -> Response {
+        let mut req = self
+            .http
+            .request(asked.method, format!("{api_url}{path_and_query}"))
+            .timeout(limit)
+            .header(TARGET, node)
+            .header(FORWARDED_BY, &self.me)
+            .body(asked.body);
+        if let Some(a) = asked.authorization {
+            req = req.header(header::AUTHORIZATION, a);
+        }
+        if let Some(c) = asked.content_type {
+            req = req.header(header::CONTENT_TYPE, c);
+        }
+        let unanswered =
+            |code: StatusCode, why: String| body(code, &json!({ "error": why, "node": node }));
+        let answer = match req.send().await {
+            Ok(a) => a,
+            Err(e) if e.is_timeout() => {
+                return unanswered(
+                    StatusCode::GATEWAY_TIMEOUT,
+                    format!("{node} didn't answer within {limit:?}"),
+                );
+            }
+            Err(e) => {
+                return unanswered(
+                    StatusCode::BAD_GATEWAY,
+                    format!("{node} didn't answer: {}", cause(&e)),
+                );
+            }
+        };
+        let code = answer.status();
+        let content_type = answer.headers().get(header::CONTENT_TYPE).cloned();
+        if answer
+            .content_length()
+            .is_some_and(|n| n > MAX_ANSWER as u64)
+        {
+            return unanswered(
+                StatusCode::BAD_GATEWAY,
+                format!("{node}'s answer is over {MAX_ANSWER} bytes"),
+            );
+        }
+        let bytes = match answer.bytes().await {
+            Ok(b) if b.len() <= MAX_ANSWER => b,
+            Ok(_) => {
+                return unanswered(
+                    StatusCode::BAD_GATEWAY,
+                    format!("{node}'s answer is over {MAX_ANSWER} bytes"),
+                );
+            }
+            Err(e) if e.is_timeout() => {
+                return unanswered(
+                    StatusCode::GATEWAY_TIMEOUT,
+                    format!("{node} didn't answer within {limit:?}"),
+                );
+            }
+            Err(e) => {
+                return unanswered(
+                    StatusCode::BAD_GATEWAY,
+                    format!("{node}'s answer: {}", cause(&e)),
+                );
+            }
+        };
+        let mut r = (code, bytes).into_response();
+        if let Some(c) = content_type {
+            r.headers_mut().insert(header::CONTENT_TYPE, c);
+        }
+        if let Ok(n) = HeaderValue::from_str(node) {
+            r.headers_mut().insert(ANSWERED_BY, n);
+        }
+        r
+    }
+}
+
+/// The innermost cause: reqwest's own message says only "error sending request".
+fn cause(e: &(dyn std::error::Error + 'static)) -> String {
+    let mut inner = e;
+    while let Some(next) = inner.source() {
+        inner = next;
+    }
+    inner.to_string()
+}
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 0139afa..bbc7079 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -285,6 +285,9 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             recorder,
             denials: Arc::new(crate::RateLimit::denials()),
             events: Arc::clone(&events),
+            peers: crate::Peers::new("node-a", None).unwrap(),
+            process: tokio::sync::watch::channel(Default::default()).1,
+            gossip: gossip.figures(),
         },
         events,
         leases,
````

`crates/data-avalanche-cluster/src/coordinator.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index a556903..d9ab9bd 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -208,6 +208,11 @@ impl ConfigCoordinator {
         *self.inner.applied.borrow()
     }
 
+    /// The generation this node runs: its number, its text's hash, who wrote it and when.
+    pub async fn generation(&self) -> GenerationDoc {
+        self.inner.running.lock().await.generation.doc.clone()
+    }
+
     /// The config this node runs: its generation's text as resolved here.
     pub async fn config(&self) -> ResolvedConfig {
         self.inner.running.lock().await.config.clone()
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 30f8038..f562457 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -7,7 +7,9 @@ use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::{Arc, OnceLock};
 use std::time::{Duration, SystemTime};
 
-use data_avalanche_api::{Api, Auth, Control, Node, RateLimit, ServeError, Stop, bind, router};
+use data_avalanche_api::{
+    Api, Auth, Control, Node, Peers, RateLimit, ServeError, Stop, bind, router,
+};
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
@@ -119,7 +121,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     // The listener answers from here: live at once, ready once every controller runs.
     let metrics = Metrics::new();
     // The process's memory and CPU every 15 s, for /metrics and the node's page (D-M13b.7).
-    let (sampled, _process) = watch::channel(ProcessFigures::default());
+    let (sampled, process) = watch::channel(ProcessFigures::default());
     let _sampler = tokio::spawn(sample_process(metrics.clone(), sampled));
     let ready = Arc::new(AtomicBool::new(false));
     let node = Arc::new(OnceLock::new());
@@ -269,6 +271,9 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         c.lease_renewal_interval,
     ));
     let (control, asked) = Control::new();
+    // Another node's detail and actions, asked with the caller's own credentials (D-M13b.4).
+    let peers = Peers::new(deps.holder.node_id.clone(), config.globals.api.tls.as_ref())
+        .map_err(Fatal::Config)?;
     let _ = node.set(Node {
         coordinator: Arc::clone(&coordinator),
         members: gossip.members(),
@@ -280,6 +285,9 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         recorder: recorder.clone(),
         denials: Arc::new(RateLimit::denials()),
         events: Arc::clone(&events),
+        peers,
+        process,
+        gossip: gossip.figures(),
     });
     ready.store(true, Ordering::SeqCst);
     let mut cluster = Some((gossip, reconciler, discovery));
````

`docs/public/openapi.json`:

````diff
diff --git a/docs/public/openapi.json b/docs/public/openapi.json
index 5703a99..9243a2b 100644
--- a/docs/public/openapi.json
+++ b/docs/public/openapi.json
@@ -504,6 +504,573 @@
         "x-min-role": "viewer"
       }
     },
+    "/v1/nodes/{node}": {
+      "get": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "A node's detail: its build and start, its file against its generation, its process and gossip figures, the leases it holds; another node's is fetched from it",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the node's detail",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
+    "/v1/nodes/{node}/config/reload": {
+      "post": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "Publish a node's own file as the next generation, through any node: its own `POST /v1/config/reload`, with the query",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
+        "responses": {
+          "200": {
+            "description": "the file is the generation the cluster runs",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "202": {
+            "description": "published",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "other nodes published first, every time",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "413": {
+            "description": "the body is over 1 MiB",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "422": {
+            "description": "invalid, or a restart-only field without restart=accept; or the note is too long, not a string, holds a control character, or the body isn't a JSON object",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "operator"
+      }
+    },
+    "/v1/nodes/{node}/config/status": {
+      "get": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "A node's generation and its own restart-pending fields: its own `GET /v1/config/status`",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the node's config status",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
+    "/v1/nodes/{node}/config/validate": {
+      "post": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "Check a node's own file (an empty body), or the body, on that node: its own `POST /v1/config/validate`",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "requestBody": {
+          "content": {
+            "application/yaml": {}
+          },
+          "required": false
+        },
+        "responses": {
+          "200": {
+            "description": "it loads: its warnings and the changes",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "422": {
+            "description": "invalid, or it changes a restart-only field: the report",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "429": {
+            "description": "posted bodies past the node's rate",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "operator"
+      }
+    },
+    "/v1/nodes/{node}/leave": {
+      "post": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "Ask a node to leave the cluster, through any node: its own `POST /v1/cluster/leave`",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
+        "responses": {
+          "202": {
+            "description": "the leave is under way",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "leaving already",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "413": {
+            "description": "the body is over 1 MiB",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "422": {
+            "description": "the note is too long, not a string, holds a control character, or the body isn't a JSON object",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "admin"
+      }
+    },
+    "/v1/nodes/{node}/shutdown": {
+      "post": {
+        "tags": [
+          "nodes"
+        ],
+        "summary": "Ask a node to shut down, through any node: its own `POST /v1/admin/shutdown`",
+        "parameters": [
+          {
+            "name": "node",
+            "in": "path",
+            "description": "a member's name, as the gossip knows it",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
+        "responses": {
+          "202": {
+            "description": "the shutdown is under way",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no member of that name in this node's view",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "shutting down already",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "413": {
+            "description": "the body is over 1 MiB",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "421": {
+            "description": "a request forwarded to a node that isn't its target",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "422": {
+            "description": "the note is too long, not a string, holds a control character, or the body isn't a JSON object",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "502": {
+            "description": "the node didn't answer",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "504": {
+            "description": "the node didn't answer within the time limit",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "admin"
+      }
+    },
     "/v1/openapi.json": {
       "get": {
         "tags": [
````

`docs/src/content/docs/reference/api.md`:

````diff
diff --git a/docs/src/content/docs/reference/api.md b/docs/src/content/docs/reference/api.md
index 7163a65..a2d8016 100644
--- a/docs/src/content/docs/reference/api.md
+++ b/docs/src/content/docs/reference/api.md
@@ -284,6 +284,125 @@ Least role: `admin`.
 | `413` | the body is over 1 MiB |
 | `422` | the note is too long, not a string, holds a control character, or the body isn't a JSON object |
 
+## nodes
+
+Any member, through any node: its detail, and the actions only it takes, sent to it with the caller's own credentials. A node that doesn't answer is named (`502`, `504`).
+
+### `GET /v1/nodes/{node}`
+
+A node's detail: its build and start, its file against its generation, its process and gossip figures, the leases it holds; another node's is fetched from it
+
+Least role: `viewer`.
+
+- Path parameter `node`: a member's name, as the gossip knows it.
+
+| Status | When |
+|---|---|
+| `200` | the node's detail |
+| `404` | no member of that name in this node's view |
+| `421` | a request forwarded to a node that isn't its target |
+| `502` | the node didn't answer |
+| `504` | the node didn't answer within the time limit |
+
+### `POST /v1/nodes/{node}/config/reload`
+
+Publish a node's own file as the next generation, through any node: its own `POST /v1/config/reload`, with the query
+
+Least role: `operator`.
+
+- Path parameter `node`: a member's name, as the gossip knows it.
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
+
+| Status | When |
+|---|---|
+| `200` | the file is the generation the cluster runs |
+| `202` | published |
+| `404` | no member of that name in this node's view |
+| `409` | other nodes published first, every time |
+| `413` | the body is over 1 MiB |
+| `421` | a request forwarded to a node that isn't its target |
+| `422` | invalid, or a restart-only field without restart=accept; or the note is too long, not a string, holds a control character, or the body isn't a JSON object |
+| `502` | the node didn't answer |
+| `504` | the node didn't answer within the time limit |
+
+### `GET /v1/nodes/{node}/config/status`
+
+A node's generation and its own restart-pending fields: its own `GET /v1/config/status`
+
+Least role: `viewer`.
+
+- Path parameter `node`: a member's name, as the gossip knows it.
+
+| Status | When |
+|---|---|
+| `200` | the node's config status |
+| `404` | no member of that name in this node's view |
+| `421` | a request forwarded to a node that isn't its target |
+| `502` | the node didn't answer |
+| `504` | the node didn't answer within the time limit |
+
+### `POST /v1/nodes/{node}/config/validate`
+
+Check a node's own file (an empty body), or the body, on that node: its own `POST /v1/config/validate`
+
+Least role: `operator`.
+
+- Path parameter `node`: a member's name, as the gossip knows it.
+- Body: `application/yaml`.
+
+| Status | When |
+|---|---|
+| `200` | it loads: its warnings and the changes |
+| `404` | no member of that name in this node's view |
+| `421` | a request forwarded to a node that isn't its target |
+| `422` | invalid, or it changes a restart-only field: the report |
+| `429` | posted bodies past the node's rate |
+| `502` | the node didn't answer |
+| `504` | the node didn't answer within the time limit |
+
+### `POST /v1/nodes/{node}/leave`
+
+Ask a node to leave the cluster, through any node: its own `POST /v1/cluster/leave`
+
+Least role: `admin`.
+
+- Path parameter `node`: a member's name, as the gossip knows it.
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
+
+| Status | When |
+|---|---|
+| `202` | the leave is under way |
+| `404` | no member of that name in this node's view |
+| `409` | leaving already |
+| `413` | the body is over 1 MiB |
+| `421` | a request forwarded to a node that isn't its target |
+| `422` | the note is too long, not a string, holds a control character, or the body isn't a JSON object |
+| `502` | the node didn't answer |
+| `504` | the node didn't answer within the time limit |
+
+### `POST /v1/nodes/{node}/shutdown`
+
+Ask a node to shut down, through any node: its own `POST /v1/admin/shutdown`
+
+Least role: `admin`.
+
+- Path parameter `node`: a member's name, as the gossip knows it.
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
+
+| Status | When |
+|---|---|
+| `202` | the shutdown is under way |
+| `404` | no member of that name in this node's view |
+| `409` | shutting down already |
+| `413` | the body is over 1 MiB |
+| `421` | a request forwarded to a node that isn't its target |
+| `422` | the note is too long, not a string, holds a control character, or the body isn't a JSON object |
+| `502` | the node didn't answer |
+| `504` | the node didn't answer within the time limit |
+
 ## events
 
 The cluster's timeline and audit trail, kept for `globals.console.events.retention`.
````

`xtask/src/api.rs`:

````diff
diff --git a/xtask/src/api.rs b/xtask/src/api.rs
index c74917b..5a53d8d 100644
--- a/xtask/src/api.rs
+++ b/xtask/src/api.rs
@@ -20,6 +20,10 @@ const TAGS: &[(&str, &str)] = &[
     ),
     ("cluster", "Membership and leases."),
     ("admin", "The node's process."),
+    (
+        "nodes",
+        "Any member, through any node: its detail, and the actions only it takes, sent to it with the caller's own credentials. A node that doesn't answer is named (`502`, `504`).",
+    ),
     (
         "events",
         "The cluster's timeline and audit trail, kept for `globals.console.events.retention`.",
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/actions.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/nodes.rs crates/data-avalanche-api/src/openapi.rs crates/data-avalanche-api/src/peers.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche-cluster/src/coordinator.rs crates/data-avalanche/src/run.rs docs/public/openapi.json docs/src/content/docs/reference/api.md xtask/src/api.rs
git commit -m "feat(api): a node's detail and its actions through any node, with the caller's own credentials"
```

---

### Task 6: Generation records and masked texts (`data-avalanche-state`, `-config`, `-cluster`, `-api`)

**Files:**
- Create: `crates/data-avalanche-api/src/generations.rs`, `crates/data-avalanche-config/src/mask.rs`, `crates/data-avalanche-state/src/history.rs`
- Modify: `crates/data-avalanche-api/openapi.json`, `crates/data-avalanche-api/src/actions.rs`, `crates/data-avalanche-api/src/config.rs`, `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-api/src/openapi.rs`, `crates/data-avalanche-cluster/src/coordinator.rs`, `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-config/src/leaf.rs`, `crates/data-avalanche-config/src/lib.rs`, `crates/data-avalanche-config/src/walk.rs`, `crates/data-avalanche-state/src/generation.rs`, `crates/data-avalanche-state/src/lib.rs`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`

**Interfaces:**
- Consumes: `ConfigGenerations` (M8b), the recording layer's note (M13a), `Walk` (M1),
  `fingerprint` and the cluster's `FingerprintKey`.
- Produces: state `GenerationRecord { generation, hash, node, at, by: Option<Actor>, note:
  Option<String>, summary: Option<String> }`, `Listed { generation, hash: Option<String>, record:
  Option<GenerationRecord>, current: bool }`, `HISTORY_PREFIX`, `history_key(u64)`,
  `ConfigGenerations::{record(&GenerationRecord), history(Option<u64>, usize) -> Vec<Listed>,
  one(u64) -> Option<(Listed, Option<String>)>}`; config `MASK`, `MaskError`, `mask_secrets(&str,
  &FingerprintKey) -> Result<String, MaskError>`, `Walk::leaves`, `Leaf::written`; cluster
  `Attribution { by: Option<Actor>, note: Option<String> }`, `ConfigCoordinator::{reload_as,
  generations, mask}`; api `Noted`, `GET /v1/config/generations` and
  `GET /v1/config/generations/{generation}`.

- [ ] **Step 1: Write the failing tests**

The tests: in the new modules (Step 3): `the_history_lists_each_generation_newest_first_with_its_record`, `a_race_s_leftover_snapshot_never_passes_for_a_generation`, `a_generations_secrets_are_masked_in_place`, `two_masks_differ_only_where_the_secret_does`, `a_password_that_is_also_the_user_is_masked_only_as_the_password`, `the_secret_keys_are_the_models`, `a_generation_is_listed_with_its_publisher_and_shown_masked`; and `a_reload_records_who_published_the_generation_and_why` (coordinator).

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 17dd5c5..e8b4e40 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -731,6 +764,8 @@ mod tests {
                 ("POST", "/v1/config/validate", Operator),
                 ("POST", "/v1/config/reload", Operator),
                 ("GET", "/v1/config/status", Viewer),
+                ("GET", "/v1/config/generations", Viewer),
+                ("GET", "/v1/config/generations/{generation}", Viewer),
                 ("GET", "/v1/cluster/status", Viewer),
                 ("GET", "/v1/pipelines", Viewer),
                 ("GET", "/v1/pipelines/{id}", Viewer),
````

`crates/data-avalanche-cluster/src/coordinator.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index d9ab9bd..6e3fffd 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -1245,6 +1299,49 @@ mod tests {
         );
     }
 
+    /// The published generation's record names who asked and why, and what it changed.
+    #[tokio::test(start_paused = true)]
+    async fn a_reload_records_who_published_the_generation_and_why() {
+        let n = lone(&["p"]).await;
+        let asked = Attribution {
+            by: Some(Actor {
+                name: "oleksii".into(),
+                role: "operator".into(),
+            }),
+            note: Some("a second pipeline".into()),
+        };
+        n.coordinator
+            .reload_as(
+                &config_text(&["p", "q"]),
+                RestartFields::Refuse,
+                asked.clone(),
+            )
+            .await
+            .unwrap();
+        let listed = n.generations.history(None, 10).await.unwrap();
+        let record = listed[0].record.clone().expect("generation 2's record");
+        assert_eq!(
+            (
+                record.generation,
+                record.node.as_str(),
+                record.by,
+                record.note
+            ),
+            (2, "n1", asked.by, asked.note)
+        );
+        assert_eq!(
+            record.summary.as_deref(),
+            Some("1 pipelines added, 0 removed, 0 changed (0 restarted), 0 global fields")
+        );
+        let [(_, _, text)] = &n.recorded(1).await[..] else {
+            panic!("one event")
+        };
+        assert!(
+            text.starts_with("config generation 2 published by oleksii on n1: "),
+            "{text}"
+        );
+    }
+
     /// The file as the cluster runs it already: nothing is published.
     #[tokio::test(start_paused = true)]
     async fn an_unchanged_file_publishes_nothing() {
````

`crates/data-avalanche-cluster/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 0ad6b55..ed556b0 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -31,8 +31,8 @@ pub use controller::{
     SOURCE_CHANGES_IN_A_ROW, Target, Update, WorkerFactory,
 };
 pub use coordinator::{
-    Checked, ConfigCoordinator, CoordinatorDeps, DocsFn, PUBLISH_ATTEMPTS, ReloadError, Reloaded,
-    RestartFields, removed_pipelines,
+    Attribution, Checked, ConfigCoordinator, CoordinatorDeps, DocsFn, PUBLISH_ATTEMPTS,
+    ReloadError, Reloaded, RestartFields, removed_pipelines,
 };
 pub use discovery::{Seeds, advertise, discover, interfaces};
 pub use error::ClusterError;
````

`crates/data-avalanche-state/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index a1b1fd1..38e7b6a 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -56,6 +57,7 @@ pub use generation::{
     ConfigGeneration, ConfigGenerations, GENERATION_KEY, GenerationDoc, config_hash,
     config_snapshot_key,
 };
+pub use history::{GenerationRecord, HISTORY_PREFIX, Listed, history_key};
 pub use lease::{Acquired, Holder, LeaseDoc, Leases, lease_key};
 pub use lifecycle::{
     Action, ActualState, REQUESTS_KEY, Request, RequestsDoc, STATUS_KEY, StatusDoc, Transition,
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-state -p data-avalanche-config -p data-avalanche-cluster -p data-avalanche-api`
Expected: the build fails: no `Attribution`, `reload_as` or `generations()`.

- [ ] **Step 3: Implement**

`openapi.json` and the site's copies are regenerated as in Task 5.

`crates/data-avalanche-api/openapi.json`:

````diff
diff --git a/crates/data-avalanche-api/openapi.json b/crates/data-avalanche-api/openapi.json
index 9243a2b..9c6743c 100644
--- a/crates/data-avalanche-api/openapi.json
+++ b/crates/data-avalanche-api/openapi.json
@@ -185,6 +185,157 @@
         "x-min-role": "viewer"
       }
     },
+    "/v1/config/generations": {
+      "get": {
+        "tags": [
+          "config"
+        ],
+        "summary": "The config generations, newest first: who published each, why, and what it changed",
+        "parameters": [
+          {
+            "name": "before",
+            "in": "query",
+            "description": "generations older than this one: the `next` of the page before",
+            "required": false,
+            "schema": {
+              "type": "integer",
+              "minimum": 1
+            }
+          },
+          {
+            "name": "limit",
+            "in": "query",
+            "description": "at most this many (1–100, 20 by default)",
+            "required": false,
+            "schema": {
+              "type": "integer",
+              "default": 20,
+              "maximum": 100,
+              "minimum": 1
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the generations, and `next` when there are more",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "a parameter that isn't one, or isn't in its form",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
+    "/v1/config/generations/{generation}": {
+      "get": {
+        "tags": [
+          "config"
+        ],
+        "summary": "One config generation and its text, its secrets masked",
+        "parameters": [
+          {
+            "name": "generation",
+            "in": "path",
+            "description": "a config generation's number",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the generation and its text",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "a generation that isn't a number",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no such generation",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "several snapshots of it and no record of which one it is",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "500": {
+            "description": "its text couldn't be masked, so it isn't shown",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
     "/v1/config/reload": {
       "post": {
         "tags": [
````

`crates/data-avalanche-api/src/actions.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/actions.rs b/crates/data-avalanche-api/src/actions.rs
index e21cedb..0611bc6 100644
--- a/crates/data-avalanche-api/src/actions.rs
+++ b/crates/data-avalanche-api/src/actions.rs
@@ -154,6 +154,7 @@ pub(crate) async fn record(State(gate): State<Gate>, req: Request, next: Next) -
             })
             .unwrap_or_default()
     };
+    parts.extensions.insert(Noted(note.clone()));
     let action = Action {
         recorded,
         by,
@@ -181,6 +182,11 @@ pub(crate) async fn record(State(gate): State<Gate>, req: Request, next: Next) -
     Response::from_parts(head, Body::from(answer_bytes))
 }
 
+/// The note a mutating call's body carried, for the route that keeps it beside its own work (a
+/// reload's generation record).
+#[derive(Debug, Clone, Default)]
+pub(crate) struct Noted(pub Option<String>);
+
 /// An action under way through a mutating route: what it records, who asked and why.
 struct Action {
     recorded: Recorded,
````

`crates/data-avalanche-api/src/config.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index ceca7eb..eb2851a 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -6,10 +6,12 @@ use std::time::Instant;
 use axum::extract::{Extension, RawQuery, State};
 use axum::http::{HeaderValue, StatusCode, header};
 use axum::response::Response;
-use data_avalanche_cluster::{Checked, ReloadError, Reloaded, RestartFields};
+use data_avalanche_cluster::{Attribution, Checked, ReloadError, Reloaded, RestartFields};
 use data_avalanche_config::Fingerprints;
+use data_avalanche_state::Actor;
 use serde_json::json;
 
+use crate::actions::Noted;
 use crate::{Api, Failure, Node, Principal, body, starting};
 
 /// A refused reload or check: 422 with the report, 409 after the retries, 503 when
@@ -111,7 +113,12 @@ pub(crate) async fn validate(
 /// changes a field only a process restart applies. With `?restart=accept` such a field is
 /// published, and each node applies it at its next start (DA-OPS-12); a field the cluster keeps
 /// for good is refused all the same. Both answers name the fields this node has pending.
-pub(crate) async fn reload(State(api): State<Api>, RawQuery(query): RawQuery) -> Response {
+pub(crate) async fn reload(
+    State(api): State<Api>,
+    Extension(by): Extension<Principal>,
+    noted: Option<Extension<Noted>>,
+    RawQuery(query): RawQuery,
+) -> Response {
     let restart = match query.as_deref() {
         None | Some("") => RestartFields::Refuse,
         Some("restart=accept") => RestartFields::Accept,
@@ -129,7 +136,14 @@ pub(crate) async fn reload(State(api): State<Api>, RawQuery(query): RawQuery) ->
         Ok(text) => text,
         Err(failed) => return failed.into(),
     };
-    match node.coordinator.reload(&text, restart).await {
+    let asked = Attribution {
+        by: Some(Actor {
+            name: by.name,
+            role: by.role.to_string(),
+        }),
+        note: noted.and_then(|Extension(Noted(note))| note),
+    };
+    match node.coordinator.reload_as(&text, restart, asked).await {
         Ok(Reloaded {
             generation,
             published: true,
````

`crates/data-avalanche-api/src/generations.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/generations.rs b/crates/data-avalanche-api/src/generations.rs
new file mode 100644
index 0000000..ee72154
--- /dev/null
+++ b/crates/data-avalanche-api/src/generations.rs
@@ -0,0 +1,228 @@
+//! `GET /v1/config/generations` and `GET /v1/config/generations/{generation}` (Admin_Console §4
+//! Config, D-M13b.9): the cluster's config generations, newest first, each with who published it
+//! and why; and one generation with its text, its secrets masked (D-M13b.10).
+
+use axum::extract::{Path, RawQuery, State};
+use axum::http::StatusCode;
+use axum::response::Response;
+use data_avalanche_state::{Listed, StateError};
+use serde_json::{Value, json};
+
+use crate::{Api, Failure, body, starting};
+
+/// The list's parameters.
+pub const PARAMETERS: &[(&str, &str)] = &[
+    (
+        "before",
+        "generations older than this one: the `next` of the page before",
+    ),
+    ("limit", "at most this many (1–100, 20 by default)"),
+];
+
+pub const DEFAULT_LIMIT: usize = 20;
+pub const MAX_LIMIT: usize = 100;
+
+fn query(raw: Option<&str>) -> Result<(Option<u64>, usize), Failure> {
+    let mut before = None;
+    let mut limit = DEFAULT_LIMIT;
+    for (name, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
+        match &*name {
+            "before" => {
+                before = Some(value.trim().parse().map_err(|_| {
+                    Failure::new(StatusCode::BAD_REQUEST, "before is a generation's number")
+                })?);
+            }
+            "limit" => {
+                limit = value
+                    .trim()
+                    .parse()
+                    .ok()
+                    .filter(|n| (1..=MAX_LIMIT).contains(n))
+                    .ok_or_else(|| {
+                        Failure::new(
+                            StatusCode::BAD_REQUEST,
+                            format!("limit is 1 to {MAX_LIMIT}"),
+                        )
+                    })?;
+            }
+            other => {
+                return Err(Failure::new(
+                    StatusCode::BAD_REQUEST,
+                    format!("unknown parameter {other:?}: the generations take before, limit"),
+                ));
+            }
+        }
+    }
+    Ok((before, limit))
+}
+
+fn listed_json(l: &Listed) -> Value {
+    let mut v = json!({
+        "generation": l.generation,
+        "hash": l.hash,
+        "current": l.current,
+    });
+    if let Some(r) = &l.record {
+        v["node"] = json!(r.node);
+        v["at"] = json!(r.at);
+        v["by"] = json!(r.by);
+        v["note"] = json!(r.note);
+        v["summary"] = json!(r.summary);
+    }
+    v
+}
+
+fn unreadable(e: &StateError) -> Response {
+    body(
+        StatusCode::SERVICE_UNAVAILABLE,
+        &json!({ "error": format!("the config generations can't be read: {e}") }),
+    )
+}
+
+/// The generations, newest first; `next` names the page after when this one is full.
+pub(crate) async fn list(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (before, limit) = match query(raw.as_deref()) {
+        Ok(q) => q,
+        Err(f) => return f.into(),
+    };
+    match node.coordinator.generations().history(before, limit).await {
+        Ok(page) => {
+            let next = (page.len() == limit)
+                .then(|| page.last().map(|l| l.generation))
+                .flatten();
+            body(
+                StatusCode::OK,
+                &json!({
+                    "generations": page.iter().map(listed_json).collect::<Vec<_>>(),
+                    "next": next,
+                }),
+            )
+        }
+        Err(e) => unreadable(&e),
+    }
+}
+
+/// One generation and its text, secrets masked.
+pub(crate) async fn one(State(api): State<Api>, Path(generation): Path<String>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let Ok(n) = generation.parse::<u64>() else {
+        return Failure::new(StatusCode::BAD_REQUEST, "a generation is a number").into();
+    };
+    match node.coordinator.generations().one(n).await {
+        Ok(None) => Failure::new(StatusCode::NOT_FOUND, format!("no config generation {n}")).into(),
+        Ok(Some((_, None))) => Failure::new(
+            StatusCode::CONFLICT,
+            format!(
+                "config generation {n} has several snapshots and no record of which one it is: a publish that lost its race left one behind"
+            ),
+        )
+        .into(),
+        Ok(Some((listed, Some(text)))) => match node.coordinator.mask(&text) {
+            Ok(masked) => {
+                let mut v = listed_json(&listed);
+                v["text"] = json!(masked);
+                body(StatusCode::OK, &v)
+            }
+            Err(e) => Failure::new(
+                StatusCode::INTERNAL_SERVER_ERROR,
+                format!("config generation {n}'s text isn't shown: {e}"),
+            )
+            .into(),
+        },
+        Err(e) => unreadable(&e),
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use std::sync::Arc;
+    use std::sync::OnceLock;
+    use std::sync::atomic::AtomicBool;
+
+    use data_avalanche_config::MASK;
+    use data_avalanche_telemetry::Metrics;
+
+    use super::*;
+    use crate::test_support::{CONFIG, json_of, lone, open, request, serve};
+
+    /// Each generation is listed with who published it and why; one generation's text shows
+    /// its secrets masked.
+    #[tokio::test]
+    async fn a_generation_is_listed_with_its_publisher_and_shown_masked() {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, CONFIG).unwrap();
+        let lone = lone(file.clone()).await;
+        let node = Arc::new(OnceLock::new());
+        node.set(lone.node.clone()).unwrap();
+        let addr = serve(Api {
+            metrics: Metrics::new(),
+            ready: Arc::new(AtomicBool::new(true)),
+            node,
+            auth: open(),
+        })
+        .await;
+        std::fs::write(
+            &file,
+            CONFIG.replace("drain_timeout: 45s", "drain_timeout: 40s"),
+        )
+        .unwrap();
+        let (code, _, _) = request(
+            addr,
+            "POST",
+            "/v1/config/reload",
+            &[],
+            r#"{"note": "shorter drain"}"#,
+        )
+        .await;
+        assert_eq!(code, 202);
+
+        let (code, v) = json_of(addr, "GET", "/v1/config/generations").await;
+        assert_eq!(code, 200, "{v}");
+        let gens = v["generations"].as_array().unwrap();
+        assert_eq!(gens.len(), 2, "{v}");
+        assert_eq!(
+            (
+                &gens[0]["generation"],
+                &gens[0]["current"],
+                &gens[0]["note"]
+            ),
+            (&json!(2), &json!(true), &json!("shorter drain"))
+        );
+        assert_eq!(gens[0]["by"]["name"], "anonymous", "{v}");
+        assert_eq!(gens[0]["node"], "node-a");
+        assert_eq!(gens[1]["summary"], "bootstrap");
+        assert_eq!(v["next"], Value::Null);
+        let (_, v) = json_of(addr, "GET", "/v1/config/generations?limit=1").await;
+        assert_eq!(
+            (v["generations"][0]["generation"].clone(), v["next"].clone()),
+            (json!(2), json!(2))
+        );
+        let (_, v) = json_of(addr, "GET", "/v1/config/generations?before=2").await;
+        assert_eq!(v["generations"][0]["generation"], 1);
+
+        let (code, v) = json_of(addr, "GET", "/v1/config/generations/2").await;
+        assert_eq!(code, 200, "{v}");
+        let text = v["text"].as_str().unwrap();
+        assert!(text.contains("drain_timeout: 40s"), "{text}");
+        assert!(
+            text.contains(&format!("password: \"{MASK} blake3:")),
+            "{text}"
+        );
+        assert!(!text.contains("password: pw"), "{text}");
+
+        for (path, want) in [
+            ("/v1/config/generations/3", 404),
+            ("/v1/config/generations/x", 400),
+            ("/v1/config/generations?limit=0", 400),
+            ("/v1/config/generations?after=1", 400),
+        ] {
+            assert_eq!(json_of(addr, "GET", path).await.0, want, "{path}");
+        }
+    }
+}
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 17dd5c5..e8b4e40 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -13,6 +13,7 @@ mod auth;
 mod cluster;
 mod config;
 mod events;
+mod generations;
 mod limit;
 mod nodes;
 mod openapi;
@@ -223,6 +224,38 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             get(config::status),
         ),
+        (
+            r(
+                "GET",
+                "/v1/config/generations",
+                Viewer,
+                "The config generations, newest first: who published each, why, and what it changed",
+                &[
+                    (200, "the generations, and `next` when there are more"),
+                    (400, "a parameter that isn't one, or isn't in its form"),
+                ],
+            ),
+            get(generations::list),
+        ),
+        (
+            r(
+                "GET",
+                "/v1/config/generations/{generation}",
+                Viewer,
+                "One config generation and its text, its secrets masked",
+                &[
+                    (200, "the generation and its text"),
+                    (400, "a generation that isn't a number"),
+                    (404, "no such generation"),
+                    (
+                        409,
+                        "several snapshots of it and no record of which one it is",
+                    ),
+                    (500, "its text couldn't be masked, so it isn't shown"),
+                ],
+            ),
+            get(generations::one),
+        ),
         (
             r(
                 "GET",
````

`crates/data-avalanche-api/src/openapi.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index 80befb6..df3b7de 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -46,6 +46,7 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
         ("id", "the pipeline's id"),
         ("job_id", "the snapshot job's id, `<pipeline id>~<suffix>`"),
         ("node", "a member's name, as the gossip knows it"),
+        ("generation", "a config generation's number"),
     ] {
         if r.path.contains(&format!("{{{name}}}")) {
             op = op.parameter(
@@ -58,6 +59,29 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
             );
         }
     }
+    if r.path == "/v1/config/generations" {
+        for (name, description) in crate::generations::PARAMETERS {
+            let schema = if *name == "limit" {
+                ObjectBuilder::new()
+                    .schema_type(Type::Integer)
+                    .minimum(Some(1))
+                    .maximum(Some(crate::generations::MAX_LIMIT))
+                    .default(Some(crate::generations::DEFAULT_LIMIT.into()))
+            } else {
+                ObjectBuilder::new()
+                    .schema_type(Type::Integer)
+                    .minimum(Some(1))
+            };
+            op = op.parameter(
+                ParameterBuilder::new()
+                    .name(*name)
+                    .parameter_in(ParameterIn::Query)
+                    .required(Required::False)
+                    .description(Some(*description))
+                    .schema(Some(schema)),
+            );
+        }
+    }
     if r.path == "/v1/events" {
         for (name, description) in crate::events::PARAMETERS {
             let schema = if *name == "limit" {
````

`crates/data-avalanche-cluster/src/coordinator.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index d9ab9bd..6e3fffd 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -11,13 +11,14 @@ use std::time::Duration;
 
 use chrono::Utc;
 use data_avalanche_config::{
-    ConfigDiff, Env, FingerprintKey, Fingerprints, Globals, Mutability, Pipeline, PipelineChange,
-    Report, ResolvedConfig, diff, diff_with, load_posted_str, load_str,
+    ConfigDiff, Env, FingerprintKey, Fingerprints, Globals, MaskError, Mutability, Pipeline,
+    PipelineChange, Report, ResolvedConfig, diff, diff_with, load_posted_str, load_str,
+    mask_secrets,
 };
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{
-    ConfigGeneration, ConfigGenerations, EventSeverity, GENERATION_KEY, GenerationDoc,
-    PipelineDocs, RemovalDoc, Removals, StateError, Versioned, config_hash,
+    Actor, ConfigGeneration, ConfigGenerations, EventSeverity, GENERATION_KEY, GenerationDoc,
+    GenerationRecord, PipelineDocs, RemovalDoc, Removals, StateError, Versioned, config_hash,
 };
 use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
@@ -71,6 +72,14 @@ impl Debug for CoordinatorDeps {
     }
 }
 
+/// Who asked for a reload, and why: the published generation's record keeps them
+/// (Admin_Console §4 Config). Both are none for a reload nobody asked for through the API.
+#[derive(Debug, Clone, Default, PartialEq, Eq)]
+pub struct Attribution {
+    pub by: Option<Actor>,
+    pub note: Option<String>,
+}
+
 /// Whether a reload may publish a change to a field only a process restart applies (config
 /// §1.2, V-061; DA-OPS-12).
 #[derive(Debug, Clone, Copy, PartialEq, Eq)]
@@ -213,6 +222,20 @@ impl ConfigCoordinator {
         self.inner.running.lock().await.generation.doc.clone()
     }
 
+    /// The cluster's config generations in `globals.state`: their records and texts.
+    pub fn generations(&self) -> &ConfigGenerations {
+        &self.inner.deps.generations
+    }
+
+    /// `text`, a generation's, with its secrets masked under the cluster's fingerprint key
+    /// ([`mask_secrets`]).
+    ///
+    /// # Errors
+    /// [`MaskError`]: the text isn't shown.
+    pub fn mask(&self, text: &str) -> Result<String, MaskError> {
+        mask_secrets(text, &self.inner.deps.key)
+    }
+
     /// The config this node runs: its generation's text as resolved here.
     pub async fn config(&self) -> ResolvedConfig {
         self.inner.running.lock().await.config.clone()
@@ -247,6 +270,21 @@ impl ConfigCoordinator {
         &self,
         text: &str,
         restart: RestartFields,
+    ) -> Result<Reloaded, ReloadError> {
+        self.reload_as(text, restart, Attribution::default()).await
+    }
+
+    /// [`Self::reload`], the generation it publishes recorded as `asked`'s: who and why, with
+    /// what it changed (`config/history/<generation>.json`). The record is written after the
+    /// pointer; a failure to write it is logged, and the generation stands.
+    ///
+    /// # Errors
+    /// As [`Self::reload`].
+    pub async fn reload_as(
+        &self,
+        text: &str,
+        restart: RestartFields,
+        asked: Attribution,
     ) -> Result<Reloaded, ReloadError> {
         let d = &self.inner.deps;
         let mut running = self.inner.running.lock().await;
@@ -291,16 +329,32 @@ impl ConfigCoordinator {
                 Ok(published) => {
                     let generation = published.pointer.doc.generation;
                     tracing::info!(generation, "published config generation {generation}");
+                    let record = GenerationRecord {
+                        generation,
+                        hash: published.pointer.doc.hash.clone(),
+                        node: d.node_id.clone(),
+                        at: published.pointer.doc.written_at,
+                        by: asked.by.clone(),
+                        note: asked.note.clone(),
+                        summary: Some(summary(&changes)),
+                    };
+                    if let Err(e) = d.generations.record(&record).await {
+                        tracing::warn!(generation, "config generation {generation}'s record: {e}");
+                    }
+                    let who = asked.by.as_ref().map_or_else(
+                        || d.node_id.clone(),
+                        |a| format!("{} on {}", a.name, d.node_id),
+                    );
                     let mut e = d.recorder.event(
                         "config.published",
                         EventSeverity::Info,
                         format!(
-                            "config generation {generation} published by {}: {}",
-                            d.node_id,
+                            "config generation {generation} published by {who}: {}",
                             summary(&changes)
                         ),
                     );
                     e.object = Some(generation.to_string());
+                    e.actor = asked.by;
                     d.recorder.record(e);
                     self.inner.adopt(&mut running, published).await;
                     return Ok(Reloaded {
````

`crates/data-avalanche-config/src/leaf.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/leaf.rs b/crates/data-avalanche-config/src/leaf.rs
index 8b9c3d6..c69ad1a 100644
--- a/crates/data-avalanche-config/src/leaf.rs
+++ b/crates/data-avalanche-config/src/leaf.rs
@@ -31,6 +31,8 @@ impl fmt::Display for Loc {
 pub(crate) struct Leaf<T> {
     text: String,
     loc: Option<Loc>,
+    /// Where the text itself is written: an alias's anchor, else `loc`.
+    written: Option<Loc>,
     _type: PhantomData<fn() -> T>,
 }
 
@@ -42,6 +44,7 @@ impl<T> Leaf<T> {
         Self {
             text: text.into(),
             loc: None,
+            written: None,
             _type: PhantomData,
         }
     }
@@ -52,6 +55,7 @@ impl<T> Leaf<T> {
         Self {
             text: text.into(),
             loc: Some(loc),
+            written: Some(loc),
             _type: PhantomData,
         }
     }
@@ -63,6 +67,12 @@ impl<T> Leaf<T> {
     pub fn loc(&self) -> Option<Loc> {
         self.loc
     }
+
+    /// Where the text is written in the file: an alias (`*pw`) is located at its anchor's
+    /// value, not at the alias.
+    pub fn written(&self) -> Option<Loc> {
+        self.written
+    }
 }
 
 // Manual impls: derives would demand `T: Clone`/`T: Debug`/`T: PartialEq`, but `T` is only a marker.
@@ -71,6 +81,7 @@ impl<T> Clone for Leaf<T> {
         Self {
             text: self.text.clone(),
             loc: self.loc,
+            written: self.written,
             _type: PhantomData,
         }
     }
@@ -99,14 +110,16 @@ impl<T> PartialEq for Leaf<T> {
 impl<'de, T> Deserialize<'de> for Leaf<T> {
     fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
         let spanned: Spanned<String> = Spanned::deserialize(d)?;
-        let at = spanned.referenced;
-        let loc = (at.line() > 0).then(|| Loc {
-            line: at.line(),
-            column: at.column(),
-        });
+        let loc_of = |at: serde_saphyr::Location| {
+            (at.line() > 0).then(|| Loc {
+                line: at.line(),
+                column: at.column(),
+            })
+        };
         Ok(Self {
+            loc: loc_of(spanned.referenced),
+            written: loc_of(spanned.defined),
             text: spanned.value,
-            loc,
             _type: PhantomData,
         })
     }
````

`crates/data-avalanche-config/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 3d7a32a..fff7c1a 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -76,6 +76,7 @@ mod field_docs;
 mod keys;
 mod leaf;
 mod load;
+mod mask;
 mod material;
 mod merge;
 mod mutability;
@@ -104,6 +105,7 @@ pub use field_docs::{DocDefault, FIELD_DOCS, FieldDoc};
 pub use keys::{Key, KeyShape, config_keys, key_mutability};
 pub use leaf::Loc;
 pub use load::{Loaded, load_file, load_posted_str, load_str, validate_str};
+pub use mask::{MASK, MaskError, mask_secrets};
 pub use material::{FileRef, Origin, Pem};
 pub use mutability::Mutability;
 pub use registry::{RULES, RuleInput, RuleLocus, RuleMeta, RuleSeverity, rule_meta};
````

`crates/data-avalanche-config/src/mask.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/mask.rs b/crates/data-avalanche-config/src/mask.rs
new file mode 100644
index 0000000..1ec3a6d
--- /dev/null
+++ b/crates/data-avalanche-config/src/mask.rs
@@ -0,0 +1,442 @@
+//! A config generation's text with its secrets masked in place (Admin_Console §4 Config,
+//! D-M13b.10): the console shows a generation's text, and its diff, without the secrets written
+//! in it. A secret written as a `${VAR}` reference stays as written: the text holds no value.
+//! A literal one becomes `"•••••• blake3:<8 hex>"`, the start of its fingerprint under the
+//! cluster's key (config §6.2), so that two generations show whether it changed. Everything else
+//! (comments, order, spacing) is left as written.
+//!
+//! The masked text is parsed again and checked before it is returned: every secret is then a
+//! mask or a reference. A text that fails the check isn't shown at all ([`MaskError`]). A
+//! secret's text that is also written as another key's value (a `user` that is the `password`)
+//! stays shown there: only the secret keys are masked.
+
+use thiserror::Error;
+
+use crate::leaf::Loc;
+use crate::parse::parse_document;
+use crate::raw::RawConfig;
+use crate::secret::{FingerprintKey, fingerprint};
+use crate::template::{Expanded, expand};
+use crate::walk::{Walk, Written, under};
+
+/// What a masked literal secret starts with.
+pub const MASK: &str = "••••••";
+
+/// The keys whose values are secrets, a pipeline's spelled under `pipelines.*` (they apply
+/// under `defaults` too). A `pem` key is a secret only as inline PEM; a path stays shown.
+const SECRET_KEYS: &[(&str, Pem)] = &[
+    ("globals.api.password", Pem::No),
+    ("globals.api.oauth2.client_secret", Pem::No),
+    ("globals.api.tls.key", Pem::Inline),
+    ("globals.tracing.headers.*", Pem::No),
+    ("globals.console.session_key", Pem::No),
+    ("globals.console.prometheus.basic.password", Pem::No),
+    ("globals.console.prometheus.bearer_token", Pem::No),
+    ("globals.console.prometheus.headers.*", Pem::No),
+    ("pipelines.*.source.password", Pem::No),
+    ("pipelines.*.source.ssl_key", Pem::Inline),
+    ("pipelines.*.sink.iceberg.catalog.hive.password", Pem::No),
+    ("pipelines.*.sink.iceberg.catalog.rest.token", Pem::No),
+    (
+        "pipelines.*.sink.iceberg.catalog.rest.oauth2.client_secret",
+        Pem::No,
+    ),
+];
+
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+enum Pem {
+    No,
+    Inline,
+}
+
+/// Why a text can't be shown masked.
+#[derive(Debug, Error, PartialEq, Eq)]
+pub enum MaskError {
+    #[error("the text doesn't parse as a config")]
+    Parse,
+    #[error("the secret at {0} couldn't be placed in the text")]
+    Unplaced(String),
+    #[error("the secret at {0} is still readable after masking")]
+    Leaked(String),
+}
+
+/// `text` with every secret masked in place, fingerprinted with `key` (the module's doc).
+///
+/// # Errors
+/// [`MaskError`]: the text isn't shown.
+pub fn mask_secrets(text: &str, key: &FingerprintKey) -> Result<String, MaskError> {
+    let secrets = secrets(text)?;
+    let mut edits: Vec<(usize, usize, String)> = vec![];
+    for s in &secrets {
+        let Some(value) = literal(&s.text) else {
+            continue;
+        };
+        let Some(at) = s.at else {
+            return Err(MaskError::Unplaced(s.path.join(".")));
+        };
+        let (start, end) =
+            span(text, at, &s.text).ok_or_else(|| MaskError::Unplaced(s.path.join(".")))?;
+        if edits.iter().any(|e| e.0 == start) {
+            continue; // one value under `defaults`, or one anchor, read by several pipelines
+        }
+        edits.push((start, end, format!("\"{}\"", masked(key, &value))));
+    }
+    edits.sort_by_key(|e| std::cmp::Reverse(e.0));
+    let mut out = text.to_owned();
+    for (start, end, with) in edits {
+        out.replace_range(start..end, &with);
+    }
+    check(&secrets, &out)?;
+    Ok(out)
+}
+
+/// A literal secret's mask.
+fn masked(key: &FingerprintKey, value: &str) -> String {
+    let fp = fingerprint(key, value.as_bytes());
+    let hex = fp.strip_prefix("blake3:").unwrap_or(&fp);
+    format!("{MASK} blake3:{}", &hex[..8])
+}
+
+/// The value of a leaf written as `text`, or `None` when it is a `${VAR}` reference.
+fn literal(text: &str) -> Option<String> {
+    match expand(text, None) {
+        Ok(Expanded::Text(t)) => Some(t),
+        Ok(Expanded::Unresolved) => None,
+        Err(_) => Some(text.to_owned()),
+    }
+}
+
+/// Every secret leaf written in `text`.
+fn secrets(text: &str) -> Result<Vec<Written>, MaskError> {
+    let raw: RawConfig = parse_document(text).map_err(|_| MaskError::Parse)?;
+    let mut all = vec![];
+    raw.globals.leaves(&["globals".to_owned()], &mut all);
+    raw.defaults.leaves(&["defaults".to_owned()], &mut all);
+    for (name, p) in raw.pipelines.iter().flatten() {
+        Some(p.clone()).leaves(&under(&["pipelines".to_owned()], name), &mut all);
+    }
+    all.retain(is_secret);
+    Ok(all)
+}
+
+fn is_secret(w: &Written) -> bool {
+    let mut path: Vec<&str> = w.path.iter().map(String::as_str).collect();
+    match path.first() {
+        Some(&"defaults") => {
+            path.splice(0..1, ["pipelines", "*"]);
+        }
+        Some(&"pipelines") if path.len() > 1 => path[1] = "*",
+        _ => {}
+    }
+    SECRET_KEYS.iter().any(|(pattern, pem)| {
+        let pattern: Vec<&str> = pattern.split('.').collect();
+        pattern.len() == path.len()
+            && pattern.iter().zip(&path).all(|(p, s)| *p == "*" || p == s)
+            && (*pem == Pem::No || w.text.trim_start().starts_with("-----BEGIN"))
+    })
+}
+
+/// The masked text's own check (the module's doc).
+fn check(before: &[Written], masked: &str) -> Result<(), MaskError> {
+    let after = secrets(masked)?;
+    for b in before {
+        let path = b.path.join(".");
+        let Some(a) = after.iter().find(|a| a.path == b.path) else {
+            // An inline PEM key masked is a secret no more: it reads as a path.
+            if b.text.trim_start().starts_with("-----BEGIN") {
+                continue;
+            }
+            return Err(MaskError::Leaked(path));
+        };
+        if literal(&b.text).is_some() && !a.text.starts_with(MASK) {
+            return Err(MaskError::Leaked(path));
+        }
+    }
+    if after
+        .iter()
+        .any(|a| literal(&a.text).is_some() && !a.text.starts_with(MASK))
+    {
+        return Err(MaskError::Leaked("a key".to_owned()));
+    }
+    Ok(())
+}
+
+/// The byte offset of `at` (1-based line, 1-based column in characters).
+fn offset(text: &str, at: Loc) -> Option<usize> {
+    let line_start = if at.line == 1 {
+        0
+    } else {
+        text.match_indices('\n')
+            .nth(usize::try_from(at.line).ok()? - 2)?
+            .0
+            + 1
+    };
+    let line = &text[line_start..];
+    let column = usize::try_from(at.column).ok()?.checked_sub(1)?;
+    let within = line
+        .char_indices()
+        .nth(column)
+        .map(|(i, _)| i)
+        .or_else(|| (line.chars().count() == column).then_some(line.len()))?;
+    Some(line_start + within)
+}
+
+/// Where the scalar written at `at` is in `text`, as a byte range to replace: a quoted scalar
+/// with its quotes, a block scalar from its `|` or `>` to its last line, a plain one without its
+/// comment. `None` when what is there doesn't read as `written` (the leaf's text).
+fn span(text: &str, at: Loc, written: &str) -> Option<(usize, usize)> {
+    let start = offset(text, at)?;
+    let rest = &text[start..];
+    match rest.chars().next()? {
+        '"' => {
+            let mut escaped = false;
+            for (i, c) in rest.char_indices().skip(1) {
+                match c {
+                    _ if escaped => escaped = false,
+                    '\\' => escaped = true,
+                    '"' => return Some((start, start + i + 1)),
+                    _ => {}
+                }
+            }
+            None
+        }
+        '\'' => {
+            let mut chars = rest.char_indices().skip(1).peekable();
+            while let Some((i, c)) = chars.next() {
+                if c == '\'' {
+                    if chars.peek().is_some_and(|(_, n)| *n == '\'') {
+                        chars.next();
+                    } else {
+                        return Some((start, start + i + 1));
+                    }
+                }
+            }
+            None
+        }
+        _ => block(text, start).or_else(|| plain(text, start, written)),
+    }
+}
+
+/// A block scalar whose first line starts at `start`: from its indicator to its last line.
+fn block(text: &str, start: usize) -> Option<(usize, usize)> {
+    let line_start = text[..start].rfind('\n').map_or(0, |i| i + 1);
+    if !text[line_start..start].chars().all(|c| c == ' ') {
+        return None;
+    }
+    let indent = start - line_start;
+    // The header: the last line above with anything on it, its comment left out.
+    let header = text[..line_start.checked_sub(1)?]
+        .lines()
+        .rev()
+        .find(|l| !l.trim().is_empty())?;
+    let header_end = text[..line_start].rfind(header)? + header.len();
+    let body = header.split(" #").next().unwrap_or(header).trim_end();
+    let indicator = body.trim_end_matches(|c: char| c.is_ascii_digit() || c == '-' || c == '+');
+    if !(indicator.ends_with('|') || indicator.ends_with('>')) {
+        return None;
+    }
+    let from = header_end - header.len() + indicator.len() - 1;
+    let mut end = start;
+    let mut at = line_start;
+    for line in text[line_start..].split_inclusive('\n') {
+        let content = line.trim_end_matches(['\n', '\r']);
+        if !content.trim().is_empty() {
+            let own = content.len() - content.trim_start_matches(' ').len();
+            if own < indent {
+                break;
+            }
+            end = at + content.len();
+        }
+        at += line.len();
+    }
+    Some((from, end))
+}
+
+/// A plain scalar starting at `start`, over as many lines as `written` folds.
+fn plain(text: &str, start: usize, written: &str) -> Option<(usize, usize)> {
+    let line_start = text[..start].rfind('\n').map_or(0, |i| i + 1);
+    let opened = text[line_start..start].chars().fold(0i32, |n, c| match c {
+        '{' | '[' => n + 1,
+        '}' | ']' => n - 1,
+        _ => n,
+    });
+    let first_end = |from: usize| {
+        let line = text[from..].split('\n').next().unwrap_or("");
+        let mut cut = line.len();
+        if let Some(i) = line.find(" #") {
+            cut = cut.min(i);
+        }
+        if opened > 0
+            && let Some(i) = line.find([',', '}', ']'])
+        {
+            cut = cut.min(i);
+        }
+        from + line[..cut].trim_end().len()
+    };
+    let mut end = first_end(start);
+    let mut read = text[start..end].to_owned();
+    while read != written {
+        if read.len() >= written.len() {
+            return None;
+        }
+        let next_line = end + text[end..].find('\n')? + 1;
+        let content_start =
+            next_line + (text[next_line..].len() - text[next_line..].trim_start_matches(' ').len());
+        end = first_end(content_start);
+        read.push(' ');
+        read.push_str(&text[content_start..end]);
+    }
+    Some((start, end))
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+    use crate::keys::config_keys;
+
+    fn key() -> FingerprintKey {
+        FingerprintKey::from_bytes([7; 32])
+    }
+
+    const TEXT: &str = "\
+config_version: 1
+# the cluster
+globals:
+  state: { bucket: app-state }
+  api:
+    auth: basic
+    user: admin
+    password: 'hunter2''s pass'   # rotate monthly
+    tls:
+      enabled: true
+      cert: /etc/da/tls.crt
+      key: |
+        -----BEGIN PRIVATE KEY-----
+        MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7
+        -----END PRIVATE KEY-----
+  tracing:
+    headers: { authorization: \"Bearer abcdefgh12345\", x-tenant: \"${TENANT}\" }
+  console:
+    session_key: ${SESSION_KEY}
+defaults:
+  source:
+    password: &pw plain-secret-value
+pipelines:
+  orders:
+    source:
+      password: *pw
+      ssl_key: /etc/da/client.key
+  users:
+    source:
+      password:
+        a folded
+        plain secret
+";
+
+    /// Every literal secret is replaced in place; a reference, a path and every other line stay
+    /// as written, comments included.
+    #[test]
+    fn a_generations_secrets_are_masked_in_place() {
+        let out = mask_secrets(TEXT, &key()).unwrap();
+        for gone in [
+            "hunter2",
+            "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7",
+            "abcdefgh12345",
+            "plain-secret-value",
+            "folded",
+        ] {
+            assert!(!out.contains(gone), "{gone} is still in:\n{out}");
+        }
+        for kept in [
+            "# the cluster",
+            "    password: \"•••••• blake3:",
+            "   # rotate monthly",
+            "      key: \"•••••• blake3:",
+            "x-tenant: \"${TENANT}\" }",
+            "session_key: ${SESSION_KEY}",
+            "password: &pw \"•••••• blake3:",
+            "      password: *pw\n",
+            "ssl_key: /etc/da/client.key",
+        ] {
+            assert!(out.contains(kept), "{kept} is missing from:\n{out}");
+        }
+        assert_eq!(out.matches(MASK).count(), 5, "{out}");
+        assert!(
+            out.contains("      password:\n        \"•••••• blake3:"),
+            "the folded scalar's two lines are one mask:\n{out}"
+        );
+    }
+
+    /// Two generations show whether a literal secret changed, and nothing more.
+    #[test]
+    fn two_masks_differ_only_where_the_secret_does() {
+        let one =
+            "config_version: 1\npipelines:\n  p:\n    source:\n      password: first-secret\n";
+        let two = one.replace("first-secret", "second-secret");
+        let (a, b) = (
+            mask_secrets(one, &key()).unwrap(),
+            mask_secrets(&two, &key()).unwrap(),
+        );
+        assert_ne!(a, b);
+        assert_eq!(a, mask_secrets(one, &key()).unwrap());
+        let shown = |t: &str| t.lines().last().unwrap().to_owned();
+        assert_eq!(
+            shown(&a),
+            format!("      password: \"{}\"", masked(&key(), "first-secret"))
+        );
+        assert!(!b.contains("second-secret"));
+    }
+
+    /// Only the secret keys are masked: a `user` written as the password's text stays shown, and
+    /// the text is still shown (the E2E's sources write `user: avalanche`, `password: avalanche`).
+    #[test]
+    fn a_password_that_is_also_the_user_is_masked_only_as_the_password() {
+        let text = "config_version: 1\npipelines:\n  p:\n    source: { user: avalanche, password: avalanche }\n";
+        let out = mask_secrets(text, &key()).unwrap();
+        assert_eq!(
+            out,
+            format!(
+                "config_version: 1\npipelines:\n  p:\n    source: {{ user: avalanche, password: \"{}\" }}\n",
+                masked(&key(), "avalanche")
+            )
+        );
+    }
+
+    /// A text the masker can't place a secret in isn't shown.
+    #[test]
+    fn a_text_that_doesnt_parse_isnt_shown() {
+        assert_eq!(mask_secrets("globals: [", &key()), Err(MaskError::Parse));
+    }
+
+    /// The list follows the model: every key it names exists, and every key named like a
+    /// secret is in it (a file's path or a token's URL isn't one).
+    #[test]
+    fn the_secret_keys_are_the_models() {
+        let keys: Vec<String> = config_keys().into_iter().map(|k| k.path).collect();
+        for (k, _) in SECRET_KEYS {
+            assert!(keys.iter().any(|m| m == k), "{k} isn't a key");
+        }
+        let named: Vec<&String> = keys
+            .iter()
+            .filter(|k| {
+                let last = k.rsplit('.').next().unwrap();
+                matches!(
+                    last,
+                    "password"
+                        | "token"
+                        | "bearer_token"
+                        | "client_secret"
+                        | "session_key"
+                        | "ssl_key"
+                ) || k.ends_with("tls.key")
+                    || k.ends_with("headers.*")
+            })
+            .collect();
+        let listed: Vec<&str> = SECRET_KEYS.iter().map(|(k, _)| *k).collect();
+        assert_eq!(named.len(), listed.len(), "{named:?}");
+        assert!(
+            named.iter().all(|k| listed.contains(&k.as_str())),
+            "{named:?}"
+        );
+    }
+}
````

`crates/data-avalanche-config/src/walk.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/walk.rs b/crates/data-avalanche-config/src/walk.rs
index c6815d7..38051b6 100644
--- a/crates/data-avalanche-config/src/walk.rs
+++ b/crates/data-avalanche-config/src/walk.rs
@@ -21,12 +21,38 @@ use std::collections::BTreeMap;
 
 use crate::coerce::LeafType;
 use crate::ctx::{At, Ctx};
-use crate::leaf::{Leaf, Tri};
+use crate::leaf::{Leaf, Loc, Tri};
 use crate::raw::*;
 
+/// A leaf present in the file: its key path (a map's key or a list's `[i]` as one segment), its
+/// text as written, and where that is.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) struct Written {
+    pub path: Vec<String>,
+    pub text: String,
+    pub at: Option<Loc>,
+}
+
 pub(crate) trait Walk {
     fn drop_empty(&mut self, ctx: &Ctx);
     fn check_values(&self, ctx: &mut Ctx, at: &At);
+    /// Every leaf present, under `path` ([`Written`]).
+    fn leaves(&self, path: &[String], out: &mut Vec<Written>);
+}
+
+/// `path` and then `key`.
+pub(crate) fn under(path: &[String], key: &str) -> Vec<String> {
+    let mut p = path.to_vec();
+    p.push(key.to_owned());
+    p
+}
+
+fn written<T>(leaf: &Leaf<T>, path: Vec<String>, out: &mut Vec<Written>) {
+    out.push(Written {
+        path,
+        text: leaf.text().to_owned(),
+        at: leaf.written(),
+    });
 }
 
 /// Maps a raw struct's field identifier to its YAML key: every field's Rust name matches, except
@@ -76,6 +102,12 @@ macro_rules! walk_fields {
                     $f, ctx, &at.key($crate::walk::walk_key!($f)),
                 ); )+
             }
+            fn leaves(&self, path: &[String], out: &mut Vec<$crate::walk::Written>) {
+                let $t { $($f),+ } = self;
+                $( $crate::walk::Walk::leaves(
+                    $f, &$crate::walk::under(path, $crate::walk::walk_key!($f)), out,
+                ); )+
+            }
         }
         impl $crate::keys::Keys for $t {
             fn keys(&self, path: &str, out: &mut Vec<$crate::keys::Key>) {
@@ -101,6 +133,11 @@ macro_rules! walk_fields {
                     $crate::walk::Walk::check_values(v, ctx, at);
                 }
             }
+            fn leaves(&self, path: &[String], out: &mut Vec<$crate::walk::Written>) {
+                if let Some(v) = self {
+                    $crate::walk::Walk::leaves(v, path, out);
+                }
+            }
         }
     };
 }
@@ -124,6 +161,12 @@ impl<T: LeafType> Walk for Option<Leaf<T>> {
             ctx.value(l, at);
         }
     }
+
+    fn leaves(&self, path: &[String], out: &mut Vec<Written>) {
+        if let Some(l) = self {
+            written(l, path.to_vec(), out);
+        }
+    }
 }
 
 /// A tri-state leaf (§9.4.1): an explicit value whose resolved text is empty is absent (use the
@@ -144,6 +187,12 @@ impl<T: LeafType> Walk for Tri<T> {
             ctx.value(l, at);
         }
     }
+
+    fn leaves(&self, path: &[String], out: &mut Vec<Written>) {
+        if let Some(Some(l)) = self {
+            written(l, path.to_vec(), out);
+        }
+    }
 }
 
 /// A list of leaves (`join_members`, `bind_addr`): elements are left alone by `drop_empty`.
@@ -155,6 +204,12 @@ impl<T: LeafType> Walk for Option<Vec<Leaf<T>>> {
             ctx.value(l, &at.index(i));
         }
     }
+
+    fn leaves(&self, path: &[String], out: &mut Vec<Written>) {
+        for (i, l) in self.iter().flatten().enumerate() {
+            written(l, under(path, &format!("[{i}]")), out);
+        }
+    }
 }
 
 /// A tri-state list (`tables.include`/`exclude`, §9.4.1 applied to a list): absent or explicit
@@ -170,6 +225,14 @@ impl<T: LeafType> Walk for Option<Option<Vec<Leaf<T>>>> {
             }
         }
     }
+
+    fn leaves(&self, path: &[String], out: &mut Vec<Written>) {
+        if let Some(Some(items)) = self {
+            for (i, l) in items.iter().enumerate() {
+                written(l, under(path, &format!("[{i}]")), out);
+            }
+        }
+    }
 }
 
 /// A map of scalar leaves (`snapshots.overrides`, `column_type_overrides`, `table_properties`): a
@@ -186,6 +249,12 @@ impl<T: LeafType> Walk for Option<BTreeMap<String, Leaf<T>>> {
             ctx.value(l, &at.key(k.as_str()));
         }
     }
+
+    fn leaves(&self, path: &[String], out: &mut Vec<Written>) {
+        for (k, l) in self.iter().flatten() {
+            written(l, under(path, k), out);
+        }
+    }
 }
 
 /// A map of lists of leaves (`role_mapping`, `primary_key`): list elements are left alone, same as
@@ -201,6 +270,15 @@ impl<T: LeafType> Walk for Option<BTreeMap<String, Vec<Leaf<T>>>> {
             }
         }
     }
+
+    fn leaves(&self, path: &[String], out: &mut Vec<Written>) {
+        for (k, items) in self.iter().flatten() {
+            let path = under(path, k);
+            for (i, l) in items.iter().enumerate() {
+                written(l, under(&path, &format!("[{i}]")), out);
+            }
+        }
+    }
 }
 
 /// `tables.partitioning`: a map of lists of *structs*, not leaves — the only field of this shape,
@@ -225,6 +303,15 @@ impl Walk for Option<BTreeMap<String, Vec<RawPartitionField>>> {
             }
         }
     }
+
+    fn leaves(&self, path: &[String], out: &mut Vec<Written>) {
+        for (k, fields) in self.iter().flatten() {
+            let path = under(path, k);
+            for (i, f) in fields.iter().enumerate() {
+                f.leaves(&under(&path, &format!("[{i}]")), out);
+            }
+        }
+    }
 }
 
 // ---- raw structs `merge_struct!` doesn't cover ----
````

`crates/data-avalanche-state/src/generation.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/generation.rs b/crates/data-avalanche-state/src/generation.rs
index 622bc74..e1fcd54 100644
--- a/crates/data-avalanche-state/src/generation.rs
+++ b/crates/data-avalanche-state/src/generation.rs
@@ -8,6 +8,7 @@ use data_avalanche_storage::StoreError;
 use serde::{Deserialize, Serialize};
 
 use crate::doc::{CREATE_ATTEMPTS, DocStore, Document, StateError, Versioned};
+use crate::history::GenerationRecord;
 
 pub const GENERATION_KEY: &str = "config/generation.json";
 
@@ -59,6 +60,10 @@ impl ConfigGenerations {
         Self { docs }
     }
 
+    pub(crate) fn docs(&self) -> &DocStore {
+        &self.docs
+    }
+
     /// The current generation and its text, or `None` before the first bootstrap.
     ///
     /// # Errors
@@ -123,6 +128,18 @@ impl ConfigGenerations {
             attempted = Some(doc.clone());
             match self.docs.create(GENERATION_KEY, &doc).await {
                 Ok(version) => {
+                    let record = GenerationRecord {
+                        generation: 1,
+                        hash: doc.hash.clone(),
+                        node: node_id.to_owned(),
+                        at: now,
+                        by: None,
+                        note: None,
+                        summary: Some("bootstrap".to_owned()),
+                    };
+                    if let Err(e) = self.record(&record).await {
+                        tracing::warn!("config generation 1's record: {e}");
+                    }
                     let pointer = Versioned { doc, version };
                     return Ok((
                         ConfigGeneration {
````

`crates/data-avalanche-state/src/history.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/history.rs b/crates/data-avalanche-state/src/history.rs
new file mode 100644
index 0000000..a0d414f
--- /dev/null
+++ b/crates/data-avalanche-state/src/history.rs
@@ -0,0 +1,300 @@
+//! Who published each config generation, and why (Admin_Console §4 Config, D-M13b.9): one
+//! record per generation, `config/history/<generation:06>.json`, written once its pointer landed.
+//! A generation without one (bootstrapped before 1.1.0, published by a 1.0 node, or a crash
+//! between the two writes) is still listed, from its snapshot.
+
+use std::collections::BTreeMap;
+
+use chrono::{DateTime, Utc};
+use serde::{Deserialize, Serialize};
+
+use crate::doc::{Document, StateError};
+use crate::events::Actor;
+use crate::generation::{ConfigGenerations, GENERATION_KEY, GenerationDoc};
+
+pub const HISTORY_PREFIX: &str = "config/history";
+
+const SNAPSHOTS_PREFIX: &str = "config/snapshots";
+
+/// The record of generation `generation`.
+pub fn history_key(generation: u64) -> String {
+    format!("{HISTORY_PREFIX}/{generation:06}.json")
+}
+
+/// One generation's record.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct GenerationRecord {
+    pub generation: u64,
+    /// `blake3:<64 hex>` of its text, as the pointer names it.
+    pub hash: String,
+    /// The node whose file it is.
+    pub node: String,
+    pub at: DateTime<Utc>,
+    /// Who asked for it; none for a bootstrap.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub by: Option<Actor>,
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub note: Option<String>,
+    /// What it changed (`2 pipelines changed · 1 restart pending`), or `bootstrap`.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub summary: Option<String>,
+}
+
+impl Document for GenerationRecord {
+    const KIND: &'static str = "config history";
+    const SCHEMA_VERSION: u64 = 1;
+}
+
+/// A generation as the history lists it.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct Listed {
+    pub generation: u64,
+    /// Its text's hash: the record's, else the pointer's for the current one, else its only
+    /// snapshot's; `None` when it has several snapshots and nothing says which (a publish
+    /// race's loser was left behind).
+    pub hash: Option<String>,
+    pub record: Option<GenerationRecord>,
+    /// The generation the pointer names now.
+    pub current: bool,
+}
+
+impl ConfigGenerations {
+    /// Writes `record`; one already written (a retried publish) is kept.
+    ///
+    /// # Errors
+    /// A storage failure.
+    pub async fn record(&self, record: &GenerationRecord) -> Result<(), StateError> {
+        match self
+            .docs()
+            .create(&history_key(record.generation), record)
+            .await
+        {
+            Ok(_) => Ok(()),
+            Err(e) if e.is_race() => Ok(()),
+            Err(e) => Err(e),
+        }
+    }
+
+    /// Every generation's snapshot hashes, by generation, and the pointer.
+    async fn snapshots(
+        &self,
+    ) -> Result<(BTreeMap<u64, Vec<String>>, Option<GenerationDoc>), StateError> {
+        let mut by: BTreeMap<u64, Vec<String>> = BTreeMap::new();
+        for key in self.docs().store().list(SNAPSHOTS_PREFIX).await? {
+            let name = key.rsplit('/').next().unwrap_or_default();
+            let Some((n, hex)) = name
+                .strip_suffix(".yaml")
+                .and_then(|stem| stem.split_once('-'))
+            else {
+                continue;
+            };
+            if let Ok(n) = n.parse::<u64>() {
+                by.entry(n).or_default().push(format!("blake3:{hex}"));
+            }
+        }
+        let pointer = self
+            .docs()
+            .load::<GenerationDoc>(GENERATION_KEY)
+            .await?
+            .map(|v| v.doc);
+        Ok((by, pointer))
+    }
+
+    async fn listed(
+        &self,
+        generation: u64,
+        hashes: &[String],
+        pointer: Option<&GenerationDoc>,
+    ) -> Result<Listed, StateError> {
+        let record = self
+            .docs()
+            .load::<GenerationRecord>(&history_key(generation))
+            .await?
+            .map(|v| v.doc);
+        let current = pointer.is_some_and(|p| p.generation == generation);
+        let hash = match (&record, pointer.filter(|_| current), hashes) {
+            (Some(r), _, _) => Some(r.hash.clone()),
+            (None, Some(p), _) => Some(p.hash.clone()),
+            (None, None, [only]) => Some(only.clone()),
+            _ => None,
+        };
+        Ok(Listed {
+            generation,
+            hash,
+            record,
+            current,
+        })
+    }
+
+    /// The generations older than `before` (all when `None`), newest first, at most `limit`, each
+    /// with its record. A generation newer than the pointer (a publish that lost its race) isn't
+    /// one.
+    ///
+    /// # Errors
+    /// A storage or decode failure.
+    pub async fn history(
+        &self,
+        before: Option<u64>,
+        limit: usize,
+    ) -> Result<Vec<Listed>, StateError> {
+        let (by, pointer) = self.snapshots().await?;
+        let newest = pointer.as_ref().map_or(0, |p| p.generation);
+        let mut out = vec![];
+        for (n, hashes) in by.iter().rev() {
+            if *n > newest || before.is_some_and(|b| *n >= b) {
+                continue;
+            }
+            if out.len() == limit {
+                break;
+            }
+            out.push(self.listed(*n, hashes, pointer.as_ref()).await?);
+        }
+        Ok(out)
+    }
+
+    /// Generation `generation` with its text: `None` when there is no such generation; the text
+    /// `None` when its hash is ambiguous ([`Listed::hash`]).
+    ///
+    /// # Errors
+    /// A storage or decode failure; `Corrupt` when its snapshot doesn't match its hash.
+    pub async fn one(
+        &self,
+        generation: u64,
+    ) -> Result<Option<(Listed, Option<String>)>, StateError> {
+        let (by, pointer) = self.snapshots().await?;
+        let newest = pointer.as_ref().map_or(0, |p| p.generation);
+        let Some(hashes) = by.get(&generation).filter(|_| generation <= newest) else {
+            return Ok(None);
+        };
+        let listed = self.listed(generation, hashes, pointer.as_ref()).await?;
+        let text = match &listed.hash {
+            Some(h) => Some(self.text(generation, h).await?),
+            None => None,
+        };
+        Ok(Some((listed, text)))
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use std::sync::Arc;
+
+    use chrono::TimeZone;
+    use data_avalanche_storage::ObjectStoreVersioned;
+    use object_store::memory::InMemory;
+
+    use super::*;
+    use crate::DocStore;
+    use crate::generation::config_hash;
+
+    fn t(s: i64) -> DateTime<Utc> {
+        Utc.timestamp_opt(1_800_000_000 + s, 0).unwrap()
+    }
+
+    fn gens() -> ConfigGenerations {
+        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
+        ConfigGenerations::new(DocStore::new(Arc::new(store)))
+    }
+
+    fn record(n: u64, text: &str, note: &str) -> GenerationRecord {
+        GenerationRecord {
+            generation: n,
+            hash: config_hash(text),
+            node: "node-b".into(),
+            at: t(n as i64),
+            by: Some(Actor {
+                name: "oleksii".into(),
+                role: "operator".into(),
+            }),
+            note: Some(note.into()),
+            summary: Some("1 pipeline changed".into()),
+        }
+    }
+
+    #[tokio::test]
+    async fn the_history_lists_each_generation_newest_first_with_its_record() {
+        let g = gens();
+        let (first, _) = g
+            .bootstrap_or_adopt("v: 1\n", "node-a", t(0))
+            .await
+            .unwrap();
+        let second = g
+            .publish(&first.pointer, "v: 2\n", "node-b", t(2))
+            .await
+            .unwrap();
+        g.record(&record(2, "v: 2\n", "faster flush"))
+            .await
+            .unwrap();
+        // A retried publish's second record is kept as the first.
+        g.record(&record(2, "v: 2\n", "again")).await.unwrap();
+        let _third = g
+            .publish(&second.pointer, "v: 3\n", "node-a", t(3))
+            .await
+            .unwrap();
+
+        let all = g.history(None, 10).await.unwrap();
+        let ns: Vec<u64> = all.iter().map(|l| l.generation).collect();
+        assert_eq!(ns, [3, 2, 1]);
+        assert!(
+            all[0].current && all[0].record.is_none(),
+            "no record written for 3"
+        );
+        assert_eq!(all[0].hash.as_deref(), Some(config_hash("v: 3\n").as_str()));
+        assert_eq!(
+            all[1].record.as_ref().unwrap().note.as_deref(),
+            Some("faster flush")
+        );
+        let boot = all[2]
+            .record
+            .as_ref()
+            .expect("the bootstrap writes generation 1's");
+        assert_eq!(
+            (boot.by.as_ref(), boot.summary.as_deref()),
+            (None, Some("bootstrap"))
+        );
+
+        let page = g.history(Some(3), 1).await.unwrap();
+        assert_eq!(page.iter().map(|l| l.generation).collect::<Vec<_>>(), [2]);
+        let (two, text) = g.one(2).await.unwrap().unwrap();
+        assert_eq!((two.generation, text.as_deref()), (2, Some("v: 2\n")));
+        assert!(g.one(4).await.unwrap().is_none());
+    }
+
+    /// A publish that lost its race leaves its snapshot behind: the generation it lost is told by
+    /// the record, or else it is ambiguous; one beyond the pointer isn't listed.
+    #[tokio::test]
+    async fn a_race_s_leftover_snapshot_never_passes_for_a_generation() {
+        let g = gens();
+        let (first, _) = g
+            .bootstrap_or_adopt("v: 1\n", "node-a", t(0))
+            .await
+            .unwrap();
+        let won = g
+            .publish(&first.pointer, "v: 2\n", "node-a", t(2))
+            .await
+            .unwrap();
+        assert!(
+            g.publish(&first.pointer, "v: 2b\n", "node-b", t(2))
+                .await
+                .is_err()
+        );
+        let _ = won;
+        let (two, _) = g.one(2).await.unwrap().unwrap();
+        assert_eq!(
+            two.hash.as_deref(),
+            Some(config_hash("v: 2\n").as_str()),
+            "the pointer's"
+        );
+        // Once the cluster moved on, nothing tells the two apart but a record.
+        let _three = g
+            .publish(&won.pointer, "v: 3\n", "node-a", t(3))
+            .await
+            .unwrap();
+        let (two, text) = g.one(2).await.unwrap().unwrap();
+        assert_eq!((two.hash, text), (None, None));
+        g.record(&record(2, "v: 2\n", "")).await.unwrap();
+        let (_, text) = g.one(2).await.unwrap().unwrap();
+        assert_eq!(text.as_deref(), Some("v: 2\n"));
+    }
+}
````

`crates/data-avalanche-state/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index a1b1fd1..38e7b6a 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -37,6 +37,7 @@ mod anchor;
 mod doc;
 mod events;
 mod generation;
+mod history;
 mod lease;
 mod lifecycle;
 mod pipeline;
````

`docs/public/openapi.json`:

````diff
diff --git a/docs/public/openapi.json b/docs/public/openapi.json
index 9243a2b..9c6743c 100644
--- a/docs/public/openapi.json
+++ b/docs/public/openapi.json
@@ -185,6 +185,157 @@
         "x-min-role": "viewer"
       }
     },
+    "/v1/config/generations": {
+      "get": {
+        "tags": [
+          "config"
+        ],
+        "summary": "The config generations, newest first: who published each, why, and what it changed",
+        "parameters": [
+          {
+            "name": "before",
+            "in": "query",
+            "description": "generations older than this one: the `next` of the page before",
+            "required": false,
+            "schema": {
+              "type": "integer",
+              "minimum": 1
+            }
+          },
+          {
+            "name": "limit",
+            "in": "query",
+            "description": "at most this many (1–100, 20 by default)",
+            "required": false,
+            "schema": {
+              "type": "integer",
+              "default": 20,
+              "maximum": 100,
+              "minimum": 1
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the generations, and `next` when there are more",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "a parameter that isn't one, or isn't in its form",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
+    "/v1/config/generations/{generation}": {
+      "get": {
+        "tags": [
+          "config"
+        ],
+        "summary": "One config generation and its text, its secrets masked",
+        "parameters": [
+          {
+            "name": "generation",
+            "in": "path",
+            "description": "a config generation's number",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the generation and its text",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "a generation that isn't a number",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no such generation",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "several snapshots of it and no record of which one it is",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "500": {
+            "description": "its text couldn't be masked, so it isn't shown",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
     "/v1/config/reload": {
       "post": {
         "tags": [
````

`docs/src/content/docs/reference/api.md`:

````diff
diff --git a/docs/src/content/docs/reference/api.md b/docs/src/content/docs/reference/api.md
index a2d8016..6d80a39 100644
--- a/docs/src/content/docs/reference/api.md
+++ b/docs/src/content/docs/reference/api.md
@@ -190,6 +190,36 @@ Least role: `viewer`.
 
 The config generation the cluster runs: validate, publish, status.
 
+### `GET /v1/config/generations`
+
+The config generations, newest first: who published each, why, and what it changed
+
+Least role: `viewer`.
+
+- Query parameter `before`: generations older than this one: the `next` of the page before.
+- Query parameter `limit`: at most this many (1–100, 20 by default).
+
+| Status | When |
+|---|---|
+| `200` | the generations, and `next` when there are more |
+| `400` | a parameter that isn't one, or isn't in its form |
+
+### `GET /v1/config/generations/{generation}`
+
+One config generation and its text, its secrets masked
+
+Least role: `viewer`.
+
+- Path parameter `generation`: a config generation's number.
+
+| Status | When |
+|---|---|
+| `200` | the generation and its text |
+| `400` | a generation that isn't a number |
+| `404` | no such generation |
+| `409` | several snapshots of it and no record of which one it is |
+| `500` | its text couldn't be masked, so it isn't shown |
+
 ### `POST /v1/config/reload`
 
 Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-state -p data-avalanche-config -p data-avalanche-cluster -p data-avalanche-api`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/actions.rs crates/data-avalanche-api/src/config.rs crates/data-avalanche-api/src/generations.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/openapi.rs crates/data-avalanche-cluster/src/coordinator.rs crates/data-avalanche-cluster/src/lib.rs crates/data-avalanche-config/src/leaf.rs crates/data-avalanche-config/src/lib.rs crates/data-avalanche-config/src/mask.rs crates/data-avalanche-config/src/walk.rs crates/data-avalanche-state/src/generation.rs crates/data-avalanche-state/src/history.rs crates/data-avalanche-state/src/lib.rs docs/public/openapi.json docs/src/content/docs/reference/api.md
git commit -m "feat(api): who published each config generation and why; a generation's text, its secrets masked"
```

---

### Task 7: The diff and each node's file (`data-avalanche-config`, `-cluster`, `-api`)

**Files:**
- Modify: `crates/data-avalanche-api/openapi.json`, `crates/data-avalanche-api/src/generations.rs`, `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-api/src/nodes.rs`, `crates/data-avalanche-api/src/openapi.rs`, `crates/data-avalanche-cluster/src/coordinator.rs`, `crates/data-avalanche-config/src/diff.rs`, `crates/data-avalanche-config/src/lib.rs`, `crates/data-avalanche-config/src/mask.rs`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`

**Interfaces:**
- Consumes: Task 6's `Walk::leaves`, `SECRET_KEYS` masking, `ConfigGenerations::one`; Task 5's
  `Peers`, `STATUS_LIMIT`, `short`; `key_mutability`, `FIXED_FIELDS` (M8b).
- Produces: config `WrittenChange { path, class: &'static str, before: Option<String>, after:
  Option<String> }`, `WrittenDiff { added, removed, changes }`, `diff_written(&str, &str,
  &FingerprintKey) -> Result<WrittenDiff, MaskError>`; cluster `ConfigCoordinator::diff_written`;
  api `GET /v1/config/diff` and `GET /v1/config/nodes` (D-M13b.11, .12).

- [ ] **Step 1: Write the failing tests**

The tests: `a_rotated_literal_secret_shows_only_its_fingerprint` (config), `each_node_s_file_and_pending_fields_are_listed` (api), the diff's answers in `a_generation_is_listed_with_its_publisher_and_shown_masked`.

`crates/data-avalanche-api/src/generations.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/generations.rs b/crates/data-avalanche-api/src/generations.rs
index ee72154..1447faf 100644
--- a/crates/data-avalanche-api/src/generations.rs
+++ b/crates/data-avalanche-api/src/generations.rs
@@ -216,7 +292,24 @@ mod tests {
         );
         assert!(!text.contains("password: pw"), "{text}");
 
+        let (code, v) = json_of(addr, "GET", "/v1/config/diff?from=1&to=2").await;
+        assert_eq!(code, 200, "{v}");
+        assert_eq!(
+            v,
+            json!({
+                "from": 1, "to": 2,
+                "pipelines": { "added": [], "removed": [] },
+                "changes": [{
+                    "path": "globals.cluster.drain_timeout", "class": "in_place",
+                    "before": "45s", "after": "40s",
+                }],
+            })
+        );
+
         for (path, want) in [
+            ("/v1/config/diff?from=1&to=3", 404),
+            ("/v1/config/diff?from=1", 400),
+            ("/v1/config/diff?from=1&to=x", 400),
             ("/v1/config/generations/3", 404),
             ("/v1/config/generations/x", 400),
             ("/v1/config/generations?limit=0", 400),
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index e8b4e40..42c408b 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -764,6 +790,8 @@ mod tests {
                 ("POST", "/v1/config/validate", Operator),
                 ("POST", "/v1/config/reload", Operator),
                 ("GET", "/v1/config/status", Viewer),
+                ("GET", "/v1/config/diff", Viewer),
+                ("GET", "/v1/config/nodes", Viewer),
                 ("GET", "/v1/config/generations", Viewer),
                 ("GET", "/v1/config/generations/{generation}", Viewer),
                 ("GET", "/v1/cluster/status", Viewer),
````

`crates/data-avalanche-api/src/nodes.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/nodes.rs b/crates/data-avalanche-api/src/nodes.rs
index 1a6e8c6..6251474 100644
--- a/crates/data-avalanche-api/src/nodes.rs
+++ b/crates/data-avalanche-api/src/nodes.rs
@@ -407,6 +494,71 @@ mod tests {
         page.events.into_iter().map(|(_, e)| e).collect()
     }
 
+    /// Each member's generation, file and pending fields: node-b's from its own status, asked
+    /// with the caller's credentials; node-c gossips no address, and a file of its own.
+    #[tokio::test]
+    async fn each_node_s_file_and_pending_fields_are_listed() {
+        let log: Log = Arc::default();
+        let kept = Arc::clone(&log);
+        let app = Router::new().fallback(move |uri: Uri, headers: HeaderMap| {
+            let kept = Arc::clone(&kept);
+            async move {
+                kept.lock().unwrap().push(Seen {
+                    method: Method::GET,
+                    uri: uri.to_string(),
+                    authorization: headers
+                        .get("authorization")
+                        .map(|v| v.to_str().unwrap().to_owned()),
+                    target: None,
+                    forwarded_by: None,
+                    body: String::new(),
+                });
+                body(
+                    StatusCode::OK,
+                    &json!({ "restart_pending": ["globals.api.address"] }),
+                )
+            }
+        });
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let b = format!("http://{}", listener.local_addr().unwrap());
+        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
+        // node-a's view, with node-c beside node-b, served again.
+        let (_, lone, _dir) = served(&b, open()).await;
+        let mut c = member("", Phase::Active);
+        c.meta.file_hash = file_hash("another file");
+        let mut view = lone.node.members.borrow().clone();
+        view.nodes.insert("node-c".into(), c);
+        let (tx, rx) = watch::channel(view);
+        let mut node = lone.node.clone();
+        node.members = rx;
+        let a = api(open());
+        a.node.set(node).unwrap();
+        let addr = serve(a).await;
+        std::mem::forget(tx);
+
+        let auth = [("Authorization", "Bearer the-callers-token")];
+        let (code, _, text) = request(addr, "GET", "/v1/config/nodes", &auth, "").await;
+        assert_eq!(code, 200, "{text}");
+        let v = json(&text);
+        assert_eq!(v["generation"], 1);
+        assert_eq!(
+            v["nodes"],
+            json!([
+                { "node": "node-a", "generation": 1, "file": "same", "restart_pending": [] },
+                { "node": "node-b", "generation": 1, "file": "same", "restart_pending": ["globals.api.address"] },
+                {
+                    "node": "node-c", "generation": 1, "file": "differs", "restart_pending": null,
+                    "error": "node-c gossips no API address",
+                },
+            ])
+        );
+        let seen = log.lock().unwrap()[0].clone();
+        assert_eq!(
+            (seen.uri.as_str(), seen.authorization.as_deref()),
+            ("/v1/config/status", Some("Bearer the-callers-token"))
+        );
+    }
+
     #[tokio::test]
     async fn this_node_s_detail_is_answered_here() {
         let (b, log) = fake(Duration::ZERO).await;
````

`crates/data-avalanche-config/src/mask.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/mask.rs b/crates/data-avalanche-config/src/mask.rs
index 31f600b..23e0563 100644
--- a/crates/data-avalanche-config/src/mask.rs
+++ b/crates/data-avalanche-config/src/mask.rs
@@ -423,6 +423,75 @@ pipelines:
         assert!(!b.contains("second-secret"));
     }
 
+    /// Two generations' differences, by path and class, secrets masked; a pipeline added is
+    /// named, not its every key.
+    #[test]
+    fn a_rotated_literal_secret_shows_only_its_fingerprint() {
+        let from = "\
+config_version: 1
+globals:
+  cluster: { tokens: 128, drain_timeout: 45s }
+pipelines:
+  a:
+    id: orders
+    source: { port: 5432, password: first-secret }
+    tables: { include: [public.orders] }
+";
+        let to = "\
+config_version: 1
+globals:
+  cluster: { tokens: 256, drain_timeout: 40s }
+pipelines:
+  a:
+    id: orders
+    source: { port: 5432, password: second-secret }
+    tables: { include: [public.orders, public.lines] }
+  b:
+    id: users
+    source: { port: 5433 }
+";
+        let d = diff_written(from, to, &key()).unwrap();
+        assert_eq!((d.added, d.removed), (vec!["users".to_owned()], vec![]));
+        let got: Vec<(&str, &str, Option<&str>, Option<&str>)> = d
+            .changes
+            .iter()
+            .map(|c| {
+                (
+                    c.path.as_str(),
+                    c.class,
+                    c.before.as_deref(),
+                    c.after.as_deref(),
+                )
+            })
+            .collect();
+        let first = masked(&key(), "first-secret");
+        let second = masked(&key(), "second-secret");
+        assert_eq!(
+            got,
+            [
+                (
+                    "globals.cluster.drain_timeout",
+                    "in_place",
+                    Some("45s"),
+                    Some("40s")
+                ),
+                ("globals.cluster.tokens", "fixed", Some("128"), Some("256")),
+                (
+                    "pipelines.orders.source.password",
+                    "pipeline_restart",
+                    Some(first.as_str()),
+                    Some(second.as_str())
+                ),
+                (
+                    "pipelines.orders.tables.include[1]",
+                    "tables",
+                    None,
+                    Some("public.lines")
+                ),
+            ]
+        );
+    }
+
     /// Only the secret keys are masked: a `user` written as the password's text stays shown, and
     /// the text is still shown (the E2E's sources write `user: avalanche`, `password: avalanche`).
     #[test]
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-config -p data-avalanche-api`
Expected: the build fails: no `diff_written`, `generations::diff` or `nodes::files`.

- [ ] **Step 3: Implement**

`openapi.json` and the site's copies are regenerated as in Task 5.

`crates/data-avalanche-api/openapi.json`:

````diff
diff --git a/crates/data-avalanche-api/openapi.json b/crates/data-avalanche-api/openapi.json
index 9c6743c..5891d44 100644
--- a/crates/data-avalanche-api/openapi.json
+++ b/crates/data-avalanche-api/openapi.json
@@ -185,6 +185,95 @@
         "x-min-role": "viewer"
       }
     },
+    "/v1/config/diff": {
+      "get": {
+        "tags": [
+          "config"
+        ],
+        "summary": "What differs between two config generations as written: each change's path, what a reload of it does, before and after, secrets masked",
+        "parameters": [
+          {
+            "name": "from",
+            "in": "query",
+            "description": "the generation compared from",
+            "required": true,
+            "schema": {
+              "type": "integer",
+              "minimum": 1
+            }
+          },
+          {
+            "name": "to",
+            "in": "query",
+            "description": "the generation compared to",
+            "required": true,
+            "schema": {
+              "type": "integer",
+              "minimum": 1
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the pipelines added and removed, and the changes",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "from or to missing, or not a number",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no such generation",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "several snapshots of one and no record of which it is",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "500": {
+            "description": "a text couldn't be compared",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
     "/v1/config/generations": {
       "get": {
         "tags": [
@@ -336,6 +425,49 @@
         "x-min-role": "viewer"
       }
     },
+    "/v1/config/nodes": {
+      "get": {
+        "tags": [
+          "config"
+        ],
+        "summary": "Each member's config generation, whether its own file is the cluster's generation, and its fields waiting for a restart",
+        "responses": {
+          "200": {
+            "description": "the generation the cluster runs, and each member",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
     "/v1/config/reload": {
       "post": {
         "tags": [
````

`crates/data-avalanche-api/src/generations.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/generations.rs b/crates/data-avalanche-api/src/generations.rs
index ee72154..1447faf 100644
--- a/crates/data-avalanche-api/src/generations.rs
+++ b/crates/data-avalanche-api/src/generations.rs
@@ -105,6 +105,88 @@ pub(crate) async fn list(State(api): State<Api>, RawQuery(raw): RawQuery) -> Res
     }
 }
 
+/// A generation's text, or the answer that says why there is none.
+async fn text_of(node: &crate::Node, n: u64) -> Result<String, Box<Response>> {
+    match node.coordinator.generations().one(n).await {
+        Ok(None) => Err(Box::new(
+            Failure::new(StatusCode::NOT_FOUND, format!("no config generation {n}")).into(),
+        )),
+        Ok(Some((_, None))) => Err(Box::new(ambiguous(n))),
+        Ok(Some((_, Some(text)))) => Ok(text),
+        Err(e) => Err(Box::new(unreadable(&e))),
+    }
+}
+
+fn ambiguous(n: u64) -> Response {
+    Failure::new(
+        StatusCode::CONFLICT,
+        format!(
+            "config generation {n} has several snapshots and no record of which one it is: a publish that lost its race left one behind"
+        ),
+    )
+    .into()
+}
+
+/// `GET /v1/config/diff?from=&to=` (D-M13b.11): what differs between two generations as written.
+pub(crate) async fn diff(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (mut from, mut to) = (None, None);
+    for (name, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
+        let slot = match &*name {
+            "from" => &mut from,
+            "to" => &mut to,
+            other => {
+                return Failure::new(
+                    StatusCode::BAD_REQUEST,
+                    format!("unknown parameter {other:?}: the diff takes from, to"),
+                )
+                .into();
+            }
+        };
+        let Ok(n) = value.trim().parse::<u64>() else {
+            return Failure::new(
+                StatusCode::BAD_REQUEST,
+                format!("{name} is a generation's number"),
+            )
+            .into();
+        };
+        *slot = Some(n);
+    }
+    let (Some(from), Some(to)) = (from, to) else {
+        return Failure::new(
+            StatusCode::BAD_REQUEST,
+            "the diff takes from and to, both generations",
+        )
+        .into();
+    };
+    let before = match text_of(node, from).await {
+        Ok(t) => t,
+        Err(r) => return *r,
+    };
+    let after = match text_of(node, to).await {
+        Ok(t) => t,
+        Err(r) => return *r,
+    };
+    match node.coordinator.diff_written(&before, &after) {
+        Ok(d) => body(
+            StatusCode::OK,
+            &json!({
+                "from": from,
+                "to": to,
+                "pipelines": { "added": d.added, "removed": d.removed },
+                "changes": d.changes,
+            }),
+        ),
+        Err(e) => Failure::new(
+            StatusCode::INTERNAL_SERVER_ERROR,
+            format!("the generations' texts can't be compared: {e}"),
+        )
+        .into(),
+    }
+}
+
 /// One generation and its text, secrets masked.
 pub(crate) async fn one(State(api): State<Api>, Path(generation): Path<String>) -> Response {
     let Some(node) = api.node.get() else {
@@ -115,13 +197,7 @@ pub(crate) async fn one(State(api): State<Api>, Path(generation): Path<String>)
     };
     match node.coordinator.generations().one(n).await {
         Ok(None) => Failure::new(StatusCode::NOT_FOUND, format!("no config generation {n}")).into(),
-        Ok(Some((_, None))) => Failure::new(
-            StatusCode::CONFLICT,
-            format!(
-                "config generation {n} has several snapshots and no record of which one it is: a publish that lost its race left one behind"
-            ),
-        )
-        .into(),
+        Ok(Some((_, None))) => ambiguous(n),
         Ok(Some((listed, Some(text)))) => match node.coordinator.mask(&text) {
             Ok(masked) => {
                 let mut v = listed_json(&listed);
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index e8b4e40..42c408b 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -224,6 +224,32 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             get(config::status),
         ),
+        (
+            r(
+                "GET",
+                "/v1/config/diff",
+                Viewer,
+                "What differs between two config generations as written: each change's path, what a reload of it does, before and after, secrets masked",
+                &[
+                    (200, "the pipelines added and removed, and the changes"),
+                    (400, "from or to missing, or not a number"),
+                    (404, "no such generation"),
+                    (409, "several snapshots of one and no record of which it is"),
+                    (500, "a text couldn't be compared"),
+                ],
+            ),
+            get(generations::diff),
+        ),
+        (
+            r(
+                "GET",
+                "/v1/config/nodes",
+                Viewer,
+                "Each member's config generation, whether its own file is the cluster's generation, and its fields waiting for a restart",
+                &[(200, "the generation the cluster runs, and each member")],
+            ),
+            get(nodes::files),
+        ),
         (
             r(
                 "GET",
````

`crates/data-avalanche-api/src/nodes.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/nodes.rs b/crates/data-avalanche-api/src/nodes.rs
index 1a6e8c6..6251474 100644
--- a/crates/data-avalanche-api/src/nodes.rs
+++ b/crates/data-avalanche-api/src/nodes.rs
@@ -11,7 +11,7 @@ use chrono::{DateTime, Utc};
 use data_avalanche_cluster::{Phase, Ring};
 use serde_json::{Value, json};
 
-use crate::peers::{ACTION_LIMIT, Asked, DETAIL_LIMIT, FORWARDED_BY, STATUS_LIMIT};
+use crate::peers::{ACTION_LIMIT, Asked, DETAIL_LIMIT, FORWARDED_BY, MAX_ANSWER, STATUS_LIMIT};
 use crate::{Api, Node, body, starting};
 
 /// The first 16 hex digits of a `blake3:<hex>` hash, as a node gossips its file's.
@@ -290,6 +290,93 @@ pub(crate) async fn config_status(
     .await
 }
 
+/// `GET /v1/config/nodes` (D-M13b.12): the generation the cluster runs, and for each member the
+/// one it runs, whether its own file is that generation's text (`same`, `differs`, or `unknown`
+/// when it gossips no hash), and its fields waiting for a restart, from its own
+/// `/v1/config/status` asked with the caller's credentials (`null`, and `error`, when it doesn't
+/// answer within the limit).
+pub(crate) async fn files(State(api): State<Api>, headers: HeaderMap) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let current = match node.coordinator.generations().current().await {
+        Ok(Some(c)) => c.pointer.doc,
+        Ok(None) => node.coordinator.generation().await,
+        Err(e) => {
+            return body(
+                StatusCode::SERVICE_UNAVAILABLE,
+                &json!({ "error": format!("the config generation can't be read: {e}") }),
+            );
+        }
+    };
+    let members = node.members.borrow().clone();
+    let mut asks = tokio::task::JoinSet::new();
+    for (name, m) in &members.nodes {
+        if *name == members.me || m.meta.api_url.is_empty() {
+            continue;
+        }
+        let (peers, name, url) = (node.peers.clone(), name.clone(), m.meta.api_url.clone());
+        let asked = Asked::from(Method::GET, &headers, Bytes::new());
+        asks.spawn(async move {
+            let answer = peers
+                .send(&name, &url, "/v1/config/status", asked, STATUS_LIMIT)
+                .await;
+            let code = answer.status();
+            let v: Value = axum::body::to_bytes(answer.into_body(), MAX_ANSWER)
+                .await
+                .ok()
+                .and_then(|b| serde_json::from_slice(&b).ok())
+                .unwrap_or(Value::Null);
+            let pending = if code == StatusCode::OK {
+                Ok(v["restart_pending"].clone())
+            } else {
+                Err(v["error"]
+                    .as_str()
+                    .map_or_else(|| format!("{name} answered {code}"), str::to_owned))
+            };
+            (name, pending)
+        });
+    }
+    let mut pending: std::collections::BTreeMap<String, Result<Value, String>> =
+        asks.join_all().await.into_iter().collect();
+    pending.insert(
+        members.me.clone(),
+        Ok(json!(node.coordinator.restart_pending().await)),
+    );
+    let nodes: Vec<Value> = members
+        .nodes
+        .iter()
+        .map(|(name, m)| {
+            let file = match m.meta.file_hash.as_str() {
+                "" => "unknown",
+                h if h == short(&current.hash) => "same",
+                _ => "differs",
+            };
+            let mut v = json!({
+                "node": name,
+                "generation": m.meta.config_generation,
+                "file": file,
+            });
+            match pending.remove(name) {
+                Some(Ok(p)) => v["restart_pending"] = p,
+                Some(Err(e)) => {
+                    v["restart_pending"] = Value::Null;
+                    v["error"] = json!(e);
+                }
+                None => {
+                    v["restart_pending"] = Value::Null;
+                    v["error"] = json!(format!("{name} gossips no API address"));
+                }
+            }
+            v
+        })
+        .collect();
+    body(
+        StatusCode::OK,
+        &json!({ "generation": current.generation, "hash": current.hash, "nodes": nodes }),
+    )
+}
+
 #[cfg(test)]
 mod tests {
     use std::net::SocketAddr;
````

`crates/data-avalanche-api/src/openapi.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index df3b7de..ab5486b 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -59,6 +59,25 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
             );
         }
     }
+    if r.path == "/v1/config/diff" {
+        for (name, description) in [
+            ("from", "the generation compared from"),
+            ("to", "the generation compared to"),
+        ] {
+            op = op.parameter(
+                ParameterBuilder::new()
+                    .name(name)
+                    .parameter_in(ParameterIn::Query)
+                    .required(Required::True)
+                    .description(Some(description))
+                    .schema(Some(
+                        ObjectBuilder::new()
+                            .schema_type(Type::Integer)
+                            .minimum(Some(1)),
+                    )),
+            );
+        }
+    }
     if r.path == "/v1/config/generations" {
         for (name, description) in crate::generations::PARAMETERS {
             let schema = if *name == "limit" {
````

`crates/data-avalanche-cluster/src/coordinator.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 6e3fffd..9276c8c 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -12,8 +12,8 @@ use std::time::Duration;
 use chrono::Utc;
 use data_avalanche_config::{
     ConfigDiff, Env, FingerprintKey, Fingerprints, Globals, MaskError, Mutability, Pipeline,
-    PipelineChange, Report, ResolvedConfig, diff, diff_with, load_posted_str, load_str,
-    mask_secrets,
+    PipelineChange, Report, ResolvedConfig, WrittenDiff, diff, diff_with, diff_written,
+    load_posted_str, load_str, mask_secrets,
 };
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{
@@ -236,6 +236,15 @@ impl ConfigCoordinator {
         mask_secrets(text, &self.inner.deps.key)
     }
 
+    /// What differs between two generations' texts as written, secrets masked under the
+    /// cluster's fingerprint key ([`diff_written`]).
+    ///
+    /// # Errors
+    /// [`MaskError::Parse`]: a text isn't a config.
+    pub fn diff_written(&self, from: &str, to: &str) -> Result<WrittenDiff, MaskError> {
+        diff_written(from, to, &self.inner.deps.key)
+    }
+
     /// The config this node runs: its generation's text as resolved here.
     pub async fn config(&self) -> ResolvedConfig {
         self.inner.running.lock().await.config.clone()
````

`crates/data-avalanche-config/src/diff.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/diff.rs b/crates/data-avalanche-config/src/diff.rs
index d952bc7..d97e57b 100644
--- a/crates/data-avalanche-config/src/diff.rs
+++ b/crates/data-avalanche-config/src/diff.rs
@@ -86,7 +86,7 @@ impl ConfigDiff {
 }
 
 /// `path` (dotted) is one of [`FIXED_FIELDS`], or inside one.
-fn is_fixed(path: &str) -> bool {
+pub(crate) fn is_fixed(path: &str) -> bool {
     FIXED_FIELDS.iter().any(|fixed| {
         path.strip_prefix(fixed)
             .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
````

`crates/data-avalanche-config/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index fff7c1a..98c140c 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -105,7 +105,7 @@ pub use field_docs::{DocDefault, FIELD_DOCS, FieldDoc};
 pub use keys::{Key, KeyShape, config_keys, key_mutability};
 pub use leaf::Loc;
 pub use load::{Loaded, load_file, load_posted_str, load_str, validate_str};
-pub use mask::{MASK, MaskError, mask_secrets};
+pub use mask::{MASK, MaskError, WrittenChange, WrittenDiff, diff_written, mask_secrets};
 pub use material::{FileRef, Origin, Pem};
 pub use mutability::Mutability;
 pub use registry::{RULES, RuleInput, RuleLocus, RuleMeta, RuleSeverity, rule_meta};
````

`crates/data-avalanche-config/src/mask.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/mask.rs b/crates/data-avalanche-config/src/mask.rs
index 23e0563..3b59046 100644
--- a/crates/data-avalanche-config/src/mask.rs
+++ b/crates/data-avalanche-config/src/mask.rs
@@ -13,9 +13,15 @@
 //! directive isn't shown ([`MaskError::MergeKey`]): a key that overrides or retags a value would
 //! hide the secret the masker reads.
 
+use std::collections::{BTreeMap, BTreeSet};
+
+use serde::Serialize;
 use thiserror::Error;
 
+use crate::diff::is_fixed;
+use crate::keys::key_mutability;
 use crate::leaf::Loc;
+use crate::mutability::Mutability;
 use crate::parse::parse_document;
 use crate::raw::RawConfig;
 use crate::secret::{FingerprintKey, fingerprint};
@@ -98,6 +104,122 @@ pub fn mask_secrets(text: &str, key: &FingerprintKey) -> Result<String, MaskErro
     Ok(out)
 }
 
+/// One value that differs between two config texts, as written, its secrets masked.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+pub struct WrittenChange {
+    /// Dotted, a pipeline by its `id` (`pipelines.orders.source.port`); a list's element as
+    /// `[i]` (`tables.include[0]`).
+    pub path: String,
+    /// What a reload of the change does: `in_place`, `tables` (a pipeline restart when the
+    /// resolved table set changes), `pipeline_restart`, `node_restart`, or `fixed` (refused).
+    pub class: &'static str,
+    /// `None` where the value isn't written.
+    pub before: Option<String>,
+    pub after: Option<String>,
+}
+
+/// What differs between two config texts as written ([`diff_written`]).
+#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
+pub struct WrittenDiff {
+    /// Pipelines by `id`.
+    pub added: Vec<String>,
+    pub removed: Vec<String>,
+    /// Outside the pipelines added and removed.
+    pub changes: Vec<WrittenChange>,
+}
+
+/// What differs between `from` and `to`, both parsed without the environment and compared value
+/// by value as written (Admin_Console §4 Config, D-M13b.11), secrets shown as [`mask_secrets`]
+/// shows them. A `${VAR}` whose value changed isn't a change here: the text is the same.
+///
+/// # Errors
+/// [`MaskError::Parse`]: one of them isn't a config.
+pub fn diff_written(from: &str, to: &str, key: &FingerprintKey) -> Result<WrittenDiff, MaskError> {
+    let (before, before_ids) = flatten(from, key)?;
+    let (after, after_ids) = flatten(to, key)?;
+    let pipeline = |path: &str| {
+        path.strip_prefix("pipelines.")
+            .and_then(|rest| rest.split('.').next())
+            .map(str::to_owned)
+    };
+    let added: Vec<String> = after_ids.difference(&before_ids).cloned().collect();
+    let removed: Vec<String> = before_ids.difference(&after_ids).cloned().collect();
+    let paths: BTreeSet<&String> = before.keys().chain(after.keys()).collect();
+    let changes = paths
+        .into_iter()
+        .filter(|p| pipeline(p).is_none_or(|id| !added.contains(&id) && !removed.contains(&id)))
+        .filter(|p| before.get(*p) != after.get(*p))
+        .map(|p| WrittenChange {
+            path: p.clone(),
+            class: class_of(p),
+            before: before.get(p).cloned(),
+            after: after.get(p).cloned(),
+        })
+        .collect();
+    Ok(WrittenDiff {
+        added,
+        removed,
+        changes,
+    })
+}
+
+/// A written path's reload class.
+fn class_of(path: &str) -> &'static str {
+    let mut segments: Vec<&str> = path
+        .split('.')
+        .map(|s| s.split('[').next().unwrap_or(s))
+        .collect();
+    if segments.first() == Some(&"defaults") {
+        segments.splice(0..1, ["pipelines", "*"]);
+    }
+    let key = segments.join(".");
+    if is_fixed(&key) {
+        return "fixed";
+    }
+    match key_mutability(&key) {
+        Some(Mutability::R) => "in_place",
+        Some(Mutability::PrIfTableSetChanges) => "tables",
+        Some(Mutability::Pr) => "pipeline_restart",
+        Some(Mutability::Proc) | None => "node_restart",
+    }
+}
+
+/// Every leaf of `text` by its dotted path (a pipeline by its `id`), its secrets masked, and
+/// the pipelines' ids.
+fn flatten(
+    text: &str,
+    key: &FingerprintKey,
+) -> Result<(BTreeMap<String, String>, BTreeSet<String>), MaskError> {
+    let raw: RawConfig = parse_document(text).map_err(|_| MaskError::Parse)?;
+    let mut all = vec![];
+    raw.globals.leaves(&["globals".to_owned()], &mut all);
+    raw.defaults.leaves(&["defaults".to_owned()], &mut all);
+    let mut ids = BTreeSet::new();
+    for (name, p) in raw.pipelines.iter().flatten() {
+        let id =
+            p.id.as_ref()
+                .map_or_else(|| name.clone(), |l| l.text().to_owned());
+        Some(p.clone()).leaves(&under(&["pipelines".to_owned()], &id), &mut all);
+        ids.insert(id);
+    }
+    let mut out = BTreeMap::new();
+    for w in all {
+        let shown = match literal(&w.text) {
+            Some(value) if is_secret(&w) => masked(key, &value),
+            _ => w.text.clone(),
+        };
+        let mut path = String::new();
+        for s in &w.path {
+            if !s.starts_with('[') && !path.is_empty() {
+                path.push('.');
+            }
+            path.push_str(s);
+        }
+        out.insert(path, shown);
+    }
+    Ok((out, ids))
+}
+
 /// A literal secret's mask.
 fn masked(key: &FingerprintKey, value: &str) -> String {
     let fp = fingerprint(key, value.as_bytes());
````

`docs/public/openapi.json`:

````diff
diff --git a/docs/public/openapi.json b/docs/public/openapi.json
index 9c6743c..5891d44 100644
--- a/docs/public/openapi.json
+++ b/docs/public/openapi.json
@@ -185,6 +185,95 @@
         "x-min-role": "viewer"
       }
     },
+    "/v1/config/diff": {
+      "get": {
+        "tags": [
+          "config"
+        ],
+        "summary": "What differs between two config generations as written: each change's path, what a reload of it does, before and after, secrets masked",
+        "parameters": [
+          {
+            "name": "from",
+            "in": "query",
+            "description": "the generation compared from",
+            "required": true,
+            "schema": {
+              "type": "integer",
+              "minimum": 1
+            }
+          },
+          {
+            "name": "to",
+            "in": "query",
+            "description": "the generation compared to",
+            "required": true,
+            "schema": {
+              "type": "integer",
+              "minimum": 1
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the pipelines added and removed, and the changes",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "from or to missing, or not a number",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "404": {
+            "description": "no such generation",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "several snapshots of one and no record of which it is",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "500": {
+            "description": "a text couldn't be compared",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
     "/v1/config/generations": {
       "get": {
         "tags": [
@@ -336,6 +425,49 @@
         "x-min-role": "viewer"
       }
     },
+    "/v1/config/nodes": {
+      "get": {
+        "tags": [
+          "config"
+        ],
+        "summary": "Each member's config generation, whether its own file is the cluster's generation, and its fields waiting for a restart",
+        "responses": {
+          "200": {
+            "description": "the generation the cluster runs, and each member",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
     "/v1/config/reload": {
       "post": {
         "tags": [
````

`docs/src/content/docs/reference/api.md`:

````diff
diff --git a/docs/src/content/docs/reference/api.md b/docs/src/content/docs/reference/api.md
index 6d80a39..402ee67 100644
--- a/docs/src/content/docs/reference/api.md
+++ b/docs/src/content/docs/reference/api.md
@@ -190,6 +190,23 @@ Least role: `viewer`.
 
 The config generation the cluster runs: validate, publish, status.
 
+### `GET /v1/config/diff`
+
+What differs between two config generations as written: each change's path, what a reload of it does, before and after, secrets masked
+
+Least role: `viewer`.
+
+- Query parameter `from`: the generation compared from.
+- Query parameter `to`: the generation compared to.
+
+| Status | When |
+|---|---|
+| `200` | the pipelines added and removed, and the changes |
+| `400` | from or to missing, or not a number |
+| `404` | no such generation |
+| `409` | several snapshots of one and no record of which it is |
+| `500` | a text couldn't be compared |
+
 ### `GET /v1/config/generations`
 
 The config generations, newest first: who published each, why, and what it changed
@@ -220,6 +237,17 @@ Least role: `viewer`.
 | `409` | several snapshots of it and no record of which one it is |
 | `500` | its text couldn't be masked, so it isn't shown |
 
+### `GET /v1/config/nodes`
+
+Each member's config generation, whether its own file is the cluster's generation, and its fields waiting for a restart
+
+Least role: `viewer`.
+
+
+| Status | When |
+|---|---|
+| `200` | the generation the cluster runs, and each member |
+
 ### `POST /v1/config/reload`
 
 Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-config -p data-avalanche-api`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/generations.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/nodes.rs crates/data-avalanche-api/src/openapi.rs crates/data-avalanche-cluster/src/coordinator.rs crates/data-avalanche-config/src/diff.rs crates/data-avalanche-config/src/lib.rs crates/data-avalanche-config/src/mask.rs docs/public/openapi.json docs/src/content/docs/reference/api.md
git commit -m "feat(api): the diff between two generations, and each node's file against the cluster's"
```

---

### Task 8: The site and the chart

**Files:**
- Modify: `crates/data-avalanche/tests/packaging.rs`, `deploy/helm/data-avalanche/ci/tls-values.yaml`, `deploy/helm/data-avalanche/templates/statefulset.yaml`, `deploy/helm/data-avalanche/values.yaml`, `docs/src/content/docs/deployment/kubernetes.md`, `docs/src/content/docs/deployment/observability.md`, `docs/src/content/docs/runbooks/upgrade.md`

**Interfaces:**
- Consumes: everything above; the chart's StatefulSet and values (M10).
- Produces: `DATA_AVALANCHE_POD_DNS_NAME` in every pod's environment
  (`$(POD_NAME).<headless>.$(POD_NAMESPACE).svc.<clusterDomain>`), documented in `values.yaml`;
  the TLS example sets `advertise_url` with it; the upgrade page's 1.0 → 1.1 section (stop every
  node); the Kubernetes page's advertised URL; the observability page's process and gossip rows.

- [ ] **Step 1: Write the failing tests**

The tests: `the_examples_configs_validate_with_the_pod_s_environment` with the new variable (`tests/packaging.rs`).

`crates/data-avalanche/tests/packaging.rs`:

````diff
diff --git a/crates/data-avalanche/tests/packaging.rs b/crates/data-avalanche/tests/packaging.rs
index 31fac09..fb6e52f 100644
--- a/crates/data-avalanche/tests/packaging.rs
+++ b/crates/data-avalanche/tests/packaging.rs
@@ -102,6 +102,10 @@ fn pod_env() -> BTreeMap<String, String> {
             "DATA_AVALANCHE_DISCOVERY_DNS_NAME",
             "data-avalanche-headless.cdc.svc.cluster.local".to_owned(),
         ),
+        (
+            "DATA_AVALANCHE_POD_DNS_NAME",
+            "data-avalanche-0.data-avalanche-headless.cdc.svc.cluster.local".to_owned(),
+        ),
         ("ORDERS_DB_PASSWORD", "secret".to_owned()),
         ("API_TLS_CERT", tls.cert.pem()),
         ("API_TLS_KEY", tls.signing_key.serialize_pem()),
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche --test packaging`
Expected: `ci/tls-values.yaml`'s config doesn't resolve: `DATA_AVALANCHE_POD_DNS_NAME` isn't set (V-063).

- [ ] **Step 3: Implement**

`deploy/helm/data-avalanche/ci/tls-values.yaml`:

````diff
diff --git a/deploy/helm/data-avalanche/ci/tls-values.yaml b/deploy/helm/data-avalanche/ci/tls-values.yaml
index 2204db5..9d49b23 100644
--- a/deploy/helm/data-avalanche/ci/tls-values.yaml
+++ b/deploy/helm/data-avalanche/ci/tls-values.yaml
@@ -7,6 +7,7 @@ config:
     state: { bucket: example-cdc-state, prefix: cdc }
     api:
       address: "0.0.0.0:8443"
+      advertise_url: "https://${DATA_AVALANCHE_POD_DNS_NAME}:8443"
       tls: { enabled: true, cert: "${API_TLS_CERT}", key: "${API_TLS_KEY}" }
     cluster:
       discovery: dns
````

`deploy/helm/data-avalanche/templates/statefulset.yaml`:

````diff
diff --git a/deploy/helm/data-avalanche/templates/statefulset.yaml b/deploy/helm/data-avalanche/templates/statefulset.yaml
index c00b75c..772804a 100644
--- a/deploy/helm/data-avalanche/templates/statefulset.yaml
+++ b/deploy/helm/data-avalanche/templates/statefulset.yaml
@@ -60,6 +60,8 @@ spec:
                   fieldPath: status.podIP
             - name: DATA_AVALANCHE_DISCOVERY_DNS_NAME
               value: "{{ include "data-avalanche.headless" . }}.$(POD_NAMESPACE).svc.{{ .Values.clusterDomain }}"
+            - name: DATA_AVALANCHE_POD_DNS_NAME
+              value: "$(POD_NAME).{{ include "data-avalanche.headless" . }}.$(POD_NAMESPACE).svc.{{ .Values.clusterDomain }}"
             {{- with .Values.env }}
             {{- toYaml . | nindent 12 }}
             {{- end }}
````

`deploy/helm/data-avalanche/values.yaml`:

````diff
diff --git a/deploy/helm/data-avalanche/values.yaml b/deploy/helm/data-avalanche/values.yaml
index 24926f4..b8e5355 100644
--- a/deploy/helm/data-avalanche/values.yaml
+++ b/deploy/helm/data-avalanche/values.yaml
@@ -4,6 +4,7 @@
 # sets on every pod:
 #   POD_NAME, POD_IP, POD_NAMESPACE           from the Downward API
 #   DATA_AVALANCHE_DISCOVERY_DNS_NAME          the headless Service's DNS name
+#   DATA_AVALANCHE_POD_DNS_NAME                the pod's own DNS name under it
 # so the cluster block reads:
 #   globals:
 #     cluster:
@@ -12,6 +13,10 @@
 #       memberlist:
 #         node_name: ${POD_NAME}
 #         advertise_addr: ${POD_IP}
+# and, with TLS on the API, the URL the other nodes reach it at (its certificate covers the name):
+#   globals:
+#     api:
+#       advertise_url: https://${DATA_AVALANCHE_POD_DNS_NAME}:8443
 # A changed config reaches a running cluster through the API's reload, not a pod restart: a
 # starting node adopts the cluster's latest config generation, not its file.
 config: {}
````

`docs/src/content/docs/deployment/kubernetes.md`:

````diff
diff --git a/docs/src/content/docs/deployment/kubernetes.md b/docs/src/content/docs/deployment/kubernetes.md
index 061de92..fbc1f41 100644
--- a/docs/src/content/docs/deployment/kubernetes.md
+++ b/docs/src/content/docs/deployment/kubernetes.md
@@ -101,3 +101,20 @@ seconds: each one resolves the Service again every 5 s ([discovery](/data-avalan
 With `globals.api.tls.enabled: true` the API and the probes speak HTTPS: set `apiTls: true` when the
 config isn't a map the chart can read it from. Pass the certificate and key as `${VAR}`s from a
 Secret, or mount them with `extraVolumes` and name their paths.
+
+### The address the other nodes use
+
+Each node gossips the URL of its API, so that any node can show another's detail and pass a node
+action on to it (`/v1/nodes/{node}`). Without TLS it is `http://` and the pod's IP. With TLS the
+other nodes check the certificate against the name they connect to, so give each pod its DNS name
+under the headless Service, which the chart sets as `DATA_AVALANCHE_POD_DNS_NAME`:
+
+```yaml
+globals:
+  api:
+    advertise_url: https://${DATA_AVALANCHE_POD_DNS_NAME}:8443
+```
+
+The certificate must cover that name: `*.<release>-data-avalanche-headless.<namespace>.svc.cluster.local`, the headless Service the chart names after the release. A
+node trusts the system's roots and every certificate in its own `api.tls.cert`: put your CA after
+the node's certificate there when it is a private one.
````

`docs/src/content/docs/deployment/observability.md`:

````diff
diff --git a/docs/src/content/docs/deployment/observability.md b/docs/src/content/docs/deployment/observability.md
index 0e39a3b..be00c44 100644
--- a/docs/src/content/docs/deployment/observability.md
+++ b/docs/src/content/docs/deployment/observability.md
@@ -23,6 +23,8 @@ to the snapshot gauges.
 | Cluster | `data_avalanche_lease_state`, `data_avalanche_lease_renew_failures_total`, `data_avalanche_lease_renew_latency_seconds`, `data_avalanche_cluster_members`, `data_avalanche_owned_pipelines`, `data_avalanche_ring_generation` |
 | Status | `data_avalanche_pipeline_status` (running, paused, stopped, error), `data_avalanche_halted_tables` |
 | Events | `data_avalanche_events_written_total`, `data_avalanche_events_dropped_total`: the node's writes to the cluster's [timeline](/data-avalanche/deployment/events/) |
+| Process | `process_resident_memory_bytes`, `process_cpu_seconds_total` (Linux, the usual names), `data_avalanche_process_memory_limit_bytes`, `data_avalanche_process_cpu_limit_cores`: the container's limits, absent when there is none |
+| Gossip | `data_avalanche_gossip_probe_rtt_seconds`: a probe's round trip; `data_avalanche_gossip_suspicions_total{direction}`: suspicions this node `raised` of another, and `received` of itself |
 
 The ones to watch first:
 
````

`docs/src/content/docs/runbooks/upgrade.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/upgrade.md b/docs/src/content/docs/runbooks/upgrade.md
index 1444b44..6432a7d 100644
--- a/docs/src/content/docs/runbooks/upgrade.md
+++ b/docs/src/content/docs/runbooks/upgrade.md
@@ -32,6 +32,20 @@ During the roll both versions run the cluster's one config generation and share
 the release notes say a version can't run beside its predecessor, stop every node first and start
 them on the new version.
 
+## From 1.0 to 1.1: stop every node first
+
+A 1.1 node's gossip packets carry the API address and a hash of the node's config file, under a
+new MAC: a 1.0 node can't open them, nor a 1.1 node a 1.0 node's. Rolled one node at a time, the
+two versions would see each other as gone, and each would place the pipelines among its own
+members only. So stop every node, then start them all on 1.1:
+
+- On Kubernetes, `kubectl scale statefulset <release> --replicas=0`, wait until its pods are
+  gone, then `helm upgrade`: it scales the StatefulSet back to the chart's `replicas`.
+- Outside Kubernetes, `SIGTERM` (or `POST /v1/admin/shutdown`) every node, wait for each to exit,
+  then start them on 1.1.
+
+The pipelines stop for as long as no node runs. Each one resumes from its committed position.
+
 ## A new config with the upgrade
 
 `helm upgrade` doesn't apply a changed config (see
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche --test packaging`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

Also: `helm lint --strict deploy/helm/data-avalanche -f <each ci/*-values.yaml>` and `helm template` of `ci/tls-values.yaml` still finds 3 `scheme: HTTPS`; `cd docs && npm run build` passes its link check.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests/packaging.rs deploy/helm/data-avalanche/ci/tls-values.yaml deploy/helm/data-avalanche/templates/statefulset.yaml deploy/helm/data-avalanche/values.yaml docs/src/content/docs/deployment/kubernetes.md docs/src/content/docs/deployment/observability.md docs/src/content/docs/runbooks/upgrade.md
git commit -m "docs: 1.1's upgrade, the API's advertised URL, the process and gossip figures; the chart's pod DNS name"
```

---

### Task 9: Nodes and config through any node, on three nodes (E2E)

**Files:**
- Create: `crates/data-avalanche/tests/e2e/console_nodes.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the E2E harness's `Cluster` (three nodes), `Proc::{call, send}`, `eventually`; Tasks
  2, 5, 6 and 7.
- Produces: `a_node_is_seen_and_acted_on_through_another`.

- [ ] **Step 1: Write the failing tests**

The tests: `a_node_is_seen_and_acted_on_through_another`.

`crates/data-avalanche/tests/e2e/console_nodes.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_nodes.rs b/crates/data-avalanche/tests/e2e/console_nodes.rs
new file mode 100644
index 0000000..74a786c
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/console_nodes.rs
@@ -0,0 +1,153 @@
+//! Nodes and config through any node (Admin_Console §4, §5; M13b), on three nodes: one node shows
+//! another's detail; each node's file is seen to differ from the generation once the shared file
+//! changes; a reload asked of n2 through n1 publishes it, recorded with its note, and the files
+//! match again; any node lists the generations and the diff; a leave asked of n3 through n1 is
+//! taken and recorded by n3.
+
+use std::time::Duration;
+
+use data_avalanche_testkit::require_it;
+use serde_json::{Value, json};
+
+use crate::harness::{Proc, eventually};
+use crate::nodes::Cluster;
+
+/// Each member's `file`, as `node` lists them.
+async fn files(node: &Proc) -> Vec<(String, String)> {
+    let (code, v) = node.call("GET", "/v1/config/nodes").await;
+    assert_eq!(code, 200, "{v}");
+    v["nodes"]
+        .as_array()
+        .expect("nodes")
+        .iter()
+        .map(|n| {
+            (
+                n["node"].as_str().unwrap().to_owned(),
+                n["file"].as_str().unwrap().to_owned(),
+            )
+        })
+        .collect()
+}
+
+fn all(file: &str) -> Vec<(String, String)> {
+    ["n1", "n2", "n3"]
+        .iter()
+        .map(|n| ((*n).to_owned(), file.to_owned()))
+        .collect()
+}
+
+#[tokio::test]
+async fn a_node_is_seen_and_acted_on_through_another() {
+    require_it!();
+    let mut c = Cluster::new("console_nodes").await;
+    c.start_all().await;
+    let n1 = c.proc("n1");
+
+    let (code, v) = n1.call("GET", "/v1/nodes/n2").await;
+    assert_eq!(code, 200, "{v}");
+    assert_eq!(
+        (&v["node"], &v["config_generation"], &v["file"]["matches"]),
+        (&json!("n2"), &json!(1), &json!(true)),
+        "{v}"
+    );
+    assert!(
+        v["process"].is_object() && v["gossip"]["peers"].is_array(),
+        "{v}"
+    );
+    assert_eq!(n1.call("GET", "/v1/nodes/n9").await.0, 404);
+    assert_eq!(files(n1).await, all("same"));
+
+    // The shared file changes: every node sees its own differ, within its file check.
+    c.e.write_config(
+        &c.e.config()
+            .replace("drain_timeout: 30s", "drain_timeout: 25s"),
+    );
+    eventually("the files differ", Duration::from_secs(45), async || {
+        files(n1).await == all("differs")
+    })
+    .await;
+
+    let (code, _, body) = n1
+        .send(
+            None,
+            "POST",
+            "/v1/nodes/n2/config/reload",
+            r#"{"note": "shorter drain"}"#,
+        )
+        .await
+        .expect("an answer");
+    assert_eq!(code, 202, "{body}");
+    eventually(
+        "the files match again",
+        Duration::from_secs(45),
+        async || files(n1).await == all("same"),
+    )
+    .await;
+
+    let n3 = c.proc("n3");
+    let (code, v) = n3.call("GET", "/v1/config/generations").await;
+    assert_eq!(code, 200, "{v}");
+    let newest = &v["generations"][0];
+    assert_eq!(
+        (
+            &newest["generation"],
+            &newest["node"],
+            &newest["note"],
+            &newest["by"]["name"]
+        ),
+        (
+            &json!(2),
+            &json!("n2"),
+            &json!("shorter drain"),
+            &json!("anonymous")
+        ),
+        "{v}"
+    );
+    let (code, v) = n3.call("GET", "/v1/config/diff?from=1&to=2").await;
+    assert_eq!(code, 200, "{v}");
+    assert_eq!(
+        v["changes"],
+        json!([{
+            "path": "globals.cluster.drain_timeout", "class": "in_place",
+            "before": "30s", "after": "25s",
+        }])
+    );
+    let (code, v) = n3.call("GET", "/v1/config/generations/2").await;
+    assert_eq!(code, 200, "{v}");
+    assert!(
+        v["text"].as_str().unwrap().contains("drain_timeout: 25s"),
+        "{v}"
+    );
+
+    let (code, _, body) = n1
+        .send(
+            None,
+            "POST",
+            "/v1/nodes/n3/leave",
+            r#"{"note": "the e2e leave"}"#,
+        )
+        .await
+        .expect("an answer");
+    assert_eq!(code, 202, "{body}");
+    eventually("n3 leaves", Duration::from_secs(60), async || {
+        let (_, v) = n1.call("GET", "/v1/cluster/status").await;
+        v["members"].as_array().is_some_and(|m| {
+            !m.iter()
+                .any(|m| m["node"] == "n3" && m["phase"] == "active")
+        })
+    })
+    .await;
+    eventually(
+        "the leave is recorded by n3",
+        Duration::from_secs(30),
+        async || {
+            let (_, v) = n1.call("GET", "/v1/events?kind=action.node.leave").await;
+            v["events"].as_array().is_some_and(|e| {
+                e.iter()
+                    .any(|e: &Value| e["node"] == "n3" && e["note"] == "the e2e leave")
+            })
+        },
+    )
+    .await;
+    c.stop().await;
+}
````

`crates/data-avalanche/tests/e2e/main.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index f6fcb21..e202b81 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -8,6 +8,7 @@
 
 mod api;
 mod cluster;
+mod console_nodes;
 mod crashes;
 mod events;
 mod failover;
````

- [ ] **Step 2: Run them to see them fail**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e console_nodes:: -- --test-threads=1`
Expected: none: the behaviour landed in Tasks 2–7, and the scenario passes at once (27 s at the plan's verification).

- [ ] **Step 3: Implement**

Nothing: the behaviour landed in the earlier tasks.

- [ ] **Step 4: Run the tests and the gates**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e console_nodes:: -- --test-threads=1`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests/e2e/console_nodes.rs crates/data-avalanche/tests/e2e/main.rs
git commit -m "test(e2e): nodes and config through any node, on three nodes"
```

---

## Exit

- [ ] Every task's tests pass, and `cargo test --workspace` (1610 tests at the plan's
  verification).
- [ ] The E2E suite on PostgreSQL 18, 53 scenarios. At the plan's verification a full run (646 s)
  passed 52 and failed `cluster::any_nodes_api_drives_the_holder`, which asserted the cluster
  status's pipeline row as 1.0 wrote it; Task 2 now updates it, and that scenario (35 s) and the
  new one (27 s) then passed on the final tree.
- [ ] `helm lint --strict` and `helm template` on the chart's CI values; the site builds with its
  link check.
- [ ] Every new route is in the OpenAPI document with its role: `/v1/nodes/{node}` and its five
  actions, `/v1/config/generations`, `/v1/config/generations/{generation}`, `/v1/config/diff`,
  `/v1/config/nodes`.
- [ ] S-M13b.1 applied.
- [ ] The PR's CI green on every target.

## Deviations during execution

Each was found by the task's review and fixed in its fix round, unless it says otherwise. Later
tasks' blocks applied on top unchanged.

- **Task 1** (fix round, `e0fcf43`): the plan's diff put `origin_url` between `http_url` and its
  doc comment; the line is back above `http_url`. V-138 accepted an empty host (`https://:8080`) and
  a port that isn't a number (`http://host:abc`, `http://a:1:2`): `origin_url` now requires a host
  and, when a port is written, 1–65535 in digits, an IPv6 host in brackets (spec §6.3 "a host and
  port only"); three cases added. `advertise_url` is stored without a trailing `/`, since Task 5
  builds a peer's request as `{api_url}{path}`.
- **Task 2** (review, no fix round): the plan's Interfaces said `Advertiser::set_file_hash` returns
  whether the hash changed; it returns `false` once the gossip is gone, which `watch_file` stops
  on. The plan's text is corrected.
- **Task 4** (fix round, `9cc0c14`; Important in its review): `process_resident_memory_bytes`
  and `process_cpu_seconds_total` were registered from the start, so a host without readings (not
  Linux, or before the first sample) exposed them as `0`, which Review Focus 5 rules out. Both are
  now created with the first reading, as the limit series are; `no_reading_exposes_no_process_series`
  pins it. The workspace total is one higher than the plan's.
- **Task 6** (four fix rounds, `59a9282`, `87b01a7`, `9f3bee1`, `9d5cad2`; two Critical in its
  review): the plan's masker showed two kinds of secret in clear. (1) A secret written with a
  reference and something more, `${DB_PASS:-hunter2}` or `hunter2-${SUFFIX}`, was kept as
  written: `literal` now keeps only exactly one bare `${NAME}`, and masks anything else as a
  literal (its text fingerprinted); Task 7's `flatten` uses the same `literal`. (2) A secret in a
  YAML merge (`<<`) that a local key overrides never reaches the model, so neither the masking
  nor the re-parse check saw it. A text the masker can't vouch for is refused instead
  (`MaskError::MergeKey`, 500): one containing `<<`, a `%` directive line, a tag on a node (a `!`
  after a line start, whitespace or `[{,:?-`), or a byte-order mark. The rounds narrowed this
  from a merge-key line shape, which a tag, an anchor, an explicit key, an escaped `"\x3c\x3c"`
  with a `%`-escaped or `%TAG` merge tag, and finally a BOM each got past; the last re-review
  checked the rule's premises against serde-saphyr 2.1.1 and granit-parser 2.1.0's source (only
  a plain `<<` or a merge tag merges; their separators are space, tab, CR, LF, NUL, and a BOM is
  the only other character skipped) and probed each shape. An inline `key`/`ssl_key` is a secret
  when `-----BEGIN` appears anywhere in it (a `Bag Attributes` header first). The over-match, a
  generation with a ` !` or a `<<` in a comment or a literal, isn't shown; it fails closed.
  Task 7's two `mask.rs` blocks are re-based on the new file (context lines only; their `+`/`-`
  lines are unchanged, and the rest of Task 7 applies as written).
- **Task 8** (fix round, `b541e21`; Important in its review): Step 2's expected failure didn't
  happen, since Step 1 puts `DATA_AVALANCHE_POD_DNS_NAME` in the test's own pod environment
  before the TLS example uses it; the guard on the chart itself was missing. The chart's
  environment test (`the_chart_sets_the_environment_its_values_document`) now lists the variable,
  and fails when the StatefulSet loses it. The upgrade page scaled `statefulset <release>`; the
  chart names it `<release>-data-avalanche` unless the release's name already holds
  `data-avalanche`, which the page now says.
