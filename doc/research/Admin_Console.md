---
updated: 2026-11-14
domain: Architecture
tags: [admin-console, web-ui, management-api, auth, sessions, audit, metrics-history, prometheus, v1.1]
---

# Admin Console — Design (1.1.0)

The web console built into data-avalanche: what it is for, what it shows, what it lets people do,
and how it is built. Written from the brainstorming of 2026-11-14 with Oleksii. Where a line is
Claude's call rather than Oleksii's decision, it says **(Claude's call)**; everything else was
decided or accepted in that conversation. The mockups it refers to are the accepted wireframes
(home page, pipeline page, lag chart v3).

## 1. Intent

Operators run data-avalanche today through `curl`, the REST API, Prometheus and logs. The console
gives them, and people who only need to look, one place to see whether anything is wrong, to dig
into the cluster, its pipelines, tables and snapshot jobs, and to act on them safely, with every
action attributed to the person who took it.

**Success:** during an incident the console is the fastest correct answer to "what is wrong, since
when, and what did anyone do about it".

**The failure to avoid (anti-goal, Oleksii):** slow pages or stale data during an incident. A
console that technically works but lags behind the cluster, or quietly shows old numbers as
current, has failed. §8 turns this into requirements.

## 2. Users and roles

The API's three roles, unchanged (`API_RBAC_and_Versioning.md` §1): `viewer` ⊂ `operator` ⊂
`admin`. A viewer sees everything the console shows and changes nothing; an operator acts on
pipelines, snapshot jobs and config reloads; an admin also acts on nodes. The console shows each
action only to roles that may take it, and the server enforces the same matrix whatever the page
shows.

## 3. Goals, non-goals

**Goals**
- Every everyday task Oleksii listed: the cluster at a glance and in detail (nodes, leases, each
  node's pipelines); soft node actions; pipelines at a glance and in detail (state, config,
  snapshots with history and estimates); soft pipeline actions and snapshot jobs; all snapshot jobs
  with their parameters, state and history; job actions; the cluster's fixed config, occasionally.
- Any other view or maintenance task on a CDC part the API can serve (Oleksii: "don't narrow your
  suggestions by my view").
- Metrics, charts and colour wherever they make state, lag or a problem stand out.
- Every sensitive or destructive action records who took it, their role, and an optional note.

**Non-goals**
- **Editing configuration.** The config stays in its files (Oleksii). The console shows it,
  validates a candidate, and applies reloads; it never writes config.
- **Several clusters in one console** (Claude's call). A console shows the cluster that serves it.
- **Restarting the process itself** (Claude's call). "Restart a node" is a graceful shutdown or
  leave; the supervisor (Kubernetes, systemd) starts it again, and the dialog says so.
- **Replacing Prometheus/Grafana** for long-term metrics; the console reads Prometheus when one is
  configured (§7).

**Off unless switched on (Oleksii).** The console is optional: a command-line flag on `run`,
`--console`, turns it on for that node, and without it the node serves no console. The default is
off. With the flag off, the console's paths (§10), its sign-in pages and sessions (§9) and its live
stream don't exist on that node (404), and a `globals.console` block in the config is still
parsed and validated but unused there. What the console *reads* is recorded whatever the flag
says (Claude's call): the events and attribution (§5, §6), the history (§7) and the new API
endpoints (§12) are part of the server, because a console on one node shows what every node
records, and the audit trail is useful through the API alone. The Helm chart follows (§11).

## 4. Information architecture

Top bar: the sections, the config generation, and the signed-in name and role. Sections are
cross-linked everywhere: a pipeline links its node, a table its pipeline, a job its pipeline and
tables, an event its object.

| Section | What it shows |
|---|---|
| **Overview** | Headline tiles (pipelines by state, worst lag vs target, nodes up, snapshots running/failed, WAL held by slots). **Needs attention** (§4.1). The pipelines with a lag sparkline, state, node and last commit. Recent events. |
| **Cluster** | Members and their state (alive, suspect, leaving), each node's config generation, the hash ring and who owns what, leases with holder, epoch and time to renewal. A node page: its pipelines, lease renewals, resource figures, node actions. |
| **Pipelines** | All pipelines with state, source → sink, node, lag, last commit, snapshot state. A pipeline page (accepted mockup): header with source, sink, node, lease; actions; lag chart; tables; source panel (PostgreSQL slot, WAL held vs `max_slot_wal_keep_size`, confirmed LSN, publication; MySQL binlog position or GTID and binlog lag); current snapshot with progress and ETA, earlier jobs; the pipeline's activity; its resolved config, secrets masked. |
| **Tables** | Every table across pipelines: state (running, halted with reason), changes per minute, last commit, Delta version, files rewritten per MERGE, schema changes (applied, soft-dropped columns, refused). A table page with its own charts and events. Also answers a lake reader's "how fresh is table X?" |
| **Snapshots** | All jobs, running and ended (each pipeline keeps its 100 newest ended jobs), with parameters, who started them and their note, progress, rows done vs estimate, rate, ETA, outcome. Job actions. |
| **Events** | The cluster's timeline (§6), filterable by kind, object, actor and time. |
| **Config** | The running config (secrets masked), generation history with a diff between any two, which node runs which generation, restart-only changes waiting. Validate a candidate; apply a reload. |

### 4.1 Needs attention

One list, ordered red → orange → yellow, each line linking to where it's fixed. At least:

| Severity | Condition |
|---|---|
| red | a table halted (with its reason); a pipeline stopped by an error; a lease with no holder; a node down |
| orange | lag over target and rising; a slot's WAL held past a share of `max_slot_wal_keep_size`; repeated lease-renewal failures; a snapshot job failed; sink commit failures |
| yellow | nodes on different config generations; restart-only changes waiting; a pending removal (tombstone); a pipeline paused or stopped by a person for longer than a while |

The thresholds are config-free defaults (the builder picks them, and the page states each one).

### 4.2 Charts

The accepted lag chart is the pattern for every time series (lag, change rates, rows written,
WAL held, snapshot throughput, lease renewal latency): axes with units and times; the target as a
dashed line where one exists; events as thin dotted lines with short labels in the page's normal
small text, stacked when close; hovering a line or label highlights the event in the activity
list and the reverse; a crosshair with the time and value at the pointer. Colour is never the only
signal: states also carry a label or icon.

## 5. Actions

Every action opens one dialog: what it does, to what, and its effect (for example: "The pipeline
stops on node-b, flushes, and starts from its committed position. Nothing is lost or replayed
twice. Applies the waiting `sink.prefix`. Lag rises for about 30 s"); an optional note; and the line
it will be recorded as ("restarted by Oleksii (operator)"). Destructive actions are styled as such
and need a second confirmation where they can't be undone.

| Object | Actions | Role | API today |
|---|---|---|---|
| Pipeline | pause, resume, stop, restart, flush | operator | yes |
| Pipeline | start a snapshot job (tables, options) | operator | yes |
| Snapshot job | cancel | operator | **new** |
| Snapshot job | retry (a new job with the same parameters) | operator | **new** (or composed from the existing create) |
| Node | leave, shut down | admin | only on the node receiving the call: **new**, any node forwards to the target |
| Config | validate, reload (with `?restart=accept`) | operator | yes |

**Attribution covers the API, not only the console** (Claude's call): every mutating API call,
from the console or `curl`, records the caller's name, role and an optional `note` field in its
request. The name and role come from the server's own verification (§9), never from the page.
The `note` field is optional, so existing callers keep working unchanged.

## 6. Events and audit trail

One durable timeline per cluster, in the cluster's object storage (under `globals.state`),
readable from any node and kept for a configurable retention (default 30 days, Claude's call).
An event has: time, kind, object (cluster, node, pipeline, table, job, config), node, actor name and
role when a person or token caused it, note, and outcome. It records at least: every action of §5
(accepted, then its outcome); lease hand-offs and failovers; config reloads and generations; halts
and schema changes (applied, degraded, refused); snapshot job lifecycle; nodes joining, leaving,
going suspect; removals and their teardown.

Events name people and describe pipelines, so the docs say to keep `globals.state` where lake
readers can't read it; the same applies to the history of §7.

Object storage has no append: the builder chooses the layout (for example one small object per
event under a per-day prefix, listed by time), which must stay cheap to list for a day and to
expire.

## 7. History for charts

- **Built in, always.** Each node samples the series it owns (§4.2) and keeps them for a
  configurable retention, **24 hours by default** (Oleksii). Samples are written to object storage
  in compact chunks, so a pipeline's history follows it across a failover and survives a restart.
  Resolution and chunking are the builder's (15 s samples is the working assumption).
- **Prometheus, optional.** When configured, ranges older than the built-in retention come from
  Prometheus. The settings: URL and scheme, authentication, TLS, and **an optional set of extra
  label matchers added to every query** (Oleksii), because a collector can add or rename labels
  (for example `cluster="prod-eu"`), and so another cluster's series never mix in.
- **Without a Prometheus config,** the console uses built-in history only: no warning, no error,
  nothing marked missing (Oleksii).
- **Configured but failing** (unreachable, or no series for the labels): charts fall back to the
  built-in range and mark the gap; the console's status shows the Prometheus connection failing.

## 8. Freshness and speed (the anti-goal as requirements)

- **Live by push.** Open pages receive state changes over server-sent events; live values are at
  most a few seconds behind the cluster.
- **Age is visible.** Every panel shows how old its data is and turns plainly stale when updates
  stop. Old data is never shown as current.
- **Degraded, not blank.** Live state comes from the nodes' memory and gossip, not object storage.
  A node down or storage slow shows as "node-c unreachable" in its place; every section has its own
  time limit, so one slow source never holds a page.
- **Now first, history second.** A page renders current state at once and adds chart history when
  it arrives.
- **Budgets, measured in CI:** first page drawn under 1 s; moving between pages under 300 ms; with
  50 pipelines on the E2E cluster.
- **Replication first.** The console's work never slows the pipelines: lower priority, its own
  request limits.

## 9. Sign-in and sessions

One sign-in page; what follows depends on `globals.api.auth`:

| Mode | Sign-in |
|---|---|
| `none` | Straight in as `admin`, with a permanent banner: the cluster has no authentication. |
| `basic` | A form (not the browser's popup, so sign-out works). The server checks the credentials and starts a session. Attempts are rate-limited. |
| `oauth2` | Authorization code with PKCE against the cluster's identity provider; the server exchanges the code. Needs a new `client_id` (registered with the provider); the cluster has no client today, only token verification. Refresh tokens are used when the provider issues them. |
| `jwt` | A request already carrying `Authorization: Bearer` (a proxy in front) goes straight through. Otherwise the page takes a pasted token once; the session lasts until the token expires (Claude's call, both ways). |

- **Session** (best practice: tokens and passwords never reach page JavaScript): an encrypted,
  `HttpOnly`, `Secure`, `SameSite=Strict` cookie, readable by every node so a load balancer can
  send any request anywhere. **The key comes from configuration** (Oleksii):
  `globals.console.session_key`, set through `${ENV}` to the same value on every node, required
  on a node started with `--console` when `auth` is not `none`: such a node refuses to start
  without it, and if a later config generation drops it, that node's console shows a
  "console misconfigured" page and logs an error, while its pipelines carry on (Claude's call). It is never kept in object storage, because people who may read the
  lake's bucket must not be able to mint an admin session. Every request is still verified the
  way the API verifies a bearer token today; the role and the recorded name always come from that
  verification.
- **Name shown and recorded:** a configurable claim, `preferred_username` by default, falling back
  to `sub`; `basic` uses the configured user; `none` records `anonymous (admin)` (Claude's call).
- **The console's public address:** `oauth2` needs the exact redirect URI the provider registered,
  which a node behind a proxy can't work out safely from forwarded headers. A
  `globals.console.public_url` setting gives it, required when `auth = oauth2` (Claude's call).
- **401** (not signed in, expired): back to sign-in, saying why. **403** (signed in, not allowed,
  or a token whose roles map to nothing): names the role needed and the role held.
- **Sign-out** clears the session and, with `oauth2`, signs out at the provider when it offers an
  end-session endpoint.
- **Same origin:** the console and the API share one listener, so no CORS opens. Mutating
  requests carry an anti-forgery check besides `SameSite`. A strict Content-Security-Policy, no
  framing.
- **Plain HTTP:** `Secure` cookies work on `localhost`; elsewhere TLS is expected (on the listener
  or a proxy in front, as V-052 already assumes).

## 10. Front end and serving

All Claude's calls, "follow best practices" (Oleksii):

- **Svelte 5 + TypeScript, built by Vite**, as a single-page app under `ui/` in the repository.
  Small bundles and no runtime framework weight suit an app embedded in a Rust binary.
- **uPlot** for time series: small, fast with long series, and able to draw the event lines,
  labels, crosshair and hover links of §4.2.
- **Embedded in the binary,** built in CI and in the Dockerfile before `cargo build`. A plain
  `cargo build` without it serves a "console not built" page, so Rust-only work needs no Node
  (Node 26 is already in the repository for the docs site). Nothing is fetched from the internet
  at runtime.
- **Served from the API listener** under its own path (for example `/console/`), every node
  serving it. Node-targeted actions and another node's live detail are fetched by the serving node
  from the target node, each with its own time limit. The serving node passes the caller's own
  credentials through, so the target verifies the caller itself and records the same name; no
  node-to-node trust is added (Claude's call). Each node's API address travels in its gossip
  metadata if it doesn't already. While a console is connected, the serving node follows its peers'
  live changes and merges them into the one stream the page reads (Claude's call; the mechanism is
  the builder's).
- **Light and dark** themes following the system setting.
- **Tests:** Vitest for components; Playwright for real flows against the E2E cluster, including
  every sign-in mode, the 401/403 pages, the actions' attribution, the §8 budgets, and a node
  without `--console` serving none of it.
- **Accessibility:** keyboard navigation, labels on every state, contrast in both themes.

## 11. Configuration surface (proposed; spec edits need approval)

New settings, to be patched into `doc/specs/Configuration_Specification.md` once approved
(names are proposals):

- `globals.api.oauth2.client_id` (and optional `scopes`): the console's client for `oauth2`.
- The command-line flag `run --console` (Oleksii): off by default; it switches the console on
  for that node (§3). `System-Overview.md` lists the command-line parameters, so it gains this one.
- `globals.console` (no `enabled` field: the flag decides): `session_key` (`${ENV}`, required on a
  node with `--console` unless `auth = none`, §9); `public_url` (required with `oauth2`); `session_ttl` (default 8 h, Claude's call);
  `username_claim` (default `preferred_username`); `history.retention` (default 24 h);
  `events.retention` (default 30 d); `prometheus` (optional: `url`, `auth`, `tls`,
  `extra_labels`).

**Helm chart** (Oleksii: "adapt helm chart accordingly"): a `console.enabled` value, `false` by
default, that adds `--console` to the container's arguments; the session key and any OAuth2
client secret reach the pods through the chart's existing `env`/`envFrom`; `NOTES.txt` gives the
console's address when it is on; the chart's `ci/` values gain a console-on case. The chart has
no Ingress today and gains none (Claude's call): the console is served on the API's Service.

Docs touched besides the spec (Oleksii: don't forget the documentation site):
`API_RBAC_and_Versioning.md` §2 (the new endpoints and their roles),
`Observability_Metrics_and_Tracing.md` (the history store), the gap tracker, the chart's
README, and the docs site: a console page (switching it on, sign-in per mode, what it shows),
`reference/configuration` (`globals.console`, `oauth2.client_id`), `reference/api` (the new
endpoints, the `note` field), `deployment/kubernetes` (`console.enabled`) and
`deployment/container` (`--console`), `deployment/observability` (history and Prometheus), and
the runbooks pointing at the console where it does the job.

## 12. New server work this needs

- The event store and its writers (§6); attribution on every mutating call (§5).
- The history sampler and store, and the Prometheus reader (§7).
- API: events (list, filter), history (series, range), a live stream (SSE), tables per pipeline
  and across pipelines, source detail (slot, WAL held, binlog position), all snapshot jobs, job
  cancel and retry, node leave and shutdown through any node, config generations and diff.
- Sign-in, sessions, the 401/403 pages (§9).
- Serving the embedded app (§10), behind the `--console` flag (§3).

## 13. Left to the builder

The attention thresholds; the event and history layouts in object storage; sampling resolution;
the exact paths of the new endpoints and the console; the SSE message shapes; how the milestones
split this (likely: server foundations — events, attribution, history, new endpoints; sign-in and
sessions; the console shell and Overview; the detail pages and actions).

## 14. Delivery

All of it ships as **1.1.0** (Oleksii): one release, built as several milestones (one branch and
one PR each, as `CLAUDE.md` sets out), with nothing tagged until the last one merges. The 1.1.0
release follows the release rule of 1.0.1: dependencies refreshed and both gates passing.
