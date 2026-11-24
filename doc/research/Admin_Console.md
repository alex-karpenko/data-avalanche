---
updated: 2026-11-24
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

**Off unless switched on (Oleksii).** The console is optional: `globals.console.enabled` in the
config file switches it on, and the default is `false`. While it's off, the console's paths
(`/ui/`, §10) and its sign-in pages and sessions (§9) don't exist (404); the rest of the
`globals.console` block is still validated. It is a reloadable setting: a reload turns the console
on or off without a restart. What the console *reads* is recorded whatever the switch
says (Claude's call): the events and attribution (§5, §6), the history (§7) and the new API
endpoints (§12) are part of the server, because a console on one node shows what every node
records, and the audit trail is useful through the API alone. The Helm chart follows (§11).

## 4. Information architecture

Top bar: the sections, the config generation, and the signed-in name and role. Sections are
cross-linked everywhere: a pipeline links its node, a table its pipeline, a job its pipeline and
tables, an event its object.

**The approved page designs are `doc/v1.1/mockups/`** (one file per page, approved by Oleksii on
2026-11-14); a page is built to its mockup, and where this section and a mockup differ, the mockup
wins. Across every list: clicking a row opens its object's page, and rows are real links (a new
tab, a copied link); filters and sort live in the address, so a link shows the same view; Back
keeps them; ↑/↓ and Enter move and open.

| Section | What it shows |
|---|---|
| **Overview** | Headline tiles (pipelines by state, worst lag vs target, nodes up, snapshots running/failed, WAL held by slots). **Needs attention** (§4.1). The pipelines with a lag sparkline, state, node and last commit. Recent events. |
| **Cluster** | Members and their state (alive, suspect, leaving), each node's config generation, leases with holder, ring owner, epoch and time to renewal. **Pipelines held away from their ring owner** stand out (Oleksii; no hash-ring drawing): a tile, a panel with holder, owner, since when, why and when each goes back, orange lease rows, and per node "held / owned". A node page: its pipelines, lease renewals, memory, CPU and gossip figures, node actions. |
| **Pipelines** | All pipelines with state, source → sink, node, lag, last commit, snapshot state, worst first; select several to act on them together (one at a time, stopping at the first failure). A pipeline page (accepted mockup): header with source, sink, node, lease; actions; lag chart; tables; source panel (PostgreSQL slot, WAL held vs `max_slot_wal_keep_size`, confirmed LSN, publication; MySQL binlog position or GTID and binlog lag); current snapshot with progress and ETA, earlier jobs; the pipeline's activity; its resolved config, secrets masked. |
| **Tables** | Every table across pipelines: state (running, halted with reason), changes per minute, last commit, Delta version, files rewritten per MERGE, schema changes (applied, soft-dropped columns, refused). A table page with its own charts and events. Also answers a lake reader's "how fresh is table X?" |
| **Snapshots** | All jobs, running and ended (each pipeline keeps its 100 newest ended jobs), with parameters, who started them and their note, progress, rows done vs estimate, rate, ETA, outcome; a 24 h timeline per pipeline. A job page: each table's progress and what lake readers see of it, the job's effect on lag. Job actions. A job is shown by the first 8 characters of its id's uuid, and copies the full id. |
| **Events** | The cluster's timeline (§6), filterable by kind, object, actor and time. |
| **Config** | Four tabs: the running config (secrets masked); generations with a diff between any two, each change tagged with its class and what it did; Nodes: which generation each node runs, whether its own file matches it, restart-only changes waiting; Validate & reload: pick the node whose file to check (a reload publishes that node's file), the report and the changes, accept restart-only changes, a note. Reload only after the same file validated. |

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
| Pipelines (several) | the same actions on a selection | operator | composed |
| Snapshot job | cancel (a table the job was bringing back from a halt halts again, reason "copy cancelled", Oleksii) | operator | **new** |
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
going suspect; removals and their teardown; **refused attempts** (a 403 on a mutating call), so the
trail shows attempts as well as successes. The Events page filters by time, kind, object, actor and
node, narrows by dragging across a histogram, and exports CSV or JSON.

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

### 8.1 The live stream (M13f)

`GET /v1/stream` (viewer) is the push above. Oleksii decided three things on 2026-11-19: it sends
**values as they change** (not "something changed, read again"), the **figures every 5 s**, and
**no replay** (a page reads the current state, then applies what follows). The rest is Claude's
call.

- **Each node publishes its own, from memory.** A pipeline's `pipeline` state (looked at every
  second, sent on a change of state, error or lease term; `standby` once when the node stops
  running it), its `figures` (the live answer, the halts' start times left out: they are in
  storage), the node's own `node` figures, each chart `samples` as taken, each `event` once
  written. Nothing is built while no stream is open on the node; a stream that opens gets every
  state again within a second. A node killed sends no `standby`: when two nodes name the same
  pipeline as theirs, the one with the higher `owner_epoch` runs it.
- **The node asked merges the others.** It follows each other member with an API address, with
  the caller's own credentials and the target and forwarded-by headers of §10's forwarding, and
  passes their messages on as sent; a stream asked for by another node is that node's own alone.
  `peer` says when a member is followed (`following`) and when it can't be (`unreachable`, with
  why: it can't be reached, it is full of streams, "it answered 503", or it refuses credentials
  the node asked still takes, "it answered 401" on other auth settings), retried after 1, 2, 5,
  then every 10 s; `members` says when the members or their phases change. Every message's data
  names the node that sent it. A member followed again sends its states again (its stream is a
  new one), but not its events from the gap: a page reads `GET /v1/events` for the gap.
- **The caller is checked by the node asked, at the open and before each follow again.** Its
  credentials kept (its `Authorization` alone, never logged) are verified again, as the route
  does, before a member is followed again; once they no longer verify (a token expired, a
  password changed, a role taken away), the stream is sent `reset` (`credentials`) and ends, and
  the page opens it again with fresh credentials. How a stream ends with its session is M14's.
- **A stream another node asked for takes what its node doesn't know.** The node asked checked
  the query against its own generation, maybe newer: a pipeline the node followed doesn't run is
  accepted (nothing about it passes until it runs it), a kind or a parameter it doesn't know is
  skipped, and it reads no config; a parameter given twice is still `400`. So a healthy member a
  generation behind is followed, not `unreachable` with a `404`.
- **Bounded.** 64 streams a node, its peers' included (`503` with `Retry-After` past that): each
  page holds a place on every node, so a node full of streams shows on other pages as
  `unreachable`, "it answered 503". A stream more than 1024 messages behind is sent `reset`
  (`lagged`, with `missed`) and reads again; a followed stream silent for 45 s (three
  keep-alives) is dropped and followed again; a followed message over 1 MiB ends that follow, and
  a refusal is read to 8 KiB, its reason clipped. A slow page holds only its own stream.
- **It never holds a node's stop.** A node ends its streams before its server's graceful stop.
- **Filters:** `pipeline` (ids) and `kind`; the control messages (`hello`, `members`, `peer`,
  `reset`) are always sent. A `pipeline` filter drops the messages that name no pipeline
  (`node`, a node's `samples`, events without a pipeline). A pipeline the generation doesn't
  have is `404`; a kind or a parameter that isn't one, or one given twice or empty, `400`. The
  filters go to the followed nodes with the request.

## 9. Sign-in and sessions

One sign-in page; what follows depends on `globals.api.auth`:

| Mode | Sign-in |
|---|---|
| `none` | Straight in as `admin`, with a permanent banner: the cluster has no authentication. |
| `basic` | A form (not the browser's popup, so sign-out works). The server checks the credentials and starts a session. Attempts are rate-limited. |
| `oauth2` | Authorization code with PKCE against the cluster's identity provider; the server exchanges the code. Needs a new `client_id` (registered with the provider); the cluster has no client today, only token verification. Refresh tokens are used when the provider issues them. The sign-in page goes to the provider by itself after 5 s, unless the person stays, and not after a sign-out or a failed sign-in (no loop); it names the provider by `oauth2.idp_name`, or the issuer's host name (Oleksii). |
| `jwt` | **No sign-in page and no session** (Oleksii): every request must carry `Authorization: Bearer` (a proxy in front sends it). Without one, or with one refused, the "No valid token" page says why. |

- **Session** (`basic` and `oauth2`; best practice: tokens and passwords never reach page JavaScript): an encrypted,
  `HttpOnly`, `Secure`, `SameSite=Strict` cookie, readable by every node so a load balancer can
  send any request anywhere. **The key comes from configuration** (Oleksii):
  `globals.console.session_key`, set through `${ENV}` to the same value on every node, required
  when the console is enabled with `auth` `basic` or `oauth2` (a validation rule, so a config without it
  is refused at start and on reload). It is never kept in object storage, because people who may read the
  lake's bucket must not be able to mint an admin session. Every request is still verified the
  way the API verifies a bearer token today; the role and the recorded name always come from that
  verification.
- **Name shown and recorded:** a configurable claim, `preferred_username` by default, falling back
  to `sub`; `basic` uses the configured user; `none` records `anonymous (admin)` (Claude's call).
- **The console's public address:** `oauth2` needs the exact redirect URI the provider registered,
  which a node behind a proxy can't work out safely from forwarded headers. A
  `globals.console.public_url` setting gives it, required when `auth = oauth2` (Claude's call).
- **Not signed in or expired** (401): "Your session has ended", saying why, with a sign-in button
  back to the page the person was on; with `jwt`, "No valid token" and no button. **Not allowed**
  (403): "Forbidden", naming the role needed, the role held and, with tokens, which roles mapped to
  it. The pages are captioned in words, not status codes (Oleksii).
- **Sign-out** clears the session and, with `oauth2`, signs out at the provider when it offers an
  end-session endpoint.
- **Same origin:** the console and the API share one listener, so no CORS opens. Mutating
  requests carry an anti-forgery check besides `SameSite`. A strict Content-Security-Policy, no
  framing.
- **Plain HTTP:** `Secure` cookies work on `localhost`; elsewhere TLS is expected (on the listener
  or a proxy in front, as V-052 already assumes).

### 9.1 Sessions as built (M14)

Oleksii decided three things on 2026-11-24: a session lasts `session_ttl` **from sign-in**, however
it is used (the config spec says so since S-M14.1); the sign-in and error pages are **rendered by
the server** (M15's app links to them); and failed basic sign-ins are counted **per client
address**. The rest is Claude's call.

- **The cookie.** `__Host-da_session` (the prefix means a sibling subdomain can't plant one),
  AES-256-GCM under a key derived from `session_key`, the key's id
  beside the ciphertext so a changed key is told from an altered cookie; `HttpOnly`, `Secure`,
  `SameSite=Strict`, `Path=/`, no `Domain`. One too long for a cookie (a provider's long tokens)
  goes on in `__Host-da_session.1` … `.3`; one longer starts no session. It holds when the person signed in, the
  anti-forgery token, and what verifies them: the basic user and a keyed fingerprint of the
  password, or the provider's access token and its refresh token. Nothing is kept server-side.
- **Every request verifies it again**, as the API verifies credentials: a basic session's user and
  password must still be the configured ones; an `oauth2` session's access token verifies as a
  bearer token does (issuer, audience, signature, roles). It ends `session_ttl` after sign-in, or
  sooner when that fails, when `session_key` or `auth` changes, or when the console is switched
  off. A request with `Authorization` is taken by its credentials, never its cookie. A session
  under inconsistent settings, or a token that can't be checked now (the provider's keys are
  unreachable), answers 503, not 401. While sessions are on, a 401 for a request without
  `Authorization` carries `WWW-Authenticate: Session realm="data-avalanche"`, so a browser never
  shows its credentials popup; a request that sent `Authorization` gets `Basic` or `Bearer`; the
  console's own 401 pages carry `Session` (`Bearer` under `jwt`), never `Basic`.
- **The API takes it too**, so the console's app calls `/v1` with no token in page JavaScript. A
  request the session authenticates that changes something sends the session's anti-forgery token
  (`X-CSRF-Token`, from `GET /ui/session`): `403` without it, before its role is looked at.
- **Peers open it** (M13f's carry-in): a node passes a caller's session cookies and anti-forgery
  token on with its `Authorization` (node actions, another node's detail, a stream's follows), and
  the peer verifies them with the key every node has.
- **A stream ends with it** (M13f's carry-in): an open stream checks its caller every minute and a
  second after the credentials' expiry; once they no longer verify it is sent `reset`
  (`credentials`) and ends, and the page opens it again.
- **`oauth2`.** Authorization code with PKCE (`S256`) and a new state, kept in a sealed
  `da_oauth2` cookie (`SameSite=Lax`, the callback's path, 10 min); the code exchanged with the
  console's client (`client_secret_basic`, or `client_secret_post` when that is all the provider
  takes); the token verified before a session starts, so a token whose roles map to nothing is
  "Forbidden" at sign-in. The callback answers a page that goes on to `next` from the console's own
  origin, since a `Strict` cookie doesn't follow a redirect the provider began; it answers 503
  when the token can't be checked now, and a callback whose state isn't the browser's own sign-in
  shows nothing from the provider. A session whose token expires within a minute is refreshed by
  the next page asked for, or by `POST /ui/session/refresh` (the app's, with the anti-forgery
  token; it refreshes whenever called, with no expiry check); a session past its end is never
  refreshed (it ends when it would have). One node makes
  one refresh per refresh token at a time and gives its outcome to the same token for 30 s, since
  a provider that rotates refresh tokens refuses one used twice. The ID token is kept in `da_id`, sent only to
  the sign-out, for the provider's `id_token_hint`.
- **The routes under `/ui/`:** `GET /ui/sign-in` (the form, or the way to the provider),
  `POST /ui/sign-in` (basic), `GET /ui/oauth2/start`, `GET /ui/oauth2/callback`,
  `POST /ui/sign-out`, `GET /ui/session`, `POST /ui/session/refresh`, `/ui/assets/…`, and every
  other page (M15's app; until then, who is signed in). A page asked for with no credentials goes
  to the sign-in; with credentials that no longer verify it is the 401 page, saying why.
- **No path prefix.** The console and `/v1` are served at the root of one host: every redirect and
  route is an absolute `/ui/…` path, and `public_url` is the origin with no path.
- **Hardening.** A Content-Security-Policy that runs no inline script or style
  (`script-src 'self'`, `form-action 'self'` and the provider's origins, `frame-ancestors
  'none'`), `X-Frame-Options: DENY`, `nosniff`, `Referrer-Policy: no-referrer`, `no-store`. A form
  posted to `/ui/` from another site (`Sec-Fetch-Site`, else `Origin` against `Host`) is refused.
  `next` is always a path under `/ui/` with no `.` or `..` segment (encoded ones too), else
  `/ui/`.
- **Failed basic credentials** are limited per client address (an IPv6 address by its /64), on the
  sign-in form and on the API's `Authorization: Basic` (and the console's pages that take Basic).
  After 5 failures in a row the address waits 1 minute on that node: `429` with `Retry-After`, even
  for the right password; a success starts the count again. Bearer tokens and sessions aren't
  counted. A node keeps at most 10 000 addresses; when its table is full, a new address's failures
  aren't counted, so a flood never locks anyone else out. A request forwarded by another node counts
  against that node's address (during a password rotation a peer may refuse one node's forwarded
  Basic requests for up to a minute). Behind a proxy every client shares the proxy's address
  (forwarded headers aren't trusted). The OpenAPI document lists `429` on every operation.
- **Signing out** removes the cookies; with `oauth2` it goes on to the provider's end-session
  endpoint, when it has one. Since nothing is kept server-side, a copy of the cookie taken before
  works until the session ends: a short `session_ttl`, or a new `session_key`, bounds that.

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
- **Served from the API listener** under the path prefix **`/ui/`** (Oleksii), every node serving
  it. The console's own server routes (sign-in, sign-out, the OAuth2 callback) are under `/ui/`
  too; the data it reads and the actions it takes go through the versioned API under `/v1/`, which
  `curl` can use as well. Node-targeted actions and another node's live detail are fetched by the serving node
  from the target node, each with its own time limit. The serving node passes the caller's own
  credentials through, so the target verifies the caller itself and records the same name; no
  node-to-node trust is added (Claude's call). Each node's API address travels in its gossip
  metadata if it doesn't already. While a console is connected, the serving node follows its peers'
  live changes and merges them into the one stream the page reads (Claude's call; the mechanism is
  the builder's).
- **Light and dark** themes following the system setting.
- **Tests:** Vitest for components; Playwright for real flows against the E2E cluster, including
  every sign-in mode, the 401/403 pages, the actions' attribution, the §8 budgets, and a cluster
  with the console disabled serving none of it.
- **Accessibility:** keyboard navigation, labels on every state, contrast in both themes.

## 11. Configuration surface

In `doc/specs/Configuration_Specification.md` §6.3.1 and §6.8 (approved 2026-11-14, with the
follow-up for `idp_name` and `session_key` in `basic` and `oauth2` only):

- `globals.api.oauth2.client_id` (and optional `client_secret`, `scopes`): the console's client for
  `oauth2`; `idp_name` (optional): the provider's name on the sign-in page.
- `globals.console`: `enabled` (default `false`, Oleksii; reloadable, §3); `session_key`
  (`${ENV}`, required when enabled with `basic` or `oauth2`, §9); `public_url` (required when enabled
  with `oauth2`); `session_ttl` (default 8 h, Claude's call);
  `username_claim` (default `preferred_username`); `history.retention` (default 24 h);
  `events.retention` (default 30 d); `prometheus` (optional: `url`, `auth`, `tls`,
  `extra_labels`).

**Helm chart** (Oleksii: "adapt helm chart accordingly"): the switch lives in the config, which the
chart already renders from `config`, so the chart gains no switch of its own. The session key and
any OAuth2 client secret reach the pods through the chart's existing `env`/`envFrom`; `NOTES.txt`
gives the console's address (`/ui/`) when the config map enables it; the values file's comments
show the console block; the chart's `ci/` values gain a console-on case. The chart has
no Ingress today and gains none (Claude's call): the console is served on the API's Service.

Docs touched besides the spec (Oleksii: don't forget the documentation site):
`API_RBAC_and_Versioning.md` §2 (the new endpoints and their roles),
`Observability_Metrics_and_Tracing.md` (the history store), the gap tracker, the chart's
README, and the docs site: a console page (switching it on, sign-in per mode, what it shows),
`reference/configuration` (`globals.console`, `oauth2.client_id`), `reference/api` (the new
endpoints, the `note` field), `deployment/kubernetes` and
`deployment/container` (switching the console on, the session key's secret), `deployment/observability` (history and Prometheus), and
the runbooks pointing at the console where it does the job.

## 12. New server work this needs

- The event store and its writers (§6); attribution on every mutating call (§5).
- The history sampler and store, and the Prometheus reader (§7).
- API: events (list, filter), history (series, range), a live stream (SSE), tables per pipeline
  and across pipelines, source detail (slot, WAL held, binlog position), all snapshot jobs, job
  cancel and retry, node leave and shutdown through any node, config generations and diff.
- Sign-in, sessions, the 401/403 pages (§9).
- New figures: process memory and CPU, gossip (peers seen, last probe, suspicions), and each node's
  file against the running generation (§4).
- A halt reason for a cancelled copy ("copy cancelled", §5), in the halted-table runbook too.
- Recording refused attempts (§6).
- Serving the embedded app under `/ui/` (§10), behind `globals.console.enabled` (§3).

## 13. Left to the builder

The attention thresholds; the event and history layouts in object storage; sampling resolution;
the exact paths of the new `/v1/` endpoints (they go into the spec's endpoint table with the first
server plan) and of the routes under `/ui/`; the SSE message shapes (settled in §8.1); how the
milestones split this (likely: server foundations — events, attribution, history, new endpoints;
sign-in and sessions; the console shell and Overview; the detail pages and actions).

## 14. Delivery

All of it ships as **1.1.0** (Oleksii): one release, built as several milestones (one branch and
one PR each, as `CLAUDE.md` sets out), with nothing tagged until the last one merges. The 1.1.0
release follows the release rule of 1.0.1: dependencies refreshed and both gates passing.
