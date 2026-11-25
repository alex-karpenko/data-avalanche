---
title: Web console
description: Switching the web console on, what it shows, how people sign in under each API auth mode, and what a session is.
sidebar:
  order: 5
---

The web console is served by every node, on the API's listener, under `/ui/`. It is off until
`globals.console.enabled: true`, and a reload turns it on or off. People sign in as the API's
auth mode (`globals.api.auth`) says, and get the same role there as through the API.

## Switching it on

```yaml
globals:
  console:
    enabled: true
    session_key: ${CONSOLE_SESSION_KEY}    # basic and oauth2
    public_url: https://cdc.example.com    # oauth2
```

`session_key` encrypts the console's session cookie. Give every node the same value, at least 32
bytes, from the environment or a secret; it is never kept in object storage. Changing it signs
everyone out. Make one with `openssl rand -base64 48`.

Serve the console over HTTPS, on the listener (`globals.api.tls`) or on a proxy in front of it: its
cookies are `Secure`, which browsers send over plain HTTP only to `localhost`.

## What it shows

Every page has the same top bar: the sections, the live connection's state, the cluster's config
generation, and who is signed in, with their role and the sign-out. This build has the
**Overview** and the **Pipelines**; the other sections say they aren't in this build yet.

The page shows the cluster as it is when it opens, then follows the cluster's live updates
(`GET /v1/stream`) through whichever node served it, so it changes as things happen without a
reload. The bar says `live`, `reconnecting` with how long since the last update, or, after 30 s
without one, the page says it is offline since then and that everything shown is as of that time,
with a way to retry at once. The connection comes back by itself.

The **Overview** answers "is anything wrong?":

- tiles: the pipelines by state, the worst lag against its target, the nodes up and their config
  generation, the snapshots running (and failed today), and the WAL the PostgreSQL slots hold;
- **Needs attention**: what is wrong now, red first, then orange, then yellow, the newest first,
  each with a link to where it's dealt with;
- every pipeline, the worst first, with its state, node, lag over the last hour against the target,
  and last commit;
- the newest events.

What needs attention, and the thresholds (fixed in this release, and stated at the foot of the
page; a later release makes them configurable):

| | |
|---|---|
| Red | A table halted; a pipeline stopped by an error; a pipeline no node has held for over 30 s; a node the others' probes get no answer from |
| Orange | A pipeline's lag over 60 s and higher than 5 min ago; a slot holding WAL past 50% of `max_slot_wal_keep_size` (16 GB when the server sets none); a lease lost or taken over in the last hour; a snapshot job failed in the last 24 h; a node the console can't follow; a pipeline that can't commit |
| Yellow | A node on another config generation; a restart-only change waiting on a node; a pipeline paused or stopped by a person over 1 h ago |

A node the console can't follow shows its pipelines as last known, marked with how long it has been
out of reach.

### The pipelines

**Pipelines** lists every pipeline, the worst first: its state, source and sink, the node that runs
it (`away` when that isn't its owner on the ring, after a failover), its lag over 6 hours against
the 60 s target, its changes a minute, its last commit, its tables (how many are halted), and its
snapshot (the running job's progress and ETA, else the last job's outcome). The filters (by state,
node, source and name) and the sort (a column's heading) are kept in the page's address, so a link
shows the same view and Back returns to it. A click anywhere on a row opens it; ↑ and ↓ move
between rows and Enter opens one.

A pipeline's page has its source and sink, its node and lease, its lag over 6 hours with the
events that marked it (actions, halts, failovers, snapshot jobs) drawn on the chart, its tables
with their changes, Delta version, files rewritten by the last MERGE and last schema change, what
its source keeps for it (PostgreSQL's slot, the WAL it holds against `max_slot_wal_keep_size`, the
confirmed LSN and the publication; MySQL's position, how long the server keeps binlogs, and how far
behind the stream reads), its snapshot jobs, and what happened to it. Hovering an event on the
chart highlights it in the list below, and the reverse.

Every part of the page says how old its data is. A part that couldn't be read keeps what it had and
says so with its age (`jobs (HTTP 503), last read 20 min ago`, or `never read`), the source's
panel says when it was read (`read 40 s ago`), and while the live connection is stale every panel
is dimmed and the banner says for how long.

### Acting on pipelines

An `operator` or an `admin` sees the actions a pipeline's state takes: flush, pause, resume,
restart, stop, and a snapshot of chosen tables (truncated first, or not). Each opens a dialog that
says what it does and to what, takes an optional note (up to 500 characters), and shows the line
it will be recorded as: `restarted by Oleksii (operator)`. The server checks the role again and
records the action, with the note, in the cluster's events, whatever the page shows. A `viewer`
sees no actions.

On **Pipelines**, select several to act on them together: the dialog names each one and its node,
and those the action doesn't apply to (a paused pipeline isn't restarted). They are asked one at a
time, in order; a failure stops the rest, and the dialog says which failed, why, and which weren't
asked. While the page is offline, actions wait.

An action on the list reaches only the selected pipelines the filters show. Those a filter hides
stay ticked and come back, selected, when the filter is cleared.

## Signing in

| `globals.api.auth` | What a person sees |
|---|---|
| `none` | No sign-in: everyone is `admin`, and every page says the cluster has no authentication. |
| `basic` | A sign-in form for the configured user. Failed passwords are limited per address (see below). |
| `oauth2` | "Continue with" the provider (`oauth2.idp_name`, else the issuer's host); the page goes there by itself after 5 s, unless the person stays, or just signed out, or the sign-in failed. |
| `jwt` | No sign-in and no session: a proxy in front of the console sends a bearer token with every request. Without a valid one, the page says "No valid token" and why. |

A person whose roles map to no role on the cluster sees "Forbidden", with the roles their token
carried; with `oauth2` that happens at sign-in.

### Failed passwords with `basic`

Failed basic credentials are counted per client address (an IPv6 address by its /64), on the
sign-in form and on the API's `Authorization: Basic` (and the console's pages that take Basic).
The API's limit applies whenever `auth` is `basic`, with or without the console. After 5 failures in a row the address waits 1 minute: `429` with `Retry-After`, even for the right
password. A success starts the count again. Bearer tokens and sessions aren't counted.

- Each node counts for itself, in memory. A request forwarded by another node counts against that
  node's address, so during a password rotation a peer may refuse one node's forwarded Basic
  requests for up to a minute.
- A node keeps at most 10 000 addresses. When its table is full, a new address's failures aren't
  counted, so a flood never locks anyone else out.
- Behind a proxy every client shares the proxy's address: forwarded headers aren't trusted. Five
  failures from one person make the proxy's address wait for everyone.

### With `oauth2`

Register the console as a client at the provider:

- `client_id` (`globals.api.oauth2.client_id`), and `client_secret` only for a confidential client;
- the redirect URI `<public_url>/ui/oauth2/callback`;
- the sign-out's return address `<public_url>/ui/sign-in?signed_out`, where the provider allows one.

The console signs in with the authorization code flow and PKCE, and exchanges the code itself. The
provider's endpoints come from its discovery document (`discovery_url`, else the issuer's
`/.well-known/openid-configuration`), so the node must reach the provider. The access token it
gets is verified as any API token is: its signature, expiry, issuer, audience and roles.

When the provider issues refresh tokens, a session's token is refreshed by the next console page
asked for once the token expires within a minute (or already has, while the session lasts), or by
the app's `POST /ui/session/refresh`. `/v1` calls with the cookie don't refresh. A node makes one
refresh per refresh token at a time and gives its outcome (a new token or a refusal) to the same
token for 30 s, since a provider that rotates refresh tokens refuses one used twice. That is per
node: behind a load balancer two nodes may refresh one session at once, so a provider that rotates
refresh tokens should allow a short reuse window. A session past its end is never refreshed: it
ends when it would have.

A provider that refuses a refresh (a 4xx other than 408 and 429: a revoked or spent refresh token)
ends the session: the app's refresh answers 401 at once, and a page says so (401) once the token has
expired. A provider that can't be reached (no connection, a timeout, a 408, a 429, a 5xx, its
discovery document unreadable) doesn't end it: the app's refresh answers 503 with `Retry-After`,
"try again in a moment", and so does a page once the token has expired; before that the old token
still verifies and the page is served. The session's cookies are kept. A node remembers an outage
(and a failed discovery) for 5 s, so requests in that time get the same answer without asking the
provider, then asks it again. If the provider's token can't be checked at the moment (its keys are
unreachable), the sign-in callback answers 503; the person tries again. Failed sign-ins and
refreshes are also logged with the provider's error, never a token. Signing out removes the
session's cookies and, when the provider has an end-session endpoint, signs out there (see
Sessions).

## Sessions

With `basic` and `oauth2`, signing in starts a session: an encrypted cookie that every node reads,
so a load balancer may send each request anywhere. It ends `session_ttl` after sign-in (8 h by
default), however it is used, and sooner when:

- the basic user or password changes;
- the provider's token expires and the provider refuses its refresh;
- `session_key` or `globals.api.auth` changes;
- the console is switched off.

A provider token whose roles no longer map is refused ("Forbidden", 403) rather than ending the
session.

An ended session's page says why (the console's own 401 pages carry `Session` in
`WWW-Authenticate`, `Bearer` under `jwt`, never `Basic`), with a way to sign in again and come back. Where a sign-in
returns (`next`) is always a path under `/ui/`, with no `.` or `..` segment; any other value
returns to `/ui/`.

The cookie is `__Host-da_session`: `HttpOnly`, `Secure`, `SameSite=Strict`, `Path=/`, and no
`Domain`, so a sibling subdomain can't plant one. A session too long for one cookie (a provider's
long tokens) goes on in `__Host-da_session.1` to `.3`. With `oauth2`, two more are set:
`__Host-da_oauth2`, the sign-in in progress (`Path=/`, `SameSite=Lax` so the provider's redirect
back brings it, 10 min), and `da_id`, the ID token for the sign-out (sent only to `/ui/sign-out`).

A request that has a session but whose settings are inconsistent, or whose token can't be
checked at the moment, answers 503, not 401.

Signing out removes the browser's cookie. The cluster keeps no list of sessions, so a copy of the
cookie taken before the sign-out still works until the session ends: keep `session_ttl` short where
that matters, or change `session_key` to end every session at once.

The API takes the session as it takes credentials, when a request has no `Authorization` header.
A request that changes something sends the session's anti-forgery token in `X-CSRF-Token`
(`GET /ui/session` gives it); without it the answer is `403`. A live stream (`GET /v1/stream`)
ends with its session, with a `reset` message, and the page opens it again.

## Behind a proxy

- Serve the console and `/v1` at the root of one host, with no path prefix: the console's redirects
  and routes are absolute `/ui/…` paths, the `__Host-` cookies must be `Path=/`, and the provider
  returns to `public_url` + `/ui/oauth2/callback`. `globals.console.public_url` is that origin, with
  no path.
- Terminate TLS in front or on the listener (see above), and pass `Host` through: when a browser
  doesn't say where a form came from (`Sec-Fetch-Site`), the console compares the form's `Origin`
  with `Host`, or with `public_url`'s origin when it is set, so a proxy that rewrites `Host` needs
  `public_url`.
- Don't strip or rewrite the `Cookie` and `Set-Cookie` headers, and don't add a `Domain`.
- Every client shares the proxy's address for the password limit above.
- A session long enough to need three or more cookies (a provider's long tokens) makes a `Cookie`
  header that can exceed a proxy's default header limit. nginx's `large_client_header_buffers` is 8 KB
  by default: raise it in front of the console.
- Without `Authorization`, an API 401 carries `WWW-Authenticate: Session realm="data-avalanche"`
  while the console's sessions are on, so a browser shows no credentials popup. Pass it through.

## Building it from source

The console is built into the binary: the release images carry it. A binary built from a checkout
carries it when `ui/` was built first, with Node.js (the version in `ui/.nvmrc`):

```sh
cd ui && npm ci && npm run build && cd ..
cargo build --release -p data-avalanche
```

Without that, the console's pages say the app isn't built into this binary, and the sign-in and
the API work as before. Set `DATA_AVALANCHE_UI=required` to make the build fail instead.
