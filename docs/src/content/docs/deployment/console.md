---
title: Web console sign-in
description: Switching the web console on, how people sign in under each API auth mode, and what a session is.
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
refresh per refresh token at a time and gives its outcome to the same token for 30 s, since a
provider that rotates refresh tokens refuses one used twice. That is per node: behind a load
balancer two nodes may refresh one session at once, so a provider that rotates refresh tokens
should allow a short reuse window. A session past
its end is never refreshed: it ends when it would have. If the provider's token can't be checked at
the moment (its keys are unreachable), the sign-in callback answers 503; the person tries again.
Signing out removes the session's cookies and, when the provider has an end-session endpoint,
signs out there (see Sessions).

## Sessions

With `basic` and `oauth2`, signing in starts a session: an encrypted cookie that every node reads,
so a load balancer may send each request anywhere. It ends `session_ttl` after sign-in (8 h by
default), however it is used, and sooner when:

- the basic user or password changes;
- the provider's token expires and can't be refreshed;
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
long tokens) goes on in `__Host-da_session.1` to `.3`. With `oauth2`, `da_oauth2` (the sign-in in
progress) and `da_id` (the ID token, for the sign-out) are also set, each scoped to its own path.

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
  and routes are absolute `/ui/…` paths, the `__Host-` cookie must be `Path=/`, and the provider
  returns to `public_url` + `/ui/oauth2/callback`. `globals.console.public_url` is that origin, with
  no path.
- Terminate TLS in front or on the listener (see above), and pass `Host` through: the console
  compares a form's `Origin` with it.
- Don't strip or rewrite the `Cookie` and `Set-Cookie` headers, and don't add a `Domain`.
- Every client shares the proxy's address for the password limit above.
- A session long enough to need three or more cookies (a provider's long tokens) makes a `Cookie`
  header that can exceed a proxy's default header limit. nginx's `large_client_header_buffers` is 8 KB
  by default: raise it in front of the console.
- Without `Authorization`, an API 401 carries `WWW-Authenticate: Session realm="data-avalanche"`
  while the console's sessions are on, so a browser shows no credentials popup. Pass it through.
